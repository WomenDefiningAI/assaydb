//! Temporary, additive guard for row-dependent conditional storage classes.
//! Validate after binding; normalization shares conservative reachability facts.
mod normalize;
use crate::{
    Result,
    bind::{Aggregate, Output, Plan},
    expr::{Expr, Kind, Literal, Op},
    rel::{CorrelatedMarker, JoinKind, Node, RelOp},
    types::Type,
};
use arrow::datatypes::Schema;
pub(crate) use normalize::normalize_expression;

const INT: u8 = 1;
const FLOAT: u8 = 2;
const NULL: u8 = 1;
const VALUE: u8 = 2;
const ERROR: &str =
    "unsupported: row-dependent mixed Int/Float conditional requires tagged numeric representation";

#[derive(Clone, Copy)]
pub(crate) struct Fact {
    classes: u8,
    nulls: u8,
    varying: bool,
    conditional: bool,
    boolean: Option<bool>,
    integer: Option<i128>,
}
impl Fact {
    fn typed(ty: &Type, nullable: bool, varying: bool) -> Self {
        Self {
            classes: match ty {
                Type::Int => INT,
                Type::Float => FLOAT,
                _ => 0,
            },
            nulls: if *ty == Type::Null {
                NULL
            } else {
                VALUE | if nullable { NULL } else { 0 }
            },
            varying,
            conditional: false,
            boolean: None,
            integer: None,
        }
    }
    fn mixed(self) -> bool {
        self.conditional && self.varying && self.classes == (INT | FLOAT)
    }
    fn union(self, other: Self) -> Self {
        Self {
            classes: self.classes | other.classes,
            nulls: self.nulls | other.nulls,
            varying: self.varying || other.varying,
            conditional: self.conditional || other.conditional,
            integer: if self.integer == other.integer {
                self.integer
            } else {
                None
            },
            boolean: if self.boolean == other.boolean {
                self.boolean
            } else {
                None
            },
        }
    }
}
pub(crate) fn fields(schema: &Schema) -> Result<Vec<Fact>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            // Unused physical payload types are legal in some relational plans.
            // Their nonnumeric provenance must not add a new type refusal.
            Ok(Fact::typed(
                &Type::from_field(f).unwrap_or(Type::Bool),
                f.is_nullable(),
                true,
            ))
        })
        .collect()
}
fn get(values: &[Fact], index: usize) -> Result<Fact> {
    values
        .get(index)
        .copied()
        .ok_or_else(|| "internal: mixed numeric provenance column".into())
}
// Same bounded, column-free proof used for conditional reachability. Failure
// remains unknown, never a planning-time runtime error. Needed for the approved
// zero-divisor NULL semantics without inventing a reachable COALESCE arm.
fn constant_zero(e: &Expr, op: Op) -> Option<bool> {
    let proven = crate::bind::branch_types::constant(e)?;
    if let Kind::Literal(Literal::Num(n)) = &proven.kind {
        return Some(*n == 0);
    }
    if proven.ty != Type::Float {
        return None;
    }
    let value = proven
        .eval(
            &crate::expr::Input {
                columns: Vec::new(),
                len: 1,
            },
            &crate::memory::Budget::new(65536),
        )
        .ok()?;
    let array = value
        .array
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()?;
    if crate::expr::is_sql_null(array, 0) {
        None
    } else {
        let value = array.value(0);
        if !value.is_finite() {
            None
        } else if op == Op::Mod {
            Some(value as i64 == 0)
        } else {
            Some(value == 0.0)
        }
    }
}
fn expression_mode(e: &Expr, input: &[Fact], guard: bool) -> Result<Fact> {
    let mut result = Fact::typed(&e.ty, true, false);
    match &e.kind {
        Kind::Literal(value) => {
            result.nulls = if matches!(value, Literal::Null) {
                NULL
            } else {
                VALUE
            };
            if result.nulls == NULL {
                result.classes = 0;
            }
            if e.ty == Type::Int
                && let Literal::Num(value) = value
            {
                result.integer = Some(*value);
            }
            if let Literal::Bool(value) = value {
                result.boolean = Some(*value);
            }
        }
        Kind::Column(column) => return get(input, *column),
        Kind::Cast(a) => {
            let a = expression_mode(a, input, guard)?;
            // A conversion fixes the consuming storage class. Its child was
            // still visited, so a cast cannot hide an earlier unsafe division.
            result.nulls = a.nulls;
            result.varying = a.varying;
            // Cast saturation/rounding is not part of this literal/count proof.
            result.integer = None;
            if a.nulls == NULL {
                result.classes = 0;
            }
        }
        Kind::Case(branches, other) => {
            let mut alternatives: Option<Fact> = None;
            let mut reachable_type = Type::Null;
            let mut varying_choice = false;
            let mut reaches_else = true;
            for (condition, value) in branches {
                let condition = expression_mode(condition, input, guard)?;
                if condition.nulls == NULL || condition.boolean == Some(false) {
                    continue;
                }
                // Column-free is not the same as proved. An unresolved
                // Boolean choice can still select either storage class; until
                // tagged values exist, uncertainty must never permit promotion.
                varying_choice |= condition.varying
                    || condition.boolean != Some(true)
                    || condition.nulls != VALUE;
                reachable_type = crate::bind::common_type(&reachable_type, &value.ty)?;
                let value = expression_mode(value, input, guard)?;
                alternatives = Some(alternatives.map_or(value, |old| old.union(value)));
                if condition.boolean == Some(true) && condition.nulls == VALUE {
                    reaches_else = false;
                    break;
                }
            }
            if reaches_else {
                reachable_type = crate::bind::common_type(&reachable_type, &other.ty)?;
                let value = expression_mode(other, input, guard)?;
                alternatives = Some(alternatives.map_or(value, |old| old.union(value)));
            }
            let mut value = alternatives.unwrap_or(Fact::typed(&Type::Null, true, false));
            value.varying |= varying_choice;
            value.conditional |= varying_choice && value.classes == (INT | FLOAT);
            // Temporary all-consumer refusal: an outer CAST, comparison, ABS,
            // aggregate or bare projection must not conceal precision loss at
            // the conditional selection itself. Dead arms were skipped above.
            if guard && value.mixed() {
                return Err(ERROR.into());
            }
            if guard && reachable_type != Type::Null && reachable_type != e.ty {
                // A bounded normalization decline must never become silent
                // promotion merely because final provenance has a stronger
                // context. Ordinary binding has completed before this guard.
                return Err(
                    "unsupported: conditional reachable-type normalization proof limit or scope"
                        .into(),
                );
            }
            return Ok(value);
        }
        Kind::NullIf(a, b) => {
            let same_exact = a == b
                && matches!(
                    a.ty,
                    Type::Int
                        | Type::Decimal(_)
                        | Type::Bool
                        | Type::Text
                        | Type::Binary
                        | Type::Date
                );
            let mut a = expression_mode(a, input, guard)?;
            let b = expression_mode(b, input, guard)?;
            // Both children are visited even when the result class/nullness is
            // known. Comparing against Float never changes the first class.
            if same_exact {
                return Ok(Fact::typed(&Type::Null, true, false));
            }
            if b.nulls != NULL {
                a.nulls |= NULL;
                a.varying |= b.varying;
            }
            return Ok(a);
        }
        Kind::Binary(op, a, b) => {
            let denominator_zero = if matches!(op, Op::Div | Op::Mod)
                && !matches!(a.ty, Type::Decimal(_))
                && !matches!(b.ty, Type::Decimal(_))
            {
                Some(constant_zero(b, *op))
            } else {
                None
            };
            let decimal_float_result = e.ty == Type::Float
                && (matches!(a.ty, Type::Decimal(_)) || matches!(b.ty, Type::Decimal(_)));
            let a = expression_mode(a, input, guard)?;
            let b = expression_mode(b, input, guard)?;
            if guard && matches!(op, Op::Div | Op::Mod) && (a.mixed() || b.mixed()) {
                return Err(ERROR.into());
            }
            result.varying = a.varying || b.varying;
            if let (VALUE, VALUE, Some(a), Some(b)) = (a.nulls, b.nulls, a.integer, b.integer) {
                result.boolean = match op {
                    Op::Eq => Some(a == b),
                    Op::Ne => Some(a != b),
                    Op::Lt => Some(a < b),
                    Op::Le => Some(a <= b),
                    Op::Gt => Some(a > b),
                    Op::Ge => Some(a >= b),
                    _ => None,
                };
            }
            result.nulls = if matches!(op, Op::And | Op::Or) {
                // Kleene operators are not strict: TRUE OR NULL and FALSE AND
                // NULL are non-NULL. Keep all possible Boolean outcomes unless
                // both inputs are proved NULL; never use arithmetic strictness.
                if a.nulls == NULL && b.nulls == NULL {
                    NULL
                } else {
                    a.nulls | b.nulls
                }
            } else if a.nulls == NULL || b.nulls == NULL {
                NULL
            } else {
                a.nulls | b.nulls
            };
            if let Some(zero) = denominator_zero {
                match zero {
                    Some(true) => result.nulls = NULL,
                    Some(false) => {}
                    None => result.nulls |= NULL,
                }
            }
            if matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod)
                && matches!(e.ty, Type::Int | Type::Float)
            {
                let mut classes = 0;
                for left in [INT, FLOAT] {
                    for right in [INT, FLOAT] {
                        if a.classes & left != 0 && b.classes & right != 0 {
                            classes |= if left == FLOAT || right == FLOAT {
                                FLOAT
                            } else {
                                INT
                            };
                        }
                    }
                }
                // Exact DECIMAL operands have no INT/FLOAT class bit, but
                // division or arithmetic with REAL has an actual Float result.
                // Preserve that class for a later conditional alternative.
                if decimal_float_result {
                    classes = FLOAT;
                }
                result.classes = if result.nulls == NULL { 0 } else { classes };
                result.conditional = (a.conditional || b.conditional) && classes == (INT | FLOAT);
            }
        }
        Kind::Neg(a) => {
            let mut a = expression_mode(a, input, guard)?;
            a.integer = a.integer.and_then(i128::checked_neg);
            return Ok(a);
        }
        Kind::Function(name, a) => {
            let a = expression_mode(a, input, guard)?;
            if guard && name == "factorial" && a.mixed() {
                return Err(ERROR.into());
            }
            if name == "abs" {
                let mut a = a;
                a.integer = None;
                return Ok(a);
            }
            result.varying = a.varying;
            result.nulls = a.nulls;
        }
        Kind::IsNull(a, negated) => {
            let a = expression_mode(a, input, guard)?;
            result.nulls = VALUE;
            result.varying = a.varying;
            result.boolean = match a.nulls {
                NULL => Some(!*negated),
                VALUE => Some(*negated),
                _ => None,
            };
        }
        Kind::Not(a) => {
            let a = expression_mode(a, input, guard)?;
            result.varying = a.varying;
            result.nulls = a.nulls;
            result.boolean = a.boolean.map(|value| !value);
        }
        Kind::Position(a, b) | Kind::Like(a, b, _) => {
            let a = expression_mode(a, input, guard)?;
            let b = expression_mode(b, input, guard)?;
            result.varying = a.varying || b.varying;
        }
        Kind::Year(a) | Kind::Substring(a, _, _) => {
            result.varying = expression_mode(a, input, guard)?.varying;
        }
    }
    Ok(result)
}
fn plan_mode(p: &Plan, input: &[Fact], input_empty: bool, guard: bool) -> Result<Vec<Fact>> {
    let mut empty = input_empty;
    if let Some(filter) = &p.filter {
        let filter = expression_mode(filter, input, guard)?;
        empty |= filter.boolean == Some(false) || filter.nulls == NULL;
    }
    let groups = p
        .groups
        .iter()
        .map(|e| expression_mode(e, input, guard))
        .collect::<Result<Vec<_>>>()?;
    let aggregates = p
        .aggregates
        .iter()
        .map(|a| {
            let value = a
                .input
                .as_ref()
                .map(|e| expression_mode(e, input, guard))
                .transpose()?;
            let no_values = empty || value.is_some_and(|v| v.nulls == NULL);
            Ok(match a.function {
                Aggregate::Min | Aggregate::Max if no_values => {
                    Fact::typed(&Type::Null, true, false)
                }
                Aggregate::Min | Aggregate::Max => {
                    let mut value = value.ok_or("internal: missing MIN/MAX provenance input")?;
                    if p.groups.is_empty() {
                        value.nulls |= NULL;
                    }
                    // Even constant input has a data-dependent empty/nonempty result.
                    value.varying = true;
                    value
                }
                Aggregate::Count if no_values => {
                    let mut value = Fact::typed(&a.ty, false, false);
                    value.integer = Some(0);
                    value
                }
                Aggregate::Count => Fact::typed(&a.ty, false, true),
                Aggregate::Sum | Aggregate::Avg if no_values => {
                    Fact::typed(&Type::Null, true, false)
                }
                Aggregate::Sum | Aggregate::Avg => Fact::typed(
                    &a.ty,
                    p.groups.is_empty() || value.is_none_or(|v| v.nulls & NULL != 0),
                    true,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    p.output
        .iter()
        .map(|output| match output {
            Output::Expr(e) => expression_mode(e, input, guard),
            Output::Group(i) => get(&groups, *i),
            Output::Aggregate(i) => get(&aggregates, *i),
        })
        .collect()
}
// Only syntax-proven empty streams, never estimates or table statistics. This
// affects aggregate result reachability, not traversal of executed children.
fn empty_stream(root: &Node, remaining: usize) -> bool {
    if remaining == 0 {
        return false;
    }
    let child = |n: &Node| empty_stream(n, remaining - 1);
    let false_filter = |e: &Expr| {
        matches!(
            crate::bind::branch_types::constant(e).map(|e| e.kind),
            Some(Kind::Literal(Literal::Bool(false) | Literal::Null))
        )
    };
    match &root.op {
        RelOp::Empty => true,
        RelOp::Limit { count, input } => *count == 0 || child(input),
        RelOp::Project { input, .. }
        | RelOp::Rename { input }
        | RelOp::Materialize { input, .. } => child(input),
        RelOp::Filter { predicate, input } => false_filter(predicate) || child(input),
        RelOp::Unary { plan, input } if !plan.aggregation || !plan.groups.is_empty() => {
            child(input) || plan.filter.as_ref().is_some_and(false_filter)
        }
        RelOp::UnionAll { left, right } => child(left) && child(right),
        _ => false,
    }
}
fn node_mode(root: &Node, guard: bool) -> Result<Vec<Fact>> {
    match &root.op {
        RelOp::Scan { .. } | RelOp::Empty => fields(&root.schema),
        RelOp::One => Ok(Vec::new()),
        RelOp::Unary { plan: p, input } => {
            plan_mode(p, &node_mode(input, guard)?, empty_stream(input, 64), guard)
        }
        RelOp::Project { input, columns } => {
            let input = node_mode(input, guard)?;
            columns.iter().map(|i| get(&input, *i)).collect()
        }
        RelOp::Filter { predicate, input } => {
            let input = node_mode(input, guard)?;
            expression_mode(predicate, &input, guard)?;
            Ok(input)
        }
        RelOp::Rename { input } | RelOp::Materialize { input, .. } | RelOp::Limit { input, .. } => {
            node_mode(input, guard)
        }
        RelOp::Scalar { input } => {
            let mut values = node_mode(input, guard)?;
            let empty = empty_stream(input, 64);
            for value in &mut values {
                if empty {
                    *value = Fact::typed(&Type::Null, true, false);
                    continue;
                }
                value.nulls |= NULL;
                value.boolean = None;
                value.integer = None;
                value.varying = true;
            }
            Ok(values)
        }
        RelOp::UnionAll { left, right } => {
            let left = node_mode(left, guard)?;
            let right = node_mode(right, guard)?;
            left.into_iter()
                .enumerate()
                .map(|(i, a)| Ok(a.union(get(&right, i)?)))
                .collect()
        }
        RelOp::Join {
            left,
            right,
            kind,
            keys,
            residual,
        } => {
            let left = node_mode(left, guard)?;
            let mut right = node_mode(right, guard)?;
            for (a, b) in keys {
                expression_mode(a, &left, guard)?;
                expression_mode(b, &right, guard)?;
            }
            if *kind == JoinKind::Left {
                for value in &mut right {
                    value.nulls |= NULL;
                    value.boolean = None;
                    value.integer = None;
                    value.varying = true;
                }
            }
            let combined = left.iter().chain(&right).copied().collect::<Vec<_>>();
            if let Some(residual) = residual {
                expression_mode(residual, &combined, guard)?;
            }
            Ok(if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                left
            } else {
                combined
            })
        }
        RelOp::Mark {
            left, right, value, ..
        }
        | RelOp::TypedMark {
            left, right, value, ..
        } => {
            let mut left = node_mode(left, guard)?;
            node_mode(right, guard)?;
            expression_mode(value, &left, guard)?;
            left.push(Fact::typed(&Type::Bool, true, true));
            Ok(left)
        }
        RelOp::CorrelatedMark {
            left,
            right,
            predicate,
            marker,
        } => {
            let mut left = node_mode(left, guard)?;
            let right = node_mode(right, guard)?;
            expression_mode(
                predicate,
                &left.iter().chain(&right).copied().collect::<Vec<_>>(),
                guard,
            )?;
            let ty = match marker {
                CorrelatedMarker::Count => Type::Int,
                CorrelatedMarker::Exists { .. } => Type::Bool,
            };
            left.push(Fact::typed(&ty, false, true));
            Ok(left)
        }
        RelOp::ScalarDefault {
            input,
            fallback,
            value,
            ..
        } => {
            let mut input = node_mode(input, guard)?;
            let fallback = node_mode(fallback, guard)?;
            input[*value] = get(&input, *value)?.union(get(&fallback, 0)?);
            Ok(input)
        }
    }
}

pub(crate) fn validate_plan(p: &Plan, schema: &Schema) -> Result<()> {
    plan_mode(p, &fields(schema)?, false, true)?;
    Ok(())
}
pub(crate) fn validate_node(root: &Node) -> Result<()> {
    node_mode(root, true)?;
    Ok(())
}
pub(crate) fn validate_expressions<'a>(
    schema: &Schema,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let input = fields(schema)?;
    for e in expressions {
        expression_mode(e, &input, true)?;
    }
    Ok(())
}

/// Planner-owned provenance; never inferred from external Arrow metadata.
pub(crate) fn binding_facts(root: &Node) -> Result<Vec<Fact>> {
    node_mode(root, false)
}
