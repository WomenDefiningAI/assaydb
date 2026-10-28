//! Compile repeated aggregate expressions into batch-local value bindings.
//! CASE arms and AND/OR operands are scope barriers: nothing conditional is
//! hoisted out of them.
use crate::{
    Result,
    bind::{Aggregate, Plan},
    expr::{Expr, Input, Kind, Op, Value},
    memory::Budget,
};

pub(crate) struct ExpressionProgram {
    width: usize,
    pub bindings: Vec<Expr>,
    groups: Vec<Expr>,
    inputs: Vec<AggregateInput>,
    pub state_sources: Vec<usize>,
    pub(crate) enabled: bool,
}

// Alias the first structurally equal rewritten input once, during planning.
// Keep the expression inline: no second index vector or per-batch allocation.
enum AggregateInput {
    Evaluate(Option<Expr>),
    Alias(usize),
}

fn children(expr: &Expr) -> Vec<&Expr> {
    match &expr.kind {
        Kind::Binary(_, a, b)
        | Kind::Like(a, b, _)
        | Kind::Position(a, b)
        | Kind::NullIf(a, b)
        | Kind::Pack(a, b) => {
            vec![a, b]
        }
        Kind::Function(_, a)
        | Kind::Cast(a)
        | Kind::DialectCast(a, _)
        | Kind::CastVarchar(a, _)
        | Kind::CastDecimal(a, _, _)
        | Kind::Not(a)
        | Kind::Neg(a)
        | Kind::IsNull(a, _)
        | Kind::Year(a)
        | Kind::Substring(a, _, _) => vec![a],
        Kind::Call(_, args) | Kind::Math(_, args) => args.iter().collect(),
        // Do not visit a CASE condition or arm with the enclosing row scope.
        Kind::Case(_, _) | Kind::Column(_) | Kind::Literal(_) => vec![],
        Kind::SortPack(keys, value) => keys
            .iter()
            .map(|(key, _, _)| key)
            .chain(std::iter::once(value.as_ref()))
            .collect(),
    }
}
fn count(expr: &Expr, candidates: &mut Vec<(Expr, usize)>) {
    if matches!(
        expr.kind,
        Kind::Column(_) | Kind::Literal(_) | Kind::Case(_, _)
    ) {
        return;
    }
    // AND/OR may skip an operand on rows the other operand decides (see
    // expr::guard), so, like CASE, they are scope barriers for hoisting: a
    // guarded operand is never computed for every row of the batch. The
    // whole AND/OR expression itself may still be shared.
    if !matches!(expr.kind, Kind::Binary(Op::And | Op::Or, _, _)) {
        for child in children(expr) {
            count(child, candidates);
        }
    }
    if let Some((_, n)) = candidates.iter_mut().find(|(e, _)| e == expr) {
        *n += 1;
    } else {
        candidates.push((expr.clone(), 1));
    }
}
fn rewrite(expr: &Expr, shared: &[Expr], width: usize) -> Expr {
    if let Some(i) = shared.iter().position(|e| e == expr) {
        return Expr {
            kind: Kind::Column(width + i),
            ty: expr.ty.clone(),
        };
    }
    let mut result = expr.clone();
    match &mut result.kind {
        Kind::Binary(_, a, b)
        | Kind::Like(a, b, _)
        | Kind::Position(a, b)
        | Kind::NullIf(a, b)
        | Kind::Pack(a, b) => {
            **a = rewrite(a, shared, width);
            **b = rewrite(b, shared, width);
        }
        Kind::Function(_, a)
        | Kind::Cast(a)
        | Kind::DialectCast(a, _)
        | Kind::CastVarchar(a, _)
        | Kind::CastDecimal(a, _, _)
        | Kind::Not(a)
        | Kind::Neg(a)
        | Kind::IsNull(a, _)
        | Kind::Year(a)
        | Kind::Substring(a, _, _) => **a = rewrite(a, shared, width),
        Kind::Call(_, args) | Kind::Math(_, args) => {
            for arg in args {
                *arg = rewrite(arg, shared, width);
            }
        }
        Kind::Case(_, _) | Kind::Column(_) | Kind::Literal(_) => {}
        Kind::SortPack(keys, value) => {
            for (key, _, _) in keys {
                *key = rewrite(key, shared, width);
            }
            **value = rewrite(value, shared, width);
        }
    }
    result
}
impl ExpressionProgram {
    pub fn new(plan: &Plan, width: usize, enabled: bool) -> Self {
        let mut candidates = Vec::new();
        if enabled {
            for expr in plan
                .groups
                .iter()
                .chain(plan.aggregates.iter().filter_map(|a| a.input.as_ref()))
            {
                count(expr, &mut candidates);
            }
        }
        let mut shared = Vec::new();
        let mut bindings = Vec::new();
        for (expr, uses) in candidates {
            if uses > 1 {
                bindings.push(rewrite(&expr, &shared, width));
                shared.push(expr);
            }
        }
        let state_sources = plan
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, agg)| {
                if enabled && agg.function.is_ordered_set() {
                    // Ordered-set states hold the group's values only, so
                    // MEDIAN(x) and PERCENTILE_DISC(0.9) ... (ORDER BY x) share.
                    (0..i)
                        .find(|j| {
                            let other = &plan.aggregates[*j];
                            other.function.shares_ordered_state(&agg.function)
                                && !other.distinct
                                && !agg.distinct
                                && agg.input == other.input
                        })
                        .unwrap_or(i)
                } else if enabled && matches!(agg.function, Aggregate::Sum | Aggregate::Avg) {
                    (0..i)
                        .find(|j| {
                            let other = &plan.aggregates[*j];
                            matches!(other.function, Aggregate::Sum | Aggregate::Avg)
                                && agg.input == other.input
                                && agg.distinct == other.distinct
                        })
                        .unwrap_or(i)
                } else {
                    i
                }
            })
            .collect();
        let mut inputs = Vec::with_capacity(plan.aggregates.len());
        for aggregate in &plan.aggregates {
            let expr = aggregate.input.as_ref().map(|e| rewrite(e, &shared, width));
            let source = enabled
                .then(|| {
                    inputs.iter().position(
                        |input| matches!(input, AggregateInput::Evaluate(other) if *other == expr),
                    )
                })
                .flatten();
            inputs.push(match source {
                Some(index) => AggregateInput::Alias(index),
                None => AggregateInput::Evaluate(expr),
            });
        }
        Self {
            width,
            bindings,
            groups: plan
                .groups
                .iter()
                .map(|e| rewrite(e, &shared, width))
                .collect(),
            inputs,
            state_sources,
            enabled,
        }
    }
    pub fn evaluate(
        &self,
        input: &mut Input,
        budget: &Budget,
    ) -> Result<(Vec<Value>, Vec<Option<Value>>)> {
        if input.columns.len() != self.width {
            return Err("expression program input width mismatch".into());
        }
        for expr in &self.bindings {
            let value = expr.eval(input, budget)?;
            input.columns.push(Some(value));
        }
        let keys = self
            .groups
            .iter()
            .map(|e| e.eval(input, budget))
            .collect::<Result<Vec<_>>>()?;
        let mut values: Vec<Option<Value>> = Vec::with_capacity(self.inputs.len());
        for aggregate in &self.inputs {
            values.push(match aggregate {
                AggregateInput::Alias(index) => values[*index].clone(),
                AggregateInput::Evaluate(expr) => {
                    expr.as_ref().map(|e| e.eval(input, budget)).transpose()?
                }
            });
        }
        // Returned values retain their own leases; discard intermediate-only
        // bindings before grouping. Nothing survives into the next input batch.
        input.columns.truncate(self.width);
        Ok((keys, values))
    }
}

/// Physical projection of the already-evaluated group and aggregate inputs.
/// Aliases share one transported column; COUNT(*) retains no input column.
pub(crate) struct Projection {
    schema: arrow::datatypes::SchemaRef,
    groups: usize,
    inputs: Vec<Option<usize>>,
    unique_inputs: Vec<usize>,
    encoded_text: bool,
}
impl ExpressionProgram {
    pub fn projection(&self) -> Projection {
        use arrow::datatypes::{DataType, Field, Schema};
        let physical = |expr: &Expr| {
            if expr.ty == crate::types::Type::Int {
                DataType::Decimal128(38, 0)
            } else {
                expr.ty.arrow()
            }
        };
        let mut fields: Vec<Field> = self
            .groups
            .iter()
            .enumerate()
            .map(|(i, e)| Field::new(format!("group_{i}"), physical(e), true))
            .collect();
        let mut inputs = Vec::with_capacity(self.inputs.len());
        let mut unique_inputs = Vec::new();
        for (i, input) in self.inputs.iter().enumerate() {
            let column = match input {
                AggregateInput::Alias(index) => inputs[*index],
                AggregateInput::Evaluate(Some(expr)) => {
                    let column = fields.len();
                    fields.push(Field::new(format!("input_{i}"), physical(expr), true));
                    unique_inputs.push(i);
                    Some(column)
                }
                AggregateInput::Evaluate(None) => None,
            };
            inputs.push(column);
        }
        let encoded_text = self.enabled
            && self.groups.len() >= 2
            && self.groups.iter().all(|e| e.ty == crate::types::Type::Text);
        if encoded_text {
            fields.push(Field::new("encoded_groups", DataType::LargeBinary, true));
        }
        Projection {
            schema: std::sync::Arc::new(Schema::new(fields)),
            groups: self.groups.len(),
            inputs,
            unique_inputs,
            encoded_text,
        }
    }
}
pub(crate) type ProjectedInputs = (Vec<Value>, Vec<Option<Value>>, Option<crate::keys::Keys>);
impl Projection {
    pub fn prepares_text_keys(&self) -> bool {
        self.encoded_text
    }
    pub fn pack(
        &self,
        rows: usize,
        keys: &[Value],
        values: &[Option<Value>],
        budget: &Budget,
    ) -> Result<(arrow::record_batch::RecordBatch, crate::memory::Reservation)> {
        let mut arrays: Vec<arrow::array::ArrayRef> = keys
            .iter()
            .map(|v| v.array.clone())
            .chain(
                self.unique_inputs
                    .iter()
                    .map(|i| values[*i].as_ref().unwrap().array.clone()),
            )
            .collect();
        // This happens only after evaluation of the complete expression program.
        let encoded_lease = if self.encoded_text {
            let encoded = crate::keys::Keys::new(keys, rows, budget)?;
            let (array, lease) = encoded.into_text_column()?;
            arrays.push(std::sync::Arc::new(array));
            Some(lease)
        } else {
            None
        };
        let batch = arrow::record_batch::RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| e.to_string())?;
        // A bridge to the existing reader/queue owner. This briefly overlaps
        // the Values' leases; no data buffer is copied or left unaccounted.
        let lease = budget.reserve(batch.get_array_memory_size())?;
        drop(encoded_lease);
        Ok((batch, lease))
    }
    pub fn unpack(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        budget: &Budget,
    ) -> Result<ProjectedInputs> {
        if batch.schema() != self.schema {
            return Err("aggregate projection schema mismatch".into());
        }
        let ordinary = self.schema.fields().len() - usize::from(self.encoded_text);
        let columns = batch.columns()[..ordinary]
            .iter()
            .map(|a| Value::adopt(a.clone(), budget))
            .collect::<Result<Vec<_>>>()?;
        let keys = columns[..self.groups].to_vec();
        let values = self
            .inputs
            .iter()
            .map(|i| i.map(|i| columns[i].clone()))
            .collect();
        let encoded = if self.encoded_text {
            let array = batch
                .column(ordinary)
                .as_any()
                .downcast_ref::<arrow::array::LargeBinaryArray>()
                .ok_or("internal: expected prepared text column")?;
            Some(crate::keys::Keys::from_text_column(array.clone(), budget)?)
        } else {
            None
        };
        Ok((keys, values, encoded))
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;
    use crate::{
        bind::Agg,
        expr::{Literal, Op},
        types::Type,
    };
    use arrow::array::{Array, Decimal128Array};
    use std::sync::Arc;

    fn plan(inputs: Vec<Option<Expr>>) -> Plan {
        Plan {
            table: None,
            path: None,
            columns: vec![],
            filter: None,
            groups: vec![],
            aggregates: inputs
                .into_iter()
                .map(|input| Agg {
                    function: Aggregate::Count,
                    input,
                    ty: Type::Int,
                    distinct: false,
                })
                .collect(),
            output: vec![],
            names: vec![],
            types: vec![],
            order: vec![],
            aggregation: true,
            projection_pushdown: false,
        }
    }
    fn number(n: i128) -> Expr {
        Expr {
            kind: Kind::Literal(Literal::Num(n)),
            ty: Type::Int,
        }
    }
    fn lazy(expr: Expr) -> Expr {
        Expr {
            kind: Kind::Case(
                vec![(
                    Expr {
                        kind: Kind::Literal(Literal::Bool(true)),
                        ty: Type::Bool,
                    },
                    expr,
                )],
                Box::new(number(0)),
            ),
            ty: Type::Int,
        }
    }

    #[test]
    fn compiled_aliases_share_lease_and_projection_across_batches() {
        // The enum replaces an existing inline expression slot, with no new
        // per-input allocation or larger descriptor reservation requirement.
        assert!(std::mem::size_of::<AggregateInput>() <= std::mem::size_of::<Option<Expr>>());
        let expression = lazy(Expr {
            kind: Kind::Column(0),
            ty: Type::Int,
        });
        let plan = plan(vec![
            Some(expression.clone()),
            None,
            Some(expression.clone()),
            None,
            Some(expression),
        ]);
        for enabled in [false, true] {
            let program = ExpressionProgram::new(&plan, 1, enabled);
            assert!(program.bindings.is_empty(), "CASE remains a scope barrier");
            assert_eq!(
                program
                    .inputs
                    .iter()
                    .filter(|input| matches!(input, AggregateInput::Alias(_)))
                    .count(),
                if enabled { 3 } else { 0 }
            );
            let projection = program.projection();
            assert_eq!(projection.unique_inputs.len(), if enabled { 1 } else { 3 });
            for expected in [vec![Some(1), None, Some(-7)], vec![None, Some(99)], vec![]] {
                let budget = Budget::new(1 << 20);
                {
                    let array = Decimal128Array::from(expected.clone())
                        .with_precision_and_scale(38, 0)
                        .unwrap();
                    let mut input = Input {
                        len: expected.len(),
                        columns: vec![Some(Value::adopt(Arc::new(array), &budget).unwrap())],
                    };
                    let (keys, values) = program.evaluate(&mut input, &budget).unwrap();
                    assert_eq!(input.columns.len(), 1);
                    for index in [0, 2, 4] {
                        assert_eq!(
                            crate::expr::numeric(&values[index].as_ref().unwrap().array)
                                .iter()
                                .collect::<Vec<_>>(),
                            expected
                        );
                    }
                    assert!(values[1].is_none() && values[3].is_none());
                    if enabled {
                        assert!(Arc::ptr_eq(
                            &values[0].as_ref().unwrap().array,
                            &values[4].as_ref().unwrap().array
                        ));
                    }
                    let (packed, credit) = projection
                        .pack(expected.len(), &keys, &values, &budget)
                        .unwrap();
                    let (_, unpacked, _) = projection.unpack(&packed, &budget).unwrap();
                    assert_eq!(
                        crate::expr::numeric(&unpacked[2].as_ref().unwrap().array)
                            .iter()
                            .collect::<Vec<_>>(),
                        expected
                    );
                    drop(unpacked);
                    drop(packed);
                    drop(credit);
                    // Aliased output remains owned after its original input dies.
                    drop(input);
                    assert_eq!(values[4].as_ref().unwrap().array.len(), expected.len());
                    assert!(budget.used() > 0);
                }
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn compiled_aliases_preserve_first_fallible_input_and_cleanup() {
        // Keep a fallible Int-typed input after INTEGER zero division became NULL.
        let mut numerator = number(1);
        numerator.ty = Type::Decimal(0);
        let divide = lazy(Expr {
            ty: Type::Int,
            kind: Kind::Cast(Box::new(
                crate::bind::binary(Op::Div, numerator, number(0)).unwrap(),
            )),
        });
        let overflow =
            lazy(crate::bind::binary(Op::Mul, number(10_i128.pow(38) - 1), number(2)).unwrap());
        for (first, second, error) in [
            (divide.clone(), overflow.clone(), "division by zero"),
            (overflow, divide, "numeric overflow"),
        ] {
            let plan = plan(vec![Some(first.clone()), Some(first), Some(second)]);
            for enabled in [false, true] {
                let budget = Budget::new(1 << 20);
                let program = ExpressionProgram::new(&plan, 0, enabled);
                let mut input = Input {
                    columns: vec![],
                    len: 3,
                };
                let actual = program.evaluate(&mut input, &budget).err().unwrap();
                assert!(actual.contains(error), "{actual}");
                drop(input);
                assert_eq!(budget.used(), 0);
            }
        }
    }
}
