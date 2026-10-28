//! Bind unavailable source columns for validation, never invent their values.
use super::*;
mod facts;

fn validation_schema(physical: &Schema, source: &Schema) -> Schema {
    Schema::new(
        physical
            .fields()
            .iter()
            .chain(source.fields())
            .cloned()
            .collect::<Vec<_>>(),
    )
}

// Range evidence follows positional plan identity only. No field names, statistics,
// metadata flags or arithmetic expressions establish a narrow INTEGER range.
fn raw_bigint_column(node: &Node, column: usize, remaining: usize) -> bool {
    if remaining == 0
        || node
            .schema
            .fields()
            .get(column)
            .is_none_or(|field| Type::from_field(field).ok() != Some(Type::Int))
    {
        return false;
    }
    let next = remaining - 1;
    match &node.op {
        RelOp::Scan { .. } => matches!(
            node.schema.field(column).data_type(),
            arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
        ),
        RelOp::Rename { input }
        | RelOp::Filter { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Materialize { input, .. }
        | RelOp::Scalar { input } => raw_bigint_column(input, column, next),
        RelOp::WindowOrder { input, .. } => raw_bigint_column(input, column, next),
        RelOp::Project { input, columns } => columns
            .get(column)
            .is_some_and(|source| raw_bigint_column(input, *source, next)),
        RelOp::Unary { plan, input } => {
            let expression = match plan.output.get(column) {
                Some(bind::Output::Expr(e)) => Some(e),
                Some(bind::Output::Group(group)) => plan.groups.get(*group),
                _ => None,
            };
            expression.is_some_and(|e| {
                if let crate::expr::Kind::Column(source) = e.kind {
                    e.ty == Type::Int && raw_bigint_column(input, source, next)
                } else {
                    false
                }
            })
        }
        RelOp::UnionAll { left, right } => {
            raw_bigint_column(left, column, next) && raw_bigint_column(right, column, next)
        }
        RelOp::Join { left, right, .. } => {
            if column < left.schema.fields().len() {
                raw_bigint_column(left, column, next)
            } else {
                raw_bigint_column(right, column - left.schema.fields().len(), next)
            }
        }
        RelOp::Mark { left, .. }
        | RelOp::TypedMark { left, .. }
        | RelOp::CorrelatedMark { left, .. } => {
            column < left.schema.fields().len() && raw_bigint_column(left, column, next)
        }
        _ => false,
    }
}

// The appended validation-only fields belong to this exact grouping input.
// Pointer identity of its shared Schema prevents matching an unrelated relation
// that merely has identical names/types. Unknown wrappers conservatively decline.
fn grouping_source<'a>(node: &'a Node, source: &Schema, remaining: usize) -> Option<&'a Node> {
    if remaining == 0 {
        return None;
    }
    match &node.op {
        RelOp::Unary { plan, input } if plan.aggregation => {
            std::ptr::eq(input.schema.as_ref(), source).then_some(input.as_ref())
        }
        RelOp::WindowOrder { input, .. }
        | RelOp::Window { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Materialize { input, .. } => grouping_source(input, source, remaining - 1),
        RelOp::Mark { left, .. }
        | RelOp::TypedMark { left, .. }
        | RelOp::CorrelatedMark { left, .. } => grouping_source(left, source, remaining - 1),
        _ => None,
    }
}
fn validation_narrow_columns(input: &Node, source: &Schema) -> Vec<bool> {
    let origin = grouping_source(input, source, 128);
    (0..input.schema.fields().len())
        .map(|column| raw_bigint_column(input, column, 128))
        .chain(
            (0..source.fields().len())
                .map(|column| origin.is_some_and(|node| raw_bigint_column(node, column, 128))),
        )
        .collect()
}

pub(super) fn expression(expr: &s::Expr, input: &Node, source: &Schema) -> Result<Expr> {
    let physical = input.schema.as_ref();
    let mut input_facts = crate::mixed_numeric::binding_facts(input)?;
    let original = match bind_node_expression(input, expr) {
        Ok(bound) => return Ok(bound),
        Err(error) if error.contains("unknown or ambiguous column:") => error,
        Err(error) => return Err(error),
    };
    input_facts.extend(crate::mixed_numeric::fields(source)?);
    let mut bound = Binder {
        schema: &validation_schema(physical, source),
        table: None,
        facts: Some(&input_facts),
    }
    .expr(expr)?;
    facts::simplify(
        &mut bound,
        &validation_schema(physical, source),
        &validation_narrow_columns(input, source),
    );
    let mut used = std::collections::BTreeSet::new();
    bound.columns(&mut used);
    if used.iter().any(|index| *index >= physical.fields().len()) {
        return Err(original);
    }
    // Physical fields are an unchanged prefix. Only validation-only fields
    // eliminated by ordinary bound-expression reachability can disappear.
    Ok(bound)
}

// A value-independent HAVING expression may still contain checked casts of
// unavailable source columns. Validate those casts on every row of each group
// before releasing the constant result. This is conservative: any out-of-range
// input refuses, and no arbitrary grouped representative is ever invented.
// Only eager expression trees and direct integer columns (optionally negated)
// qualify; CASE, subqueries, functions and other conversions keep their barriers.
pub(super) fn checked_cast_having(
    expr: &s::Expr,
    input: &mut Node,
    source: &Schema,
) -> Result<Option<Expr>> {
    use crate::expr::{Kind, Literal, Op, cast_dialect::Target};
    fn source_integer(e: &Expr, width: usize) -> bool {
        e.ty == Type::Int
            && match &e.kind {
                Kind::Column(i) => *i >= width,
                Kind::Neg(a) => source_integer(a, width),
                _ => false,
            }
    }
    fn collect(e: &mut Expr, width: usize, casts: &mut Vec<Expr>, left: &mut usize) -> bool {
        if *left == 0 {
            return false;
        }
        *left -= 1;
        match &mut e.kind {
            Kind::DialectCast(a, Target::Integer | Target::BigInt) if source_integer(a, width) => {
                casts.push(e.clone());
                let Kind::DialectCast(a, _) =
                    std::mem::replace(&mut e.kind, Kind::Literal(Literal::Null))
                else {
                    unreachable!()
                };
                // A temporary proof expression, never installed in the plan.
                e.kind = Kind::Cast(a);
                true
            }
            Kind::Binary(_, a, b) | Kind::NullIf(a, b) => {
                collect(a, width, casts, left) && collect(b, width, casts, left)
            }
            Kind::Neg(a) | Kind::Not(a) | Kind::IsNull(a, _) => collect(a, width, casts, left),
            Kind::Literal(_) | Kind::Column(_) => true,
            _ => false,
        }
    }
    let RelOp::Unary {
        plan,
        input: grouping_input,
    } = &input.op
    else {
        return Ok(None);
    };
    if !plan.aggregation || !std::ptr::eq(grouping_input.schema.as_ref(), source) {
        return Ok(None);
    }
    let width = input.schema.fields().len();
    let schema = validation_schema(&input.schema, source);
    let mut input_facts = crate::mixed_numeric::binding_facts(input)?;
    input_facts.extend(crate::mixed_numeric::fields(source)?);
    let mut constant = Binder {
        schema: &schema,
        table: None,
        facts: Some(&input_facts),
    }
    .expr(expr)?;
    let mut casts = Vec::new();
    if !collect(&mut constant, width, &mut casts, &mut 256) || casts.is_empty() {
        return Ok(None);
    }
    facts::simplify(
        &mut constant,
        &schema,
        &validation_narrow_columns(input, source),
    );
    if constant.ty != Type::Bool
        || !matches!(
            constant.kind,
            Kind::Literal(Literal::Null | Literal::Bool(_))
        )
    {
        return Ok(None);
    }
    let mapping = (0..width)
        .map(|_| None)
        .chain((0..source.fields().len()).map(Some))
        .collect::<Vec<_>>();
    for cast in &mut casts {
        cast.remap_columns(&mapping)?;
    }
    let RelOp::Unary { plan, .. } = &mut input.op else {
        unreachable!()
    };
    let mut schema_fields = input.schema.fields().to_vec();
    let mut predicate = constant;
    for cast in casts {
        let mut used = std::collections::BTreeSet::from_iter(plan.columns.iter().copied());
        cast.columns(&mut used);
        plan.columns = used.into_iter().collect();
        let aggregate = plan.aggregates.len();
        let column = plan.output.len();
        plan.aggregates.push(bind::Agg {
            function: bind::Aggregate::Count,
            input: Some(cast),
            ty: Type::Int,
            distinct: false,
        });
        plan.output.push(bind::Output::Aggregate(aggregate));
        let mut name = format!("__assay_checked_cast_{column}");
        while source.fields().iter().chain(schema_fields.iter()).any(|f| {
            f.name()
                .rsplit('.')
                .next()
                .is_some_and(|n| n.eq_ignore_ascii_case(&name))
        }) {
            name.push('_');
        }
        plan.names.push(name.clone());
        plan.types.push(Type::Int);
        schema_fields.push(Arc::new(Type::Int.field(&name, false)));
        // Count is always nonnegative. Keeping this read in HAVING prevents
        // projection pruning from discarding the required validation aggregate.
        let checked = Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                Op::Ge,
                Box::new(Expr {
                    ty: Type::Int,
                    kind: Kind::Column(column),
                }),
                Box::new(Expr {
                    ty: Type::Int,
                    kind: Kind::Literal(Literal::Num(0)),
                }),
            ),
        };
        predicate = Expr {
            ty: Type::Bool,
            kind: Kind::Binary(Op::And, Box::new(checked), Box::new(predicate)),
        };
    }
    input.schema = Arc::new(Schema::new(schema_fields));
    Ok(Some(predicate))
}

pub(super) fn projection(query: &s::Query, input: Node, source: &Schema) -> Result<Node> {
    let input_facts = crate::mixed_numeric::binding_facts(&input)?;
    let plan = match bind::bind_with_facts(query, &input.schema, &input_facts) {
        Ok(plan) => plan,
        Err(original) if original.contains("unknown or ambiguous column:") => {
            let schema = validation_schema(&input.schema, source);
            let mut facts = input_facts.clone();
            facts.extend(crate::mixed_numeric::fields(source)?);
            let mut plan = bind::bind_with_facts(query, &schema, &facts)?;
            // This is the post-aggregate projection, never an aggregate input
            // rewrite. Keep all aggregation/cardinality/error work upstream.
            if plan.aggregation {
                return Err(original);
            }
            let mut used = std::collections::BTreeSet::new();
            let narrow_columns = validation_narrow_columns(&input, source);
            if let Some(filter) = &mut plan.filter {
                facts::simplify(filter, &schema, &narrow_columns);
                filter.columns(&mut used);
            }
            for output in &mut plan.output {
                if let bind::Output::Expr(expr) = output {
                    facts::simplify(expr, &schema, &narrow_columns);
                    expr.columns(&mut used);
                }
            }
            plan.columns = used.into_iter().collect();
            if plan
                .columns
                .iter()
                .any(|index| *index >= input.schema.fields().len())
            {
                return Err(original);
            }
            plan
        }
        Err(error) => return Err(error),
    };
    Ok(Node {
        schema: schema(&plan.names, &plan.types),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            plan,
            input: Box::new(input),
        },
    })
}

/// SQLite resolves an input name before a SELECT alias. Ambiguous input names
/// therefore cannot silently fall through to an output alias.
pub(super) fn having_aliases(select: &mut s::Select, source: &Schema) -> Result<()> {
    let Some(having) = &mut select.having else {
        return Ok(());
    };
    syntax::rewrite(having, &mut |expr| {
        let s::Expr::Identifier(name) = expr else {
            return Ok(false);
        };
        let input_name = source.fields().iter().any(|field| {
            if name.quote_style.is_some() {
                field.name() == &name.value
                    || (!name.value.contains('.')
                        && field.name().rsplit('.').next() == Some(name.value.as_str()))
            } else {
                let name = name.value.to_lowercase();
                field.name().to_lowercase() == name
                    || (!name.contains('.')
                        && field.name().to_lowercase().rsplit('.').next() == Some(name.as_str()))
            }
        });
        if input_name {
            return Ok(false);
        }
        let mut aliases = select.projection.iter().filter_map(|item| match item {
            s::SelectItem::ExprWithAlias { expr, alias }
                if if name.quote_style.is_some() {
                    alias.value == name.value
                } else {
                    alias.value.eq_ignore_ascii_case(&name.value)
                } =>
            {
                Some(expr)
            }
            _ => None,
        });
        let Some(replacement) = aliases.next() else {
            return Ok(false);
        };
        // Leave duplicate alias ambiguity to normal binding rather than picking
        // an arbitrary expression or extending this repair's naming policy.
        if aliases.next().is_some() {
            return Ok(false);
        }
        *expr = replacement.clone();
        Ok(true)
    })
}
