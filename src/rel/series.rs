//! Independent integer table-function path. Temporal/lateral overloads remain
//! separate work; supported coercion and NULL/zero contracts have pinned oracle controls.
use super::integer_series::{Endpoint, IntegerSeries};
use super::*;
use crate::expr::{Kind, Literal};

#[derive(Debug, Clone, Serialize)]
pub struct Specification {
    pub arguments: [Expr; 3],
    pub inclusive: bool,
    pub ordinality: bool,
    /// Set only through proven total row-preserving wrappers below LIMIT.
    pub max_rows: Option<u128>,
}

pub(super) fn bind(
    name: &str,
    args: &s::TableFunctionArgs,
    ordinality: bool,
    alias: Option<&s::TableAlias>,
) -> Result<Node> {
    if args.settings.is_some() || !(1..=3).contains(&args.args.len()) {
        return Err("range/generate_series requires one to three positional arguments".into());
    }
    let empty = Schema::empty();
    let mut values = Vec::new();
    for arg in &args.args {
        let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) = arg else {
            return Err("unsupported: named/wildcard series argument".into());
        };
        // Empty scope deliberately refuses dependent/lateral arguments. Their
        // future binder must capture an explicit parameterized scope.
        let mut value = binder(&empty).expr(expr)?;
        let mut syntax = expr;
        while let s::Expr::Nested(inner) = syntax {
            syntax = inner;
        }
        // DuckDB accepts an untyped string literal here, not arbitrary VARCHAR
        // expressions. Keep this local implicit coercion out of public casts.
        let string_literal = matches!(syntax, s::Expr::Value(value)
            if matches!(&value.value, s::Value::SingleQuotedString(_)));
        if !(matches!(value.ty, Type::Int | Type::Null) || value.ty == Type::Text && string_literal)
        {
            return Err("unsupported: integer series requires integer arguments".into());
        }
        if string_literal {
            value = crate::expr::cast_dialect::bind(
                value,
                &s::DataType::Decimal(s::ExactNumberInfo::PrecisionAndScale(38, 0)),
            )?;
        }
        values.push(crate::expr::cast_dialect::bind(
            value,
            &s::DataType::BigInt(None),
        )?);
    }
    let literal = |value| Expr {
        ty: Type::Int,
        kind: Kind::Literal(Literal::Num(value)),
    };
    let arguments = match values.len() {
        1 => [literal(0), values.remove(0), literal(1)],
        2 => [values.remove(0), values.remove(0), literal(1)],
        3 => [values.remove(0), values.remove(0), values.remove(0)],
        _ => unreachable!(),
    };
    let mut names = vec![name.to_owned()];
    if ordinality {
        names.push("ordinality".into());
    }
    let mut fields = names
        .iter()
        .map(|name| {
            Type::Int
                .field(name, false)
                .with_metadata([(Type::UNQUALIFIED_OUTPUT.into(), "true".into())].into())
        })
        .collect::<Vec<_>>();
    if let Some(alias) = alias.filter(|alias| alias.columns.is_empty()) {
        // Output naming differs from identifier binding: g.* exposes g, while
        // g.range still resolves the function's original column name.
        let mut metadata = fields[0].metadata().clone();
        metadata.insert(Type::SERIES_WILDCARD.into(), alias.name.value.clone());
        fields[0] = fields[0].clone().with_metadata(metadata);
    }
    Ok(Node {
        schema: Arc::new(Schema::new(fields)),
        // A planning hint, never a claim that an unknown invocation is empty.
        estimated_rows: 1024,
        op: RelOp::Generate {
            specification: Box::new(Specification {
                arguments,
                inclusive: name == "generate_series",
                ordinality,
                max_rows: None,
            }),
        },
    })
}

/// Does not cross filters, fallible computation, sorting, aggregation, joins,
/// windows, or materialization. Existing non-generator error draining is intact.
pub(super) fn cap_identity_source(node: &mut Node, maximum: u128) {
    match &mut node.op {
        RelOp::Generate { specification } => {
            specification.max_rows =
                Some(specification.max_rows.map_or(maximum, |n| n.min(maximum)));
        }
        RelOp::Rename { input } | RelOp::Project { input, .. } => {
            cap_identity_source(input, maximum)
        }
        RelOp::Limit {
            input,
            count,
            offset,
        } => {
            let demanded = maximum.min(*count as u128);
            let prefix = if demanded == 0 {
                0
            } else {
                *offset as u128 + demanded
            };
            cap_identity_source(input, prefix);
        }
        RelOp::Unary { input, plan }
            if !plan.aggregation
                && plan.filter.is_none()
                && plan.order.is_empty()
                && plan.output.iter().all(|out| {
                    matches!(
                        out,
                        bind::Output::Expr(Expr {
                            kind: Kind::Column(_) | Kind::Literal(_),
                            ..
                        })
                    )
                }) =>
        {
            cap_identity_source(input, maximum);
        }
        _ => (),
    }
}

/// Revisit after CTE inlining and pruning. Walk children first, then propagate
/// each local LIMIT: outer bounds refine inner caps by minimum. Traversal may
/// visit limits *inside* retained materializations, but cap_identity_source never
/// moves an outer consumer's demand across that materialization boundary.
pub(super) fn apply_final_caps(node: &mut Node) {
    // Recursive working relations require their own demand semantics. Do not
    // propagate even local final-pass caps into recursive anchor/step scopes.
    if matches!(
        node.op,
        RelOp::RecursiveCte { .. } | RelOp::RecursiveScan { .. }
    ) {
        return;
    }
    for child in crate::cte::children(node) {
        apply_final_caps(child);
    }
    if let RelOp::Limit {
        input,
        count,
        offset,
    } = &mut node.op
    {
        let maximum = if *count == 0 {
            0
        } else {
            *count as u128 + *offset as u128
        };
        cap_identity_source(input, maximum);
    }
}

fn arguments(spec: &Specification, ctx: &mut Context<'_>) -> Result<[Option<i64>; 3]> {
    let cancel = ctx.options.cancellation.clone();
    let scalar = Input {
        columns: Vec::new(),
        len: 1,
    };
    let mut args = [None; 3];
    // Conversion errors survive LIMIT0 in the pinned DuckDB2 oracle.
    for (index, expression) in spec.arguments.iter().enumerate() {
        cancel.check()?;
        let value = expression.eval(&scalar, &ctx.budget)?;
        if !is_sql_null(value.array.as_ref(), 0) {
            args[index] = Some(
                i64::try_from(crate::expr::numeric(&value.array).value(0))
                    .map_err(|_| crate::Error::from("series argument out of BIGINT range"))?,
            );
        }
    }
    Ok(args)
}

/// LIMIT0 only: no generated rows means no row-dependent computation or step
/// validation, but scalar argument conversions still occur. This path accepts
/// only a single independent generator chain. It never touches a materialization
/// cache or changes any other consumer's Generate cap.
pub(super) fn try_zero_limit(node: &Node, ctx: &mut Context<'_>) -> Result<bool> {
    let spec = match &node.op {
        RelOp::Generate { specification } => specification,
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::WindowOrder { input, .. }
        | RelOp::Materialize { input, .. } => return try_zero_limit(input, ctx),
        _ => return Ok(false),
    };
    arguments(spec, ctx)?;
    Ok(true)
}

pub(super) fn run(
    node: &Node,
    spec: &Specification,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let cancel = ctx.options.cancellation.clone();
    let args = arguments(spec, ctx)?;
    if spec.max_rows == Some(0) {
        return Ok(());
    }
    let mut cursor = IntegerSeries::new(
        args[0],
        args[1],
        args[2],
        if spec.inclusive {
            Endpoint::Inclusive
        } else {
            Endpoint::Exclusive
        },
        &cancel,
    )?;
    let mut remaining = spec.max_rows.unwrap_or(u128::MAX).min(cursor.remaining());
    let mut ordinal = 1u128;
    while remaining > 0 {
        cancel.check()?;
        let size = remaining.min(ctx.options.batch_size.max(1) as u128) as usize;
        let Some(batch) = cursor.next_batch(size, &ctx.budget, &cancel)? else {
            break;
        };
        let (values, credit) = batch.into_parts();
        let len = values.len();
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values))];
        let ordinal_credit = if spec.ordinality {
            let bytes = len
                .checked_mul(8)
                .and_then(|n| n.checked_add(1024))
                .ok_or("series ordinality allocation overflow")?;
            let lease = ctx.budget.reserve(bytes)?;
            let mut ordinals = Vec::new();
            ordinals
                .try_reserve_exact(len)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            for index in 0..len {
                if index % 256 == 0 {
                    cancel.check()?;
                }
                ordinals.push(
                    i64::try_from(ordinal + index as u128)
                        .map_err(|_| crate::Error::from("series ordinality overflow"))?,
                );
            }
            arrays.push(Arc::new(Int64Array::from(ordinals)));
            Some(lease)
        } else {
            None
        };
        let output = RecordBatch::try_new(node.schema.clone(), arrays)
            .map_err(|e| crate::Error::from(e.to_string()))?;
        emit(output)?;
        // Source credits remain alive through emission. Retaining consumers
        // must acquire their usual owned-buffer credits before retaining arrays.
        drop(ordinal_credit);
        drop(credit);
        remaining -= len as u128;
        ordinal += len as u128;
    }
    Ok(())
}

#[cfg(test)]
mod cap_tests {
    use super::*;
    fn caps(sql: &str) -> Vec<Option<u128>> {
        let options = Options::default();
        let prepared = super::super::prepare_with_local_files(
            sql,
            &HashMap::new(),
            &options,
            &HashMap::new(),
            None,
        )
        .unwrap();
        fn collect(node: &mut Node, caps: &mut Vec<Option<u128>>) {
            if let RelOp::Generate { specification } = &node.op {
                caps.push(specification.max_rows);
            }
            for child in crate::cte::children(node) {
                collect(child, caps);
            }
        }
        let mut result = Vec::new();
        collect(prepared.relational.unwrap().as_mut(), &mut result);
        result
    }
    #[test]
    fn exact_composed_caps_and_final_cte_inlining() {
        assert_eq!(
            caps(
                "SELECT range FROM (SELECT * FROM range(10000) LIMIT 9000 OFFSET 100) q LIMIT 2 OFFSET 3"
            ),
            vec![Some(105)]
        );
        assert_eq!(
            caps(
                "SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 1000000000000 OFFSET 20) q LIMIT 1 OFFSET 2"
            ),
            vec![Some(23)]
        );
        assert_eq!(
            caps("WITH r AS (SELECT * FROM range(9223372036854775807)) SELECT * FROM r LIMIT 1"),
            vec![Some(1)]
        );
        assert_eq!(
            caps("SELECT * FROM range(9223372036854775807) LIMIT 0 OFFSET 9223372036854775807"),
            vec![Some(0)]
        );
        assert_eq!(
            caps(
                "SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 2 OFFSET 9223372036854775807) q LIMIT 0"
            ),
            vec![Some(0)]
        );
        assert_eq!(
            caps(
                "SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 0 OFFSET 9223372036854775807) q LIMIT 2"
            ),
            vec![Some(0)]
        );
    }
    #[test]
    fn caps_stop_at_computation_filters_order_and_retained_cte() {
        assert_eq!(
            caps(
                "SELECT * FROM (SELECT CAST(CASE WHEN range=9 THEN 'bad' ELSE '1' END AS BIGINT) AS v FROM range(10) LIMIT 10) q LIMIT 1"
            ),
            vec![None]
        );
        assert_eq!(
            caps("SELECT * FROM range(10) WHERE range>2 LIMIT 1"),
            vec![None]
        );
        assert_eq!(
            caps("SELECT * FROM range(10) ORDER BY range DESC LIMIT 1"),
            vec![None]
        );
        let repeated = caps(
            "WITH r AS (SELECT * FROM range(10)) SELECT a.range FROM r a JOIN r b ON a.range=b.range LIMIT 1",
        );
        assert!(!repeated.is_empty());
        assert!(repeated.iter().all(Option::is_none));
    }
}
