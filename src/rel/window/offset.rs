//! Offset windows use the ranking order (including the canonical ordinal).
//! LEAD reverses that entire order, then uses the same bounded history as LAG.
use super::*;
use crate::expr::{Input, Kind, Literal};
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize)]
pub struct Offset {
    pub input: Expr,
    pub default: Expr,
    pub distance: u64,
    pub lead: bool,
    /// LAG/LEAD(x [, n [, default]] IGNORE NULLS) (DuckDB 1.5.6; PostgreSQL 17
    /// has no null treatment): count only rows whose value is not NULL. The
    /// current row's own value is returned for distance 0.
    pub ignore_nulls: bool,
}

fn literal(expr: &s::Expr) -> bool {
    match expr {
        s::Expr::Value(_) | s::Expr::TypedString { .. } => true,
        s::Expr::Nested(e) | s::Expr::Cast { expr: e, .. } => literal(e),
        s::Expr::UnaryOp {
            op: s::UnaryOperator::Plus | s::UnaryOperator::Minus,
            expr,
        } => literal(expr),
        _ => false,
    }
}

fn distance(expr: &s::Expr) -> Result<u64> {
    match expr {
        s::Expr::Nested(e)
        | s::Expr::UnaryOp {
            op: s::UnaryOperator::Plus,
            expr: e,
        } => distance(e),
        s::Expr::Value(s::ValueWithSpan {
            value: s::Value::Number(n, false),
            ..
        }) if n.bytes().all(|b| b.is_ascii_digit()) => n
            .parse::<i64>()
            .map(|n| n as u64)
            .map_err(|_| "unsupported: window offset exceeds BIGINT range".into()),
        _ => Err("unsupported: window offset must be a non-negative integer literal".into()),
    }
}

pub(super) fn bind(
    function: &s::Function,
    input: Expr,
    bind: &impl Fn(&s::Expr) -> Result<Expr>,
) -> Result<Offset> {
    let s::FunctionArguments::List(args) = &function.args else {
        unreachable!()
    };
    let expressions = args
        .args
        .iter()
        .map(|a| match a {
            s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(e)) => Ok(e),
            _ => Err(crate::Error::from(
                "unsupported: offset window requires unnamed expressions",
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let distance = expressions.get(1).map_or(Ok(1), |e| distance(e))?;
    let mut default = match expressions.get(2) {
        Some(e) if literal(e) => bind(e)?,
        Some(_) => return Err("unsupported: window default must be a literal or NULL".into()),
        None => Expr {
            kind: Kind::Literal(Literal::Null),
            ty: Type::Null,
        },
    };
    if input.ty == Type::Null && default.ty != Type::Null {
        return Err(
            "unsupported: untyped NULL offset input with non-NULL default (reference disagreement)"
                .into(),
        );
    }
    if default.ty == Type::Null {
        default.ty = input.ty.clone();
    } else if input.ty != default.ty {
        default = match (&input.ty, &default.ty) {
            (Type::Int, Type::Decimal(_)) => Expr {
                ty: Type::Int,
                kind: Kind::CastDecimal(Box::new(default), 38, 0),
            },
            (Type::Decimal(scale), Type::Int | Type::Decimal(_)) => Expr {
                ty: input.ty.clone(),
                kind: Kind::CastDecimal(Box::new(default), 38, *scale),
            },
            (Type::Float, Type::Int | Type::Decimal(_)) => {
                crate::expr::casts::bind(default, Type::Float)?
            }
            _ => return Err("unsupported: window default requires a compatible type".into()),
        };
    }
    if input.ty == Type::Binary {
        return Err("unsupported: Binary offset windows".into());
    }
    Ok(Offset {
        input,
        default,
        distance,
        lead: function.name.to_string().eq_ignore_ascii_case("lead"),
        ignore_nulls: args.clauses.iter().any(|clause| {
            matches!(
                clause,
                s::FunctionArgumentClause::IgnoreOrRespectNulls(s::NullTreatment::IgnoreNulls)
            )
        }),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    node: &Node,
    spec: &Specification,
    offset: &Offset,
    sorter: Sorter,
    mut restore: Sorter,
    keyed_schema: SchemaRef,
    restore_schema: SchemaRef,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let width = node.schema.fields().len() - 1;
    let argument = width + spec.partition.len() + spec.order.len();
    let budget = ctx.budget.clone();
    let default = offset.default.eval(
        &Input {
            columns: vec![],
            len: 1,
        },
        &budget,
    )?;
    let mut history: VecDeque<(ArrayRef, Reservation)> = VecDeque::new();
    let mut previous: Option<(RecordBatch, Reservation)> = None;
    let metrics = sorter.finish(&mut |batch| {
        ctx.options.cancellation.check()?;
        let _metadata =
            budget.reserve(batch.num_rows().saturating_mul(128).saturating_add(4096))?;
        let mut values = Vec::with_capacity(batch.num_rows());
        let mut credits = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                ctx.options.cancellation.check()?;
            }
            let old = if row == 0 {
                previous.as_ref().map(|(b, _)| (b, 0))
            } else {
                Some((&batch, row - 1))
            };
            if !old.is_some_and(|(b, r)| equal_keys(b, r, &batch, row, width, spec.partition.len()))
            {
                history = VecDeque::new();
            }
            // Compact values, so one long-lived offset cannot pin a whole run.
            let source = batch.column(argument);
            let mut credit = budget.reserve(
                source
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(2048),
            )?;
            let indices = UInt64Array::from(vec![row as u64]);
            let current =
                take(source, &indices, None).map_err(|e| crate::Error::from(e.to_string()))?;
            credit.release_excess(current.get_array_memory_size().saturating_add(512));
            if offset.distance == 0 {
                values.push(current);
                credits.push(credit);
            } else if offset.ignore_nulls {
                // History holds the last `distance` non-NULL values; its
                // front is the value `distance` non-NULL rows back.
                if history.len() as u64 == offset.distance {
                    values.push(history.front().unwrap().0.clone());
                } else {
                    values.push(default.array.clone());
                }
                if !is_sql_null(source.as_ref(), row) {
                    history.push_back((current, credit));
                    if history.len() as u64 > offset.distance {
                        history.pop_front();
                    }
                }
            } else {
                if history.len() as u64 == offset.distance {
                    let (value, lease) = history.pop_front().unwrap();
                    values.push(value);
                    credits.push(lease);
                } else {
                    values.push(default.array.clone());
                }
                history.push_back((current, credit));
            }
        }
        if batch.num_rows() > 0 {
            let mut credit = budget.reserve(
                batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(batch.num_columns() * 1024),
            )?;
            let indices = UInt64Array::from(vec![(batch.num_rows() - 1) as u64]);
            let arrays = batch
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| crate::Error::from(e.to_string())))
                .collect::<Result<Vec<_>>>()?;
            let last = RecordBatch::try_new(keyed_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            credit.release_excess(
                last.get_array_memory_size()
                    .saturating_add(batch.num_columns() * 256),
            );
            previous = Some((last, credit));
            let bytes = values
                .iter()
                .fold(0_usize, |n, a| n.saturating_add(a.get_array_memory_size()));
            let _credit = budget.reserve(bytes.saturating_mul(2).saturating_add(2048))?;
            let refs = values.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let result =
                arrow::compute::concat(&refs).map_err(|e| crate::Error::from(e.to_string()))?;
            let mut arrays = batch.columns()[..width].to_vec();
            arrays.push(result);
            arrays.push(batch.column(batch.num_columns() - 1).clone());
            restore.push(
                RecordBatch::try_new(restore_schema.clone(), arrays)
                    .map_err(|e| crate::Error::from(e.to_string()))?,
            )?;
        }
        Ok(())
    })?;
    drop(history);
    drop(previous);
    drop(default);
    sort::merge_metrics(&mut ctx.metrics, metrics);
    let metrics = restore.finish(&mut |batch| {
        ctx.options.cancellation.check()?;
        let _metadata = budget.reserve((width + 1) * 256 + 1024)?;
        emit(
            RecordBatch::try_new(node.schema.clone(), batch.columns()[..width + 1].to_vec())
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}
