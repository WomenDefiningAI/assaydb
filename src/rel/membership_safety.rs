//! Shared semantic barrier for every membership/key movement, including demand.
//! K1's aggregate proof uses physical row/type bounds, never selectivity guesses.
//! These checks prove value-error totality, not the absence of resource errors.
use crate::{
    bind::{Aggregate, Output, Plan},
    expr::{Expr, Kind, Literal, Op},
    rel::{JoinKind, Node, RelOp},
    types::Type,
};
use arrow::datatypes::DataType;

const LIMIT: u128 = 10_u128.pow(38);

fn bounded(value: Option<u128>) -> Option<u128> {
    value.filter(|n| *n < LIMIT)
}

// Only a Scan's estimated_rows is the exact physical row count from its
// immutable Parquet metadata. Never use a filter/group/join cardinality guess.
fn rows(node: &Node) -> Option<u128> {
    match &node.op {
        RelOp::Scan { .. } => Some(node.estimated_rows as u128),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => rows(input),
        RelOp::Unary { input, plan } if !plan.aggregation || !plan.groups.is_empty() => rows(input),
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => rows(left)?.checked_mul(rows(right)?),
        _ => None,
    }
}

fn column_bound(node: &Node, column: usize) -> Option<u128> {
    match &node.op {
        RelOp::Scan { .. } => match node.schema.fields().get(column)?.data_type() {
            DataType::Int32 => Some(1 << 31),
            DataType::Int64 => Some(1 << 63),
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision) && (0..=38).contains(scale) =>
            {
                Some(10_u128.pow(u32::from(*precision)) - 1)
            }
            DataType::Null => Some(0),
            _ => None,
        },
        RelOp::Rename { input }
        | RelOp::Filter { input, .. }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => column_bound(input, column),
        RelOp::Project { input, columns } => column_bound(input, *columns.get(column)?),
        // Join predicates use the combined input scope even for SEMI/ANTI.
        // Null extension can only decrease a column's absolute bound.
        RelOp::Join { left, right, .. } => {
            let width = left.schema.fields().len();
            if column < width {
                column_bound(left, column)
            } else {
                column_bound(right, column.checked_sub(width)?)
            }
        }
        RelOp::Unary { input, plan } => match plan.output.get(column)? {
            Output::Expr(e) => magnitude(e, input),
            Output::Group(i) => magnitude(plan.groups.get(*i)?, input),
            Output::Aggregate(i) => {
                let aggregate = plan.aggregates.get(*i)?;
                match aggregate.function {
                    Aggregate::Count => rows(input),
                    Aggregate::Sum => sum_bound(aggregate.input.as_ref()?, input),
                    Aggregate::Min | Aggregate::Max => magnitude(aggregate.input.as_ref()?, input),
                    // With a nonempty group, count * 10^scale >= 1. Twice
                    // the absolute sum bound also covers binary64 rounding;
                    // the aggregate's own totality is checked at admission.
                    Aggregate::Avg => sum_bound(aggregate.input.as_ref()?, input)?.checked_mul(2),
                }
            }
        },
        _ => None,
    }
}

fn scaled_bound(e: &Expr, input: &Node, scale: i8) -> Option<u128> {
    let delta = u32::try_from(scale.checked_sub(e.ty.scale().ok()?)?).ok()?;
    bounded(magnitude(e, input)?.checked_mul(10_u128.checked_pow(delta)?))
}

// Absolute mantissa bounds prove every exact intermediate, including mixed
// signs. Float bounds use real units with conservative rounding slack, never
// estimates or observed values. Neither bound authorizes reassociation.
fn magnitude(e: &Expr, input: &Node) -> Option<u128> {
    match &e.kind {
        Kind::Column(i) => column_bound(input, *i),
        Kind::Literal(Literal::Num(n)) => bounded(Some(n.unsigned_abs())),
        Kind::Literal(Literal::Null) => Some(0),
        Kind::Neg(a) => bounded(magnitude(a, input)),
        Kind::Binary(op @ (Op::Add | Op::Sub | Op::Mul), a, b) if e.ty == Type::Float => {
            let a = float_operand_bound(a, input)?;
            let b = float_operand_bound(b, input)?;
            let result = match op {
                Op::Mul => a.checked_mul(b)?,
                _ => a.checked_add(b)?,
            };
            // All admitted bounds fit u128, far below f64::MAX. Doubling
            // covers upward rounding at every operation, including conversion.
            result.checked_mul(2)
        }
        Kind::Binary(Op::Mul, a, b) => {
            let scale = a.ty.scale().ok()?.checked_add(b.ty.scale().ok()?)?;
            if scale != e.ty.scale().ok()? {
                return None;
            }
            bounded(magnitude(a, input)?.checked_mul(magnitude(b, input)?))
        }
        Kind::Binary(Op::Add | Op::Sub, a, b) => {
            let scale = e.ty.scale().ok()?;
            bounded(scaled_bound(a, input, scale)?.checked_add(scaled_bound(b, input, scale)?))
        }
        _ => None,
    }
}

fn float_operand_bound(e: &Expr, input: &Node) -> Option<u128> {
    let bound = magnitude(e, input)?;
    if e.ty == Type::Float {
        Some(bound)
    } else if (0..=38).contains(&e.ty.scale().ok()?) {
        // Ignoring a nonnegative decimal scale only enlarges the bound.
        bound.checked_mul(2)
    } else {
        None
    }
}

pub(in crate::rel) fn expression_is_total(e: &Expr, input: &Node) -> bool {
    if crate::scan_predicate::total_for_membership(e) {
        return true;
    }
    match &e.kind {
        Kind::Column(_) | Kind::Literal(_) => true,
        Kind::IsNull(a, _) | Kind::Not(a) => expression_is_total(a, input),
        // LIKE only matches Unicode text; SUBSTRING uses character offsets with
        // bounds validated by the binder, returning empty beyond either end.
        // Neither adds a value error, but their operands must still be total.
        Kind::Like(a, b, _) => {
            a.ty == Type::Text
                && b.ty == Type::Text
                && expression_is_total(a, input)
                && expression_is_total(b, input)
        }
        Kind::Substring(a, _, _) => a.ty == Type::Text && expression_is_total(a, input),
        Kind::Binary(Op::And | Op::Or, a, b) => {
            expression_is_total(a, input) && expression_is_total(b, input)
        }
        Kind::Binary(Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge, a, b) => {
            expression_is_total(a, input)
                && expression_is_total(b, input)
                && (a.ty == b.ty
                    || a.ty.scale().ok().zip(b.ty.scale().ok()).is_some_and(
                        |(a_scale, b_scale)| {
                            let scale = a_scale.max(b_scale);
                            scaled_bound(a, input, scale).is_some()
                                && scaled_bound(b, input, scale).is_some()
                        },
                    ))
        }
        Kind::Neg(_) | Kind::Binary(Op::Add | Op::Sub | Op::Mul, _, _) => {
            magnitude(e, input).is_some()
        }
        _ => false,
    }
}

/// One operator-level rule, used by both target traversals and IN rewriting.
/// The caller still preserves cardinality boundaries and remaps demanded keys.
pub(super) fn can_move_membership_across(node: &Node) -> bool {
    match &node.op {
        RelOp::Scan { .. }
        | RelOp::Rename { .. }
        | RelOp::Project { .. }
        | RelOp::Materialize {
            shared_scan: true, ..
        } => true,
        RelOp::Filter { input, predicate } => expression_is_total(predicate, input),
        RelOp::Join {
            kind: JoinKind::Inner,
            ..
        } => join_conditions_are_total(node),
        RelOp::Unary { input, plan } if plan.order.is_empty() && plan.filter.is_none() => {
            (!plan.aggregation || aggregate_is_total(plan, input))
                && plan.output.iter().all(|o| match o {
                    Output::Expr(e) => expression_is_total(e, input),
                    Output::Group(_) | Output::Aggregate(_) => plan.aggregation,
                })
        }
        _ => false,
    }
}

// Predicate totality is distinct from permission to move membership through
// an operator: LEFT/SEMI/ANTI remain cardinality barriers in that traversal.
pub(super) fn join_conditions_are_total(node: &Node) -> bool {
    let RelOp::Join {
        left,
        right,
        keys,
        residual,
        ..
    } = &node.op
    else {
        return false;
    };
    keys.iter()
        .all(|(a, b)| a.ty == b.ty && expression_is_total(a, left) && expression_is_total(b, right))
        && residual
            .as_ref()
            .is_none_or(|e| expression_is_total(e, node))
}

// Aggregate demand can suppress whole groups and evaluates the probe first.
// Prove the entire build subtree total, including operators off the key's path.
pub(super) fn input_is_total(node: &Node) -> bool {
    if let RelOp::Join { left, right, .. } = &node.op {
        return join_conditions_are_total(node) && input_is_total(left) && input_is_total(right);
    }
    if !can_move_membership_across(node) {
        return false;
    }
    match &node.op {
        RelOp::Scan { .. } => true,
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Materialize { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => input_is_total(input),
        _ => false,
    }
}

fn sum_bound(e: &Expr, input: &Node) -> Option<u128> {
    bounded(magnitude(e, input)?.checked_mul(rows(input)?))
}

pub(in crate::rel) fn aggregate_is_total(plan: &Plan, input: &Node) -> bool {
    if plan.groups.is_empty()
        || !plan
            .groups
            .iter()
            .all(|e| matches!(e.kind, Kind::Column(_)))
        || !input_is_total(input)
        || rows(input).is_none_or(|n| n > i64::MAX as u128)
    {
        return false;
    }
    plan.aggregates.iter().all(|a| {
        if a.input
            .as_ref()
            .is_some_and(|e| !matches!(e.kind, Kind::Column(_)))
        {
            return false;
        }
        match a.function {
            // Count fits even a public BIGINT; direct MIN/MAX select existing
            // values without arithmetic. DISTINCT only reduces contributions.
            Aggregate::Count | Aggregate::Min | Aggregate::Max => true,
            Aggregate::Sum | Aggregate::Avg => a
                .input
                .as_ref()
                .is_some_and(|e| e.ty == Type::Null || sum_bound(e, input).is_some()),
        }
    })
}
