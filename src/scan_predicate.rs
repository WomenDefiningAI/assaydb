//! Whole, total predicates only; the original Filter remains in the plan.
use crate::{
    Result,
    expr::{Expr, Kind, Op},
    rel::{Node, RelOp},
    types::Type,
};

fn value(expr: &Expr) -> bool {
    matches!(
        expr.ty,
        Type::Int | Type::Date | Type::Text | Type::Bool | Type::Null
    ) && matches!(expr.kind, Kind::Column(_) | Kind::Literal(_))
}
pub(crate) fn total(expr: &Expr) -> bool {
    match &expr.kind {
        Kind::Binary(Op::And | Op::Or, a, b) => total(a) && total(b),
        Kind::Binary(Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge, a, b) => {
            value(a) && value(b) && a.ty == b.ty
        }
        Kind::Not(a) => total(a),
        Kind::IsNull(a, _) => value(a),
        Kind::Literal(_) | Kind::Column(_) => expr.ty == Type::Bool && value(expr),
        _ => false,
    }
}

pub(crate) fn target(node: &Node, mut predicate: Expr) -> Result<Option<(usize, Expr)>> {
    match &node.op {
        RelOp::Scan { .. } => Ok(Some((node as *const Node as usize, predicate))),
        RelOp::Rename { input } => target(input, predicate),
        RelOp::Project { input, columns } => {
            predicate.remap_columns(&columns.iter().map(|i| Some(*i)).collect::<Vec<_>>())?;
            target(input, predicate)
        }
        // In particular, never change a shared materialization, cross another
        // filter or projection expression, or move through cardinality barriers.
        _ => Ok(None),
    }
}
