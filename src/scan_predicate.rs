//! Whole, total predicates only; the original Filter remains in the plan.
use crate::{
    Result,
    expr::{Expr, Kind, Literal, Op},
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

// Prove constant arithmetic without rewriting/evaluating the predicate early.
// Check every intermediate, including scaled operands, just as Expr::eval does.
// In particular, later cancellation cannot justify an overflowing intermediate.
fn numeric_constant(expr: &Expr) -> Option<i128> {
    use crate::types::{checked, factor, rescale};
    let scale = expr.ty.scale().ok()?;
    factor(scale).ok()?;
    let result = match &expr.kind {
        Kind::Literal(Literal::Num(n)) => Some(*n),
        Kind::Neg(a) => numeric_constant(a)?.checked_neg(),
        Kind::Binary(op @ (Op::Add | Op::Sub | Op::Mul), a, b) => {
            let x = numeric_constant(a)?;
            let y = numeric_constant(b)?;
            if *op == Op::Mul {
                x.checked_mul(y)
            } else {
                let x = rescale(x, a.ty.scale().ok()?, scale).ok()?;
                let y = rescale(y, b.ty.scale().ok()?, scale).ok()?;
                if *op == Op::Add {
                    x.checked_add(y)
                } else {
                    x.checked_sub(y)
                }
            }
        }
        _ => return None,
    };
    checked(result).ok()
}

fn numeric_at_scale(expr: &Expr, scale: i8) -> bool {
    let Ok(old) = expr.ty.scale() else {
        return false;
    };
    if matches!(expr.kind, Kind::Literal(Literal::Null)) {
        return true;
    }
    if matches!(expr.kind, Kind::Column(_)) {
        // Column values are normalized to precision 38; increasing their scale
        // could overflow, even when the expression is only a comparison.
        return old == scale;
    }
    numeric_constant(expr).is_some_and(|n| crate::types::rescale(n, old, scale).is_ok())
}

/// A membership filter may cross comparisons whose operands and any rescaling
/// are total. Keep Parquet static-predicate admission (`total`) unchanged.
pub(crate) fn total_for_membership(expr: &Expr) -> bool {
    match &expr.kind {
        Kind::Binary(Op::And | Op::Or, a, b) => total_for_membership(a) && total_for_membership(b),
        Kind::Not(a) => total_for_membership(a),
        Kind::Binary(Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge, a, b) => {
            total(expr)
                || match (a.ty.scale(), b.ty.scale()) {
                    (Ok(sa), Ok(sb)) => {
                        let scale = sa.max(sb);
                        numeric_at_scale(a, scale) && numeric_at_scale(b, scale)
                    }
                    _ => false,
                }
        }
        _ => total(expr),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn membership_numeric_comparisons_prove_every_rescaling_and_intermediate() {
        let schema = Schema::new(vec![
            Field::new("q", DataType::Decimal128(15, 2), true),
            Field::new("other", DataType::Decimal128(38, 2), true),
            Field::new("wide", DataType::Decimal128(38, 0), true),
            Field::new("k", DataType::Int64, true),
            Field::new("tag", DataType::Utf8, true),
        ]);
        for (predicate, expected) in [
            ("q >= 1 AND q <= 1 + 10", true),
            ("q >= 10 AND q <= 10 + 10", true),
            ("q >= 20 AND q <= 20 + 10", true),
            ("q > -2 * 3 + 1", true),
            ("q <= 1.25 + 2.5", true),
            ("q <= other", true),
            ("q >= NULL", true),
            ("NOT (q < 1 OR (q > 1 + 10 AND tag IN ('a','b')))", true),
            ("k <= 1 + 10", true),
            ("q + 1 > 0", false),
            ("q * 10000000000000000000000000000000000000 > 0", false),
            ("q < wide", false),
            ("wide > q", false),
            ("q < 1.001", false),
            ("q < 1000000000000000000000000000000000000", false),
            (
                "q < (99999999999999999999999999999999999999 + 1) - 1",
                false,
            ),
            (
                "q < 99999999999999999999999999999999999999 + 0.01 - 99999999999999999999999999999999999999",
                false,
            ),
            (
                "q > 0 AND k * 10000000000000000000000000000000000000 > 0",
                false,
            ),
        ] {
            let plan = crate::bind::bind(
                &crate::bind::parse(&format!("SELECT q WHERE {predicate}")).unwrap(),
                &schema,
                None,
                true,
            )
            .unwrap();
            assert_eq!(
                total_for_membership(plan.filter.as_ref().unwrap()),
                expected,
                "{predicate}"
            );
        }
    }
}
