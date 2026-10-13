//! Facts from the exact one-row source, without removing its aggregation.
use super::*;
use crate::{
    bind::{Aggregate, Output},
    expr::{Kind, Literal},
};

pub(super) fn aggregate_constants(node: &Node) -> Vec<Option<s::Expr>> {
    let RelOp::Unary { input, plan, .. } = &node.op else {
        return Vec::new();
    };
    if !matches!(input.op, RelOp::One) || !plan.groups.is_empty() || plan.filter.is_some() {
        return vec![None; plan.output.len()];
    }
    plan.output
        .iter()
        .map(|output| {
            let Output::Aggregate(index) = output else {
                return None;
            };
            let aggregate = &plan.aggregates[*index];
            let value = aggregate
                .input
                .as_ref()
                .and_then(bind::branch_types::constant);
            let sql = match (&aggregate.function, &aggregate.input, value) {
                (Aggregate::Count, None, _) => "1".to_owned(),
                (Aggregate::Count, Some(_), Some(v)) => {
                    if matches!(v.kind, Kind::Literal(Literal::Null)) {
                        "0"
                    } else {
                        "1"
                    }
                    .to_owned()
                }
                (Aggregate::Sum | Aggregate::Avg | Aggregate::Min | Aggregate::Max, _, Some(v))
                    if matches!(v.kind, Kind::Literal(Literal::Null)) =>
                {
                    match aggregate.ty {
                        Type::Float => "CAST(NULL AS REAL)",
                        Type::Int => "CAST(NULL AS BIGINT)",
                        _ => return None,
                    }
                    .to_owned()
                }
                (
                    Aggregate::Sum | Aggregate::Min | Aggregate::Max,
                    _,
                    Some(Expr {
                        ty: Type::Int,
                        kind: Kind::Literal(Literal::Num(n)),
                    }),
                ) if i64::try_from(n).is_ok() => n.to_string(),
                _ => return None,
            };
            let query = bind::parse(&format!("SELECT {sql}")).ok()?;
            let s::SetExpr::Select(select) = *query.body else {
                return None;
            };
            let s::SelectItem::UnnamedExpr(expr) = select.projection.into_iter().next()? else {
                return None;
            };
            Some(expr)
        })
        .collect()
}
