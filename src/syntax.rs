//! Traversal of the expression subset, with subqueries treated as scope barriers.
use crate::Result;
use sqlparser::ast as s;
pub fn rewrite(e: &mut s::Expr, f: &mut impl FnMut(&mut s::Expr) -> Result<bool>) -> Result<()> {
    if f(e)? {
        return Ok(());
    }
    use s::Expr::*;
    match e {
        Position {
            expr: left,
            r#in: right,
        }
        | BinaryOp { left, right, .. } => {
            rewrite(left, f)?;
            rewrite(right, f)?;
        }
        Nested(e)
        | UnaryOp { expr: e, .. }
        | IsNull(e)
        | IsNotNull(e)
        | Cast { expr: e, .. }
        | Extract { expr: e, .. } => rewrite(e, f)?,
        Between {
            expr, low, high, ..
        } => {
            rewrite(expr, f)?;
            rewrite(low, f)?;
            rewrite(high, f)?;
        }
        InList { expr, list, .. } => {
            rewrite(expr, f)?;
            for e in list {
                rewrite(e, f)?;
            }
        }
        Like { expr, pattern, .. } => {
            rewrite(expr, f)?;
            rewrite(pattern, f)?;
        }
        Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            rewrite(expr, f)?;
            for e in [substring_from, substring_for].into_iter().flatten() {
                rewrite(e, f)?;
            }
        }
        Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(e) = operand {
                rewrite(e, f)?;
            }
            for w in conditions {
                rewrite(&mut w.condition, f)?;
                rewrite(&mut w.result, f)?;
            }
            if let Some(e) = else_result {
                rewrite(e, f)?;
            }
        }
        Function(fun) => {
            if let s::FunctionArguments::List(args) = &mut fun.args {
                for a in &mut args.args {
                    if let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(e)) = a {
                        rewrite(e, f)?;
                    }
                }
            }
            if let Some(s::WindowType::WindowSpec(spec)) = &mut fun.over {
                for key in &mut spec.partition_by {
                    rewrite(key, f)?;
                }
                for key in &mut spec.order_by {
                    rewrite(&mut key.expr, f)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
pub fn ident(name: &str) -> s::Expr {
    s::Expr::Identifier(s::Ident::new(name))
}
pub fn conjuncts(e: s::Expr) -> Vec<s::Expr> {
    match e {
        s::Expr::Nested(e) => conjuncts(*e),
        s::Expr::BinaryOp {
            left,
            op: s::BinaryOperator::And,
            right,
        } => {
            let mut v = conjuncts(*left);
            v.extend(conjuncts(*right));
            v
        }
        e => vec![e],
    }
}
pub fn and(es: Vec<s::Expr>) -> Option<s::Expr> {
    es.into_iter().reduce(|a, b| s::Expr::BinaryOp {
        left: Box::new(a),
        op: s::BinaryOperator::And,
        right: Box::new(b),
    })
}
/// Aggregate function names bound by `bind::Binder::aggregate` (lowercase).
pub const AGGREGATE_NAMES: &[&str] = &[
    "sum",
    "avg",
    "count",
    "min",
    "max",
    // STRING_AGG(text, separator ORDER BY ...) (src/rel/aggregate_modifiers.rs).
    "string_agg",
    // Statistical and boolean aggregates (src/bind/statistics.rs).
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "covar_samp",
    "covar_pop",
    "corr",
    "regr_slope",
    "regr_intercept",
    "regr_count",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
    "bool_and",
    "bool_or",
    "every",
    "count_if",
    "arg_max",
    "arg_min",
    "max_by",
    "min_by",
    // Ordered-set and distribution aggregates (src/bind/ordered_set.rs).
    "median",
    "quantile",
    "quantile_cont",
    "quantile_disc",
    "percentile_cont",
    "percentile_disc",
    "mode",
    "approx_count_distinct",
];
pub fn is_aggregate(e: &s::Expr) -> bool {
    matches!(e,s::Expr::Function(f) if f.over.is_none() && AGGREGATE_NAMES.contains(&f.name.to_string().to_lowercase().as_str()))
}
