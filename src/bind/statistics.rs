//! Statistical and boolean aggregates (maintainer scope, October 17, 2026).
//!
//! DuckDB 1.5.6 and PostgreSQL 17 agree on the accepted forms; where they
//! differ the form is refused (SEMANTICS-LOG.md, "Statistical and boolean
//! aggregates"). BOOL_AND/BOOL_OR and COUNT_IF are rewritten to existing
//! aggregates: MIN/MAX over BOOLEAN (false < true) and SUM over 1/0.
use super::*;

const ONE: &[(&str, Moment)] = &[
    ("stddev", Moment::StddevSamp),
    ("stddev_samp", Moment::StddevSamp),
    ("stddev_pop", Moment::StddevPop),
    ("variance", Moment::VarSamp),
    ("var_samp", Moment::VarSamp),
    ("var_pop", Moment::VarPop),
];
const TWO: &[(&str, Moment)] = &[
    ("covar_samp", Moment::CovarSamp),
    ("covar_pop", Moment::CovarPop),
    ("corr", Moment::Corr),
    ("regr_slope", Moment::RegrSlope),
    ("regr_intercept", Moment::RegrIntercept),
    ("regr_r2", Moment::RegrR2),
    ("regr_sxx", Moment::RegrSxx),
    ("regr_syy", Moment::RegrSyy),
    ("regr_sxy", Moment::RegrSxy),
    ("regr_avgx", Moment::RegrAvgX),
    ("regr_avgy", Moment::RegrAvgY),
];
const ARG: &[(&str, bool)] = &[
    ("arg_max", false),
    ("max_by", false),
    ("arg_min", true),
    ("min_by", true),
];

fn numeric(ty: &Type) -> bool {
    matches!(ty, Type::Int | Type::Decimal(_) | Type::Float | Type::Null)
}

fn pack(a: Expr, b: Expr) -> Expr {
    Expr {
        ty: Type::Binary,
        kind: Kind::Pack(Box::new(a), Box::new(b)),
    }
}

impl Binder<'_> {
    /// Two-argument statistical aggregates and the ARG_MAX family; `None` for
    /// every other name.
    pub(super) fn statistical_aggregate(
        &self,
        name: &str,
        args: &sql::FunctionArgumentList,
    ) -> Result<Option<Agg>> {
        statistical_with(name, args, &|e| self.expr(e))
    }
}

/// `Binder::statistical_aggregate` with the caller's argument binder (window
/// aggregates bind arguments over the window input).
pub(crate) fn statistical_with(
    name: &str,
    args: &sql::FunctionArgumentList,
    bind: &dyn Fn(&sql::Expr) -> Result<Expr>,
) -> Result<Option<Agg>> {
    let lower = name.to_lowercase();
    let moment = TWO.iter().find(|(n, _)| *n == lower).map(|(_, m)| *m);
    let minimum = ARG.iter().find(|(n, _)| *n == lower).map(|(_, m)| *m);
    if moment.is_none() && minimum.is_none() && lower != "regr_count" {
        return Ok(None);
    }
    let upper = lower.to_uppercase();
    if !args.clauses.is_empty() {
        return Err(unsupported(format!("{upper} clauses")));
    }
    let distinct = args.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct);
    let expressions = args
        .args
        .iter()
        .map(|argument| match argument {
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Ok(e),
            sql::FunctionArg::Unnamed(_) => Err(format!("{upper} requires expressions").into()),
            _ => Err(unsupported("named aggregate arguments")),
        })
        .collect::<Result<Vec<_>>>()?;
    if minimum.is_some() && expressions.len() == 3 {
        return Err(unsupported(format!(
            "{upper} with a count argument (DuckDB returns a LIST; Assay has no LIST type)"
        )));
    }
    if expressions.len() != 2 {
        return Err(format!("{upper} requires two arguments").into());
    }
    let first = bind(expressions[0])?;
    let second = bind(expressions[1])?;
    if let Some(minimum) = minimum {
        if first.ty == Type::Binary || second.ty == Type::Binary {
            return Err(unsupported(format!("{upper} over BLOB")));
        }
        // DISTINCT cannot change which rows reach the extreme value; the
        // permitted results are identical, so it is accepted and ignored.
        return Ok(Some(Agg {
            function: Aggregate::ArgExtreme {
                minimum,
                arg: first.ty.clone(),
                value: second.ty.clone(),
            },
            ty: first.ty.clone(),
            input: Some(pack(first, second)),
            distinct: false,
        }));
    }
    for argument in [&first, &second] {
        if !numeric(&argument.ty) {
            return Err(
                format!("{upper} requires numeric arguments, got {:?}", argument.ty).into(),
            );
        }
    }
    if distinct && (first.ty == Type::Float || second.ty == Type::Float) {
        return Err(unsupported(format!("{upper}(DISTINCT ...) over REAL")));
    }
    let inputs = vec![first.ty.clone(), second.ty.clone()];
    let input = Some(pack(first, second));
    Ok(Some(match moment {
        // REGR_COUNT is COUNT over rows where both arguments are present.
        None => Agg {
            function: Aggregate::Count,
            input,
            ty: Type::Int,
            distinct,
        },
        Some(function) => Agg {
            function: Aggregate::Moment { function, inputs },
            input,
            ty: Type::Float,
            distinct,
        },
    }))
}

/// One-argument statistical and boolean aggregates; `None` for other names.
pub(super) fn bind_one(name: &str, input: Option<Expr>, distinct: bool) -> Result<Option<Agg>> {
    let upper = name.to_uppercase();
    if TWO.iter().any(|(n, _)| *n == name)
        || ARG.iter().any(|(n, _)| *n == name)
        || name == "regr_count"
    {
        return Err(format!("{upper} requires two arguments").into());
    }
    if name == "every" {
        return Err(unsupported(
            "EVERY (DuckDB 1.5.6 has no EVERY; BOOL_AND is equivalent)",
        ));
    }
    let moment = ONE.iter().find(|(n, _)| *n == name).map(|(_, m)| *m);
    let boolean = matches!(name, "bool_and" | "bool_or" | "count_if");
    if moment.is_none() && !boolean {
        return Ok(None);
    }
    let value =
        input.ok_or_else(|| crate::Error::from(format!("{upper} requires an expression")))?;
    if let Some(function) = moment {
        if !numeric(&value.ty) {
            return Err(format!("{upper} requires a numeric argument, got {:?}", value.ty).into());
        }
        return Ok(Some(Agg {
            function: Aggregate::Moment {
                function,
                inputs: vec![value.ty.clone()],
            },
            input: Some(value),
            ty: Type::Float,
            distinct,
        }));
    }
    if !matches!(value.ty, Type::Bool | Type::Null) {
        return Err(format!("{upper} requires a BOOLEAN argument, got {:?}", value.ty).into());
    }
    Ok(Some(match name {
        "bool_and" | "bool_or" => Agg {
            function: if name == "bool_and" {
                Aggregate::Min
            } else {
                Aggregate::Max
            },
            ty: value.ty.clone(),
            input: Some(value),
            distinct,
        },
        // DuckDB 1.5.6 COUNT_IF is SUM(CAST(b AS INTEGER)): NULL for empty and
        // all-NULL input, and DISTINCT applies to the 1/0 values.
        _ => {
            let int = |n: Option<i128>| Expr {
                ty: Type::Int,
                kind: Kind::Literal(n.map_or(Literal::Null, Literal::Num)),
            };
            let input = if value.ty == Type::Null {
                int(None)
            } else {
                Expr {
                    ty: Type::Int,
                    kind: Kind::Case(
                        vec![
                            (value.clone(), int(Some(1))),
                            (bool_expr(Kind::Not(Box::new(value))), int(Some(0))),
                        ],
                        Box::new(int(None)),
                    ),
                }
            };
            Agg {
                function: Aggregate::Sum,
                input: Some(input),
                ty: Type::Int,
                distinct,
            }
        }
    }))
}
