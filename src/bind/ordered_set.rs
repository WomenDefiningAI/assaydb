//! Ordered-set and distribution aggregates (maintainer scope, October 17):
//! MEDIAN, QUANTILE_CONT/QUANTILE_DISC/QUANTILE, PERCENTILE_CONT/PERCENTILE_DISC
//! WITHIN GROUP, MODE and APPROX_COUNT_DISTINCT.
//!
//! The semantics follow DuckDB 1.5.6 and PostgreSQL 17 where they agree; see
//! SEMANTICS-LOG.md (2026-10-17) and tests/fixtures/ordered_set for the
//! independent references. Finalization lives in
//! `execute::aggregation::ordered_set`.
use super::{Agg, Aggregate, Binder, branch_types, unsupported};
use crate::{
    Result,
    expr::{Input, Kind, Literal},
    memory::Budget,
    types::Type,
};
use arrow::array::Float64Array;
use serde::Serialize;
use sqlparser::ast as sql;

/// A constant quantile fraction, without its sign (the sign selects DESC).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Fraction {
    /// `digits / 10^scale` for an exact INTEGER/DECIMAL literal. DuckDB picks
    /// the discrete row with exact integer arithmetic for a DECIMAL fraction.
    pub exact: Option<(i128, i8)>,
    /// The binary64 value both references use for interpolation positions.
    pub value: f64,
}

/// One quantile aggregate. The state is the group's non-NULL input values.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Quantile {
    /// Interpolating (PERCENTILE_CONT/QUANTILE_CONT/numeric MEDIAN) or
    /// discrete (PERCENTILE_DISC/QUANTILE_DISC/QUANTILE/non-numeric MEDIAN).
    pub continuous: bool,
    /// WITHIN GROUP (ORDER BY x DESC) or DuckDB's negative QUANTILE fraction.
    pub descending: bool,
    pub fraction: Fraction,
    /// PERCENTILE_CONT/PERCENTILE_DISC are PostgreSQL functions as well: a
    /// group where PostgreSQL's binary64 computation would select a different
    /// row (or overflow differently) is refused instead of answered.
    pub postgres: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Spelling {
    Median,
    QuantileCont,
    QuantileDisc,
    PercentileCont,
    PercentileDisc,
    Mode,
}

impl Binder<'_> {
    /// Bind an ordered-set aggregate call, or return `None` for other names.
    pub(super) fn ordered_set_aggregate(&self, f: &sql::Function) -> Result<Option<Agg>> {
        self.ordered_set_with(f, &|e| self.expr(e))
    }

    /// `ordered_set_aggregate` with the caller's binder for the value
    /// expression (window aggregates bind it over the window input).
    pub(crate) fn ordered_set_with(
        &self,
        f: &sql::Function,
        bind: &dyn Fn(&sql::Expr) -> Result<crate::expr::Expr>,
    ) -> Result<Option<Agg>> {
        let name = f.name.to_string().to_lowercase();
        let spelling = match name.as_str() {
            "median" => Spelling::Median,
            "quantile_cont" => Spelling::QuantileCont,
            "quantile_disc" | "quantile" => Spelling::QuantileDisc,
            "percentile_cont" => Spelling::PercentileCont,
            "percentile_disc" => Spelling::PercentileDisc,
            "mode" => Spelling::Mode,
            "approx_count_distinct" => return self.approx_count_distinct(f).map(Some),
            _ => return Ok(None),
        };
        if f.filter.is_some()
            || f.over.is_some()
            || f.null_treatment.is_some()
            || f.uses_odbc_syntax
            || !matches!(f.parameters, sql::FunctionArguments::None)
        {
            return Err(unsupported("aggregate modifiers"));
        }
        let args: &[sql::FunctionArg] = match &f.args {
            sql::FunctionArguments::None => &[],
            sql::FunctionArguments::List(list) => {
                if list.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct) {
                    return Err(unsupported(format!("DISTINCT in {}", name.to_uppercase())));
                }
                if !list.clauses.is_empty() {
                    return Err(unsupported(format!(
                        "argument clauses in {}",
                        name.to_uppercase()
                    )));
                }
                &list.args
            }
            sql::FunctionArguments::Subquery(_) => {
                return Err(unsupported("aggregate arguments"));
            }
        };
        let argument = |i: usize| match &args[i] {
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Ok(e),
            _ => Err(crate::Error::from(format!(
                "semantic: {} requires expression arguments",
                name.to_uppercase()
            ))),
        };
        let within = !f.within_group.is_empty();
        let ordered = matches!(
            spelling,
            Spelling::PercentileCont | Spelling::PercentileDisc
        ) || (spelling == Spelling::Mode && within);
        // (fraction expression, value expression, ORDER BY DESC)
        let (fraction, value, descending) = if ordered {
            let expected = usize::from(spelling != Spelling::Mode);
            if !within || f.within_group.len() != 1 || args.len() != expected {
                return Err(format!(
                    "semantic: {} requires {} argument(s) and WITHIN GROUP (ORDER BY one expression)",
                    name.to_uppercase(),
                    expected
                )
                .into());
            }
            let order = &f.within_group[0];
            if order.with_fill.is_some() {
                return Err(unsupported("WITHIN GROUP ... WITH FILL"));
            }
            // NULLS FIRST/LAST has no effect: NULL inputs are skipped.
            let fraction = (expected == 1).then(|| argument(0)).transpose()?;
            (fraction, &order.expr, order.options.asc == Some(false))
        } else {
            if within {
                return Err(format!(
                    "semantic: {} does not take WITHIN GROUP",
                    name.to_uppercase()
                )
                .into());
            }
            let fraction = matches!(spelling, Spelling::QuantileCont | Spelling::QuantileDisc);
            let expected = 1 + usize::from(fraction);
            if args.len() != expected {
                return Err(format!(
                    "semantic: {} requires {expected} argument(s)",
                    name.to_uppercase()
                )
                .into());
            }
            (
                fraction.then(|| argument(1)).transpose()?,
                argument(0)?,
                false,
            )
        };
        let input = bind(value)?;
        let label = name.to_uppercase();
        if input.ty == Type::Binary {
            return Err(unsupported(format!("{label} over BLOB")));
        }
        // TIMESTAMP/TIMESTAMPTZ: DuckDB interpolates MEDIAN/QUANTILE_CONT and
        // PostgreSQL has no MEDIAN and no timestamp PERCENTILE_CONT; the
        // discrete forms and MODE would need a timestamp value store that the
        // ordered-set state does not have yet (integration candidate 3).
        if matches!(input.ty, Type::Timestamp | Type::TimestampTz) {
            return Err(unsupported(format!("{label} over TIMESTAMP")));
        }
        if spelling == Spelling::Mode {
            return Ok(Some(Agg {
                function: Aggregate::Mode { descending },
                ty: input.ty.clone(),
                input: Some(input),
                distinct: false,
            }));
        }
        let numeric = matches!(input.ty, Type::Int | Type::Decimal(_) | Type::Float);
        let continuous = match spelling {
            // DuckDB's MEDIAN interpolates numeric types and picks the lower
            // middle value of other types (PostgreSQL has no MEDIAN).
            Spelling::Median => numeric || input.ty == Type::Date,
            Spelling::QuantileCont | Spelling::PercentileCont => true,
            _ => false,
        };
        if continuous {
            match input.ty {
                Type::Int | Type::Decimal(_) | Type::Float | Type::Null => {}
                Type::Date => {
                    return Err(unsupported(format!(
                        "{label} over DATE (DuckDB interpolates to TIMESTAMP, which Assay does not have yet; PostgreSQL rejects it)"
                    )));
                }
                _ => {
                    return Err(format!(
                        "semantic: {label} requires a numeric argument, got {:?}",
                        input.ty
                    )
                    .into());
                }
            }
        }
        let (fraction, negative) = match fraction {
            None => (
                Fraction {
                    exact: None,
                    value: 0.5,
                },
                false,
            ),
            Some(e) => self.fraction(e, &label, ordered)?,
        };
        let descending = descending || negative;
        if ordered && descending && fraction.value == 0.0 {
            // DuckDB 1.5.6 answers fraction 0 with DESC as if ascending (the
            // minimum); PostgreSQL returns the first value in DESC order.
            return Err(unsupported(format!(
                "{label}(0) WITHIN GROUP (ORDER BY ... DESC): DuckDB returns the minimum and PostgreSQL the maximum"
            )));
        }
        // Interpolation returns DOUBLE for INTEGER (both references) and for
        // DECIMAL (PostgreSQL; maintainer decision October 17 — DuckDB keeps
        // DECIMAL with direction-dependent rounding).
        let ty = if continuous && matches!(input.ty, Type::Int | Type::Decimal(_)) {
            Type::Float
        } else {
            input.ty.clone()
        };
        Ok(Some(Agg {
            function: Aggregate::Quantile(Quantile {
                continuous,
                descending,
                fraction,
                postgres: matches!(
                    spelling,
                    Spelling::PercentileCont | Spelling::PercentileDisc
                ),
            }),
            input: Some(input),
            ty,
            distinct: false,
        }))
    }

    /// A constant fraction in [0, 1] (PERCENTILE_*) or [-1, 1] (DuckDB's
    /// QUANTILE_*, where a negative fraction means descending order).
    fn fraction(&self, e: &sql::Expr, label: &str, ordered: bool) -> Result<(Fraction, bool)> {
        if matches!(e, sql::Expr::Array(_)) {
            return Err(unsupported(format!(
                "{label} with a list of fractions (needs a LIST result type)"
            )));
        }
        let bound = self.expr(e)?;
        let Some(constant) = branch_types::constant(&bound) else {
            return Err(unsupported(format!("{label} with a non-constant fraction")));
        };
        let (exact, value) = match (&constant.kind, &constant.ty) {
            (Kind::Literal(Literal::Null), _) => {
                return Err(unsupported(format!(
                    "{label} with a NULL fraction (DuckDB raises an error, PostgreSQL returns NULL)"
                )));
            }
            (Kind::Literal(Literal::Num(n)), Type::Int) => (Some((*n, 0)), *n as f64),
            (Kind::Literal(Literal::Num(n)), Type::Decimal(s)) => (
                Some((*n, *s)),
                // DuckDB's DECIMAL -> DOUBLE: one correctly rounded division.
                *n as f64 / crate::types::factor(*s)? as f64,
            ),
            (_, Type::Float) => {
                let value = constant.eval(
                    &Input {
                        columns: Vec::new(),
                        len: 1,
                    },
                    &Budget::new(65536),
                )?;
                let value = value
                    .array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("internal: REAL fraction")?
                    .value(0);
                (None, value)
            }
            _ => {
                return Err(format!(
                    "semantic: {label} fraction must be numeric, got {:?}",
                    constant.ty
                )
                .into());
            }
        };
        let low = if ordered { 0.0 } else { -1.0 };
        if !(low..=1.0).contains(&value) {
            return Err(
                format!("semantic: {label} fraction {value} is not between {low} and 1").into(),
            );
        }
        let negative = value < 0.0;
        Ok((
            Fraction {
                exact: exact.map(|(n, s)| (n.abs(), s)),
                value: value.abs(),
            },
            negative,
        ))
    }

    /// APPROX_COUNT_DISTINCT returns the exact distinct count: DuckDB returns a
    /// HyperLogLog estimate, and an exact count always satisfies "approximately
    /// the number of distinct values" (decision recorded in SEMANTICS-LOG.md).
    fn approx_count_distinct(&self, f: &sql::Function) -> Result<Agg> {
        if f.filter.is_some()
            || f.over.is_some()
            || f.null_treatment.is_some()
            || !f.within_group.is_empty()
            || f.uses_odbc_syntax
            || !matches!(f.parameters, sql::FunctionArguments::None)
        {
            return Err(unsupported("aggregate modifiers"));
        }
        let sql::FunctionArguments::List(args) = &f.args else {
            return Err(unsupported("aggregate arguments"));
        };
        let [sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e))] = args.args.as_slice()
        else {
            return Err("semantic: APPROX_COUNT_DISTINCT requires one expression argument".into());
        };
        if !args.clauses.is_empty() {
            return Err(unsupported("argument clauses in APPROX_COUNT_DISTINCT"));
        }
        Agg::bind_input("count", Some(self.expr(e)?), true)
    }
}
