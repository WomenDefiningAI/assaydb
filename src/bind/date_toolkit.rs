//! Bind DATE-only operations without admitting an INTERVAL value/type.
use super::*;
use crate::expr::date::DateUnit;
use crate::expr::timestamp::{MICROS_PER_DAY, MICROS_PER_SECOND, Unit as TimestampUnit};

/// A validated `+/- INTERVAL 'n' UNIT` right operand: a signed count of
/// days, months or (time units) microseconds.
pub(super) struct IntervalShift {
    component: Component,
    count: i64,
    /// WEEK: PostgreSQL parses `INTERVAL '1' WEEK` as `INTERVAL '1'` (one
    /// second) aliased `week`, so every WEEK interval refuses.
    week: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Component {
    Days,
    Months,
    Micros,
}

impl Binder<'_> {
    pub(super) fn date_trunc(&self, args: &[sql::FunctionArg]) -> Result<Expr> {
        let [
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(sql::Expr::Value(
                sql::ValueWithSpan {
                    value: sql::Value::SingleQuotedString(unit),
                    ..
                },
            ))),
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(source)),
        ] = args
        else {
            return Err(unsupported(
                "DATE_TRUNC requires a literal unit and one DATE or TIMESTAMP input",
            ));
        };
        let input = self.expr(source)?;
        if input.ty == Type::TimestampTz {
            let unit = TimestampUnit::parse(unit)
                .filter(|_| !unit.eq_ignore_ascii_case("quarters"))
                .ok_or_else(|| {
                    unsupported(
                        "TIMESTAMPTZ DATE_TRUNC unit; expected microsecond/millisecond/second/minute/hour/day/week/month/quarter/year",
                    )
                })?;
            let zone = crate::zone::statement()?.zone;
            return Ok(Expr {
                ty: Type::TimestampTz,
                kind: Kind::Function(
                    crate::zone::function(&format!("trunc_{}", unit.name()), &zone),
                    Box::new(input),
                ),
            });
        }
        if input.ty == Type::Timestamp {
            // Both references return TIMESTAMP. DuckDB and PostgreSQL differ
            // on CENTURY/MILLENNIUM (2000 vs 2001) and on plural QUARTERS.
            let unit = TimestampUnit::parse(unit)
                .filter(|_| !unit.eq_ignore_ascii_case("quarters"))
                .ok_or_else(|| {
                    unsupported(
                        "TIMESTAMP DATE_TRUNC unit; expected microsecond/millisecond/second/minute/hour/day/week/month/quarter/year",
                    )
                })?;
            return Ok(Expr {
                ty: Type::Timestamp,
                kind: Kind::Function(format!("$ts_trunc_{}", unit.name()), Box::new(input)),
            });
        }
        let unit = DateUnit::parse(unit)
            .ok_or_else(|| unsupported("DATE_TRUNC unit; expected year/quarter/month/week/day"))?;
        if input.ty != Type::Date {
            return Err(unsupported("DATE_TRUNC requires DATE or TIMESTAMP input"));
        }
        Ok(Expr {
            ty: Type::Date,
            kind: Kind::Function(format!("$date_trunc_{}", unit.name()), Box::new(input)),
        })
    }

    /// DATE +/- a literal INTERVAL: validates the right operand and returns
    /// the shift function name, or None when the right side is no INTERVAL.
    pub(super) fn date_interval_shift(
        op: &sql::BinaryOperator,
        right: &sql::Expr,
    ) -> Result<Option<IntervalShift>> {
        let mut right = right;
        while let sql::Expr::Nested(inner) = right {
            right = inner;
        }
        let sql::Expr::Interval(interval) = right else {
            return Ok(None);
        };
        if !matches!(op, sql::BinaryOperator::Plus | sql::BinaryOperator::Minus) {
            return Err(unsupported(
                "INTERVAL is only supported as DATE +/- a literal interval",
            ));
        }
        if interval.leading_precision.is_some()
            || interval.last_field.is_some()
            || interval.fractional_seconds_precision.is_some()
        {
            return Err(unsupported("compound or precision-qualified INTERVAL"));
        }
        let sql::Expr::Value(sql::ValueWithSpan {
            value: sql::Value::SingleQuotedString(value),
            ..
        }) = interval.value.as_ref()
        else {
            return Err(unsupported(
                "INTERVAL requires a quoted integer; unquoted spelling is not shared with PostgreSQL",
            ));
        };
        let field = interval
            .leading_field
            .as_ref()
            .map(|u| u.to_string().to_ascii_lowercase());
        // Time units: both references read `INTERVAL 'n' HOUR/MINUTE/SECOND`
        // as an exact microsecond span (TIMESTAMP results only).
        let time = match field.as_deref() {
            Some("hour") => Some(3_600 * MICROS_PER_SECOND),
            Some("minute") => Some(60 * MICROS_PER_SECOND),
            Some("second") => Some(MICROS_PER_SECOND),
            _ => None,
        };
        let unit = match time {
            Some(_) => DateUnit::Day,
            None => field
                .as_deref()
                .and_then(DateUnit::parse)
                .filter(|u| *u != DateUnit::Quarter)
                .ok_or_else(|| {
                    unsupported("INTERVAL unit; expected YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND")
                })?,
        };
        let value = value.trim();
        let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(unsupported("INTERVAL requires a whole integer count"));
        }
        let mut count: i64 = value
            .parse()
            .map_err(|_| unsupported("INTERVAL count out of range"))?;
        if let Some(micros) = time {
            count = count
                .checked_mul(micros)
                .ok_or_else(|| unsupported("INTERVAL count out of range"))?;
            if *op == sql::BinaryOperator::Minus {
                count = -count;
            }
            return Ok(Some(IntervalShift {
                component: Component::Micros,
                count,
                week: false,
            }));
        }
        // Both engines store the day/month component in a signed 32-bit field.
        count = count
            .checked_mul(match unit {
                DateUnit::Week => 7,
                DateUnit::Year => 12,
                _ => 1,
            })
            .filter(|n| i32::try_from(*n).is_ok())
            .ok_or_else(|| unsupported("INTERVAL count out of range"))?;
        if *op == sql::BinaryOperator::Minus {
            count = -count;
        }
        let component = if matches!(unit, DateUnit::Month | DateUnit::Year) {
            Component::Months
        } else {
            Component::Days
        };
        Ok(Some(IntervalShift {
            component,
            count,
            week: unit == DateUnit::Week,
        }))
    }

    /// Apply a validated `date_interval_shift` to the bound left operand.
    /// DATE +/- whole days/months keeps the DATE toolkit's DATE result;
    /// TIMESTAMP +/- an interval yields TIMESTAMP, as in both references.
    pub(super) fn date_shift(mut input: Expr, shift: IntervalShift) -> Result<Expr> {
        let time = shift.component == Component::Micros;
        if input.ty == Type::TimestampTz {
            if shift.week {
                return Err(unsupported(
                    "INTERVAL 'n' WEEK (PostgreSQL reads WEEK as a column alias of a one-second INTERVAL); use DAY",
                ));
            }
            // Hours/minutes/seconds are exact; days and months are calendar
            // steps in the session zone (both references).
            let name = match shift.component {
                Component::Micros => format!("$ts_shift_micros_{}", shift.count),
                Component::Days => crate::zone::function(
                    &format!("shift_days_{}", shift.count),
                    &crate::zone::statement()?.zone,
                ),
                Component::Months => crate::zone::function(
                    &format!("shift_months_{}", shift.count),
                    &crate::zone::statement()?.zone,
                ),
            };
            return Ok(Expr {
                ty: Type::TimestampTz,
                kind: Kind::Function(name, Box::new(input)),
            });
        }
        if time && input.ty != Type::Timestamp {
            // DATE + INTERVAL 'n' HOUR returns TIMESTAMP in both references;
            // the DATE toolkit keeps its DATE-only boundary (and NULL's type
            // differs: PostgreSQL resolves it to INTERVAL).
            return Err(unsupported(
                "INTERVAL HOUR/MINUTE/SECOND requires TIMESTAMP input",
            ));
        }
        if shift.week {
            // Correction (Oct 17): live PostgreSQL 17.11 parses
            // `INTERVAL '1' WEEK` as INTERVAL '1' (one second) aliased `week`
            // (and `... WEEK AS x` is a syntax error); DuckDB adds 7 days.
            return Err(unsupported(
                "INTERVAL 'n' WEEK (PostgreSQL reads WEEK as a column alias of a one-second INTERVAL); use DAY",
            ));
        }
        coerce_null(&mut input, &Type::Date);
        if input.ty == Type::Date {
            let component = if shift.component == Component::Months {
                "months"
            } else {
                "days"
            };
            return Ok(Expr {
                ty: Type::Date,
                kind: Kind::Function(
                    format!("$date_shift_{component}_{}", shift.count),
                    Box::new(input),
                ),
            });
        }
        if input.ty != Type::Timestamp {
            return Err(unsupported(
                "INTERVAL arithmetic requires DATE or TIMESTAMP input",
            ));
        }
        let name = match shift.component {
            Component::Months => format!("$ts_shift_months_{}", shift.count),
            Component::Micros => format!("$ts_shift_micros_{}", shift.count),
            Component::Days => format!(
                "$ts_shift_micros_{}",
                shift
                    .count
                    .checked_mul(MICROS_PER_DAY)
                    .ok_or_else(|| unsupported("INTERVAL count out of range"))?
            ),
        };
        Ok(Expr {
            ty: Type::Timestamp,
            kind: Kind::Function(name, Box::new(input)),
        })
    }
}
