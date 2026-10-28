//! Calendar fields for DATE only. Internal function names reuse the existing
//! unary expression traversal, accounting and conservative totality rules.
use super::{ArrayRef, Date32Array, Op, Result, decimal, numeric};
use crate::types::Type;
use arrow::array::Date32Builder;
use chrono::{Datelike, Duration, NaiveDate};
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
}

impl DateUnit {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "week" => Self::Week,
            "day" => Self::Day,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Week => "week",
            Self::Day => "day",
        }
    }
}

const RANGE: &str = "unsupported: DATE toolkit outside supported AD calendar range";

fn origin() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

fn calendar(days: i32) -> Result<NaiveDate> {
    origin()
        .checked_add_signed(Duration::days(i64::from(days)))
        .filter(|d| d.year() >= 1)
        .ok_or_else(|| RANGE.into())
}

fn date32(date: Option<NaiveDate>) -> Result<i32> {
    let date = date.filter(|d| d.year() >= 1).ok_or(RANGE)?;
    i32::try_from(date.signed_duration_since(origin()).num_days()).map_err(|_| RANGE.into())
}

fn shift_months(date: NaiveDate, months: i64) -> Result<i32> {
    let month = i64::from(date.year()) * 12 + i64::from(date.month0()) + months;
    let year = i32::try_from(month.div_euclid(12)).map_err(|_| RANGE)?;
    let month = month.rem_euclid(12) as u32 + 1;
    // Clamp once at the destination month, not once per intervening month.
    // A leap day plus four years therefore remains a leap day.
    let target = (1..=date.day())
        .rev()
        .find_map(|day| NaiveDate::from_ymd_opt(year, month, day));
    date32(target)
}

/// Unary operations retain the existing expression workspace reservation and
/// conservative (fallible) function treatment in optimizer totality checks.
pub(crate) fn toolkit(name: &str, array: &ArrayRef) -> Result<ArrayRef> {
    let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
    let trunc = name.strip_prefix("$date_trunc_").and_then(DateUnit::parse);
    let shift = name.strip_prefix("$date_shift_").map(|s| {
        let (component, count) = s.split_once('_').expect("bound DATE shift");
        (component, count.parse::<i64>().expect("bound DATE count"))
    });
    let mut output = Date32Builder::with_capacity(array.len());
    for days in dates.iter() {
        let value = days
            .map(|days| {
                let date = calendar(days)?;
                if let Some(unit) = trunc {
                    date32(match unit {
                        DateUnit::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1),
                        DateUnit::Quarter => {
                            NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1)
                        }
                        DateUnit::Month => date.with_day(1),
                        DateUnit::Week => date.checked_sub_signed(Duration::days(i64::from(
                            date.weekday().num_days_from_monday(),
                        ))),
                        DateUnit::Day => Some(date),
                    })
                } else {
                    let (component, count) = shift.expect("bound DATE toolkit function");
                    if component == "months" {
                        shift_months(date, count)
                    } else {
                        date32(date.checked_add_signed(Duration::days(count)))
                    }
                }
            })
            .transpose()?;
        output.append_option(value);
    }
    Ok(Arc::new(output.finish()))
}

pub(crate) fn arithmetic(
    op: Op,
    a: &ArrayRef,
    at: &Type,
    b: &ArrayRef,
    bt: &Type,
) -> Result<ArrayRef> {
    if *at == Type::Date && *bt == Type::Date {
        let a = a.as_any().downcast_ref::<Date32Array>().unwrap();
        let b = b.as_any().downcast_ref::<Date32Array>().unwrap();
        return decimal(
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| {
                    a.zip(b)
                        .map(|(a, b)| {
                            calendar(a)?;
                            calendar(b)?;
                            Ok(i128::from(a) - i128::from(b))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?,
            0,
        );
    }
    let (dates, offsets) = if *at == Type::Date { (a, b) } else { (b, a) };
    let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
    let mut output = Date32Builder::with_capacity(a.len());
    for (date, offset) in dates.iter().zip(numeric(offsets).iter()) {
        output.append_option(
            date.zip(offset)
                .map(|(days, offset)| {
                    let offset = i32::try_from(offset).map_err(
                        |_| "unsupported: DATE day offset outside signed 32-bit INTEGER range",
                    )?;
                    let offset = if op == Op::Sub {
                        -i64::from(offset)
                    } else {
                        i64::from(offset)
                    };
                    date32(calendar(days)?.checked_add_signed(Duration::days(offset)))
                })
                .transpose()?,
        );
    }
    Ok(Arc::new(output.finish()))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateField {
    Year,
    Quarter,
    Month,
    Day,
    Dow,
    Isodow,
    Doy,
    Week,
    Isoyear,
}

impl DateField {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "day" => Self::Day,
            "dow" => Self::Dow,
            "isodow" => Self::Isodow,
            "doy" => Self::Doy,
            "week" => Self::Week,
            "isoyear" => Self::Isoyear,
            _ => return None,
        })
    }

    pub(crate) fn function_name(self) -> String {
        let name = match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Day => "day",
            Self::Dow => "dow",
            Self::Isodow => "isodow",
            Self::Doy => "doy",
            Self::Week => "week",
            Self::Isoyear => "isoyear",
        };
        format!("$date_part_{name}")
    }

    pub(crate) fn eval(self, array: &ArrayRef) -> Result<ArrayRef> {
        let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
        let origin = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        decimal(
            dates
                .iter()
                .map(|days| {
                    days.map(|days| {
                        let date = origin
                            .checked_add_signed(Duration::days(i64::from(days)))
                            .ok_or("unsupported: DATE field outside supported calendar range")?;
                        Ok(match self {
                            Self::Year => i128::from(date.year()),
                            Self::Quarter => i128::from(date.month0() / 3 + 1),
                            Self::Month => i128::from(date.month()),
                            Self::Day => i128::from(date.day()),
                            Self::Dow => i128::from(date.weekday().num_days_from_sunday()),
                            Self::Isodow => i128::from(date.weekday().number_from_monday()),
                            Self::Doy => i128::from(date.ordinal()),
                            Self::Week => i128::from(date.iso_week().week()),
                            Self::Isoyear => {
                                let year = date.iso_week().year();
                                if year <= 0 {
                                    return Err("unsupported: ISOYEAR before AD 1 requires a BC year-numbering decision".into());
                                }
                                i128::from(year)
                            }
                        })
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?,
            0,
        )
    }
}
