//! TIMESTAMP (without time zone): microseconds since 1970-01-01 00:00:00,
//! the precision PostgreSQL 17 and DuckDB 1.5.6 use for TIMESTAMP.
//!
//! Values are compared, grouped and sorted as plain i64. Calendar operations
//! (text output, fields, truncation, interval arithmetic, DATE casts) cover
//! AD 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999, where both references
//! print the same text; outside that range they refuse with `unsupported:`.
use super::{ArrayRef, Result, decimal};
use crate::types::Type;
use arrow::array::{
    Array, Date32Array, Float64Array, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::sync::Arc;

/// 0001-01-01 00:00:00.
pub(crate) const MIN: i64 = -62_135_596_800_000_000;
/// 9999-12-31 23:59:59.999999.
pub(crate) const MAX: i64 = 253_402_300_799_999_999;
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;
pub(crate) const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;
const RANGE: &str = "unsupported: TIMESTAMP outside the supported range 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999";

pub(crate) fn checked(micros: i64) -> Result<i64> {
    if (MIN..=MAX).contains(&micros) {
        Ok(micros)
    } else {
        Err(RANGE.into())
    }
}

fn checked_i128(micros: i128) -> Result<i64> {
    i64::try_from(micros)
        .map_err(|_| crate::Error::from(RANGE))
        .and_then(checked)
}

pub(crate) fn naive(micros: i64) -> Result<NaiveDateTime> {
    checked(micros)?;
    Ok(DateTime::from_timestamp_micros(micros)
        .expect("supported TIMESTAMP range")
        .naive_utc())
}

pub(crate) fn from_naive(value: NaiveDateTime) -> Result<i64> {
    checked(value.and_utc().timestamp_micros())
}

/// DATE days to TIMESTAMP midnight, inside the supported range.
pub(crate) fn from_days(days: i32) -> Result<i64> {
    checked_i128(i128::from(days) * i128::from(MICROS_PER_DAY))
}

/// TIMESTAMP to DATE: the calendar day, i.e. floor division by one day.
pub(crate) fn to_days(micros: i64) -> Result<i32> {
    checked(micros)?;
    Ok(micros.div_euclid(MICROS_PER_DAY) as i32)
}

/// `YYYY-MM-DD HH:MM:SS[.f]`, fraction without trailing zeros: the text both
/// references print for TIMESTAMP inside the supported range.
pub fn format(micros: i64) -> Result<String> {
    let value = naive(micros)?;
    let mut text = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        value.year(),
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    );
    let fraction = micros.rem_euclid(MICROS_PER_SECOND);
    if fraction != 0 {
        let digits = format!("{fraction:06}");
        text.push('.');
        text.push_str(digits.trim_end_matches('0'));
    }
    Ok(text)
}

/// Why a text value is not a TIMESTAMP.
pub(crate) enum TextError {
    /// Both references reject this text: an ordinary data error.
    Invalid(String),
    /// Outside the grammar both references accept identically.
    Unsupported(&'static str),
}

impl From<TextError> for crate::Error {
    fn from(error: TextError) -> Self {
        match error {
            TextError::Invalid(message) => message.into(),
            TextError::Unsupported(reason) => {
                format!("unsupported: TIMESTAMP text {reason}").into()
            }
        }
    }
}

const GRAMMAR: &str =
    "outside the form PostgreSQL and DuckDB share: YYYY-MM-DD[( |T)HH:MM[:SS[.ffffff]]]";

fn digits(bytes: &[u8], at: &mut usize, min: usize, max: usize) -> Option<u32> {
    let start = *at;
    while *at < bytes.len() && *at - start < max && bytes[*at].is_ascii_digit() {
        *at += 1;
    }
    if *at - start < min {
        return None;
    }
    std::str::from_utf8(&bytes[start..*at]).ok()?.parse().ok()
}

/// Parse TIMESTAMP text on the grammar PostgreSQL 17 and DuckDB 1.5.6 both
/// accept with the same value (probed in tools/testing/timestamp_reference.py):
/// a four-digit year, one- or two-digit month/day/hour/minute/second, a
/// space(s) or `T` separator, an optional fraction of at most six digits, and
/// surrounding blanks. Offsets, zone names, `BC`, special words (`epoch`,
/// `infinity`), compact forms and leap seconds are accepted by only one
/// reference or with different values, so they refuse.
pub(crate) fn parse(original: &str) -> std::result::Result<i64, TextError> {
    // Only spaces: PostgreSQL also trims tabs/newlines, DuckDB rejects them.
    // DuckDB rejects trailing spaces unless the seconds field is present.
    let text = original.trim_start_matches(' ');
    let trailing = text.len() != text.trim_end_matches(' ').len();
    let text = text.trim_end_matches(' ');
    let bytes = text.as_bytes();
    let unsupported = || TextError::Unsupported(GRAMMAR);
    let out_of_range = || {
        TextError::Invalid(format!(
            "date/time field value out of range: \"{original}\""
        ))
    };
    let mut at = 0;
    let year = digits(bytes, &mut at, 4, 4).ok_or_else(unsupported)?;
    let expect = |at: &mut usize, byte: u8| {
        if bytes.get(*at) == Some(&byte) {
            *at += 1;
            Ok(())
        } else {
            Err(unsupported())
        }
    };
    expect(&mut at, b'-')?;
    let month = digits(bytes, &mut at, 1, 2).ok_or_else(unsupported)?;
    expect(&mut at, b'-')?;
    let day = digits(bytes, &mut at, 1, 2).ok_or_else(unsupported)?;
    let (mut hour, mut minute, mut second, mut micros) = (0, 0, 0, 0);
    let mut seconds_field = false;
    if at < bytes.len() {
        match bytes[at] {
            b'T' => at += 1,
            b' ' => {
                while bytes.get(at) == Some(&b' ') {
                    at += 1;
                }
            }
            _ => return Err(unsupported()),
        }
        hour = digits(bytes, &mut at, 1, 2).ok_or_else(unsupported)?;
        expect(&mut at, b':')?;
        minute = digits(bytes, &mut at, 1, 2).ok_or_else(unsupported)?;
        if bytes.get(at) == Some(&b':') {
            at += 1;
            second = digits(bytes, &mut at, 1, 2).ok_or_else(unsupported)?;
            seconds_field = true;
            if bytes.get(at) == Some(&b'.') {
                at += 1;
                let start = at;
                while at < bytes.len() && bytes[at].is_ascii_digit() {
                    at += 1;
                }
                let fraction = &text[start..at];
                if fraction.len() > 6 {
                    // PostgreSQL rounds the seventh digit; DuckDB truncates.
                    return Err(TextError::Unsupported(
                        "with more than six fractional digits (PostgreSQL rounds, DuckDB truncates)",
                    ));
                }
                micros = format!("{fraction:0<6}").parse::<u32>().unwrap_or(0);
            }
        }
        if at != bytes.len() {
            return Err(unsupported());
        }
    }
    if trailing && !seconds_field {
        return Err(unsupported());
    }
    if year == 0 {
        return Err(TextError::Unsupported(
            "year 0000 (no common AD/BC reading)",
        ));
    }
    let date = NaiveDate::from_ymd_opt(year as i32, month, day).ok_or_else(out_of_range)?;
    if minute > 59 || hour > 24 {
        return Err(out_of_range());
    }
    if second == 60 {
        return Err(TextError::Unsupported(
            "leap second :60 (PostgreSQL rolls over, DuckDB rejects)",
        ));
    }
    if second > 60 {
        return Err(out_of_range());
    }
    if hour == 24 {
        if minute != 0 || second != 0 || micros != 0 {
            return Err(out_of_range());
        }
        // Both references read 24:00:00 as midnight of the next day.
        let next = date.succ_opt().ok_or_else(out_of_range)?;
        return from_naive(next.and_hms_opt(0, 0, 0).unwrap())
            .map_err(|_| TextError::Unsupported("outside 0001-01-01 to 9999-12-31"));
    }
    let value = date
        .and_hms_micro_opt(hour, minute, second, micros)
        .ok_or_else(out_of_range)?;
    from_naive(value).map_err(|_| TextError::Unsupported("outside 0001-01-01 to 9999-12-31"))
}

/// DATE_TRUNC units shared by both references for TIMESTAMP input.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Unit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        let name = name.strip_suffix('s').unwrap_or(&name);
        Some(match name {
            "microsecond" => Self::Microsecond,
            "millisecond" => Self::Millisecond,
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Microsecond => "microsecond",
            Self::Millisecond => "millisecond",
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }
}

pub(crate) fn values(array: &ArrayRef) -> &TimestampMicrosecondArray {
    array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("bound TIMESTAMP array")
}

/// Map values, keeping the input's TIMESTAMPTZ label (if any).
pub(crate) fn map(array: &ArrayRef, f: impl Fn(i64) -> Result<i64>) -> Result<ArrayRef> {
    let input = values(array);
    let mut output = TimestampMicrosecondBuilder::with_capacity(input.len());
    for value in input.iter() {
        output.append_option(value.map(&f).transpose()?);
    }
    Ok(Arc::new(
        output.finish().with_timezone_opt(input.timezone()),
    ))
}

pub(crate) fn truncate(micros: i64, unit: Unit) -> Result<i64> {
    let fixed = |step: i64| checked(micros).map(|m| m - m.rem_euclid(step));
    match unit {
        Unit::Microsecond => checked(micros),
        Unit::Millisecond => fixed(1_000),
        Unit::Second => fixed(MICROS_PER_SECOND),
        Unit::Minute => fixed(60 * MICROS_PER_SECOND),
        Unit::Hour => fixed(3_600 * MICROS_PER_SECOND),
        Unit::Day => fixed(MICROS_PER_DAY),
        _ => {
            let date = naive(micros)?.date();
            let start = match unit {
                Unit::Week => {
                    date - chrono::Duration::days(i64::from(date.weekday().num_days_from_monday()))
                }
                Unit::Month => date.with_day(1).unwrap(),
                Unit::Quarter => {
                    NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1).unwrap()
                }
                _ => NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap(),
            };
            // Monday of week 1 of AD 0001 is 0001-01-01, so this stays in range.
            from_naive(start.and_hms_opt(0, 0, 0).unwrap())
        }
    }
}

pub(crate) fn shift_months(micros: i64, months: i64) -> Result<i64> {
    let value = naive(micros)?;
    let date = value.date();
    let month = i64::from(date.year()) * 12 + i64::from(date.month0()) + months;
    let year = i32::try_from(month.div_euclid(12)).map_err(|_| crate::Error::from(RANGE))?;
    let month = month.rem_euclid(12) as u32 + 1;
    // Clamp once at the destination month (end-of-month rule shared with the
    // DATE toolkit, PostgreSQL and DuckDB); the time of day is unchanged.
    let target = (1..=date.day())
        .rev()
        .find_map(|day| NaiveDate::from_ymd_opt(year, month, day))
        .ok_or_else(|| crate::Error::from(RANGE))?;
    from_naive(target.and_time(value.time()))
}

/// Internal unary function names bound by the binder:
/// `$ts_trunc_<unit>`, `$ts_shift_months_<n>`, `$ts_shift_micros_<n>`,
/// `$ts_from_date` and `$ts_to_date`.
pub(crate) fn is_function(name: &str) -> bool {
    name.starts_with("$ts_")
}

pub(crate) fn evaluate(name: &str, array: &ArrayRef) -> Result<ArrayRef> {
    if name == "$ts_from_date" {
        let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
        let mut output = TimestampMicrosecondBuilder::with_capacity(dates.len());
        for days in dates.iter() {
            output.append_option(days.map(from_days).transpose()?);
        }
        return Ok(Arc::new(output.finish()));
    }
    if name == "$ts_to_date" {
        let input = values(array);
        return Ok(Arc::new(
            input
                .iter()
                .map(|v| v.map(to_days).transpose())
                .collect::<Result<Date32Array>>()?,
        ));
    }
    if let Some(field) = name.strip_prefix("$ts_part_") {
        return Field::parse(field)
            .expect("bound TIMESTAMP field")
            .eval(array);
    }
    if let Some(unit) = name.strip_prefix("$ts_trunc_") {
        let unit = Unit::parse(unit).expect("bound TIMESTAMP unit");
        return map(array, |m| truncate(m, unit));
    }
    if let Some(count) = name.strip_prefix("$ts_shift_months_") {
        let count = count.parse::<i64>().expect("bound TIMESTAMP shift");
        return map(array, |m| shift_months(m, count));
    }
    if let Some(count) = name.strip_prefix("$ts_shift_micros_") {
        let count = count.parse::<i64>().expect("bound TIMESTAMP shift");
        return map(array, |m| {
            checked(m)?;
            checked_i128(i128::from(m) + i128::from(count))
        });
    }
    unreachable!("bound TIMESTAMP function {name}")
}

/// EXTRACT / DATE_PART fields on TIMESTAMP.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Field {
    Year,
    Quarter,
    Month,
    Day,
    Dow,
    Isodow,
    Doy,
    Week,
    Isoyear,
    Hour,
    Minute,
    /// Whole seconds; a fractional second refuses (DuckDB returns the whole
    /// seconds as BIGINT, PostgreSQL the fraction as NUMERIC).
    Second,
    /// Seconds in milliseconds; a sub-millisecond remainder refuses.
    Millisecond,
    /// Seconds in microseconds (both references include the seconds).
    Microsecond,
    /// Seconds since 1970-01-01 as DOUBLE, DuckDB's type; PostgreSQL returns
    /// the same value as NUMERIC.
    Epoch,
}

impl Field {
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
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" | "milliseconds" => Self::Millisecond,
            "microsecond" | "microseconds" => Self::Microsecond,
            "epoch" => Self::Epoch,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Day => "day",
            Self::Dow => "dow",
            Self::Isodow => "isodow",
            Self::Doy => "doy",
            Self::Week => "week",
            Self::Isoyear => "isoyear",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Millisecond => "millisecond",
            Self::Microsecond => "microsecond",
            Self::Epoch => "epoch",
        }
    }

    pub(crate) fn function_name(self) -> String {
        format!("$ts_part_{}", self.name())
    }

    pub(crate) fn ty(self) -> Type {
        if self == Self::Epoch {
            Type::Float
        } else {
            Type::Int
        }
    }

    fn eval(self, array: &ArrayRef) -> Result<ArrayRef> {
        let input = values(array);
        if self == Self::Epoch {
            return Ok(Arc::new(
                input
                    .iter()
                    .map(|v| {
                        v.map(|m| {
                            checked(m)?;
                            // DuckDB 1.5.6: epoch microseconds / 1e6 in binary64.
                            Ok(m as f64 / MICROS_PER_SECOND as f64)
                        })
                        .transpose()
                    })
                    .collect::<Result<Float64Array>>()?,
            ));
        }
        decimal(
            input
                .iter()
                .map(|v| v.map(|m| self.int(m)).transpose())
                .collect::<Result<Vec<_>>>()?,
            0,
        )
    }

    pub(crate) fn int(self, micros: i64) -> Result<i128> {
        let value = naive(micros)?;
        let date = value.date();
        let sub_minute = micros.rem_euclid(60 * MICROS_PER_SECOND);
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
                    return Err(
                        "unsupported: ISOYEAR before AD 1 requires a BC year-numbering decision"
                            .into(),
                    );
                }
                i128::from(year)
            }
            Self::Hour => i128::from(value.hour()),
            Self::Minute => i128::from(value.minute()),
            Self::Second => {
                if sub_minute % MICROS_PER_SECOND != 0 {
                    return Err("unsupported: EXTRACT(SECOND) of a fractional second (DuckDB returns whole seconds, PostgreSQL the fraction); use EXTRACT(MICROSECOND)".into());
                }
                i128::from(sub_minute / MICROS_PER_SECOND)
            }
            Self::Millisecond => {
                if sub_minute % 1_000 != 0 {
                    return Err("unsupported: EXTRACT(MILLISECOND) with a sub-millisecond remainder (DuckDB truncates, PostgreSQL keeps the fraction); use EXTRACT(MICROSECOND)".into());
                }
                i128::from(sub_minute / 1_000)
            }
            Self::Microsecond => i128::from(sub_minute),
            Self::Epoch => unreachable!("Float field"),
        })
    }
}

/// Attach the internal UTC label for TIMESTAMPTZ arrays (same values).
pub(crate) fn labelled(array: TimestampMicrosecondArray, ty: &Type) -> ArrayRef {
    if *ty == Type::TimestampTz {
        Arc::new(array.with_timezone("UTC"))
    } else {
        Arc::new(array)
    }
}

/// TIMESTAMP value of a non-NULL row, for callers holding a generic array.
pub(crate) fn value(array: &dyn Array, row: usize) -> i64 {
    array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("bound TIMESTAMP array")
        .value(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> std::result::Result<String, String> {
        parse(s)
            .map(|m| format(m).unwrap())
            .map_err(|e| crate::Error::from(e).to_string())
    }

    #[test]
    fn range_bounds_format() {
        assert_eq!(format(MIN).unwrap(), "0001-01-01 00:00:00");
        assert_eq!(format(MAX).unwrap(), "9999-12-31 23:59:59.999999");
        assert!(format(MIN - 1).is_err());
        assert!(format(MAX + 1).is_err());
        assert_eq!(format(-500_000).unwrap(), "1969-12-31 23:59:59.5");
    }

    #[test]
    fn text_grammar() {
        assert_eq!(text("2024-3-1 1:2:3").unwrap(), "2024-03-01 01:02:03");
        assert_eq!(text(" 2024-03-01T10:30").unwrap(), "2024-03-01 10:30:00");
        assert_eq!(
            text(" 2024-03-01 10:30:00 ").unwrap(),
            "2024-03-01 10:30:00"
        );
        assert!(
            text("2024-03-01T10:30 ")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(text("2024-03-01 ").unwrap_err().starts_with("unsupported:"));
        assert!(
            text("\t2024-03-01 10:30:00")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert_eq!(text("2024-03-01 10:30:00.").unwrap(), "2024-03-01 10:30:00");
        assert_eq!(text("2024-03-01 24:00:00").unwrap(), "2024-03-02 00:00:00");
        assert!(
            text("2024-03-01 10:30:00+02")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(
            text("2024-03-01 23:59:60")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(
            text("2024-03-01 10:30:00.1234567")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(
            text("2024-02-30")
                .unwrap_err()
                .starts_with("date/time field value out of range")
        );
        assert!(
            text("2024-03-01 25:00:00")
                .unwrap_err()
                .starts_with("date/time field")
        );
        assert!(
            text("9999-12-31 24:00:00")
                .unwrap_err()
                .starts_with("unsupported:")
        );
    }
}
