//! Session time zone and statement clock for TIMESTAMPTZ (maintainer decision,
//! October 17, 2026): the session zone defaults to UTC and is changed per
//! session; NOW()/CURRENT_TIMESTAMP are the statement's start instant, with a
//! fixed clock for reproducible runs. Zone rules come from the IANA tzdb
//! compiled into `chrono-tz` (release `IANA_TZDB_VERSION`).
//!
//! Binding reads the statement context from a thread-local set by the
//! prepare entry points; zone-dependent kernels carry the zone name in their
//! bound function name, so evaluation needs no context.
use crate::{Result, expr::timestamp};
use chrono::{Duration, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;
use std::{cell::RefCell, str::FromStr};

pub use chrono_tz::IANA_TZDB_VERSION;

/// A validated session or literal time zone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionZone(Tz);

impl Default for SessionZone {
    fn default() -> Self {
        Self(Tz::UTC)
    }
}

impl SessionZone {
    /// IANA region names (`America/New_York`, case-insensitive, as both
    /// references accept) and `UTC`/`GMT`. Abbreviations and POSIX/offset
    /// spellings refuse: DuckDB and PostgreSQL read `PST` and `'+05'`
    /// differently (PostgreSQL inverts POSIX offset signs).
    pub fn parse(name: &str) -> Result<Self> {
        let upper = name.to_ascii_uppercase();
        if upper == "UTC" || upper == "GMT" {
            return Ok(Self(if upper == "UTC" { Tz::UTC } else { Tz::GMT }));
        }
        if !name.contains('/') {
            return Err(format!(
                "unsupported: time zone {name:?}: only IANA region names (Area/City), UTC and GMT are accepted"
            )
            .into());
        }
        if let Ok(tz) = Tz::from_str(name) {
            return Ok(Self(tz));
        }
        chrono_tz::TZ_VARIANTS
            .iter()
            .find(|tz| tz.name().eq_ignore_ascii_case(name))
            .map(|tz| Self(*tz))
            .ok_or_else(|| format!("time zone \"{name}\" not recognized").into())
    }

    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    /// Offset (local minus UTC, seconds) at an instant.
    pub(crate) fn offset_at(&self, micros: i64) -> Result<i32> {
        let utc = timestamp::naive(micros)?;
        Ok(self
            .0
            .offset_from_utc_datetime(&utc)
            .fix()
            .local_minus_utc())
    }

    /// The session-zone wall clock of an instant (both within AD 0001..9999).
    pub(crate) fn local(&self, micros: i64) -> Result<NaiveDateTime> {
        let offset = self.offset_at(micros)?;
        let local = timestamp::naive(micros)? + Duration::seconds(i64::from(offset));
        timestamp::from_naive(local)?;
        Ok(local)
    }

    /// The instant of a session-zone wall clock. Both references read a time
    /// skipped by a forward transition with the offset before it, and a
    /// repeated time with the offset after it: in both cases the smaller of
    /// the two offsets around the transition (the later instant). DuckDB's
    /// interval arithmetic and hour/day truncation keep the earlier offset for
    /// a repeated time instead, so those callers pass `refuse_repeated`.
    pub(crate) fn instant(&self, local: NaiveDateTime, refuse_repeated: bool) -> Result<i64> {
        timestamp::from_naive(local)?;
        let candidates = [local - Duration::days(1), local + Duration::days(1)].map(|probe| {
            self.0
                .offset_from_utc_datetime(&probe)
                .fix()
                .local_minus_utc()
        });
        let offset = match self.0.offset_from_local_datetime(&local) {
            chrono::LocalResult::Single(offset) => offset.fix().local_minus_utc(),
            chrono::LocalResult::Ambiguous(a, b) => {
                if refuse_repeated {
                    return Err(format!(
                        "unsupported: result {local} repeats in time zone {} (DuckDB and PostgreSQL choose different offsets)",
                        self.name()
                    )
                    .into());
                }
                a.fix().local_minus_utc().min(b.fix().local_minus_utc())
            }
            chrono::LocalResult::None => candidates[0].min(candidates[1]),
        };
        timestamp::from_naive(local - Duration::seconds(i64::from(offset)))
    }

    /// `YYYY-MM-DD HH:MM:SS[.f]±HH[:MM]`, as both references print TIMESTAMPTZ.
    pub(crate) fn format(&self, micros: i64) -> Result<String> {
        let offset = self.offset_at(micros)?;
        if offset % 60 != 0 {
            return Err(format!(
                "unsupported: TIMESTAMPTZ text with a seconds UTC offset in {} (DuckDB drops the seconds, PostgreSQL prints them)",
                self.name()
            )
            .into());
        }
        let local = self.local(micros)?;
        let mut text = timestamp::format(timestamp::from_naive(local)?)?;
        let sign = if offset < 0 { '-' } else { '+' };
        let (hours, minutes) = (offset.abs() / 3600, offset.abs() % 3600 / 60);
        text.push_str(&format!("{sign}{hours:02}"));
        if minutes != 0 {
            text.push_str(&format!(":{minutes:02}"));
        }
        Ok(text)
    }
}

/// A fixed clock given as TIMESTAMPTZ text (`2024-03-10 03:00:00+00`),
/// for reproducible NOW()/CURRENT_* (CLI field, environment, Python).
pub fn fixed_clock(text: &str) -> Result<i64> {
    parse_text(text, &SessionZone::default()).map_err(crate::Error::from)
}

/// Label TIMESTAMPTZ result columns with the session zone, as DuckDB's Arrow
/// results do; values (UTC instants) are unchanged.
pub fn relabel(
    batch: &arrow::record_batch::RecordBatch,
    zone: &SessionZone,
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::{array::*, datatypes::*};
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Timestamp(_, Some(_))))
    {
        return Ok(batch.clone());
    }
    let label: std::sync::Arc<str> = zone.name().into();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if let Some(values) = column.as_any().downcast_ref::<TimestampMicrosecondArray>()
            && values.timezone().is_some()
        {
            fields.push(field.as_ref().clone().with_data_type(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(label.clone()),
            )));
            columns.push(std::sync::Arc::new(
                values.clone().with_timezone(label.clone()),
            ));
        } else {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new_with_options(
        std::sync::Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| crate::Error::from(e.to_string()))
}

/// Text of a TIMESTAMPTZ value in the zone its Arrow column is labelled with.
pub fn format_labelled(array: &dyn arrow::array::Array, row: usize) -> Result<String> {
    let values = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .ok_or("TIMESTAMPTZ output array")?;
    let zone = match values.timezone() {
        Some(name) => SessionZone::parse(name)?,
        None => SessionZone::default(),
    };
    zone.format(values.value(row))
}

/// TIMESTAMPTZ text: the TIMESTAMP grammar, then an attached offset
/// (`+05`, `+0530`, `+05:30`, `+05:30:15`, at most 15 hours), `Z`, or a
/// space and a zone accepted by `SessionZone::parse`. Without a zone the
/// wall clock is read in `session`. Spaced offsets refuse (DuckDB rejects
/// them), as do abbreviations (`PST` differs between the references).
pub(crate) fn parse_text(
    original: &str,
    session: &SessionZone,
) -> std::result::Result<i64, timestamp::TextError> {
    use timestamp::TextError::{Invalid, Unsupported};
    let text = original.trim_start_matches(' ');
    let unsupported = || {
        Unsupported(
            "zone outside the forms both references read (±HH[:MM[:SS]], Z, UTC, Area/City)",
        )
    };
    let wall = |part: &str| timestamp::parse(part);
    let local = |micros: i64| {
        timestamp::naive(micros).map_err(|_| Unsupported("outside 0001-01-01 to 9999-12-31"))
    };
    let resolve = |zone: &SessionZone, micros: i64| {
        zone.instant(local(micros)?, false)
            .map_err(|_| Unsupported("outside 0001-01-01 to 9999-12-31"))
    };
    // A zone name after the last space (it holds a letter).
    if let Some((head, name)) = text.rsplit_once(' ')
        && name.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
    {
        if !head.contains(':') {
            return Err(unsupported());
        }
        let zone = SessionZone::parse(name).map_err(|error| {
            if error.starts_with("unsupported:") {
                unsupported()
            } else {
                Invalid(format!("time zone \"{name}\" not recognized"))
            }
        })?;
        let micros = wall(head)?;
        return resolve(&zone, micros);
    }
    if let Some(head) = text.strip_suffix('Z')
        && head.contains(':')
    {
        let micros = wall(head)?;
        return Ok(micros);
    }
    // An attached numeric offset after the time of day.
    let time_start = text.find([' ', 'T']).filter(|_| text.contains(':'));
    if let Some(start) = time_start
        && let Some(at) = text[start..].rfind(['+', '-']).map(|at| start + at)
    {
        let (head, offset) = text.split_at(at);
        if head.ends_with(' ') || offset.ends_with(' ') {
            return Err(unsupported());
        }
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let digits = &offset[1..];
        let parts = if digits.contains(':') {
            digits.split(':').collect::<Vec<_>>()
        } else if digits.len() == 4 {
            vec![&digits[..2], &digits[2..]]
        } else {
            vec![digits]
        };
        let fields = parts
            .iter()
            .map(|p| {
                (p.len() == 2 && p.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| p.parse::<i64>().unwrap())
            })
            .collect::<Option<Vec<_>>>()
            .filter(|f| (1..=3).contains(&f.len()))
            .ok_or_else(unsupported)?;
        let (h, m, s) = (
            fields[0],
            *fields.get(1).unwrap_or(&0),
            *fields.get(2).unwrap_or(&0),
        );
        if h > 15 || m > 59 || s > 59 {
            return Err(unsupported());
        }
        let micros = wall(head)?;
        let offset = sign * (h * 3600 + m * 60 + s) * timestamp::MICROS_PER_SECOND;
        return timestamp::checked(micros - offset)
            .map_err(|_| Unsupported("outside 0001-01-01 to 9999-12-31"));
    }
    let micros = wall(text)?;
    resolve(session, micros)
}

/// Per-statement context: the session zone and the statement's start instant.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Statement {
    pub zone: SessionZone,
    pub now: i64,
}

thread_local! {
    static STATEMENT: RefCell<Option<Statement>> = const { RefCell::new(None) };
}

/// Restores the previous context when a prepare entry point returns.
pub(crate) struct Guard(Option<Option<Statement>>);

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            STATEMENT.with(|s| *s.borrow_mut() = previous);
        }
    }
}

/// Enter a statement unless one is already active on this thread (nested
/// prepare calls share the outer statement's clock).
pub(crate) fn enter(zone: SessionZone, fixed_clock: Option<i64>) -> Guard {
    STATEMENT.with(|s| {
        let mut current = s.borrow_mut();
        if current.is_some() {
            return Guard(None);
        }
        let now = fixed_clock.unwrap_or_else(|| {
            let elapsed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        });
        let previous = current.replace(Statement { zone, now });
        Guard(Some(previous))
    })
}

/// The active statement, for binding zone- and clock-dependent expressions.
pub(crate) fn statement() -> Result<Statement> {
    STATEMENT.with(|s| *s.borrow()).ok_or_else(|| {
        "internal: TIMESTAMPTZ or clock expression bound outside a statement context".into()
    })
}

/// Local calendar date of a wall clock, as DATE days.
pub(crate) fn days(local: NaiveDateTime) -> i32 {
    (local.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
}

/// Hours/minutes/seconds of a fixed offset, for EXTRACT(TIMEZONE_*).
pub(crate) fn offset_parts(offset: i32) -> (i32, i32) {
    (offset / 3600, offset % 3600 / 60)
}

/// Bound TIMESTAMPTZ function name: `$tz_<op>@<zone>`.
pub(crate) fn function(op: &str, zone: &SessionZone) -> String {
    format!("$tz_{op}@{}", zone.name())
}

pub(crate) fn is_function(name: &str) -> bool {
    name.starts_with("$tz_")
}

/// Offset fields only TIMESTAMPTZ has (both references agree).
pub(crate) fn offset_field(name: &str) -> Option<&'static str> {
    Some(match name.to_ascii_lowercase().as_str() {
        "timezone" => "timezone",
        "timezone_hour" => "timezone_hour",
        "timezone_minute" => "timezone_minute",
        _ => return None,
    })
}

fn wall(zone: &SessionZone, micros: i64) -> Result<i64> {
    timestamp::from_naive(zone.local(micros)?)
}

fn labelled(output: arrow::array::TimestampMicrosecondArray) -> arrow::array::ArrayRef {
    std::sync::Arc::new(output.with_timezone("UTC"))
}

/// Evaluate `$tz_<op>@<zone>`; every op is computed in that zone.
pub(crate) fn evaluate(
    name: &str,
    array: &arrow::array::ArrayRef,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::*;
    use std::sync::Arc;
    let (op, zone) = name
        .strip_prefix("$tz_")
        .and_then(|rest| rest.rsplit_once('@'))
        .expect("bound TIMESTAMPTZ function");
    let zone = SessionZone::parse(zone)?;
    let instants = || timestamp::values(array);
    let to_instants = |f: &dyn Fn(i64) -> Result<i64>| -> Result<ArrayRef> {
        let input = timestamp::values(array);
        Ok(labelled(
            input
                .iter()
                .map(|v| v.map(f).transpose())
                .collect::<Result<TimestampMicrosecondArray>>()?,
        ))
    };
    if let Some(field) = op.strip_prefix("part_") {
        if field == "epoch" {
            return Ok(Arc::new(
                instants()
                    .iter()
                    .map(|v| {
                        v.map(|m| {
                            timestamp::checked(m)?;
                            Ok(m as f64 / timestamp::MICROS_PER_SECOND as f64)
                        })
                        .transpose()
                    })
                    .collect::<Result<Float64Array>>()?,
            ));
        }
        let values = instants()
            .iter()
            .map(|v| {
                v.map(|m| -> Result<i128> {
                    if let Some(offset) = offset_field(field) {
                        let seconds = zone.offset_at(m)?;
                        let (hours, minutes) = offset_parts(seconds);
                        return Ok(i128::from(match offset {
                            "timezone" => seconds,
                            "timezone_hour" => hours,
                            _ => minutes,
                        }));
                    }
                    let field = timestamp::Field::parse(field).expect("bound TIMESTAMPTZ field");
                    field.int(wall(&zone, m)?)
                })
                .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .map_err(|e| crate::Error::from(e.to_string()))?,
        ));
    }
    if let Some(unit) = op.strip_prefix("trunc_") {
        let unit = timestamp::Unit::parse(unit).expect("bound TIMESTAMPTZ unit");
        return to_instants(&|m| {
            let local = wall(&zone, m)?;
            let truncated = timestamp::truncate(local, unit)?;
            let offset = zone.offset_at(m)?;
            if matches!(
                unit,
                timestamp::Unit::Microsecond
                    | timestamp::Unit::Millisecond
                    | timestamp::Unit::Second
                    | timestamp::Unit::Minute
            ) && offset % 60 == 0
            {
                // Within one whole-minute offset: both references keep it.
                return timestamp::checked(m - (local - truncated));
            }
            // Hour and longer units: DuckDB and PostgreSQL resolve a
            // truncated wall clock that is skipped or repeated differently
            // (Lord Howe 02:00, New York 01:00), so those refuse.
            let local = timestamp::naive(truncated)?;
            if !matches!(
                zone.0.offset_from_local_datetime(&local),
                chrono::LocalResult::Single(_)
            ) {
                return Err(format!(
                    "unsupported: DATE_TRUNC result {local} is skipped or repeated in time zone {} (DuckDB and PostgreSQL resolve it differently)",
                    zone.name()
                )
                .into());
            }
            zone.instant(local, true)
        });
    }
    if let Some(count) = op.strip_prefix("shift_days_") {
        let days = count.parse::<i64>().expect("bound shift");
        return to_instants(&|m| {
            let local = timestamp::naive(wall(&zone, m)?)?;
            let shifted = local
                .checked_add_signed(Duration::days(days))
                .ok_or("unsupported: TIMESTAMPTZ outside the supported range")?;
            zone.instant(shifted, true)
        });
    }
    if let Some(count) = op.strip_prefix("shift_months_") {
        let months = count.parse::<i64>().expect("bound shift");
        return to_instants(&|m| {
            let shifted = timestamp::shift_months(wall(&zone, m)?, months)?;
            zone.instant(timestamp::naive(shifted)?, true)
        });
    }
    match op {
        "from_ts" => to_instants(&|m| zone.instant(timestamp::naive(m)?, false)),
        "to_ts" => Ok(Arc::new(
            instants()
                .iter()
                .map(|v| v.map(|m| wall(&zone, m)).transpose())
                .collect::<Result<TimestampMicrosecondArray>>()?,
        )),
        "from_date" => {
            let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(labelled(
                dates
                    .iter()
                    .map(|d| {
                        d.map(|d| zone.instant(timestamp::naive(timestamp::from_days(d)?)?, false))
                            .transpose()
                    })
                    .collect::<Result<TimestampMicrosecondArray>>()?,
            ))
        }
        "to_date" => Ok(Arc::new(
            instants()
                .iter()
                .map(|v| v.map(|m| Ok(days(zone.local(m)?))).transpose())
                .collect::<Result<Date32Array>>()?,
        )),
        "text" => {
            let values = instants()
                .iter()
                .map(|v| v.map(|m| zone.format(m)).transpose())
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        "parse" => {
            let texts = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(labelled(
                texts
                    .iter()
                    .map(|t| {
                        t.map(|t| parse_text(t, &zone).map_err(crate::Error::from))
                            .transpose()
                    })
                    .collect::<Result<TimestampMicrosecondArray>>()?,
            ))
        }
        _ => unreachable!("bound TIMESTAMPTZ function {name}"),
    }
}

/// TO_TIMESTAMP(seconds since the epoch) -> TIMESTAMPTZ. Both references
/// round the binary64 product with the microsecond factor to even.
pub(crate) fn to_timestamp(
    array: &arrow::array::ArrayRef,
    ty: &crate::types::Type,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::*;
    let micros = |seconds: f64| -> Result<i64> {
        if seconds.is_nan() {
            return Err("timestamp cannot be NaN".into());
        }
        if seconds.is_infinite() {
            return Err("unsupported: TO_TIMESTAMP of infinity (PostgreSQL returns infinity, DuckDB errors)".into());
        }
        let scale = timestamp::MICROS_PER_SECOND as f64;
        // DuckDB scales the seconds directly; PostgreSQL first subtracts its
        // 2000-01-01 epoch (946684800 s) in binary64, then rounds to even.
        // Answer only when both readings give the same microsecond.
        let duckdb = seconds * scale;
        const PG_EPOCH: f64 = 946_684_800.0;
        let postgres = ((seconds - PG_EPOCH) * scale).round_ties_even();
        if !(-9.2e18..=9.2e18).contains(&duckdb) || !(-9.2e18..=9.2e18).contains(&postgres) {
            return Err("timestamp out of range".into());
        }
        let postgres = postgres as i64 + 946_684_800_000_000;
        // Both round to even (DuckDB 2.5e-6 s -> 2 us; PostgreSQL rint).
        if duckdb.round_ties_even() as i64 != postgres {
            return Err("unsupported: TO_TIMESTAMP value rounds to different microseconds in DuckDB and PostgreSQL".into());
        }
        timestamp::checked(postgres)
    };
    let values = match ty {
        crate::types::Type::Float => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(micros).transpose())
            .collect::<Result<TimestampMicrosecondArray>>()?,
        crate::types::Type::Int | crate::types::Type::Decimal(_) => {
            let scale = ty.scale()?;
            crate::expr::numeric(array)
                .iter()
                .map(|v| {
                    v.map(|n| {
                        // DECIMAL goes through DOUBLE in both references;
                        // whole seconds are exact.
                        if scale == 0 {
                            let product = n
                                .checked_mul(i128::from(timestamp::MICROS_PER_SECOND))
                                .and_then(|p| i64::try_from(p).ok())
                                .ok_or_else(|| crate::Error::from("timestamp out of range"))?;
                            timestamp::checked(product)
                        } else {
                            micros(crate::numeric_float::scaled(n, scale)?)
                        }
                    })
                    .transpose()
                })
                .collect::<Result<TimestampMicrosecondArray>>()?
        }
        crate::types::Type::Null => TimestampMicrosecondArray::from(vec![None; array.len()]),
        _ => unreachable!("bound TO_TIMESTAMP input"),
    };
    Ok(labelled(values))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zone(name: &str) -> SessionZone {
        SessionZone::parse(name).unwrap()
    }

    fn text(s: &str, z: &str) -> String {
        let zone = zone(z);
        parse_text(s, &zone)
            .map(|m| zone.format(m).unwrap())
            .unwrap_or_else(|e| crate::Error::from(e).to_string())
    }

    #[test]
    fn zones_and_names() {
        assert_eq!(zone("america/new_york").name(), "America/New_York");
        assert_eq!(zone("utc").name(), "UTC");
        assert!(
            SessionZone::parse("PST")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(
            SessionZone::parse("+05")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert!(
            !SessionZone::parse("Mars/Olympus")
                .unwrap_err()
                .starts_with("unsupported:")
        );
        assert_eq!(IANA_TZDB_VERSION, "2025b");
    }

    #[test]
    fn transitions_take_the_later_instant() {
        let ny = zone("America/New_York");
        assert_eq!(
            text("2024-03-10 02:30:00", "America/New_York"),
            "2024-03-10 03:30:00-04"
        );
        assert_eq!(
            text("2024-11-03 01:30:00", "America/New_York"),
            "2024-11-03 01:30:00-05"
        );
        assert_eq!(
            text("2024-10-06 02:15:00", "Australia/Lord_Howe"),
            "2024-10-06 02:45:00+11"
        );
        assert_eq!(
            text("2024-04-07 01:45:00", "Australia/Lord_Howe"),
            "2024-04-07 01:45:00+10:30"
        );
        let repeated = chrono::NaiveDate::from_ymd_opt(2024, 11, 3)
            .unwrap()
            .and_hms_opt(1, 30, 0)
            .unwrap();
        assert!(
            ny.instant(repeated, true)
                .unwrap_err()
                .starts_with("unsupported:")
        );
    }

    #[test]
    fn to_timestamp_special_values() {
        use arrow::array::{Array, ArrayRef, Float64Array};
        let run = |x: f64| {
            let array: ArrayRef = std::sync::Arc::new(Float64Array::from(vec![x]));
            to_timestamp(&array, &crate::types::Type::Float).map(|a| a.len())
        };
        assert_eq!(run(f64::NAN).unwrap_err(), "timestamp cannot be NaN");
        assert!(run(f64::INFINITY).unwrap_err().starts_with("unsupported:"));
        assert_eq!(run(1e20).unwrap_err(), "timestamp out of range");
        assert!(run(1.0000005).unwrap_err().starts_with("unsupported:"));
        assert_eq!(run(2.5e-6).unwrap(), 1);
    }

    #[test]
    fn text_forms() {
        assert_eq!(
            text("2024-03-10 06:30:00+05", "UTC"),
            "2024-03-10 01:30:00+00"
        );
        assert_eq!(
            text("2024-03-10 06:30:00+0530", "UTC"),
            "2024-03-10 01:00:00+00"
        );
        assert_eq!(
            text("2024-03-10 06:30:00-05:30", "UTC"),
            "2024-03-10 12:00:00+00"
        );
        assert_eq!(
            text("2024-03-10 06:30:00+05:30:15", "UTC"),
            "2024-03-10 00:59:45+00"
        );
        assert_eq!(
            text("2024-03-10 06:30:00Z", "Asia/Kolkata"),
            "2024-03-10 12:00:00+05:30"
        );
        assert_eq!(
            text("2024-03-10 06:30:00 america/new_york", "UTC"),
            "2024-03-10 10:30:00+00"
        );
        assert_eq!(
            text("2024-03-10", "America/New_York"),
            "2024-03-10 00:00:00-05"
        );
        assert!(text("2024-03-10 06:30:00 PST", "UTC").starts_with("unsupported:"));
        assert!(text("2024-03-10 06:30:00 +05:30", "UTC").starts_with("unsupported:"));
        assert!(text("2024-03-10 06:30:00+16", "UTC").starts_with("unsupported:"));
        assert!(text("2024-03-10 06:30:00 Mars/Olympus", "UTC").starts_with("time zone"));
        assert!(
            zone("Asia/Kolkata")
                .format(-2_840_140_800_000_000)
                .unwrap_err()
                .starts_with("unsupported:")
        );
    }
}
