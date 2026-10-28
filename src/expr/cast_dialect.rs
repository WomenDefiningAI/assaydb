//! Explicit PostgreSQL 17 / DuckDB 1.5.6 common-domain casts.
//! INTEGER keeps Assay's (SQLite's) 64-bit meaning, the same as its INTEGER
//! column type, and REAL, FLOAT8 and DOUBLE use binary64 (maintainer decision,
//! October 16, 2026: REAL keeps Assay's existing meaning, and INTEGER width
//! follows the same rule). PostgreSQL and DuckDB INTEGER is 32-bit; this is a
//! documented difference. Internal exact-integer coercions are separate.
use super::*;
use crate::types::{decimal_literal, rescale};
use sqlparser::ast::{CharacterLength, DataType as SqlType, ExactNumberInfo, TimezoneInfo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Target {
    /// INTEGER/INT: Assay's 64-bit INTEGER (the column type's range, not
    /// PostgreSQL/DuckDB's 32-bit INTEGER), rounding like PostgreSQL and
    /// DuckDB. Errors name `integer`, following PostgreSQL's wording.
    Integer,
    /// BIGINT: the same signed 64-bit range; errors name `bigint`.
    BigInt,
    /// DOUBLE/FLOAT8 from TEXT: complete-input parse, refused on the base.
    Float,
    /// REAL from TEXT: complete-input syntax check, then the base SQLite value.
    Real,
    Decimal(u8, i8),
    Bool,
    Date,
    /// TIMESTAMP (without time zone) from TEXT, DATE or TIMESTAMP.
    Timestamp,
    /// TIMESTAMPTZ from TEXT, DATE, TIMESTAMP (session zone) or itself.
    TimestampTz,
    /// BOOLEAN to TEXT renders 'true'/'false' like PostgreSQL and DuckDB.
    BoolText,
}
impl Target {
    fn ty(&self) -> Type {
        match self {
            Self::Integer | Self::BigInt => Type::Int,
            Self::Float | Self::Real => Type::Float,
            Self::Decimal(_, s) => Type::Decimal(*s),
            Self::Bool => Type::Bool,
            Self::Date => Type::Date,
            Self::Timestamp => Type::Timestamp,
            Self::TimestampTz => Type::TimestampTz,
            Self::BoolText => Type::Text,
        }
    }
}
fn refused(source: &Type, target: &Target) -> crate::Error {
    format!("unsupported: CAST {source:?} AS {target:?} (PostgreSQL/DuckDB disagree)").into()
}
fn unknown(source: &Type, target: &Target) -> crate::Error {
    format!(
        "unsupported: CAST {source:?} AS {target:?} (PostgreSQL/DuckDB agreement unestablished)"
    )
    .into()
}
/// Data errors that PostgreSQL and DuckDB both raise are ordinary execution
/// errors (like "division by zero"), never "unsupported:" refusals. Messages
/// follow PostgreSQL's spelling. Owner decision, October 16, 2026.
fn invalid(type_name: &str, text: &str) -> crate::Error {
    format!("invalid input syntax for type {type_name}: \"{text}\"").into()
}
fn integer_name(target: &Target) -> &'static str {
    if *target == Target::BigInt {
        "bigint"
    } else {
        "integer"
    }
}
fn out_of_range(target: &Target) -> crate::Error {
    format!("{} out of range", integer_name(target)).into()
}
fn typed_null(ty: Type) -> Expr {
    Expr {
        ty,
        kind: Kind::Literal(Literal::Null),
    }
}
/// TEXT targets keep the base conversions except BOOLEAN (owner decision,
/// October 15, 2026), which renders 'true'/'false'.
fn text_target(value: Expr) -> Result<Expr> {
    if value.ty != Type::Bool || matches!(value.kind, Kind::Literal(Literal::Null)) {
        return casts::bind(value, Type::Text);
    }
    Ok(Expr {
        ty: Type::Text,
        kind: Kind::DialectCast(Box::new(value), Target::BoolText),
    })
}
/// INTEGER/INT and REAL retain the public SQLite-compatible conversion kernels
/// (owner decision, October 21, 2026). BIGINT and DOUBLE/FLOAT8 keep their
/// separately introduced checked dialect semantics. TEXT and
/// VARCHAR targets, REAL/INTEGER from non-TEXT sources other than the checked
/// numeric ones, exact DECIMAL (including Float sources and bare
/// DECIMAL = DECIMAL(18,3)) and literal DATE text all use the base binder.
pub(crate) fn bind(value: Expr, sql: &SqlType) -> Result<Expr> {
    let target = match sql {
        SqlType::Text | SqlType::String(None) => return text_target(value),
        SqlType::Varchar(length)
        | SqlType::CharacterVarying(length)
        | SqlType::CharVarying(length) => {
            let limit = match length {
                None => None,
                Some(CharacterLength::IntegerLength { length, unit: None }) if *length > 0 => {
                    Some(*length)
                }
                _ => {
                    return Err(
                        "unsupported: VARCHAR cast length must be a positive character count without units"
                            .into(),
                    );
                }
            };
            let value = text_target(value)?;
            return Ok(match limit {
                None => value,
                Some(limit) => Expr {
                    kind: Kind::CastVarchar(Box::new(value), limit),
                    ty: Type::Text,
                },
            });
        }
        SqlType::Integer(None) | SqlType::Int(None) => {
            // Public INTEGER conversions consume a numeric prefix in text,
            // truncate fractional values and saturate at signed 64-bit bounds.
            // Reuse the existing kernel, including exact-integer identity and
            // its established NULL/nonfinite handling; arithmetic is unchanged.
            return casts::bind(value, Type::Int);
        }
        SqlType::BigInt(None) => Target::BigInt,
        SqlType::Real => {
            // REAL's public text conversion is permissive prefix parsing,
            // including zero for empty/invalid text, with SQLite rounding.
            return casts::bind(value, Type::Float);
        }
        SqlType::Double(ExactNumberInfo::None) | SqlType::DoublePrecision | SqlType::Float8 => {
            if matches!(
                value.ty,
                Type::Int | Type::Decimal(_) | Type::Float | Type::Null
            ) {
                return casts::bind(value, Type::Float);
            }
            Target::Float
        }
        SqlType::Decimal(info) | SqlType::Numeric(info) | SqlType::Dec(info) => {
            let (p, s) = match info {
                ExactNumberInfo::None => (18, 3),
                ExactNumberInfo::PrecisionAndScale(p, s) => (*p, *s),
                ExactNumberInfo::Precision(_) => {
                    return Err(format!("unsupported: CAST AS {sql}").into());
                }
            };
            if !(1..=38).contains(&p) || s > 18 || s > p {
                return Err(format!("unsupported: CAST AS {sql}").into());
            }
            let ty = Type::Decimal(s as i8);
            // The base lossless literal-text contract keeps its answers and
            // its overflow error; other literal text uses the rounded parser.
            if let Kind::Literal(Literal::Text(text)) = &value.kind
                && let Ok((n, old)) = decimal_literal(text)
                && let Ok(n) = rescale(n, old, s as i8)
            {
                if n.unsigned_abs() >= factor(p as i8)? as u128 {
                    return Err(crate::Error::decimal_precision_overflow());
                }
                return Ok(Expr {
                    ty,
                    kind: Kind::Literal(Literal::Num(n)),
                });
            }
            if matches!(
                value.ty,
                Type::Int | Type::Decimal(_) | Type::Float | Type::Null
            ) {
                // Float sources round the exact binary value half away from
                // zero (owner decision, October 15, 2026).
                return Ok(Expr {
                    kind: Kind::CastDecimal(Box::new(value), p as u8, s as i8),
                    ty,
                });
            }
            if value.ty != Type::Text {
                return Err(
                    "unsupported: DECIMAL cast requires an exact numeric source or Float".into(),
                );
            }
            Target::Decimal(p as u8, s as i8)
        }
        SqlType::Float(None) => {
            return Err(
                "unsupported: CAST AS FLOAT precision differs between references; use DOUBLE or FLOAT8 explicitly"
                    .into(),
            );
        }
        SqlType::Boolean | SqlType::Bool => Target::Bool,
        SqlType::Date => {
            // The base binder accepted any chrono-parsable literal text.
            if let Kind::Literal(Literal::Text(text)) = &value.kind
                && let Ok(date) = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
                && let Ok(days) = i32::try_from(
                    date.signed_duration_since(
                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                    )
                    .num_days(),
                )
            {
                return Ok(Expr {
                    ty: Type::Date,
                    kind: Kind::Literal(Literal::Date(days)),
                });
            }
            Target::Date
        }
        SqlType::Timestamp(None, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Target::Timestamp
        }
        SqlType::Timestamp(None, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            Target::TimestampTz
        }
        SqlType::Timestamp(Some(_), _) => {
            return Err(
                "unsupported: TIMESTAMP(p) precision (PostgreSQL rounds to p digits, DuckDB ignores p)"
                    .into(),
            );
        }
        _ => return Err(format!("unsupported: CAST AS {sql}").into()),
    };
    // Unknown source/target combinations refuse even on empty input, while
    // known casts preserve typed NULL without parsing a fabricated value.
    let supported = match target {
        Target::Integer | Target::BigInt => matches!(
            value.ty,
            Type::Int | Type::Decimal(_) | Type::Float | Type::Text | Type::Null
        ),
        Target::Float | Target::Real | Target::Decimal(_, _) => {
            matches!(value.ty, Type::Text | Type::Null)
        }
        Target::Bool => matches!(value.ty, Type::Bool | Type::Text | Type::Null),
        Target::Date => matches!(
            value.ty,
            Type::Date | Type::Timestamp | Type::TimestampTz | Type::Text | Type::Null
        ),
        Target::Timestamp => matches!(
            value.ty,
            Type::Timestamp | Type::TimestampTz | Type::Date | Type::Text | Type::Null
        ),
        Target::TimestampTz => matches!(
            value.ty,
            Type::Timestamp | Type::TimestampTz | Type::Date | Type::Text | Type::Null
        ),
        Target::BoolText => value.ty == Type::Bool,
    };
    if !supported {
        return Err(unknown(&value.ty, &target));
    }
    if matches!(value.kind, Kind::Literal(Literal::Null)) {
        return Ok(typed_null(target.ty()));
    }
    // A successful constant DATE conversion is a total value, just like a
    // typed DATE literal. Keep it visible to scan pruning, predicate pushdown
    // and membership/fusion admission instead of reparsing it for every row.
    // Failed conversions stay in the expression: binding must not introduce
    // errors on empty inputs or in CASE arms that never execute.
    if target == Target::Date
        && let Kind::Literal(Literal::Text(text)) = &value.kind
        && let Ok(days) = date_text(text, &value.ty, &target)
    {
        return Ok(Expr {
            ty: Type::Date,
            kind: Kind::Literal(Literal::Date(days)),
        });
    }
    if target == Target::Timestamp {
        if value.ty == Type::Timestamp {
            return Ok(value);
        }
        if let Kind::Literal(Literal::Text(text)) = &value.kind
            && let Ok(micros) = crate::expr::timestamp::parse(text)
        {
            return Ok(Expr {
                ty: Type::Timestamp,
                kind: Kind::Literal(Literal::Timestamp(micros)),
            });
        }
        if value.ty == Type::Date {
            return Ok(Expr {
                ty: Type::Timestamp,
                kind: Kind::Function("$ts_from_date".into(), Box::new(value)),
            });
        }
    }
    // TIMESTAMPTZ conversions read and write wall clocks in the session zone.
    let zoned = |op: &str, ty: Type, value: Expr| -> Result<Expr> {
        let zone = crate::zone::statement()?.zone;
        Ok(Expr {
            ty,
            kind: Kind::Function(crate::zone::function(op, &zone), Box::new(value)),
        })
    };
    match (&target, &value.ty) {
        (Target::TimestampTz, Type::TimestampTz) => return Ok(value),
        (Target::TimestampTz, Type::Timestamp) => {
            return zoned("from_ts", Type::TimestampTz, value);
        }
        (Target::TimestampTz, Type::Date) => return zoned("from_date", Type::TimestampTz, value),
        (Target::TimestampTz, Type::Text) => {
            let zone = crate::zone::statement()?.zone;
            if let Kind::Literal(Literal::Text(text)) = &value.kind
                && let Ok(micros) = crate::zone::parse_text(text, &zone)
            {
                return Ok(Expr {
                    ty: Type::TimestampTz,
                    kind: Kind::Literal(Literal::Timestamp(micros)),
                });
            }
            return zoned("parse", Type::TimestampTz, value);
        }
        (Target::Timestamp, Type::TimestampTz) => return zoned("to_ts", Type::Timestamp, value),
        (Target::Date, Type::TimestampTz) => return zoned("to_date", Type::Date, value),
        _ => {}
    }
    if target == Target::Date && value.ty == Type::Timestamp {
        return Ok(Expr {
            ty: Type::Date,
            kind: Kind::Function("$ts_to_date".into(), Box::new(value)),
        });
    }
    Ok(Expr {
        ty: target.ty(),
        kind: Kind::DialectCast(Box::new(value), target),
    })
}
fn trim(s: &str) -> &str {
    s.trim_matches([' ', '\t', '\n', '\r', '\u{000b}', '\u{000c}'])
}
fn date_text(original: &str, source: &Type, target: &Target) -> Result<i32> {
    let s = trim(original);
    if s.len() != 10
        || s.as_bytes()[4] != b'-'
        || s.as_bytes()[7] != b'-'
        || !s
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return Err(unknown(source, target));
    }
    // Both references reject an impossible ISO calendar date.
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| format!("date/time field value out of range: \"{original}\""))?;
    if s.starts_with("0000") {
        return Err(refused(source, target));
    }
    Ok(date
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32)
}
fn text(value: &Value, row: usize) -> &str {
    value
        .array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}
fn float(value: &Value, row: usize) -> f64 {
    value
        .array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row)
}
/// DuckDB accepts underscores and radix prefixes in numeric text; whether
/// PostgreSQL's input functions do is not established, so these refuse.
fn unestablished_spelling(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    digits.contains('_')
        || ["0x", "0X", "0o", "0O", "0b", "0B"]
            .iter()
            .any(|prefix| digits.starts_with(prefix))
}
fn round_decimal(n: i128, scale: i8) -> Result<i128> {
    let divisor = factor(scale)?;
    let quotient = n / divisor;
    Ok(
        if scale > 0 && (n % divisor).unsigned_abs() >= (divisor / 2) as u128 {
            quotient + n.signum()
        } else {
            quotient
        },
    )
}
fn integer(value: &Value, source: &Type, target: &Target, row: usize) -> Result<i128> {
    // Owner decision, October 15, 2026: round like PostgreSQL and DuckDB.
    // Decimal ties go away from zero; binary64 ties go to even (PostgreSQL
    // rint and DuckDB 1.5.6 agree: 2.5 -> 2, 3.5 -> 4).
    let n = match source {
        Type::Int => numeric(&value.array).value(row),
        Type::Decimal(s) => round_decimal(numeric(&value.array).value(row), *s)?,
        Type::Float => {
            let x = float(value, row).round_ties_even();
            // -2^63 is exact in binary64 and fits; 2^63 does not.
            let bound = 9223372036854775808.0;
            if !x.is_finite() || x < -bound || x >= bound {
                return Err(out_of_range(target));
            }
            x as i128
        }
        Type::Text => {
            let original = text(value, row);
            let s = trim(original);
            let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                // Fractional/exponent/hex/underscore acceptance is deliberately
                // not inferred from the other engine's permissive parser.
                if unestablished_spelling(s) {
                    return Err(unknown(source, target));
                }
                if s.parse::<f64>().is_ok_and(f64::is_finite) {
                    return Err(refused(source, target));
                }
                return Err(invalid(integer_name(target), original));
            }
            s.parse::<i128>().map_err(|_| out_of_range(target))?
        }
        _ => return Err(unknown(source, target)),
    };
    if i64::try_from(n).is_err() {
        return Err(out_of_range(target));
    }
    Ok(n)
}

/// Decimal syntax is validated in full. Accumulate only the output digits and
/// one rounding digit, so long fractions and exponents cannot overflow i128 or
/// require a copied, unaccounted mantissa. Rounding is half away from zero.
/// Overflow keeps the base "decimal cast precision overflow" data error.
fn decimal_text(
    original: &str,
    source: &Type,
    target: &Target,
    precision: u8,
    scale: i8,
) -> Result<i128> {
    let s = trim(original);
    if unestablished_spelling(s) {
        return Err(unknown(source, target));
    }
    // PostgreSQL 17 numeric accepts NaN and infinities; DuckDB rejects them.
    let word = s.strip_prefix(['+', '-']).unwrap_or(s);
    if ["nan", "inf", "infinity"]
        .iter()
        .any(|w| word.eq_ignore_ascii_case(w))
    {
        return Err(refused(source, target));
    }
    let negative = s.starts_with('-');
    let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = if let Some(at) = unsigned.find(['e', 'E']) {
        (
            &unsigned[..at],
            unsigned[at + 1..]
                .parse::<i32>()
                .map_err(|_| invalid("numeric", original))?,
        )
    } else {
        (unsigned, 0)
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(invalid("numeric", original));
    }
    let point = whole.len() as i64 + i64::from(exponent) + i64::from(scale);
    let limit = factor(precision as i8)?;
    let mut n = 0_i128;
    let mut round = false;
    let mut count = 0_i64;
    for (i, digit) in whole.bytes().chain(fraction.bytes()).enumerate() {
        count += 1;
        if (i as i64) < point {
            n = n
                .checked_mul(10)
                .and_then(|n| n.checked_add(i128::from(digit - b'0')))
                .filter(|n| *n < limit)
                .ok_or_else(crate::Error::decimal_precision_overflow)?;
        } else if i as i64 == point {
            round = digit >= b'5';
        }
    }
    if n != 0 && point > count {
        let power = point - count;
        if power > 38 {
            return Err(crate::Error::decimal_precision_overflow());
        }
        n = n
            .checked_mul(factor(power as i8)?)
            .filter(|n| *n < limit)
            .ok_or_else(crate::Error::decimal_precision_overflow)?;
    }
    if round {
        n += 1;
    }
    if n >= limit {
        return Err(crate::Error::decimal_precision_overflow());
    }
    Ok(if negative { -n } else { n })
}
fn real(value: &Value, source: &Type, target: &Target, row: usize) -> Result<f64> {
    if *source != Type::Text {
        return Err(unknown(source, target));
    }
    let original = text(value, row);
    let s = trim(original);
    if unestablished_spelling(s) {
        return Err(unknown(source, target));
    }
    // Rust parses a complete value; no SQLite prefix or suffix removal.
    let x = s
        .parse::<f64>()
        .map_err(|_| invalid("double precision", original))?;
    let alphabetic = s
        .bytes()
        .any(|b| b.is_ascii_alphabetic() && !matches!(b, b'e' | b'E'));
    if *target == Target::Real && !alphabetic {
        // Complete numeric syntax: return the base SQLite REAL value,
        // including its underflow and nonfinite behaviour.
        return casts::sqlite_real(original.as_bytes());
    }
    if x == 0.0
        && s.split(['e', 'E'])
            .next()
            .unwrap()
            .bytes()
            .any(|b| (b'1'..=b'9').contains(&b))
    {
        return Err(refused(source, target)); // PG underflow error, DuckDB zero.
    }
    if !x.is_finite() {
        return Err("unsupported: floating cast nonfinite result".into());
    }
    Ok(x)
}
/// PostgreSQL 17 boolin (datatype-boolean.html): case-insensitive unique
/// prefixes of true/false/yes/no, on/off with at least two letters, 1 and 0.
fn postgres_boolean(s: &str) -> bool {
    let s = s.to_ascii_lowercase();
    !s.is_empty()
        && (["true", "false", "yes", "no"]
            .iter()
            .any(|word| word.starts_with(&s))
            || (s.len() >= 2 && (s == "on" || "off".starts_with(&s)))
            || s == "1"
            || s == "0")
}
pub(crate) fn evaluate(
    expr: &Expr,
    target: &Target,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let value = expr.eval(input, budget)?;
    let rows = value.array.len();
    let lease = budget.reserve(
        workspace(rows, 0)?
            .checked_add(if *target == Target::BoolText {
                rows.checked_mul(5).ok_or("cast workspace overflow")?
            } else {
                0
            })
            .and_then(|n| n.checked_add(128))
            .ok_or("cast workspace overflow")?,
    )?;
    let array: ArrayRef = match target {
        Target::Integer | Target::BigInt | Target::Decimal(_, _) => {
            let (precision, scale) = if let Target::Decimal(p, s) = target {
                (*p, *s)
            } else {
                (0, 0)
            };
            let mut builder = Decimal128Builder::with_capacity(rows)
                .with_precision_and_scale(38, scale)
                .map_err(|e| e.to_string())?;
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                } else {
                    builder.append_value(match target {
                        Target::Integer | Target::BigInt => integer(&value, &expr.ty, target, row)?,
                        _ if expr.ty == Type::Text => {
                            decimal_text(text(&value, row), &expr.ty, target, precision, scale)?
                        }
                        _ => return Err(unknown(&expr.ty, target)),
                    });
                }
            }
            Arc::new(builder.finish())
        }
        Target::Float | Target::Real => {
            let mut builder = Float64Builder::with_capacity(rows);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                } else {
                    builder.append_value(real(&value, &expr.ty, target, row)?);
                }
            }
            Arc::new(builder.finish())
        }
        Target::Bool => {
            let mut builder = BooleanBuilder::with_capacity(rows);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                    continue;
                }
                let b = if expr.ty == Type::Bool {
                    boolean(&value.array).value(row)
                } else {
                    let original = text(&value, row);
                    let s = trim(original);
                    if ["true", "t", "yes", "y", "1"]
                        .iter()
                        .any(|v| s.eq_ignore_ascii_case(v))
                    {
                        true
                    } else if ["false", "f", "no", "n", "0"]
                        .iter()
                        .any(|v| s.eq_ignore_ascii_case(v))
                    {
                        false
                    } else if postgres_boolean(s) {
                        // PostgreSQL accepts on/off and unique prefixes;
                        // DuckDB rejects them.
                        return Err(refused(&expr.ty, target));
                    } else {
                        return Err(invalid("boolean", original));
                    }
                };
                builder.append_value(b);
            }
            Arc::new(builder.finish())
        }
        Target::BoolText => {
            let mut builder = StringBuilder::with_capacity(rows, rows * 5);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                } else {
                    builder.append_value(if boolean(&value.array).value(row) {
                        "true"
                    } else {
                        "false"
                    });
                }
            }
            Arc::new(builder.finish())
        }
        Target::Date => {
            let mut builder = Date32Builder::with_capacity(rows);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                    continue;
                }
                let days = if expr.ty == Type::Date {
                    value
                        .array
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(row)
                } else {
                    date_text(text(&value, row), &expr.ty, target)?
                };
                builder.append_value(days);
            }
            Arc::new(builder.finish())
        }
        Target::TimestampTz => unreachable!("TIMESTAMPTZ casts bind as zone functions"),
        Target::Timestamp => {
            // DATE and TIMESTAMP sources were bound as functions above.
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                    continue;
                }
                builder.append_value(
                    crate::expr::timestamp::parse(text(&value, row)).map_err(crate::Error::from)?,
                );
            }
            Arc::new(builder.finish())
        }
    };
    Ok(Value::allocated(array, lease))
}
