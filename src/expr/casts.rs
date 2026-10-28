//! Explicit numeric/text casts, including exact DECIMAL. No implicit conversion rules live here.
use super::*;
mod sqlite_real;

pub(crate) fn bind(value: Expr, target: Type) -> Result<Expr> {
    let supported = match target {
        Type::Int | Type::Float => matches!(
            value.ty,
            Type::Int
                | Type::Decimal(_)
                | Type::Float
                | Type::Bool
                | Type::Text
                | Type::Binary
                | Type::Null
        ),
        // DOUBLE/REAL text uses DuckDB 1.5.6's shortest round-trip format
        // (approved October 16, 2026), not SQLite's %!.15g; see float_text.
        Type::Text => matches!(
            value.ty,
            Type::Int
                | Type::Decimal(_)
                | Type::Float
                | Type::Date
                | Type::Timestamp
                | Type::Bool
                | Type::Text
                | Type::Binary
                | Type::Null
        ),
        _ => false,
    };
    if target == Type::Text && value.ty == Type::TimestampTz {
        // Printed in the session zone with its offset, as both references do.
        let zone = crate::zone::statement()?.zone;
        return Ok(Expr {
            kind: Kind::Function(crate::zone::function("text", &zone), Box::new(value)),
            ty: Type::Text,
        });
    }
    if !supported {
        return Err(format!("unsupported: CAST {:?} AS {target:?}", value.ty).into());
    }
    if matches!(value.kind, Kind::Literal(Literal::Null)) {
        return Ok(Expr {
            kind: Kind::Literal(Literal::Null),
            ty: target,
        });
    }
    // INTEGER-to-INTEGER is identity under the exact-intermediate rule.
    // Conversions from other storage classes retain SQLite saturation.
    if value.ty == Type::Text && target == Type::Text {
        return Ok(value);
    }
    Ok(Expr {
        kind: Kind::Cast(Box::new(value)),
        ty: target,
    })
}

fn leading(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .take_while(|b| matches!(**b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r'))
        .count()
}
fn integer_prefix(bytes: &[u8]) -> i64 {
    let mut at = leading(bytes);
    let negative = bytes.get(at) == Some(&b'-');
    if matches!(bytes.get(at), Some(b'+' | b'-')) {
        at += 1;
    }
    let limit = if negative {
        1u64 << 63
    } else {
        i64::MAX as u64
    };
    let mut value = 0u64;
    while let Some(byte) = bytes.get(at).filter(|b| b.is_ascii_digit()) {
        value = value
            .saturating_mul(10)
            .saturating_add(u64::from(*byte - b'0'))
            .min(limit);
        at += 1;
    }
    if negative {
        if value == 1u64 << 63 {
            i64::MIN
        } else {
            -(value as i64)
        }
    } else {
        value as i64
    }
}
/// The base SQLite TEXT-to-REAL value, used by explicit REAL casts once the
/// complete input has been validated.
pub(crate) fn sqlite_real(bytes: &[u8]) -> Result<f64> {
    real_prefix(bytes)
}
fn real_prefix(bytes: &[u8]) -> Result<f64> {
    let value = sqlite_real::parse(bytes);
    if !value.is_finite() {
        // Native JSON/session storage cannot consistently expose infinities.
        return Err("unsupported: REAL cast nonfinite result".into());
    }
    Ok(value)
}
fn bytes_at<'a>(value: &'a Value, source: &Type, row: usize) -> &'a [u8] {
    if *source == Type::Text {
        value
            .array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .as_bytes()
    } else {
        value
            .array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row)
    }
}
fn finite_float(value: &Value, row: usize) -> Result<f64> {
    let x = value
        .array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row);
    if x.is_nan() {
        Err("unsupported: cast from NaN".into())
    } else {
        Ok(x)
    }
}
fn integer_at(value: &Value, source: &Type, row: usize) -> Result<i128> {
    if *source == Type::Int {
        return Ok(numeric(&value.array).value(row));
    }
    Ok(i128::from(match source {
        Type::Int | Type::Decimal(_) => {
            let n = numeric(&value.array).value(row) / factor(source.scale()?)?;
            n.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
        }
        // Rust's specified saturating float-to-integer conversion truncates
        // toward zero and clamps both infinities/finite out-of-range values.
        Type::Float => finite_float(value, row)? as i64,
        Type::Bool => i64::from(boolean(&value.array).value(row)),
        Type::Text | Type::Binary => integer_prefix(bytes_at(value, source, row)),
        _ => return Err("invalid INTEGER cast source".into()),
    }))
}
fn real_at(value: &Value, source: &Type, row: usize) -> Result<f64> {
    let x = match source {
        Type::Int | Type::Decimal(_) => {
            crate::numeric_float::scaled(numeric(&value.array).value(row), source.scale()?)?
        }
        Type::Float => finite_float(value, row)?,
        Type::Bool => {
            if boolean(&value.array).value(row) {
                1.0
            } else {
                0.0
            }
        }
        Type::Text | Type::Binary => real_prefix(bytes_at(value, source, row))?,
        _ => return Err("invalid REAL cast source".into()),
    };
    if !x.is_finite() {
        return Err("unsupported: REAL cast nonfinite result".into());
    }
    Ok(x)
}

#[inline(never)]
pub(super) fn evaluate(
    expr: &Expr,
    target: &Type,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let value = expr.eval(input, budget)?;
    let rows = value.array.len();
    if *target == Type::Text && expr.ty == Type::Text {
        return Ok(value);
    }
    let payload = if *target == Type::Text {
        match expr.ty {
            Type::Binary => (0..rows).try_fold(0usize, |n, row| {
                let size = if is_sql_null(value.array.as_ref(), row) {
                    0
                } else {
                    bytes_at(&value, &expr.ty, row).len()
                };
                n.checked_add(size).ok_or("cast text size overflow")
            })?,
            Type::Int => rows.checked_mul(40).ok_or("cast text size overflow")?,
            // Precision-38 coefficients, a sign/decimal point, and leading
            // fractional zeroes fit in 42 bytes even at scale 38.
            Type::Decimal(_) => rows.checked_mul(42).ok_or("cast text size overflow")?,
            Type::Date => rows.checked_mul(10).ok_or("cast text size overflow")?,
            // "9999-12-31 23:59:59.999999".
            Type::Timestamp => rows.checked_mul(26).ok_or("cast text size overflow")?,
            // "-1.7976931348623157e+308" and "-0.00012345678901234567" fit.
            Type::Float => rows.checked_mul(26).ok_or("cast text size overflow")?,
            Type::Bool => rows,
            Type::Null => 0,
            _ => return Err("unsupported: numeric TEXT formatting".into()),
        }
    } else {
        0
    };
    if *target == Type::Text && payload > i32::MAX as usize {
        return Err("resource-limit: cast text exceeds 32-bit offsets".into());
    }
    let bytes = workspace(rows, 0)?
        .checked_add(payload)
        .ok_or("cast workspace overflow")?;
    let lease = budget.reserve(bytes)?;
    let array: ArrayRef = match target {
        Type::Int => {
            let mut builder = Decimal128Builder::with_capacity(rows)
                .with_precision_and_scale(38, 0)
                .map_err(|e| e.to_string())?;
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                } else {
                    builder.append_value(integer_at(&value, &expr.ty, row)?);
                }
            }
            Arc::new(builder.finish())
        }
        Type::Float => {
            let mut builder = Float64Builder::with_capacity(rows);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                } else {
                    builder.append_value(real_at(&value, &expr.ty, row)?);
                }
            }
            Arc::new(builder.finish())
        }
        Type::Text => {
            let mut builder = StringBuilder::with_capacity(rows, payload);
            for row in 0..rows {
                if is_sql_null(value.array.as_ref(), row) {
                    builder.append_null();
                    continue;
                }
                match expr.ty {
                    Type::Int => builder.append_value(numeric(&value.array).value(row).to_string()),
                    Type::Decimal(scale) => builder.append_value(crate::types::decimal_text(
                        numeric(&value.array).value(row),
                        scale,
                    )),
                    Type::Date => {
                        use chrono::Datelike;
                        let days = value
                            .array
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap()
                            .value(row);
                        let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_signed(chrono::Duration::days(i64::from(days)))
                            .filter(|date| (1..=9999).contains(&date.year()))
                            .ok_or("unsupported: DATE to TEXT requires a year in 0001..9999")?;
                        builder.append_value(date.format("%Y-%m-%d").to_string());
                    }
                    // PostgreSQL and DuckDB print the same text in range.
                    Type::Timestamp => builder.append_value(crate::expr::timestamp::format(
                        crate::expr::timestamp::value(value.array.as_ref(), row),
                    )?),
                    Type::Float => builder.append_value(float_text(
                        value
                            .array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row),
                    )?),
                    Type::Bool => builder.append_value(if boolean(&value.array).value(row) {
                        "1"
                    } else {
                        "0"
                    }),
                    Type::Binary => builder.append_value(
                        std::str::from_utf8(bytes_at(&value, &expr.ty, row))
                            .map_err(|_| "unsupported: BLOB to TEXT requires valid UTF-8")?,
                    ),
                    _ => return Err("invalid TEXT cast source".into()),
                }
            }
            Arc::new(builder.finish())
        }
        _ => return Err("invalid cast target".into()),
    };
    Ok(Value::allocated(array, lease))
}

pub const FLOAT_TEXT_NONFINITE: &str =
    "unsupported: non-finite DOUBLE to TEXT (DuckDB prints inf/nan; PostgreSQL Infinity/NaN)";
pub const FLOAT_TEXT_NEGATIVE_ZERO: &str = "unsupported: negative zero DOUBLE to TEXT (Assay's -0.0 literal is REAL; DuckDB and PostgreSQL read DECIMAL zero, and their -0 formats differ)";

/// DuckDB 1.5.6 DOUBLE-to-VARCHAR text: the shortest digits that round-trip,
/// positional when the decimal exponent is in -4..16 (with at least one
/// fractional digit, e.g. 2.0), otherwise d[.ddd]e+XX / e-XX with at least two
/// exponent digits. PostgreSQL 17 uses the same digits but prints 2 for 2.0
/// and switches to exponents at 1e15; DuckDB is followed (Oct 16 decision).
pub fn float_text(x: f64) -> Result<String> {
    if !x.is_finite() {
        return Err(FLOAT_TEXT_NONFINITE.into());
    }
    if x == 0.0 {
        if x.is_sign_negative() {
            return Err(FLOAT_TEXT_NEGATIVE_ZERO.into());
        }
        return Ok("0.0".into());
    }
    // Rust's `{:e}` without a precision prints the shortest round-trip digits.
    let scientific = format!("{x:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits = mantissa.replace('.', "");
    let mut out = String::with_capacity(26);
    out.push_str(sign);
    if !(-4..16).contains(&exponent) {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push_str(if exponent < 0 { "e-" } else { "e+" });
        if exponent.unsigned_abs() < 10 {
            out.push('0');
        }
        out.push_str(&exponent.unsigned_abs().to_string());
    } else if exponent < 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-exponent - 1) as usize));
        out.push_str(&digits);
    } else {
        let integral = exponent as usize + 1;
        if digits.len() <= integral {
            out.push_str(&digits);
            out.extend(std::iter::repeat_n('0', integral - digits.len()));
            out.push_str(".0");
        } else {
            out.push_str(&digits[..integral]);
            out.push('.');
            out.push_str(&digits[integral..]);
        }
    }
    Ok(out)
}

/// Validate an already formatted TEXT value without truncating or copying it.
/// This remains fallible, so optimizers must not move it across row filters.
pub(crate) fn evaluate_varchar(
    expr: &Expr,
    limit: u64,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let value = expr.eval(input, budget)?;
    let strings = value.array.as_any().downcast_ref::<StringArray>().unwrap();
    for text in strings.iter().flatten() {
        if text.chars().count() as u64 > limit {
            return Err(format!(
                "unsupported: VARCHAR({limit}) result exceeds its character limit; truncation semantics differ"
            ).into());
        }
    }
    Ok(value)
}

/// Round the exact dyadic value, not a decimal rendering or a floating product.
/// For the admitted scales 0..=18, significand * 5^scale needs at most 95 bits.
fn float_decimal(value: f64, fives: u128, scale: i8, limit: i128) -> Result<i128> {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    if exponent == 0x7ff {
        return Err("decimal cast nonfinite source".into());
    }
    let fraction = bits & ((1_u64 << 52) - 1);
    let (significand, power) = if exponent == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1_u64 << 52), exponent - 1023 - 52)
    };
    let numerator = u128::from(significand) * fives;
    let power = power + i32::from(scale);
    let magnitude = if power >= 0 {
        // Check the destination bound before shifting: checked_shl only checks
        // the shift count, not loss of high bits. Zero/subnormals go below.
        if power >= 128 || numerator > ((limit as u128 - 1) >> power) {
            return Err(crate::Error::decimal_precision_overflow());
        }
        numerator << power
    } else {
        let shift = -power;
        if shift >= 128 {
            // A <=95-bit numerator divided by >=2^128 is strictly below 1/2.
            0
        } else {
            let quotient = numerator >> shift;
            // The top discarded bit alone decides >= half, including ties.
            quotient + ((numerator >> (shift - 1)) & 1)
        }
    };
    if magnitude >= limit as u128 {
        return Err(crate::Error::decimal_precision_overflow());
    }
    let coefficient = magnitude as i128;
    Ok(if bits >> 63 == 0 {
        coefficient
    } else {
        -coefficient
    })
}

/// Explicit numeric conversion. Implicit rescaling remains lossless.
pub(crate) fn evaluate_decimal(
    expr: &Expr,
    precision: u8,
    scale: i8,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let value = expr.eval(input, budget)?;
    let lease = budget.reserve(workspace(input.len, 0)?)?;
    let mut builder = Decimal128Builder::with_capacity(input.len)
        .with_precision_and_scale(38, scale)
        .map_err(|e| e.to_string())?;
    let limit = factor(precision as i8)?;
    let floats = value.array.as_any().downcast_ref::<Float64Array>();
    let fives = if floats.is_some() {
        5_u128.pow(scale as u32)
    } else {
        1
    };
    // NULL has no numeric scale; Float uses its exact dyadic conversion below.
    let old = if matches!(expr.ty, Type::Null | Type::Float) {
        scale
    } else {
        expr.ty.scale()?
    };
    let adjustment = factor((scale - old).abs())?;
    for row in 0..input.len {
        if is_sql_null(value.array.as_ref(), row) {
            builder.append_null();
            continue;
        }
        if let Some(floats) = floats {
            builder.append_value(float_decimal(floats.value(row), fives, scale, limit)?);
            continue;
        }
        let n = numeric(&value.array).value(row);
        let converted = if scale >= old {
            n.checked_mul(adjustment)
                .ok_or_else(crate::Error::decimal_precision_overflow)?
        } else {
            let divisor = adjustment;
            let quotient = n / divisor;
            let remainder = n % divisor;
            // Remainders are strictly smaller than 10^38, so this comparison
            // avoids a potentially overflowing multiply-by-two.
            if remainder.unsigned_abs() >= (divisor / 2) as u128 {
                quotient + n.signum()
            } else {
                quotient
            }
        };
        if converted <= -limit || converted >= limit {
            return Err(crate::Error::decimal_precision_overflow());
        }
        builder.append_value(converted);
    }
    Ok(Value::allocated(Arc::new(builder.finish()), lease))
}
