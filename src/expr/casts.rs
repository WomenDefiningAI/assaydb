//! Explicit INTEGER/REAL/TEXT casts. No implicit conversion rules live here.
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
        // SQLite's REAL formatter is not Rust's Display implementation. Keep
        // unsupported conversions explicit rather than approximate its output.
        Type::Text => matches!(
            value.ty,
            Type::Int | Type::Bool | Type::Text | Type::Binary | Type::Null
        ),
        _ => false,
    };
    if !supported {
        return Err(format!("unsupported: CAST {:?} AS {target:?}", value.ty));
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
