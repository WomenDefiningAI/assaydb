//! `Kind::Pack(a, b)`: two aggregate arguments carried as one Binary value.
//!
//! Two-argument aggregates (CORR, REGR_*, ARG_MAX, ...) reuse the one-input
//! aggregate pipeline, including projection, partitioning and spill, by
//! packing both arguments into one row value. The row is NULL when either
//! argument is NULL, which is exactly the set of rows these aggregates skip.
//! The bytes are the two encodings back to back; the binder records both
//! argument types, which the consuming aggregate uses to decode them:
//!
//! * INTEGER/DECIMAL: the i128 mantissa, 16 bytes little-endian;
//! * REAL: the binary64 bits, 8 bytes little-endian (bits preserved);
//! * DATE: i32 days, 4 bytes; TIMESTAMP/TIMESTAMPTZ: i64 microseconds, 8
//!   bytes; BOOLEAN: one byte;
//! * TEXT: u32 little-endian byte length, then the UTF-8 bytes.
use super::*;
use crate::execute::Key;

fn width(ty: &Type) -> Result<Option<usize>> {
    Ok(match ty {
        Type::Int | Type::Decimal(_) => Some(16),
        Type::Float => Some(8),
        Type::Date => Some(4),
        Type::Timestamp | Type::TimestampTz => Some(8),
        Type::Bool => Some(1),
        Type::Text => None,
        Type::Null => Some(0),
        Type::Binary => return Err("unsupported: BLOB aggregate argument".into()),
    })
}

fn encoded_len(value: &Value, ty: &Type, row: usize) -> Result<usize> {
    Ok(match width(ty)? {
        Some(n) => n,
        None => {
            4 + value
                .array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("bound TEXT argument")
                .value(row)
                .len()
        }
    })
}

fn encode(out: &mut Vec<u8>, value: &Value, ty: &Type, row: usize) {
    let array = value.array.as_any();
    match ty {
        Type::Int | Type::Decimal(_) => {
            out.extend_from_slice(&numeric(&value.array).value(row).to_le_bytes())
        }
        Type::Float => out.extend_from_slice(
            &array
                .downcast_ref::<Float64Array>()
                .expect("bound REAL argument")
                .value(row)
                .to_bits()
                .to_le_bytes(),
        ),
        Type::Date => out.extend_from_slice(
            &array
                .downcast_ref::<Date32Array>()
                .expect("bound DATE argument")
                .value(row)
                .to_le_bytes(),
        ),
        Type::Timestamp | Type::TimestampTz => out.extend_from_slice(
            &crate::expr::timestamp::value(value.array.as_ref(), row).to_le_bytes(),
        ),
        Type::Bool => out.push(u8::from(boolean(&value.array).value(row))),
        Type::Text => {
            let text = array
                .downcast_ref::<StringArray>()
                .expect("bound TEXT argument")
                .value(row);
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        Type::Null | Type::Binary => {}
    }
}

pub(super) fn evaluate(a: &Expr, b: &Expr, input: &Input, budget: &Budget) -> Result<Value> {
    let n = input.len;
    let left = a.eval(input, budget)?;
    let right = b.eval(input, budget)?;
    let mut bytes = 0_usize;
    let mut valid = Vec::with_capacity(n);
    for row in 0..n {
        let present = !is_sql_null(left.array.as_ref(), row)
            && !is_sql_null(right.array.as_ref(), row)
            && a.ty != Type::Null
            && b.ty != Type::Null;
        if present {
            bytes = encoded_len(&left, &a.ty, row)?
                .checked_add(encoded_len(&right, &b.ty, row)?)
                .and_then(|len| bytes.checked_add(len))
                .ok_or("resource-limit: binary array exceeds 32-bit offsets")?;
        }
        valid.push(present);
    }
    if bytes > i32::MAX as usize {
        return Err("resource-limit: binary array exceeds 32-bit offsets".into());
    }
    // Exact preallocation: the payload, offsets and validity are funded first.
    let lease = budget.reserve(
        bytes
            .checked_add(n.checked_mul(8).ok_or("pack workspace overflow")?)
            .and_then(|total| total.checked_add(1024))
            .ok_or("pack workspace overflow")?,
    )?;
    let mut builder = BinaryBuilder::with_capacity(n, bytes);
    let mut scratch = Vec::new();
    for (row, present) in valid.into_iter().enumerate() {
        if present {
            scratch.clear();
            encode(&mut scratch, &left, &a.ty, row);
            encode(&mut scratch, &right, &b.ty, row);
            builder.append_value(&scratch);
        } else {
            builder.append_null();
        }
    }
    Ok(Value::allocated(Arc::new(builder.finish()), lease))
}

/// One decoded argument and the remaining bytes.
pub(crate) fn split<'a>(bytes: &'a [u8], ty: &Type) -> (&'a [u8], &'a [u8]) {
    let len = match width(ty).expect("bound pack argument") {
        Some(n) => n,
        None => 4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize,
    };
    bytes.split_at(len)
}

pub(crate) fn exact(bytes: &[u8]) -> i128 {
    i128::from_le_bytes(bytes.try_into().expect("16-byte exact argument"))
}
pub(crate) fn real(bytes: &[u8]) -> f64 {
    f64::from_bits(u64::from_le_bytes(
        bytes.try_into().expect("8-byte REAL argument"),
    ))
}
/// A numeric argument as binary64 (exact numbers rounded once).
pub(crate) fn as_f64(bytes: &[u8], ty: &Type) -> Result<f64> {
    match ty {
        Type::Float => Ok(real(bytes)),
        _ => crate::numeric_float::scaled(exact(bytes), ty.scale()?),
    }
}

/// The output key of one encoded argument. REAL keeps its exact bits
/// (ARG_MAX returns -0.0 as -0.0, as DuckDB does).
pub(crate) fn key(bytes: &[u8], ty: &Type) -> Key {
    match ty {
        Type::Int | Type::Decimal(_) => Key::Num(exact(bytes)),
        Type::Float => Key::Float(real(bytes).to_bits()),
        Type::Date => Key::Date(i32::from_le_bytes(bytes.try_into().unwrap())),
        Type::Timestamp | Type::TimestampTz => {
            Key::Timestamp(i64::from_le_bytes(bytes.try_into().unwrap()))
        }
        Type::Bool => Key::Bool(bytes[0] != 0),
        Type::Text => Key::Text(String::from_utf8(bytes[4..].to_vec()).expect("UTF-8 argument")),
        Type::Null | Type::Binary => Key::Null,
    }
}

/// Order two encoded arguments of one type exactly as MIN/MAX order their keys.
pub(crate) fn compare(a: &[u8], b: &[u8], ty: &Type) -> std::cmp::Ordering {
    match ty {
        Type::Int | Type::Decimal(_) => exact(a).cmp(&exact(b)),
        // Text keys order by their UTF-8 bytes, like String's Ord.
        Type::Text => a[4..].cmp(&b[4..]),
        // MIN/MAX compare canonical REAL keys (-0.0 equals 0.0).
        Type::Float => crate::execute::compare_extreme_keys(
            &crate::execute::float_key(real(a)),
            &crate::execute::float_key(real(b)),
        ),
        _ => crate::execute::compare_extreme_keys(&key(a, ty), &key(b, ty)),
    }
}
