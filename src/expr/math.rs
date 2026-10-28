//! Scalar math functions. Result types follow DuckDB 1.5.6 `typeof`; values
//! follow DuckDB 1.5.6 and PostgreSQL 17 where they agree. Cases where they
//! disagree raise a typed error instead of choosing one engine's answer:
//! finite overflow keeps the arithmetic `floating arithmetic overflow` error,
//! and POWER's complex/undefined results are `unsupported:`. Underflow to zero
//! returns zero, the recorded Assay/DuckDB arithmetic behavior.
//!
//! Binary64 functions call the platform libm through Rust's `f64` methods, the
//! same `std::` functions DuckDB calls, so results are bit-identical to DuckDB
//! on the same host (verified on macOS). LOG(b, x) is DuckDB's
//! `log10(x) / log10(b)`; RADIANS is `x * (pi / 180)`.
use super::*;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Function {
    Sqrt,
    Cbrt,
    Exp,
    Ln,
    /// LOG(x) and LOG10(x): base 10 in both references.
    Log10,
    Log2,
    /// LOG(base, x).
    LogBase,
    Power,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Radians,
    Pi,
    Floor,
    Ceil,
    /// TRUNC(x [, digits]); `None` is a NULL digits literal (NULL result).
    Trunc(Option<i32>),
    Sign,
    Greatest,
    Least,
}

pub(crate) const SQRT_NEGATIVE: &str = "cannot take square root of a negative number";
pub(crate) const LOG_ZERO: &str = "cannot take logarithm of zero";
pub(crate) const LOG_NEGATIVE: &str = "cannot take logarithm of a negative number";
pub(crate) const ASIN_RANGE: &str = "ASIN is undefined outside [-1,1]";
pub(crate) const ACOS_RANGE: &str = "ACOS is undefined outside [-1,1]";
pub(crate) const SIN_RANGE: &str = "SIN input is out of range";
pub(crate) const COS_RANGE: &str = "COS input is out of range";
pub(crate) const TAN_RANGE: &str = "TAN input is out of range";
pub(crate) const POWER_ZERO_NEGATIVE: &str = "unsupported: POWER of zero to a negative exponent (DuckDB returns infinity, PostgreSQL raises an error)";
pub(crate) const LOG_NAN: &str = "unsupported: LOG with NaN beside an out-of-domain argument (DuckDB raises an error, PostgreSQL returns NaN)";
pub(crate) const LOG_INFINITY: &str =
    "unsupported: LOG of infinity to a base in (0, 1] (DuckDB and PostgreSQL differ)";
pub(crate) const POWER_NEGATIVE_FRACTION: &str = "unsupported: POWER of a negative number to a non-integer exponent (DuckDB returns NaN, PostgreSQL raises an error)";
const OVERFLOW: &str = "floating arithmetic overflow";
const DIVISION_BY_ZERO: &str = "division by zero";

/// Row-evaluation failures raised here, for deterministic error replay.
/// `division by zero` and `floating arithmetic overflow` are already listed.
pub(crate) const RUNTIME_ERRORS: [&str; 12] = [
    SQRT_NEGATIVE,
    LOG_ZERO,
    LOG_NEGATIVE,
    ASIN_RANGE,
    ACOS_RANGE,
    SIN_RANGE,
    COS_RANGE,
    TAN_RANGE,
    POWER_ZERO_NEGATIVE,
    POWER_NEGATIVE_FRACTION,
    LOG_NAN,
    LOG_INFINITY,
];

fn logarithm(x: f64, f: fn(f64) -> f64) -> Result<f64> {
    if x == 0.0 {
        Err(LOG_ZERO.into())
    } else if x < 0.0 {
        Err(LOG_NEGATIVE.into())
    } else {
        Ok(f(x))
    }
}

fn unary(function: Function, x: f64) -> Result<f64> {
    Ok(match function {
        Function::Sqrt if x < 0.0 => return Err(SQRT_NEGATIVE.into()),
        Function::Sqrt => x.sqrt(),
        Function::Cbrt => x.cbrt(),
        Function::Exp => {
            let result = x.exp();
            if result.is_infinite() && x.is_finite() {
                return Err(OVERFLOW.into());
            }
            result
        }
        Function::Ln => logarithm(x, f64::ln)?,
        Function::Log10 => logarithm(x, f64::log10)?,
        Function::Log2 => logarithm(x, f64::log2)?,
        Function::Sin if x.is_infinite() => return Err(SIN_RANGE.into()),
        Function::Cos if x.is_infinite() => return Err(COS_RANGE.into()),
        Function::Tan if x.is_infinite() => return Err(TAN_RANGE.into()),
        Function::Sin => x.sin(),
        Function::Cos => x.cos(),
        Function::Tan => x.tan(),
        Function::Asin if !(-1.0..=1.0).contains(&x) && !x.is_nan() => {
            return Err(ASIN_RANGE.into());
        }
        Function::Acos if !(-1.0..=1.0).contains(&x) && !x.is_nan() => {
            return Err(ACOS_RANGE.into());
        }
        Function::Asin => x.asin(),
        Function::Acos => x.acos(),
        Function::Atan => x.atan(),
        Function::Radians => x.to_radians(),
        Function::Floor => x.floor(),
        Function::Ceil => x.ceil(),
        _ => unreachable!("bound unary Float function"),
    })
}

fn binary_float(function: Function, x: f64, y: f64) -> Result<f64> {
    Ok(match function {
        Function::Power => {
            if x == 0.0 && y < 0.0 {
                return Err(POWER_ZERO_NEGATIVE.into());
            }
            if x < 0.0 && y.is_finite() && y.fract() != 0.0 {
                return Err(POWER_NEGATIVE_FRACTION.into());
            }
            let result = x.powf(y);
            if result.is_infinite() && x.is_finite() && y.is_finite() {
                return Err(OVERFLOW.into());
            }
            result
        }
        Function::Atan2 => x.atan2(y),
        Function::LogBase => {
            // PostgreSQL's NUMERIC log returns NaN for any NaN operand, and
            // +infinity for log(b, +infinity) with 0 < b < 1; DuckDB raises
            // its domain error or returns -infinity there.
            if (x.is_nan() && y <= 0.0) || (y.is_nan() && (x <= 0.0 || x == 1.0)) {
                return Err(LOG_NAN.into());
            }
            if y == f64::INFINITY && x > 0.0 && x <= 1.0 {
                return Err(LOG_INFINITY.into());
            }
            // DuckDB's order: the base's domain, then the zero denominator,
            // then the argument's domain.
            let base = logarithm(x, f64::log10)?;
            if base == 0.0 {
                return Err(DIVISION_BY_ZERO.into());
            }
            logarithm(y, f64::log10)? / base
        }
        _ => unreachable!("bound binary Float function"),
    })
}

/// DuckDB 1.5.6 TRUNC(DOUBLE, INTEGER): scale, truncate, unscale, keeping the
/// input when the result is not finite (unlike ROUND's zero for negative
/// precision). Uses ROUND's correctly rounded powers of ten.
pub(crate) fn truncate_float(value: f64, digits: i32) -> f64 {
    if !(-308..=308).contains(&digits) {
        return value;
    }
    let power = round::pow10(digits.abs());
    let result = if digits < 0 {
        (value / power).trunc() * power
    } else {
        (value * power).trunc() / power
    };
    if result.is_finite() { result } else { value }
}

/// Exact Decimal/Int truncation toward zero at `digits` places of a value
/// with `scale`; the result is at scale `clamp(digits, 0, scale)`.
fn truncate_exact(value: i128, scale: i8, digits: i32) -> Result<i128> {
    let scale = i32::from(scale);
    if digits >= scale {
        return Ok(value);
    }
    let places = i64::from(scale) - i64::from(digits);
    if places > 38 {
        return Ok(0);
    }
    let quotient = value / factor(places as i8)?;
    if digits >= 0 {
        return Ok(quotient);
    }
    // |quotient * 10^-digits| <= |value| / 10^scale, so this cannot exceed
    // the input's integral magnitude (precision 38).
    checked(quotient.checked_mul(factor((-digits) as i8)?))
}

fn floor_ceil_exact(value: i128, scale: i8, ceil: bool) -> Result<i128> {
    let divisor = factor(scale)?;
    let quotient = value / divisor;
    let remainder = value % divisor;
    Ok(if ceil && remainder > 0 {
        quotient + 1
    } else if !ceil && remainder < 0 {
        quotient - 1
    } else {
        quotient
    })
}

fn floats(value: &Value, ty: &Type) -> Result<Float64Array> {
    to_float(&value.array, ty)
}

fn float_array(values: impl Iterator<Item = Result<Option<f64>>>) -> Result<ArrayRef> {
    Ok(Arc::new(values.collect::<Result<Float64Array>>()?))
}

/// NaN is greater than every other value and equal to itself (both
/// references); -0 equals 0, so the earlier argument wins that tie.
/// This is the engine's one SQL Float order (`numeric_compare::real_order`).
fn compare_float(a: f64, b: f64) -> Ordering {
    super::numeric_compare::real_order(a, b)
}

/// Index of the selected argument per row: NULL arguments are ignored (both
/// references); the result is NULL only when every argument is NULL. A later
/// argument replaces the current one only when strictly greater (or less).
fn select(
    n: usize,
    args: usize,
    greatest: bool,
    valid: impl Fn(usize, usize) -> bool,
    compare: impl Fn(usize, usize, usize) -> Ordering,
) -> Vec<Option<usize>> {
    let wanted = if greatest {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    (0..n)
        .map(|row| {
            let mut best = None;
            for arg in 0..args {
                if !valid(arg, row) {
                    continue;
                }
                match best {
                    Some(current) if compare(arg, current, row) != wanted => {}
                    _ => best = Some(arg),
                }
            }
            best
        })
        .collect()
}

fn extreme(
    args: &[Expr],
    values: &[Value],
    ty: &Type,
    greatest: bool,
    n: usize,
    budget: &Budget,
    lease: Reservation,
) -> Result<Value> {
    let valid = |arg: usize, row: usize| !is_sql_null(values[arg].array.as_ref(), row);
    match ty {
        Type::Null => Ok(Value::allocated(new_null_array(&DataType::Null, n), lease)),
        Type::Float => {
            let columns = values
                .iter()
                .zip(args)
                .map(|(v, a)| floats(v, &a.ty))
                .collect::<Result<Vec<_>>>()?;
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                compare_float(columns[a].value(row), columns[b].value(row))
            });
            let array: ArrayRef = Arc::new(Float64Array::from_iter(
                chosen
                    .iter()
                    .enumerate()
                    .map(|(row, arg)| arg.map(|arg| columns[arg].value(row))),
            ));
            Ok(Value::allocated(array, lease))
        }
        Type::Int | Type::Decimal(_) => {
            let scale = ty.scale()?;
            let columns = values
                .iter()
                .zip(args)
                .map(|(v, a)| {
                    let array = numeric(&v.array);
                    let from = a.ty.scale().unwrap_or(scale);
                    (0..n)
                        .map(|row| {
                            if valid_value(array, row) {
                                rescale(array.value(row), from, scale)
                            } else {
                                Ok(0)
                            }
                        })
                        .collect::<Result<Vec<i128>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                columns[a][row].cmp(&columns[b][row])
            });
            let array = decimal(
                chosen
                    .iter()
                    .enumerate()
                    .map(|(row, arg)| arg.map(|arg| columns[arg][row]))
                    .collect(),
                scale,
            )?;
            Ok(Value::allocated(array, lease))
        }
        Type::Date => {
            let columns = values
                .iter()
                .map(|v| v.array.as_any().downcast_ref::<Date32Array>().unwrap())
                .collect::<Vec<_>>();
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                columns[a].value(row).cmp(&columns[b].value(row))
            });
            let array: ArrayRef = Arc::new(Date32Array::from_iter(
                chosen
                    .iter()
                    .enumerate()
                    .map(|(row, arg)| arg.map(|arg| columns[arg].value(row))),
            ));
            Ok(Value::allocated(array, lease))
        }
        // TIMESTAMP and TIMESTAMPTZ compare their microseconds (TIMESTAMPTZ
        // instants are UTC), as both references do.
        Type::Timestamp | Type::TimestampTz => {
            let columns = values
                .iter()
                .map(|v| crate::expr::timestamp::values(&v.array))
                .collect::<Vec<_>>();
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                columns[a].value(row).cmp(&columns[b].value(row))
            });
            let array = crate::expr::timestamp::labelled(
                TimestampMicrosecondArray::from_iter(
                    chosen
                        .iter()
                        .enumerate()
                        .map(|(row, arg)| arg.map(|arg| columns[arg].value(row))),
                ),
                ty,
            );
            Ok(Value::allocated(array, lease))
        }
        Type::Bool => {
            let columns = values.iter().map(|v| boolean(&v.array)).collect::<Vec<_>>();
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                columns[a].value(row).cmp(&columns[b].value(row))
            });
            let array: ArrayRef = Arc::new(BooleanArray::from_iter(
                chosen
                    .iter()
                    .enumerate()
                    .map(|(row, arg)| arg.map(|arg| columns[arg].value(row))),
            ));
            Ok(Value::allocated(array, lease))
        }
        Type::Text => {
            // Byte order, the same comparison as Assay's TEXT `<`/`>`.
            let columns = values
                .iter()
                .map(|v| v.array.as_any().downcast_ref::<StringArray>().unwrap())
                .collect::<Vec<_>>();
            let chosen = select(n, args.len(), greatest, valid, |a, b, row| {
                columns[a].value(row).cmp(columns[b].value(row))
            });
            let bytes = chosen
                .iter()
                .enumerate()
                .try_fold(0_usize, |total, (row, arg)| {
                    total.checked_add(arg.map_or(0, |arg| columns[arg].value(row).len()))
                })
                .filter(|total| *total <= i32::MAX as usize)
                .ok_or("resource-limit: GREATEST/LEAST text exceeds 32-bit offsets")?;
            let text = budget.reserve(
                bytes
                    .checked_add(n.checked_mul(8).ok_or("batch size overflow")?)
                    .and_then(|total| total.checked_add(256))
                    .ok_or("batch size overflow")?,
            )?;
            let mut builder = StringBuilder::with_capacity(n, bytes);
            for (row, arg) in chosen.iter().enumerate() {
                match arg {
                    Some(arg) => builder.append_value(columns[*arg].value(row)),
                    None => builder.append_null(),
                }
            }
            drop(lease);
            Ok(Value::allocated(Arc::new(builder.finish()), text))
        }
        Type::Binary => Err("unsupported: GREATEST/LEAST over BLOB".into()),
    }
}

fn valid_value(array: &Decimal128Array, row: usize) -> bool {
    !array.is_null(row)
}

pub(super) fn evaluate(
    function: Function,
    args: &[Expr],
    ty: &Type,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let n = input.len;
    // Output plus one exact-number column per argument (GREATEST/LEAST
    // rescales DECIMAL arguments), like Expr::eval's workspace.
    let lease = budget.reserve(workspace(
        n,
        16_usize
            .checked_mul(args.len())
            .ok_or("batch size overflow")?,
    )?)?;
    if function == Function::Pi {
        return Ok(Value::allocated(
            Arc::new(Float64Array::from(vec![std::f64::consts::PI; n])),
            lease,
        ));
    }
    let values = args
        .iter()
        .map(|a| a.eval(input, budget))
        .collect::<Result<Vec<_>>>()?;
    let array: ArrayRef = match function {
        Function::Greatest | Function::Least => {
            return extreme(
                args,
                &values,
                ty,
                function == Function::Greatest,
                n,
                budget,
                lease,
            );
        }
        Function::Power | Function::Atan2 | Function::LogBase => {
            let x = floats(&values[0], &args[0].ty)?;
            let y = floats(&values[1], &args[1].ty)?;
            float_array(x.iter().zip(y.iter()).map(|pair| match pair {
                (Some(x), Some(y)) => binary_float(function, x, y).map(Some),
                _ => Ok(None),
            }))?
        }
        Function::Trunc(None) => new_null_array(&internal_type(ty), n),
        Function::Floor | Function::Ceil | Function::Trunc(_) | Function::Sign
            if args[0].ty != Type::Float =>
        {
            let input_scale = args[0].ty.scale()?;
            let exact = |v: i128| -> Result<i128> {
                match function {
                    Function::Floor => floor_ceil_exact(v, input_scale, false),
                    Function::Ceil => floor_ceil_exact(v, input_scale, true),
                    Function::Trunc(Some(digits)) => truncate_exact(v, input_scale, digits),
                    _ => Ok(v.signum()),
                }
            };
            decimal(
                numeric(&values[0].array)
                    .iter()
                    .map(|v| v.map(exact).transpose())
                    .collect::<Result<Vec<_>>>()?,
                ty.scale()?,
            )?
        }
        Function::Sign => decimal(
            floats(&values[0], &args[0].ty)?
                .iter()
                .map(|v| {
                    v.map(|x| {
                        if x > 0.0 {
                            1
                        } else if x < 0.0 {
                            -1
                        } else {
                            0
                        }
                    })
                })
                .collect(),
            0,
        )?,
        Function::Trunc(Some(digits)) => float_array(
            floats(&values[0], &args[0].ty)?
                .iter()
                .map(|v| Ok(v.map(|x| truncate_float(x, digits)))),
        )?,
        _ => float_array(
            floats(&values[0], &args[0].ty)?
                .iter()
                .map(|v| v.map(|x| unary(function, x)).transpose()),
        )?,
    };
    drop(values);
    Ok(Value::allocated(array, lease))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_rounding_directions() {
        // -2.55, -0.004, 2.55 at scale 2/3; floor goes down, ceil up, trunc to zero.
        assert_eq!(floor_ceil_exact(-255, 2, false).unwrap(), -3);
        assert_eq!(floor_ceil_exact(-255, 2, true).unwrap(), -2);
        assert_eq!(floor_ceil_exact(-4, 3, false).unwrap(), -1);
        assert_eq!(floor_ceil_exact(-4, 3, true).unwrap(), 0);
        assert_eq!(floor_ceil_exact(255, 2, true).unwrap(), 3);
        assert_eq!(floor_ceil_exact(300, 2, true).unwrap(), 3);
        assert_eq!(truncate_exact(-255, 2, 1).unwrap(), -25);
        assert_eq!(truncate_exact(-2555, 2, -1).unwrap(), -20);
        assert_eq!(truncate_exact(-255, 2, 5).unwrap(), -255);
        assert_eq!(
            truncate_exact(i128::from(i64::MAX), 0, -18).unwrap(),
            9 * 10_i128.pow(18)
        );
        assert_eq!(truncate_exact(i128::from(i64::MAX), 0, -19).unwrap(), 0);
        assert_eq!(truncate_exact(123, 0, i32::MIN).unwrap(), 0);
        assert_eq!(truncate_exact(123, 0, i32::MAX).unwrap(), 123);
    }

    #[test]
    fn float_truncation_follows_the_reference_contract() {
        assert_eq!(truncate_float(-2.567, 2), -2.56);
        assert_eq!(truncate_float(12345.678, -2), 12300.0);
        assert_eq!(truncate_float(12345.678, -308), 0.0);
        assert_eq!(truncate_float(12345.678, -309), 12345.678);
        assert_eq!(truncate_float(12345.678, i32::MIN), 12345.678);
        assert_eq!(truncate_float(12345.678, 400), 12345.678);
        assert_eq!(truncate_float(f64::INFINITY, 2), f64::INFINITY);
        assert!(truncate_float(f64::NAN, -2).is_nan());
        assert_eq!(truncate_float(-0.004, 2).to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn nan_orders_last_and_signed_zeros_tie() {
        assert_eq!(compare_float(f64::NAN, f64::INFINITY), Ordering::Greater);
        assert_eq!(compare_float(f64::NAN, f64::NAN), Ordering::Equal);
        assert_eq!(compare_float(-0.0, 0.0), Ordering::Equal);
        assert_eq!(compare_float(f64::NEG_INFINITY, -1.0), Ordering::Less);
    }
}
