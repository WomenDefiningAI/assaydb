//! Exact Int/Float relation without first rounding the integer to binary64.
use super::*;
use std::cmp::Ordering;

/// Bounds make Rust's float-to-integer conversion exact and non-saturating.
/// Precision-38 Int intermediates use the same relation as public i64 values.
/// NaN stays unordered, preserving existing floating comparison policy.
pub(crate) fn integer_real(integer: i128, real: f64) -> Option<Ordering> {
    if real.is_nan() {
        return None;
    }
    if real < i128::MIN as f64 {
        return Some(Ordering::Greater);
    }
    // i128::MAX rounds up to 2^127; compare against that exclusive bound.
    if real >= -(i128::MIN as f64) {
        return Some(Ordering::Less);
    }
    let truncated = real as i128;
    let whole = integer.cmp(&truncated);
    if whole != Ordering::Equal {
        return Some(whole);
    }
    // Only the fractional remainder can distinguish equal truncated parts.
    // For large representable integers fract() is exactly zero.
    0.0_f64.partial_cmp(&real.fract())
}

/// A Float can equal an Int only when its original binary value is integral.
/// Used for mixed membership keys; neither operand is rounded to match.
pub(crate) fn exact_integer(real: f64) -> Option<i128> {
    if real.is_finite()
        && real >= i128::MIN as f64
        && real < -(i128::MIN as f64)
        && real.fract() == 0.0
    {
        Some(real as i128)
    } else {
        None
    }
}

#[inline(never)]
pub(super) fn compare(op: Op, av: &Value, bv: &Value, left_integer: bool) -> ArrayRef {
    let (integers, reals) = if left_integer { (av, bv) } else { (bv, av) };
    let integers = numeric(&integers.array);
    let reals = reals.array.as_any().downcast_ref::<Float64Array>().unwrap();
    Arc::new(BooleanArray::from_iter(
        integers
            .iter()
            .zip(reals.iter())
            .map(|(integer, real)| match (integer, real) {
                (Some(integer), Some(real)) => {
                    let relation = integer_real(integer, real)
                        .map(|order| if left_integer { order } else { order.reverse() });
                    Some(match op {
                        Op::Eq => relation == Some(Ordering::Equal),
                        Op::Ne => relation != Some(Ordering::Equal),
                        Op::Lt => relation == Some(Ordering::Less),
                        Op::Le => matches!(relation, Some(Ordering::Less | Ordering::Equal)),
                        Op::Gt => relation == Some(Ordering::Greater),
                        Op::Ge => matches!(relation, Some(Ordering::Greater | Ordering::Equal)),
                        _ => unreachable!("bound comparison operator"),
                    })
                }
                _ => None,
            }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_relation_covers_fraction_sign_and_wide_bounds() {
        use Ordering::*;
        for (integer, real, expected) in [
            (9_007_199_254_740_993, 9_007_199_254_740_992.0, Greater),
            (-9_007_199_254_740_993, -9_007_199_254_740_992.0, Less),
            (i64::MAX as i128, 9_223_372_036_854_775_808.0, Less),
            (i64::MIN as i128, -9_223_372_036_854_775_808.0, Equal),
            (i128::MIN, i128::MIN as f64, Equal),
            (i128::MIN + 1, i128::MIN as f64, Greater),
            (i128::MAX, -(i128::MIN as f64), Less),
            (1_i128 << 100, 2_f64.powi(100), Equal),
            ((1_i128 << 100) + 1, 2_f64.powi(100), Greater),
            (0, f64::from_bits(1), Less),
            (0, -f64::from_bits(1), Greater),
            (0, -0.0, Equal),
            (1, 1.5, Less),
            (-1, -1.5, Greater),
            (1, f64::INFINITY, Less),
            (-1, f64::NEG_INFINITY, Greater),
        ] {
            assert_eq!(integer_real(integer, real), Some(expected));
        }
        assert_eq!(integer_real(0, f64::NAN), None);
        for real in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.5,
            -0.5,
            -(i128::MIN as f64),
        ] {
            assert_eq!(exact_integer(real), None);
        }
        assert_eq!(exact_integer(-0.0), Some(0));
        assert_eq!(exact_integer(i128::MIN as f64), Some(i128::MIN));
    }
}
