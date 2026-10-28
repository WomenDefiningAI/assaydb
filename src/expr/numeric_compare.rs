//! SQL float total order and exact Int/Float comparison without integer rounding.
use super::*;
use std::cmp::Ordering;

const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// PostgreSQL/DuckDB numeric equality: all NaNs are peers, as are signed zeros.
#[inline]
pub(crate) fn real_equal(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

/// Keep ordinary comparison operations on the fast path; no integer conversion
/// or sortable key allocation is needed for a scalar/vector comparison.
#[inline]
pub(crate) fn real_compare(op: Op, a: f64, b: f64) -> bool {
    match op {
        Op::Eq => real_equal(a, b),
        Op::Ne => !real_equal(a, b),
        Op::Lt => !a.is_nan() && (b.is_nan() || a < b),
        Op::Le => b.is_nan() || (!a.is_nan() && a <= b),
        Op::Gt => !b.is_nan() && (a.is_nan() || a > b),
        Op::Ge => a.is_nan() || (!b.is_nan() && a >= b),
        _ => unreachable!("bound comparison operator"),
    }
}

#[inline]
pub(crate) fn real_order(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()))
}

/// Private hash/sort keys only: preserve source payloads and signed-zero bits
/// in projected values. Positive canonical NaN also sorts above +inf in Arrow.
#[inline]
pub(crate) fn canonical(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::from_bits(CANONICAL_NAN_BITS)
    } else {
        value
    }
}

pub(crate) fn needs_canonical(array: &ArrayRef) -> bool {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .is_some_and(|a| {
            a.values().iter().any(|n| {
                (n.is_nan() && n.to_bits() != CANONICAL_NAN_BITS)
                    || (*n == 0.0 && n.is_sign_negative())
            })
        })
}

pub(crate) fn canonical_bytes(array: &ArrayRef) -> Result<usize> {
    if needs_canonical(array) {
        array
            .len()
            .checked_mul(9)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| "float key size overflow".into())
    } else {
        Ok(0)
    }
}

/// Caller funds one replacement float buffer before calling (9 bytes/row +
/// Arrow overhead). Ordinary finite arrays share their existing buffers.
pub(crate) fn canonical_array(array: &ArrayRef) -> ArrayRef {
    if needs_canonical(array) {
        let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
        Arc::new(Float64Array::from_iter(a.iter().map(|v| v.map(canonical))))
    } else {
        array.clone()
    }
}

/// Bounds make Rust's float-to-integer conversion exact and non-saturating.
/// Precision-38 Int intermediates use the same relation as public i64 values.
/// Every integer is below NaN, the greatest non-NULL SQL numeric value.
pub(crate) fn integer_real(integer: i128, real: f64) -> Option<Ordering> {
    if real.is_nan() {
        return Some(Ordering::Less);
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
/// Retained as a test control for the exact comparison kernel. Mixed
/// membership now uses rounded DOUBLE keys instead.
#[cfg(test)]
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
    fn sql_order_equates_all_nan_payloads_and_signed_zeros() {
        let ordered = [
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::from_bits(0xfff8000000000001),
            f64::from_bits(0x7ff0000000000001),
            f64::from_bits(0xfff0000000000001),
            f64::NAN,
        ];
        let ranks = [0, 1, 2, 2, 3, 4, 5, 5, 5, 5];
        for (i, a) in ordered.into_iter().enumerate() {
            for (j, b) in ordered.into_iter().enumerate() {
                let order = ranks[i].cmp(&ranks[j]);
                assert_eq!(real_order(a, b), order);
                assert_eq!(
                    canonical(a).to_bits() == canonical(b).to_bits(),
                    order.is_eq()
                );
                for (op, want) in [
                    (Op::Eq, order.is_eq()),
                    (Op::Ne, !order.is_eq()),
                    (Op::Lt, order.is_lt()),
                    (Op::Le, !order.is_gt()),
                    (Op::Gt, order.is_gt()),
                    (Op::Ge, !order.is_lt()),
                ] {
                    assert_eq!(real_compare(op, a, b), want, "{a:?} {op:?} {b:?}");
                }
            }
        }
    }

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
        assert_eq!(integer_real(0, f64::NAN), Some(Less));
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
