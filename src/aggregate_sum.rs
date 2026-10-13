//! Exact aggregate totals; SQL output range is checked only at finalization.
//! With at most u64::MAX signed-i128 inputs, |sum| < 2^191, within signed i256.
use crate::{Result, types::checked};
use arrow::datatypes::i256;

#[inline]
pub(crate) fn add(sum: i256, value: i128) -> Result<i256> {
    sum.checked_add(i256::from_i128(value))
        .ok_or_else(|| "aggregate accumulator overflow".into())
}

/// INTEGER running totals obey the same precision-38 limit as scalar nodes.
/// Keep DECIMAL's separately approved wide-accumulator policy unchanged.
#[inline]
pub(crate) fn add_integer(sum: i256, value: i128) -> Result<i256> {
    let next = add(sum, value)?;
    finish(next)?;
    Ok(next)
}

pub(crate) fn finish(sum: i256) -> Result<i128> {
    // Values wider than i128 necessarily exceed the SQL precision-38 range.
    checked(Some(sum.to_i128().ok_or("decimal precision 38 overflow")?))
}

/// Round an exact integer to binary64, ties to even. Preserve the previous
/// native cast for all i128 totals. No intermediate SUM precision check, heap
/// allocation or foundation conversion that truncates discarded bits.
pub(crate) fn as_float(sum: i256) -> f64 {
    if let Some(narrow) = sum.to_i128() {
        return narrow as f64;
    }
    if sum == i256::MIN {
        return -2_f64.powi(255);
    }
    let negative = sum < i256::ZERO;
    let magnitude = if negative { -sum } else { sum };
    let shift = (256 - magnitude.leading_zeros() - 53) as u8;
    let mut significand = (magnitude >> shift).as_i128() as u64;
    let remainder = magnitude & ((i256::ONE << shift) - i256::ONE);
    let halfway = i256::ONE << (shift - 1);
    if remainder > halfway || (remainder == halfway && significand & 1 != 0) {
        significand += 1;
    }
    let value = significand as f64 * 2_f64.powi(i32::from(shift));
    if negative { -value } else { value }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_range_allows_cancellation_but_rejects_out_of_range_output() {
        let limit = 10_i128.pow(38) - 1;
        let sum = add(add(i256::from_i128(limit), 1).unwrap(), -1).unwrap();
        assert_eq!(finish(sum).unwrap(), limit);
        assert!(
            finish(add(sum, 1).unwrap())
                .unwrap_err()
                .contains("overflow")
        );
        let wide = add(i256::from_i128(limit), limit).unwrap();
        assert!(wide.to_i128().is_none());
        assert_eq!(as_float(wide) / 2., limit as f64);
        assert!(finish(wide).is_err());
        assert_eq!(finish(add(wide, -limit).unwrap()).unwrap(), limit);
    }

    #[test]
    fn wide_integer_conversion_rounds_ties_even_and_retains_sticky_bits() {
        for shift in [76_u8, 100, 138, 190, 202] {
            for significand in [(1_u64 << 52), (1_u64 << 52) + 1, (1_u64 << 53) - 1] {
                let start = i256::from_i128(significand as i128) << shift;
                let half = i256::ONE << (shift - 1);
                for extra in [half - i256::ONE, half, half + i256::ONE] {
                    let value = start + extra;
                    let increment =
                        usize::from(extra > half || (extra == half && significand & 1 != 0));
                    let expected =
                        (significand + increment as u64) as f64 * 2_f64.powi(i32::from(shift));
                    assert_eq!(as_float(value).to_bits(), expected.to_bits());
                    assert_eq!(as_float(-value).to_bits(), (-expected).to_bits());
                    // Independent standard decimal parser also retains low bits.
                    assert_eq!(
                        as_float(value).to_bits(),
                        value.to_string().parse::<f64>().unwrap().to_bits()
                    );
                }
            }
        }
        for value in [i128::MIN, -1, 0, 1, i128::MAX] {
            assert_eq!(
                as_float(i256::from_i128(value)).to_bits(),
                (value as f64).to_bits()
            );
        }
        assert_eq!(as_float(i256::MIN), -2_f64.powi(255));
    }
}
