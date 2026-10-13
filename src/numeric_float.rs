//! One rounding at the exact-number -> binary64 boundary.
//! Never round a decimal mantissa before dividing by its scale. The ratio
//! kernel also finalizes wide AVG states without rounding sum/scale/count in
//! separate steps. Its supported bounds contain every precision-38/u64-count
//! aggregate state; larger internal inputs fail explicitly.
use crate::{Result, types::factor};
use arrow::datatypes::i256;

#[inline]
pub(crate) fn scaled(value: i128, scale: i8) -> Result<f64> {
    scaled_by(value, factor(scale)?)
}

#[inline]
pub(crate) fn scaled_by(value: i128, divisor: i128) -> Result<f64> {
    if divisor <= 0 {
        return Err("numeric conversion needs a positive scale factor".into());
    }
    if divisor == 1 {
        // A single integer cast already rounds once, ties to even.
        return Ok(value as f64);
    }
    if value.unsigned_abs() <= (1_u128 << 53) && divisor <= (1_i128 << 53) {
        // Both operands are exact binary64 integers; only division rounds.
        return Ok(value as f64 / divisor as f64);
    }
    ratio(i256::from_i128(value), i256::from_i128(divisor))
}

pub(crate) fn average(sum: i256, scale: i8, count: u64) -> Result<f64> {
    let denominator = i256::from_i128(factor(scale)?)
        .checked_mul(i256::from_i128(i128::from(count)))
        .ok_or("numeric conversion denominator overflow")?;
    ratio(sum, denominator)
}

fn shift(value: i256, bits: i32) -> Result<i256> {
    if !(0..255).contains(&bits) || 256 - value.leading_zeros() + bits as u32 > 255 {
        return Err(
            "unsupported: exact numeric-to-REAL conversion exceeds bounded workspace".into(),
        );
    }
    Ok(value << bits as u8)
}

pub(crate) fn ratio(numerator: i256, denominator: i256) -> Result<f64> {
    if denominator <= i256::ZERO {
        return Err("numeric conversion needs a positive denominator".into());
    }
    if denominator == i256::ONE {
        return Ok(crate::aggregate_sum::as_float(numerator));
    }
    if numerator == i256::ZERO {
        return Ok(0.0);
    }
    if let (Some(n), Some(d)) = (numerator.to_i128(), denominator.to_i128())
        && n.unsigned_abs() <= (1_u128 << 53)
        && d <= (1_i128 << 53)
    {
        return Ok(n as f64 / d as f64);
    }
    let negative = numerator < i256::ZERO;
    let magnitude = if negative {
        numerator
            .checked_neg()
            .ok_or("unsupported: numeric-to-REAL magnitude overflow")?
    } else {
        numerator
    };
    let mut exponent = denominator.leading_zeros() as i32 - magnitude.leading_zeros() as i32;
    // Establish 2^exponent <= magnitude/denominator < 2^(exponent+1).
    let below = if exponent >= 0 {
        magnitude < shift(denominator, exponent)?
    } else {
        shift(magnitude, -exponent)? < denominator
    };
    if below {
        exponent -= 1;
    }
    let places = 52 - exponent;
    let (n, d) = if places >= 0 {
        (shift(magnitude, places)?, denominator)
    } else {
        (magnitude, shift(denominator, -places)?)
    };
    let quotient = n / d;
    let remainder = n % d;
    let mut significand = u64::try_from(quotient.to_i128().ok_or("numeric significand overflow")?)
        .map_err(|_| "numeric significand overflow")?;
    // Compare r with d-r instead of doubling r, avoiding workspace overflow.
    let complement = d - remainder;
    if remainder > complement || (remainder == complement && significand & 1 != 0) {
        significand += 1;
    }
    // Every supported precision-38/u64-count ratio is normal (never subnormal),
    // and this exact power-of-two scaling cannot introduce a second rounding.
    let power = exponent - 52;
    if !(-1022..=1023).contains(&power) {
        return Err("unsupported: numeric-to-REAL exponent outside bounded domain".into());
    }
    let value = significand as f64 * f64::from_bits(((power + 1023) as u64) << 52);
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_fraction_oracle_covers_wide_ratios_and_halfway_neighbors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/exact-ratio-rounding.json"))
                .unwrap();
        let rows = fixture["cases"].as_array().unwrap();
        assert_eq!(rows.len(), 786);
        for row in rows {
            let n = row["numerator"].as_str().unwrap().parse::<i256>().unwrap();
            let d = row["denominator"]
                .as_str()
                .unwrap()
                .parse::<i256>()
                .unwrap();
            let expected = u64::from_str_radix(row["bits"].as_str().unwrap(), 16).unwrap();
            assert_eq!(ratio(n, d).unwrap().to_bits(), expected, "{n}/{d}");
        }
    }
    #[test]
    fn scaled_values_round_once_including_below_integer_boundaries() {
        for scale in 0..=38 {
            for value in [
                0,
                1,
                -1,
                i64::MAX as i128,
                i128::MAX,
                i128::MIN,
                19_999_999_999_999_998,
                -19_999_999_999_999_998,
            ] {
                let reference = crate::types::decimal_text(value, scale)
                    .parse::<f64>()
                    .unwrap();
                assert_eq!(
                    scaled(value, scale).unwrap().to_bits(),
                    reference.to_bits(),
                    "{value} scale {scale}"
                );
            }
        }
        assert_eq!(
            scaled(19_999_999_999_999_998, 16).unwrap().to_bits(),
            0x3fff_ffff_ffff_ffff
        );
    }
    #[test]
    fn rational_ties_use_even_and_wide_average_has_one_rounding() {
        for integer in [(1_i128 << 52), (1_i128 << 52) + 1] {
            for offset in [1, 2, 3] {
                let n = integer * 4 + offset;
                let expected = if offset < 2 || (offset == 2 && integer & 1 == 0) {
                    integer
                } else {
                    integer + 1
                };
                assert_eq!(
                    ratio(i256::from_i128(n), i256::from_i128(4)).unwrap(),
                    expected as f64
                );
            }
        }
        let n = i256::from_i128(19_999_999_999_999_998) * i256::from_i128(3);
        assert_eq!(average(n, 16, 3).unwrap().to_bits(), 0x3fff_ffff_ffff_ffff);
        assert!(average(n, 16, 0).is_err());
        // Legal widest sum/count bound stays within the fixed integer workspace.
        let max = i256::from_i128(10_i128.pow(38) - 1) * i256::from_i128(u64::MAX as i128);
        assert_eq!(average(max, 38, u64::MAX).unwrap(), 1.0);
    }
}
