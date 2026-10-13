//! Scalar integer arithmetic used by the real expression evaluator.
//! All arithmetic results stay exact within precision 38. BIGINT narrowing
//! belongs only to returned/stored values, never to division or remainder.
use crate::{Result, types::checked};

#[inline]
pub(super) fn divide(x: Option<i128>, y: Option<i128>) -> Result<Option<i128>> {
    Ok(match (x, y) {
        (Some(_), Some(0)) => None,
        (Some(x), Some(y)) => Some(checked(x.checked_div(y))?),
        _ => None,
    })
}

#[inline]
pub(super) fn remainder(x: Option<i128>, y: Option<i128>) -> Result<Option<i128>> {
    Ok(match (x, y) {
        (_, Some(0)) | (None, _) | (_, None) => None,
        (Some(_), Some(-1)) => Some(0), // MIN % -1 is zero, not overflow.
        (Some(x), Some(y)) => Some(checked(x.checked_rem(y))?),
    })
}

#[inline]
pub(super) fn multiply(x: i128, y: i128) -> Result<i128> {
    if let (Ok(x), Ok(y)) = (i64::try_from(x), i64::try_from(y)) {
        // Widen before multiplying. The largest possible magnitude is 2^126,
        // which fits both i128 and precision 38, including MIN * MIN.
        Ok(i128::from(x) * i128::from(y))
    } else {
        checked(x.checked_mul(y))
    }
}
#[inline]
pub(super) fn add_sub(x: i128, y: i128, af: i128, bf: i128, subtract: bool) -> Result<i128> {
    if let (Ok(x), Ok(y)) = (i64::try_from(x), i64::try_from(y))
        && (0..=1_000_000_000_000_000_000).contains(&af)
        && (0..=1_000_000_000_000_000_000).contains(&bf)
    {
        // Each scaled operand has magnitude <= 2^63 * 10^18. Their sum
        // or difference is <= 2^64 * 10^18 < 10^38, including signed MIN.
        // Widen first; neither intermediate nor result can overflow.
        let x = i128::from(x) * i128::from(af as i64);
        let y = i128::from(y) * i128::from(bf as i64);
        Ok(if subtract { x - y } else { x + y })
    } else {
        let x = checked(x.checked_mul(af))?;
        let y = checked(y.checked_mul(bf))?;
        checked(if subtract {
            x.checked_sub(y)
        } else {
            x.checked_add(y)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_zero_and_wide_result_boundaries_preserve_contract() {
        let minimum = i128::from(i64::MIN);
        assert_eq!(divide(Some(minimum), Some(-1)).unwrap(), Some(-minimum));
        assert_eq!(remainder(Some(minimum), Some(-1)).unwrap(), Some(0));
        for x in [None, Some(minimum), Some(17)] {
            assert_eq!(divide(x, Some(0)).unwrap(), None);
            assert_eq!(remainder(x, Some(0)).unwrap(), None);
            assert_eq!(divide(x, None).unwrap(), None);
            assert_eq!(remainder(x, None).unwrap(), None);
        }
        for (x, y, quotient, rest) in [(-7, 3, -2, -1), (7, -3, -2, 1), (-7, -3, 2, -1)] {
            assert_eq!(divide(Some(x), Some(y)).unwrap(), Some(quotient));
            assert_eq!(remainder(Some(x), Some(y)).unwrap(), Some(rest));
        }
        let wide = i128::from(i64::MAX) + 1;
        assert_eq!(divide(Some(wide), Some(2)).unwrap(), Some(wide / 2));
        assert_eq!(divide(Some(wide), Some(1)).unwrap(), Some(wide));
        assert_eq!(remainder(Some(wide), Some(wide + 1)).unwrap(), Some(wide));
        // The helper accepts raw i128, although these extreme operands are not
        // normal precision-38 inputs. Preserve checked behavior at API limits.
        assert_eq!(
            divide(Some(i128::MIN), Some(-1)).unwrap_err(),
            "numeric overflow"
        );
        assert_eq!(remainder(Some(i128::MIN), Some(-1)).unwrap(), Some(0));
    }

    #[test]
    fn precision_and_scaled_operand_errors_are_not_reordered() {
        let limit = 10_i128.pow(38);
        assert_eq!(
            multiply(i128::from(i64::MIN), i128::from(i64::MIN)).unwrap(),
            1_i128 << 126
        );
        assert_eq!(multiply(limit - 1, 1).unwrap(), limit - 1);
        assert_eq!(multiply(limit - 1, 2).unwrap_err(), "numeric overflow");
        assert_eq!(
            add_sub(limit - 1, 1, 1, 1, false).unwrap_err(),
            "decimal precision 38 overflow"
        );
        // The left precision failure must precede the right machine overflow,
        // even if a later operation might cancel an out-of-range intermediate.
        assert_eq!(
            add_sub(limit, i128::MAX, 1, 2, true).unwrap_err(),
            "decimal precision 38 overflow"
        );
        assert_eq!(
            add_sub(i128::MAX, limit, 2, 1, true).unwrap_err(),
            "numeric overflow"
        );
        assert_eq!(add_sub(limit - 1, limit - 1, 1, 1, true).unwrap(), 0);
    }
}
