//! Remainder (`%` and MOD) with DuckDB 1.5.6 / PostgreSQL semantics (Helen,
//! October 17, replacing SQLite's integer remainder for REAL): the result has
//! the dividend's sign. REAL is C `fmod` on the binary64 operands (exact; an
//! INTEGER or DECIMAL operand converts to binary64 first, as DuckDB casts it).
//! INTEGER and DECIMAL are exact at the larger scale. Zero divisors keep the
//! approved Assay rules where the references disagree (DuckDB NULL or NaN,
//! PostgreSQL error): NULL for INTEGER/REAL operands, `division by zero` when
//! a DECIMAL operand is involved, as for division; a NaN dividend gives NaN
//! (both references agree, decision 2026-10-17). Every input expression is
//! evaluated before this kernel; NULL/zero does not suppress operand errors.
use super::*;

#[inline(never)]
pub(super) fn evaluate(
    a: &ArrayRef,
    at: &Type,
    b: &ArrayRef,
    bt: &Type,
    ty: &Type,
) -> Result<ArrayRef> {
    let decimal_operand = matches!(at, Type::Decimal(_)) || matches!(bt, Type::Decimal(_));
    if *ty == Type::Float {
        let x = to_float(a, at)?;
        let y = to_float(b, bt)?;
        let values = x
            .iter()
            .zip(y.iter())
            .map(|pair| match pair {
                // Signed zero is zero; NaN is not.
                (Some(x), Some(y)) => {
                    if y != 0.0 {
                        Ok(Some(x % y))
                    } else if x.is_nan() {
                        // NaN % 0 is NaN in DuckDB 1.5.6 and in PostgreSQL 17
                        // (numeric), for REAL and DECIMAL zeros: the NaN / 0
                        // decision of 2026-10-17 applies to the remainder.
                        Ok(Some(x))
                    } else if decimal_operand {
                        Err("division by zero".into())
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(Float64Array::from(values)));
    }
    let scale = ty.scale()?;
    let (sa, sb) = (at.scale()?, bt.scale()?);
    let (x, y) = (numeric(a), numeric(b));
    let mut values = Vec::with_capacity(a.len());
    for row in 0..a.len() {
        if x.is_null(row) || y.is_null(row) {
            values.push(None);
            continue;
        }
        if *ty == Type::Int {
            values.push(integer::remainder(Some(x.value(row)), Some(y.value(row)))?);
            continue;
        }
        let dividend = rescale(x.value(row), sa, scale)?;
        let divisor = rescale(y.value(row), sb, scale)?;
        if divisor == 0 {
            return Err("division by zero".into());
        }
        // |dividend| < 10^38 and divisor != 0, so i128 remainder cannot fail.
        values.push(Some(dividend % divisor));
    }
    decimal(values, scale)
}
