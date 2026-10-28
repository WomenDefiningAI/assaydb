//! Literal-precision ROUND; Float follows the DuckDB 1.5.6 scale/round/unscale
//! contract, including its nonfinite fallback and negative precision behavior.
use super::*;

pub(crate) fn precision(expr: &Expr) -> Result<i32> {
    fn integer(expr: &Expr) -> Option<i128> {
        match &expr.kind {
            Kind::Literal(Literal::Num(n)) if expr.ty == Type::Int => Some(*n),
            Kind::Neg(e) => integer(e)?.checked_neg(),
            _ => None,
        }
    }
    integer(expr)
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| "unsupported: ROUND precision must fit INTEGER".into())
}

pub(crate) fn bind(mut input: Expr, digits: Option<i32>) -> Result<Expr> {
    if input.ty == Type::Null {
        input.ty = Type::Int;
    }
    let ty = match input.ty {
        Type::Int | Type::Float => input.ty.clone(),
        Type::Decimal(scale) => {
            Type::Decimal(digits.map_or(scale, |n| n.clamp(0, i32::from(scale)) as i8))
        }
        _ => return Err("unsupported: ROUND requires Int, Decimal or Float".into()),
    };
    Ok(Expr {
        ty,
        kind: Kind::Function(
            format!(
                "$round_{}",
                digits.map_or_else(|| "null".into(), |n| n.to_string())
            ),
            Box::new(input),
        ),
    })
}

pub(super) fn evaluate(
    name: &str,
    array: &ArrayRef,
    input: &Type,
    output: &Type,
) -> Result<ArrayRef> {
    let digits = name.strip_prefix("$round_").expect("bound ROUND");
    if digits == "null" {
        return Ok(new_null_array(&internal_type(output), array.len()));
    }
    let digits = digits.parse::<i32>().expect("bound ROUND precision");
    if *input == Type::Float {
        return Ok(Arc::new(Float64Array::from_iter(
            to_float(array, input)?
                .iter()
                .map(|v| v.map(|v| float(v, digits))),
        )));
    }
    let scale = i32::from(input.scale()?);
    let target = i32::from(output.scale()?);
    decimal(
        numeric(array)
            .iter()
            .map(|v| {
                v.map(|v| {
                    if digits >= scale {
                        return Ok(v);
                    }
                    let places = i64::from(scale) - i64::from(digits);
                    if places > 38 {
                        return Ok(0);
                    }
                    let divisor = factor(places as i8)?;
                    let quotient = v / divisor;
                    let remainder = v % divisor;
                    let rounded = quotient
                        + if remainder.abs() >= divisor / 2 {
                            v.signum()
                        } else {
                            0
                        };
                    let multiplier = factor((target - digits) as i8)?;
                    let result = rounded.checked_mul(multiplier).ok_or("numeric overflow")?;
                    if digits < 0 && result != 0 {
                        // DuckDB's INTEGER/BIGINT kernels wrap on a rounding
                        // carry past their storage width. Assay's Int erases
                        // the original narrow width and must not guess it.
                        if *input == Type::Int
                            && ((i32::try_from(v).is_ok() && i32::try_from(result).is_err())
                                || i64::try_from(result).is_err())
                        {
                            return Err("unsupported: negative ROUND at integral-width boundary (reference precision policy differs)".into());
                        }
                        // DuckDB returns zero when negative precision reaches
                        // the declared integral width. Our Decimal type keeps
                        // scale but widens that width to 38, so refuse a carry
                        // into a new integral digit rather than guess the lost
                        // declaration. Below this boundary both rules agree.
                        let integral = v.unsigned_abs() / factor(scale as i8)? as u128;
                        let mut next_digit = 10_u128;
                        while integral >= next_digit {
                            next_digit *= 10;
                        }
                        if result.unsigned_abs() >= next_digit {
                            return Err("unsupported: negative ROUND at integral-width boundary (reference precision policy differs)".into());
                        }
                    }
                    checked(Some(result))
                })
                .transpose()
            })
            .collect::<Result<Vec<_>>>()?,
        output.scale()?,
    )
}

/// Correctly rounded 10^n for 0 <= n <= 308. `f64::powf` defers to the platform libm, whose
/// last-bit result differs between macOS and glibc (e.g. 10^23); the decimal parser is exact
/// everywhere and equals the DuckDB 1.5.6 reference bits, so ROUND is identical on all hosts.
pub(super) fn pow10(n: i32) -> f64 {
    static TABLE: std::sync::OnceLock<Vec<f64>> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        (0..=308)
            .map(|n| format!("1e{n}").parse().expect("finite power of ten"))
            .collect()
    })[n as usize]
}

pub(crate) fn float(value: f64, digits: i32) -> f64 {
    if digits < 0 {
        // Avoid negating i32::MIN, and match the reference's +0 fallback.
        if digits < -308 {
            return 0.0;
        }
        let power = pow10(-digits);
        let result = (value / power).round() * power;
        if result.is_finite() { result } else { 0.0 }
    } else {
        if digits > 308 {
            return value;
        }
        let power = pow10(digits);
        let result = (value * power).round() / power;
        if result.is_finite() { result } else { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duckdb_float_bits_include_nonfinite_extreme_precision_and_subnormals() {
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/offset_windows/expected.json"
        ))
        .unwrap();
        for case in expected["float_controls"].as_array().unwrap() {
            let input = u64::from_str_radix(case["input"].as_str().unwrap(), 16).unwrap();
            let output = u64::from_str_radix(case["output"].as_str().unwrap(), 16).unwrap();
            let digits = case["digits"].as_i64().unwrap() as i32;
            assert_eq!(
                float(f64::from_bits(input), digits).to_bits(),
                output,
                "{case}"
            );
        }
    }
}
