//! SQLite remainder for the admitted INTEGER/REAL domain, not exact DECIMAL.
//! Pinned SQLite 3.54: OP_Remainder and sqlite3RealToI64. Every input expression
//! is evaluated before this kernel; NULL/zero does not suppress operand errors.
use super::*;

fn operand(array: &ArrayRef, ty: &Type, row: usize) -> Result<Option<i128>> {
    if is_sql_null(array.as_ref(), row) {
        return Ok(None);
    }
    match ty {
        Type::Int => Ok(Some(numeric(array).value(row))),
        Type::Float => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            if !value.is_finite() {
                return Err("floating arithmetic overflow".into());
            }
            // Rust's finite float-to-int cast truncates and saturates. This
            // matches sqlite3RealToI64, including the representable endpoints.
            Ok(Some(i128::from(value as i64)))
        }
        _ => Err("internal: remainder input was not bound INTEGER/REAL".into()),
    }
}

#[inline(never)]
pub(super) fn evaluate(
    a: &ArrayRef,
    at: &Type,
    b: &ArrayRef,
    bt: &Type,
    ty: &Type,
) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(a.len());
    for row in 0..a.len() {
        // Both operands precede NULL/zero handling; there is no implicit
        // conversion of text, binary, or exact DECIMAL into this new operator.
        let x = operand(a, at, row)?;
        let y = operand(b, bt, row)?;
        values.push(integer::remainder(x, y)?);
    }
    if *ty == Type::Float {
        Ok(Arc::new(Float64Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as f64)),
        )))
    } else {
        decimal(values, 0)
    }
}
