//! NULLIF evaluates both operands once, but never promotes its returned value.
use super::*;
use arrow::buffer::NullBuffer;

fn equal(a: &Expr, av: &Value, b: &Expr, bv: &Value, row: usize) -> Result<bool> {
    if is_sql_null(av.array.as_ref(), row) || is_sql_null(bv.array.as_ref(), row) {
        return Ok(false);
    }
    if a.ty == Type::Float || b.ty == Type::Float {
        let real = |value: &Value| {
            value
                .array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
        };
        return Ok(match (&a.ty, &b.ty) {
            (Type::Int, Type::Float) => {
                numeric_compare::integer_real(numeric(&av.array).value(row), real(bv))
                    == Some(std::cmp::Ordering::Equal)
            }
            (Type::Float, Type::Int) => {
                numeric_compare::integer_real(numeric(&bv.array).value(row), real(av))
                    == Some(std::cmp::Ordering::Equal)
            }
            (Type::Float, Type::Float) => real(av) == real(bv),
            // DECIMAL is the engine's existing exact-numeric extension; retain
            // its ordinary Float comparison conversion without a common cast.
            (Type::Decimal(scale), Type::Float) => {
                crate::numeric_float::scaled(numeric(&av.array).value(row), *scale)? == real(bv)
            }
            (Type::Float, Type::Decimal(scale)) => {
                real(av) == crate::numeric_float::scaled(numeric(&bv.array).value(row), *scale)?
            }
            _ => return Err("invalid NULLIF comparison types".into()),
        });
    }
    if let (Ok(sa), Ok(sb)) = (a.ty.scale(), b.ty.scale()) {
        let scale = sa.max(sb);
        return Ok(rescale(numeric(&av.array).value(row), sa, scale)?
            == rescale(numeric(&bv.array).value(row), sb, scale)?);
    }
    Ok(match (&a.ty, &b.ty) {
        (Type::Text, Type::Text) => {
            av.array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                == bv
                    .array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
        }
        (Type::Binary, Type::Binary) => {
            av.array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row)
                == bv
                    .array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row)
        }
        (Type::Bool, Type::Bool) => boolean(&av.array).value(row) == boolean(&bv.array).value(row),
        (Type::Date, Type::Date) => {
            av.array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row)
                == bv
                    .array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .value(row)
        }
        _ => return Err("invalid NULLIF comparison types".into()),
    })
}

#[inline(never)]
pub(super) fn evaluate(
    a: &Expr,
    b: &Expr,
    result_type: &Type,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let av = a.eval(input, budget)?;
    let bv = b.eval(input, budget)?;
    // Charge both the validity construction and retained buffer ownership before
    // allocating. Cloned Arrow buffers are conservatively charged a second time
    // while the original input Value exists; no borrowed owner escapes.
    let bytes = av
        .array
        .get_array_memory_size()
        .checked_add(
            input
                .len
                .checked_mul(32)
                .and_then(|n| n.checked_add(256))
                .ok_or("NULLIF mask size overflow")?,
        )
        .ok_or("NULLIF output size overflow")?;
    let mut lease = budget.reserve(bytes)?;
    if a.ty == Type::Null {
        let array = new_null_array(&internal_type(result_type), input.len);
        lease.release_excess(array.get_array_memory_size());
        return Ok(Value::allocated(array, lease));
    }
    let mut valid = Vec::with_capacity(input.len);
    for row in 0..input.len {
        valid.push(!is_sql_null(av.array.as_ref(), row) && !equal(a, &av, b, &bv, row)?);
    }
    let array = make_array(
        av.array
            .to_data()
            .into_builder()
            .nulls(Some(NullBuffer::from(valid)))
            .build()
            .map_err(|e| e.to_string())?,
    );
    lease.release_excess(array.get_array_memory_size());
    Ok(Value::allocated(array, lease))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_binary_mask_is_funded_and_failure_releases_credit() {
        let budget = Budget::new(1024 * 1024);
        let av = Value::adopt(
            Arc::new(BinaryArray::from(vec![
                Some(&b"large\0payload"[..]),
                None,
                Some(&b"different"[..]),
            ])),
            &budget,
        )
        .unwrap();
        let bv = Value::adopt(
            Arc::new(BinaryArray::from(vec![
                Some(&b"large\0payload"[..]),
                Some(&b"x"[..]),
                Some(&b"x"[..]),
            ])),
            &budget,
        )
        .unwrap();
        let a = Expr {
            ty: Type::Binary,
            kind: Kind::Column(0),
        };
        let b = Expr {
            ty: Type::Binary,
            kind: Kind::Column(1),
        };
        let input = Input {
            columns: vec![Some(av), Some(bv)],
            len: 3,
        };
        let out = evaluate(&a, &b, &Type::Binary, &input, &budget).unwrap();
        drop(input);
        assert!(budget.used() >= out.array.get_array_memory_size());
        assert!(out.array.is_null(0));
        assert!(out.array.is_null(1));
        assert_eq!(
            out.array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(2),
            b"different"
        );
        drop(out);
        assert_eq!(budget.used(), 0);
        let low = Budget::new(1);
        let e = Expr {
            ty: Type::Int,
            kind: Kind::Literal(Literal::Num(1)),
        };
        assert!(
            evaluate(
                &e,
                &e,
                &Type::Int,
                &Input {
                    columns: vec![],
                    len: 1
                },
                &low
            )
            .is_err()
        );
        assert_eq!(low.used(), 0);
    }
}
