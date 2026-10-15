//! Residual evaluation over candidate indices, before output payload gathering.
use crate::{
    Result,
    expr::{Expr, Input, Value, normalize_input},
    memory::{Budget, Reservation},
    types::Type,
};
use arrow::{array::*, compute::take, datatypes::Schema, record_batch::RecordBatch};

pub(crate) struct JoinFilter<'a> {
    expr: &'a Expr,
    columns: Vec<(usize, Type)>,
    width: usize,
}
impl<'a> JoinFilter<'a> {
    pub fn new(expr: &'a Expr, schema: &Schema) -> Result<Self> {
        let mut needed = std::collections::BTreeSet::new();
        expr.columns(&mut needed);
        let columns = needed
            .into_iter()
            .map(|i| Ok((i, Type::from_field(schema.field(i))?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            expr,
            columns,
            width: schema.fields().len(),
        })
    }
    pub fn eval(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
        li: &UInt32Array,
        ri: &UInt32Array,
        budget: &Budget,
    ) -> Result<(Value, usize)> {
        let mut columns = vec![None; self.width];
        let mut bytes = 0;
        for (i, ty) in &self.columns {
            let (array, indices) = if *i < left.num_columns() {
                (left.column(*i), li)
            } else {
                (right.column(*i - left.num_columns()), ri)
            };
            let (gathered, _lease) = gather(array, indices, budget)?;
            bytes += gathered.get_array_memory_size();
            columns[*i] = Some(normalize_input(&gathered, ty, budget)?);
        }
        let input = Input {
            columns,
            len: li.len(),
        };
        Ok((self.expr.eval(&input, budget)?, bytes))
    }
}

/// Supported flat types need at most sixteen data bytes + validity per row,
/// except strings/binary, whose selected byte lengths include repeated indices.
/// Reserve before Arrow allocates and retain the lease with the gathered array.
pub(crate) fn gather(
    array: &ArrayRef,
    indices: &UInt32Array,
    budget: &Budget,
) -> Result<(ArrayRef, Reservation)> {
    #[cfg(test)]
    tests::gather_checkpoint(array, indices, budget, false);
    let lease = budget.reserve(gather_bytes(array, indices)?)?;
    let gathered = take(array, indices, None).map_err(|e| e.to_string())?;
    #[cfg(test)]
    tests::gather_checkpoint(array, indices, budget, true);
    Ok((gathered, lease))
}

/// Bound the same flat Arrow take buffers, including every repeated string index.
pub(crate) fn gather_bytes(array: &ArrayRef, indices: &UInt32Array) -> Result<usize> {
    let mut bytes = indices
        .len()
        .checked_mul(17)
        .and_then(|n| n.checked_add(256))
        .ok_or("join gather size overflow")?;
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        for i in indices.iter().flatten() {
            let row = i as usize;
            if strings.is_valid(row) {
                bytes = bytes
                    .checked_add(strings.value(row).len())
                    .ok_or("join gather size overflow")?;
            }
        }
    }
    if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        for i in indices.iter().flatten() {
            let row = i as usize;
            if binary.is_valid(row) {
                bytes = bytes
                    .checked_add(binary.value(row).len())
                    .ok_or("join gather size overflow")?;
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Arc;
    type GatherHook = Box<dyn FnMut(&ArrayRef, &UInt32Array, &Budget, bool)>;
    thread_local! {
        pub(crate) static GATHER_HOOK: std::cell::RefCell<Option<GatherHook>> =
            const { std::cell::RefCell::new(None) };
    }
    pub(super) fn gather_checkpoint(
        array: &ArrayRef,
        indices: &UInt32Array,
        budget: &Budget,
        allocated: bool,
    ) {
        GATHER_HOOK.with_borrow_mut(|hook| {
            if let Some(hook) = hook {
                hook(array, indices, budget, allocated);
            }
        });
    }
    #[test]
    fn repeated_string_indices_are_reserved_and_nullable_gathers_match_arrow() {
        let long = "é雪".repeat(16384);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some(long.as_str()),
                None,
                Some("z"),
            ])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(NullArray::new(3)),
            Arc::new(BinaryArray::from(vec![
                Some(long.as_bytes()),
                None,
                Some(&b"x"[..]),
            ])),
        ];
        let indices = UInt32Array::from(vec![Some(0), Some(0), None, Some(1), Some(2)]);
        let budget = Budget::new(1 << 20);
        for array in &arrays {
            let (actual, lease) = gather(array, &indices, &budget).unwrap();
            let expected = take(array, &indices, None).unwrap();
            assert_eq!(actual.to_data(), expected.to_data());
            assert!(budget.used() >= actual.get_buffer_memory_size());
            drop(actual);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
        let small = Budget::new(long.len());
        assert!(
            gather(&arrays[0], &indices, &small)
                .unwrap_err()
                .starts_with("resource-limit:")
        );
        assert_eq!(small.used(), 0);
    }
}
