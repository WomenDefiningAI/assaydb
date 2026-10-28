//! Typed global UNION admission.
//! Current scalar Type domain only. Nested SQL types require canonical keys first.
use crate::{
    Result,
    execute::{Key, key},
    expr::{is_sql_null, normalize_input},
    memory::{Budget, Reservation},
    spill::Cancellation,
    types::Type,
};
use arrow::{
    array::{Array, BinaryArray, BooleanArray, StringArray},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::collections::HashMap;

// Arrow 57.3.1 filter_record_batch automatically optimizes multi-column
// batches (and some composite single columns). Its index/run vectors require
// the existing execute::filter_input allowance: rows*32+128, not rows*16.
// Vec<bool> is byte-per-element in Rust; BooleanArray conversion creates a
// separate packed buffer while that Vec is still live. Our mask has no NULLs,
// so Arrow does not need the additional nullable-mask conjunction bitmap.
fn filter_workspace_bytes(batch: &RecordBatch) -> Result<usize> {
    let rows = batch.num_rows();
    let arrow_strategy = rows.checked_mul(32).and_then(|n| n.checked_add(128));
    let selected_vec = rows.checked_mul(std::mem::size_of::<bool>());
    let packed_mask = rows.div_ceil(8).checked_add(128);
    let output = batch.get_array_memory_size().checked_mul(2);
    // Covers normalized Value/ArrayRef descriptors, output column vector,
    // RecordBatch metadata and small-vector capacity/alignment overhead.
    let descriptors = batch
        .num_columns()
        .checked_mul(256)
        .and_then(|n| n.checked_add(1024));
    [
        arrow_strategy,
        selected_vec,
        packed_mask,
        output,
        descriptors,
    ]
    .into_iter()
    .try_fold(0usize, |sum, part| sum.checked_add(part?))
    .ok_or_else(|| "recursive filter accounting overflow".into())
}

pub(super) struct Admission {
    schema: SchemaRef,
    types: Vec<Type>,
    seen: Option<HashMap<Vec<Key>, Reservation>>,
    budget: Budget,
    cancel: Cancellation,
    _schema_credit: Reservation,
}
impl Admission {
    pub(super) fn new(
        schema: SchemaRef,
        distinct: bool,
        budget: Budget,
        cancel: Cancellation,
    ) -> Result<Self> {
        cancel.check()?;
        let credit = budget.reserve(
            schema
                .fields()
                .len()
                .checked_mul(256)
                .and_then(|n| n.checked_add(512))
                .ok_or("recursive schema accounting overflow")?,
        )?;
        let types = schema
            .fields()
            .iter()
            .map(|f| Type::from_field(f))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            types,
            seen: distinct.then(HashMap::new),
            budget,
            cancel,
            _schema_credit: credit,
        })
    }
    pub(super) fn admit(
        &mut self,
        batch: RecordBatch,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        self.cancel.check()?;
        if batch.schema() != self.schema {
            return Err("recursive admission schema mismatch".into());
        }
        let Some(seen) = &mut self.seen else {
            return emit(batch);
        };
        // Normalize_input funds each Value's buffers independently. This
        // reservation covers descriptors, simultaneous selection/mask storage,
        // Arrow's optimized predicate and filtered output allocation overlap.
        let bytes = filter_workspace_bytes(&batch)?;
        let _scratch = self.budget.reserve(bytes)?;
        let values = batch
            .columns()
            .iter()
            .zip(&self.types)
            .map(|(a, t)| normalize_input(a, t, &self.budget))
            .collect::<Result<Vec<_>>>()?;
        let mut selected = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            self.cancel.check()?;
            let mut bytes = self
                .types
                .len()
                .checked_mul(std::mem::size_of::<Key>())
                .and_then(|n| n.checked_add(256))
                .ok_or("recursive key accounting overflow")?;
            for value in &values {
                if is_sql_null(value.array.as_ref(), row) {
                    continue;
                }
                let payload = if let Some(a) = value.array.as_any().downcast_ref::<StringArray>() {
                    a.value(row).len()
                } else if let Some(a) = value.array.as_any().downcast_ref::<BinaryArray>() {
                    a.value(row).len()
                } else {
                    0
                };
                bytes = bytes
                    .checked_add(payload)
                    .ok_or("recursive key payload overflow")?;
            }
            // Conservative per-entry allowance covers hash bucket/control bytes
            // and capacity growth overlap. Credit precedes key-owned allocations.
            let credit = self.budget.reserve(
                bytes
                    .checked_mul(4)
                    .ok_or("recursive key capacity overflow")?,
            )?;
            let keys = values
                .iter()
                .zip(&self.types)
                .map(|(v, t)| key(v, t, row))
                .collect::<Vec<_>>();
            if seen.contains_key(&keys) {
                selected.push(false);
                continue;
            }
            seen.try_reserve(1)
                .map_err(|_| "resource-limit: recursive seen-set allocation")?;
            seen.insert(keys, credit);
            selected.push(true);
        }
        if selected.iter().any(|v| *v) {
            let mask = BooleanArray::from(selected);
            let accepted = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            emit(accepted)?;
        }
        self.cancel.check()
    }
}
// Seen state is query-owned and discarded on any error; admission cannot be
// retried in-place after callback failure. Whole-query retry starts fresh.
// This index is budgeted but NOT spillable. Spill-enabled frontiers do not make
// UNION dedup disk-backed: exceeding the seen budget is an explicit resource
// error. External dedup and keyed replacement/aggregate payloads remain required.

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Float64Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;
    fn batch(values: Vec<Option<f64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)])),
            vec![Arc::new(Float64Array::from(values))],
        )
        .unwrap()
    }
    #[test]
    fn global_keys_deduplicate_null_nan_and_signed_zero_across_iterations() {
        let budget = Budget::new(1 << 20);
        let first = batch(vec![None, Some(f64::NAN), Some(-0.0), Some(7.0)]);
        let second = batch(vec![
            None,
            Some(f64::from_bits(0x7ff8000000000001)),
            Some(0.0),
            Some(9.0),
        ]);
        let mut admission = Admission::new(
            first.schema(),
            true,
            budget.clone(),
            Cancellation::default(),
        )
        .unwrap();
        let mut rows = 0;
        admission
            .admit(first, &mut |b| {
                rows += b.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 4);
        admission
            .admit(second, &mut |b| {
                rows += b.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 5);
        drop(admission);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn union_all_preserves_duplicates() {
        let budget = Budget::new(1 << 20);
        let first = batch(vec![None, None, Some(1.0), Some(1.0)]);
        let mut admission = Admission::new(
            first.schema(),
            false,
            budget.clone(),
            Cancellation::default(),
        )
        .unwrap();
        let mut rows = 0;
        admission
            .admit(first.clone(), &mut |b| {
                rows += b.num_rows();
                Ok(())
            })
            .unwrap();
        admission
            .admit(first, &mut |b| {
                rows += b.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 8);
        drop(admission);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn multicolumn_filter_workspace_is_reserved_before_keys_or_callback() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Float64, false),
                Field::new("b", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(
                    (0..4096).map(|i| (i % 17) as f64).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..4096).map(|i| (i % 23) as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let needed = filter_workspace_bytes(&input).unwrap();
        let schema_bytes = input.num_columns() * 256 + 512;
        let budget = Budget::new(schema_bytes + needed - 1);
        let mut admission = Admission::new(
            input.schema(),
            true,
            budget.clone(),
            Cancellation::default(),
        )
        .unwrap();
        let mut called = false;
        let error = admission
            .admit(input.clone(), &mut |_| {
                called = true;
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("resource-limit"));
        assert!(!called);
        assert!(admission.seen.as_ref().unwrap().is_empty());
        assert_eq!(budget.used(), schema_bytes);
        drop(admission);
        assert_eq!(budget.used(), 0);

        // Exercise the same multi-column mask with funded filtering, including
        // persistent seen keys plus independently funded normalized arrays.
        let budget = Budget::new(16 << 20);
        let mut admission = Admission::new(
            input.schema(),
            true,
            budget.clone(),
            Cancellation::default(),
        )
        .unwrap();
        let mut rows = 0;
        admission
            .admit(input, &mut |accepted| {
                assert!(budget.used() >= schema_bytes + needed);
                rows += accepted.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 17 * 23);
        drop(admission);
        assert_eq!(budget.used(), 0);
    }
}
