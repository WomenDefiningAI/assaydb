//! Recursive-owned physical normalization; no changes to public cast policy.
//! Funded physical normalization; logical anchor/step coercion is guarded separately.
use crate::{Result, memory::Budget, spill::Cancellation, types::Type};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};

fn workspace(batch: &RecordBatch, target: &SchemaRef) -> Result<usize> {
    if batch.num_columns() != target.fields().len() {
        return Err("recursive physical normalization width mismatch".into());
    }
    let mut bytes = target
        .fields()
        .len()
        .checked_mul(256)
        .and_then(|n| n.checked_add(512))
        .ok_or("recursive normalization metadata overflow")?;
    for (source, dest) in batch.schema().fields().iter().zip(target.fields()) {
        if Type::from_field(source)? != Type::from_field(dest)? {
            return Err("unsupported: recursive logical type coercion".into());
        }
        if source.data_type() == dest.data_type() {
            continue;
        }
        // Scalar types currently supported by Assay. Variable-size payloads are
        // already physically identical under the exact logical-type guard.
        let width = match dest.data_type() {
            DataType::Int8 | DataType::UInt8 | DataType::Boolean => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date64 => 8,
            DataType::Decimal128(_, _) => 16,
            DataType::Null => 0,
            _ => return Err("unsupported: recursive physical normalization target".into()),
        };
        // Fund destination values, validity and temporary conversion overlap;
        // 2x source bytes below also covers Arrow intermediate scalar buffers.
        bytes = bytes
            .checked_add(
                batch
                    .num_rows()
                    .checked_mul(width)
                    .and_then(|n| n.checked_add(batch.num_rows().div_ceil(8)))
                    .and_then(|n| n.checked_add(256))
                    .ok_or("recursive normalization payload overflow")?,
            )
            .ok_or("recursive normalization accounting overflow")?;
    }
    bytes
        .checked_add(
            batch
                .get_array_memory_size()
                .checked_mul(2)
                .ok_or("recursive normalization source accounting overflow")?,
        )
        .ok_or_else(|| "recursive normalization workspace overflow".into())
}

pub(super) fn emit(
    batch: RecordBatch,
    target: &SchemaRef,
    budget: &Budget,
    cancel: &Cancellation,
    send: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    cancel.check()?;
    // This lease MUST span send: Admission and Builder receive newly allocated
    // physical buffers and obtain their own retention leases inside the callback.
    let _allocation = budget.reserve(workspace(&batch, target)?)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (array, field) in batch.columns().iter().zip(target.fields()) {
        cancel.check()?;
        arrays.push(if array.data_type() == field.data_type() {
            array.clone()
        } else {
            arrow::compute::cast_with_options(
                array,
                field.data_type(),
                &arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|e| crate::Error::from(e.to_string()))?
        });
    }
    let normalized = RecordBatch::try_new(target.clone(), arrays)
        .map_err(|e| crate::Error::from(e.to_string()))?;
    send(normalized)?;
    cancel.check()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Decimal128Array, Int64Array},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;
    fn input() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Type::Int.field("n", true)])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(11), None, Some(33)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )],
        )
        .unwrap()
    }
    fn target() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
    }
    #[test]
    fn converted_values_remain_funded_during_callback() {
        let input = input();
        let target = target();
        let budget = Budget::new(1 << 20);
        let needed = workspace(&input, &target).unwrap();
        emit(
            input,
            &target,
            &budget,
            &Cancellation::default(),
            &mut |normalized| {
                assert!(budget.used() >= needed);
                let array = normalized
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(
                    array.iter().collect::<Vec<_>>(),
                    vec![Some(11), None, Some(33)]
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn insufficient_workspace_refuses_before_conversion_or_callback() {
        let input = input();
        let target = target();
        let budget = Budget::new(workspace(&input, &target).unwrap() - 1);
        let mut called = false;
        let error = emit(
            input,
            &target,
            &budget,
            &Cancellation::default(),
            &mut |_| {
                called = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("resource-limit"));
        assert!(!called);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn overflow_and_callback_failure_release_conversion_credit() {
        let target = target();
        let budget = Budget::new(1 << 20);
        let bad = RecordBatch::try_new(
            input().schema(),
            vec![Arc::new(
                Decimal128Array::from(vec![i128::from(i64::MAX) + 1])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )],
        )
        .unwrap();
        assert!(
            emit(bad, &target, &budget, &Cancellation::default(), &mut |_| {
                panic!("overflow must not emit")
            })
            .is_err()
        );
        assert_eq!(budget.used(), 0);
        assert!(
            emit(
                input(),
                &target,
                &budget,
                &Cancellation::default(),
                &mut |_| { Err("injected normalization consumer failure".into()) }
            )
            .is_err()
        );
        assert_eq!(budget.used(), 0);
    }
}
