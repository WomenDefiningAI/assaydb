//! Lossless external Arrow layout conversion, before scan predicates or consumers.
use super::{FilterError, TransformedBatch};
use crate::{
    Result,
    memory::{Budget, Reservation},
    types::Type,
};
use arrow::{
    array::*,
    datatypes::*,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use std::sync::Arc;

pub(super) fn needed(
    metadata: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
    projection: &parquet::arrow::ProjectionMask,
) -> bool {
    (0..metadata.parquet_schema().num_columns()).any(|leaf| {
        projection.leaf_included(leaf)
            && Type::scan_widening(
                metadata
                    .schema()
                    .field(metadata.parquet_schema().get_column_root_idx(leaf))
                    .data_type(),
            )
            .is_some()
    })
}

fn text(array: &dyn Array, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! dictionary {
        ($key:ty) => {{
            let dictionary = array
                .as_any()
                .downcast_ref::<DictionaryArray<$key>>()
                .unwrap();
            return dictionary
                .key(row)
                .and_then(|key| text(dictionary.values().as_ref(), key));
        }};
    }
    match array.data_type() {
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
        ),
        DataType::LargeUtf8 => Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Utf8View => Some(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary!(Int8Type),
            DataType::Int16 => dictionary!(Int16Type),
            DataType::Int32 => dictionary!(Int32Type),
            DataType::Int64 => dictionary!(Int64Type),
            DataType::UInt8 => dictionary!(UInt8Type),
            DataType::UInt16 => dictionary!(UInt16Type),
            DataType::UInt32 => dictionary!(UInt32Type),
            DataType::UInt64 => dictionary!(UInt64Type),
            _ => unreachable!("admitted dictionary key"),
        },
        _ => unreachable!("admitted text layout"),
    }
}

pub(super) fn batch(
    input: RecordBatch,
    decoded: Reservation,
    budget: &Budget,
) -> Result<TransformedBatch> {
    // Hold the decoded input and construction peak until all old buffers drop.
    // `decoded` is the input's existing credit (the decoder lease's prepaid
    // batch output, or the predicate's own input charge); only any shortfall
    // and the construction allowance are newly reserved. The resulting lease
    // then follows the batch through the ordinary reader bridge / parallel
    // queue, including errors, cancellation and early drop.
    let need = input
        .get_array_memory_size()
        .checked_add(
            input
                .num_columns()
                .checked_mul(512)
                .ok_or("scan widening size overflow")?,
        )
        .ok_or("scan widening size overflow")?;
    let mut lease = decoded;
    if need > lease.bytes() {
        lease.absorb(budget.reserve(need - lease.bytes())?);
    }
    let mut columns = Vec::with_capacity(input.num_columns());
    for (field, array) in input.schema().fields().iter().zip(input.columns()) {
        let Some(target) = Type::scan_widening(array.data_type()) else {
            columns.push(array.clone());
            continue;
        };
        let refusal = |reason: &str| {
            crate::Error::from(format!("unsupported: column '{}': {reason}", field.name()))
        };
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>()
            && values.iter().flatten().any(|value| value > i64::MAX as u64)
        {
            return Err(refusal("UInt64 value exceeds Int64 maximum"));
        }
        if let Some(values) = array.as_any().downcast_ref::<Date64Array>() {
            for value in values.iter().flatten() {
                if value % 86_400_000 != 0 {
                    return Err(refusal("Date64 value is not exact midnight"));
                }
                if i32::try_from(value / 86_400_000).is_err() {
                    return Err(refusal("Date64 day exceeds Date range"));
                }
            }
        }
        // Seconds/milliseconds widen to microseconds without overflow, and
        // nanoseconds only when every value is a whole microsecond (DuckDB
        // keeps TIMESTAMP_NS; PostgreSQL has no nanosecond type).
        macro_rules! timestamps {
            ($array:ty, $check:expr) => {
                if let Some(values) = array.as_any().downcast_ref::<$array>() {
                    for value in values.iter().flatten() {
                        $check(value)?;
                    }
                }
            };
        }
        timestamps!(TimestampSecondArray, |v: i64| v
            .checked_mul(1_000_000)
            .map(|_| ())
            .ok_or_else(|| refusal(
                "TIMESTAMP seconds exceed the microsecond range"
            )));
        timestamps!(TimestampMillisecondArray, |v: i64| v
            .checked_mul(1_000)
            .map(|_| ())
            .ok_or_else(|| refusal(
                "TIMESTAMP milliseconds exceed the microsecond range"
            )));
        timestamps!(TimestampNanosecondArray, |v: i64| if v % 1_000 == 0 {
            Ok(())
        } else {
            Err(refusal(
                "TIMESTAMP_NS value is not a whole microsecond (Assay TIMESTAMP has microsecond precision)",
            ))
        });
        let string_bytes = if target == DataType::Utf8 {
            let bytes = (0..array.len()).try_fold(0usize, |bytes, row| {
                bytes
                    .checked_add(text(array.as_ref(), row).map_or(0, str::len))
                    .ok_or("scan widening size overflow")
            })?;
            if bytes > i32::MAX as usize {
                return Err(refusal("text exceeds Utf8 offset range"));
            }
            bytes
        } else {
            0
        };
        // 16 bytes/row covers primitive values or string offsets + validity;
        // capacity rounding and array headers have an additional allowance.
        let bytes = array
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(string_bytes))
            .and_then(|n| n.checked_add(array.nulls().map_or(0, |n| n.buffer().capacity())))
            .and_then(|n| n.checked_add(1024))
            .ok_or("scan widening size overflow")?;
        lease.absorb(budget.reserve(bytes)?);
        let output: ArrayRef = if target == DataType::Utf8 {
            let mut builder = StringBuilder::with_capacity(array.len(), string_bytes);
            for row in 0..array.len() {
                builder.append_option(text(array.as_ref(), row));
            }
            Arc::new(builder.finish())
        } else {
            // Every conversion is exact after the guards above. In particular,
            // Date64 truncation and UInt64 wrapping are never delegated unchecked.
            arrow::compute::cast_with_options(
                array,
                &target,
                &arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|e| refusal(&e.to_string()))?
        };
        columns.push(output);
    }
    let output = RecordBatch::try_new_with_options(
        Arc::new(Type::input_schema(&input.schema())),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
    )
    .map_err(|e| e.to_string())?;
    drop(input);
    lease.release_excess(output.get_array_memory_size());
    Ok(TransformedBatch {
        batch: output,
        lease,
        preserve_empty: true,
    })
}

pub(super) fn filter(
    filter: RowFilter,
    budget: &Budget,
    error: &FilterError,
    metadata: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
) -> RowFilter {
    RowFilter::new(
        filter
            .into_predicates()
            .into_iter()
            .map(|mut predicate| {
                if !needed(metadata, predicate.projection()) {
                    return predicate;
                }
                let budget = budget.clone();
                let error = error.clone();
                Box::new(ArrowPredicateFn::new(
                    predicate.projection().clone(),
                    move |input| {
                        let bytes = input.get_array_memory_size();
                        let output = budget
                            .reserve(bytes)
                            .and_then(|decoded| batch(input, decoded, &budget))
                            .map_err(|message| {
                                *error.lock().unwrap() = Some(message.clone());
                                arrow::error::ArrowError::ComputeError(message.into())
                            })?;
                        // Keep the conversion lease live until the predicate releases input.
                        predicate.evaluate(output.batch)
                    },
                )) as Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-lease contract: the decoded input is charged to the budget
    /// before widening (now the caller's credit), then construction peaks.
    fn charged(input: RecordBatch, budget: &Budget) -> Result<TransformedBatch> {
        let decoded = budget.reserve(input.get_array_memory_size())?;
        batch(input, decoded, budget)
    }

    fn input(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap()
    }

    #[test]
    fn checked_dates_unsigned_and_budget_release() {
        for (array, message) in [
            (
                Arc::new(UInt64Array::from(vec![Some(0), None, Some(1 << 63)])) as ArrayRef,
                "UInt64 value exceeds Int64 maximum",
            ),
            (
                Arc::new(Date64Array::from(vec![Some(0), None, Some(1)])),
                "Date64 value is not exact midnight",
            ),
            (
                Arc::new(Date64Array::from(vec![Some(-1)])),
                "Date64 value is not exact midnight",
            ),
            (
                Arc::new(Date64Array::from(vec![
                    (i64::from(i32::MAX) + 1) * 86_400_000,
                ])),
                "Date64 day exceeds Date range",
            ),
        ] {
            let budget = Budget::new(1 << 20);
            assert_eq!(
                charged(input(array), &budget).err().unwrap(),
                format!("unsupported: column 'value': {message}")
            );
            assert_eq!(budget.used(), 0);
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-0.0)])),
            Arc::new(UInt64Array::from(vec![
                Some(i64::MAX as u64),
                None,
                Some(0),
            ])),
            Arc::new(Date64Array::from(vec![
                Some(-86_400_000),
                None,
                Some(86_400_000),
            ])),
            Arc::new(LargeStringArray::from(vec![Some("猫"), None, Some("")])),
            Arc::new(StringViewArray::from(vec![
                Some("a long string requiring out of line storage"),
                None,
                Some(""),
            ])),
            Arc::new(DictionaryArray::<Int16Type>::from_iter([
                Some("long repeated dictionary value"),
                None,
                Some("long repeated dictionary value"),
            ])),
        ];
        for array in arrays {
            let input = input(array);
            let budget = Budget::new(1 << 20);
            let output = charged(input.clone(), &budget).unwrap();
            let peak = budget.peak();
            assert_eq!(budget.used(), output.batch.get_array_memory_size());
            assert_eq!(output.batch.column(0).null_count(), 1);
            drop(output);
            assert_eq!(budget.used(), 0);
            let tiny = Budget::new(peak - 1);
            assert!(
                charged(input.clone(), &tiny)
                    .err()
                    .unwrap()
                    .starts_with("resource-limit:")
            );
            assert_eq!(tiny.used(), 0);
            let exact = Budget::new(peak);
            drop(charged(input, &exact).unwrap());
            assert_eq!(exact.used(), 0);
        }
    }

    #[test]
    fn float_bits_and_dictionary_null_values_survive_slices() {
        let bits = [
            0,
            0x8000_0000,
            1,
            0x7f7f_ffff,
            0x7f80_0000,
            0xff80_0000,
            0x7fc0_0123,
            0xffc0_0123,
        ];
        let floats = Float32Array::from_iter(
            std::iter::once(None).chain(bits.map(|b| Some(f32::from_bits(b)))),
        );
        let budget = Budget::new(1 << 20);
        let output = charged(input(Arc::new(floats.slice(1, bits.len()))), &budget).unwrap();
        let widened = output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for (index, bits) in bits.into_iter().enumerate() {
            assert_eq!(
                widened.value(index).to_bits(),
                f64::from(f32::from_bits(bits)).to_bits()
            );
        }
        drop(output);
        assert_eq!(budget.used(), 0);
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(0), Some(2)]),
            Arc::new(LargeStringArray::from(vec![Some("猫"), None, Some("")])),
        )
        .unwrap();
        let output = charged(input(Arc::new(dictionary.slice(1, 4))), &budget).unwrap();
        let decoded = output
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            vec![None, None, Some("猫"), Some("")]
        );
        drop(output);
        assert_eq!(budget.used(), 0);
    }
}
