//! A real join-build producer whose output carries its prepaid payload owner.
use crate::{
    Result,
    memory::{Budget, Reservation},
    spill::Cancellation,
};
use arrow::{
    array::ArrayRef,
    compute::concat_batches,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};

/// Construction is private: callers cannot pair arbitrary arrays with a lease.
/// Payload buffers drop before their final backing reservation on every exit.
pub(super) struct FundedBatch {
    batch: RecordBatch,
    credit: Reservation,
}
/// The borrowed production join also carries arbitrary unevaluated physical
/// payloads. Keep their legacy reservation path distinct from the proven
/// movable receipt; it cannot be passed to OwnedBuild::from_funded.
pub(super) enum JoinBuild {
    Funded(FundedBatch),
    Legacy {
        batch: RecordBatch,
        _credit: Reservation,
    },
}
impl JoinBuild {
    pub(super) fn batch(&self) -> &RecordBatch {
        match self {
            Self::Funded(batch) => batch.batch(),
            Self::Legacy { batch, .. } => batch,
        }
    }
}
fn admitted(schema: &SchemaRef) -> bool {
    schema.fields().iter().all(|field| {
        matches!(
            field.data_type(),
            DataType::Null
                | DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Date32
                | DataType::Decimal128(_, _)
                | DataType::Utf8
        )
    })
}
/// Select before funded prepayment. Unsupported physical payloads may still be
/// legal unused build columns (notably SEMI/ANTI joins without pushdown).
pub(super) fn for_join(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    budget: &Budget,
    cancel: &Cancellation,
) -> Result<JoinBuild> {
    if admitted(schema) {
        return concat(schema, batches, budget, cancel).map(JoinBuild::Funded);
    }
    cancel.check()?;
    let bytes = batches
        .iter()
        .try_fold(0usize, |n, batch| add(n, batch.get_array_memory_size()))?;
    let credit = budget.reserve(bytes)?;
    let batch = concat_batches(schema, batches).map_err(|e| e.to_string())?;
    Ok(JoinBuild::Legacy {
        batch,
        _credit: credit,
    })
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| "join concat size overflow".into())
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| "join concat size overflow".into())
}
fn envelope(columns: usize) -> Result<usize> {
    add(
        add(std::mem::size_of::<FundedBatch>(), 512)?,
        mul(columns, std::mem::size_of::<ArrayRef>())?,
    )
}
impl FundedBatch {
    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }
    pub(super) fn validate(&self, budget: &Budget) -> Result<()> {
        if !self.credit.belongs_to(budget) {
            return Err("join build payload funding domain mismatch".into());
        }
        if self.credit.bytes()
            < add(
                self.batch.get_array_memory_size(),
                envelope(self.batch.num_columns())?,
            )?
        {
            return Err("join build payload exceeds prepaid reservation".into());
        }
        Ok(())
    }
    fn from_prepaid(batch: RecordBatch, credit: Reservation, budget: &Budget) -> Result<Self> {
        // Assemble the owner before any fallible check, preserving buffer/credit
        // destruction order even for malformed injected receipts in tests.
        let mut owned = Self { batch, credit };
        owned.validate(budget)?;
        let actual = add(
            owned.batch.get_array_memory_size(),
            envelope(owned.batch.num_columns())?,
        )?;
        owned.credit.release_excess(actual);
        Ok(owned)
    }
}

/// Inputs remain funded by the caller through this call. Existing Node.run
/// collection is unchanged; only the independently allocated concat output's
/// credit becomes movable. This is also the production borrowed join's owner.
pub(super) fn concat(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    budget: &Budget,
    cancel: &Cancellation,
) -> Result<FundedBatch> {
    cancel.check()?;
    // These are the proven flat payload types, not every physical column that
    // the borrowed production join may carry without evaluating. Other shapes
    // use for_join's legacy path rather than receiving an unproven receipt.
    // Arrow concat either uses exact logical lengths or shares full backing of
    // its sole input; source footprints plus per-column slack cover both paths.
    if !admitted(schema) {
        return Err("unsupported: funded join concat physical type".into());
    }
    let mut rows = 0usize;
    let mut data = 0usize;
    for batch in batches {
        cancel.check()?;
        if batch.num_columns() != schema.fields().len()
            || batch
                .columns()
                .iter()
                .zip(schema.fields())
                .any(|(array, field)| array.data_type() != field.data_type())
        {
            return Err("join concat input schema mismatch".into());
        }
        rows = add(rows, batch.num_rows())?;
        data = add(data, batch.get_array_memory_size())?;
    }
    let retained = add(
        add(data, envelope(schema.fields().len())?)?,
        mul(schema.fields().len(), 512)?,
    )?;
    // Arrow concat_batches materializes input and per-column reference vectors;
    // primitive/UTF8 concat also makes a typed input-reference vector. Cover
    // these simultaneous descriptor arrays before invoking Arrow.
    // Null concat uses Arrow's ArrayData fallback, with larger per-input
    // descriptors and extension closures than primitive/UTF8 concat.
    let per_input = if schema
        .fields()
        .iter()
        .any(|f| f.data_type() == &DataType::Null)
    {
        1024
    } else {
        64
    };
    let scratch = add(
        add(
            mul(batches.len(), per_input)?,
            mul(schema.fields().len(), 32)?,
        )?,
        512,
    )?;
    let credit = budget.reserve(retained)?;
    let _scratch = budget.reserve(scratch)?;
    let batch = concat_batches(schema, batches).map_err(|e| e.to_string())?;
    let owned = FundedBatch::from_prepaid(batch, credit, budget)?;
    if owned.batch.num_rows() != rows {
        return Err("join concat row count mismatch".into());
    }
    cancel.check()?;
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatchOptions,
    };
    use std::sync::Arc;

    fn fixture() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(i64::MAX),
                    Some(1),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("é\0雪"),
                    None,
                    Some(""),
                    Some("payload"),
                ])),
            ],
        )
        .unwrap()
    }
    #[test]
    fn real_concat_preserves_empty_unicode_nulls_and_sliced_backing() {
        let budget = Budget::new(1 << 20);
        let input = fixture();
        let source = budget.reserve(input.get_array_memory_size()).unwrap();
        for batches in [
            vec![],
            vec![input.slice(1, 2)],
            vec![input.slice(0, 3), input.slice(2, 2)],
        ] {
            let expected = concat_batches(&input.schema(), &batches).unwrap();
            let output =
                concat(&input.schema(), &batches, &budget, &Cancellation::default()).unwrap();
            assert_eq!(output.batch(), &expected);
            assert_eq!(
                output.credit.bytes(),
                output.batch.get_array_memory_size() + envelope(2).unwrap()
            );
            drop(output);
            assert_eq!(budget.used(), source.bytes());
        }
        let schema = Arc::new(Schema::empty());
        let empty = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap();
        let output = concat(&schema, &[empty], &budget, &Cancellation::default()).unwrap();
        assert_eq!(output.batch().num_rows(), 7);
        drop(output);
        drop(input);
        drop(source);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn unsupported_payload_uses_only_the_legacy_borrowed_owner() {
        use arrow::array::BinaryArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "unused",
            DataType::Binary,
            true,
        )]));
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![
                Some(b"opaque".as_slice()),
                None,
            ]))],
        )
        .unwrap();
        let budget = Budget::new(input.get_array_memory_size());
        let legacy = for_join(
            &schema,
            std::slice::from_ref(&input),
            &budget,
            &Cancellation::default(),
        )
        .unwrap();
        assert!(matches!(&legacy, JoinBuild::Legacy { .. }));
        assert_eq!(legacy.batch(), &input);
        assert_eq!(budget.used(), input.get_array_memory_size());
        // Funded path rejects this shape before making any reservation.
        assert!(
            concat(
                &schema,
                std::slice::from_ref(&input),
                &budget,
                &Cancellation::default()
            )
            .is_err()
        );
        drop(legacy);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn receipts_reject_foreign_short_and_resource_or_cancellation_failures_cleanly() {
        let budget = Budget::new(1 << 20);
        let foreign = Budget::new(1 << 20);
        assert!(
            FundedBatch::from_prepaid(fixture(), foreign.reserve(8192).unwrap(), &budget).is_err()
        );
        assert_eq!(foreign.used(), 0);
        assert!(FundedBatch::from_prepaid(fixture(), budget.reserve(1).unwrap(), &budget).is_err());
        assert_eq!(budget.used(), 0);
        let input = fixture();
        let small = Budget::new(1);
        assert!(
            concat(
                &input.schema(),
                std::slice::from_ref(&input),
                &small,
                &Cancellation::default()
            )
            .is_err()
        );
        assert_eq!(small.used(), 0);
        let canceled = Cancellation::default();
        canceled.cancel();
        assert!(
            concat(
                &input.schema(),
                std::slice::from_ref(&input),
                &budget,
                &canceled
            )
            .is_err()
        );
        assert_eq!(budget.used(), 0);
        assert!(add(usize::MAX, 1).is_err());
        assert!(mul(usize::MAX, 2).is_err());
    }
}
