//! Draft only: immutable recursive-frontier storage, not yet wired or compiled.
//! Intended destination: src/rel/recursive/worktable.rs.
//! Row admission (UNION seen set / keyed updates) belongs before Builder::push.
use crate::{
    Result,
    memory::{Budget, Reservation},
    spill::{Cancellation, SpillFile, SpillStore},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

enum Payload {
    Memory(RecordBatch),
    Spill(SpillFile),
}
struct Entry {
    payload: Payload,
    // Payload drops before its memory credit.
    _lease: Reservation,
}

/// A completed iteration is immutable while any recursive reference reads it.
/// Owner supplies Arc sharing, with its allocation separately funded.
pub(super) struct Snapshot {
    schema: SchemaRef,
    entries: Vec<Entry>,
    rows: usize,
}
impl Snapshot {
    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn replay(
        &self,
        cancel: &Cancellation,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        cancel.check()?;
        for entry in &self.entries {
            cancel.check()?;
            match &entry.payload {
                Payload::Memory(batch) => emit(batch.clone())?,
                Payload::Spill(file) => {
                    // Keep SpillBatch's decode reservation alive through emit.
                    let decoded = file.read_batch()?;
                    if decoded.batch.schema() != self.schema {
                        return Err("internal: recursive spill schema mismatch".into());
                    }
                    emit(decoded.batch.clone())?;
                }
            }
        }
        cancel.check()
    }
}

pub(super) struct Builder {
    snapshot: Snapshot,
    budget: Budget,
    cancel: Cancellation,
    spill: Option<SpillStore>,
}
impl Builder {
    pub(super) fn new(
        schema: SchemaRef,
        budget: Budget,
        cancel: Cancellation,
        spill: Option<SpillStore>,
    ) -> Result<Self> {
        cancel.check()?;
        Ok(Self {
            snapshot: Snapshot {
                schema,
                entries: Vec::new(),
                rows: 0,
            },
            budget,
            cancel,
            spill,
        })
    }
    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.cancel.check()?;
        if batch.schema() != self.snapshot.schema {
            return Err("internal: recursive frontier schema mismatch".into());
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = self
            .snapshot
            .rows
            .checked_add(batch.num_rows())
            .ok_or("recursive frontier row count overflow")?;
        // Four entry slots per batch cover Vec growth and old/new overlap,
        // matching existing conservative retained-materialization accounting.
        let metadata = std::mem::size_of::<Entry>()
            .checked_mul(4)
            .and_then(|n| n.checked_add(256))
            .ok_or("recursive frontier metadata overflow")?;
        let payload_bytes = if self.spill.is_some() {
            0
        } else {
            batch.get_array_memory_size()
        };
        let lease = self.budget.reserve(
            metadata
                .checked_add(payload_bytes)
                .ok_or("recursive frontier allocation overflow")?,
        )?;
        let payload = if let Some(store) = &self.spill {
            Payload::Spill(store.write_batch(&batch)?)
        } else {
            Payload::Memory(batch)
        };
        self.snapshot
            .entries
            .try_reserve(1)
            .map_err(|_| "resource-limit: recursive frontier metadata allocation")?;
        self.snapshot.entries.push(Entry {
            payload,
            _lease: lease,
        });
        self.snapshot.rows = rows;
        Ok(())
    }
    pub(super) fn finish(self) -> Result<Snapshot> {
        self.cancel.check()?;
        Ok(self.snapshot)
    }
}

// Integration must NOT simply call Node::run repeatedly in the same Context:
// it must install a delta snapshot, establish a fresh dependent-cache epoch,
// evaluate the complete recursive arm into a new Builder, and restore scope on
// every success/error/cancel path. UNION filtering requires a separately funded
// global seen set. recurring.cte requires an accumulated snapshot. USING KEY
// requires keyed replacement/aggregate payload state. None is supplied here.

// Frontier lifetime, resource accounting and replay regression tests.
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![11, 22, 33]))],
        )
        .unwrap()
    }
    fn values(snapshot: &Snapshot, cancel: &Cancellation) -> Vec<i64> {
        let mut values = Vec::new();
        snapshot
            .replay(cancel, &mut |b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                values.extend(a.values().iter().copied());
                Ok(())
            })
            .unwrap();
        values
    }
    #[test]
    fn two_references_read_identical_immutable_frontier_and_release_credit() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let b = batch();
        let mut build = Builder::new(b.schema(), budget.clone(), cancel.clone(), None).unwrap();
        build.push(b).unwrap();
        let snapshot = Arc::new(build.finish().unwrap());
        let reference = snapshot.clone();
        assert_eq!(snapshot.rows(), 3);
        assert_eq!(values(&snapshot, &cancel), vec![11, 22, 33]);
        assert_eq!(values(&reference, &cancel), vec![11, 22, 33]);
        drop(snapshot);
        assert!(budget.used() > 0);
        drop(reference);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn tiny_budget_refuses_before_retention_and_does_not_publish_rows() {
        let budget = Budget::new(1);
        let b = batch();
        let mut build =
            Builder::new(b.schema(), budget.clone(), Cancellation::default(), None).unwrap();
        assert!(
            build
                .push(b)
                .unwrap_err()
                .to_string()
                .contains("resource-limit")
        );
        assert_eq!(build.snapshot.rows, 0);
        assert!(build.snapshot.entries.is_empty());
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn row_counter_overflow_is_error_without_allocation() {
        let budget = Budget::new(1 << 20);
        let b = batch();
        let mut build =
            Builder::new(b.schema(), budget.clone(), Cancellation::default(), None).unwrap();
        // Exercise checked counter directly without allocating usize::MAX rows.
        build.snapshot.rows = usize::MAX - 1;
        assert!(
            build
                .push(b)
                .unwrap_err()
                .to_string()
                .contains("row count overflow")
        );
        assert!(build.snapshot.entries.is_empty());
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn cancellation_before_push_and_between_replay_batches_is_not_success() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let b = batch();
        let mut build = Builder::new(b.schema(), budget.clone(), cancel.clone(), None).unwrap();
        build.push(b.clone()).unwrap();
        build.push(b.clone()).unwrap();
        let snapshot = build.finish().unwrap();
        let mut calls = 0;
        let error = snapshot
            .replay(&cancel, &mut |_| {
                calls += 1;
                cancel.cancel();
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(calls, 1);
        drop(snapshot);
        assert_eq!(budget.used(), 0);
        assert!(Builder::new(b.schema(), budget, cancel, None).is_err());
    }
    #[test]
    fn consumer_error_releases_frontier_on_owner_drop() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let b = batch();
        let mut build = Builder::new(b.schema(), budget.clone(), cancel.clone(), None).unwrap();
        build.push(b).unwrap();
        let snapshot = build.finish().unwrap();
        let error = snapshot
            .replay(&cancel, &mut |_| Err("injected consumer failure".into()))
            .unwrap_err();
        assert!(error.to_string().contains("injected consumer failure"));
        drop(snapshot);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn spill_decode_credit_remains_live_inside_callback_then_is_released() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "assay-recursive-worktable-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let budget = Budget::new(16 << 20);
        let cancel = Cancellation::default();
        let store = SpillStore::new(&path, 16 << 20, budget.clone(), cancel.clone()).unwrap();
        let b = batch();
        let mut build = Builder::new(
            b.schema(),
            budget.clone(),
            cancel.clone(),
            Some(store.clone()),
        )
        .unwrap();
        build.push(b).unwrap();
        let snapshot = build.finish().unwrap();
        let retained = budget.used();
        snapshot
            .replay(&cancel, &mut |decoded| {
                assert_eq!(decoded.num_rows(), 3);
                assert!(
                    budget.used() > retained,
                    "decoded arrays must remain funded through callback"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(budget.used(), retained);
        assert_eq!(values(&snapshot, &cancel), vec![11, 22, 33]);
        drop(snapshot);
        store.finish().unwrap();
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
        std::fs::remove_dir(path).unwrap();
    }
}
