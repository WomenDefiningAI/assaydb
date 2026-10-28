//! Optional append-only accumulated frontier index. Ordinary recursion does not
//! allocate one. Every view fixes its end index; later generations are invisible.
use super::worktable::Snapshot;
use crate::{
    Result,
    memory::{Budget, Reservation},
    spill::Cancellation,
};
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, Mutex};
struct Entry {
    snapshot: Arc<Snapshot>,
    _credit: Reservation,
}
pub(super) struct History {
    entries: Mutex<Vec<Entry>>,
    budget: Budget,
    _credit: Reservation,
}
#[derive(Clone)]
pub(super) struct View {
    history: Arc<History>,
    end: usize,
}
impl History {
    pub(super) fn new(budget: Budget) -> Result<Arc<Self>> {
        let credit = budget.reserve(std::mem::size_of::<Self>() + 256)?;
        Ok(Arc::new(Self {
            entries: Mutex::new(Vec::new()),
            budget,
            _credit: credit,
        }))
    }
    pub(super) fn append(
        self: &Arc<Self>,
        snapshot: Arc<Snapshot>,
        cancel: &Cancellation,
    ) -> Result<View> {
        cancel.check()?;
        let credit = self
            .budget
            .reserve(4 * std::mem::size_of::<Entry>() + 256)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "internal: recursive history lock poisoned")?;
        entries
            .try_reserve(1)
            .map_err(|_| "resource-limit: recurring history allocation")?;
        entries.push(Entry {
            snapshot,
            _credit: credit,
        });
        Ok(View {
            history: self.clone(),
            end: entries.len(),
        })
    }
}
impl View {
    pub(super) fn replay(
        &self,
        cancel: &Cancellation,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        for index in 0..self.end {
            cancel.check()?;
            // Never hold the mutex through emit: two references in a join can
            // synchronously replay this same history from inside the callback.
            let snapshot = {
                let entries = self
                    .history
                    .entries
                    .lock()
                    .map_err(|_| "internal: recursive history lock poisoned")?;
                entries
                    .get(index)
                    .ok_or("internal: recurring history prefix missing")?
                    .snapshot
                    .clone()
            };
            snapshot.replay(cancel, emit)?;
        }
        cancel.check()
    }
}
// Vec ownership drops entries iteratively, with no linked Arc chain. Snapshots
// are already funded by generation storage; index credits fund only handles and
// vector growth. History descriptors drop after entries and their reservations.
#[cfg(test)]
mod tests {
    use super::super::worktable::Builder;
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    fn snapshot(budget: &Budget, n: i64) -> Arc<Snapshot> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let mut b = Builder::new(
            schema.clone(),
            budget.clone(),
            Cancellation::default(),
            None,
        )
        .unwrap();
        b.push(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))]).unwrap())
            .unwrap();
        Arc::new(b.finish().unwrap())
    }
    #[test]
    fn prefix_is_immutable_and_reentrant_multiple_references_do_not_lock_each_other() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let history = History::new(budget.clone()).unwrap();
        let first = history.append(snapshot(&budget, 1), &cancel).unwrap();
        let second = history.append(snapshot(&budget, 2), &cancel).unwrap();
        let mut rows = 0;
        first
            .replay(&cancel, &mut |b| {
                rows += b.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 1);
        let mut pairs = 0;
        second
            .replay(&cancel, &mut |_| {
                second.replay(&cancel, &mut |_| {
                    pairs += 1;
                    Ok(())
                })
            })
            .unwrap();
        assert_eq!(pairs, 4);
        drop(first);
        drop(second);
        drop(history);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn long_history_cleanup_is_iterative_and_cancellation_releases_all_credits() {
        let budget = Budget::new(64 << 20);
        let cancel = Cancellation::default();
        let data = snapshot(&budget, 1);
        let history = History::new(budget.clone()).unwrap();
        let mut view = None;
        for _ in 0..50000 {
            view = Some(history.append(data.clone(), &cancel).unwrap());
        }
        cancel.cancel();
        assert!(
            view.as_ref()
                .unwrap()
                .replay(&cancel, &mut |_| Ok(()))
                .is_err()
        );
        drop(view);
        drop(history);
        drop(data);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn failed_append_keeps_existing_prefix_and_releases_temporary_credit() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let data = snapshot(&budget, 1);
        let history = History::new(budget.clone()).unwrap();
        let view = history.append(data.clone(), &cancel).unwrap();
        let entry_bytes = 4 * std::mem::size_of::<Entry>() + 256;
        let blocker = budget
            .reserve((1 << 20) - budget.used() - (entry_bytes - 1))
            .unwrap();
        let baseline = budget.used();
        assert!(history.append(data.clone(), &cancel).is_err());
        assert_eq!(budget.used(), baseline);
        assert_eq!(history.entries.lock().unwrap().len(), 1);
        drop(blocker);
        let mut rows = 0;
        view.replay(&cancel, &mut |b| {
            rows += b.num_rows();
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 1);
        drop(view);
        drop(history);
        drop(data);
        assert_eq!(budget.used(), 0);
    }
}
