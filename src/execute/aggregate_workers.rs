//! Independent canonical states, with original row order and a batch barrier.
//! No row-partial SUM merging: each state has exactly one lifetime owner.
use super::{Aggregate, Options, States, Value, bind};
use crate::{
    Result,
    memory::{Budget, Reservation, Workers as Permits},
};
use std::{
    sync::{
        Arc,
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

pub(super) struct Batch {
    pub values: Vec<Option<Value>>,
    pub groups: Vec<usize>,
    pub rows: usize,
    pub group_count: usize,
    pub _group_lease: Reservation,
}
struct Report {
    error: Option<(usize, String)>,
    bounded: usize,
    seconds: f64,
}
type OwnedStates = Vec<(usize, States)>;
pub(super) struct Workers {
    senders: Vec<SyncSender<Arc<Batch>>>,
    receivers: Vec<Receiver<Report>>,
    handles: Vec<crate::memory::Joined<JoinHandle<OwnedStates>>>,
    owners: Vec<Option<usize>>,
    scalar: bool,
    cancellation: crate::spill::Cancellation,
    _permits: Permits,
    _metadata: Reservation,
}
impl Workers {
    pub fn new(
        states: &mut [States],
        aggregates: &[bind::Agg],
        sources: &[usize],
        scalar: bool,
        options: &Options,
        budget: &Budget,
    ) -> Result<Option<Self>> {
        let canonical_count = sources
            .iter()
            .enumerate()
            .filter(|(i, source)| *i == **source)
            .count();
        if canonical_count < 2
            || aggregates.iter().enumerate().any(|(i, agg)| {
                sources[i] == i
                    && (agg.distinct
                        || !matches!(
                            agg.function,
                            Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                        ))
            })
        {
            return Ok(None);
        }
        // Leave at least half the background slots available for scan decoding.
        let permits = budget.workers(
            (canonical_count - 1).min((options.workers - 1) / 2),
            options.workers - 1,
        );
        if permits.count == 0 {
            return Ok(None);
        }
        // One outstanding batch, channel envelopes, reports, descriptors and
        // Arc headers. Values and group-index buffers retain their own leases.
        let Ok(metadata) = budget.reserve(permits.count * 4096 + aggregates.len() * 512) else {
            return Ok(None);
        };
        let canonical: Vec<usize> = sources
            .iter()
            .enumerate()
            .filter_map(|(i, source)| (i == *source).then_some(i))
            .collect();
        let mut workers = Self {
            senders: Vec::new(),
            receivers: Vec::new(),
            handles: Vec::new(),
            owners: vec![None; aggregates.len()],
            scalar,
            cancellation: options.cancellation.clone(),
            _permits: permits,
            _metadata: metadata,
        };
        for (position, index) in canonical.iter().enumerate() {
            let owner = position % (workers._permits.count + 1);
            if owner != 0 {
                workers.owners[*index] = Some(owner - 1);
            }
        }
        for owner in 0..workers._permits.count {
            let mut owned: Vec<(usize, Aggregate, States)> = canonical
                .iter()
                .filter(|i| workers.owners[**i] == Some(owner))
                .map(|i| {
                    (
                        *i,
                        aggregates[*i].function.clone(),
                        std::mem::replace(&mut states[*i], States::new(&aggregates[*i])),
                    )
                })
                .collect();
            let (send, receive) = mpsc::sync_channel::<Arc<Batch>>(1);
            let (report, result) = mpsc::sync_channel(1);
            workers.senders.push(send);
            workers.receivers.push(result);
            let cancellation = options.cancellation.clone();
            let live = budget.thread_started();
            let handle = thread::Builder::new()
                .name(format!("assaydb-aggregate-{owner}"))
                .spawn(move || {
                    while let Ok(batch) = receive.recv() {
                        let start = Instant::now();
                        let mut error = None;
                        let mut bounded = 0;
                        for (index, function, state) in &mut owned {
                            let update = (|| {
                                cancellation.check()?;
                                // Coordinator reserved every new group's state first.
                                while state.counts.len() < batch.group_count {
                                    state.push();
                                }
                                state.update_plain(
                                    function,
                                    batch.values[*index].as_ref(),
                                    &batch.groups,
                                    batch.rows,
                                    scalar,
                                    true,
                                )
                            })();
                            match update {
                                Ok(n) => bounded += n,
                                Err(e) => {
                                    error = Some((*index, e));
                                    break;
                                }
                            }
                        }
                        // Acknowledgement is the lifetime barrier for batch buffers.
                        drop(batch);
                        if report
                            .send(Report {
                                error,
                                bounded,
                                seconds: start.elapsed().as_secs_f64(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    owned
                        .into_iter()
                        .map(|(index, _, state)| (index, state))
                        .collect()
                })
                .map_err(|e| format!("aggregate worker spawn: {e}"))?;
            workers
                .handles
                .push(crate::memory::Joined::new(handle, live));
        }
        Ok(Some(workers))
    }
    pub fn owns(&self, index: usize) -> bool {
        self.owners[index].is_some()
    }
    pub fn update(
        &mut self,
        states: &mut [States],
        aggregates: &[bind::Agg],
        sources: &[usize],
        batch: Batch,
    ) -> Result<(usize, f64)> {
        self.cancellation.check()?;
        let batch = Arc::new(batch);
        for send in &self.senders {
            send.send(batch.clone())
                .map_err(|_| "aggregate worker stopped before batch")?;
        }
        let mut error: Option<(usize, String)> = None;
        let mut bounded = 0;
        let mut seconds = 0.;
        for (index, aggregate) in aggregates.iter().enumerate() {
            if sources[index] != index || self.owns(index) {
                continue;
            }
            match states[index].update_plain(
                &aggregate.function,
                batch.values[index].as_ref(),
                &batch.groups,
                batch.rows,
                self.scalar,
                true,
            ) {
                Ok(n) => bounded += n,
                Err(e) => {
                    error = Some((index, e));
                    break;
                }
            }
        }
        // Do not cancel lower-index work when a later state fails. Finish the
        // barrier and choose the original aggregate-index error, not the race.
        for receive in &self.receivers {
            let report = receive
                .recv()
                .map_err(|_| "aggregate worker stopped before acknowledgement")?;
            bounded += report.bounded;
            seconds += report.seconds;
            if let Some(candidate) = report.error
                && error.as_ref().is_none_or(|e| candidate.0 < e.0)
            {
                error = Some(candidate);
            }
        }
        if let Some((_, error)) = error {
            return Err(error);
        }
        self.cancellation.check()?;
        Ok((bounded, seconds))
    }
    pub fn finish(mut self, states: &mut [States]) -> Result<()> {
        self.senders.clear();
        self.receivers.clear();
        let mut failed = false;
        for handle in self.handles.drain(..) {
            match handle.join() {
                Ok(returned) => {
                    for (index, state) in returned {
                        states[index] = state;
                    }
                }
                Err(_) => failed = true,
            }
        }
        if failed {
            Err("aggregate worker panicked".into())
        } else {
            Ok(())
        }
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        // Disconnect both directions first: a worker may be waiting on an
        // acknowledgement send when the coordinator encounters an error.
        self.senders.clear();
        self.receivers.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Decimal128Array},
        datatypes::i256,
    };
    fn aggregates(functions: &[Aggregate]) -> Vec<bind::Agg> {
        functions
            .iter()
            .map(|function| bind::Agg {
                function: function.clone(),
                input: None,
                ty: crate::types::Type::Decimal(0),
                distinct: false,
            })
            .collect()
    }
    fn batch(
        budget: &Budget,
        values: &[Option<i128>],
        groups: Vec<usize>,
        count: usize,
        columns: usize,
    ) -> Batch {
        let array: ArrayRef = Arc::new(Decimal128Array::from(values.to_vec()));
        let value = Value::adopt(array, budget).unwrap();
        let lease = budget
            .reserve(groups.len() * std::mem::size_of::<usize>())
            .unwrap();
        Batch {
            rows: values.len(),
            values: vec![Some(value); columns],
            groups,
            group_count: count,
            _group_lease: lease,
        }
    }
    #[test]
    fn aliases_late_groups_nulls_and_batch_buffers_keep_one_state_owner() {
        let budget = Budget::new(1 << 20);
        let state_charge = budget.reserve(65536).unwrap();
        let aggregates = aggregates(&[
            Aggregate::Sum,
            Aggregate::Count,
            Aggregate::Avg,
            Aggregate::Sum,
        ]);
        let sources = [0, 1, 0, 3];
        let mut states: Vec<_> = aggregates.iter().map(States::new).collect();
        let options = Options {
            workers: 4,
            ..Options::default()
        };
        let mut pool = Workers::new(&mut states, &aggregates, &sources, false, &options, &budget)
            .unwrap()
            .unwrap();
        let steady = budget.used();
        for (values, groups, count) in [
            (vec![Some(2), None, Some(-1)], vec![0, 0, 0], 1),
            (vec![Some(7), Some(3), None], vec![1, 0, 1], 2),
        ] {
            for (i, state) in states.iter_mut().enumerate() {
                if sources[i] == i && !pool.owns(i) {
                    while state.counts.len() < count {
                        state.push();
                    }
                }
            }
            pool.update(
                &mut states,
                &aggregates,
                &sources,
                batch(&budget, &values, groups, count, 4),
            )
            .unwrap();
            assert_eq!(
                budget.used(),
                steady,
                "workers must drop input owners before acknowledgement"
            );
        }
        pool.finish(&mut states).unwrap();
        assert_eq!(
            states[0].sums.as_ref().unwrap(),
            &[i256::from_i128(4), i256::from_i128(7)]
        );
        assert_eq!(
            states[3].sums.as_ref().unwrap(),
            &[i256::from_i128(4), i256::from_i128(7)]
        );
        assert_eq!(states[1].counts, [3, 1]);
        assert!(
            states[2].counts.is_empty(),
            "SUM/AVG alias never owns duplicate state"
        );
        assert_eq!(budget.used(), 65536);
        drop(states);
        drop(state_charge);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.workers(3, 3).count, 3);
    }
    #[test]
    fn earlier_worker_error_beats_later_coordinator_error() {
        let budget = Budget::new(1 << 20);
        let state_charge = budget.reserve(65536).unwrap();
        let aggregates = aggregates(&[Aggregate::Count, Aggregate::Sum, Aggregate::Count]);
        let sources = [0, 1, 2];
        let mut states: Vec<_> = aggregates.iter().map(States::new).collect();
        for state in &mut states {
            state.push();
        }
        // Final-only SQL overflow no longer fails during update. Inject an
        // impossible wide-state overflow to keep worker-error arbitration covered.
        states[1].sums.as_mut().unwrap()[0] = i256::MAX;
        states[2].counts[0] = u64::MAX;
        let options = Options {
            workers: 4,
            ..Options::default()
        };
        let mut pool = Workers::new(&mut states, &aggregates, &sources, true, &options, &budget)
            .unwrap()
            .unwrap();
        assert!(pool.owns(1));
        assert!(!pool.owns(2));
        let error = pool
            .update(
                &mut states,
                &aggregates,
                &sources,
                batch(&budget, &[Some(1)], vec![0], 1, 3),
            )
            .unwrap_err();
        assert_eq!(error, "aggregate accumulator overflow".to_string());
        drop(pool);
        drop(states);
        drop(state_charge);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn pending_acknowledgements_drop_without_blocking_or_retaining_batches() {
        for cancel in [false, true] {
            let budget = Budget::new(1 << 20);
            let state_charge = budget.reserve(65536).unwrap();
            let aggregates = aggregates(&[Aggregate::Count, Aggregate::Sum]);
            let sources = [0, 1];
            let mut states: Vec<_> = aggregates.iter().map(States::new).collect();
            for state in &mut states {
                state.push();
            }
            let options = Options {
                workers: 4,
                ..Options::default()
            };
            let pool = Workers::new(&mut states, &aggregates, &sources, true, &options, &budget)
                .unwrap()
                .unwrap();
            // Fill the ACK slot, then enqueue work whose acknowledgement can
            // block. A third command can remain queued during shutdown. This
            // intentionally exercises channel teardown below the batch barrier.
            for _ in 0..3 {
                pool.senders[0]
                    .send(Arc::new(batch(&budget, &[Some(1)], vec![0], 1, 2)))
                    .unwrap();
            }
            if cancel {
                options.cancellation.cancel();
            }
            drop(pool);
            assert_eq!(budget.used(), 65536);
            drop(states);
            drop(state_charge);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(3, 3).count, 3);
        }
    }
    #[test]
    fn multiple_helper_errors_and_earlier_coordinator_error_keep_index_priority() {
        for coordinator_error in [false, true] {
            let budget = Budget::new(1 << 20);
            let aggregates = aggregates(&[Aggregate::Count, Aggregate::Sum, Aggregate::Count]);
            let sources = [0, 1, 2];
            let mut states: Vec<_> = aggregates.iter().map(States::new).collect();
            for state in &mut states {
                state.push();
            }
            if coordinator_error {
                states[0].counts[0] = u64::MAX;
            }
            // Final-only SQL overflow no longer fails during update. Inject an
            // impossible wide-state overflow to keep worker-error arbitration covered.
            states[1].sums.as_mut().unwrap()[0] = i256::MAX;
            states[2].counts[0] = u64::MAX;
            let options = Options {
                workers: 8,
                ..Options::default()
            };
            let mut pool =
                Workers::new(&mut states, &aggregates, &sources, true, &options, &budget)
                    .unwrap()
                    .unwrap();
            assert!(pool.owns(1) && pool.owns(2));
            let error = pool
                .update(
                    &mut states,
                    &aggregates,
                    &sources,
                    batch(&budget, &[Some(1)], vec![0], 1, 3),
                )
                .unwrap_err();
            assert_eq!(
                error,
                if coordinator_error {
                    "aggregate count overflow".into()
                } else {
                    "aggregate accumulator overflow".to_string()
                }
            );
            drop(pool);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn unavailable_resources_fall_back_and_cancelled_pool_releases_permits() {
        let aggregates = aggregates(&[Aggregate::Sum, Aggregate::Count]);
        let sources = [0, 1];
        for mode in [0, 1, 2] {
            let budget = Budget::new(if mode == 0 { 1 } else { 1 << 20 });
            let held = (mode == 1).then(|| budget.workers(3, 3));
            let options = Options {
                workers: 4,
                ..Options::default()
            };
            let mut states: Vec<_> = aggregates.iter().map(States::new).collect();
            for state in &mut states {
                state.push();
            }
            let pool =
                Workers::new(&mut states, &aggregates, &sources, true, &options, &budget).unwrap();
            if mode < 2 {
                assert!(pool.is_none());
            } else {
                let mut pool = pool.unwrap();
                options.cancellation.cancel();
                assert!(
                    pool.update(
                        &mut states,
                        &aggregates,
                        &sources,
                        batch(&budget, &[], vec![], 1, 2)
                    )
                    .unwrap_err()
                    .starts_with("cancelled:")
                );
                drop(pool);
            }
            drop(held);
            drop(states);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(3, 3).count, 3);
        }
    }
}
