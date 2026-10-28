//! Owned partition updates, with one bounded input batch admitted at a time.
//! This module defines transfer/error/funding contracts; it creates no threads.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
mod routing;
pub(in crate::execute) use routing::{RoutedPartial, RoutingPlan};

struct BatchReceipt {
    abandoned: AtomicBool,
    routing: RoutingPlan,
    _metadata: Reservation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ErrorKey {
    phase: u8,
    canonical_state: usize,
    local_group: usize,
}
struct IndexedError {
    key: ErrorKey,
    message: crate::Error,
}
struct Flight {
    generation: u64,
    remaining: usize,
    receipt: Arc<BatchReceipt>,
    error: Option<IndexedError>,
}
/// Neither producer fragments nor completion messages contain a mutable shared
/// state handle. A vacant slot has exactly one outstanding job/completion owner.
pub(in crate::execute) struct OwnedPartitions {
    slots: Vec<Option<Partition>>,
    identity: Arc<()>,
    routing: RoutingPlan,
    generation: u64,
    flight: Option<Flight>,
    failed: Option<crate::Error>,
    finished: bool,
    // Base metadata outlives all owner-held slot/receipt/identity buffers.
    inner: Partitioned,
}
pub(in crate::execute) struct OwnedJob {
    partition: Option<Partition>,
    raw: Arc<RawPartial>,
    groups: Vec<usize>,
    funding: Option<Reservation>,
    state_bytes: usize,
    slot: usize,
    generation: u64,
    identity: Arc<()>,
    receipt: Arc<BatchReceipt>,
    armed: bool,
    _base: Arc<Reservation>,
}
pub(in crate::execute) struct OwnedCompletion {
    partition: Option<Partition>,
    error: Option<IndexedError>,
    slot: usize,
    generation: u64,
    identity: Arc<()>,
    receipt: Arc<BatchReceipt>,
    armed: bool,
    _base: Arc<Reservation>,
}
impl OwnedCompletion {
    /// Read-only terminal-error observation before a runtime discards receipts.
    pub(in crate::execute) fn error_message(&self) -> Option<&str> {
        self.error.as_ref().map(|error| error.message.as_str())
    }
}
impl Drop for OwnedJob {
    fn drop(&mut self) {
        if self.armed {
            self.receipt.abandoned.store(true, Ordering::Release);
        }
    }
}
impl Drop for OwnedCompletion {
    fn drop(&mut self) {
        if self.armed {
            self.receipt.abandoned.store(true, Ordering::Release);
        }
    }
}
impl OwnedPartitions {
    pub(in crate::execute) fn new(
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        budget: Budget,
        count: usize,
    ) -> Result<Self> {
        let mut inner = Partitioned::new(p, expressions, budget, count)?;
        let slots = std::mem::take(&mut inner.partitions)
            .into_iter()
            .map(Some)
            .collect();
        let identity = Arc::new(());
        let routing = RoutingPlan::new(p, expressions, &inner, identity.clone(), count)?;
        Ok(Self {
            inner,
            slots,
            identity,
            routing,
            generation: 0,
            flight: None,
            failed: None,
            finished: false,
        })
    }
    fn check(&mut self) -> Result<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self
            .flight
            .as_ref()
            .is_some_and(|f| f.receipt.abandoned.load(Ordering::Acquire))
        {
            return self.fail("owned partition job/completion was abandoned".into());
        }
        Ok(())
    }
    fn fail<T>(&mut self, error: crate::Error) -> Result<T> {
        self.failed = Some(error.clone());
        Err(error)
    }
    #[cfg(test)]
    pub(in crate::execute) fn next_sequence(&self) -> u64 {
        self.generation
    }
    #[cfg(test)]
    pub(in crate::execute) fn submit(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        raw: Arc<RawPartial>,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<Vec<OwnedJob>> {
        let result = self.submit_inner(p, expressions, raw, cancellation);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }
    /// Clone once into each producer; routing never borrows mutable partitions.
    pub(in crate::execute) fn routing_plan(&self) -> RoutingPlan {
        self.routing.clone()
    }
    #[cfg(test)]
    fn submit_inner(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        raw: Arc<RawPartial>,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<Vec<OwnedJob>> {
        self.check_admission(cancellation)?;
        self.routing.check_layout(p, expressions)?;
        let routed = self.routing.route(raw, cancellation)?;
        self.submit_routed_inner(routed, cancellation)
    }
    fn check_admission(&mut self, cancellation: &crate::spill::Cancellation) -> Result<()> {
        self.check()?;
        if self.finished {
            return self.fail("partition input already finalized".into());
        }
        if self.flight.is_some() {
            return self.fail("previous partition batch has unacknowledged jobs".into());
        }
        cancellation.check()
    }
    /// The producer already hashed every group. Admission visits partition
    /// descriptors only, acquires ALL destination funding, then moves slots.
    pub(in crate::execute) fn submit_routed(
        &mut self,
        routed: RoutedPartial,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<Vec<OwnedJob>> {
        let result = self.submit_routed_inner(routed, cancellation);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }
    fn submit_routed_inner(
        &mut self,
        mut routed: RoutedPartial,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<Vec<OwnedJob>> {
        self.check_admission(cancellation)?;
        self.routing.validate_fragment(&routed)?;
        let next = self
            .generation
            .checked_add(1)
            .ok_or("partition batch sequence overflow")?;
        // Keep the owning fragment intact across every fallible reservation,
        // so routing buffers always drop before their prepaid metadata credit.
        // No partition moves and no destination mutates until every lease exists.
        if routed.raw.numeric_group_keys.is_some() {
            for (slot, groups) in routed.groups.iter().enumerate() {
                let state = &self.slots[slot].as_ref().expect("idle partition").state;
                let needed = state
                    .numeric_merge_headroom_sources(routed.plan.state_sources(), groups.len())?
                    .checked_add(groups.len() * ORDER_BYTES_PER_GROUP)
                    .ok_or("partition growth overflow")?;
                routed.growth[slot] = routed.growth[slot].max(needed);
            }
        }
        let funding = routed
            .growth
            .iter()
            .map(|n| self.inner.budget.reserve(*n))
            .collect::<Result<Vec<_>>>()?;
        let routing::RoutedPartial {
            raw,
            groups,
            growth: _,
            plan,
            credit,
        } = routed;
        let state_bytes = plan.state_bytes();
        let receipt = Arc::new(BatchReceipt {
            abandoned: AtomicBool::new(false),
            routing: plan,
            _metadata: credit,
        });
        let count = groups.iter().filter(|g| !g.is_empty()).count();
        let mut jobs = Vec::with_capacity(count);
        for (slot, (groups, funding)) in groups.into_iter().zip(funding).enumerate() {
            if groups.is_empty() {
                continue;
            }
            let partition = self.slots[slot]
                .take()
                .expect("admission requires idle slots");
            jobs.push(OwnedJob {
                partition: Some(partition),
                raw: raw.clone(),
                groups,
                funding: Some(funding),
                state_bytes,
                slot,
                generation: self.generation,
                identity: self.identity.clone(),
                receipt: receipt.clone(),
                armed: true,
                _base: self.inner._metadata.clone(),
            });
        }
        if count == 0 {
            self.generation = next;
        } else {
            self.flight = Some(Flight {
                generation: self.generation,
                remaining: count,
                receipt,
                error: None,
            });
        }
        Ok(jobs)
    }
    pub(in crate::execute) fn acknowledge(
        &mut self,
        mut completion: OwnedCompletion,
    ) -> Result<()> {
        self.check()?;
        let valid = self.flight.as_ref().is_some_and(|f| {
            f.generation == completion.generation && Arc::ptr_eq(&f.receipt, &completion.receipt)
        }) && Arc::ptr_eq(&self.identity, &completion.identity)
            && self.slots.get(completion.slot).is_some_and(Option::is_none)
            && completion.partition.is_some();
        if !valid {
            return self.fail("stale, duplicate or wrong-owner partition completion".into());
        }
        self.slots[completion.slot] = completion.partition.take();
        completion.armed = false;
        let flight = self.flight.as_mut().unwrap();
        if let Some(error) = completion.error.take()
            && flight.error.as_ref().is_none_or(|old| error.key < old.key)
        {
            flight.error = Some(error);
        }
        flight.remaining -= 1;
        if flight.remaining == 0 {
            let flight = self.flight.take().unwrap();
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or("partition batch sequence overflow")?;
            if let Some(error) = flight.error {
                return self.fail(error.message);
            }
        }
        Ok(())
    }
    pub(in crate::execute) fn finish(
        &mut self,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<()> {
        self.check()?;
        if self.flight.is_some() {
            return self.fail("partition input finished with missing acknowledgements".into());
        }
        if self.finished {
            return Ok(());
        }
        self.inner.partitions = self
            .slots
            .iter_mut()
            .map(|p| p.take().expect("all partitions acknowledged"))
            .collect();
        if let Err(error) = self.inner.finish(cancellation) {
            return self.fail(error);
        }
        self.finished = true;
        Ok(())
    }
    pub(in crate::execute) fn output_batch(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if !self.finished {
            return Err("partition output before completion".into());
        }
        self.inner.output_batch(p, expressions, schema, range)
    }
    pub(in crate::execute) fn output_bytes(
        &self,
        p: &Plan,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        self.inner.output_bytes(p, range)
    }
    /// Internal post-barrier query only; resident slots are not a progress metric
    /// while ownership is in flight. Executor calls this after finish().
    pub(in crate::execute) fn group_count(&self) -> usize {
        if self.finished {
            self.inner.group_count()
        } else {
            self.slots
                .iter()
                .flatten()
                .map(|p| p.state.group_count)
                .sum()
        }
    }
}
impl OwnedJob {
    pub(in crate::execute) fn run(
        mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        cancellation: &crate::spill::Cancellation,
    ) -> OwnedCompletion {
        let partition = self.partition.as_mut().unwrap();
        partition
            .state
            .add_merge_credit(self.funding.take().unwrap());
        let mut destinations = Vec::with_capacity(self.groups.len());
        let mut error = self
            .receipt
            .routing
            .check_layout(p, expressions)
            .err()
            .map(|message| IndexedError {
                key: ErrorKey {
                    phase: 0,
                    canonical_state: 0,
                    local_group: 0,
                },
                message,
            });
        for &local in &self.groups {
            if error.is_some() {
                break;
            }
            let before = partition.state.group_count;
            match partition.state.locate_partial_group(
                p,
                expressions,
                self.raw.view(),
                local,
                self.raw.encoded.get(local).and_then(Option::as_deref),
                self.state_bytes,
                ORDER_BYTES_PER_GROUP,
                cancellation,
            ) {
                Ok(group) => {
                    if group == before {
                        partition.first_seen.push(self.raw.encounters[local]);
                    } else {
                        partition.first_seen[group] =
                            partition.first_seen[group].min(self.raw.encounters[local]);
                    }
                    destinations.push(group);
                }
                Err(message) => {
                    error = Some(IndexedError {
                        key: ErrorKey {
                            phase: 0,
                            canonical_state: 0,
                            local_group: local,
                        },
                        message,
                    });
                    break;
                }
            }
        }
        if error.is_none() {
            'states: for (index, canonical) in expressions.state_sources.iter().enumerate() {
                if index != *canonical {
                    continue;
                }
                for (&local, &group) in self.groups.iter().zip(&destinations) {
                    let result = cancellation.check().and_then(|()| {
                        partition
                            .state
                            .merge_partial_state(self.raw.view(), local, group, index)
                    });
                    if let Err(message) = result {
                        error = Some(IndexedError {
                            key: ErrorKey {
                                phase: 1,
                                canonical_state: index,
                                local_group: local,
                            },
                            message,
                        });
                        break 'states;
                    }
                }
            }
        }
        partition.state.retain_merge_credit();
        self.armed = false;
        OwnedCompletion {
            partition: self.partition.take(),
            error,
            slot: self.slot,
            generation: self.generation,
            identity: self.identity.clone(),
            receipt: self.receipt.clone(),
            armed: true,
            _base: self._base.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{batch, plan, state};
    use super::*;
    use crate::{expr_program::ExpressionProgram, spill::Cancellation};
    fn submit_rows(
        owner: &mut OwnedPartitions,
        p: &Plan,
        e: &ExpressionProgram,
        rows: &[super::super::tests::Row],
        budget: &Budget,
    ) -> Vec<OwnedJob> {
        let raw = state(p, e, rows, budget)
            .into_raw(
                RawFunding::SharedQuery(budget.clone()),
                owner.next_sequence(),
            )
            .unwrap();
        owner
            .submit(p, e, Arc::new(raw), &Cancellation::default())
            .unwrap()
    }
    #[test]
    fn owned_partitions_keep_tokens_output_and_credits_across_reverse_completions() {
        fn send<T: Send>() {}
        send::<crate::execute::aggregation::OwnedJob>();
        send::<crate::execute::aggregation::OwnedCompletion>();
        let (p, e, schema) = plan(
            "SELECT k,s,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k,s",
            false,
        );
        let rows = (0..80)
            .map(|i| {
                (
                    Some(i % 13),
                    Some(if i % 2 == 0 { "é\0" } else { "x" }),
                    Some(i),
                )
            })
            .collect::<Vec<_>>();
        for partitions in [1, 4] {
            let budget = Budget::new(16 << 20);
            let expected = state(&p, &e, &rows, &budget);
            let mut actual = OwnedPartitions::new(&p, &e, budget.clone(), partitions).unwrap();
            for chunk in rows.chunks(7) {
                let mut jobs = submit_rows(&mut actual, &p, &e, chunk, &budget);
                jobs.reverse();
                let mut completions = Vec::new();
                for job in jobs {
                    // Transfer ownership across a real thread, with no shared
                    // mutable table and no concurrent query scheduler enabled.
                    let permits = budget.workers(1, 1);
                    assert_eq!(permits.count, 1);
                    let completion = std::thread::scope(|scope| {
                        scope
                            .spawn(|| job.run(&p, &e, &Cancellation::default()))
                            .join()
                            .unwrap()
                    });
                    drop(permits);
                    completions.push(completion);
                }
                completions.reverse();
                for completion in completions {
                    actual.acknowledge(completion).unwrap();
                }
            }
            actual.finish(&Cancellation::default()).unwrap();
            for start in (0..expected.group_count).step_by(3) {
                let range = start..(start + 3).min(expected.group_count);
                assert_eq!(
                    actual.output_batch(&p, &e, &schema, range.clone()).unwrap(),
                    expected.output_batch(&p, &e, &schema, range).unwrap()
                );
            }
            drop(actual);
            drop(expected);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.peak_workers(), 1);
        }
    }
    #[test]
    fn owned_merge_competing_errors_match_ordinary_canonical_state_order() {
        let (p, e, _) = plan("SELECT SUM(v),COUNT(*) FROM t GROUP BY k", false);
        let rows = (0..16)
            .map(|i| (Some(i), None, Some(1)))
            .collect::<Vec<_>>();
        for count in [1, 4] {
            let budget = Budget::new(8 << 20);
            let mut ordinary = state(&p, &e, &rows, &budget);
            let mut actual = OwnedPartitions::new(&p, &e, budget.clone(), count).unwrap();
            let jobs = submit_rows(&mut actual, &p, &e, &rows, &budget);
            for job in jobs {
                actual
                    .acknowledge(job.run(&p, &e, &Cancellation::default()))
                    .unwrap();
            }
            // COUNT in the first group must lose to SUM in a later group,
            // because the ordinary backend visits SUM's canonical state first.
            ordinary.states[1].counts[0] = u64::MAX;
            ordinary.states[0].sums.as_mut().unwrap()[15] = i256::MAX;
            for partition in actual.slots.iter_mut().flatten() {
                if let Some(group) = partition.state.numeric_map.get(0) {
                    partition.state.states[1].counts[group] = u64::MAX;
                }
                if let Some(group) = partition.state.numeric_map.get(15) {
                    partition.state.states[0].sums.as_mut().unwrap()[group] = i256::MAX;
                }
            }
            let expected = ordinary
                .update(&p, &e, &mut Metrics::default(), batch(&p, &rows, &budget))
                .unwrap_err();
            assert!(expected.contains("accumulator overflow"), "{expected}");
            let mut jobs = submit_rows(&mut actual, &p, &e, &rows, &budget);
            jobs.reverse();
            let mut observed = None;
            for job in jobs {
                if let Err(error) = actual.acknowledge(job.run(&p, &e, &Cancellation::default())) {
                    observed = Some(error);
                }
            }
            assert_eq!(observed, Some(expected.clone()));
            assert_eq!(
                actual.finish(&Cancellation::default()).unwrap_err(),
                expected
            );
            drop(actual);
            drop(ordinary);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn owned_abandoned_jobs_completions_and_bad_generations_are_sticky_failures() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let rows = (0..20).map(|i| (Some(i), None, None)).collect::<Vec<_>>();
        for mode in [
            "job",
            "completion",
            "generation",
            "owner",
            "duplicate",
            "missing",
        ] {
            let budget = Budget::new(4 << 20);
            let mut actual = OwnedPartitions::new(&p, &e, budget.clone(), 4).unwrap();
            actual
                .routing
                .set_hash(ahash::RandomState::with_seeds(1, 2, 3, 4));
            let mut jobs = submit_rows(&mut actual, &p, &e, &rows, &budget);
            assert!(jobs.len() > 2);
            let job = jobs.pop().unwrap();
            let error = match mode {
                "job" => {
                    drop(job);
                    actual.finish(&Cancellation::default()).unwrap_err()
                }
                "completion" => {
                    drop(job.run(&p, &e, &Cancellation::default()));
                    actual.finish(&Cancellation::default()).unwrap_err()
                }
                "duplicate" => {
                    let slot = job.slot;
                    actual
                        .acknowledge(job.run(&p, &e, &Cancellation::default()))
                        .unwrap();
                    let mut completion = jobs.pop().expect("multiple occupied partitions").run(
                        &p,
                        &e,
                        &Cancellation::default(),
                    );
                    completion.slot = slot;
                    actual.acknowledge(completion).unwrap_err()
                }
                "generation" | "owner" => {
                    let mut completion = job.run(&p, &e, &Cancellation::default());
                    if mode == "generation" {
                        completion.generation += 1;
                    } else {
                        completion.identity = Arc::new(());
                    }
                    actual.acknowledge(completion).unwrap_err()
                }
                _ => {
                    let error = actual.finish(&Cancellation::default()).unwrap_err();
                    drop(job);
                    error
                }
            };
            assert_eq!(actual.finish(&Cancellation::default()).unwrap_err(), error);
            drop(actual);
            assert!(budget.used() > 0, "outstanding jobs must retain backing");
            drop(jobs);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn raw_funding_domain_and_owned_preflight_are_verified_before_mutation() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let rows = [(Some(1), None, None)];
        let query = Budget::new(4 << 20);
        let private = Budget::new(1 << 20);
        assert!(
            state(&p, &e, &rows, &private)
                .into_raw(RawFunding::SharedQuery(query.clone()), 0)
                .is_err()
        );
        assert!(
            state(&p, &e, &rows, &private)
                .into_raw(RawFunding::PrepaidPrivate(query.reserve(100).unwrap()), 0)
                .is_err()
        );
        assert_eq!(query.used(), 0);
        assert_eq!(private.used(), 0);
        let raw = Arc::new(
            state(&p, &e, &rows, &query)
                .into_raw(RawFunding::SharedQuery(query.clone()), 0)
                .unwrap(),
        );
        let mut actual = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
        let blocker = query.reserve(query.limit() - query.used() - 100).unwrap();
        let before = query.used();
        let error = actual
            .submit(&p, &e, raw.clone(), &Cancellation::default())
            .err()
            .unwrap();
        assert!(error.contains("resource-limit"));
        assert!(actual.slots.iter().all(Option::is_some));
        assert_eq!(query.used(), before);
        assert_eq!(actual.finish(&Cancellation::default()).unwrap_err(), error);
        drop(blocker);
        drop(actual);
        drop(raw);
        assert_eq!(query.used(), 0);
    }
    #[test]
    fn owned_cancellation_returns_state_and_prevents_publication() {
        let (p, e, _) = plan("SELECT k,SUM(v) FROM t GROUP BY k", false);
        let rows = (0..20)
            .map(|i| (Some(i), None, Some(i)))
            .collect::<Vec<_>>();
        let budget = Budget::new(4 << 20);
        let mut actual = OwnedPartitions::new(&p, &e, budget.clone(), 4).unwrap();
        let jobs = submit_rows(&mut actual, &p, &e, &rows, &budget);
        let cancel = Cancellation::default();
        cancel.cancel();
        let mut error = None;
        for job in jobs {
            if let Err(e) = actual.acknowledge(job.run(&p, &e, &cancel)) {
                error = Some(e);
            }
        }
        assert!(error.unwrap().contains("cancelled"));
        assert!(actual.finish(&Cancellation::default()).is_err());
        drop(actual);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn owned_all_destination_funding_precedes_any_slot_transfer() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let rows = (0..20).map(|i| (Some(i), None, None)).collect::<Vec<_>>();
        let query = Budget::new(4 << 20);
        let raw = Arc::new(
            state(&p, &e, &rows, &query)
                .into_raw(RawFunding::SharedQuery(query.clone()), 0)
                .unwrap(),
        );
        let other = Budget::new(4 << 20);
        let mut wrong = OwnedPartitions::new(&p, &e, other.clone(), 4).unwrap();
        let before = other.used();
        assert!(
            wrong
                .submit(&p, &e, raw.clone(), &Cancellation::default())
                .err()
                .unwrap()
                .contains("funding domain mismatch")
        );
        assert!(wrong.slots.iter().all(Option::is_some));
        assert_eq!(other.used(), before);
        drop(wrong);
        assert_eq!(other.used(), 0);
        let mut actual = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
        actual
            .routing
            .set_hash(ahash::RandomState::with_seeds(1, 2, 3, 4));
        let mut counts = [0usize; 4];
        for &key in raw.numeric_group_keys.as_ref().unwrap() {
            let mut h = actual.routing.hash_for_test().build_hasher();
            true.hash(&mut h);
            key.hash(&mut h);
            counts[(h.finish() % 4) as usize] += 1;
        }
        assert!(counts.iter().filter(|&&n| n > 0).count() > 1);
        let metadata = raw.group_count * 64 + 4 * 2048 + 8192;
        let first = *counts.iter().find(|&&n| n > 0).unwrap();
        let growth = 128 + 4 * (16 + 8) + ORDER_BYTES_PER_GROUP;
        let remaining = metadata + first * growth;
        let blocker = query
            .reserve(query.limit() - query.used() - remaining)
            .unwrap();
        let before = query.used();
        assert!(
            actual
                .submit(&p, &e, raw.clone(), &Cancellation::default())
                .err()
                .unwrap()
                .contains("resource-limit")
        );
        assert!(actual.slots.iter().all(Option::is_some));
        assert_eq!(query.used(), before);
        drop(blocker);
        drop(actual);
        drop(raw);
        assert_eq!(query.used(), 0);
    }
    #[test]
    fn transferred_partition_base_lease_outlives_owner_and_last_completion() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let rows = (0..20).map(|i| (Some(i), None, None)).collect::<Vec<_>>();
        let budget = Budget::new(4 << 20);
        let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), 4).unwrap();
        let weak = Arc::downgrade(&owner.inner._metadata);
        let bytes = owner.inner._metadata.bytes();
        let mut jobs = submit_rows(&mut owner, &p, &e, &rows, &budget);
        let completion = jobs.pop().unwrap().run(&p, &e, &Cancellation::default());
        drop(owner);
        assert!(weak.upgrade().is_some());
        assert!(budget.used() >= bytes);
        drop(jobs);
        assert!(
            weak.upgrade().is_some(),
            "completion still owns its partition backing"
        );
        assert!(budget.used() >= bytes);
        drop(completion);
        assert!(weak.upgrade().is_none());
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn persistent_producers_keep_original_tokens_across_batches_and_flush_order() {
        for sql in [
            "SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k",
            "SELECT k,s,SUM(v),COUNT(*) FROM t GROUP BY k,s",
        ] {
            let (p, e, schema) = plan(sql, false);
            let sources = [
                vec![
                    (Some(5), Some("é"), Some(30)),
                    (Some(3), Some("é"), Some(40)),
                ],
                vec![(Some(9), Some("é"), Some(1)), (Some(7), Some("é"), Some(2))],
                vec![
                    (Some(7), Some("é"), Some(10)),
                    (Some(3), Some("é"), Some(20)),
                ],
            ];
            let budget = Budget::new(8 << 20);
            let expected = state(&p, &e, &sources.concat(), &budget);
            let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), 4).unwrap();
            for order in [&[2usize, 0][..], &[1usize][..]] {
                let mut producer = Aggregation::new(&p, &e, budget.clone(), true).unwrap();
                producer.disable_dense();
                for &source in order {
                    let tokens = (0..sources[source].len())
                        .map(|row| EncounterToken::new(source as u64, 0, row))
                        .collect::<Vec<_>>();
                    producer
                        .update_with_encounters(
                            &p,
                            &e,
                            &mut Metrics::default(),
                            batch(&p, &sources[source], &budget),
                            &tokens,
                        )
                        .unwrap();
                }
                let raw = Arc::new(
                    producer
                        .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
                        .unwrap(),
                );
                let mut jobs = owner.submit(&p, &e, raw, &Cancellation::default()).unwrap();
                jobs.reverse();
                for job in jobs {
                    owner
                        .acknowledge(job.run(&p, &e, &Cancellation::default()))
                        .unwrap();
                }
            }
            let mut backend = crate::execute::aggregation::Backend::from_owned_partitions(
                owner,
                budget.clone(),
                Cancellation::default(),
            );
            backend.finish_updates().unwrap();
            assert_eq!(
                backend
                    .output_batch(&p, &e, &schema, 0..backend.group_count())
                    .unwrap(),
                expected
                    .output_batch(&p, &e, &schema, 0..expected.group_count)
                    .unwrap()
            );
            drop(backend);
            drop(expected);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn failed_tokenized_update_keeps_grown_token_storage_funded() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let budget = Budget::new(4 << 20);
        let mut producer = Aggregation::new(&p, &e, budget.clone(), true).unwrap();
        producer.disable_dense();
        producer
            .update_with_encounters(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, &[(Some(0), None, None)], &budget),
                &[EncounterToken::new(0, 0, 0)],
            )
            .unwrap();
        producer.states[0].counts[0] = u64::MAX;
        let old_capacity = producer.encounters.capacity();
        let rows = (1..64)
            .chain(std::iter::once(0))
            .map(|k| (Some(k), None, None))
            .collect::<Vec<_>>();
        let tokens = (0..rows.len())
            .map(|row| EncounterToken::new(0, 1, row))
            .collect::<Vec<_>>();
        let error = producer
            .update_with_encounters(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, &rows, &budget),
                &tokens,
            )
            .unwrap_err();
        assert_eq!(error, "aggregate count overflow");
        let added =
            (producer.encounters.capacity() - old_capacity) * std::mem::size_of::<EncounterToken>();
        assert!(added > 0);
        assert_eq!(producer.state_leases.last().unwrap().bytes(), added);
        assert!(!producer.encounters_valid);
        assert!(
            producer
                .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
                .is_err()
        );
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn failed_raw_export_keeps_backing_through_private_buffer_destruction() {
        let (p, e, _) = plan("SELECT s,COUNT(*) FROM t GROUP BY s", false);
        let rows = [(None, Some("owned private key"), None)];
        for failure in ["tokens", "descriptors"] {
            let query = Budget::new(2 << 20);
            let private = Budget::new(1 << 20);
            let backing = query.reserve(private.limit()).unwrap();
            let mut producer = Aggregation::new(&p, &e, private.clone(), true).unwrap();
            producer
                .update_with_encounters(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    batch(&p, &rows, &private),
                    &[EncounterToken::new(0, 0, 0)],
                )
                .unwrap();
            let observed = Arc::new(AtomicBool::new(false));
            producer.export_drop_audit = Some(ExportDropAudit {
                query: query.clone(),
                minimum_backing: private.limit(),
                observed: observed.clone(),
            });
            let blocker = if failure == "descriptors" {
                Some(private.reserve(private.limit() - private.used()).unwrap())
            } else {
                producer.encounters_valid = false;
                None
            };
            let error = producer
                .into_raw_with_encounters(RawFunding::PrepaidPrivate(backing))
                .err()
                .unwrap();
            assert!(
                observed.load(Ordering::Acquire),
                "private destructor was observed"
            );
            assert!(error.contains(if failure == "descriptors" {
                "resource-limit: accounted memory"
            } else {
                "encounter tokens"
            }));
            assert_eq!(query.used(), 0);
            assert_eq!(
                private.used(),
                blocker.as_ref().map_or(0, Reservation::bytes)
            );
            drop(blocker);
            assert_eq!(private.used(), 0);
        }
    }
    #[test]
    fn recorded_token_export_rejects_untracked_updates_and_releases_credits() {
        let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
        let budget = Budget::new(4 << 20);
        let rows = [(Some(1), None, None)];
        let ordinary = state(&p, &e, &rows, &budget);
        assert!(
            ordinary
                .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
                .is_err()
        );
        assert_eq!(budget.used(), 0);
        let mut producer = Aggregation::new(&p, &e, budget.clone(), true).unwrap();
        assert!(
            producer
                .update_with_encounters(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    batch(&p, &rows, &budget),
                    &[]
                )
                .is_err()
        );
        producer
            .update_with_encounters(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, &rows, &budget),
                &[EncounterToken::new(1, 2, 3)],
            )
            .unwrap();
        producer
            .update(&p, &e, &mut Metrics::default(), batch(&p, &rows, &budget))
            .unwrap();
        assert!(
            producer
                .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
                .is_err()
        );
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod runtime_tests;
