//! Shared owned state for direct, relational and spill aggregation.
//! Input evaluation and public output scheduling stay in the executor.
//! Future partial merges must use raw wide states, never finalized SUM arrays.
use super::aggregate_workers;
use super::{
    Key, Metrics, Options, Value, count_array, key, keys_array, materialize, output_array,
    physical_output_type,
};
use crate::{
    Result,
    bind::{self, Aggregate, Output, Plan},
    expr::{is_sql_null, numeric},
    memory::{Budget, Reservation},
    types::Type,
};
use arrow::{
    array::*,
    datatypes::{SchemaRef, i256},
    record_batch::RecordBatch,
};
use std::{collections::HashSet, sync::Arc, time::Instant};
mod output;
pub(super) use output::{AggregateOutput, PartitionOutput};
mod ordered_source;
mod preflight;
pub(super) use ordered_source::SourceBatch;

pub(super) struct EvaluatedBatch {
    pub rows: usize,
    pub keys: Vec<Value>,
    pub values: Vec<Option<Value>>,
    pub prepared_keys: Option<crate::keys::Keys>,
}

pub(super) struct States {
    pub(super) integer_sum: bool,
    pub(super) counts: Vec<u64>,
    pub(super) sums: Option<Vec<i256>>,
    pub(super) extremes: Option<Vec<Option<Key>>>,
    #[allow(clippy::type_complexity, clippy::box_collection)]
    pub(super) distinct: Option<Vec<Option<Box<HashSet<Key, ahash::RandomState>>>>>,
}
impl States {
    pub(super) fn update_plain(
        &mut self,
        function: &Aggregate,
        value: Option<&Value>,
        groups: &[usize],
        rows: usize,
        scalar: bool,
        optimized: bool,
    ) -> Result<usize> {
        match function {
            Aggregate::Count => {
                let nulls = value.and_then(|v| v.array.logical_nulls());
                if scalar {
                    let count = rows - nulls.as_ref().map_or(0, |n| n.null_count());
                    self.counts[0] = self.counts[0]
                        .checked_add(count as u64)
                        .ok_or("aggregate count overflow")?;
                } else {
                    for (row, group) in groups.iter().enumerate() {
                        if nulls.as_ref().is_none_or(|n| n.is_valid(row)) {
                            self.counts[*group] = self.counts[*group]
                                .checked_add(1)
                                .ok_or("aggregate count overflow")?;
                        }
                    }
                }
            }
            Aggregate::Sum | Aggregate::Avg => {
                let array = &value.expect("bound aggregate input").array;
                if array.data_type() != &arrow::datatypes::DataType::Null {
                    let numbers = numeric(array);
                    if optimized && self.add_narrow_batch(numbers, groups) {
                        return Ok(1);
                    }
                    if scalar {
                        let sum = &mut self.sums.as_mut().unwrap()[0];
                        for number in numbers.iter().flatten() {
                            *sum = if self.integer_sum {
                                crate::aggregate_sum::add_integer(*sum, number)?
                            } else {
                                crate::aggregate_sum::add(*sum, number)?
                            };
                        }
                        self.counts[0] = self.counts[0]
                            .checked_add((numbers.len() - numbers.null_count()) as u64)
                            .ok_or("aggregate count overflow")?;
                    } else {
                        let sums = self.sums.as_mut().unwrap();
                        for (number, group) in numbers.iter().zip(groups) {
                            if let Some(number) = number {
                                sums[*group] = if self.integer_sum {
                                    crate::aggregate_sum::add_integer(sums[*group], number)?
                                } else {
                                    crate::aggregate_sum::add(sums[*group], number)?
                                };
                                self.counts[*group] = self.counts[*group]
                                    .checked_add(1)
                                    .ok_or("aggregate count overflow")?;
                            }
                        }
                    }
                }
            }
            _ => return Err("internal: unsupported independent aggregate state".into()),
        }
        Ok(0)
    }
    pub(super) fn add_narrow_batch(&mut self, numbers: &Decimal128Array, groups: &[usize]) -> bool {
        // For a small set of states, prove every possible prefix stays inside
        // precision 38 once per batch. Checking magnitudes is conservative even
        // with mixed signs, arbitrary group interleaving and cancellation.
        if self.counts.len() > 64 || numbers.null_count() != 0 {
            return false;
        }
        let Some(growth) = (1_i128 << 63).checked_mul(numbers.len() as i128) else {
            return false;
        };
        const LIMIT: i128 = 10_i128.pow(38) - 1;
        let Some(margin) = LIMIT.checked_sub(growth).filter(|n| *n >= 0) else {
            return false;
        };
        let Some(sums) = &mut self.sums else {
            return false;
        };
        if self
            .counts
            .iter()
            .any(|count| *count > u64::MAX - numbers.len() as u64)
            || sums.iter().any(|sum| {
                sum.to_i128()
                    .is_none_or(|value| value.unsigned_abs() > margin as u128)
            })
            || numbers.values().iter().any(|n| i64::try_from(*n).is_err())
        {
            return false;
        }
        if self.counts.len() == 1 {
            // The bounds above prove every prefix and the final reduction fit.
            // One state needs no per-row indirect group lookup or state write.
            sums[0] += i256::from_i128(numbers.values().iter().sum::<i128>());
            self.counts[0] += numbers.len() as u64;
        } else {
            for (number, group) in numbers.values().iter().zip(groups) {
                sums[*group] += i256::from_i128(*number);
                self.counts[*group] += 1;
            }
        }
        true
    }
    pub(super) fn new(aggregate: &bind::Agg) -> Self {
        Self {
            integer_sum: aggregate.input.as_ref().is_some_and(|e| e.ty == Type::Int),
            counts: Vec::new(),
            sums: matches!(aggregate.function, Aggregate::Sum | Aggregate::Avg).then(Vec::new),
            extremes: matches!(aggregate.function, Aggregate::Min | Aggregate::Max).then(Vec::new),
            distinct: aggregate.distinct.then(Vec::new),
        }
    }
    pub(super) fn bytes_per_group(&self) -> usize {
        std::mem::size_of::<u64>()
            + self
                .sums
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<i256>())
            + self
                .extremes
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<Option<Key>>())
            + self.distinct.as_ref().map_or(0, |_| {
                std::mem::size_of::<Option<Box<HashSet<Key, ahash::RandomState>>>>()
            })
    }
    pub(super) fn push(&mut self) {
        self.counts.push(0);
        if let Some(sums) = &mut self.sums {
            sums.push(i256::ZERO);
        }
        if let Some(extremes) = &mut self.extremes {
            extremes.push(None);
        }
        if let Some(distinct) = &mut self.distinct {
            distinct.push(None);
        }
    }
}
#[cfg(test)]
struct ExportDropAudit {
    query: Budget,
    minimum_backing: usize,
    observed: Arc<std::sync::atomic::AtomicBool>,
}
#[cfg(test)]
impl Drop for ExportDropAudit {
    fn drop(&mut self) {
        // This field drops BEFORE any private state buffer or lease. Observe
        // the real failure destructor, not only the eventual zero balance.
        assert!(
            self.query.used() >= self.minimum_backing,
            "private state outlived its query backing"
        );
        self.observed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
pub(super) struct Aggregation {
    #[cfg(test)]
    export_drop_audit: Option<ExportDropAudit>,
    allow_dense: bool,
    encounters: Vec<EncounterToken>,
    encounters_valid: bool,
    last_ordered_batch: Option<SourceBatch>,
    budget: Budget,
    pub(super) group_count: usize,
    group_keys: Vec<Vec<Key>>,
    numeric_group_keys: Option<Vec<i128>>,
    // Join helpers before dropping their state and the accounting leases.
    aggregate_workers: Option<aggregate_workers::Workers>,
    pub(super) states: Vec<States>,
    state_slack: usize,
    map: crate::group_lookup::EncodedGroups,
    numeric_map: crate::group_index::NumericGroups,
    null_group: Option<usize>,
    state_leases: Vec<Reservation>,
}
/// Immutable ownership boundary for partition-routing jobs. In particular this
/// contains no worker receivers or mutable lookup indexes. Buffers precede both
/// private-budget leases and their real-query backing in destruction order.
pub(super) struct RawPartial {
    group_count: usize,
    group_keys: Vec<Vec<Key>>,
    numeric_group_keys: Option<Vec<i128>>,
    states: Vec<States>,
    null_group: Option<usize>,
    encoded: Vec<Option<Box<[u8]>>>,
    encounters: Vec<EncounterToken>,
    _state_leases: Vec<Reservation>,
    funding: RawFunding,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EncounterToken {
    source: u64,
    batch: u64,
    row: usize,
}
impl EncounterToken {
    pub(super) fn new(source: u64, batch: u64, row: usize) -> Self {
        Self { source, batch, row }
    }
}
pub(super) enum RawFunding {
    SharedQuery(Budget),
    PrepaidPrivate(Reservation),
}
impl RawPartial {
    fn query_budget(&self) -> Budget {
        match &self.funding {
            RawFunding::SharedQuery(query) => query.clone(),
            RawFunding::PrepaidPrivate(backing) => backing.budget(),
        }
    }
    fn view(&self) -> PartialView<'_> {
        PartialView {
            group_keys: &self.group_keys,
            numeric_group_keys: self.numeric_group_keys.as_deref(),
            states: &self.states,
            null_group: self.null_group,
        }
    }
}
#[derive(Clone, Copy)]
struct PartialView<'a> {
    group_keys: &'a [Vec<Key>],
    numeric_group_keys: Option<&'a [i128]>,
    states: &'a [States],
    null_group: Option<usize>,
}
impl Aggregation {
    #[cfg(test)]
    pub(super) fn into_raw(self, funding: RawFunding, sequence: u64) -> Result<RawPartial> {
        self.into_raw_inner(funding, Some(sequence))
    }
    pub(super) fn into_raw_with_encounters(self, funding: RawFunding) -> Result<RawPartial> {
        self.into_raw_inner(funding, None)
    }
    #[allow(dead_code)] // Preserved general API for non-monotonic source owners.
    pub(super) fn update_with_encounters(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
        tokens: &[EncounterToken],
    ) -> Result<()> {
        if p.groups.is_empty() || tokens.len() != batch.rows || !self.encounters_valid {
            return Err("tokenized aggregation requires complete source row tokens".into());
        }
        self.update_tracked::<false>(p, expressions, metrics, batch, Some(tokens), None)
    }
    #[allow(clippy::too_many_arguments)]
    fn update_tracked<const ORDERED: bool>(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
        tokens: Option<&[EncounterToken]>,
        source: Option<SourceBatch>,
    ) -> Result<()> {
        // Reserve new vector's worst capacity before growth. Old token storage
        // remains separately funded until reallocation frees it.
        let upper = self
            .group_count
            .checked_add(batch.rows)
            .and_then(|n| n.checked_mul(std::mem::size_of::<EncounterToken>() * 2))
            .and_then(|n| n.checked_add(256))
            .ok_or("encounter capacity overflow")?;
        let mut credit = self.budget.reserve(upper)?;
        let old_capacity = self.encounters.capacity();
        // An arbitrary-token update may precede any source prefix; ordered
        // callers cannot subsequently assume their existing groups came first.
        if !ORDERED {
            self.last_ordered_batch = None;
        }
        let result =
            self.update_inner::<true, ORDERED>(p, expressions, metrics, batch, tokens, source);
        // Even a failed update can have grown the token vector. Retain that
        // storage's credit until the failed state is dropped; do not publish it.
        let added =
            (self.encounters.capacity() - old_capacity) * std::mem::size_of::<EncounterToken>();
        if added > upper {
            return Err("encounter capacity exceeds prepaid storage".into());
        }
        credit.release_excess(added);
        if added > 0 {
            self.state_leases.push(credit);
        }
        result?;
        self.encounters_valid = true;
        Ok(())
    }
    fn into_raw_inner(
        self,
        funding: RawFunding,
        batch_sequence: Option<u64>,
    ) -> Result<RawPartial> {
        // Function arguments would drop funding before self on an early Err.
        // Keep the private buffers FIRST and their real-query backing LAST
        // through every validation/allocation; success transfers that order to
        // RawPartial. No fallible work follows the final destructuring.
        struct Export {
            state: Aggregation,
            funding: RawFunding,
        }
        let mut export = Export {
            state: self,
            funding,
        };
        let state = &mut export.state;
        if batch_sequence.is_none()
            && (!state.encounters_valid || state.encounters.len() != state.group_count)
        {
            return Err("raw partial lacks complete original encounter tokens".into());
        }
        match &export.funding {
            RawFunding::SharedQuery(query) if !state.budget.same_domain(query) => {
                return Err("raw partial has unbacked private-budget state".into());
            }
            RawFunding::PrepaidPrivate(backing)
                if backing.bytes() < state.budget.limit()
                    || backing.budget().same_domain(&state.budget) =>
            {
                return Err(
                    "raw private budget requires separate full-budget query backing".into(),
                );
            }
            _ => (),
        }
        state.finish_updates()?;
        let count = if state.numeric_group_keys.is_some() {
            0
        } else {
            state.group_count
        };
        // Fund encounter-indexed descriptors while the source hash table is
        // still live. Move its owned key boxes, without copying their bytes.
        let lease = state.budget.reserve(
            state
                .group_count
                .checked_mul(
                    std::mem::size_of::<Option<Box<[u8]>>>()
                        + std::mem::size_of::<EncounterToken>(),
                )
                .and_then(|bytes| bytes.checked_add(512))
                .ok_or("raw partial descriptors overflow")?,
        )?;
        let mut encoded: Vec<Option<Box<[u8]>>> = (0..count).map(|_| None).collect();
        for (key, group) in std::mem::take(&mut state.map).into_entries() {
            encoded[group] = Some(key);
        }
        state.state_leases.push(lease);
        let encounters = if let Some(source) = batch_sequence {
            (0..state.group_count)
                .map(|row| EncounterToken::new(source, 0, row))
                .collect()
        } else {
            std::mem::take(&mut state.encounters)
        };
        let Export { state, funding } = export;
        Ok(RawPartial {
            group_count: state.group_count,
            group_keys: state.group_keys,
            numeric_group_keys: state.numeric_group_keys,
            states: state.states,
            null_group: state.null_group,
            encoded,
            encounters,
            _state_leases: state.state_leases,
            funding,
        })
    }
    pub(super) fn new(
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        budget: Budget,
        optimizations: bool,
    ) -> Result<Self> {
        let mut state = Self {
            #[cfg(test)]
            export_drop_audit: None,
            allow_dense: true,
            encounters: Vec::new(),
            encounters_valid: true,
            last_ordered_batch: None,
            budget: budget.clone(),
            group_count: 0,
            group_keys: p.groups.iter().map(|_| Vec::new()).collect(),
            numeric_group_keys: None,
            aggregate_workers: None,
            states: p.aggregates.iter().map(States::new).collect(),
            state_slack: 0,
            map: crate::group_lookup::EncodedGroups::default(),
            numeric_map: crate::group_index::NumericGroups::default(),
            null_group: None,
            state_leases: Vec::new(),
        };
        if optimizations
            && p.groups.len() == 1
            && matches!(p.groups[0].ty, Type::Int | Type::Decimal(_))
        {
            // Fixed headers, the minimum small hash table and vector capacities
            // must be covered even when chunked state reservation falls back to
            // charging just one group under a very small budget.
            state.state_leases.push(budget.reserve(2048)?);
        }
        if p.aggregation && p.groups.is_empty() {
            // Scalar vectors also grow to minimum capacity on their first push.
            // Wide totals require 32 bytes per cell, in addition to counts and
            // other state. Reserve canonical buffers plus existing metadata before
            // constructing any state; aliases retain empty vectors.
            let state_bytes = state
                .states
                .iter()
                .enumerate()
                .filter(|(index, _)| expressions.state_sources[*index] == *index)
                .try_fold(256 + p.aggregates.len() * 128, |bytes, (_, state)| {
                    state
                        .bytes_per_group()
                        .checked_mul(4)
                        .and_then(|state| bytes.checked_add(state))
                        .ok_or("scalar aggregate state size overflow")
                })?;
            state.state_leases.push(budget.reserve(state_bytes)?);
            state.group_count = 1;
            for (index, states) in state.states.iter_mut().enumerate() {
                if expressions.state_sources[index] == index {
                    states.push();
                }
            }
        }
        Ok(state)
    }
    pub(super) fn disable_dense(&mut self) {
        self.allow_dense = false;
    }
    pub(super) fn add_merge_credit(&mut self, bytes: usize) {
        self.state_slack += bytes;
    }
    pub(super) fn retain_merge_credit(&mut self, mut lease: Reservation, bytes: usize) {
        let unused = bytes.min(self.state_slack);
        self.state_slack -= unused;
        lease.release_excess(bytes - unused);
        if unused != bytes {
            self.state_leases.push(lease);
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn locate_partial_group(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        source: PartialView<'_>,
        local: usize,
        encoded: Option<&[u8]>,
        state_bytes: usize,
        extra_per_group: usize,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<usize> {
        cancellation.check()?;
        let numeric = source.numeric_group_keys.as_ref().map(|keys| keys[local]);
        let is_null = source.null_group == Some(local);
        let found = if p.groups.is_empty() {
            Some(0)
        } else if let Some(n) = numeric {
            if is_null {
                self.null_group
            } else {
                self.numeric_map.get(n)
            }
        } else {
            self.map.get(encoded.expect("encoded group key"))
        };
        let group = if let Some(id) = found {
            id
        } else {
            let bytes = if numeric.is_some() {
                128 + 4 * (16 + state_bytes) + extra_per_group
            } else {
                512 + encoded.unwrap().len() * 6
                    + 4 * (p.groups.len() * std::mem::size_of::<Key>() + state_bytes)
                    + extra_per_group
            };
            // All destination growth is funded before any worker is dispatched.
            self.state_slack = self
                .state_slack
                .checked_sub(bytes)
                .ok_or("internal: insufficient merge credit")?;
            let id = self.group_count;
            self.group_count += 1;
            if let Some(n) = numeric {
                self.numeric_group_keys.get_or_insert_with(Vec::new).push(n);
                if is_null {
                    self.null_group = Some(id);
                } else {
                    self.numeric_map.insert(n, id);
                }
            } else {
                self.map.insert(encoded.unwrap().into(), id);
                for (output, input) in self.group_keys.iter_mut().zip(source.group_keys) {
                    output.push(input[local].clone());
                }
            }
            for (i, state) in self.states.iter_mut().enumerate() {
                if expressions.state_sources[i] == i {
                    state.push();
                }
            }
            id
        };
        Ok(group)
    }
    #[cfg(test)]
    fn partial_view(&self) -> PartialView<'_> {
        PartialView {
            group_keys: &self.group_keys,
            numeric_group_keys: self.numeric_group_keys.as_deref(),
            states: &self.states,
            null_group: self.null_group,
        }
    }
    fn merge_partial_state(
        &mut self,
        source: PartialView<'_>,
        local: usize,
        group: usize,
        index: usize,
    ) -> Result<()> {
        let dest = &mut self.states[index];
        let src = &source.states[index];
        dest.counts[group] = dest.counts[group]
            .checked_add(src.counts[local])
            .ok_or("aggregate count overflow")?;
        if let Some(sums) = &mut dest.sums {
            sums[group] = sums[group]
                .checked_add(src.sums.as_ref().unwrap()[local])
                .ok_or("aggregate accumulator overflow")?;
            if dest.integer_sum {
                crate::aggregate_sum::finish(sums[group])?;
            }
        }
        Ok(())
    }
    pub(super) fn start_workers(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        options: &Options,
    ) -> Result<()> {
        self.aggregate_workers = aggregate_workers::Workers::new(
            &mut self.states,
            &p.aggregates,
            &expressions.state_sources,
            p.groups.is_empty(),
            options,
            &self.budget,
        )?;
        Ok(())
    }
    pub(super) fn finish_updates(&mut self) -> Result<()> {
        if let Some(workers) = self.aggregate_workers.take() {
            workers.finish(&mut self.states)?;
        }
        Ok(())
    }
    pub(super) fn update(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
    ) -> Result<()> {
        self.update_inner::<false, false>(p, expressions, metrics, batch, None, None)
    }
    #[allow(clippy::too_many_arguments)]
    fn update_inner<const TRACK_ENCOUNTERS: bool, const ORDERED: bool>(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
        tokens: Option<&[EncounterToken]>,
        source: Option<SourceBatch>,
    ) -> Result<()> {
        // An untracked update permanently invalidates an otherwise tracked
        // partial too. This is one store per batch, not a per-row branch.
        self.encounters_valid = false;
        let EvaluatedBatch {
            rows,
            keys,
            values,
            prepared_keys,
        } = batch;
        let start = Instant::now();
        // Store a single numeric key directly in the table. Keep NULL separate
        // so every numeric bit pattern remains available, without a sentinel.
        let numeric_keys = if expressions.enabled && keys.len() == 1 {
            keys[0].array.as_any().downcast_ref::<Decimal128Array>()
        } else {
            None
        };
        let allow_dense = self.allow_dense;
        if numeric_keys.is_some() && allow_dense {
            metrics.dense_group_rebuilds += usize::from(self.numeric_map.prepare(&self.budget));
            let bytes = self.numeric_map.dense_bytes();
            metrics.dense_group_batches += usize::from(bytes > 0);
            metrics.dense_group_index_peak_bytes = metrics.dense_group_index_peak_bytes.max(bytes);
        }
        let encoded = if prepared_keys.is_some() {
            prepared_keys
        } else if numeric_keys.is_none() {
            Some(crate::keys::Keys::with_text_encoding(
                &keys,
                rows,
                &self.budget,
                expressions.enabled,
            )?)
        } else {
            None
        };
        metrics.expression_seconds += start.elapsed().as_secs_f64();
        let start = Instant::now();
        // For one numeric aggregate, update the state while its group is hot
        // in cache. Multiple aggregates share a group-index batch instead.
        let single_numeric = if p.aggregates.len() == 1
            && !p.groups.is_empty()
            && !p.aggregates[0].distinct
            && matches!(p.aggregates[0].function, Aggregate::Sum | Aggregate::Avg)
        {
            values[0]
                .as_ref()
                .and_then(|v| v.array.as_any().downcast_ref::<Decimal128Array>())
        } else {
            None
        };
        let index_len = if single_numeric.is_some() { 0 } else { rows };
        let _group_ids_lease = self.budget.reserve(
            index_len
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or("group index size overflow")?,
        )?;
        let mut group_ids = Vec::with_capacity(index_len);
        let state_bytes: usize = self
            .states
            .iter()
            .enumerate()
            .filter(|(i, _)| expressions.state_sources[*i] == *i)
            .map(|(_, states)| states.bytes_per_group())
            .sum();
        let mut short_cache = crate::group_lookup::ShortCache::new(
            expressions.enabled
                && numeric_keys.is_none()
                && !p.groups.is_empty()
                && rows >= 64
                && self.map.len() <= 64
                && encoded.as_ref().is_some_and(|keys| keys.row(0).len() <= 16),
            &mut self.state_slack,
        );
        let group_result: Result<()> = (|| {
            for row in 0..rows {
                let group = if p.groups.is_empty() {
                    0
                } else {
                    let bytes = encoded.as_ref().map(|keys| keys.row(row));
                    let existing = if let Some(keys) = numeric_keys {
                        if keys.is_null(row) {
                            self.null_group
                        } else {
                            self.numeric_map.get(keys.value(row))
                        }
                    } else {
                        short_cache.get(&self.map, bytes.unwrap())
                    };
                    if let Some(i) = existing {
                        i
                    } else {
                        // Numeric groups use an inline i128 index and one compact
                        // i128 output column, with a separate NULL group. They own
                        // neither boxed encoded keys nor a Vec<Key> output column.
                        // 128 index bytes/group covers buckets, controls and a
                        // simultaneous rehash; fourfold vector allowances cover
                        // old/new capacities during growth. Dense slots reserve
                        // their own additional allocation in NumericGroups.
                        let allocation = if numeric_keys.is_some() {
                            128 + 4 * (std::mem::size_of::<i128>() + state_bytes)
                        } else {
                            // Encoded bytes also cover decoded string payloads.
                            512 + bytes.unwrap().len() * 6
                                + 4 * (p.groups.len() * std::mem::size_of::<Key>() + state_bytes)
                        };
                        // Amortize reservation bookkeeping in bounded chunks rather
                        // than allocating one mutex-backed lease per group.
                        if self.state_slack < allocation {
                            // Return borrowed cache credit before deciding whether
                            // this group needs a new reservation. No state changed.
                            short_cache.release(&mut self.state_slack);
                        }
                        if self.state_slack < allocation {
                            let chunk = allocation.max(64 * 1024);
                            let (lease, reserved) = match self.budget.reserve(chunk) {
                                Ok(lease) => (lease, chunk),
                                Err(_) => (self.budget.reserve(allocation)?, allocation),
                            };
                            self.state_leases.push(lease);
                            self.state_slack += reserved;
                        }
                        self.state_slack -= allocation;
                        let i = self.group_count;
                        self.group_count += 1;
                        if TRACK_ENCOUNTERS && ORDERED {
                            // Stable filtering preserves encounter order. Existing
                            // groups already have an earlier source coordinate.
                            self.encounters
                                .push(source.expect("ordered source prefix").token(row));
                        }
                        if let Some(keys) = numeric_keys {
                            self.numeric_group_keys.get_or_insert_with(Vec::new).push(
                                if keys.is_null(row) {
                                    0
                                } else {
                                    keys.value(row)
                                },
                            );
                        } else {
                            for ((column, value), expr) in
                                self.group_keys.iter_mut().zip(&keys).zip(&p.groups)
                            {
                                column.push(key(value, &expr.ty, row));
                            }
                        }
                        for (index, states) in self.states.iter_mut().enumerate() {
                            if expressions.state_sources[index] == index
                                && self
                                    .aggregate_workers
                                    .as_ref()
                                    .is_none_or(|workers| !workers.owns(index))
                            {
                                states.push();
                            }
                        }
                        if let Some(keys) = numeric_keys {
                            if keys.is_null(row) {
                                self.null_group = Some(i);
                            } else {
                                self.numeric_map.insert(keys.value(row), i);
                            }
                        } else {
                            self.map.insert(bytes.unwrap().into(), i);
                        }
                        i
                    }
                };
                // Separate monomorphizations keep encounter tracking out of
                // the ordinary aggregation loop, including single-core queries.
                if TRACK_ENCOUNTERS && !ORDERED {
                    let tokens = tokens.expect("tracked aggregation has source tokens");
                    self.encounters.resize(
                        self.group_count,
                        EncounterToken::new(u64::MAX, u64::MAX, usize::MAX),
                    );
                    self.encounters[group] = self.encounters[group].min(tokens[row]);
                }
                if let Some(numbers) = single_numeric {
                    if numbers.is_valid(row) {
                        let state = &mut self.states[0];
                        let sum = &mut state.sums.as_mut().unwrap()[group];
                        *sum = if state.integer_sum {
                            crate::aggregate_sum::add_integer(*sum, numbers.value(row))?
                        } else {
                            crate::aggregate_sum::add(*sum, numbers.value(row))?
                        };
                        state.counts[group] = state.counts[group]
                            .checked_add(1)
                            .ok_or("aggregate count overflow")?;
                    }
                } else {
                    group_ids.push(group);
                }
            }
            Ok(())
        })();
        short_cache.release(&mut self.state_slack);
        metrics.short_group_cache_batches += usize::from(short_cache.bytes > 0);
        metrics.short_group_cache_hits += short_cache.hits;
        metrics.short_group_cache_misses += short_cache.misses;
        metrics.short_group_cache_bypasses += short_cache.bypasses;
        metrics.short_group_cache_peak_bytes =
            metrics.short_group_cache_peak_bytes.max(short_cache.bytes);
        group_result?;
        if single_numeric.is_some() {
            metrics.aggregate_seconds += start.elapsed().as_secs_f64();
            return Ok(());
        }
        if let Some(workers) = &mut self.aggregate_workers {
            let (bounded, seconds) = workers.update(
                &mut self.states,
                &p.aggregates,
                &expressions.state_sources,
                aggregate_workers::Batch {
                    values,
                    groups: group_ids,
                    rows,
                    group_count: self.group_count,
                    _group_lease: _group_ids_lease,
                },
            )?;
            metrics.bounded_sum_batches += bounded;
            metrics.parallel_aggregate_batches += 1;
            metrics.parallel_aggregate_worker_seconds += seconds;
            metrics.aggregate_seconds += start.elapsed().as_secs_f64();
            return Ok(());
        }
        for (index, (agg, value)) in p.aggregates.iter().zip(&values).enumerate() {
            if expressions.state_sources[index] != index {
                continue;
            }
            if !agg.distinct
                && matches!(
                    agg.function,
                    Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                )
            {
                metrics.bounded_sum_batches += self.states[index].update_plain(
                    &agg.function,
                    value.as_ref(),
                    &group_ids,
                    rows,
                    p.groups.is_empty(),
                    expressions.enabled,
                )?;
                continue;
            }
            // DISTINCT and MIN/MAX retain the general typed-key path.
            let _binary_scratch = value
                .as_ref()
                .map(|value| super::binary_key_scratch(value, &self.budget))
                .transpose()?;
            for (row, group) in group_ids.iter().enumerate() {
                let state = &mut self.states[index];
                if value
                    .as_ref()
                    .is_some_and(|v| is_sql_null(v.array.as_ref(), row))
                {
                    continue;
                }
                let k = || {
                    key(
                        value.as_ref().unwrap(),
                        &agg.input.as_ref().unwrap().ty,
                        row,
                    )
                };
                if agg.distinct {
                    if value.is_none() {
                        return Err("unsupported: DISTINCT wildcard".into());
                    }
                    let k = k();
                    let distinct = &mut state.distinct.as_mut().unwrap()[*group];
                    if distinct.as_ref().is_some_and(|set| set.contains(&k)) {
                        continue;
                    }
                    let bytes = 128 + k.payload_bytes();
                    self.state_leases.push(self.budget.reserve(bytes)?);
                    distinct.get_or_insert_with(Default::default).insert(k);
                }
                state.counts[*group] = state.counts[*group]
                    .checked_add(1)
                    .ok_or("aggregate count overflow")?;
                match agg.function {
                    Aggregate::Count => {}
                    Aggregate::Min | Aggregate::Max => {
                        let k = k();
                        let extreme = &mut state.extremes.as_mut().unwrap()[*group];
                        let replace = extreme.as_ref().is_none_or(|old| {
                            let cmp = if let (Key::Float(a), Key::Float(b)) = (&k, old) {
                                f64::from_bits(*a).total_cmp(&f64::from_bits(*b))
                            } else {
                                k.cmp(old)
                            };
                            if matches!(agg.function, Aggregate::Min) {
                                cmp.is_lt()
                            } else {
                                cmp.is_gt()
                            }
                        });
                        if replace {
                            *extreme = Some(k);
                        }
                    }
                    _ => {
                        let v = numeric(&value.as_ref().unwrap().array).value(row);
                        let sum = &mut state.sums.as_mut().unwrap()[*group];
                        *sum = if state.integer_sum {
                            crate::aggregate_sum::add_integer(*sum, v)?
                        } else {
                            crate::aggregate_sum::add(*sum, v)?
                        };
                    }
                }
            }
        }
        metrics.aggregate_seconds += start.elapsed().as_secs_f64();
        Ok(())
    }
    pub(super) fn output_batch(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        output_schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        let mut arrays = Vec::new();
        for (o, t) in p.output.iter().zip(&p.types) {
            let physical_ty =
                physical_output_type(t, Type::is_wide_integer(output_schema.field(arrays.len())));
            let t = &physical_ty;
            let array: ArrayRef = match o {
                Output::Group(i) => {
                    if let Some(keys) = &self.numeric_group_keys {
                        let mut builder = Decimal128Builder::with_capacity(range.len());
                        for (offset, value) in keys[range.clone()].iter().enumerate() {
                            if self.null_group == Some(range.start + offset) {
                                builder.append_null();
                            } else {
                                builder.append_value(*value);
                            }
                        }
                        let values: ArrayRef = Arc::new(
                            builder
                                .finish()
                                .with_precision_and_scale(38, t.scale()?)
                                .map_err(|e| e.to_string())?,
                        );
                        materialize(&values, t)?
                    } else {
                        keys_array(&self.group_keys[*i][range.clone()], t)?
                    }
                }
                Output::Aggregate(_) if *t == Type::Null => {
                    arrow::array::new_null_array(&t.arrow(), range.len())
                }
                Output::Aggregate(i) => {
                    let a = &p.aggregates[*i];
                    let states = &self.states[expressions.state_sources[*i]];
                    match a.function {
                        Aggregate::Count => {
                            count_array(states.counts[range.clone()].iter().copied(), t)?
                        }
                        Aggregate::Sum => {
                            // Finalize the complete column before materializing
                            // BIGINT or moving to the next output column. Direct
                            // Arrow append removes only the Option<i128> staging
                            // buffer; wide range/error chronology stays unchanged.
                            let mut builder = Decimal128Builder::with_capacity(range.len());
                            for (count, sum) in states.counts[range.clone()]
                                .iter()
                                .zip(&states.sums.as_ref().unwrap()[range.clone()])
                            {
                                if *count == 0 {
                                    builder.append_null();
                                } else {
                                    builder.append_value(crate::aggregate_sum::finish(*sum)?);
                                }
                            }
                            let array: ArrayRef = Arc::new(
                                builder
                                    .finish()
                                    .with_precision_and_scale(38, t.scale()?)
                                    .map_err(|e| e.to_string())?,
                            );
                            materialize(&array, t)?
                        }
                        Aggregate::Min | Aggregate::Max => keys_array(
                            &states.extremes.as_ref().unwrap()[range.clone()]
                                .iter()
                                .map(|extreme| extreme.clone().unwrap_or(Key::Null))
                                .collect::<Vec<_>>(),
                            t,
                        )?,
                        Aggregate::Avg => {
                            let scale = a.input.as_ref().unwrap().ty.scale()?;
                            Arc::new(Float64Array::from(
                                states.counts[range.clone()]
                                    .iter()
                                    .zip(&states.sums.as_ref().unwrap()[range.clone()])
                                    .map(|(count, sum)| {
                                        if *count == 0 {
                                            Ok(None)
                                        } else {
                                            crate::numeric_float::average(*sum, scale, *count)
                                                .map(Some)
                                        }
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            ))
                        }
                    }
                }
                Output::Expr(_) => unreachable!(),
            };
            arrays.push(output_array(array, output_schema.field(arrays.len()))?);
        }
        RecordBatch::try_new(output_schema.clone(), arrays).map_err(|e| e.to_string())
    }
    pub(super) fn output_bytes(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        let mut strings = 0usize;
        for output in &p.output {
            let keys = match output {
                Output::Group(i) if self.numeric_group_keys.is_none() => {
                    Some(&self.group_keys[*i][range.clone()])
                }
                _ => None,
            };
            if let Some(keys) = keys {
                for key in keys {
                    strings = strings
                        .checked_add(key.payload_bytes())
                        .ok_or("output size overflow")?;
                }
            }
            if let Output::Aggregate(i) = output {
                let state = &self.states[expressions.state_sources[*i]];
                if let Some(values) = &state.extremes {
                    for key in values[range.clone()].iter().flatten() {
                        strings = strings
                            .checked_add(key.payload_bytes())
                            .ok_or("output size overflow")?;
                    }
                }
            }
        }
        range
            .len()
            .checked_mul((p.output.len() + 1) * 128)
            .and_then(|bytes| strings.checked_mul(4).and_then(|s| bytes.checked_add(s)))
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| "output size overflow".into())
    }
}

mod partitioned;
pub(super) mod source_owned;

/// Forced-partition test oracle; production ordinary state is concrete and
/// production partitions use a separate statically dispatched output owner.
#[cfg(test)]
#[allow(clippy::large_enum_variant)] // Avoid adding a heap allocation to ordinary aggregation.
pub(super) enum Backend {
    Single(Aggregation),
    Partitioned {
        state: partitioned::owned::OwnedPartitions,
        #[cfg(test)]
        budget: Budget,
        cancellation: crate::spill::Cancellation,
    },
}
#[cfg(test)]
impl Backend {
    pub(super) fn from_owned_partitions(
        state: OwnedPartitions,
        _budget: Budget,
        cancellation: crate::spill::Cancellation,
    ) -> Self {
        Self::Partitioned {
            state,
            #[cfg(test)]
            budget: _budget,
            cancellation,
        }
    }

    pub(super) fn new(
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        budget: Budget,
        optimizations: bool,
    ) -> Result<Self> {
        Ok(Self::Single(Aggregation::new(
            p,
            expressions,
            budget,
            optimizations,
        )?))
    }
    #[cfg(test)]
    pub(super) fn force_partitioned(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        budget: Budget,
        options: &Options,
    ) -> Result<()> {
        let Some(count) = options.test_aggregate_partitions else {
            return Ok(());
        };
        if !p.aggregation
            || p.groups.is_empty()
            || options.spill.is_some()
            || !options.relational_optimizations
            || p.aggregates.iter().any(|a| {
                a.distinct
                    || !matches!(
                        a.function,
                        Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                    )
                    || (matches!(a.function, Aggregate::Sum | Aggregate::Avg)
                        && !a.input.as_ref().is_some_and(|e| {
                            matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Null)
                        }))
            })
        {
            return Ok(());
        }
        *self = Self::Partitioned {
            state: partitioned::owned::OwnedPartitions::new(p, expressions, budget.clone(), count)?,
            budget,
            cancellation: options.cancellation.clone(),
        };
        Ok(())
    }
    pub(super) fn group_count(&self) -> usize {
        match self {
            Self::Single(state) => state.group_count,
            Self::Partitioned { state, .. } => state.group_count(),
        }
    }
    pub(super) fn start_workers(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        options: &Options,
    ) -> Result<()> {
        match self {
            Self::Single(state) => state.start_workers(p, expressions, options),
            Self::Partitioned { .. } => Ok(()),
        }
    }
    pub(super) fn update(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
    ) -> Result<()> {
        match self {
            Self::Single(state) => state.update(p, expressions, metrics, batch),
            #[cfg(not(test))]
            Self::Partitioned { .. } => {
                Err("partitioned aggregation requires producer-owned partials".into())
            }
            #[cfg(test)]
            Self::Partitioned {
                state,
                budget,
                cancellation,
            } => {
                cancellation.check()?;
                let mut partial = Aggregation::new(p, expressions, budget.clone(), true)?;
                partial.disable_dense();
                partial.update(p, expressions, metrics, batch)?;
                partial.finish_updates()?;
                let raw = Arc::new(partial.into_raw(
                    RawFunding::SharedQuery(budget.clone()),
                    state.next_sequence(),
                )?);
                let jobs = state.submit(p, expressions, raw, cancellation)?;
                for job in jobs {
                    let completion = job.run(p, expressions, cancellation);
                    state.acknowledge(completion)?;
                }
                Ok(())
            }
        }
    }
    pub(super) fn finish_updates(&mut self) -> Result<()> {
        match self {
            Self::Single(state) => state.finish_updates(),
            Self::Partitioned {
                state,
                cancellation,
                ..
            } => state.finish(cancellation),
        }
    }
    pub(super) fn output_batch(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        match self {
            Self::Single(state) => state.output_batch(p, expressions, schema, range),
            Self::Partitioned { state, .. } => state.output_batch(p, expressions, schema, range),
        }
    }
    pub(super) fn output_bytes(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        match self {
            Self::Single(state) => state.output_bytes(p, expressions, range),
            Self::Partitioned { state, .. } => state.output_bytes(p, range),
        }
    }
}

#[cfg(test)]
pub(super) use partitioned::owned::OwnedJob;
pub(super) use partitioned::owned::{OwnedCompletion, OwnedPartitions, RoutedPartial, RoutingPlan};

#[cfg(test)]
mod output_builder_tests;
