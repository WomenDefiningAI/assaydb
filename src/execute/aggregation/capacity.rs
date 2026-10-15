//! Capacity funding for numeric keys and their canonical aggregate vectors.
use super::*;

fn target(capacity: usize, groups: usize) -> Result<usize> {
    if groups <= capacity {
        return Ok(capacity);
    }
    groups
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(|| "numeric vector capacity overflow".into())
}

fn growth<T>(values: &Vec<T>, groups: usize) -> Result<usize> {
    if groups <= values.capacity() {
        return Ok(0);
    }
    target(values.capacity(), groups)?
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| "numeric vector capacity overflow".into())
}

fn grow<T>(
    values: &mut Vec<T>,
    groups: usize,
    retained: &mut Reservation,
    credit: &mut Reservation,
) -> Result<()> {
    let bytes = growth(values, groups)?;
    if bytes == 0 {
        return Ok(());
    }
    let old = values.capacity() * std::mem::size_of::<T>();
    let old_allocation = retained.split_off(old)?;
    let allocation = match credit.split_off(bytes) {
        Ok(allocation) => allocation,
        Err(error) => {
            retained.absorb(old_allocation);
            return Err(error);
        }
    };
    let mut next = Vec::with_capacity(target(values.capacity(), groups)?);
    // Vec with the global allocator requests exactly the specified capacity.
    assert_eq!(next.capacity() * std::mem::size_of::<T>(), bytes);
    next.append(values);
    *values = next; // Old allocation is freed before its credit is reused.
    credit.absorb(old_allocation);
    retained.absorb(allocation);
    Ok(())
}

// Rebuilding a tombstoned table can return more credit than was supplied.
fn merge_slack(slack: usize, before: usize, after: usize) -> Result<usize> {
    if after <= before {
        slack
            .checked_sub(before - after)
            .ok_or_else(|| "internal: insufficient merge credit".into())
    } else {
        slack
            .checked_add(after - before)
            .ok_or_else(|| "internal: merge credit overflow".into())
    }
}

impl Aggregation {
    pub(in crate::execute) fn enable_numeric_slack(&mut self) -> Result<()> {
        // Bounded producers reuse a refill chunk within their prepaid slice.
        // Its preflight already includes up to 64 KiB of unused growth credit.
        if self.numeric_slack.is_none() {
            self.numeric_slack = Some(self.budget.reserve(0)?);
        }
        Ok(())
    }

    /// Sum of replacement allocations, not net growth: old storage remains
    /// funded throughout copying. Freed capacities return to prepaid credit.
    pub(super) fn numeric_headroom(
        &self,
        sources: impl IntoIterator<Item = usize>,
        groups: usize,
        hash_entries: usize,
    ) -> Result<usize> {
        let mut bytes = self.numeric_map.growth_bytes(hash_entries)?;
        let empty = Vec::<i128>::new();
        let keys = self.numeric_group_keys.as_ref().unwrap_or(&empty);
        bytes = bytes
            .checked_add(growth(keys, groups)?)
            .ok_or("numeric capacity overflow")?;
        for (i, (state, source)) in self.states.iter().zip(sources).enumerate() {
            if source != i {
                continue;
            }
            let mut add = |n: usize| -> Result<()> {
                bytes = bytes.checked_add(n).ok_or("numeric capacity overflow")?;
                Ok(())
            };
            add(growth(&state.counts, groups)?)?;
            if let Some(values) = &state.sums {
                add(growth(values, groups)?)?;
            }
            if let Some(values) = &state.extremes {
                add(growth(values, groups)?)?;
            }
            if let Some(values) = &state.distinct {
                add(growth(values, groups)?)?;
            }
        }
        Ok(bytes)
    }

    pub(super) fn numeric_merge_headroom(
        &self,
        expressions: &crate::expr_program::ExpressionProgram,
        additional: usize,
    ) -> Result<usize> {
        self.numeric_merge_headroom_sources(expressions.state_sources.iter().copied(), additional)
    }

    pub(super) fn numeric_merge_headroom_sources(
        &self,
        sources: impl IntoIterator<Item = usize>,
        additional: usize,
    ) -> Result<usize> {
        let replacements = self.numeric_headroom(
            sources,
            self.group_count
                .checked_add(additional)
                .ok_or("numeric group count overflow")?,
            self.numeric_map
                .hash_len()
                .checked_add(additional)
                .ok_or("numeric hash capacity overflow")?,
        )?;
        // A batch may cross several doublings. Besides the final capacity,
        // fund the previous intermediate capacity while copying into it.
        replacements
            .checked_mul(2)
            .ok_or_else(|| "numeric capacity overflow".into())
    }

    pub(super) fn grow_numeric(
        &mut self,
        expressions: &crate::expr_program::ExpressionProgram,
        key: Option<i128>,
    ) -> Result<()> {
        let groups = self
            .group_count
            .checked_add(1)
            .ok_or("group count overflow")?;
        let hash_insert = key.is_some_and(|key| self.numeric_map.needs_hash(key, self.group_count));
        let hash_entries = self.numeric_map.hash_len() + usize::from(hash_insert);
        let bytes = self.numeric_headroom(
            expressions.state_sources.iter().copied(),
            groups,
            hash_entries,
        )?;
        if bytes == 0 {
            return Ok(());
        }
        let merging = self.merge_credit.is_some();
        let mut credit = if merging {
            self.merge_credit.take().unwrap()
        } else if let Some(slack) = &mut self.numeric_slack {
            if slack.bytes() < bytes {
                let missing = bytes - slack.bytes();
                let refill = self
                    .budget
                    .reserve(missing.max(64 * 1024))
                    .or_else(|_| self.budget.reserve(missing))?;
                slack.absorb(refill);
            }
            slack.split_off(slack.bytes())?
        } else {
            self.budget.reserve(bytes)?
        };
        let before = credit.bytes();
        let result = (|| {
            let retained = &mut self.numeric_lease;
            grow(
                self.numeric_group_keys.get_or_insert_with(Vec::new),
                groups,
                retained,
                &mut credit,
            )?;
            for (i, state) in self.states.iter_mut().enumerate() {
                if expressions.state_sources[i] != i {
                    continue;
                }
                grow(&mut state.counts, groups, retained, &mut credit)?;
                if let Some(values) = &mut state.sums {
                    grow(values, groups, retained, &mut credit)?;
                }
                if let Some(values) = &mut state.extremes {
                    grow(values, groups, retained, &mut credit)?;
                }
                if let Some(values) = &mut state.distinct {
                    grow(values, groups, retained, &mut credit)?;
                }
            }
            self.numeric_map.grow(hash_entries, retained, &mut credit)
        })();
        if merging {
            let slack = merge_slack(self.state_slack, before, credit.bytes());
            self.merge_credit = Some(credit);
            self.state_slack = slack?;
        } else if let Some(slack) = &mut self.numeric_slack {
            // Old buffers have been freed; retain only one reusable chunk.
            credit.release_excess(credit.bytes().min(64 * 1024));
            slack.absorb(credit);
        }
        // Unbounded direct updates drop unused credit after old storage is
        // freed. Bounded producers keep at most one chunk, and merges keep
        // their prepaid credit until the job ends, including on partial failure.
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_program::ExpressionProgram;
    use arrow::datatypes::{DataType, Field, Schema};

    fn fixture(sql: &str, budget: &Budget) -> (Plan, ExpressionProgram, Aggregation) {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]);
        let plan = bind::bind(&bind::parse(sql).unwrap(), &schema, None, false).unwrap();
        let expressions = ExpressionProgram::new(&plan, 2, true);
        let state = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
        (plan, expressions, state)
    }

    fn row(plan: &Plan, key: Option<i128>, budget: &Budget) -> EvaluatedBatch {
        let value = |number| {
            Value::adopt(
                Arc::new(
                    Decimal128Array::from(vec![number])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                budget,
            )
            .unwrap()
        };
        EvaluatedBatch {
            rows: 1,
            keys: vec![value(key)],
            values: plan
                .aggregates
                .iter()
                .map(|aggregate| aggregate.input.as_ref().map(|_| value(Some(7))))
                .collect(),
            prepared_keys: None,
        }
    }

    fn vector_bytes(state: &Aggregation) -> usize {
        type DistinctSlot = Option<Box<HashSet<Key, ahash::RandomState>>>;
        state.numeric_group_keys.as_ref().unwrap().capacity() * std::mem::size_of::<i128>()
            + state
                .states
                .iter()
                .map(|state| {
                    state.counts.capacity() * std::mem::size_of::<u64>()
                        + state
                            .sums
                            .as_ref()
                            .map_or(0, |v| v.capacity() * std::mem::size_of::<i256>())
                        + state
                            .extremes
                            .as_ref()
                            .map_or(0, |v| v.capacity() * std::mem::size_of::<Option<Key>>())
                        + state
                            .distinct
                            .as_ref()
                            .map_or(0, |v| v.capacity() * std::mem::size_of::<DistinctSlot>())
                })
                .sum::<usize>()
    }

    #[test]
    fn many_aggregates_fund_outer_metadata_before_updates_and_through_growth() {
        // Numeric, encoded numeric, encoded text, and scalar grouping. Distinct
        // inputs prevent the expression program from aliasing the 100 states.
        for mode in 0..4 {
            let scalar = mode == 3;
            let text = mode == 2;
            let mut fields = vec![Field::new(
                "k",
                if text {
                    DataType::Utf8
                } else {
                    DataType::Int64
                },
                true,
            )];
            fields.extend((0..100).map(|i| Field::new(format!("v{i}"), DataType::Int64, true)));
            let schema = Schema::new(fields);
            let sums = (0..100)
                .map(|i| format!("SUM(v{i})"))
                .collect::<Vec<_>>()
                .join(",");
            let sql = if scalar {
                format!("SELECT {sums} FROM t")
            } else {
                format!("SELECT k,{sums} FROM t GROUP BY k")
            };
            let plan = bind::bind(&bind::parse(&sql).unwrap(), &schema, None, false).unwrap();
            let enabled = mode != 1;
            let expressions = ExpressionProgram::new(&plan, 101, enabled);
            assert_eq!(plan.aggregates.len(), 100);
            assert!(
                expressions
                    .state_sources
                    .iter()
                    .enumerate()
                    .all(|(i, s)| i == *s)
            );
            let outer_bytes = plan.groups.len() * std::mem::size_of::<Vec<Key>>()
                + plan.aggregates.len() * std::mem::size_of::<States>();
            let insufficient = Budget::new(outer_bytes - 1);
            let error = Aggregation::new(&plan, &expressions, insufficient.clone(), enabled)
                .err()
                .unwrap();
            assert!(error.starts_with("resource-limit:"));
            assert_eq!(insufficient.used(), 0);

            let budget = Budget::new(32 << 20);
            let mut state = Aggregation::new(&plan, &expressions, budget.clone(), enabled).unwrap();
            state.disable_dense();
            let check = |state: &Aggregation| {
                let outer = state.group_keys.capacity() * std::mem::size_of::<Vec<Key>>()
                    + state.states.capacity() * std::mem::size_of::<States>();
                assert_eq!(outer, outer_bytes);
                assert!(state.metadata_lease.bytes() >= outer);
                // Also include inner buffers so their funding cannot conceal a
                // missing outer charge once groups start growing.
                let inner = state
                    .group_keys
                    .iter()
                    .map(|keys| {
                        keys.capacity() * std::mem::size_of::<Key>()
                            + keys.iter().map(Key::payload_bytes).sum::<usize>()
                    })
                    .sum::<usize>()
                    + state
                        .numeric_group_keys
                        .as_ref()
                        .map_or(0, |keys| keys.capacity() * std::mem::size_of::<i128>())
                    + state
                        .states
                        .iter()
                        .map(|s| {
                            s.counts.capacity() * std::mem::size_of::<u64>()
                                + s.sums.as_ref().unwrap().capacity() * std::mem::size_of::<i256>()
                        })
                        .sum::<usize>();
                assert!(budget.used() >= outer + inner);
            };
            check(&state);
            let initial_capacity = state.states[0].counts.capacity();
            for group in 0..65 {
                let keys = if scalar {
                    Vec::new()
                } else {
                    let array: ArrayRef = if text {
                        Arc::new(StringArray::from(vec![group.to_string()]))
                    } else {
                        Arc::new(
                            Decimal128Array::from(vec![group as i128])
                                .with_precision_and_scale(38, 0)
                                .unwrap(),
                        )
                    };
                    vec![Value::adopt(array, &budget).unwrap()]
                };
                let input = Value::adopt(
                    Arc::new(
                        Decimal128Array::from(vec![7_i128])
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    ),
                    &budget,
                )
                .unwrap();
                let batch = EvaluatedBatch {
                    rows: 1,
                    keys,
                    values: (0..100).map(|_| Some(input.clone())).collect(),
                    prepared_keys: None,
                };
                drop(input);
                state
                    .update(&plan, &expressions, &mut Metrics::default(), batch)
                    .unwrap();
                check(&state);
                assert_eq!(state.group_count, if scalar { 1 } else { group + 1 });
                assert_eq!(state.numeric_group_keys.is_some(), mode == 0);
            }
            if !scalar {
                assert!(state.states[0].counts.capacity() > initial_capacity);
                let raw = state
                    .into_raw(RawFunding::SharedQuery(budget.clone()), 0)
                    .unwrap();
                let outer = raw.group_keys.capacity() * std::mem::size_of::<Vec<Key>>()
                    + raw.states.capacity() * std::mem::size_of::<States>();
                assert!(raw._metadata_lease.bytes() >= outer);
                drop(raw);
            } else {
                drop(state);
            }
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn numeric_capacity_reservations_cover_every_growth_and_raw_export() {
        for dense in [false, true] {
            for sql in [
                "SELECT k,SUM(v) FROM t GROUP BY k",
                "SELECT k,SUM(v),SUM(v),AVG(v),COUNT(*),MIN(v),MAX(v),COUNT(DISTINCT v) FROM t GROUP BY k",
            ] {
                let budget = Budget::new(16 << 20);
                let (plan, expressions, mut state) = fixture(sql, &budget);
                if !dense {
                    state.disable_dense();
                }
                let mut doublings = 0;
                let mut capacity = 0;
                for group in 0..1025 {
                    state
                        .update(
                            &plan,
                            &expressions,
                            &mut Metrics::default(),
                            row(&plan, (group != 0).then_some(group as i128), &budget),
                        )
                        .unwrap();
                    assert_eq!(state.group_count, group + 1);
                    let next = state.numeric_group_keys.as_ref().unwrap().capacity();
                    doublings += usize::from(next != capacity);
                    capacity = next;
                    let allocated = vector_bytes(&state) + state.numeric_map.allocation_bytes();
                    // Inspect real Vec capacities and hashbrown's reported full
                    // allocation (buckets, padding and controls), after EVERY
                    // insertion, including every vector/hash capacity doubling.
                    assert!(state.numeric_lease.bytes() >= allocated);
                    assert_eq!(state.numeric_lease.bytes(), allocated);
                    let heap = state
                        .states
                        .iter()
                        .map(|aggregate| {
                            let real = aggregate
                                .distinct
                                .iter()
                                .flatten()
                                .flatten()
                                .map(|set| {
                                    std::mem::size_of_val(set.as_ref())
                                        + set.allocation_size()
                                        + set.iter().map(Key::payload_bytes).sum::<usize>()
                                })
                                .sum::<usize>();
                            assert_eq!(
                                aggregate.heap_lease.as_ref().map_or(0, Reservation::bytes),
                                real
                            );
                            real
                        })
                        .sum::<usize>();
                    assert!(budget.used() >= allocated + heap + state.numeric_map.dense_bytes());
                }
                assert!(doublings >= 9);
                let vectors = vector_bytes(&state);
                let raw = state
                    .into_raw(RawFunding::SharedQuery(budget.clone()), 0)
                    .unwrap();
                assert_eq!(raw._numeric_lease.bytes(), vectors);
                let heap = raw
                    .states
                    .iter()
                    .filter_map(|state| state.heap_lease.as_ref())
                    .map(Reservation::bytes)
                    .sum::<usize>();
                assert!(budget.used() >= vectors + heap);
                drop(raw);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn numeric_capacity_refusal_preserves_buffers_and_credit() {
        let budget = Budget::new(1 << 20);
        let (plan, expressions, mut state) = fixture("SELECT k,SUM(v) FROM t GROUP BY k", &budget);
        state.disable_dense();
        for key in 0..4 {
            state
                .update(
                    &plan,
                    &expressions,
                    &mut Metrics::default(),
                    row(&plan, Some(key), &budget),
                )
                .unwrap();
        }
        let before = budget.used();
        let allocated = vector_bytes(&state) + state.numeric_map.allocation_bytes();
        let needed = state
            .numeric_headroom(expressions.state_sources.iter().copied(), 5, 5)
            .unwrap();
        let blocker = budget
            .reserve(budget.limit() - before - needed + 1)
            .unwrap();
        let error = state.grow_numeric(&expressions, Some(4)).unwrap_err();
        assert!(error.contains("resource-limit:"));
        assert_eq!(state.group_count, 4);
        assert_eq!(
            vector_bytes(&state) + state.numeric_map.allocation_bytes(),
            allocated
        );
        assert_eq!(state.numeric_lease.bytes(), allocated);
        drop(blocker);
        state.grow_numeric(&expressions, Some(4)).unwrap();
        assert_eq!(
            state.numeric_lease.bytes(),
            vector_bytes(&state) + state.numeric_map.allocation_bytes()
        );
        assert_eq!(
            budget.used(),
            before - allocated + state.numeric_lease.bytes()
        );
        drop(state);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn numeric_vector_copy_holds_old_and_new_capacity_until_copy_finishes() {
        let width = std::mem::size_of::<u64>();
        let budget = Budget::new(12 * width);
        let mut retained = budget.reserve(4 * width).unwrap();
        let mut values = Vec::with_capacity(4);
        values.extend(0_u64..4);
        let mut credit = budget.reserve(8 * width).unwrap();
        grow(&mut values, 5, &mut retained, &mut credit).unwrap();
        assert_eq!(values, [0, 1, 2, 3]);
        assert_eq!(values.capacity(), 8);
        assert_eq!(budget.peak(), 12 * width);
        assert_eq!(retained.bytes(), values.capacity() * width);
        assert_eq!(credit.bytes(), 4 * width);
        drop(credit);
        assert_eq!(budget.used(), 8 * width);
        drop(values);
        drop(retained);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn numeric_vector_failed_credit_split_never_replaces_the_buffer() {
        let width = std::mem::size_of::<u64>();
        for missing_old in [false, true] {
            let budget = Budget::new(12 * width);
            let mut retained = budget
                .reserve(if missing_old { 0 } else { 4 * width })
                .unwrap();
            let mut credit = budget
                .reserve(if missing_old { 8 * width } else { 0 })
                .unwrap();
            // Model legacy storage funded outside the numeric capacity lease.
            let backing = missing_old.then(|| budget.reserve(4 * width).unwrap());
            let mut values = Vec::with_capacity(4);
            values.extend(0_u64..4);
            let pointer = values.as_ptr();
            let before = (retained.bytes(), credit.bytes(), budget.used());
            assert!(grow(&mut values, 5, &mut retained, &mut credit).is_err());
            assert_eq!(values.as_ptr(), pointer);
            assert_eq!(values.capacity(), 4);
            assert_eq!(values, [0, 1, 2, 3]);
            assert_eq!((retained.bytes(), credit.bytes(), budget.used()), before);
            drop(values);
            drop(backing);
        }
    }

    #[test]
    fn merge_slack_accounts_for_credit_returned_by_a_smaller_rebuild() {
        assert_eq!(merge_slack(128, 128, 256).unwrap(), 256);
        assert_eq!(merge_slack(128, 128, 32).unwrap(), 32);
        assert_eq!(merge_slack(128, 128, 128).unwrap(), 128);
        assert!(merge_slack(0, 128, 0).is_err());
        assert!(merge_slack(usize::MAX, 0, 1).is_err());
    }

    #[test]
    fn legacy_numeric_partial_merge_keeps_its_per_group_funding() {
        let budget = Budget::new(4 << 20);
        let (plan, expressions, mut source) =
            fixture("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", &budget);
        source.disable_dense();
        let mut destination = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
        destination.disable_dense();
        destination.precise_numeric = false;
        for key in 0..4 {
            destination
                .update(
                    &plan,
                    &expressions,
                    &mut Metrics::default(),
                    row(&plan, Some(key), &budget),
                )
                .unwrap();
        }
        source
            .update(
                &plan,
                &expressions,
                &mut Metrics::default(),
                row(&plan, Some(4), &budget),
            )
            .unwrap();
        let state_bytes = destination
            .states
            .iter()
            .map(States::bytes_per_group)
            .sum::<usize>();
        let bytes = 128 + 4 * (std::mem::size_of::<i128>() + state_bytes);
        destination.add_merge_credit(budget.reserve(bytes).unwrap());
        let group = destination
            .locate_partial_group(
                &plan,
                &expressions,
                source.partial_view(),
                0,
                None,
                state_bytes,
                0,
                &crate::spill::Cancellation::default(),
            )
            .unwrap();
        assert_eq!(group, 4);
        assert_eq!(destination.numeric_lease.bytes(), 0);
        destination.retain_merge_credit();
        let real = vector_bytes(&destination) + destination.numeric_map.allocation_bytes();
        assert!(
            destination
                .state_leases
                .iter()
                .map(Reservation::bytes)
                .sum::<usize>()
                >= real
        );
    }

    #[test]
    fn numeric_merge_credit_covers_repeated_doublings_and_releases_unused_funding() {
        let budget = Budget::new(4 << 20);
        let (plan, expressions, mut source) =
            fixture("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", &budget);
        source.disable_dense();
        for key in 0..129 {
            source
                .update(
                    &plan,
                    &expressions,
                    &mut Metrics::default(),
                    row(&plan, Some(key), &budget),
                )
                .unwrap();
        }
        let mut destination = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
        destination.disable_dense();
        let before = budget.used();
        let headroom = destination
            .numeric_merge_headroom(&expressions, source.group_count)
            .unwrap();
        destination.add_merge_credit(budget.reserve(headroom).unwrap());
        for local in 0..source.group_count {
            let group = destination
                .locate_partial_group(
                    &plan,
                    &expressions,
                    source.partial_view(),
                    local,
                    None,
                    0,
                    0,
                    &crate::spill::Cancellation::default(),
                )
                .unwrap();
            assert_eq!(group, local);
            let allocated = vector_bytes(&destination) + destination.numeric_map.allocation_bytes();
            assert_eq!(destination.numeric_lease.bytes(), allocated);
            assert!(budget.used() >= before + allocated);
        }
        destination.retain_merge_credit();
        assert_eq!(destination.state_slack, 0);
        assert_eq!(budget.used(), before + destination.numeric_lease.bytes());
        drop(destination);
        drop(source);
        assert_eq!(budget.used(), 0);
    }
}
