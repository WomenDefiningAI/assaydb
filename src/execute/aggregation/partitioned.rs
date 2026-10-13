//! Persistent raw states with a first-seen ordered output view.
//! Exclusive-owner jobs mutate partitions; serial routing remains a test oracle.
use super::*;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    hash::{BuildHasher, Hash, Hasher},
};

// First-seen vector growth plus the final (partition, group) descriptor view.
const ORDER_BYTES_PER_GROUP: usize = 128;
struct Partition {
    // Token buffers die before the state leases that fund their capacity.
    first_seen: Vec<EncounterToken>,
    state: Aggregation,
}
pub(in crate::execute) struct Partitioned {
    // The final descriptor view is funded by leases held in the partitions.
    order: Option<Vec<(usize, usize)>>,
    partitions: Vec<Partition>,
    #[cfg(test)]
    next_ordinal: usize,
    hash: ahash::RandomState,
    budget: Budget,
    failed: Option<String>,
    _metadata: Arc<Reservation>,
}
impl Partitioned {
    pub(super) fn new(
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        budget: Budget,
        count: usize,
    ) -> Result<Self> {
        if count == 0
            || count > 64
            || p.groups.is_empty()
            || p.aggregates.iter().any(|a| {
                a.distinct
                    || !matches!(
                        a.function,
                        Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                    )
            })
        {
            return Err("unsupported partitioned aggregate contract".into());
        }
        let metadata =
            budget.reserve(4096 + count * (4096 + (p.groups.len() + p.aggregates.len()) * 256))?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let mut state = Aggregation::new(p, expressions, budget.clone(), true)?;
            // Do not multiply the optional dense allowance by partition count.
            // A query-wide dense allowance is required before enabling it here.
            state.disable_dense();
            partitions.push(Partition {
                state,
                first_seen: Vec::new(),
            });
        }
        Ok(Self {
            partitions,
            order: None,
            #[cfg(test)]
            next_ordinal: 0,
            hash: ahash::RandomState::new(),
            budget,
            failed: None,
            _metadata: Arc::new(metadata),
        })
    }
    pub(super) fn group_count(&self) -> usize {
        self.partitions.iter().map(|p| p.state.group_count).sum()
    }
    #[cfg(test)]
    fn partition(&self, source: &Aggregation, local: usize, encoded: Option<&[u8]>) -> usize {
        let mut h = self.hash.build_hasher();
        if let Some(keys) = &source.numeric_group_keys {
            // NULL has its own tag; every i128 bit pattern remains legal.
            (source.null_group != Some(local)).hash(&mut h);
            if source.null_group != Some(local) {
                keys[local].hash(&mut h);
            }
        } else {
            encoded.expect("encoded group key").hash(&mut h);
        }
        (h.finish() % self.partitions.len() as u64) as usize
    }
    #[cfg(test)]
    pub(super) fn merge(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        source: &Aggregation,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.order.is_some() {
            return Err("partitioned input already finalized".into());
        }
        let result = self.merge_inner(p, expressions, source, cancellation);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }
    #[cfg(test)]
    fn merge_inner(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        source: &Aggregation,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<()> {
        cancellation.check()?;
        let next = self
            .next_ordinal
            .checked_add(source.group_count)
            .ok_or("group ordinal overflow")?;
        let _scratch = self.budget.reserve(
            source
                .group_count
                .checked_mul(32)
                .ok_or("routing size overflow")?
                .checked_add(self.partitions.len() * 128)
                .ok_or("routing size overflow")?,
        )?;
        let mut encoded = vec![None; source.group_count];
        for (key, id) in source.map.entries() {
            encoded[id] = Some(key);
        }
        let state_bytes: usize = source
            .states
            .iter()
            .enumerate()
            .filter(|(i, _)| expressions.state_sources[*i] == *i)
            .map(|(_, state)| state.bytes_per_group())
            .sum();
        let mut growth = vec![0usize; self.partitions.len()];
        let mut route = Vec::with_capacity(source.group_count);
        for (local, key) in encoded.iter().enumerate() {
            let partition = self.partition(source, local, *key);
            let bytes = if source.numeric_group_keys.is_some() {
                128usize.checked_add(4 * (16 + state_bytes))
            } else {
                key.unwrap().len().checked_mul(6).and_then(|n| {
                    n.checked_add(
                        512 + 4 * (p.groups.len() * std::mem::size_of::<Key>() + state_bytes),
                    )
                })
            }
            .and_then(|n| n.checked_add(ORDER_BYTES_PER_GROUP))
            .ok_or("partition growth overflow")?;
            growth[partition] = growth[partition]
                .checked_add(bytes)
                .ok_or("partition growth overflow")?;
            route.push(partition);
        }
        // Acquire every partition's complete allowance before any mutation.
        // On failure, already acquired allowances drop without changing state.
        let funding = growth
            .iter()
            .map(|bytes| self.budget.reserve(*bytes))
            .collect::<Result<Vec<_>>>()?;
        for (partition, bytes) in self.partitions.iter_mut().zip(&growth) {
            partition.state.add_merge_credit(*bytes);
        }
        let result = (|| {
            // Insert all groups first; preserve ordinary state/row update priority.
            let mut destinations = Vec::with_capacity(source.group_count);
            for (local, &partition) in route.iter().enumerate() {
                let partition = &mut self.partitions[partition];
                let before = partition.state.group_count;
                let group = partition.state.locate_partial_group(
                    p,
                    expressions,
                    source.partial_view(),
                    local,
                    encoded[local],
                    state_bytes,
                    ORDER_BYTES_PER_GROUP,
                    cancellation,
                )?;
                if group == before {
                    partition.first_seen.push(EncounterToken {
                        source: 0,
                        batch: 0,
                        row: self.next_ordinal + local,
                    });
                }
                destinations.push(group);
            }
            // Match the ordinary multi-state update: canonical aggregate first,
            // then original encounter order. Partition completion order is irrelevant.
            for (index, canonical) in expressions.state_sources.iter().enumerate() {
                if index != *canonical {
                    continue;
                }
                for (local, &partition) in route.iter().enumerate() {
                    cancellation.check()?;
                    self.partitions[partition].state.merge_partial_state(
                        source.partial_view(),
                        local,
                        destinations[local],
                        index,
                    )?;
                }
            }
            Ok(())
        })();
        // Keep any consumed growth even on a partially applied error.
        for ((partition, lease), bytes) in self.partitions.iter_mut().zip(funding).zip(growth) {
            partition.state.retain_merge_credit(lease, bytes);
        }
        if result.is_ok() {
            self.next_ordinal = next;
        }
        result
    }
    pub(super) fn finish(&mut self, cancellation: &crate::spill::Cancellation) -> Result<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.order.is_some() {
            return Ok(());
        }
        cancellation.check()?;
        let count = self.partitions.iter().try_fold(0usize, |n, p| {
            n.checked_add(p.state.group_count)
                .ok_or("partition group count overflow")
        })?;
        // ORDER_BYTES_PER_GROUP funds this vector and each first_seen vector.
        // The bounded heap is covered by partition metadata funding.
        let mut order = Vec::with_capacity(count);
        // Persistent producers may flush out of original source order. Sort
        // lightweight descriptors within each partition; never renumber state.
        let sorted = self
            .partitions
            .iter()
            .map(|p| {
                let mut indices = (0..p.first_seen.len()).collect::<Vec<_>>();
                indices.sort_unstable_by_key(|&group| p.first_seen[group]);
                indices
            })
            .collect::<Vec<_>>();
        let mut heap = BinaryHeap::with_capacity(self.partitions.len());
        for (partition, indices) in sorted.iter().enumerate() {
            if let Some(&group) = indices.first() {
                heap.push(Reverse((
                    self.partitions[partition].first_seen[group],
                    partition,
                    0usize,
                )));
            }
        }
        while let Some(Reverse((_, partition, position))) = heap.pop() {
            cancellation.check()?;
            order.push((partition, sorted[partition][position]));
            if let Some(&group) = sorted[partition].get(position + 1) {
                heap.push(Reverse((
                    self.partitions[partition].first_seen[group],
                    partition,
                    position + 1,
                )));
            }
        }
        debug_assert_eq!(order.len(), count);
        self.order = Some(order);
        Ok(())
    }
    pub(super) fn output_batch(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let order = self
            .order
            .as_ref()
            .ok_or("partitioned output before input completion")?;
        let selected = order.get(range).ok_or("partitioned output range")?;
        let mut arrays = Vec::with_capacity(p.output.len());
        // Columns, then encounter-ordered rows: do not finalize whole shards
        // independently, which changes public error precedence.
        for (output, ty) in p.output.iter().zip(&p.types) {
            let physical_ty =
                physical_output_type(ty, Type::is_wide_integer(schema.field(arrays.len())));
            let ty = &physical_ty;
            let rows = || {
                selected
                    .iter()
                    .map(|(partition, group)| (&self.partitions[*partition].state, *group))
            };
            let array: ArrayRef = match output {
                Output::Group(column) => {
                    let keys = rows()
                        .map(|(state, group)| {
                            if let Some(keys) = &state.numeric_group_keys {
                                if state.null_group == Some(group) {
                                    Key::Null
                                } else {
                                    Key::Num(keys[group])
                                }
                            } else {
                                state.group_keys[*column][group].clone()
                            }
                        })
                        .collect::<Vec<_>>();
                    keys_array(&keys, ty)?
                }
                Output::Aggregate(_) if *ty == Type::Null => {
                    arrow::array::new_null_array(&ty.arrow(), selected.len())
                }
                Output::Aggregate(index) => {
                    let aggregate = &p.aggregates[*index];
                    let states = || {
                        rows().map(|(state, group)| {
                            (&state.states[expressions.state_sources[*index]], group)
                        })
                    };
                    match aggregate.function {
                        Aggregate::Count => {
                            count_array(states().map(|(state, group)| state.counts[group]), ty)?
                        }
                        Aggregate::Sum => {
                            let values = states()
                                .map(|(state, group)| {
                                    if state.counts[group] == 0 {
                                        Ok(None)
                                    } else {
                                        crate::aggregate_sum::finish(
                                            state.sums.as_ref().unwrap()[group],
                                        )
                                        .map(Some)
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let values: ArrayRef = Arc::new(
                                Decimal128Array::from(values)
                                    .with_precision_and_scale(38, ty.scale()?)
                                    .map_err(|e| e.to_string())?,
                            );
                            materialize(&values, ty)?
                        }
                        Aggregate::Avg => {
                            let scale = aggregate.input.as_ref().unwrap().ty.scale()?;
                            Arc::new(Float64Array::from(
                                states()
                                    .map(|(state, group)| {
                                        if state.counts[group] == 0 {
                                            Ok(None)
                                        } else {
                                            crate::numeric_float::average(
                                                state.sums.as_ref().unwrap()[group],
                                                scale,
                                                state.counts[group],
                                            )
                                            .map(Some)
                                        }
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            ))
                        }
                        _ => unreachable!("partitioned admission rejects unsupported states"),
                    }
                }
                Output::Expr(_) => unreachable!(),
            };
            arrays.push(output_array(array, schema.field(arrays.len()))?);
        }
        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())
    }
    pub(super) fn output_bytes(&self, p: &Plan, range: std::ops::Range<usize>) -> Result<usize> {
        let order = self
            .order
            .as_ref()
            .ok_or("partitioned output before input completion")?;
        let selected = order.get(range).ok_or("partitioned output range")?;
        let mut strings = 0usize;
        for output in &p.output {
            if let Output::Group(column) = output {
                for (partition, group) in selected {
                    let state = &self.partitions[*partition].state;
                    if state.numeric_group_keys.is_none() {
                        strings = strings
                            .checked_add(state.group_keys[*column][*group].payload_bytes())
                            .ok_or("output size overflow")?;
                    }
                }
            }
        }
        selected
            .len()
            .checked_mul((p.output.len() + 1) * 128)
            .and_then(|n| strings.checked_mul(4).and_then(|s| n.checked_add(s)))
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(|| "output size overflow".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Kind, expr_program::ExpressionProgram, spill::Cancellation};
    use arrow::datatypes::{DataType, Field, Schema};
    #[test]
    fn binary_partition_output_prepays_large_payloads_and_preserves_bytes() {
        let schema = Schema::new(vec![Field::new("b", DataType::Binary, true)]);
        let p = bind::bind(
            &bind::parse("SELECT b,COUNT(*) FROM t GROUP BY b").unwrap(),
            &schema,
            None,
            true,
        )
        .unwrap();
        let expressions = ExpressionProgram::new(&p, 1, true);
        let output = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Binary, true),
            Field::new(&p.names[1], DataType::Int64, true),
        ]));
        let budget = Budget::new(8 << 20);
        let a = vec![0xff; 4096];
        let b = vec![0; 8193];
        let input = arrow::array::BinaryArray::from(vec![
            Some(a.as_slice()),
            None,
            Some(b.as_slice()),
            Some(&[][..]),
            Some(a.as_slice()),
        ]);
        let mut source = Aggregation::new(&p, &expressions, budget.clone(), true).unwrap();
        source
            .update(
                &p,
                &expressions,
                &mut Metrics::default(),
                EvaluatedBatch {
                    rows: input.len(),
                    keys: vec![Value::adopt(Arc::new(input), &budget).unwrap()],
                    values: vec![None],
                    prepared_keys: None,
                },
            )
            .unwrap();
        let mut actual = owned::OwnedPartitions::new(&p, &expressions, budget.clone(), 4).unwrap();
        let raw = source
            .into_raw(RawFunding::SharedQuery(budget.clone()), 0)
            .unwrap();
        let jobs = actual
            .submit(&p, &expressions, Arc::new(raw), &Cancellation::default())
            .unwrap();
        for job in jobs.into_iter().rev() {
            actual
                .acknowledge(job.run(&p, &expressions, &Cancellation::default()))
                .unwrap();
        }
        actual.finish(&Cancellation::default()).unwrap();
        let allowance = actual.output_bytes(&p, 0..4).unwrap();
        let lease = budget.reserve(allowance).unwrap();
        let result = actual
            .output_batch(&p, &expressions, &output, 0..4)
            .unwrap();
        assert!(allowance >= result.get_array_memory_size());
        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(a.as_slice()), None, Some(b.as_slice()), Some(&[][..])]
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 1, 1, 1]
        );
        drop(result);
        drop(lease);
        drop(actual);
        assert_eq!(budget.used(), 0);
    }
    pub(super) type Row = (Option<i128>, Option<&'static str>, Option<i128>);
    pub(super) fn plan(sql: &str, bigint: bool) -> (Plan, ExpressionProgram, SchemaRef) {
        let schema = Schema::new(vec![
            Field::new(
                "k",
                if bigint {
                    DataType::Int64
                } else {
                    DataType::Decimal128(38, 0)
                },
                true,
            ),
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Decimal128(38, 2), true),
        ]);
        let p = bind::bind(&bind::parse(sql).unwrap(), &schema, None, true).unwrap();
        let expressions = ExpressionProgram::new(&p, 3, true);
        let output = Arc::new(Schema::new(
            p.names
                .iter()
                .zip(&p.types)
                .map(|(n, t)| Field::new(n, t.arrow(), true))
                .collect::<Vec<_>>(),
        ));
        (p, expressions, output)
    }
    pub(super) fn batch(p: &Plan, rows: &[Row], budget: &Budget) -> EvaluatedBatch {
        let columns = [
            Value::adopt(
                Arc::new(
                    Decimal128Array::from_iter(rows.iter().map(|r| r.0))
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                budget,
            )
            .unwrap(),
            Value::adopt(
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
                budget,
            )
            .unwrap(),
            Value::adopt(
                Arc::new(
                    Decimal128Array::from_iter(rows.iter().map(|r| r.2))
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ),
                budget,
            )
            .unwrap(),
        ];
        let value = |expr: &crate::expr::Expr| {
            let Kind::Column(column) = expr.kind else {
                panic!("fixture requires columns")
            };
            columns[column].clone()
        };
        EvaluatedBatch {
            rows: rows.len(),
            keys: p.groups.iter().map(value).collect(),
            values: p
                .aggregates
                .iter()
                .map(|a| a.input.as_ref().map(value))
                .collect(),
            prepared_keys: None,
        }
    }
    pub(super) fn state(
        p: &Plan,
        expressions: &ExpressionProgram,
        rows: &[Row],
        budget: &Budget,
    ) -> Aggregation {
        let mut state = Aggregation::new(p, expressions, budget.clone(), true).unwrap();
        state
            .update(
                p,
                expressions,
                &mut Metrics::default(),
                batch(p, rows, budget),
            )
            .unwrap();
        state
    }
    #[test]
    fn partitioned_raw_states_preserve_order_chunks_nulls_aliases_and_wide_cancellation() {
        let limit = 10_i128.pow(38) - 1;
        let mut rows = Vec::new();
        for v in [
            Some(limit),
            Some(limit),
            None,
            Some(-limit),
            Some(-limit),
            Some(17),
        ] {
            for (k, s) in [
                (Some(9), Some("é\0a")),
                (None, Some("a\0b")),
                (Some(3), None),
                (Some(i64::MAX as i128 + 1), Some("é")),
                (Some(-12), Some("")),
            ] {
                rows.push((k, s, v));
            }
        }
        rows.push((Some(4), Some("all null"), None));
        for sql in [
            "SELECT k,SUM(v),AVG(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
            "SELECT s,k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY s,k",
            "SELECT COUNT(*),AVG(v) FROM t GROUP BY k",
        ] {
            let (p, expressions, schema) = plan(sql, false);
            for partitions in [1, 2, 4, 8] {
                for chunk in [1, 3, 8, rows.len()] {
                    let budget = Budget::new(16 << 20);
                    let expected = state(&p, &expressions, &rows, &budget);
                    let mut actual =
                        Partitioned::new(&p, &expressions, budget.clone(), partitions).unwrap();
                    for rows in rows.chunks(chunk) {
                        let partial = state(&p, &expressions, rows, &budget);
                        actual
                            .merge(&p, &expressions, &partial, &Cancellation::default())
                            .unwrap();
                    }
                    actual.finish(&Cancellation::default()).unwrap();
                    for width in [1, 2, expected.group_count.max(1)] {
                        for start in (0..expected.group_count).step_by(width) {
                            let range = start..(start + width).min(expected.group_count);
                            let lease = budget
                                .reserve(actual.output_bytes(&p, range.clone()).unwrap())
                                .unwrap();
                            assert_eq!(
                                actual
                                    .output_batch(&p, &expressions, &schema, range.clone())
                                    .unwrap(),
                                expected
                                    .output_batch(&p, &expressions, &schema, range)
                                    .unwrap(),
                                "{sql}; partitions={partitions}, input={chunk}, output={width}"
                            );
                            drop(lease);
                        }
                    }
                    assert_eq!(actual.order.as_ref().unwrap().len(), expected.group_count);
                    drop(actual);
                    drop(expected);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }
    #[test]
    fn public_overflow_uses_column_then_encounter_order_and_raw_bigint_keys_stay_wide() {
        let limit = 10_i128.pow(38) - 1;
        for sql in [
            "SELECT SUM(v),COUNT(*) FROM t GROUP BY k",
            "SELECT COUNT(*),SUM(v) FROM t GROUP BY k",
        ] {
            let (p, expressions, schema) = plan(sql, true);
            let budget = Budget::new(4 << 20);
            let rows = [
                (Some(i64::MAX as i128 + 1), None, Some(1)),
                (Some(3), None, Some(limit)),
                (Some(3), None, Some(limit)),
            ];
            let mut expected = state(&p, &expressions, &rows, &budget);
            let count_index = p
                .aggregates
                .iter()
                .position(|a| matches!(a.function, Aggregate::Count))
                .unwrap();
            expected.states[expressions.state_sources[count_index]].counts[0] = u64::MAX;
            let mut actual = Partitioned::new(&p, &expressions, budget.clone(), 4).unwrap();
            actual
                .merge(&p, &expressions, &expected, &Cancellation::default())
                .unwrap();
            actual.finish(&Cancellation::default()).unwrap();
            for range in [0..1, 1..2, 0..2] {
                assert_eq!(
                    actual
                        .output_batch(&p, &expressions, &schema, range.clone())
                        .unwrap_err(),
                    expected
                        .output_batch(&p, &expressions, &schema, range)
                        .unwrap_err()
                );
            }
            drop(actual);
            drop(expected);
            assert_eq!(budget.used(), 0);
        }
        for projection in ["COUNT(*)", "k,COUNT(*)"] {
            let (p, expressions, schema) =
                plan(&format!("SELECT {projection} FROM t GROUP BY k"), true);
            let budget = Budget::new(4 << 20);
            let source = state(
                &p,
                &expressions,
                &[(Some(i64::MAX as i128 + 1), None, None)],
                &budget,
            );
            let mut actual = Partitioned::new(&p, &expressions, budget.clone(), 4).unwrap();
            actual
                .merge(&p, &expressions, &source, &Cancellation::default())
                .unwrap();
            actual.finish(&Cancellation::default()).unwrap();
            assert_eq!(
                actual.output_batch(&p, &expressions, &schema, 0..1),
                source.output_batch(&p, &expressions, &schema, 0..1)
            );
            drop(actual);
            drop(source);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn immutable_partial_is_send_sync_and_retains_real_backing_through_last_fragment() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<RawPartial>();
        let query = Budget::new(4 << 20);
        for sql in [
            "SELECT k,AVG(v),COUNT(*) FROM t GROUP BY k",
            "SELECT s,k,AVG(v),COUNT(*) FROM t GROUP BY s,k",
        ] {
            let private = Budget::new(1 << 20);
            let backing = query.reserve(1 << 20).unwrap();
            let (p, expressions, _) = plan(sql, false);
            let limit = 10_i128.pow(38) - 1;
            let source = state(
                &p,
                &expressions,
                &[
                    (Some(9), Some("é\0"), Some(limit)),
                    (Some(9), Some("é\0"), Some(limit)),
                    (None, None, None),
                ],
                &private,
            );
            let pointers = source
                .map
                .entries()
                .map(|(bytes, id)| (id, bytes.as_ptr()))
                .collect::<Vec<_>>();
            let raw = Arc::new(
                source
                    .into_raw(RawFunding::PrepaidPrivate(backing), 0)
                    .unwrap(),
            );
            assert_eq!(raw.group_count, 2);
            assert!(raw.states[0].sums.as_ref().unwrap()[0].to_i128().is_none());
            assert_eq!(raw.states[0].counts, [2, 0]);
            for (id, pointer) in pointers {
                assert_eq!(raw.encoded[id].as_ref().unwrap().as_ptr(), pointer);
            }
            if p.groups.len() == 1 {
                assert!(raw.encoded.is_empty());
                assert_eq!(raw.null_group, Some(1));
            } else {
                assert_eq!(raw.group_keys[0][0], Key::Text("é\0".into()));
            }
            let fragment = raw.clone();
            drop(raw);
            assert_eq!(query.used(), 1 << 20);
            assert!(private.used() > 0);
            std::thread::spawn(move || drop(fragment)).join().unwrap();
            assert_eq!(query.used(), 0);
            assert_eq!(private.used(), 0);
        }
        // Failed export releases both consumed source state and query backing.
        let (p, expressions, _) = plan("SELECT s,COUNT(*) FROM t GROUP BY s", false);
        let private = Budget::new(1 << 20);
        let source = state(&p, &expressions, &[(None, Some("key"), None)], &private);
        let blocker = private.reserve(private.limit() - private.used()).unwrap();
        let backing = query.reserve(1 << 20).unwrap();
        assert!(
            source
                .into_raw(RawFunding::PrepaidPrivate(backing), 0)
                .err()
                .unwrap()
                .starts_with("resource-limit:")
        );
        assert_eq!(query.used(), 0);
        drop(blocker);
        assert_eq!(private.used(), 0);
    }
    #[test]
    fn empty_and_nonnumeric_group_domains_match_single_table() {
        let domains: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                None,
                Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                Some(f64::INFINITY),
                Some(f64::NAN),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(-1),
                Some(0),
                None,
                Some(i32::MAX),
                Some(-1),
            ])),
            new_null_array(&DataType::Null, 4),
        ];
        for keys in domains {
            let schema = Schema::new(vec![Field::new("k", keys.data_type().clone(), true)]);
            let p = bind::bind(
                &bind::parse(if keys.data_type() == &DataType::Null {
                    // Bare NULL output is outside the existing SQL contract;
                    // NULL grouping itself must still produce the right count.
                    "SELECT COUNT(*) FROM t GROUP BY k"
                } else {
                    "SELECT k,COUNT(*) FROM t GROUP BY k"
                })
                .unwrap(),
                &schema,
                None,
                true,
            )
            .unwrap();
            let expressions = ExpressionProgram::new(&p, 1, true);
            let output = Arc::new(Schema::new(
                p.names
                    .iter()
                    .zip(&p.types)
                    .map(|(n, t)| Field::new(n, t.arrow(), true))
                    .collect::<Vec<_>>(),
            ));
            for length in [0, keys.len()] {
                let budget = Budget::new(4 << 20);
                let mut source = Aggregation::new(&p, &expressions, budget.clone(), true).unwrap();
                source
                    .update(
                        &p,
                        &expressions,
                        &mut Metrics::default(),
                        EvaluatedBatch {
                            rows: length,
                            keys: vec![Value::adopt(keys.slice(0, length), &budget).unwrap()],
                            values: vec![None],
                            prepared_keys: None,
                        },
                    )
                    .unwrap();
                let mut actual = Partitioned::new(&p, &expressions, budget.clone(), 4).unwrap();
                actual
                    .merge(&p, &expressions, &source, &Cancellation::default())
                    .unwrap();
                actual.finish(&Cancellation::default()).unwrap();
                assert_eq!(
                    actual
                        .output_batch(&p, &expressions, &output, 0..source.group_count)
                        .unwrap(),
                    source
                        .output_batch(&p, &expressions, &output, 0..source.group_count)
                        .unwrap()
                );
                drop(actual);
                drop(source);
                assert_eq!(budget.used(), 0);
            }
        }
    }
    #[test]
    fn average_can_finalize_a_total_wider_than_i128_without_a_sum_output() {
        let (p, expressions, schema) = plan("SELECT k,AVG(v),AVG(v) FROM t GROUP BY k", false);
        let budget = Budget::new(4 << 20);
        let rows = [(Some(1), None, Some(10_i128.pow(38) - 1)); 2];
        let source = state(&p, &expressions, &rows, &budget);
        assert!(
            source.states[0].sums.as_ref().unwrap()[0]
                .to_i128()
                .is_none()
        );
        let mut actual = Partitioned::new(&p, &expressions, budget.clone(), 4).unwrap();
        actual
            .merge(&p, &expressions, &source, &Cancellation::default())
            .unwrap();
        actual.finish(&Cancellation::default()).unwrap();
        assert_eq!(
            actual
                .output_batch(&p, &expressions, &schema, 0..1)
                .unwrap(),
            source
                .output_batch(&p, &expressions, &schema, 0..1)
                .unwrap()
        );
        drop(actual);
        drop(source);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn funded_growth_failure_cancellation_and_sticky_merge_errors_release_every_lease() {
        let (p, expressions, _) = plan("SELECT COUNT(*),SUM(v) FROM t GROUP BY k", false);
        for mode in ["memory", "cancel", "merge"] {
            let budget = Budget::new(4 << 20);
            let rows = (0..20)
                .map(|k| (Some(k), None, Some(1)))
                .collect::<Vec<_>>();
            let source = state(&p, &expressions, &rows, &budget);
            let mut actual = Partitioned::new(&p, &expressions, budget.clone(), 4).unwrap();
            actual.hash = ahash::RandomState::with_seeds(1, 2, 3, 4);
            actual
                .merge(&p, &expressions, &source, &Cancellation::default())
                .unwrap();
            let before = budget.used();
            let groups = actual
                .partitions
                .iter()
                .map(|p| p.state.group_count)
                .collect::<Vec<_>>();
            let cancellation = Cancellation::default();
            let blocker = if mode == "memory" {
                // Leave routing scratch and exactly the first occupied shard's
                // full worst-case growth: a later shard must fail after earlier
                // reservations succeeded, before any merge mutation.
                let mut counts = [0usize; 4];
                for i in 0..source.group_count {
                    counts[actual.partition(&source, i, None)] += 1;
                }
                assert!(counts.iter().filter(|n| **n > 0).count() > 1);
                let first = *counts.iter().find(|n| **n > 0).unwrap();
                // 128 index + 4*(16 numeric key + 8 COUNT + 40 SUM) +128 order.
                let available = source.group_count * 32 + 4 * 128 + first * 512;
                Some(budget.reserve(budget.limit() - before - available).unwrap())
            } else {
                None
            };
            if mode == "cancel" {
                cancellation.cancel();
            }
            if mode == "merge" {
                // The first encountered group must win regardless of hash bucket.
                let first = actual.partition(&source, 0, None);
                actual.partitions[first].state.states[0].counts[0] = u64::MAX;
            }
            let error = actual
                .merge(&p, &expressions, &source, &cancellation)
                .unwrap_err();
            assert!(error.contains(match mode {
                "memory" => "resource-limit:",
                "cancel" => "cancelled:",
                _ => "count overflow",
            }));
            drop(blocker);
            if mode != "merge" {
                assert_eq!(
                    groups,
                    actual
                        .partitions
                        .iter()
                        .map(|p| p.state.group_count)
                        .collect::<Vec<_>>()
                );
                assert_eq!(budget.used(), before);
            }
            assert_eq!(actual.finish(&Cancellation::default()).unwrap_err(), error);
            assert_eq!(
                actual
                    .merge(&p, &expressions, &source, &Cancellation::default())
                    .unwrap_err(),
                error
            );
            drop(actual);
            drop(source);
            assert_eq!(budget.used(), 0);
        }
    }
}

pub(super) mod owned;
