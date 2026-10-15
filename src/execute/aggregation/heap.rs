//! Fund typed aggregate payloads and DISTINCT tables before allocating them.
use super::*;

type DistinctSet = HashSet<Key, ahash::RandomState>;

// Field order keeps temporary payloads funded even on an early return.
struct FundedKey {
    key: Key,
    lease: Option<Reservation>,
}
impl FundedKey {
    fn new(value: &Value, ty: &Type, row: usize, retained: &Reservation) -> Result<Self> {
        let bytes = match ty {
            Type::Text => value
                .array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .len(),
            Type::Binary => value
                .array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row)
                .len(),
            _ => 0,
        };
        // Fixed-width keys (and empty strings/blobs) allocate no payload.
        // Avoid a query-budget mutex round trip for every numeric DISTINCT
        // candidate; the table and its replacement peak are funded separately.
        let lease = if bytes == 0 {
            None
        } else {
            Some(retained.budget().reserve(bytes)?)
        };
        let key = key(value, ty, row);
        // key() copies slices into exactly sized String/Vec allocations.
        debug_assert_eq!(payload_capacity(&key), bytes);
        Ok(Self { key, lease })
    }
}

fn payload_capacity(key: &Key) -> usize {
    match key {
        Key::Text(value) => value.capacity(),
        Key::Binary(value) => value.capacity(),
        _ => 0,
    }
}

fn table_headroom(entries: usize) -> Result<usize> {
    // Pinned hashbrown 0.16: power-of-two buckets, 7/8 load (one
    // empty bucket for tiny tables), and at most 16 trailing controls.
    // Key's bucket stride is a multiple of the control alignment.
    let buckets = if entries < 4 {
        4
    } else if entries < 8 {
        8
    } else if entries < 15 {
        16
    } else {
        entries
            .checked_mul(8)
            .and_then(|n| (n / 7).checked_next_power_of_two())
            .ok_or("distinct capacity overflow")?
    };
    buckets
        .checked_mul(std::mem::size_of::<Key>() + 1)
        .and_then(|n| n.checked_add(16))
        .ok_or_else(|| "distinct capacity overflow".into())
}

// Lazy boxes keep the per-group vector compact until a non-NULL value arrives.
#[allow(clippy::box_collection)]
pub(super) fn insert_distinct(
    slot: &mut Option<Box<DistinctSet>>,
    retained: &mut Reservation,
    value: &Value,
    ty: &Type,
    row: usize,
) -> Result<bool> {
    let candidate = FundedKey::new(value, ty, row, retained)?;
    if slot
        .as_ref()
        .is_some_and(|set| set.contains(&candidate.key))
    {
        return Ok(false);
    }
    if slot.as_ref().is_none_or(|set| set.len() == set.capacity()) {
        let entries = slot.as_ref().map_or(Ok(1), |set| {
            set.len().checked_add(1).ok_or("distinct capacity overflow")
        })?;
        let box_bytes = if slot.is_none() {
            std::mem::size_of::<DistinctSet>()
        } else {
            0
        };
        let bytes = table_headroom(entries)?
            .checked_add(box_bytes)
            .ok_or("distinct capacity overflow")?;
        let old = slot.as_ref().map_or(0, |set| set.allocation_size());
        let kept = retained
            .bytes()
            .checked_sub(old)
            .ok_or("internal: insufficient distinct capacity credit")?;
        // The old table and all its keys stay funded throughout rehashing.
        let mut allocation = retained.budget().reserve(bytes)?;
        let hasher = slot
            .as_ref()
            .map_or_else(ahash::RandomState::new, |set| set.hasher().clone());
        let mut next = DistinctSet::with_capacity_and_hasher(entries, hasher);
        let actual = next.allocation_size() + box_bytes;
        assert!(actual <= bytes);
        if let Some(set) = slot {
            next.extend(set.drain());
            **set = next; // Drop the old buckets before releasing their lease.
        } else {
            *slot = Some(Box::new(next));
        }
        retained.release_excess(kept);
        allocation.release_excess(actual);
        retained.absorb(allocation);
    }
    if let Some(lease) = candidate.lease {
        retained.absorb(lease);
    }
    slot.as_mut().unwrap().insert(candidate.key);
    Ok(true)
}

pub(super) fn update_extreme(
    slot: &mut Option<Key>,
    retained: &mut Reservation,
    value: &Value,
    ty: &Type,
    row: usize,
    minimum: bool,
) -> Result<()> {
    // Compare borrowed variable-width payloads before funding a copy. Equal
    // and losing candidates need no extra heap or budget headroom.
    if !is_sql_null(value.array.as_ref(), row) {
        let borrowed_cmp = match (ty, slot.as_ref()) {
            (Type::Text, Some(Key::Text(old))) => Some(
                value
                    .array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .cmp(old.as_str()),
            ),
            (Type::Binary, Some(Key::Binary(old))) => Some(
                value
                    .array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row)
                    .cmp(old.as_slice()),
            ),
            _ => None,
        };
        if borrowed_cmp.is_some_and(|cmp| if minimum { !cmp.is_lt() } else { !cmp.is_gt() }) {
            return Ok(());
        }
    }
    let candidate = FundedKey::new(value, ty, row, retained)?;
    let replace = slot.as_ref().is_none_or(|old| {
        let cmp = if let (Key::Float(a), Key::Float(b)) = (&candidate.key, old) {
            f64::from_bits(*a).total_cmp(&f64::from_bits(*b))
        } else {
            candidate.key.cmp(old)
        };
        if minimum { cmp.is_lt() } else { cmp.is_gt() }
    });
    if replace {
        let old = slot.as_ref().map_or(0, payload_capacity);
        let kept = retained
            .bytes()
            .checked_sub(old)
            .ok_or("internal: insufficient extreme payload credit")?;
        *slot = Some(candidate.key); // Old payload drops while still funded.
        retained.release_excess(kept);
        if let Some(lease) = candidate.lease {
            retained.absorb(lease);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(text: &str, binary: bool, budget: &Budget) -> (Value, Type) {
        let (array, ty): (ArrayRef, Type) = if binary {
            (
                Arc::new(BinaryArray::from(vec![text.as_bytes()])),
                Type::Binary,
            )
        } else {
            (Arc::new(StringArray::from(vec![text])), Type::Text)
        };
        (Value::adopt(array, budget).unwrap(), ty)
    }

    fn distinct_bytes(set: &DistinctSet) -> usize {
        std::mem::size_of::<DistinctSet>()
            + set.allocation_size()
            + set.iter().map(payload_capacity).sum::<usize>()
    }

    #[test]
    fn inline_distinct_candidates_skip_payload_leases_but_fund_rehash_peaks() {
        let input_budget = Budget::new(1 << 20);
        for ty in [Type::Int, Type::Decimal(2)] {
            let value = Value::adopt(
                Arc::new(
                    Decimal128Array::from(vec![i64::MIN as i128, i64::MAX as i128, 0, -1])
                        .with_precision_and_scale(38, ty.scale().unwrap())
                        .unwrap(),
                ),
                &input_budget,
            )
            .unwrap();
            let budget = Budget::new(1 << 20);
            let mut lease = budget.reserve(0).unwrap();
            let mut set = None;
            for row in 0..3 {
                let candidate = FundedKey::new(&value, &ty, row, &lease).unwrap();
                assert!(candidate.lease.is_none());
                assert_eq!(payload_capacity(&candidate.key), 0);
                assert!(insert_distinct(&mut set, &mut lease, &value, &ty, row).unwrap());
                assert_eq!(lease.bytes(), distinct_bytes(set.as_ref().unwrap()));
                assert_eq!(budget.used(), lease.bytes());
            }
            assert_eq!(set.as_ref().unwrap().capacity(), 3);
            let old_bytes = lease.bytes();
            let blocker = budget.reserve(budget.limit() - budget.used()).unwrap();
            // A duplicate needs neither payload nor table space at a full budget.
            assert!(!insert_distinct(&mut set, &mut lease, &value, &ty, 0).unwrap());
            let error = insert_distinct(&mut set, &mut lease, &value, &ty, 3).unwrap_err();
            assert!(error.starts_with("resource-limit: accounted memory"));
            assert_eq!(set.as_ref().unwrap().len(), 3);
            assert_eq!(lease.bytes(), old_bytes);
            assert_eq!(budget.used(), old_bytes + blocker.bytes());
            drop(blocker);
            assert!(insert_distinct(&mut set, &mut lease, &value, &ty, 3).unwrap());
            assert_eq!(lease.bytes(), distinct_bytes(set.as_ref().unwrap()));
            for row in 0..4 {
                assert!(!insert_distinct(&mut set, &mut lease, &value, &ty, row).unwrap());
            }
            drop(set);
            drop(lease);
            assert_eq!(budget.used(), 0);

            // On a fresh budget the peak comes from overlapping old/new tables,
            // rather than from the deliberate blocker above.
            let budget = Budget::new(1 << 20);
            let mut lease = budget.reserve(0).unwrap();
            let mut set = None;
            for row in 0..3 {
                insert_distinct(&mut set, &mut lease, &value, &ty, row).unwrap();
            }
            let old_bytes = budget.used();
            insert_distinct(&mut set, &mut lease, &value, &ty, 3).unwrap();
            assert!(budget.peak() >= old_bytes + set.as_ref().unwrap().allocation_size());
            assert_eq!(budget.used(), distinct_bytes(set.as_ref().unwrap()));
            drop(set);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
        assert_eq!(input_budget.used(), 0);
    }

    #[test]
    fn distinct_heap_covers_boxes_tables_and_payloads_after_every_insert() {
        for binary in [false, true] {
            let budget = Budget::new(32 << 20);
            let mut lease = budget.reserve(0).unwrap();
            let mut groups: Vec<Option<Box<DistinctSet>>> = (0..129).map(|_| None).collect();
            for row in 0..65 {
                let text = format!("{row:04}{}", "x".repeat(row * 3));
                let (value, ty) = value(&text, binary, &budget);
                let mut real = groups
                    .iter()
                    .flatten()
                    .map(|set| distinct_bytes(set))
                    .sum::<usize>();
                for group in &mut groups {
                    let old = group.as_ref().map_or(0, |set| distinct_bytes(set));
                    assert!(insert_distinct(group, &mut lease, &value, &ty, 0).unwrap());
                    real = real - old + distinct_bytes(group.as_ref().unwrap());
                    assert_eq!(lease.bytes(), real);
                    assert!(budget.used() >= real);
                }
                let before = lease.bytes();
                assert!(!insert_distinct(&mut groups[0], &mut lease, &value, &ty, 0).unwrap());
                assert_eq!(lease.bytes(), before);
            }
            drop(groups);
            drop(lease);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn non_replacing_extremes_need_no_temporary_payload_budget() {
        for binary in [false, true] {
            for minimum in [false, true] {
                let first = if minimum { "a" } else { "z" }.repeat(2048);
                let losing = if minimum { "z" } else { "a" }.repeat(8192);
                let input_budget = Budget::new(1 << 20);
                let (first_value, ty) = value(&first, binary, &input_budget);
                let (losing_value, _) = value(&losing, binary, &input_budget);
                // Exactly enough for the stored extreme, with no credit for
                // even one equal or losing temporary copy.
                let tight = Budget::new(first.len());
                let unconstrained = Budget::new(1 << 20);
                let mut tight_lease = tight.reserve(0).unwrap();
                let mut reference_lease = unconstrained.reserve(0).unwrap();
                let mut extreme = None;
                let mut reference = None;
                for row in 0..1025 {
                    let candidate = if row % 2 == 0 {
                        &first_value
                    } else {
                        &losing_value
                    };
                    update_extreme(&mut extreme, &mut tight_lease, candidate, &ty, 0, minimum)
                        .unwrap();
                    update_extreme(
                        &mut reference,
                        &mut reference_lease,
                        candidate,
                        &ty,
                        0,
                        minimum,
                    )
                    .unwrap();
                    assert_eq!(extreme, reference);
                    assert_eq!(tight.used(), first.len());
                    assert_eq!(tight.peak(), first.len());
                }
                assert_eq!(extreme, Some(key(&first_value, &ty, 0)));
                drop(extreme);
                drop(tight_lease);
                drop(reference);
                drop(reference_lease);
                assert_eq!(tight.used(), 0);
                assert_eq!(unconstrained.used(), 0);
            }
        }
    }

    #[test]
    fn extreme_heap_tracks_growing_and_shrinking_replacements() {
        for binary in [false, true] {
            for minimum in [false, true] {
                let budget = Budget::new(1 << 20);
                let mut lease = budget.reserve(0).unwrap();
                let mut extreme = None;
                for row in 0..257 {
                    let order = if minimum { 256 - row } else { row };
                    let text = format!("{order:04}{}", "x".repeat((row % 7) * 257));
                    let (value, ty) = value(&text, binary, &budget);
                    let old = extreme.as_ref().map_or(0, payload_capacity);
                    let live = budget.used();
                    update_extreme(&mut extreme, &mut lease, &value, &ty, 0, minimum).unwrap();
                    assert_eq!(extreme, Some(key(&value, &ty, 0)));
                    assert_eq!(lease.bytes(), text.len());
                    assert_eq!(budget.used(), live - old + text.len());
                    assert!(budget.peak() >= live + text.len());
                    // Equal candidates must free their temporary payload.
                    let before = budget.used();
                    update_extreme(&mut extreme, &mut lease, &value, &ty, 0, minimum).unwrap();
                    assert_eq!(budget.used(), before);
                }
                drop(extreme);
                drop(lease);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn distinct_rehash_refusal_keeps_old_table_funded_and_unchanged() {
        let budget = Budget::new(1 << 20);
        let mut lease = budget.reserve(0).unwrap();
        let mut set = None;
        for row in 0..3 {
            let (value, ty) = value(&row.to_string(), false, &budget);
            insert_distinct(&mut set, &mut lease, &value, &ty, 0).unwrap();
        }
        let (value, ty) = value("new", false, &budget);
        let real = distinct_bytes(set.as_ref().unwrap());
        let before = budget.used();
        // Allow the candidate payload, but refuse overlapping old/new tables.
        let blocker = budget.reserve(budget.limit() - before - 3).unwrap();
        let error = insert_distinct(&mut set, &mut lease, &value, &ty, 0).unwrap_err();
        assert!(error.starts_with("resource-limit:"));
        assert_eq!(set.as_ref().unwrap().len(), 3);
        assert_eq!(lease.bytes(), real);
        assert_eq!(distinct_bytes(set.as_ref().unwrap()), real);
        assert_eq!(budget.used(), before + blocker.bytes());
        drop(blocker);
        let live = budget.used();
        insert_distinct(&mut set, &mut lease, &value, &ty, 0).unwrap();
        assert_eq!(lease.bytes(), distinct_bytes(set.as_ref().unwrap()));
        assert!(budget.peak() >= live + 3 + set.as_ref().unwrap().allocation_size());
    }

    #[test]
    fn aggregate_updates_fund_heap_for_numeric_encoded_and_scalar_groups() {
        use crate::expr_program::ExpressionProgram;
        use arrow::datatypes::{DataType, Field, Schema};

        for binary in [false, true] {
            for mode in 0..3 {
                let scalar = mode == 2;
                let budget = Budget::new(64 << 20);
                let schema = Schema::new(vec![
                    Field::new("k", DataType::Int64, true),
                    Field::new(
                        "v",
                        if binary {
                            DataType::Binary
                        } else {
                            DataType::Utf8
                        },
                        true,
                    ),
                ]);
                // Binary MIN/MAX are rejected by the binder; their payload accounting
                // is covered directly by the extreme replacement test above.
                let sql = if binary && scalar {
                    "SELECT COUNT(DISTINCT v) FROM t"
                } else if binary {
                    "SELECT k,COUNT(DISTINCT v) FROM t GROUP BY k"
                } else if scalar {
                    "SELECT COUNT(DISTINCT v),MIN(DISTINCT v),MAX(v) FROM t"
                } else {
                    "SELECT k,COUNT(DISTINCT v),MIN(DISTINCT v),MAX(v) FROM t GROUP BY k"
                };
                let plan = bind::bind(&bind::parse(sql).unwrap(), &schema, None, false).unwrap();
                let expressions = ExpressionProgram::new(&plan, 2, mode != 1);
                let mut state =
                    Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
                state.disable_dense();
                let groups = if scalar { 1 } else { 129 };
                for row in 0..17 {
                    // Alternating bounds replace both MIN and MAX, with varying lengths.
                    let order = if row % 2 == 0 { 100 - row } else { 100 + row };
                    let text = format!("{order:04}{}", "x".repeat((row % 5) * 257));
                    let (input, _) = value(&text, binary, &budget);
                    for group in 0..groups {
                        let keys = if scalar {
                            Vec::new()
                        } else {
                            vec![
                                Value::adopt(
                                    Arc::new(
                                        Decimal128Array::from(vec![Some(group as i128)])
                                            .with_precision_and_scale(38, 0)
                                            .unwrap(),
                                    ),
                                    &budget,
                                )
                                .unwrap(),
                            ]
                        };
                        state
                            .update(
                                &plan,
                                &expressions,
                                &mut Metrics::default(),
                                EvaluatedBatch {
                                    rows: 1,
                                    keys,
                                    values: plan
                                        .aggregates
                                        .iter()
                                        .map(|_| Some(input.clone()))
                                        .collect(),
                                    prepared_keys: None,
                                },
                            )
                            .unwrap();
                        let mut total = 0;
                        for aggregate in &state.states {
                            let real = aggregate
                                .distinct
                                .iter()
                                .flatten()
                                .flatten()
                                .map(|set| distinct_bytes(set))
                                .sum::<usize>()
                                + aggregate
                                    .extremes
                                    .iter()
                                    .flatten()
                                    .flatten()
                                    .map(payload_capacity)
                                    .sum::<usize>();
                            assert_eq!(
                                aggregate.heap_lease.as_ref().map_or(0, Reservation::bytes),
                                real
                            );
                            total += real;
                        }
                        assert!(budget.used() >= total);
                    }
                }
                assert_eq!(state.group_count, groups);
                for (index, aggregate) in state.states.iter().enumerate() {
                    if expressions.state_sources[index] == index {
                        assert!(aggregate.counts.iter().all(|count| *count == 17));
                    }
                }
                drop(state);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn extreme_refusal_preserves_old_payload_and_lease() {
        let budget = Budget::new(1 << 20);
        let mut lease = budget.reserve(0).unwrap();
        let mut extreme = None;
        let (first, ty) = value("a", false, &budget);
        update_extreme(&mut extreme, &mut lease, &first, &ty, 0, false).unwrap();
        let (next, _) = value(&"z".repeat(4096), false, &budget);
        let blocker = budget
            .reserve(budget.limit() - budget.used() - 4095)
            .unwrap();
        let before = budget.used();
        let error = update_extreme(&mut extreme, &mut lease, &next, &ty, 0, false).unwrap_err();
        assert!(error.starts_with("resource-limit:"));
        assert_eq!(extreme, Some(Key::Text("a".into())));
        assert_eq!(lease.bytes(), 1);
        assert_eq!(budget.used(), before);
        drop(blocker);
    }
}
