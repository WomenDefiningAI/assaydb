//! Partition equality work, then reconstruct encounter IDs in original order.
use super::*;
use hashbrown::{HashMap, hash_map::RawEntryMut};

type Interned = (Vec<i64>, Vec<u32>);

fn shard(hash: u64, count: usize) -> usize {
    // Avoid both hashbrown's low bucket bits and its top seven tag bits (P3).
    ((hash >> 32) as usize) & (count - 1)
}

pub(super) fn try_intern(
    values: &[crate::expr::Value],
    rows: usize,
    budget: &Budget,
    cancel: &crate::spill::Cancellation,
    workers: usize,
) -> Result<Option<Interned>> {
    cancel.check()?;
    if workers < 2 || rows < 65_536 || rows > u32::MAX as usize {
        return Ok(None);
    }
    let count = (workers.min(16) * 4).next_power_of_two();
    // The caller's 80 bytes/row funds the <=58 bytes/row hash tables and
    // 12 bytes/row retained IDs/representatives. This additional credit covers
    // routed rows, task outputs, first-occurrence reconstruction and descriptors.
    // All vector capacities are exact; no table grows beyond its input count.
    let Some(bytes) = rows
        .checked_mul(32)
        .and_then(|n| n.checked_add(count.checked_mul(8192)?))
    else {
        return Ok(None);
    };
    let Ok(_scratch) = budget.reserve(bytes) else {
        return Ok(None);
    };
    let hasher = ahash::RandomState::new();
    let Some(encoded) = crate::keys::partitioned::Partitioned::try_new(
        values, rows, budget, cancel, workers, &hasher,
    )?
    else {
        return Ok(None);
    };
    let mut counts = vec![0usize; count];
    for (row, hash) in encoded.hashes().enumerate() {
        if row % 1024 == 0 {
            cancel.check()?;
        }
        counts[shard(hash, count)] += 1;
    }
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0usize);
    for n in &counts {
        offsets.push(offsets.last().unwrap() + n);
    }
    let mut positions = offsets[..count].to_vec();
    let mut routed = vec![0u32; rows];
    for (row, hash) in encoded.hashes().enumerate() {
        if row % 1024 == 0 {
            cancel.check()?;
        }
        let partition = shard(hash, count);
        routed[positions[partition]] = row as u32;
        positions[partition] += 1;
    }
    // A task owns several disjoint hash partitions. This bounds thread startup
    // to workers, while retaining small, cache-local maps and skew correctness.
    let lanes = workers.min(16).min(count);
    let (parts, _) = crate::parallel_prepare::map(lanes, workers, budget, |lane| {
        #[cfg(test)]
        let _task = cancel.2.task();
        let capacity = (lane..count).step_by(lanes).map(|p| counts[p]).sum();
        let mut firsts = Vec::with_capacity(capacity);
        for partition in (lane..count).step_by(lanes) {
            let mut map = HashMap::<&[u8], u32, ahash::RandomState>::with_capacity_and_hasher(
                counts[partition],
                hasher.clone(),
            );
            #[cfg(test)]
            if partition == 0 {
                cancel.2.checkpoint("dictionary allocated", cancel, budget);
            }
            for (i, row) in routed[offsets[partition]..offsets[partition + 1]]
                .iter()
                .copied()
                .enumerate()
            {
                if i % 1024 == 0 {
                    cancel.check()?;
                }
                let key = encoded.row(row as usize);
                let hash = encoded.hash(row as usize);
                let first = match map.raw_entry_mut().from_hash(hash, |stored| *stored == key) {
                    RawEntryMut::Occupied(entry) => *entry.get(),
                    RawEntryMut::Vacant(entry) => *entry.insert_hashed_nocheck(hash, key, row).1,
                };
                firsts.push((row, first));
            }
        }
        Ok(firsts)
    })?;
    let mut firsts = vec![0u32; rows];
    for part in parts {
        for (row, first) in part {
            firsts[row as usize] = first;
        }
    }
    drop(encoded);
    drop(routed);
    let mut ids = Vec::with_capacity(rows);
    let mut representatives = Vec::with_capacity(rows);
    for (row, first) in firsts.into_iter().enumerate() {
        if row % 1024 == 0 {
            cancel.check()?;
        }
        #[cfg(test)]
        if row == 1 {
            cancel.2.checkpoint("dictionary merge", cancel, budget);
        }
        let id = if row == first as usize {
            let id = representatives.len() as i64;
            representatives.push(first);
            id
        } else {
            ids[first as usize]
        };
        ids.push(id);
    }
    Ok(Some((ids, representatives)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Value, keys::Keys, spill::Cancellation};

    fn serial(values: &[Value], rows: usize, budget: &Budget) -> (Vec<i64>, Vec<u32>) {
        let encoded = Keys::new(values, rows, budget).unwrap();
        let mut map = std::collections::HashMap::new();
        let mut ids = Vec::with_capacity(rows);
        let mut representatives = Vec::with_capacity(rows);
        for row in 0..rows {
            let next = representatives.len() as i64;
            ids.push(*map.entry(encoded.row(row)).or_insert_with(|| {
                representatives.push(row as u32);
                next
            }));
        }
        (ids, representatives)
    }

    #[test]
    fn parallel_dictionary_preserves_first_representatives_nulls_duplicates_and_skew() {
        let rows = 65_543;
        for skew in [false, true] {
            let budget = Budget::new(64 << 20);
            {
                let _base = budget.reserve(rows * 80 + 4096).unwrap();
                let values = vec![
                    Value::adopt(
                        Arc::new(StringArray::from_iter((0..rows).map(|i| {
                            if i % 13 == 0 {
                                None
                            } else if skew || i % 3 == 0 {
                                Some("é\0雪")
                            } else {
                                Some("")
                            }
                        }))),
                        &budget,
                    )
                    .unwrap(),
                    Value::adopt(
                        Arc::new(
                            Decimal128Array::from_iter((0..rows).map(|i| {
                                if i % 17 == 0 {
                                    None
                                } else {
                                    Some(if skew { 7 } else { (i % 997) as i128 })
                                }
                            }))
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                        ),
                        &budget,
                    )
                    .unwrap(),
                ];
                let expected = serial(&values, rows, &budget);
                assert_eq!(expected.0[0], 0);
                assert_eq!(expected.1[0], 0);
                for workers in [1, 2, 4, 10] {
                    let live = budget.used();
                    let actual =
                        try_intern(&values, rows, &budget, &Cancellation::default(), workers)
                            .unwrap();
                    if workers == 1 {
                        assert!(actual.is_none());
                    } else {
                        assert_eq!(actual.unwrap(), expected);
                    }
                    assert_eq!(budget.used(), live);
                    assert_eq!(budget.active_workers(), 0);
                    assert!(budget.peak() <= budget.limit());
                }
                let live = budget.used();
                let blocker = budget.reserve(budget.limit() - live).unwrap();
                assert!(
                    try_intern(&values, rows, &budget, &Cancellation::default(), 10)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(budget.used(), budget.limit());
                drop(blocker);
                assert_eq!(serial(&values, rows, &budget), expected);
                let cancel = Cancellation::default();
                cancel.cancel();
                assert_eq!(
                    try_intern(&values, rows, &budget, &cancel, 10).unwrap_err(),
                    "cancelled: query execution"
                );
                assert_eq!(budget.used(), live);
                assert_eq!(budget.active_workers(), 0);
            }
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn small_and_empty_dictionaries_decline_without_allocations() {
        let budget = Budget::new(0);
        for rows in [0, 1, 7, 8192] {
            assert!(
                try_intern(&[], rows, &budget, &Cancellation::default(), 10)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak_workers(), 0);
    }
}
