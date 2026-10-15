//! Completed numeric hash builds, partitioned without changing duplicate order.
use super::{JoinIndex, Storage};
use crate::{Result, keys::Keys, parallel_prepare, spill::Cancellation};
use hashbrown::{HashTable, hash_table::Entry as TableEntry};
use std::sync::Mutex;

#[derive(Clone, Copy)]
struct Entry {
    key_row: u32,
    head: u32,
}
#[derive(Clone, Copy, Default)]
struct Row {
    hash: u64,
    row: u32,
    previous: u32,
}

pub(super) struct Index {
    maps: Vec<HashTable<Entry>>,
    hash: ahash::RandomState,
    len: usize,
}

// Keep routing bits separate from hashbrown's bucket bits and control tags.
fn shard(hash: u64, count: usize) -> usize {
    ((hash >> 32) as usize) & (count - 1)
}

impl Index {
    pub(super) fn get(&self, key: &[u8], keys: &Keys) -> Option<&u32> {
        let hash = self.hash.hash_one(key);
        self.maps[shard(hash, self.maps.len())]
            .find(hash, |entry| keys.row(entry.key_row as usize) == key)
            .map(|entry| &entry.head)
    }
    pub(super) fn remove(&mut self, key: &[u8], keys: &Keys) -> Option<u32> {
        let hash = self.hash.hash_one(key);
        let partition = shard(hash, self.maps.len());
        let entry = self.maps[partition]
            .find_entry(hash, |entry| keys.row(entry.key_row as usize) == key)
            .ok()?;
        let head = entry.remove().0.head;
        self.len -= 1;
        Some(head)
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn key_rows(&self) -> impl Iterator<Item = u32> + '_ {
        self.maps
            .iter()
            .flat_map(|map| map.iter().map(|entry| entry.key_row))
    }
    pub(super) fn heads_mut(&mut self) -> impl Iterator<Item = &mut u32> {
        self.maps
            .iter_mut()
            .flat_map(|map| map.iter_mut().map(|entry| &mut entry.head))
    }
}

impl<'a> JoinIndex<'a> {
    pub(crate) fn populate_prepared(
        &mut self,
        keys: &'a Keys,
        next: &mut [u32],
        workers: usize,
        cancel: &Cancellation,
    ) -> Result<usize> {
        cancel.check()?;
        if let Some(tasks) = self.try_populate_hash(keys, next, workers, cancel)? {
            return Ok(tasks);
        }
        self.populate(keys, next);
        Ok(0)
    }

    fn try_populate_hash(
        &mut self,
        keys: &'a Keys,
        next: &mut [u32],
        workers: usize,
        cancel: &Cancellation,
    ) -> Result<Option<usize>> {
        let rows = next.len();
        if workers < 2 || rows < 262_144 || rows > u32::MAX as usize {
            return Ok(None);
        }
        let Storage::Hash(old) = &self.storage else {
            return Ok(None);
        };
        let Some(numbers) = keys.numeric_values() else {
            return Ok(None);
        };
        assert!(old.is_empty());
        assert_eq!(numbers.len(), rows);
        // Unique builds retain their existing lane-fusion handoff. A bounded
        // sample must witness an actual non-NULL duplicate, and enough changes
        // of key to avoid paying for partitioning an almost constant build.
        let mut duplicates = 0;
        let mut changes = 0;
        for row in (1..rows).step_by(rows.div_ceil(1024)) {
            if keys.valid(row - 1) && keys.valid(row) {
                if numbers[row - 1] == numbers[row] {
                    duplicates += 1;
                } else {
                    changes += 1;
                }
            }
        }
        if duplicates == 0 || changes < 16 {
            return Ok(None);
        }
        let lanes = workers.min(16);
        let partitions = (lanes * 4).next_power_of_two();
        // Two 16-byte routing buffers plus task/partition metadata are optional
        // scratch. The unchanged 64 bytes/row index credit covers next and all
        // retained tables: 8-byte entries + control bytes at <= 16/7 buckets
        // per input row, plus per-partition allocation/header rounding.
        let Some(bytes) = rows
            .checked_mul(40)
            .and_then(|n| n.checked_add(partitions.checked_mul(8192)?))
        else {
            return Ok(None);
        };
        let budget = self._lease.budget();
        let Ok(_scratch) = budget.reserve(bytes) else {
            return Ok(None);
        };
        // Retire the empty ordinary table before allocating replacement tables.
        // Every optional refusal above left it untouched. After this point only
        // cancellation/worker failure can abort, and no probe has started.
        drop(std::mem::replace(
            &mut self.storage,
            Storage::Hash(super::HashIndex::new(keys, 0)),
        ));
        let hash = ahash::RandomState::new();
        let width = rows.div_ceil(lanes);
        let (chunks, hashing_tasks) = parallel_prepare::map(lanes, workers, &budget, |lane| {
            #[cfg(test)]
            let _task = cancel.2.task();
            let start = lane * width;
            let end = (start + width).min(rows);
            let mut chunk = Vec::with_capacity(end - start);
            for row in start..end {
                if row % 4096 == 0 {
                    cancel.check()?;
                }
                if keys.valid(row) {
                    chunk.push(Row {
                        hash: hash.hash_one(keys.row(row)),
                        row: row as u32,
                        previous: u32::MAX,
                    });
                }
            }
            Ok(chunk)
        })?;
        let mut counts = vec![0usize; partitions];
        for chunk in &chunks {
            cancel.check()?;
            for row in chunk {
                counts[shard(row.hash, partitions)] += 1;
            }
        }
        let mut positions = Vec::with_capacity(partitions);
        let mut total = 0;
        for count in &counts {
            positions.push(total);
            total += count;
        }
        let mut routed = vec![Row::default(); total];
        // map returns chunks in source order; stable scatter therefore preserves
        // every key's original insertion sequence, independent of worker count.
        for chunk in chunks {
            cancel.check()?;
            for row in chunk {
                let partition = shard(row.hash, partitions);
                routed[positions[partition]] = row;
                positions[partition] += 1;
            }
        }
        let mut jobs = Vec::with_capacity(partitions);
        let mut remaining = routed.as_mut_slice();
        for (partition, count) in counts.into_iter().enumerate() {
            let (rows, rest) = remaining.split_at_mut(count);
            remaining = rest;
            if !rows.is_empty() {
                jobs.push(Mutex::new(Some((partition, rows))));
            }
        }
        let lanes = lanes.min(jobs.len());
        let (built, building_tasks) = parallel_prepare::map(lanes, workers, &budget, |lane| {
            #[cfg(test)]
            let _task = cancel.2.task();
            let mut maps = Vec::with_capacity(jobs.len().div_ceil(lanes));
            for job in jobs.iter().skip(lane).step_by(lanes) {
                // Each slice has exactly one owner; this mutex only transfers
                // it through the shared closure, never serializes table work.
                let (partition, rows) = job.lock().unwrap().take().unwrap();
                let mut map = HashTable::<Entry>::with_capacity(rows.len());
                #[cfg(test)]
                if std::ptr::eq(job, &jobs[0]) {
                    cancel.2.checkpoint("index allocated", cancel, &budget);
                }
                for (i, row) in rows.iter_mut().enumerate() {
                    if i % 4096 == 0 {
                        cancel.check()?;
                    }
                    let key = keys.row(row.row as usize);
                    match map.entry(
                        row.hash,
                        |entry| keys.row(entry.key_row as usize) == key,
                        |entry| hash.hash_one(keys.row(entry.key_row as usize)),
                    ) {
                        TableEntry::Occupied(mut entry) => {
                            row.previous = std::mem::replace(&mut entry.get_mut().head, row.row);
                        }
                        TableEntry::Vacant(entry) => {
                            entry.insert(Entry {
                                key_row: row.row,
                                head: row.row,
                            });
                        }
                    }
                }
                maps.push((partition, map));
            }
            Ok(maps)
        })?;
        drop(jobs);
        let mut maps = (0..partitions)
            .map(|_| HashTable::new())
            .collect::<Vec<_>>();
        let mut len = 0;
        for part in built {
            for (partition, map) in part {
                len += map.len();
                maps[partition] = map;
            }
        }
        // Workers have all joined. Copy each disjoint row's predecessor once;
        // NULL rows keep MAX. No duplicate chain is reversed or reassociated.
        for (i, row) in routed.into_iter().enumerate() {
            #[cfg(test)]
            if i == 1 {
                cancel.2.checkpoint("index merge", cancel, &budget);
            }
            if i % 4096 == 0 {
                cancel.check()?;
            }
            next[row.row as usize] = row.previous;
        }
        cancel.check()?;
        self.storage = Storage::Prepared {
            index: Index { maps, hash, len },
            keys,
        };
        self.populated_keys = Some(keys);
        self.rejection = None;
        Ok(Some(hashing_tasks + building_tasks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Value, memory::Budget};
    use arrow::array::Decimal128Array;
    use std::sync::Arc;

    fn assert_same_index(actual: &JoinIndex<'_>, expected: &JoinIndex<'_>, keys: &Keys) {
        assert_eq!(actual.len(), expected.len());
        for row in 0..keys.len() {
            assert_eq!(actual.get(keys.row(row)), expected.get(keys.row(row)));
        }
    }

    #[test]
    fn midflight_cancellation_drops_partition_tables_without_publishing_index() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        let value = Value::adopt(
            Arc::new(Decimal128Array::from_iter((0..rows).map(|i| {
                (i % 17 != 0).then_some((i / 3 * 8191 % (rows / 3)) as i128 * 1024)
            }))),
            &budget,
        )
        .unwrap();
        let keys = Keys::new(&[value], rows, &budget).unwrap();
        for workers in [2, 4, 10] {
            for point in ["index allocated", "index merge"] {
                let mut index = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                let baseline = budget.used();
                let cancel = Cancellation::default();
                cancel.2.arm(point);
                assert_eq!(
                    index
                        .populate_prepared(&keys, &mut next, workers, &cancel)
                        .unwrap_err(),
                    "cancelled: query execution"
                );
                cancel.2.assert_drained(&budget, baseline);
                assert!(index.is_empty());
                assert!(index.populated_keys.is_none());
                assert!(!matches!(index.storage, Storage::Prepared { .. }));
                for row in 0..rows {
                    assert!(index.get(keys.row(row)).is_none());
                }
            }
        }
        drop(keys);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn prepared_hash_keeps_heads_links_nulls_skew_and_extreme_bases() {
        let rows = 262_147;
        let unique = rows / 3;
        let budget = Budget::new(128 << 20);
        for (base, nullable, skew) in [
            (i128::MIN, false, false),
            (i128::MAX - (unique * 1024) as i128, true, false),
            (-1_000_000, true, true),
        ] {
            let mut padded = vec![Some(0), None, Some(1)];
            padded.extend((0..rows).map(|i| {
                if nullable && i % 17 == 0 {
                    None
                } else {
                    let slot = if skew && i % 5 == 0 {
                        0
                    } else {
                        i / 3 * 8191 % unique
                    };
                    Some(base + (slot * 1024) as i128)
                }
            }));
            let value = Value::adopt(
                Arc::new(Decimal128Array::from(padded).slice(3, rows)),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            assert!(!serial.is_dense());
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                let live = budget.used();
                let tasks = actual
                    .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                    .unwrap();
                assert_eq!(tasks > 0, workers > 1);
                assert_eq!(next, expected);
                assert_same_index(&actual, &serial, &keys);
                assert_eq!(budget.used(), live);
                assert_eq!(budget.active_workers(), 0);
                actual.restore_build_order(&mut next);
                let mut restored = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut links = vec![u32::MAX; rows];
                restored.populate(&keys, &mut links);
                restored.restore_build_order(&mut links);
                assert_eq!(next, links);
                assert_same_index(&actual, &restored, &keys);
                // Removal invalidates the populated-key shortcut on both paths;
                // membership still consists of precisely the remaining keys.
                for row in 0..64 {
                    assert_eq!(actual.remove(keys.row(row)), restored.remove(keys.row(row)));
                }
                assert_eq!(actual.len(), restored.len());
                assert_eq!(actual.keys().count(), restored.keys().count());
            }
            let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            let live = budget.used();
            let blocker = budget.reserve(budget.limit() - live).unwrap();
            assert_eq!(
                actual
                    .populate_prepared(&keys, &mut next, 10, &Cancellation::default())
                    .unwrap(),
                0
            );
            assert_eq!(next, expected);
            assert_same_index(&actual, &serial, &keys);
            assert_eq!(budget.used(), budget.limit());
            drop(blocker);
            assert_eq!(budget.used(), live);
            let cancel = Cancellation::default();
            cancel.cancel();
            assert_eq!(
                actual
                    .populate_prepared(&keys, &mut next, 10, &cancel)
                    .unwrap_err(),
                "cancelled: query execution"
            );
            assert_eq!(budget.used(), live);
            assert_eq!(budget.active_workers(), 0);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn empty_small_dense_unique_and_concentrated_builds_keep_serial_population() {
        let budget = Budget::new(128 << 20);
        for numbers in [
            vec![],
            vec![None; 7],
            vec![Some(i128::MIN), Some(i128::MAX), None, Some(i128::MIN)],
            (0..262_147).map(|i| Some((i / 3) as i128)).collect(),
            (0..262_147).map(|i| Some(i as i128 * 1024)).collect(),
            (0..262_147)
                .map(|i| Some(if i == 1 { i128::MAX } else { 0 }))
                .collect(),
        ] {
            let rows = numbers.len();
            let value = Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                assert_eq!(
                    actual
                        .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                        .unwrap(),
                    0
                );
                assert_eq!(next, expected);
                assert_same_index(&actual, &serial, &keys);
            }
        }
        assert_eq!(budget.used(), 0);
    }
}
