//! Completed numeric builds, partitioned without changing duplicate order.
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

#[derive(Clone, Copy)]
struct DenseRow {
    row: u32,
    previous: u32,
}

pub(super) struct Index {
    maps: Vec<HashTable<Entry>>,
    hash: PreparedHash,
    len: usize,
}

// The collision override is carried with the index, including owned handoff;
// production hashing remains the same ahash state and complete key bytes.
struct PreparedHash {
    state: ahash::RandomState,
    #[cfg(test)]
    collide: bool,
}
impl PreparedHash {
    fn new() -> Self {
        Self {
            state: ahash::RandomState::new(),
            #[cfg(test)]
            collide: false,
        }
    }
    fn hash_one(&self, key: &[u8]) -> u64 {
        #[cfg(test)]
        if self.collide {
            return 0;
        }
        self.state.hash_one(key)
    }
}

// Dense population has cheaper serial updates than hashing. Bound its extra
// heads allocation and require enough non-NULL work per runnable lane.
fn dense_lanes(heads: usize, valid_rows: usize, workers: usize) -> usize {
    if heads > (64 << 20) / std::mem::size_of::<u32>() {
        return 0;
    }
    workers.min(16).min(valid_rows / 65_536)
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
        if let Some(tasks) = self.try_populate_dense(keys, next, workers, cancel)? {
            return Ok(tasks);
        }
        if let Some(tasks) = self.try_populate_hash(keys, next, workers, cancel)? {
            return Ok(tasks);
        }
        self.populate(keys, next);
        Ok(0)
    }

    fn try_populate_dense(
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
        let Storage::Dense {
            base,
            heads,
            count,
            keys: source,
        } = &self.storage
        else {
            return Ok(None);
        };
        assert_eq!(*count, 0);
        assert!(std::ptr::eq(*source, keys));
        // A compact, repeatedly reused slot array is cheap to populate in place.
        // Routing every input row and copying duplicate links costs more than
        // those serial slot updates when there are over four rows per slot.
        // This is only a cost gate; NULLs and duplicates still use exact keys.
        if rows > heads.len().saturating_mul(4) {
            return Ok(None);
        }
        let numbers = keys.numeric_values().unwrap();
        assert_eq!(numbers.len(), rows);
        let base = *base;
        let lanes = dense_lanes(heads.len(), keys.valid_len(), workers);
        if lanes < 2 {
            return Ok(None);
        }
        let slot_width = heads.len().div_ceil(lanes);
        let partitions = heads.len().div_ceil(slot_width);
        if partitions < 2 {
            return Ok(None);
        }
        // dense_range proved subtraction and slot bounds for these exact keys.
        // Sampling affects cost only: concentrated partitions and locally
        // repeated keys keep the cache-local serial path. Parallel routing adds
        // two input passes and a link-copy pass even for adjacent duplicates.
        let mut sampled = [0usize; 16];
        let mut samples = 0;
        let mut adjacent_repeats = 0;
        let mut adjacent_pairs = 0;
        let mut nearby = 0;
        let mut valid_pairs = 0;
        let mut valid_nearby = 0;
        for row in (0..rows).step_by(rows.div_ceil(1024)) {
            if keys.valid(row) {
                sampled[(numbers[row] - base) as usize / slot_width] += 1;
                samples += 1;
                // NULL gaps do not interrupt serial slot locality. Find the
                // preceding valid key, not the preceding sampled key. Searches
                // cannot overlap: an earlier valid sample bounds each search.
                if let Some(previous) = (0..row).rev().find(|&previous| keys.valid(previous)) {
                    // At most one cache line of heads apart: serial updates
                    // already have locality, even with unique/reversed keys.
                    // abs_diff is safe at both i128 extremes.
                    let close = usize::from(numbers[previous].abs_diff(numbers[row]) <= 16);
                    valid_pairs += 1;
                    valid_nearby += close;
                    if previous + 1 == row {
                        adjacent_pairs += 1;
                        adjacent_repeats += usize::from(numbers[previous] == numbers[row]);
                        nearby += close;
                    }
                }
            }
        }
        // With too few valid pairs, retain BP3b's physical-neighbor decision;
        // missing evidence alone must not decline a large scattered build.
        if valid_pairs >= 32 {
            adjacent_pairs = valid_pairs;
            nearby = valid_nearby;
        }
        if samples == 0
            || sampled.into_iter().max().unwrap() * 4 > samples * 3
            || adjacent_repeats * 4 > samples
            || (adjacent_pairs > 0 && nearby * 4 >= adjacent_pairs * 3)
        {
            return Ok(None);
        }
        // One 8-byte routing entry per non-NULL row, replacement dense heads,
        // and bounded task/bin metadata. The original slots and links remain
        // funded and untouched until all population workers have joined.
        let Some(bytes) = rows
            .checked_mul(std::mem::size_of::<DenseRow>())
            .and_then(|n| n.checked_add(heads.len().checked_mul(4)?))
            .and_then(|n| n.checked_add(lanes.checked_mul(partitions)?.checked_mul(1024)?))
            .and_then(|n| n.checked_add(lanes.checked_mul(8192)?))
        else {
            return Ok(None);
        };
        let budget = self._lease.budget();
        let Ok(_scratch) = budget.reserve(bytes) else {
            return Ok(None);
        };
        let width = rows.div_ceil(lanes);
        let (chunks, routing_tasks) = parallel_prepare::map(lanes, workers, &budget, |lane| {
            #[cfg(test)]
            let _task = cancel.2.task();
            let start = lane * width;
            let end = (start + width).min(rows);
            let mut counts = [0usize; 16];
            for row in start..end {
                if row % 4096 == 0 {
                    cancel.check()?;
                }
                if keys.valid(row) {
                    counts[(numbers[row] - base) as usize / slot_width] += 1;
                }
            }
            let mut bins = counts[..partitions]
                .iter()
                .map(|count| Vec::with_capacity(*count))
                .collect::<Vec<_>>();
            for row in start..end {
                if row % 4096 == 0 {
                    cancel.check()?;
                }
                if keys.valid(row) {
                    bins[(numbers[row] - base) as usize / slot_width].push(DenseRow {
                        row: row as u32,
                        previous: u32::MAX,
                    });
                }
            }
            Ok(bins)
        })?;
        let mut routed = (0..partitions)
            .map(|_| Vec::with_capacity(lanes))
            .collect::<Vec<_>>();
        // Only bin owners move here, not rows. Each partition receives source
        // chunks in canonical order, including duplicates across chunk borders.
        for chunk in chunks {
            for (partition, bin) in chunk.into_iter().enumerate() {
                routed[partition].push(bin);
            }
        }
        let mut replacement = vec![u32::MAX; heads.len()];
        let jobs = replacement
            .chunks_mut(slot_width)
            .zip(routed)
            .map(|job| Mutex::new(Some(job)))
            .collect::<Vec<_>>();
        let (built, population_tasks) =
            parallel_prepare::map(partitions, workers, &budget, |part| {
                #[cfg(test)]
                let _task = cancel.2.task();
                let (slots, mut bins) = jobs[part].lock().unwrap().take().unwrap();
                #[cfg(test)]
                if part == 0 {
                    cancel.2.checkpoint("dense allocated", cancel, &budget);
                }
                let mut count = 0;
                for bin in &mut bins {
                    for (i, entry) in bin.iter_mut().enumerate() {
                        if i % 4096 == 0 {
                            cancel.check()?;
                        }
                        let slot =
                            (numbers[entry.row as usize] - base) as usize - part * slot_width;
                        entry.previous = std::mem::replace(&mut slots[slot], entry.row);
                        count += usize::from(entry.previous == u32::MAX);
                    }
                }
                Ok((count, bins))
            })?;
        drop(jobs);
        #[cfg(test)]
        cancel.2.checkpoint("dense merge", cancel, &budget);
        cancel.check()?;
        let mut count = 0;
        for (distinct, bins) in built {
            count += distinct;
            for bin in bins {
                for (i, entry) in bin.into_iter().enumerate() {
                    if i % 4096 == 0 {
                        cancel.check()?;
                    }
                    // Unique and NULL links already contain MAX; avoid writing
                    // the entire links array on the common unique-build path.
                    if entry.previous != u32::MAX {
                        next[entry.row as usize] = entry.previous;
                        #[cfg(test)]
                        if cancel.check().is_ok() {
                            cancel.2.checkpoint("dense links copied", cancel, &budget);
                        }
                    }
                }
            }
        }
        cancel.check()?;
        self.storage = Storage::Dense {
            base,
            heads: replacement,
            count,
            keys,
        };
        self.populated_keys = Some(keys);
        self.rejection = None;
        Ok(Some(routing_tasks + population_tasks))
    }

    fn try_populate_hash(
        &mut self,
        keys: &'a Keys,
        next: &mut [u32],
        workers: usize,
        cancel: &Cancellation,
    ) -> Result<Option<usize>> {
        self.try_populate_hash_with_state(keys, next, workers, cancel, PreparedHash::new)
    }

    fn try_populate_hash_with_state(
        &mut self,
        keys: &'a Keys,
        next: &mut [u32],
        workers: usize,
        cancel: &Cancellation,
        make_hash: impl FnOnce() -> PreparedHash,
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
        // Prepared tables retain the owned lane-fusion handoff for unique keys.
        // Avoid paying for partitioning an almost constant build; uniqueness
        // itself is never inferred from this bounded cost sample.
        let mut changes = 0;
        for row in (1..rows).step_by(rows.div_ceil(1024)) {
            if keys.valid(row - 1) && keys.valid(row) && numbers[row - 1] != numbers[row] {
                changes += 1;
            }
        }
        if changes < 16 {
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
        let hash = make_hash();
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

    // The serial head of every row's key, looked up once per fixture rather
    // than once per worker count; assert_heads then checks exactly what
    // assert_same_index checks against that serial index.
    fn heads_of(index: &JoinIndex<'_>, keys: &Keys) -> Vec<Option<u32>> {
        (0..keys.len())
            .map(|row| index.get(keys.row(row)).copied())
            .collect()
    }

    fn assert_heads(actual: &JoinIndex<'_>, len: usize, heads: &[Option<u32>], keys: &Keys) {
        assert_eq!(actual.len(), len);
        assert_eq!(heads.len(), keys.len());
        for (row, head) in heads.iter().enumerate() {
            assert_eq!(actual.get(keys.row(row)).copied(), *head);
        }
    }

    #[test]
    fn dense_cost_caps_replacement_and_lanes_by_valid_work() {
        let cap = (64 << 20) / std::mem::size_of::<u32>();
        assert_eq!(dense_lanes(cap, 1_048_576, 10), 10);
        assert_eq!(dense_lanes(cap + 1, 1_048_576, 10), 0);
        assert_eq!(dense_lanes(262_144, 131_071, 10), 1);
        assert_eq!(dense_lanes(262_144, 131_072, 10), 2);
        assert_eq!(dense_lanes(262_144, 262_144, 10), 4);
        assert_eq!(dense_lanes(262_144, usize::MAX, 32), 16);
        assert_eq!(dense_lanes(262_144, 0, 10), 0);
    }

    // Populate one build at workers 1/2/4/10. Dispatch must match the expected
    // admission exactly; whichever path runs, links, heads, funding and worker
    // cleanup must equal serial population. Workers 1 always stays serial.
    fn check_dispatch(numbers: Vec<Option<i128>>, dense: bool, admitted: bool, label: &str) {
        let rows = numbers.len();
        let budget = Budget::new(128 << 20);
        let value = Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
        let keys = Keys::new(&[value], rows, &budget).unwrap();
        let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
        assert_eq!(serial.is_dense(), dense, "{label}");
        let mut expected = vec![u32::MAX; rows];
        serial.populate(&keys, &mut expected);
        let heads = heads_of(&serial, &keys);
        for workers in [1, 2, 4, 10] {
            let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            let baseline = budget.used();
            let peak = budget.peak();
            let tasks = actual
                .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                .unwrap();
            assert_eq!(tasks > 0, admitted && workers > 1, "{label}/{workers}");
            assert_eq!(next, expected, "{label}/{workers}");
            assert_heads(&actual, serial.len(), &heads, &keys);
            assert_eq!(budget.used(), baseline);
            if tasks == 0 {
                assert_eq!(budget.peak(), peak);
            }
            assert!(budget.peak() <= budget.limit());
            assert_eq!(budget.active_workers(), 0);
        }
        drop(serial);
        drop(keys);
        assert_eq!(budget.used(), 0);
    }

    // 8191 is prime, so `row * 8191 % rows` permutes 0..rows whenever rows is
    // not a multiple of 8191 (true for every row count used here); neighbouring
    // rows (and neighbouring samples) therefore hold distant keys.
    fn scattered(rows: usize) -> impl Iterator<Item = i128> {
        (0..rows).map(move |row| (row * 8191 % rows) as i128)
    }

    #[test]
    fn dense_and_hash_dispatch_start_at_262144_build_rows() {
        for rows in [262_143, 262_144] {
            let admitted = rows == 262_144;
            check_dispatch(
                scattered(rows).map(Some).collect(),
                true,
                admitted,
                "dense rows",
            );
            check_dispatch(
                scattered(rows).map(|key| Some(key * 1024)).collect(),
                false,
                admitted,
                "hash rows",
            );
        }
    }

    #[test]
    fn dense_dispatch_reaches_ten_lanes() {
        // 655,360 valid rows give min(workers, 16, 655_360 / 65_536) = 10 lanes at
        // ten workers (and 2/4 lanes at two/four), restoring coverage of dense
        // population with more than four lanes (the former 524,288-row fixtures
        // reached seven). Exact links, heads and funding are checked per worker.
        check_dispatch(
            scattered(655_360).map(Some).collect(),
            true,
            true,
            "ten lanes",
        );
    }

    #[test]
    fn dense_dispatch_needs_two_lanes_of_valid_rows_and_four_rows_per_slot_at_most() {
        let rows = 262_144;
        // 131,071 valid rows give one 65,536-row lane; 131,072 give two.
        for valid in [131_071, 131_072] {
            check_dispatch(
                scattered(rows)
                    .map(|key| (key < valid).then_some(key))
                    .collect(),
                true,
                valid == 131_072,
                "valid lanes",
            );
        }
        // Scattered duplicates: exactly four rows per slot is admitted, and one
        // slot fewer (just over four rows per slot) keeps serial population.
        for slots in [65_536, 65_535] {
            check_dispatch(
                scattered(rows).map(|key| Some(key % slots)).collect(),
                true,
                slots == 65_536,
                "rows per slot",
            );
        }
    }

    #[test]
    fn dense_dispatch_declines_above_75_percent_concentration_nearby_or_25_percent_repeats() {
        let rows = 262_144;
        let step = rows / 1024;
        // Samples are rows 0, 256, ... At most 75% of samples in the lowest
        // partition is admitted; one more sample there declines. The rest go
        // to the highest partition for every lane count (two to four).
        for low in [768, 769] {
            let mut numbers = scattered(rows).map(Some).collect::<Vec<_>>();
            for sample in 0..1024 {
                numbers[sample * step] = Some(if sample < low {
                    sample as i128
                } else {
                    (rows - 1 - sample) as i128
                });
            }
            check_dispatch(numbers, true, low == 768, "concentration");
        }
        // 1,023 sampled pairs. Declining needs nearby*4 >= pairs*3: 767 close
        // pairs are admitted, 768 decline.
        for close in [767, 768] {
            let mut numbers = scattered(rows).map(Some).collect::<Vec<_>>();
            for sample in 1..=close {
                numbers[sample * step - 1] = Some(numbers[sample * step].unwrap() - 1);
            }
            check_dispatch(numbers, true, close == 767, "nearby pairs");
        }
        // Equal adjacent keys: 256 of 1,024 samples is admitted, 257 decline.
        for repeats in [256, 257] {
            let mut numbers = scattered(rows).map(Some).collect::<Vec<_>>();
            for sample in 1..=repeats {
                numbers[sample * step - 1] = numbers[sample * step];
            }
            check_dispatch(numbers, true, repeats == 256, "adjacent repeats");
        }
    }

    #[test]
    fn hash_dispatch_needs_16_sampled_key_changes() {
        let rows = 262_144;
        // An almost constant wide-range build: each changed sampled pair is
        // (256j, 256j+1). Fifteen changes decline; sixteen are admitted, and
        // the constant key's long duplicate chain then crosses every lane.
        for changes in [15, 16] {
            let mut numbers = vec![Some(0); rows];
            for change in 0..changes {
                numbers[change * 256 + 1] = Some((change as i128 + 1) << 40);
            }
            check_dispatch(numbers, false, changes == 16, "hash changes");
        }
    }

    #[test]
    fn ordered_unique_dense_builds_and_null_heavy_builds_keep_serial_without_scratch() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        for (reverse, sparse_valid) in [(false, false), (true, false), (false, true)] {
            let value = Value::adopt(
                Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                    (!sparse_valid || row % 4 == 0).then_some(
                        i128::MIN
                            + if sparse_valid {
                                (row * 8191 % rows) as i128
                            } else if reverse {
                                (rows - row) as i128
                            } else {
                                row as i128
                            },
                    )
                }))),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            assert_eq!(
                keys.valid_len(),
                if sparse_valid { rows.div_ceil(4) } else { rows }
            );
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let heads = heads_of(&serial, &keys);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                assert!(actual.is_dense());
                let mut next = vec![u32::MAX; rows];
                let baseline = budget.used();
                let peak = budget.peak();
                assert_eq!(
                    actual
                        .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                        .unwrap(),
                    0
                );
                assert_eq!(next, expected);
                assert_heads(&actual, serial.len(), &heads, &keys);
                assert_eq!(budget.used(), baseline);
                assert_eq!(budget.peak(), peak);
                assert_eq!(budget.active_workers(), 0);
            }
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn null_separated_local_dense_keys_keep_serial_without_scratch() {
        let rows = 262_144;
        for workers in [1, 2, 4, 10] {
            let budget = Budget::new(128 << 20);
            let value = Value::adopt(
                Arc::new(Decimal128Array::from_iter(
                    (0..rows).map(|row| (row % 2 == 0).then_some((row / 2) as i128)),
                )),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            assert_eq!(keys.valid_len(), 131_072);
            assert_eq!(
                dense_lanes(131_072, keys.valid_len(), workers),
                workers.min(2)
            );
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            assert!(actual.is_dense());
            let mut next = vec![u32::MAX; rows];
            let baseline = budget.used();
            let peak = budget.peak();
            assert_eq!(
                actual
                    .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                    .unwrap(),
                0
            );
            assert_eq!(next, expected);
            assert_same_index(&actual, &serial, &keys);
            for (row, link) in next.iter().enumerate() {
                assert_eq!(*link, u32::MAX);
                assert_eq!(
                    actual.get(keys.row(row)).copied(),
                    (row % 2 == 0).then_some(row as u32)
                );
            }
            assert_eq!(budget.used(), baseline);
            assert_eq!(budget.peak(), peak);
            assert_eq!(budget.active_workers(), 0);
            drop(actual);
            drop(serial);
            drop(keys);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn scattered_dense_keys_with_null_gaps_keep_parallel_population() {
        // The smallest admitted build: 262,144 rows, so one sample every 256
        // rows. Every sample is the first row of a 256-row block; the block's
        // trailing NULL run precedes it, exactly as at 524,288 rows/512 blocks.
        let rows = 262_144;
        let block = rows / 1024;
        for null_gap in [1, 64] {
            for duplicates in [false, true] {
                let budget = Budget::new(128 << 20);
                let value = Value::adopt(
                    Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                        // An odd multiplier permutes this power-of-two
                        // range. Include NULL runs longer than BP3c's
                        // bounded backward search, with no loss of locality
                        // evidence or blanket decline for missing pairs.
                        let slot = row * 8191 % rows;
                        (row % block < block - null_gap)
                            .then_some(i128::MIN + (slot / if duplicates { 2 } else { 1 }) as i128)
                    }))),
                    &budget,
                )
                .unwrap();
                let keys = Keys::new(&[value], rows, &budget).unwrap();
                // Two to three lanes remain: valid work does not reach four.
                assert!(keys.valid_len() >= 2 * 65_536);
                let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut expected = vec![u32::MAX; rows];
                serial.populate(&keys, &mut expected);
                let heads = heads_of(&serial, &keys);
                for workers in [1, 2, 4, 10] {
                    // Each worker count starts from, and must return to, the same live credit.
                    let live = budget.used();
                    let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                    assert!(actual.is_dense());
                    let mut next = vec![u32::MAX; rows];
                    let baseline = budget.used();
                    let tasks = actual
                        .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                        .unwrap();
                    assert_eq!(tasks > 0, workers > 1, "{null_gap}/{duplicates}/{workers}");
                    assert_eq!(next, expected);
                    assert_heads(&actual, serial.len(), &heads, &keys);
                    for (row, link) in next.iter().enumerate() {
                        if !keys.valid(row) {
                            assert_eq!(*link, u32::MAX);
                            assert_eq!(actual.get(keys.row(row)), None);
                        }
                    }
                    assert_eq!(budget.used(), baseline);
                    assert!(budget.peak() <= budget.limit());
                    assert_eq!(budget.active_workers(), 0);
                    drop(actual);
                    assert_eq!(budget.used(), live);
                }
                drop(serial);
                drop(keys);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn sparse_locality_evidence_keeps_previous_dispatch_until_32_pairs() {
        // 262,144 rows sample every 256th row; each 256-row block mirrors the
        // former 512-row block, so the sampled pairs are unchanged.
        let rows: usize = 262_144;
        let block = rows / 1024;
        for pairs in [31, 32] {
            let budget = Budget::new(128 << 20);
            let value = Value::adopt(
                Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                    let sample = row / block;
                    let valid = match row % block {
                        0 => sample % 32 == 1 && sample / 32 < pairs,
                        offset if offset == block - 1 => false,
                        _ => true,
                    };
                    // Every sampled pair skips one NULL and differs by
                    // one; all other keys are scattered. BP3b saw no pairs.
                    let slot = if row % block == block - 2 {
                        ((row + 2) * 8191 % rows).saturating_sub(1)
                    } else {
                        row * 8191 % rows
                    };
                    valid.then_some(slot as i128)
                }))),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let heads = heads_of(&serial, &keys);
            for workers in [1, 2, 4, 10] {
                // Each worker count starts from, and must return to, the same live credit.
                let live = budget.used();
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                assert!(actual.is_dense());
                let mut next = vec![u32::MAX; rows];
                let baseline = budget.used();
                let peak = budget.peak();
                let tasks = actual
                    .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                    .unwrap();
                assert_eq!(tasks > 0, workers > 1 && pairs == 31, "{pairs}/{workers}");
                assert_eq!(next, expected);
                assert_heads(&actual, serial.len(), &heads, &keys);
                assert_eq!(budget.used(), baseline);
                if tasks == 0 {
                    assert_eq!(budget.peak(), peak);
                }
                assert_eq!(budget.active_workers(), 0);
                drop(actual);
                assert_eq!(budget.used(), live);
            }
            drop(serial);
            drop(keys);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn prepared_hash_collisions_survive_owned_handoff_with_exact_heads_and_links() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        let value = Value::adopt(
            Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                (row % 17 != 0).then_some((row % 32) as i128 * 1_000_000)
            }))),
            &budget,
        )
        .unwrap();
        let keys = Arc::new(Keys::new(&[value], rows, &budget).unwrap());
        let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
        let mut expected = vec![u32::MAX; rows];
        serial.populate(&keys, &mut expected);
        let heads = heads_of(&serial, &keys);
        for workers in [1, 2, 4, 10] {
            let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            let baseline = budget.used();
            let tasks = actual
                .try_populate_hash_with_state(
                    &keys,
                    &mut next,
                    workers,
                    &Cancellation::default(),
                    || PreparedHash {
                        state: ahash::RandomState::with_seeds(1, 2, 3, 4),
                        collide: true,
                    },
                )
                .unwrap();
            if workers == 1 {
                assert!(tasks.is_none());
                actual.populate(&keys, &mut next);
            } else {
                assert!(tasks.unwrap() > 0);
                assert!(matches!(actual.storage, Storage::Prepared { .. }));
            }
            assert_eq!(next, expected);
            assert_heads(&actual, serial.len(), &heads, &keys);
            assert_eq!(budget.used(), baseline);
            let owned = super::super::owned::OwnedJoinIndex::from_borrowed(
                actual,
                keys.clone(),
                next,
                &budget,
            )
            .ok()
            .expect("funded prepared handoff");
            assert_eq!(owned.links(), expected);
            assert_eq!(owned.len(), 32);
            for row in 0..64 {
                assert_eq!(
                    owned.head(keys.row(row)),
                    serial.get(keys.row(row)).copied()
                );
            }
            assert_eq!(owned.head(&(-1i128).to_ne_bytes()), None);
            assert_eq!(owned.head(&(32_000_000i128).to_ne_bytes()), None);
            assert_eq!(budget.active_workers(), 0);
        }
        drop(serial);
        drop(keys);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dense_cancellation_after_duplicate_link_copy_discards_partial_build() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        let value = Value::adopt(
            Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                (row % 17 != 0).then_some((row * 8191 % (rows / 3)) as i128)
            }))),
            &budget,
        )
        .unwrap();
        let keys = Keys::new(&[value], rows, &budget).unwrap();
        for workers in [2, 4, 10] {
            let mut index = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            let baseline = budget.used();
            let cancel = Cancellation::default();
            cancel.2.arm("dense links copied");
            assert_eq!(
                index
                    .populate_prepared(&keys, &mut next, workers, &cancel)
                    .unwrap_err(),
                "cancelled: query execution"
            );
            assert!(next.iter().any(|link| *link != u32::MAX));
            assert!(index.is_empty());
            assert!(index.populated_keys.is_none());
            cancel.2.assert_drained(&budget, baseline);
        }
        drop(keys);
        assert_eq!(budget.used(), 0);
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
    fn empty_small_and_concentrated_builds_decline_dense_and_unique_builds_dispatch() {
        let budget = Budget::new(128 << 20);
        for (fixture, numbers) in [
            vec![],
            vec![None; 7],
            vec![Some(i128::MIN), Some(i128::MAX), None, Some(i128::MIN)],
            (0..262_147).map(|i| Some((i / 3) as i128)).collect(),
            (0..262_147).map(|i| Some(i as i128 * 1024)).collect(),
            (0..262_147)
                .map(|i| Some(if i == 1 { i128::MAX } else { 0 }))
                .collect(),
        ]
        .into_iter()
        .enumerate()
        {
            let rows = numbers.len();
            let value = Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let heads = heads_of(&serial, &keys);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                assert_eq!(
                    actual
                        .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                        .unwrap()
                        > 0,
                    workers > 1 && fixture == 4
                );
                assert_eq!(next, expected);
                assert_heads(&actual, serial.len(), &heads, &keys);
            }
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn compact_and_locally_repeated_dense_builds_keep_serial_population() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        for clustered in [false, true] {
            let value = Value::adopt(
                Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                    (row % 17 != 0).then_some(
                        i128::MIN
                            + if clustered {
                                // A wide range isolates the adjacent-repeat
                                // gate from the compact-domain gate.
                                (row / 4 * 16) as i128
                            } else {
                                // Dispersed duplicates isolate the domain gate
                                // from both locality and partition-skew gates.
                                (row * 8191 % (rows / 8)) as i128
                            },
                    )
                }))),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            assert!(serial.is_dense());
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let heads = heads_of(&serial, &keys);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                let baseline = budget.used();
                let peak = budget.peak();
                assert_eq!(
                    actual
                        .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                        .unwrap(),
                    0
                );
                assert_eq!(next, expected);
                assert_heads(&actual, serial.len(), &heads, &keys);
                assert_eq!(budget.used(), baseline);
                assert_eq!(budget.peak(), peak);
                assert_eq!(budget.active_workers(), 0);
                let mut expected_ordered = expected.clone();
                // Check the LEFT-join encounter-order restoration as well as
                // the ordinary newest-first chains, including NULL links.
                let mut ordered = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                ordered.populate(&keys, &mut expected_ordered);
                ordered.restore_build_order(&mut expected_ordered);
                actual.restore_build_order(&mut next);
                assert_eq!(next, expected_ordered);
                assert_same_index(&actual, &ordered, &keys);
            }
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dense_and_unique_population_preserves_links_representatives_and_owned_handoff() {
        let rows = 262_147;
        let budget = Budget::new(256 << 20);
        for (base, stride, duplicates, nullable, skew) in [
            (i128::MIN, 1, false, false, false),
            (i128::MAX - rows as i128, 1, true, true, false),
            (-1_000_000, 1, true, true, true),
            (i128::MIN, 1024, false, true, false),
            (i128::MAX - rows as i128 * 1024, 1024, false, false, false),
        ] {
            let mut padded = vec![Some(0), None, Some(1)];
            padded.extend((0..rows).map(|row| {
                if nullable && row % 17 == 0 {
                    None
                } else {
                    let slot = if skew && row % 5 == 0 {
                        0
                    } else if duplicates {
                        row * 8191 % (rows / 3)
                    } else {
                        row * 8191 % rows
                    };
                    Some(base + slot as i128 * stride)
                }
            }));
            let value = Value::adopt(
                Arc::new(Decimal128Array::from(padded).slice(3, rows)),
                &budget,
            )
            .unwrap();
            let keys = Arc::new(Keys::new(&[value], rows, &budget).unwrap());
            let mut serial = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut expected = vec![u32::MAX; rows];
            serial.populate(&keys, &mut expected);
            let heads = heads_of(&serial, &keys);
            for workers in [1, 2, 4, 10] {
                let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                let mut next = vec![u32::MAX; rows];
                let baseline = budget.used();
                let tasks = actual
                    .populate_prepared(&keys, &mut next, workers, &Cancellation::default())
                    .unwrap();
                assert_eq!(tasks > 0, workers > 1);
                assert_eq!(actual.is_dense(), stride == 1);
                assert_eq!(next, expected);
                assert_heads(&actual, serial.len(), &heads, &keys);
                if let (Storage::Prepared { index, .. }, Storage::Hash(original)) =
                    (&actual.storage, &serial.storage)
                {
                    for entry in index.maps.iter().flat_map(|map| map.iter()) {
                        let first = keys.row(entry.key_row as usize);
                        assert_eq!(original.key_row(first), Some(entry.key_row));
                    }
                }
                assert_eq!(budget.used(), baseline);
                assert_eq!(budget.active_workers(), 0);
                let blocker = budget.reserve(budget.limit() - baseline).unwrap();
                let (actual_back, links_back) =
                    match super::super::owned::OwnedJoinIndex::from_borrowed(
                        actual,
                        keys.clone(),
                        next,
                        &budget,
                    ) {
                        Err(fallback) => fallback,
                        Ok(_) => panic!("handoff must decline without envelope funding"),
                    };
                drop(blocker);
                let owned = super::super::owned::OwnedJoinIndex::from_borrowed(
                    actual_back,
                    keys.clone(),
                    links_back,
                    &budget,
                )
                .ok()
                .expect("funded handoff");
                assert_eq!(owned.links(), expected);
                assert_eq!(owned.len(), serial.len());
                for (row, head) in heads.iter().enumerate() {
                    assert_eq!(owned.head(keys.row(row)), *head);
                }
                assert!(budget.peak() <= budget.limit());
            }
            // Optional scratch denial must preserve today's successful path.
            let mut actual = JoinIndex::new(&keys, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            let baseline = budget.used();
            let blocker = budget.reserve(budget.limit() - baseline).unwrap();
            assert_eq!(
                actual
                    .populate_prepared(&keys, &mut next, 10, &Cancellation::default(),)
                    .unwrap(),
                0
            );
            assert_eq!(next, expected);
            assert_same_index(&actual, &serial, &keys);
            drop(blocker);
            assert_eq!(budget.used(), baseline);
            actual.restore_build_order(&mut next);
            serial.restore_build_order(&mut expected);
            assert_eq!(next, expected);
            assert_same_index(&actual, &serial, &keys);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dense_and_unique_midflight_cancellation_never_publishes_and_drains_workers() {
        let rows = 262_147;
        let budget = Budget::new(128 << 20);
        for dense in [true, false] {
            let value = Value::adopt(
                Arc::new(Decimal128Array::from_iter((0..rows).map(|row| {
                    (row % 17 != 0)
                        .then_some((row * 8191 % rows) as i128 * if dense { 1 } else { 1024 })
                }))),
                &budget,
            )
            .unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            for workers in [2, 4, 10] {
                for point in if dense {
                    ["dense allocated", "dense merge"]
                } else {
                    ["index allocated", "index merge"]
                } {
                    let mut index = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                    let mut next = vec![u32::MAX; rows];
                    let baseline = budget.used();
                    let cancel = Cancellation::default();
                    cancel.2.arm(point);
                    assert_eq!(
                        index
                            .populate_prepared(&keys, &mut next, workers, &cancel,)
                            .unwrap_err(),
                        "cancelled: query execution"
                    );
                    cancel.2.assert_drained(&budget, baseline);
                    assert!(index.is_empty());
                    assert!(index.populated_keys.is_none());
                    assert!(next.iter().all(|link| *link == u32::MAX));
                }
            }
        }
        assert_eq!(budget.used(), 0);
    }
}
