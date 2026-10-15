//! Immutable equality index for lane probes and owned pipeline jobs.
//! Hash entries retain row IDs and resolve key bytes through one owned Keys Arc.
//! Duplicate chains remain newest-first. Production lanes admit only unique
//! builds; production LEFT uses JoinIndex::restore_build_order.
#[cfg(test)]
use super::Result;
use super::{Budget, Entry, Keys, Reservation, slot};
#[cfg(test)]
use crate::spill::Cancellation;
use hashbrown::HashTable;
use std::sync::Arc;

mod pair;

#[derive(Default)]
#[cfg(test)]
struct BuildOptions {
    allow_dense: bool,
    collision_hash: bool,
    exercise_growth: bool,
    cancel_at: Option<usize>,
}
enum Storage {
    Dense {
        base: i128,
        heads: Vec<u32>,
        count: usize,
    },
    Hash(HashTable<Entry>),
    Pair(pair::PairIndex),
}

/// Buffers precede their backing leases in destruction order. No borrowed bytes
/// or mutable probe counters cross the ownership boundary.
pub(crate) struct OwnedJoinIndex {
    storage: Storage,
    next: Vec<u32>,
    keys: Arc<Keys>,
    hash: ahash::RandomState,
    collision_hash: bool,
    rejection: Option<crate::membership::NumericMembership>,
    _lease: Reservation,
}

#[cfg(test)]
fn row_count(rows: usize) -> Result<()> {
    // MAX is the end-of-chain sentinel, leaving IDs 0..MAX-1 usable.
    if rows > u32::MAX as usize {
        Err("join build row count exceeds u32 row IDs".into())
    } else {
        Ok(())
    }
}
fn hash_bytes(hash: &ahash::RandomState, collision_hash: bool, key: &[u8]) -> u64 {
    if collision_hash {
        0
    } else {
        hash.hash_one(key)
    }
}
#[cfg(test)]
fn range(keys: &Keys, rows: usize, cancellation: &Cancellation) -> Result<Option<(i128, usize)>> {
    let Some(numbers) = keys.numeric_values() else {
        return Ok(None);
    };
    let mut bounds: Option<(i128, i128)> = None;
    for (row, number) in numbers.iter().copied().enumerate() {
        if row % 1024 == 0 {
            cancellation.check()?;
        }
        if keys.valid(row) {
            bounds = Some(match bounds {
                None => (number, number),
                Some((low, high)) => (low.min(number), high.max(number)),
            });
        }
    }
    let result = bounds.and_then(|(low, high)| {
        let count = usize::try_from(high.checked_sub(low)?.checked_add(1)?).ok()?;
        (count <= rows.saturating_mul(12)).then_some((low, count))
    });
    Ok(result)
}
impl OwnedJoinIndex {
    /// Move occupied hash storage only within the compacted retained-credit bound.
    /// Otherwise compact distinct entries, preserving keys, heads and links.
    // A denied optional reservation must return the fallback without a Box allocation.
    #[allow(clippy::result_large_err)]
    pub(crate) fn from_borrowed<'a>(
        index: super::JoinIndex<'a>,
        keys: Arc<Keys>,
        next: Vec<u32>,
        budget: &Budget,
    ) -> std::result::Result<Self, (super::JoinIndex<'a>, Vec<u32>)> {
        assert!(keys.funded_by(budget));
        assert_eq!(keys.len(), next.len());
        // Prepared hash admission witnesses duplicates. Production lane fusion
        // admits unique builds only, so keep the existing borrowed fallback.
        if matches!(index.storage, super::Storage::Prepared { .. }) {
            return Err((index, next));
        }
        #[cfg(test)]
        if matches!(index.storage, super::Storage::Inserted(_)) {
            return Err((index, next));
        }
        // A unique dense component can address a numeric pair without hashing.
        // Build it while the original table is still funded and intact: an
        // unsuitable domain, collision or denial keeps the existing handoff.
        if let super::Storage::Hash(old) = &index.storage
            && let Some(pair) = pair::PairIndex::new(&keys, old, budget)
        {
            let super::JoinIndex {
                storage,
                rejection,
                mut _lease,
                ..
            } = index;
            drop(storage);
            _lease.release_excess(next.capacity().saturating_mul(4).saturating_add(256));
            return Ok(Self {
                storage: Storage::Pair(pair),
                next,
                keys,
                hash: ahash::RandomState::new(),
                collision_hash: false,
                rejection,
                _lease,
            });
        }
        let compact_bytes = index.len().checked_mul(64).and_then(|n| n.checked_add(512));
        let links_bytes = next
            .capacity()
            .checked_mul(4)
            .and_then(|n| n.checked_add(256));
        // The moved allocation keeps all of its credit. Admit it only when
        // that credit plus its envelope is no larger than the base's compact
        // table plus retained links. Low occupancy must free the old storage.
        // All-distinct, non-NULL builds request exactly the same capacity as
        // compaction, including small-table minima. Decline other occupancies
        // conservatively: capacity() can also lose growth credit to tombstones,
        // so it alone does not measure the allocation we would retain.
        let same_capacity = index.len() == keys.len();
        let move_hash = same_capacity
            && compact_bytes
                .zip(links_bytes)
                .is_some_and(|(table, links)| {
                    index
                        ._lease
                        .bytes()
                        .checked_add(256)
                        .zip(table.checked_add(links))
                        .is_some_and(|(moved, compact)| moved <= compact)
                });
        let bytes = if index.is_dense() || move_hash {
            Some(256)
        } else {
            compact_bytes
        };
        let Some(mut lease) = bytes.and_then(|bytes| budget.reserve(bytes).ok()) else {
            return Err((index, next));
        };
        let count = index.len();
        let mut hash = ahash::RandomState::new();
        let super::JoinIndex {
            storage,
            rejection,
            mut _lease,
            ..
        } = index;
        let storage = match storage {
            super::Storage::Dense {
                base,
                heads,
                count,
                keys: source,
            } => {
                assert!(std::ptr::eq(source, keys.as_ref()));
                Storage::Dense { base, heads, count }
            }
            super::Storage::Hash(old) => {
                assert!(std::ptr::eq(old.keys, keys.as_ref()));
                hash = old.hash.clone();
                debug_assert!(old.map.iter().all(|entry| {
                    keys.row(entry.key_row as usize) == keys.row(entry.head as usize)
                }));
                if move_hash {
                    Storage::Hash(old.map)
                } else {
                    let mut map = HashTable::with_capacity(old.len());
                    for entry in old.map.iter() {
                        map.insert_unique(
                            hash.hash_one(keys.row(entry.key_row as usize)),
                            *entry,
                            |entry| hash.hash_one(keys.row(entry.key_row as usize)),
                        );
                    }
                    drop(old);
                    _lease.release_excess(links_bytes.expect("funded link capacity"));
                    Storage::Hash(map)
                }
            }
            super::Storage::Prepared { .. } => unreachable!("returned borrowed fallback above"),
            #[cfg(test)]
            super::Storage::Inserted(_) => unreachable!("returned test insertion oracle above"),
        };
        lease.absorb(_lease);
        let owned = Self {
            storage,
            next,
            keys,
            hash,
            collision_hash: false,
            rejection,
            _lease: lease,
        };
        debug_assert_eq!(owned.len(), count);
        Ok(owned)
    }
    #[cfg(test)]
    pub(crate) fn new(
        keys: Arc<Keys>,
        rows: usize,
        budget: &Budget,
        allow_dense: bool,
        cancellation: &Cancellation,
    ) -> Result<Self> {
        Self::build(
            keys,
            rows,
            budget,
            cancellation,
            BuildOptions {
                allow_dense,
                ..BuildOptions::default()
            },
        )
    }
    #[cfg(test)]
    fn build(
        keys: Arc<Keys>,
        rows: usize,
        budget: &Budget,
        cancellation: &Cancellation,
        options: BuildOptions,
    ) -> Result<Self> {
        let BuildOptions {
            allow_dense,
            collision_hash,
            exercise_growth,
            cancel_at,
        } = options;
        row_count(rows)?;
        if keys.len() != rows {
            return Err("owned join key row count mismatch".into());
        }
        if !keys.funded_by(budget) {
            return Err("owned join key funding domain mismatch".into());
        }
        cancellation.check()?;
        let dense = if allow_dense {
            range(&keys, rows, cancellation)?
        } else {
            None
        };
        // Includes duplicate links, hash/dense capacity, ownership envelopes, and
        // peak old/new hash allocations if growth is exercised in tests. It does
        // not charge key bytes again: Keys owns its existing buffer reservation.
        let bytes = match dense {
            Some((_, count)) => count
                .checked_mul(4)
                .and_then(|n| rows.checked_mul(4).and_then(|r| n.checked_add(r))),
            None => rows.checked_mul(64),
        }
        .and_then(|n| n.checked_add(512))
        .ok_or("owned join index size overflow")?;
        let lease = budget.reserve(bytes)?;
        let storage = match dense {
            Some((base, count)) => Storage::Dense {
                base,
                heads: vec![u32::MAX; count],
                count: 0,
            },
            None => Storage::Hash(HashTable::with_capacity(if exercise_growth {
                0
            } else {
                rows
            })),
        };
        let mut index = Self {
            storage,
            next: vec![u32::MAX; rows],
            keys,
            hash: ahash::RandomState::new(),
            collision_hash,
            rejection: None,
            _lease: lease,
        };
        for row in 0..rows {
            if cancel_at == Some(row) {
                cancellation.cancel();
            }
            if row % 1024 == 0 {
                cancellation.check()?;
            }
            if !index.keys.valid(row) {
                continue;
            }
            let row_id = u32::try_from(row).map_err(|_| "join row ID overflow")?;
            let key = index.keys.row(row);
            match &mut index.storage {
                Storage::Dense { base, heads, count } => {
                    let position = slot(key, *base).ok_or("invalid owned dense join key")?;
                    let head = heads
                        .get_mut(position)
                        .ok_or("owned dense join key outside range")?;
                    let old = std::mem::replace(head, row_id);
                    *count += usize::from(old == u32::MAX);
                    index.next[row] = old;
                }
                Storage::Hash(map) => {
                    let hash = hash_bytes(&index.hash, collision_hash, key);
                    if let Some(entry) =
                        map.find_mut(hash, |e| index.keys.row(e.key_row as usize) == key)
                    {
                        index.next[row] = std::mem::replace(&mut entry.head, row_id);
                    } else {
                        // Rehash must use key bytes, never the row ID's Hash impl.
                        map.insert_unique(
                            hash,
                            Entry {
                                key_row: row_id,
                                head: row_id,
                            },
                            |e| {
                                hash_bytes(
                                    &index.hash,
                                    collision_hash,
                                    index.keys.row(e.key_row as usize),
                                )
                            },
                        );
                    }
                }
                Storage::Pair(_) => unreachable!("pair index is a completed-table handoff"),
            }
        }
        cancellation.check()?;
        Ok(index)
    }
    pub(crate) fn head(&self, key: &[u8]) -> Option<u32> {
        match &self.storage {
            Storage::Dense { base, heads, .. } => heads
                .get(slot(key, *base)?)
                .copied()
                .filter(|h| *h != u32::MAX),
            Storage::Hash(map) => super::hash_head(
                map,
                &self.keys,
                hash_bytes(&self.hash, self.collision_hash, key),
                key,
            )
            .copied(),
            Storage::Pair(pair) => pair.head(key, &self.keys),
        }
    }
    /// Retain the borrowed view's exact negative test; positive bits still check key equality.
    pub(crate) fn probe(&self, key: &[u8], counts: &mut super::ProbeCounts) -> Option<u32> {
        if let Some(bitmap) = &self.rejection {
            counts.probes += 1;
            let member = <[u8; 16]>::try_from(key)
                .ok()
                .is_some_and(|n| bitmap.contains(i128::from_ne_bytes(n)));
            if !member {
                counts.rejected += 1;
                return None;
            }
        }
        self.head(key)
    }
    #[cfg(test)]
    pub(crate) fn next(&self, row: u32) -> Option<u32> {
        self.next
            .get(row as usize)
            .copied()
            .filter(|r| *r != u32::MAX)
    }
    pub(crate) fn links(&self) -> &[u32] {
        &self.next
    }
    pub(crate) fn len(&self) -> usize {
        match &self.storage {
            Storage::Dense { count, .. } => *count,
            Storage::Hash(map) => map.len(),
            Storage::Pair(pair) => pair.len(),
        }
    }
    #[cfg(test)]
    pub(crate) fn is_dense(&self) -> bool {
        matches!(self.storage, Storage::Dense { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Value, join_index::JoinIndex};
    use arrow::array::{ArrayRef, Decimal128Array, StringArray};

    #[test]
    fn sparse_handoff_keeps_base_credit_and_later_reservation_headroom() {
        let budget = Budget::new(1 << 20);
        let mut numbers = vec![None; 1024];
        numbers[0] = Some(0);
        numbers[1] = Some(1_000_000);
        let encoded = keys(
            vec![Arc::new(Decimal128Array::from(numbers))],
            1024,
            &budget,
            true,
        );
        let key_bytes = budget.used();
        let mut old = JoinIndex::new(&encoded, 1024, &budget, true).unwrap();
        let mut next = vec![u32::MAX; 1024];
        old.populate(&encoded, &mut next);
        assert!(!old.is_dense());
        assert_eq!(old._lease.bytes(), 65_792);
        assert!(next.iter().all(|link| *link == u32::MAX));
        // U includes the unrelated live reservation: exactly U + 1,280 fits.
        let held = budget
            .reserve(budget.limit() - budget.used() - 1280)
            .unwrap();
        let u = budget.used();
        assert_eq!(budget.limit(), u + 1280);
        let owned = OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget)
            .unwrap_or_else(|_| panic!("funded compaction must succeed"));
        assert_eq!(owned._lease.bytes(), 4992);
        assert_eq!(budget.used(), key_bytes + held.bytes() + 4992);
        assert_eq!(owned.head(&0_i128.to_ne_bytes()), Some(0));
        assert_eq!(owned.head(&1_000_000_i128.to_ne_bytes()), Some(1));
        assert_eq!(owned.head(&1_i128.to_ne_bytes()), None);
        let later = budget.reserve(4096).expect("base post-transfer headroom");
        drop((later, owned, held, encoded));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn compacted_duplicate_chains_keep_handwritten_heads_and_order() {
        for reverse in [false, true] {
            for deny in [false, true] {
                let budget = Budget::new(1 << 20);
                let encoded = keys(
                    vec![Arc::new(Decimal128Array::from(
                        (0..1024)
                            .map(|row| match row % 3 {
                                0 => Some(0),
                                1 => Some(1_000_000),
                                _ => None,
                            })
                            .collect::<Vec<_>>(),
                    ))],
                    1024,
                    &budget,
                    true,
                );
                let mut old = JoinIndex::new(&encoded, 1024, &budget, true).unwrap();
                let mut next = vec![u32::MAX; 1024];
                old.populate(&encoded, &mut next);
                if reverse {
                    old.restore_build_order(&mut next);
                }
                let expected = (0_u32..1024)
                    .map(|row| {
                        if row % 3 == 2 {
                            u32::MAX
                        } else if reverse {
                            if row + 3 < 1024 { row + 3 } else { u32::MAX }
                        } else if row < 3 {
                            u32::MAX
                        } else {
                            row - 3
                        }
                    })
                    .collect::<Vec<_>>();
                let blocker = deny.then(|| {
                    budget
                        .reserve(budget.limit() - budget.used() - 639)
                        .unwrap()
                });
                match OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget) {
                    Ok(owned) => {
                        assert!(!deny);
                        assert_eq!(owned._lease.bytes(), 4992);
                        assert_eq!(owned.links(), expected);
                        assert_eq!(
                            owned.head(&0_i128.to_ne_bytes()),
                            Some(if reverse { 0 } else { 1023 })
                        );
                        assert_eq!(
                            owned.head(&1_000_000_i128.to_ne_bytes()),
                            Some(if reverse { 1 } else { 1021 })
                        );
                    }
                    Err((old, next)) => {
                        assert!(deny);
                        assert_eq!(old._lease.bytes(), 65_792);
                        assert_eq!(next, expected);
                        assert_eq!(
                            old.get(&0_i128.to_ne_bytes()),
                            Some(&if reverse { 0 } else { 1023 })
                        );
                        assert_eq!(
                            old.get(&1_000_000_i128.to_ne_bytes()),
                            Some(&if reverse { 1 } else { 1021 })
                        );
                    }
                }
                drop((blocker, encoded));
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn capacity_boundary_compacts_even_when_moved_credit_fits() {
        let budget = Budget::new(1 << 20);
        let mut values = (0..1792).map(|n| Some(n * 1_000_000)).collect::<Vec<_>>();
        values.push(None);
        let encoded = keys(
            vec![Arc::new(Decimal128Array::from(values))],
            1793,
            &budget,
            true,
        );
        let mut old = JoinIndex::new(&encoded, 1793, &budget, true).unwrap();
        let mut next = vec![u32::MAX; 1793];
        old.populate(&encoded, &mut next);
        let compact_credit = 1792 * 64 + 512 + next.capacity() * 4 + 256;
        assert!(old._lease.bytes() + 256 <= compact_credit);
        let super::super::Storage::Hash(hash) = &old.storage else {
            panic!("hash expected")
        };
        let old_capacity = hash.map.capacity();
        let owned = OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget)
            .unwrap_or_else(|_| panic!("funded compaction must succeed"));
        let Storage::Hash(map) = &owned.storage else {
            panic!("hash expected")
        };
        assert!(map.capacity() < old_capacity);
        assert_eq!(owned._lease.bytes(), compact_credit);
        for row in 0..1792 {
            assert_eq!(
                owned.head(&(row as i128 * 1_000_000).to_ne_bytes()),
                Some(row)
            );
        }
        assert!(owned.links().iter().all(|link| *link == u32::MAX));
        drop((owned, encoded));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn unique_sparse_builds_keep_the_move_path() {
        // Unique order/customer/nation-style equality builds: domain width
        // prevents dense storage, while distinct keys equal all build rows.
        for rows in [25, 1024, 4096] {
            let budget = Budget::new(2 << 20);
            let encoded = keys(
                vec![Arc::new(Decimal128Array::from_iter_values(
                    (0..rows).map(|row| row as i128 * 1_000_000),
                ))],
                rows,
                &budget,
                true,
            );
            let mut old = JoinIndex::new(&encoded, rows, &budget, true).unwrap();
            let mut next = vec![u32::MAX; rows];
            old.populate(&encoded, &mut next);
            assert_eq!(old.len(), rows);
            let super::super::Storage::Hash(hash) = &old.storage else {
                panic!("hash expected")
            };
            let entry = hash.map.iter().next().unwrap();
            let row = entry.key_row;
            let address = entry as *const Entry;
            let before = budget.used();
            let held = budget.reserve(budget.limit() - before - 256).unwrap();
            let owned = OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget)
                .unwrap_or_else(|_| panic!("unique build must move with envelope-only funding"));
            let Storage::Hash(map) = &owned.storage else {
                panic!("hash expected")
            };
            let moved = map
                .find(owned.hash.hash_one(encoded.row(row as usize)), |e| {
                    e.key_row == row
                })
                .unwrap();
            assert_eq!(moved as *const Entry, address);
            assert_eq!(budget.used(), before + held.bytes() + 256);
            drop((owned, held, encoded));
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn hash_handoff_compacts_entries_and_preserves_first_key_rows() {
        for pair in [false, true] {
            for reverse in [false, true] {
                let budget = Budget::new(1 << 20);
                let mut arrays: Vec<ArrayRef> = vec![Arc::new(Decimal128Array::from(vec![
                    Some(-1000),
                    None,
                    Some(4000),
                    Some(-1000),
                    Some(4000),
                    Some(7),
                ]))];
                if pair {
                    arrays.push(Arc::new(Decimal128Array::from(vec![Some(3); 6])));
                }
                let encoded = keys(arrays, 6, &budget, true);
                let mut old = JoinIndex::new(&encoded, 6, &budget, false).unwrap();
                let mut links = vec![u32::MAX; 6];
                old.populate(&encoded, &mut links);
                if reverse {
                    old.restore_build_order(&mut links);
                }
                let super::super::Storage::Hash(hash) = &old.storage else {
                    panic!("hash build")
                };
                let entry = hash
                    .map
                    .find(hash.hash.hash_one(encoded.row(0)), |e| e.key_row == 0)
                    .unwrap();
                assert_eq!(entry.key_row, 0);
                assert_eq!(entry.head, if reverse { 0 } else { 3 });
                let address = entry as *const Entry;
                let capacity = hash.map.capacity();
                let expected_links = links.clone();
                let expected_heads = (0..6)
                    .map(|row| old.get(encoded.row(row)).copied())
                    .collect::<Vec<_>>();
                let before = budget.used();
                // Three distinct keys compact a seven-slot table. Fund the
                // replacement peak before releasing any old storage credit.
                let held = budget
                    .reserve(budget.limit() - before - (3 * 64 + 512))
                    .unwrap();
                let owned =
                    match OwnedJoinIndex::from_borrowed(old, encoded.clone(), links, &budget) {
                        Ok(index) => index,
                        Err(_) => panic!("funded compaction must succeed"),
                    };
                let Storage::Hash(map) = &owned.storage else {
                    panic!("hash handoff")
                };
                assert!(map.capacity() < capacity);
                let entry = map
                    .find(owned.hash.hash_one(encoded.row(0)), |e| e.key_row == 0)
                    .unwrap();
                assert_ne!(entry as *const Entry, address);
                assert_eq!(entry.key_row, 0);
                assert_eq!(entry.head, if reverse { 0 } else { 3 });
                assert_eq!(owned.links(), expected_links);
                for (row, expected) in expected_heads.into_iter().enumerate() {
                    assert_eq!(owned.head(encoded.row(row)), expected);
                }
                drop(held);
                drop(owned);
                drop(encoded);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn pair_handoff_checks_both_columns_nulls_extremes_and_replacement_credit() {
        for swap in [false, true] {
            for deny_pair in [false, true] {
                let budget = Budget::new(1 << 20);
                let mut arrays: Vec<ArrayRef> = vec![
                    Arc::new(Decimal128Array::from(vec![
                        Some(i128::MIN),
                        Some(i128::MAX),
                        None,
                        Some(i128::MIN),
                        Some(i128::MAX),
                        Some(0),
                    ])),
                    Arc::new(Decimal128Array::from(vec![
                        Some(-2),
                        Some(-1),
                        Some(0),
                        Some(0),
                        Some(2),
                        None,
                    ])),
                ];
                if swap {
                    arrays.swap(0, 1);
                }
                let encoded = keys(arrays, 6, &budget, true);
                let encoded_bytes = budget.used();
                let mut old = JoinIndex::new(&encoded, 6, &budget, true).unwrap();
                let mut links = vec![u32::MAX; 6];
                old.populate(&encoded, &mut links);
                let borrowed_bytes = budget.used();
                let expected = links.clone();
                let probes = [
                    [i128::MIN, -2],
                    [i128::MAX, -1],
                    [i128::MIN, 0],
                    [i128::MAX, 2],
                    [i128::MAX, -2],
                    [i128::MIN, -1],
                    [0, 0],
                    [0, 1],
                    [0, i128::MIN],
                    [0, i128::MAX],
                ]
                .map(|mut pair| {
                    if swap {
                        pair.swap(0, 1);
                    }
                    let mut key = [1; 34];
                    key[2..18].copy_from_slice(&pair[0].to_ne_bytes());
                    key[18..34].copy_from_slice(&pair[1].to_ne_bytes());
                    key
                });
                let heads = probes.map(|key| old.get(&key).copied());
                let held =
                    deny_pair.then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
                match OwnedJoinIndex::from_borrowed(old, encoded.clone(), links, &budget) {
                    Ok(owned) => {
                        assert!(!deny_pair);
                        assert!(matches!(owned.storage, Storage::Pair(_)));
                        assert_eq!(owned.len(), 4);
                        assert_eq!(owned.links(), expected);
                        for (key, head) in probes.iter().zip(heads) {
                            assert_eq!(owned.head(key), head);
                        }
                        for row in [2, 5] {
                            assert_eq!(owned.head(encoded.row(row)), None);
                        }
                        assert_eq!(owned.head(&[1; 16]), None);
                        assert_eq!(budget.used(), encoded_bytes + (6 * 4 + 256) + (5 * 4 + 256));
                        assert!(budget.peak() >= borrowed_bytes + 5 * 4 + 256);
                    }
                    Err((fallback, links)) => {
                        assert!(deny_pair);
                        assert_eq!(links, expected);
                        for (key, head) in probes.iter().zip(heads) {
                            assert_eq!(fallback.get(key).copied(), head);
                        }
                    }
                }
                drop(held);
                drop(encoded);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn pair_handoff_declines_unordered_duplicate_sparse_and_overflowing_domains() {
        for pairs in [
            vec![],
            vec![(0, 2), (0, 1), (1, 0)],
            vec![(0, 0), (0, 0)],
            vec![(0, 0), (1_000_000, 1_000_000)],
            vec![(i128::MIN, i128::MIN), (i128::MAX, i128::MAX)],
        ] {
            let budget = Budget::new(1 << 20);
            let encoded = keys(
                (0..2)
                    .map(|column| {
                        Arc::new(Decimal128Array::from_iter_values(
                            pairs.iter().map(|&(a, b)| if column == 0 { a } else { b }),
                        )) as ArrayRef
                    })
                    .collect(),
                pairs.len(),
                &budget,
                true,
            );
            let mut old = JoinIndex::new(&encoded, pairs.len(), &budget, true).unwrap();
            let mut links = vec![u32::MAX; pairs.len()];
            old.populate(&encoded, &mut links);
            let expected = links.clone();
            let heads = (0..pairs.len())
                .map(|row| old.get(encoded.row(row)).copied())
                .collect::<Vec<_>>();
            let owned = match OwnedJoinIndex::from_borrowed(old, encoded.clone(), links, &budget) {
                Ok(owned) => owned,
                Err(_) => panic!("funded hash handoff must succeed"),
            };
            assert!(matches!(owned.storage, Storage::Hash(_)));
            assert_eq!(owned.links(), expected);
            for (row, head) in heads.into_iter().enumerate() {
                assert_eq!(owned.head(encoded.row(row)), head);
            }
            drop(owned);
            drop(encoded);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn handoff_retains_rejection_bitmap_counts_and_credit_on_success_or_denial() {
        for deny in [false, true] {
            let budget = Budget::new(1 << 20);
            let encoded = keys(
                vec![Arc::new(Decimal128Array::from_iter_values(
                    (0..16).map(|n| n * 8),
                ))],
                16,
                &budget,
                true,
            );
            let mut old = JoinIndex::new(&encoded, 16, &budget, false).unwrap();
            let mut next = vec![u32::MAX; 16];
            old.populate(&encoded, &mut next);
            assert!(old.enable_rejection(1024, &budget));
            let bytes = old.rejection_stats().0;
            assert!(bytes > 0);
            let probes = [i128::MIN, -1, 0, 1, 8, 63, 64, 120, 121, i128::MAX];
            let mut expected_counts = super::super::ProbeCounts::default();
            let expected = probes
                .iter()
                .map(|n| {
                    old.view()
                        .get(&n.to_ne_bytes(), &mut expected_counts)
                        .copied()
                })
                .collect::<Vec<_>>();
            let blocker = deny.then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
            let mut counts = super::super::ProbeCounts::default();
            match OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget) {
                Ok(owned) => {
                    assert!(!deny);
                    assert_eq!(owned.rejection.as_ref().unwrap().reserved_bytes(), bytes);
                    for (n, expected) in probes.iter().zip(&expected) {
                        assert_eq!(owned.probe(&n.to_ne_bytes(), &mut counts), *expected);
                    }
                }
                Err((fallback, _)) => {
                    assert!(deny);
                    assert_eq!(fallback.rejection_stats().0, bytes);
                    for (n, expected) in probes.iter().zip(&expected) {
                        assert_eq!(
                            fallback.view().get(&n.to_ne_bytes(), &mut counts).copied(),
                            *expected
                        );
                    }
                }
            }
            assert_eq!(counts.probes, expected_counts.probes);
            assert_eq!(counts.rejected, expected_counts.rejected);
            assert!(counts.rejected > 0);
            drop(blocker);
            drop(encoded);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn handoff_preserves_heads_links_and_dense_allocation_with_funded_fallback() {
        for dense in [false, true] {
            for deny in [false, true] {
                let budget = Budget::new(1 << 20);
                let encoded = keys(
                    vec![Arc::new(Decimal128Array::from(vec![
                        Some(-2),
                        None,
                        Some(4),
                        Some(-2),
                        Some(1),
                        Some(4),
                    ]))],
                    6,
                    &budget,
                    true,
                );
                let mut old = JoinIndex::new(&encoded, 6, &budget, dense).unwrap();
                let mut next = vec![u32::MAX; 6];
                old.populate(&encoded, &mut next);
                let expected = next.clone();
                let heads = (0..6)
                    .map(|row| old.get(encoded.row(row)).copied())
                    .collect::<Vec<_>>();
                let slots = match &old.storage {
                    super::super::Storage::Dense { heads, .. } => Some(heads.as_ptr()),
                    _ => None,
                };
                let used = budget.used();
                let blocker = deny.then(|| budget.reserve(budget.limit() - used).unwrap());
                match OwnedJoinIndex::from_borrowed(old, encoded.clone(), next, &budget) {
                    Ok(owned) => {
                        assert!(!deny);
                        assert_eq!(owned.links(), expected);
                        for (row, expected_head) in heads.iter().enumerate() {
                            assert_eq!(owned.head(encoded.row(row)), *expected_head);
                        }
                        if let Storage::Dense { heads, .. } = &owned.storage {
                            assert_eq!(
                                Some(heads.as_ptr()),
                                slots,
                                "dense slots move without allocation"
                            );
                        }
                        drop(owned);
                    }
                    Err((fallback, links)) => {
                        assert!(deny);
                        assert_eq!(budget.used(), budget.limit());
                        assert_eq!(links, expected);
                        for (row, expected_head) in heads.iter().enumerate() {
                            assert_eq!(fallback.get(encoded.row(row)).copied(), *expected_head);
                        }
                        drop(fallback);
                    }
                }
                drop(blocker);
                drop(encoded);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    fn keys(arrays: Vec<ArrayRef>, rows: usize, budget: &Budget, compact: bool) -> Arc<Keys> {
        let values = arrays
            .into_iter()
            .map(|a| Value::adopt(a, budget).unwrap())
            .collect::<Vec<_>>();
        Arc::new(Keys::with_text_encoding(&values, rows, budget, compact).unwrap())
    }
    fn check(keys: Arc<Keys>, rows: usize, budget: &Budget) {
        for dense in [false, true] {
            let mut old = JoinIndex::new(&keys, rows, budget, dense).unwrap();
            let mut expected_next = vec![u32::MAX; rows];
            old.populate(&keys, &mut expected_next);
            for collision_hash in [false, true] {
                for exercise_growth in [false, true] {
                    let index = OwnedJoinIndex::build(
                        keys.clone(),
                        rows,
                        budget,
                        &Cancellation::default(),
                        BuildOptions {
                            allow_dense: dense,
                            collision_hash,
                            exercise_growth,
                            cancel_at: None,
                        },
                    )
                    .unwrap();
                    assert_eq!(index.is_dense(), old.is_dense());
                    assert_eq!(index.len(), old.len());
                    for (row, &expected) in expected_next.iter().enumerate() {
                        let bytes = keys.row(row);
                        assert_eq!(index.head(bytes), old.get(bytes).copied());
                        assert_eq!(
                            index.next(row as u32),
                            (expected != u32::MAX).then_some(expected)
                        );
                        if keys.valid(row) {
                            let mut chain = Vec::new();
                            let mut head = index.head(bytes);
                            while let Some(row) = head {
                                chain.push(row as usize);
                                head = index.next(row);
                            }
                            let expected = (0..rows)
                                .rev()
                                .filter(|&r| keys.valid(r) && keys.row(r) == bytes)
                                .collect::<Vec<_>>();
                            assert_eq!(chain, expected);
                        }
                    }
                    for bytes in [
                        b"not-a-key".as_slice(),
                        &i128::MIN.to_ne_bytes(),
                        &i128::MAX.to_ne_bytes(),
                    ] {
                        assert_eq!(index.head(bytes), old.get(bytes).copied());
                    }
                    assert_eq!(index.next(u32::MAX), None);
                }
            }
        }
    }

    #[test]
    fn owned_numeric_dense_sparse_slices_extremes_and_nulls_match_borrowed_index() {
        let budget = Budget::new(16 << 20);
        for numbers in [
            vec![],
            vec![None, None],
            vec![Some(7), Some(7), Some(8), Some(6)],
            vec![None, Some(-2), Some(0), Some(-1), Some(-2)],
            vec![Some(i128::MIN), Some(i128::MIN + 2), Some(i128::MIN)],
            vec![Some(i128::MAX), Some(i128::MAX - 2), None],
            vec![Some(i128::MIN), Some(i128::MAX), Some(0)],
            (0..300).map(|n| Some(n * 10000)).collect(),
        ] {
            let rows = numbers.len();
            let mut padded = vec![Some(99), None];
            padded.extend(numbers);
            padded.push(Some(-99));
            let keys = keys(
                vec![Arc::new(Decimal128Array::from(padded).slice(2, rows))],
                rows,
                &budget,
                true,
            );
            check(keys, rows, &budget);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn owned_text_composite_and_zero_column_keys_collide_without_false_matches() {
        let budget = Budget::new(32 << 20);
        let values = [
            Some(""),
            None,
            Some("é🙂"),
            Some("a\0b"),
            Some("ab"),
            Some("a"),
            Some("bc"),
            Some("c"),
        ];
        for width in [1, 2] {
            for compact in [false, true] {
                let rows = 192;
                let arrays = (0..width)
                    .map(|column| {
                        let mut padded = vec![Some("prefix")];
                        padded.extend((0..rows).map(|row| {
                            values[if column == 0 {
                                row % values.len()
                            } else {
                                (row / values.len()) % values.len()
                            }]
                        }));
                        padded.push(Some("suffix"));
                        Arc::new(StringArray::from(padded).slice(1, rows)) as ArrayRef
                    })
                    .collect();
                check(keys(arrays, rows, &budget, compact), rows, &budget);
                assert_eq!(budget.used(), 0);
            }
        }
        let a = vec![None, None, Some(1), Some(1), Some(i128::MIN), Some(1)];
        let b = vec![Some(1), None, None, Some(2), Some(i128::MAX), Some(2)];
        check(
            keys(
                vec![
                    Arc::new(Decimal128Array::from(a)),
                    Arc::new(Decimal128Array::from(b)),
                ],
                6,
                &budget,
                true,
            ),
            6,
            &budget,
        );
        for rows in [0, 3] {
            check(keys(vec![], rows, &budget, true), rows, &budget);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn immutable_owned_index_and_key_backing_survive_cross_thread_last_arc() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<OwnedJoinIndex>();
        let budget = Budget::new(8 << 20);
        let keys = keys(
            vec![Arc::new(StringArray::from(vec![
                Some("雪\0"),
                None,
                Some(""),
                Some("雪\0"),
            ]))],
            4,
            &budget,
            true,
        );
        let key_weak = Arc::downgrade(&keys);
        let pointer = keys.row(0).as_ptr();
        let index = Arc::new(
            OwnedJoinIndex::new(keys.clone(), 4, &budget, true, &Cancellation::default()).unwrap(),
        );
        assert_eq!(
            index.keys.row(0).as_ptr(),
            pointer,
            "index must not copy key bytes"
        );
        drop(keys);
        let used = budget.used();
        let permits = budget.workers(3, 3);
        assert_eq!(permits.count, 3);
        std::thread::scope(|scope| {
            let handles = (0..3)
                .map(|_| {
                    let index = index.clone();
                    scope.spawn(move || {
                        for _ in 0..100 {
                            assert_eq!(index.head("雪\0".as_bytes()), Some(3));
                            assert_eq!(index.next(3), Some(0));
                        }
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
        });
        drop(permits);
        let (ready, started) = std::sync::mpsc::sync_channel(0);
        let (release, wait) = std::sync::mpsc::sync_channel(0);
        let held = index.clone();
        let permit = budget.workers(1, 1);
        let thread = std::thread::spawn(move || {
            ready.send(()).unwrap();
            wait.recv().unwrap();
            assert_eq!(held.head("雪\0".as_bytes()), Some(3));
            assert_eq!(held.next(3), Some(0));
            assert_eq!(held.head(b""), Some(2));
            assert_eq!(held.head(&[0xff]), None);
        });
        started.recv().unwrap();
        drop(index);
        assert!(key_weak.upgrade().is_some());
        assert_eq!(budget.used(), used);
        release.send(()).unwrap();
        thread.join().unwrap();
        drop(permit);
        assert!(key_weak.upgrade().is_none());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn owned_prepared_text_and_mixed_key_layouts_retain_exact_bytes() {
        let budget = Budget::new(8 << 20);
        let arrays = vec![
            Arc::new(StringArray::from(vec![
                Some("ab"),
                Some("a"),
                Some("ab"),
                None,
                Some(""),
            ])),
            Arc::new(StringArray::from(vec![
                Some("c"),
                Some("bc"),
                Some("c"),
                Some("x"),
                Some("\0"),
            ])),
        ];
        let values = arrays
            .into_iter()
            .map(|a| Value::adopt(a, &budget).unwrap())
            .collect::<Vec<_>>();
        let original = Keys::new(&values, 5, &budget).unwrap();
        let (column, encoding_credit) = original.into_text_column().unwrap();
        let pointer = column.value(1).as_ptr();
        let prepared = Arc::new(Keys::from_text_column(column.slice(1, 4), &budget).unwrap());
        assert_eq!(prepared.len(), 4);
        assert_eq!(prepared.row(0).as_ptr(), pointer);
        drop(values);
        drop(column);
        drop(encoding_credit);
        check(prepared, 4, &budget);
        assert_eq!(budget.used(), 0);
        check(
            keys(
                vec![
                    Arc::new(Decimal128Array::from(vec![
                        Some(1),
                        Some(1),
                        None,
                        Some(2),
                        Some(1),
                    ])),
                    Arc::new(StringArray::from(vec![
                        Some("雪"),
                        Some("雪"),
                        Some("雪"),
                        None,
                        Some("x\0"),
                    ])),
                ],
                5,
                &budget,
                true,
            ),
            5,
            &budget,
        );
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn owned_build_rejection_and_mid_build_cancellation_release_only_own_credits() {
        let budget = Budget::new(8 << 20);
        let keys = keys(
            vec![Arc::new(Decimal128Array::from(
                (0..3000).map(|i| Some(i * 10000)).collect::<Vec<_>>(),
            ))],
            3000,
            &budget,
            true,
        );
        let before = budget.used();
        for rows in [2999, 3001, u32::MAX as usize + 1] {
            assert!(
                OwnedJoinIndex::new(keys.clone(), rows, &budget, false, &Cancellation::default())
                    .is_err()
            );
            assert_eq!(budget.used(), before);
        }
        assert!(
            OwnedJoinIndex::new(
                keys.clone(),
                3000,
                &Budget::new(8 << 20),
                false,
                &Cancellation::default()
            )
            .is_err()
        );
        let blocker = budget.reserve(budget.limit() - before - 1).unwrap();
        assert!(
            OwnedJoinIndex::new(keys.clone(), 3000, &budget, false, &Cancellation::default())
                .err()
                .unwrap()
                .contains("resource-limit")
        );
        drop(blocker);
        assert_eq!(budget.used(), before);
        for at in [0, 1024, 2048] {
            let cancellation = Cancellation::default();
            assert!(
                OwnedJoinIndex::build(
                    keys.clone(),
                    3000,
                    &budget,
                    &cancellation,
                    BuildOptions {
                        cancel_at: Some(at),
                        ..BuildOptions::default()
                    }
                )
                .err()
                .unwrap()
                .contains("cancel")
            );
            assert_eq!(budget.used(), before);
        }
        drop(keys);
        assert_eq!(budget.used(), 0);
    }
}
