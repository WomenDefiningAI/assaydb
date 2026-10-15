//! Equality join indexing: direct addressing for dense numeric key ranges,
//! row-ID hashing otherwise. Duplicate chains remain owned by the join.
pub(crate) mod owned;
mod prepare;
use crate::{
    Result,
    keys::Keys,
    memory::{Budget, Reservation},
};
use hashbrown::{HashTable, hash_table::Entry as TableEntry};
use std::cell::Cell;

#[derive(Clone, Copy)]
struct Entry {
    key_row: u32,
    head: u32,
}

// Every stored key_row names a valid build key. Select its physical layout
// once per lookup, rather than rechecking layout and NULLs at each candidate.
fn hash_head<'a>(map: &'a HashTable<Entry>, keys: &Keys, hash: u64, key: &[u8]) -> Option<&'a u32> {
    let entry = if let Some(numbers) = keys.numeric_values() {
        let number = i128::from_ne_bytes(key.try_into().ok()?);
        map.find(hash, |e| numbers[e.key_row as usize] == number)
    } else if let Some(pairs) = keys.numeric_pairs() {
        let pair: &[u8; 34] = key.try_into().ok()?;
        map.find(hash, |e| &pairs[e.key_row as usize] == pair)
    } else {
        map.find(hash, |e| keys.row(e.key_row as usize) == key)
    };
    entry.map(|e| &e.head)
}

/// The exact same table and hash seed are used before and after ownership transfer.
struct HashIndex<'a> {
    map: HashTable<Entry>,
    keys: &'a Keys,
    hash: ahash::RandomState,
}
impl<'a> HashIndex<'a> {
    fn new(keys: &'a Keys, rows: usize) -> Self {
        Self {
            map: HashTable::with_capacity(rows),
            keys,
            hash: ahash::RandomState::new(),
        }
    }
    fn get(&self, key: &[u8]) -> Option<&u32> {
        hash_head(&self.map, self.keys, self.hash.hash_one(key), key)
    }

    /// Test oracle: the first `key_row` recorded for `key`.
    #[cfg(test)]
    pub(crate) fn key_row(&self, key: &[u8]) -> Option<u32> {
        self.map
            .find(self.hash.hash_one(key), |e| {
                self.keys.row(e.key_row as usize) == key
            })
            .map(|e| e.key_row)
    }

    fn populate(&mut self, next: &mut [u32]) {
        let keys = self.keys;
        if let Some(numbers) = keys.numeric_values() {
            self.populate_rows(next, |row| numbers[row].to_ne_bytes());
        } else if let Some(pairs) = keys.numeric_pairs() {
            self.populate_rows(next, |row| &pairs[row]);
        } else {
            self.populate_rows(next, |row| keys.row(row));
        }
    }

    // The typed readers produce exactly Keys::row's bytes for valid rows.
    // Hash slices (not arrays/integers), preserving the probe hash contract.
    fn populate_rows<K: AsRef<[u8]> + PartialEq>(
        &mut self,
        next: &mut [u32],
        key_at: impl Fn(usize) -> K,
    ) {
        assert_eq!(next.len(), self.keys.len());
        let mut put = |row: usize, link: &mut u32| {
            let key = key_at(row);
            match self.map.entry(
                self.hash.hash_one(key.as_ref()),
                |e| key_at(e.key_row as usize) == key,
                |e| self.hash.hash_one(key_at(e.key_row as usize).as_ref()),
            ) {
                TableEntry::Occupied(mut e) => {
                    *link = std::mem::replace(&mut e.get_mut().head, row as u32);
                }
                TableEntry::Vacant(e) => {
                    e.insert(Entry {
                        key_row: row as u32,
                        head: row as u32,
                    });
                }
            }
        };
        if self.keys.all_valid() {
            for (row, link) in next.iter_mut().enumerate() {
                put(row, link);
            }
        } else {
            for (row, link) in next.iter_mut().enumerate() {
                if self.keys.valid(row) {
                    put(row, link);
                }
            }
        }
    }
    fn remove(&mut self, key: &[u8]) -> Option<u32> {
        self.map
            .find_entry(self.hash.hash_one(key), |e| {
                self.keys.row(e.key_row as usize) == key
            })
            .ok()
            .map(|e| e.remove().0.head)
    }
    fn len(&self) -> usize {
        self.map.len()
    }
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

pub(crate) struct JoinIndex<'a> {
    storage: Storage<'a>,
    numeric: bool,
    // Present only while the index still contains precisely this completed build.
    // Borrow its already-funded rows instead of repeatedly chasing hash keys.
    populated_keys: Option<&'a Keys>,
    // Bounds from dense admission remain valid for this immutable completed
    // build even when its domain is too sparse for dense slots.
    numeric_bounds: Option<(i128, i128)>,
    rejection: Option<crate::membership::NumericMembership>,
    probes: Cell<usize>,
    rejected: Cell<usize>,
    _lease: Reservation,
}
enum Storage<'a> {
    Dense {
        base: i128,
        heads: Vec<u32>,
        count: usize,
        keys: &'a Keys,
    },
    Hash(HashIndex<'a>),
    // The legacy mutation oracle permits synthetic keys/row IDs outside Keys.
    // Production only inserts rows of the immutable completed build.
    #[cfg(test)]
    Inserted(std::collections::HashMap<&'a [u8], u32, ahash::RandomState>),
    Prepared {
        index: prepare::Index,
        keys: &'a Keys,
    },
}
#[derive(Clone, Copy)]
pub(crate) struct View<'v, 'k> {
    storage: &'v Storage<'k>,
    rejection: Option<&'v crate::membership::NumericMembership>,
}
#[derive(Default)]
pub(crate) struct ProbeCounts {
    pub probes: usize,
    pub rejected: usize,
}
impl<'v> View<'v, '_> {
    pub fn get(&self, key: &[u8], counts: &mut ProbeCounts) -> Option<&'v u32> {
        if let Some(bitmap) = self.rejection {
            counts.probes += 1;
            let member = <[u8; 16]>::try_from(key)
                .ok()
                .is_some_and(|n| bitmap.contains(i128::from_ne_bytes(n)));
            if !member {
                counts.rejected += 1;
                return None;
            }
        }
        self.storage.get(key)
    }
}
impl Storage<'_> {
    fn get(&self, key: &[u8]) -> Option<&u32> {
        match self {
            Storage::Dense { base, heads, .. } => {
                heads.get(slot(key, *base)?).filter(|v| **v != u32::MAX)
            }
            Storage::Hash(map) => map.get(key),
            #[cfg(test)]
            Storage::Inserted(map) => map.get(key),
            Storage::Prepared { index, keys } => index.get(key, keys),
        }
    }
}
fn dense_range((min, max): (i128, i128), rows: usize) -> Option<(i128, usize)> {
    let range = usize::try_from(max.checked_sub(min)?.checked_add(1)?).ok()?;
    // Four bytes per direct slot plus five per build row remains below the
    // hash path's 64-byte/row allowance at a range/build-row ratio of twelve.
    (range <= rows.saturating_mul(12)).then_some((min, range))
}
fn slot(bytes: &[u8], base: i128) -> Option<usize> {
    let number = i128::from_ne_bytes(bytes.try_into().ok()?);
    usize::try_from(number.checked_sub(base)?).ok()
}
impl<'a> JoinIndex<'a> {
    pub fn view(&self) -> View<'_, 'a> {
        View {
            storage: &self.storage,
            rejection: self.rejection.as_ref(),
        }
    }

    pub fn new(keys: &'a Keys, rows: usize, budget: &Budget, allow_dense: bool) -> Result<Self> {
        let numeric_bounds = allow_dense
            .then(|| crate::membership::numeric_bounds(keys))
            .flatten();
        let range = numeric_bounds.and_then(|bounds| dense_range(bounds, rows));
        // Include duplicate chains and reverse-existence match flags in the
        // reservation, as the previous hash-table accounting did.
        let bytes = if let Some((_, len)) = range {
            len.checked_mul(4)
                .and_then(|n| rows.checked_mul(5).and_then(|r| n.checked_add(r)))
        } else {
            rows.checked_mul(64)
        };
        let lease = budget.reserve(
            bytes
                .and_then(|n| n.checked_add(256))
                .ok_or("join index size overflow")?,
        )?;
        let storage = if let Some((base, len)) = range {
            Storage::Dense {
                base,
                heads: vec![u32::MAX; len],
                count: 0,
                keys,
            }
        } else {
            Storage::Hash(HashIndex::new(keys, rows))
        };
        Ok(Self {
            storage,
            numeric: keys.numeric_values().is_some(),
            populated_keys: None,
            numeric_bounds,
            rejection: None,
            probes: Cell::new(0),
            rejected: Cell::new(0),
            _lease: lease,
        })
    }
    /// Populate a fresh index and its MAX-initialized duplicate links. Dispatch
    /// on index representation once, rather than for every build row.
    pub fn populate(&mut self, keys: &'a Keys, next: &mut [u32]) {
        assert!(self.is_empty());
        self.populated_keys = None;
        self.rejection = None;
        match &mut self.storage {
            Storage::Dense {
                base,
                heads,
                count,
                keys: source,
            } => {
                assert!(std::ptr::eq(*source, keys));
                let numbers = keys.numeric_values().unwrap();
                assert_eq!(numbers.len(), next.len());
                let mut put = |row: usize, number: i128, link: &mut u32| {
                    // dense_range checked max-min+1 and usize conversion over
                    // these exact non-NULL values. Subtraction and indexing are
                    // therefore safe without repeating checked i128 decoding.
                    let slot = (number - *base) as usize;
                    let old = std::mem::replace(&mut heads[slot], row as u32);
                    *count += usize::from(old == u32::MAX);
                    *link = old;
                };
                if keys.all_valid() {
                    for (row, (number, link)) in numbers.iter().zip(next).enumerate() {
                        put(row, *number, link);
                    }
                } else {
                    for (row, (number, link)) in numbers.iter().zip(next).enumerate() {
                        if keys.valid(row) {
                            put(row, *number, link);
                        }
                    }
                }
            }
            Storage::Hash(map) => {
                assert!(std::ptr::eq(map.keys, keys));
                map.populate(next);
            }
            Storage::Prepared { .. } => unreachable!("already populated build"),
            #[cfg(test)]
            Storage::Inserted(_) => unreachable!("test insertion oracle"),
        }
        self.populated_keys = Some(keys);
    }

    /// The completed build's non-NULL keys define the same optional scan bitmap as the exact index.
    pub(crate) fn scan_membership(
        &self,
        budget: &Budget,
    ) -> Option<crate::membership::NumericMembership> {
        if !self.numeric {
            return None;
        }
        if let Some(keys) = self.populated_keys
            && keys.len() <= self.len().saturating_mul(2)
        {
            return self.numeric_membership(keys, 8 << 20, budget);
        }
        crate::membership::NumericMembership::from_keys(|| self.keys(), budget)
    }

    /// Fund only the component whose scan guards have already passed.
    pub(crate) fn scan_pair_membership(
        &self,
        component: usize,
        budget: &Budget,
    ) -> Option<crate::membership::NumericMembership> {
        if component >= 2 {
            return None;
        }
        if let Some(keys) = self.populated_keys
            && keys.numeric_pairs().is_some()
            && keys.len() <= self.len().saturating_mul(2)
        {
            return crate::membership::NumericMembership::from_pair_rows(
                keys,
                [component == 0, component == 1],
                budget,
            )[component]
                .take();
        }
        crate::membership::NumericMembership::from_pair_keys(|| self.keys(), component, budget)
    }

    fn numeric_membership(
        &self,
        keys: &Keys,
        max_bytes: usize,
        budget: &Budget,
    ) -> Option<crate::membership::NumericMembership> {
        if let Some(bounds) = self.numeric_bounds {
            crate::membership::NumericMembership::from_numeric_keys_in_range(
                keys, bounds, max_bytes, budget,
            )
        } else {
            crate::membership::NumericMembership::from_numeric_keys(keys, max_bytes, budget)
        }
    }

    /// Reverse populate's newest-first duplicate chains into build-row order.
    /// Each chain is disjoint, so existing heads and links suffice without any
    /// additional allocation or changes to key membership.
    pub fn restore_build_order(&mut self, next: &mut [u32]) {
        let mut reverse = |head: &mut u32| {
            let mut row = *head;
            let mut previous = u32::MAX;
            while row != u32::MAX {
                let following = std::mem::replace(&mut next[row as usize], previous);
                previous = row;
                row = following;
            }
            *head = previous;
        };
        match &mut self.storage {
            Storage::Dense { heads, .. } => heads.iter_mut().for_each(&mut reverse),
            Storage::Hash(map) => map.map.iter_mut().for_each(|e| reverse(&mut e.head)),
            #[cfg(test)]
            Storage::Inserted(map) => map.values_mut().for_each(reverse),
            Storage::Prepared { index, .. } => index.heads_mut().for_each(reverse),
        }
    }

    // Optional rejection only: positive bits still require exact map lookup.
    // Use this after build completion and only without an equivalent scan mask.
    pub fn enable_rejection(&mut self, estimated_probes: usize, budget: &Budget) -> bool {
        if self.is_dense() {
            return false;
        }
        let count = self.len();
        if !self.numeric || count < 8 || estimated_probes < count.saturating_mul(4) {
            return false;
        }
        // Two sequential passes over the funded build replace four passes over
        // hash keys (range, layout, range again, population). With heavy
        // duplication, keep walking distinct keys instead. Both sources contain
        // the same non-NULL values, and the byte/selectivity guards are unchanged.
        if let Some(keys) = self.populated_keys
            && keys.len() <= count.saturating_mul(2)
        {
            self.rejection = self.numeric_membership(keys, count.saturating_mul(16), budget);
            return self.rejection.is_some();
        }
        let mut values = self
            .keys()
            .map(|k| i128::from_ne_bytes(k.try_into().unwrap()));
        let first = values.next().unwrap();
        let (lo, hi) = values.fold((first, first), |(lo, hi), n| (lo.min(n), hi.max(n)));
        let Some(bytes) = hi
            .checked_sub(lo)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| usize::try_from(n).ok())
            .map(|n| n.div_ceil(8))
        else {
            return false;
        };
        // Construction and cache footprint must be small relative to the map;
        // probe estimates influence optional costing only, never correctness.
        if bytes > count.saturating_mul(16) {
            return false;
        }
        self.rejection = crate::membership::NumericMembership::from_keys(|| self.keys(), budget);
        self.rejection.is_some()
    }
    pub fn rejection_stats(&self) -> (usize, usize, usize) {
        (
            self.rejection.as_ref().map_or(0, |b| b.reserved_bytes()),
            self.probes.get(),
            self.rejected.get(),
        )
    }
    #[inline]
    fn rejects(&self, key: &[u8]) -> bool {
        let Some(bitmap) = &self.rejection else {
            return false;
        };
        self.probes.set(self.probes.get() + 1);
        let member = <[u8; 16]>::try_from(key)
            .ok()
            .is_some_and(|n| bitmap.contains(i128::from_ne_bytes(n)));
        if !member {
            self.rejected.set(self.rejected.get() + 1);
        }
        !member
    }
    pub fn is_dense(&self) -> bool {
        matches!(self.storage, Storage::Dense { .. })
    }
    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Dense { count, .. } => *count,
            Storage::Hash(map) => map.len(),
            #[cfg(test)]
            Storage::Inserted(map) => map.len(),
            Storage::Prepared { index, .. } => index.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[cfg(test)]
    pub fn insert(&mut self, key: &'a [u8], row: u32) -> Option<u32> {
        // A later insert can introduce a key outside the original bitmap.
        self.populated_keys = None;
        self.rejection = None;
        if let Storage::Hash(old) = &self.storage {
            // Fund the oracle replacement peak without weakening production or
            // the existing arbitrary-key mutation tests.
            let credit = self
                ._lease
                .budget()
                .reserve((old.len() + 1) * 64 + 256)
                .unwrap();
            let mut map = std::collections::HashMap::with_capacity_and_hasher(
                old.len() + 1,
                ahash::RandomState::new(),
            );
            for e in &old.map {
                map.insert(old.keys.row(e.key_row as usize), e.head);
            }
            self.storage = Storage::Inserted(map);
            self._lease.absorb(credit);
        }
        match &mut self.storage {
            Storage::Dense {
                base, heads, count, ..
            } => {
                // The range was derived from these immutable, non-NULL keys.
                let old = std::mem::replace(&mut heads[slot(key, *base).unwrap()], row);
                if old == u32::MAX {
                    *count += 1;
                    None
                } else {
                    Some(old)
                }
            }
            Storage::Inserted(map) => map.insert(key, row),
            Storage::Hash(_) => unreachable!("converted to test insertion oracle"),
            Storage::Prepared { .. } => unreachable!("completed build"),
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<&u32> {
        if self.rejects(key) {
            return None;
        }
        self.storage.get(key)
    }
    pub fn remove(&mut self, key: &[u8]) -> Option<u32> {
        if self.rejects(key) {
            return None;
        }
        // Stale positive bits after removal are safe; the exact map decides.
        self.populated_keys = None;
        match &mut self.storage {
            Storage::Dense {
                base, heads, count, ..
            } => {
                let head = heads.get_mut(slot(key, *base)?)?;
                if *head == u32::MAX {
                    return None;
                }
                *count -= 1;
                Some(std::mem::replace(head, u32::MAX))
            }
            Storage::Hash(map) => map.remove(key),
            #[cfg(test)]
            Storage::Inserted(map) => map.remove(key),
            Storage::Prepared { index, keys } => index.remove(key, keys),
        }
    }
    pub fn keys(&self) -> Box<dyn Iterator<Item = &[u8]> + '_> {
        match &self.storage {
            Storage::Dense { heads, keys, .. } => Box::new(
                heads
                    .iter()
                    .filter(|row| **row != u32::MAX)
                    .map(|row| keys.row(*row as usize)),
            ),
            Storage::Hash(map) => {
                Box::new(map.map.iter().map(|e| map.keys.row(e.key_row as usize)))
            }
            #[cfg(test)]
            Storage::Inserted(map) => Box::new(map.keys().copied()),
            Storage::Prepared { index, keys } => {
                Box::new(index.key_rows().map(|row| keys.row(row as usize)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Value;
    use arrow::array::Decimal128Array;
    use std::sync::Arc;

    #[test]
    fn admitted_numeric_bounds_keep_sparse_extreme_and_mutated_memberships_exact() {
        for base in [i128::MIN, -512, i128::MAX - 1024] {
            for copies in [1, 4] {
                let budget = Budget::new(1 << 20);
                let mut numbers = vec![None];
                for _ in 0..copies {
                    numbers.extend([Some(base), Some(base + 512), Some(base + 1024)]);
                }
                let rows = numbers.len();
                let value =
                    Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
                let keys = Keys::new(&[value], rows, &budget).unwrap();
                let mut index = JoinIndex::new(&keys, rows, &budget, true).unwrap();
                assert!(!index.is_dense());
                assert_eq!(index.numeric_bounds, Some((base, base + 1024)));
                index.populate(&keys, &mut vec![u32::MAX; rows]);
                // A refused bitmap keeps the already completed index intact.
                let used = budget.used();
                let held = budget.reserve(budget.limit() - used).unwrap();
                assert!(index.scan_membership(&budget).is_none());
                assert_eq!(
                    index.get(&base.to_ne_bytes()),
                    Some(&(1 + (copies - 1) * 3))
                );
                drop(held);
                let bitmap = index.scan_membership(&budget).unwrap();
                assert_eq!(bitmap.cardinality(), 3);
                for offset in 0..=1024 {
                    assert_eq!(bitmap.contains(base + offset), offset % 512 == 0);
                }
                drop(bitmap);
                index.remove(&base.to_ne_bytes()).unwrap();
                assert!(index.populated_keys.is_none());
                let bitmap = index.scan_membership(&budget).unwrap();
                assert_eq!(bitmap.cardinality(), 2);
                assert!(!bitmap.contains(base));
                assert!(bitmap.contains(base + 1024));
                drop(bitmap);
                drop(index);
                drop(keys);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn immutable_views_share_lookups_but_keep_worker_counters_local() {
        let budget = Budget::new(1 << 20);
        for numbers in [
            (0..32).map(|n| Some(n * 2)).collect::<Vec<_>>(),
            vec![None, Some(i128::MIN), Some(i128::MAX), Some(0), Some(0)],
        ] {
            let len = numbers.len();
            let value = Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
            let keys = Keys::new(&[value], len, &budget).unwrap();
            for dense in [false, true] {
                let mut index = JoinIndex::new(&keys, len, &budget, dense).unwrap();
                index.populate(&keys, &mut vec![u32::MAX; len]);
                index.enable_rejection(10000, &budget);
                let probes = [i128::MIN, i128::MAX, -1, 0, 1, 2, 62, 63, 64];
                let expected = probes
                    .iter()
                    .map(|n| index.get(&n.to_ne_bytes()).copied())
                    .collect::<Vec<_>>();
                let (_, counted, rejected) = index.rejection_stats();
                let view = index.view();
                std::thread::scope(|scope| {
                    let workers = (0..3)
                        .map(|_| {
                            scope.spawn(|| {
                                let mut counts = ProbeCounts::default();
                                let actual = probes
                                    .iter()
                                    .map(|n| view.get(&n.to_ne_bytes(), &mut counts).copied())
                                    .collect::<Vec<_>>();
                                assert_eq!(actual, expected);
                                assert_eq!((counts.probes, counts.rejected), (counted, rejected));
                            })
                        })
                        .collect::<Vec<_>>();
                    for worker in workers {
                        worker.join().unwrap();
                    }
                });
                assert_eq!(index.rejection_stats().1, counted);
            }
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn bulk_population_matches_ordered_insertion_for_dense_hash_nulls_and_extremes() {
        let budget = Budget::new(1 << 20);
        let cases = vec![
            vec![],
            vec![None, None, None],
            vec![Some(7), Some(7), Some(8), Some(6), Some(7)],
            vec![None, Some(-1), Some(-1), None, Some(0), Some(-2)],
            vec![Some(i128::MIN), Some(i128::MIN + 2), Some(i128::MIN), None],
            vec![Some(i128::MAX), Some(i128::MAX - 2), Some(i128::MAX), None],
            vec![Some(i128::MIN), Some(i128::MAX), None, Some(0)],
        ];
        for numbers in cases {
            let len = numbers.len();
            // Nonzero offsets must preserve both the numeric and validity slices.
            let mut padded = vec![Some(123), None];
            padded.extend(numbers);
            padded.push(Some(456));
            let sliced = Decimal128Array::from(padded).slice(2, len);
            let value = Value::adopt(Arc::new(sliced), &budget).unwrap();
            let keys = Keys::new(&[value], len, &budget).unwrap();
            for dense in [false, true] {
                let mut bulk = JoinIndex::new(&keys, len, &budget, dense).unwrap();
                let mut plain = JoinIndex::new(&keys, len, &budget, dense).unwrap();
                let mut actual = vec![u32::MAX; len];
                let mut expected = actual.clone();
                bulk.populate(&keys, &mut actual);
                for (row, link) in expected.iter_mut().enumerate() {
                    if keys.valid(row)
                        && let Some(old) = plain.insert(keys.row(row), row as u32)
                    {
                        *link = old;
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(bulk.len(), plain.len());
                assert_eq!(bulk.is_dense(), plain.is_dense());
                for row in 0..len {
                    assert_eq!(bulk.get(keys.row(row)), plain.get(keys.row(row)));
                    assert_eq!(bulk.remove(keys.row(row)), plain.remove(keys.row(row)));
                }
            }
        }
        let a = Value::adopt(
            Arc::new(arrow::array::StringArray::from(vec![
                Some(""),
                None,
                Some("é"),
                Some(""),
                Some("a\0b"),
            ])),
            &budget,
        )
        .unwrap();
        let keys = Keys::new(&[a], 5, &budget).unwrap();
        let mut index = JoinIndex::new(&keys, 5, &budget, true).unwrap();
        let mut next = vec![u32::MAX; 5];
        index.populate(&keys, &mut next);
        assert_eq!(next, vec![u32::MAX, u32::MAX, u32::MAX, 0, u32::MAX]);
        assert_eq!(index.get(keys.row(0)), Some(&3));
        assert!(index.get(keys.row(1)).is_none());
        drop(index);
        drop(keys);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn populated_rejection_matches_hash_keys_with_nulls_duplicates_and_mutation() {
        for copies in [1, 4] {
            let budget = Budget::new(1 << 20);
            let mut numbers = (0..copies)
                .flat_map(|_| (0..64).map(|i| Some(1024 + i * 64)))
                .collect::<Vec<_>>();
            numbers.extend([None, Some(1024)]);
            let rows = numbers.len();
            let value = Value::adopt(Arc::new(Decimal128Array::from(numbers)), &budget).unwrap();
            let keys = Keys::new(&[value], rows, &budget).unwrap();
            let late = 999_999_i128.to_ne_bytes();
            let mut populated = JoinIndex::new(&keys, rows, &budget, false).unwrap();
            let mut inserted = JoinIndex::new(&keys, rows, &budget, false).unwrap();
            let mut next = vec![u32::MAX; rows];
            populated.populate(&keys, &mut next);
            for row in 0..rows {
                if keys.valid(row) {
                    inserted.insert(keys.row(row), row as u32);
                }
            }
            // Exercise both the sequential-build choice and the duplicate-heavy
            // distinct-key fallback, without changing the admitted bitmap.
            assert_eq!(rows <= populated.len() * 2, copies == 1);
            assert!(populated.populated_keys.is_some());
            assert!(inserted.populated_keys.is_none());
            let exhausted = Budget::new(0);
            assert!(!populated.enable_rejection(8192, &exhausted));
            assert_eq!(exhausted.used(), 0);
            assert!(populated.enable_rejection(8192, &budget));
            assert!(inserted.enable_rejection(8192, &budget));
            assert_eq!(populated.rejection_stats().0, inserted.rejection_stats().0);
            let scan = populated.scan_membership(&budget).unwrap();
            assert!(populated.scan_membership(&exhausted).is_none());
            for n in (0_i128..6000).chain([i128::MIN, i128::MAX]) {
                assert_eq!(
                    populated.get(&n.to_ne_bytes()),
                    inserted.get(&n.to_ne_bytes())
                );
                assert_eq!(scan.contains(n), inserted.get(&n.to_ne_bytes()).is_some());
            }
            let removed = 1024_i128.to_ne_bytes();
            assert_eq!(populated.remove(&removed), inserted.remove(&removed));
            assert!(populated.populated_keys.is_none());
            assert_eq!(populated.get(&removed), None);
            assert!(!populated.scan_membership(&budget).unwrap().contains(1024));
            assert_eq!(populated.insert(&late, 99), inserted.insert(&late, 99));
            assert_eq!(populated.rejection_stats().0, 0);
            assert_eq!(populated.get(&late), Some(&99));
            assert!(
                populated
                    .scan_membership(&budget)
                    .unwrap()
                    .contains(999_999)
            );
            drop(scan);
            drop(populated);
            drop(inserted);
            drop(keys);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn sparse_rejection_preserves_lookup_removal_late_insert_and_budget_fallback() {
        let budget = Budget::new(1 << 20);
        let mut numbers: Vec<_> = (0..64).map(|i| Some(i * 64 - 2048)).collect();
        numbers.extend([None, Some(-2048)]);
        let value =
            Value::adopt(Arc::new(Decimal128Array::from(numbers.clone())), &budget).unwrap();
        let keys = Keys::new(&[value], numbers.len(), &budget).unwrap();
        let late = 999_999_i128.to_ne_bytes();
        let mut filtered = JoinIndex::new(&keys, numbers.len(), &budget, true).unwrap();
        let mut plain = JoinIndex::new(&keys, numbers.len(), &budget, false).unwrap();
        for (row, n) in numbers.iter().enumerate() {
            if n.is_some() {
                assert_eq!(
                    filtered.insert(keys.row(row), row as u32),
                    plain.insert(keys.row(row), row as u32)
                );
            }
        }
        assert!(!filtered.is_dense());
        assert!(!filtered.enable_rejection(1, &budget));
        let exhausted = Budget::new(0);
        assert!(!filtered.enable_rejection(8192, &exhausted));
        assert_eq!(exhausted.used(), 0);
        assert!(filtered.enable_rejection(8192, &budget));
        let bytes = filtered.rejection_stats().0;
        assert!(bytes > 0 && bytes <= 64 * 16 + 128);
        for n in (-3000_i128..3000).chain([i128::MIN, i128::MAX]) {
            let key = n.to_ne_bytes();
            assert_eq!(filtered.get(&key), plain.get(&key));
            assert_eq!(filtered.remove(&key), plain.remove(&key));
            assert_eq!(filtered.get(&key), plain.get(&key));
        }
        assert!(filtered.rejection_stats().2 > 0);
        assert_eq!(filtered.insert(&late, 99), plain.insert(&late, 99));
        assert_eq!(filtered.rejection_stats().0, 0);
        assert_eq!(filtered.get(&late), Some(&99));
        drop(filtered);
        drop(plain);
        drop(keys);
        assert_eq!(budget.used(), 0);

        // Huge spans and nonnumeric sixteen-byte keys must not be interpreted
        // as a bounded numeric domain, even when a hash table is forced.
        for numbers in [
            vec![
                Some(i128::MIN),
                Some(i128::MAX),
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ],
            (0..8).map(|i| Some(i * 1_000_000)).collect(),
        ] {
            let value =
                Value::adopt(Arc::new(Decimal128Array::from(numbers.clone())), &budget).unwrap();
            let keys = Keys::new(&[value], numbers.len(), &budget).unwrap();
            let mut map = JoinIndex::new(&keys, numbers.len(), &budget, false).unwrap();
            for row in 0..numbers.len() {
                map.insert(keys.row(row), row as u32);
            }
            assert!(!map.enable_rejection(1000, &budget));
        }
        let value = Value::adopt(
            Arc::new(arrow::array::StringArray::from(vec!["abcdefghijklmnop"; 8])),
            &budget,
        )
        .unwrap();
        let keys = Keys::with_text_encoding(&[value], 8, &budget, true).unwrap();
        let mut map = JoinIndex::new(&keys, 8, &budget, false).unwrap();
        for row in 0..8 {
            map.insert(keys.row(row), row as u32);
        }
        assert!(!map.enable_rejection(1000, &budget));
    }

    #[test]
    fn dense_index_matches_hash_for_nulls_duplicates_signed_extremes_and_removal() {
        let budget = Budget::new(1 << 20);
        for (case, numbers) in [
            vec![Some(-2), None, Some(0), Some(-1), Some(-2)],
            vec![Some(i128::MIN), Some(i128::MIN + 1), Some(i128::MIN)],
            vec![Some(i128::MAX - 1), Some(i128::MAX), None],
            vec![Some(-7), Some(7)],
            vec![Some(i128::MIN), Some(i128::MAX)],
            vec![Some(0), Some(999_999)],
            vec![None, None],
            vec![],
        ]
        .into_iter()
        .enumerate()
        {
            let value =
                Value::adopt(Arc::new(Decimal128Array::from(numbers.clone())), &budget).unwrap();
            let keys = Keys::new(&[value], numbers.len(), &budget).unwrap();
            let mut dense = JoinIndex::new(&keys, numbers.len(), &budget, true).unwrap();
            assert_eq!(dense.is_dense(), case < 4);
            let mut hash = JoinIndex::new(&keys, numbers.len(), &budget, false).unwrap();
            for (row, n) in numbers.iter().enumerate() {
                if n.is_some() {
                    assert_eq!(
                        dense.insert(keys.row(row), row as u32),
                        hash.insert(keys.row(row), row as u32)
                    );
                }
            }
            assert_eq!(dense.len(), hash.len());
            let mut dk: Vec<_> = dense.keys().collect();
            let mut hk: Vec<_> = hash.keys().collect();
            dk.sort();
            hk.sort();
            assert_eq!(dk, hk);
            for n in numbers.iter().flatten().copied().chain([
                i128::MIN,
                -10,
                -2,
                0,
                1,
                999_999,
                i128::MAX,
            ]) {
                let bytes = n.to_ne_bytes();
                assert_eq!(dense.get(&bytes), hash.get(&bytes));
                assert_eq!(dense.remove(&bytes), hash.remove(&bytes));
                assert_eq!(dense.get(&bytes), hash.get(&bytes));
            }
            assert!(dense.is_empty());
        }
        assert_eq!(budget.used(), 0);
    }
}
