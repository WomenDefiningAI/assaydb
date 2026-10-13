//! Immutable equality-index ownership foundation; no production join dispatch.
//! Hash entries retain row IDs and resolve key bytes through one owned Keys Arc.
use super::{Budget, Keys, Reservation, Result, slot};
use crate::spill::Cancellation;
use hashbrown::HashTable;
use std::sync::Arc;

#[derive(Clone, Copy)]
struct Entry {
    key_row: u32,
    head: u32,
}
#[derive(Default)]
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
}

/// Buffers precede their backing leases in destruction order. No borrowed bytes
/// or mutable probe counters cross the ownership boundary.
pub(crate) struct OwnedJoinIndex {
    storage: Storage,
    next: Vec<u32>,
    keys: Arc<Keys>,
    hash: ahash::RandomState,
    collision_hash: bool,
    _lease: Reservation,
}

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
            Storage::Hash(map) => map
                .find(hash_bytes(&self.hash, self.collision_hash, key), |e| {
                    self.keys.row(e.key_row as usize) == key
                })
                .map(|e| e.head),
        }
    }
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
        }
    }
    pub(crate) fn is_dense(&self) -> bool {
        matches!(self.storage, Storage::Dense { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Value, join_index::JoinIndex};
    use arrow::array::{ArrayRef, Decimal128Array, StringArray};

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
