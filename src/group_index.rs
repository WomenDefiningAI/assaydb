//! Adaptive numeric group lookup. Stable group IDs keep aggregate state and
//! encounter order independent of the index representation. NULL stays outside.
//! Dense slots compact representable IDs; wider IDs remain exact in hashing.
use crate::memory::{Budget, Reservation};
use std::collections::HashMap;

struct Dense {
    base: i128,
    slots: Vec<u32>,
    _lease: Reservation,
}
pub(crate) struct NumericGroups {
    hash: HashMap<i128, usize, ahash::RandomState>,
    dense: Option<Dense>,
    bounds: Option<(i128, i128)>,
    // Only compact IDs contribute to dense admission and key-domain bounds.
    count: usize,
    pending_dense: usize,
    has_wide_ids: bool,
}
impl Default for NumericGroups {
    fn default() -> Self {
        Self {
            hash: HashMap::with_hasher(ahash::RandomState::new()),
            dense: None,
            bounds: None,
            count: 0,
            pending_dense: 0,
            has_wide_ids: false,
        }
    }
}
fn offset(key: i128, base: i128) -> Option<usize> {
    usize::try_from(key.checked_sub(base)?).ok()
}
// Copy a contained old window directly, initializing its prefix/suffix only.
// Even unused MAX slots keep the same key-relative position. If old padding
// extends outside the new window, relocate occupied cells individually instead.
fn grown_slots(old: Option<&Dense>, base: i128, len: usize) -> Vec<u32> {
    if let Some(old) = old
        && let Some(start) = offset(old.base, base)
        && let Some(end) = start.checked_add(old.slots.len())
        && end <= len
    {
        let mut slots = Vec::with_capacity(len);
        slots.resize(start, u32::MAX);
        slots.extend_from_slice(&old.slots);
        slots.resize(len, u32::MAX);
        return slots;
    }
    let mut slots = vec![u32::MAX; len];
    if let Some(old) = old {
        for (i, group) in old.slots.iter().enumerate() {
            if *group != u32::MAX {
                let key = old.base.checked_add(i as i128).unwrap();
                slots[offset(key, base).unwrap()] = *group;
            }
        }
    }
    slots
}
impl NumericGroups {
    /// The source-domain proof has already established all keys lie in this
    /// exact window and that every local group ID fits below the u32 sentinel.
    pub(crate) fn with_proven_window(
        base: i128,
        len: usize,
        budget: &Budget,
    ) -> crate::Result<Self> {
        let bytes = len
            .checked_mul(std::mem::size_of::<u32>())
            .and_then(|n| n.checked_add(256))
            .ok_or("source dense window overflow")?;
        let lease = budget.reserve(bytes)?;
        Ok(Self {
            dense: Some(Dense {
                base,
                slots: vec![u32::MAX; len],
                _lease: lease,
            }),
            ..Self::default()
        })
    }
    pub fn get(&self, key: i128) -> Option<usize> {
        if let Some(dense) = &self.dense
            && let Some(slot) = offset(key, dense.base).and_then(|i| dense.slots.get(i))
        {
            if *slot != u32::MAX {
                return Some(*slot as usize);
            }
            if !self.has_wide_ids {
                return None;
            }
        }
        self.hash.get(&key).copied()
    }
    // The caller reserves new group state (including hash capacity) first,
    // and only inserts a new key after a failed lookup.
    pub fn insert(&mut self, key: i128, group: usize) {
        assert_ne!(group, usize::MAX);
        let Some(compact) = compact_id(group) else {
            self.has_wide_ids = true;
            self.hash.insert(key, group);
            return;
        };
        self.count += 1;
        self.bounds = Some(match self.bounds {
            Some((lo, hi)) => (lo.min(key), hi.max(key)),
            None => (key, key),
        });
        if let Some(dense) = &mut self.dense
            && let Some(slot) = offset(key, dense.base).and_then(|i| dense.slots.get_mut(i))
        {
            *slot = compact;
            return;
        }
        self.hash.insert(key, group);
        self.pending_dense += 1;
    }
    /// Reconsider once per batch using actual distinct keys, never estimated
    /// domain assumptions. Sparse outliers remain in hashing indefinitely.
    /// Returns true only when a new dense allocation was installed.
    pub fn prepare(&mut self, budget: &Budget) -> bool {
        if self.pending_dense == 0 {
            return false;
        }
        let Some((min, max)) = self.bounds else {
            return false;
        };
        let Some(span) = max
            .checked_sub(min)
            .and_then(|v| v.checked_add(1))
            .and_then(|v| usize::try_from(v).ok())
        else {
            return false;
        };
        let Some(mut len) = span.checked_next_power_of_two() else {
            return false;
        };
        // Geometric growth avoids repeatedly copying a nearly identical index
        // when successive batches extend either end of an ordered key range.
        if let Some(old) = &self.dense {
            let Some(grown) = old.slots.len().checked_mul(2) else {
                return false;
            };
            len = len.max(grown);
        }
        let Some(bytes) = len
            .checked_mul(std::mem::size_of::<u32>())
            .and_then(|v| v.checked_add(256))
        else {
            return false;
        };
        // The existing hash accounting allows 96 key/index bytes per group.
        // Bound this optional extra allocation even under very large budgets.
        // A portable 32 MiB payload cap bounds the compact table to 2^23 slots;
        // a smaller slot must not automatically double optional domain coverage.
        if bytes > self.count.saturating_mul(96) || bytes > 32 * 1024 * 1024 + 256 {
            return false;
        }
        let Ok(lease) = budget.reserve(bytes) else {
            return false;
        };
        let base = min.checked_sub(((len - span) / 2) as i128).unwrap_or(min);
        // Hold old and new reservations simultaneously throughout copying.
        let mut slots = grown_slots(self.dense.as_ref(), base, len);
        if self.has_wide_ids {
            // Wide IDs keep exact usize values in hashing. Remove migrated
            // entries, then shrink the remainder under the ordinary old/new
            // hash allowance. A separately growing remainder could require
            // three simultaneous allocations, exceeding that allowance.
            self.hash.retain(|key, group| {
                if let Some(compact) = compact_id(*group) {
                    slots[offset(*key, base).unwrap()] = compact;
                    false
                } else {
                    true
                }
            });
            self.hash.shrink_to_fit();
        } else {
            for (key, group) in &self.hash {
                // Every inserted ID fits and differs from the sentinel.
                slots[offset(*key, base).unwrap()] = *group as u32;
            }
            self.hash = HashMap::with_hasher(ahash::RandomState::new());
        }
        self.dense = Some(Dense {
            base,
            slots,
            _lease: lease,
        });
        self.pending_dense = 0;
        true
    }
    pub fn dense_bytes(&self) -> usize {
        self.dense
            .as_ref()
            .map_or(0, |d| d.slots.len() * std::mem::size_of::<u32>() + 256)
    }
}

fn compact_id(group: usize) -> Option<u32> {
    u32::try_from(group).ok().filter(|id| *id != u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_slots_charge_their_compact_width() {
        let budget = Budget::new(4096);
        let mut index = NumericGroups::default();
        for n in 0..64 {
            index.insert(n, n as usize);
        }
        assert!(index.prepare(&budget));
        assert_eq!(index.dense_bytes(), 64 * 4 + 256);
        assert_eq!(budget.used(), index.dense_bytes());
        assert_eq!(compact_id((u32::MAX - 1) as usize), Some(u32::MAX - 1));
        drop(index);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dense_cap_rejects_growth_without_losing_sparse_or_dense_ids() {
        let budget = Budget::new(128 * 1024 * 1024);
        let mut index = NumericGroups::default();
        // Enough distinct keys for density admission of the next 64 MiB table,
        // and enough budget for old/new overlap: the payload cap rejects it.
        for n in 0..700_000 {
            index.insert(n as i128 * 11, n);
        }
        assert!(index.prepare(&budget));
        assert_eq!(index.dense_bytes(), 32 * 1024 * 1024 + 256);
        let dense = index.dense.as_ref().unwrap();
        let outside = dense.base + dense.slots.len() as i128;
        index.insert(outside, 700_000);
        let before = (budget.used(), budget.peak());
        for _ in 0..3 {
            assert!(!index.prepare(&budget));
            assert_eq!((budget.used(), budget.peak()), before);
            assert_eq!(index.get(outside), Some(700_000));
        }
        for n in 0..700_000 {
            assert_eq!(index.get(n as i128 * 11), Some(n));
        }
        assert_eq!(index.get(outside + 1), None);
        drop(index);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn wide_ids_keep_exact_hash_fallback_without_repeated_dense_growth() {
        let budget = Budget::new(4096);
        let mut index = NumericGroups::default();
        let wide = u32::MAX as usize;
        let mut wide_first = NumericGroups::default();
        wide_first.insert(i128::MIN, wide);
        assert!(!wide_first.prepare(&budget));
        wide_first.insert(0, wide - 1);
        assert!(!wide_first.prepare(&budget)); // Too small to amortize metadata.
        wide_first.insert(1, 0);
        wide_first.insert(2, 1);
        assert!(wide_first.prepare(&budget));
        assert_eq!(wide_first.get(i128::MIN), Some(wide));
        assert_eq!(wide_first.get(0), Some(wide - 1));
        assert_eq!(wide_first.get(1), Some(0));
        assert_eq!(wide_first.get(2), Some(1));
        assert!(!wide_first.prepare(&budget));
        drop(wide_first);
        assert_eq!(budget.used(), 0);
        let mut expected = HashMap::new();
        for n in 0..64 {
            index.insert(n * 2, n as usize);
            expected.insert(n * 2, n as usize);
        }
        assert!(index.prepare(&budget));
        // The sentinel boundary, a larger ID inside the existing dense window,
        // and wide-ID keys outside it all preserve their original usize IDs.
        for (key, id) in [(1, wide), (3, wide + 1), (i128::MAX, usize::MAX - 1)] {
            index.insert(key, id);
            expected.insert(key, id);
        }
        let before = budget.used();
        for _ in 0..4 {
            assert!(!index.prepare(&budget));
            assert_eq!(budget.used(), before);
        }
        for (key, id) in [(127, wide - 1), (200, 64)] {
            index.insert(key, id);
            expected.insert(key, id);
        }
        let occupied = budget.reserve(4096 - budget.used()).unwrap();
        assert!(!index.prepare(&budget));
        for (key, id) in &expected {
            assert_eq!(index.get(*key), Some(*id));
        }
        drop(occupied);
        assert!(index.prepare(&budget));
        let before = budget.used();
        for _ in 0..4 {
            assert!(!index.prepare(&budget));
            assert_eq!(budget.used(), before);
        }
        for (key, id) in expected {
            assert_eq!(index.get(key), Some(id));
        }
        assert_eq!(index.get(5), None);
        assert_eq!(index.get(i128::MIN), None);
        assert_eq!(index.hash.len(), 3);
        assert!(index.hash.capacity() < 64);
        drop(index);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dense_growth_copies_contained_windows_and_relocates_occupied_padding() {
        let budget = Budget::new(4096);
        for (old_base, base, positions) in [
            (3, 0, vec![0, 3, 7]),
            (-3, -9, vec![0, 2, 7]),
            (0, 2, vec![3, 4]),
            (12, 0, vec![0, 2]),
            (i128::MIN, i128::MIN, vec![0, 3, 7]),
            (i128::MAX - 3, i128::MAX - 6, vec![0, 2, 3]),
        ] {
            let mut old = Dense {
                base: old_base,
                slots: vec![u32::MAX; 8],
                _lease: budget.reserve(320).unwrap(),
            };
            let mut expected = vec![u32::MAX; 16];
            for (id, position) in positions.into_iter().enumerate() {
                old.slots[position] = id as u32;
                let key = old_base.checked_add(position as i128).unwrap();
                expected[offset(key, base).unwrap()] = id as u32;
            }
            let allocation = budget.reserve(384).unwrap();
            let actual = grown_slots(Some(&old), base, 16);
            assert_eq!(actual, expected);
            drop(actual);
            drop(allocation);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn adaptive_lookup_preserves_ids_through_growth_extremes_and_sparse_outliers() {
        for base in [-1000, i128::MIN, i128::MAX - 10000] {
            let budget = Budget::new(1 << 20);
            let mut index = NumericGroups::default();
            let mut expected = HashMap::new();
            for batch in 0..16 {
                // Descending and ascending bounds, repeated keys, stable IDs.
                for n in 0..64 {
                    let key = base + ((batch * 37 + n * 17) % 700) as i128;
                    assert_eq!(index.get(key), expected.get(&key).copied());
                    if !expected.contains_key(&key) {
                        let id = expected.len();
                        expected.insert(key, id);
                        index.insert(key, id);
                    }
                }
                index.prepare(&budget);
                for (key, id) in &expected {
                    assert_eq!(index.get(*key), Some(*id));
                }
            }
            assert!(index.dense_bytes() > 0);
            for key in [i128::MIN, i128::MAX, -1, 0, 1] {
                if !expected.contains_key(&key) {
                    let id = expected.len();
                    expected.insert(key, id);
                    index.insert(key, id);
                }
            }
            assert!(!index.prepare(&budget));
            for (key, id) in expected {
                assert_eq!(index.get(key), Some(id));
            }
            drop(index);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn sparse_and_low_budget_paths_keep_hashing_without_losing_groups() {
        let budget = Budget::new(4096);
        let mut index = NumericGroups::default();
        assert!(!index.prepare(&budget));
        for n in 0..100 {
            index.insert(n * 1000000, n as usize);
        }
        assert!(!index.prepare(&budget));
        assert_eq!(budget.used(), 0);
        let mut compact = NumericGroups::default();
        for n in 0..100 {
            compact.insert(n, n as usize);
        }
        let occupied = budget.reserve(4096).unwrap();
        assert!(!compact.prepare(&budget));
        for n in 0..100 {
            assert_eq!(compact.get(n), Some(n as usize));
        }
        drop(occupied);
        assert!(compact.prepare(&budget));
        let original_bytes = compact.dense_bytes();
        let remaining = budget.reserve(4096 - budget.used()).unwrap();
        for n in -100..0 {
            compact.insert(n, (100 - n) as usize);
        }
        assert!(!compact.prepare(&budget));
        assert_eq!(compact.dense_bytes(), original_bytes);
        for n in -100..100 {
            assert_eq!(
                compact.get(n),
                Some(if n < 0 {
                    (100 - n) as usize
                } else {
                    n as usize
                })
            );
        }
        drop(remaining);
        assert!(compact.prepare(&budget));
        assert_eq!(compact.get(i128::MIN), None);
        assert_eq!(compact.get(i128::MAX), None);
        drop(compact);
        assert_eq!(budget.used(), 0);
    }
}
