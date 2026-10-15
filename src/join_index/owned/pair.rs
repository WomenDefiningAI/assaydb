//! Direct addressing for a completed numeric-pair index with one unique component.
use crate::{
    keys::Keys,
    memory::{Budget, Reservation},
};

pub(super) struct PairIndex {
    heads: Vec<u32>,
    base: i128,
    component: usize,
    count: usize,
    _lease: Reservation,
}

impl PairIndex {
    /// A checked ascending component locates one head; full pair equality remains mandatory.
    pub(super) fn new(
        keys: &Keys,
        old: &super::super::HashIndex<'_>,
        budget: &Budget,
    ) -> Option<Self> {
        let pairs = keys.numeric_pairs()?;
        if old.is_empty() {
            return None;
        }
        // Prove uniqueness in source order, stopping as soon as both columns
        // repeat or descend. Typical many-to-many pair keys then decline after
        // a short prefix instead of paying another full hash-table traversal.
        let mut valid = pairs
            .iter()
            .enumerate()
            .filter(|(row, _)| keys.valid(*row))
            .map(|(_, pair)| crate::keys::decode_numeric_pair(pair));
        let low = valid.next()??;
        let mut high = low;
        let mut ascending = [true; 2];
        for pair in valid {
            let pair = pair?;
            for i in 0..2 {
                ascending[i] &= pair[i] > high[i];
            }
            if !ascending[0] && !ascending[1] {
                return None;
            }
            high = pair;
        }
        let (component, slots) = (0..2)
            .filter(|&i| ascending[i])
            .filter_map(|i| {
                let slots = usize::try_from(high[i].checked_sub(low[i])?.checked_add(1)?).ok()?;
                // A smaller domain cannot be unique; sparse domains retain hashing.
                (slots >= old.len() && slots <= old.len().saturating_mul(12)).then_some((i, slots))
            })
            .min_by_key(|(_, slots)| *slots)?;
        let lease = budget
            .reserve(slots.checked_mul(4)?.checked_add(256)?)
            .ok()?;
        let mut index = Self {
            heads: vec![u32::MAX; slots],
            base: low[component],
            component,
            count: old.len(),
            _lease: lease,
        };
        for entry in &old.map {
            let head = entry.head;
            let pair = crate::keys::decode_numeric_pair(&pairs[head as usize])?;
            // Bounds were checked over these exact immutable heads above.
            let slot = usize::try_from(pair[component].checked_sub(index.base)?).ok()?;
            let target = index.heads.get_mut(slot)?;
            if *target != u32::MAX {
                return None;
            }
            *target = head;
        }
        Some(index)
    }

    /// NULLs, out-of-range components and crossed pairs miss exactly as in the hash table.
    pub(super) fn head(&self, key: &[u8], keys: &Keys) -> Option<u32> {
        let pair = crate::keys::decode_numeric_pair(key)?;
        let slot = usize::try_from(pair[self.component].checked_sub(self.base)?).ok()?;
        let head = *self.heads.get(slot)?;
        (head != u32::MAX && keys.row(head as usize) == key).then_some(head)
    }

    /// Each occupied slot corresponds to one distinct full key from the old table.
    pub(super) fn len(&self) -> usize {
        self.count
    }
}
