//! Exact encoded-key lookup with portable inline comparisons for short keys.
use std::hash::{BuildHasher, Hash, Hasher};

pub(crate) struct EncodedGroups {
    map: hashbrown::HashMap<Box<[u8]>, usize, ahash::RandomState>,
}
impl Default for EncodedGroups {
    fn default() -> Self {
        Self {
            map: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
        }
    }
}
impl EncodedGroups {
    pub fn into_entries(self) -> impl Iterator<Item = (Box<[u8]>, usize)> {
        self.map.into_iter()
    }
    #[cfg(test)]
    pub fn entries(&self) -> impl Iterator<Item = (&[u8], usize)> {
        self.map.iter().map(|(key, id)| (key.as_ref(), *id))
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<usize> {
        lookup(&self.map, key)
    }
    pub fn insert(&mut self, key: Box<[u8]>, group: usize) {
        self.map.insert(key, group);
    }
}

const CACHE_SLOTS: usize = 128;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ShortKey {
    head: u64,
    tail: u64,
    len: usize,
}
impl ShortKey {
    #[inline]
    fn new(key: &[u8]) -> Option<Self> {
        let len = key.len();
        let (head, tail) = match len {
            0 => (0, 0),
            1 => (u64::from(key[0]), 0),
            2..=3 => (
                u64::from(u16::from_ne_bytes(key[..2].try_into().unwrap())),
                u64::from(u16::from_ne_bytes(key[len - 2..].try_into().unwrap())),
            ),
            4..=7 => (
                u64::from(u32::from_ne_bytes(key[..4].try_into().unwrap())),
                u64::from(u32::from_ne_bytes(key[len - 4..].try_into().unwrap())),
            ),
            8..=16 => (
                u64::from_ne_bytes(key[..8].try_into().unwrap()),
                u64::from_ne_bytes(key[len - 8..].try_into().unwrap()),
            ),
            _ => return None,
        };
        Some(Self { head, tail, len })
    }
    #[inline]
    fn slot(self) -> usize {
        // Only an index: complete length/head/tail equality remains mandatory.
        // Overlapping windows cover all bytes, including embedded NUL bytes.
        let mixed = (self.head ^ self.tail.rotate_left(23) ^ self.len as u64)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15);
        (mixed >> 57) as usize
    }
}
#[derive(Clone, Copy)]
struct Entry {
    key: ShortKey,
    group: usize,
}
/// Batch-local acceleration funded only from already reserved, unspent group
/// state credit. Release it before a credit refill so it cannot cause a new
/// reservation or change the fitting-budget boundary. No query-global cache.
pub(crate) struct ShortCache {
    entries: Option<Box<[Entry]>>,
    pub hits: usize,
    pub misses: usize,
    pub bypasses: usize,
    pub bytes: usize,
}
impl ShortCache {
    const BYTES: usize = CACHE_SLOTS * std::mem::size_of::<Entry>() + 256;
    pub fn new(enabled: bool, credit: &mut usize) -> Self {
        let entries = if enabled && *credit >= Self::BYTES {
            *credit -= Self::BYTES;
            Some(
                vec![
                    Entry {
                        key: ShortKey::default(),
                        group: usize::MAX
                    };
                    CACHE_SLOTS
                ]
                .into_boxed_slice(),
            )
        } else {
            None
        };
        let bytes = usize::from(entries.is_some()) * Self::BYTES;
        Self {
            entries,
            hits: 0,
            misses: 0,
            bypasses: 0,
            bytes,
        }
    }
    #[inline]
    pub fn get(&mut self, map: &EncodedGroups, key: &[u8]) -> Option<usize> {
        let Some(entries) = &mut self.entries else {
            return map.get(key);
        };
        let Some(short) = ShortKey::new(key) else {
            self.bypasses += 1;
            return map.get(key);
        };
        let entry = &mut entries[short.slot()];
        if entry.group != usize::MAX && entry.key == short {
            self.hits += 1;
            return Some(entry.group);
        }
        self.misses += 1;
        let group = map.get(key)?;
        *entry = Entry { key: short, group };
        Some(group)
    }
    pub fn release(&mut self, credit: &mut usize) {
        if let Some(entries) = self.entries.take() {
            drop(entries);
            *credit += Self::BYTES;
        }
    }
}
#[inline]
#[allow(clippy::manual_hash_one)] // Match hashbrown insertion, not specialized hash_one.
fn lookup<S: BuildHasher>(
    map: &hashbrown::HashMap<Box<[u8]>, usize, S>,
    key: &[u8],
) -> Option<usize> {
    let mut hasher = map.hasher().build_hasher();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    map.raw_entry()
        .from_hash(hash, |stored| equal(stored, key))
        .map(|(_, group)| *group)
}

#[inline]
fn equal(a: &[u8], b: &[u8]) -> bool {
    let n = a.len();
    if n != b.len() {
        return false;
    }
    // Equal-length head/tail windows cover every byte in each size class.
    // Native-endian loads preserve equality on every architecture. No unaligned
    // pointer reads, CPU intrinsics, padding reads or hash-only matches are used.
    if n > 32 {
        a == b
    } else if n >= 16 {
        u128::from_ne_bytes(a[..16].try_into().unwrap())
            == u128::from_ne_bytes(b[..16].try_into().unwrap())
            && u128::from_ne_bytes(a[n - 16..].try_into().unwrap())
                == u128::from_ne_bytes(b[n - 16..].try_into().unwrap())
    } else if n >= 8 {
        u64::from_ne_bytes(a[..8].try_into().unwrap())
            == u64::from_ne_bytes(b[..8].try_into().unwrap())
            && u64::from_ne_bytes(a[n - 8..].try_into().unwrap())
                == u64::from_ne_bytes(b[n - 8..].try_into().unwrap())
    } else if n >= 4 {
        u32::from_ne_bytes(a[..4].try_into().unwrap())
            == u32::from_ne_bytes(b[..4].try_into().unwrap())
            && u32::from_ne_bytes(a[n - 4..].try_into().unwrap())
                == u32::from_ne_bytes(b[n - 4..].try_into().unwrap())
    } else if n >= 2 {
        u16::from_ne_bytes(a[..2].try_into().unwrap())
            == u16::from_ne_bytes(b[..2].try_into().unwrap())
            && u16::from_ne_bytes(a[n - 2..].try_into().unwrap())
                == u16::from_ne_bytes(b[n - 2..].try_into().unwrap())
    } else {
        n == 0 || a[0] == b[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasherDefault, Hasher};

    #[test]
    fn equality_matches_every_byte_and_length_at_unaligned_offsets() {
        for len in (0..=80).chain([127, 128, 255, 256, 1024]) {
            let bytes = (0..len + 16).map(|i| (i * 37) as u8).collect::<Vec<_>>();
            for offset in 0..16 {
                let a = &bytes[offset..offset + len];
                let mut b = a.to_vec();
                assert!(equal(a, &b));
                for index in 0..len {
                    b[index] ^= 0x80;
                    assert!(
                        !equal(a, &b),
                        "missed byte {index} at length {len}, offset {offset}"
                    );
                    b[index] ^= 0x80;
                }
                b.push(0);
                assert!(!equal(a, &b));
                if len > 0 {
                    assert!(!equal(a, &a[..len - 1]));
                }
            }
        }
    }

    #[derive(Default)]
    struct CollisionHasher;
    impl Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }
    #[test]
    fn forced_hash_collisions_keep_complete_byte_equality_across_growth() {
        let mut map =
            hashbrown::HashMap::with_hasher(BuildHasherDefault::<CollisionHasher>::default());
        let mut control = std::collections::HashMap::<Box<[u8]>, usize>::new();
        for len in 0..=80 {
            for variant in 0..4 {
                let key = (0..len)
                    .map(|i| (i * 17 + variant * 31) as u8)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let group = len * 4 + variant;
                map.insert(key.clone(), group);
                control.insert(key, group);
                for (key, group) in &control {
                    assert_eq!(lookup(&map, key), Some(*group));
                }
            }
        }
        for key in control.keys() {
            for index in 0..key.len() {
                let mut missing = key.to_vec();
                missing[index] ^= 0x80;
                assert_eq!(
                    lookup(&map, &missing),
                    control.get(missing.as_slice()).copied()
                );
            }
        }
    }

    #[test]
    fn seeded_lookup_matches_standard_slice_hashing() {
        let seed = ahash::RandomState::with_seeds(1, 2, 3, 4);
        let mut map = hashbrown::HashMap::with_hasher(seed.clone());
        let mut control = std::collections::HashMap::with_hasher(seed);
        for len in 0..=129 {
            let key = vec![len as u8; len].into_boxed_slice();
            map.insert(key.clone(), len);
            control.insert(key, len);
        }
        for (key, group) in &control {
            assert_eq!(lookup(&map, key), Some(*group));
        }
    }

    #[test]
    fn short_cache_compares_every_byte_and_length_under_slot_collisions() {
        let mut map = EncodedGroups::default();
        let mut keys = Vec::new();
        for len in 0..=33 {
            for variant in 0..19 {
                let mut key = vec![0; len];
                for (i, byte) in key.iter_mut().enumerate() {
                    *byte = (variant * 19 + i * 37) as u8;
                }
                if map.get(&key).is_none() {
                    let group = keys.len();
                    map.insert(key.clone().into_boxed_slice(), group);
                    keys.push(key);
                }
            }
        }
        let mut credit = ShortCache::BYTES;
        let mut cache = ShortCache::new(true, &mut credit);
        assert_eq!(credit, 0);
        for _ in 0..3 {
            for (group, key) in keys.iter().enumerate().rev() {
                assert_eq!(cache.get(&map, key), Some(group));
                assert_eq!(cache.get(&map, key), Some(group));
                for i in 0..key.len() {
                    let mut changed = key.clone();
                    changed[i] ^= 0x80;
                    assert_eq!(cache.get(&map, &changed), map.get(&changed));
                    if key.len() <= 16 {
                        assert_ne!(ShortKey::new(key), ShortKey::new(&changed));
                    }
                }
                let mut longer = key.clone();
                longer.push(0);
                assert_eq!(cache.get(&map, &longer), map.get(&longer));
            }
        }
        assert!(cache.hits > 0 && cache.misses > CACHE_SLOTS && cache.bypasses > 0);
        // Pigeonhole: these distinct short keys must contend for cache slots.
        assert!(keys.iter().filter(|k| k.len() <= 16).count() > CACHE_SLOTS);
        cache.release(&mut credit);
        assert_eq!(credit, ShortCache::BYTES);
        cache.release(&mut credit);
        assert_eq!(credit, ShortCache::BYTES);
    }

    #[test]
    fn cache_credit_is_prepaid_and_refunded_before_group_growth() {
        for initial in [0, ShortCache::BYTES - 1, ShortCache::BYTES, 64 * 1024] {
            let mut credit = initial;
            let mut cache = ShortCache::new(true, &mut credit);
            assert_eq!(cache.bytes > 0, initial >= ShortCache::BYTES);
            assert_eq!(credit + cache.bytes, initial);
            // The allocation would fit without the optional cache. Release
            // before evaluating the ordinary refill condition and it fits too.
            let allocation = initial;
            if credit < allocation {
                cache.release(&mut credit);
            }
            assert!(credit >= allocation);
            credit -= allocation;
            cache.release(&mut credit);
            assert_eq!(credit, 0);
        }
        let mut credit = 64 * 1024;
        let mut cache = ShortCache::new(false, &mut credit);
        cache.release(&mut credit);
        assert_eq!(credit, 64 * 1024);
    }
}
