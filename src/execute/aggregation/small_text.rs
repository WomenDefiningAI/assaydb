//! Exact direct lookup for two validated one-byte text fields; no heap scratch.
use super::Key;
use crate::keys::CompactText;

pub(super) const TABLE_BYTES: usize = 128 * 128;

/// Known ASCII pairs map to their original group IDs; a miss leaves IDs empty and state untouched.
pub(super) fn fill_existing(
    compact: &CompactText<'_>,
    keys: &[Vec<Key>],
    ids: &mut Vec<usize>,
) -> bool {
    let Some((left, right)) = compact.byte_pairs() else {
        return false;
    };
    if keys.len() != 2
        || keys[0].is_empty()
        || keys[0].len() > super::narrow::GROUPS
        || keys[0].len() != keys[1].len()
        || !ids.is_empty()
        || ids.capacity() < left.len()
    {
        return false;
    }
    // One-byte valid UTF-8 is ASCII, so both coordinates are strictly below 128.
    // A u8::MAX entry is disjoint from every admitted group ID (0..64).
    let mut table = [u8::MAX; TABLE_BYTES];
    for (group, (a, b)) in keys[0].iter().zip(&keys[1]).enumerate() {
        if let (Key::Text(a), Key::Text(b)) = (a, b)
            && a.len() == 1
            && b.len() == 1
        {
            table[usize::from(a.as_bytes()[0]) * 128 + usize::from(b.as_bytes()[0])] = group as u8;
        }
    }
    let mut missing = false;
    for (&a, &b) in left.iter().zip(right) {
        let group = table[usize::from(a) * 128 + usize::from(b)];
        missing |= group == u8::MAX;
        ids.push(usize::from(group));
    }
    if missing {
        ids.clear();
    }
    !missing
}
