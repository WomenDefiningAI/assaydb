//! STRING_AGG finalization: sort each group's packed entries by ORDER BY key,
//! refuse ties between different values, then join values with the separator.
//!
//! Entries are sorted by (key bytes, value bytes), so the result never depends
//! on input order, workers, batches or spill. Equal keys with equal values are
//! indistinguishable; equal keys with different values have an unspecified SQL
//! order (DuckDB and PostgreSQL differ), so that case is refused.
use super::*;
use crate::execute::Key;

fn entries(buffer: &[u8]) -> Result<Vec<(&[u8], &[u8])>> {
    let mut out = Vec::new();
    let mut rest = buffer;
    while !rest.is_empty() {
        let (length, tail) = rest
            .split_first_chunk::<4>()
            .ok_or("internal: malformed STRING_AGG state")?;
        let length = u32::from_le_bytes(*length) as usize;
        if length > tail.len() {
            return Err("internal: malformed STRING_AGG state".into());
        }
        let (entry, tail) = tail.split_at(length);
        out.push(crate::expr::sort_pack::split(entry)?);
        rest = tail;
    }
    Ok(out)
}

pub(super) fn output(slots: &[Option<Key>], separator: &str, budget: &Budget) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(slots.len(), 0);
    let mut total = 0_usize;
    let mut joined = Vec::new();
    for slot in slots {
        let buffer = match slot {
            None => {
                builder.append_null();
                continue;
            }
            Some(Key::Binary(buffer)) => buffer,
            Some(_) => return Err("internal: STRING_AGG state is not a buffer".into()),
        };
        let _sort = budget.reserve(
            buffer
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(1024))
                .ok_or("STRING_AGG output size overflow")?,
        )?;
        let mut values = entries(buffer)?;
        values.sort_unstable();
        if values
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1)
        {
            return Err("unsupported: STRING_AGG ORDER BY ties between different values (their order is unspecified)".into());
        }
        joined.clear();
        for (index, (_, value)) in values.iter().enumerate() {
            if index > 0 {
                joined.extend_from_slice(separator.as_bytes());
            }
            joined.extend_from_slice(value);
        }
        total = total
            .checked_add(joined.len())
            .filter(|n| *n <= i32::MAX as usize)
            .ok_or("resource-limit: STRING_AGG output batch exceeds 2 GiB")?;
        // Every value came from a valid UTF-8 TEXT array and the separator is
        // UTF-8; concatenation preserves validity.
        builder.append_value(std::str::from_utf8(&joined).map_err(|e| e.to_string())?);
    }
    Ok(Arc::new(builder.finish()))
}
