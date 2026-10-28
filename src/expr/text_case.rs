//! UPPER/LOWER with DuckDB 1.5.6's one-to-one Unicode case mapping (approved
//! October 16, 2026). Every code point maps to exactly one code point: no
//! locale, no final-sigma context and no multi-character expansions. The table
//! is generated from DuckDB itself; see tools/testing/string_functions_reference.py.
use super::*;
mod table;

fn lookup(table: &[u32], c: char) -> char {
    let pairs = table.len() / 2;
    let (mut low, mut high) = (0, pairs);
    while low < high {
        let middle = (low + high) / 2;
        match table[2 * middle].cmp(&(c as u32)) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                return char::from_u32(table[2 * middle + 1]).expect("generated scalar value");
            }
        }
    }
    c
}

/// Map one Unicode scalar value. ASCII stays on the locale-independent path.
pub(crate) fn map_char(c: char, upper: bool) -> char {
    if c.is_ascii() {
        if upper {
            c.to_ascii_uppercase()
        } else {
            c.to_ascii_lowercase()
        }
    } else if upper {
        lookup(&table::UPPER, c)
    } else {
        lookup(&table::LOWER, c)
    }
}

/// Exact UTF-8 length of the converted text.
fn mapped_len(text: &str, upper: bool) -> usize {
    if text.is_ascii() {
        return text.len();
    }
    text.chars().map(|c| map_char(c, upper).len_utf8()).sum()
}

pub(crate) fn convert_into(text: &str, upper: bool, out: &mut String) {
    if text.is_ascii() {
        let start = out.len();
        out.push_str(text);
        if upper {
            out[start..].make_ascii_uppercase();
        } else {
            out[start..].make_ascii_lowercase();
        }
        return;
    }
    out.extend(text.chars().map(|c| map_char(c, upper)));
}

pub(super) fn evaluate(array: &ArrayRef, upper: bool, budget: &Budget) -> Result<Value> {
    let input = array.as_any().downcast_ref::<StringArray>().unwrap();
    let mut bytes = 0usize;
    let mut scratch = 0usize;
    for text in input.iter().flatten() {
        let converted = mapped_len(text, upper);
        bytes = bytes
            .checked_add(converted)
            .ok_or("resource-limit: case conversion size overflow")?;
        scratch = scratch.max(converted);
    }
    if bytes > i32::MAX as usize {
        return Err("resource-limit: case conversion exceeds 32-bit offsets".into());
    }
    // Prepay output values, offsets/validity (including alignment), builder
    // metadata and the largest temporary converted string before allocating.
    let reserve = input
        .len()
        .checked_add(1)
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(bytes))
        .and_then(|n| n.checked_add(scratch))
        .and_then(|n| n.checked_add(512))
        .ok_or("resource-limit: case conversion size overflow")?;
    let lease = budget.reserve(reserve)?;
    let mut output = StringBuilder::with_capacity(input.len(), bytes);
    let mut converted = String::with_capacity(scratch);
    for text in input {
        if let Some(text) = text {
            converted.clear();
            convert_into(text, upper, &mut converted);
            output.append_value(&converted);
        } else {
            output.append_null();
        }
    }
    Ok(Value::allocated(Arc::new(output.finish()), lease))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_conversion_reserves_payload_and_releases_on_errors_and_drop() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("AbC".repeat(4096)),
            None,
            Some(String::new()),
            Some("Straße İ ΟΔΟΣ".into()),
        ]));
        let small = Budget::new(1024);
        assert!(
            evaluate(&input, false, &small)
                .err()
                .unwrap()
                .starts_with("resource-limit:")
        );
        let budget = Budget::new(1 << 20);
        let value = evaluate(&input, false, &budget).unwrap();
        let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(0), "abc".repeat(4096));
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "");
        assert_eq!(array.value(3), "straße i οδοσ");
        drop(value);
        let upper = evaluate(&input, true, &budget).unwrap();
        let array = upper.array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(3), "STRAẞE İ ΟΔΟΣ");
        drop(upper);
        // The full budget can be reserved after both the output and failed
        // conversion have gone out of scope; neither path leaks its credit.
        let _all = budget.reserve(1 << 20).unwrap();
    }

    #[test]
    fn generated_table_is_sorted_and_maps_scalars() {
        for table in [&table::UPPER[..], &table::LOWER[..]] {
            assert_eq!(table.len() % 2, 0);
            for pair in table.chunks(2) {
                assert!(char::from_u32(pair[0]).is_some() && char::from_u32(pair[1]).is_some());
            }
            assert!(
                table
                    .chunks(2)
                    .zip(table.chunks(2).skip(1))
                    .all(|(a, b)| a[0] < b[0])
            );
        }
    }
}
