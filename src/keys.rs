//! Batch key encoding. Borrow row bytes for lookups; own keys only when needed.
//! Equality-only layouts avoid sortable encodings for numeric and UTF-8 keys;
//! Arrow provides the general mixed-type fallback.
use crate::{
    Result,
    expr::Value,
    memory::{Budget, Reservation},
};
use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, LargeBinaryArray, StringArray},
    buffer::{Buffer, NullBuffer, OffsetBuffer},
    row::{RowConverter, Rows, SortField},
};
use std::sync::Arc;

pub(crate) mod partitioned;

// Keep tiny field copies inline instead of dispatching a variable-size memcpy
// for every row component. The canonical length-prefixed bytes stay identical.
#[inline]
fn append_text(bytes: &mut Vec<u8>, value: &[u8]) {
    match value {
        [] => {}
        [b0] => bytes.push(*b0),
        [b0, b1] => bytes.extend_from_slice(&[*b0, *b1]),
        [b0, b1, b2] => bytes.extend_from_slice(&[*b0, *b1, *b2]),
        [b0, b1, b2, b3] => bytes.extend_from_slice(&[*b0, *b1, *b2, *b3]),
        [b0, b1, b2, b3, b4] => bytes.extend_from_slice(&[*b0, *b1, *b2, *b3, *b4]),
        [b0, b1, b2, b3, b4, b5] => bytes.extend_from_slice(&[*b0, *b1, *b2, *b3, *b4, *b5]),
        [b0, b1, b2, b3, b4, b5, b6] => {
            bytes.extend_from_slice(&[*b0, *b1, *b2, *b3, *b4, *b5, *b6])
        }
        [b0, b1, b2, b3, b4, b5, b6, b7] => {
            bytes.extend_from_slice(&[*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7])
        }
        _ => bytes.extend_from_slice(value),
    }
}

struct TextRows {
    bytes: Vec<u8>,
    offsets: Vec<usize>,
}

/// Validated fixed-width UTF-8 fields produce the existing canonical tuple bytes.
pub(crate) struct CompactText<'a> {
    fields: [Option<(&'a [u8], usize)>; 3],
    template: [u8; 16],
    len: usize,
}
impl<'a> CompactText<'a> {
    /// Admission checks every offset and NULL bit before borrowing short payloads.
    pub(crate) fn new(values: &'a [Value], rows: usize) -> Option<Self> {
        if rows == 0 || !(2..=3).contains(&values.len()) {
            return None;
        }
        let mut compact = Self {
            fields: [None; 3],
            template: [0; 16],
            len: 0,
        };
        for (slot, value) in values.iter().enumerate() {
            let array = value.array.as_any().downcast_ref::<StringArray>()?;
            if array.len() != rows || array.null_count() != 0 {
                return None;
            }
            let offsets = array.value_offsets();
            let width = usize::try_from(offsets[1] - offsets[0]).ok()?;
            let end = compact.len.checked_add(4)?.checked_add(width)?;
            if end > compact.template.len()
                || offsets
                    .windows(2)
                    .any(|pair| (pair[1] - pair[0]) as usize != width)
            {
                return None;
            }
            compact.template[compact.len..compact.len + 4]
                .copy_from_slice(&(width as u32).to_ne_bytes());
            compact.fields[slot] = Some((
                &array.value_data()[offsets[0] as usize..offsets[rows] as usize],
                width,
            ));
            compact.len = end;
        }
        Some(compact)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Validated one-byte UTF-8 fields are ASCII slices in the original row order.
    pub(crate) fn byte_pairs(&self) -> Option<(&'a [u8], &'a [u8])> {
        match self.fields {
            [Some((a, 1)), Some((b, 1)), None] => Some((a, b)),
            _ => None,
        }
    }

    /// The validated widths make this stack key byte-identical to the fallback.
    #[inline]
    pub(crate) fn row(&self, row: usize) -> [u8; 16] {
        let mut key = self.template;
        let mut offset = 0;
        for (bytes, width) in self.fields.iter().flatten() {
            offset += 4;
            key[offset..offset + width].copy_from_slice(&bytes[row * width..(row + 1) * width]);
            offset += width;
        }
        key
    }

    /// The caller's encoder lease funds canonical transport bytes and both offset vectors.
    fn encode(&self, rows: usize) -> Result<TextRows> {
        let capacity = rows.checked_mul(self.len).ok_or("text key size overflow")?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut offsets =
            Vec::with_capacity(rows.checked_add(1).ok_or("text key offsets overflow")?);
        if let Some((left, right)) = self.byte_pairs() {
            // Constant positions avoid tuple construction/variable-size copies
            // for each field; the canonical length prefixes remain unchanged.
            let mut key: [u8; 10] = self.template[..10].try_into().unwrap();
            for (&a, &b) in left.iter().zip(right) {
                offsets.push(bytes.len());
                key[4] = a;
                key[9] = b;
                bytes.extend_from_slice(&key);
            }
        } else {
            for row in 0..rows {
                offsets.push(bytes.len());
                bytes.extend_from_slice(&self.row(row)[..self.len]);
            }
        }
        offsets.push(bytes.len());
        Ok(TextRows { bytes, offsets })
    }
}
pub(crate) struct Keys {
    len: usize,
    rows: Option<Rows>,
    numeric: Option<Buffer>,
    numeric_pair: Option<Vec<[u8; 34]>>,
    text: Option<StringArray>,
    text_rows: Option<TextRows>,
    prepared_text: Option<LargeBinaryArray>,
    nulls: Option<NullBuffer>,
    _lease: Reservation,
}
impl Keys {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn funded_by(&self, budget: &Budget) -> bool {
        self._lease.budget().same_domain(budget)
    }
    pub fn new(values: &[Value], len: usize, budget: &Budget) -> Result<Self> {
        Self::with_text_encoding(values, len, budget, true)
    }
    pub fn with_text_encoding(
        values: &[Value],
        len: usize,
        budget: &Budget,
        compact: bool,
    ) -> Result<Self> {
        let estimate = values
            .iter()
            .try_fold(256_usize, |size, v| {
                size.checked_add(v.array.get_array_memory_size().saturating_mul(2))
                    .ok_or("key buffer size overflow")
            })?
            .checked_add(
                len.checked_mul(16 + values.len() * 2)
                    .ok_or("key offsets overflow")?,
            )
            .ok_or("key buffer size overflow")?;
        let lease = budget.reserve(estimate)?;
        Self::with_credit(values, len, compact, lease)
    }

    // Private to the encoder and its funded partition constructor.
    fn with_credit(
        values: &[Value],
        len: usize,
        compact: bool,
        lease: Reservation,
    ) -> Result<Self> {
        if values.len() == 1
            && let Some(array) = values[0].array.as_any().downcast_ref::<Decimal128Array>()
        {
            // Equality hashing needs no sortable encoding. Keep the existing
            // 16-byte numeric values, including slice offsets, without copying.
            // A NULL uses an empty slice, distinct from every 16-byte value.
            // Key layouts are private to one typed operator, never persisted.
            return Ok(Self {
                rows: None,
                text: None,
                text_rows: None,
                prepared_text: None,
                numeric: Some(array.values().inner().clone()),
                numeric_pair: None,
                nulls: array.logical_nulls(),
                len,
                _lease: lease,
            });
        }
        if values.len() == 2
            && let (Some(a), Some(b)) = (
                values[0].array.as_any().downcast_ref::<Decimal128Array>(),
                values[1].array.as_any().downcast_ref::<Decimal128Array>(),
            )
        {
            // Equality keys need neither byte-order conversion nor per-row
            // offsets. Keep both validity bits, so GROUP BY distinguishes
            // (NULL, x), (x, NULL), and (NULL, NULL), with canonical NULL bytes.
            len.checked_mul(34).ok_or("pair key size overflow")?;
            let encode = |a: Option<i128>, b: Option<i128>| {
                let mut key = [0; 34];
                key[0] = u8::from(a.is_some());
                key[1] = u8::from(b.is_some());
                key[2..18].copy_from_slice(&a.unwrap_or(0).to_ne_bytes());
                key[18..34].copy_from_slice(&b.unwrap_or(0).to_ne_bytes());
                key
            };
            let pairs = if a.null_count() == 0 && b.null_count() == 0 {
                a.values()
                    .iter()
                    .zip(b.values())
                    .map(|(a, b)| encode(Some(*a), Some(*b)))
                    .collect()
            } else {
                a.iter().zip(b.iter()).map(|(a, b)| encode(a, b)).collect()
            };
            return Ok(Self {
                rows: None,
                text: None,
                text_rows: None,
                prepared_text: None,
                numeric: None,
                numeric_pair: Some(pairs),
                nulls: NullBuffer::union(a.nulls(), b.nulls()),
                len,
                _lease: lease,
            });
        }
        if compact
            && !values.is_empty()
            && let Some(arrays) = values
                .iter()
                .map(|v| v.array.as_any().downcast_ref::<StringArray>())
                .collect::<Option<Vec<_>>>()
        {
            let mut nulls = None;
            for a in &arrays {
                nulls = NullBuffer::union(nulls.as_ref(), a.nulls());
            }
            let (text, text_rows) = if arrays.len() == 1 {
                // UTF-8 can never contain the standalone byte 0xff. It serves
                // as NULL's private key, distinct from empty and every string.
                // Clone only buffer owners; slices retain their actual offset.
                (Some(arrays[0].clone()), None)
            } else if let Some(compact) = CompactText::new(values, len) {
                (None, Some(compact.encode(len)?))
            } else {
                let prefixes = len
                    .checked_mul(arrays.len())
                    .and_then(|n| n.checked_mul(4))
                    .ok_or("text key size overflow")?;
                let capacity = arrays.iter().try_fold(prefixes, |n, a| {
                    let offsets = a.value_offsets();
                    let size = (offsets[offsets.len() - 1] - offsets[0]) as usize;
                    n.checked_add(size).ok_or("text key size overflow")
                })?;
                let mut bytes = Vec::with_capacity(capacity);
                let mut offsets =
                    Vec::with_capacity(len.checked_add(1).ok_or("text key offsets overflow")?);
                for row in 0..len {
                    offsets.push(bytes.len());
                    for a in &arrays {
                        if a.is_null(row) {
                            bytes.extend_from_slice(&u32::MAX.to_ne_bytes());
                        } else {
                            let value = a.value(row).as_bytes();
                            // Utf8 uses signed 32-bit offsets, so MAX is never
                            // a valid length. Prefixes make field boundaries
                            // unambiguous even for embedded NUL or long values.
                            bytes.extend_from_slice(&(value.len() as u32).to_ne_bytes());
                            append_text(&mut bytes, value);
                        }
                    }
                }
                offsets.push(bytes.len());
                (None, Some(TextRows { bytes, offsets }))
            };
            return Ok(Self {
                rows: None,
                numeric: None,
                numeric_pair: None,
                text,
                text_rows,
                prepared_text: None,
                nulls,
                len,
                _lease: lease,
            });
        }
        let mut nulls = None;
        let arrays = values
            .iter()
            .map(|v| {
                nulls = NullBuffer::union(nulls.as_ref(), v.array.logical_nulls().as_ref());
                // SQL grouping and joins equate signed zero. Row encoding otherwise
                // retains the existing typed keys, including NULL and NaN payloads.
                if let Some(a) = v.array.as_any().downcast_ref::<Float64Array>()
                    && a.values().iter().any(|n| *n == 0.0 && n.is_sign_negative())
                {
                    Arc::new(Float64Array::from_iter(
                        a.iter().map(|n| n.map(|n| if n == 0.0 { 0.0 } else { n })),
                    )) as ArrayRef
                } else {
                    v.array.clone()
                }
            })
            .collect::<Vec<_>>();
        let rows = if arrays.is_empty() {
            None
        } else {
            let converter = RowConverter::new(
                arrays
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect(),
            )
            .map_err(|e| e.to_string())?;
            Some(
                converter
                    .convert_columns(&arrays)
                    .map_err(|e| e.to_string())?,
            )
        };
        Ok(Self {
            rows,
            text: None,
            text_rows: None,
            prepared_text: None,
            numeric: None,
            numeric_pair: None,
            nulls,
            len,
            _lease: lease,
        })
    }
    // Move the private canonical composite-text bytes into a real Arrow column.
    // Large offsets avoid imposing a new 2 GiB limit on combined Utf8 keys.
    // Keep the encoder reservation alive until the projection bridge is funded.
    pub(crate) fn into_text_column(self) -> Result<(LargeBinaryArray, Reservation)> {
        let rows = self
            .text_rows
            .ok_or("internal: expected composite text keys")?;
        // The encoder allowance includes both old/new offset vectors here.
        let mut offsets = Vec::with_capacity(rows.offsets.len());
        for offset in rows.offsets {
            offsets.push(i64::try_from(offset).map_err(|_| "text key offset overflow")?);
        }
        let array = LargeBinaryArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from_vec(rows.bytes),
            self.nulls,
        );
        Ok((array, self._lease))
    }
    pub(crate) fn from_text_column(array: LargeBinaryArray, budget: &Budget) -> Result<Self> {
        let len = array.len();
        let lease = budget.reserve(array.get_array_memory_size().saturating_add(256))?;
        Ok(Self {
            rows: None,
            numeric: None,
            numeric_pair: None,
            text: None,
            text_rows: None,
            nulls: array.nulls().cloned(),
            prepared_text: Some(array),
            len,
            _lease: lease,
        })
    }
    pub fn all_valid(&self) -> bool {
        self.nulls
            .as_ref()
            .is_none_or(|nulls| nulls.null_count() == 0)
    }
    pub(crate) fn valid_len(&self) -> usize {
        self.len - self.nulls.as_ref().map_or(0, NullBuffer::null_count)
    }
    pub fn numeric_values(&self) -> Option<&[i128]> {
        self.numeric.as_ref().map(|b| b.typed_data())
    }
    /// Expose only the typed two-numeric equality layout, never arbitrary row bytes.
    pub(crate) fn numeric_pairs(&self) -> Option<&[[u8; 34]]> {
        self.numeric_pair.as_deref()
    }
    pub fn row(&self, i: usize) -> &[u8] {
        if let Some(values) = &self.numeric {
            return if self.valid(i) {
                &values.as_slice()[i * 16..(i + 1) * 16]
            } else {
                &[]
            };
        }
        if let Some(pairs) = &self.numeric_pair {
            return &pairs[i];
        }
        if let Some(text) = &self.text {
            return if text.is_null(i) {
                &[0xff]
            } else {
                text.value(i).as_bytes()
            };
        }
        if let Some(rows) = &self.prepared_text {
            // NULL validity is for joins; GROUP BY still uses the full bytes.
            return rows.value(i);
        }
        if let Some(rows) = &self.text_rows {
            return &rows.bytes[rows.offsets[i]..rows.offsets[i + 1]];
        }
        self.rows
            .as_ref()
            .map(|rows| rows.row(i).data())
            .unwrap_or(&[])
    }
    pub fn valid(&self, i: usize) -> bool {
        self.nulls.as_ref().is_none_or(|n| n.is_valid(i))
    }
}

/// Private, query-local equality-key layout; never persisted or sorted.
pub(crate) fn decode_numeric_pair(bytes: &[u8]) -> Option<[i128; 2]> {
    if bytes.len() != 34 || bytes[0] != 1 || bytes[1] != 1 {
        return None;
    }
    Some([
        i128::from_ne_bytes(bytes[2..18].try_into().ok()?),
        i128::from_ne_bytes(bytes[18..34].try_into().ok()?),
    ])
}

#[cfg(test)]
mod compact_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execute::key, expr::normalize_input, types::Type};
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray, new_null_array,
    };
    use arrow::datatypes::DataType;
    #[test]
    fn prepared_text_transport_preserves_bytes_nulls_slices_and_ownership() {
        let budget = Budget::new(32 << 20);
        let long = "é雪".repeat(8192);
        let samples = [
            None,
            Some(""),
            Some("ab"),
            Some("a"),
            Some("bc"),
            Some("c"),
            Some("\0"),
            Some("é🙂"),
            Some(long.as_str()),
        ];
        let len = samples.len() * samples.len();
        let values: Vec<Value> = (0..2)
            .map(|column| {
                let mut rows = vec![Some("discarded prefix")];
                rows.extend((0..len).map(|i| {
                    samples[if column == 0 {
                        i % samples.len()
                    } else {
                        i / samples.len()
                    }]
                }));
                rows.push(Some("discarded suffix"));
                Value::adopt(Arc::new(StringArray::from(rows).slice(1, len)), &budget).unwrap()
            })
            .collect();
        let baseline = budget.used();
        for offset in [0, 1, 7, len] {
            let len = len - offset;
            let sliced = values
                .iter()
                .map(|v| Value::adopt(v.array.slice(offset, len), &budget).unwrap())
                .collect::<Vec<_>>();
            let keys = Keys::new(&sliced, len, &budget).unwrap();
            let expected = (0..len)
                .map(|i| (keys.row(i).to_vec(), keys.valid(i)))
                .collect::<Vec<_>>();
            let data_ptr = keys.text_rows.as_ref().unwrap().bytes.as_ptr();
            let (array, encoding_lease) = keys.into_text_column().unwrap();
            assert_eq!(array.value_data().as_ptr(), data_ptr);
            let steady = budget.used();
            let blocker = budget.reserve(budget.limit() - steady).unwrap();
            assert!(Keys::from_text_column(array.clone(), &budget).is_err());
            drop(blocker);
            assert_eq!(budget.used(), steady);
            let adopted = Keys::from_text_column(array.clone(), &budget).unwrap();
            for (i, (bytes, valid)) in expected.iter().enumerate() {
                assert_eq!(adopted.row(i), bytes);
                assert_eq!(adopted.valid(i), *valid);
            }
            for start in [0, len / 2, len] {
                let subset =
                    Keys::from_text_column(array.slice(start, len - start), &budget).unwrap();
                for (i, (bytes, valid)) in expected[start..].iter().enumerate() {
                    assert_eq!(subset.row(i), bytes);
                    assert_eq!(subset.valid(i), *valid);
                }
            }
            drop(adopted);
            drop(array);
            drop(encoding_lease);
            drop(sliced);
            assert_eq!(budget.used(), baseline);
        }
        drop(values);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn text_equality_layouts_preserve_slices_field_boundaries_and_batch_independence() {
        let budget = Budget::new(32 << 20);
        let long = "é雪".repeat(4096);
        let samples = [
            None,
            Some(""),
            Some("\0"),
            Some("a\0b"),
            Some("ÿ"),
            Some("é🙂"),
            Some("a"),
            Some("abcd"),
            Some("abcde"),
            Some("abcdefg"),
            Some("abcdefgh"),
            Some("ab"),
            Some("bc"),
            Some("c"),
            Some(long.as_str()),
        ];
        let len = samples.len() * samples.len();
        for width in 1..=3 {
            let values = (0..width)
                .map(|column| {
                    let mut rows = vec![Some("prefix")];
                    rows.extend((0..len).map(|i| {
                        samples[match column {
                            0 => i % samples.len(),
                            1 => i / samples.len(),
                            _ => (i * 3) % samples.len(),
                        }]
                    }));
                    rows.push(Some("suffix"));
                    Value::adopt(Arc::new(StringArray::from(rows).slice(1, len)), &budget).unwrap()
                })
                .collect::<Vec<_>>();
            let compact = Keys::new(&values, len, &budget).unwrap();
            let generic = Keys::with_text_encoding(&values, len, &budget, false).unwrap();
            assert_eq!(compact.text.is_some(), width == 1);
            assert_eq!(compact.text_rows.is_some(), width > 1);
            assert!(compact.rows.is_none());
            for i in 0..len {
                assert_eq!(compact.valid(i), generic.valid(i));
                for j in 0..len {
                    let expected = values
                        .iter()
                        .all(|v| key(v, &Type::Text, i) == key(v, &Type::Text, j));
                    assert_eq!(compact.row(i) == compact.row(j), expected);
                    assert_eq!(generic.row(i) == generic.row(j), expected);
                }
            }
            for start in (0..len).step_by(7) {
                let count = 7.min(len - start);
                let slice = values
                    .iter()
                    .map(|v| Value::adopt(v.array.slice(start, count), &budget).unwrap())
                    .collect::<Vec<_>>();
                let encoded = Keys::new(&slice, count, &budget).unwrap();
                for i in 0..count {
                    assert_eq!(compact.row(start + i), encoded.row(i));
                }
            }
            // A retained encoding remains valid after the input owners drop.
            drop(values);
            assert!(!compact.row(0).is_empty());
        }
        assert_eq!(budget.used(), 0);
        let value = Value::adopt(Arc::new(StringArray::from(vec![Some("abc")])), &budget).unwrap();
        let tiny = Budget::new(1);
        assert!(Keys::new(&[value], 1, &tiny).is_err());
        assert_eq!(tiny.used(), 0);
    }
    #[test]
    fn fixed_numeric_pairs_preserve_each_null_position_duplicates_and_slices() {
        let budget = Budget::new(1 << 20);
        let a = [
            Some(9),
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
            Some(i128::MIN),
            Some(i128::MAX),
        ];
        let b = [
            None,
            None,
            Some(1),
            None,
            Some(2),
            Some(2),
            Some(i128::MAX),
            Some(i128::MIN),
        ];
        let values = [a, b].map(|items| {
            Value::adopt(
                Arc::new(Decimal128Array::from(items.to_vec()).slice(1, 7)),
                &budget,
            )
            .unwrap()
        });
        let encoded = Keys::new(&values, 7, &budget).unwrap();
        assert!(encoded.numeric_pair.is_some());
        assert!(encoded.rows.is_none());
        for i in 0..7 {
            let expected = [a[i + 1], b[i + 1]];
            assert_eq!(encoded.valid(i), expected.iter().all(Option::is_some));
            assert_eq!(
                decode_numeric_pair(encoded.row(i)),
                expected[0].zip(expected[1]).map(|(a, b)| [a, b])
            );
            for j in 0..7 {
                assert_eq!(
                    encoded.row(i) == encoded.row(j),
                    expected == [a[j + 1], b[j + 1]]
                );
            }
        }
        drop(encoded);
        drop(values);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn numeric_keys_retain_slices_and_distinguish_null_from_all_bit_patterns() {
        let budget = Budget::new(1 << 20);
        let raw = Decimal128Array::from(vec![Some(99), None, Some(0), Some(-1), Some(i128::MIN)]);
        let array: ArrayRef = Arc::new(raw.slice(1, 4));
        let value = Value::adopt(array, &budget).unwrap();
        let keys = Keys::new(std::slice::from_ref(&value), 4, &budget).unwrap();
        drop(value);
        assert!(keys.row(0).is_empty());
        assert!(!keys.valid(0));
        for (i, n) in [0_i128, -1, i128::MIN].iter().enumerate() {
            assert_eq!(keys.row(i + 1), &n.to_ne_bytes());
            assert!(keys.valid(i + 1));
        }
        assert!(budget.used() > 0);
        drop(keys);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn encoding_preserves_typed_key_equality_and_nulls() {
        let budget = Budget::new(1024 * 1024);
        let arrays: Vec<(ArrayRef, Type)> = vec![
            (
                Arc::new(Int64Array::from(vec![
                    Some(0),
                    None,
                    Some(-1),
                    Some(1),
                    Some(0),
                    Some(1),
                ])),
                Type::Int,
            ),
            (
                Arc::new(StringArray::from(vec![
                    Some(""),
                    None,
                    Some("é🙂"),
                    Some("a\0b"),
                    Some(""),
                    Some("a\0b"),
                ])),
                Type::Text,
            ),
            (
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    None,
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(false),
                ])),
                Type::Bool,
            ),
            (
                Arc::new(Date32Array::from(vec![
                    Some(0),
                    None,
                    Some(-1),
                    Some(1),
                    Some(0),
                    Some(1),
                ])),
                Type::Date,
            ),
            (
                Arc::new(Float64Array::from(vec![
                    Some(0.),
                    None,
                    Some(-3.),
                    Some(1.),
                    Some(-0.),
                    Some(1.),
                ])),
                Type::Float,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(0),
                        None,
                        Some(-101),
                        Some(201),
                        Some(0),
                        Some(201),
                    ])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
                Type::Decimal(2),
            ),
            (new_null_array(&DataType::Null, 6), Type::Null),
        ];
        for (a, ty) in arrays {
            let v = normalize_input(&a, &ty, &budget).unwrap();
            let encoded = Keys::new(std::slice::from_ref(&v), 6, &budget).unwrap();
            for i in 0..6 {
                assert_eq!(
                    encoded.valid(i),
                    key(&v, &ty, i) != crate::execute::Key::Null
                );
                for j in 0..6 {
                    assert_eq!(
                        encoded.row(i) == encoded.row(j),
                        key(&v, &ty, i) == key(&v, &ty, j),
                        "{ty:?}: {i} {j}"
                    );
                }
            }
        }
    }
}
