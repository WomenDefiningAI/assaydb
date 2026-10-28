//! Optional parallel equality encoding for a completed grouping dictionary.
//! Join Keys and their probe kernels keep their contiguous layouts unchanged.
use super::*;
use crate::{parallel_prepare, spill::Cancellation};
use arrow::datatypes::DataType;
use std::sync::Mutex;

struct Part {
    keys: Keys,
    hashes: Vec<u64>,
}

pub(crate) struct Partitioned {
    parts: Vec<Part>,
    width: usize,
    _credit: Reservation,
}

// Only logical slice payloads are copied by encoding. The complete original
// backing is already held by Value owners. Include offsets, validity, headers,
// signed-zero replacement and the old/new row-converter buffers in the bound.
fn capacity(values: &[Value], start: usize, len: usize) -> Option<usize> {
    let payload = values.iter().try_fold(256usize, |total, value| {
        let width = match value.array.data_type() {
            DataType::Null => 0,
            DataType::Boolean => 1,
            DataType::Int32 | DataType::Date32 => 4,
            DataType::Int64 | DataType::Float64 | DataType::Timestamp(_, _) => 8,
            DataType::Decimal128(_, _) => 16,
            DataType::Utf8 => {
                let array = value.array.as_any().downcast_ref::<StringArray>()?;
                let offsets = array.value_offsets();
                let bytes = usize::try_from(offsets[start + len] - offsets[start]).ok()?;
                return total
                    .checked_add(bytes.checked_add(len.checked_mul(5)?)?.checked_add(1024)?);
            }
            _ => return None,
        };
        total.checked_add(len.checked_mul(width + 1)?.checked_add(1024)?)
    })?;
    payload
        .checked_mul(2)?
        .checked_add(len.checked_mul(16 + values.len().checked_mul(2)?)?)
}

impl Partitioned {
    pub(crate) fn try_new(
        values: &[Value],
        len: usize,
        budget: &Budget,
        cancel: &Cancellation,
        workers: usize,
        hasher: &ahash::RandomState,
    ) -> Result<Option<Self>> {
        cancel.check()?;
        let count = workers.min(16).min(len / 32_768);
        if count < 2 || values.is_empty() {
            return Ok(None);
        }
        let width = len.div_ceil(count);
        let count = len.div_ceil(width);
        let Some(metadata) = values
            .len()
            .checked_mul(1024)
            .and_then(|n| n.checked_add(8192))
            .and_then(|n| n.checked_mul(count))
            .and_then(|n| n.checked_add(len.checked_mul(8)?))
        else {
            return Ok(None);
        };
        let Ok(credit) = budget.reserve(metadata) else {
            return Ok(None);
        };
        let sizes = (0..count)
            .map(|i| capacity(values, i * width, width.min(len - i * width)))
            .collect::<Option<Vec<_>>>();
        let Some(sizes) = sizes else { return Ok(None) };
        let Some(total) = sizes.iter().try_fold(0usize, |sum, n| sum.checked_add(*n)) else {
            return Ok(None);
        };
        let Ok(encoding) = budget.reserve(total) else {
            return Ok(None);
        };
        let encoding = Mutex::new(encoding);
        let (parts, _) = parallel_prepare::map(count, workers, budget, |part| {
            cancel.check()?;
            let start = part * width;
            let len = width.min(len - start);
            let lease = encoding.lock().unwrap().split_off(sizes[part])?;
            let slice = values
                .iter()
                .map(|value| {
                    let mut slice = value.clone();
                    slice.array = value.array.slice(start, len);
                    slice
                })
                .collect::<Vec<_>>();
            let keys = Keys::with_credit(&slice, len, true, lease)?;
            let mut hashes = Vec::with_capacity(len);
            for row in 0..len {
                if row % 1024 == 0 {
                    cancel.check()?;
                }
                hashes.push(hasher.hash_one(keys.row(row)));
            }
            Ok(Part { keys, hashes })
        })?;
        cancel.check()?;
        Ok(Some(Self {
            parts,
            width,
            _credit: credit,
        }))
    }

    pub(crate) fn row(&self, row: usize) -> &[u8] {
        self.parts[row / self.width].keys.row(row % self.width)
    }
    pub(crate) fn hash(&self, row: usize) -> u64 {
        self.parts[row / self.width].hashes[row % self.width]
    }
    pub(crate) fn hashes(&self) -> impl Iterator<Item = u64> + '_ {
        self.parts
            .iter()
            .flat_map(|part| part.hashes.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_encodings_match_contiguous_bytes_for_every_admitted_layout() {
        let rows = 65_543;
        let budget = Budget::new(128 << 20);
        {
            let numeric = Value::adopt(
                Arc::new(
                    Decimal128Array::from_iter(
                        (0..rows).map(|i| (i % 13 != 0).then_some((i % 991) as i128)),
                    )
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
                &budget,
            )
            .unwrap();
            let text = Value::adopt(
                Arc::new(StringArray::from_iter((0..rows).map(|i| match i % 4 {
                    0 => None,
                    1 => Some(""),
                    2 => Some("é\0雪"),
                    _ => Some("\0"),
                }))),
                &budget,
            )
            .unwrap();
            let float = Value::adopt(
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| if i % 2 == 0 { -0.0 } else { 0.0 }),
                )),
                &budget,
            )
            .unwrap();
            for values in [
                vec![numeric.clone()],
                vec![numeric.clone(), numeric.clone()],
                vec![text.clone()],
                vec![text.clone(), text.clone()],
                vec![numeric, text, float],
            ] {
                let expected = Keys::new(&values, rows, &budget).unwrap();
                let hasher = ahash::RandomState::new();
                for workers in [2, 4, 10] {
                    let actual = Partitioned::try_new(
                        &values,
                        rows,
                        &budget,
                        &Cancellation::default(),
                        workers,
                        &hasher,
                    )
                    .unwrap()
                    .unwrap();
                    for row in 0..rows {
                        assert_eq!(actual.row(row), expected.row(row));
                        assert_eq!(actual.hash(row), hasher.hash_one(expected.row(row)));
                    }
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
        assert_eq!(budget.used(), 0);
    }
}
