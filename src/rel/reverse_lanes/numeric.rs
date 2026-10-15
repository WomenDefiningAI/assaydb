//! Exact numeric membership with dense distinct-key ordinals, without a hash build.
use super::*;
use crate::keys::Keys;
use arrow::{array::PrimitiveArray, datatypes::ArrowPrimitiveType};

pub(super) struct NumericIndex {
    base: i128,
    span: usize,
    members: Vec<u64>,
    ranks: Vec<u32>,
    slots: usize,
    keys: Arc<Keys>,
    // Includes both vectors and their ownership envelope; keys retain their own lease.
    _lease: Reservation,
}

impl NumericIndex {
    /// Only actual non-NULL build values define the bounded range; denial leaves fallback intact.
    pub(super) fn new(
        keys: &Arc<Keys>,
        budget: &Budget,
        cancel: &Cancellation,
    ) -> Result<Option<Self>> {
        let Some(numbers) = keys.numeric_values() else {
            return Ok(None);
        };
        let mut bounds: Option<(i128, i128)> = None;
        for (row, &value) in numbers.iter().enumerate() {
            if row.is_multiple_of(1024) {
                cancel.check()?;
            }
            if keys.valid(row) {
                bounds = Some(match bounds {
                    None => (value, value),
                    Some((lo, hi)) => (lo.min(value), hi.max(value)),
                });
            }
        }
        let Some((base, high)) = bounds else {
            return Ok(None);
        };
        let span = high
            .checked_sub(base)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| usize::try_from(n).ok());
        // At most 8 MiB of membership plus 4 MiB of rank directories. The
        // density bound caps that setup at 24 bytes per retained occurrence.
        let Some(span) =
            span.filter(|&n| n <= 64 * 1024 * 1024 && n <= keys.len().saturating_mul(128))
        else {
            return Ok(None);
        };
        let words = span.div_ceil(64);
        let Some(lease) = words
            .checked_mul(12)
            .and_then(|n| n.checked_add(256))
            .and_then(|bytes| budget.reserve(bytes).ok())
        else {
            return Ok(None);
        };
        let mut members = vec![0_u64; words];
        for (row, &value) in numbers.iter().enumerate() {
            if row.is_multiple_of(1024) {
                cancel.check()?;
            }
            if keys.valid(row) {
                let offset = (value - base) as usize;
                members[offset / 64] |= 1 << (offset % 64);
            }
        }
        let mut ranks = Vec::with_capacity(words);
        let mut count = 0_u32;
        for (word, bits) in members.iter().enumerate() {
            if word.is_multiple_of(1024) {
                cancel.check()?;
            }
            ranks.push(count);
            // One bit per distinct key; the bounded range also fits u32.
            count += bits.count_ones();
        }
        Ok(Some(Self {
            base,
            span,
            members,
            ranks,
            slots: count as usize,
            keys: keys.clone(),
            _lease: lease,
        }))
    }

    pub(super) fn slots(&self) -> usize {
        self.slots
    }

    /// Exact rank, not a hash: rejected gaps never alias a retained key, even at i128 extremes.
    fn rank(&self, value: i128) -> Option<usize> {
        let offset = value.wrapping_sub(self.base) as u128;
        if offset >= self.span as u128 {
            return None;
        }
        let offset = offset as usize;
        let word = offset / 64;
        let bit = 1_u64 << (offset % 64);
        let members = self.members[word];
        if members & bit == 0 {
            return None;
        }
        Some(self.ranks[word] as usize + (members & (bit - 1)).count_ones() as usize)
    }

    /// Widen Arrow's physical integer exactly, skip NULLs, and write only the calling lane's bits.
    fn probe_array<T: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<T>,
        matched: &mut [u64],
        cancel: &Cancellation,
    ) -> Result<()>
    where
        T::Native: Into<i128>,
    {
        use arrow::array::Array;
        let nulls = array.nulls();
        for (row, &value) in array.values().iter().enumerate() {
            if row.is_multiple_of(1024) {
                cancel.check()?;
            }
            if nulls.is_none_or(|nulls| nulls.is_valid(row))
                && let Some(rank) = self.rank(value.into())
            {
                matched[rank / 64] |= 1 << (rank % 64);
            }
        }
        Ok(())
    }

    /// Dispatch once per batch; unusual physical layouts retain the canonical key encoder.
    pub(super) fn probe(
        &self,
        keys: &probe::ProbeKeys<'_>,
        rows: usize,
        matched: &mut [u64],
        cancel: &Cancellation,
    ) -> Result<()> {
        match keys {
            probe::ProbeKeys::Single(probe::Numbers::Small(a)) => {
                self.probe_array(a, matched, cancel)
            }
            probe::ProbeKeys::Single(probe::Numbers::Integer(a)) => {
                self.probe_array(a, matched, cancel)
            }
            probe::ProbeKeys::Single(probe::Numbers::Wide(a)) => {
                self.probe_array(a, matched, cancel)
            }
            keys => {
                let mut scratch = [0; 34];
                for row in 0..rows {
                    if row.is_multiple_of(1024) {
                        cancel.check()?;
                    }
                    if let Some(key) = keys.key(row, &mut scratch)
                        && let Ok(bytes) = key.try_into()
                        && let Some(rank) = self.rank(i128::from_ne_bytes(bytes))
                    {
                        matched[rank / 64] |= 1 << (rank % 64);
                    }
                }
                Ok(())
            }
        }
    }

    /// Expand successful-source membership onto every retained occurrence in its original order.
    pub(super) fn finish(
        &self,
        bits: &[u64],
        matched: &mut [bool],
        cancel: &Cancellation,
    ) -> Result<()> {
        for (row, &value) in self.keys.numeric_values().unwrap().iter().enumerate() {
            if row.is_multiple_of(1024) {
                cancel.check()?;
            }
            if self.keys.valid(row) {
                let rank = self.rank(value).unwrap();
                matched[row] = bits[rank / 64] & (1 << (rank % 64)) != 0;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Value;
    use arrow::array::{ArrayRef, Decimal128Array, Int32Array, Int64Array};

    fn keys(values: Vec<Option<i128>>, budget: &Budget) -> Arc<Keys> {
        let rows = values.len();
        let values = [Value::adopt(Arc::new(Decimal128Array::from(values)), budget).unwrap()];
        Arc::new(Keys::new(&values, rows, budget).unwrap())
    }

    #[test]
    fn rank_is_exact_across_word_boundaries_sparse_gaps_duplicates_nulls_and_extremes() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        for base in [i128::MIN, -7500, i128::MAX - 15000] {
            let mut numbers: Vec<_> = (0..150).map(|i| Some(base + i * 100)).collect();
            numbers.extend([Some(base + 6400), None, Some(base + 100)]);
            let encoded = keys(numbers.clone(), &budget);
            let index = NumericIndex::new(&encoded, &budget, &cancel)
                .unwrap()
                .unwrap();
            assert_eq!(index.slots(), 150);
            for offset in 0..15000 {
                assert_eq!(
                    index.rank(base + offset),
                    (offset % 100 == 0).then_some(offset as usize / 100)
                );
            }
            for value in [i128::MIN, i128::MAX, 0] {
                let expected = numbers[..150].iter().position(|n| *n == Some(value));
                assert_eq!(index.rank(value), expected);
            }
            let mut matched = vec![false; numbers.len()];
            index.finish(&[1, 1, 1], &mut matched, &cancel).unwrap();
            for (row, &value) in numbers.iter().enumerate() {
                assert_eq!(
                    matched[row],
                    [Some(base), Some(base + 6400), Some(base + 12800)].contains(&value)
                );
            }
            drop(index);
            drop(encoded);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn sliced_physical_numeric_keys_set_identical_dense_bits() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let numbers: Vec<_> = (0..150).map(|i| Some(i * 100)).collect();
        let encoded = keys(numbers, &budget);
        let index = NumericIndex::new(&encoded, &budget, &cancel)
            .unwrap()
            .unwrap();
        let values = [
            Some(300),
            Some(0),
            None,
            Some(6400),
            Some(14900),
            Some(1),
            Some(300),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(Int64Array::from(values.map(|n| n.map(i64::from)).to_vec())),
            Arc::new(Decimal128Array::from(
                values.map(|n| n.map(i128::from)).to_vec(),
            )),
        ];
        for array in arrays {
            let array = array.slice(1, 5);
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "k",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let e = Expr {
                kind: Kind::Column(0),
                ty: crate::types::Type::Int,
            };
            let keys = [(e.clone(), e)];
            let probe = probe::ProbeKeys::new(&batch, &keys, &budget, true).unwrap();
            let mut bits = [0; 3];
            index
                .probe(&probe, batch.num_rows(), &mut bits, &cancel)
                .unwrap();
            assert_eq!(bits, [1, 1, 1 << 21]);
        }
        drop(index);
        drop(encoded);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn wide_domains_all_nulls_denial_and_cancellation_leave_no_index_credit() {
        let budget = Budget::new(1 << 20);
        for values in [
            vec![None; 3],
            vec![Some(i128::MIN), Some(i128::MAX)],
            vec![Some(0), Some(64 * 1024 * 1024)],
        ] {
            let encoded = keys(values, &budget);
            let before = budget.used();
            assert!(
                NumericIndex::new(&encoded, &budget, &Cancellation::default())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(budget.used(), before);
            drop(encoded);
            assert_eq!(budget.used(), 0);
        }
        let encoded = keys(vec![Some(-3), None, Some(3), Some(-3)], &budget);
        let before = budget.used();
        let held = budget.reserve(budget.limit() - before).unwrap();
        assert!(
            NumericIndex::new(&encoded, &budget, &Cancellation::default())
                .unwrap()
                .is_none()
        );
        assert_eq!(budget.used(), budget.limit());
        drop(held);
        assert_eq!(budget.used(), before);
        let cancel = Cancellation::default();
        let index = NumericIndex::new(&encoded, &budget, &cancel)
            .unwrap()
            .unwrap();
        let mut bits = [0];
        let input = Decimal128Array::from(vec![Some(-3_i128), None, Some(3)]);
        index.probe_array(&input, &mut bits, &cancel).unwrap();
        assert_eq!(bits, [3]);
        cancel.cancel();
        assert_eq!(
            index.probe_array(&input, &mut bits, &cancel).unwrap_err(),
            "cancelled: query execution"
        );
        let mut matched = [false; 4];
        assert_eq!(
            index.finish(&bits, &mut matched, &cancel).unwrap_err(),
            "cancelled: query execution"
        );
        assert_eq!(matched, [false; 4]);
        drop(index);
        assert_eq!(budget.used(), before);
        assert!(NumericIndex::new(&encoded, &budget, &cancel).is_err());
        drop(encoded);
        assert_eq!(budget.used(), 0);
    }
}
