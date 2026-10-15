//! Optional bounded numeric membership bitmap for runtime scan filters.
use crate::memory::{Budget, Reservation};
use crate::{
    Result,
    expr::{Expr, Kind},
    types::Type,
};
use arrow::{
    array::*,
    buffer::{BooleanBuffer, Buffer},
    datatypes::{ArrowPrimitiveType, DataType},
    record_batch::RecordBatch,
};
use std::collections::HashSet;

enum Numbers<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Decimal(&'a Decimal128Array),
}
fn column<'a>(batch: &'a RecordBatch, expression: &Expr) -> Option<Numbers<'a>> {
    let Kind::Column(i) = expression.kind else {
        return None;
    };
    let array = batch.column(i);
    match (&expression.ty, array.data_type()) {
        (Type::Int, DataType::Int32) => Some(Numbers::Int32(array.as_any().downcast_ref()?)),
        (Type::Int, DataType::Int64) => Some(Numbers::Int64(array.as_any().downcast_ref()?)),
        (Type::Decimal(a), DataType::Decimal128(_, b)) if a == b => {
            Some(Numbers::Decimal(array.as_any().downcast_ref()?))
        }
        _ => None,
    }
}
struct MaskStorage {
    bits: BooleanBuffer,
    _lease: Reservation,
}
impl AsRef<[u8]> for MaskStorage {
    fn as_ref(&self) -> &[u8] {
        self.bits.values()
    }
}
fn mask(len: usize, budget: &Budget, predicate: impl FnMut(usize) -> bool) -> Result<BooleanArray> {
    let bytes = len
        .div_ceil(8)
        .checked_add(256)
        .ok_or("scan mask size overflow")?;
    let lease = budget.reserve(bytes)?;
    let bits = BooleanBuffer::collect_bool(len, predicate);
    // The reservation follows the Arrow buffer through clones and slices,
    // including while the Parquet reader retains predicate masks.
    let owner = bytes::Bytes::from_owner(MaskStorage {
        bits,
        _lease: lease,
    });
    Ok(BooleanArray::new(
        BooleanBuffer::new(Buffer::from(owner), 0, len),
        None,
    ))
}
// Keep the replacement mask funded while both input masks are still live.
pub(crate) fn intersect(
    a: &BooleanArray,
    b: &BooleanArray,
    budget: &Budget,
) -> Result<BooleanArray> {
    debug_assert_eq!(a.len(), b.len());
    mask(a.len(), budget, |i| a.value(i) && b.value(i))
}
fn single<T: ArrowPrimitiveType>(
    a: &PrimitiveArray<T>,
    bitmap: &NumericMembership,
    budget: &Budget,
) -> Result<BooleanArray>
where
    T::Native: Into<i128>,
{
    mask(a.len(), budget, |i| {
        a.is_valid(i) && bitmap.contains(a.value(i).into())
    })
}
fn pair<L: ArrowPrimitiveType, R: ArrowPrimitiveType>(
    a: &PrimitiveArray<L>,
    b: &PrimitiveArray<R>,
    pairs: &NumericPairMembership,
    budget: &Budget,
) -> Result<BooleanArray>
where
    L::Native: Into<i128>,
    R::Native: Into<i128>,
{
    mask(a.len(), budget, |i| {
        a.is_valid(i) && b.is_valid(i) && pairs.contains(a.value(i).into(), b.value(i).into())
    })
}
fn right_pair<L: ArrowPrimitiveType>(
    a: &PrimitiveArray<L>,
    b: Numbers<'_>,
    pairs: &NumericPairMembership,
    budget: &Budget,
) -> Result<BooleanArray>
where
    L::Native: Into<i128>,
{
    match b {
        Numbers::Int32(b) => pair(a, b, pairs, budget),
        Numbers::Int64(b) => pair(a, b, pairs, budget),
        Numbers::Decimal(b) => pair(a, b, pairs, budget),
    }
}
pub(crate) fn direct_mask(
    batch: &RecordBatch,
    expressions: &[Expr],
    bitmap: Option<&NumericMembership>,
    pairs: Option<&NumericPairMembership>,
    budget: &Budget,
) -> Result<Option<BooleanArray>> {
    if let (Some(bitmap), [expression]) = (bitmap, expressions)
        && let Some(a) = column(batch, expression)
    {
        return match a {
            Numbers::Int32(a) => single(a, bitmap, budget),
            Numbers::Int64(a) => single(a, bitmap, budget),
            Numbers::Decimal(a) => single(a, bitmap, budget),
        }
        .map(Some);
    }
    if let (Some(pairs), [a, b]) = (pairs, expressions)
        && let (Some(a), Some(b)) = (column(batch, a), column(batch, b))
    {
        return match a {
            Numbers::Int32(a) => right_pair(a, b, pairs, budget),
            Numbers::Int64(a) => right_pair(a, b, pairs, budget),
            Numbers::Decimal(a) => right_pair(a, b, pairs, budget),
        }
        .map(Some);
    }
    Ok(None)
}

fn value_bounds(values: impl Iterator<Item = i128>) -> Option<(i128, i128)> {
    let mut values = values;
    let first = values.next()?;
    Some(values.fold((first, first), |(lo, hi), n| (lo.min(n), hi.max(n))))
}

// With a common upper word, signed i128 order is unsigned lower-word order.
// Validate that fact in bounded blocks, so the usual integer-key scan needs
// only 64-bit extrema. A differing upper word resumes full-width comparison
// from that block; it cannot contaminate the already-validated prefix bounds.
fn contiguous_bounds(numbers: &[i128]) -> Option<(i128, i128)> {
    let first = *numbers.first()?;
    let upper = first & !i128::from(u64::MAX);
    let mut bounds = (first, first);
    for (block, values) in numbers.chunks(256).enumerate() {
        let mut low = u64::MAX;
        let mut high = 0_u64;
        let mut different = 0_u64;
        for &number in values {
            let lower = number as u64;
            low = low.min(lower);
            high = high.max(lower);
            different |= ((number ^ first) >> 64) as u64;
        }
        if different != 0 {
            return Some(
                numbers[block * 256..]
                    .iter()
                    .fold(bounds, |(lo, hi), &n| (lo.min(n), hi.max(n))),
            );
        }
        bounds.0 = bounds.0.min(upper | i128::from(low));
        bounds.1 = bounds.1.max(upper | i128::from(high));
    }
    Some(bounds)
}

/// Bounds of exactly the valid numeric build rows, without per-row NULL work
/// for the common non-null layout. Dense admission and membership share them.
pub(crate) fn numeric_bounds(keys: &crate::keys::Keys) -> Option<(i128, i128)> {
    let numbers = keys.numeric_values()?;
    if keys.all_valid() && numbers.len() >= 8 {
        contiguous_bounds(numbers)
    } else if keys.all_valid() {
        value_bounds(numbers.iter().copied())
    } else {
        value_bounds(
            numbers
                .iter()
                .enumerate()
                .filter(|(row, _)| keys.valid(*row))
                .map(|(_, n)| *n),
        )
    }
}

pub(crate) struct NumericMembership {
    base: i128,
    bits: Vec<u8>,
    _lease: Reservation,
}
impl NumericMembership {
    pub fn new(members: &HashSet<Box<[u8]>, ahash::RandomState>, budget: &Budget) -> Option<Self> {
        Self::from_keys(|| members.iter().map(AsRef::as_ref), budget)
    }
    // The factory walks the same immutable key set on each pass. Borrow join
    // keys directly so a successful bitmap needs no per-key Box or hash copy.
    pub fn from_keys<'a, I: Iterator<Item = &'a [u8]>>(
        keys: impl Fn() -> I,
        budget: &Budget,
    ) -> Option<Self> {
        Self::from_checked_values(
            || keys().map(|key| key.try_into().ok().map(i128::from_ne_bytes)),
            budget,
        )
    }
    /// Project a canonical, non-NULL numeric pair without copying tuple keys.
    /// A positive component bit is only a superset of exact pair membership.
    pub(crate) fn from_pair_keys<'a, I: Iterator<Item = &'a [u8]>>(
        keys: impl Fn() -> I,
        component: usize,
        budget: &Budget,
    ) -> Option<Self> {
        if component >= 2 {
            return None;
        }
        Self::from_checked_values(
            || keys().map(|key| crate::keys::decode_numeric_pair(key).map(|pair| pair[component])),
            budget,
        )
    }
    pub(crate) fn cardinality(&self) -> usize {
        self.bits
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum()
    }
    /// The completed numeric build contributes exactly its non-NULL keys; duplicates set the same bit.
    pub(crate) fn from_numeric_keys(
        keys: &crate::keys::Keys,
        max_bytes: usize,
        budget: &Budget,
    ) -> Option<Self> {
        Self::from_numeric_keys_in_range(keys, numeric_bounds(keys)?, max_bytes, budget)
    }

    /// `bounds` were computed over these exact immutable valid build rows.
    /// A checked span proves every population offset fits without repeating
    /// checked i128 subtraction/conversion at each row.
    pub(crate) fn from_numeric_keys_in_range(
        keys: &crate::keys::Keys,
        bounds: (i128, i128),
        max_bytes: usize,
        budget: &Budget,
    ) -> Option<Self> {
        let numbers = keys.numeric_values()?;
        if keys.all_valid() {
            Self::from_range(numbers.iter().copied(), bounds, max_bytes, budget)
        } else {
            Self::from_range(
                numbers
                    .iter()
                    .enumerate()
                    .filter(|(row, _)| keys.valid(*row))
                    .map(|(_, n)| *n),
                bounds,
                max_bytes,
                budget,
            )
        }
    }
    /// Validate and discover both ranges in one pass over completed pair rows,
    /// then populate only requested, funded bitmaps in one pass. NULL tuples
    /// contribute neither component; repeated values only set an existing bit.
    pub(crate) fn from_pair_rows(
        keys: &crate::keys::Keys,
        eligible: [bool; 2],
        budget: &Budget,
    ) -> [Option<Self>; 2] {
        let Some(pairs) = keys.numeric_pairs() else {
            return [None, None];
        };
        let mut valid = pairs.iter().enumerate().filter(|(row, _)| keys.valid(*row));
        let Some((_, first)) = valid.next() else {
            return [None, None];
        };
        let Some(first) = crate::keys::decode_numeric_pair(first) else {
            return [None, None];
        };
        let (mut low, mut high) = (first, first);
        for (_, pair) in valid {
            let Some(pair) = crate::keys::decode_numeric_pair(pair) else {
                return [None, None];
            };
            for i in 0..2 {
                low[i] = low[i].min(pair[i]);
                high[i] = high[i].max(pair[i]);
            }
        }
        let mut result = std::array::from_fn(|i| {
            eligible[i]
                .then(|| Self::empty_range(low[i], high[i], 8 << 20, budget))
                .flatten()
        });
        if result.iter().all(Option::is_none) {
            return result;
        }
        for (row, pair) in pairs.iter().enumerate() {
            if !keys.valid(row) {
                continue;
            }
            // The immutable non-NULL layout and ranges were validated above.
            let pair = crate::keys::decode_numeric_pair(pair).unwrap();
            for (i, bitmap) in result.iter_mut().enumerate() {
                if let Some(bitmap) = bitmap {
                    let offset = (pair[i] - bitmap.base) as usize;
                    bitmap.bits[offset / 8] |= 1 << (offset % 8);
                }
            }
        }
        result
    }
    pub(crate) fn from_values<I: Iterator<Item = i128>>(
        values: impl Fn() -> I,
        budget: &Budget,
    ) -> Option<Self> {
        let bounds = value_bounds(values())?;
        Self::from_range(values(), bounds, 8 << 20, budget)
    }

    // Combine layout validation and range discovery for the distinct-key
    // fallback too. The factory must revisit the same immutable key set.
    fn from_checked_values<I: Iterator<Item = Option<i128>>>(
        values: impl Fn() -> I,
        budget: &Budget,
    ) -> Option<Self> {
        let mut range = values();
        let first = range.next()??;
        let (mut min, mut max) = (first, first);
        for n in range {
            let n = n?;
            min = min.min(n);
            max = max.max(n);
        }
        Self::from_range(
            values().map(|n| n.expect("validated immutable key layout")),
            (min, max),
            8 << 20,
            budget,
        )
    }

    /// The range is validated before funding or allocation; only the same
    /// immutable values are populated. Every offset is in the checked span.
    fn from_range(
        values: impl Iterator<Item = i128>,
        (min, max): (i128, i128),
        max_bytes: usize,
        budget: &Budget,
    ) -> Option<Self> {
        let mut bitmap = Self::empty_range(min, max, max_bytes, budget)?;
        for n in values {
            let offset = (n - min) as usize;
            bitmap.bits[offset / 8] |= 1 << (offset % 8);
        }
        Some(bitmap)
    }
    /// Allocate only after funding the checked bounded domain and its envelope.
    fn empty_range(min: i128, max: i128, max_bytes: usize, budget: &Budget) -> Option<Self> {
        let span = usize::try_from(max.checked_sub(min)?.checked_add(1)?).ok()?;
        let bytes = span.checked_add(7)? / 8;
        // Keep sparse or extreme numeric domains on the existing hash path.
        // This is an optional acceleration: inability to reserve is a fallback,
        // not a new query failure. Eight MiB covers 67 million numeric slots.
        if bytes > max_bytes.min(8 * 1024 * 1024) {
            return None;
        }
        let lease = budget.reserve(bytes + 128).ok()?;
        Some(Self {
            base: min,
            bits: vec![0; bytes],
            _lease: lease,
        })
    }
    /// Exact implication, including sparse bitmaps with different origins.
    pub(crate) fn is_subset_of(&self, other: &Self) -> bool {
        self.bits.iter().enumerate().all(|(byte, value)| {
            let mut bits = *value;
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                let Some(number) = self.base.checked_add((byte * 8 + bit) as i128) else {
                    return false;
                };
                if !other.contains(number) {
                    return false;
                }
                bits &= bits - 1;
            }
            true
        })
    }

    pub fn reserved_bytes(&self) -> usize {
        self.bits.len() + 128
    }
    #[inline]
    pub fn contains(&self, number: i128) -> bool {
        let Some(offset) = number
            .checked_sub(self.base)
            .and_then(|n| usize::try_from(n).ok())
        else {
            return false;
        };
        self.bits
            .get(offset / 8)
            .is_some_and(|b| b & (1 << (offset % 8)) != 0)
    }
}

/// Decode a small, typed build set once. Component bitmaps are only rejection
/// filters: the full numeric pair is always checked, including crossed pairs.
pub(crate) struct NumericPairMembership {
    pairs: HashSet<[i128; 2], ahash::RandomState>,
    bitmaps: [Option<NumericMembership>; 2],
    _lease: Reservation,
}
impl NumericPairMembership {
    pub fn new(
        types: &[crate::types::Type],
        members: &HashSet<Box<[u8]>, ahash::RandomState>,
        budget: &Budget,
    ) -> Option<Self> {
        if types.len() != 2 || types.iter().any(|ty| ty.scale().is_err()) {
            return None;
        }
        // Reserve before collecting the exact tuples. Optional acceleration
        // falls back to ordinary encoded-key membership if memory is tight.
        let lease = budget
            .reserve(members.len().checked_mul(128)?.checked_add(256)?)
            .ok()?;
        let pairs: HashSet<[i128; 2], ahash::RandomState> = members
            .iter()
            .map(|key| crate::keys::decode_numeric_pair(key))
            .collect::<Option<_>>()?;
        let bitmaps = [
            NumericMembership::from_values(|| pairs.iter().map(|pair| pair[0]), budget),
            NumericMembership::from_values(|| pairs.iter().map(|pair| pair[1]), budget),
        ];
        Some(Self {
            pairs,
            bitmaps,
            _lease: lease,
        })
    }
    #[inline]
    pub fn contains(&self, a: i128, b: i128) -> bool {
        self.bitmaps[0]
            .as_ref()
            .is_none_or(|bitmap| bitmap.contains(a))
            && self.bitmaps[1]
                .as_ref()
                .is_none_or(|bitmap| bitmap.contains(b))
            && self.pairs.contains(&[a, b])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn common_upper_word_bounds_validate_every_block_before_narrowing() {
        assert_eq!(contiguous_bounds(&[]), None);
        for upper in [i64::MIN, -2, -1, 0, 1, i64::MAX] {
            for rows in [1, 7, 8, 255, 256, 257, 513, 1025] {
                let mut values = (0..rows)
                    .map(|row| {
                        let lower = match row % 5 {
                            0 => 0,
                            1 => u64::MAX,
                            2 => 1 << 63,
                            3 => (1 << 63) - 1,
                            _ => 17,
                        };
                        (i128::from(upper) << 64) | i128::from(lower)
                    })
                    .collect::<Vec<_>>();
                let expected = |values: &[i128]| {
                    values
                        .iter()
                        .min()
                        .copied()
                        .zip(values.iter().max().copied())
                };
                assert_eq!(contiguous_bounds(&values), expected(&values));
                for row in [0, 1, 7, 254, 255, 256, 257, 511, 512, rows - 1] {
                    if row >= rows {
                        continue;
                    }
                    let original = values[row];
                    for delta in [-(1_i128 << 64), 1_i128 << 64] {
                        values[row] = original.wrapping_add(delta);
                        assert_eq!(contiguous_bounds(&values), expected(&values));
                    }
                    values[row] = original;
                }
            }
        }
    }

    #[test]
    fn numeric_bounds_lanes_cover_tails_nulls_slices_and_extremes_without_credit() {
        use crate::{expr::Value, keys::Keys};
        use std::sync::Arc;

        for rows in (0..=20).chain([4097]) {
            for nulls in [false, true] {
                for constant in [false, true] {
                    for extreme_row in 0..4 {
                        let budget = Budget::new(1 << 20);
                        let values = (0..rows)
                            .map(|row| {
                                if nulls && row % 3 == 0 {
                                    None
                                } else if constant {
                                    Some(-7)
                                } else if row == extreme_row {
                                    Some(i128::MIN)
                                } else if row + 1 == rows {
                                    Some(i128::MAX)
                                } else {
                                    Some((row as i128 % 7) - 3)
                                }
                            })
                            .collect::<Vec<_>>();
                        let expected = values
                            .iter()
                            .flatten()
                            .min()
                            .copied()
                            .zip(values.iter().flatten().max().copied());
                        // Extremes outside the slice must not enter its bounds.
                        let padded = [Some(i128::MIN); 3]
                            .into_iter()
                            .chain(values)
                            .chain([Some(i128::MAX); 3])
                            .collect::<Vec<_>>();
                        let array = Decimal128Array::from(padded).slice(3, rows);
                        let value = Value::adopt(Arc::new(array), &budget).unwrap();
                        let keys = Keys::new(&[value], rows, &budget).unwrap();
                        let before = (budget.used(), budget.peak());
                        assert_eq!(numeric_bounds(&keys), expected);
                        assert_eq!((budget.used(), budget.peak()), before);
                        drop(keys);
                        assert_eq!(budget.used(), 0);
                    }
                }
            }
        }
    }

    #[test]
    fn encoded_membership_validates_and_finds_range_in_one_pass() {
        let budget = Budget::new(4096);
        let passes = std::cell::Cell::new(0);
        let keys = [-2_i128, 7, -2, 1].map(i128::to_ne_bytes);
        let bitmap = NumericMembership::from_keys(
            || {
                passes.set(passes.get() + 1);
                keys.iter().map(|key| key.as_slice())
            },
            &budget,
        )
        .unwrap();
        assert_eq!(passes.get(), 2);
        assert_eq!(bitmap.cardinality(), 3);
        assert!(bitmap.contains(-2));
        assert!(bitmap.contains(7));
        assert!(!bitmap.contains(0));
        drop(bitmap);
        assert_eq!(budget.used(), 0);

        let before = budget.peak();
        assert!(
            NumericMembership::from_keys(|| [keys[0].as_slice(), &[1, 2, 3]].into_iter(), &budget,)
                .is_none()
        );
        assert_eq!(budget.peak(), before);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn contiguous_pair_bitmaps_match_valid_tuples_and_release_denied_credit() {
        use crate::{expr::Value, keys::Keys};
        use std::sync::Arc;
        for pairs in [
            vec![],
            vec![(None, Some(1)), (Some(2), None)],
            vec![
                (Some(-2), Some(7)),
                (Some(0), Some(9)),
                (Some(-2), Some(7)),
                (None, Some(99)),
                (Some(88), None),
            ],
            vec![(Some(i128::MIN), Some(0)), (Some(i128::MAX), Some(1))],
            vec![(Some(i128::MAX - 1), Some(-3)), (Some(i128::MAX), Some(-2))],
        ] {
            let source = Budget::new(1 << 20);
            let values = (0..2)
                .map(|component| {
                    // Include sliced buffers and NULLs in either component.
                    let mut padded = vec![Some(123)];
                    padded.extend(
                        pairs
                            .iter()
                            .map(|&(a, b)| if component == 0 { a } else { b }),
                    );
                    let array = Decimal128Array::from(padded).slice(1, pairs.len());
                    Value::adopt(Arc::new(array), &source).unwrap()
                })
                .collect::<Vec<_>>();
            let keys = Keys::new(&values, pairs.len(), &source).unwrap();
            for limit in [0, 129, 258, 1 << 20] {
                let budget = Budget::new(limit);
                let actual = NumericMembership::from_pair_rows(&keys, [true, true], &budget);
                let oracle_budget = Budget::new(limit);
                let expected: [Option<NumericMembership>; 2] = std::array::from_fn(|component| {
                    NumericMembership::from_pair_keys(
                        || {
                            (0..keys.len())
                                .filter(|row| keys.valid(*row))
                                .map(|row| keys.row(row))
                        },
                        component,
                        &oracle_budget,
                    )
                });
                for (actual, expected) in actual.iter().zip(&expected) {
                    assert_eq!(actual.is_some(), expected.is_some());
                    if let (Some(a), Some(e)) = (actual, expected) {
                        assert_eq!(a.base, e.base);
                        assert_eq!(a.bits, e.bits);
                        assert_eq!(a.cardinality(), e.cardinality());
                    }
                }
                assert_eq!(budget.used(), oracle_budget.used());
                assert!(budget.peak() <= limit);
                drop(actual);
                assert_eq!(budget.used(), 0);
            }
            drop(keys);
            drop(values);
            assert_eq!(source.used(), 0);
        }
    }

    #[test]
    fn numeric_membership_walks_values_exactly_twice() {
        let passes = std::cell::Cell::new(0);
        let budget = Budget::new(4096);
        let bitmap = NumericMembership::from_values(
            || {
                passes.set(passes.get() + 1);
                [7, -2, 7, 1].into_iter()
            },
            &budget,
        )
        .unwrap();
        assert_eq!(passes.get(), 2);
        assert_eq!(bitmap.cardinality(), 3);
        assert!(!bitmap.contains(0));
        drop(bitmap);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn numeric_build_bitmap_obeys_range_and_funding_limits_before_allocation() {
        use crate::{expr::Value, keys::Keys};
        use std::sync::Arc;
        for numbers in [
            vec![None, None],
            vec![Some(i128::MIN), None, Some(i128::MAX)],
            vec![Some(1), None, Some(16)],
        ] {
            let source_budget = Budget::new(4096);
            let rows = numbers.len();
            let value =
                Value::adopt(Arc::new(Decimal128Array::from(numbers)), &source_budget).unwrap();
            let keys = Keys::new(&[value], rows, &source_budget).unwrap();
            let budget = Budget::new(4096);
            assert!(NumericMembership::from_numeric_keys(&keys, 1, &budget).is_none());
            assert_eq!(budget.peak(), 0);
            let exhausted = Budget::new(0);
            assert!(NumericMembership::from_numeric_keys(&keys, 4096, &exhausted).is_none());
            assert_eq!(exhausted.used(), 0);
            drop(keys);
            assert_eq!(source_budget.used(), 0);
        }
    }

    #[test]
    fn projected_pairs_decline_invalid_layouts_wide_domains_and_denied_funding() {
        let pair = |a: i128, b: i128| {
            let mut bytes = [0_u8; 34];
            bytes[..2].copy_from_slice(&[1, 1]);
            bytes[2..18].copy_from_slice(&a.to_ne_bytes());
            bytes[18..].copy_from_slice(&b.to_ne_bytes());
            bytes
        };
        let keys = [pair(-2, 4), pair(-2, 7), pair(3, 4)];
        let budget = Budget::new(4096);
        let bitmap =
            NumericMembership::from_pair_keys(|| keys.iter().map(|k| k.as_slice()), 0, &budget)
                .unwrap();
        assert_eq!(bitmap.cardinality(), 2);
        assert!(bitmap.contains(-2) && bitmap.contains(3));
        assert!(!bitmap.contains(4));
        let used = budget.used();
        for component in [0, 1, 2] {
            assert!(
                NumericMembership::from_pair_keys(
                    || [&[0_u8; 16][..]].into_iter(),
                    component,
                    &budget
                )
                .is_none()
            );
            assert_eq!(budget.used(), used);
        }
        let wide = [pair(i128::MIN, 0), pair(i128::MAX, 0)];
        assert!(
            NumericMembership::from_pair_keys(|| wide.iter().map(|k| k.as_slice()), 0, &budget)
                .is_none()
        );
        let tiny = Budget::new(0);
        assert!(
            NumericMembership::from_pair_keys(|| keys.iter().map(|k| k.as_slice()), 0, &tiny)
                .is_none()
        );
        assert_eq!(tiny.used(), 0);
        drop(bitmap);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn typed_masks_match_exact_membership_and_retain_reservations() {
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;
        let sources: Vec<(ArrayRef, Type)> = vec![
            (
                Arc::new(Int32Array::from(vec![
                    None,
                    Some(-3),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(i32::MIN),
                    Some(i32::MAX),
                ])),
                Type::Int,
            ),
            (
                Arc::new(Int64Array::from(vec![
                    None,
                    Some(-3),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(i64::MIN),
                    Some(i64::MAX),
                ])),
                Type::Int,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![
                        None,
                        Some(-3),
                        Some(-1),
                        Some(0),
                        Some(1),
                        Some(2),
                        Some(i128::MIN),
                        Some(i128::MAX),
                    ])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
                Type::Decimal(2),
            ),
        ];
        let budget = Budget::new(1 << 20);
        let bitmap = NumericMembership::new(&set(&[-3, 0, 2]), &budget).unwrap();
        let exact: HashSet<[i128; 2], ahash::RandomState> =
            [[-3, -3], [0, 0], [1, 2], [i64::MIN as i128, i128::MAX]]
                .into_iter()
                .collect();
        let pairs = NumericPairMembership {
            pairs: exact.clone(),
            bitmaps: [None, None],
            _lease: budget.reserve(1024).unwrap(),
        };
        fn values(a: &ArrayRef) -> Vec<Option<i128>> {
            match a.data_type() {
                DataType::Int32 => a
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(i128::from))
                    .collect(),
                DataType::Int64 => a
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(i128::from))
                    .collect(),
                _ => a
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect(),
            }
        }
        for (a, at) in &sources {
            for (b, bt) in &sources {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", a.data_type().clone(), true),
                    Field::new("b", b.data_type().clone(), true),
                ]));
                let batch = RecordBatch::try_new(schema, vec![a.clone(), b.clone()]).unwrap();
                let expressions = [
                    Expr {
                        kind: Kind::Column(0),
                        ty: at.clone(),
                    },
                    Expr {
                        kind: Kind::Column(1),
                        ty: bt.clone(),
                    },
                ];
                for offset in 0..=batch.num_rows() {
                    let batch = batch.slice(offset, batch.num_rows() - offset);
                    let av = values(batch.column(0));
                    let bv = values(batch.column(1));
                    for single in [false, true] {
                        let mask_budget = Budget::new(1024);
                        let mask = if single {
                            direct_mask(
                                &batch,
                                &expressions[..1],
                                Some(&bitmap),
                                None,
                                &mask_budget,
                            )
                        } else {
                            direct_mask(&batch, &expressions, None, Some(&pairs), &mask_budget)
                        }
                        .unwrap()
                        .unwrap();
                        let expected = BooleanArray::from(
                            av.iter()
                                .zip(&bv)
                                .map(|(a, b)| {
                                    if single {
                                        a.is_some_and(|a| [-3, 0, 2].contains(&a))
                                    } else {
                                        a.zip(*b).is_some_and(|(a, b)| exact.contains(&[a, b]))
                                    }
                                })
                                .collect::<Vec<_>>(),
                        );
                        assert_eq!(mask, expected);
                        assert_eq!(mask.null_count(), 0);
                        if mask.len() > 1 {
                            let retained = mask.slice(1, mask.len() - 1);
                            drop(mask);
                            assert!(mask_budget.used() > 0);
                            drop(retained);
                        } else {
                            drop(mask);
                        }
                        assert_eq!(mask_budget.used(), 0);
                        let tiny = Budget::new(0);
                        assert!(
                            direct_mask(&batch, &expressions[..1], Some(&bitmap), None, &tiny)
                                .is_err()
                        );
                        assert_eq!(tiny.used(), 0);
                    }
                }
                let computed = Expr {
                    ty: at.clone(),
                    kind: Kind::Neg(Box::new(expressions[0].clone())),
                };
                assert!(
                    direct_mask(&batch, &[computed], Some(&bitmap), None, &budget)
                        .unwrap()
                        .is_none()
                );
                let mismatch = Expr {
                    ty: Type::Decimal(3),
                    kind: Kind::Column(0),
                };
                assert!(
                    direct_mask(&batch, &[mismatch], Some(&bitmap), None, &budget)
                        .unwrap()
                        .is_none()
                );
            }
        }
        drop(bitmap);
        drop(pairs);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn direct_borrowed_bitmap_handles_large_sets_without_copying_encoded_keys() {
        let values: Vec<_> = (-100000_i128..300000)
            .step_by(3)
            .map(i128::to_ne_bytes)
            .collect();
        let budget = Budget::new(65536);
        let bitmap =
            NumericMembership::from_keys(|| values.iter().map(|v| v.as_slice()), &budget).unwrap();
        assert!(budget.used() <= 65536);
        for n in -100010_i128..300010 {
            assert_eq!(
                bitmap.contains(n),
                (-100000..300000).contains(&n) && (n + 100000) % 3 == 0
            );
        }
        let before = budget.used();
        assert!(NumericMembership::from_keys(|| [&[1_u8, 2][..]].into_iter(), &budget).is_none());
        assert_eq!(budget.used(), before);
        drop(bitmap);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn numeric_pair_filter_matches_encoded_membership_and_rejects_crossed_pairs() {
        use crate::{expr::Value, keys::Keys, types::Type};
        use arrow::array::Decimal128Array;
        use std::sync::Arc;
        let budget = Budget::new(16 * 1024 * 1024);
        for pairs in [
            vec![],
            vec![[1, 10], [2, 20], [1, 10], [-7, 30]],
            vec![[i128::MIN, -1], [i128::MAX, 1]],
        ] {
            let values = [0, 1].map(|i| {
                Value::adopt(
                    Arc::new(
                        Decimal128Array::from(pairs.iter().map(|pair| pair[i]).collect::<Vec<_>>())
                            .with_precision_and_scale(38, i as i8 * 2)
                            .unwrap(),
                    ),
                    &budget,
                )
                .unwrap()
            });
            let encoded = Keys::new(&values, pairs.len(), &budget).unwrap();
            let members = (0..pairs.len()).map(|i| encoded.row(i).into()).collect();
            let filter =
                NumericPairMembership::new(&[Type::Int, Type::Decimal(2)], &members, &budget)
                    .unwrap();
            for a in [i128::MIN, i128::MAX, -7, 0, 1, 2, 3] {
                for b in [-1, 0, 1, 10, 20, 30, 40] {
                    assert_eq!(filter.contains(a, b), pairs.contains(&[a, b]), "{a},{b}");
                }
            }
            let tiny = Budget::new(1);
            assert!(
                NumericPairMembership::new(&[Type::Int, Type::Decimal(2)], &members, &tiny)
                    .is_none()
            );
            assert_eq!(tiny.used(), 0);
            assert!(
                NumericPairMembership::new(&[Type::Int, Type::Text], &members, &budget).is_none()
            );
        }
        assert_eq!(budget.used(), 0);
    }
    fn set(values: &[i128]) -> HashSet<Box<[u8]>, ahash::RandomState> {
        values
            .iter()
            .map(|v| Box::<[u8]>::from(v.to_ne_bytes()))
            .collect()
    }
    #[test]
    fn numeric_bitmap_matches_set_and_falls_back_for_wide_ranges_or_low_budget() {
        let budget = Budget::new(1 << 20);
        for values in [
            vec![-37, 0, 2, 63, 1_000_000],
            vec![i128::MIN, i128::MIN + 2],
            vec![i128::MAX - 1, i128::MAX],
        ] {
            let members = set(&values);
            let bitmap = NumericMembership::new(&members, &budget).unwrap();
            for n in values.iter().copied().chain([
                -39,
                -38,
                -37,
                -1,
                0,
                1,
                2,
                3,
                63,
                64,
                1_000_000,
                1_000_001,
                i128::MIN,
                i128::MAX,
            ]) {
                assert_eq!(bitmap.contains(n), values.contains(&n));
            }
        }
        assert_eq!(budget.used(), 0);
        assert!(NumericMembership::new(&set(&[i128::MIN, i128::MAX]), &budget).is_none());
        assert!(NumericMembership::new(&set(&[0, 1_000_000_000]), &budget).is_none());
        let tiny = Budget::new(8);
        assert!(NumericMembership::new(&set(&[1, 2]), &tiny).is_none());
        assert_eq!(tiny.used(), 0);
    }
}
