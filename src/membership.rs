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
        if keys().any(|key| key.len() != 16) {
            return None;
        }
        Self::from_values(
            || keys().map(|key| i128::from_ne_bytes(key.try_into().unwrap())),
            budget,
        )
    }
    fn from_values<I: Iterator<Item = i128>>(
        values: impl Fn() -> I,
        budget: &Budget,
    ) -> Option<Self> {
        let first = values().next()?;
        let (mut min, mut max) = (first, first);
        for n in values() {
            min = min.min(n);
            max = max.max(n);
        }
        let span = usize::try_from(max.checked_sub(min)?.checked_add(1)?).ok()?;
        let bytes = span.checked_add(7)? / 8;
        // Keep sparse or extreme numeric domains on the existing hash path.
        // This is an optional acceleration: inability to reserve is a fallback,
        // not a new query failure. Eight MiB covers 67 million numeric slots.
        if bytes > 8 * 1024 * 1024 {
            return None;
        }
        let lease = budget.reserve(bytes + 128).ok()?;
        let mut bits = vec![0; bytes];
        for n in values() {
            let offset = usize::try_from(n.checked_sub(min)?).ok()?;
            bits[offset / 8] |= 1 << (offset % 8);
        }
        Some(Self {
            base: min,
            bits,
            _lease: lease,
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
