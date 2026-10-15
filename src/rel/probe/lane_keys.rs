//! Equality bytes borrowed from total numeric columns in an admitted lane probe.
use super::*;
use crate::{expr::Kind, keys::Keys, types::Type};
use arrow::{
    array::{Array, Decimal128Array, Int32Array, Int64Array},
    datatypes::DataType,
};

pub(in crate::rel) enum Numbers<'a> {
    Small(&'a Int32Array),
    Integer(&'a Int64Array),
    Wide(&'a Decimal128Array),
}

impl<'a> Numbers<'a> {
    /// Only physical columns whose normalization preserves every integer bit qualify.
    fn column(batch: &'a RecordBatch, expression: &Expr) -> Option<Self> {
        let Kind::Column(i) = expression.kind else {
            return None;
        };
        let array = batch.column(i);
        match (&expression.ty, array.data_type()) {
            (Type::Int, DataType::Int32) => Some(Self::Small(array.as_any().downcast_ref()?)),
            (Type::Int, DataType::Int64) => Some(Self::Integer(array.as_any().downcast_ref()?)),
            (Type::Int, DataType::Decimal128(_, 0)) => {
                Some(Self::Wide(array.as_any().downcast_ref()?))
            }
            (Type::Decimal(scale), DataType::Decimal128(_, physical)) if scale == physical => {
                Some(Self::Wide(array.as_any().downcast_ref()?))
            }
            _ => None,
        }
    }

    /// Widening is exact; Arrow access honors offsets and NULL validity.
    fn value(&self, row: usize) -> Option<i128> {
        match self {
            Self::Small(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            Self::Integer(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            Self::Wide(a) => a.is_valid(row).then(|| a.value(row)),
        }
    }
}

// Keep the fallback inline rather than allocating an unreserved Box.
#[allow(clippy::large_enum_variant)]
pub(in crate::rel) enum ProbeKeys<'a> {
    Single(Numbers<'a>),
    Pair(Numbers<'a>, Numbers<'a>),
    Encoded(Keys),
}
impl<'a> ProbeKeys<'a> {
    /// One/two numeric columns have Keys' native equality layout; all other keys use Keys itself.
    pub(in crate::rel) fn new(
        batch: &'a RecordBatch,
        keys: &[(Expr, Expr)],
        budget: &Budget,
        compact: bool,
    ) -> Result<Self> {
        match keys {
            [(a, _)] => {
                if let Some(a) = Numbers::column(batch, a) {
                    return Ok(Self::Single(a));
                }
            }
            [(a, _), (b, _)] => {
                if let (Some(a), Some(b)) = (Numbers::column(batch, a), Numbers::column(batch, b)) {
                    return Ok(Self::Pair(a, b));
                }
            }
            _ => {}
        }
        let input = input(batch, budget, keys.iter().map(|(e, _)| e))?;
        let values = keys
            .iter()
            .map(|(e, _)| e.eval(&input, budget))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Encoded(Keys::with_text_encoding(
            &values,
            batch.num_rows(),
            budget,
            compact,
        )?))
    }

    /// NULL join keys never match; non-NULL bytes equal Keys::row including pair validity tags.
    pub(in crate::rel) fn key<'b>(
        &'b self,
        row: usize,
        scratch: &'b mut [u8; 34],
    ) -> Option<&'b [u8]> {
        match self {
            Self::Single(a) => {
                scratch[..16].copy_from_slice(&a.value(row)?.to_ne_bytes());
                Some(&scratch[..16])
            }
            Self::Pair(a, b) => {
                let (a, b) = (a.value(row)?, b.value(row)?);
                scratch[..2].copy_from_slice(&[1, 1]);
                scratch[2..18].copy_from_slice(&a.to_ne_bytes());
                scratch[18..].copy_from_slice(&b.to_ne_bytes());
                Some(scratch)
            }
            Self::Encoded(keys) => keys.valid(row).then(|| keys.row(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Float64Array, StringArray},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;

    /// Compare bytes to the existing encoder, including sliced buffers and all NULL positions.
    fn check(arrays: Vec<ArrayRef>, types: Vec<Type>, direct: bool) {
        let budget = Budget::new(1 << 20);
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(i.to_string(), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema, arrays).unwrap().slice(1, 5);
        let keys = types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| {
                let e = Expr {
                    ty,
                    kind: Kind::Column(i),
                };
                (e.clone(), e)
            })
            .collect::<Vec<_>>();
        let source = input(&batch, &budget, keys.iter().map(|(e, _)| e)).unwrap();
        let values = keys
            .iter()
            .map(|(e, _)| e.eval(&source, &budget).unwrap())
            .collect::<Vec<_>>();
        drop(source);
        let expected = Keys::with_text_encoding(&values, batch.num_rows(), &budget, true).unwrap();
        let held = direct.then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
        let actual = ProbeKeys::new(&batch, &keys, &budget, true).unwrap();
        assert_eq!(!matches!(actual, ProbeKeys::Encoded(_)), direct);
        let mut scratch = [0; 34];
        for row in 0..batch.num_rows() {
            assert_eq!(
                actual.key(row, &mut scratch),
                expected.valid(row).then(|| expected.row(row))
            );
        }
        drop(actual);
        drop(held);
        drop(expected);
        drop(values);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn numeric_lane_bytes_match_normalized_keys_without_allocating() {
        let columns: Vec<(ArrayRef, Type)> = vec![
            (
                Arc::new(Int32Array::from(vec![
                    Some(7),
                    Some(i32::MIN),
                    None,
                    Some(0),
                    Some(i32::MAX),
                    Some(-1),
                    Some(7),
                ])),
                Type::Int,
            ),
            (
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                    Some(0),
                    Some(-1),
                    Some(7),
                ])),
                Type::Int,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(7),
                        Some(-10_i128.pow(37)),
                        None,
                        Some(0),
                        Some(10_i128.pow(37)),
                        Some(-1),
                        Some(7),
                    ])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
                Type::Int,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(7),
                        Some(-100),
                        Some(0),
                        Some(100),
                        None,
                        Some(-1),
                        Some(7),
                    ])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
                Type::Decimal(2),
            ),
        ];
        for (a, ta) in &columns {
            check(vec![a.clone()], vec![ta.clone()], true);
            for (b, tb) in &columns {
                check(
                    vec![a.clone(), b.clone()],
                    vec![ta.clone(), tb.clone()],
                    true,
                );
            }
        }
    }

    #[test]
    fn other_lane_keys_keep_the_existing_encoder() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            Some("skip"),
            Some(""),
            None,
            Some("é\0x"),
            Some("same"),
            Some("same"),
            Some("skip"),
        ]));
        let float: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(7.),
            Some(-0.),
            Some(0.),
            None,
            Some(-1.5),
            Some(1.5),
            Some(7.),
        ]));
        check(vec![text.clone()], vec![Type::Text], false);
        check(vec![float.clone()], vec![Type::Float], false);
        check(vec![text, float], vec![Type::Text, Type::Float], false);
    }
}
