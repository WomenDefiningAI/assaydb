use super::*;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, NullArray, StringArray};
use std::{cell::Cell, sync::Arc};

thread_local! {
    static CANCEL_AT: Cell<Option<u32>> = const { Cell::new(None) };
}

pub(super) fn checkpoint(row: u32, cancel: &Cancellation) {
    CANCEL_AT.with(|at| {
        if at.get() == Some(row) {
            at.set(None);
            cancel.cancel();
        }
    });
}

#[test]
fn heap_unique_prefix_matches_sorted_oracle_and_legacy_with_nulls_and_float_bits() {
    const ROWS: usize = 257;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(
            (0..ROWS).map(|i| (i % 9 != 0).then_some((i * 37 % 31) as i64)),
        )),
        Arc::new(StringArray::from_iter((0..ROWS).map(|i| {
            (i % 11 != 0).then_some(["", "é", "a\0b", "雪"][i % 4])
        }))),
        Arc::new(Float64Array::from_iter((0..ROWS).map(|i| {
            (i % 13 != 0)
                .then_some([-0.0, 0.0, f64::NAN, f64::NEG_INFINITY, f64::INFINITY, 4.5][i % 6])
        }))),
        Arc::new(NullArray::new(ROWS)),
    ];
    let ordinals: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS).map(|i| (i * 73 % ROWS) as i64),
    ));
    for array in arrays {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let columns = vec![
                    SortColumn {
                        values: array.clone(),
                        options: Some(SortOptions {
                            descending,
                            nulls_first,
                        }),
                    },
                    SortColumn {
                        values: ordinals.clone(),
                        options: None,
                    },
                ];
                let full = sort_indices(columns.clone(), ROWS, &Budget::new(1 << 20)).unwrap();
                let nonnull = columns
                    .iter()
                    .filter(|c| c.values.data_type() != &arrow::datatypes::DataType::Null)
                    .map(|c| SortColumn {
                        values: crate::expr::numeric_compare::canonical_array(&c.values),
                        options: c.options,
                    })
                    .collect::<Vec<_>>();
                let cmp =
                    arrow::compute::kernels::sort::LexicographicalComparator::try_new(&nonnull)
                        .unwrap();
                let mut ordered = (0..ROWS as u32).collect::<Vec<_>>();
                ordered.sort_by(|a, b| cmp.compare(*a as usize, *b as usize));
                for k in [1, 7, 20] {
                    let budget = Budget::new(1 << 20);
                    let (selected, credit) =
                        try_top_k_indices(&columns, ROWS, k, &budget, &Cancellation::default())
                            .unwrap()
                            .unwrap();
                    assert_eq!(selected.values().as_ref(), &ordered[..k]);
                    assert_eq!(selected, full.slice(0, k));
                    assert!(budget.used() >= selected.get_array_memory_size());
                    drop(selected);
                    drop(credit);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }
}

#[test]
fn heap_declines_only_observable_ties_and_large_limits() {
    for (values, k, admitted) in [
        (vec![0, 1, 2, 9, 9, 9, 9, 9, 9, 9, 9, 9], 2, true),
        (vec![0, 1, 1, 9, 9, 9, 9, 9, 9, 9, 9, 9], 2, false),
        (vec![0, 0, 2, 9, 9, 9, 9, 9, 9, 9, 9, 9], 2, false),
        (vec![1; 32], 1, false),
        (vec![], 1, false),
        (vec![1; 32], 0, false),
        (vec![1; 32], 32, false),
    ] {
        let rows = values.len();
        let columns = vec![SortColumn {
            values: Arc::new(Int64Array::from(values)),
            options: None,
        }];
        let budget = Budget::new(1 << 20);
        let result =
            try_top_k_indices(&columns, rows, k, &budget, &Cancellation::default()).unwrap();
        assert_eq!(result.is_some(), admitted);
        drop(result);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn heap_reservation_denial_and_mid_selection_cancel_release_every_byte() {
    let columns = vec![SortColumn {
        values: Arc::new(Int64Array::from_iter_values((0..4096).rev())),
        options: None,
    }];
    let denied = Budget::new(1);
    assert!(
        try_top_k_indices(&columns, 4096, 20, &denied, &Cancellation::default())
            .unwrap()
            .is_none()
    );
    assert_eq!(denied.used(), 0);
    let budget = Budget::new(1 << 20);
    let cancel = Cancellation::default();
    CANCEL_AT.with(|at| at.set(Some(1024)));
    assert_eq!(
        try_top_k_indices(&columns, 4096, 20, &budget, &cancel)
            .err()
            .unwrap(),
        "cancelled: query execution"
    );
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn complete_key_ties_decline_and_use_the_unchanged_full_sort() {
    // No ordinal/unique secondary key: ties occur both inside and across K.
    const ROWS: usize = 257;
    for values in [
        Arc::new(Int64Array::from_iter(
            (0..ROWS).map(|i| (i % 5 != 0).then_some((i % 3) as i64)),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter(
            (0..ROWS).map(|i| (i % 5 != 0).then_some(["é", "a", "雪"][i % 3])),
        )),
        Arc::new(NullArray::new(ROWS)),
    ] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let columns = vec![SortColumn {
                    values: values.clone(),
                    options: Some(SortOptions {
                        descending,
                        nulls_first,
                    }),
                }];
                let full = sort_indices(columns.clone(), ROWS, &Budget::new(1 << 20)).unwrap();
                for k in [1, 7, 20] {
                    let budget = Budget::new(1 << 20);
                    let selected =
                        try_top_k_indices(&columns, ROWS, k, &budget, &Cancellation::default())
                            .unwrap();
                    assert!(selected.is_none(), "ties must use the parent permutation");
                    let actual = match selected {
                        Some((indices, _credit)) => indices,
                        None => sort_indices(columns.clone(), ROWS, &Budget::new(1 << 20))
                            .unwrap()
                            .slice(0, k),
                    };
                    assert_eq!(actual, full.slice(0, k));
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }
}
