use super::*;

#[test]
fn compact_text_matches_canonical_bytes_with_slices_unicode_and_zero_bytes() {
    let budget = Budget::new(1 << 20);
    for samples in [
        vec![vec!["b", "a", "\0", "b"], vec!["y", "x", "y", "y"]],
        vec![vec!["é", "ÿ", "é", "é"], vec!["雪", "雨", "雪", "雪"]],
        vec![
            vec!["", "", "", ""],
            vec!["a\0b", "a\0c", "a\0b", "a\0b"],
            vec!["q", "r", "q", "q"],
        ],
    ] {
        let values = samples
            .iter()
            .map(|column| {
                let rows = [vec!["discarded prefix"], column.clone(), vec!["suffix"]].concat();
                Value::adopt(Arc::new(StringArray::from(rows).slice(1, 4)), &budget).unwrap()
            })
            .collect::<Vec<_>>();
        let before = budget.used();
        let compact = CompactText::new(&values, 4).unwrap();
        assert_eq!(
            budget.used(),
            before,
            "borrowed compact keys allocate nothing"
        );
        let encoded = Keys::new(&values, 4, &budget).unwrap();
        for row in 0..4 {
            let mut expected = Vec::new();
            for column in &samples {
                expected.extend_from_slice(&(column[row].len() as u32).to_ne_bytes());
                expected.extend_from_slice(column[row].as_bytes());
            }
            assert_eq!(&compact.row(row)[..compact.len()], expected);
            assert_eq!(encoded.row(row), expected);
        }
        assert_eq!(compact.row(0), compact.row(3));
        assert_ne!(compact.row(0), compact.row(1));
        let (array, lease) = encoded.into_text_column().unwrap();
        let prepared = Keys::from_text_column(array.clone(), &budget).unwrap();
        for row in 0..4 {
            assert_eq!(prepared.row(row), &compact.row(row)[..compact.len()]);
        }
        drop(prepared);
        drop(array);
        drop(lease);
        assert_eq!(budget.used(), before);
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_admission_declines_nulls_variable_width_long_and_empty_batches() {
    let budget = Budget::new(1 << 20);
    for rows in [
        vec![Some("a"), None],
        vec![Some(""), Some("a")],
        vec![Some("long payload"); 2],
        vec![],
    ] {
        let value = Value::adopt(Arc::new(StringArray::from(rows.clone())), &budget).unwrap();
        let values = vec![value.clone(), value];
        assert!(CompactText::new(&values, rows.len()).is_none());
        let keys = Keys::new(&values, rows.len(), &budget).unwrap();
        for (row, value) in rows.iter().enumerate() {
            assert_eq!(keys.valid(row), value.is_some());
        }
    }
    assert_eq!(budget.used(), 0);
}
