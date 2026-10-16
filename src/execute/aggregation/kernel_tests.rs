use super::*;

fn states(groups: usize, integer_sum: bool) -> States {
    States {
        integer_sum,
        counts: vec![0; groups],
        sums: Some(vec![i256::ZERO; groups]),
        extremes: None,
        distinct: None,
        heap_lease: None,
    }
}

#[test]
fn local_deltas_equal_ordered_updates_with_nulls_interleaving_and_batches() {
    let budget = Budget::new(1 << 20);
    for integer in [false, true] {
        for groups in [1, 2, 64] {
            let mut fast = states(groups, integer);
            let mut serial = states(groups, integer);
            for batch in 0..5 {
                let values = (0..257)
                    .map(|row| match (row + batch) % 5 {
                        0 => {
                            if batch == 2 {
                                None
                            } else {
                                Some(0)
                            }
                        }
                        1 => Some(i128::from(i64::MIN)),
                        2 => Some(i128::from(i64::MAX)),
                        _ => Some(17),
                    })
                    .collect::<Vec<_>>();
                let ids = (0..values.len())
                    .map(|row| row % groups)
                    .collect::<Vec<_>>();
                let value =
                    Value::adopt(Arc::new(Decimal128Array::from(values.clone())), &budget).unwrap();
                assert_eq!(
                    fast.update_plain(
                        &Aggregate::Sum,
                        Some(&value),
                        &ids,
                        values.len(),
                        groups == 1,
                        true
                    )
                    .unwrap(),
                    usize::from(batch != 2)
                );
                for (value, group) in values.iter().zip(&ids) {
                    if let Some(value) = value {
                        let sum = &mut serial.sums.as_mut().unwrap()[*group];
                        *sum = if integer {
                            crate::aggregate_sum::add_integer(*sum, *value).unwrap()
                        } else {
                            crate::aggregate_sum::add(*sum, *value).unwrap()
                        };
                        serial.counts[*group] += 1;
                    }
                }
                assert_eq!(fast.sums, serial.sums);
                assert_eq!(fast.counts, serial.counts);
            }
            let before = fast.counts.clone();
            let value =
                Value::adopt(Arc::new(Decimal128Array::from(vec![None; 257])), &budget).unwrap();
            assert_eq!(
                fast.update_plain(
                    &Aggregate::Sum,
                    Some(&value),
                    &[0; 257],
                    257,
                    groups == 1,
                    true
                )
                .unwrap(),
                0
            );
            assert_eq!(fast.counts, before);
        }
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_live_state_releases_credit_on_cancellation_and_resource_failure() {
    use crate::execute::{Prepared, execute_stream};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let plan = bind::bind(
        &bind::parse("SELECT a,b,SUM(v) FROM t GROUP BY a,b").unwrap(),
        &schema,
        None,
        false,
    )
    .unwrap();
    let prepared = Prepared {
        plan,
        schema: schema.clone(),
        relational: None,
        auto_simple: None,
        internal_output: false,
    };
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["b", "a", "b"])),
            Arc::new(StringArray::from(vec!["x", "x", "x"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    for workers in [1, 2, 4, 10] {
        for mode in 0..3 {
            let options = Options {
                workers,
                ..Options::default()
            };
            let budget = Budget::new(1 << 20);
            let result = execute_stream(&prepared, &options, budget.clone(), |emit| {
                emit(batch.clone())?;
                assert!(budget.used() > 0, "live aggregate state must be funded");
                if mode == 1 {
                    options.cancellation.cancel();
                }
                let _blocker =
                    (mode == 2).then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
                emit(batch.clone())
            });
            match mode {
                0 => {
                    let result = result.unwrap();
                    assert_eq!(result.metrics.bounded_sum_batches, 2);
                    assert_eq!(
                        result
                            .batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .as_ref(),
                        &[8, 4]
                    );
                }
                1 => assert_eq!(result.err().unwrap(), "cancelled: query execution"),
                _ => assert!(result.err().unwrap().starts_with("resource-limit:")),
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn unsafe_bounds_decline_without_mutation_and_integer_prefix_errors_remain() {
    let limit = 10_i128.pow(38) - 1;
    for mode in 0..5 {
        let mut state = states(if mode == 0 { 65 } else { 1 }, true);
        let values = if mode == 1 {
            vec![limit, 1, -1]
        } else {
            vec![1, -1]
        };
        if mode == 2 {
            state.sums.as_mut().unwrap()[0] = i256::from_i128(limit);
        }
        if mode == 3 {
            state.counts[0] = u64::MAX;
        }
        if mode == 4 {
            state.sums.as_mut().unwrap()[0] = i256::from_i128(limit) * i256::from_i128(2);
        }
        let sums = state.sums.clone();
        let counts = state.counts.clone();
        let groups = vec![0; values.len()];
        assert!(!state.add_narrow_batch(&Decimal128Array::from(values), &groups));
        assert_eq!(state.sums, sums);
        assert_eq!(state.counts, counts);
    }
    let budget = Budget::new(1 << 20);
    let value = Value::adopt(Arc::new(Decimal128Array::from(vec![limit, 1, -1])), &budget).unwrap();
    let mut integer = states(1, true);
    assert_eq!(
        integer
            .update_plain(&Aggregate::Sum, Some(&value), &[0; 3], 3, false, true)
            .unwrap_err(),
        "decimal precision 38 overflow"
    );
    let mut decimal = states(1, false);
    decimal
        .update_plain(&Aggregate::Sum, Some(&value), &[0; 3], 3, false, true)
        .unwrap();
    assert_eq!(decimal.sums.unwrap(), vec![i256::from_i128(limit)]);
    drop(value);
    assert_eq!(budget.used(), 0);
}

#[test]
fn fused_validation_rejects_late_wide_values_before_publishing_any_delta() {
    for group_count in [1, 3, 64] {
        for value in [i128::MIN, i128::MAX, (1_i128 << 64) + 7, -(1_i128 << 64)] {
            let mut state = states(group_count, true);
            state.sums.as_mut().unwrap()[0] = i256::from_i128(123);
            state.counts[0] = 9;
            let mut values = vec![i128::from(i64::MAX); 257];
            values[255] = value;
            let ids: Vec<_> = (0..values.len()).map(|row| row % group_count).collect();
            let sums = state.sums.clone();
            let counts = state.counts.clone();
            assert!(!state.add_narrow_batch(&Decimal128Array::from(values), &ids));
            assert_eq!(state.sums, sums);
            assert_eq!(state.counts, counts);
        }
    }
}

#[test]
fn direct_text_lookup_preserves_ids_and_declines_late_unknown_pairs() {
    let budget = Budget::new(1 << 20);
    let keys = vec![
        vec![Key::Text("b".into()), Key::Text("\0".into()), Key::Null],
        vec![
            Key::Text("x".into()),
            Key::Text("\u{7f}".into()),
            Key::Text("x".into()),
        ],
    ];
    for missing in [false, true] {
        let values = vec![
            Value::adopt(
                Arc::new(
                    StringArray::from(vec!["skip", "b", "\0", "b", "\0", "suffix"]).slice(1, 4),
                ),
                &budget,
            )
            .unwrap(),
            Value::adopt(
                Arc::new(
                    StringArray::from(vec![
                        "skip",
                        "x",
                        "\u{7f}",
                        "x",
                        if missing { "x" } else { "\u{7f}" },
                        "suffix",
                    ])
                    .slice(1, 4),
                ),
                &budget,
            )
            .unwrap(),
        ];
        let compact = crate::keys::CompactText::new(&values, 4).unwrap();
        let lease = budget.reserve(4 * std::mem::size_of::<usize>()).unwrap();
        let mut ids = Vec::with_capacity(4);
        let before = budget.used();
        assert_eq!(
            small_text::fill_existing(&compact, &keys, &mut ids),
            !missing
        );
        if missing {
            assert!(ids.is_empty());
        } else {
            assert_eq!(ids, [0, 1, 0, 1]);
        }
        assert_eq!(budget.used(), before, "lookup scratch is stack-only");
        drop(ids);
        drop(lease);
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn fused_columns_match_ordered_states_and_decline_excess_groups_or_columns() {
    let budget = Budget::new(8 << 20);
    for group_count in [1, 64, 65] {
        for columns in 1..=9 {
            let mut aggregates = (0..columns)
                .map(|i| bind::Agg {
                    function: if i % 2 == 0 {
                        Aggregate::Sum
                    } else {
                        Aggregate::Avg
                    },
                    input: None,
                    ty: Type::Int,
                    distinct: false,
                })
                .collect::<Vec<_>>();
            aggregates.push(bind::Agg {
                function: Aggregate::Count,
                input: None,
                ty: Type::Int,
                distinct: false,
            });
            let sources = (0..aggregates.len()).collect::<Vec<_>>();
            let mut fast = (0..=columns)
                .map(|_| states(group_count, true))
                .collect::<Vec<_>>();
            let mut serial = (0..=columns)
                .map(|_| states(group_count, true))
                .collect::<Vec<_>>();
            fast[columns].sums = None;
            serial[columns].sums = None;
            for batch in 0..3 {
                let mut values = (0..columns)
                    .map(|column| {
                        Some(
                            Value::adopt(
                                Arc::new(Decimal128Array::from_iter_values((0..257).map(|row| {
                                    match (row + batch + column) % 3 {
                                        0 => i128::from(i64::MIN),
                                        1 => i128::from(i64::MAX),
                                        _ => column as i128,
                                    }
                                }))),
                                &budget,
                            )
                            .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                values.push(None);
                let groups = (0..257).map(|row| row % group_count).collect::<Vec<_>>();
                let before = budget.used();
                let applied =
                    narrow::update_many(&mut fast, &aggregates, &sources, &values, &groups);
                assert_eq!(
                    applied,
                    (group_count <= 64 && columns <= 8).then_some(columns)
                );
                assert_eq!(budget.used(), before, "fused scratch is stack-only");
                for (i, agg) in aggregates.iter().enumerate() {
                    if applied.is_none() {
                        // A declined attempt must not have published any prefix.
                        assert_eq!(fast[i].sums, serial[i].sums);
                        assert_eq!(fast[i].counts, serial[i].counts);
                        fast[i]
                            .update_plain(
                                &agg.function,
                                values[i].as_ref(),
                                &groups,
                                groups.len(),
                                group_count == 1,
                                false,
                            )
                            .unwrap();
                    }
                    serial[i]
                        .update_plain(
                            &agg.function,
                            values[i].as_ref(),
                            &groups,
                            groups.len(),
                            group_count == 1,
                            false,
                        )
                        .unwrap();
                    assert_eq!(fast[i].sums, serial[i].sums);
                    assert_eq!(fast[i].counts, serial[i].counts);
                }
            }
        }
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn fused_rejection_preserves_all_states_and_first_error() {
    let budget = Budget::new(1 << 20);
    let limit = 10_i128.pow(38) - 1;
    for mode in 0..8 {
        let mut aggregates = vec![
            bind::Agg {
                function: Aggregate::Sum,
                input: None,
                ty: Type::Int,
                distinct: false,
            },
            bind::Agg {
                function: Aggregate::Avg,
                input: None,
                ty: Type::Int,
                distinct: false,
            },
            bind::Agg {
                function: Aggregate::Count,
                input: None,
                ty: Type::Int,
                distinct: false,
            },
        ];
        let mut state = vec![states(2, true), states(2, true), states(2, false)];
        state[2].sums = None;
        let mut late = vec![Some(1_i128); 257];
        if mode == 0 {
            late[256] = Some(i128::MAX);
        }
        if mode == 1 {
            late[256] = None;
        }
        let mut values = vec![
            Some(
                Value::adopt(Arc::new(Decimal128Array::from(vec![1_i128; 257])), &budget).unwrap(),
            ),
            Some(Value::adopt(Arc::new(Decimal128Array::from(late)), &budget).unwrap()),
            None,
        ];
        if mode == 2 {
            state[0].sums.as_mut().unwrap()[0] = i256::from_i128(limit);
        }
        if mode == 2 || mode == 3 {
            state[2].counts[0] = u64::MAX;
        }
        if mode == 4 {
            values[1] =
                Some(Value::adopt(Arc::new(Float64Array::from(vec![1.; 257])), &budget).unwrap());
        }
        if mode == 5 {
            values[2] = Some(Value::adopt(Arc::new(NullArray::new(257)), &budget).unwrap());
        }
        if mode == 6 {
            aggregates[1].distinct = true;
        }
        if mode == 7 {
            aggregates[1].function = Aggregate::Min;
        }
        let before = state
            .iter()
            .map(|s| (s.sums.clone(), s.counts.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            narrow::update_many(&mut state, &aggregates, &[0, 1, 2], &values, &[0; 257]),
            None
        );
        for (s, (sums, counts)) in state.iter().zip(before) {
            assert_eq!(s.sums, sums);
            assert_eq!(s.counts, counts);
        }
        if mode == 2 {
            assert_eq!(
                state[0]
                    .update_plain(
                        &Aggregate::Sum,
                        values[0].as_ref(),
                        &[0; 257],
                        257,
                        false,
                        true
                    )
                    .unwrap_err(),
                "decimal precision 38 overflow"
            );
        }
    }
    assert_eq!(budget.used(), 0);
}
