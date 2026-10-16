use super::*;
use arrow::datatypes::{DataType, TimeUnit};

#[test]
fn compact_counts_require_schema_proof_through_projection() {
    let scan = Node {
        schema: Arc::new(Schema::new(vec![
            Field::new("required", DataType::Int64, false),
            Field::new("optional", DataType::Int64, true),
            Field::new("null", DataType::Null, false),
        ])),
        estimated_rows: 0,
        op: RelOp::Scan {
            path: PathBuf::new(),
            columns: vec![0, 1, 2],
            statistics: vec![],
            distinct: vec![],
        },
    };
    assert!(required_counts(&scan, &[Some(0), None, Some(0)]));
    assert!(!required_counts(&scan, &[Some(0), Some(1)]));
    assert!(!required_counts(&scan, &[Some(2)]));
    let projected = Node {
        schema: scan.schema.clone(),
        estimated_rows: 0,
        op: RelOp::Project {
            input: Box::new(scan),
            columns: vec![1, 0],
        },
    };
    assert!(required_counts(&projected, &[Some(1)]));
    assert!(!required_counts(&projected, &[Some(0)]));
}

#[test]
fn compact_windows_bound_widen_merge_and_release() {
    for bound in [0, i64::from(u32::MAX), i64::from(u32::MAX) + 1, -1] {
        let budget = Budget::new(2 << 20);
        {
            let cell_bytes = Counts::compact_cell_bytes(bound);
            assert_eq!(
                cell_bytes,
                if (0..=i64::from(u32::MAX)).contains(&bound) {
                    4
                } else {
                    8
                }
            );
            let mut counts =
                Counts::with_compact_window(1, 3, bound, budget.reserve(3 * cell_bytes).unwrap())
                    .unwrap();
            assert_eq!(budget.used(), 3 * cell_bytes);
            assert_eq!(counts.narrow.len(), if cell_bytes == 4 { 3 } else { 0 });
            let source = batch(
                vec![Some(2), Some(2), None],
                Arc::new(Int64Array::from(vec![7; 3])),
            );
            let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
            assert_eq!(
                counts
                    .add(
                        &keys,
                        &source,
                        &[Some(1)],
                        &[],
                        &budget,
                        &Options::default().cancellation
                    )
                    .unwrap(),
                3
            );
            assert_eq!(counts.value(1, 0), 2);
            assert_eq!(counts.value(1, 1), 2);
            assert_eq!(keys.group(2, &counts), None);
            // Hints cannot exclude outliers; widening preserves the old counts.
            let outlier = batch(vec![Some(99)], Arc::new(Int64Array::from(vec![7])));
            let outlier_keys = BatchKeys::new(&outlier, &[column(0)], &budget).unwrap();
            counts
                .add(
                    &outlier_keys,
                    &outlier,
                    &[Some(1)],
                    &[],
                    &budget,
                    &Options::default().cancellation,
                )
                .unwrap();
            assert!(counts.narrow.is_empty());
            assert_eq!(counts.value(1, 1), 2);
            assert_eq!(counts.value(outlier_keys.group(0, &counts).unwrap(), 1), 1);
            assert!(
                budget.used() >= counts.values.capacity() * 8 + counts.numeric.allocation_bytes()
            );
        }
        assert_eq!(budget.used(), 0);
    }
    let budget = Budget::new(2 << 20);
    {
        let mut counts =
            Counts::with_compact_window(1, 1, i64::from(u32::MAX), budget.reserve(4).unwrap())
                .unwrap();
        counts.narrow[0] = u32::MAX;
        let tables = Tables::single(counts, &budget).unwrap();
        let source = batch(
            vec![Some(1), Some(1), None],
            Arc::new(Int64Array::from(vec![7; 3])),
        );
        let options = Options::default();
        let probe = Probe {
            tables: &tables,
            keys: &[column(0)],
            groups: &[column(1)],
            filters: &[],
            columns: &[Some(1), None],
            descriptors: 4096,
            budget: &budget,
            options: &options,
        };
        let result = probe.run(&source).unwrap();
        let weights = result.weights[0]
            .array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(
            weights.values().as_ref(),
            &[u64::from(u32::MAX), u64::from(u32::MAX), 0]
        );
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_window_denial_cancellation_and_masks_are_exact() {
    let budget = Budget::new(2 << 20);
    {
        assert!(Counts::with_compact_window(1, 3, 3, budget.reserve(11).unwrap()).is_err());
        assert_eq!(budget.used(), 0);
        let mut counts = Counts::with_compact_window(1, 3, 3, budget.reserve(12).unwrap()).unwrap();
        let source = batch(
            vec![Some(2), None, Some(3)],
            Arc::new(Int64Array::from(vec![7; 3])),
        );
        let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
        let mask = Value::adopt(
            Arc::new(BooleanArray::from(vec![Some(true), Some(true), None])),
            &budget,
        )
        .unwrap();
        let cancellation = crate::spill::Cancellation::default();
        assert_eq!(
            counts
                .add(&keys, &source, &[Some(1)], &[mask], &budget, &cancellation)
                .unwrap(),
            2
        );
        assert_eq!(counts.narrow, vec![0, 1, 0]);
        let pressure = budget.reserve(budget.limit() - budget.used()).unwrap();
        assert!(
            counts
                .widen(&budget)
                .unwrap_err()
                .starts_with("resource-limit:")
        );
        assert_eq!(counts.narrow, vec![0, 1, 0]);
        drop(pressure);
        cancellation.cancel();
        assert_eq!(
            counts
                .add(&keys, &source, &[Some(1)], &[], &budget, &cancellation)
                .unwrap_err(),
            "cancelled: query execution"
        );
        assert_eq!(counts.narrow, vec![0, 1, 0]);
        counts.narrow[1] = u32::MAX;
        assert_eq!(
            counts
                .add(
                    &keys,
                    &source,
                    &[Some(1)],
                    &[],
                    &budget,
                    &crate::spill::Cancellation::default()
                )
                .unwrap_err(),
            "resource-limit: join build indices"
        );
        assert_eq!(counts.narrow[1], u32::MAX);
    }
    assert_eq!(budget.used(), 0);
}

fn column(index: usize) -> Expr {
    Expr {
        kind: Kind::Column(index),
        ty: Type::Int,
    }
}

fn batch(keys: Vec<Option<i64>>, groups: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("g", groups.data_type().clone(), true),
        ])),
        vec![Arc::new(Int64Array::from(keys)), groups],
    )
    .unwrap()
}

#[test]
fn count_wave_late_exits_hold_slots_through_delivery_errors_and_unwind() {
    use std::panic::AssertUnwindSafe;
    for workers in [2, 4] {
        for mode in 0..5 {
            let budget = Budget::new(8 << 20);
            let options = Options {
                workers,
                ..Options::default()
            };
            {
                let source = batch(
                    vec![Some(1), Some(1), None],
                    Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])),
                );
                let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
                let mut counts = Counts::new(2, &budget).unwrap();
                counts
                    .add(
                        &keys,
                        &source,
                        &[Some(1)],
                        &[],
                        &budget,
                        &options.cancellation,
                    )
                    .unwrap();
                drop(keys);
                let tables = Tables::single(counts, &budget).unwrap();
                let probe = Probe {
                    tables: &tables,
                    keys: &[column(0)],
                    groups: &[column(1)],
                    filters: &[],
                    columns: &[Some(1)],
                    descriptors: 4096,
                    budget: &budget,
                    options: &options,
                };
                let mut wave = Wave::new(&options, &budget).unwrap();
                let count = workers + 2;
                // Exercise flush's scheduler independently of the normal wave
                // size cap. Prepay every descriptor before constructing this
                // adversarial wave with more tasks than available threads.
                assert!(wave._metadata.try_grow((count - 1) * 8192));
                wave.pending.reserve_exact(count);
                for index in 0..count {
                    let groups: ArrayRef = if mode == 4 && index == 1 {
                        Arc::new(Time32SecondArray::from(vec![Some(1); 3]))
                    } else {
                        Arc::new(Int64Array::from_iter_values(
                            (index * 3) as i64..(index * 3 + 3) as i64,
                        ))
                    };
                    let batch = batch(vec![Some(1), None, Some(99)], groups);
                    let lease = budget.reserve(batch.get_array_memory_size()).unwrap();
                    wave.pending.push(Pending {
                        batch,
                        _lease: lease,
                    });
                }
                let mut release = Some(budget.hold_helper_exits());
                let mut seen = Vec::new();
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    wave.flush(&probe, &mut |input, weights| {
                        if let Some(_release) = release.take() {
                            assert_eq!(budget.active_workers(), 0);
                            assert_eq!(budget.live_threads(), workers);
                            assert_eq!(budget.workers(1, workers - 1).count, 0);
                            assert!(budget.peak_threads() <= workers);
                            match mode {
                                1 => return Err("first downstream error".into()),
                                2 => panic!("downstream panic"),
                                3 => {
                                    options.cancellation.cancel();
                                    options.cancellation.check()?;
                                }
                                _ => {}
                            }
                        }
                        let groups =
                            crate::expr::numeric(&input.columns[1].as_ref().unwrap().array);
                        let weights = weights[0]
                            .array
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        for row in 0..input.len {
                            seen.push((groups.value(row), weights.value(row)));
                        }
                        Ok(())
                    })
                }));
                match mode {
                    0 => {
                        result.unwrap().unwrap();
                        assert_eq!(
                            seen,
                            (0..count * 3)
                                .map(|i| (i as i128, u64::from(i % 3 == 0)))
                                .collect::<Vec<_>>()
                        );
                    }
                    1 => assert_eq!(result.unwrap().unwrap_err(), "first downstream error"),
                    2 => assert!(result.is_err()),
                    3 => assert_eq!(result.unwrap().unwrap_err(), "cancelled: query execution"),
                    4 => {
                        assert_eq!(
                            result.unwrap().unwrap_err(),
                            format!(
                                "unsupported input type: {}",
                                DataType::Time32(TimeUnit::Second)
                            )
                        );
                        assert_eq!(seen, vec![(0, 1), (1, 0), (2, 0)]);
                    }
                    _ => unreachable!(),
                }
                assert_eq!(wave.background, workers - 1);
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(budget.live_threads(), 1);
                assert!(budget.peak_threads() <= workers);
                assert_eq!(budget.workers(workers - 1, workers - 1).count, workers - 1);
            }
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn dense_hints_do_not_exclude_outliers_nulls_or_zero_count_matches() {
    let budget = Budget::new(1 << 20);
    let source = batch(
        vec![Some(2), Some(2), None, Some(-9), Some(999)],
        Arc::new(Int64Array::from(vec![
            None,
            Some(7),
            Some(8),
            Some(9),
            None,
        ])),
    );
    let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
    let mut counts = Counts::with_window(2, 1, 3, budget.reserve(3 * 2 * 8).unwrap()).unwrap();
    counts
        .add(
            &keys,
            &source,
            &[Some(1)],
            &[],
            &budget,
            &crate::spill::Cancellation::default(),
        )
        .unwrap();
    assert_eq!(keys.group(2, &counts), None);
    for (row, expected) in [(0, [2, 1]), (1, [2, 1]), (3, [1, 1]), (4, [1, 0])] {
        let group = keys.group(row, &counts).unwrap();
        assert_eq!(&counts.values[group * 2..group * 2 + 2], &expected);
    }
    assert!(
        budget.used()
            >= counts.values.capacity() * 8
                + counts.numeric.allocation_bytes()
                + counts.numeric.dense_bytes()
    );
    drop(counts);
    drop(keys);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn ordered_count_waves_interleave_matches_and_misses_in_probe_order() {
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(8 << 20);
        let options = Options {
            workers,
            ..Options::default()
        };
        {
            let source = batch(
                vec![Some(1), Some(1), Some(2), None],
                Arc::new(Int64Array::from(vec![Some(7), None, Some(9), Some(11)])),
            );
            let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
            let mut counts = Counts::new(3, &budget).unwrap();
            counts
                .add(
                    &keys,
                    &source,
                    &[Some(1), None],
                    &[],
                    &budget,
                    &options.cancellation,
                )
                .unwrap();
            drop(keys);
            let tables = Tables::single(counts, &budget).unwrap();
            let probe = Probe {
                tables: &tables,
                keys: &[column(0)],
                groups: &[column(1)],
                filters: &[],
                columns: &[Some(1), None],
                descriptors: 4096,
                budget: &budget,
                options: &options,
            };
            let mut wave = Wave::new(&options, &budget).unwrap();
            let mut seen = Vec::new();
            let mut send = |input: Input, weights: Vec<Value>| {
                let groups = crate::expr::numeric(&input.columns[1].as_ref().unwrap().array);
                let counts = weights
                    .iter()
                    .map(|weight| weight.array.as_any().downcast_ref::<UInt64Array>().unwrap())
                    .collect::<Vec<_>>();
                for row in 0..input.len {
                    seen.push((
                        groups.value(row),
                        counts[0].value(row),
                        counts[1].value(row),
                    ));
                }
                Ok(())
            };
            let batches = workers + 2;
            for index in 0..batches {
                let start = (index * 5) as i64;
                let source = batch(
                    vec![None, Some(1), Some(99), Some(2), Some(1)],
                    Arc::new(Int64Array::from_iter_values(start..start + 5)),
                );
                wave.push(source, &probe, &mut send).unwrap();
            }
            wave.flush(&probe, &mut send).unwrap();
            let expected = (0..batches * 5)
                .map(|row| {
                    (
                        row as i128,
                        [0, 1, 0, 1, 1][row % 5],
                        [1, 2, 1, 1, 2][row % 5],
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(seen, expected);
            assert_eq!(wave.tasks, batches);
            assert_eq!(wave.background > 0, workers > 1);
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        assert!(budget.peak_workers() < workers);
    }
}

#[test]
fn ordered_waves_release_funding_on_success_error_and_cancellation() {
    for workers in [1, 2, 4, 10] {
        for mode in [0, 1, 2, 3] {
            let budget = Budget::new(4 << 20);
            let options = Options {
                workers,
                ..Options::default()
            };
            let baseline = budget.used();
            {
                let tables = Tables::single(Counts::new(2, &budget).unwrap(), &budget).unwrap();
                let probe = Probe {
                    tables: &tables,
                    keys: &[column(0)],
                    groups: &[column(1)],
                    filters: &[],
                    columns: &[Some(1)],
                    descriptors: 4096,
                    budget: &budget,
                    options: &options,
                };
                let mut wave = Wave::new(&options, &budget).unwrap();
                let mut seen = Vec::new();
                let mut send = |input: Input, weights: Vec<Value>| {
                    if mode == 3 {
                        return Err("earlier aggregate error".into());
                    }
                    assert_eq!(
                        weights[0]
                            .array
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                    seen.push(
                        crate::expr::numeric(&input.columns[1].as_ref().unwrap().array).value(0),
                    );
                    Ok(())
                };
                let result = (|| -> Result<()> {
                    // Two different later input errors ensure output/error
                    // precedence follows batches, not worker completion order.
                    for index in 0..workers + 2 {
                        let groups: ArrayRef = if mode == 1 && index == 1 {
                            Arc::new(Time32SecondArray::from(vec![Some(1)]))
                        } else if (mode == 1 && index == 2) || (mode == 3 && index == 1) {
                            Arc::new(Time64NanosecondArray::from(vec![Some(1)]))
                        } else {
                            Arc::new(Int64Array::from(vec![index as i64]))
                        };
                        let source = batch(vec![None], groups);
                        if mode == 2 && index == workers - 1 {
                            options.cancellation.cancel();
                        }
                        wave.push(source, &probe, &mut send)?;
                    }
                    wave.flush(&probe, &mut send)
                })();
                match mode {
                    0 => {
                        result.unwrap();
                        assert_eq!(
                            seen,
                            (0..workers + 2).map(|i| i as i128).collect::<Vec<_>>()
                        );
                        assert_eq!(wave.tasks, workers + 2);
                        assert_eq!(wave.background > 0, workers > 1);
                    }
                    1 => {
                        assert_eq!(
                            result.unwrap_err(),
                            format!(
                                "unsupported input type: {}",
                                DataType::Time32(TimeUnit::Second)
                            )
                        );
                        assert_eq!(seen, vec![0]);
                    }
                    2 => assert_eq!(result.unwrap_err(), "cancelled: query execution"),
                    3 => assert_eq!(result.unwrap_err(), "earlier aggregate error"),
                    _ => unreachable!(),
                }
                assert_eq!(budget.active_workers(), 0);
            }
            assert_eq!(budget.used(), baseline);
            assert!(budget.peak_workers() < workers);
        }
    }
}

#[test]
fn cancelled_count_update_and_denied_wave_leave_no_owners() {
    let budget = Budget::new(1 << 20);
    let source = batch(
        vec![Some(2); 4096],
        Arc::new(Int64Array::from(vec![7; 4096])),
    );
    {
        let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
        let mut counts = Counts::with_window(2, 1, 3, budget.reserve(48).unwrap()).unwrap();
        let cancellation = crate::spill::Cancellation::default();
        cancellation.cancel();
        assert!(
            counts
                .add(&keys, &source, &[Some(1)], &[], &budget, &cancellation)
                .unwrap_err()
                .starts_with("cancelled:")
        );
        assert_eq!(counts.values, vec![0; 6]);
    }
    assert_eq!(budget.used(), 0);
    let small = Budget::new(8191);
    assert!(
        Wave::new(&Options::default(), &small)
            .err()
            .unwrap()
            .starts_with("resource-limit:")
    );
    assert_eq!(small.used(), 0);
    assert_eq!(small.active_workers(), 0);
}

#[test]
fn count_window_and_probe_allocation_failures_release_replacement_peaks() {
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(2 << 20);
        {
            let options = Options {
                workers,
                ..Options::default()
            };
            let mut counts = Counts::with_window(2, 0, 4, budget.reserve(64).unwrap()).unwrap();
            let source = batch(vec![Some(99)], Arc::new(Int64Array::from(vec![1])));
            let keys = BatchKeys::new(&source, &[column(0)], &budget).unwrap();
            let pressure = budget.reserve(budget.limit() - budget.used()).unwrap();
            assert!(
                counts
                    .add(
                        &keys,
                        &source,
                        &[Some(1)],
                        &[],
                        &budget,
                        &options.cancellation
                    )
                    .unwrap_err()
                    .starts_with("resource-limit:")
            );
            assert_eq!(counts.values, vec![0; 8]);
            drop(pressure);
            drop(keys);
            let tables = Tables::single(counts, &budget).unwrap();
            let probe = Probe {
                tables: &tables,
                keys: &[column(0)],
                groups: &[column(1)],
                filters: &[],
                columns: &[Some(1)],
                descriptors: 4096,
                budget: &budget,
                options: &options,
            };
            let mut wave = Wave::new(&options, &budget).unwrap();
            // Hold already funded input across a denied worker allocation.
            let lease = budget.reserve(source.get_array_memory_size()).unwrap();
            wave.pending.push(Pending {
                batch: source,
                _lease: lease,
            });
            let pressure = budget.reserve(budget.limit() - budget.used()).unwrap();
            assert!(
                wave.flush(&probe, &mut |_, _| panic!("failed probe emitted output"))
                    .unwrap_err()
                    .starts_with("resource-limit:")
            );
            assert!(wave.pending.is_empty());
            assert_eq!(budget.active_workers(), 0);
            drop(pressure);
        }
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn wave_admission_shrinks_after_downstream_pressure_and_releases_metadata() {
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(8 << 20);
        let options = Options {
            workers,
            ..Options::default()
        };
        {
            let tables = Tables::single(Counts::new(2, &budget).unwrap(), &budget).unwrap();
            let probe = Probe {
                tables: &tables,
                keys: &[column(0)],
                groups: &[column(1)],
                filters: &[],
                columns: &[Some(1)],
                descriptors: 4096,
                budget: &budget,
                options: &options,
            };
            let source = batch(vec![None; 64], Arc::new(Int64Array::from(vec![7; 64])));
            let mut wave = Wave::new(&options, &budget).unwrap();
            let mut rows = 0;
            let mut send = |input: Input, _: Vec<Value>| {
                rows += input.len;
                Ok(())
            };
            assert_eq!(probe.wave_limit(&source), workers);
            for _ in 0..workers {
                // The real scan keeps this charge live through the callback.
                let _scan = budget.reserve(source.get_array_memory_size()).unwrap();
                wave.push(source.clone(), &probe, &mut send).unwrap();
            }
            assert!(wave.pending.is_empty());
            assert_eq!(wave.background > 0, workers > 1);
            let background = wave.background;
            assert_eq!(wave._metadata.bytes(), 8192);
            let pressure = budget
                .reserve(budget.limit() - budget.used() - (128 << 10))
                .unwrap();
            assert_eq!(probe.wave_limit(&source), 1);
            for _ in 0..workers + 1 {
                let _scan = budget.reserve(source.get_array_memory_size()).unwrap();
                wave.push(source.clone(), &probe, &mut send).unwrap();
                assert!(
                    wave.pending.is_empty(),
                    "a one-slot wave must deliver immediately"
                );
                assert_eq!(wave._metadata.bytes(), 8192);
            }
            assert_eq!(wave.background, background);
            assert_eq!(rows, (2 * workers + 1) * 64);
            drop(pressure);
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}
