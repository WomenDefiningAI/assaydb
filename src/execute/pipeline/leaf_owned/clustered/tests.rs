use super::*;
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new(keys: Vec<Option<i64>>, stats: EnabledStatistics) -> Self {
        Self::with_group_rows(keys, stats, 7)
    }

    fn with_group_rows(
        keys: Vec<Option<i64>>,
        stats: EnabledStatistics,
        group_rows: usize,
    ) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "assay-clustered-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let rows = keys.len();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i64)),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(group_rows)
                    .set_statistics_enabled(stats)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn prepared(&self, options: &Options) -> Prepared {
        let mut prepared = crate::execute::prepare(
            "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k",
            &HashMap::from([("t".into(), self.0.clone())]),
            options,
        )
        .unwrap();
        prepared.internal_output = true;
        prepared
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

fn keys() -> Vec<Option<i64>> {
    (0..35)
        .map(|i| Some((i / 7 * 3 + [2, 0, 1, 2, 1, 0, 2][i % 7]) as i64))
        .collect()
}

#[test]
fn metadata_requires_complete_nondecreasing_domains_and_validated_non_null_rows() {
    for (keys, stats, admitted) in [
        (keys(), EnabledStatistics::Chunk, true),
        (keys(), EnabledStatistics::None, false),
        (
            (0..35).map(|i| Some((i % 4) as i64)).collect(),
            EnabledStatistics::Chunk,
            false,
        ),
        (
            (0..35).map(|i| Some((i / 7 * 2 + i % 3) as i64)).collect(),
            EnabledStatistics::Chunk,
            true,
        ),
        (
            (0..35).map(|i| (i % 13 != 0).then_some(i as i64)).collect(),
            EnabledStatistics::Chunk,
            false,
        ),
        (vec![], EnabledStatistics::Chunk, false),
    ] {
        let shared_boundary = admitted && keys.first() == Some(&Some(0));
        let fixture = Fixture::new(keys, stats);
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 4,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let prepared = fixture.prepared(&options);
            let mut batches = Vec::new();
            let actual = try_execute(
                &prepared,
                &options,
                budget.clone(),
                &fixture.0,
                &[0, 1],
                None,
                &mut |b| {
                    batches.push(b);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(actual.is_some(), admitted);
            if admitted {
                assert_eq!(actual.unwrap().clustered_row_groups, 5);
                let out = concat_batches(&batches[0].schema(), &batches).unwrap();
                let expected: Vec<i128> = if shared_boundary {
                    // The third row group starts at i=14: key 6 precedes key 5.
                    vec![0, 1, 2, 3, 4, 6, 5, 7, 8, 9, 10]
                } else {
                    (0..5).flat_map(|g| [g * 3 + 2, g * 3, g * 3 + 1]).collect()
                };
                assert_eq!(
                    out.column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    expected
                );
                assert_eq!(
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .collect::<Vec<_>>(),
                    if shared_boundary {
                        vec![4, 4, 3]
                    } else {
                        vec![4, 4, 4, 3]
                    }
                );
            } else {
                assert!(batches.is_empty());
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
    assert!(!contains(
        &(Arc::new(Int64Array::from(vec![Some(1), Some(3)])) as ArrayRef),
        (1, 2)
    ));
    assert!(!contains(
        &(Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef),
        (1, 2)
    ));
}

#[test]
fn metadata_missing_null_count_or_bounds_and_inconsistent_counts_decline() {
    let fixture = Fixture::new(keys(), EnabledStatistics::Chunk);
    let metadata =
        crate::parquet_scan::external_metadata(&File::open(&fixture.0).unwrap()).unwrap();
    for statistics in [
        Statistics::int64(Some(0), Some(2), None, None, false),
        Statistics::int64(None, Some(2), None, Some(0), false),
        Statistics::int64(Some(0), None, None, Some(0), false),
        Statistics::int64(Some(3), Some(2), None, Some(0), false),
        Statistics::int64(Some(0), Some(2), None, Some(8), false),
    ] {
        let mut groups = metadata.metadata().row_groups().to_vec();
        let mut columns = groups[0].columns().to_vec();
        columns[0] = columns[0]
            .clone()
            .into_builder()
            .set_statistics(statistics)
            .build()
            .unwrap();
        groups[0] = groups[0]
            .clone()
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap();
        let parquet = metadata
            .metadata()
            .as_ref()
            .clone()
            .into_builder()
            .set_row_groups(groups)
            .build();
        let edited = ArrowReaderMetadata::try_new(Arc::new(parquet), Default::default()).unwrap();
        assert!(domains(&edited, 0, 2).is_none());
    }
}

#[test]
fn cancellation_and_reservation_denial_never_emit_or_leak() {
    let fixture = Fixture::new(keys(), EnabledStatistics::Chunk);
    for workers in [1, 2, 4, 10] {
        for bytes in [0, 65536, 200000, 4 << 20] {
            let options = Options {
                workers,
                batch_size: 4,
                ..Options::default()
            };
            let prepared = fixture.prepared(&options);
            let budget = Budget::new(bytes);
            let mut emits = 0;
            let result = try_execute(
                &prepared,
                &options,
                budget.clone(),
                &fixture.0,
                &[0, 1],
                None,
                &mut |_| {
                    emits += 1;
                    Ok(())
                },
            )
            .unwrap();
            if result.is_none() {
                assert_eq!(emits, 0);
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
            options.cancellation.cancel();
            let result = try_execute(
                &prepared,
                &options,
                budget.clone(),
                &fixture.0,
                &[0, 1],
                None,
                &mut |_| panic!("cancelled output"),
            );
            assert_eq!(result.unwrap_err(), "cancelled: query execution");
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
        let options = Options {
            workers,
            batch_size: 4,
            ..Options::default()
        };
        let prepared = fixture.prepared(&options);
        let budget = Budget::new(options.memory_bytes);
        let mut emits = 0;
        let result = try_execute(
            &prepared,
            &options,
            budget.clone(),
            &fixture.0,
            &[0, 1],
            None,
            &mut |_| {
                emits += 1;
                options.cancellation.cancel();
                Ok(())
            },
        );
        assert_eq!(result.unwrap_err(), "cancelled: query execution");
        assert_eq!(emits, 1);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn live_clustered_failure_joins_jobs_and_discards_every_partial() {
    let fixture = Fixture::with_group_rows(
        (0..4096).map(Some).collect(),
        EnabledStatistics::Chunk,
        1024,
    );
    for workers in [1, 2, 4, 10] {
        for cancel in [false, true] {
            let options = Options {
                workers,
                batch_size: 7,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let prepared = fixture.prepared(&options);
            let executor =
                super::super::super::super::executor(&prepared, budget.clone(), true).unwrap();
            let schema = executor.output_schema.clone();
            let metadata =
                crate::parquet_scan::external_metadata(&File::open(&fixture.0).unwrap()).unwrap();
            let bounds = domains(&metadata, 0, workers).unwrap();
            let fired = Arc::new(AtomicUsize::new(0));
            let observed = fired.clone();
            let hook: AfterBatch = Arc::new(move |lane, cancellation| {
                if lane.rows <= (MORSEL_BATCHES * 7) as u64 {
                    return Ok(());
                }
                assert!(lane.cursor.is_some());
                assert!(lane.state.as_ref().unwrap().group_count > 0);
                observed.fetch_add(1, Ordering::Relaxed);
                if cancel {
                    cancellation.cancel();
                    cancellation.check()
                } else {
                    lane.source
                        .budget
                        .reserve(lane.source.budget.limit())
                        .map(|_| ())
                }
            });
            let source = Arc::new(Source {
                path: fixture.0.clone(),
                projection: ProjectionMask::roots(metadata.parquet_schema(), [0, 1]),
                key_projection: ProjectionMask::roots(metadata.parquet_schema(), [0]),
                metadata,
                plan: Arc::new(prepared.plan.clone()),
                expressions: executor.expressions.clone(),
                schema: prepared.schema.clone(),
                options: options.clone(),
                budget: budget.clone(),
                dense: budget.source_dense_allowance(DENSE_ALLOWANCE),
                validate_bounds: true,
                after_batch: Some(hook),
                _metadata_funding: budget.reserve(64 << 10).unwrap().into(),
                funding: Arc::new(budget.reserve(1 << 20).unwrap()),
            });
            drop(executor);
            let result = collect(source, &bounds, Having::All(true), schema);
            if cancel {
                assert_eq!(result.err().unwrap(), "cancelled: query execution");
            } else {
                assert!(result.unwrap().is_none());
            }
            assert!(fired.load(Ordering::Relaxed) > 0);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn late_reservation_denial_after_local_finalization_still_declines() {
    let fixture = Fixture::with_group_rows(
        (0..4096).map(Some).collect(),
        EnabledStatistics::Chunk,
        1024,
    );
    // One lane ensures the first group has finalized before group 2 starts.
    let options = Options {
        workers: 1,
        batch_size: 7,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let prepared = fixture.prepared(&options);
    let executor = super::super::super::super::executor(&prepared, budget.clone(), true).unwrap();
    let schema = executor.output_schema.clone();
    let metadata =
        crate::parquet_scan::external_metadata(&File::open(&fixture.0).unwrap()).unwrap();
    let bounds = domains(&metadata, 0, 1).unwrap();
    let fired = Arc::new(AtomicUsize::new(0));
    let observed = fired.clone();
    let hook: AfterBatch = Arc::new(move |lane, _| {
        if lane.bounds.unwrap().0 < 2048 {
            return Ok(());
        }
        observed.fetch_add(1, Ordering::Relaxed);
        lane.source
            .budget
            .reserve(lane.source.budget.limit())
            .map(|_| ())
    });
    let source = Arc::new(Source {
        path: fixture.0.clone(),
        projection: ProjectionMask::roots(metadata.parquet_schema(), [0, 1]),
        key_projection: ProjectionMask::roots(metadata.parquet_schema(), [0]),
        metadata,
        plan: Arc::new(prepared.plan.clone()),
        expressions: executor.expressions.clone(),
        schema: prepared.schema.clone(),
        options,
        budget: budget.clone(),
        dense: budget.source_dense_allowance(DENSE_ALLOWANCE),
        validate_bounds: true,
        after_batch: Some(hook),
        _metadata_funding: budget.reserve(64 << 10).unwrap().into(),
        funding: Arc::new(budget.reserve(1 << 20).unwrap()),
    });
    drop(executor);
    assert!(
        collect(source, &bounds, Having::All(true), schema)
            .unwrap()
            .is_none()
    );
    assert_eq!(fired.load(Ordering::Relaxed), 1);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn boundary_chains_keep_the_first_owner_and_its_local_encounter_order() {
    let fixture = Fixture::with_group_rows(
        [
            2, 0, 1, 2, 2, 2, 2, 2, 4, 2, 3, 4, 4, 4, 4, 4, 6, 4, 5, 6, 7, 8, 7, 8,
        ]
        .into_iter()
        .map(Some)
        .collect(),
        EnabledStatistics::Chunk,
        4,
    );
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let prepared = fixture.prepared(&options);
            let mut batches = Vec::new();
            let metrics = try_execute(
                &prepared,
                &options,
                budget.clone(),
                &fixture.0,
                &[0, 1],
                None,
                &mut |batch| {
                    batches.push(batch);
                    Ok(())
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(metrics.clustered_row_groups, 6);
            assert_eq!(metrics.rows_read, 24);
            let output = concat_batches(&batches[0].schema(), &batches).unwrap();
            for (column, expected) in [
                vec![2, 0, 1, 4, 3, 6, 5, 7, 8],
                vec![27, 1, 2, 76, 10, 35, 18, 42, 23],
                vec![7, 1, 1, 7, 1, 2, 1, 2, 2],
            ]
            .into_iter()
            .enumerate()
            {
                assert_eq!(
                    output
                        .column(column)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    expected
                );
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn semantic_completion_error_emits_nothing_and_releases_all_reservations() {
    let fixture = Fixture::new(vec![Some(0), Some(1), Some(1)], EnabledStatistics::Chunk);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(38, 0), false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 1])),
            Arc::new(
                Decimal128Array::from(vec![0, 10_i128.pow(38) - 1, 10_i128.pow(38) - 1])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&fixture.0).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(1)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let prepared = fixture.prepared(&options);
            let budget = Budget::new(options.memory_bytes);
            let result = try_execute(
                &prepared,
                &options,
                budget.clone(),
                &fixture.0,
                &[0, 1],
                Some(&crate::expr::Expr {
                    kind: crate::expr::Kind::Literal(crate::expr::Literal::Bool(false)),
                    ty: Type::Bool,
                }),
                &mut |_| panic!("completion barrier must precede all output"),
            );
            // Speculative completion errors decline to fresh ordinary replay;
            // only that path can trust totals after all decoded rows validate.
            assert!(result.unwrap().is_none());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
