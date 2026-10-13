use super::*;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
#[test]
fn source_owned_real_sql_keeps_materialization_reuse_and_one_worker_baseline() {
    let fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", arrow::datatypes::DataType::Int64, false),
        Field::new("v", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut writer = ArrowWriter::try_new(
        File::create(&fixture.0).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(23)
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(
                        (0..512).map(|n| n / 2).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(vec![1; 512])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let sql = "SELECT a.k,COUNT(*),SUM(a.v),AVG(a.v) FROM t a WHERE a.k IN (SELECT k FROM t GROUP BY k HAVING SUM(v)>0) GROUP BY a.k ORDER BY 1";
    for workers in [1, 2, 4, 10] {
        let report = Arc::new(std::sync::Mutex::new(
            execute::pipeline::source_owned::Report::default(),
        ));
        let opt = Options {
            workers,
            batch_size: 8,
            test_source_owned_pipeline: true,
            test_source_owned_report: Some(report.clone()),
            ..Options::default()
        };
        let prepared =
            execute::prepare(sql, &HashMap::from([("t".into(), fixture.0.clone())]), &opt).unwrap();
        let expected = execute::execute(
            &prepared,
            &Options {
                test_source_owned_pipeline: false,
                ..opt.clone()
            },
        )
        .unwrap();
        let budget = Budget::new(opt.memory_bytes);
        let actual = execute_in_budget(
            prepared.relational.as_ref().unwrap(),
            &opt,
            budget.clone(),
            false,
        )
        .unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.scan_materializations, 1);
        assert_eq!(actual.metrics.scan_reuses, 1);
        assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
        if workers > 1 {
            assert!(report.lock().unwrap().admitted);
        }
        assert_eq!(actual.metrics.pipeline_source_routes, 0);
        drop(actual);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-shared-pipeline-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Int64, true),
            Field::new("v", arrow::datatypes::DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..512)
                        .map(|i| if i % 17 == 0 { None } else { Some(i % 53) })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..512)
                        .map(|i| if i % 19 == 0 { None } else { Some(i % 7 - 2) })
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(23)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
fn options(workers: usize) -> Options {
    Options {
        workers,
        batch_size: 8,
        test_shared_batch_pipeline: true,
        test_pipeline_slice_bytes: Some(128 << 10),
        ..Options::default()
    }
}
#[test]
fn real_sql_preserves_populate_once_reuse_nulls_and_worker_limits() {
    let fixture = Fixture::new();
    let sql = "SELECT a.k,COUNT(*),SUM(a.v),AVG(a.v) FROM t a WHERE a.k IN (SELECT k FROM t GROUP BY k HAVING SUM(v)>0) GROUP BY a.k ORDER BY 1";
    for workers in [1, 2, 4, 10] {
        let opt = options(workers);
        let prepared =
            execute::prepare(sql, &HashMap::from([("t".into(), fixture.0.clone())]), &opt).unwrap();
        let old = Options {
            test_shared_batch_pipeline: false,
            ..opt.clone()
        };
        let expected = execute::execute(&prepared, &old).unwrap();
        let budget = Budget::new(opt.memory_bytes);
        let result = execute_in_budget(
            prepared.relational.as_ref().unwrap(),
            &opt,
            budget.clone(),
            false,
        )
        .unwrap();
        assert_eq!(result.batch, expected.batch);
        assert_eq!(result.metrics.scan_materializations, 1);
        assert_eq!(result.metrics.scan_reuses, 1);
        assert_eq!(result.metrics.rows_read, expected.metrics.rows_read);
        assert_eq!(result.metrics.pipeline_producers > 0, workers > 1);
        assert_eq!(
            result.metrics.pipeline_runtime_submitted,
            result.metrics.pipeline_runtime_completed
        );
        assert!(result.metrics.worker_threads_peak <= workers.saturating_sub(1));
        drop(result);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        assert!(opt.cancellation.check().is_ok());
    }
}
fn stored(
    rows: &[(i64, i128)],
    batch_size: usize,
    budget: &Budget,
) -> (Prepared, Arc<Materialized>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", arrow::datatypes::DataType::Int64, false),
        Field::new("v", arrow::datatypes::DataType::Decimal128(38, 0), false),
    ]));
    let plan = bind::bind(
        &bind::parse("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k").unwrap(),
        &schema,
        None,
        true,
    )
    .unwrap();
    let mut batches = Vec::new();
    let mut leases = Vec::new();
    for chunk in rows.chunks(batch_size) {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    chunk.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(
                    Decimal128Array::from(chunk.iter().map(|r| r.1).collect::<Vec<_>>())
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        leases.push(budget.reserve(batch.get_array_memory_size() + 256).unwrap());
        batches.push(batch);
    }
    (
        Prepared {
            internal_output: false,
            plan,
            schema,
            relational: None,
        },
        Arc::new(Materialized {
            budget: budget.clone(),
            batches,
            files: Vec::new(),
            _leases: leases,
        }),
    )
}
#[test]
fn pressure_replay_holds_storage_and_joins_before_chunk_callbacks() {
    let budget = Budget::new(32 << 20);
    let rows = (0..2048).map(|i| (i, 1)).collect::<Vec<_>>();
    let (prepared, storage) = stored(&rows, 8, &budget);
    let initial = budget.used();
    let mut output = Vec::new();
    let opt = options(4);
    let metrics = execute::pipeline::shared::execute(
        &prepared,
        &opt,
        budget.clone(),
        storage.clone(),
        &mut |batch| {
            assert_eq!(budget.active_workers(), 0);
            assert!(budget.used() >= initial);
            assert!(batch.num_rows() <= opt.batch_size);
            output.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(output, (0..2048).collect::<Vec<_>>());
    assert!(metrics.pipeline_pressure_flushes > 0);
    assert_eq!(metrics.pipeline_source_batches, storage.batches.len());
    assert_eq!(budget.used(), initial);
    drop(storage);
    assert_eq!(budget.used(), 0);
}
#[test]
fn finalization_and_downstream_errors_keep_original_chunk_chronology() {
    let limit = 10_i128.pow(38) - 1;
    let rows = [(0, 1), (1, 1), (2, limit), (2, 1)];
    let opt = Options {
        batch_size: 2,
        ..options(2)
    };
    for downstream in [false, true] {
        let budget = Budget::new(16 << 20);
        let (prepared, storage) = stored(&rows, 2, &budget);
        let initial = budget.used();
        let mut calls = 0;
        let error = execute::pipeline::shared::execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            &mut |_| {
                calls += 1;
                assert_eq!(budget.active_workers(), 0);
                if downstream {
                    Err("downstream first-chunk failure".into())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(calls, 1);
        if downstream {
            assert_eq!(error, "downstream first-chunk failure");
        } else {
            assert!(error.contains("overflow"), "{error}");
        }
        assert_eq!(budget.used(), initial);
        assert_eq!(budget.active_workers(), 0);
        assert!(opt.cancellation.check().is_ok());
        drop(storage);
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn resource_and_cancellation_cleanup_preserve_original_storage() {
    for mode in ["resource", "cancel", "callback"] {
        let budget = Budget::new(16 << 20);
        let (prepared, storage) = stored(&[(0, 1), (1, 2), (2, 3), (3, 4)], 2, &budget);
        let initial = budget.used();
        let mut opt = options(2);
        opt.batch_size = 2;
        if mode == "resource" {
            opt.test_pipeline_slice_bytes = Some(4096);
        }
        if mode == "cancel" {
            opt.cancellation.cancel();
        }
        let mut calls = 0;
        let error = execute::pipeline::shared::execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            &mut |_| {
                calls += 1;
                opt.cancellation.cancel();
                Ok(())
            },
        )
        .unwrap_err();
        if mode == "resource" {
            assert!(error.contains("empty pipeline slice"), "{error}");
            assert_eq!(calls, 0);
            assert!(opt.cancellation.check().is_ok());
        } else {
            assert!(opt.cancellation.check().is_err());
        }
        assert_eq!(budget.used(), initial);
        assert_eq!(budget.active_workers(), 0);
        drop(storage);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn cancelled_tight_budget_precedes_candidate_reservations() {
    let budget = Budget::new(16 << 20);
    let (prepared, storage) = stored(&[(0, 1), (1, 2)], 1, &budget);
    let held = budget.reserve(budget.limit() - budget.used()).unwrap();
    let opt = options(2);
    opt.cancellation.cancel();
    let expected = opt.cancellation.check().unwrap_err();
    let error = execute::pipeline::shared::execute(
        &prepared,
        &opt,
        budget.clone(),
        storage.clone(),
        &mut |_| panic!("cancelled entry emitted output"),
    )
    .unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(budget.active_workers(), 0);
    assert_eq!(budget.used(), budget.limit());
    drop(held);
    drop(storage);
    assert_eq!(budget.used(), 0);
}

#[test]
fn reference_transform_declines_candidate_and_baseline_filters_cached_rows() {
    let budget = Budget::new(16 << 20);
    let (prepared, storage) = stored(&[(0, 1), (1, 2), (2, 3)], 1, &budget);
    let opt = options(4);
    let source = Node {
        schema: prepared.schema.clone(),
        estimated_rows: 3,
        op: RelOp::Scan {
            path: PathBuf::from("unused-already-cached-source"),
            columns: vec![0, 1],
            distinct: vec![3, 3],
            statistics: Vec::new(),
        },
    };
    let reference = Node {
        schema: prepared.schema.clone(),
        estimated_rows: 3,
        op: RelOp::Materialize {
            id: 7,
            input: Box::new(source),
            shared_scan: true,
        },
    };
    let mut ctx = Context {
        shared_attempt: Some(Arc::new(
            crate::execute::pipeline::fallback::Attempt::default(),
        )),
        options: &opt,
        scan_filters: HashMap::new(),
        static_filters: HashMap::new(),
        scan_transforms: HashMap::new(),
        replay_transforms: HashMap::new(),
        materialized: HashMap::from([(7, storage.clone())]),
        spill: None,
        budget: budget.clone(),
        metrics: Metrics::default(),
    };
    ctx.replay_transforms.insert(
        &reference as *const Node as usize,
        Expr {
            kind: Kind::Literal(crate::expr::Literal::Bool(false)),
            ty: Type::Bool,
        },
    );
    let before = budget.used();
    assert!(
        try_execute(&prepared.plan, &reference, &mut ctx, &mut |_| panic!(
            "nonadmitted candidate emitted output"
        ))
        .unwrap()
        .is_none()
    );
    assert_eq!(budget.used(), before);
    assert_eq!(ctx.metrics.scan_reuses, 0);
    let result = execute::execute_stream(&prepared, &opt, budget.clone(), |send| {
        reference.run(&mut ctx, send)
    })
    .unwrap();
    assert_eq!(result.batch.num_rows(), 0);
    assert_eq!(ctx.metrics.scan_reuses, 1);
    assert_eq!(
        storage
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    drop(result);
    drop(ctx);
    drop(storage);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}
