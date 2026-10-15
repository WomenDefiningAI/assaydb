use super::*;
use crate::execute::pipeline::fallback::{Failure, ResourceReason};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shared-retry-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..32).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(vec![1; 32])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(5)
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
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", arrow::datatypes::DataType::Int64, false),
        Field::new("v", arrow::datatypes::DataType::Int64, false),
    ]))
}
fn scan(path: &std::path::Path) -> Node {
    Node {
        schema: schema(),
        estimated_rows: 32,
        op: RelOp::Scan {
            path: path.to_owned(),
            columns: vec![0, 1],
            distinct: vec![32, 1],
            statistics: Vec::new(),
        },
    }
}
fn root(path: &std::path::Path) -> Node {
    let schema = schema();
    let plan = bind::bind(
        &bind::parse("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k").unwrap(),
        &schema,
        None,
        true,
    )
    .unwrap();
    let output = Arc::new(Schema::new(
        plan.names
            .iter()
            .zip(&plan.types)
            .map(|(name, ty)| ty.field(name, true))
            .collect::<Vec<_>>(),
    ));
    Node {
        schema: output,
        estimated_rows: 32,
        op: RelOp::Unary {
            plan,
            input: Box::new(Node {
                schema,
                estimated_rows: 32,
                op: RelOp::Materialize {
                    id: 1,
                    input: Box::new(scan(path)),
                    shared_scan: true,
                },
            }),
        },
    }
}
fn options() -> Options {
    Options {
        workers: 4,
        batch_size: 2,
        memory_bytes: 16 << 20,
        test_shared_batch_pipeline: true,
        test_pipeline_slice_bytes: Some(128 << 10),
        ..Options::default()
    }
}
#[test]
fn full_replay_after_root_emission_has_no_duplicate_outputs_or_leaks() {
    let fixture = Fixture::new();
    let node = root(&fixture.0);
    let mut opt = options();
    let baseline = execute_once(&node, &opt, Budget::new(opt.memory_bytes), None, false).unwrap();
    opt.test_shared_after_emit_failure = true;
    let budget = Budget::new(opt.memory_bytes);
    let held = budget.reserve(73).unwrap();
    let mut report = PipelineAttemptReport::default();
    let result = execute_reported(&node, &opt, budget.clone(), &mut report, false).unwrap();
    assert_eq!(result.batch, baseline.batch);
    assert_eq!(result.batch.num_rows(), 32);
    assert_eq!(report.candidate_attempts, 1);
    assert_eq!(report.fallback_attempts, 1);
    assert!(report.candidate_seconds > 0.0);
    assert!(report.fallback_seconds > 0.0);
    assert!(report.total_seconds >= report.candidate_seconds + report.fallback_seconds);
    assert_eq!(result.metrics.peak_reserved_bytes, budget.peak());
    drop(result);
    assert_eq!(budget.used(), 73);
    assert_eq!(budget.active_workers(), 0);
    assert!(opt.cancellation.check().is_ok());
    drop(held);
}
#[test]
fn failed_guard_capture_defers_to_baseline_sql_chronology() {
    let fixture = Fixture::new();
    let mut node = root(&fixture.0);
    let opt = options();
    let missing = scan(&fixture.0.with_extension("absent"));
    node = Node {
        schema: node.schema.clone(),
        estimated_rows: 64,
        op: RelOp::UnionAll {
            left: Box::new(node),
            right: Box::new(missing),
        },
    };
    let budget = Budget::new(opt.memory_bytes);
    let mut report = PipelineAttemptReport::default();
    let mut calls = 0;
    let error = run(&node, &opt, &budget, &mut report, |attempt| {
        calls += 1;
        assert!(attempt.is_none());
        Err("division by zero".into())
    })
    .err()
    .unwrap();
    assert_eq!(error, "division by zero");
    assert_eq!(calls, 1);
    assert_eq!(report.fallback_attempts, 0);
    assert_eq!(budget.used(), 0);
}
#[test]
fn mixed_resource_and_sql_replay_once_but_terminal_and_cancel_never_retry() {
    let fixture = Fixture::new();
    let node = root(&fixture.0);
    for mode in ["mixed", "sql", "terminal", "cancel", "panic"] {
        let opt = options();
        let budget = Budget::new(opt.memory_bytes);
        let mut report = PipelineAttemptReport::default();
        let mut calls = 0;
        let result = run(&node, &opt, &budget, &mut report, |attempt| {
            calls += 1;
            if let Some(attempt) = attempt {
                attempt.admitted.store(true, Ordering::Release);
                attempt.record(0, Failure::sql("division by zero".into()));
                if mode != "sql" {
                    attempt.record(1, Failure::Resource(ResourceReason::OversizedBatch));
                }
                if mode == "terminal" {
                    attempt.record(2, Failure::Terminal("injected invariant failure".into()));
                }
                if mode == "cancel" {
                    opt.cancellation.cancel();
                }
                if mode == "panic" {
                    let mut runtime = crate::pipeline_runtime::Runtime::<()>::new(
                        &budget,
                        2,
                        2,
                        opt.cancellation.clone(),
                    )
                    .unwrap();
                    runtime
                        .try_submit(crate::pipeline_runtime::JobKind::Morsel, |_| {
                            panic!("injected worker panic")
                        })
                        .map_err(|e| e.reason.to_string())?;
                    let _ = runtime.next_completion();
                    let error = runtime.shutdown_for_retry().err().unwrap();
                    attempt.record(2, Failure::Terminal(error));
                }
                Err("division by zero".into())
            } else {
                Err("baseline exact SQL error".into())
            }
        });
        let error = result.err().unwrap();
        assert_eq!(calls, if mode == "mixed" { 2 } else { 1 });
        assert_eq!(report.fallback_attempts, usize::from(mode == "mixed"));
        if mode == "mixed" {
            assert_eq!(error, "baseline exact SQL error");
            assert_eq!(
                report.candidate_sql_error.as_deref(),
                Some("division by zero")
            );
        }
        if mode == "sql" {
            assert_eq!(error, "division by zero");
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        if mode != "cancel" {
            assert!(opt.cancellation.check().is_ok());
        }
    }
}
#[test]
fn source_replacement_forbids_resource_replay_and_sql_only_keeps_its_error() {
    for sql_only in [false, true] {
        let fixture = Fixture::new();
        let replacement = Fixture::new();
        let node = root(&fixture.0);
        let opt = options();
        let budget = Budget::new(opt.memory_bytes);
        let mut report = PipelineAttemptReport::default();
        let mut calls = 0;
        let error = run(&node, &opt, &budget, &mut report, |attempt| {
            calls += 1;
            let attempt = attempt.unwrap();
            attempt.admitted.store(true, Ordering::Release);
            std::fs::rename(&replacement.0, &fixture.0).unwrap();
            std::fs::copy(&fixture.0, &replacement.0).unwrap();
            let failure = if sql_only {
                Failure::sql("division by zero".into())
            } else {
                Failure::Resource(ResourceReason::OversizedBatch)
            };
            Err(attempt.record(0, failure))
        })
        .err()
        .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(report.fallback_attempts, 0);
        if sql_only {
            assert_eq!(error, "division by zero");
        } else {
            assert!(error.contains("source changed"), "{error}");
        }
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn source_walk_includes_scalar_fallback_correlated_mark_and_deduplicates_references() {
    let first = Fixture::new();
    let second = Fixture::new();
    let third = Fixture::new();
    let budget = Budget::new(1 << 20);
    let opt = options();
    let predicate = Expr {
        kind: crate::expr::Kind::Literal(crate::expr::Literal::Bool(true)),
        ty: Type::Bool,
    };
    let node = Node {
        schema: schema(),
        estimated_rows: 32,
        op: RelOp::ScalarDefault {
            input: Box::new(Node {
                schema: schema(),
                estimated_rows: 32,
                op: RelOp::CorrelatedMark {
                    left: Box::new(scan(&first.0)),
                    right: Box::new(scan(&second.0)),
                    predicate,
                    marker: CorrelatedMarker::Exists { negated: false },
                },
            }),
            fallback: Box::new(Node {
                schema: schema(),
                estimated_rows: 64,
                op: RelOp::UnionAll {
                    left: Box::new(scan(&third.0)),
                    right: Box::new(scan(&first.0)),
                },
            }),
            value: 0,
            marker: 1,
        },
    };
    let sources = Sources::capture(&node, &budget, &opt).unwrap();
    assert_eq!(sources.entries.len(), 3);
    sources.verify(&opt).unwrap();
    drop(sources);
    assert_eq!(budget.used(), 0);
}
#[test]
fn guard_credit_headroom_is_charged_and_tight_baseline_error_is_preserved() {
    let fixture = Fixture::new();
    let node = root(&fixture.0);
    let opt = options();
    let budget = Budget::new(4096);
    let mut report = PipelineAttemptReport::default();
    let mut calls = 0;
    let mut guard_bytes = 0;
    // A bare baseline reservation fits exactly; optional snapshot ownership is
    // deliberately not hidden from the same budget during the one replay.
    drop(budget.reserve(4096).unwrap());
    let error = run(&node, &opt, &budget, &mut report, |attempt| {
        calls += 1;
        if let Some(attempt) = attempt {
            attempt.admitted.store(true, Ordering::Release);
            guard_bytes = budget.used();
            Err(attempt.record(0, Failure::Resource(ResourceReason::OversizedBatch)))
        } else {
            budget.reserve(4096).map(|_| unreachable!())
        }
    })
    .err()
    .unwrap();
    assert!(guard_bytes > 0);
    assert!(error.starts_with("resource-limit: accounted memory "));
    assert_eq!(calls, 2);
    assert_eq!(budget.used(), 0);
}
#[test]
fn successful_candidate_is_not_published_after_source_replacement() {
    let fixture = Fixture::new();
    let replacement = Fixture::new();
    let node = root(&fixture.0);
    let opt = options();
    let budget = Budget::new(opt.memory_bytes);
    let mut report = PipelineAttemptReport::default();
    let error = run(&node, &opt, &budget, &mut report, |attempt| {
        attempt.unwrap().admitted.store(true, Ordering::Release);
        let result = execute_once(&node, &opt, budget.clone(), None, false)?;
        std::fs::rename(&replacement.0, &fixture.0).unwrap();
        std::fs::copy(&fixture.0, &replacement.0).unwrap();
        Ok(result)
    })
    .err()
    .unwrap();
    assert!(error.contains("source changed"), "{error}");
    assert_eq!(report.fallback_attempts, 0);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn allocation_failure_during_materialization_is_not_an_admitted_attempt_or_replay() {
    let fixture = Fixture::new();
    let node = root(&fixture.0);
    let opt = options();
    let budget = Budget::new(opt.memory_bytes);
    let mut report = PipelineAttemptReport::default();
    let mut calls = 0;
    let error = run(&node, &opt, &budget, &mut report, |attempt| {
        calls += 1;
        let attempt = attempt.expect("optional candidate policy before storage population");
        assert!(!attempt.admitted.load(Ordering::Acquire));
        // Force a real allocation failure in the original scan/materialization
        // path, before shared grouping has begun; no retry permission exists.
        let pressure = budget.reserve(budget.limit() - budget.used() - 1).unwrap();
        let mut ctx = Context {
            filtered_reuse: None,
            build_groups: None,
            demand_cache: None,
            shared_attempt: Some(attempt.clone()),
            options: &opt,
            scan_filters: HashMap::new(),
            static_filters: HashMap::new(),
            scan_transforms: HashMap::new(),
            lane_transforms: HashMap::new(),
            replay_transforms: HashMap::new(),
            materialized: HashMap::new(),
            sideways_registry_lease: None,
            spill: None,
            budget: budget.clone(),
            metrics: Metrics::default(),
        };
        let error = materialized_storage(7, &scan(&fixture.0), true, &mut ctx)
            .err()
            .unwrap();
        assert!(ctx.materialized.is_empty());
        assert!(!attempt.admitted.load(Ordering::Acquire));
        drop(ctx);
        drop(pressure);
        Err(error)
    })
    .err()
    .unwrap();
    assert!(
        error.starts_with("resource-limit: accounted memory "),
        "{error}"
    );
    assert_eq!(calls, 1);
    assert_eq!(report.candidate_attempts, 0);
    assert_eq!(report.fallback_attempts, 0);
    assert_eq!(report.candidate_seconds, 0.0);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}
#[test]
fn public_entry_checks_worker_count_and_cancellation_before_source_capture() {
    let node = root(std::path::Path::new(
        "missing-source-that-must-not-preempt-validation",
    ));
    let zero = Options {
        workers: 0,
        ..options()
    };
    assert_eq!(
        super::super::execute(&node, &zero).err().unwrap(),
        "workers must be 1..64"
    );
    let cancelled = options();
    cancelled.cancellation.cancel();
    let expected = cancelled.cancellation.check().err().unwrap();
    assert_eq!(
        super::super::execute(&node, &cancelled).err().unwrap(),
        expected
    );
}

#[test]
fn nonnumeric_source_owned_shapes_never_capture_or_hold_guard_credit() {
    for ty in [Type::Binary, Type::Text] {
        let schema = Arc::new(Schema::new(vec![Field::new("k", ty.arrow(), true)]));
        let plan = bind::bind(
            &bind::parse("SELECT k,COUNT(*) FROM t GROUP BY k").unwrap(),
            &schema,
            None,
            true,
        )
        .unwrap();
        let node = Node {
            schema: Arc::new(Schema::new(vec![
                Field::new("k", ty.arrow(), true),
                Field::new(&plan.names[1], Type::Int.arrow(), true),
            ])),
            estimated_rows: 4096,
            op: RelOp::Unary {
                plan,
                input: Box::new(Node {
                    schema: schema.clone(),
                    estimated_rows: 4096,
                    op: RelOp::Materialize {
                        id: 1,
                        shared_scan: true,
                        input: Box::new(Node {
                            schema,
                            estimated_rows: 4096,
                            op: RelOp::Scan {
                                path: PathBuf::from("nonexistent-guard-must-not-open.parquet"),
                                columns: vec![0],
                                distinct: vec![3],
                                statistics: vec![],
                            },
                        }),
                    },
                }),
            },
        };
        let opt = Options {
            test_source_owned_pipeline: true,
            test_shared_batch_pipeline: false,
            ..options()
        };
        assert!(!potential(&node, &opt));
        let oracle = Options {
            test_source_owned_pipeline: false,
            test_shared_batch_pipeline: true,
            ..opt.clone()
        };
        assert!(potential(&node, &oracle));
        let budget = Budget::new(4096);
        let mut report = PipelineAttemptReport::default();
        let mut calls = 0;
        let error = run(&node, &opt, &budget, &mut report, |attempt| {
            calls += 1;
            assert!(attempt.is_none());
            // Every byte remains available to the unchanged ordinary executor.
            let held = budget.reserve(4096)?;
            assert_eq!(budget.used(), 4096);
            drop(held);
            Err("baseline sentinel".into())
        })
        .err()
        .unwrap();
        assert_eq!(error, "baseline sentinel");
        assert_eq!(calls, 1);
        assert_eq!(report.candidate_attempts, 0);
        assert_eq!(report.fallback_attempts, 0);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn source_guard_and_visitor_include_both_typed_membership_inputs() {
    let first = Fixture::new();
    let second = Fixture::new();
    let budget = Budget::new(1 << 20);
    let opt = options();
    let tables = HashMap::from([
        ("a".into(), first.0.clone()),
        ("b".into(), second.0.clone()),
    ]);
    let prepared =
        crate::execute::prepare("SELECT k+0.0 IN(SELECT k FROM b) FROM a", &tables, &opt).unwrap();
    let root = prepared.relational.as_ref().unwrap();
    let mut typed = 0;
    let mut scans = 0;
    visit(root, &mut |node| {
        typed += usize::from(matches!(node.op, RelOp::TypedMark { .. }));
        scans += usize::from(matches!(node.op, RelOp::Scan { .. }));
        Ok(())
    })
    .unwrap();
    assert_eq!(typed, 1);
    assert_eq!(scans, 2);
    let sources = Sources::capture(root, &budget, &opt).unwrap();
    assert_eq!(sources.entries.len(), 2);
    sources.verify(&opt).unwrap();
    drop(sources);
    assert_eq!(budget.used(), 0);
}
