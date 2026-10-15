use super::*;
use arrow::datatypes::DataType;
type Row = (Option<i128>, Option<i128>, Option<i128>);
pub(super) fn options(workers: usize) -> Options {
    Options {
        workers,
        batch_size: 3,
        test_source_owned_pipeline: true,
        ..Options::default()
    }
}
pub(super) fn stored(
    chunks: &[Vec<Row>],
    key_type: DataType,
    sql: &str,
    budget: &Budget,
) -> (Prepared, Arc<crate::rel::Materialized>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", key_type.clone(), true),
        Field::new("v", DataType::Decimal128(38, 0), true),
        Field::new("w", DataType::Decimal128(38, 0), true),
    ]));
    let plan = bind::bind(&bind::parse(sql).unwrap(), &schema, None, true).unwrap();
    let batches = chunks
        .iter()
        .map(|rows| {
            let keys: ArrayRef = match key_type {
                DataType::Int32 => Arc::new(Int32Array::from(
                    rows.iter()
                        .map(|r| r.0.map(|n| n as i32))
                        .collect::<Vec<_>>(),
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r.0.map(|n| n as i64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Decimal128(precision, scale) => Arc::new(
                    Decimal128Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ),
                _ => unreachable!(),
            };
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    keys,
                    Arc::new(
                        Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    ),
                    Arc::new(
                        Decimal128Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap()
        })
        .collect();
    let storage = Arc::new(crate::rel::Materialized::from_test_batches(batches, budget).unwrap());
    (
        Prepared {
            auto_simple: None,
            internal_output: false,
            plan,
            schema,
            relational: None,
        },
        storage,
    )
}
pub(super) fn keys(chunks: &[&[i128]]) -> Vec<Vec<Row>> {
    chunks
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|key| (Some(*key), Some(*key % 7 - 3), Some(1)))
                .collect()
        })
        .collect()
}
fn baseline(
    prepared: &Prepared,
    storage: &crate::rel::Materialized,
    opt: &Options,
    budget: &Budget,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    crate::execute::execute_aggregate_stream(
        prepared,
        opt,
        budget.clone(),
        |consume| {
            for batch in &storage.batches {
                consume(batch.clone())?;
            }
            Ok(())
        },
        &mut |batch| {
            batches.push(batch);
            Ok(())
        },
    )
    .unwrap();
    batches
}
#[test]
fn source_owned_preserves_unsorted_local_encounters_boundaries_and_dense_budget() {
    for sql in [
        "SELECT k, SUM(v), AVG(v), COUNT(*), COUNT(v), SUM(v) FROM t GROUP BY k",
        "SELECT k, AVG(v), SUM(v), COUNT(*), COUNT(v), AVG(v) FROM t GROUP BY k",
    ] {
        for key_type in [
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(38, 2),
        ] {
            for chunks in [
                keys(&[&[2, 0, 1, 2], &[5, 3, 2, 4], &[5, 5, 5], &[8, 6, 5, 7]]),
                keys(&[&[8, 6, 7], &[3, 5, 6, 4], &[3, 3], &[0, 2, 3, 1]]),
            ] {
                for workers in [2, 4, 10] {
                    let budget = Budget::new(64 << 20);
                    let (prepared, storage) = stored(&chunks, key_type.clone(), sql, &budget);
                    let initial = budget.used();
                    let report = Arc::new(std::sync::Mutex::new(Report::default()));
                    let opt = Options {
                        test_source_owned_report: Some(report.clone()),
                        ..options(workers)
                    };
                    let expected = baseline(&prepared, &storage, &opt, &budget);
                    let mut actual = Vec::new();
                    let metrics = try_execute(
                        &prepared,
                        &opt,
                        budget.clone(),
                        storage.clone(),
                        Arc::new(Attempt::default()),
                        &mut |batch| {
                            assert_eq!(budget.active_workers(), 0);
                            actual.push(batch);
                            Ok(())
                        },
                    )
                    .unwrap()
                    .expect("proved source domains");
                    assert_eq!(actual, expected);
                    assert_eq!(metrics.pipeline_source_batches, chunks.len());
                    assert_eq!(metrics.pipeline_source_routes, 0);
                    assert_eq!(metrics.pipeline_source_routed_groups, 0);
                    assert_eq!(metrics.pipeline_runtime_submitted[1..], [0, 0]);
                    assert_eq!(
                        metrics.pipeline_runtime_submitted,
                        metrics.pipeline_runtime_completed
                    );
                    assert_eq!(metrics.dense_group_batches, chunks.len());
                    assert!(metrics.dense_group_index_peak_bytes <= DENSE_ALLOWANCE);
                    assert!(metrics.worker_threads_peak <= workers);
                    let report = report.lock().unwrap();
                    assert!(report.admitted);
                    assert_eq!(report.dense_bytes, metrics.dense_group_index_peak_bytes);
                    assert!(report.boundary_merges < metrics.pipeline_producers);
                    assert!(
                        report.proof_seconds
                            + report.owners_seconds
                            + report.boundary_seconds
                            + report.output_seconds
                            <= metrics.execution_seconds + 0.001
                    );
                    assert_eq!(budget.used(), initial);
                    assert!(opt.cancellation.check().is_ok());
                    drop(storage);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }
}

#[test]
fn source_owned_alias_headers_are_prepaid_for_every_empty_owner() {
    let budget = Budget::new(3 << 20);
    let sql = format!(
        "SELECT k,{} FROM t GROUP BY k",
        vec!["SUM(v)"; 512].join(",")
    );
    let (prepared, storage) = stored(&vec![Vec::new(); 64], DataType::Int64, &sql, &budget);
    let initial = budget.used();
    let attempt = Arc::new(Attempt::default());
    let result = try_execute(
        &prepared,
        &options(64),
        budget.clone(),
        storage.clone(),
        attempt.clone(),
        &mut |_| panic!("underfunded headers emitted"),
    )
    .unwrap();
    assert!(result.is_none());
    assert!(!attempt.admitted.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(budget.used(), initial);
    assert_eq!(budget.active_workers(), 0);
    let batches = baseline(&prepared, &storage, &options(64), &budget);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(budget.used(), initial);
}
#[test]
fn source_owned_declines_overlap_null_sparse_and_one_worker_without_emission() {
    for chunks in [
        keys(&[&[0, 2], &[1, 3]]),
        vec![
            vec![(None, Some(1), Some(1))],
            vec![(Some(2), Some(1), Some(1))],
        ],
        keys(&[&[0, 100_000_000], &[100_000_001]]),
    ] {
        let budget = Budget::new(1 << 20);
        let (prepared, storage) = stored(
            &chunks,
            DataType::Int64,
            "SELECT k,SUM(v) FROM t GROUP BY k",
            &budget,
        );
        let initial = budget.used();
        for workers in [1, 2] {
            let attempt = Arc::new(Attempt::default());
            assert!(
                try_execute(
                    &prepared,
                    &options(workers),
                    budget.clone(),
                    storage.clone(),
                    attempt.clone(),
                    &mut |_| panic!("declined candidate emitted")
                )
                .unwrap()
                .is_none()
            );
            assert!(!attempt.admitted.load(std::sync::atomic::Ordering::Acquire));
            assert_eq!(budget.used(), initial);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
#[test]
fn source_owned_handles_empty_owners_skew_equality_chain_and_wide_cancellation() {
    let limit = 10_i128.pow(38) - 1;
    let mut chunks = vec![vec![]];
    chunks.push(vec![(Some(4), Some(limit), None); 19]);
    chunks.push(vec![]);
    chunks.push(vec![(Some(4), Some(-limit), None); 19]);
    chunks.push(vec![(Some(4), None, None), (Some(5), Some(2), None)]);
    let budget = Budget::new(8 << 20);
    let (prepared, storage) = stored(
        &chunks,
        DataType::Int64,
        "SELECT k,SUM(v),AVG(v),COUNT(*),COUNT(v) FROM t GROUP BY k",
        &budget,
    );
    let initial = budget.used();
    let opt = options(10);
    let expected = baseline(&prepared, &storage, &opt, &budget);
    let mut actual = Vec::new();
    try_execute(
        &prepared,
        &opt,
        budget.clone(),
        storage.clone(),
        Arc::new(Attempt::default()),
        &mut |batch| {
            actual.push(batch);
            Ok(())
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(budget.used(), initial);
}
#[test]
fn source_owned_resumes_original_batches_and_releases_all_owners_on_resource_failure() {
    let chunks = (0..80)
        .map(|batch| {
            (0..32)
                .map(|row| (Some(batch * 32 + row), Some(1), Some(1)))
                .collect()
        })
        .collect::<Vec<_>>();
    for available in [90_000, 16 << 20] {
        let budget = Budget::new(32 << 20);
        let (prepared, storage) = stored(
            &chunks,
            DataType::Int64,
            "SELECT k,SUM(v) FROM t GROUP BY k",
            &budget,
        );
        let occupied = budget
            .reserve(budget.limit() - budget.used() - available)
            .unwrap();
        let initial = budget.used();
        let opt = options(2);
        let result = try_execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            Arc::new(Attempt::default()),
            &mut |_| Ok(()),
        );
        if available < 100_000 {
            assert!(
                result
                    .unwrap_err()
                    .starts_with("resource-limit: accounted memory ")
            );
        } else {
            let metrics = result.unwrap().unwrap();
            assert!(metrics.pipeline_step_jobs > metrics.pipeline_producers);
            assert_eq!(metrics.pipeline_source_batches, 80);
        }
        assert_eq!(budget.used(), initial);
        assert_eq!(budget.active_workers(), 0);
        assert!(opt.cancellation.check().is_ok());
        drop(occupied);
    }
}
#[test]
fn source_owned_global_chunks_preserve_finalization_and_downstream_error_order() {
    let limit = 10_i128.pow(38) - 1;
    let chunks = vec![
        vec![(Some(0), Some(1), Some(1)), (Some(1), Some(1), Some(1))],
        vec![(Some(2), Some(limit), Some(1)), (Some(2), Some(1), Some(1))],
    ];
    for downstream in [false, true] {
        let budget = Budget::new(4 << 20);
        let (prepared, storage) = stored(
            &chunks,
            DataType::Int64,
            "SELECT k,SUM(v) FROM t GROUP BY k",
            &budget,
        );
        let initial = budget.used();
        let opt = Options {
            batch_size: 2,
            ..options(2)
        };
        let mut emitted = 0;
        let error = try_execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            Arc::new(Attempt::default()),
            &mut |batch| {
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(batch.num_rows(), 2);
                emitted += 1;
                if downstream {
                    Err("downstream chosen error".into())
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(emitted, 1);
        assert_eq!(
            error,
            if downstream {
                "downstream chosen error"
            } else {
                "decimal precision 38 overflow"
            }
        );
        assert_eq!(budget.used(), initial);
        assert_eq!(budget.active_workers(), 0);
    }
}
#[test]
fn source_owned_cancellation_precedes_credit_failure_and_after_emit_never_replays() {
    let budget = Budget::new(4 << 20);
    let (prepared, storage) = stored(
        &keys(&[&[0, 1], &[2, 3]]),
        DataType::Int64,
        "SELECT k,COUNT(*) FROM t GROUP BY k",
        &budget,
    );
    let initial = budget.used();
    let opt = options(2);
    opt.cancellation.cancel();
    let occupied = budget.reserve(budget.limit() - budget.used()).unwrap();
    let expected = opt.cancellation.check().unwrap_err();
    assert_eq!(
        try_execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            Arc::new(Attempt::default()),
            &mut |_| panic!("cancelled emission")
        )
        .unwrap_err(),
        expected
    );
    drop(occupied);
    let opt = Options {
        batch_size: 1,
        ..options(2)
    };
    let mut emitted = 0;
    let error = try_execute(
        &prepared,
        &opt,
        budget.clone(),
        storage.clone(),
        Arc::new(Attempt::default()),
        &mut |_| {
            emitted += 1;
            opt.cancellation.cancel();
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error, opt.cancellation.check().unwrap_err());
    assert_eq!(emitted, 1);
    assert_eq!(budget.used(), initial);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn aggregate_jobs_keep_proof_metadata_after_early_scope_exit_until_drain() {
    use std::sync::mpsc;
    use std::time::Duration;
    struct QueuedOwner {
        producer: Option<Producer>,
        dropped: mpsc::Sender<()>,
    }
    impl Drop for QueuedOwner {
        fn drop(&mut self) {
            drop(self.producer.take());
            self.dropped.send(()).unwrap();
        }
    }
    let budget = Budget::new(8 << 20);
    let opt = options(2);
    let (prepared, storage) = stored(
        &keys(&[&[1], &[2]]),
        DataType::Int64,
        "SELECT k,SUM(v) FROM t GROUP BY k",
        &budget,
    );
    let entry = budget.used();
    let mut runtime = None;
    let proof = prove(
        &prepared,
        &opt,
        &budget,
        storage.clone(),
        &mut runtime,
        &mut Report::default(),
        None,
    )
    .unwrap()
    .unwrap();
    let backing = Arc::downgrade(&proof._metadata);
    let metadata_bytes = proof._metadata.bytes();
    let executor = super::super::executor(&prepared, budget.clone(), true).unwrap();
    let plan = Arc::new(prepared.plan.clone());
    let mut producers = proof
        .domains
        .iter()
        .enumerate()
        .map(|(rank, domain)| Producer {
            attempt: Arc::new(Attempt::default()),
            rank,
            next: domain.batches.clone(),
            state: new_state(&plan, &executor.expressions, &budget, domain.window).unwrap(),
            plan: plan.clone(),
            expressions: executor.expressions.clone(),
            schema: prepared.schema.clone(),
            budget: budget.clone(),
            metrics: Metrics::default(),
            storage: storage.clone(),
            _metadata: proof._metadata.clone(),
        });
    let running = producers.next().unwrap();
    let queued = producers.next().unwrap();
    drop(producers);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (queued_tx, queued_rx) = mpsc::channel();
    let mut runtime = runtime.take().unwrap();
    assert!(
        runtime
            .try_submit(JobKind::Morsel, move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(SourceJob::Aggregate(running))
            })
            .is_ok()
    );
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let queued = QueuedOwner {
        producer: Some(queued),
        dropped: queued_tx,
    };
    assert!(
        runtime
            .try_submit(JobKind::Morsel, move |_| {
                // The sole helper remains blocked above. Shutdown must discard this
                // queued owner; touching the producer keeps the whole capture owned.
                drop(queued);
                panic!("queued aggregate unexpectedly ran");
            })
            .is_ok()
    );
    // Match early Result-closure unwinding: local proof/executor/plan retire
    // before explicit outer runtime shutdown drains transferred producers.
    drop(proof);
    drop(executor);
    drop(plan);
    assert!(backing.upgrade().is_some());
    assert!(budget.used() >= entry + metadata_bytes);
    let join = std::thread::spawn(move || runtime.shutdown_for_retry());
    queued_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // The queued producer is gone, but the running producer still requires
    // this exact backing, not just some unrelated nonzero budget reservation.
    assert!(backing.upgrade().is_some());
    assert!(budget.used() >= entry + metadata_bytes);
    release_tx.send(()).unwrap();
    join.join().unwrap().unwrap();
    assert!(backing.upgrade().is_none());
    assert_eq!(budget.active_workers(), 0);
    assert_eq!(budget.used(), entry);
    assert!(opt.cancellation.check().is_ok());
    drop(storage);
    assert_eq!(budget.used(), 0);
}
