use super::tests::{Fixture, test_producer};
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

type BeforeStep = dyn Fn(&mut Producer, bool) + Send + Sync;
type AfterStep = dyn Fn(usize, &Result<ProducerStep>) + Send + Sync;
type Submitted = dyn Fn(&mut Runtime<JobOutput>) + Send + Sync;

#[derive(Default)]
pub(crate) struct Hooks {
    before_step: Option<Box<BeforeStep>>,
    after_step: Option<Box<AfterStep>>,
    all_submitted: Option<Box<Submitted>>,
}
impl Hooks {
    pub(super) fn before(&self, producer: &mut Producer, background: bool) {
        if let Some(hook) = &self.before_step {
            hook(producer, background);
        }
    }
    pub(super) fn after(&self, source: usize, step: &Result<ProducerStep>) {
        if let Some(hook) = &self.after_step {
            hook(source, step);
        }
    }
    pub(super) fn submitted(&self, runtime: &mut Runtime<JobOutput>) {
        if let Some(hook) = &self.all_submitted {
            hook(runtime);
        }
    }
}

fn restored(budget: &Budget, workers: usize) {
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
    let permits = budget.workers(workers - 1, workers - 1);
    assert_eq!(permits.count, workers - 1);
    drop(permits);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn autonomous_q01_pipeline_counts_live_threads_through_join() {
    let fixture = Fixture::new(256, 8, false);
    let serial = Options {
        workers: 1,
        batch_size: 2,
        ..Options::default()
    };
    let prepared = fixture.prepared(
        "SELECT s,k,SUM(v),SUM(v * 0.95),SUM(v * 0.95 * 1.02),AVG(v),COUNT(*) FROM t WHERE d=0 GROUP BY s,k ORDER BY s,k",
        &serial,
    );
    let expected = crate::execute::execute(&prepared, &serial).unwrap().batch;
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(serial.memory_bytes);
        let stepping_budget = budget.clone();
        let submitted_budget = budget.clone();
        let hooks = (workers > 1).then(|| {
            Arc::new(Hooks {
                before_step: Some(Box::new(move |producer, background| {
                    assert!(background, "sources must progress autonomously");
                    assert_eq!(producer.options.workers, 1);
                    assert_eq!(producer.metrics.parallel_scan_tasks, 0);
                    assert_eq!(stepping_budget.live_threads(), workers);
                })),
                all_submitted: Some(Box::new(move |runtime| {
                    // Let every source finish without caller draining. Even
                    // idle helpers still occupy live thread slots until join.
                    runtime.test_wait_ready(workers);
                    let stats = runtime.statistics();
                    assert_eq!(stats.caller_started, [0; 3]);
                    assert!(stats.background_started[0] > workers);
                    assert_eq!(stats.completed, stats.submitted);
                    assert_eq!(submitted_budget.live_threads(), workers);
                    assert_eq!(submitted_budget.workers(1, workers - 1).count, 0);
                })),
                ..Hooks::default()
            })
        });
        let options = Options {
            workers,
            test_pipeline_partitions: Some(4),
            test_pipeline_continuation: hooks,
            ..serial.clone()
        };
        let actual = execute(&prepared, &options, budget.clone()).unwrap();
        assert_eq!(actual.batch, expected, "workers={workers}");
        assert_eq!(actual.metrics.pipeline_producers, workers);
        assert_eq!(actual.metrics.pipeline_fallbacks, 0);
        assert!(actual.metrics.worker_threads_peak <= workers);
        assert_eq!(actual.metrics.worker_threads_peak, workers);
        assert_eq!(budget.peak_threads(), workers);
        assert_eq!(budget.live_threads(), 1);
        assert_eq!(
            actual.metrics.pipeline_runtime_submitted,
            actual.metrics.pipeline_runtime_completed
        );
        assert!(options.cancellation.check().is_ok());
        drop(actual);
        restored(&budget, workers);
    }
}

#[test]
fn background_later_source_errors_cannot_suppress_delayed_lower_source_error() {
    const DEADLINE: Duration = Duration::from_secs(10);
    for workers in [2, 4, 10] {
        for slice in [None, Some(1 << 20)] {
            let fixture = Fixture::new(workers * 32, 4, false);
            let serial = Options {
                batch_size: 1,
                ..Options::default()
            };
            let prepared = fixture.prepared(
                "SELECT k,COUNT(CAST(1.0 / CASE WHEN v=3 THEN 0 ELSE 1 END AS INTEGER)),COUNT(CASE WHEN v>=27 THEN v+99999999999999999999999999999999999999 ELSE 0 END) FROM t GROUP BY k",
                &serial,
            );
            assert_eq!(
                crate::execute::execute(&prepared, &serial).err().unwrap(),
                "division by zero"
            );
            let (submit, submitted) = mpsc::channel();
            let submitted = Mutex::new(submitted);
            let (pause, paused) = mpsc::channel();
            let (release, gate) = mpsc::channel();
            let gate = Mutex::new(gate);
            let (error, errors) = mpsc::channel();
            let steps = Arc::new(
                (0..workers)
                    .map(|_| AtomicUsize::new(0))
                    .collect::<Vec<_>>(),
            );
            let calls = steps.clone();
            let hooks = Hooks {
                before_step: Some(Box::new(move |producer, background| {
                    assert!(background, "coordinator must not execute the test sources");
                    let step = calls[producer.source].fetch_add(1, Ordering::SeqCst);
                    assert_eq!(step, producer.metrics.pipeline_step_jobs);
                    if producer.source == 0 {
                        if step == 0 {
                            // Queue every source before the first autonomous requeue,
                            // including the one-helper (workers=2) case.
                            submitted.lock().unwrap().recv_timeout(DEADLINE).unwrap();
                        } else if step == 1 {
                            assert_eq!(producer.metrics.rows_read, 4);
                            assert!(producer.state.is_some());
                            pause.send(()).unwrap();
                            gate.lock().unwrap().recv_timeout(DEADLINE).unwrap();
                        }
                    }
                })),
                after_step: Some(Box::new(move |source, step| {
                    if let Err(message) = step {
                        error.send((source, message.clone())).unwrap();
                    }
                })),
                all_submitted: Some(Box::new(move |runtime| {
                    submit.send(()).unwrap();
                    runtime.test_wait_ready(workers);
                    let stats = runtime.statistics();
                    assert_eq!(stats.caller_started, [0; 3]);
                    assert_eq!(stats.background_started, stats.submitted);
                    assert_eq!(stats.completed, stats.submitted);
                    assert_eq!(stats.peak_outstanding, workers);
                    assert!(stats.peak_queued <= workers);
                })),
            };
            let options = Options {
                workers,
                batch_size: 1,
                test_pipeline_partitions: Some(4),
                test_pipeline_slice_bytes: slice,
                test_pipeline_continuation: Some(Arc::new(hooks)),
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let owned_budget = budget.clone();
            let (send, receive) = mpsc::channel();
            let handle = std::thread::spawn(move || {
                let result = execute(&prepared, &options, owned_budget);
                send.send(result.err().unwrap()).unwrap();
            });
            paused.recv_timeout(DEADLINE).unwrap();
            let mut seen = std::collections::HashSet::new();
            for _ in 1..workers {
                let (source, message) = errors.recv_timeout(DEADLINE).unwrap();
                assert_ne!(source, 0);
                assert!(seen.insert(source));
                assert_eq!(message, "decimal precision 38 overflow");
            }
            release.send(()).unwrap();
            let (source, message) = errors.recv_timeout(DEADLINE).unwrap();
            assert_eq!((source, message.as_str()), (0, "division by zero"));
            assert!(seen.insert(source));
            assert_eq!(receive.recv_timeout(DEADLINE).unwrap(), "division by zero");
            handle.join().unwrap();
            assert_eq!(seen.len(), workers);
            assert!(errors.try_recv().is_err(), "duplicate source error receipt");
            assert_eq!(steps[0].load(Ordering::SeqCst), 3);
            for step in &steps[1..] {
                assert_eq!(step.load(Ordering::SeqCst), 1);
            }
            restored(&budget, workers);
        }
    }
}

#[test]
fn bounded_flush_fallback_after_background_progress_replays_with_clean_ownership() {
    for workers in [2, 4, 10] {
        let fixture = Fixture::new(workers * 256, 8, false);
        let serial = Options {
            batch_size: 8,
            ..Options::default()
        };
        let prepared = fixture.prepared("SELECT v,COUNT(*) FROM t GROUP BY v", &serial);
        let expected = crate::execute::execute(&prepared, &serial).unwrap().batch;
        let flushes = Arc::new(
            (0..workers)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let observed = flushes.clone();
        let refused = Arc::new(AtomicUsize::new(0));
        let refused_hook = refused.clone();
        let hooks = Hooks {
            before_step: Some(Box::new(move |producer, _| {
                if producer.metrics.pipeline_pressure_flushes > 0 {
                    // The coordinator has acknowledged and released the previous
                    // raw slice. Withdraw private capacity only on this resumed
                    // attempt, so the unchanged query budget can fund replay.
                    assert!(producer.metrics.pipeline_step_jobs > 1);
                    assert!(producer.state.is_none());
                    assert!(producer.backing.is_none());
                    assert!(producer.pending.is_some());
                    producer.slice_bytes = Some(4096);
                    refused_hook.fetch_add(1, Ordering::SeqCst);
                }
            })),
            after_step: Some(Box::new(move |source, step| {
                if let Ok(ProducerStep::Flush { producer, .. }) = step {
                    assert!(producer.background);
                    assert!(producer.metrics.pipeline_step_jobs > 1);
                    assert_eq!(observed[source].fetch_add(1, Ordering::SeqCst), 0);
                }
            })),
            all_submitted: Some(Box::new(move |runtime| {
                // Every source progresses autonomously through several real
                // row groups to a pressure flush before the caller drains any.
                runtime.test_wait_ready(workers);
                let stats = runtime.statistics();
                assert_eq!(stats.caller_started, [0; 3]);
                assert!(stats.background_started[0] > workers);
                assert_eq!(stats.completed, stats.submitted);
                assert_eq!(stats.peak_outstanding, workers);
                assert!(stats.peak_queued <= workers);
            })),
        };
        let report = Arc::new(Mutex::new(PipelineAttemptReport::default()));
        let options = Options {
            workers,
            batch_size: 8,
            test_pipeline_partitions: Some(4),
            test_pipeline_slice_bytes: Some(128 << 10),
            test_pipeline_fallback: true,
            test_pipeline_report: Some(report.clone()),
            test_pipeline_continuation: Some(Arc::new(hooks)),
            ..Options::default()
        };
        let budget = Budget::new(options.memory_bytes);
        let output = execute(&prepared, &options, budget.clone()).unwrap();
        assert_eq!(output.batch, expected);
        assert_eq!(output.metrics.pipeline_candidate_attempts, 1);
        assert_eq!(output.metrics.pipeline_fallbacks, 1);
        assert_eq!(
            output.metrics.pipeline_candidate_error.as_deref(),
            Some("resource-limit: evaluated batch cannot fit empty pipeline slice")
        );
        assert!(refused.load(Ordering::SeqCst) > 0);
        for flush in flushes.iter() {
            assert_eq!(flush.load(Ordering::SeqCst), 1);
        }
        let report = report.lock().unwrap();
        assert_eq!(report.candidate_attempts, 1);
        assert_eq!(report.fallback_attempts, 1);
        assert!(report.candidate_sql_error.is_none());
        assert!(options.cancellation.check().is_ok());
        drop(output);
        restored(&budget, workers);
    }
}

#[test]
fn full_8192_batches_engage_parallel_admission_and_preserve_serial_sql() {
    let rows = 40 * 8192 + 17;
    let fixture = Fixture::new(rows, 2 * 8192, false);
    let serial = Options {
        batch_size: 8192,
        // Encoded-key preflight conservatively funds an all-new 8192-row batch
        // per source even though this fixture has only a handful of groups.
        memory_bytes: 1 << 30,
        ..Options::default()
    };
    for sql in [
        "SELECT k,SUM(v),SUM(k),COUNT(k),COUNT(*) FROM t GROUP BY k",
        "SELECT s,k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY s,k",
        "SELECT CAST(k AS REAL) AS g,SUM(v),COUNT(*) FROM t GROUP BY CAST(k AS REAL)",
        "SELECT COALESCE(k,CAST(2.0 AS INTEGER)) AS g,SUM(v),COUNT(*) FROM t GROUP BY COALESCE(k,CAST(2.0 AS INTEGER))",
        "SELECT k,SUM(v),COUNT(*) FROM t WHERE v < -10 GROUP BY k",
    ] {
        let prepared = fixture.prepared(sql, &serial);
        let expected = crate::execute::execute(&prepared, &serial).unwrap().batch;
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                test_pipeline_default: true,
                ..serial.clone()
            };
            let budget = Budget::new(options.memory_bytes);
            let actual = execute(&prepared, &options, budget.clone()).unwrap();
            assert_eq!(actual.batch, expected, "workers={workers}: {sql}");
            assert_eq!(actual.metrics.rows_read, rows);
            if workers > 1 {
                assert_eq!(
                    actual.metrics.pipeline_producers, workers,
                    "{sql}: fallback={:?}",
                    actual.metrics.pipeline_candidate_error
                );
                assert_eq!(actual.metrics.pipeline_source_batches, 41);
                assert!(actual.metrics.pipeline_runtime_background[0] > 0);
                assert!(actual.metrics.pipeline_step_jobs > workers);
                assert_eq!(
                    actual.metrics.pipeline_runtime_submitted,
                    actual.metrics.pipeline_runtime_completed
                );
                assert!(actual.metrics.pipeline_runtime_peak_outstanding <= 2 * workers);
                assert!(actual.metrics.pipeline_runtime_peak_queued <= 2 * workers);
                assert!(actual.metrics.pipeline_exchange_peak <= workers);
            } else {
                assert_eq!(actual.metrics.pipeline_producers, 0);
            }
            drop(actual);
            restored(&budget, workers);
        }
    }
    // FLOAT aggregate inputs bind and use one ordered accumulator per group;
    // they must never be admitted and reassociated by the continuation path.
    for function in ["SUM", "AVG"] {
        let sql = format!("SELECT k,{function}(CAST(v AS REAL)) FROM t GROUP BY k");
        let prepared = fixture.prepared(&sql, &serial);
        let expected = crate::execute::execute(&prepared, &serial).unwrap().batch;
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                test_pipeline_default: true,
                ..serial.clone()
            };
            assert!(!eligible(&prepared, &options));
            let actual = crate::execute::execute(&prepared, &options).unwrap();
            assert_eq!(actual.batch, expected, "workers={workers}: {sql}");
            assert_eq!(actual.metrics.pipeline_producers, 0);
        }
    }
    for sql in [
        "SELECT SUM(v) FROM t",
        "SELECT k,COUNT(DISTINCT s) FROM t GROUP BY k",
    ] {
        let prepared = fixture.prepared(sql, &serial);
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                test_pipeline_default: true,
                ..serial.clone()
            };
            assert!(!eligible(&prepared, &options));
        }
    }
}

#[test]
fn cancellation_joins_live_background_producer_and_discards_ready_fragments() {
    const DEADLINE: Duration = Duration::from_secs(10);
    for workers in [2, 4, 10] {
        // Each producer needs two full 16-batch quanta followed by EOF. With
        // one helper, FIFO redispatch completes the lower sources before the
        // last source's EOF quantum. More helpers may finish in any order.
        let fixture = Fixture::new(workers * 32, 32, false);
        let (pause, paused) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let gate = Mutex::new(gate);
        let (ready, receipts_ready) = mpsc::channel();
        let calls = Arc::new(
            (0..workers)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let observed = calls.clone();
        let hooks = Hooks {
            before_step: Some(Box::new(move |producer, background| {
                assert!(background);
                let step = observed[producer.source].fetch_add(1, Ordering::SeqCst);
                assert_eq!(step, producer.metrics.pipeline_step_jobs);
                if producer.source == workers - 1 && step == 2 {
                    assert_eq!(producer.metrics.rows_read, 32);
                    assert!(producer.cursor.is_some());
                    assert!(producer.state.is_some());
                    assert!(producer.backing.is_some());
                    pause.send(()).unwrap();
                    gate.lock().unwrap().recv_timeout(DEADLINE).unwrap();
                }
            })),
            all_submitted: Some(Box::new(move |runtime| {
                runtime.test_wait_ready(workers - 1);
                ready.send(()).unwrap();
                runtime.test_wait_ready(workers);
                let stats = runtime.statistics();
                assert_eq!(stats.caller_started, [0; 3]);
                assert_eq!(stats.submitted, [3 * workers, 0, 0]);
                assert_eq!(stats.completed, stats.submitted);
                assert_eq!(stats.peak_outstanding, workers);
                assert!(stats.peak_queued <= workers);
            })),
            ..Hooks::default()
        };
        let options = Options {
            workers,
            batch_size: 1,
            test_pipeline_partitions: Some(4),
            test_pipeline_slice_bytes: Some(1 << 20),
            test_pipeline_continuation: Some(Arc::new(hooks)),
            ..Options::default()
        };
        let cancel = options.cancellation.clone();
        let prepared = fixture.prepared("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", &options);
        let budget = Budget::new(options.memory_bytes);
        let owned_budget = budget.clone();
        let (send, receive) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            send.send(execute(&prepared, &options, owned_budget).err().unwrap())
                .unwrap();
        });
        paused.recv_timeout(DEADLINE).unwrap();
        receipts_ready.recv_timeout(DEADLINE).unwrap();
        assert!(budget.used() > 0);
        assert_eq!(budget.active_workers(), workers - 1);
        cancel.cancel();
        release.send(()).unwrap();
        assert!(
            receive
                .recv_timeout(DEADLINE)
                .unwrap()
                .starts_with("cancelled:")
        );
        handle.join().unwrap();
        for calls in calls.iter() {
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
        restored(&budget, workers);
    }
}

#[test]
fn autonomous_sources_match_serial_values_order_nulls_and_errors() {
    // Twenty physical groups, repeated/NULL keys, uneven text-key frequency,
    // and values crossing zero. One-worker unaligned batches decline cleanly.
    let fixture = Fixture::new(280, 14, false);
    let serial = Options {
        batch_size: 1,
        ..Options::default()
    };
    for sql in [
        "SELECT k,SUM(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
        "SELECT CASE WHEN v<200 THEN 'common' ELSE s END AS g,SUM(v),COUNT(*) FROM t GROUP BY CASE WHEN v<200 THEN 'common' ELSE s END",
        "SELECT k,SUM(k),COUNT(*) FROM t GROUP BY k",
        "SELECT s,k,SUM(v),COUNT(*) AS n FROM t GROUP BY s,k ORDER BY n DESC",
        "SELECT k,SUM(v+k),COUNT(*) FROM t GROUP BY k",
        "SELECT k,COUNT(*) FROM t WHERE v>1000 GROUP BY k",
        "SELECT k,COUNT(CAST(1.0/v AS INTEGER)) FROM t GROUP BY k",
    ] {
        let prepared = fixture.prepared(sql, &serial);
        let expected = crate::execute::execute(&prepared, &serial).map(|out| out.batch);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for slice in [None, Some(1 << 20)] {
                    let options = Options {
                        workers,
                        batch_size,
                        test_pipeline_partitions: Some(4),
                        test_pipeline_slice_bytes: slice,
                        ..Options::default()
                    };
                    assert!(eligible(&prepared, &options), "{sql}");
                    let budget = Budget::new(options.memory_bytes);
                    let actual = execute(&prepared, &options, budget.clone()).map(|out| out.batch);
                    assert_eq!(
                        actual, expected,
                        "workers={workers},batch={batch_size},slice={slice:?}: {sql}"
                    );
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
    }
}

#[test]
fn empty_sources_and_denied_or_cancelled_attempts_release_all_owners() {
    for rows in [0, 280] {
        let fixture = Fixture::new(rows, 14, false);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    test_pipeline_partitions: Some(4),
                    test_pipeline_slice_bytes: Some(64 << 10),
                    ..Options::default()
                };
                let prepared =
                    fixture.prepared("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", &options);
                let budget = Budget::new(options.memory_bytes);
                if rows == 0 {
                    let output = execute(&prepared, &options, budget.clone()).unwrap();
                    assert_eq!(output.batch.num_rows(), 0);
                    drop(output);
                }
                let tiny = Budget::new(128);
                assert!(execute(&prepared, &options, tiny.clone()).is_err());
                assert_eq!(tiny.used(), 0);
                assert_eq!(tiny.active_workers(), 0);
                options.cancellation.cancel();
                assert!(
                    execute(&prepared, &options, budget.clone())
                        .err()
                        .unwrap()
                        .contains("cancel")
                );
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn caller_producer_quantum_stops_after_one_unchanged_batch() {
    let fixture = Fixture::new(28, 14, false);
    let options = Options {
        batch_size: 7,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let producer = test_producer(&fixture, &options, &budget);
    let ProducerStep::Continue(producer) = producer.step_quantum(true, &mut |_| {}).unwrap() else {
        panic!("caller must yield after one batch")
    };
    assert_eq!(producer.metrics.rows_read, 7);
    assert_eq!(producer.metrics.pipeline_source_batches, 1);
    assert_eq!(producer.metrics.pipeline_step_jobs, 1);
    assert!(producer.cursor.is_some());
    options.cancellation.cancel();
    assert!(
        producer
            .step_quantum(true, &mut |_| {})
            .err()
            .unwrap()
            .contains("cancel")
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn reservation_denial_after_autonomous_yield_drops_live_scan_and_aggregate() {
    let fixture = Fixture::new(28, 14, false);
    let options = Options {
        batch_size: 7,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let producer = test_producer(&fixture, &options, &budget);
    let attempt = producer.attempt.clone();
    let mut runtime = Runtime::new(&budget, 1, 1, options.cancellation.clone()).unwrap();
    let mut active = [None];
    submit_producer(&mut runtime, &mut active, 0, producer).unwrap();
    assert!(runtime.help_one());
    assert!(runtime.try_completion().is_none());
    assert!(budget.used() > 0);
    budget.test_refuse_reservations();
    let done = runtime.next_completion().unwrap();
    assert_eq!(Some(done.id), active[0]);
    let error = done.result.err().unwrap();
    assert!(error.starts_with("resource-limit: accounted memory "));
    assert_eq!(attempt.diagnostics(), (Some(error), None));
    assert!(runtime.next_completion().is_none());
    runtime.shutdown_for_retry().unwrap();
    assert!(options.cancellation.check().is_ok());
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}
