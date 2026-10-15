use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

const DEADLINE: Duration = Duration::from_secs(10);

fn restored(budget: &Budget, workers: usize) {
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
    let permits = budget.workers(workers - 1, workers - 1);
    assert_eq!(permits.count, workers - 1);
    drop(permits);
    assert_eq!(budget.active_workers(), 0);
}

struct PanicOnDrop {
    drops: Arc<AtomicUsize>,
    _credit: Reservation,
}
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("injected capture destructor panic");
    }
}

// No owned allocations: containment deliberately forgets this nested payload.
struct NestedPanicPayload;
impl Drop for NestedPanicPayload {
    fn drop(&mut self) {
        panic!("nested panic payload must never be dropped");
    }
}

struct PanickingPayload {
    drops: Arc<AtomicUsize>,
    _credit: Reservation,
}
impl Drop for PanickingPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(NestedPanicPayload);
    }
}

struct PayloadPanicOnDrop {
    drops: Arc<AtomicUsize>,
    payload: Option<PanickingPayload>,
    _credit: Reservation,
}
impl Drop for PayloadPanicOnDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(payload) = self.payload.take() {
            std::panic::panic_any(payload);
        }
    }
}

#[test]
fn panicking_payload_destructors_publish_one_receipt_and_forbid_retry() {
    for workers in [1, 2, 4, 10] {
        // Independently cover the invocation and capture-destruction catches,
        // then both in one dispatch, including a previously yielded owner.
        for (invocation_panics, capture_panics) in [(true, false), (false, true), (true, true)] {
            for terminal_step in [1, 4] {
                let budget = Budget::new(1 << 20);
                let cancel = Cancellation::default();
                let mut runtime =
                    Runtime::<usize>::new(&budget, workers, 1, cancel.clone()).unwrap();
                let metadata_bytes = budget.used();
                let drops = Arc::new(AtomicUsize::new(0));
                let payload_drops = Arc::new(AtomicUsize::new(0));
                let payload = || PanickingPayload {
                    drops: payload_drops.clone(),
                    _credit: budget.reserve(4096).unwrap(),
                };
                let mut invocation_payload = invocation_panics.then(payload);
                let owner = PayloadPanicOnDrop {
                    drops: drops.clone(),
                    payload: capture_panics.then(payload),
                    _credit: budget.reserve(4096).unwrap(),
                };
                let mut step = 0;
                let id = runtime
                    .try_submit_continuation(JobKind::Morsel, move |_, background| {
                        let _retain = &owner;
                        assert_eq!(background, workers > 1);
                        step += 1;
                        if step < terminal_step {
                            return Ok(None);
                        }
                        if let Some(payload) = invocation_payload.take() {
                            std::panic::panic_any(payload);
                        }
                        Ok(Some(7))
                    })
                    .ok()
                    .unwrap();
                let observed_budget = budget.clone();
                let (send, receive) = mpsc::channel();
                let handle = thread::spawn(move || {
                    // Do not let the caller steal background regression cases.
                    if workers > 1 {
                        runtime.test_wait_ready(1);
                    }
                    let done = runtime.next_completion().unwrap();
                    assert_eq!(done.id, id);
                    assert_eq!(done.kind, JobKind::Morsel);
                    assert_eq!(done.result.unwrap_err(), "pipeline job panicked");
                    assert!(runtime.next_completion().is_none());
                    {
                        let state = runtime.shared.state.lock().unwrap();
                        assert!(state.panicked);
                        assert_eq!(state.outstanding, 0);
                        assert!(state.queue.is_empty());
                        assert!(state.ready.is_empty());
                        assert!(state.slots.iter().all(|slot| matches!(slot, Slot::Empty)));
                    }
                    let stats = runtime.statistics();
                    assert_eq!(stats.submitted, [terminal_step, 0, 0]);
                    assert_eq!(stats.completed, stats.submitted);
                    assert_eq!(
                        stats.caller_started[0],
                        if workers == 1 { terminal_step } else { 0 }
                    );
                    assert_eq!(
                        stats.background_started[0],
                        if workers > 1 { terminal_step } else { 0 }
                    );
                    assert_eq!(observed_budget.used(), metadata_bytes);
                    assert_eq!(
                        runtime.shutdown_for_retry().unwrap_err(),
                        "pipeline job or worker panicked; retry forbidden"
                    );
                    send.send(()).unwrap();
                });
                receive
                    .recv_timeout(DEADLINE)
                    .expect("panic payload destruction must not lose a receipt or hang shutdown");
                handle.join().unwrap();
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert_eq!(
                    payload_drops.load(Ordering::SeqCst),
                    usize::from(invocation_panics) + usize::from(capture_panics)
                );
                assert_eq!(Arc::strong_count(&drops), 1);
                assert_eq!(Arc::strong_count(&payload_drops), 1);
                assert!(cancel.check().is_ok());
                restored(&budget, workers);
            }
        }
    }
}

#[test]
fn capture_destructor_panics_publish_one_receipt_and_forbid_retry() {
    for workers in [1, 2, 4, 10] {
        for outcome in [0, 1, 2] {
            for terminal_step in [1, 4] {
                let budget = Budget::new(1 << 20);
                let cancel = Cancellation::default();
                let mut runtime =
                    Runtime::<usize>::new(&budget, workers, 1, cancel.clone()).unwrap();
                let drops = Arc::new(AtomicUsize::new(0));
                let owner = PanicOnDrop {
                    drops: drops.clone(),
                    _credit: budget.reserve(4096).unwrap(),
                };
                let mut step = 0;
                let id = runtime
                    .try_submit_continuation(JobKind::Morsel, move |_, background| {
                        let _retain = &owner;
                        assert_eq!(background, workers > 1);
                        step += 1;
                        if step < terminal_step {
                            return Ok(None);
                        }
                        match outcome {
                            0 => Ok(Some(step)),
                            1 => Err("original operator error".into()),
                            _ => panic!("injected invocation panic before destructor panic"),
                        }
                    })
                    .ok()
                    .unwrap();
                // No caller stealing: every dispatch must run on a helper.
                if workers > 1 {
                    runtime.test_wait_ready(1);
                }
                let (send, receive) = mpsc::channel();
                let handle = thread::spawn(move || {
                    let done = runtime.next_completion().unwrap();
                    assert_eq!(done.id, id);
                    assert_eq!(done.result.unwrap_err(), "pipeline job panicked");
                    assert!(runtime.next_completion().is_none());
                    let stats = runtime.statistics();
                    assert_eq!(stats.submitted, [terminal_step, 0, 0]);
                    assert_eq!(stats.completed, stats.submitted);
                    assert_eq!(stats.peak_outstanding, 1);
                    assert_eq!(stats.peak_queued, 1);
                    assert_eq!(
                        runtime.shutdown_for_retry().unwrap_err(),
                        "pipeline job or worker panicked; retry forbidden"
                    );
                    send.send(()).unwrap();
                });
                receive
                    .recv_timeout(DEADLINE)
                    .expect("panic receipt must not hang");
                handle.join().unwrap();
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert!(cancel.check().is_ok());
                restored(&budget, workers);
            }
        }
    }
}

#[test]
fn stopping_a_running_continuation_contains_its_capture_destructor() {
    for workers in [2, 4, 10] {
        for finished in [false, true] {
            let budget = Budget::new(1 << 20);
            let cancel = Cancellation::default();
            let mut runtime = Runtime::<usize>::new(&budget, workers, 1, cancel.clone()).unwrap();
            let shared = runtime.shared.clone();
            let drops = Arc::new(AtomicUsize::new(0));
            let owner = PanicOnDrop {
                drops: drops.clone(),
                _credit: budget.reserve(4096).unwrap(),
            };
            let (start, started) = mpsc::channel();
            let (release, gate) = mpsc::channel();
            assert!(
                runtime
                    .try_submit_continuation(JobKind::Morsel, move |_, background| {
                        let _retain = &owner;
                        assert!(background);
                        start.send(()).unwrap();
                        gate.recv_timeout(DEADLINE).unwrap();
                        Ok(finished.then_some(7))
                    })
                    .is_ok()
            );
            started.recv_timeout(DEADLINE).unwrap();
            let (send, receive) = mpsc::channel();
            let handle = thread::spawn(move || send.send(runtime.shutdown_for_retry()).unwrap());
            let (state, _) = shared
                .changed
                .wait_timeout_while(shared.state.lock().unwrap(), DEADLINE, |s| !s.stopping)
                .unwrap();
            assert!(state.stopping);
            drop(state);
            release.send(()).unwrap();
            assert_eq!(
                receive.recv_timeout(DEADLINE).unwrap().unwrap_err(),
                "pipeline job or worker panicked; retry forbidden"
            );
            handle.join().unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            let state = shared.state.lock().unwrap();
            assert!(state.panicked);
            assert_eq!(state.outstanding, 0);
            assert!(state.slots.iter().all(|s| matches!(s, Slot::Empty)));
            drop(state);
            drop(shared);
            assert!(cancel.check().is_ok());
            restored(&budget, workers);
        }
    }
}

#[test]
fn saturated_background_continuations_execute_each_source_step_exactly_once() {
    for workers in [2, 4, 10] {
        let slots = 2 * workers;
        let budget = Budget::new(4 << 20);
        let mut runtime =
            Runtime::<usize>::new(&budget, workers, slots, Cancellation::default()).unwrap();
        let mut seen = std::collections::HashSet::new();
        for wave in 0..3 {
            let counts = Arc::new(
                (0..slots)
                    .map(|_| (0..19).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
            );
            let mut releases = Vec::new();
            let mut ids = Vec::new();
            for source in 0..slots {
                let (release, gate) = mpsc::channel();
                releases.push(release);
                let calls = counts.clone();
                let credit = budget.reserve(4096).unwrap();
                let last = [1, 3, 19][(source + wave) % 3];
                let mut step = 0;
                ids.push(
                    runtime
                        .try_submit_continuation(JobKind::Morsel, move |_, background| {
                            let _retain = &credit;
                            assert!(background);
                            if step == 0 {
                                gate.recv_timeout(DEADLINE).unwrap();
                            }
                            assert_eq!(calls[source][step].fetch_add(1, Ordering::SeqCst), 0);
                            step += 1;
                            if step < last {
                                Ok(None)
                            } else if source % 2 == 0 {
                                Ok(Some(source))
                            } else {
                                Err(format!("source {source}, step {last}"))
                            }
                        })
                        .ok()
                        .unwrap(),
                );
            }
            assert!(
                runtime
                    .try_submit_continuation(JobKind::Morsel, |_, _| unreachable!())
                    .is_err()
            );
            for release in releases {
                release.send(()).unwrap();
            }
            runtime.test_wait_ready(slots);
            for _ in 0..slots {
                let done = runtime.next_completion().unwrap();
                assert!(seen.insert(done.id.0), "duplicate terminal receipt");
                let source = ids.iter().position(|id| *id == done.id).unwrap();
                let last = [1, 3, 19][(source + wave) % 3];
                if source % 2 == 0 {
                    assert_eq!(done.result.unwrap(), source);
                } else {
                    assert_eq!(
                        done.result.unwrap_err(),
                        format!("source {source}, step {last}")
                    );
                }
                for step in 0..19 {
                    assert_eq!(
                        counts[source][step].load(Ordering::SeqCst),
                        usize::from(step < last)
                    );
                }
            }
            assert!(runtime.next_completion().is_none());
            let stats = runtime.statistics();
            assert_eq!(stats.caller_started, [0; 3]);
            assert_eq!(stats.background_started, stats.submitted);
            assert_eq!(stats.completed, stats.submitted);
            assert_eq!(stats.peak_outstanding, slots);
            assert!(stats.peak_queued <= slots);
        }
        assert_eq!(seen.len(), 3 * slots);
        runtime.shutdown_for_retry().unwrap();
        restored(&budget, workers);
    }
}

#[test]
fn shutdown_contains_queued_capture_and_ready_output_destructors() {
    for workers in [2, 4, 10] {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let mut runtime =
            Runtime::<PanicOnDrop>::new(&budget, workers, workers + 1, cancel.clone()).unwrap();
        let shared = runtime.shared.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut output = Some(PanicOnDrop {
            drops: drops.clone(),
            _credit: budget.reserve(4096).unwrap(),
        });
        assert!(
            runtime
                .try_submit_continuation(JobKind::Morsel, move |_, background| {
                    assert!(background);
                    Ok(output.take())
                })
                .is_ok()
        );
        runtime.test_wait_ready(1);
        let (start, started) = mpsc::channel();
        let mut releases = Vec::new();
        for _ in 1..workers {
            let (release, gate) = mpsc::channel();
            releases.push(release);
            let start = start.clone();
            assert!(
                runtime
                    .try_submit_continuation(JobKind::Morsel, move |_, background| {
                        assert!(background);
                        start.send(()).unwrap();
                        gate.recv_timeout(DEADLINE).unwrap();
                        Ok(None)
                    })
                    .is_ok()
            );
        }
        for _ in 1..workers {
            started.recv_timeout(DEADLINE).unwrap();
        }
        let queued = PanicOnDrop {
            drops: drops.clone(),
            _credit: budget.reserve(4096).unwrap(),
        };
        assert!(
            runtime
                .try_submit_continuation(JobKind::Morsel, move |_, _| {
                    let _retain = &queued;
                    panic!("queued continuation must not run");
                })
                .is_ok()
        );
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || send.send(runtime.shutdown_for_retry()).unwrap());
        let (state, _) = shared
            .changed
            .wait_timeout_while(shared.state.lock().unwrap(), DEADLINE, |s| !s.stopping)
            .unwrap();
        assert!(state.stopping);
        drop(state);
        for release in releases {
            release.send(()).unwrap();
        }
        assert_eq!(
            receive.recv_timeout(DEADLINE).unwrap().unwrap_err(),
            "pipeline job or worker panicked; retry forbidden"
        );
        handle.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(shared.state.lock().unwrap().outstanding, 0);
        drop(shared);
        assert!(cancel.check().is_ok());
        restored(&budget, workers);
    }
}

#[test]
fn cancellation_with_ready_running_and_queued_continuations_releases_every_owner() {
    for workers in [2, 4, 10] {
        let slots = workers + 1;
        let budget = Budget::new(4 << 20);
        let cancel = Cancellation::default();
        let mut runtime =
            Runtime::<Reservation>::new(&budget, workers, slots, cancel.clone()).unwrap();
        let mut credit = Some(budget.reserve(4096).unwrap());
        let mut step = 0;
        let ready = runtime
            .try_submit_continuation(JobKind::Morsel, move |_, background| {
                assert!(background);
                step += 1;
                Ok(if step == 3 { credit.take() } else { None })
            })
            .ok()
            .unwrap();
        runtime.test_wait_ready(1);
        let (start, started) = mpsc::channel();
        let mut releases = Vec::new();
        let calls = Arc::new(
            (0..workers)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        for source in 0..workers {
            let (release, gate) = mpsc::channel();
            releases.push(release);
            let start = start.clone();
            let calls = calls.clone();
            let credit = budget.reserve(4096).unwrap();
            assert!(
                runtime
                    .try_submit_continuation(JobKind::Morsel, move |cancel, background| {
                        let _retain = &credit;
                        assert!(background);
                        let step = calls[source].fetch_add(1, Ordering::SeqCst);
                        if source < workers - 1 {
                            if step == 0 {
                                return Ok(None);
                            }
                            assert_eq!(step, 1);
                            start.send(()).unwrap();
                            gate.recv_timeout(DEADLINE).unwrap();
                        }
                        cancel.check()?;
                        panic!("cancellation must be observed");
                    })
                    .is_ok()
            );
            if source < workers - 1 {
                started.recv_timeout(DEADLINE).unwrap();
            }
        }
        {
            let state = runtime.shared.state.lock().unwrap();
            assert_eq!(state.ready.len(), 1);
            assert_eq!(state.queue.len(), 1);
            assert_eq!(state.outstanding, slots);
        }
        cancel.cancel();
        for release in releases.into_iter().take(workers - 1) {
            release.send(()).unwrap();
        }
        runtime.test_wait_ready(slots);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..slots {
            let done = runtime.next_completion().unwrap();
            assert!(seen.insert(done.id.0));
            if done.id == ready {
                drop(done.result.unwrap());
            } else {
                assert!(done.result.err().unwrap().starts_with("cancelled:"));
            }
        }
        assert!(runtime.next_completion().is_none());
        for (source, calls) in calls.iter().enumerate() {
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if source < workers - 1 { 2 } else { 1 }
            );
        }
        let stats = runtime.statistics();
        assert_eq!(stats.caller_started, [0; 3]);
        assert_eq!(stats.completed, stats.submitted);
        assert_eq!(stats.peak_outstanding, slots);
        assert!(stats.peak_queued <= slots);
        assert!(
            runtime
                .shutdown_for_retry()
                .unwrap_err()
                .starts_with("cancelled:")
        );
        restored(&budget, workers);
    }
}
