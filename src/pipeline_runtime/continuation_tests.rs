use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

#[test]
fn worker_requeues_without_coordinator_or_additional_credit() {
    let budget = Budget::new(16 << 20);
    let mut runtime = Runtime::new(&budget, 2, 1, Cancellation::default()).unwrap();
    let mut payload = Some(budget.reserve(4096).unwrap());
    let (started, wait_started) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let (finished, wait_finished) = mpsc::channel();
    let mut steps = 0;
    let id = runtime
        .try_submit_continuation(JobKind::Morsel, move |cancel, background| {
            assert!(background);
            cancel.check()?;
            steps += 1;
            if steps == 1 {
                started.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            if steps == 64 {
                finished.send(()).unwrap();
                Ok(Some((steps, payload.take().unwrap())))
            } else {
                Ok(None)
            }
        })
        .ok()
        .unwrap();
    wait_started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        runtime
            .try_submit(JobKind::Morsel, |_| unreachable!())
            .is_err()
    );
    let full = budget.reserve(budget.limit() - budget.used()).unwrap();
    release.send(()).unwrap();
    // All 64 steps finish without any caller completion drain or new reservation.
    wait_finished.recv_timeout(Duration::from_secs(5)).unwrap();
    let done = runtime.next_completion().unwrap();
    assert_eq!(done.id, id);
    let (steps, payload) = done.result.unwrap();
    assert_eq!(steps, 64);
    assert_eq!(
        runtime.statistics(),
        Statistics {
            submitted: [64, 0, 0],
            caller_started: [0; 3],
            background_started: [64, 0, 0],
            completed: [64, 0, 0],
            peak_outstanding: 1,
            peak_queued: 1,
        }
    );
    assert!(runtime.next_completion().is_none());
    drop(payload);
    drop(full);
    drop(runtime);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn caller_yields_and_drains_ready_receipts_before_more_work() {
    let budget = Budget::new(16 << 20);
    let mut runtime = Runtime::new(&budget, 1, 2, Cancellation::default()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let id = runtime
        .try_submit_continuation(JobKind::Morsel, move |_, background| {
            assert!(!background);
            let call = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((call == 3).then_some(call))
        })
        .ok()
        .unwrap();
    let other = runtime
        .try_submit(JobKind::Partition, |_| Ok(17))
        .ok()
        .unwrap();
    assert!(runtime.help_one()); // One caller quantum, then requeue behind Partition.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(runtime.help_one()); // Leave Partition's receipt ready.
    assert!(!runtime.help(true)); // Atomically prefer the receipt to another quantum.
    let done = runtime.next_completion().unwrap();
    assert_eq!((done.id, done.result.unwrap()), (other, 17));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let done = runtime.next_completion().unwrap();
    assert_eq!((done.id, done.result.unwrap()), (id, 3));
    assert_eq!(runtime.statistics().caller_started, [3, 1, 0]);
    drop(runtime);
    assert_eq!(budget.used(), 0);
}

struct Discarded(mpsc::Sender<()>);
impl Drop for Discarded {
    fn drop(&mut self) {
        self.0.send(()).unwrap();
    }
}

#[test]
fn retry_joins_running_continuation_without_requeue_or_cancellation() {
    let budget = Budget::new(16 << 20);
    let cancel = Cancellation::default();
    let mut runtime = Runtime::<usize>::new(&budget, 2, 2, cancel.clone()).unwrap();
    let credit = budget.reserve(4096).unwrap();
    let (started, wait_started) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    assert!(
        runtime
            .try_submit_continuation(JobKind::Morsel, move |_, _| {
                let _retain = &credit;
                started.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(None)
            })
            .is_ok()
    );
    wait_started.recv_timeout(Duration::from_secs(5)).unwrap();
    let (discarded, wait_discarded) = mpsc::channel();
    let owner = Discarded(discarded);
    assert!(
        runtime
            .try_submit(JobKind::Partition, move |_| {
                drop(owner);
                panic!("queued job must be discarded")
            })
            .is_ok()
    );
    let handle = thread::spawn(move || runtime.shutdown_for_retry());
    wait_discarded.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(budget.used() >= 4096);
    release.send(()).unwrap();
    let stats = handle.join().unwrap().unwrap();
    assert_eq!(stats.submitted, [1, 1, 0]);
    assert_eq!(stats.completed, [1, 0, 0]);
    assert!(cancel.check().is_ok());
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn cancellation_and_panic_after_yield_release_the_retained_owner() {
    for panic in [false, true] {
        let budget = Budget::new(16 << 20);
        let cancel = Cancellation::default();
        let mut runtime = Runtime::<usize>::new(&budget, 1, 1, cancel.clone()).unwrap();
        let credit = budget.reserve(4096).unwrap();
        let mut first = true;
        assert!(
            runtime
                .try_submit_continuation(JobKind::Morsel, move |cancel, _| {
                    let _retain = &credit;
                    if std::mem::take(&mut first) {
                        return Ok(None);
                    }
                    assert!(!panic, "injected continuation panic");
                    cancel.check()?;
                    Ok(Some(0))
                })
                .is_ok()
        );
        assert!(runtime.help_one());
        if !panic {
            cancel.cancel();
        }
        let error = runtime.next_completion().unwrap().result.unwrap_err();
        assert!(error.contains(if panic { "panicked" } else { "cancelled" }));
        assert!(runtime.shutdown_for_retry().is_err());
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}
