//! Deterministic retry shutdown checks; channels establish every handoff.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

struct Dropped(mpsc::Sender<()>);
impl Drop for Dropped {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
struct Payload {
    // The notification proves the corresponding credit has already dropped.
    _credit: Reservation,
    _dropped: Dropped,
}
fn payload(budget: &Budget, bytes: usize) -> (Payload, mpsc::Receiver<()>) {
    let (sent, received) = mpsc::channel();
    (
        Payload {
            _credit: budget.reserve(bytes).unwrap(),
            _dropped: Dropped(sent),
        },
        received,
    )
}
fn submit<T: Send + 'static>(
    runtime: &mut Runtime<T>,
    kind: JobKind,
    work: impl FnOnce(&Cancellation) -> Result<T> + Send + 'static,
) {
    if let Err(rejected) = runtime.try_submit(kind, work) {
        panic!("unexpected submission failure: {}", rejected.reason);
    }
}
fn received(channel: &mpsc::Receiver<()>) {
    channel.recv_timeout(Duration::from_secs(5)).unwrap();
}
fn restored(budget: &Budget) {
    assert_eq!(budget.used(), 0);
    let permits = budget.workers(1, 1);
    assert_eq!(permits.count, 1);
}

#[test]
fn retry_discards_queued_and_ready_owners_but_joins_active_work_without_cancel() {
    let budget = Budget::new(16 << 20);
    let cancel = Cancellation::default();
    let mut runtime = Runtime::new(&budget, 2, 3, cancel.clone()).unwrap();
    let (active, active_dropped) = payload(&budget, 16 << 10);
    let (started, wait_start) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    submit(&mut runtime, JobKind::Morsel, move |cancel| {
        started.send(()).unwrap();
        gate.recv().unwrap();
        cancel.check()?;
        Ok(active)
    });
    received(&wait_start);
    let (ready, ready_dropped) = payload(&budget, 4 << 10);
    submit(&mut runtime, JobKind::Partition, move |_| Ok(ready));
    assert!(runtime.help_one()); // sole helper remains held by the first job
    let (queued, queued_dropped) = payload(&budget, 8 << 10);
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    submit(&mut runtime, JobKind::Finalize, move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(queued)
    });
    let (finished, wait_finish) = mpsc::channel();
    let handle = thread::spawn(move || {
        let result = runtime.shutdown_for_retry();
        finished.send(result).unwrap();
    });
    received(&ready_dropped);
    received(&queued_dropped); // shutdown has stopped and discarded unstarted work
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(wait_finish.try_recv().is_err());
    assert!(active_dropped.try_recv().is_err());
    assert!(budget.used() >= 16 << 10);
    assert!(cancel.check().is_ok());
    let unavailable = budget.workers(1, 1);
    assert_eq!(unavailable.count, 0);
    drop(unavailable);
    release.send(()).unwrap();
    let statistics = wait_finish
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    handle.join().unwrap();
    received(&active_dropped);
    assert_eq!(statistics.submitted, [1, 1, 1]);
    assert_eq!(statistics.completed, [1, 1, 0]);
    assert_eq!(statistics.background_started, [1, 0, 0]);
    assert_eq!(statistics.caller_started, [0, 1, 0]);
    assert!(cancel.check().is_ok());
    restored(&budget);
}

#[test]
fn retry_preserves_caller_cancellation_arriving_during_join() {
    let budget = Budget::new(16 << 20);
    let cancel = Cancellation::default();
    let mut runtime = Runtime::new(&budget, 2, 2, cancel.clone()).unwrap();
    let (active, active_dropped) = payload(&budget, 4096);
    let (started, wait_start) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    submit(&mut runtime, JobKind::Morsel, move |cancel| {
        started.send(()).unwrap();
        gate.recv().unwrap();
        cancel.check()?;
        Ok(active)
    });
    received(&wait_start);
    let (queued, queued_dropped) = payload(&budget, 2048);
    submit(&mut runtime, JobKind::Partition, move |_| Ok(queued));
    let handle = thread::spawn(move || runtime.shutdown_for_retry());
    received(&queued_dropped);
    assert!(cancel.check().is_ok());
    cancel.cancel();
    release.send(()).unwrap();
    assert!(
        handle
            .join()
            .unwrap()
            .unwrap_err()
            .starts_with("cancelled:")
    );
    received(&active_dropped);
    assert!(cancel.check().is_err());
    restored(&budget);
}

#[test]
fn retry_surfaces_job_panic_after_ready_completion_was_discarded_or_consumed() {
    for consume in [false, true] {
        let budget = Budget::new(16 << 20);
        let cancel = Cancellation::default();
        let mut runtime = Runtime::<usize>::new(&budget, 1, 1, cancel.clone()).unwrap();
        let credit = budget.reserve(4096).unwrap();
        submit(&mut runtime, JobKind::Morsel, move |_| {
            let _credit = credit;
            panic!("injected retry panic");
        });
        assert!(runtime.help_one());
        if consume {
            assert_eq!(
                runtime.next_completion().unwrap().result.unwrap_err(),
                "pipeline job panicked"
            );
        }
        assert!(
            runtime
                .shutdown_for_retry()
                .unwrap_err()
                .contains("panicked")
        );
        assert!(cancel.check().is_ok());
        restored(&budget);
    }
}

#[test]
fn retry_surfaces_panic_in_active_job_during_shutdown_and_releases_backing() {
    let budget = Budget::new(16 << 20);
    let cancel = Cancellation::default();
    let mut runtime = Runtime::<Payload>::new(&budget, 2, 2, cancel.clone()).unwrap();
    let (active, active_dropped) = payload(&budget, 4096);
    let (started, wait_start) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    submit(&mut runtime, JobKind::Morsel, move |_| {
        let _active = active;
        started.send(()).unwrap();
        gate.recv().unwrap();
        panic!("injected active retry panic");
    });
    received(&wait_start);
    let (queued, queued_dropped) = payload(&budget, 2048);
    submit(&mut runtime, JobKind::Partition, move |_| Ok(queued));
    let handle = thread::spawn(move || runtime.shutdown_for_retry());
    received(&queued_dropped);
    assert!(active_dropped.try_recv().is_err());
    release.send(()).unwrap();
    assert!(handle.join().unwrap().unwrap_err().contains("panicked"));
    received(&active_dropped);
    assert!(cancel.check().is_ok());
    restored(&budget);
}

#[test]
fn retry_single_worker_discards_unstarted_work_and_does_not_classify_operator_errors() {
    let budget = Budget::new(16 << 20);
    let cancel = Cancellation::default();
    let mut runtime = Runtime::<usize>::new(&budget, 1, 2, cancel.clone()).unwrap();
    submit(&mut runtime, JobKind::Morsel, |_| {
        Err("operator error".into())
    });
    assert!(runtime.help_one());
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let credit = budget.reserve(4096).unwrap();
    submit(&mut runtime, JobKind::Partition, move |_| {
        let _credit = credit;
        count.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    });
    let statistics = runtime.shutdown_for_retry().unwrap();
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(statistics.submitted, [1, 1, 0]);
    assert_eq!(statistics.completed, [1, 0, 0]);
    assert!(cancel.check().is_ok());
    restored(&budget);
}
