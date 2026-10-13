//! Scoped batch workers with one active input and one queued output per lane.
//! Delivery stays in input-batch order, including downstream and child errors.
use crate::{
    Result,
    memory::{Budget, Reservation, Workers},
    spill::Cancellation,
};
use arrow::record_batch::RecordBatch;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::{self, ScopedJoinHandle},
    time::{Duration, Instant},
};

pub(in crate::rel) struct Configuration {
    permits: Workers,
    _metadata: Reservation,
    target_bytes: usize,
}
impl Configuration {
    pub(in crate::rel) fn with_permits(budget: &Budget, permits: Workers) -> Option<Self> {
        if permits.count == 0 {
            return None;
        }
        let metadata = budget.reserve(permits.count.checked_mul(4096)?).ok()?;
        let target_bytes =
            (budget.limit().saturating_sub(budget.used()) / (2 * permits.count)).max(1);
        Some(Self {
            permits,
            _metadata: metadata,
            target_bytes,
        })
    }
}
pub(in crate::rel) trait Report: Default + Send {
    fn merge(&mut self, other: Self);
}
#[derive(Default)]
pub(in crate::rel) struct Statistics<R: Report> {
    pub report: R,
    pub tasks: usize,
    pub batches: usize,
    pub pending_peak: usize,
    pub pending_bytes_peak: usize,
    pub seconds: f64,
}
#[derive(Default)]
struct Counters {
    lanes: Vec<(usize, usize)>,
    pending: usize,
    bytes: usize,
    peak: usize,
    bytes_peak: usize,
}
#[derive(Default)]
struct Credit {
    counts: Mutex<Counters>,
    changed: Condvar,
}
impl Credit {
    fn wait(
        &self,
        lane: usize,
        target: usize,
        stop: &AtomicBool,
        cancel: &Cancellation,
    ) -> Result<()> {
        let mut counts = self.counts.lock().unwrap();
        while counts.lanes[lane].0 > 0 && counts.lanes[lane].1 >= target {
            check_stop(stop, cancel)?;
            counts = self
                .changed
                .wait_timeout(counts, Duration::from_millis(20))
                .unwrap()
                .0;
        }
        check_stop(stop, cancel)
    }
}
fn check_stop(stop: &AtomicBool, cancel: &Cancellation) -> Result<()> {
    if stop.load(Ordering::Acquire) {
        return Err("batch pipeline stopped".into());
    }
    cancel.check()
}
struct Pending {
    credit: Arc<Credit>,
    lane: usize,
    bytes: usize,
}
impl Pending {
    fn new(credit: &Arc<Credit>, lane: usize, bytes: usize) -> Self {
        let mut c = credit.counts.lock().unwrap();
        c.lanes[lane].0 += 1;
        c.lanes[lane].1 += bytes;
        c.pending += 1;
        c.bytes += bytes;
        c.peak = c.peak.max(c.pending);
        c.bytes_peak = c.bytes_peak.max(c.bytes);
        Self {
            credit: credit.clone(),
            lane,
            bytes,
        }
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        let mut c = self.credit.counts.lock().unwrap();
        c.lanes[self.lane].0 -= 1;
        c.lanes[self.lane].1 -= self.bytes;
        c.pending -= 1;
        c.bytes -= self.bytes;
        drop(c);
        self.credit.changed.notify_all();
    }
}
struct Task {
    batch: RecordBatch,
    _lease: Reservation,
}
struct Output {
    batch: RecordBatch,
    _lease: Reservation,
    _pending: Pending,
}
enum Message<R: Report> {
    Batch(Output),
    End(Result<R>),
}
struct Pool<'scope, R: Report> {
    senders: Vec<SyncSender<Task>>,
    receivers: Vec<Receiver<Message<R>>>,
    handles: Vec<ScopedJoinHandle<'scope, ()>>,
    pending: VecDeque<usize>,
    next_lane: usize,
    stop: Arc<AtomicBool>,
    credit: Arc<Credit>,
    cancel: Cancellation,
    budget: Budget,
    first_error: Option<String>,
    stats: Statistics<R>,
}
impl<R: Report> Pool<'_, R> {
    fn drain_one(&mut self, emit: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        let lane = *self.pending.front().expect("pending batch task");
        let result: Result<()> = (|| {
            loop {
                self.cancel.check()?;
                match self.receivers[lane].recv_timeout(Duration::from_millis(20)) {
                    Ok(Message::Batch(output)) => {
                        let Output {
                            batch,
                            _lease,
                            _pending,
                        } = output;
                        self.stats.batches += 1;
                        // Keep the queue lease alive throughout downstream delivery.
                        emit(batch)?;
                    }
                    Ok(Message::End(result)) => {
                        self.pending.pop_front();
                        self.stats.report.merge(result?);
                        return Ok(());
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err("batch worker stopped before task completion".into());
                    }
                }
            }
        })();
        if let Err(error) = &result {
            self.first_error.get_or_insert_with(|| error.clone());
        }
        result
    }
    fn push(
        &mut self,
        batch: RecordBatch,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        if let Some(error) = &self.first_error {
            return Err(error.clone());
        }
        if self.pending.len() == self.senders.len() {
            self.drain_one(emit)?;
        }
        self.cancel.check()?;
        let lease = self
            .budget
            .reserve(batch.get_array_memory_size().saturating_add(256))?;
        let lane = self.next_lane;
        self.senders[lane]
            .send(Task {
                batch,
                _lease: lease,
            })
            .map_err(|_| "batch worker stopped before task submission")?;
        self.pending.push_back(lane);
        self.next_lane = (lane + 1) % self.senders.len();
        self.stats.tasks += 1;
        Ok(())
    }
    fn drain_all(&mut self, emit: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        if let Some(error) = &self.first_error {
            return Err(error.clone());
        }
        while !self.pending.is_empty() {
            self.drain_one(emit)?;
        }
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        // Disconnect blocked output sends before waiting for any worker.
        self.receivers.clear();
        self.senders.clear();
        self.credit.changed.notify_all();
        let mut panicked = false;
        for handle in self.handles.drain(..) {
            panicked |= handle.join().is_err();
        }
        if panicked {
            Err("batch worker panicked".into())
        } else {
            Ok(())
        }
    }
}
impl<R: Report> Drop for Pool<'_, R> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(in crate::rel) fn run<R: Report, Process, Produce>(
    config: Configuration,
    budget: &Budget,
    cancel: &Cancellation,
    process: Process,
    produce: Produce,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Statistics<R>>
where
    Process: Fn(
            RecordBatch,
            &mut dyn FnMut(RecordBatch) -> Result<()>,
            &dyn Fn() -> Result<()>,
        ) -> Result<R>
        + Sync,
    Produce: FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
{
    let start = Instant::now();
    thread::scope(|scope| {
        let mut pool = Pool {
            senders: Vec::new(),
            receivers: Vec::new(),
            handles: Vec::new(),
            pending: VecDeque::new(),
            next_lane: 0,
            stop: Arc::new(AtomicBool::new(false)),
            credit: Arc::new(Credit {
                counts: Mutex::new(Counters {
                    lanes: vec![(0, 0); config.permits.count],
                    ..Counters::default()
                }),
                changed: Condvar::new(),
            }),
            cancel: cancel.clone(),
            budget: budget.clone(),
            first_error: None,
            stats: Statistics::default(),
        };
        for lane in 0..config.permits.count {
            let (send, receive) = mpsc::sync_channel::<Task>(1);
            let (output, results) = mpsc::sync_channel::<Message<R>>(1);
            let stop = pool.stop.clone();
            let credit = pool.credit.clone();
            let budget = budget.clone();
            let cancel = cancel.clone();
            let process = &process;
            let target = config.target_bytes;
            let handle = thread::Builder::new()
                .name(format!("assaydb-batch-{lane}"))
                .spawn_scoped(scope, move || {
                    while let Ok(Task { batch, _lease }) = receive.recv() {
                        let check = || credit.wait(lane, target, &stop, &cancel);
                        let result = check().and_then(|_| {
                            process(
                                batch,
                                &mut |batch| {
                                    check_stop(&stop, &cancel)?;
                                    let bytes = batch.get_array_memory_size();
                                    let lease = budget.reserve(bytes.saturating_add(256))?;
                                    let pending = Pending::new(&credit, lane, bytes);
                                    output
                                        .send(Message::Batch(Output {
                                            batch,
                                            _lease: lease,
                                            _pending: pending,
                                        }))
                                        .map_err(|_| "batch output receiver stopped".into())
                                },
                                &check,
                            )
                        });
                        drop(_lease);
                        if output.send(Message::End(result)).is_err()
                            || stop.load(Ordering::Acquire)
                        {
                            break;
                        }
                    }
                })
                .map_err(|e| format!("batch worker spawn: {e}"))?;
            pool.senders.push(send);
            pool.receivers.push(results);
            pool.handles.push(handle);
        }
        let source_result = produce(&mut |batch| pool.push(batch, emit));
        // A later child/preparation error cannot leap over earlier task output.
        // An already observed callback/worker error stops delivery immediately.
        let result = pool.drain_all(emit).and(source_result);
        let joined = pool.close();
        result?;
        joined?;
        {
            let c = pool.credit.counts.lock().unwrap();
            assert_eq!((c.pending, c.bytes), (0, 0));
            pool.stats.pending_peak = c.peak;
            pool.stats.pending_bytes_peak = c.bytes_peak;
        }
        pool.stats.seconds = start.elapsed().as_secs_f64();
        Ok(std::mem::take(&mut pool.stats))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execute::Options, rel::probe::Statistics as ProbeStatistics};
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    fn batch(n: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![n]))],
        )
        .unwrap()
    }
    fn number(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
    fn config(budget: &Budget) -> Configuration {
        Configuration::new(
            &Options {
                workers: 6,
                batch_size: 2,
                ..Options::default()
            },
            budget,
            1000,
        )
        .unwrap()
    }
    fn clean(budget: &Budget) {
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.workers(5, 5).count, 5);
    }

    #[test]
    fn full_later_lane_keeps_task_order_and_bounded_output() {
        for target in [1, usize::MAX] {
            let budget = Budget::new(8 << 20);
            let mut config = config(&budget);
            config.target_bytes = target;
            let ready = (Mutex::new(false), Condvar::new());
            let mut actual = Vec::new();
            let stats = run(
                config,
                &budget,
                &Cancellation::default(),
                |input, send, check| {
                    let task = number(&input);
                    if task == 0 {
                        let (guard, timeout) = ready
                            .1
                            .wait_timeout_while(
                                ready.0.lock().unwrap(),
                                Duration::from_secs(5),
                                |v| !*v,
                            )
                            .unwrap();
                        assert!(
                            !timeout.timed_out() && *guard,
                            "later lane must enqueue while first waits"
                        );
                    }
                    for output in 0..100 {
                        check()?;
                        send(batch(task * 100 + output))?;
                        if task == 1 && output == 0 {
                            *ready.0.lock().unwrap() = true;
                            ready.1.notify_all();
                        }
                    }
                    Ok(ProbeStatistics::default())
                },
                |send| {
                    for task in 0..3 {
                        send(batch(task))?;
                    }
                    Ok(())
                },
                &mut |output| {
                    actual.push(number(&output));
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(actual, (0..300).collect::<Vec<_>>());
            assert_eq!((stats.tasks, stats.batches), (3, 300));
            assert!(stats.pending_peak <= 5);
            assert!(stats.pending_bytes_peak > 0);
            clean(&budget);
        }
    }

    #[test]
    fn earlier_worker_and_callback_errors_precede_later_child_errors() {
        for callback_fails in [false, true] {
            let budget = Budget::new(8 << 20);
            let cancel = Cancellation::default();
            let mut actual = Vec::new();
            let error = run(
                config(&budget),
                &budget,
                &cancel,
                |input, send, check| -> Result<ProbeStatistics> {
                    let task = number(&input);
                    for i in 0..100 {
                        check()?;
                        send(batch(task * 100 + i))?;
                        if task == 0 {
                            return Err("earlier worker".into());
                        }
                    }
                    Err("later worker".into())
                },
                |send| {
                    send(batch(0))?;
                    send(batch(1))?;
                    Err("child".into())
                },
                &mut |output| {
                    actual.push(number(&output));
                    if callback_fails {
                        Err("downstream".into())
                    } else {
                        Ok(())
                    }
                },
            )
            .err()
            .unwrap();
            assert_eq!(
                error,
                if callback_fails {
                    "downstream"
                } else {
                    "earlier worker"
                }
            );
            assert_eq!(actual, vec![0]);
            assert!(cancel.check().is_ok());
            clean(&budget);
        }
    }

    #[test]
    fn source_error_drains_prior_successes_and_empty_tasks() {
        let budget = Budget::new(8 << 20);
        let mut actual = Vec::new();
        let error = run(
            config(&budget),
            &budget,
            &Cancellation::default(),
            |input, send, _| {
                if number(&input) != 1 {
                    send(input)?;
                }
                Ok(ProbeStatistics::default())
            },
            |send| {
                for i in 0..3 {
                    send(batch(i))?;
                }
                Err("child".into())
            },
            &mut |output| {
                actual.push(number(&output));
                Ok(())
            },
        )
        .err()
        .unwrap();
        assert_eq!(error, "child");
        assert_eq!(actual, vec![0, 2]);
        clean(&budget);
    }

    #[test]
    fn cancellation_wakes_credit_waiters_and_releases_every_lease() {
        let budget = Budget::new(8 << 20);
        let cancel = Cancellation::default();
        let mut config = config(&budget);
        config.target_bytes = 1;
        let ready = (Mutex::new(0), Condvar::new());
        let error = run(
            config,
            &budget,
            &cancel,
            |_, send, check| {
                send(batch(0))?;
                {
                    let mut count = ready.0.lock().unwrap();
                    *count += 1;
                    ready.1.notify_all();
                }
                for i in 1..100 {
                    check()?;
                    send(batch(i))?;
                }
                Ok(ProbeStatistics::default())
            },
            |send| {
                send(batch(0))?;
                send(batch(1))?;
                let (guard, timeout) = ready
                    .1
                    .wait_timeout_while(ready.0.lock().unwrap(), Duration::from_secs(5), |n| *n < 2)
                    .unwrap();
                assert!(!timeout.timed_out() && *guard == 2);
                cancel.cancel();
                Ok(())
            },
            &mut |_| panic!("cancelled outputs must not be delivered"),
        )
        .err()
        .unwrap();
        assert!(error.contains("cancelled"), "{error}");
        clean(&budget);
    }

    #[test]
    fn worker_panic_disconnects_full_later_lane() {
        let budget = Budget::new(8 << 20);
        let error = run(
            config(&budget),
            &budget,
            &Cancellation::default(),
            |input, send, check| {
                if number(&input) == 0 {
                    panic!("deliberate worker panic");
                }
                for i in 0..100 {
                    check()?;
                    send(batch(i))?;
                }
                Ok(ProbeStatistics::default())
            },
            |send| {
                send(batch(0))?;
                send(batch(1))
            },
            &mut |_| Ok(()),
        )
        .err()
        .unwrap();
        assert!(
            error.contains("worker stopped") || error.contains("worker panicked"),
            "{error}"
        );
        clean(&budget);
    }

    #[test]
    fn output_reservation_failure_releases_input_and_worker_permits() {
        let budget = Budget::new(8 << 20);
        let error = run(
            config(&budget),
            &budget,
            &Cancellation::default(),
            |_, send, _| {
                let _all = budget.reserve(budget.limit() - budget.used())?;
                send(batch(0))?;
                Ok(ProbeStatistics::default())
            },
            |send| send(batch(0)),
            &mut |_| panic!("unreserved output"),
        )
        .err()
        .unwrap();
        assert!(error.contains("resource-limit"), "{error}");
        clean(&budget);
    }

    #[test]
    fn unavailable_permits_or_metadata_fall_back_without_leaks() {
        let options = Options {
            workers: 6,
            batch_size: 2,
            ..Options::default()
        };
        let budget = Budget::new(8 << 20);
        let occupied = budget.workers(5, 5);
        assert!(Configuration::new(&options, &budget, 1000).is_none());
        drop(occupied);
        assert!(Configuration::new(&options, &budget, 1).is_none());
        clean(&budget);
        let tiny = Budget::new(1);
        assert!(Configuration::new(&options, &tiny, 1000).is_none());
        clean(&tiny);
    }
}
