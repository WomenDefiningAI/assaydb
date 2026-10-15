//! Query-lifetime scheduler for funded, exclusively owned jobs.
//! Payload buffers retain their operator-owned leases; this layer prepays queue,
//! completion, closure, and thread metadata. It never interprets operator errors.
#[cfg(test)]
mod concurrent_tests;
#[cfg(test)]
mod continuation_tests;
#[cfg(test)]
mod retry_tests;
use crate::{
    Result,
    memory::{Budget, Reservation, Workers},
    spill::Cancellation,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobKind {
    Morsel,
    Partition,
    #[cfg(test)]
    Finalize,
}
impl JobKind {
    fn index(self) -> usize {
        match self {
            Self::Morsel => 0,
            Self::Partition => 1,
            #[cfg(test)]
            Self::Finalize => 2,
        }
    }
}
/// Counts actual task starts, not requested workers or the thread returning a
/// producer's final result. Array order is Morsel, Partition, Finalize. These
/// counts do not estimate CPU time or a query's serial wall-time fraction.
/// Each continuation dispatch counts separately, even while retaining one JobId
/// and one outstanding slot until its final completion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Statistics {
    pub submitted: [usize; 3],
    pub caller_started: [usize; 3],
    pub background_started: [usize; 3],
    pub completed: [usize; 3],
    pub peak_outstanding: usize,
    pub peak_queued: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JobId(pub u64);
pub(crate) struct Completion<T> {
    pub id: JobId,
    pub kind: JobKind,
    pub result: Result<T>,
}
/// Rejected submission returns the closure, preserving any owned operator state.
pub(crate) struct Rejected<F> {
    pub reason: &'static str,
    // Production drops a rejected continuation for clean candidate shutdown;
    // bounded scheduler tests also exercise taking and resubmitting it. The
    // field must retain ownership until the Rejected value is destroyed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub work: F,
}
type Work<T> = Box<dyn FnOnce(&Cancellation) -> Result<T> + Send>;
type Continuation<T> = Box<dyn FnMut(&Cancellation, bool) -> Result<Option<T>> + Send>;
enum Task<T> {
    Once(Option<Work<T>>),
    Continuation(Continuation<T>),
}
struct Queued<T> {
    id: JobId,
    kind: JobKind,
    work: Task<T>,
    // Keep funding alive until the closure/captures have been destroyed.
    _funding: Reservation,
}
enum Slot<T> {
    Empty,
    Queued(Queued<T>),
    Running,
    Ready(Completion<T>),
}
struct State<T> {
    slots: Vec<Slot<T>>,
    queue: VecDeque<usize>,
    ready: VecDeque<usize>,
    stopping: bool,
    outstanding: usize,
    statistics: Statistics,
    // Sticky independently of ready/consumed/discarded completions. A retry
    // must never turn an infrastructure panic into successful baseline replay.
    panicked: bool,
}
struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
    cancel: Cancellation,
}
pub(crate) struct Runtime<T: Send + 'static> {
    shared: Arc<Shared<T>>,
    handles: Vec<crate::memory::Joined<JoinHandle<()>>>,
    budget: Budget,
    next_id: u64,
    // Explicitly taken only after all worker handles have joined.
    permits: Option<Workers>,
    _metadata: Reservation,
    shutdown_finished: bool,
}

impl<T: Send + 'static> Runtime<T> {
    pub fn new(
        budget: &Budget,
        workers: usize,
        slots: usize,
        cancel: Cancellation,
    ) -> Result<Self> {
        if !(1..=64).contains(&workers) || slots == 0 {
            return Err("invalid pipeline worker/slot count".into());
        }
        let permits = budget.workers(workers - 1, workers - 1);
        let metadata = slots
            .checked_mul(std::mem::size_of::<Slot<T>>() + 2 * std::mem::size_of::<usize>() + 128)
            .and_then(|n| n.checked_add((permits.count + 1) * 4096))
            .ok_or("pipeline metadata overflow")?;
        let metadata = budget.reserve(metadata)?;
        let mut runtime = Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    slots: std::iter::repeat_with(|| Slot::Empty).take(slots).collect(),
                    queue: VecDeque::with_capacity(slots),
                    ready: VecDeque::with_capacity(slots),
                    stopping: false,
                    outstanding: 0,
                    statistics: Statistics::default(),
                    panicked: false,
                }),
                changed: Condvar::new(),
                cancel,
            }),
            handles: Vec::with_capacity(permits.count),
            budget: budget.clone(),
            next_id: 0,
            permits: Some(permits),
            _metadata: metadata,
            shutdown_finished: false,
        };
        for lane in 0..runtime.permits.as_ref().unwrap().count {
            let shared = runtime.shared.clone();
            let live = budget.thread_started();
            runtime.handles.push(crate::memory::Joined::new(
                thread::Builder::new()
                    .name(format!("assaydb-pipeline-{lane}"))
                    .spawn(move || {
                        loop {
                            let job = {
                                let mut state = shared.state.lock().unwrap();
                                loop {
                                    if state.stopping {
                                        return;
                                    }
                                    if let Some(job) = claim(&mut state, true) {
                                        break job;
                                    }
                                    state = shared.changed.wait(state).unwrap();
                                }
                            };
                            run(&shared, job, true);
                        }
                    })
                    .map_err(|e| format!("pipeline worker spawn: {e}"))?,
                live,
            ));
        }
        Ok(runtime)
    }

    /// Does not block or run user code. A full queue returns the untouched owner.
    /// Capture and output allocations must already carry their operator funding.
    pub fn try_submit<F>(
        &mut self,
        kind: JobKind,
        work: F,
    ) -> std::result::Result<JobId, Rejected<F>>
    where
        F: FnOnce(&Cancellation) -> Result<T> + Send + 'static,
    {
        self.try_submit_owned(kind, work, |work| Task::Once(Some(Box::new(work))))
    }

    /// `None` yields the exclusively owned continuation back to this same slot.
    /// Its closure allocation, funding and job identity survive every dispatch;
    /// only a final value or error is delivered to the coordinator. The boolean
    /// identifies background execution so caller work can use a smaller quantum.
    pub fn try_submit_continuation<F>(
        &mut self,
        kind: JobKind,
        work: F,
    ) -> std::result::Result<JobId, Rejected<F>>
    where
        F: FnMut(&Cancellation, bool) -> Result<Option<T>> + Send + 'static,
    {
        self.try_submit_owned(kind, work, |work| Task::Continuation(Box::new(work)))
    }

    fn try_submit_owned<F>(
        &mut self,
        kind: JobKind,
        work: F,
        boxed: impl FnOnce(F) -> Task<T>,
    ) -> std::result::Result<JobId, Rejected<F>> {
        let mut state = self.shared.state.lock().unwrap();
        if state.stopping {
            return Err(Rejected {
                reason: "pipeline stopped",
                work,
            });
        }
        let Some(index) = state.slots.iter().position(|s| matches!(s, Slot::Empty)) else {
            return Err(Rejected {
                reason: "pipeline completion slots full",
                work,
            });
        };
        let Some(next) = self.next_id.checked_add(1) else {
            return Err(Rejected {
                reason: "pipeline job ID exhausted",
                work,
            });
        };
        // F is still on the caller's stack. Reserve its box before allocation.
        let Ok(funding) = self.budget.reserve(std::mem::size_of::<F>()) else {
            return Err(Rejected {
                reason: "pipeline closure memory limit",
                work,
            });
        };
        let id = JobId(self.next_id);
        self.next_id = next;
        state.slots[index] = Slot::Queued(Queued {
            id,
            kind,
            work: boxed(work),
            _funding: funding,
        });
        state.queue.push_back(index);
        state.outstanding += 1;
        state.statistics.submitted[kind.index()] += 1;
        state.statistics.peak_outstanding =
            state.statistics.peak_outstanding.max(state.outstanding);
        state.statistics.peak_queued = state.statistics.peak_queued.max(state.queue.len());
        drop(state);
        self.shared.changed.notify_all();
        Ok(id)
    }

    /// The single caller is the already-counted final execution participant.
    #[cfg(test)]
    pub fn help_one(&mut self) -> bool {
        self.help(false)
    }

    fn help(&mut self, yield_to_ready: bool) -> bool {
        let job = {
            let mut state = self.shared.state.lock().unwrap();
            if state.stopping || (yield_to_ready && !state.ready.is_empty()) {
                None
            } else {
                claim(&mut state, false)
            }
        };
        if let Some(job) = job {
            run(&self.shared, job, false);
            true
        } else {
            false
        }
    }

    pub fn try_completion(&mut self) -> Option<Completion<T>> {
        let mut state = self.shared.state.lock().unwrap();
        let index = state.ready.pop_front()?;
        let Slot::Ready(completion) = std::mem::replace(&mut state.slots[index], Slot::Empty)
        else {
            unreachable!("ready slot")
        };
        state.outstanding -= 1;
        Some(completion)
    }

    /// Completion order is deliberately not SQL error or group encounter order.
    /// Operator receipts carry that ordering. Cancellation is passed to jobs;
    /// it does not discard lower-index jobs after a higher-index operator error.
    pub fn next_completion(&mut self) -> Option<Completion<T>> {
        loop {
            if let Some(done) = self.try_completion() {
                return Some(done);
            }
            if self.shared.state.lock().unwrap().outstanding == 0 {
                return None;
            }
            // Check ready receipts atomically with claiming caller work. A
            // continuation yields after its caller quantum before we help again.
            if self.help(true) {
                continue;
            }
            let state = self.shared.state.lock().unwrap();
            if state.ready.is_empty() && state.queue.is_empty() && state.outstanding > 0 {
                drop(
                    self.shared
                        .changed
                        .wait_timeout(state, Duration::from_millis(20))
                        .unwrap(),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn background_workers(&self) -> usize {
        self.handles.len()
    }
    pub fn statistics(&self) -> Statistics {
        self.shared.state.lock().unwrap().statistics.clone()
    }

    #[cfg(test)]
    pub fn test_wait_ready(&self, count: usize) {
        let (state, _) = self
            .shared
            .changed
            .wait_timeout_while(
                self.shared.state.lock().unwrap(),
                Duration::from_secs(10),
                |state| state.ready.len() < count,
            )
            .unwrap();
        let ready = state.ready.len();
        drop(state);
        assert_eq!(ready, count, "background completion deadline");
    }

    /// Abandon this candidate attempt without cancelling the caller's token.
    /// Unstarted jobs and unaccepted results are discarded; already-running
    /// bounded jobs finish before their permits and backing can be released.
    /// This consumes the runtime, so no continuation can be resubmitted.
    ///
    /// Operator errors are not classified here. The caller must already have
    /// selected a retryable failure, discard its external operator owners, and
    /// verify its budget is restored before replay. Panics and caller cancellation
    /// are terminal. A successful return never resets or replaces cancellation.
    #[allow(dead_code)] // Candidate fallback integration follows this stage.
    pub fn shutdown_for_retry(mut self) -> Result<Statistics> {
        let join_failed = self.stop_and_join(false);
        let state = self.shared.state.lock().unwrap();
        if join_failed || state.panicked {
            return Err("pipeline job or worker panicked; retry forbidden".into());
        }
        if state.outstanding != 0 {
            return Err("pipeline retry shutdown retained unfinished slots".into());
        }
        self.shared.cancel.check()?;
        Ok(state.statistics.clone())
    }

    /// `abort` is exclusively ordinary Drop's policy. Retry shutdown never
    /// writes the application-owned cancellation flag, including on failure.
    fn stop_and_join(&mut self, abort: bool) -> bool {
        if self.shutdown_finished {
            return false;
        }
        let mut state = self.shared.state.lock().unwrap();
        state.stopping = true;
        if abort && state.outstanding > 0 {
            self.shared.cancel.cancel();
        }
        state.queue.clear();
        state.ready.clear();
        let slots = state.slots.len();
        drop(state);
        for index in 0..slots {
            let discard = {
                let mut state = self.shared.state.lock().unwrap();
                if matches!(state.slots[index], Slot::Running | Slot::Empty) {
                    None
                } else {
                    state.outstanding -= 1;
                    Some(std::mem::replace(&mut state.slots[index], Slot::Empty))
                }
            };
            // Operator abandonment guards and leases never run under our mutex.
            if drop_panicked(discard) {
                self.shared.state.lock().unwrap().panicked = true;
            }
        }
        self.shared.changed.notify_all();
        let mut join_failed = false;
        for handle in self.handles.drain(..) {
            join_failed |= handle.join().is_err();
        }
        drop(self.permits.take());
        self.shutdown_finished = true;
        join_failed
    }
}
fn claim<T>(state: &mut State<T>, background: bool) -> Option<(usize, Queued<T>)> {
    let index = state.queue.pop_front()?;
    let Slot::Queued(job) = std::mem::replace(&mut state.slots[index], Slot::Running) else {
        unreachable!("queued slot")
    };
    if background {
        state.statistics.background_started[job.kind.index()] += 1;
    } else {
        state.statistics.caller_started[job.kind.index()] += 1;
    }
    Some((index, job))
}
// A caught payload can itself panic on destruction. Contain that destruction
// once, then abandon any nested payload: recursively dropping arbitrary panic
// payloads cannot guarantee either termination or a no-unwind boundary.
fn dispose_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    if let Err(nested) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(nested);
    }
}
// Destruction is a separate unwind boundary: a continuation may retain a
// panicking capture even after its invocation has already unwound.
fn drop_panicked<T>(value: T) -> bool {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        Ok(()) => false,
        Err(payload) => {
            dispose_panic_payload(payload);
            true
        }
    }
}
fn run<T>(shared: &Shared<T>, (index, job): (usize, Queued<T>), background: bool) {
    let Queued {
        id,
        kind,
        mut work,
        _funding,
    } = job;
    // A job panic is a completion, never a lost worker or an unfilled slot.
    let (mut result, mut panicked) =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match &mut work {
            Task::Once(work) => work.take().expect("unstarted job")(&shared.cancel).map(Some),
            Task::Continuation(work) => work(&shared.cancel, background),
        })) {
            Ok(result) => (result, false),
            Err(payload) => {
                dispose_panic_payload(payload);
                (Err("pipeline job panicked".into()), true)
            }
        };
    if matches!(result, Ok(None)) {
        let mut state = shared.state.lock().unwrap();
        state.statistics.completed[kind.index()] += 1;
        if state.stopping {
            state.slots[index] = Slot::Empty;
            state.outstanding -= 1;
            drop(state);
            // Destroy captured operator owners before releasing closure funding,
            // and never run their destructors while holding the scheduler lock.
            if drop_panicked(work) {
                shared.state.lock().unwrap().panicked = true;
            }
            drop(_funding);
        } else {
            state.slots[index] = Slot::Queued(Queued {
                id,
                kind,
                work,
                _funding,
            });
            state.queue.push_back(index);
            state.statistics.submitted[kind.index()] += 1;
            state.statistics.peak_queued = state.statistics.peak_queued.max(state.queue.len());
            drop(state);
        }
        shared.changed.notify_all();
        return;
    }
    if drop_panicked(work) {
        // Discard a successful payload under containment too, before replacing
        // it with the infrastructure error. Funding still covers the captures.
        drop_panicked(result);
        result = Err("pipeline job panicked".into());
        panicked = true;
    }
    drop(_funding);
    let result = result.map(|value| value.expect("completed job"));
    let completion = Completion { id, kind, result };
    let mut state = shared.state.lock().unwrap();
    state.panicked |= panicked;
    state.statistics.completed[kind.index()] += 1;
    if state.stopping {
        state.slots[index] = Slot::Empty;
        state.outstanding -= 1;
        drop(state);
        if drop_panicked(completion) {
            shared.state.lock().unwrap().panicked = true;
        }
    } else {
        state.slots[index] = Slot::Ready(completion);
        state.ready.push_back(index);
        drop(state);
    }
    shared.changed.notify_all();
}
impl<T: Send + 'static> Drop for Runtime<T> {
    fn drop(&mut self) {
        // Abort any abandoned query work, then destroy queued owners/results
        // outside locks. Operator Drop guards observe abandoned receipts.
        self.stop_and_join(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    struct Owner(Arc<AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn submit<T: Send + 'static, F>(r: &mut Runtime<T>, kind: JobKind, f: F) -> JobId
    where
        F: FnOnce(&Cancellation) -> Result<T> + Send + 'static,
    {
        match r.try_submit(kind, f) {
            Ok(id) => id,
            Err(e) => panic!("{}", e.reason),
        }
    }
    #[test]
    fn caller_progress_bounded_slots_and_returned_owners() {
        let budget = Budget::new(1 << 20);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut runtime = Runtime::new(&budget, 1, 1, Cancellation::default()).unwrap();
        assert_eq!(runtime.background_workers(), 0);
        let id = submit(&mut runtime, JobKind::Morsel, |_| Ok(7));
        let owner = Owner(drops.clone());
        let rejected = runtime
            .try_submit(JobKind::Partition, move |_| {
                drop(owner);
                Ok(9)
            })
            .err()
            .unwrap();
        assert_eq!(rejected.reason, "pipeline completion slots full");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(runtime.help_one());
        // A completed but unconsumed slot is still occupied and fully funded.
        assert!(runtime.try_submit(JobKind::Morsel, |_| Ok(10)).is_err());
        let done = runtime.next_completion().unwrap();
        assert_eq!(
            (done.id, done.kind, done.result.unwrap()),
            (id, JobKind::Morsel, 7)
        );
        submit(&mut runtime, JobKind::Partition, rejected.work);
        assert_eq!(runtime.next_completion().unwrap().result.unwrap(), 9);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(runtime.next_completion().is_none());
        assert_eq!(
            runtime.statistics(),
            Statistics {
                submitted: [1, 1, 0],
                caller_started: [1, 1, 0],
                background_started: [0; 3],
                completed: [1, 1, 0],
                peak_outstanding: 1,
                peak_queued: 1,
            }
        );
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn completion_order_is_not_error_order_and_workers_survive_source_eof() {
        let budget = Budget::new(1 << 20);
        let mut runtime = Runtime::new(&budget, 3, 2, Cancellation::default()).unwrap();
        let (started, wait_start) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let first = submit(&mut runtime, JobKind::Morsel, move |_| {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Err("earlier input error".into())
        });
        wait_start.recv_timeout(Duration::from_secs(5)).unwrap();
        let later = submit(&mut runtime, JobKind::Partition, |_| Ok(2));
        let done = runtime.next_completion().unwrap();
        assert_eq!(done.id, later);
        release.send(()).unwrap();
        let done = runtime.next_completion().unwrap();
        assert_eq!(done.id, first);
        assert_eq!(done.result.err().unwrap(), "earlier input error");
        // End of one source is not runtime shutdown. A replay/finalize phase
        // still executes on background workers, without caller helping.
        let (sent, received) = mpsc::channel();
        submit(&mut runtime, JobKind::Finalize, move |_| {
            sent.send(thread::current().name().unwrap_or("").to_string())
                .unwrap();
            Ok(3)
        });
        assert!(
            received
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("assaydb-pipeline-")
        );
        assert_eq!(runtime.next_completion().unwrap().result.unwrap(), 3);
        assert_eq!(budget.peak_workers(), 2);
        let statistics = runtime.statistics();
        assert_eq!(statistics.submitted, [1, 1, 1]);
        assert_eq!(statistics.completed, statistics.submitted);
        assert_eq!(statistics.background_started[0], 1);
        assert_eq!(statistics.background_started[2], 1);
        for kind in 0..3 {
            assert_eq!(
                statistics.caller_started[kind] + statistics.background_started[kind],
                statistics.completed[kind]
            );
        }
        assert_eq!(statistics.peak_outstanding, 2);
        assert!(statistics.peak_queued <= 2);
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn panic_and_cancellation_complete_without_discarding_other_jobs() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let mut runtime = Runtime::<usize>::new(&budget, 2, 3, cancel.clone()).unwrap();
        submit(&mut runtime, JobKind::Morsel, |_| {
            panic!("injected job panic")
        });
        let called = Arc::new(AtomicUsize::new(0));
        let c = called.clone();
        cancel.cancel();
        submit(&mut runtime, JobKind::Partition, move |cancel| {
            c.fetch_add(1, Ordering::SeqCst);
            cancel.check()?;
            Ok(2)
        });
        let mut errors = Vec::new();
        while let Some(done) = runtime.next_completion() {
            errors.push(done.result.err().unwrap());
        }
        assert_eq!(called.load(Ordering::SeqCst), 1);
        assert!(errors.iter().any(|s| s == "pipeline job panicked"));
        assert!(errors.iter().any(|s| s.contains("cancelled")));
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn drop_joins_running_jobs_before_releasing_permits_and_owned_payloads() {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        let mut runtime = Runtime::new(&budget, 2, 3, cancel.clone()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let (started, wait_start) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let owner = Owner(drops.clone());
        submit(&mut runtime, JobKind::Morsel, move |_| {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Ok(owner)
        });
        wait_start.recv_timeout(Duration::from_secs(5)).unwrap();
        let owner = Owner(drops.clone());
        submit(&mut runtime, JobKind::Partition, move |_| Ok(owner));
        let (dropped, wait_drop) = mpsc::channel();
        let handle = thread::spawn(move || {
            drop(runtime);
            dropped.send(()).unwrap();
        });
        assert!(wait_drop.recv_timeout(Duration::from_millis(30)).is_err());
        assert!(cancel.check().is_err());
        let competing = budget.workers(1, 1);
        assert_eq!(competing.count, 0);
        drop(competing);
        release.send(()).unwrap();
        wait_drop.recv_timeout(Duration::from_secs(5)).unwrap();
        handle.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        let available = budget.workers(1, 1);
        assert_eq!(available.count, 1);
        drop(available);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn completed_payload_keeps_its_operator_funding_until_acceptance_or_drop() {
        let budget = Budget::new(1 << 20);
        let mut runtime = Runtime::new(&budget, 2, 1, Cancellation::default()).unwrap();
        let baseline = budget.used();
        let credit = budget.reserve(4096).unwrap();
        let (sent, received) = mpsc::channel();
        submit(&mut runtime, JobKind::Partition, move |_| {
            sent.send(()).unwrap();
            Ok(credit)
        });
        received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(budget.used() >= baseline + 4096);
        let done = runtime.next_completion().unwrap();
        assert!(budget.used() >= baseline + 4096);
        drop(done);
        assert_eq!(budget.used(), baseline);
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn exact_once_under_caller_stealing_and_saturated_completion_slots() {
        let budget = Budget::new(1 << 20);
        let mut runtime = Runtime::new(&budget, 4, 3, Cancellation::default()).unwrap();
        let counts = Arc::new((0..128).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let mut completed = Vec::new();
        for index in 0..128 {
            let counts = counts.clone();
            let mut work = move |_: &Cancellation| {
                counts[index].fetch_add(1, Ordering::SeqCst);
                Ok(index)
            };
            loop {
                match runtime.try_submit(JobKind::Morsel, work) {
                    Ok(_) => break,
                    Err(rejected) => {
                        assert_eq!(rejected.reason, "pipeline completion slots full");
                        work = rejected.work;
                        completed.push(runtime.next_completion().unwrap().result.unwrap());
                    }
                }
            }
        }
        while let Some(done) = runtime.next_completion() {
            completed.push(done.result.unwrap());
        }
        completed.sort_unstable();
        assert_eq!(completed, (0..128).collect::<Vec<_>>());
        assert!(counts.iter().all(|n| n.load(Ordering::SeqCst) == 1));
        assert!(budget.peak_workers() <= 3);
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn failed_metadata_and_capture_admission_leave_owners_and_permits_intact() {
        let tiny = Budget::new(128);
        assert!(Runtime::<usize>::new(&tiny, 4, 2, Cancellation::default()).is_err());
        assert_eq!(tiny.used(), 0);
        let available = tiny.workers(3, 3);
        assert_eq!(available.count, 3);
        drop(available);
        let budget = Budget::new(10_000);
        let mut runtime = Runtime::new(&budget, 1, 1, Cancellation::default()).unwrap();
        let reserve = budget.reserve(budget.limit() - budget.used()).unwrap();
        let capture = [1u8; 1024];
        let rejected = runtime
            .try_submit(JobKind::Morsel, move |_| Ok(capture[0]))
            .err()
            .unwrap();
        assert_eq!(rejected.reason, "pipeline closure memory limit");
        drop(reserve);
        submit(&mut runtime, JobKind::Morsel, rejected.work);
        assert_eq!(runtime.next_completion().unwrap().result.unwrap(), 1);
        drop(runtime);
        assert_eq!(budget.used(), 0);
    }
}
