//! Ordered tasks over already-complete input. Callers prepay descriptors and
//! output/scratch capacity before dispatch; tasks never pull an upstream node.
use crate::{
    Result,
    memory::{Budget, Joined},
};

pub(crate) fn map<T: Send>(
    count: usize,
    workers: usize,
    budget: &Budget,
    run: impl Fn(usize) -> Result<T> + Sync,
) -> Result<(Vec<T>, usize)> {
    std::thread::scope(|scope| {
        let mut tasks = Vec::with_capacity(count.saturating_sub(1));
        let mut background = 0;
        for index in 1..count {
            let permit = budget.workers(1, workers.saturating_sub(1));
            let task = if permit.count == 0 {
                None
            } else {
                let run = &run;
                // A failed spawn has not run the closure: retry on the caller.
                // The parent owns admission until join, including unwinding.
                Joined::spawn_scoped(scope, "assaydb-build-prepare", permit, move || run(index))
                    .ok()
            };
            background += usize::from(task.is_some());
            tasks.push(task);
        }
        let mut output = Vec::with_capacity(count);
        let mut error = None;
        let first = (count != 0).then(|| run(0));
        for result in
            first
                .into_iter()
                .chain(tasks.into_iter().enumerate().map(|(offset, task)| {
                    match task {
                        Some(handle) => handle
                            .join()
                            .unwrap_or_else(|_| Err("build preparation worker panicked".into())),
                        None => run(offset + 1),
                    }
                }))
        {
            match result {
                Ok(value) => output.push(value),
                Err(failure) if error.is_none() => error = Some(failure),
                Err(_) => {}
            }
        }
        // Drain every handle even on error; storage and funding outlive workers.
        match error {
            Some(error) => Err(error),
            None => Ok((output, background)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::Cancellation;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn late_helper_exits_retain_slots_through_join_and_unwind() {
        use std::{panic::AssertUnwindSafe, sync::Mutex};
        for workers in [2, 4] {
            for mode in 0..5 {
                let budget = Budget::new(1 << 20);
                let release = Mutex::new(Some(budget.hold_helper_exits()));
                let count = workers + 2;
                let credit = budget.reserve(count * 8192).unwrap();
                let cancel = Cancellation::default();
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    map(count, workers, &budget, |index| {
                        if index == 0 {
                            // Every spawned helper has finished its work and is
                            // still executing its late destructor. Release on
                            // both ordinary return and assertion/payload panic.
                            let _release = release.lock().unwrap().take().unwrap();
                            assert_eq!(budget.active_workers(), 0);
                            assert_eq!(budget.live_threads(), workers);
                            assert_eq!(budget.workers(1, workers - 1).count, 0);
                            assert!(budget.peak_threads() <= workers);
                            if mode == 3 {
                                panic!("coordinator panic");
                            }
                            if mode == 4 {
                                cancel.cancel();
                            }
                        }
                        cancel.check()?;
                        if mode == 1 && (index == 0 || index == 1) {
                            return Err(format!("failure {index}"));
                        }
                        if mode == 2 && index == 1 {
                            panic!("helper panic");
                        }
                        Ok(index)
                    })
                }));
                match mode {
                    0 => {
                        let (values, background) = result.unwrap().unwrap();
                        assert_eq!(values, (0..count).collect::<Vec<_>>());
                        assert_eq!(background, workers - 1);
                    }
                    1 => assert_eq!(result.unwrap().unwrap_err(), "failure 0"),
                    2 => assert_eq!(
                        result.unwrap().unwrap_err(),
                        "build preparation worker panicked"
                    ),
                    3 => assert!(result.is_err()),
                    4 => assert_eq!(result.unwrap().unwrap_err(), "cancelled: query execution"),
                    _ => unreachable!(),
                }
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(budget.live_threads(), 1);
                assert!(budget.peak_threads() <= workers);
                assert_eq!(budget.workers(workers - 1, workers - 1).count, workers - 1);
                drop(credit);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn ordered_errors_and_cancellation_join_every_task_and_release_permits() {
        for workers in [1, 2, 4, 10] {
            let budget = Budget::new(1 << 20);
            let completed = AtomicUsize::new(0);
            let credit = budget.reserve(8192 * 10).unwrap();
            let error = map(10, workers, &budget, |index| -> Result<usize> {
                completed.fetch_add(1, Ordering::Relaxed);
                if index == 3 {
                    Err("first canonical failure".into())
                } else if index == 7 {
                    Err("later failure".into())
                } else {
                    Ok(index)
                }
            })
            .unwrap_err();
            assert_eq!(error, "first canonical failure");
            assert_eq!(completed.load(Ordering::Relaxed), 10);
            assert_eq!(budget.active_workers(), 0);
            let cancel = Cancellation::default();
            let error = map(10, workers, &budget, |index| {
                if index == 0 {
                    cancel.cancel();
                }
                cancel.check()?;
                Ok(index)
            })
            .unwrap_err();
            assert_eq!(error, "cancelled: query execution");
            assert_eq!(budget.active_workers(), 0);
            assert!(budget.peak_workers() < workers);
            drop(credit);
            assert_eq!(budget.used(), 0);
        }
    }
}

/// Per-cancellation-token instrumentation: no global state, timing, or waits.
/// Hooks only request cancellation; production checks must return the error.
#[cfg(test)]
pub(crate) mod testing {
    use crate::{memory::Budget, spill::Cancellation};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    pub(crate) struct Hooks {
        target: Mutex<Option<&'static str>>,
        reached: AtomicUsize,
        funded: AtomicUsize,
        started: AtomicUsize,
        finished: AtomicUsize,
    }
    pub(crate) struct Task<'a>(&'a Hooks);
    impl Drop for Task<'_> {
        fn drop(&mut self) {
            self.0.finished.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Hooks {
        pub(crate) fn arm(&self, target: &'static str) {
            assert!(self.target.lock().unwrap().replace(target).is_none());
        }
        pub(crate) fn task(&self) -> Task<'_> {
            self.started.fetch_add(1, Ordering::SeqCst);
            Task(self)
        }
        pub(crate) fn checkpoint(&self, point: &str, cancel: &Cancellation, budget: &Budget) {
            if self.target.lock().unwrap().as_ref().copied() == Some(point) {
                self.reached.fetch_add(1, Ordering::SeqCst);
                self.funded.store(budget.used(), Ordering::SeqCst);
                cancel.cancel();
            }
        }
        pub(crate) fn assert_drained(&self, budget: &Budget, baseline: usize) {
            assert_eq!(self.reached.load(Ordering::SeqCst), 1);
            assert!(self.funded.load(Ordering::SeqCst) > baseline);
            assert!(self.started.load(Ordering::SeqCst) > 0);
            assert_eq!(
                self.started.load(Ordering::SeqCst),
                self.finished.load(Ordering::SeqCst)
            );
            assert_eq!(budget.active_workers(), 0);
            assert_eq!(budget.used(), baseline);
        }
    }
}
