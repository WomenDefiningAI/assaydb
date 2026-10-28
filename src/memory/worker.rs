//! Parent-owned thread lifetimes. Explicit joins and unwinding release thread
//! admission only after the helper has exited, including its destructors.
use super::{LiveThread, ScanReaders, Workers};
use std::thread::{self, JoinHandle, Scope, ScopedJoinHandle};

pub(crate) trait Join {
    type Output;
    fn join(self) -> thread::Result<Self::Output>;
}
impl<T> Join for JoinHandle<T> {
    type Output = T;
    fn join(self) -> thread::Result<T> {
        self.join()
    }
}
impl<T> Join for ScopedJoinHandle<'_, T> {
    type Output = T;
    fn join(self) -> thread::Result<T> {
        self.join()
    }
}

pub(crate) struct Joined<H: Join> {
    handle: Option<H>,
    // Field order matters: decrement the counter before returning admission.
    _live: LiveThread,
    _slots: Option<ScanReaders>,
}
impl<H: Join> Joined<H> {
    pub(crate) fn new(handle: H, live: LiveThread) -> Self {
        Self {
            handle: Some(handle),
            _live: live,
            _slots: None,
        }
    }
    pub(crate) fn join(mut self) -> thread::Result<H::Output> {
        self.handle.take().unwrap().join()
    }
}
impl<H: Join> Drop for Joined<H> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            // Cleanup must not replace an earlier error or an unwinding panic.
            let _ = handle.join();
        }
    }
}
impl<'scope, T: Send + 'scope> Joined<ScopedJoinHandle<'scope, T>> {
    pub(crate) fn spawn_scoped<'env, F>(
        scope: &'scope Scope<'scope, 'env>,
        name: &str,
        permit: Workers,
        run: F,
    ) -> std::io::Result<Self>
    where
        F: FnOnce() -> T + Send + 'scope,
    {
        let memory = permit
            .budget
            .reserve(super::THREAD_MEMORY_BYTES)
            .map_err(std::io::Error::other)?;
        Self::spawn_scoped_funded(scope, name, permit, memory, run)
    }
    pub(crate) fn spawn_scoped_funded<'env, F>(
        scope: &'scope Scope<'scope, 'env>,
        name: &str,
        mut permit: Workers,
        memory: super::Reservation,
        run: F,
    ) -> std::io::Result<Self>
    where
        F: FnOnce() -> T + Send + 'scope,
    {
        let slots = permit.take_thread_slots();
        let live = permit.budget.thread_started_with(memory);
        #[cfg(test)]
        let hook = permit.budget.0.helper_exit.lock().unwrap().clone();
        #[cfg(test)]
        let worker_hook = hook.clone();
        let handle = thread::Builder::new()
            .stack_size(crate::engine_stack::WORKER_STACK_BYTES)
            .name(name.into())
            .spawn_scoped(scope, move || {
                // The test parks during late teardown, after runnable work has
                // been released but while this OS thread still exists.
                #[cfg(test)]
                let _exit = worker_hook.map(testing::OnExit);
                let _permit = permit;
                run()
            })?;
        let joined = Self {
            handle: Some(handle),
            _live: live,
            _slots: Some(slots),
        };
        #[cfg(test)]
        if let Some(hook) = hook {
            hook.wait_for_exit();
        }
        Ok(joined)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::super::Budget;
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::Duration;

    #[derive(Debug)]
    pub(crate) struct ExitHook {
        arrived: mpsc::Sender<()>,
        arrivals: Mutex<mpsc::Receiver<()>>,
        released: Mutex<bool>,
        ready: Condvar,
    }
    pub(super) struct OnExit(pub Arc<ExitHook>);
    impl Drop for OnExit {
        fn drop(&mut self) {
            self.0.arrived.send(()).unwrap();
            let (released, _) = self
                .0
                .ready
                .wait_timeout_while(
                    self.0.released.lock().unwrap(),
                    Duration::from_secs(10),
                    |released| !*released,
                )
                .unwrap();
            // A bounded escape prevents a failed regression from hanging the
            // test runner. Successful tests release by handshake, never time.
            assert!(*released, "test did not release helper exit");
        }
    }
    impl ExitHook {
        pub(super) fn wait_for_exit(&self) {
            self.arrivals
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("helper did not reach late teardown");
        }
    }
    pub(crate) struct ReleaseExits(Arc<ExitHook>);
    impl Drop for ReleaseExits {
        fn drop(&mut self) {
            *self.0.released.lock().unwrap() = true;
            self.0.ready.notify_all();
        }
    }
    impl Budget {
        pub(crate) fn hold_helper_exits(&self) -> ReleaseExits {
            let (arrived, arrivals) = mpsc::channel();
            let hook = Arc::new(ExitHook {
                arrived,
                arrivals: Mutex::new(arrivals),
                released: Mutex::new(false),
                ready: Condvar::new(),
            });
            assert!(
                self.0
                    .helper_exit
                    .lock()
                    .unwrap()
                    .replace(hook.clone())
                    .is_none()
            );
            ReleaseExits(hook)
        }
    }

    #[test]
    fn scoped_spawn_panic_returns_admission_and_live_count() {
        let budget = Budget::new(1 << 20);
        let result = std::panic::catch_unwind(|| {
            std::thread::scope(|scope| {
                // A NUL in a thread name deterministically rejects construction
                // before an OS thread can be launched, without exhausting host
                // resources or invoking a platform-dependent stack-size limit.
                let _ = super::Joined::spawn_scoped(
                    scope,
                    "invalid\0thread",
                    budget.workers(1, 1),
                    || unreachable!("rejected spawn must not run"),
                );
            });
        });
        assert!(result.is_err());
        assert_eq!(budget.active_workers(), 0);
        assert_eq!(budget.live_threads(), 1);
        assert_eq!(budget.workers(1, 1).count, 1);
        assert_eq!(budget.used(), 0);
    }
}
