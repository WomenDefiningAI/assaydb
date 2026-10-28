//! Accounted engine buffers/state, not an OS or third-party decoder memory cap.
use crate::Result;
use std::sync::{
    Arc, Condvar, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
mod worker;
pub(crate) use worker::Joined;
pub(crate) const THREAD_MEMORY_BYTES: usize = 512 * 1024;
#[derive(Debug)]
struct State {
    used: usize,
    peak: usize,
    limit: usize,
    workers: usize,
    peak_workers: usize,
    thread_slots: usize,
    live_threads: usize,
    peak_live_threads: usize,
    source_dense_bytes: usize,
    decoder_refusals: usize,
    parallel_transform_refusals: usize,
}
#[derive(Clone, Debug)]
pub struct Budget(Arc<Shared>);
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    worker_ready: Condvar,
    metadata: Mutex<std::collections::HashMap<usize, MetadataLease>>,
    #[cfg(test)]
    helper_exit: Mutex<Option<Arc<worker::testing::ExitHook>>>,
}
#[derive(Debug)]
struct MetadataLease {
    // A weak owner prevents the allocation address being reused while its
    // registry entry is live, without retaining the footer's heap buffers.
    owner: Weak<parquet::file::metadata::ParquetMetaData>,
    credit: Weak<Reservation>,
}
#[derive(Debug)]
pub struct Reservation {
    budget: Budget,
    bytes: usize,
}
impl Reservation {
    pub(crate) fn belongs_to(&self, budget: &Budget) -> bool {
        Arc::ptr_eq(&self.budget.0, &budget.0)
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn budget(&self) -> Budget {
        self.budget.clone()
    }

    /// Move prepaid credit without changing the budget's live or peak total.
    pub(crate) fn split_off(&mut self, bytes: usize) -> Result<Self> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or("internal: insufficient capacity credit")?;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }

    pub(crate) fn absorb(&mut self, mut other: Self) {
        assert!(other.belongs_to(&self.budget));
        self.bytes += other.bytes;
        other.bytes = 0;
    }

    /// Extend an optional allocation's lease before allocating. Refusal leaves
    /// both the lease and the shared accounting unchanged.
    pub(crate) fn try_grow(&mut self, bytes: usize) -> bool {
        let mut state = self.budget.0.state.lock().unwrap();
        let Some(used) = state.used.checked_add(bytes) else {
            return false;
        };
        if used > state.limit {
            return false;
        }
        state.used = used;
        state.peak = state.peak.max(used);
        self.bytes += bytes;
        true
    }

    /// Release temporary workspace after it has been freed. This never grows
    /// a reservation; callers still reserve the construction peak up front.
    pub(crate) fn release_excess(&mut self, retained_bytes: usize) {
        if retained_bytes < self.bytes {
            self.budget.0.state.lock().unwrap().used -= self.bytes - retained_bytes;
            self.bytes = retained_bytes;
        }
    }
}
pub(crate) struct Workers {
    budget: Budget,
    pub count: usize,
    thread_slots: bool,
}
impl Workers {
    /// Transfer admission to the join owner; this guard retains runnable work.
    fn take_thread_slots(&mut self) -> ScanReaders {
        assert!(self.thread_slots);
        self.thread_slots = false;
        ScanReaders {
            budget: self.budget.clone(),
            count: self.count,
        }
    }
}
/// Capacity for admitted threads, independent of runnable work permits.
pub(crate) struct ScanReaders {
    budget: Budget,
    pub count: usize,
}
impl Drop for ScanReaders {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().thread_slots -= self.count;
    }
}
/// Instrument an admitted thread from before spawn until after join.
pub(crate) struct LiveThread {
    budget: Budget,
    _memory: Reservation,
}
impl Drop for LiveThread {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().live_threads -= 1;
    }
}
/// A query-wide optional dense-index allowance, separate from byte funding.
pub(crate) struct SourceDenseAllowance {
    budget: Budget,
    pub bytes: usize,
}
impl Drop for SourceDenseAllowance {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().source_dense_bytes -= self.bytes;
    }
}
impl Budget {
    pub(crate) fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new(limit: usize) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                used: 0,
                peak: 0,
                limit,
                workers: 0,
                peak_workers: 0,
                thread_slots: 0,
                live_threads: 0,
                peak_live_threads: 0,
                source_dense_bytes: 0,
                decoder_refusals: 0,
                parallel_transform_refusals: 0,
            }),
            worker_ready: Condvar::new(),
            metadata: Mutex::new(std::collections::HashMap::new()),
            #[cfg(test)]
            helper_exit: Mutex::new(None),
        }))
    }
    pub fn reserve(&self, bytes: usize) -> Result<Reservation> {
        let mut s = self.0.state.lock().unwrap();
        let used = s
            .used
            .checked_add(bytes)
            .ok_or("memory accounting overflow")?;
        if used > s.limit {
            return Err(format!(
                "resource-limit: accounted memory {used} exceeds budget {}; requested {bytes}, live {}",
                s.limit, s.used
            ).into());
        }
        s.used = used;
        s.peak = s.peak.max(used);
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
    /// One charge per shared metadata allocation in this query domain. Owners
    /// retain both the metadata and this lease; the registry owns neither.
    pub(crate) fn metadata(
        &self,
        metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
    ) -> Result<Arc<Reservation>> {
        let key = Arc::as_ptr(metadata) as usize;
        let mut shared = self.0.metadata.lock().unwrap();
        if let Some(lease) = shared.get(&key).and_then(|entry| entry.credit.upgrade()) {
            return Ok(lease);
        }
        shared
            .retain(|_, entry| entry.owner.strong_count() != 0 && entry.credit.strong_count() != 0);
        // memory_size traverses every row group. Measure only on first charge,
        // not once again for every reader of this same immutable allocation.
        let lease = Arc::new(self.reserve(metadata.memory_size())?);
        shared.insert(
            key,
            MetadataLease {
                owner: Arc::downgrade(metadata),
                credit: Arc::downgrade(&lease),
            },
        );
        Ok(lease)
    }
    pub fn peak(&self) -> usize {
        self.0.state.lock().unwrap().peak
    }
    pub fn used(&self) -> usize {
        self.0.state.lock().unwrap().used
    }
    pub fn limit(&self) -> usize {
        self.0.state.lock().unwrap().limit
    }
    pub(crate) fn record_decoder_refusal(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.decoder_refusals = state.decoder_refusals.saturating_add(1);
    }
    pub(crate) fn decoder_refusals(&self) -> usize {
        self.0.state.lock().unwrap().decoder_refusals
    }
    pub(crate) fn record_parallel_transform_refusal(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.parallel_transform_refusals = state.parallel_transform_refusals.saturating_add(1);
    }
    pub(crate) fn parallel_transform_refusals(&self) -> usize {
        self.0.state.lock().unwrap().parallel_transform_refusals
    }
    pub(crate) fn source_dense_allowance(&self, cap: usize) -> SourceDenseAllowance {
        let mut state = self.0.state.lock().unwrap();
        let bytes = cap.saturating_sub(state.source_dense_bytes);
        state.source_dense_bytes += bytes;
        SourceDenseAllowance {
            budget: self.clone(),
            bytes,
        }
    }
    /// Withdraw capacity without releasing live leases. Subsequent real
    /// reservations fail, including a baseline replay using this same domain;
    /// a forced-spill retry receives a fresh, unaffected Budget.
    #[cfg(test)]
    pub(crate) fn test_refuse_reservations(&self) {
        self.0.state.lock().unwrap().limit = 0;
    }
    pub(crate) fn workers(&self, wanted: usize, limit: usize) -> Workers {
        let mut s = self.0.state.lock().unwrap();
        // `limit` excludes the coordinator. Blocked scan lanes return runnable
        // permits, but retain thread slots until shutdown has joined them.
        // Downstream work without a free thread slot runs on its caller.
        let count = wanted.min(limit.saturating_sub(s.thread_slots));
        s.thread_slots += count;
        s.workers += count;
        s.peak_workers = s.peak_workers.max(s.workers);
        Workers {
            budget: self.clone(),
            count,
            thread_slots: true,
        }
    }
    pub(crate) fn scan_readers(&self, wanted: usize, limit: usize) -> ScanReaders {
        let mut s = self.0.state.lock().unwrap();
        let count = wanted.min(limit.saturating_sub(s.thread_slots));
        s.thread_slots += count;
        ScanReaders {
            budget: self.clone(),
            count,
        }
    }
    /// Called only with ready decode work; no permit is held during this wait.
    pub(crate) fn scan_worker(
        &self,
        limit: usize,
        stop: &AtomicBool,
        cancel: &crate::spill::Cancellation,
    ) -> Result<Option<Workers>> {
        let mut s = self.0.state.lock().unwrap();
        loop {
            cancel.check()?;
            if stop.load(Ordering::Acquire) {
                return Ok(None);
            }
            if s.workers < limit {
                s.workers += 1;
                s.peak_workers = s.peak_workers.max(s.workers);
                return Ok(Some(Workers {
                    budget: self.clone(),
                    count: 1,
                    thread_slots: false,
                }));
            }
            s = self
                .0
                .worker_ready
                .wait_timeout(s, Duration::from_millis(20))
                .unwrap()
                .0;
        }
    }
    pub(crate) fn active_workers(&self) -> usize {
        self.0.state.lock().unwrap().workers
    }
    pub fn peak_workers(&self) -> usize {
        self.0.state.lock().unwrap().peak_workers
    }
    /// Called by the parent before spawning, after thread admission. Keep the
    /// guard until join returns, then drop it before releasing the thread slot.
    pub(crate) fn thread_started(&self) -> Result<LiveThread> {
        // SF1 scan measurements: approximately 512 KiB resident per helper,
        // including stack and thread-local allocator state. This is an estimate,
        // not an OS stack/RSS limit. Keep it funded through the parent join.
        let memory = self.reserve(THREAD_MEMORY_BYTES)?;
        Ok(self.thread_started_with(memory))
    }
    /// Optional pools prepay all helpers before transferring any operator state.
    pub(crate) fn thread_started_with(&self, memory: Reservation) -> LiveThread {
        assert!(memory.belongs_to(self));
        assert_eq!(memory.bytes(), THREAD_MEMORY_BYTES);
        let mut s = self.0.state.lock().unwrap();
        s.live_threads += 1;
        debug_assert!(s.live_threads <= s.thread_slots);
        s.peak_live_threads = s.peak_live_threads.max(s.live_threads);
        LiveThread {
            budget: self.clone(),
            _memory: memory,
        }
    }
    /// Spawn-to-join execution-thread peak, including the query's coordinator.
    pub fn peak_threads(&self) -> usize {
        self.0.state.lock().unwrap().peak_live_threads + 1
    }
    #[cfg(test)]
    pub(crate) fn live_threads(&self) -> usize {
        self.0.state.lock().unwrap().live_threads + 1
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        let mut s = self.budget.0.state.lock().unwrap();
        s.workers -= self.count;
        if self.thread_slots {
            s.thread_slots -= self.count;
        }
        drop(s);
        if self.count != 0 {
            self.budget.0.worker_ready.notify_all();
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().used -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn idle_scan_readers_release_permits_without_losing_upstream_progress() {
        let budget = Budget::new(2 << 20);
        let readers = budget.scan_readers(3, 3);
        assert_eq!(readers.count, 3);
        assert_eq!(budget.active_workers(), 0);
        let downstream = budget.workers(3, 3);
        assert_eq!(downstream.count, 0);
        let stop = AtomicBool::new(false);
        let cancel = crate::spill::Cancellation::default();
        let decode = budget.scan_worker(3, &stop, &cancel).unwrap().unwrap();
        assert_eq!(budget.active_workers(), 1);
        drop(decode);
        assert_eq!(budget.active_workers(), 0);
        assert_eq!(budget.workers(1, 3).count, 0);
        assert_eq!(budget.scan_readers(3, 3).count, 0);
        stop.store(true, Ordering::Release);
        assert!(budget.scan_worker(3, &stop, &cancel).unwrap().is_none());
        cancel.cancel();
        assert!(budget.scan_worker(3, &stop, &cancel).is_err());
        drop(downstream);
        drop(readers);
        assert_eq!(budget.workers(3, 3).count, 3);
        assert_eq!(budget.active_workers(), 0);
    }

    #[test]
    fn source_dense_allowance_is_shared_until_the_last_owner_releases_it() {
        let budget = Budget::new(1 << 20);
        let source = budget.source_dense_allowance(32 << 20);
        let downstream = budget.clone().source_dense_allowance(32 << 20);
        assert_eq!(source.bytes, 32 << 20);
        assert_eq!(downstream.bytes, 0);
        // An allowance is not byte credit: actual arrays still need funding.
        assert!(budget.reserve(2 << 20).is_err());
        assert_eq!(budget.used(), 0);
        drop(source);
        let next = budget.source_dense_allowance(32 << 20);
        assert_eq!(next.bytes, 32 << 20);
        drop(downstream);
        assert_eq!(budget.source_dense_allowance(32 << 20).bytes, 0);
        drop(next);
        assert_eq!(budget.source_dense_allowance(32 << 20).bytes, 32 << 20);
    }

    #[test]
    fn completed_workspace_releases_excess_but_retains_the_output_charge() {
        let budget = Budget::new(1024);
        let mut result = budget.reserve(1024).unwrap();
        assert!(budget.reserve(1).is_err());
        result.release_excess(128);
        assert_eq!(budget.used(), 128);
        assert_eq!(budget.peak(), 1024);
        let downstream = budget.reserve(896).unwrap();
        result.release_excess(2048); // This operation cannot grow the lease.
        assert_eq!(budget.used(), 1024);
        drop(result);
        assert_eq!(budget.used(), 896);
        drop(downstream);
        assert_eq!(budget.used(), 0);
    }
}
