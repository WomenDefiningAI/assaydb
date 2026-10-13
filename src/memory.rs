//! Accounted engine buffers/state, not an OS or third-party decoder memory cap.
use crate::Result;
use std::sync::{Arc, Mutex};
#[derive(Debug)]
struct State {
    used: usize,
    peak: usize,
    limit: usize,
    workers: usize,
    peak_workers: usize,
}
#[derive(Clone, Debug)]
pub struct Budget(Arc<Mutex<State>>);
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

    /// Release temporary workspace after it has been freed. This never grows
    /// a reservation; callers still reserve the construction peak up front.
    pub(crate) fn release_excess(&mut self, retained_bytes: usize) {
        if retained_bytes < self.bytes {
            self.budget.0.lock().unwrap().used -= self.bytes - retained_bytes;
            self.bytes = retained_bytes;
        }
    }
}
pub(crate) struct Workers {
    budget: Budget,
    pub count: usize,
}
impl Budget {
    pub(crate) fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            used: 0,
            peak: 0,
            limit,
            workers: 0,
            peak_workers: 0,
        })))
    }
    pub fn reserve(&self, bytes: usize) -> Result<Reservation> {
        let mut s = self.0.lock().unwrap();
        let used = s
            .used
            .checked_add(bytes)
            .ok_or("memory accounting overflow")?;
        if used > s.limit {
            return Err(format!(
                "resource-limit: accounted memory {used} exceeds budget {}; requested {bytes}, live {}",
                s.limit, s.used
            ));
        }
        s.used = used;
        s.peak = s.peak.max(used);
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
    pub fn peak(&self) -> usize {
        self.0.lock().unwrap().peak
    }
    pub fn used(&self) -> usize {
        self.0.lock().unwrap().used
    }
    pub fn limit(&self) -> usize {
        self.0.lock().unwrap().limit
    }
    pub(crate) fn workers(&self, wanted: usize, limit: usize) -> Workers {
        let mut s = self.0.lock().unwrap();
        let count = wanted.min(limit.saturating_sub(s.workers));
        s.workers += count;
        s.peak_workers = s.peak_workers.max(s.workers);
        Workers {
            budget: self.clone(),
            count,
        }
    }
    pub(crate) fn active_workers(&self) -> usize {
        self.0.lock().unwrap().workers
    }
    pub fn peak_workers(&self) -> usize {
        self.0.lock().unwrap().peak_workers
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.budget.0.lock().unwrap().workers -= self.count;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.0.lock().unwrap().used -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
