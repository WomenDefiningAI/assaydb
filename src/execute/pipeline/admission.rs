//! Conservative proof boundary for exact direct-source partial aggregation.
//!
//! Metadata validation alone is insufficient: each producer must call
//! `GroupProgress::observe` before evaluating/updating a decoded batch and
//! `finish` at EOF. Disjoint source ranges then bound the whole query's actual
//! inputs. This module retains no allocation or duplicated footer data.

use crate::Result;
#[cfg(test)]
mod physical_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SourceAdmission {
    total_rows: u64,
}

impl SourceAdmission {
    /// Conservative serial/occupied-budget entry: nonfinal groups must align.
    #[cfg(test)]
    pub(super) fn new(
        file_rows: i64,
        group_rows: impl IntoIterator<Item = i64>,
        batch_size: usize,
    ) -> Result<Option<Self>> {
        Self::for_direct_scan(file_rows, group_rows, batch_size, 1, 0)
    }

    /// Admit the unchanged direct baseline's batch layout. With an exclusively
    /// orchestrated top-level budget initially holding no worker permits, its
    /// aggregate helpers take at most floor((workers-1)/2), leaving at least one
    /// permit for the baseline's physical-row-group ParallelReader. Candidate
    /// cursors use those same group boundaries, so misalignment is then safe.
    ///
    /// The caller must capture active permits before candidate/helper creation,
    /// reject relational/RowFilter/pruning plans, and prevent external concurrent
    /// permit acquisition. Exact post-decode WHERE/projection transforms preserve
    /// this proof. Otherwise only aligned layouts are admitted. Invalid metadata
    /// is an error, including a group sum inconsistent with the file footer.
    pub(super) fn for_direct_scan(
        file_rows: i64,
        group_rows: impl IntoIterator<Item = i64>,
        batch_size: usize,
        workers: usize,
        entry_active_workers: usize,
    ) -> Result<Option<Self>> {
        if batch_size == 0 {
            return Err("pipeline admission requires a positive batch size".into());
        }
        if !(1..=64).contains(&workers) {
            return Err("pipeline admission workers must be 1..64".into());
        }
        let file_rows = u64::try_from(file_rows)
            .map_err(|_| "pipeline file row count is negative".to_string())?;
        let mut groups = group_rows.into_iter().peekable();
        let mut total_rows = 0_u64;
        let mut group_count = 0usize;
        let mut aligned = true;
        while let Some(rows) = groups.next() {
            group_count = group_count
                .checked_add(1)
                .ok_or("pipeline group count overflow")?;
            let rows = u64::try_from(rows)
                .map_err(|_| "pipeline row-group count is negative".to_string())?;
            total_rows = total_rows
                .checked_add(rows)
                .filter(|&n| n <= i64::MAX as u64)
                .ok_or("pipeline total row count exceeds i64::MAX")?;
            if groups.peek().is_some() && rows % batch_size as u64 != 0 {
                aligned = false;
            }
        }
        if total_rows != file_rows {
            return Err("pipeline file and row-group counts disagree".into());
        }
        let baseline_uses_physical_groups =
            workers > 1 && group_count > 1 && entry_active_workers == 0;
        Ok((aligned || baseline_uses_physical_groups).then_some(Self { total_rows }))
    }

    pub(super) fn total_rows(self) -> u64 {
        self.total_rows
    }
}

#[derive(Debug)]
pub(super) struct GroupProgress {
    expected: u64,
    observed: u64,
    failed: bool,
}

impl GroupProgress {
    pub(super) fn new(expected: i64) -> Result<Self> {
        Ok(Self {
            expected: u64::try_from(expected)
                .map_err(|_| "pipeline row-group count is negative".to_string())?,
            observed: 0,
            failed: false,
        })
    }

    /// Must precede expressions and aggregate updates for this entire batch.
    /// Rejected rows are never added to a local partial, even transiently.
    pub(super) fn observe(&mut self, rows: usize) -> Result<()> {
        if self.failed {
            return Err("pipeline row-group progress already failed".into());
        }
        let next = u64::try_from(rows)
            .ok()
            .and_then(|rows| self.observed.checked_add(rows))
            .filter(|&n| n <= self.expected);
        match next {
            Some(next) => {
                self.observed = next;
                Ok(())
            }
            None => {
                self.failed = true;
                Err("pipeline row group emitted more rows than its footer".into())
            }
        }
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.failed || self.observed != self.expected {
            return Err("pipeline row-group delivery does not match its footer".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_exact_count_boundary_without_allocating_rows() {
        let admitted = SourceAdmission::new(i64::MAX, [i64::MAX - 1, 1], 2)
            .unwrap()
            .unwrap();
        assert_eq!(admitted.total_rows(), i64::MAX as u64);
        assert_eq!(
            SourceAdmission::new(0, [], 1024)
                .unwrap()
                .unwrap()
                .total_rows(),
            0
        );
        assert!(SourceAdmission::new(3, [0, 3], 1024).unwrap().is_some());
    }

    #[test]
    fn alignment_uses_every_nonfinal_group_and_preserves_footer_validation() {
        assert!(SourceAdmission::new(7, [4, 3], 4).unwrap().is_some());
        assert!(SourceAdmission::new(7, [3, 4], 4).unwrap().is_none());
        assert!(SourceAdmission::new(8, [3, 4], 4).is_err());
        assert!(SourceAdmission::new(7, [3, -1, 5], 4).is_err());
    }

    #[test]
    fn rejects_invalid_and_overflowing_metadata() {
        assert!(SourceAdmission::new(-1, [], 1).is_err());
        assert!(SourceAdmission::new(0, [-1], 1).is_err());
        assert!(SourceAdmission::new(i64::MAX, [i64::MAX, 1], 1).is_err());
        assert!(SourceAdmission::new(i64::MAX, [i64::MAX, i64::MAX, i64::MAX], 1).is_err());
        assert!(SourceAdmission::new(1, [], 1).is_err());
        assert!(SourceAdmission::new(0, [], 0).is_err());
    }

    #[test]
    fn fresh_parallel_direct_budgets_prove_physical_batches_without_alignment() {
        for workers in [1, 2, 4, 10, 64] {
            let admission = SourceAdmission::for_direct_scan(6, [2, 2, 2], 3, workers, 0).unwrap();
            assert_eq!(admission.is_some(), workers > 1);
            if workers > 1 {
                // This is the maximum aggregate helper claim in Workers::new;
                // fewer canonical states or failed metadata funding claim less.
                let budget = crate::memory::Budget::new(1024);
                let aggregate = budget.workers((workers - 1) / 2, workers - 1);
                let scan = budget.workers(3, workers - 1);
                assert!(scan.count > 0);
                drop(scan);
                drop(aggregate);
            }
            for occupied in [1, workers.max(2) - 1] {
                assert!(
                    SourceAdmission::for_direct_scan(6, [2, 2, 2], 3, workers, occupied)
                        .unwrap()
                        .is_none()
                );
                assert!(
                    SourceAdmission::for_direct_scan(7, [4, 3], 4, workers, occupied)
                        .unwrap()
                        .is_some()
                );
            }
        }
        assert!(SourceAdmission::for_direct_scan(6, [2, 2, 2], 3, 0, 0).is_err());
        assert!(SourceAdmission::for_direct_scan(6, [2, 2, 2], 3, 65, 0).is_err());
        assert!(SourceAdmission::for_direct_scan(7, [2, 2, 2], 3, 10, 0).is_err());
        assert!(SourceAdmission::for_direct_scan(6, [2, -1, 5], 3, 10, 0).is_err());
    }

    #[test]
    fn delivery_requires_exact_eof_and_rejects_excess_before_update() {
        let mut progress = GroupProgress::new(5).unwrap();
        progress.observe(2).unwrap();
        progress.observe(3).unwrap();
        progress.finish().unwrap();
        GroupProgress::new(0).unwrap().finish().unwrap();
        assert!(GroupProgress::new(-1).is_err());
        let mut short = GroupProgress::new(5).unwrap();
        short.observe(4).unwrap();
        assert!(short.finish().is_err());
        let mut excess = GroupProgress::new(5).unwrap();
        excess.observe(4).unwrap();
        assert!(excess.observe(2).is_err());
        assert_eq!(excess.observed, 4);
        assert!(excess.observe(1).is_err());
        assert!(excess.finish().is_err());
    }
}
