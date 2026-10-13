//! Join-specific admission for the shared ordered batch scheduler.
pub(in crate::rel) use super::super::ordered_batches::{Configuration, run};
use crate::{execute::Options, memory::Budget};

impl Configuration {
    pub fn new(options: &Options, budget: &Budget, estimated_rows: usize) -> Option<Self> {
        // A small probe pool overlaps gathering with decoding without taking
        // most scan capacity away from selective, decode-heavy children.
        let wanted = ((options.workers - 1) / 2).min(2);
        // Require several complete batches per helper to amortize setup.
        if wanted < 2
            || estimated_rows < options.batch_size.saturating_mul(wanted).saturating_mul(4)
        {
            return None;
        }
        let permits = budget.workers(wanted, options.workers - 1);
        if permits.count < 2 {
            return None;
        }
        Self::with_permits(budget, permits)
    }
}
