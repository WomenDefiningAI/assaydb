//! Original analytical SQL execution with bounded workers. Arrow/Parquet and a syntax
//! parser are foundations; no reference query engine is linked or invoked.
pub mod bind;
pub mod execute;
pub mod expr;
pub mod memory;
pub mod session;
pub mod types;
pub mod zone;

mod error;
pub use error::{Error, ErrorCause};
pub type Result<T> = std::result::Result<T, Error>;
pub const IMPLEMENTATION_STAGE: &str = "M3: measured single-worker execution";

mod cte_hint_syntax;
mod dialect;
pub mod engine_stack;
mod fetch_syntax;
mod frame_exclusion;
mod mixed_numeric;
#[doc(hidden)]
pub mod nesting;
pub mod rel;
mod syntax;

mod keys;

mod prune;

mod expr_program;

mod cte;

mod join_index;

mod equivalence;
mod membership;
mod parallel_prepare;
mod parallel_scan;
mod parquet_scan;

mod aggregate_sum;
mod group_index;
mod group_lookup;
mod moments;
mod numeric_float;

mod join_filter;

mod cardinality;

mod scan_predicate;
mod selection;

pub mod spill;

mod external_aggregate;
mod external_join;
mod external_sort;
mod spill_partition;

mod pipeline_runtime;

/// Read-only catalog access for the separately built verified crate.
#[cfg(feature = "verified-hooks")]
pub mod verified_hooks {
    pub use crate::dialect::QueryDialect;
    pub use crate::session::catalog_view::*;

    /// Shrinks a memory reservation to the bytes a verified result still holds.
    pub fn release_excess(reservation: &mut crate::memory::Reservation, retained_bytes: usize) {
        reservation.release_excess(retained_bytes);
    }

    /// Builds an execution result without reservations, for comparator controls in tests.
    #[doc(hidden)]
    pub fn synthetic_execution(
        batch: arrow::record_batch::RecordBatch,
    ) -> crate::execute::Execution {
        crate::execute::Execution {
            batch,
            metrics: Default::default(),
            _reservations: Vec::new(),
        }
    }
}
