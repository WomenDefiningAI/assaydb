//! Original analytical SQL execution with bounded workers. Arrow/Parquet and a syntax
//! parser are foundations; no reference query engine is linked or invoked.
pub mod bind;
pub mod execute;
pub mod expr;
pub mod memory;
pub mod session;
pub mod types;

pub type Result<T> = std::result::Result<T, String>;
pub const IMPLEMENTATION_STAGE: &str = "M3: measured single-worker execution";

mod dialect;
mod mixed_numeric;
pub mod rel;
mod syntax;

mod keys;

mod prune;

mod expr_program;

mod cte;

mod join_index;

mod equivalence;
mod membership;
mod parallel_scan;
mod parquet_scan;

mod aggregate_sum;
mod group_index;
mod group_lookup;
mod numeric_float;

mod join_filter;

mod cardinality;

mod scan_predicate;

pub mod spill;

mod external_aggregate;
mod external_join;
mod external_sort;
mod spill_partition;

mod pipeline_runtime;
