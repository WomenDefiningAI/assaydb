//! Error-only, whole-query replay. The caller retains the prepared plan object;
//! this module never binds SQL or invokes an optimizer. Stored inputs obey the
//! same immutable-file contract as automatic spill and shared-pipeline retry.
//!
//! Success does no replay work. Failure can cost one complete serial traversal
//! of P, with one-row batches (rather than just the original failing prefix).
//! Vector setup costs can therefore grow by the original batch width; there is
//! no constant time bound relative to an early parallel failure. Replay has the
//! original cancellation token and accounted memory ceiling. Spill is disabled
//! to avoid partition-order error selection; insufficient memory leaves the
//! original error in place and is observable as `unavailable`.
//!
//! Preparation does not select a different relational tree for worker count or
//! batch size. Those options do select parallel scans, fused aggregates and
//! partitioned executors at runtime. Canonical execution borrows the original
//! Prepared/Node, bypasses those optional kernels, and preserves its child order,
//! join orientation, expressions, filters and LIMIT nodes. It uses fresh CTE,
//! join, aggregate and output state. Column/literal aggregate and join keys are
//! admitted; potentially failing computed keys/aggregate inputs decline, as do
//! reverse-existence and aggregate-demand joins that can consume probes first. Scalar
//! projection/filter/residual expressions retain the existing evaluator and its
//! conditional demand rules. No SQL text is reparsed and no successful query is
//! re-evaluated.
use super::{Execution, Options};
use crate::{Result, memory::Budget};
use serde::Serialize;
use std::sync::{Arc, Mutex};

/// Build keys are evaluated after their entire input is materialized, and an
/// aggregate evaluates all arguments before updating its states. One-row scan
/// batches alone do not order competing failures at those two boundaries.
/// Decline potentially failing expressions there until those kernels have a
/// canonical expression/update interface. Column/literal arguments cannot add
/// a competing evaluation error, so ordinary ordered updates remain sufficient.
pub(crate) fn atomic(expr: &crate::expr::Expr) -> bool {
    matches!(
        expr.kind,
        crate::expr::Kind::Column(_) | crate::expr::Kind::Literal(_)
    )
}

pub(crate) fn admit_aggregate(plan: &crate::bind::Plan) -> Result<()> {
    if plan.aggregation
        && (!plan.groups.iter().all(atomic)
            || !plan
                .aggregates
                .iter()
                .all(|aggregate| aggregate.input.as_ref().is_none_or(atomic)))
    {
        return Err(
            "canonical replay unavailable: computed aggregate argument or grouping key".into(),
        );
    }
    Ok(())
}

/// Error diagnostics live outside `Execution`, which does not exist on failure.
/// Cloned options share cumulative counters; a new default starts at zero.
#[derive(Clone, Debug, Default)]
pub struct ErrorReplay {
    metrics: Arc<Mutex<ErrorReplayMetrics>>,
    canonical: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ErrorReplayMetrics {
    pub attempts: usize,
    pub semantic_errors: usize,
    pub succeeded: usize,
    pub unavailable: usize,
    pub peak_reserved_bytes: usize,
    pub peak_workers: usize,
    pub original_error: Option<String>,
    pub replay_error: Option<String>,
}

impl ErrorReplay {
    pub fn metrics(&self) -> ErrorReplayMetrics {
        self.metrics.lock().unwrap().clone()
    }

    pub(crate) fn canonical(&self) -> bool {
        self.canonical
    }
}

/// Recognize evaluation failures only. A resource/decode/invariant failure is
/// never promoted to a semantic error merely because its text says overflow.
pub(crate) fn semantic_error(message: &str) -> bool {
    let mut message = message
        .split_once("; automatic spill retry after ")
        .map_or(message, |(retry, _)| retry);
    // Each enclosing spilled join can append context to a downstream failure.
    // Peel only known wrappers; classification still requires an exact cause.
    while let Some(cause) = message
        .strip_suffix("; external join build partitioning")
        .or_else(|| message.strip_suffix("; external join probe partitioning"))
        .or_else(|| message.strip_suffix("; external join probe"))
        .or_else(|| message.strip_suffix("; external join unmatched/mark output"))
    {
        message = cause;
    }
    matches!(
        message,
        "division by zero"
            | "floating arithmetic overflow"
            | "numeric overflow"
            | "decimal precision 38 overflow"
            | "FACTORIAL argument outside 0..20"
            | "date out of range"
            | "BIGINT overflow"
            | "aggregate count overflow"
            | "aggregate accumulator overflow"
            | "scalar subquery returned multiple rows"
            | "unsupported: REAL cast nonfinite result"
            | "unsupported: cast from NaN"
            | "unsupported: BLOB to TEXT requires valid UTF-8"
    )
}

pub(crate) fn select(
    original: Result<Execution>,
    options: &Options,
    replay: impl FnOnce(&Options, Budget) -> Result<Execution>,
) -> Result<Execution> {
    let original = match original {
        Err(error) if semantic_error(&error) => error,
        other => return other,
    };
    let mut canonical = options.clone();
    canonical.workers = 1;
    canonical.batch_size = 1;
    canonical.spill = None;
    canonical.error_replay.canonical = true;
    // Test injections describe the failed attempt, not the fresh replay.
    #[cfg(test)]
    {
        canonical.test_source_owned_pipeline = false;
        canonical.test_shared_batch_pipeline = false;
        canonical.test_shared_after_emit_failure = false;
        canonical.test_aggregate_partitions = None;
        canonical.test_pipeline_default = false;
        canonical.test_pipeline_partitions = None;
        canonical.test_pipeline_slice_bytes = None;
        canonical.test_pipeline_fallback = false;
        canonical.test_pipeline_interruption = None;
        canonical.test_pipeline_continuation = None;
    }
    let budget = Budget::new(options.memory_bytes);
    let result = replay(&canonical, budget.clone());
    let succeeded = result.is_ok();
    // Drop every successful replay result as well: it can never be published.
    let replay_error = result.err();
    let clean = budget.used() == 0 && budget.active_workers() == 0;
    let selected = replay_error
        .as_ref()
        .filter(|error| clean && semantic_error(error))
        .cloned();
    {
        let mut metrics = options.error_replay.metrics.lock().unwrap();
        metrics.attempts += 1;
        metrics.succeeded += usize::from(succeeded);
        metrics.semantic_errors += usize::from(selected.is_some());
        metrics.unavailable += usize::from(!succeeded && selected.is_none());
        metrics.peak_reserved_bytes = metrics.peak_reserved_bytes.max(budget.peak());
        metrics.peak_workers = metrics.peak_workers.max(budget.peak_workers());
        metrics.original_error = Some(original.clone());
        metrics.replay_error = replay_error;
    }
    debug_assert!(
        !succeeded,
        "semantic error replay succeeded after: {original}"
    );
    Err(selected.unwrap_or(original))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn spilled_join_context_does_not_hide_semantic_errors_or_promote_resources() {
        for context in [
            "; external join build partitioning",
            "; external join probe partitioning",
            "; external join probe",
            "; external join unmatched/mark output",
        ] {
            let original = format!("floating arithmetic overflow{context}{context}");
            let options = Options::default();
            let result = select(Err(original.clone()), &options, |_, _| {
                Err("decimal precision 38 overflow".into())
            });
            assert_eq!(result.err().unwrap(), "decimal precision 38 overflow");
            let metrics = options.error_replay.metrics();
            assert_eq!(metrics.attempts, 1);
            assert_eq!(metrics.semantic_errors, 1);
            assert_eq!(metrics.original_error.as_deref(), Some(original.as_str()));
            for cause in [
                "resource-limit: accounted memory exhausted",
                "cancelled: query execution",
                "decoder overflow",
            ] {
                assert!(!semantic_error(&format!("{cause}{context}{context}")));
            }
        }
        assert!(!semantic_error("numeric overflow; unknown context"));
    }

    #[test]
    fn replay_has_fresh_state_and_preserves_the_budget_after_a_spill_retry() {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 4096,
                    spill: Some(crate::spill::SpillOptions {
                        mode: crate::spill::SpillMode::Auto,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let attempts = Cell::new(0);
                let initial = super::super::execution_policy(&options, |_, budget| {
                    attempts.set(attempts.get() + 1);
                    let _state = budget.reserve(1024)?;
                    if attempts.get() == 1 {
                        Err("resource-limit: accounted memory injected".into())
                    } else {
                        Err("floating arithmetic overflow".into())
                    }
                });
                let result = select(initial, &options, |canonical, budget| {
                    assert_eq!(attempts.get(), 2);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.limit(), options.memory_bytes);
                    assert_eq!(canonical.workers, 1);
                    assert_eq!(canonical.batch_size, 1);
                    assert!(canonical.spill.is_none());
                    let _fresh_state = budget.reserve(1024)?;
                    Err("decimal precision 38 overflow".into())
                });
                assert_eq!(result.err().unwrap(), "decimal precision 38 overflow");
                let metrics = options.error_replay.metrics();
                assert_eq!(metrics.attempts, 1);
                assert_eq!(metrics.semantic_errors, 1);
                assert_eq!(metrics.unavailable, 0);
                assert_eq!(metrics.succeeded, 0);
            }
        }
    }

    #[test]
    fn replay_resource_cancellation_and_unknown_failures_keep_the_original() {
        for replay_error in [
            "resource-limit: accounted memory exhausted",
            "cancelled: query execution",
            "pipeline source changed",
        ] {
            let options = Options::default();
            let error = select(Err("BIGINT overflow".into()), &options, |_, _| {
                Err(replay_error.into())
            })
            .err()
            .unwrap();
            assert_eq!(error, "BIGINT overflow");
            assert_eq!(options.error_replay.metrics().unavailable, 1);
        }
    }

    #[test]
    fn unexpected_success_is_recorded_before_the_debug_assertion() {
        let options = Options::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            select(Err("BIGINT overflow".into()), &options, |_, _| {
                Ok(Execution {
                    batch: arrow::record_batch::RecordBatch::new_empty(Arc::new(
                        arrow::datatypes::Schema::empty(),
                    )),
                    metrics: super::super::Metrics::default(),
                    _reservations: Vec::new(),
                })
            })
        }));
        if cfg!(debug_assertions) {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().err().unwrap(), "BIGINT overflow");
        }
        assert_eq!(options.error_replay.metrics().succeeded, 1);
    }
}
