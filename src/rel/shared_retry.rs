//! One whole-query retry before publishing any relational result.
use super::*;
use crate::execute::{
    PipelineAttemptReport,
    pipeline::fallback::{Attempt, Source},
};
use std::{path::Path, sync::atomic::Ordering};

/// Exhaustive traversal includes rarely executed fallback/conditional subtrees.
fn visit(node: &Node, f: &mut impl FnMut(&Node) -> Result<()>) -> Result<()> {
    f(node)?;
    match &node.op {
        RelOp::Scan { .. } | RelOp::One | RelOp::Empty => Ok(()),
        RelOp::WindowOrder { input, .. }
        | RelOp::Window { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => visit(input, f),
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. }
        | RelOp::UnionAll { left, right } => {
            visit(left, f)?;
            visit(right, f)
        }
        RelOp::ScalarDefault {
            input, fallback, ..
        } => {
            visit(input, f)?;
            visit(fallback, f)
        }
    }
}
fn potential(root: &Node, options: &Options) -> bool {
    if !shared_pipeline::enabled(options) {
        return false;
    }
    let mut found = false;
    let _ = visit(root, &mut |node| {
        if let RelOp::Unary { plan, input } = &node.op {
            found |= shared_pipeline::shape(plan, input, options);
        }
        Ok(())
    });
    found
}
pub(super) struct Sources<'a> {
    entries: Vec<(&'a Path, Source)>,
    _credit: Reservation,
}
impl<'a> Sources<'a> {
    pub(super) fn capture(root: &'a Node, budget: &Budget, options: &Options) -> Result<Self> {
        let mut count = 0usize;
        visit(root, &mut |node| {
            if matches!(node.op, RelOp::Scan { .. }) {
                count = count.checked_add(1).ok_or("source guard count overflow")?;
            }
            Ok(())
        })?;
        let bytes = count
            .checked_mul(std::mem::size_of::<(&Path, Source)>())
            .and_then(|n| n.checked_add(512))
            .ok_or("source guard metadata overflow")?;
        let credit = budget.reserve(bytes)?;
        let mut sources = Self {
            entries: Vec::with_capacity(count),
            _credit: credit,
        };
        // This helper borrows paths directly from the immutable plan. No path
        // canonicalization or full-data copy changes the baseline source contract.
        fn collect<'a>(node: &'a Node, sources: &mut Sources<'a>, options: &Options) -> Result<()> {
            options.cancellation.check()?;
            if let RelOp::Scan { path, .. } = &node.op
                && !sources
                    .entries
                    .iter()
                    .any(|(old, _)| *old == path.as_path())
            {
                sources.entries.push((
                    path,
                    Source::capture_checked(path, &|| options.cancellation.check())?,
                ));
            }
            match &node.op {
                RelOp::Scan { .. } | RelOp::One | RelOp::Empty => Ok(()),
                RelOp::WindowOrder { input, .. }
                | RelOp::Window { input, .. }
                | RelOp::Unary { input, .. }
                | RelOp::Filter { input, .. }
                | RelOp::Rename { input }
                | RelOp::Project { input, .. }
                | RelOp::Limit { input, .. }
                | RelOp::Scalar { input }
                | RelOp::Materialize { input, .. } => collect(input, sources, options),
                RelOp::Join { left, right, .. }
                | RelOp::Mark { left, right, .. }
                | RelOp::TypedMark { left, right, .. }
                | RelOp::CorrelatedMark { left, right, .. }
                | RelOp::UnionAll { left, right } => {
                    collect(left, sources, options)?;
                    collect(right, sources, options)
                }
                RelOp::ScalarDefault {
                    input, fallback, ..
                } => {
                    collect(input, sources, options)?;
                    collect(fallback, sources, options)
                }
            }
        }
        collect(root, &mut sources, options)?;
        Ok(sources)
    }
    pub(super) fn verify(&self, options: &Options) -> Result<()> {
        for (path, source) in &self.entries {
            options.cancellation.check()?;
            source.verify_checked(path, &|| options.cancellation.check())?;
        }
        Ok(())
    }
}

pub(super) fn execute(
    root: &Node,
    options: &Options,
    budget: Budget,
    internal_output: bool,
) -> Result<Execution> {
    let mut report = PipelineAttemptReport::default();
    let result = execute_reported(root, options, budget, &mut report, internal_output);
    #[cfg(test)]
    if let Some(target) = &options.test_pipeline_report {
        *target.lock().unwrap() = report;
    }
    result
}

/// Reuse the immutable-source guard and the same fresh whole-query state owner
/// as resource retry. The caller supplies the original Node, never a replan.
pub(super) fn canonical(
    root: &Node,
    options: &Options,
    budget: Budget,
    internal_output: bool,
) -> Result<Execution> {
    visit(root, &mut |node| {
        match &node.op {
            RelOp::Unary { plan, .. } => crate::execute::error_replay::admit_aggregate(plan)?,
            RelOp::Join {
                left,
                right,
                kind,
                keys,
                ..
            } => {
                if !keys.iter().all(|(left, right)| {
                    crate::execute::error_replay::atomic(left)
                        && crate::execute::error_replay::atomic(right)
                }) {
                    return Err("canonical replay unavailable: computed join key".into());
                }
                if options.relational_optimizations {
                    // These runtime alternatives consume the logical probe
                    // before the build. Reusing Node alone does not pin their
                    // dependency order across spill, so do not claim rule E.
                    if matches!(kind, JoinKind::Semi | JoinKind::Anti)
                        && left.estimated_rows.saturating_mul(4) < right.estimated_rows
                    {
                        return Err("canonical replay unavailable: reversed existence join".into());
                    }
                    if matches!(kind, JoinKind::Inner | JoinKind::Left)
                        && !keys.is_empty()
                        && left.estimated_rows <= right.estimated_rows
                        && aggregate_scan_target(
                            right,
                            keys.iter().map(|(_, key)| key.clone()).collect(),
                        )?
                        .is_some()
                    {
                        return Err("canonical replay unavailable: aggregate demand join".into());
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    let sources = Sources::capture(root, &budget, options)?;
    let result = execute_once(root, options, budget.clone(), None, internal_output);
    sources.verify(options)?;
    result
}
fn execute_reported(
    root: &Node,
    options: &Options,
    budget: Budget,
    report: &mut PipelineAttemptReport,
    internal_output: bool,
) -> Result<Execution> {
    let started = Instant::now();
    let result = run(root, options, &budget, report, |attempt| {
        execute_once(root, options, budget.clone(), attempt, internal_output)
    });
    report.total_seconds = started.elapsed().as_secs_f64();
    report.peak_reserved_bytes = budget.peak();
    report.peak_workers = budget.peak_workers();
    result.map(|mut output| {
        output.metrics.execution_seconds = report.total_seconds;
        output.metrics.peak_reserved_bytes = budget.peak();
        output.metrics.worker_threads_peak = budget.peak_threads();
        output.metrics.pipeline_candidate_attempts = report.candidate_attempts;
        output.metrics.pipeline_fallbacks = report.fallback_attempts;
        output.metrics.pipeline_candidate_seconds = report.candidate_seconds;
        output.metrics.pipeline_fallback_seconds = report.fallback_seconds;
        output.metrics.pipeline_candidate_peak_reserved_bytes =
            report.candidate_peak_reserved_bytes;
        output.metrics.pipeline_candidate_error = report.candidate_error.clone();
        output.metrics.pipeline_candidate_sql_error = report.candidate_sql_error.clone();
        output
    })
}
fn run(
    root: &Node,
    options: &Options,
    budget: &Budget,
    report: &mut PipelineAttemptReport,
    mut execute: impl FnMut(Option<Arc<Attempt>>) -> Result<Execution>,
) -> Result<Execution> {
    options.cancellation.check()?;
    if !potential(root, options) || budget.active_workers() != 0 {
        return execute(None);
    }
    let entry_used = budget.used();
    let entry_workers = budget.active_workers();
    let sources = match Sources::capture(root, budget, options) {
        Ok(sources) => sources,
        Err(_) => {
            // A speculative guard must not preempt earlier baseline SQL errors.
            options.cancellation.check()?;
            return execute(None);
        }
    };
    let guarded_used = budget.used();
    let attempt = Arc::new(Attempt::default());
    let started = Instant::now();
    let outcome = execute(Some(attempt.clone()));
    report.candidate_attempts = usize::from(attempt.admitted.load(Ordering::Acquire));
    report.candidate_seconds = if report.candidate_attempts > 0 {
        started.elapsed().as_secs_f64()
    } else {
        0.0
    };
    report.candidate_peak_reserved_bytes = if report.candidate_attempts > 0 {
        budget.peak()
    } else {
        0
    };
    (report.candidate_error, report.candidate_sql_error) = attempt.diagnostics();
    options.cancellation.check()?;
    match outcome {
        Ok(output) => {
            sources.verify(options)?;
            Ok(output)
        }
        Err(error) => {
            if !attempt.admitted.load(Ordering::Acquire) {
                return Err(error);
            }
            // Downstream/root allocations can fail while optional partition state
            // still lives. Only the exact accounted-budget category grants retry.
            attempt.returned(error.clone());
            (report.candidate_error, report.candidate_sql_error) = attempt.diagnostics();
            let _ = attempt.decision(error)?;
            if budget.used() != guarded_used || budget.active_workers() != entry_workers {
                return Err("shared candidate cleanup did not restore entry ownership".into());
            }
            sources.verify(options)?;
            options.cancellation.check()?;
            report.fallback_attempts = 1;
            let fallback_started = Instant::now();
            let replay = execute(None);
            report.fallback_seconds = fallback_started.elapsed().as_secs_f64();
            options.cancellation.check()?;
            match replay {
                Ok(output) => {
                    sources.verify(options)?;
                    Ok(output)
                }
                Err(error) => {
                    // Preserve the baseline's SQL/decode chronology; no speculative
                    // post-error identity check replaces its observable error.
                    drop(sources);
                    if budget.used() != entry_used || budget.active_workers() != entry_workers {
                        return Err(
                            "shared baseline cleanup did not restore entry ownership".into()
                        );
                    }
                    Err(error)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
