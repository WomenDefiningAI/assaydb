//! Optional aggregation over the existing shared materialization boundary.
use super::*;
use crate::{bind::Aggregate, expr::Kind};
fn target(node: &Node) -> Option<(usize, &Node, &Node)> {
    match &node.op {
        RelOp::Rename { input } => target(input),
        RelOp::Materialize {
            id,
            input,
            shared_scan: true,
        } if matches!(input.op, RelOp::Scan { .. }) => Some((*id, input, node)),
        _ => None,
    }
}
pub(super) fn shape(plan: &Plan, child: &Node, options: &Options) -> bool {
    // Production has only the proven single numeric source-owner path. Decline
    // impossible domains before shared_retry captures charged source guards.
    // The broader hash-partition experiment remains an explicit test oracle.
    #[cfg(not(test))]
    let source_owned = true;
    #[cfg(test)]
    let source_owned = !options.test_shared_batch_pipeline || options.test_source_owned_pipeline;
    if source_owned
        && (!(2..=64).contains(&options.workers)
            || plan.groups.len() != 1
            || !matches!(plan.groups[0].ty, Type::Int | Type::Decimal(_)))
    {
        return false;
    }
    if !plan.aggregation
        || !execute::pipeline::integer_totals_reorder_safe(plan, &child.schema)
        || plan.groups.is_empty()
        || !plan.order.is_empty()
        || plan.filter.is_some()
        || !plan
            .groups
            .iter()
            .all(|e| matches!(e.kind, Kind::Column(_)))
        || plan.aggregates.iter().any(|a| {
            a.distinct
                || !matches!(
                    a.function,
                    Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                )
                || a.input
                    .as_ref()
                    .is_some_and(|e| !matches!(e.kind, Kind::Column(_)))
                || (!matches!(a.function, Aggregate::Count)
                    && !a
                        .input
                        .as_ref()
                        .is_some_and(|e| matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Null)))
        })
    {
        return false;
    }
    target(child).is_some()
        && child
            .schema
            .fields()
            .iter()
            .all(|field| Type::from_field(field).is_ok())
}
pub(super) fn enabled(options: &Options) -> bool {
    #[cfg(test)]
    if !options.test_shared_batch_pipeline && !options.test_source_owned_pipeline {
        return false;
    }
    options.workers > 1 && options.relational_optimizations && options.spill.is_none()
}
pub(super) fn try_execute(
    plan: &Plan,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    let Some(attempt) = ctx.shared_attempt.clone() else {
        return Ok(None);
    };
    if !enabled(ctx.options)
        || ctx.spill.is_some()
        || ctx.budget.active_workers() != 0
        || !shape(plan, child, ctx.options)
    {
        return Ok(None);
    }
    let Some((id, input, materialize)) = target(child) else {
        return Ok(None);
    };
    #[cfg(not(test))]
    if input.estimated_rows
        < ctx
            .options
            .batch_size
            .saturating_mul(ctx.options.workers)
            .saturating_mul(2)
            .max(1024)
    {
        return Ok(None);
    }
    // Preserve both source and per-reference transforms by declining them.
    for node in [input, materialize] {
        let key = node as *const Node as usize;
        if ctx.scan_filters.contains_key(&key)
            || ctx.static_filters.contains_key(&key)
            || ctx.scan_transforms.contains_key(&key)
            || ctx.replay_transforms.contains_key(&key)
        {
            return Ok(None);
        }
    }
    if child
        .schema
        .fields()
        .iter()
        .any(|f| Type::from_field(f).is_err())
    {
        return Ok(None);
    }
    let storage = materialized_storage(id, input, true, ctx)?;
    if !storage.files.is_empty() {
        return Err("shared pipeline admitted spilled storage".into());
    }
    let mut plan = plan.clone();
    if !ctx.options.projection_pushdown {
        plan.columns = (0..child.schema.fields().len()).collect();
    }
    let prepared = Prepared {
        internal_output: true,
        plan,
        schema: child.schema.clone(),
        relational: None,
    };
    #[cfg(test)]
    if ctx.options.test_shared_batch_pipeline && !ctx.options.test_source_owned_pipeline {
        // Retain the failed hash-partition experiment only as a test oracle.
        return execute::pipeline::shared::execute_with_attempt(
            &prepared,
            ctx.options,
            ctx.budget.clone(),
            storage,
            attempt,
            emit,
        )
        .map(Some);
    }
    execute::pipeline::source_owned::try_execute(
        &prepared,
        ctx.options,
        ctx.budget.clone(),
        storage,
        attempt,
        emit,
    )
}
pub(super) fn merge_metrics(a: &mut Metrics, b: &Metrics) {
    // Original physical scan already accounted rows/bytes; ordinary rel::merge
    // handles replay aggregate batches and operator work without counting scan rows.
    a.source_domain_proof_seconds += b.source_domain_proof_seconds;
    a.source_boundary_merge_seconds += b.source_boundary_merge_seconds;
    a.source_boundary_merges += b.source_boundary_merges;
    a.pipeline_producers += b.pipeline_producers;
    a.pipeline_background_producers += b.pipeline_background_producers;
    a.pipeline_source_batches += b.pipeline_source_batches;
    a.pipeline_step_jobs += b.pipeline_step_jobs;
    a.pipeline_pressure_flushes += b.pipeline_pressure_flushes;
    a.pipeline_exchange_peak = a.pipeline_exchange_peak.max(b.pipeline_exchange_peak);
    a.pipeline_worker_handoffs += b.pipeline_worker_handoffs;
    a.pipeline_source_routes += b.pipeline_source_routes;
    a.pipeline_source_routed_groups += b.pipeline_source_routed_groups;
    a.pipeline_background_routes += b.pipeline_background_routes;
    for i in 0..3 {
        a.pipeline_runtime_submitted[i] += b.pipeline_runtime_submitted[i];
        a.pipeline_runtime_caller[i] += b.pipeline_runtime_caller[i];
        a.pipeline_runtime_background[i] += b.pipeline_runtime_background[i];
        a.pipeline_runtime_completed[i] += b.pipeline_runtime_completed[i];
    }
    a.pipeline_runtime_peak_outstanding = a
        .pipeline_runtime_peak_outstanding
        .max(b.pipeline_runtime_peak_outstanding);
    a.pipeline_runtime_peak_queued = a
        .pipeline_runtime_peak_queued
        .max(b.pipeline_runtime_peak_queued);
}
#[cfg(test)]
mod tests;
