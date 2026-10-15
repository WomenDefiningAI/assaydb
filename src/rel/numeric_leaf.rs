//! Only an unfiltered physical leaf may own scan/aggregate workers together.
use super::*;

pub(super) fn merge_metrics(dst: &mut Metrics, src: &Metrics) {
    // The fused leaf replaces the Scan too. Only scan row counts need an
    // explicit addition: ordinary rel::merge already includes decode time.
    dst.rows_read += src.rows_read;
    shared_pipeline::merge_metrics(dst, src);
    merge(dst, src);
}

fn target<'a>(node: &'a Node, ctx: &Context<'_>) -> Option<(&'a PathBuf, &'a [usize])> {
    let id = node as *const Node as usize;
    if ctx.scan_filters.contains_key(&id)
        || ctx.static_filters.contains_key(&id)
        || ctx.scan_transforms.contains_key(&id)
        || ctx.lane_transforms.contains_key(&id)
        || ctx.replay_transforms.contains_key(&id)
    {
        return None;
    }
    match &node.op {
        RelOp::Rename { input } => target(input, ctx),
        RelOp::Scan { path, columns, .. } => Some((path, columns)),
        _ => None,
    }
}

pub(super) fn try_execute(
    prepared: &Prepared,
    child: &Node,
    ctx: &Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    try_execute_having(prepared, child, ctx, None, emit)
}

/// Preserve leaf admission and metrics while passing only a total output predicate.
pub(super) fn try_execute_having(
    prepared: &Prepared,
    child: &Node,
    ctx: &Context<'_>,
    having: Option<&execute::Having>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    if ctx.spill.is_some()
        || ctx.budget.active_workers() != 0
        || !execute::pipeline::leaf_owned::eligible(prepared, ctx.options)
    {
        return Ok(None);
    }
    let Some((path, columns)) = target(child, ctx) else {
        return Ok(None);
    };
    if having.is_none() {
        return execute::pipeline::leaf_owned::try_execute(
            prepared,
            ctx.options,
            ctx.budget.clone(),
            path,
            columns,
            emit,
        );
    }
    execute::pipeline::leaf_owned::try_execute_having(
        prepared,
        ctx.options,
        ctx.budget.clone(),
        path,
        columns,
        having,
        emit,
    )
}

/// Re-reading a shared physical scan leaves its cache untouched and never crosses a filter.
fn clustered_target<'a>(node: &'a Node, ctx: &Context<'_>) -> Option<(&'a PathBuf, &'a [usize])> {
    match &node.op {
        RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        }
        | RelOp::Rename { input } => {
            let id = node as *const Node as usize;
            if ctx.scan_filters.contains_key(&id)
                || ctx.static_filters.contains_key(&id)
                || ctx.scan_transforms.contains_key(&id)
                || ctx.lane_transforms.contains_key(&id)
                || ctx.replay_transforms.contains_key(&id)
            {
                return None;
            }
            clustered_target(input, ctx)
        }
        _ => target(node, ctx),
    }
}

/// Fusion changes neither the retained aggregation plan nor any child expression demand.
fn clustered(
    prepared: &Prepared,
    child: &Node,
    having: Option<&Expr>,
    ctx: &Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    if ctx.spill.is_some() {
        return Ok(None);
    }
    let Some((path, columns)) = clustered_target(child, ctx) else {
        return Ok(None);
    };
    execute::pipeline::leaf_owned::clustered::try_execute(
        prepared,
        ctx.options,
        ctx.budget.clone(),
        path,
        columns,
        having,
        emit,
    )
}

/// Only an immediate HAVING over a plain numeric leaf is eligible for lane-local filtering.
pub(super) fn try_filtered(
    child: &Node,
    predicate: &Expr,
    ctx: &Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    let RelOp::Unary { plan, input } = &child.op else {
        return Ok(None);
    };
    try_clustered_plan(plan, input, Some(predicate), ctx, emit)
}

/// Attempt metadata ownership before the existing shared materialization is created.
pub(super) fn try_clustered_plan(
    plan: &Plan,
    child: &Node,
    having: Option<&Expr>,
    ctx: &Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    if !plan.aggregation || !ctx.options.relational_optimizations {
        return Ok(None);
    }
    let prepared = Prepared {
        auto_simple: None,
        internal_output: true,
        plan: plan.clone(),
        schema: child.schema.clone(),
        relational: None,
    };
    clustered(&prepared, child, having, ctx, emit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_decode_time_and_scan_rows_are_merged_once() {
        let mut total = Metrics {
            rows_read: 7,
            scan_decode_seconds: 0.5,
            ..Metrics::default()
        };
        let leaf = Metrics {
            rows_read: 1024,
            scan_decode_seconds: 0.25,
            pipeline_producers: 4,
            ..Metrics::default()
        };
        merge_metrics(&mut total, &leaf);
        assert_eq!(total.rows_read, 1031);
        assert_eq!(total.scan_decode_seconds, 0.75);
        assert_eq!(total.pipeline_producers, 4);
        merge_metrics(&mut total, &leaf);
        assert_eq!(total.rows_read, 2055);
        assert_eq!(total.scan_decode_seconds, 1.0);
        assert_eq!(total.pipeline_producers, 8);
    }
}
