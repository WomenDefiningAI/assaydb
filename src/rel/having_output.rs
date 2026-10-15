//! Fuse only a total filter immediately above an in-memory numeric aggregate.
use super::*;

/// Scan chains have no alternative join/shared aggregate sink to bypass.
fn scan_chain(node: &Node) -> bool {
    match &node.op {
        RelOp::Scan { .. } | RelOp::Empty | RelOp::One => true,
        RelOp::Rename { input } | RelOp::Project { input, .. } | RelOp::Filter { input, .. } => {
            scan_chain(input)
        }
        _ => false,
    }
}

/// Admit before input starts; reuse the ordinary or source-owned sink unchanged.
pub(super) fn try_execute(
    child: &Node,
    predicate: &Expr,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    if !ctx.options.relational_optimizations || ctx.spill.is_some() {
        return Ok(false);
    }
    let RelOp::Unary { plan, input } = &child.op else {
        return Ok(false);
    };
    if !scan_chain(input) {
        return Ok(false);
    }
    let Some(having) = execute::Having::for_plan(plan, predicate) else {
        return Ok(false);
    };
    let start = Instant::now();
    let prepared = Prepared {
        auto_simple: None,
        internal_output: true,
        plan: plan.clone(),
        schema: input.schema.clone(),
        relational: None,
    };
    let metrics = if let Some(metrics) =
        numeric_leaf::try_execute_having(&prepared, input, ctx, Some(&having), emit)?
    {
        numeric_leaf::merge_metrics(&mut ctx.metrics, &metrics);
        metrics
    } else {
        let metrics = execute::execute_having_stream(
            &prepared,
            ctx.options,
            ctx.budget.clone(),
            Some(&having),
            |send| input.run(ctx, send),
            emit,
        )?;
        merge(&mut ctx.metrics, &metrics);
        metrics
    };
    // Retain the aggregate's logical pre-HAVING row count in operator profiles.
    ctx.metrics.operator_profiles.push(serde_json::json!({
        "operator": "Aggregate", "estimated_rows": child.estimated_rows,
        "output_rows": metrics.having_input_groups,
        "inclusive_seconds": start.elapsed().as_secs_f64(),
        "timing_scope": "includes child execution and downstream consumer; do not sum",
    }));
    Ok(true)
}
