//! Complete uncorrelated scalar dependencies before candidate-side existence.
//! This is an execution barrier, not permission to move membership through a
//! scalar/aggregate or to suppress any of its input, completion or value errors.
use super::*;
use crate::expr::{Kind, Op};

fn total_expression(expr: &Expr, input: &Node) -> bool {
    if membership_safety::expression_is_total(expr, input) {
        return true;
    }
    match &expr.kind {
        Kind::Binary(Op::And | Op::Or, a, b) => {
            total_expression(a, input) && total_expression(b, input)
        }
        Kind::Not(a) => total_expression(a, input),
        Kind::Binary(Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge, a, b)
            if a.ty == Type::Float || b.ty == Type::Float =>
        {
            // Expr::eval compares INTEGER/Float exactly; DECIMAL/Float uses
            // to_float, with no checked decimal rescale or arithmetic. Finite
            // precision-38 mantissas at scales 0..38 fit binary64; conversion's
            // shifted numerator needs at most 180 of its 255 available bits.
            // Completion of the scalar producing a Float is still required.
            let numeric = |ty: &Type| {
                matches!(ty, Type::Int | Type::Float | Type::Null)
                    || matches!(ty, Type::Decimal(scale) if (0..=38).contains(scale))
            };
            numeric(&a.ty)
                && numeric(&b.ty)
                && total_expression(a, input)
                && total_expression(b, input)
        }
        _ => false,
    }
}

/// Count dependencies without allocating. Only a scalar on the build side of
/// an INNER cross join is special; every surrounding operator must be total.
/// Global aggregates, limits, correlated/default scalars and other barriers
/// retain their original admission rules.
fn dependencies(node: &Node) -> Option<usize> {
    if membership_safety::input_is_total(node) {
        return Some(0);
    }
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } => dependencies(input),
        RelOp::Filter { input, predicate } if total_expression(predicate, input) => {
            dependencies(input)
        }
        RelOp::Unary { input, plan }
            if !plan.aggregation
                && plan.order.is_empty()
                && plan.filter.is_none()
                && plan.output.iter().all(
                    |output| matches!(output, bind::Output::Expr(e) if total_expression(e, input)),
                ) =>
        {
            dependencies(input)
        }
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            keys,
            residual,
        } if keys.is_empty()
            && residual.as_ref().is_none_or(|e| total_expression(e, node))
            && matches!(right.op, RelOp::Scalar { .. }) =>
        {
            dependencies(left)?.checked_add(1)
        }
        _ => None,
    }
}

fn complete(node: &Node, ctx: &mut Context<'_>) -> Result<()> {
    match &node.op {
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => complete(input, ctx),
        RelOp::Join { left, right, .. } if matches!(right.op, RelOp::Scalar { .. }) => {
            // Match hash_join's right-before-left traversal even for nested
            // scalar dependencies. Run Scalar itself: it drains its child,
            // checks cardinality and constructs the empty-input NULL normally.
            let id = right.as_ref() as *const Node as usize;
            if !ctx.materialized.contains_key(&id) {
                sideways::fund_registry(ctx, 1)?;
                let budget = ctx.budget.clone();
                let metadata = budget.reserve(2048)?;
                let mut batches = Vec::with_capacity(1);
                let mut leases = Vec::with_capacity(2);
                right.run(ctx, &mut |batch| {
                    leases.push(budget.reserve(batch.get_array_memory_size())?);
                    batches.push(batch);
                    Ok(())
                })?;
                leases.push(metadata);
                ctx.materialized.insert(
                    id,
                    Arc::new(Materialized {
                        budget,
                        batches,
                        files: Vec::new(),
                        _leases: leases,
                    }),
                );
            }
            complete(left, ctx)
        }
        _ => Ok(()),
    }
}

pub(super) fn try_execute(
    node: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    try_candidate(node, None, ctx, emit)
}

pub(super) fn try_filtered(
    node: &Node,
    predicate: &Expr,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    try_candidate(node, Some(predicate), ctx, emit)
}

fn try_candidate(
    node: &Node,
    predicate: Option<&Expr>,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    let RelOp::Join {
        left,
        right,
        kind,
        keys,
        residual,
    } = &node.op
    else {
        return Ok(false);
    };
    if !ctx.options.relational_optimizations
        || ctx.options.error_replay.canonical()
        || ctx.spill.is_some()
        || ctx.demand_cache.is_none()
        || !matches!(kind, JoinKind::Semi | JoinKind::Anti)
        || residual.is_some()
        || left.estimated_rows.saturating_mul(4) >= right.estimated_rows
        || !membership_safety::join_conditions_are_total(node)
        || !membership_safety::input_is_total(right)
        || dependencies(left).is_none_or(|count| count == 0)
        || predicate.is_some_and(|predicate| !total_expression(predicate, left))
    {
        return Ok(false);
    }
    // Any failed optional retention is retried from fresh state by execute_once,
    // with this path disabled. No partially computed match/output can escape.
    ctx.demand_cache
        .unwrap()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    complete(left, ctx)?;
    let budget = ctx.budget.clone();
    let mut candidate = BufferedOutput {
        batches: Vec::new(),
        reservations: Vec::new(),
    };
    let mut filter_seconds = 0.;
    left.run(ctx, &mut |batch| {
        let Some(predicate) = predicate else {
            return paired_existence::retain(&mut candidate, batch, &budget);
        };
        // SEMI/ANTI preserve the left schema and never duplicate occurrences.
        // A total predicate may therefore select candidates first. Scalars
        // have already completed; no scalar input or error is filtered away.
        let start = Instant::now();
        let mask = predicate.eval_filter(&input(&batch, &budget, [predicate])?, &budget)?;
        // Filtering selects each row at most once: all source payload bytes
        // plus per-column allocation/alignment overhead bound the copy peak.
        let bytes = batch
            .num_columns()
            .checked_mul(512)
            .and_then(|n| n.checked_add(batch.get_array_memory_size()))
            .and_then(|n| n.checked_add(1024))
            .ok_or("candidate filter size overflow")?;
        let mut credit = budget.reserve(bytes)?;
        let filtered =
            filter_record_batch(&batch, boolean(&mask.array)).map_err(|e| e.to_string())?;
        credit.release_excess(filtered.get_array_memory_size().saturating_add(1024));
        filter_seconds += start.elapsed().as_secs_f64();
        if filtered.num_rows() == 0 {
            return Ok(());
        }
        paired_existence::retain_funded(&mut candidate, filtered, vec![credit], &budget)
    })?;
    ctx.metrics.filter_seconds += filter_seconds;
    let rows = candidate.batches.iter().try_fold(0usize, |rows, batch| {
        rows.checked_add(batch.num_rows())
            .ok_or("candidate existence row count overflow")
    })?;
    // Estimated selectivity controls speculation only. The actual retained
    // cardinality decides orientation, without a second candidate evaluation.
    if rows.saturating_mul(4) >= right.estimated_rows {
        node.hash_join_with_probe(
            ctx,
            emit,
            left,
            right,
            *kind,
            keys,
            None,
            None,
            Some(candidate),
            None,
        )?;
    } else {
        node.reverse_existence_join(ctx, emit, left, right, *kind, keys, None, Some(candidate))?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn completed_scalar_retention_is_funded_once_and_released_on_cancel_or_denial() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            true,
        )]));
        let scalar = Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op: RelOp::Scalar {
                input: Box::new(Node {
                    schema,
                    estimated_rows: 0,
                    op: RelOp::Empty,
                }),
            },
        };
        let left = Node {
            schema: Arc::new(Schema::new(vec![Field::new(
                "k",
                arrow::datatypes::DataType::Int64,
                true,
            )])),
            estimated_rows: 2,
            op: RelOp::Scan {
                path: PathBuf::new(),
                columns: vec![0],
                distinct: vec![2],
                statistics: Vec::new(),
            },
        };
        let node = Node {
            schema: combined(&left.schema, &scalar.schema),
            estimated_rows: 2,
            op: RelOp::Join {
                left: Box::new(left),
                right: Box::new(scalar),
                kind: JoinKind::Inner,
                keys: Vec::new(),
                residual: None,
            },
        };
        for memory_bytes in [1, 4096, 8192] {
            let options = Options {
                memory_bytes,
                ..Options::default()
            };
            let budget = Budget::new(memory_bytes);
            let used = AtomicBool::new(false);
            let mut ctx = Context {
                filtered_reuse: None,
                build_groups: None,
                demand_cache: Some(&used),
                shared_attempt: None,
                options: &options,
                scan_filters: HashMap::new(),
                static_filters: HashMap::new(),
                scan_transforms: HashMap::new(),
                lane_transforms: HashMap::new(),
                replay_transforms: HashMap::new(),
                materialized: HashMap::new(),
                recursive_bindings: None,
                sideways_registry_lease: None,
                spill: None,
                budget: budget.clone(),
                metrics: Metrics::default(),
            };
            match complete(&node, &mut ctx) {
                Ok(()) => {
                    assert_eq!(memory_bytes, 8192);
                    assert_eq!(ctx.materialized.len(), 1);
                    let before = budget.used();
                    assert!(before > 0);
                    let profiles = ctx.metrics.operator_profiles.len();
                    complete(&node, &mut ctx).unwrap();
                    assert_eq!(budget.used(), before);
                    assert_eq!(ctx.metrics.operator_profiles.len(), profiles);
                    options.cancellation.cancel();
                    assert_eq!(
                        node.run(&mut ctx, &mut |_| panic!("cancelled output"))
                            .unwrap_err(),
                        "cancelled: query execution"
                    );
                }
                Err(error) => {
                    assert!(memory_bytes < 8192);
                    assert!(error.starts_with("resource-limit:"), "{error}");
                    assert!(ctx.materialized.is_empty());
                }
            }
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
