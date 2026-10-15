//! Two total SEMI/ANTI consumers of one physical source, in one scan.
//! Each consumer keeps its own predicate and match bits; neither inherits the
//! other's row restrictions. The retained left rows are emitted in original order.
use super::*;
use crate::expr::{Kind, Literal, Op};

struct Leaf<'a> {
    scan: &'a Node,
    columns: Vec<usize>,
    predicates: Vec<Expr>,
}

// Bound every cloned expression, including its tree, before allocating.
fn simple(e: &Expr, remaining: &mut usize) -> bool {
    let Some(n) = remaining.checked_sub(1) else {
        return false;
    };
    *remaining = n;
    match &e.kind {
        Kind::Column(_)
        | Kind::Literal(Literal::Num(_) | Literal::Bool(_) | Literal::Date(_) | Literal::Null) => {
            true
        }
        Kind::Binary(
            Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::And | Op::Or,
            a,
            b,
        ) => simple(a, remaining) && simple(b, remaining),
        Kind::Not(a) | Kind::IsNull(a, _) => simple(a, remaining),
        _ => false,
    }
}

/// An identity expression can retag DECIMAL precision (or widen INTEGER) in
/// Unary without changing its SQL value. Consumers normalize the raw column
/// to the same Type themselves. No expression, sort, filter or unused dependency
/// may disappear when mapping this projection onto the physical scan.
fn columns(node: &Node, input: &Node, plan: &Plan) -> Option<Vec<usize>> {
    if plan.aggregation
        || !plan.order.is_empty()
        || plan.filter.is_some()
        || !plan.groups.is_empty()
        || !plan.aggregates.is_empty()
        || plan.output.len() != node.schema.fields().len()
        || plan.output.len() != plan.types.len()
    {
        return None;
    }
    let result = plan
        .output
        .iter()
        .enumerate()
        .map(|(i, output)| {
            let bind::Output::Expr(Expr {
                kind: Kind::Column(column),
                ty,
            }) = output
            else {
                return None;
            };
            (Type::from_field(input.schema.fields().get(*column)?).ok()? == *ty
                && Type::from_field(node.schema.field(i)).ok()? == *ty
                && plan.types[i] == *ty)
                .then_some(*column)
        })
        .collect::<Option<Vec<_>>>()?;
    let mut dependencies = result.clone();
    dependencies.sort_unstable();
    dependencies.dedup();
    (dependencies == plan.columns).then_some(result)
}

fn leaf<'a>(node: &'a Node, remaining: &mut usize) -> Option<Leaf<'a>> {
    *remaining = remaining.checked_sub(1)?;
    if node.schema.fields().len() > 64 {
        return None;
    }
    match &node.op {
        RelOp::Scan { .. } => Some(Leaf {
            scan: node,
            columns: (0..node.schema.fields().len()).collect(),
            predicates: Vec::new(),
        }),
        RelOp::Rename { input } => leaf(input, remaining),
        RelOp::Project { input, columns } => {
            let mut leaf = leaf(input, remaining)?;
            leaf.columns = columns
                .iter()
                .map(|i| leaf.columns.get(*i).copied())
                .collect::<Option<_>>()?;
            Some(leaf)
        }
        RelOp::Unary { input, plan } => {
            let columns = columns(node, input, plan)?;
            let mut leaf = leaf(input, remaining)?;
            leaf.columns = columns
                .iter()
                .map(|i| leaf.columns.get(*i).copied())
                .collect::<Option<_>>()?;
            Some(leaf)
        }
        RelOp::Filter { input, predicate }
            if simple(predicate, remaining)
                && membership_safety::expression_is_total(predicate, input) =>
        {
            let mut leaf = leaf(input, remaining)?;
            let mut predicate = predicate.clone();
            predicate
                .remap_columns(&leaf.columns.iter().copied().map(Some).collect::<Vec<_>>())
                .ok()?;
            leaf.predicates.push(predicate);
            Some(leaf)
        }
        _ => None,
    }
}

fn mapping(from: &Node, to: &Node) -> Option<Vec<usize>> {
    let (
        RelOp::Scan {
            path: a,
            columns: ac,
            ..
        },
        RelOp::Scan {
            path: b,
            columns: bc,
            ..
        },
    ) = (&from.op, &to.op)
    else {
        return None;
    };
    if a != b {
        return None;
    }
    ac.iter()
        .enumerate()
        .map(|(i, column)| {
            let j = bc.iter().position(|c| c == column)?;
            (from.schema.field(i) == to.schema.field(j)).then_some(j)
        })
        .collect()
}

fn condition(
    leaf: &Leaf<'_>,
    mapping: &[usize],
    residual: Option<&Expr>,
    width: usize,
) -> Option<Expr> {
    let mut expression = residual.cloned().unwrap_or(Expr {
        kind: Kind::Literal(Literal::Bool(true)),
        ty: Type::Bool,
    });
    let map = (0..width)
        .map(Some)
        .chain(leaf.columns.iter().map(|i| Some(width + mapping[*i])))
        .collect::<Vec<_>>();
    expression.remap_columns(&map).ok()?;
    let map = mapping.iter().map(|i| Some(width + *i)).collect::<Vec<_>>();
    for predicate in &leaf.predicates {
        let mut predicate = predicate.clone();
        predicate.remap_columns(&map).ok()?;
        expression = Expr {
            kind: Kind::Binary(Op::And, Box::new(expression), Box::new(predicate)),
            ty: Type::Bool,
        };
    }
    Some(expression)
}

// Fund the payload, per-column metadata and both vectors' replacement capacity
// before retaining a callback batch. The buffers drop before their reservations.
fn retain(output: &mut BufferedOutput, batch: RecordBatch, budget: &Budget) -> Result<()> {
    let bytes = batch
        .num_columns()
        .checked_mul(128)
        .and_then(|n| n.checked_add(1024))
        .and_then(|n| n.checked_add(batch.get_array_memory_size()))
        .ok_or("paired existence retention size overflow")?;
    let credit = budget.reserve(bytes)?;
    output.batches.try_reserve(1).map_err(|e| e.to_string())?;
    output
        .reservations
        .try_reserve(1)
        .map_err(|e| e.to_string())?;
    output.batches.push(batch);
    output.reservations.push(credit);
    Ok(())
}

// Gather leases already own the payload charge. Reserve only the retention
// envelope, then transfer the leases without a second payload reservation.
pub(super) fn retain_funded(
    output: &mut BufferedOutput,
    batch: RecordBatch,
    credits: Vec<Reservation>,
    budget: &Budget,
) -> Result<()> {
    let mut prepare = || {
        let bytes = batch
            .num_columns()
            .checked_mul(128)
            .and_then(|n| n.checked_add(1024))
            .ok_or("paired existence retention size overflow")?;
        let credit = budget.reserve(bytes)?;
        output.batches.try_reserve(1).map_err(|e| e.to_string())?;
        output
            .reservations
            .try_reserve(1)
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(credit)
    };
    match prepare() {
        Ok(mut credit) => {
            for lease in credits {
                credit.absorb(lease);
            }
            output.batches.push(batch);
            output.reservations.push(credit);
            Ok(())
        }
        Err(error) => {
            // The buffers must die before their gather leases on refusal too.
            drop(batch);
            drop(credits);
            Err(error)
        }
    }
}

/// Both original joins must qualify for total reverse-existence traversal.
/// The wider ORIGINAL scan must cover the other; no new physical columns are read.
pub(super) fn try_execute(
    node: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    if !ctx.options.relational_optimizations || ctx.spill.is_some() || ctx.demand_cache.is_none() {
        return Ok(false);
    }
    let RelOp::Join {
        left: inner,
        right: r2,
        kind: k2,
        keys: keys2,
        residual: p2,
    } = &node.op
    else {
        return Ok(false);
    };
    let RelOp::Join {
        left,
        right: r1,
        kind: k1,
        keys: keys1,
        residual: p1,
    } = &inner.op
    else {
        return Ok(false);
    };
    if !matches!(k1, JoinKind::Semi | JoinKind::Anti)
        || !matches!(k2, JoinKind::Semi | JoinKind::Anti)
        || keys1.is_empty()
        || keys1.len() != keys2.len()
        || keys1.len() > 4
        || left.schema.fields().len() > 64
        || inner.schema != left.schema
        || node.schema != left.schema
        || !membership_safety::join_conditions_are_total(node)
        || !membership_safety::join_conditions_are_total(inner)
        || !membership_safety::input_is_total(left)
        || !membership_safety::input_is_total(r1)
        || !membership_safety::input_is_total(r2)
    {
        return Ok(false);
    }
    let mut remaining = 64;
    if p1.iter().chain(p2).any(|e| !simple(e, &mut remaining)) {
        return Ok(false);
    }
    let budget = ctx.budget.clone();
    // At most 64 schema columns/tree nodes per leaf and four key expressions.
    let Ok(_descriptors) = budget.reserve(128 << 10) else {
        return Ok(false);
    };
    let Some(a) = leaf(r1, &mut remaining) else {
        return Ok(false);
    };
    let Some(b) = leaf(r2, &mut remaining) else {
        return Ok(false);
    };
    let (scan, ma, mb) = if let Some(ma) = mapping(a.scan, b.scan) {
        (
            b.scan,
            ma,
            (0..b.scan.schema.fields().len()).collect::<Vec<_>>(),
        )
    } else if let Some(mb) = mapping(b.scan, a.scan) {
        (
            a.scan,
            (0..a.scan.schema.fields().len()).collect::<Vec<_>>(),
            mb,
        )
    } else {
        return Ok(false);
    };
    if [a.scan, b.scan].iter().any(|scan| {
        let id = *scan as *const Node as usize;
        ctx.scan_filters.contains_key(&id)
            || ctx.static_filters.contains_key(&id)
            || ctx.scan_transforms.contains_key(&id)
            || ctx.lane_transforms.contains_key(&id)
    }) {
        return Ok(false);
    }
    let mut right_keys = Vec::with_capacity(keys1.len());
    for ((l1, v1), (l2, v2)) in keys1.iter().zip(keys2) {
        let (Kind::Column(_), Kind::Column(i), Kind::Column(j)) = (&l1.kind, &v1.kind, &v2.kind)
        else {
            return Ok(false);
        };
        if l1 != l2 || v1.ty != v2.ty || ma[a.columns[*i]] != mb[b.columns[*j]] {
            return Ok(false);
        }
        right_keys.push(Expr {
            kind: Kind::Column(ma[a.columns[*i]]),
            ty: v1.ty.clone(),
        });
    }
    let Some(c1) = condition(&a, &ma, p1.as_ref(), left.schema.fields().len()) else {
        return Ok(false);
    };
    let Some(c2) = condition(&b, &mb, p2.as_ref(), left.schema.fields().len()) else {
        return Ok(false);
    };
    ctx.demand_cache
        .unwrap()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut probe = BufferedOutput {
        batches: Vec::new(),
        reservations: Vec::new(),
    };
    left.run(ctx, &mut |batch| retain(&mut probe, batch, &budget))?;
    let rows = probe.batches.iter().try_fold(0usize, |rows, batch| {
        rows.checked_add(batch.num_rows())
            .ok_or("paired existence row count overflow")
    })?;
    // Filter/join estimates can share the same opaque selectivity guess. The
    // actual retained count is available without another pass, and bounds both
    // match arrays (the first existence join can only remove occurrences).
    // Right estimates affect cost only; neither predicate nor match semantics
    // depends on them. Keep the original fourfold amortization threshold.
    if rows.saturating_mul(4) >= r1.estimated_rows || rows.saturating_mul(4) >= r2.estimated_rows {
        let mut intermediate = BufferedOutput {
            batches: Vec::new(),
            reservations: Vec::new(),
        };
        inner.hash_join_with_probe(
            ctx,
            emit,
            left,
            r1,
            *k1,
            keys1,
            p1.as_ref(),
            None,
            Some(probe),
            Some(&mut intermediate),
        )?;
        node.hash_join_with_probe(
            ctx,
            emit,
            inner,
            r2,
            *k2,
            keys2,
            p2.as_ref(),
            None,
            Some(intermediate),
            None,
        )?;
        return Ok(true);
    }
    if rows > u32::MAX as usize {
        return Err("resource-limit: join build indices".into());
    }
    let retained = funded_concat::for_join(
        &left.schema,
        &probe.batches,
        &budget,
        &ctx.options.cancellation,
    )?;
    drop(probe);
    let build = retained.batch();
    let values = input(build, &budget, keys1.iter().map(|(e, _)| e))?;
    let keys = keys1
        .iter()
        .map(|(e, _)| e.eval(&values, &budget))
        .collect::<Result<Vec<_>>>()?;
    let encoded = crate::keys::Keys::with_text_encoding(&keys, rows, &budget, true)?;
    drop(keys);
    drop(values);
    let _state = budget.reserve(rows.saturating_mul(8).saturating_add(1024))?;
    let mut index = crate::join_index::JoinIndex::new(&encoded, rows, &budget, true)?;
    let mut next = vec![u32::MAX; rows];
    let mut matched1 = vec![false; rows];
    let mut matched2 = vec![false; rows];
    index.populate(&encoded, &mut next);
    // JoinFilter only needs types/positions, so share immutable field metadata
    // instead of deep-cloning arbitrary names and metadata into a nullable schema.
    let schema = Arc::new(Schema::new(
        left.schema
            .fields()
            .iter()
            .chain(scan.schema.fields())
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let conditions = [
        crate::join_filter::JoinFilter::new(&c1, &schema)?,
        crate::join_filter::JoinFilter::new(&c2, &schema)?,
    ];
    let quantum = ctx.options.batch_size;
    let _scratch = budget.reserve(quantum.saturating_mul(32).saturating_add(4096))?;
    let mut li = Vec::with_capacity(quantum);
    let mut ri = Vec::with_capacity(quantum);
    let installed = runtime_filter::install(ctx, scan, right_keys.clone(), &index)?;
    let cache = ctx.filtered_reuse.take();
    let mut seconds = 0.;
    let mut candidates = 0;
    let mut gathered = 0;
    let cancellation = ctx.options.cancellation.clone();
    let result = scan.run(ctx, &mut |batch| {
        let start = Instant::now();
        let values = input(&batch, &budget, &right_keys)?;
        let keys = right_keys
            .iter()
            .map(|e| e.eval(&values, &budget))
            .collect::<Result<Vec<_>>>()?;
        let encoded =
            crate::keys::Keys::with_text_encoding(&keys, batch.num_rows(), &budget, true)?;
        let mut flush = |li: &mut Vec<u32>, ri: &mut Vec<u32>| -> Result<()> {
            if li.is_empty() {
                return Ok(());
            }
            cancellation.check()?;
            let ls = UInt32Array::from(li.clone());
            let rs = UInt32Array::from(ri.clone());
            for (condition, matched) in conditions.iter().zip([&mut matched1, &mut matched2]) {
                let (mask, bytes) = condition.eval(build, &batch, &ls, &rs, &budget)?;
                candidates += li.len();
                gathered += bytes;
                for (row, yes) in li.iter().zip(boolean(&mask.array).iter()) {
                    if yes == Some(true) {
                        matched[*row as usize] = true;
                    }
                }
            }
            li.clear();
            ri.clear();
            Ok(())
        };
        for row in 0..batch.num_rows() {
            if !encoded.valid(row) {
                continue;
            }
            if let Some(first) = index.get(encoded.row(row)) {
                let mut slot = *first;
                while slot != u32::MAX {
                    li.push(slot);
                    ri.push(row as u32);
                    if li.len() == quantum {
                        flush(&mut li, &mut ri)?;
                    }
                    slot = next[slot as usize];
                }
            }
        }
        flush(&mut li, &mut ri)?;
        seconds += start.elapsed().as_secs_f64();
        Ok(())
    });
    ctx.filtered_reuse = cache;
    if let Some(installed) = installed {
        installed.restore(ctx);
    }
    result?;
    let mut keep = Vec::with_capacity(quantum);
    let mut output_rows = 0;
    let mut flush = |keep: &mut Vec<u32>| -> Result<()> {
        if keep.is_empty() {
            return Ok(());
        }
        ctx.options.cancellation.check()?;
        let indices = UInt32Array::from(keep.clone());
        let bytes = build.columns().iter().try_fold(1024usize, |bytes, array| {
            bytes
                .checked_add(crate::join_filter::gather_bytes(array, &indices)?)
                .ok_or_else(|| "join output size overflow".to_owned())
        })?;
        let _output = budget.reserve(bytes)?;
        let arrays = build
            .columns()
            .iter()
            .map(|array| take(array, &indices, None).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>>>()?;
        output_rows += keep.len();
        emit(RecordBatch::try_new(node.schema.clone(), arrays).map_err(|e| e.to_string())?)?;
        keep.clear();
        Ok(())
    };
    for row in 0..rows {
        if matched1[row] == (*k1 == JoinKind::Semi) && matched2[row] == (*k2 == JoinKind::Semi) {
            keep.push(row as u32);
            if keep.len() == quantum {
                flush(&mut keep)?;
            }
        }
    }
    flush(&mut keep)?;
    ctx.metrics.paired_existence_scans += 1;
    ctx.metrics.reverse_existence_joins += 2;
    ctx.metrics.join_build_rows += rows;
    ctx.metrics.join_output_rows += output_rows;
    ctx.metrics.join_seconds += seconds;
    ctx.metrics.residual_candidate_rows += candidates;
    ctx.metrics.residual_gathered_bytes += gathered;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funded_retention_transfers_payload_credit_without_double_charging() {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["wide".repeat(16384)]));
        let indices = UInt32Array::from(vec![0, 0]);
        let payload = crate::join_filter::gather_bytes(&text, &indices).unwrap();
        let envelope = 128 + 1024;
        let budget = Budget::new(payload + envelope);
        let (array, credit) = crate::join_filter::gather(&text, &indices, &budget).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            text.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let mut output = BufferedOutput {
            batches: Vec::new(),
            reservations: Vec::new(),
        };
        retain_funded(&mut output, batch, vec![credit], &budget).unwrap();
        assert_eq!(budget.used(), payload + envelope);
        assert_eq!(budget.peak(), payload + envelope);
        assert_eq!(output.reservations.len(), 1);
        assert_eq!(output.reservations[0].bytes(), payload + envelope);
        assert_eq!(output.batches[0].num_rows(), 2);
        drop(output);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn retained_batch_denial_preserves_prior_credit_until_payload_drop() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            arrow::datatypes::DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(1)]))],
        )
        .unwrap();
        let bytes = batch.get_array_memory_size() + 128 + 1024;
        let budget = Budget::new(bytes * 2 - 1);
        let mut output = BufferedOutput {
            batches: Vec::new(),
            reservations: Vec::new(),
        };
        retain(&mut output, batch.clone(), &budget).unwrap();
        assert_eq!(budget.used(), bytes);
        assert_eq!(
            retain(&mut output, batch.clone(), &budget).unwrap_err(),
            format!(
                "resource-limit: accounted memory {} exceeds budget {}; requested {bytes}, live {bytes}",
                bytes * 2,
                bytes * 2 - 1
            )
        );
        assert_eq!(output.batches, vec![batch]);
        assert_eq!(output.reservations.len(), 1);
        assert_eq!(budget.used(), bytes);
        drop(output);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}
