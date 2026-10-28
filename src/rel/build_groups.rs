//! Build-key interning for an ordinary, encounter-ordered post-join aggregate.
//! The private materialization executes at the original build position. No
//! aggregate input, join, membership test or arithmetic update moves across it.
use super::*;
use crate::expr::{Kind, Literal, Op};
use arrow::datatypes::DataType;

mod late_payload;
mod parallel;
pub(super) use late_payload::try_ordered_limit;

pub(super) const SOURCE_ID: usize = usize::MAX;
const ID_NAME: &str = "__assay_build_group_id";

#[derive(Clone)]
pub(super) struct Request {
    groups: Vec<Expr>,
    dictionary: Arc<Mutex<Option<Dictionary>>>,
}
struct Dictionary {
    build: funded_concat::JoinBuild,
    representatives: UInt32Array,
    _credit: Reservation,
}
fn with_id(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .cloned()
            .chain([Arc::new(Type::Int.field(ID_NAME, false))])
            .collect::<Vec<_>>(),
    ))
}

/// A magnitude bound in the expression's own scale. Every intermediate is
/// bounded, so decimal multiplication/addition cannot fail before the ID lookup.
fn magnitude(expr: &Expr, schema: &Schema) -> Option<i128> {
    let bound = match &expr.kind {
        Kind::Column(i) => match schema.field(*i).data_type() {
            DataType::Int32 => 1_i128 << 31,
            DataType::Int64 => 1_i128 << 63,
            DataType::Decimal128(p, _) => 10_i128.checked_pow(u32::from(*p))?.checked_sub(1)?,
            _ => return None,
        },
        Kind::Literal(Literal::Num(n)) => n.checked_abs()?,
        Kind::Binary(op @ (Op::Add | Op::Sub | Op::Mul), a, b) => {
            let amax = magnitude(a, schema)?;
            let bmax = magnitude(b, schema)?;
            if *op == Op::Mul {
                amax.checked_mul(bmax)?
            } else {
                let scale = expr.ty.scale().ok()?;
                amax.checked_mul(crate::types::factor(scale - a.ty.scale().ok()?).ok()?)?
                    .checked_add(
                        bmax.checked_mul(crate::types::factor(scale - b.ty.scale().ok()?).ok()?)?,
                    )?
            }
        }
        _ => return None,
    };
    (bound < 10_i128.pow(38)).then_some(bound)
}
fn total_input(expr: &Expr, schema: &Schema) -> bool {
    expr.ty != Type::Float
        && (matches!(expr.kind, Kind::Column(_) | Kind::Literal(_))
            || magnitude(expr, schema).is_some())
}

// No pre-existing materialization identity or LEFT subtree is rewritten. This
// also prevents nesting the private SOURCE_ID and keeps consumer pointer maps
// outside the cloned plan. Other expressions remain at their original nodes.
fn supported_tree(node: &Node) -> bool {
    match &node.op {
        RelOp::Scan { .. } | RelOp::Empty | RelOp::One => true,
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => supported_tree(input),
        RelOp::Join {
            left, right, kind, ..
        } => *kind != JoinKind::Left && supported_tree(left) && supported_tree(right),
        RelOp::Mark { left, right, .. } => supported_tree(left) && supported_tree(right),
        _ => false,
    }
}

/// Locate a single build containing every direct grouping column. The returned
/// columns are in that build's schema; NULLs use grouping (not join) equality.
fn build_columns(node: &Node, columns: &[usize]) -> Option<Vec<usize>> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Filter { input, .. } => build_columns(input, columns),
        RelOp::Project {
            input,
            columns: selected,
        } => build_columns(
            input,
            &columns.iter().map(|i| selected[*i]).collect::<Vec<_>>(),
        ),
        RelOp::Mark { left, .. } if columns.iter().all(|i| *i < left.schema.fields().len()) => {
            build_columns(left, columns)
        }
        RelOp::Join {
            left,
            kind: JoinKind::Inner,
            keys,
            residual: None,
            ..
        } if !keys.is_empty() => columns
            .iter()
            .map(|i| i.checked_sub(left.schema.fields().len()))
            .collect(),
        _ => None,
    }
}
fn build_node(node: &Node) -> &Node {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } | RelOp::Filter { input, .. } => {
            build_node(input)
        }
        RelOp::Mark { left, .. } => build_node(left),
        RelOp::Join { right, .. } => right,
        _ => unreachable!("build_columns admitted the path"),
    }
}

/// Append one private field without changing any original column index. A Mark
/// needs a projection because its synthetic Boolean always follows its left.
fn append_id(node: Node) -> Node {
    let schema = with_id(&node.schema);
    let estimated_rows = node.estimated_rows;
    let op = match node.op {
        RelOp::Rename { input } => RelOp::Rename {
            input: Box::new(append_id(*input)),
        },
        RelOp::Filter { input, predicate } => RelOp::Filter {
            input: Box::new(append_id(*input)),
            predicate,
        },
        RelOp::Project { input, mut columns } => {
            columns.push(input.schema.fields().len());
            RelOp::Project {
                input: Box::new(append_id(*input)),
                columns,
            }
        }
        RelOp::Mark {
            left,
            right,
            value,
            negated,
        } => {
            let width = left.schema.fields().len();
            let left = append_id(*left);
            let mark_schema = Arc::new(Schema::new(
                left.schema
                    .fields()
                    .iter()
                    .cloned()
                    .chain([node.schema.fields()[width].clone()])
                    .collect::<Vec<_>>(),
            ));
            let marked = Node {
                schema: mark_schema,
                estimated_rows,
                op: RelOp::Mark {
                    left: Box::new(left),
                    right,
                    value,
                    negated,
                },
            };
            RelOp::Project {
                input: Box::new(marked),
                columns: (0..width).chain([width + 1, width]).collect(),
            }
        }
        RelOp::Join {
            left,
            right,
            kind,
            keys,
            residual,
        } => {
            let right = Node {
                schema: with_id(&right.schema),
                estimated_rows: right.estimated_rows,
                op: RelOp::Materialize {
                    id: SOURCE_ID,
                    input: right,
                    shared_scan: false,
                    forced: false,
                },
            };
            RelOp::Join {
                left,
                right: Box::new(right),
                kind,
                keys,
                residual,
            }
        }
        _ => unreachable!("build_columns admitted the path"),
    };
    Node {
        schema,
        estimated_rows,
        op,
    }
}

// Count variable payloads without allocating. Saturation makes an unrepresentable
// optional plan decline. The multiplier pays for clone/prune replacement peaks,
// schemas, vectors, BTreeSet nodes, remapping arrays and request descriptors.
fn expression_bytes(expr: &Expr) -> usize {
    let payload = match &expr.kind {
        Kind::Literal(Literal::Text(s)) => s.len(),
        Kind::Literal(Literal::Binary(b)) => b.len(),
        Kind::Binary(_, a, b)
        | Kind::Like(a, b, _)
        | Kind::Position(a, b)
        | Kind::NullIf(a, b)
        | Kind::Pack(a, b) => expression_bytes(a).saturating_add(expression_bytes(b)),
        Kind::Function(name, a) => name.len().saturating_add(expression_bytes(a)),
        Kind::Cast(a)
        | Kind::DialectCast(a, _)
        | Kind::CastVarchar(a, _)
        | Kind::CastDecimal(a, _, _)
        | Kind::Not(a)
        | Kind::Neg(a)
        | Kind::IsNull(a, _)
        | Kind::Year(a)
        | Kind::Substring(a, _, _) => expression_bytes(a),
        Kind::Case(branches, other) => {
            branches.iter().fold(expression_bytes(other), |n, (a, b)| {
                n.saturating_add(expression_bytes(a))
                    .saturating_add(expression_bytes(b))
            })
        }
        _ => 0,
    };
    256_usize.saturating_add(payload)
}
fn plan_bytes(plan: &Plan) -> usize {
    let mut bytes = plan.names.iter().fold(1024_usize, |n, s| {
        n.saturating_add(s.len()).saturating_add(256)
    });
    bytes = bytes
        .saturating_add(plan.columns.len().saturating_mul(64))
        .saturating_add(plan.order.len().saturating_mul(64));
    bytes = bytes.saturating_add(plan.table.as_ref().map_or(0, String::len));
    bytes = bytes.saturating_add(plan.path.as_ref().map_or(0, |p| p.as_os_str().len()));
    for e in plan
        .filter
        .iter()
        .chain(&plan.groups)
        .chain(plan.aggregates.iter().filter_map(|a| a.input.as_ref()))
        .chain(plan.output.iter().filter_map(|o| {
            if let bind::Output::Expr(e) = o {
                Some(e)
            } else {
                None
            }
        }))
    {
        bytes = bytes.saturating_add(expression_bytes(e));
    }
    bytes
        .saturating_add(plan.aggregates.len().saturating_mul(256))
        .saturating_add(plan.output.len().saturating_mul(256))
}
pub(super) fn node_bytes(node: &Node) -> usize {
    let schema = node.schema.fields().iter().fold(1024_usize, |n, f| {
        f.metadata().iter().fold(
            n.saturating_add(1024).saturating_add(f.name().len()),
            |n, (k, v)| {
                n.saturating_add(k.len())
                    .saturating_add(v.len())
                    .saturating_add(128)
            },
        )
    });
    let payload = match &node.op {
        RelOp::RecursiveCte { .. } | RelOp::RecursiveScan { .. } => usize::MAX,
        RelOp::Scan { path, .. } => path.as_os_str().len(),
        RelOp::Rename { input }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => node_bytes(input),
        RelOp::Project { input, columns } => {
            node_bytes(input).saturating_add(columns.len().saturating_mul(64))
        }
        RelOp::Filter { input, predicate } => {
            node_bytes(input).saturating_add(expression_bytes(predicate))
        }
        RelOp::Unary { input, plan } => node_bytes(input).saturating_add(plan_bytes(plan)),
        RelOp::Mark {
            left, right, value, ..
        } => node_bytes(left)
            .saturating_add(node_bytes(right))
            .saturating_add(expression_bytes(value)),
        RelOp::Join {
            left,
            right,
            keys,
            residual,
            ..
        } => keys.iter().fold(
            node_bytes(left)
                .saturating_add(node_bytes(right))
                .saturating_add(residual.as_ref().map_or(0, expression_bytes)),
            |n, (a, b)| {
                n.saturating_add(expression_bytes(a))
                    .saturating_add(expression_bytes(b))
            },
        ),
        RelOp::ScalarDefault {
            input, fallback, ..
        } => node_bytes(input).saturating_add(node_bytes(fallback)),
        RelOp::UnionAll { left, right } => node_bytes(left).saturating_add(node_bytes(right)),
        RelOp::Generate { .. } => usize::MAX, // no scan-owned fused path
        RelOp::One | RelOp::Empty => 0,
        _ => usize::MAX,
    };
    let schema = node.schema.metadata().iter().fold(schema, |n, (k, v)| {
        n.saturating_add(k.len())
            .saturating_add(v.len())
            .saturating_add(128)
    });
    schema.saturating_add(payload)
}

impl Request {
    /// Assign dense IDs once, borrowing encoded keys while interning. Reserve the
    /// complete hash/index peak before allocating and retain source/ID backing.
    pub(super) fn materialize(
        &self,
        input_node: &Node,
        ctx: &mut Context<'_>,
    ) -> Result<Arc<Materialized>> {
        let budget = ctx.budget.clone();
        let cancel = ctx.options.cancellation.clone();
        let mut leases = Vec::new();
        let mut batches = Vec::new();
        input_node.run(ctx, &mut |batch| {
            leases.push(budget.reserve(batch.get_array_memory_size().saturating_add(256))?);
            batches.push(batch);
            Ok(())
        })?;
        let start = Instant::now();
        let (build, tasks) = funded_concat::for_join_parallel(
            &input_node.schema,
            &batches,
            &budget,
            &cancel,
            ctx.options.workers,
        )?;
        ctx.metrics.build_prepare_concat_seconds += start.elapsed().as_secs_f64();
        ctx.metrics.build_prepare_column_tasks += tasks;
        drop(batches);
        drop(leases);
        let batch = build.batch();
        let rows = batch.num_rows();
        if rows > u32::MAX as usize {
            return Err("resource-limit: join build indices".into());
        }
        let descriptors = (batch.num_columns() + self.groups.len())
            .checked_mul(512)
            .and_then(|n| n.checked_add(2048))
            .ok_or("group ID metadata overflow")?;
        let mut credit = budget.reserve(
            rows.checked_mul(80)
                .and_then(|n| n.checked_add(descriptors))
                .ok_or("group ID index overflow")?,
        )?;
        let values = input(batch, &budget, &self.groups)?;
        let keys = self
            .groups
            .iter()
            .map(|e| e.eval(&values, &budget))
            .collect::<Result<Vec<_>>>()?;
        let start = Instant::now();
        let parallel = if self
            .groups
            .iter()
            .all(|e| matches!(e.kind, Kind::Column(_)))
        {
            parallel::try_intern(&keys, rows, &budget, &cancel, ctx.options.workers)?
        } else {
            None
        };
        ctx.metrics.build_group_parallel_dictionaries += usize::from(parallel.is_some());
        let (ids, representatives) = match parallel {
            Some(result) => result,
            None => {
                let encoded = crate::keys::Keys::new(&keys, rows, &budget)?;
                let mut map = HashMap::<&[u8], u32, ahash::RandomState>::with_capacity_and_hasher(
                    rows,
                    ahash::RandomState::new(),
                );
                let mut ids = Vec::with_capacity(rows);
                let mut representatives = Vec::with_capacity(rows);
                for row in 0..rows {
                    if row % 1024 == 0 {
                        cancel.check()?;
                    }
                    let next = representatives.len() as u32;
                    let id = *map.entry(encoded.row(row)).or_insert_with(|| {
                        representatives.push(row as u32);
                        next
                    });
                    ids.push(i64::from(id));
                }
                (ids, representatives)
            }
        };
        ctx.metrics.build_group_dictionary_seconds += start.elapsed().as_secs_f64();
        drop(keys);
        drop(values);
        let mut arrays = batch.columns().to_vec();
        arrays.push(Arc::new(Int64Array::from(ids)));
        let augmented =
            RecordBatch::try_new(with_id(&batch.schema()), arrays).map_err(|e| e.to_string())?;
        let groups = representatives.len();
        let representatives = UInt32Array::from(representatives);
        credit.release_excess(rows * 12 + descriptors);
        *self.dictionary.lock().unwrap() = Some(Dictionary {
            build,
            representatives,
            _credit: credit,
        });
        ctx.metrics.build_group_id_rows += rows;
        ctx.metrics.build_group_id_groups += groups;
        Ok(Arc::new(Materialized {
            budget,
            batches: vec![augmented],
            files: Vec::new(),
            _leases: Vec::new(),
        }))
    }

    /// Aggregate output IDs are in probe encounter order. Resolve only those
    /// selected representatives, including NULL components, without changing any
    /// aggregate values or their finalization order.
    fn output(
        &self,
        batch: RecordBatch,
        prepared: &Prepared,
        id_column: usize,
        budget: &Budget,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let _indices_credit =
            budget.reserve(batch.num_rows().saturating_mul(4).saturating_add(1024))?;
        let guard = self.dictionary.lock().unwrap();
        let dictionary = guard
            .as_ref()
            .ok_or("internal: missing build group dictionary")?;
        let ids = crate::expr::numeric(batch.column(id_column));
        let indices = UInt32Array::from_iter_values(
            ids.values()
                .iter()
                .map(|id| dictionary.representatives.value(*id as usize)),
        );
        let schema = schema(&prepared.plan.names, &prepared.plan.types);
        let mut leases = Vec::new();
        let mut arrays = Vec::with_capacity(batch.num_columns());
        for (i, output) in prepared.plan.output.iter().enumerate() {
            if let bind::Output::Group(group) = output {
                let Kind::Column(column) = self.groups[*group].kind else {
                    unreachable!()
                };
                let (array, lease) = crate::join_filter::gather(
                    dictionary.build.batch().column(column),
                    &indices,
                    budget,
                )?;
                leases.push(lease);
                // The output owner uses wide INTEGER fields; fund the widening
                // peak while retaining the gathered source until it is dropped.
                leases
                    .push(budget.reserve(batch.num_rows().saturating_mul(17).saturating_add(256))?);
                let value = normalize_input(&array, &prepared.plan.types[i], budget)?;
                arrays.push(execute::materialize(
                    &value.array,
                    &execute::physical_output_type(&prepared.plan.types[i], true),
                )?);
            } else {
                arrays.push(batch.column(i).clone());
            }
        }
        let result = RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())?;
        drop(guard);
        emit(result)
    }
}

/// A storage cap from physical input counts, never a selectivity estimate.
/// It is NOT a join-cardinality proof: duplicate fan-out can exceed it, in
/// which case the optional attempt is discarded before output and replayed.
fn distinct_pair_cap(node: &Node) -> Option<usize> {
    match &node.op {
        RelOp::Scan { .. } => Some(node.estimated_rows),
        RelOp::One => Some(1),
        RelOp::Empty => Some(0),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => distinct_pair_cap(input),
        RelOp::Mark { left, .. } => distinct_pair_cap(left),
        RelOp::Join { left, right, .. } => {
            Some(distinct_pair_cap(left)?.max(distinct_pair_cap(right)?))
        }
        _ => None,
    }
}

pub(super) fn try_execute(
    prepared: &Prepared,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    Ok(try_execute_inner(prepared, child, ctx, emit, None)?.map(|(metrics, _)| metrics))
}

// The optional selection returns an independently funded result. Only the
// ordinary streaming path invokes emit here, so selection failures can fall
// back before any consumer state has changed. Admission and finalization stay
// shared, and this scope drops all optional plans and dictionary ownership.
fn try_execute_inner(
    prepared: &Prepared,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    selection: Option<late_payload::Selection<'_>>,
) -> Result<Option<(Metrics, Option<late_payload::Selected>)>> {
    let plan = &prepared.plan;
    if !ctx.options.relational_optimizations
        || ctx.options.error_replay.canonical()
        || ctx.spill.is_some()
        || ctx.build_groups.is_some()
        || ctx.materialized.contains_key(&SOURCE_ID)
        || !ctx.scan_filters.is_empty()
        || !ctx.static_filters.is_empty()
        || !ctx.scan_transforms.is_empty()
        || !ctx.lane_transforms.is_empty()
        || !ctx.replay_transforms.is_empty()
        || !plan.aggregation
        || plan.groups.is_empty()
        || plan.filter.is_some()
        || !plan.order.is_empty()
        || !plan.groups.iter().any(|e| e.ty == Type::Text)
        || !supported_tree(child)
        || plan
            .output
            .iter()
            .any(|o| matches!(o, bind::Output::Expr(_)))
        || plan.aggregates.iter().any(|a| {
            a.input
                .as_ref()
                .is_some_and(|e| !total_input(e, &child.schema))
        })
    {
        return Ok(None);
    }
    let budget = ctx.budget.clone();
    // Cloned plans, expressions and descriptors are optional work too. Decline
    // before cloning if this credit cannot be obtained.
    let Ok(_plan_credit) = budget.reserve(
        node_bytes(child)
            .saturating_add(plan_bytes(plan))
            .saturating_mul(16)
            .saturating_add(64 * 1024),
    ) else {
        return Ok(None);
    };
    let Some(columns) = plan
        .groups
        .iter()
        .map(|e| {
            if let Kind::Column(i) = e.kind {
                Some(i)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let Some(columns) = build_columns(child, &columns) else {
        return Ok(None);
    };
    let source = build_node(child);
    // Exact logical types and flat physical fields: no coercion or floating-key
    // canonicalization is introduced by interning unobserved build rows.
    if columns.iter().zip(&plan.groups).any(|(i, e)| {
        !matches!(
            e.ty,
            Type::Int | Type::Decimal(_) | Type::Text | Type::Bool | Type::Date | Type::Null
        ) || Type::from_field(source.schema.field(*i)).ok().as_ref() != Some(&e.ty)
    }) {
        return Ok(None);
    }
    let Some(id_column) = selection.as_ref().map_or_else(
        || {
            plan.output
                .iter()
                .position(|o| matches!(o, bind::Output::Group(_)))
        },
        |selection| selection.id_column(plan),
    ) else {
        return Ok(None);
    };
    let request = Request {
        groups: columns
            .iter()
            .zip(&plan.groups)
            .map(|(i, e)| Expr {
                kind: Kind::Column(*i),
                ty: e.ty.clone(),
            })
            .collect(),
        dictionary: Arc::new(Mutex::new(None)),
    };
    let mut numeric = plan.clone();
    numeric.groups = vec![Expr {
        kind: Kind::Column(child.schema.fields().len()),
        ty: Type::Int,
    }];
    for (i, output) in numeric.output.iter_mut().enumerate() {
        if matches!(output, bind::Output::Group(_)) {
            *output = bind::Output::Group(0);
            numeric.types[i] = Type::Int;
        }
    }
    let mut needed = std::collections::BTreeSet::new();
    numeric.groups[0].columns(&mut needed);
    for a in &numeric.aggregates {
        if let Some(e) = &a.input {
            e.columns(&mut needed);
        }
    }
    numeric.columns = needed.into_iter().collect();
    let root = Node {
        schema: schema(&numeric.names, &numeric.types),
        estimated_rows: child.estimated_rows,
        op: RelOp::Unary {
            plan: numeric,
            input: Box::new(append_id(child.clone())),
        },
    };
    let root = crate::prune::prune_build_groups(root)?;
    let RelOp::Unary { plan, input } = root.op else {
        unreachable!()
    };
    let numeric = Prepared {
        auto_simple: None,
        internal_output: true,
        plan,
        schema: input.schema.clone(),
        relational: None,
    };
    ctx.build_groups = Some(request.clone());
    let emitted = std::cell::Cell::new(false);
    let mut output = |batch| {
        emitted.set(true);
        emit(batch)
    };
    let result = if let Some(selection) = selection {
        selection.execute(&request, (prepared, id_column), ctx, &mut |ctx, send| {
            run_aggregate(&numeric, &input, child, &request, ctx, send)
        })
    } else {
        run_aggregate(&numeric, &input, child, &request, ctx, &mut |batch| {
            request.output(batch, prepared, id_column, &budget, &mut output)
        })
        .map(|metrics| (metrics, None))
    };
    // Cache batches share the dictionary backing; release them before its lease.
    ctx.materialized.remove(&SOURCE_ID);
    ctx.build_groups = None;
    match result {
        Ok((mut metrics, selected)) => {
            metrics.build_group_id_aggregates += 1;
            Ok(Some((metrics, selected)))
        }
        Err(error) if !emitted.get() && error.starts_with("resource-limit: accounted memory") => {
            ctx.metrics.build_group_id_fallbacks += 1;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Both output owners consume the identical stream, including every completion
/// check. A DISTINCT capacity miss replays only before its first output batch.
fn run_aggregate(
    numeric: &Prepared,
    input: &Node,
    child: &Node,
    request: &Request,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    let options = ctx.options;
    let budget = ctx.budget.clone();
    let produced = std::cell::Cell::new(false);
    let mut output = |batch| {
        produced.set(true);
        emit(batch)
    };
    let mut result = execute::execute_bounded_distinct_stream(
        numeric,
        options,
        budget.clone(),
        distinct_pair_cap(child).unwrap_or(0),
        |send| input.run(ctx, send),
        &mut output,
    );
    if !produced.get()
        && result
            .as_ref()
            .is_err_and(|error| error == execute::SORTED_DISTINCT_DECLINED)
    {
        // No values escaped. Drop the candidate's entire materialization and
        // dictionary, then replay this same retained plan with fresh hash state.
        ctx.materialized.remove(&SOURCE_ID);
        *request.dictionary.lock().unwrap() = None;
        ctx.metrics.sorted_distinct_fallbacks += 1;
        result = execute::execute_aggregate_stream(
            numeric,
            options,
            budget.clone(),
            |send| input.run(ctx, send),
            &mut output,
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn context<'a>(options: &'a Options, budget: &Budget) -> Context<'a> {
        Context {
            filtered_reuse: None,
            build_groups: None,
            demand_cache: None,
            shared_attempt: None,
            options,
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
        }
    }
    pub(super) fn request() -> Request {
        Request {
            groups: vec![
                Expr {
                    kind: Kind::Column(0),
                    ty: Type::Text,
                },
                Expr {
                    kind: Kind::Column(1),
                    ty: Type::Int,
                },
            ],
            dictionary: Arc::new(Mutex::new(None)),
        }
    }
    pub(super) fn source(options: &Options) -> Node {
        *super::super::prepare("SELECT 'b' AS g, 2 AS k UNION ALL SELECT NULL AS g, 1 AS k UNION ALL SELECT 'b' AS g, 2 AS k UNION ALL SELECT NULL AS g, NULL AS k", &HashMap::new(),options).unwrap().relational.unwrap()
    }
    #[test]
    fn midflight_dictionary_cancellation_never_publishes_or_emits() {
        let rows = 65_543;
        let batch = RecordBatch::try_new(
            schema(&["g".into(), "k".into()], &[Type::Text, Type::Int]),
            vec![
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|i| if i % 13 == 0 { None } else { Some("é\0雪") }),
                )),
                Arc::new(
                    Decimal128Array::from_iter(
                        (0..rows).map(|i| (i % 17 != 0).then_some((i % 997) as i128)),
                    )
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let budget = Budget::new(128 << 20);
        let stored = Arc::new(
            Materialized::from_test_batches(
                (0..rows)
                    .step_by(8192)
                    .map(|start| batch.slice(start, (rows - start).min(8192)))
                    .collect(),
                &budget,
            )
            .unwrap(),
        );
        let input = Node {
            schema: batch.schema(),
            estimated_rows: rows,
            op: RelOp::Materialize {
                id: 0,
                input: Box::new(Node {
                    schema: batch.schema(),
                    estimated_rows: 0,
                    op: RelOp::Empty,
                }),
                shared_scan: false,
                forced: false,
            },
        };
        let baseline = budget.used();
        let mut expected = None;
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                ..Options::default()
            };
            let mut ctx = context(&options, &budget);
            ctx.materialized.insert(0, stored.clone());
            let request = request();
            let output = request.materialize(&input, &mut ctx).unwrap();
            let actual = output.batches[0].clone();
            if let Some(expected) = &expected {
                assert_eq!(&actual, expected);
            } else {
                expected = Some(actual);
            }
            drop(output);
            drop(request);
            drop(ctx);
            assert_eq!(budget.used(), baseline);
        }
        drop(expected);
        for workers in [2, 4, 10] {
            for point in ["dictionary allocated", "dictionary merge"] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                options.cancellation.2.arm(point);
                let mut ctx = context(&options, &budget);
                ctx.materialized.insert(0, stored.clone());
                let request = request();
                ctx.build_groups = Some(request.clone());
                let root = Node {
                    schema: with_id(&input.schema),
                    estimated_rows: rows,
                    op: RelOp::Materialize {
                        id: SOURCE_ID,
                        input: Box::new(input.clone()),
                        shared_scan: false,
                        forced: false,
                    },
                };
                let mut emitted = 0;
                let error = root
                    .run(&mut ctx, &mut |_| {
                        emitted += 1;
                        Ok(())
                    })
                    .unwrap_err();
                assert_eq!(error, "cancelled: query execution");
                assert_eq!(emitted, 0);
                assert!(request.dictionary.lock().unwrap().is_none());
                assert!(!ctx.materialized.contains_key(&SOURCE_ID));
                options.cancellation.2.assert_drained(&budget, baseline);
            }
        }
        drop(batch);
        drop(stored);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dictionary_owns_duplicate_null_ids_and_releases_on_callback_cancellation() {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let budget = Budget::new(8 << 20);
                {
                    let mut ctx = context(&options, &budget);
                    let request = request();
                    let stored = request.materialize(&source(&options), &mut ctx).unwrap();
                    let ids = stored.batches[0]
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(ids.values().as_ref(), &[0, 1, 0, 2]);
                    let live_bytes = stored.batches[0].get_array_memory_size();
                    assert!(budget.used() >= live_bytes);
                    assert_eq!(ctx.metrics.build_group_id_rows, 4);
                    assert_eq!(ctx.metrics.build_group_id_groups, 3);
                    let prepared = Prepared {
                        auto_simple: None,
                        internal_output: true,
                        relational: None,
                        schema: stored.batches[0].schema(),
                        plan: Plan {
                            table: None,
                            path: None,
                            columns: vec![0, 1],
                            filter: None,
                            groups: request.groups.clone(),
                            aggregates: vec![],
                            output: vec![bind::Output::Group(0), bind::Output::Group(1)],
                            names: vec!["g".into(), "k".into()],
                            types: vec![Type::Text, Type::Int],
                            order: vec![],
                            aggregation: true,
                            projection_pushdown: true,
                        },
                    };
                    let ids: ArrayRef = Arc::new(
                        Decimal128Array::from(vec![2, 0, 1])
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    );
                    let batch = RecordBatch::try_new(
                        schema(&prepared.plan.names, &[Type::Int, Type::Int]),
                        vec![ids.clone(), ids],
                    )
                    .unwrap();
                    let error = request
                        .output(batch, &prepared, 0, &budget, &mut |batch| {
                            let strings = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            assert_eq!(
                                strings.iter().collect::<Vec<_>>(),
                                vec![None, Some("b"), None]
                            );
                            assert_eq!(
                                crate::expr::numeric(batch.column(1))
                                    .iter()
                                    .collect::<Vec<_>>(),
                                vec![None, Some(2), Some(1)]
                            );
                            assert!(budget.used() >= live_bytes + batch.get_array_memory_size());
                            options.cancellation.cancel();
                            options.cancellation.check()
                        })
                        .unwrap_err();
                    assert_eq!(error, "cancelled: query execution");
                    drop(stored);
                }
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.peak_workers(), 0);
            }
        }
    }
    #[test]
    fn dictionary_reservation_refusal_and_cancelled_build_leave_no_state_or_leases() {
        for limit in [0, 1024, 4096] {
            let options = Options::default();
            let budget = Budget::new(limit);
            {
                let mut ctx = context(&options, &budget);
                let request = request();
                let error = request
                    .materialize(&source(&options), &mut ctx)
                    .err()
                    .unwrap();
                assert!(
                    error.starts_with("resource-limit: accounted memory"),
                    "{error}"
                );
                assert!(request.dictionary.lock().unwrap().is_none());
            }
            assert_eq!(budget.used(), 0);
        }
        let options = Options::default();
        let source = source(&options);
        options.cancellation.cancel();
        let budget = Budget::new(1 << 20);
        let mut ctx = context(&options, &budget);
        let request = request();
        assert_eq!(
            request.materialize(&source, &mut ctx).err().unwrap(),
            "cancelled: query execution"
        );
        assert!(request.dictionary.lock().unwrap().is_none());
        assert_eq!(budget.used(), 0);
    }
}
