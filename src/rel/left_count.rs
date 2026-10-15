//! COUNT-only reduction of a residual-free LEFT equijoin. The build retains
//! counts per exact key, never duplicate payload rows. Group encounters follow
//! the join: each probe row contributes its matches or one NULL-extended miss.
use super::*;
use crate::{
    bind::Aggregate,
    expr::{Kind, Literal, Op, Value},
};
mod parallel;

fn total_filter(expr: &Expr) -> bool {
    match &expr.kind {
        Kind::Binary(Op::And | Op::Or, a, b) => total_filter(a) && total_filter(b),
        Kind::Not(a) => total_filter(a),
        Kind::Like(a, b, _) => {
            a.ty == Type::Text
                && b.ty == Type::Text
                && matches!(a.kind, Kind::Column(_))
                && matches!(b.kind, Kind::Literal(Literal::Text(_)))
        }
        _ => crate::scan_predicate::total(expr),
    }
}

fn total_source(node: &Node, ctx: &Context<'_>) -> bool {
    let id = node as *const Node as usize;
    // Do not bypass a consumer-specific transform or a shared execution boundary.
    if ctx.scan_filters.contains_key(&id)
        || ctx.static_filters.contains_key(&id)
        || ctx.scan_transforms.contains_key(&id)
        || ctx.replay_transforms.contains_key(&id)
    {
        return false;
    }
    match &node.op {
        RelOp::Scan { .. } | RelOp::Empty => true,
        RelOp::Rename { input } | RelOp::Project { input, .. } => total_source(input, ctx),
        RelOp::Filter { predicate, input } => total_filter(predicate) && total_source(input, ctx),
        _ => false,
    }
}

// Pruning can project away join-only columns between the join and aggregate.
// Filters are crossed only after filter_bytes proves they are total and left-only.
fn join_input(node: &Node) -> &Node {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } | RelOp::Filter { input, .. } => {
            join_input(input)
        }
        _ => node,
    }
}
fn join_column(node: &Node, column: usize) -> Option<usize> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Filter { input, .. } => join_column(input, column),
        RelOp::Project { input, columns } => join_column(input, *columns.get(column)?),
        RelOp::Join { .. } => (column < node.schema.fields().len()).then_some(column),
        _ => None,
    }
}

// Validate and fund the supported predicate tree without allocating a clone.
fn predicate_bytes(expr: &Expr, input: &Node, left_width: usize) -> Option<usize> {
    let children = match &expr.kind {
        Kind::Column(i) => {
            join_column(input, *i).filter(|i| *i < left_width)?;
            0
        }
        Kind::Literal(Literal::Text(text)) => text.len(),
        Kind::Literal(_) => 0,
        Kind::Binary(_, a, b) | Kind::Like(a, b, _) => predicate_bytes(a, input, left_width)?
            .checked_add(predicate_bytes(b, input, left_width)?)?,
        Kind::Not(a) | Kind::IsNull(a, _) => predicate_bytes(a, input, left_width)?,
        _ => return None,
    };
    std::mem::size_of::<Expr>().checked_add(children)
}

fn filter_bytes(node: &Node, left_width: usize) -> Option<usize> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } => filter_bytes(input, left_width),
        RelOp::Filter { predicate, input } if total_filter(predicate) => {
            filter_bytes(input, left_width)?
                .checked_add(predicate_bytes(predicate, input, left_width)?.checked_mul(2)?)?
                .checked_add(
                    input
                        .schema
                        .fields()
                        .len()
                        .checked_mul(std::mem::size_of::<Option<usize>>())?,
                )
        }
        RelOp::Join { .. } => Some(0),
        _ => None,
    }
}

fn collect_filters(node: &Node, left_width: usize, filters: &mut Vec<Expr>) -> Result<()> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } => {
            collect_filters(input, left_width, filters)?;
        }
        RelOp::Filter { predicate, input } => {
            collect_filters(input, left_width, filters)?;
            let mapping = (0..input.schema.fields().len())
                .map(|i| join_column(input, i).filter(|i| *i < left_width))
                .collect::<Vec<_>>();
            let mut predicate = predicate.clone();
            predicate.remap_columns(&mapping)?;
            filters.push(predicate);
        }
        _ => {}
    }
    Ok(())
}

// Indices are a permutation of a subset, so even variable-width payloads cannot
// grow beyond their source footprint. Hold old/new funding through Arrow take.
fn select_input(values: Input, indices: &UInt32Array, budget: &Budget) -> Result<Input> {
    let columns = values
        .columns
        .into_iter()
        .map(|value| {
            value
                .map(|value| {
                    let bytes = value
                        .array
                        .get_array_memory_size()
                        .checked_add(
                            indices
                                .len()
                                .checked_mul(17)
                                .ok_or("count join selection size overflow")?,
                        )
                        .and_then(|n| n.checked_add(256))
                        .ok_or("count join selection size overflow")?;
                    let _copy = budget.reserve(bytes)?;
                    Value::adopt(
                        take(&value.array, indices, None).map_err(|e| e.to_string())?,
                        budget,
                    )
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    Ok(Input {
        columns,
        len: indices.len(),
    })
}

enum BatchKeys {
    Numeric(Value),
    Encoded(Box<crate::keys::Keys>),
}
impl BatchKeys {
    fn new(batch: &RecordBatch, expressions: &[Expr], budget: &Budget) -> Result<Self> {
        let input = input(batch, budget, expressions.iter())?;
        let mut values = expressions
            .iter()
            .map(|e| e.eval(&input, budget))
            .collect::<Result<Vec<_>>>()?;
        if values.len() == 1 && values[0].array.as_any().is::<Decimal128Array>() {
            Ok(Self::Numeric(values.pop().unwrap()))
        } else {
            Ok(Self::Encoded(Box::new(crate::keys::Keys::new(
                &values,
                batch.num_rows(),
                budget,
            )?)))
        }
    }
    fn valid(&self, row: usize) -> bool {
        match self {
            Self::Numeric(value) => value.array.is_valid(row),
            Self::Encoded(keys) => keys.valid(row),
        }
    }
    fn group(&self, row: usize, counts: &Counts) -> Option<usize> {
        if !self.valid(row) {
            return None;
        }
        match self {
            Self::Numeric(value) => {
                let key = crate::expr::numeric(&value.array).value(row);
                if let Some(group) = counts.window_group(key) {
                    return (counts.value(group, 0) != 0).then_some(group);
                }
                counts.numeric.get(key)
            }
            Self::Encoded(keys) => counts.encoded.get(keys.row(row)),
        }
    }
}

struct Counts {
    numeric: crate::group_index::NumericGroups,
    encoded: crate::group_lookup::EncodedGroups,
    // Slot zero is match cardinality; the other slots count each right column.
    values: Vec<u64>,
    // Required counted columns share the match counter. A bounded scan can
    // additionally use four-byte cells; out-of-window keys widen before growth.
    narrow: Vec<u32>,
    collapsed: bool,
    width: usize,
    groups: usize,
    capacity: usize,
    payload: usize,
    // Optional direct-address window. Statistics choose storage only; every
    // decoded key is range-checked and outliers use the ordinary exact map.
    window: Option<(i128, usize)>,
    // Buffers and maps drop before their funding.
    storage_lease: Reservation,
    payload_lease: Reservation,
}
impl Counts {
    fn new(width: usize, budget: &Budget) -> Result<Self> {
        Ok(Self {
            numeric: Default::default(),
            encoded: Default::default(),
            values: Vec::new(),
            narrow: Vec::new(),
            collapsed: false,
            width,
            groups: 0,
            capacity: 0,
            payload: 0,
            window: None,
            storage_lease: budget.reserve(0)?,
            payload_lease: budget.reserve(0)?,
        })
    }

    fn with_window(width: usize, base: i128, len: usize, lease: Reservation) -> Result<Self> {
        let cells = len
            .checked_mul(width)
            .ok_or("count join capacity overflow")?;
        let bytes = cells.checked_mul(8).ok_or("count join capacity overflow")?;
        if lease.bytes() < bytes {
            return Err("internal: underfunded count window".into());
        }
        let mut counts = Self::new(width, &lease.budget())?;
        counts.values = vec![0; cells];
        counts.groups = len;
        counts.capacity = len;
        counts.window = Some((base, len));
        counts.storage_lease = lease;
        Ok(counts)
    }

    fn window_group(&self, key: i128) -> Option<usize> {
        let (base, len) = self.window?;
        let offset = usize::try_from(key.checked_sub(base)?).ok()?;
        (offset < len).then_some(offset)
    }

    fn compact_cell_bytes(rows: i64) -> usize {
        if u32::try_from(rows).is_ok() { 4 } else { 8 }
    }

    fn with_compact_window(base: i128, len: usize, rows: i64, lease: Reservation) -> Result<Self> {
        let bytes = len
            .checked_mul(Self::compact_cell_bytes(rows))
            .ok_or("count join capacity overflow")?;
        if lease.bytes() < bytes {
            return Err("internal: underfunded count window".into());
        }
        let mut counts = if Self::compact_cell_bytes(rows) == 4 {
            let mut counts = Self::new(1, &lease.budget())?;
            counts.narrow = vec![0; len];
            counts.groups = len;
            counts.capacity = len;
            counts.window = Some((base, len));
            counts.storage_lease = lease;
            counts
        } else {
            Self::with_window(1, base, len, lease)?
        };
        counts.collapsed = true;
        Ok(counts)
    }

    fn value(&self, group: usize, slot: usize) -> u64 {
        if !self.narrow.is_empty() {
            u64::from(self.narrow[group])
        } else {
            self.values[group * self.width + if self.collapsed { 0 } else { slot }]
        }
    }

    fn widen(&mut self, budget: &Budget) -> Result<()> {
        if self.narrow.is_empty() {
            return Ok(());
        }
        let bytes = self
            .narrow
            .len()
            .checked_mul(8)
            .ok_or("count join capacity overflow")?;
        // Retain the old lease until the old buffer is freed, including on denial.
        let lease = budget.reserve(bytes)?;
        self.values = self.narrow.iter().map(|n| u64::from(*n)).collect();
        self.narrow = Vec::new();
        self.storage_lease = lease;
        Ok(())
    }

    fn insert(&mut self, keys: &BatchKeys, row: usize, budget: &Budget) -> Result<usize> {
        if let BatchKeys::Numeric(value) = keys
            && let Some(group) = self.window_group(crate::expr::numeric(&value.array).value(row))
        {
            return Ok(group);
        }
        self.widen(budget)?;
        if self.groups == self.capacity {
            let capacity = self
                .capacity
                .max(128)
                .checked_mul(2)
                .ok_or("count join capacity overflow")?;
            let cells = capacity
                .checked_mul(self.width)
                .ok_or("count join capacity overflow")?;
            // Both hash maps have at most a 32-byte bucket plus control byte.
            // 128 bytes/key covers old + replacement power-of-two tables at
            // hashbrown's 7/8 load factor (including tiny tables' padding).
            // Dense storage reserves its own old/new peak through NumericGroups.
            let bytes = capacity
                .checked_mul(128)
                .and_then(|n| cells.checked_mul(8).and_then(|v| n.checked_add(v)))
                .and_then(|n| n.checked_add(1024))
                .ok_or("count join capacity overflow")?;
            let lease = budget.reserve(bytes)?;
            let mut values = Vec::with_capacity(cells);
            assert_eq!(values.capacity(), cells);
            values.extend_from_slice(&self.values);
            self.values = values; // Free the old vector before releasing its lease.
            self.storage_lease = lease;
            self.capacity = capacity;
        }
        let group = self.groups;
        match keys {
            BatchKeys::Numeric(value) => self
                .numeric
                .insert(crate::expr::numeric(&value.array).value(row), group),
            BatchKeys::Encoded(keys) => {
                let key = keys.row(row);
                let payload = self
                    .payload
                    .checked_add(key.len())
                    .ok_or("count join key size overflow")?;
                if payload > self.payload_lease.bytes() {
                    let needed = payload - self.payload_lease.bytes();
                    let lease = budget
                        .reserve(needed.max(64 * 1024))
                        .or_else(|_| budget.reserve(needed))?;
                    self.payload_lease.absorb(lease);
                }
                self.encoded.insert(key.into(), group);
                self.payload = payload;
            }
        }
        self.groups += 1;
        self.values.resize(self.groups * self.width, 0);
        Ok(group)
    }

    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        keys: &BatchKeys,
        batch: &RecordBatch,
        columns: &[Option<usize>],
        masks: &[Value],
        budget: &Budget,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<usize> {
        self.numeric.prepare(budget);
        let mut selected = 0;
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                cancellation.check()?;
            }
            if !masks.iter().all(|mask| {
                !is_sql_null(mask.array.as_ref(), row) && boolean(&mask.array).value(row)
            }) {
                continue;
            }
            selected += 1; // Includes selected NULL keys, as scan accounting does.
            if !keys.valid(row) {
                continue;
            }
            let group = match keys.group(row, self) {
                Some(group) => group,
                None => self.insert(keys, row, budget)?,
            };
            if !self.narrow.is_empty() {
                self.narrow[group] = self.narrow[group]
                    .checked_add(1)
                    .ok_or("resource-limit: join build indices")?;
                continue;
            }
            let values = &mut self.values[group * self.width..(group + 1) * self.width];
            values[0] = values[0]
                .checked_add(1)
                .ok_or("resource-limit: join build indices")?;
            if self.collapsed {
                continue;
            }
            for (i, column) in columns.iter().enumerate() {
                if column.is_some_and(|column| !is_sql_null(batch.column(column).as_ref(), row)) {
                    values[i + 1] = values[i + 1]
                        .checked_add(1)
                        .ok_or("resource-limit: join build indices")?;
                }
            }
        }
        Ok(selected)
    }
}

/// Decline ordinary plans before touching retry accounting or its execution frame.
pub(super) fn try_execute(
    prepared: &Prepared,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    let plan = &prepared.plan;
    if ctx.options.error_replay.canonical()
        || !ctx.options.relational_optimizations
        || ctx.spill.is_some()
        || !plan.aggregation
        || plan.groups.is_empty()
        || plan.aggregates.is_empty()
        || plan.filter.is_some()
        || !plan.order.is_empty()
        || plan
            .output
            .iter()
            .any(|o| !matches!(o, bind::Output::Group(_) | bind::Output::Aggregate(_)))
        || !matches!(
            &join_input(child).op,
            RelOp::Join {
                kind: JoinKind::Left,
                residual: None,
                ..
            }
        )
    {
        return Ok(None);
    }
    execute_with_retry(prepared, child, ctx, emit)
}

/// Replay only unpublished memory refusals, keeping this frame off declined plans.
#[inline(never)]
fn execute_with_retry(
    prepared: &Prepared,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    if ctx.options.workers == 1 {
        return try_execute_once(prepared, child, ctx, emit);
    }
    let entry_bytes = ctx.budget.used();
    let entry_workers = ctx.budget.active_workers();
    let mut emitted = false;
    let mut result = try_execute_once(prepared, child, ctx, &mut |batch| {
        // A downstream callback can have irreversible effects even on error.
        // Never retry after entering it. The counted aggregate buffers all
        // groups until source/probe consumption has completed successfully.
        emitted = true;
        emit(batch)
    });
    if !emitted
        && matches!(&result, Err(error) if error.starts_with("resource-limit: accounted memory"))
    {
        if ctx.budget.used() != entry_bytes || ctx.budget.active_workers() != entry_workers {
            result = Err("internal: counted join retry retained owners".into());
        } else {
            let options = Options {
                workers: 1,
                ..ctx.options.clone()
            };
            // Admission accepts only independent scan/filter/project sources:
            // no shared materializations, transforms or consumer filters can
            // have been modified by the failed attempt. All owners have exited.
            let mut serial = Context {
                filtered_reuse: None,
                build_groups: None,
                demand_cache: None,
                options: &options,
                shared_attempt: ctx.shared_attempt.clone(),
                scan_filters: HashMap::new(),
                static_filters: HashMap::new(),
                scan_transforms: HashMap::new(),
                lane_transforms: HashMap::new(),
                replay_transforms: HashMap::new(),
                materialized: HashMap::new(),
                sideways_registry_lease: None,
                spill: None,
                budget: ctx.budget.clone(),
                // Retain the failed attempt's work/profiles as evidence; the
                // explicit retry counter distinguishes it from delivered rows.
                metrics: std::mem::take(&mut ctx.metrics),
            };
            result = try_execute_once(prepared, child, &mut serial, emit);
            ctx.metrics = serial.metrics;
            ctx.metrics.fused_left_count_serial_retries += 1;
        }
    }
    result
}

/// Preserve the existing COUNT fusion admission and build/probe/aggregate order.
fn try_execute_once(
    prepared: &Prepared,
    child: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    let plan = &prepared.plan;
    let RelOp::Join {
        left,
        right,
        kind: JoinKind::Left,
        keys,
        residual: None,
    } = &join_input(child).op
    else {
        return Ok(None);
    };
    let left_width = left.schema.fields().len();
    let Some(filter_bytes) = filter_bytes(child, left_width) else {
        return Ok(None);
    };
    if keys.is_empty() || !total_source(left, ctx) || !total_source(right, ctx)
        || !plan.groups.iter().all(|e| matches!(e.kind, Kind::Column(i) if join_column(child, i).is_some_and(|i| i < left_width)))
        || !keys.iter().all(|(a, b)| a.ty == b.ty
            && matches!(a.ty, Type::Int | Type::Decimal(_) | Type::Date | Type::Text | Type::Bool)
            && matches!(a.kind, Kind::Column(_)) && matches!(b.kind, Kind::Column(_)))
        || plan.aggregates.iter().any(|a| !matches!(a.function, Aggregate::Count) || a.distinct
            || a.input.as_ref().is_some_and(|e| !matches!(e.kind, Kind::Column(i) if join_column(child, i).is_some_and(|i| i >= left_width))))
    {
        return Ok(None);
    }
    let budget = ctx.budget.clone();
    let options = ctx.options;
    // Fund the bounded expression/column descriptors; no source plan is cloned.
    let descriptors = plan
        .aggregates
        .len()
        .checked_add(plan.groups.len())
        .and_then(|n| n.checked_add(keys.len().saturating_mul(2)))
        .and_then(|n| n.checked_add(left_width))
        .and_then(|n| n.checked_add(right.schema.fields().len()))
        .and_then(|n| n.checked_mul(512))
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| n.checked_add(filter_bytes))
        .ok_or("count join metadata overflow")?;
    let _metadata = budget.reserve(descriptors)?;
    let mut filters = Vec::new();
    collect_filters(child, left_width, &mut filters)?;
    let columns = plan
        .aggregates
        .iter()
        .map(|a| {
            a.input.as_ref().map(|e| {
                let Kind::Column(column) = e.kind else {
                    unreachable!()
                };
                join_column(child, column).unwrap() - left_width
            })
        })
        .collect::<Vec<_>>();
    let left_groups = plan
        .groups
        .iter()
        .map(|e| {
            let Kind::Column(column) = e.kind else {
                unreachable!()
            };
            Expr {
                kind: Kind::Column(join_column(child, column).unwrap()),
                ty: e.ty.clone(),
            }
        })
        .collect::<Vec<_>>();
    let left_keys = keys.iter().map(|(a, _)| a.clone()).collect::<Vec<_>>();
    let right_keys = keys.iter().map(|(_, b)| b.clone()).collect::<Vec<_>>();
    let tables = if let Some(tables) = parallel::build(right, &right_keys, &columns, ctx)? {
        tables
    } else {
        let mut counts = Counts::new(columns.len() + 1, &budget)?;
        let mut build_rows = 0_usize;
        let mut seconds = 0.;
        // Right-before-left execution is unchanged for every admitted shape.
        right.run(ctx, &mut |batch| {
            options.cancellation.check()?;
            let start = Instant::now();
            build_rows = build_rows
                .checked_add(batch.num_rows())
                .ok_or("resource-limit: join build indices")?;
            let keys = BatchKeys::new(&batch, &right_keys, &budget)?;
            counts.add(&keys, &batch, &columns, &[], &budget, &options.cancellation)?;
            seconds += start.elapsed().as_secs_f64();
            Ok(())
        })?;
        if build_rows > u32::MAX as usize {
            return Err("resource-limit: join build indices".into());
        }
        ctx.metrics.join_build_rows += build_rows;
        ctx.metrics.join_seconds += seconds;
        parallel::Tables::single(counts, &budget)?
    };
    // Keep serial reader boundaries, but probe complete batches concurrently.
    // Each probe row contributes matches or its miss in place; waves drain in order.
    // total_source excludes shared state and consumer-installed transforms, so
    // this leaf-only context needs no materializations or inherited filters.
    let probe_options = Options {
        workers: 1,
        relational_optimizations: false,
        ..options.clone()
    };
    let mut wave = parallel::Wave::new(options, &budget)?;
    let mut probe_ctx = Context {
        filtered_reuse: None,
        build_groups: None,
        demand_cache: None,
        options: &probe_options,
        shared_attempt: ctx.shared_attempt.clone(),
        scan_filters: HashMap::new(),
        static_filters: HashMap::new(),
        scan_transforms: HashMap::new(),
        lane_transforms: HashMap::new(),
        replay_transforms: HashMap::new(),
        materialized: HashMap::new(),
        sideways_registry_lease: None,
        spill: None,
        budget: budget.clone(),
        metrics: std::mem::take(&mut ctx.metrics),
    };
    let result = execute::execute_counted_stream(
        prepared,
        &left_groups,
        options,
        budget.clone(),
        |send| {
            let probe = parallel::Probe {
                tables: &tables,
                keys: &left_keys,
                groups: &left_groups,
                filters: &filters,
                columns: &columns,
                descriptors,
                budget: &budget,
                options,
            };
            let source = left.run(&mut probe_ctx, &mut |batch| wave.push(batch, &probe, send));
            // Earlier buffered probe/aggregate errors precede a later source
            // failure. On a callback failure push already drained its wave.
            wave.flush(&probe, send)?;
            // The aggregate's output/downstream phase must not retain parallel
            // count tables after their last probe. All scoped workers exited.
            drop(tables);
            source
        },
        emit,
    );
    ctx.metrics = probe_ctx.metrics;
    let mut metrics = result?;
    metrics.fused_left_count_joins = 1;
    metrics.fused_left_count_rows = wave.rows;
    ctx.metrics.join_seconds += wave.seconds;
    ctx.metrics.parallel_joins += usize::from(wave.background > 0);
    ctx.metrics.parallel_join_tasks += wave.tasks;
    ctx.metrics.parallel_join_batches += wave.tasks;
    Ok(Some(metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn counted_duplicate_products_keep_64_bits_across_batches() {
        let group = Expr {
            kind: Kind::Column(0),
            ty: Type::Int,
        };
        let count_value = bind::Agg {
            function: Aggregate::Count,
            input: Some(Expr {
                kind: Kind::Column(1),
                ty: Type::Int,
            }),
            ty: Type::Int,
            distinct: false,
        };
        let prepared = Prepared {
            auto_simple: None,
            internal_output: false,
            relational: None,
            schema: Arc::new(Schema::new(vec![
                Type::Int.field("g", false),
                Type::Int.field("v", true),
            ])),
            plan: bind::Plan {
                table: None,
                path: None,
                columns: vec![0, 1],
                filter: None,
                groups: vec![group.clone()],
                aggregates: vec![
                    count_value.clone(),
                    bind::Agg {
                        function: Aggregate::Count,
                        input: None,
                        ty: Type::Int,
                        distinct: false,
                    },
                    count_value,
                ],
                output: vec![
                    bind::Output::Group(0),
                    bind::Output::Aggregate(0),
                    bind::Output::Aggregate(1),
                    bind::Output::Aggregate(2),
                ],
                names: vec!["g".into(), "n".into(), "all_rows".into(), "again".into()],
                types: vec![Type::Int; 4],
                order: vec![],
                aggregation: true,
                projection_pushdown: true,
            },
        };
        // A build with 65,537 matches, 32,769 non-NULL payloads, and 65,537
        // duplicate left rows has these exact per-row weights. Exercise the
        // weighted stream directly: a generic join would emit over 2^32 rows.
        for workers in [1, 2, 4, 10] {
            for batch_size in [257, 8192] {
                let budget = Budget::new(8 << 20);
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let mut output = Vec::new();
                execute::execute_counted_stream(
                    &prepared,
                    std::slice::from_ref(&group),
                    &options,
                    budget.clone(),
                    |send| {
                        for start in (0..65_537).step_by(batch_size) {
                            let len = batch_size.min(65_537 - start);
                            let values = Input {
                                len,
                                columns: vec![
                                    Some(Value::adopt(
                                        Arc::new(
                                            Decimal128Array::from(vec![7; len])
                                                .with_precision_and_scale(38, 0)
                                                .unwrap(),
                                        ),
                                        &budget,
                                    )?),
                                    None,
                                ],
                            };
                            let weights = [32_769, 65_537, 32_769]
                                .into_iter()
                                .map(|weight| {
                                    Value::adopt(
                                        Arc::new(UInt64Array::from(vec![weight; len])),
                                        &budget,
                                    )
                                })
                                .collect::<Result<Vec<_>>>()?;
                            send(values, weights)?;
                        }
                        Ok(())
                    },
                    &mut |batch| {
                        output.push(batch);
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(output.len(), 1);
                assert_eq!(output[0].num_rows(), 1);
                for (column, expected) in [7, 2_147_581_953, 4_295_098_369, 2_147_581_953]
                    .into_iter()
                    .enumerate()
                {
                    assert_eq!(output[0].column(column).data_type(), &DataType::Int64);
                    assert_eq!(
                        output[0]
                            .column(column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        expected,
                    );
                }
                drop(output);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.peak_workers(), 0);
            }
        }
    }

    #[test]
    fn count_index_growth_is_funded_and_failed_growth_leaves_no_buffers() {
        for limit in [20_000, 1 << 20] {
            let budget = Budget::new(limit);
            let key =
                Value::adopt(Arc::new(Decimal128Array::from_iter_values(0..900)), &budget).unwrap();
            let keys = BatchKeys::Numeric(key);
            let mut counts = Counts::new(3, &budget).unwrap();
            if limit == 20_000 {
                assert!(
                    counts
                        .insert(&keys, 0, &budget)
                        .unwrap_err()
                        .starts_with("resource-limit:")
                );
                assert_eq!(counts.groups, 0);
                assert_eq!(counts.values.capacity(), 0);
            } else {
                for row in 0..900 {
                    let group = counts.insert(&keys, row, &budget).unwrap();
                    assert_eq!(group, row);
                    assert_eq!(keys.group(row, &counts), Some(group));
                    assert!(
                        budget.used()
                            >= counts.values.capacity() * 8
                                + counts.numeric.allocation_bytes()
                                + counts.numeric.dense_bytes()
                    );
                }
                counts.numeric.prepare(&budget);
                for row in 0..900 {
                    assert_eq!(keys.group(row, &counts), Some(row));
                }
                assert!(
                    budget.used()
                        >= counts.values.capacity() * 8
                            + counts.numeric.allocation_bytes()
                            + counts.numeric.dense_bytes()
                );
            }
            drop(counts);
            drop(keys);
            assert_eq!(budget.used(), 0);
        }
    }
}
