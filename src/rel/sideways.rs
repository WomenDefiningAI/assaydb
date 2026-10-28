//! Retain a selective INNER build once, then transfer necessary membership to
//! an equivalent column in an earlier build. Exact joins and their order remain.
use super::*;
use crate::expr::Kind;

const MAX_SOURCE_ROWS: usize = 4096;
const MAX_SOURCE_BYTES: usize = 8 << 20;

pub(super) struct Installed {
    id: usize,
    previous: Option<Arc<ScanKeyFilter>>,
}
impl Installed {
    pub(super) fn restore(self, ctx: &mut Context<'_>) {
        if let Some(previous) = self.previous {
            ctx.scan_filters.insert(self.id, previous);
        } else {
            ctx.scan_filters.remove(&self.id);
        }
    }
}

// Both earlier execution and rejection cross the entire subtree, including
// off-key branches. Cardinality and finalization boundaries remain barriers.
fn eligible(node: &Node) -> bool {
    if node.schema.fields().len() >= 128 || !membership_safety::can_move_membership_across(node) {
        return false;
    }
    match &node.op {
        RelOp::Scan { .. } => true,
        RelOp::Rename { input } | RelOp::Project { input, .. } | RelOp::Filter { input, .. } => {
            eligible(input)
        }
        RelOp::Unary { input, plan } => !plan.aggregation && eligible(input),
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => eligible(left) && eligible(right),
        _ => false,
    }
}

#[derive(Clone, Copy)]
struct Source<'a> {
    node: &'a Node,
    column: usize,
}

// Direct same-type INNER equalities are undirected implications for surviving
// rows. Inline masks bound traversal scratch without allocating a key graph.
fn source(node: &Node, mut columns: u128, domain: usize) -> Option<Source<'_>> {
    if columns == 0 || node.schema.fields().len() >= 128 {
        return None;
    }
    match &node.op {
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            keys,
            ..
        } => {
            let width = left.schema.fields().len();
            loop {
                let before = columns;
                for (a, b) in keys {
                    if let (Kind::Column(a_col), Kind::Column(b_col)) = (&a.kind, &b.kind)
                        && a.ty == b.ty
                        && a.ty.scale().is_ok()
                    {
                        let pair = (1_u128 << a_col) | (1_u128 << (width + b_col));
                        if columns & pair != 0 {
                            columns |= pair;
                        }
                    }
                }
                if columns == before {
                    break;
                }
            }
            let rhs = columns >> width;
            // A tiny build's row estimate is not its completed key cardinality:
            // filters, duplicates and NULLs can make the latter much smaller.
            // Allow a bounded estimated subset to reach prepare's actual-NDV
            // gate; only that gate may install the selective prefilter.
            let here = (rhs != 0
                && right.estimated_rows <= MAX_SOURCE_ROWS
                && right.estimated_rows < domain)
                .then(|| Source {
                    node: right,
                    column: rhs.trailing_zeros() as usize,
                });
            [
                here,
                source(left, columns & ((1_u128 << width) - 1), domain),
                source(right, rhs, domain),
            ]
            .into_iter()
            .flatten()
            .min_by_key(|s| s.node.estimated_rows)
        }
        RelOp::Rename { input } | RelOp::Filter { input, .. } => source(input, columns, domain),
        RelOp::Project {
            input,
            columns: projection,
        } => {
            let mut mapped = 0;
            for (i, target) in projection.iter().enumerate() {
                if columns & (1_u128 << i) != 0 && *target < 128 {
                    mapped |= 1_u128 << target;
                }
            }
            source(input, mapped, domain)
        }
        RelOp::Unary { input, plan } if !plan.aggregation => {
            let mut mapped = 0;
            for (i, output) in plan.output.iter().enumerate() {
                if columns & (1_u128 << i) != 0
                    && let bind::Output::Expr(Expr {
                        kind: Kind::Column(target),
                        ..
                    }) = output
                    && *target < 128
                {
                    mapped |= 1_u128 << target;
                }
            }
            source(input, mapped, domain)
        }
        _ => None,
    }
}

// Keep completed output, including NULLs and duplicate occurrences. Fund all
// retained buffers and vector replacement peaks before retaining/allocating.
fn retain(node: &Node, ctx: &mut Context<'_>) -> Result<Arc<Materialized>> {
    let id = node as *const Node as usize;
    if let Some(stored) = ctx.materialized.get(&id) {
        return Ok(stored.clone());
    }
    let budget = ctx.budget.clone();
    let mut metadata = budget.reserve(2048)?;
    let mut batches = Vec::with_capacity(8);
    let mut leases = Vec::with_capacity(8);
    let mut rows = 0_usize;
    let mut bytes = 0_usize;
    node.run(ctx, &mut |batch| {
        rows = rows.saturating_add(batch.num_rows());
        bytes = bytes.saturating_add(batch.get_array_memory_size());
        if rows > MAX_SOURCE_ROWS || bytes > MAX_SOURCE_BYTES {
            return Err("resource-limit: optional sideways build retention cap".into());
        }
        if batches.len() + 1 >= batches.capacity() {
            let capacity = batches.capacity().saturating_mul(2);
            let extra = capacity.saturating_mul(
                2 * (std::mem::size_of::<RecordBatch>() + std::mem::size_of::<Reservation>()),
            );
            if !metadata.try_grow(extra) {
                return Err("resource-limit: optional sideways build metadata".into());
            }
            batches.reserve_exact(capacity - batches.len());
            leases.reserve_exact(capacity - leases.len());
        }
        leases.push(budget.reserve(batch.get_array_memory_size().saturating_add(256))?);
        batches.push(batch);
        Ok(())
    })?;
    leases.push(metadata);
    let stored = Arc::new(Materialized {
        budget,
        batches,
        files: Vec::new(),
        _leases: leases,
    });
    fund_registry(ctx, 1)?;
    ctx.materialized.insert(id, stored.clone());
    ctx.metrics.sideways_prepared_builds += 1;
    Ok(stored)
}

// The registry can outlive every retained build. Keep its credit in Context,
// including the old + new table peak when an insertion grows the allocation.
// usize/Arc entries plus control bytes need <128 bytes per usable slot across
// a doubling; 2048 also covers the initial small table and alignment slack.
// Retain the high-water credit until Context frees the map, even after remove.
pub(super) fn fund_registry(ctx: &mut Context<'_>, additional: usize) -> Result<()> {
    let capacity = ctx
        .materialized
        .capacity()
        .max(ctx.materialized.len().saturating_add(additional));
    let bytes = 2048_usize.saturating_add(capacity.saturating_mul(128));
    if let Some(lease) = &mut ctx.sideways_registry_lease {
        if !lease.try_grow(bytes.saturating_sub(lease.bytes())) {
            return Err("resource-limit: optional sideways registry metadata".into());
        }
    } else {
        ctx.sideways_registry_lease = Some(ctx.budget.reserve(bytes)?);
    }
    Ok(())
}

pub(super) fn number(batch: &RecordBatch, column: usize, row: usize) -> Option<i128> {
    let array = batch.column(column);
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => Some(i128::from(
            array.as_any().downcast_ref::<Int32Array>()?.value(row),
        )),
        arrow::datatypes::DataType::Int64 => Some(i128::from(
            array.as_any().downcast_ref::<Int64Array>()?.value(row),
        )),
        arrow::datatypes::DataType::Decimal128(_, _) => {
            Some(array.as_any().downcast_ref::<Decimal128Array>()?.value(row))
        }
        _ => None,
    }
}

pub(super) fn prepare(node: &Node, ctx: &mut Context<'_>) -> Result<Option<Installed>> {
    let RelOp::Join {
        left,
        right,
        kind: JoinKind::Inner,
        keys,
        ..
    } = &node.op
    else {
        return Ok(None);
    };
    if !ctx.options.relational_optimizations
        || ctx.spill.is_some()
        || ctx.options.error_replay.canonical()
        || ctx.demand_cache.is_none()
        || ctx.options.parquet_cache_bytes.is_none_or(|n| n == 0)
        || right.estimated_rows < 4096
    {
        return Ok(None);
    }
    for (a, b) in keys {
        let (Kind::Column(column), Kind::Column(_)) = (&a.kind, &b.kind) else {
            continue;
        };
        if a.ty != b.ty || a.ty.scale().is_err() || *column >= 128 {
            continue;
        }
        let domain = distinct(right, b);
        let Some(source) = source(left, 1_u128 << column, domain) else {
            continue;
        };
        if !eligible(node) {
            return Ok(None);
        }
        let Ok(descriptor) = ctx
            .budget
            .reserve(runtime_filter::traversal_bytes(right).saturating_add(4096))
        else {
            return Ok(None);
        };
        let Some(target) = scan_target(right, vec![b.clone()])? else {
            continue;
        };
        if target.payload < 8 || ctx.scan_filters.contains_key(&target.id) {
            continue;
        }
        // Later failures replay the immutable plan with fresh state and no
        // optional retention; no partial output or build escapes that retry.
        ctx.demand_cache
            .unwrap()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        ctx.options.cancellation.check()?;
        let stored = retain(source.node, ctx)?;
        let bitmap = crate::membership::NumericMembership::from_values(
            || {
                stored.batches.iter().flat_map(|batch| {
                    (0..batch.num_rows()).filter_map(move |row| number(batch, source.column, row))
                })
            },
            &ctx.budget,
        );
        let Some(bitmap) = bitmap else {
            continue;
        };
        if bitmap.cardinality().saturating_mul(4) >= target.key_domain {
            continue;
        }
        let filter = Arc::new(ScanKeyFilter {
            expressions: target.expressions,
            previous: None,
            optional_total: true,
            members: Arc::new(HashSet::default()),
            numeric: Some(Arc::new(bitmap)),
            numeric_pair: None,
            _lease: Arc::new(descriptor),
        });
        let previous = ctx.scan_filters.insert(target.id, filter);
        ctx.metrics.sideways_scan_filters += 1;
        return Ok(Some(Installed {
            id: target.id,
            previous,
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(super) fn col(i: usize) -> Expr {
        Expr {
            kind: Kind::Column(i),
            ty: Type::Int,
        }
    }
    pub(super) fn scan(rows: usize) -> Node {
        Node {
            schema: Arc::new(Schema::new(vec![
                Field::new("k", arrow::datatypes::DataType::Int64, true),
                Field::new("v", arrow::datatypes::DataType::Int64, true),
            ])),
            estimated_rows: rows,
            op: RelOp::Scan {
                path: PathBuf::new(),
                columns: vec![0, 1],
                distinct: vec![1000; 2],
                statistics: vec![],
            },
        }
    }
    pub(super) fn join(left: Node, right: Node) -> Node {
        Node {
            schema: combined(&left.schema, &right.schema),
            estimated_rows: 10_000,
            op: RelOp::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind: JoinKind::Inner,
                keys: vec![(col(0), col(0))],
                residual: None,
            },
        }
    }
    pub(super) fn context<'a>(
        options: &'a Options,
        budget: &Budget,
        used: &'a AtomicBool,
    ) -> Context<'a> {
        Context {
            filtered_reuse: None,
            build_groups: None,
            demand_cache: Some(used),
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
    #[test]
    fn denied_transfer_leaves_state_and_budget_untouched() {
        let node = join(join(scan(10_000), scan(2)), scan(10_000));
        let options = Options::default();
        let budget = Budget::new(1);
        let used = AtomicBool::new(false);
        let mut ctx = context(&options, &budget, &used);
        assert!(prepare(&node, &mut ctx).unwrap().is_none());
        assert!(!used.load(Ordering::Relaxed));
        assert!(ctx.scan_filters.is_empty());
        assert!(ctx.materialized.is_empty());
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn consumed_builds_leave_live_registry_capacity_funded() {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                ..Options::default()
            };
            let budget = Budget::new(1 << 20);
            let used = AtomicBool::new(false);
            let mut ctx = context(&options, &budget, &used);
            // Grow through several map capacities, then consume newest first:
            // the oldest build's metadata cannot fund the enlarged registry.
            let nodes = (0..20)
                .map(|_| Node {
                    schema: Arc::new(Schema::empty()),
                    estimated_rows: 1,
                    op: RelOp::One,
                })
                .collect::<Vec<_>>();
            let assert_funded = |ctx: &Context<'_>, capacity: usize| {
                let buckets = (capacity + 1).next_power_of_two();
                let heap = buckets * (std::mem::size_of::<(usize, Arc<Materialized>)>() + 1) + 16;
                assert!(ctx.sideways_registry_lease.as_ref().unwrap().bytes() >= heap);
                assert!(budget.used() >= heap);
            };
            for node in &nodes {
                drop(retain(node, &mut ctx).unwrap());
                assert_funded(&ctx, ctx.materialized.capacity());
            }
            let capacity = ctx.materialized.capacity();
            for node in nodes.iter().rev() {
                let mut rows = 0;
                node.run(&mut ctx, &mut |batch| {
                    rows += batch.num_rows();
                    Ok(())
                })
                .unwrap();
                assert_eq!(rows, 1);
                // Tombstones can reduce reported insertion capacity without
                // freeing buckets. Check the original allocation throughout.
                assert!(ctx.materialized.capacity() > 0);
                assert_funded(&ctx, capacity);
            }
            assert!(ctx.materialized.is_empty());
            assert_eq!(
                budget.used(),
                ctx.sideways_registry_lease.as_ref().unwrap().bytes()
            );
            drop(ctx);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn registry_growth_refusal_preserves_credit_and_bulk_growth_is_funded() {
        let options = Options::default();
        let budget = Budget::new(1 << 20);
        let used = AtomicBool::new(false);
        let mut ctx = context(&options, &budget, &used);
        let node = Node {
            schema: Arc::new(Schema::empty()),
            estimated_rows: 1,
            op: RelOp::One,
        };
        let stored = retain(&node, &mut ctx).unwrap();
        let old_capacity = ctx.materialized.capacity();
        let old_credit = ctx.sideways_registry_lease.as_ref().unwrap().bytes();
        let blocker = budget.reserve(budget.limit() - budget.used()).unwrap();
        assert_eq!(
            fund_registry(&mut ctx, 100).unwrap_err(),
            "resource-limit: optional sideways registry metadata"
        );
        assert_eq!(ctx.materialized.capacity(), old_capacity);
        assert_eq!(
            ctx.sideways_registry_lease.as_ref().unwrap().bytes(),
            old_credit
        );
        assert_eq!(budget.used(), budget.limit());
        drop(blocker);
        // Demand preparation transfers several completed stores in one reserve.
        fund_registry(&mut ctx, 100).unwrap();
        ctx.materialized.try_reserve(100).unwrap();
        let buckets = (ctx.materialized.capacity() + 1).next_power_of_two();
        let heap = buckets * (std::mem::size_of::<(usize, Arc<Materialized>)>() + 1) + 16;
        ctx.materialized.clear();
        drop(stored);
        assert!(budget.used() >= heap);
        assert!(ctx.sideways_registry_lease.as_ref().unwrap().bytes() >= heap);
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn completed_membership_and_cancelled_replay_release_every_reservation() {
        for workers in [1, 2, 4, 10] {
            let node = join(join(scan(10_000), scan(2)), scan(10_000));
            let RelOp::Join {
                left,
                right: target,
                ..
            } = &node.op
            else {
                unreachable!()
            };
            let RelOp::Join { right: source, .. } = &left.op else {
                unreachable!()
            };
            let options = Options {
                workers,
                ..Options::default()
            };
            let budget = Budget::new(1 << 20);
            let used = AtomicBool::new(false);
            let mut ctx = context(&options, &budget, &used);
            let batch = RecordBatch::try_new(
                source.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(3), None, Some(3)])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            )
            .unwrap();
            let stored =
                Materialized::from_test_batches(vec![batch.clone(), batch], &budget).unwrap();
            ctx.materialized
                .insert(source.as_ref() as *const Node as usize, Arc::new(stored));
            let installed = prepare(&node, &mut ctx).unwrap().unwrap();
            assert!(used.load(Ordering::Relaxed));
            let filter = &ctx.scan_filters[&(target.as_ref() as *const Node as usize)];
            assert_eq!(filter.numeric.as_ref().unwrap().cardinality(), 1);
            assert!(filter.numeric.as_ref().unwrap().contains(3));
            assert!(!filter.numeric.as_ref().unwrap().contains(2));
            installed.restore(&mut ctx);
            assert!(ctx.scan_filters.is_empty());
            let mut emitted = 0;
            let result = source.run(&mut ctx, &mut |_| {
                emitted += 1;
                options.cancellation.cancel();
                Ok(())
            });
            assert_eq!(
                result.unwrap_err(),
                options.cancellation.check().unwrap_err()
            );
            assert_eq!(emitted, 1);
            assert!(ctx.materialized.is_empty());
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(10, 10).count, 10);
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::tests::{col, join, scan};
    use super::*;

    #[test]
    fn equality_closure_remaps_aliases_but_declines_mixed_types_and_outer_joins() {
        let inner = join(scan(10_000), scan(2));
        let schema = inner.schema.clone();
        let alias = Node {
            schema: schema.clone(),
            estimated_rows: 10_000,
            op: RelOp::Rename {
                input: Box::new(inner),
            },
        };
        let project = Node {
            schema,
            estimated_rows: 10_000,
            op: RelOp::Project {
                input: Box::new(alias),
                columns: vec![2, 0, 1, 3],
            },
        };
        assert_eq!(source(&project, 1, 1000).unwrap().column, 0);
        for kind in [JoinKind::Left, JoinKind::Semi, JoinKind::Anti] {
            let mut node = join(scan(10_000), scan(2));
            let RelOp::Join { kind: actual, .. } = &mut node.op else {
                unreachable!()
            };
            *actual = kind;
            assert!(!eligible(&node));
            assert!(source(&node, 1, 1000).is_none());
        }
        let mut mixed = join(scan(10_000), scan(2));
        let RelOp::Join { keys, .. } = &mut mixed.op else {
            unreachable!()
        };
        keys[0] = (
            col(0),
            Expr {
                kind: Kind::Column(0),
                ty: Type::Float,
            },
        );
        assert!(!eligible(&mixed));
        assert!(source(&mixed, 1, 1000).is_none());
    }
}

#[cfg(test)]
#[path = "sideways/sql_tests.rs"]
mod sql_tests;
