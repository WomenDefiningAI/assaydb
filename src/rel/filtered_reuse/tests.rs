use super::*;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::atomic::{AtomicUsize, Ordering};

mod review;

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture {
    path: PathBuf,
    dimension: PathBuf,
    schema: SchemaRef,
}
impl Fixture {
    fn new(empty: bool) -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("filtered-reuse-{}-{id}", std::process::id());
        let path = std::env::temp_dir().join(format!("{prefix}-fact.parquet"));
        let dimension = std::env::temp_dir().join(format!("{prefix}-dim.parquet"));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Int64, true),
            Field::new("s", arrow::datatypes::DataType::Int64, true),
            Field::new("v", arrow::datatypes::DataType::Decimal128(15, 2), true),
            Field::new("w", arrow::datatypes::DataType::Decimal128(15, 2), true),
        ]));
        let rows = if empty { 0 } else { 4099 };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 37 != 0).then_some(i % 1024)),
                )),
                Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                    (i % 19 != 0).then_some(if i % 1024 == 3 { 2 } else { (i / 1024) % 3 })
                }))),
                Arc::new(
                    Decimal128Array::from_iter((0..rows).map(|i| {
                        (i % 23 != 0).then_some(match i % 1024 {
                            1 | 2 => {
                                if (i / 1024) % 3 == i % 1024 - 1 {
                                    200
                                } else {
                                    0
                                }
                            }
                            4 => 200,
                            _ => (i % 11) as i128 * 100,
                        })
                    }))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from_iter((0..rows).map(|i| {
                        (i % 29 != 0).then_some(if matches!(i % 1024, 1 | 2 | 4) {
                            100
                        } else {
                            (i % 7) as i128 * 100
                        })
                    }))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(127)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let dim = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(3),
                    Some(4),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(0),
                    None,
                ])),
                Arc::new(
                    Decimal128Array::from(vec![100, 200, 200, 300, 400, 0])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![0; 6])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&dimension).unwrap(), schema.clone(), None)
                .unwrap();
        writer.write(&dim).unwrap();
        writer.close().unwrap();
        Self {
            path,
            dimension,
            schema,
        }
    }
    fn scan(&self) -> Node {
        Node {
            schema: self.schema.clone(),
            estimated_rows: 1_000_000,
            op: RelOp::Scan {
                path: self.path.clone(),
                columns: vec![0, 1, 2, 3],
                distinct: vec![1_000_000; 4],
                statistics: Vec::new(),
            },
        }
    }
    fn tables(&self) -> HashMap<String, PathBuf> {
        HashMap::from([
            ("f".into(), self.path.clone()),
            ("d".into(), self.dimension.clone()),
        ])
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).unwrap();
        std::fs::remove_file(&self.dimension).unwrap();
    }
}
fn context<'a>(options: &'a Options, budget: &Budget) -> Context<'a> {
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
fn filter(keys: &[i128], budget: &Budget) -> Arc<ScanKeyFilter> {
    let lease = budget.reserve(4096).unwrap();
    let members = keys
        .iter()
        .map(|n| n.to_ne_bytes().to_vec().into_boxed_slice())
        .collect::<HashSet<_, ahash::RandomState>>();
    let expressions = vec![Expr {
        kind: crate::expr::Kind::Column(0),
        ty: Type::Int,
    }];
    Arc::new(ScanKeyFilter {
        numeric: numeric_membership(&expressions, &members, budget),
        expressions,
        previous: None,
        optional_total: false,
        members: Arc::new(members),
        numeric_pair: None,
        _lease: Arc::new(lease),
    })
}
fn collected(node: &Node, ctx: &mut Context<'_>) -> Result<RecordBatch> {
    let mut batches = Vec::new();
    node.run(ctx, &mut |b| {
        batches.push(b);
        Ok(())
    })?;
    Ok(concat_batches(&node.schema, &batches).map_err(|e| e.to_string())?)
}

#[test]
fn filtered_subset_reuse_preserves_nulls_duplicates_and_order_at_all_workers() {
    for empty in [false, true] {
        let fixture = Fixture::new(empty);
        for (batch_size, bitmap) in [1, 31, 257]
            .into_iter()
            .flat_map(|n| [(n, true), (n, false)])
        {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let mut narrow = fixture.scan();
                narrow.schema = Arc::new(fixture.schema.project(&[0, 2]).unwrap());
                let RelOp::Scan { columns, .. } = &mut narrow.op else {
                    unreachable!()
                };
                *columns = vec![0, 2];
                let root = Node {
                    schema: fixture.schema.clone(),
                    estimated_rows: 1_000_000,
                    op: RelOp::Join {
                        left: Box::new(fixture.scan()),
                        right: Box::new(narrow),
                        kind: JoinKind::Inner,
                        keys: Vec::new(),
                        residual: None,
                    },
                };
                let RelOp::Join { left, right, .. } = &root.op else {
                    unreachable!()
                };
                let mut ctx = context(&options, &budget);
                ctx.filtered_reuse = Cache::new(&root, &budget);
                let _capture = Capture::new(&ctx);
                ctx.scan_filters.insert(
                    left.as_ref() as *const Node as usize,
                    filter(&[1, 2, 3], &budget),
                );
                ctx.scan_filters.insert(
                    right.as_ref() as *const Node as usize,
                    filter(&[2, 3], &budget),
                );
                if !bitmap {
                    for membership in ctx.scan_filters.values_mut() {
                        Arc::get_mut(membership).unwrap().numeric = None;
                    }
                }
                let first = collected(left, &mut ctx).unwrap();
                assert_eq!(first.column(0).null_count(), 0);
                assert_eq!(ctx.metrics.filtered_scan_materializations, 1);
                let before = Metrics {
                    dynamic_scan_filters: ctx.metrics.dynamic_scan_filters,
                    bitmap_scan_filters: ctx.metrics.bitmap_scan_filters,
                    numeric_pair_scan_filters: ctx.metrics.numeric_pair_scan_filters,
                    cached_scan_filters: ctx.metrics.cached_scan_filters,
                    filter_seconds: ctx.metrics.filter_seconds,
                    ..Metrics::default()
                };
                let actual = collected(right, &mut ctx).unwrap();
                assert_eq!(ctx.metrics.filtered_scan_reuses, 1);
                let serial = Options {
                    workers: 1,
                    batch_size,
                    ..Options::default()
                };
                let mut ordinary = context(&serial, &budget);
                ordinary.scan_filters.insert(
                    right.as_ref() as *const Node as usize,
                    filter(&[2, 3], &budget),
                );
                if !bitmap {
                    for membership in ordinary.scan_filters.values_mut() {
                        Arc::get_mut(membership).unwrap().numeric = None;
                    }
                }
                let expected = collected(right, &mut ordinary).unwrap();
                // Replay applies the same one membership filter as a fresh scan;
                // only replay also counts it as cached. Elapsed work is measured,
                // not expected to equal the fresh scan's different input volume.
                assert_eq!(ordinary.metrics.dynamic_scan_filters, 1);
                assert_eq!(ordinary.metrics.bitmap_scan_filters, usize::from(bitmap));
                assert_eq!(
                    ctx.metrics.dynamic_scan_filters - before.dynamic_scan_filters,
                    ordinary.metrics.dynamic_scan_filters
                );
                assert_eq!(
                    ctx.metrics.bitmap_scan_filters - before.bitmap_scan_filters,
                    ordinary.metrics.bitmap_scan_filters
                );
                assert_eq!(
                    ctx.metrics.numeric_pair_scan_filters - before.numeric_pair_scan_filters,
                    ordinary.metrics.numeric_pair_scan_filters
                );
                assert_eq!(
                    ctx.metrics.cached_scan_filters - before.cached_scan_filters,
                    1
                );
                assert_eq!(ordinary.metrics.cached_scan_filters, 0);
                if !empty {
                    assert!(ctx.metrics.filter_seconds > before.filter_seconds);
                }
                assert_eq!(actual, expected, "workers={workers}, batch={batch_size}");
                if !empty {
                    assert!(actual.num_rows() > 2);
                }
                // A less restrictive consumer cannot use the stored subset.
                ctx.scan_filters.insert(
                    right.as_ref() as *const Node as usize,
                    filter(&[2, 4], &budget),
                );
                ordinary.scan_filters.insert(
                    right.as_ref() as *const Node as usize,
                    filter(&[2, 4], &budget),
                );
                assert_eq!(
                    collected(right, &mut ctx).unwrap(),
                    collected(right, &mut ordinary).unwrap()
                );
                assert_eq!(ctx.metrics.filtered_scan_reuses, 1);
                drop(_capture);
                drop(ctx);
                drop(ordinary);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn incomplete_cancelled_or_over_limit_stores_are_never_published() {
    let fixture = Fixture::new(false);
    for mode in ["row-limit", "byte-limit", "reservation", "error", "cancel"] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 1,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let root = Node {
                schema: fixture.schema.clone(),
                estimated_rows: 1_000_000,
                op: RelOp::UnionAll {
                    left: Box::new(fixture.scan()),
                    right: Box::new(fixture.scan()),
                },
            };
            let RelOp::UnionAll { left, .. } = &root.op else {
                unreachable!()
            };
            let mut ctx = context(&options, &budget);
            ctx.filtered_reuse = Cache::new(&root, &budget);
            let _capture = Capture::new(&ctx);
            ctx.scan_filters.insert(
                left.as_ref() as *const Node as usize,
                filter(&[1, 2, 3], &budget),
            );
            if matches!(mode, "error" | "cancel") {
                let result = left.run(&mut ctx, &mut |_| {
                    if mode == "cancel" {
                        options.cancellation.cancel();
                        Ok(())
                    } else {
                        Err("decimal precision 38 overflow".into())
                    }
                });
                assert_eq!(
                    result.unwrap_err(),
                    if mode == "cancel" {
                        "cancelled: query execution"
                    } else {
                        "decimal precision 38 overflow"
                    }
                );
            } else {
                let mut pending = Pending::new(left, &ctx).unwrap();
                if mode == "row-limit" {
                    pending.0.row_limit = 0;
                }
                if mode == "byte-limit" {
                    pending.0.byte_limit = pending.0.credit.bytes();
                }
                let held = (mode == "reservation")
                    .then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
                if mode != "row-limit" {
                    assert!(!pending.push(&RecordBatch::new_empty(fixture.schema.clone())));
                }
                // A nonempty batch exceeds the zero-row ceiling too.
                if mode == "row-limit" {
                    let b = RecordBatch::try_new(
                        fixture.schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(vec![1])),
                            Arc::new(Int64Array::from(vec![1])),
                            Arc::new(
                                Decimal128Array::from(vec![1])
                                    .with_precision_and_scale(15, 2)
                                    .unwrap(),
                            ),
                            Arc::new(
                                Decimal128Array::from(vec![1])
                                    .with_precision_and_scale(15, 2)
                                    .unwrap(),
                            ),
                        ],
                    )
                    .unwrap();
                    assert!(!pending.push(&b));
                }
                drop(held);
                drop(pending);
            }
            assert!(
                ctx.filtered_reuse
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .entries
                    .is_empty()
            );
            drop(_capture);
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

// Disable the existing raw cache in tiny fixtures, which otherwise hides the
// large-source dispatch. Only admission estimates change; SQL and data do not.
fn large_scans(node: &mut Node, fact: &std::path::Path) {
    if matches!(
        node.op,
        RelOp::Materialize {
            shared_scan: true,
            ..
        }
    ) {
        let RelOp::Materialize { input, .. } = std::mem::replace(&mut node.op, RelOp::Empty) else {
            unreachable!()
        };
        *node = *input;
    }
    match &mut node.op {
        RelOp::Scan { path, distinct, .. } if path == fact => {
            node.estimated_rows = 1_000_000;
            distinct.fill(1_000_000);
        }
        RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => large_scans(input, fact),
        RelOp::Join { left, right, .. }
        | RelOp::UnionAll { left, right }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. } => {
            large_scans(left, fact);
            large_scans(right, fact);
        }
        RelOp::ScalarDefault {
            input, fallback, ..
        } => {
            large_scans(input, fact);
            large_scans(fallback, fact);
        }
        _ => (),
    }
}

#[test]
fn sql_filtered_aggregate_and_independent_existence_predicates_match_serial() {
    let fixture = Fixture::new(false);
    for (sql, paired) in [
        (
            "SELECT SUM(f.w) FROM f JOIN d ON f.k=d.k WHERE f.v < (SELECT 0.2*AVG(g.v) FROM f g WHERE g.k=d.k)",
            false,
        ),
        (
            "SELECT d.k,d.s FROM d WHERE EXISTS (SELECT * FROM f a WHERE a.k=d.k AND a.s<>d.s) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.s<>d.s AND b.v>b.w)",
            true,
        ),
    ] {
        for batch_size in [3, 127] {
            let options = Options {
                batch_size,
                ..Options::default()
            };
            // Keep physical projections distinct; tiny raw shared scans otherwise
            // hide the same large-file shapes exercised in production.
            let planning = Options {
                memory_bytes: 256 << 10,
                ..options.clone()
            };
            let mut prepared = execute::prepare(sql, &fixture.tables(), &planning).unwrap();
            let root = prepared.relational.as_mut().unwrap();
            large_scans(root, &fixture.path);
            let budget = Budget::new(options.memory_bytes);
            let serial = super::super::execute_once_with_demand_cache(
                root,
                &options,
                budget.clone(),
                None,
                false,
                None,
            )
            .unwrap();
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let actual =
                    super::super::execute_once(root, &options, budget.clone(), None, false)
                        .unwrap();
                assert_eq!(actual.batch, serial.batch, "{sql}: workers={workers}");
                if paired {
                    // L2 needs the non-late other-supplier rows for keys 1/2.
                    // Key 2 is duplicated; key 3 lacks another supplier and
                    // key 4 has another late supplier. NULL matches neither.
                    assert_eq!(
                        actual
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![Some(1), Some(2), Some(2)]
                    );
                    assert_eq!(
                        actual.metrics.paired_existence_scans,
                        1,
                        "{}",
                        outline(root)
                    );
                } else {
                    assert!(actual.metrics.filtered_scan_reuses > 0, "{}", outline(root));
                }
                drop(actual);
            }
            drop(serial);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn sql_mixed_keys_empty_sides_and_error_barriers_match_serial() {
    for empty in [false, true] {
        let fixture = Fixture::new(empty);
        for sql in [
            "SELECT d.k FROM d WHERE EXISTS (SELECT * FROM f a WHERE a.k=d.k AND a.s<>d.s) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.s<>d.s AND b.v>b.w)",
            "SELECT d.k FROM d WHERE d.k<0 AND EXISTS (SELECT * FROM f a WHERE a.k=d.k) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.v>b.w)",
            "SELECT d.k FROM d WHERE EXISTS (SELECT * FROM f a WHERE a.k=d.v) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.v>b.w)",
            "SELECT SUM(f.w) FROM f JOIN d ON f.k=d.k WHERE f.v < (SELECT AVG(g.v*99999999999999999999999999999999999999) FROM f g WHERE g.k=d.k)",
        ] {
            let options = Options {
                batch_size: 31,
                ..Options::default()
            };
            // Keep physical projections distinct; tiny raw shared scans otherwise
            // hide the same large-file shapes exercised in production.
            let planning = Options {
                memory_bytes: 256 << 10,
                ..options.clone()
            };
            let mut prepared = execute::prepare(sql, &fixture.tables(), &planning).unwrap();
            let root = prepared.relational.as_mut().unwrap();
            large_scans(root, &fixture.path);
            let expected = super::super::execute_once_with_demand_cache(
                root,
                &options,
                Budget::new(options.memory_bytes),
                None,
                false,
                None,
            )
            .map(|e| e.batch);
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size: 31,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let actual =
                    super::super::execute_once(root, &options, budget.clone(), None, false)
                        .map(|e| e.batch);
                assert_eq!(actual, expected, "{sql}, workers={workers}");
                assert_eq!(budget.used(), 0);
                options.cancellation.cancel();
                assert_eq!(
                    super::super::execute_once(root, &options, budget.clone(), None, false)
                        .err()
                        .unwrap(),
                    "cancelled: query execution"
                );
                assert_eq!(budget.used(), 0);
            }
        }
    }
}

fn outline(node: &Node) -> String {
    fn walk(node: &Node, depth: usize, lines: &mut Vec<String>) {
        let label = match &node.op {
            RelOp::Scan { columns, .. } => format!("Scan {columns:?}"),
            RelOp::Join {
                kind,
                keys,
                left,
                right,
                ..
            } => format!(
                "{kind:?} {keys:?} same_schema={} conditions_total={} left_total={} right_total={}",
                node.schema == left.schema,
                membership_safety::join_conditions_are_total(node),
                membership_safety::input_is_total(left),
                membership_safety::input_is_total(right)
            ),
            RelOp::Project { columns, .. } => format!("Project {columns:?}"),
            RelOp::Filter { predicate, .. } => format!("Filter {predicate:?}"),
            RelOp::Unary { input, plan } => format!(
                "Unary projection={:?} total={} columns={:?} output={:?}",
                super::super::column_projection::columns(plan, &input.schema, &node.schema),
                membership_safety::input_is_total(node),
                plan.columns,
                plan.output
            ),
            RelOp::Rename { .. } => "Rename".into(),
            _ => "other".into(),
        };
        lines.push(format!(
            "{}{} rows={} fields={}",
            " ".repeat(depth),
            label,
            node.estimated_rows,
            node.schema.fields().len()
        ));
        match &node.op {
            RelOp::Unary { input, .. }
            | RelOp::Filter { input, .. }
            | RelOp::Project { input, .. }
            | RelOp::Rename { input } => walk(input, depth + 1, lines),
            RelOp::Join { left, right, .. } => {
                walk(left, depth + 1, lines);
                walk(right, depth + 1, lines);
            }
            _ => (),
        }
    }
    let mut lines = Vec::new();
    walk(node, 0, &mut lines);
    lines.join("\n")
}

#[test]
fn paired_existence_active_cancellation_releases_build_and_scan_credit() {
    let fixture = Fixture::new(false);
    let sql = "SELECT d.k,d.s FROM d WHERE EXISTS (SELECT * FROM f a WHERE a.k=d.k AND a.s<>d.s) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.s<>d.s AND b.v>b.w)";
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 1,
            ..Options::default()
        };
        // Keep physical projections distinct; tiny raw shared scans otherwise
        // hide the same large-file shapes exercised in production.
        let planning = Options {
            memory_bytes: 256 << 10,
            ..options.clone()
        };
        let mut prepared = execute::prepare(sql, &fixture.tables(), &planning).unwrap();
        let root = prepared.relational.as_mut().unwrap();
        large_scans(root, &fixture.path);
        let budget = Budget::new(options.memory_bytes);
        let mut ctx = context(&options, &budget);
        let attempted = std::sync::atomic::AtomicBool::new(false);
        ctx.demand_cache = Some(&attempted);
        let mut emitted = 0;
        let result = root.run(&mut ctx, &mut |_| {
            emitted += 1;
            options.cancellation.cancel();
            Ok(())
        });
        assert_eq!(result.unwrap_err(), "cancelled: query execution");
        assert!(attempted.load(Ordering::Relaxed));
        assert_eq!(emitted, 1);
        drop(ctx);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn optional_reuse_keeps_the_ordinary_outcome_under_memory_pressure() {
    let fixture = Fixture::new(false);
    for sql in [
        "SELECT SUM(f.w) FROM f JOIN d ON f.k=d.k WHERE f.v < (SELECT 0.2*AVG(g.v) FROM f g WHERE g.k=d.k)",
        "SELECT d.k,d.s FROM d WHERE EXISTS (SELECT * FROM f a WHERE a.k=d.k AND a.s<>d.s) AND NOT EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.s<>d.s AND b.v>b.w)",
    ] {
        for memory_bytes in [1 << 20, 4 << 20] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    memory_bytes,
                    batch_size: 31,
                    ..Options::default()
                };
                let mut prepared = execute::prepare(sql, &fixture.tables(), &options).unwrap();
                let root = prepared.relational.as_mut().unwrap();
                large_scans(root, &fixture.path);
                let budget = Budget::new(memory_bytes);
                let expected = super::super::execute_once_with_demand_cache(
                    root,
                    &options,
                    budget.clone(),
                    None,
                    false,
                    None,
                )
                .map(|e| e.batch);
                assert_eq!(budget.used(), 0);
                let actual =
                    super::super::execute_once(root, &options, budget.clone(), None, false)
                        .map(|e| e.batch);
                assert_eq!(
                    actual, expected,
                    "workers={workers}, budget={memory_bytes}, {sql}"
                );
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}
