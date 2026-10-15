//! Optional direct-column rejection filters. Exact join probing remains mandatory.
use super::*;

type SavedFilter = (usize, Option<Arc<ScanKeyFilter>>);
#[derive(Default)]
pub(super) struct Installed {
    // A numeric pair may project onto two different scans, or twice onto one.
    // Stack storage avoids an unfunded scope vector and replacement allocation.
    saved: [Option<SavedFilter>; 8],
}
impl Installed {
    pub(super) fn restore(self, ctx: &mut Context<'_>) {
        for (id, previous) in self.saved.into_iter().rev().flatten() {
            if let Some(previous) = previous {
                ctx.scan_filters.insert(id, previous);
            } else {
                ctx.scan_filters.remove(&id);
            }
        }
    }
}

pub(super) fn traversal_bytes(node: &Node) -> usize {
    let children = match &node.op {
        RelOp::WindowOrder { input, .. } | RelOp::Window { input, .. } => traversal_bytes(input),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Materialize { input, .. } => traversal_bytes(input),
        RelOp::Join { left, right, .. } => {
            traversal_bytes(left).saturating_add(traversal_bytes(right))
        }
        _ => 0,
    };
    children.saturating_add(
        node.schema
            .fields()
            .len()
            .saturating_add(1)
            .saturating_mul(256),
    )
}

/// Payload saved versus a key pass, membership construction and setup. This is
/// an admission estimate only; no estimated value participates in matching.
pub(super) fn payload_worthwhile(target: &ScanTarget, domain: usize, count: usize) -> bool {
    if domain == 0 || count >= domain {
        return false;
    }
    let rejected = (target.probe_rows as u128) * (domain - count) as u128 / domain as u128;
    let saved = rejected * (target.payload as u128 + 16);
    let work = target.probe_rows as u128 * 8 + count as u128 * 48 + 64 * 1024;
    saved * 2 > work * 3
}

/// Wide unfiltered scans can earn a key pass even at moderate selectivity.
/// Keep at least half the estimated domain rejected and 50% cost headroom.
pub(super) fn wide_worthwhile(target: &ScanTarget, domain: usize, count: usize) -> bool {
    target.payload >= 32
        && !target.has_filter
        && count.saturating_mul(2) < domain
        && payload_worthwhile(target, domain, count)
}

/// Cost estimates only choose a rejection pre-filter; every survivor still needs the exact join.
fn worthwhile(target: &ScanTarget, domain: usize, count: usize) -> bool {
    let narrow = count.saturating_mul(16) < domain;
    if (8..16).contains(&target.payload) {
        // On an unfiltered narrow scan, rejection also avoids normalizing a
        // 16-byte join key for each eliminated row. Charge an 8-byte key pass,
        // three 16-byte passes per distinct build key, and a setup allowance.
        // Require 50% headroom over that work estimate; no page skipping credit.
        if target.has_filter || count.saturating_mul(8) >= domain {
            return false;
        }
        return payload_worthwhile(target, domain, count);
    }
    (!target.has_filter || narrow)
        && count.saturating_mul(4) < domain
        && target.payload >= if narrow { 16 } else { 32 }
}

pub(super) fn install(
    ctx: &mut Context<'_>,
    node: &Node,
    expressions: Vec<Expr>,
    keys: &crate::join_index::JoinIndex<'_>,
) -> Result<Option<Installed>> {
    install_after_transfer(ctx, node, expressions, keys, None)
}

// A completed sideways-restricted build can restrict a later build too. The
// saved index/key construction earns a pass even on a narrow, filtered scan.
// Estimates choose work only; the bitmap and retained join decide membership.
fn build_target(node: &Node, id: usize, in_build: bool) -> bool {
    if node as *const Node as usize == id {
        return in_build;
    }
    match &node.op {
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => build_target(left, id, in_build) || build_target(right, id, true),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => build_target(input, id, in_build),
        _ => false,
    }
}

pub(super) fn install_after_transfer(
    ctx: &mut Context<'_>,
    node: &Node,
    expressions: Vec<Expr>,
    keys: &crate::join_index::JoinIndex<'_>,
    transferred: Option<(&RecordBatch, &[(Expr, Expr)])>,
) -> Result<Option<Installed>> {
    // Save only inline direct numeric expressions for the optional extension.
    // The ordinary path consumes the original vector without a deep clone.
    let numeric = (1..=if transferred.is_some() { 8 } else { 2 }).contains(&expressions.len())
        && expressions
            .iter()
            .all(|e| e.ty.scale().is_ok() && matches!(e.kind, crate::expr::Kind::Column(_)));
    let components: [Option<Expr>; 8] = std::array::from_fn(|i| {
        if numeric {
            expressions.get(i).cloned()
        } else {
            None
        }
    });
    let arity = expressions.len();
    // Existing small exact filters never replace an installed filter.
    if let Some(id) = install_exact_scan_filter(ctx, node, expressions, keys)? {
        let mut installed = Installed::default();
        installed.saved[0] = Some((id, None));
        return Ok(Some(installed));
    }
    if !ctx.options.relational_optimizations
        || ctx
            .options
            .parquet_cache_bytes
            .is_none_or(|bytes| bytes == 0)
        || !numeric
        || keys.is_empty()
        || keys.len() > 1 << 22
    {
        return Ok(None);
    }
    ctx.options.cancellation.check()?;
    let mut installed = Installed::default();
    for (component, expression) in components.iter().flatten().enumerate() {
        // Fund descriptors, traversal/remapping scratch, and predicate clones
        // before allocating. The bitmap owns a separate, at-most-8-MiB lease.
        // Both old and new filters remain charged throughout replacement.
        let descriptor_bytes = traversal_bytes(node)
            .saturating_add(ctx.options.workers.saturating_add(2).saturating_mul(2048));
        let Ok(lease) = ctx.budget.reserve(descriptor_bytes) else {
            continue;
        };
        let Ok(Some(target)) = scan_target(node, vec![expression.clone()]) else {
            continue;
        };
        let previous = ctx.scan_filters.get(&target.id);
        // A new predicate must not suppress a previously installed expression's
        // error. Bound chain metadata/work, independently of the byte budget.
        if previous.is_some_and(|filter| {
            filter.iter().count() >= 8
                || filter.iter().any(|filter| {
                    filter
                        .expressions
                        .iter()
                        .any(|e| !matches!(e.kind, crate::expr::Kind::Column(_)))
                })
        }) {
            continue;
        }
        let domain = target
            .key_domain
            .min(filter_distinct(node, std::slice::from_ref(expression)));
        let building = transferred.is_some() && build_target(node, target.id, false);
        let worthwhile = |count: usize| {
            worthwhile(&target, domain, count)
                || (transferred.is_some()
                    && target.probe_rows >= 4096
                    && target.payload >= 8
                    && count.saturating_mul(4) < domain
                    && building)
        };
        // A single-key index already knows the exact non-NULL cardinality.
        // Decide whether the scan can earn the key pass before walking any keys
        // or funding a bitmap that the later payload/static-filter guard rejects.
        // Merge of TK1 (wide demand) and SIP1 (sideways transfer): `worthwhile` is SIP1's closure,
        // which extends the original single-key rule; TK1's wide path applies only when it declines.
        let ordinary = arity != 1 || worthwhile(keys.len());
        let wide = !ordinary
            && ctx
                .options
                .parquet_cache_bytes
                .is_some_and(|bytes| bytes > 0)
            && expression.ty == Type::Int
            && wide_worthwhile(&target, domain, keys.len())
            && membership_safety::input_is_total(node);
        if target.payload < if arity == 1 || building { 8 } else { 16 } || (!ordinary && !wide) {
            continue;
        }
        let bitmap = if arity == 1 {
            keys.scan_membership(&ctx.budget)
        } else if arity == 2 {
            keys.scan_pair_membership(component, &ctx.budget)
        } else {
            let Some((build, join_keys)) = transferred else {
                continue;
            };
            let crate::expr::Kind::Column(column) = join_keys[component].1.kind else {
                continue;
            };
            // More than two keys use the completed typed build, not an assumed
            // byte layout. Rows NULL in another component may add false positives
            // only; the exact tuple join remains mandatory for every survivor.
            crate::membership::NumericMembership::from_values(
                || (0..build.num_rows()).filter_map(|row| sideways::number(build, column, row)),
                &ctx.budget,
            )
        };
        let Some(bitmap) = bitmap else {
            continue;
        };
        // Use component cardinality, not tuple count: projecting duplicate
        // components is a superset even when the tuple domain is much larger.
        // Tuple NDV is not component NDV. Defer only projected-pair admission
        // until the funded bitmap tells us the component's actual cardinality.
        if arity >= 2 && !worthwhile(bitmap.cardinality()) {
            continue;
        }
        let filter = Arc::new(ScanKeyFilter {
            previous: previous.cloned(),
            optional_total: true,
            expressions: target.expressions,
            members: Arc::new(HashSet::default()),
            numeric: Some(Arc::new(bitmap)),
            numeric_pair: None,
            _lease: Arc::new(lease),
        });
        installed.saved[component] = Some((target.id, ctx.scan_filters.insert(target.id, filter)));
    }
    if let Err(error) = ctx.options.cancellation.check() {
        installed.restore(ctx);
        return Err(error);
    }
    Ok(installed
        .saved
        .iter()
        .any(Option::is_some)
        .then_some(installed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::{Kind, Value},
        join_index::JoinIndex,
        keys::Keys,
    };

    fn column(i: usize) -> Expr {
        Expr {
            kind: Kind::Column(i),
            ty: Type::Int,
        }
    }
    fn scan() -> Node {
        Node {
            schema: Arc::new(Schema::new(vec![
                Field::new("x", arrow::datatypes::DataType::Int64, true),
                Field::new("y", arrow::datatypes::DataType::Int64, true),
                Field::new("payload", arrow::datatypes::DataType::Utf8, true),
            ])),
            estimated_rows: 32_000_000,
            op: RelOp::Scan {
                path: PathBuf::new(),
                columns: vec![0, 1, 2],
                distinct: vec![32_000_000; 3],
                statistics: Vec::new(),
            },
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
            lane_transforms: Default::default(),
            replay_transforms: HashMap::new(),
            materialized: HashMap::new(),
            sideways_registry_lease: None,
            spill: None,
            budget: budget.clone(),
            metrics: Metrics::default(),
        }
    }
    fn with_keys(
        columns: Vec<Vec<Option<i128>>>,
        budget: &Budget,
        run: impl FnOnce(&JoinIndex<'_>),
    ) {
        let rows = columns[0].len();
        let values = columns
            .into_iter()
            .map(|column| Value::adopt(Arc::new(Decimal128Array::from(column)), budget).unwrap())
            .collect::<Vec<_>>();
        let encoded = Keys::new(&values, rows, budget).unwrap();
        let mut index = JoinIndex::new(&encoded, rows, budget, true).unwrap();
        let mut next = vec![u32::MAX; rows];
        index.populate(&encoded, &mut next);
        run(&index);
    }
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            scan().schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(1),
                    Some(1),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(8),
                    Some(7),
                    Some(7),
                    None,
                    Some(7),
                ])),
                Arc::new(StringArray::from(vec!["payload"; 6])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn pair_components_pay_only_after_their_own_scan_guards() {
        for eligible in [0, 1] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let budget = Budget::new(1 << 20);
                let input = Box::new(scan());
                let sql = if eligible == 1 {
                    "SELECT x+1 AS x,y,payload"
                } else {
                    "SELECT x,y+1 AS y,payload"
                };
                let plan =
                    bind::bind(&bind::parse(sql).unwrap(), &input.schema, None, true).unwrap();
                let node = Node {
                    schema: input.schema.clone(),
                    estimated_rows: input.estimated_rows,
                    op: RelOp::Unary { input, plan },
                };
                assert!(
                    scan_target(&node, vec![column(eligible)])
                        .unwrap()
                        .is_some()
                );
                assert!(
                    scan_target(&node, vec![column(1 - eligible)])
                        .unwrap()
                        .is_none()
                );
                let mut columns = vec![vec![Some(0), Some(1)]; 2];
                columns[eligible] = vec![Some(7), Some(8)];
                with_keys(columns, &budget, |keys| {
                    let before = budget.used();
                    let descriptors = traversal_bytes(&node) + (workers + 2) * 2048;
                    // One descriptor and one two-bit bitmap fit exactly. An
                    // ineligible component must not consume any of this space.
                    let held = budget
                        .reserve(budget.limit() - before - descriptors - 129)
                        .unwrap();
                    let mut ctx = context(&options, &budget);
                    let installed = install(&mut ctx, &node, vec![column(0), column(1)], keys)
                        .unwrap()
                        .expect("eligible component must retain its funding");
                    assert!(installed.saved[eligible].is_some());
                    assert!(installed.saved[1 - eligible].is_none());
                    let filter = ctx.scan_filters.values().next().unwrap();
                    assert_eq!(filter.expressions, vec![column(eligible)]);
                    let bitmap = filter.numeric.as_ref().unwrap();
                    assert_eq!(bitmap.cardinality(), 2);
                    assert!(bitmap.contains(7) && bitmap.contains(8));
                    assert!(!bitmap.contains(0));
                    assert_eq!(budget.used(), budget.limit());
                    installed.restore(&mut ctx);
                    drop((ctx, held));
                    assert_eq!(budget.used(), before);
                });
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn sparse_transferred_query_keeps_batches_errors_and_live_cancellation() {
        use arrow::datatypes::DataType;
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
        let numbers = |array: &ArrayRef| -> Vec<Option<i128>> {
            if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                array.iter().map(|n| n.map(i128::from)).collect()
            } else {
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect()
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("n", DataType::Int64, false),
        ]));
        let mut tables = HashMap::new();
        for (name, rows) in [("probe", 257), ("build", 1024)] {
            let path = std::env::temp_dir().join(format!(
                "assaydb-sparse-transfer-{}-{name}.parquet",
                std::process::id()
            ));
            let x = (0..rows)
                .map(|row| {
                    if name == "build" {
                        match row {
                            0 => Some(0),
                            1 => Some(1_000_000),
                            _ => None,
                        }
                    } else {
                        match row % 4 {
                            0 => Some(0),
                            1 => Some(1_000_000),
                            2 => None,
                            _ => Some(-1),
                        }
                    }
                })
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(x)),
                    Arc::new(Int64Array::from_iter_values(0..rows)),
                ],
            )
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create_new(&path).unwrap(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(31)
                        .build(),
                ),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            tables.insert(name.to_string(), path);
        }
        let scan = |name: &str, rows| Node {
            schema: schema.clone(),
            estimated_rows: rows,
            op: RelOp::Scan {
                path: tables[name].clone(),
                columns: vec![0, 1],
                distinct: vec![rows; 2],
                statistics: vec![],
            },
        };
        // Fixed plan for SELECT p.x,p.n,b.x,b.n FROM probe p JOIN build b
        // ON p.x=b.x. Estimates affect admission only, never the expected rows.
        let root = Node {
            schema: combined(&schema, &schema),
            estimated_rows: 1_000_000,
            op: RelOp::Join {
                left: Box::new(scan("probe", 1_000_000)),
                right: Box::new(scan("build", 1024)),
                kind: JoinKind::Inner,
                keys: vec![(column(0), column(0))],
                residual: None,
            },
        };
        let expected = (0_i128..257)
            .filter(|n| n % 4 < 2)
            .map(|n| {
                let x = if n % 4 == 0 { 0 } else { 1_000_000 };
                vec![Some(x), Some(n), Some(x), Some(n % 4)]
            })
            .collect::<Vec<_>>();
        for batch_size in [1, 7, 8192] {
            let mut serial = None;
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 2 << 20,
                    parquet_cache_bytes: Some(64 << 10),
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let mut ctx = context(&options, &budget);
                let mut batches = Vec::new();
                root.run(&mut ctx, &mut |batch| {
                    let columns = batch.columns().iter().map(&numbers).collect::<Vec<_>>();
                    batches.push(
                        (0..batch.num_rows())
                            .map(|row| columns.iter().map(|column| column[row]).collect::<Vec<_>>())
                            .collect::<Vec<_>>(),
                    );
                    Ok(())
                })
                .unwrap();
                assert_eq!(
                    batches.iter().flatten().cloned().collect::<Vec<_>>(),
                    expected
                );
                if let Some(serial) = &serial {
                    assert_eq!(
                        &batches, serial,
                        "emitted boundaries: workers={workers} batch={batch_size}"
                    );
                } else {
                    serial = Some(batches);
                }
                assert_eq!(ctx.metrics.lane_fused_joins, usize::from(workers > 1));
                drop(ctx);
                assert_eq!(budget.used(), 0);

                // Cancel from the consumer after the transferred index has
                // actually produced a batch, while the pipeline is still live.
                let mut ctx = context(&options, &budget);
                let mut emitted = 0;
                let error = root
                    .run(&mut ctx, &mut |_| {
                        emitted += 1;
                        options.cancellation.cancel();
                        options.cancellation.check()
                    })
                    .unwrap_err();
                assert_eq!(emitted, 1);
                assert_eq!(error, options.cancellation.check().unwrap_err());
                assert_eq!(ctx.metrics.lane_fused_joins, usize::from(workers > 1));
                assert!(ctx.lane_transforms.is_empty());
                drop(ctx);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
        // Handwritten competing errors, both expression orders. No candidate
        // serial result serves as the expected error oracle.
        for (projection, expected_error) in [
            (
                "CAST('1e308' AS REAL)*CAST(p.n+2 AS REAL),1.0/(p.n-p.n)",
                "floating arithmetic overflow",
            ),
            (
                "1.0/(p.n-p.n),CAST('1e308' AS REAL)*CAST(p.n+2 AS REAL)",
                "division by zero",
            ),
        ] {
            let sql = format!("SELECT {projection} FROM probe p JOIN build b ON p.x=b.x");
            let prepared = execute::prepare(&sql, &tables, &Options::default()).unwrap();
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        memory_bytes: 2 << 20,
                        ..Options::default()
                    };
                    assert_eq!(
                        execute::execute(&prepared, &options).err().unwrap(),
                        expected_error
                    );
                }
            }
        }
        // A constrained-budget SQL execution of the sparse build also returns
        // explicit values, without using serial execution as its oracle.
        let sql = "SELECT p.n,b.n FROM probe p JOIN build b ON p.x=b.x ORDER BY 1";
        let prepared = execute::prepare(sql, &tables, &Options::default()).unwrap();
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 7,
                memory_bytes: 2 << 20,
                ..Options::default()
            };
            let result = execute::execute(&prepared, &options).unwrap();
            assert_eq!(
                numbers(result.batch.column(0)),
                expected.iter().map(|row| row[1]).collect::<Vec<_>>()
            );
            assert_eq!(
                numbers(result.batch.column(1)),
                expected.iter().map(|row| row[3]).collect::<Vec<_>>()
            );
        }
        for path in tables.values() {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn nested_filters_intersect_restore_and_cancel_without_leaking() {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 2,
                ..Options::default()
            };
            let budget = Budget::new(16 << 20);
            let mut ctx = context(&options, &budget);
            let node = scan();
            with_keys(vec![vec![Some(1), Some(1), None]], &budget, |keys| {
                let outer = install(&mut ctx, &node, vec![column(0)], keys)
                    .unwrap()
                    .unwrap();
                let id = &node as *const Node as usize;
                let original = ctx.scan_filters[&id].clone();
                with_keys(vec![vec![Some(7), Some(7), None]], &budget, |keys| {
                    let inner = install(&mut ctx, &node, vec![column(1)], keys)
                        .unwrap()
                        .unwrap();
                    let filter = ctx.scan_filters[&id].clone();
                    assert_eq!(filter.iter().count(), 2);
                    let mask = scan_key_mask(&batch(), &filter, &budget).unwrap();
                    assert_eq!(
                        mask.iter().collect::<Vec<_>>(),
                        vec![
                            Some(true),
                            Some(false),
                            Some(false),
                            Some(false),
                            Some(false),
                            Some(true)
                        ]
                    );
                    let batches = (0..24)
                        .flat_map(|_| {
                            let b = batch();
                            [b.slice(0, 2), b.slice(2, 2), b.slice(4, 2)]
                        })
                        .collect::<Vec<_>>();
                    let mut output = Vec::new();
                    replay::run(
                        &batches,
                        &node.schema,
                        Some(&filter),
                        None,
                        &options,
                        &budget,
                        &mut |b| {
                            output.extend(
                                b.column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .iter(),
                            );
                            Ok(())
                        },
                    )
                    .unwrap();
                    assert_eq!(output, vec![Some(1); 48]);
                    let error = replay::run(
                        &batches,
                        &node.schema,
                        Some(&filter),
                        None,
                        &options,
                        &budget,
                        &mut |_| {
                            options.cancellation.cancel();
                            options.cancellation.check()
                        },
                    )
                    .err()
                    .unwrap();
                    assert_eq!(error, options.cancellation.check().unwrap_err());
                    assert_eq!(budget.workers(10, 10).count, 10);
                    inner.restore(&mut ctx);
                    assert!(Arc::ptr_eq(&ctx.scan_filters[&id], &original));
                });
                outer.restore(&mut ctx);
                assert!(ctx.scan_filters.is_empty());
            });
            drop(ctx);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn raised_caps_project_pairs_and_retain_exact_join_authority() {
        let options = Options::default();
        let budget = Budget::new(256 << 20);
        let node = scan();
        let mut ctx = context(&options, &budget);
        let rows = 65_537_i128;
        let a = (0..rows)
            .map(|i| Some(i / 64))
            .chain([None, Some(1024)])
            .collect();
        let b = (0..rows)
            .map(|i| Some(i % 64))
            .chain([Some(0), None])
            .collect();
        with_keys(vec![a, b], &budget, |keys| {
            let installed = install(&mut ctx, &node, vec![column(0), column(1)], keys)
                .unwrap()
                .unwrap();
            let filter = &ctx.scan_filters[&(&node as *const Node as usize)];
            assert_eq!(filter.iter().count(), 2);
            assert!(
                filter
                    .iter()
                    .all(|f| f.members.is_empty() && f.numeric.is_some())
            );
            let probe = RecordBatch::try_new(
                node.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1024), Some(1024), None])),
                    Arc::new(Int64Array::from(vec![Some(0), Some(63), Some(0)])),
                    Arc::new(StringArray::from(vec!["p"; 3])),
                ],
            )
            .unwrap();
            let mask = scan_key_mask(&probe, filter, &budget).unwrap();
            // The crossed pair is deliberately retained by the superset filter.
            assert_eq!(
                mask.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(true), Some(false)]
            );
            let values = input(&probe, &budget, [&column(0), &column(1)]).unwrap();
            let values = [column(0), column(1)]
                .iter()
                .map(|e| e.eval(&values, &budget).unwrap())
                .collect::<Vec<_>>();
            let exact = Keys::new(&values, 3, &budget).unwrap();
            assert!(keys.get(exact.row(0)).is_some());
            assert!(keys.get(exact.row(1)).is_none());
            installed.restore(&mut ctx);
        });
        // A compact single-column bitmap above the former 1,048,576-key cap.
        with_keys(vec![(0..1_048_577).map(Some).collect()], &budget, |keys| {
            let installed = install(&mut ctx, &node, vec![column(0)], keys)
                .unwrap()
                .unwrap();
            assert_eq!(
                ctx.scan_filters[&(&node as *const Node as usize)]
                    .numeric
                    .as_ref()
                    .unwrap()
                    .cardinality(),
                1_048_577
            );
            installed.restore(&mut ctx);
        });
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn narrow_scan_costing_requires_selectivity_work_and_an_unfiltered_target() {
        for workers in [1, 2, 4, 10] {
            for rows in [64, 2_000_000] {
                for filtered in [false, true] {
                    let options = Options {
                        workers,
                        ..Options::default()
                    };
                    let budget = Budget::new(1 << 20);
                    let mut ctx = context(&options, &budget);
                    let mut input = Box::new(scan());
                    input.schema = Arc::new(Schema::new(vec![
                        Field::new("x", arrow::datatypes::DataType::Int64, true),
                        Field::new("y", arrow::datatypes::DataType::Int64, true),
                    ]));
                    input.estimated_rows = rows;
                    if let RelOp::Scan {
                        columns, distinct, ..
                    } = &mut input.op
                    {
                        *columns = vec![0, 1];
                        *distinct = vec![64, 64];
                    }
                    let schema = input.schema.clone();
                    let predicate = bind::bind(
                        &bind::parse("SELECT x WHERE x >= 0").unwrap(),
                        &schema,
                        None,
                        true,
                    )
                    .unwrap()
                    .filter
                    .unwrap();
                    let node = Node {
                        schema,
                        estimated_rows: rows,
                        op: if filtered {
                            RelOp::Filter { input, predicate }
                        } else {
                            RelOp::Rename { input }
                        },
                    };
                    for count in [3, 4, 7, 8] {
                        let mut values = (0..count).map(Some).collect::<Vec<_>>();
                        values.extend([None, Some(0)]);
                        with_keys(vec![values], &budget, |keys| {
                            let before = budget.used();
                            let installed =
                                install(&mut ctx, &node, vec![column(0)], keys).unwrap();
                            assert_eq!(installed.is_some(), !filtered && rows > 64 && count < 8);
                            if let Some(installed) = installed {
                                let filter = ctx.scan_filters.values().next().unwrap();
                                let probe = batch().project(&[0, 1]).unwrap();
                                assert_eq!(
                                    scan_key_mask(&probe, filter, &budget)
                                        .unwrap()
                                        .iter()
                                        .collect::<Vec<_>>(),
                                    vec![
                                        Some(true),
                                        Some(true),
                                        Some(true),
                                        Some(false),
                                        Some(true),
                                        Some(true)
                                    ]
                                );
                                installed.restore(&mut ctx);
                            }
                            assert!(ctx.scan_filters.is_empty());
                            assert_eq!(budget.used(), before);
                        });
                    }
                    drop(ctx);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }

    #[test]
    fn rejected_single_filters_do_not_fund_a_discarded_bitmap() {
        for workers in [1, 2, 4, 10] {
            for filtered in [false, true] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let budget = Budget::new(1 << 20);
                let source_budget = Budget::new(1 << 20);
                let mut ctx = context(&options, &budget);
                let mut input = Box::new(scan());
                if !filtered {
                    // Sixteen payload bytes cannot earn a moderately selective
                    // key pass; the filtered case has a wide payload instead.
                    input.schema = Arc::new(Schema::new(vec![
                        Field::new("x", arrow::datatypes::DataType::Int64, true),
                        Field::new("y", arrow::datatypes::DataType::Int64, true),
                        Field::new("payload", arrow::datatypes::DataType::Int64, true),
                    ]));
                }
                if let RelOp::Scan { distinct, .. } = &mut input.op {
                    distinct.fill(64);
                }
                let schema = input.schema.clone();
                let predicate = bind::bind(
                    &bind::parse("SELECT x WHERE x >= 0").unwrap(),
                    &schema,
                    None,
                    true,
                )
                .unwrap()
                .filter
                .unwrap();
                let node = Node {
                    schema,
                    estimated_rows: input.estimated_rows,
                    op: if filtered {
                        RelOp::Filter { input, predicate }
                    } else {
                        RelOp::Rename { input }
                    },
                };
                with_keys(
                    vec![vec![Some(0), Some(262_143), Some(524_287), Some(1_048_575)]],
                    &source_budget,
                    |keys| {
                        assert!(
                            install(&mut ctx, &node, vec![column(0)], keys)
                                .unwrap()
                                .is_none()
                        );
                        assert!(ctx.scan_filters.is_empty());
                        assert_eq!(budget.used(), 0);
                        // Traversal scratch is small. The formerly discarded
                        // 128-KiB bitmap must never be reserved in either case.
                        assert!(budget.peak() < 64 << 10);
                    },
                );
                assert_eq!(source_budget.used(), 0);
            }
        }
    }

    #[test]
    fn filtered_components_keep_the_stronger_selectivity_guard() {
        for workers in [1, 2, 4, 10] {
            for filtered in [false, true] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let budget = Budget::new(1 << 20);
                let mut ctx = context(&options, &budget);
                let mut input = Box::new(scan());
                if let RelOp::Scan { distinct, .. } = &mut input.op {
                    distinct.fill(64);
                }
                let id = &*input as *const Node as usize;
                let schema = input.schema.clone();
                let predicate = bind::bind(
                    &bind::parse("SELECT x WHERE x > 0").unwrap(),
                    &schema,
                    None,
                    true,
                )
                .unwrap()
                .filter
                .unwrap();
                let node = Node {
                    schema,
                    estimated_rows: input.estimated_rows,
                    op: if filtered {
                        RelOp::Filter { input, predicate }
                    } else {
                        RelOp::Rename { input }
                    },
                };
                with_keys(vec![vec![Some(7)]], &budget, |keys| {
                    let outer = install(&mut ctx, &node, vec![column(1)], keys)
                        .unwrap()
                        .unwrap();
                    let previous = ctx.scan_filters[&id].clone();
                    // Force the optional composition path, below, at and above
                    // the 1/16 boundary. Pair projections use their own NDV.
                    for count in [3, 4, 5] {
                        for arity in [1, 2] {
                            let mut columns = vec![(0..count).map(Some).collect::<Vec<_>>()];
                            if arity == 2 {
                                columns.push(vec![Some(7); count as usize]);
                            }
                            with_keys(columns, &budget, |keys| {
                                let before = budget.used();
                                let installed = install(
                                    &mut ctx,
                                    &node,
                                    (0..arity).map(column).collect(),
                                    keys,
                                )
                                .unwrap();
                                let admits_x = !filtered || count < 4;
                                assert_eq!(installed.is_some(), admits_x || arity == 2);
                                assert_eq!(
                                    ctx.scan_filters[&id]
                                        .iter()
                                        .any(|f| f.expressions == vec![column(0)]),
                                    admits_x
                                );
                                if let Some(installed) = installed {
                                    installed.restore(&mut ctx);
                                }
                                assert!(Arc::ptr_eq(&ctx.scan_filters[&id], &previous));
                                assert_eq!(budget.used(), before);
                            });
                        }
                    }
                    outer.restore(&mut ctx);
                });
                drop(ctx);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn strict_targets_decline_error_and_cardinality_barriers() {
        let schema = scan().schema;
        let predicate = |sql| {
            bind::bind(&bind::parse(sql).unwrap(), &schema, None, true)
                .unwrap()
                .filter
                .unwrap()
        };
        for op in [
            RelOp::Filter {
                input: Box::new(scan()),
                predicate: predicate(
                    "SELECT x WHERE x * 10000000000000000000000000000000000000 > 0",
                ),
            },
            RelOp::Limit {
                input: Box::new(scan()),
                count: 1,
            },
            RelOp::Scalar {
                input: Box::new(scan()),
            },
            RelOp::Mark {
                left: Box::new(scan()),
                right: Box::new(scan()),
                value: column(0),
                negated: false,
            },
            RelOp::Join {
                left: Box::new(scan()),
                right: Box::new(scan()),
                kind: JoinKind::Left,
                keys: vec![(column(0), column(0))],
                residual: None,
            },
            RelOp::Join {
                left: Box::new(scan()),
                right: Box::new(scan()),
                kind: JoinKind::Inner,
                keys: vec![(column(0), column(0))],
                residual: Some(predicate(
                    "SELECT x WHERE x * 10000000000000000000000000000000000000 > 0",
                )),
            },
        ] {
            let node = Node {
                schema: schema.clone(),
                estimated_rows: 1000,
                op,
            };
            assert!(scan_target(&node, vec![column(0)]).unwrap().is_none());
        }
        let total = Node {
            schema: schema.clone(),
            estimated_rows: 1000,
            op: RelOp::Filter {
                input: Box::new(scan()),
                predicate: predicate("SELECT x WHERE x > 0"),
            },
        };
        assert!(scan_target(&total, vec![column(0)]).unwrap().is_some());
    }

    #[test]
    fn every_installer_observes_error_barriers_and_releases_reservations() {
        let schema = scan().schema;
        let fallible = || {
            bind::bind(
                &bind::parse("SELECT x WHERE x * 10000000000000000000000000000000000000 > 0")
                    .unwrap(),
                &schema,
                None,
                true,
            )
            .unwrap()
            .filter
            .unwrap()
        };
        for op in [
            RelOp::Filter {
                input: Box::new(scan()),
                predicate: fallible(),
            },
            RelOp::Unary {
                input: Box::new(scan()),
                plan: bind::bind(
                    &bind::parse(
                        "SELECT x, y, x * 10000000000000000000000000000000000000 AS payload",
                    )
                    .unwrap(),
                    &schema,
                    None,
                    true,
                )
                .unwrap(),
            },
            RelOp::Join {
                left: Box::new(scan()),
                right: Box::new(scan()),
                kind: JoinKind::Inner,
                keys: vec![(column(0), column(0))],
                residual: Some(fallible()),
            },
            RelOp::Join {
                left: Box::new(scan()),
                right: Box::new(scan()),
                kind: JoinKind::Inner,
                keys: vec![(
                    bind::bind(
                        &bind::parse("SELECT x * 10000000000000000000000000000000000000").unwrap(),
                        &schema,
                        None,
                        true,
                    )
                    .unwrap()
                    .output
                    .into_iter()
                    .find_map(|output| match output {
                        bind::Output::Expr(expr) => Some(expr),
                        _ => None,
                    })
                    .unwrap(),
                    column(0),
                )],
                residual: None,
            },
        ] {
            let node = Node {
                schema: schema.clone(),
                estimated_rows: 32_000_000,
                op,
            };
            for parquet_cache_bytes in [None, Some(0), Some(16 * 1024 * 1024)] {
                for arity in [1, 2] {
                    let options = Options {
                        parquet_cache_bytes,
                        ..Options::default()
                    };
                    let budget = Budget::new(1 << 20);
                    let mut ctx = context(&options, &budget);
                    with_keys(vec![vec![Some(1)]; arity], &budget, |keys| {
                        let before = budget.used();
                        // Exercise the formerly unguarded early-return path,
                        // including encoded pairs and cached numeric bitmaps.
                        assert!(
                            install_exact_scan_filter(
                                &mut ctx,
                                &node,
                                (0..arity).map(column).collect(),
                                keys,
                            )
                            .unwrap()
                            .is_none()
                        );
                        // Its fallback must observe exactly the same barrier.
                        assert!(
                            install(&mut ctx, &node, (0..arity).map(column).collect(), keys,)
                                .unwrap()
                                .is_none()
                        );
                        assert!(ctx.scan_filters.is_empty());
                        assert_eq!(budget.used(), before);
                    });
                    drop(ctx);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
    }

    #[test]
    fn optional_funding_denial_keeps_prior_filter_and_releases_partial_state() {
        let options = Options::default();
        let budget = Budget::new(1 << 20);
        let mut ctx = context(&options, &budget);
        let node = scan();
        with_keys(vec![vec![Some(1), Some(2)]], &budget, |keys| {
            let outer = install(&mut ctx, &node, vec![column(0)], keys)
                .unwrap()
                .unwrap();
            let id = &node as *const Node as usize;
            let original = ctx.scan_filters[&id].clone();
            let hold = budget
                .reserve(budget.limit() - budget.used() - 512)
                .unwrap();
            let before = budget.used();
            assert!(
                install(&mut ctx, &node, vec![column(1)], keys)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(budget.used(), before);
            assert!(Arc::ptr_eq(&ctx.scan_filters[&id], &original));
            drop(hold);
            options.cancellation.cancel();
            assert_eq!(
                install(&mut ctx, &node, vec![column(1)], keys)
                    .err()
                    .unwrap(),
                options.cancellation.check().unwrap_err()
            );
            assert!(Arc::ptr_eq(&ctx.scan_filters[&id], &original));
            outer.restore(&mut ctx);
        });
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn pair_components_can_target_different_inner_join_inputs() {
        let options = Options::default();
        let budget = Budget::new(1 << 20);
        let mut ctx = context(&options, &budget);
        let left = Box::new(scan());
        let right = Box::new(scan());
        let left_id = &*left as *const Node as usize;
        let right_id = &*right as *const Node as usize;
        let node = Node {
            schema: combined(&left.schema, &right.schema),
            estimated_rows: 32_000_000,
            op: RelOp::Join {
                left,
                right,
                kind: JoinKind::Inner,
                keys: vec![(column(0), column(0))],
                residual: None,
            },
        };
        with_keys(
            vec![vec![Some(1), Some(2)], vec![Some(7), Some(8)]],
            &budget,
            |keys| {
                let installed = install(&mut ctx, &node, vec![column(0), column(4)], keys)
                    .unwrap()
                    .unwrap();
                assert_eq!(ctx.scan_filters.len(), 2);
                assert_eq!(ctx.scan_filters[&left_id].expressions, vec![column(0)]);
                assert_eq!(ctx.scan_filters[&right_id].expressions, vec![column(1)]);
                installed.restore(&mut ctx);
                assert!(ctx.scan_filters.is_empty());
            },
        );
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }
}
