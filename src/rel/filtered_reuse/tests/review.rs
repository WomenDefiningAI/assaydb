//! Handwritten FR1 review controls. Initial attempts never go through retry.
use super::*;
use crate::expr::{Kind, Op};
use std::sync::atomic::AtomicBool;

fn union(left: Node, right: Node) -> Node {
    Node {
        schema: left.schema.clone(),
        estimated_rows: left.estimated_rows + right.estimated_rows,
        op: RelOp::UnionAll {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn typed_filter(
    column: usize,
    ty: Type,
    keys: &[i128],
    bitmap: bool,
    budget: &Budget,
) -> Arc<ScanKeyFilter> {
    let mut filter = filter(keys, budget);
    let value = Arc::get_mut(&mut filter).unwrap();
    value.expressions[0] = Expr {
        kind: Kind::Column(column),
        ty,
    };
    if !bitmap {
        value.numeric = None;
    }
    filter
}

#[test]
fn sparse_membership_cannot_pin_eight_mib_under_a_four_mib_cap() {
    let fixture = Fixture::new(false);
    let options = Options {
        memory_bytes: 128 << 20,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let root = union(fixture.scan(), fixture.scan());
    let RelOp::UnionAll { left, .. } = &root.op else {
        unreachable!()
    };
    let mut ctx = context(&options, &budget);
    ctx.filtered_reuse = Cache::new(&root, &budget);
    let capture = Capture::new(&ctx);
    let filter = filter(&[0, 67_108_863], &budget);
    assert_eq!(
        filter.numeric.as_ref().unwrap().reserved_bytes(),
        (8 << 20) + 128
    );
    let weak = Arc::downgrade(&filter);
    ctx.scan_filters
        .insert(left.as_ref() as *const Node as usize, filter);
    assert!(Pending::new(left, &ctx).is_none());
    collected(left, &mut ctx).unwrap();
    assert_eq!(ctx.metrics.filtered_scan_materializations, 0);
    ctx.scan_filters.clear(); // Drop the original runtime owner before measuring.
    assert!(weak.upgrade().is_none());
    let cache = ctx.filtered_reuse.as_ref().unwrap().lock().unwrap();
    assert!(cache.entries.is_empty());
    assert_eq!(budget.used(), cache.retained_bytes());
    assert!(budget.used() <= 4 << 20);
    drop(cache);
    drop(capture);
    drop(ctx);
    assert_eq!(budget.used(), 0);
}

#[test]
fn membership_counts_cumulatively_at_admission_and_publication() {
    let fixture = Fixture::new(false);
    let mut dimension = fixture.scan();
    let RelOp::Scan { path, .. } = &mut dimension.op else {
        unreachable!()
    };
    *path = fixture.dimension.clone();
    let root = union(
        union(fixture.scan(), fixture.scan()),
        union(dimension.clone(), dimension),
    );
    let mut leaves = Vec::new();
    scans(&root, &mut |node| leaves.push(node));
    for overlap in [false, true] {
        let options = Options {
            memory_bytes: 128 << 20,
            ..Options::default()
        };
        let budget = Budget::new(options.memory_bytes);
        let mut ctx = context(&options, &budget);
        ctx.filtered_reuse = Cache::new(&root, &budget);
        let capture = Capture::new(&ctx);
        // Each 2.5 MiB bitmap fits alone; both cannot fit the 4 MiB cap.
        for node in [leaves[0], leaves[2]] {
            ctx.scan_filters.insert(
                node as *const Node as usize,
                filter(&[0, (20 << 20) - 1], &budget),
            );
        }
        let first = Pending::new(leaves[0], &ctx).unwrap();
        let concurrent = overlap.then(|| Pending::new(leaves[2], &ctx).unwrap());
        first.finish(&mut ctx);
        if let Some(second) = concurrent {
            second.finish(&mut ctx);
        } else {
            assert!(Pending::new(leaves[2], &ctx).is_none());
        }
        assert_eq!(ctx.metrics.filtered_scan_materializations, 1);
        ctx.scan_filters.clear();
        let cache = ctx.filtered_reuse.as_ref().unwrap().lock().unwrap();
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(budget.used(), cache.retained_bytes());
        assert!(budget.used() <= 4 << 20);
        assert!(budget.used() > 2 << 20);
        drop(cache);
        drop(capture);
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn sparse_decimal_implication_replays_exact_mantissas_and_declines_widening() {
    let fixture = Fixture::new(false);
    let root = union(fixture.scan(), fixture.scan());
    let RelOp::UnionAll { left, right } = &root.op else {
        unreachable!()
    };
    for workers in [1, 2, 4, 10] {
        for (producer_bitmap, consumer_bitmap) in [(true, true), (true, false), (false, false)] {
            let options = Options {
                workers,
                batch_size: 31,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let mut ctx = context(&options, &budget);
            ctx.filtered_reuse = Cache::new(&root, &budget);
            let capture = Capture::new(&ctx);
            ctx.scan_filters.insert(
                left.as_ref() as *const Node as usize,
                typed_filter(
                    2,
                    Type::Decimal(2),
                    &[-10_000, 0, 200, 90_000],
                    producer_bitmap,
                    &budget,
                ),
            );
            collected(left, &mut ctx).unwrap();
            assert_eq!(ctx.metrics.filtered_scan_materializations, 1);
            // A different bitmap origin and decimal mantissa (200 means 2.00).
            let current = typed_filter(2, Type::Decimal(2), &[200], consumer_bitmap, &budget);
            ctx.scan_filters
                .insert(right.as_ref() as *const Node as usize, current.clone());
            let actual = collected(right, &mut ctx).unwrap();
            assert_eq!(ctx.metrics.filtered_scan_reuses, 1);
            let serial = Options {
                workers: 1,
                ..options.clone()
            };
            let mut ordinary = context(&serial, &budget);
            ordinary
                .scan_filters
                .insert(right.as_ref() as *const Node as usize, current);
            assert_eq!(actual, collected(right, &mut ordinary).unwrap());
            assert!(actual.num_rows() > 1);
            assert!(
                actual
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .all(|v| v == Some(200))
            );
            for (ty, keys) in [
                (Type::Decimal(2), vec![200, 300]),
                (Type::Decimal(1), vec![200]),
            ] {
                ctx.scan_filters.insert(
                    right.as_ref() as *const Node as usize,
                    typed_filter(2, ty, &keys, consumer_bitmap, &budget),
                );
                assert!(!replay(right, &mut ctx, &mut |_| panic!("incompatible replay")).unwrap());
            }
            drop(capture);
            drop(ctx);
            drop(ordinary);
            drop(actual);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn capture_fusion_and_post_decode_barriers_do_not_mix_replay_outputs() {
    let fixture = Fixture::new(false);
    let root = union(fixture.scan(), fixture.scan());
    let RelOp::UnionAll { left, right } = &root.op else {
        unreachable!()
    };
    let options = Options {
        workers: 4,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let mut ctx = context(&options, &budget);
    ctx.filtered_reuse = Cache::new(&root, &budget);
    for node in [left, right] {
        ctx.scan_filters.insert(
            node.as_ref() as *const Node as usize,
            filter(&[1, 2], &budget),
        );
    }
    assert!(lane_fusion::admitted(left, &ctx).unwrap());
    let capture = Capture::new(&ctx);
    assert!(!lane_fusion::admitted(left, &ctx).unwrap());
    collected(left, &mut ctx).unwrap();
    assert_eq!(ctx.metrics.filtered_scan_materializations, 1);
    let id = right.as_ref() as *const Node as usize;
    ctx.lane_transforms
        .insert(id, Arc::new(|_, _| panic!("admission must not run a lane")));
    assert!(Pending::new(right, &ctx).is_none());
    assert!(!replay(right, &mut ctx, &mut |_| panic!("transformed replay")).unwrap());
    ctx.lane_transforms.clear();
    let predicate = bind::bind(
        &bind::parse("SELECT k WHERE v>w").unwrap(),
        &right.schema,
        None,
        true,
    )
    .unwrap()
    .filter
    .unwrap();
    ctx.scan_transforms.insert(id, predicate.clone());
    assert!(!eligible(right, &ctx));
    assert!(!replay(right, &mut ctx, &mut |_| panic!("post-decode replay")).unwrap());
    ctx.scan_transforms.clear();
    ctx.static_filters.insert(id, predicate);
    assert!(!eligible(right, &ctx));
    assert!(!replay(right, &mut ctx, &mut |_| panic!("static-filter replay")).unwrap());
    ctx.static_filters.clear();
    // Gr2 prunes a cloned subtree; DS1's internal replay of that subtree must
    // not acquire entries keyed by the original plan's scan addresses.
    let cloned = right.clone();
    ctx.scan_filters.insert(
        cloned.as_ref() as *const Node as usize,
        filter(&[1, 2], &budget),
    );
    assert!(!eligible(&cloned, &ctx));
    assert!(!replay(&cloned, &mut ctx, &mut |_| panic!("cloned-plan replay")).unwrap());
    assert_eq!(ctx.metrics.filtered_scan_reuses, 0);
    drop(capture);
    assert!(lane_fusion::admitted(left, &ctx).unwrap());
    drop(ctx);
    assert_eq!(budget.used(), 0);
}

#[test]
fn all_paired_semi_anti_combinations_have_handwritten_first_attempt_results() {
    for empty in [false, true] {
        let fixture = Fixture::new(empty);
        for (a, b, expected) in [
            ("", "", if empty { vec![] } else { vec![Some(4)] }),
            (
                "",
                "NOT ",
                if empty {
                    vec![]
                } else {
                    vec![Some(1), Some(2), Some(2)]
                },
            ),
            ("NOT ", "", vec![]),
            (
                "NOT ",
                "NOT ",
                if empty {
                    vec![Some(1), Some(2), Some(2), Some(3), Some(4), None]
                } else {
                    vec![Some(3), None]
                },
            ),
        ] {
            let sql = format!(
                "SELECT d.k,d.s FROM d WHERE {a}EXISTS (SELECT * FROM f a WHERE a.k=d.k AND a.s<>d.s) AND {b}EXISTS (SELECT * FROM f b WHERE b.k=d.k AND b.s<>d.s AND b.v>b.w)"
            );
            let planning = Options {
                memory_bytes: 256 << 10,
                ..Options::default()
            };
            let mut prepared = execute::prepare(&sql, &fixture.tables(), &planning).unwrap();
            let root = prepared.relational.as_mut().unwrap();
            large_scans(root, &fixture.path);
            for batch_size in [1, 127] {
                let serial_options = Options {
                    batch_size,
                    ..Options::default()
                };
                let serial = crate::rel::execute_once_with_demand_cache(
                    root,
                    &serial_options,
                    Budget::new(serial_options.memory_bytes),
                    None,
                    false,
                    None,
                )
                .unwrap();
                for workers in [1, 2, 4, 10] {
                    let options = Options {
                        workers,
                        ..serial_options.clone()
                    };
                    let budget = Budget::new(options.memory_bytes);
                    let attempted = AtomicBool::new(false);
                    let actual = crate::rel::execute_once_with_demand_cache(
                        root,
                        &options,
                        budget.clone(),
                        None,
                        false,
                        Some(&attempted),
                    )
                    .unwrap();
                    assert_eq!(
                        actual.metrics.paired_existence_scans,
                        usize::from(!empty),
                        "{sql}: {}",
                        outline(root)
                    );
                    assert_eq!(actual.batch, serial.batch, "{sql}, workers={workers}");
                    assert_eq!(
                        actual
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .collect::<Vec<_>>(),
                        expected,
                        "{sql}"
                    );
                    drop(actual);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
    }
}

fn predicate_node(input: Node, predicate: &str) -> Node {
    let plan = bind::bind(
        &bind::parse(&format!("SELECT k WHERE {predicate}")).unwrap(),
        &input.schema,
        None,
        true,
    )
    .unwrap();
    Node {
        schema: input.schema.clone(),
        estimated_rows: input.estimated_rows,
        op: RelOp::Filter {
            input: Box::new(input),
            predicate: plan.filter.unwrap(),
        },
    }
}

fn projection_node(input: Node, sql: &str) -> Node {
    let plan = bind::bind(&bind::parse(sql).unwrap(), &input.schema, None, true).unwrap();
    Node {
        schema: schema(&plan.names, &plan.types),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            input: Box::new(input),
            plan,
        },
    }
}

fn assert_initial_and_public_error(root: &Node, expected: &str) {
    for batch_size in [1, 31, 257] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            for enabled in [false, true] {
                let budget = Budget::new(options.memory_bytes);
                let attempted = AtomicBool::new(false);
                let error = crate::rel::execute_once_with_demand_cache(
                    root,
                    &options,
                    budget.clone(),
                    None,
                    false,
                    enabled.then_some(&attempted),
                )
                .err()
                .unwrap();
                assert_eq!(
                    error, expected,
                    "initial: workers={workers}, batch={batch_size}, enabled={enabled}"
                );
                assert_eq!(attempted.load(Ordering::Relaxed), enabled);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
            assert_eq!(
                crate::rel::execute(root, &options).err().unwrap(),
                expected,
                "public: workers={workers}, batch={batch_size}"
            );
        }
    }
}

#[test]
fn discarded_rows_and_competing_operator_errors_obey_rule_e_on_initial_attempt() {
    let fixture = Fixture::new(false);
    // Row 130 lies beyond a Parquet row-group boundary. Its eager derived
    // projection must fail even though the later filter discards every row.
    let projected = projection_node(
        fixture.scan(),
        "SELECT k, (CASE WHEN k=130 THEN 10 ELSE 0 END)*10000000000000000000000000000000000000 AS s, v, w",
    );
    let discarded = predicate_node(projected, "k<0");
    let key = Expr {
        kind: Kind::Column(0),
        ty: Type::Int,
    };
    assert!(
        scan_target(&discarded, vec![key.clone()])
            .unwrap()
            .is_none()
    );
    // UNION drains its first branch, irrespective of the earlier physical row
    // bearing the distinguishable float error in the second branch.
    let floating = predicate_node(fixture.scan(), "CAST(k AS REAL)*CAST('1e308' AS REAL)>0");
    assert!(scan_target(&floating, vec![key]).unwrap().is_none());
    assert_initial_and_public_error(
        &union(discarded.clone(), floating.clone()),
        "decimal precision 38 overflow",
    );
    assert_initial_and_public_error(&union(floating, discarded), "floating arithmetic overflow");
}

fn paired_plan(fixture: &Fixture, first: Node, second: Node) -> Node {
    let mut left = fixture.scan();
    let RelOp::Scan { path, .. } = &mut left.op else {
        unreachable!()
    };
    *path = fixture.dimension.clone();
    left.estimated_rows = 6;
    let key = Expr {
        kind: Kind::Column(0),
        ty: Type::Int,
    };
    let residual = Expr {
        ty: Type::Bool,
        kind: Kind::Binary(
            Op::Ne,
            Box::new(Expr {
                kind: Kind::Column(1),
                ty: Type::Int,
            }),
            Box::new(Expr {
                kind: Kind::Column(5),
                ty: Type::Int,
            }),
        ),
    };
    let inner = Node {
        schema: left.schema.clone(),
        estimated_rows: 6,
        op: RelOp::Join {
            left: Box::new(left),
            right: Box::new(first),
            kind: JoinKind::Semi,
            keys: vec![(key.clone(), key.clone())],
            residual: Some(residual.clone()),
        },
    };
    Node {
        schema: inner.schema.clone(),
        estimated_rows: 6,
        op: RelOp::Join {
            left: Box::new(inner),
            right: Box::new(second),
            kind: JoinKind::Anti,
            keys: vec![(key.clone(), key)],
            residual: Some(residual),
        },
    }
}

#[test]
fn unsafe_predicate_or_projection_on_either_paired_rhs_declines_before_execution() {
    let fixture = Fixture::new(false);
    for rhs in [0, 1] {
        for projection in [false, true] {
            let unsafe_rhs = if projection {
                projection_node(
                    fixture.scan(),
                    "SELECT k,s,v*10000000000000000000000000000000000000 AS v,w",
                )
            } else {
                predicate_node(fixture.scan(), "v*10000000000000000000000000000000000000>0")
            };
            assert!(!membership_safety::input_is_total(&unsafe_rhs));
            let other = predicate_node(fixture.scan(), "v>w");
            let root = if rhs == 0 {
                paired_plan(&fixture, unsafe_rhs, other)
            } else {
                paired_plan(&fixture, other, unsafe_rhs)
            };
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let attempted = AtomicBool::new(false);
                let mut ctx = context(&options, &budget);
                ctx.demand_cache = Some(&attempted);
                ctx.filtered_reuse = Cache::new(&root, &budget);
                assert!(
                    !paired_existence::try_execute(&root, &mut ctx, &mut |_| panic!(
                        "unsafe paired RHS must decline"
                    ))
                    .unwrap()
                );
                assert_eq!(ctx.metrics.rows_read, 0);
                assert_eq!(ctx.metrics.paired_existence_scans, 0);
                assert!(!attempted.load(Ordering::Relaxed));
                drop(ctx);
                assert_eq!(budget.used(), 0);
            }
            assert_initial_and_public_error(&root, "numeric overflow");
        }
    }
}

// Reproduce the quarter-selectivity tie without changing SQL values, physical
// scan cardinalities (used by totality proofs), or the retained plan's order.
fn paired_estimates(root: &mut Node, rhs_rows: usize) {
    let RelOp::Join {
        left: inner, right, ..
    } = &mut root.op
    else {
        panic!("expected outer existence join")
    };
    right.estimated_rows = rhs_rows;
    inner.estimated_rows = 3_750_000;
    let RelOp::Join { left, .. } = &mut inner.op else {
        panic!("expected inner existence join")
    };
    // Put the guess on a total filter, never on the exact physical scan.
    **left = predicate_node((**left).clone(), "k IS NULL OR k IS NOT NULL");
    left.estimated_rows = 3_750_000;
}

#[test]
fn paired_actual_cardinality_overrides_estimate_ties_in_retained_order() {
    for empty in [false, true] {
        let fixture = Fixture::new(empty);
        for (first, second, expected) in [
            (
                JoinKind::Semi,
                JoinKind::Semi,
                if empty { vec![] } else { vec![Some(4)] },
            ),
            (
                JoinKind::Semi,
                JoinKind::Anti,
                if empty {
                    vec![]
                } else {
                    vec![Some(1), Some(2), Some(2)]
                },
            ),
            (JoinKind::Anti, JoinKind::Semi, vec![]),
            (
                JoinKind::Anti,
                JoinKind::Anti,
                if empty {
                    vec![Some(1), Some(2), Some(2), Some(3), Some(4), None]
                } else {
                    vec![Some(3), None]
                },
            ),
        ] {
            let mut root = paired_plan(
                &fixture,
                predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
                predicate_node(fixture.scan(), "v>w"),
            );
            let RelOp::Join {
                kind, left: inner, ..
            } = &mut root.op
            else {
                unreachable!()
            };
            *kind = second;
            let RelOp::Join { kind, .. } = &mut inner.op else {
                unreachable!()
            };
            *kind = first;
            paired_estimates(&mut root, 14_996_513);
            let serial_options = Options {
                relational_optimizations: false,
                batch_size: 1,
                ..Options::default()
            };
            let expected_batch = crate::rel::execute(&root, &serial_options).unwrap().batch;
            assert_eq!(
                expected_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
            for batch_size in [1, 7, 8192] {
                for workers in [1, 2, 4, 10] {
                    let options = Options {
                        batch_size,
                        workers,
                        ..Options::default()
                    };
                    let budget = Budget::new(options.memory_bytes);
                    let attempted = AtomicBool::new(false);
                    // Inspect the first attempt: retry cannot hide a bad path.
                    let actual = crate::rel::execute_once_with_demand_cache(
                        &root,
                        &options,
                        budget.clone(),
                        None,
                        false,
                        Some(&attempted),
                    )
                    .unwrap();
                    assert_eq!(
                        actual.batch, expected_batch,
                        "{first:?}/{second:?}, empty={empty}, workers={workers}, batch={batch_size}"
                    );
                    assert_eq!(actual.metrics.paired_existence_scans, 1);
                    assert_eq!(actual.metrics.reverse_existence_joins, 2);
                    assert_eq!(actual.metrics.join_build_rows, 6);
                    assert_eq!(actual.metrics.rows_read, if empty { 6 } else { 4105 });
                    assert!(attempted.load(Ordering::Relaxed));
                    drop(actual);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
    }
}

#[test]
fn paired_actual_cost_miss_reuses_the_completed_left_once() {
    let fixture = Fixture::new(false);
    let mut root = paired_plan(
        &fixture,
        predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
        predicate_node(fixture.scan(), "v>w"),
    );
    // Six actual retained rows fail the unchanged strict fourfold threshold.
    paired_estimates(&mut root, 24);
    let serial = Options {
        relational_optimizations: false,
        batch_size: 1,
        ..Options::default()
    };
    let expected = crate::rel::execute(&root, &serial).unwrap().batch;
    assert_eq!(
        expected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(2)]
    );
    for batch_size in [1, 7, 8192] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                batch_size,
                workers,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let attempted = AtomicBool::new(false);
            let actual = crate::rel::execute_once_with_demand_cache(
                &root,
                &options,
                budget.clone(),
                None,
                false,
                Some(&attempted),
            )
            .unwrap();
            assert_eq!(actual.batch, expected);
            assert_eq!(actual.metrics.paired_existence_scans, 0);
            assert_eq!(actual.metrics.reverse_existence_joins, 0);
            // Exactly one dimension traversal plus the two ordinary RHS scans.
            assert_eq!(actual.metrics.rows_read, 8204);
            assert!(attempted.load(Ordering::Relaxed));
            drop(actual);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn paired_cost_fallback_funds_wide_gathers_before_allocation() {
    use crate::join_filter::{gather_bytes, tests::GATHER_HOOK};
    use std::{cell::RefCell, rc::Rc};

    let fixture = Fixture::new(false);
    let schema = Arc::new(Schema::new(vec![
        fixture.schema.field(0).clone(),
        fixture.schema.field(1).clone(),
        fixture.schema.field(2).clone(),
        Field::new("w", arrow::datatypes::DataType::Utf8, true),
    ]));
    let text = "é雪".repeat(32768);
    let dimension = RecordBatch::try_new(
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
            Arc::new(StringArray::from(vec![
                Some(text.as_str()),
                Some(text.as_str()),
                Some(text.as_str()),
                Some(text.as_str()),
                Some(text.as_str()),
                None,
            ])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&fixture.dimension).unwrap(),
        schema.clone(),
        None,
    )
    .unwrap();
    writer.write(&dimension).unwrap();
    writer.close().unwrap();
    let mut root = paired_plan(
        &fixture,
        predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
        predicate_node(fixture.scan(), "v>w"),
    );
    root.schema = schema.clone();
    let RelOp::Join { left: inner, .. } = &mut root.op else {
        unreachable!()
    };
    inner.schema = schema.clone();
    let RelOp::Join { left, .. } = &mut inner.op else {
        unreachable!()
    };
    left.schema = schema;
    paired_estimates(&mut root, 24);
    let serial = Options {
        relational_optimizations: false,
        batch_size: 1,
        ..Options::default()
    };
    let expected = crate::rel::execute(&root, &serial).unwrap().batch;
    assert_eq!(
        expected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(2)]
    );

    #[derive(Default)]
    struct Observation {
        before: usize,
        allocated: usize,
        pressure: Option<Reservation>,
        live: usize,
        requested: usize,
    }
    struct HookGuard;
    impl Drop for HookGuard {
        fn drop(&mut self) {
            GATHER_HOOK.with_borrow_mut(|hook| *hook = None);
        }
    }
    for workers in [1, 2, 4, 10] {
        for mode in ["deny", "fit", "cancel"] {
            let options = Options {
                workers,
                batch_size: 2,
                memory_bytes: 64 << 20,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let state = Rc::new(RefCell::new(Observation::default()));
            let observed = state.clone();
            let cancellation = options.cancellation.clone();
            GATHER_HOOK.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move |array, indices, budget, allocated| {
                    if !array.as_any().is::<StringArray>() {
                        return;
                    }
                    let mut state = observed.borrow_mut();
                    if allocated {
                        state.allocated += 1;
                        if state.before == 2 {
                            assert_eq!(budget.used(), state.live + state.requested);
                            state.pressure = None;
                            if mode == "cancel" {
                                cancellation.cancel();
                            }
                        }
                    } else {
                        state.before += 1;
                        if state.before == 2 {
                            // Earlier output, retained probes and the RHS remain live.
                            // Leave exactly enough for this gather, or one byte less.
                            let requested = gather_bytes(array, indices).unwrap();
                            assert!(requested > 100_000);
                            let headroom = requested - usize::from(mode == "deny");
                            state.pressure = Some(
                                budget
                                    .reserve(budget.limit() - budget.used() - headroom)
                                    .unwrap(),
                            );
                            state.live = budget.used();
                            state.requested = requested;
                        }
                    }
                }))
            });
            let guard = HookGuard;
            let attempted = AtomicBool::new(false);
            let mut ctx = context(&options, &budget);
            ctx.demand_cache = Some(&attempted);
            let mut batches = Vec::new();
            let result = paired_existence::try_execute(&root, &mut ctx, &mut |batch| {
                batches.push(batch);
                Ok(())
            });
            drop(guard);
            assert!(attempted.load(Ordering::Relaxed));
            assert_eq!(ctx.metrics.paired_existence_scans, 0);
            let mut state = state.borrow_mut();
            if mode == "deny" {
                assert_eq!(state.before, 2);
                assert_eq!(state.allocated, 1, "denied gather must never allocate");
                assert_eq!(
                    result.unwrap_err(),
                    format!(
                        "resource-limit: accounted memory {} exceeds budget {}; requested {}, live {}",
                        state.live + state.requested,
                        budget.limit(),
                        state.requested,
                        state.live
                    )
                );
                assert!(batches.is_empty());
            } else if mode == "cancel" {
                assert_eq!(result.unwrap_err(), "cancelled: query execution");
                assert!(batches.is_empty());
            } else {
                assert!(result.unwrap());
                assert!(state.allocated >= 3, "both fallback joins must fund output");
                assert_eq!(
                    arrow::compute::concat_batches(&root.schema, &batches).unwrap(),
                    expected
                );
            }
            drop(batches);
            drop(ctx);
            state.pressure = None;
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn paired_actual_admission_preserves_empty_left_mixed_keys_and_errors() {
    let fixture = Fixture::new(false);
    for case in ["empty", "mixed", "skew", "error"] {
        let mut root = paired_plan(
            &fixture,
            predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
            predicate_node(fixture.scan(), "v>w"),
        );
        paired_estimates(&mut root, 14_996_513);
        let RelOp::Join {
            left: inner,
            keys: outer_keys,
            right,
            ..
        } = &mut root.op
        else {
            unreachable!()
        };
        let RelOp::Join { left, keys, .. } = &mut inner.op else {
            unreachable!()
        };
        match case {
            "empty" => **left = predicate_node((**left).clone(), "k<0"),
            "mixed" => {
                keys[0].0 = Expr {
                    kind: Kind::Column(2),
                    ty: Type::Decimal(2),
                };
                keys[0].1.ty = Type::Decimal(2);
                outer_keys[0] = keys[0].clone();
            }
            "skew" => {
                // Existing duplicate retained occurrences meet thousands of
                // candidate RHS rows concentrated into only three keys.
                keys[0] = (
                    Expr {
                        kind: Kind::Column(1),
                        ty: Type::Int,
                    },
                    Expr {
                        kind: Kind::Column(1),
                        ty: Type::Int,
                    },
                );
                outer_keys[0] = keys[0].clone();
            }
            "error" => {
                **right =
                    predicate_node((**right).clone(), "CAST(k AS REAL)*CAST('1e308' AS REAL)>0")
            }
            _ => unreachable!(),
        }
        let serial = Options {
            relational_optimizations: false,
            batch_size: 1,
            ..Options::default()
        };
        let expected = crate::rel::execute(&root, &serial).map(|e| e.batch);
        if case == "error" {
            assert_eq!(
                expected.as_ref().unwrap_err(),
                "floating arithmetic overflow"
            );
        } else if case == "empty" || case == "skew" {
            assert_eq!(expected.as_ref().unwrap().num_rows(), 0);
        }
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    batch_size,
                    workers,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let attempted = AtomicBool::new(false);
                if case == "error" {
                    let mut ctx = context(&options, &budget);
                    ctx.demand_cache = Some(&attempted);
                    assert!(
                        !paired_existence::try_execute(&root, &mut ctx, &mut |_| panic!(
                            "unsafe pair emitted"
                        ))
                        .unwrap()
                    );
                    assert!(!attempted.load(Ordering::Relaxed));
                    assert_eq!(ctx.metrics.rows_read, 0);
                    drop(ctx);
                    assert_eq!(budget.used(), 0);
                }
                let actual = crate::rel::execute_once_with_demand_cache(
                    &root,
                    &options,
                    budget.clone(),
                    None,
                    false,
                    Some(&attempted),
                );
                if case == "error" {
                    // Whole-query execution also uses this flag for its
                    // independent filtered-scan cache, not just pairing.
                    assert!(attempted.load(Ordering::Relaxed));
                } else {
                    assert_eq!(actual.as_ref().unwrap().metrics.paired_existence_scans, 1);
                }
                assert_eq!(
                    actual.map(|e| e.batch),
                    expected,
                    "{case}, workers={workers}, batch={batch_size}"
                );
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn paired_actual_admission_denial_and_cancellation_release_retention() {
    let fixture = Fixture::new(false);
    for rhs_rows in [24, 14_996_513] {
        let mut root = paired_plan(
            &fixture,
            predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
            predicate_node(fixture.scan(), "v>w"),
        );
        paired_estimates(&mut root, rhs_rows);
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 1,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let attempted = AtomicBool::new(false);
            let mut ctx = context(&options, &budget);
            ctx.demand_cache = Some(&attempted);
            let mut emitted = 0;
            let error = root
                .run(&mut ctx, &mut |_| {
                    emitted += 1;
                    options.cancellation.cancel();
                    Ok(())
                })
                .unwrap_err();
            assert_eq!(error, "cancelled: query execution");
            assert_eq!(emitted, 1);
            assert!(attempted.load(Ordering::Relaxed));
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);

            // Deterministic descriptor denial precedes all input work.
            let denial = Budget::new((128 << 10) - 1);
            let fresh = Options {
                workers,
                ..Options::default()
            };
            let mut ctx = context(&fresh, &denial);
            let attempted = AtomicBool::new(false);
            ctx.demand_cache = Some(&attempted);
            assert!(
                !paired_existence::try_execute(&root, &mut ctx, &mut |_| panic!(
                    "denied pair emitted"
                ))
                .unwrap()
            );
            assert!(!attempted.load(Ordering::Relaxed));
            assert_eq!(ctx.metrics.rows_read, 0);
            drop(ctx);
            assert_eq!(denial.used(), 0);
            assert_eq!(denial.active_workers(), 0);

            for memory_bytes in [4 << 20, 8 << 20] {
                let options = Options {
                    workers,
                    memory_bytes,
                    batch_size: 7,
                    ..Options::default()
                };
                let budget = Budget::new(memory_bytes);
                let expected = crate::rel::execute_once_with_demand_cache(
                    &root,
                    &options,
                    budget.clone(),
                    None,
                    false,
                    None,
                )
                .map(|e| e.batch);
                assert_eq!(budget.used(), 0);
                let actual = crate::rel::execute_once(&root, &options, budget.clone(), None, false)
                    .map(|e| e.batch);
                assert_eq!(
                    actual, expected,
                    "rhs={rhs_rows}, workers={workers}, budget={memory_bytes}"
                );
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn paired_actual_admission_fits_where_ordinary_rhs_builds_exceed_budget() {
    let fixture = Fixture::new(false);
    let mut root = paired_plan(
        &fixture,
        predicate_node(fixture.scan(), "k IS NULL OR k IS NOT NULL"),
        predicate_node(fixture.scan(), "v>w"),
    );
    paired_estimates(&mut root, 14_996_513);
    let reference_options = Options {
        batch_size: 7,
        ..Options::default()
    };
    let reference = crate::rel::execute_once_with_demand_cache(
        &root,
        &reference_options,
        Budget::new(reference_options.memory_bytes),
        None,
        false,
        None,
    )
    .unwrap();
    let options = Options {
        memory_bytes: 1 << 20,
        ..reference_options
    };
    let budget = Budget::new(options.memory_bytes);
    let error = crate::rel::execute_once_with_demand_cache(
        &root,
        &options,
        budget.clone(),
        None,
        false,
        None,
    )
    .err()
    .unwrap();
    assert!(
        error.starts_with("resource-limit: accounted memory"),
        "{error}"
    );
    assert_eq!(budget.used(), 0);
    let paired_budget = Budget::new(options.memory_bytes);
    let attempted = AtomicBool::new(false);
    let actual = crate::rel::execute_once_with_demand_cache(
        &root,
        &options,
        paired_budget.clone(),
        None,
        false,
        Some(&attempted),
    )
    .unwrap();
    assert_eq!(actual.batch, reference.batch);
    assert_eq!(actual.metrics.paired_existence_scans, 1);
    assert_eq!(actual.metrics.join_build_rows, 6);
    assert!(actual.metrics.peak_reserved_bytes < reference.metrics.peak_reserved_bytes);
    assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
    drop(actual);
    assert_eq!(paired_budget.used(), 0);
    assert_eq!(paired_budget.active_workers(), 0);
}
