use super::super::tests::{context, request, source};
use super::*;

struct ConsumerFixture {
    directory: std::path::PathBuf,
    tables: HashMap<String, std::path::PathBuf>,
}

impl ConsumerFixture {
    fn new() -> Self {
        use parquet::arrow::ArrowWriter;
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "assay-late-payload-consumer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut tables = HashMap::new();
        for (name, names, arrays) in [
            (
                "b",
                vec!["k", "g", "balance", "nation", "address", "phone", "comment"],
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
                    Arc::new(
                        Decimal128Array::from(vec![12345])
                            .with_precision_and_scale(15, 2)
                            .unwrap(),
                    ) as ArrayRef,
                    Arc::new(StringArray::from(vec!["nation"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["address"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["phone"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["comment"])) as ArrayRef,
                ],
            ),
            (
                "p",
                vec!["k", "v"],
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(
                        Decimal128Array::from(vec![9876])
                            .with_precision_and_scale(15, 2)
                            .unwrap(),
                    ) as ArrayRef,
                ],
            ),
        ] {
            let schema = Arc::new(Schema::new(
                names
                    .iter()
                    .zip(&arrays)
                    .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
            let path = directory.join(format!("{name}.parquet"));
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            tables.insert(name.into(), path);
        }
        Self { directory, tables }
    }
}

impl Drop for ConsumerFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

const CONSUMER_SQL: &str =
    "SELECT b.g, COUNT(*) n FROM p JOIN b ON p.k=b.k GROUP BY b.g ORDER BY n LIMIT 1";

#[test]
fn releases_dictionary_and_plan_credits_before_downstream_reservation() {
    let fixture = ConsumerFixture::new();
    for workers in [1, 2, 4, 10] {
        let mut reference_entry_bytes = None;
        for optimized in [false, true] {
            let options = Options {
                workers,
                batch_size: 1,
                memory_bytes: 8 << 20,
                spill: None,
                relational_optimizations: optimized,
                ..Options::default()
            };
            let prepared = crate::rel::prepare(CONSUMER_SQL, &fixture.tables, &options).unwrap();
            let budget = Budget::new(options.memory_bytes);
            let mut ctx = context(&options, &budget);
            let mut emitted = 0;
            prepared
                .relational
                .as_ref()
                .unwrap()
                .run(&mut ctx, &mut |batch| {
                    emitted += 1;
                    if let Some(reference) = reference_entry_bytes {
                        assert!(budget.used() <= reference);
                    } else {
                        reference_entry_bytes = Some(budget.used());
                    }
                    let _consumer = budget.reserve((8 << 20) - (64 << 10))?;
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "x"
                    );
                    assert_eq!(crate::expr::numeric(batch.column(1)).value(0), 1);
                    Ok(())
                })
                .unwrap();
            assert_eq!(emitted, 1);
            assert_eq!(ctx.metrics.late_payload_sorts, usize::from(optimized));
            assert!(ctx.materialized.is_empty());
            assert!(ctx.build_groups.is_none());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn shared_schema_preserves_consumer_headroom_for_long_aliases() {
    let fixture = ConsumerFixture::new();
    for alias in ["g".to_string(), "a".repeat(60 << 10), "a".repeat(65_536)] {
        // SQL text is capped at 64 KiB. Exercise a legal long quoted alias
        // end to end, and install the reviewer's 65,536-byte alias directly
        // on the bound ORDER BY output to test the same consumer boundary.
        let sql_alias = if alias.len() == 65_536 { "g" } else { &alias };
        let sql = format!(
            "SELECT b.g AS \"{sql_alias}\", COUNT(*) n FROM p JOIN b ON p.k=b.k GROUP BY b.g ORDER BY n LIMIT 1"
        );
        let mut serial_batch = None;
        for workers in [1, 2, 4, 10] {
            let mut reference_entry_bytes = None;
            for optimized in [false, true] {
                let options = Options {
                    workers,
                    batch_size: 1,
                    memory_bytes: 8 << 20,
                    spill: None,
                    relational_optimizations: optimized,
                    ..Options::default()
                };
                let mut prepared = crate::rel::prepare(&sql, &fixture.tables, &options).unwrap();
                if alias.len() == 65_536 {
                    let root = prepared.relational.as_mut().unwrap();
                    let RelOp::Limit { input, .. } = &mut root.op else {
                        panic!("expected LIMIT");
                    };
                    let RelOp::Unary { plan, .. } = &mut input.op else {
                        panic!("expected ORDER BY");
                    };
                    plan.names[0] = alias.clone();
                    input.schema = schema(&plan.names, &plan.types);
                    root.schema = input.schema.clone();
                }
                let budget = Budget::new(options.memory_bytes);
                let mut ctx = context(&options, &budget);
                let root = prepared.relational.as_ref().unwrap();
                let RelOp::Limit { count, input, .. } = &root.op else {
                    panic!("expected LIMIT");
                };
                let order_schema = &input.schema;
                if optimized && alias.len() > 1024 {
                    // A computed projection still declines before consumption,
                    // regardless of alias length; keep the zero-credit control.
                    let RelOp::Unary { plan, input: child } = &input.op else {
                        panic!("expected ORDER BY");
                    };
                    let mut computed = plan.clone();
                    computed.output[0] = bind::Output::Expr(Expr {
                        kind: Kind::Literal(Literal::Text("x".into())),
                        ty: Type::Text,
                    });
                    assert!(
                        !try_ordered_limit(
                            &computed,
                            order_schema,
                            child,
                            *count,
                            &mut ctx,
                            &mut |_| panic!("declining must not emit"),
                        )
                        .unwrap()
                    );
                    assert_eq!(budget.peak(), 0);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                    assert!(ctx.materialized.is_empty());
                    assert!(ctx.build_groups.is_none());
                }
                let mut emitted = 0;
                root.run(&mut ctx, &mut |batch| {
                    emitted += 1;
                    if optimized {
                        assert!(Arc::ptr_eq(&batch.schema(), order_schema));
                    }
                    if let Some(reference) = reference_entry_bytes {
                        assert!(budget.used() <= reference);
                    } else {
                        reference_entry_bytes = Some(budget.used());
                    }
                    let _consumer = budget.reserve((8 << 20) - (64 << 10))?;
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(batch.schema().field(0).name(), &alias);
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "x"
                    );
                    assert_eq!(crate::expr::numeric(batch.column(1)).value(0), 1);
                    if let Some(expected) = &serial_batch {
                        assert_eq!(&batch, expected);
                    } else {
                        serial_batch = Some(batch);
                    }
                    Ok(())
                })
                .unwrap();
                assert_eq!(emitted, 1);
                assert_eq!(ctx.metrics.late_payload_sorts, usize::from(optimized));
                assert!(ctx.materialized.is_empty());
                assert!(ctx.build_groups.is_none());
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn eight_column_grouped_sum_shares_order_schema_and_preserves_consumer_headroom() {
    let fixture = ConsumerFixture::new();
    let sql = "SELECT b.k,b.g,SUM(p.v) revenue,b.balance,b.nation,b.address,b.phone,b.comment \
               FROM p JOIN b ON p.k=b.k \
               GROUP BY b.k,b.g,b.balance,b.nation,b.address,b.phone,b.comment \
               ORDER BY revenue DESC LIMIT 1";
    let mut serial_batch = None;
    for workers in [1, 2, 4, 10] {
        let mut reference_entry_bytes = None;
        for optimized in [false, true] {
            let options = Options {
                workers,
                batch_size: 1,
                memory_bytes: 8 << 20,
                spill: None,
                relational_optimizations: optimized,
                ..Options::default()
            };
            let prepared = crate::rel::prepare(sql, &fixture.tables, &options).unwrap();
            let root = prepared.relational.as_ref().unwrap();
            let RelOp::Limit { input, .. } = &root.op else {
                panic!("expected LIMIT");
            };
            let order_schema = &input.schema;
            let budget = Budget::new(options.memory_bytes);
            let mut ctx = context(&options, &budget);
            let mut emitted = 0;
            root.run(&mut ctx, &mut |batch| {
                emitted += 1;
                if let Some(reference) = reference_entry_bytes {
                    assert!(budget.used() <= reference);
                } else {
                    reference_entry_bytes = Some(budget.used());
                }
                if optimized {
                    assert!(Arc::ptr_eq(&batch.schema(), order_schema));
                    assert_eq!(budget.used(), batch.get_array_memory_size() + 1024);
                }
                let _consumer = budget.reserve((8 << 20) - (64 << 10))?;
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 8);
                assert_eq!(crate::expr::numeric(batch.column(2)).value(0), 9876);
                if let Some(expected) = &serial_batch {
                    assert_eq!(&batch, expected);
                } else {
                    serial_batch = Some(batch);
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(emitted, 1);
            assert_eq!(ctx.metrics.late_payload_sorts, usize::from(optimized));
            assert!(ctx.materialized.is_empty());
            assert!(ctx.build_groups.is_none());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn downstream_failure_or_cancellation_never_replays_and_releases_selected_result() {
    let fixture = ConsumerFixture::new();
    for workers in [1, 2, 4, 10] {
        for cancel in [false, true] {
            let options = Options {
                workers,
                batch_size: 1,
                memory_bytes: 8 << 20,
                spill: None,
                ..Options::default()
            };
            let prepared = crate::rel::prepare(CONSUMER_SQL, &fixture.tables, &options).unwrap();
            let budget = Budget::new(options.memory_bytes);
            let mut ctx = context(&options, &budget);
            let mut emitted = 0;
            let error = prepared
                .relational
                .as_ref()
                .unwrap()
                .run(&mut ctx, &mut |batch| {
                    // Deliberately mutate consumer state before failing.
                    emitted += batch.num_rows();
                    let _consumer = budget.reserve((8 << 20) - (64 << 10))?;
                    if cancel {
                        options.cancellation.cancel();
                        options.cancellation.check()
                    } else {
                        budget.reserve(64 << 10).map(|_| ())
                    }
                })
                .unwrap_err();
            if cancel {
                assert_eq!(error, "cancelled: query execution");
            } else {
                assert!(error.starts_with("resource-limit: accounted memory"));
            }
            assert_eq!(emitted, 1);
            assert_eq!(ctx.metrics.late_payload_sorts, 1);
            assert_eq!(ctx.metrics.build_group_id_fallbacks, 0);
            assert!(ctx.materialized.is_empty());
            assert!(ctx.build_groups.is_none());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn selected_gathers_are_funded_and_failures_release_sort_and_payload_buffers() {
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for failure in ["none", "cancel after input", "deny sort", "cancel output"] {
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
                    let _plan_credit = budget.reserve(128 << 10).unwrap();
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
                            output: vec![
                                bind::Output::Group(0),
                                bind::Output::Group(1),
                                bind::Output::Aggregate(0),
                            ],
                            names: vec!["g".into(), "k".into(), "score".into()],
                            types: vec![Type::Text, Type::Int, Type::Int],
                            order: vec![],
                            aggregation: true,
                            projection_pushdown: true,
                        },
                    };
                    let mut order = prepared.plan.clone();
                    order.aggregation = false;
                    order.groups.clear();
                    order.output = order
                        .types
                        .iter()
                        .enumerate()
                        .map(|(i, ty)| {
                            bind::Output::Expr(Expr {
                                kind: Kind::Column(i),
                                ty: ty.clone(),
                            })
                        })
                        .collect();
                    order.order = vec![bind::Sort {
                        column: 2,
                        descending: true,
                        nulls_first: false,
                    }];
                    let output_schema = schema(&order.names, &order.types);
                    let selection = Selection {
                        order: &order,
                        output_schema: &output_schema,
                        count: 1,
                    };
                    let _input_credit = budget.reserve(4096).unwrap();
                    let ids: ArrayRef = Arc::new(
                        Decimal128Array::from(vec![0, 1, 2])
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    );
                    let values: ArrayRef = Arc::new(
                        Decimal128Array::from(vec![30, 10, 20])
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    );
                    let batch = RecordBatch::try_new(
                        schema(&prepared.plan.names, &[Type::Int, Type::Int, Type::Int]),
                        vec![ids.clone(), ids, values],
                    )
                    .unwrap();
                    let baseline = budget.used();
                    let text_gathers = std::rc::Rc::new(std::cell::Cell::new(0));
                    let observed = text_gathers.clone();
                    crate::join_filter::tests::GATHER_HOOK.with(|hook| {
                        *hook.borrow_mut() =
                            Some(Box::new(move |array, indices, budget, after| {
                                if array.data_type() == &DataType::Utf8 {
                                    assert_eq!(indices.values().as_ref(), &[0]);
                                    if after {
                                        observed.set(observed.get() + 1);
                                        assert!(budget.used() > baseline);
                                    }
                                }
                            }));
                    });
                    let mut pressure = None;
                    let completed = std::cell::Cell::new(0);
                    let mut emitted = 0;
                    let result = selection
                        .execute(&request, (&prepared, 0), &mut ctx, &mut |_, send| {
                            for start in (0..3).step_by(batch_size) {
                                let rows = batch_size.min(3 - start);
                                completed.set(completed.get() + rows);
                                send(batch.slice(start, rows))?;
                            }
                            if failure == "cancel after input" {
                                options.cancellation.cancel();
                            } else if failure == "deny sort" {
                                pressure =
                                    Some(budget.reserve((8 << 20) - budget.used() - 512).unwrap());
                            }
                            Ok(Metrics::default())
                        })
                        .and_then(|(metrics, selected)| {
                            if let Some(selected) = selected {
                                selected.emit(&mut |output| {
                                    emitted += 1;
                                    assert_eq!(completed.get(), 3);
                                    assert_eq!(output.num_rows(), 1);
                                    assert_eq!(
                                        output
                                            .column(0)
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .unwrap()
                                            .value(0),
                                        "b"
                                    );
                                    assert_eq!(crate::expr::numeric(output.column(1)).value(0), 2);
                                    assert_eq!(crate::expr::numeric(output.column(2)).value(0), 30);
                                    assert!(
                                        budget.used() >= baseline + output.get_array_memory_size()
                                    );
                                    if failure == "cancel output" {
                                        options.cancellation.cancel();
                                    }
                                    options.cancellation.check()
                                })?;
                            }
                            Ok(metrics)
                        });
                    crate::join_filter::tests::GATHER_HOOK.with(|hook| *hook.borrow_mut() = None);
                    match failure {
                        "none" => assert_eq!(result.unwrap().late_payload_rows, 1),
                        "deny sort" => assert!(
                            result
                                .err()
                                .unwrap()
                                .starts_with("resource-limit: accounted memory")
                        ),
                        _ => assert_eq!(result.err().unwrap(), "cancelled: query execution"),
                    }
                    assert_eq!(completed.get(), 3);
                    let selected = usize::from(matches!(failure, "none" | "cancel output"));
                    assert_eq!(emitted, selected);
                    assert_eq!(text_gathers.get(), selected);
                    drop(pressure);
                    assert_eq!(budget.used(), baseline);
                    assert_eq!(budget.active_workers(), 0);
                }
                assert_eq!(budget.used(), 0);
            }
        }
    }
}
