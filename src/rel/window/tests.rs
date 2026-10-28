use super::*;

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
        spill: options.spill.as_ref().map(|settings| {
            crate::spill::SpillStore::new(
                &settings.directory,
                settings.disk_quota_bytes,
                budget.clone(),
                options.cancellation.clone(),
            )
            .unwrap()
        }),
        budget: budget.clone(),
        metrics: Metrics::default(),
    }
}

#[test]
fn window_cancellation_and_errors_release_every_reservation() {
    let query = "SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS r FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id UNION ALL SELECT 3 AS id UNION ALL SELECT 4 AS id) t";
    for spill in [false, true] {
        let directory = std::env::temp_dir().join(format!(
            "assay-window-cancel-{}-{spill}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let options = Options {
            batch_size: 1,
            spill: spill.then(|| crate::spill::SpillOptions {
                directory: directory.clone(),
                sort_run_bytes: 128,
                ..Default::default()
            }),
            ..Options::default()
        };
        let prepared = crate::rel::prepare(query, &HashMap::new(), &options).unwrap();
        let root = prepared.relational.unwrap();
        let budget = Budget::new(8 << 20);
        let mut ctx = context(&options, &budget);
        let mut emitted = 0;
        let error = root
            .run(&mut ctx, &mut |batch| {
                assert!(budget.used() >= batch.get_array_memory_size());
                emitted += batch.num_rows();
                options.cancellation.cancel();
                Ok(())
            })
            .unwrap_err();
        assert!(error.starts_with("cancelled:"), "{error}");
        assert_eq!(emitted, 1);
        drop(ctx);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(directory).unwrap();
    }
    for memory in [1, 1 << 20] {
        let options = Options::default();
        let prepared = crate::rel::prepare(
            "SELECT RANK() OVER (ORDER BY 1.0/0)",
            &HashMap::new(),
            &options,
        )
        .unwrap();
        let budget = Budget::new(memory);
        let outcome = crate::rel::execute_in_budget(
            prepared.relational.as_ref().unwrap(),
            &options,
            budget.clone(),
            false,
        );
        assert!(outcome.is_err());
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn canonical_input_sort_preserves_schema_and_releases_resources() {
    for spill in [false, true] {
        let directory = std::env::temp_dir().join(format!(
            "assay-window-input-cancel-{}-{spill}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let options = Options {
            batch_size: 1,
            spill: spill.then(|| crate::spill::SpillOptions {
                directory: directory.clone(),
                sort_run_bytes: 128,
                ..Default::default()
            }),
            ..Default::default()
        };
        let prepared = crate::rel::prepare(
            "SELECT 4 AS id UNION ALL SELECT 1 AS id UNION ALL SELECT 3 AS id UNION ALL SELECT 2 AS id",
            &HashMap::new(), &options,
        ).unwrap();
        let input = prepared.relational.unwrap();
        let root = Node {
            schema: input.schema.clone(),
            estimated_rows: input.estimated_rows,
            op: RelOp::WindowOrder {
                input,
                order: vec![bind::Sort {
                    column: 0,
                    descending: false,
                    nulls_first: false,
                }],
            },
        };
        let denied = Budget::new(8 << 20);
        let mut denied_ctx = context(&options, &denied);
        let pressure = denied.reserve(denied.limit() - denied.used() - 1).unwrap();
        let denial = root
            .run(&mut denied_ctx, &mut |_| panic!("no output under pressure"))
            .unwrap_err();
        // This first refusal is the key-vector lease, before Sorter creation
        // or child execution. SpillStore itself was successfully funded above.
        assert!(denial.starts_with("resource-limit:"), "{denial}");
        assert!(
            denial.contains(&format!(
                "requested {},",
                std::mem::size_of::<bind::Sort>() + 256
            )),
            "{denial}"
        );
        drop(denied_ctx);
        drop(pressure);
        assert_eq!(denied.used(), 0);
        let budget = Budget::new(8 << 20);
        let mut ctx = context(&options, &budget);
        let mut emitted = 0;
        let error = root
            .run(&mut ctx, &mut |batch| {
                assert_eq!(batch.schema(), root.schema);
                assert!(budget.used() >= batch.get_array_memory_size());
                emitted += batch.num_rows();
                options.cancellation.cancel();
                Ok(())
            })
            .unwrap_err();
        assert!(error.starts_with("cancelled:"), "{error}");
        assert_eq!(emitted, 1);
        drop(ctx);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(directory).unwrap();
    }
}

#[test]
fn aggregate_windows_release_state_buffers_and_spills_on_every_exit() {
    let source = "(SELECT 1 AS id, 1 AS k, 2 AS v UNION ALL SELECT 2,1,4 UNION ALL SELECT 3,2,8) t";
    for spill in [false, true] {
        let directory = std::env::temp_dir().join(format!(
            "assay-aggregate-window-cleanup-{}-{spill}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        for frame in [
            "",
            "ORDER BY k",
            "ORDER BY k ROWS UNBOUNDED PRECEDING",
            "ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            "ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            "ORDER BY k ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
        ] {
            // Enough for the spill-store metadata, too little for the operator.
            for memory in [8 << 10, 2 << 20] {
                let options = Options {
                    batch_size: 1,
                    spill: spill.then(|| crate::spill::SpillOptions {
                        directory: directory.clone(),
                        sort_run_bytes: 128,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let query = format!("SELECT id,SUM(v) OVER ({frame}) FROM {source}");
                let prepared = crate::rel::prepare(&query, &HashMap::new(), &options).unwrap();
                let budget = Budget::new(memory);
                let mut ctx = context(&options, &budget);
                let mut emitted = 0;
                let result = prepared.relational.unwrap().run(&mut ctx, &mut |batch| {
                    assert!(budget.used() >= batch.get_array_memory_size());
                    emitted += batch.num_rows();
                    options.cancellation.cancel();
                    Ok(())
                });
                let error = result.unwrap_err();
                if memory == 8 << 10 {
                    assert!(error.starts_with("resource-limit:"), "{error}");
                    assert_eq!(emitted, 0);
                } else {
                    assert!(error.starts_with("cancelled:"), "{error}");
                    assert_eq!(emitted, 1);
                }
                drop(ctx);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
            }
        }
        let options = Options {
            batch_size: 1,
            spill: spill.then(|| crate::spill::SpillOptions {
                directory: directory.clone(),
                sort_run_bytes: 128,
                ..Default::default()
            }),
            ..Default::default()
        };
        for query in [
            "SELECT SUM(v*99999999999999999999999999999999999999) OVER () FROM (SELECT 2 AS v) t",
            "SELECT MIN(s) OVER () FROM (SELECT 'long-payload' AS s UNION ALL SELECT 'a') t",
            "SELECT SUM(v) OVER () FROM (SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS v UNION ALL SELECT CAST(1 AS DECIMAL(38,0))) t",
            // Monotone MIN queue with funded text payloads; picked values.
            "SELECT MIN(s) OVER (ORDER BY s ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (SELECT 'long-payload' AS s UNION ALL SELECT 'a') t",
            "SELECT LAST_VALUE(s) OVER (ORDER BY s ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (SELECT 'long-payload' AS s UNION ALL SELECT 'a') t",
            // Running-total queues: the second prefix 2X exceeds precision 38.
            "SELECT SUM(v*9223372036854775807*9223372036854775807) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (SELECT 1 AS v UNION ALL SELECT 1) t",
        ] {
            let prepared = crate::rel::prepare(query, &HashMap::new(), &options).unwrap();
            let budget = Budget::new(2 << 20);
            let mut ctx = context(&options, &budget);
            let mut emitted = 0;
            let result = prepared.relational.unwrap().run(&mut ctx, &mut |batch| {
                emitted += batch.num_rows();
                Ok(())
            });
            if query.contains("(s)") {
                assert!(result.is_ok());
                assert_eq!(emitted, 2);
            } else {
                assert!(result.is_err());
                assert_eq!(emitted, 0);
            }
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        }
        std::fs::remove_dir(directory).unwrap();
    }
}

#[test]
fn offset_errors_and_cancel_release_history_and_sort_reservations() {
    for function in ["LAG", "LEAD"] {
        let sql = format!(
            "SELECT {function}(v,2,'default') OVER (ORDER BY id) FROM (SELECT 1 AS id,'one' AS v UNION ALL SELECT 2,'two' UNION ALL SELECT 3,'three') t"
        );
        for limit in [1, 16_384, 8 << 20] {
            let options = Options {
                batch_size: 1,
                ..Default::default()
            };
            let prepared = crate::rel::prepare(&sql, &HashMap::new(), &options).unwrap();
            let budget = Budget::new(limit);
            let mut ctx = context(&options, &budget);
            let error = prepared
                .relational
                .unwrap()
                .run(&mut ctx, &mut |_| {
                    options.cancellation.cancel();
                    Err("cancelled: offset test".into())
                })
                .unwrap_err();
            assert!(
                error.starts_with("resource-limit:") || error.starts_with("cancelled:"),
                "{error}"
            );
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
