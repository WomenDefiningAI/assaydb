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
