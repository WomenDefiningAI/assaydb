use super::*;
use parquet::arrow::ArrowWriter;

#[test]
fn planner_filters_and_reverse_joins_release_state_on_active_cancellation() {
    let directory =
        std::env::temp_dir().join(format!("assaydb-planner-cancel-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let mut tables = HashMap::new();
    for (name, count) in [("p", 4), ("r", 512)] {
        let path = directory.join(format!("{name}.parquet"));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![Arc::new(Int64Array::from_iter_values(0..count))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        tables.insert(name.into(), path);
    }
    for sql in [
        "SELECT p.k FROM p JOIN r ON p.k=r.k AND r.k IN (0,1,2)",
        "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM r WHERE p.k=r.k AND r.k IN (0,1,2))",
        "SELECT p.k FROM p WHERE NOT EXISTS(SELECT 1 FROM r WHERE p.k=r.k AND r.k IN (0,1,2))",
    ] {
        for relational_optimizations in [false, true] {
            let base = Options {
                relational_optimizations,
                ..Options::default()
            };
            let prepared = execute::prepare(sql, &tables, &base).unwrap();
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        cancellation: Default::default(),
                        ..base.clone()
                    };
                    let budget = Budget::new(options.memory_bytes);
                    let mut ctx = Context {
                        filtered_reuse: None,
                        build_groups: None,
                        demand_cache: None,
                        shared_attempt: None,
                        options: &options,
                        scan_filters: HashMap::new(),
                        static_filters: HashMap::new(),
                        scan_transforms: HashMap::new(),
                        lane_transforms: HashMap::new(),
                        replay_transforms: HashMap::new(),
                        materialized: HashMap::new(),
                        sideways_registry_lease: None,
                        spill: None,
                        budget: budget.clone(),
                        metrics: Metrics::default(),
                    };
                    let mut delivered = 0;
                    let error = prepared
                        .relational
                        .as_ref()
                        .unwrap()
                        .run(&mut ctx, &mut |batch| {
                            // Cancel after real join output, not at entry. The
                            // callback propagates cancellation even for a final
                            // one-batch result, exercising stack unwinding.
                            if batch.num_rows() > 0 {
                                delivered += batch.num_rows();
                                options.cancellation.cancel();
                            }
                            options.cancellation.check()
                        })
                        .unwrap_err();
                    assert_eq!(error, "cancelled: query execution", "{sql}");
                    assert!(delivered > 0, "{sql}");
                    assert!(ctx.scan_filters.is_empty(), "{sql}");
                    assert!(ctx.static_filters.is_empty(), "{sql}");
                    drop(ctx);
                    assert_eq!(budget.used(), 0, "{sql}");
                    assert_eq!(budget.active_workers(), 0, "{sql}");
                }
            }
        }
    }
    for path in tables.values() {
        std::fs::remove_file(path).unwrap();
    }
    std::fs::remove_dir(directory).unwrap();
}
