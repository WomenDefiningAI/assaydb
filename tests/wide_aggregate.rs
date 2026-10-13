use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn final_only_wide_sum_avg_agree_across_execution_paths() {
    let root = std::env::temp_dir().join(format!("assaydb-wide-aggregate-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let limit = 10_i128.pow(38) - 1;
    for (index, values, total) in [
        (0, vec![Some(limit), Some(1), Some(-1)], Some(limit)),
        (
            1,
            vec![Some(limit), Some(limit), Some(-limit), Some(-limit)],
            Some(0),
        ),
        (2, vec![Some(-limit), Some(-1), Some(1)], Some(-limit)),
        (3, vec![None, None, None], None),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(38, 0), true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1; values.len()])),
            Arc::new(
                Decimal128Array::from(values.clone())
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ];
        let path = root.join(format!("t{index}.parquet"));
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        let tables = HashMap::from([("t".into(), path)]);
        let count = values.iter().flatten().count();
        for sql in [
            "SELECT SUM(v),AVG(v),COUNT(v) FROM t",
            "SELECT SUM(v),AVG(v),COUNT(v) FROM t GROUP BY k",
            "SELECT SUM(v),AVG(v),COUNT(v) FROM (SELECT k,v FROM t) q GROUP BY k",
        ] {
            for optimized in [false, true] {
                for workers in [1, 4] {
                    for spill in [false, true] {
                        let spill_path =
                            root.join(format!("spill-{index}-{workers}-{optimized}-{spill}"));
                        fs::create_dir_all(&spill_path).unwrap();
                        let options = Options {
                            workers,
                            batch_size: 1,
                            memory_bytes: 4 << 20,
                            relational_optimizations: optimized,
                            spill: spill.then(|| SpillOptions {
                                directory: spill_path.clone(),
                                ..SpillOptions::default()
                            }),
                            ..Options::default()
                        };
                        let result =
                            execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                        let sums = result
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap();
                        assert_eq!(sums.iter().collect::<Vec<_>>(), vec![total], "{sql}");
                        let avgs = result
                            .batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        assert_eq!(avgs.is_null(0), total.is_none());
                        if let Some(total) = total {
                            assert_eq!(avgs.value(0), (total as f64) / (count as f64));
                        }
                        let counts = result
                            .batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        assert_eq!(counts.value(0), count as i64);
                        assert!(fs::read_dir(&spill_path).unwrap().next().is_none());
                    }
                }
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn avg_does_not_narrow_shared_sum_and_distinct_remains_global() {
    let root = std::env::temp_dir().join(format!("assaydb-wide-average-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let limit = 10_i128.pow(38) - 1;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(38, 0),
        true,
    )]));
    let path = root.join("t.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![Arc::new(
                    Decimal128Array::from(vec![Some(limit), Some(limit), Some(1), Some(-1), None])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                )],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for workers in [1, 4] {
        for optimized in [false, true] {
            for spill in [false, true] {
                let dir = root.join(format!("spill-{workers}-{optimized}-{spill}"));
                fs::create_dir(&dir).unwrap();
                let options = Options {
                    workers,
                    batch_size: 1,
                    memory_bytes: 4 << 20,
                    relational_optimizations: optimized,
                    spill: spill.then(|| SpillOptions {
                        directory: dir.clone(),
                        ..SpillOptions::default()
                    }),
                    ..Options::default()
                };
                let sql = "SELECT AVG(v) AS a,AVG(v) AS b FROM t WHERE v>1";
                let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                for i in 0..2 {
                    let a = result
                        .batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    assert_eq!(a.value(0), limit as f64);
                }
                let sql = "SELECT SUM(DISTINCT v),AVG(DISTINCT v),COUNT(DISTINCT v) FROM t";
                let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    result
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .value(0),
                    limit
                );
                assert_eq!(
                    result
                        .batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    limit as f64 / 3.
                );
                assert_eq!(
                    result
                        .batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    3
                );
                for sql in [
                    "SELECT SUM(v) FROM t",
                    "SELECT AVG(v),SUM(v) FROM t",
                    "SELECT SUM(v+1) FROM t",
                ] {
                    // First two fail at final output; the last still fails in
                    // its input expression. Semantic v2 does not widen casts/addition.
                    let error = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                        .err()
                        .unwrap();
                    assert!(error.contains("overflow"), "{sql}: {error}");
                }
                let sql = "SELECT SUM(v),AVG(v),COUNT(v) FROM t WHERE 1=0";
                let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert!(result.batch.column(0).is_null(0));
                assert!(result.batch.column(1).is_null(0));
                assert_eq!(
                    result
                        .batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
                assert!(fs::read_dir(&dir).unwrap().next().is_none());
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn scaled_wide_average_and_admitted_workers_preserve_cancellation() {
    let root = std::env::temp_dir().join(format!("assaydb-wide-workers-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let limit = 10_i128.pow(38) - 1;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(38, 2), false),
    ]));
    let path = root.join("t.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
    // The first batch alone exceeds i128. The full sum cancels exactly.
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1; 4096])),
                    Arc::new(
                        Decimal128Array::from_iter_values(
                            (0..4096).map(|i| if i < 2048 { limit } else { -limit }),
                        )
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                    ),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for optimized in [false, true] {
        for workers in [1, 4] {
            for spill in [false, true] {
                let dir = root.join(format!("spill-{workers}-{optimized}-{spill}"));
                fs::create_dir(&dir).unwrap();
                let options = Options {
                    workers,
                    batch_size: 1024,
                    memory_bytes: 16 << 20,
                    relational_optimizations: optimized,
                    spill: spill.then(|| SpillOptions {
                        directory: dir.clone(),
                        ..SpillOptions::default()
                    }),
                    ..Options::default()
                };
                for suffix in ["", " GROUP BY k"] {
                    let sql = format!("SELECT COUNT(*),SUM(v),AVG(v) FROM t{suffix}");
                    let result =
                        execute(&prepare(&sql, &tables, &options).unwrap(), &options).unwrap();
                    assert_eq!(
                        result
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        4096
                    );
                    assert_eq!(
                        result
                            .batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                    assert_eq!(
                        result
                            .batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(0),
                        0.
                    );
                    if workers == 4 && !spill && optimized {
                        assert!(
                            result.metrics.parallel_aggregate_batches > 0,
                            "{sql}, optimized={optimized}"
                        );
                    }
                    let sql = format!("SELECT COUNT(*),AVG(v) FROM t WHERE v>0{suffix}");
                    let result =
                        execute(&prepare(&sql, &tables, &options).unwrap(), &options).unwrap();
                    assert_eq!(
                        result
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        2048
                    );
                    assert_eq!(
                        result
                            .batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(0),
                        limit as f64 / 100.
                    );
                    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                }
                assert!(fs::read_dir(&dir).unwrap().next().is_none());
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
