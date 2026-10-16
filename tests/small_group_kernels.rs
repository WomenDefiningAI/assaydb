use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn earlier_slow_batch_error_wins_over_later_unsafe_batch_at_every_worker_count() {
    let root = std::env::temp_dir().join(format!(
        "assaydb-small-group-error-batches-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let path = root.join("t.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("k", DataType::Int64, false),
        Field::new("z", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"; 1024])),
            Arc::new(Int64Array::from_iter_values(0..1024)),
            Arc::new(Int64Array::from_iter_values(
                (0..1024).map(|k| i64::from(k < 128)),
            )),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(128)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    // The first batch rejects narrow admission on its first operand and then
    // overflows precision 38 on its second ordered INTEGER addition. Later
    // batches also contain a wide operand; the second aggregate additionally
    // raises a distinct expression error there, so choosing it is observable.
    for aggregates in [
        "SUM(CASE WHEN k=0 OR k=128 THEN 99999999999999999999999999999999999999 ELSE 1 END)",
        "SUM(CASE WHEN k=0 OR k=128 THEN 99999999999999999999999999999999999999 ELSE 1 END),SUM(CAST(1.0/z AS INTEGER))",
    ] {
        let sql = format!("SELECT g,{aggregates} FROM t GROUP BY g");
        let serial = Options {
            workers: 1,
            batch_size: 128,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(&sql, &tables, &serial).unwrap(), &serial)
            .err()
            .unwrap();
        assert_eq!(expected, "decimal precision 38 overflow");
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                relational_optimizations: true,
                ..serial.clone()
            };
            let actual = execute(&prepare(&sql, &tables, &options).unwrap(), &options)
                .err()
                .unwrap();
            assert_eq!(actual, expected, "workers={workers}: {sql}");
            if aggregates.contains("1.0/z") {
                let later_sql = format!("SELECT g,{aggregates} FROM t WHERE k>=128 GROUP BY g");
                let later = execute(&prepare(&later_sql, &tables, &options).unwrap(), &options)
                    .err()
                    .unwrap();
                assert!(later.contains("zero"), "later-batch control: {later}");
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compact_groups_and_bounded_sums_match_serial_at_every_worker_count() {
    let root = std::env::temp_dir().join(format!(
        "assaydb-small-group-kernels-{}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let path = root.join("t.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("k", DataType::Int64, false),
        Field::new("i", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 2), true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter((0..1025).map(|row| {
            if row < 513 {
                Some(["B", "A", "\0"][row % 3])
            } else {
                [Some(""), Some("ab"), Some("é"), None][row % 4]
            }
        }))),
        Arc::new(StringArray::from_iter(
            (0..1025).map(|row| Some(["y", "x"][row % 2])),
        )),
        Arc::new(Int64Array::from_iter_values(
            (0..1025).map(|row| (row * 17 % 79) as i64),
        )),
        Arc::new(Int64Array::from_iter(
            (0..1025).map(|row| (row % 3 != 0).then_some(row as i64 - 512)),
        )),
        Arc::new(
            Decimal128Array::from_iter(
                (0..1025).map(|row| (row % 3 != 0).then_some(row as i128 * 13 - 7000)),
            )
            .with_precision_and_scale(38, 2)
            .unwrap(),
        ),
    ];
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(257)
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for batch_size in [1, 7, 8192] {
        let serial = Options {
            batch_size,
            relational_optimizations: false,
            memory_bytes: 16 << 20,
            ..Options::default()
        };
        for sql in [
            "SELECT a,b,SUM(i),AVG(i),SUM(d),AVG(d),COUNT(i),COUNT(*) FROM t GROUP BY a,b",
            "SELECT a,b,SUM(i) FROM t GROUP BY a,b",
            "SELECT a,b,SUM(k),SUM(k+1),AVG(k) FROM t GROUP BY a,b",
            "SELECT a,b,SUM(k) FROM t GROUP BY a,b",
            "SELECT a,b,AVG(d) FROM t GROUP BY a,b",
            "SELECT k,SUM(i) FROM t GROUP BY k",
            "SELECT k,SUM(k) FROM t GROUP BY k",
            "SELECT a,k,SUM(i),AVG(d),COUNT(*) FROM t GROUP BY a,k",
            "SELECT SUM(i),AVG(d),COUNT(*) FROM t",
            "SELECT a,b,SUM(i) FROM t WHERE k<0 GROUP BY a,b",
            "SELECT SUM(i),AVG(d),COUNT(*) FROM t WHERE k<0",
            "SELECT a,b,SUM(CASE WHEN k<0 THEN i ELSE NULL END) FROM t GROUP BY a,b",
            "SELECT a,b,SUM(k+0.25),AVG(d+k),COUNT(NULL) FROM t GROUP BY a,b",
            "SELECT a,b,AVG(CASE WHEN k<40 THEN 999999999999999999999999999999999999.99 ELSE -999999999999999999999999999999999999.99 END) FROM t GROUP BY a,b",
            "SELECT l.a,l.b,SUM(r.i),AVG(r.d),COUNT(*) FROM t l JOIN t r ON l.k=r.k WHERE l.k<2 GROUP BY l.a,l.b ORDER BY 1,2",
            "SELECT l.a,l.b,SUM(r.i),COUNT(r.i) FROM t l LEFT JOIN t r ON l.k=r.k AND r.k<0 GROUP BY l.a,l.b ORDER BY 1,2",
            "SELECT l.a,l.b,SUM(r.i),COUNT(*) FROM t l JOIN t r ON l.k=r.k WHERE l.k<0 GROUP BY l.a,l.b",
        ] {
            let expected = execute(&prepare(sql, &tables, &serial).unwrap(), &serial).unwrap();
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    relational_optimizations: true,
                    ..serial.clone()
                };
                let prepared = prepare(sql, &tables, &options).unwrap();
                let actual = execute(&prepared, &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "workers={workers}, batch={batch_size}: {sql}"
                );
                assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert!(actual.metrics.worker_threads_peak <= workers);
            }
        }
        for (sql, cause) in [
            (
                "SELECT a,b,SUM(CASE WHEN k=0 THEN 99999999999999999999999999999999999999 ELSE 1 END) FROM t GROUP BY a,b",
                "overflow",
            ),
            (
                "SELECT a,b,SUM(CAST(1.0/(k-k) AS INTEGER)),SUM(i) FROM t GROUP BY a,b",
                "zero",
            ),
        ] {
            let expected = execute(&prepare(sql, &tables, &serial).unwrap(), &serial)
                .err()
                .unwrap();
            assert!(expected.contains(cause), "{sql}: {expected}");
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    relational_optimizations: true,
                    ..serial.clone()
                };
                let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                    .err()
                    .unwrap();
                assert_eq!(actual, expected, "workers={workers}: {sql}");
            }
        }
    }
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            ..Options::default()
        };
        let prepared = prepare("SELECT a,b,SUM(i) FROM t GROUP BY a,b", &tables, &options).unwrap();
        options.cancellation.cancel();
        assert_eq!(
            execute(&prepared, &options).err().unwrap(),
            "cancelled: query execution"
        );
        let fresh = Options {
            workers,
            ..Options::default()
        };
        assert!(execute(&prepared, &fresh).is_ok());
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_byte_pairs_and_late_groups_match_serial_across_source_batches() {
    let root =
        std::env::temp_dir().join(format!("assaydb-small-group-pairs-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let path = root.join("pairs.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("v", DataType::Decimal128(15, 2), false),
    ]));
    let rows = 8192;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
                if row % 1024 >= 900 {
                    "C"
                } else {
                    ["B", "A", "\0"][row % 3]
                }
            }))),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| ["y", "x"][row % 2]),
            )),
            Arc::new(
                Decimal128Array::from_iter_values((0..rows).map(|row| row as i128 - 4096))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(1024)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for sql in [
        "SELECT a,b,SUM(v),AVG(v),SUM(v*2),COUNT(*) FROM t GROUP BY a,b",
        "SELECT a,b,SUM(v),MIN(v),COUNT(DISTINCT v) FROM t GROUP BY a,b",
        "SELECT a,b,SUM(v),AVG(v),COUNT(*) FROM t WHERE v>99999 GROUP BY a,b",
        "SELECT a,b,SUM(CAST(1.0/(v-v) AS INTEGER)),COUNT(*) FROM t GROUP BY a,b",
    ] {
        let serial = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables, &serial).unwrap(), &serial).map(|r| r.batch);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 128, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options);
                if let Ok(result) = &actual {
                    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                    if result.batch.num_rows() > 0 && batch_size == 128 {
                        assert!(result.metrics.short_group_cache_hits > 0);
                    }
                    if workers > 1 && batch_size == 128 && sql.contains("SUM(v*2)") {
                        assert!(result.metrics.pipeline_source_batches > 0);
                        assert!(result.metrics.bounded_sum_batches > 0);
                    }
                }
                assert_eq!(
                    actual.map(|r| r.batch),
                    expected,
                    "workers={workers}, batch={batch_size}: {sql}"
                );
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
