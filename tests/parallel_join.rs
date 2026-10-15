use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
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
fn parallel_probe_preserves_duplicate_null_payload_and_aggregate_results() {
    let root = std::env::temp_dir().join(format!("assaydb-probe-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let mut tables = HashMap::new();
    for (name, rows) in [("l", 1024), ("r", 97)] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 13 != 0).then_some(i % 37)),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 17 != 0).then(|| format!("é\0{:02}{}", i % 37, "x".repeat(4096)))
            }))),
        ];
        let path = root.join(format!("{name}.parquet"));
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
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        tables.insert(name.into(), path);
    }
    for batch_size in [17, 64] {
        let base = Options {
            batch_size,
            memory_bytes: 64 << 20,
            relational_optimizations: false,
            ..Options::default()
        };
        for (sql, eligible) in [
            (
                "SELECT a.i AS l,b.i AS r,a.s AS ls,b.s AS rs FROM l a LEFT JOIN r b ON a.k=b.k ORDER BY l,r NULLS FIRST",
                true,
            ),
            (
                "SELECT a.i AS l,b.i AS r FROM l a LEFT JOIN r b ON a.s=b.s ORDER BY l,r NULLS FIRST",
                true,
            ),
            (
                "SELECT COUNT(*) AS n,SUM(a.i) AS total FROM l a LEFT JOIN r b ON a.k=b.k",
                true,
            ),
            (
                "SELECT a.k,COUNT(*) AS n,SUM(b.i) AS total FROM l a JOIN r b ON a.k=b.k GROUP BY a.k ORDER BY 1",
                true,
            ),
            (
                "SELECT a.i AS l,b.i AS r FROM l a LEFT JOIN r b ON a.k=b.k AND a.i<b.i ORDER BY l,r NULLS FIRST",
                false,
            ),
            (
                "SELECT a.i FROM l a SEMI JOIN r b ON a.k=b.k ORDER BY 1",
                false,
            ),
        ] {
            let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
            for workers in [1, 4, 6, 10] {
                let options = Options {
                    workers,
                    relational_optimizations: true,
                    ..base.clone()
                };
                let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "{workers}/{batch_size}: {sql}"
                );
                if workers >= 6 && eligible {
                    assert!(
                        actual.metrics.parallel_joins > 0,
                        "parallel path not exercised: {workers}/{batch_size}: {sql}"
                    );
                    assert!(
                        actual.metrics.parallel_join_pending_batches_peak
                            <= 2 * ((workers - 1) / 2) + 1
                    );
                } else {
                    assert_eq!(actual.metrics.parallel_joins, 0, "{sql}");
                }
                assert!(actual.metrics.worker_threads_peak <= workers);
            }
        }
        // Also compare scan-only parallelism with parallel probing. Rule O
        // requires identical raw order independently of row-group batching.
        let raw_sql =
            "SELECT a.i AS l,b.i AS r,a.s AS ls,b.s AS rs FROM l a LEFT JOIN r b ON a.k=b.k";
        let scan_control = Options {
            workers: 4,
            relational_optimizations: true,
            ..base.clone()
        };
        let expected = execute(
            &prepare(raw_sql, &tables, &scan_control).unwrap(),
            &scan_control,
        )
        .unwrap();
        assert_eq!(expected.metrics.parallel_joins, 0);
        for workers in [6, 10] {
            let options = Options {
                workers,
                ..scan_control.clone()
            };
            let actual = execute(&prepare(raw_sql, &tables, &options).unwrap(), &options).unwrap();
            assert!(
                actual.batch == expected.batch,
                "unsorted pair order changed at {workers}/{batch_size}"
            );
            assert!(actual.metrics.parallel_joins > 0);
        }
        // A duplicate-heavy join must stream into its aggregate, not retain all output.
        let sql = "SELECT COUNT(*) AS n FROM l a LEFT JOIN r b ON a.k-a.k=b.k-b.k";
        let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
        let options = Options {
            workers: 6,
            relational_optimizations: true,
            ..base
        };
        let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert!(actual.metrics.parallel_joins > 0);
        assert!(actual.metrics.parallel_join_batches > 1000);
        assert!(actual.metrics.parallel_join_pending_batches_peak <= 5);
        let wide_sql = "SELECT COUNT(DISTINCT b.s) AS n FROM l a LEFT JOIN r b ON a.k-a.k=b.k-b.k";
        let serial = Options {
            workers: 1,
            relational_optimizations: false,
            ..options.clone()
        };
        let expected = execute(&prepare(wide_sql, &tables, &serial).unwrap(), &serial).unwrap();
        let constrained = Options {
            memory_bytes: 8 << 20,
            ..options
        };
        let actual = execute(
            &prepare(wide_sql, &tables, &constrained).unwrap(),
            &constrained,
        )
        .unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert!(actual.metrics.parallel_joins > 0);
        assert!(actual.metrics.parallel_join_batches > 1000);
        assert!(actual.metrics.parallel_join_pending_bytes_peak < constrained.memory_bytes);
    }
    fs::remove_dir_all(root).unwrap();
}
