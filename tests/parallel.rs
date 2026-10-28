// Zero-NULL approval: deliberate error/laziness markers use checked DECIMAL
// division, with an INTEGER cast where the original scalar type matters.
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn worker_sweep_preserves_order_filters_joins_shared_scans_and_spilled_results() {
    let root = std::env::temp_dir().join(format!("assaydb-parallel-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    fs::create_dir(root.join("spill")).unwrap();
    let path = root.join("t.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Decimal128(38, 0), false),
    ]));
    let high = 10_i128.pow(38) - 1;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..513)),
        Arc::new(Int64Array::from_iter(
            (0..513).map(|i| (i % 13 != 0).then_some(i % 23)),
        )),
        Arc::new(StringArray::from_iter((0..513).map(|i| {
            (i % 17 != 0).then(|| format!("{:04}:é\0{}", i % 43, "x".repeat(128)))
        }))),
        Arc::new(
            Decimal128Array::from_iter_values((0..513).map(|i| match i % 3 {
                0 => high,
                1 => -high,
                _ => 0,
            }))
            .with_precision_and_scale(38, 0)
            .unwrap(),
        ),
    ];
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(31)
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path.clone())]);
    for projection_pushdown in [false, true] {
        for optimized in [false, true] {
            for spill in [false, true] {
                let base = Options {
                    batch_size: 17,
                    projection_pushdown,
                    memory_bytes: 64 << 20,
                    relational_optimizations: optimized,
                    spill: spill.then(|| SpillOptions {
                        mode: Default::default(),
                        directory: root.join("spill"),
                        disk_quota_bytes: 128 << 20,
                        sort_run_bytes: 16 << 10,
                        merge_fan_in: 2,
                    }),
                    ..Options::default()
                };
                for workers in [1, 2, 4, 8, 10] {
                    let options = Options {
                        workers,
                        ..base.clone()
                    };
                    let sql = "SELECT SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS n FROM t";
                    let error = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                        .err()
                        .unwrap();
                    assert!(error.contains("overflow"), "{error}");
                    assert_eq!(fs::read_dir(root.join("spill")).unwrap().count(), 0);
                }
                for sql in [
                    "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE 100.0/(i-40)>0) q",
                    "SELECT SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS n FROM t WHERE CASE WHEN i<40 THEN TRUE ELSE 100.0/(i-40)>0 END",
                    "SELECT SUM(i) AS n FROM t WHERE CASE WHEN i<40 THEN FALSE ELSE 100.0/(i-40)>0 END",
                    "SELECT SUM(CASE WHEN d<0 THEN -d ELSE d END) AS n FROM (SELECT i,d FROM t WHERE CASE WHEN i>=40 THEN 100.0/(i-40)>0 ELSE TRUE END) q",
                ] {
                    let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base)
                        .err()
                        .unwrap();
                    for workers in [2, 4, 8, 10] {
                        let options = Options {
                            workers,
                            ..base.clone()
                        };
                        let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                            .err()
                            .unwrap();
                        assert_eq!(
                            actual, expected,
                            "ordered error at {workers} workers: {sql}"
                        );
                        assert_eq!(fs::read_dir(root.join("spill")).unwrap().count(), 0);
                    }
                }
                for sql in [
                    "SELECT i,k,s FROM t",
                    "SELECT i,s FROM t WHERE k<3 ORDER BY i",
                    "SELECT k,COUNT(*) AS n,SUM(i) AS total FROM t GROUP BY k ORDER BY k",
                    "SELECT COUNT(DISTINCT s) AS n FROM t",
                    "SELECT SUM(d) AS n FROM t",
                    "SELECT a.i AS l,b.i AS r FROM t a LEFT JOIN t b ON a.k=b.k AND a.i<b.i ORDER BY l,r NULLS FIRST LIMIT 31",
                    "SELECT a.i AS i FROM t a SEMI JOIN t b ON a.k=b.k AND a.i<b.i ORDER BY i",
                    "SELECT a.i AS i FROM t a ANTI JOIN t b ON a.k=b.k AND a.i<b.i ORDER BY i",
                    "SELECT i,s IN(SELECT s FROM t WHERE i<70) AS yes,s NOT IN(SELECT s FROM t WHERE i<70) AS no FROM t ORDER BY i",
                    "WITH x AS (SELECT i,k,s FROM t) SELECT a.i AS i,b.s AS s FROM x a JOIN x b ON a.i=b.i WHERE b.i<70 ORDER BY i",
                    "SELECT a.i AS i,b.s AS s FROM (SELECT i FROM t WHERE i<7) a JOIN t b ON a.i=b.i ORDER BY i",
                    "SELECT i FROM t WHERE i<0",
                    "SELECT COUNT(*) AS n FROM t WHERE i<0",
                    "SELECT COUNT(*) AS n FROM t WHERE FALSE",
                    "SELECT COUNT(*) AS n FROM t WHERE TRUE",
                    "SELECT COUNT(*) AS n,SUM(i) AS s FROM t WHERE i<0",
                    "SELECT s,i FROM t WHERE k IS NULL OR k<3 ORDER BY i",
                    "SELECT s FROM t WHERE k IS NULL OR k<3 ORDER BY s",
                    "SELECT COUNT(*) AS n FROM t WHERE d<0",
                    "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE i<70) q",
                    "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE CASE WHEN i=0 THEN TRUE ELSE 100/i>0 END) q",
                    "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE s IS NULL OR k<3) q",
                    "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE i<0) q",
                ] {
                    let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
                    for workers in [2, 4, 8, 10] {
                        let options = Options {
                            workers,
                            ..base.clone()
                        };
                        let actual =
                            execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                        assert_eq!(actual.batch, expected.batch, "{workers}: {sql}");
                        assert_eq!(actual.metrics.workers, workers);
                        if optimized
                            && !spill
                            && sql == "SELECT COUNT(*) AS n,SUM(i) AS s FROM t WHERE i<0"
                        {
                            let control_sql = "SELECT COUNT(*) AS n,SUM(i) AS s FROM t";
                            let control = execute(
                                &prepare(control_sql, &tables, &options).unwrap(),
                                &options,
                            )
                            .unwrap();
                            assert_eq!(actual.metrics.rows_read, 513);
                            assert_eq!(actual.metrics.batches, control.metrics.batches);
                            assert_eq!(
                                actual.metrics.batches,
                                actual.metrics.post_decode_filter_batches
                            );
                            assert_eq!(
                                actual.metrics.decoded_arrow_bytes_total,
                                control.metrics.decoded_arrow_bytes_total
                            );
                            assert_eq!(actual.metrics.post_decode_filter_output_rows, 0);
                            assert!(actual.metrics.post_decode_filter_batches > 0);
                        }
                        assert!(
                            actual.metrics.worker_threads_peak > 0
                                && actual.metrics.worker_threads_peak <= workers,
                            "{sql}"
                        );
                        assert!(actual.metrics.parallel_scan_tasks > 0, "{sql}");
                        if optimized
                            && sql == "SELECT SUM(i) AS n FROM (SELECT i FROM t WHERE i<70) q"
                        {
                            assert!(actual.metrics.post_decode_scan_filters > 0, "{sql}");
                        }
                        // The bounded lookahead reader has queue_slots messages
                        // per lane, one blocked producer batch per lane, and at
                        // most one received batch whose Pending is being dropped.
                        // The caller can also retain one bounded speculative queue;
                        // it is included in the requested worker limit.
                        let queue_slots = actual.metrics.parallel_scan_queue_slots_max;
                        assert!((6..=9).contains(&queue_slots), "{sql}");
                        assert!(
                            actual.metrics.parallel_scan_pending_batches_peak
                                <= (queue_slots + 1) * workers + 1,
                            "workers={workers}, queue_slots={queue_slots}: {sql}"
                        );
                        assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                        assert_eq!(fs::read_dir(root.join("spill")).unwrap().count(), 0);
                    }
                }
            }
        }
    }
    for workers in [0, 65] {
        assert!(
            prepare(
                "SELECT 1",
                &tables,
                &Options {
                    workers,
                    ..Options::default()
                }
            )
            .is_err()
        );
    }
    fs::remove_file(path).unwrap();
    fs::remove_dir(root.join("spill")).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn direct_filter_keeps_projected_type_validation_even_when_all_rows_reject() {
    use arrow::array::{TimestampMicrosecondArray, UInt64Array};
    let root = std::env::temp_dir().join(format!("assaydb-filter-types-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    // Old -> new (exact Parquet widening): an in-range UInt64 column used to
    // refuse here; it is now an exact Int64 input, so the all-rows-rejected
    // aggregate returns COUNT(*) = 0 and SUM = NULL (DuckDB 1.5.6 gives the
    // same 0 / NULL over this file). Old -> new (TIMESTAMP and TIMESTAMPTZ
    // cards, Oct 17): wall-clock and time-zone-aware Timestamp columns are now
    // TIMESTAMP and TIMESTAMPTZ and join the supported side; the
    // projected-type validation this test guards is kept with a TIME column,
    // which remains refused.
    let cases: [(&str, ArrayRef, bool); 4] = [
        (
            "u.parquet",
            Arc::new(UInt64Array::from_iter_values(0..69)),
            true,
        ),
        (
            "ts.parquet",
            Arc::new(TimestampMicrosecondArray::from_iter_values(0..69)),
            true,
        ),
        (
            "tz.parquet",
            Arc::new(TimestampMicrosecondArray::from_iter_values(0..69).with_timezone("UTC")),
            true,
        ),
        (
            "time.parquet",
            Arc::new(arrow::array::Time64MicrosecondArray::from_iter_values(
                0..69,
            )),
            false,
        ),
    ];
    for (name, column, supported) in cases {
        let path = root.join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("unsupported", column.data_type().clone(), false),
        ]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..69)), column];
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(17)
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        let tables = HashMap::from([("t".into(), path)]);
        for workers in [1, 4] {
            let options = Options {
                workers,
                batch_size: 7,
                ..Options::default()
            };
            let sql = "SELECT COUNT(*) AS n,SUM(i) AS s FROM t WHERE i<0";
            let prepared = prepare(sql, &tables, &options).unwrap();
            let result = execute(&prepared, &options).unwrap();
            assert_eq!(result.metrics.rows_after_filter, 0);
            let options = Options {
                projection_pushdown: false,
                ..options
            };
            for sql in [sql, "SELECT COUNT(*) AS n,SUM(i) AS s FROM t WHERE FALSE"] {
                let prepared = prepare(sql, &tables, &options).unwrap();
                let result = execute(&prepared, &options);
                if supported {
                    let batch = result.unwrap().batch;
                    assert_eq!(batch.num_rows(), 1, "{name} {workers} {sql}");
                    let n = batch.column(0).as_any().downcast_ref::<Int64Array>();
                    assert_eq!(n.unwrap().value(0), 0, "{name} {workers} {sql}");
                    assert!(batch.column(1).is_null(0), "{name} {workers} {sql}");
                } else {
                    assert_eq!(
                        result.err().unwrap(),
                        "unsupported: unsupported input type: Time64(µs)",
                        "{name} {workers} {sql}"
                    );
                }
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
