//! Production dispatch through public Options; no cfg(test) force switches.
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Execution, Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new(rows: usize, row_group: usize) -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-public-pipeline-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
            Field::new("d", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| [Some(3), None, Some(1), Some(2)][i % 4]),
                )),
                Arc::new(StringArray::from_iter((0..rows).map(|i| {
                    [Some("é"), Some(""), None, Some("a"), Some("longer")][i % 5]
                }))),
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some((i % 17) as i64 - 8)),
                )),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| i64::from(i != 1)),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn run(&self, sql: &str, options: &Options) -> Result<Execution, String> {
        let prepared = prepare(sql, &HashMap::from([("t".into(), self.0.clone())]), options)?;
        execute(&prepared, options)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn options(workers: usize) -> Options {
    Options {
        workers,
        batch_size: 64,
        ..Options::default()
    }
}

#[test]
fn default_ten_worker_pipeline_matches_serial_results_and_encounter_order() {
    // 16 groups, with 257-row boundaries deliberately misaligned to 64-row
    // batches. 4096 rows exceeds max(1024, 2 * 64 * 10) admission threshold.
    let fixture = Fixture::new(4096, 257);
    let serial = options(1);
    let parallel = options(10);
    for sql in [
        "SELECT k,SUM(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
        "SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k",
        "SELECT s,k,SUM(v),COUNT(*) FROM t WHERE v > 0 GROUP BY s,k ORDER BY s,k",
        "SELECT k,COUNT(*) FROM t WHERE v > 100 GROUP BY k",
    ] {
        let reference = fixture.run(sql, &serial).unwrap();
        assert_eq!(reference.metrics.pipeline_candidate_attempts, 0);
        assert_eq!(reference.metrics.pipeline_source_routes, 0);
        let actual = fixture.run(sql, &parallel).unwrap();
        assert_eq!(actual.batch, reference.batch, "{sql}");
        let metrics = actual.metrics;
        assert_eq!(metrics.workers, 10);
        assert_eq!(metrics.pipeline_candidate_attempts, 1);
        assert_eq!(metrics.pipeline_fallbacks, 0);
        assert_eq!(metrics.pipeline_producers, 10);
        assert_eq!(metrics.rows_read, 4096);
        assert_eq!(metrics.pipeline_source_batches, 79);
        assert_eq!(
            metrics.pipeline_source_routes,
            metrics.pipeline_pressure_flushes + metrics.pipeline_producers
        );
        assert_eq!(
            metrics.pipeline_runtime_submitted,
            metrics.pipeline_runtime_completed
        );
        for kind in 0..3 {
            assert_eq!(
                metrics.pipeline_runtime_submitted[kind],
                metrics.pipeline_runtime_caller[kind] + metrics.pipeline_runtime_background[kind]
            );
        }
        assert!(metrics.pipeline_runtime_peak_outstanding <= 20);
        assert!(metrics.worker_threads_peak < parallel.workers);
        assert!(metrics.pipeline_background_routes <= metrics.pipeline_source_routes);
        assert!(parallel.cancellation.check().is_ok());
    }
}

#[test]
fn default_dispatch_bypasses_small_single_group_and_unsupported_aggregates() {
    let parallel = options(10);
    for fixture in [Fixture::new(32, 7), Fixture::new(4096, 4096)] {
        let output = fixture
            .run("SELECT k,COUNT(*) FROM t GROUP BY k", &parallel)
            .unwrap();
        assert_eq!(output.metrics.pipeline_candidate_attempts, 0);
        assert_eq!(output.metrics.pipeline_source_routes, 0);
    }
    let fixture = Fixture::new(4096, 257);
    for sql in [
        "SELECT k,MIN(v) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT v) FROM t GROUP BY k",
        "SELECT COUNT(*) FROM t",
    ] {
        let reference = fixture.run(sql, &options(1)).unwrap();
        let output = fixture.run(sql, &parallel).unwrap();
        assert_eq!(output.batch, reference.batch, "{sql}");
        assert_eq!(output.metrics.pipeline_candidate_attempts, 0, "{sql}");
        assert_eq!(output.metrics.pipeline_source_routes, 0);
    }
}

#[test]
fn default_low_memory_candidate_replays_baseline_once_without_cancelling_caller() {
    let fixture = Fixture::new(4096, 257);
    let sql = "SELECT k,COUNT(*) FROM t GROUP BY k";
    let constrained = Options {
        memory_bytes: 1024 * 1024,
        parquet_cache_bytes: Some(0),
        ..options(10)
    };
    // Public optimizer switch gives an independent baseline at identical worker,
    // batch, cache and budget settings. Small fixed cardinality fits this budget.
    let baseline = Options {
        relational_optimizations: false,
        ..constrained.clone()
    };
    let expected = fixture.run(sql, &baseline).unwrap();
    assert_eq!(expected.metrics.pipeline_candidate_attempts, 0);
    let output = fixture.run(sql, &constrained).unwrap();
    assert_eq!(output.batch, expected.batch);
    assert_eq!(output.metrics.pipeline_candidate_attempts, 1);
    assert_eq!(output.metrics.pipeline_fallbacks, 1);
    // Even before fixed metadata, 1 MiB / (4 * 10) is below the generic 64 KiB
    // private-slice floor, so this failure precedes producer source consumption.
    assert_eq!(output.metrics.pipeline_producers, 0);
    assert_eq!(output.metrics.pipeline_source_routes, 0);
    assert_eq!(output.metrics.workers, constrained.workers);
    assert!(output.metrics.parallel_scan_tasks > 0);
    assert!(output.metrics.pipeline_candidate_error.is_some());
    assert!(output.metrics.pipeline_candidate_sql_error.is_none());
    assert!(
        output.metrics.execution_seconds
            >= output.metrics.pipeline_candidate_seconds + output.metrics.pipeline_fallback_seconds
    );
    assert!(
        output.metrics.peak_reserved_bytes >= output.metrics.pipeline_candidate_peak_reserved_bytes
    );
    assert!(output.metrics.peak_reserved_bytes <= constrained.memory_bytes);
    assert!(output.metrics.worker_threads_peak < constrained.workers);
    assert!(constrained.cancellation.check().is_ok());
}

#[test]
fn public_sql_error_and_cancellation_remain_errors() {
    let fixture = Fixture::new(4096, 257);
    let sql = "SELECT k,COUNT(CAST(1.0/d AS INTEGER)) FROM t GROUP BY k";
    let parallel = options(10);
    let baseline = Options {
        relational_optimizations: false,
        ..parallel.clone()
    };
    let expected = fixture.run(sql, &baseline).err().unwrap();
    assert!(expected.contains("division by zero"));
    assert_eq!(fixture.run(sql, &parallel).err().unwrap(), expected);
    assert!(parallel.cancellation.check().is_ok());
    parallel.cancellation.cancel();
    assert!(
        fixture
            .run("SELECT k,COUNT(*) FROM t GROUP BY k", &parallel)
            .err()
            .unwrap()
            .contains("cancel")
    );
}

#[test]
fn default_direct_pipeline_preserves_large_binary_group_keys() {
    let fixture = Fixture::new(4096, 257);
    let expression = format!(
        "CASE WHEN k IS NULL THEN NULL WHEN k=1 THEN X'' WHEN k=2 THEN X'{}' ELSE X'00ff' END",
        "00ff".repeat(2048),
    );
    let sql = format!("SELECT {expression} AS b,COUNT(*) FROM t GROUP BY {expression}");
    let serial = fixture.run(&sql, &options(1)).unwrap();
    let parallel = fixture.run(&sql, &options(4)).unwrap();
    assert!(parallel.metrics.pipeline_producers > 0);
    assert_eq!(parallel.metrics.pipeline_fallbacks, 0);
    assert_eq!(parallel.batch, serial.batch);
    assert_eq!(parallel.batch.num_rows(), 4);
}
