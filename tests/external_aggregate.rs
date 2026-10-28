use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
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
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
#[test]
fn distinct_set_operations_share_forced_spill_and_cleanup() {
    let fixture = Fixture::new(2048, 127, 24);
    let options = fixture.options(32 << 20, 128, true);
    for sql in [
        "SELECT k,s FROM (SELECT k,s FROM t UNION SELECT k,s FROM t WHERE v>0) u ORDER BY k,s",
        "SELECT k,s FROM (SELECT k,s FROM t INTERSECT SELECT k,s FROM t WHERE v>0 OR k IS NULL) u ORDER BY k,s",
        "SELECT k,s FROM (SELECT k,s FROM t EXCEPT SELECT k,s FROM t WHERE v>0 OR k IS NULL) u ORDER BY k,s",
        "SELECT f FROM (SELECT f FROM t UNION SELECT f FROM t) u ORDER BY f",
    ] {
        fixture.check(sql, &options);
        assert!(
            std::fs::read_dir(fixture.root.join("spill"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}
#[test]
fn select_distinct_reuses_forced_spill_and_cleanup() {
    let fixture = Fixture::new(2048, 127, 24);
    let options = fixture.options(32 << 20, 128, true);
    for sql in [
        "SELECT DISTINCT k,s FROM t ORDER BY k NULLS FIRST,s NULLS FIRST",
        "SELECT DISTINCT f FROM t ORDER BY f NULLS FIRST",
        "SELECT DISTINCT COUNT(*) AS n FROM t GROUP BY k ORDER BY n LIMIT 3",
    ] {
        fixture.check(sql, &options);
    }
}
struct Fixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new(rows: usize, groups: usize, width: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "assaydb-aggregate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("spill")).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Decimal128(38, 0), true),
        ]));
        let limit = 10_i128.pow(38) - 1;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 17 != 0).then_some((i % groups) as i64)),
            )),
            Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                (i % 13 != 0).then_some((i % (rows / 2).max(1)) as i64 - 57)
            }))),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 11 != 0)
                    .then(|| format!("{:08}:é🦀\0{}", i % (rows / 2).max(1), "w".repeat(width)))
            }))),
            Arc::new(Float64Array::from_iter((0..rows).map(|i| {
                (i % 7 != 0).then_some(if i % 2 == 0 { -0.0 } else { 0.0 })
            }))),
            Arc::new(
                Decimal128Array::from_iter_values((0..rows).map(|i| [limit, -limit, 1, -1][i % 4]))
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ];
        let path = root.join("t.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(256)
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        Self {
            root,
            tables: HashMap::from([("t".into(), path)]),
        }
    }
    fn options(&self, memory: usize, batch: usize, optimized: bool) -> Options {
        Options {
            memory_bytes: memory,
            batch_size: batch,
            parquet_cache_bytes: None,
            relational_optimizations: optimized,
            spill: Some(SpillOptions {
                mode: Default::default(),
                directory: self.root.join("spill"),
                disk_quota_bytes: 256 << 20,
                sort_run_bytes: 64 << 10,
                merge_fan_in: 2,
            }),
            ..Options::default()
        }
    }
    fn check(&self, sql: &str, options: &Options) -> assaydb::execute::Execution {
        let plain = Options {
            memory_bytes: 256 << 20,
            spill: None,
            ..options.clone()
        };
        let expected = execute(&prepare(sql, &self.tables, &plain).unwrap(), &plain).unwrap();
        let actual = execute(&prepare(sql, &self.tables, options).unwrap(), options).unwrap();
        assert_eq!(actual.batch, expected.batch, "{sql}");
        assert!(actual.metrics.external_aggregate_partitions > 0, "{sql}");
        assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
        assert_eq!(actual.metrics.spill.live_bytes, 0);
        assert_eq!(actual.metrics.spill.live_files, 0);
        self.clean();
        actual
    }
    fn clean(&self) {
        assert_eq!(fs::read_dir(self.root.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.clean();
        fs::remove_file(self.root.join("t.parquet")).unwrap();
        fs::remove_dir(self.root.join("spill")).unwrap();
        fs::remove_dir(&self.root).unwrap();
    }
}

#[test]
fn partitioned_groups_match_null_distinct_expression_and_empty_semantics() {
    let fixture = Fixture::new(257, 23, 8);
    for batch in [7, 64] {
        for optimized in [false, true] {
            let options = fixture.options(8 << 20, batch, optimized);
            for sql in [
                "SELECT k,COUNT(*),COUNT(NULL),SUM(v),AVG(v),MIN(s),MAX(s),COUNT(DISTINCT s),SUM(DISTINCT v),AVG(DISTINCT v) FROM t GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT f,COUNT(*),COUNT(DISTINCT s) FROM t GROUP BY f ORDER BY f NULLS FIRST",
                "SELECT k+3 AS shifted,COUNT(*),SUM(CASE WHEN v IS NULL THEN 7 ELSE v+1 END) FROM t WHERE k IS NOT NULL GROUP BY k+3 ORDER BY shifted",
                "SELECT COUNT(DISTINCT s),SUM(DISTINCT v),AVG(DISTINCT v),COUNT(*) FROM t",
                "SELECT k,COUNT(DISTINCT s) FROM t WHERE k < 0 GROUP BY k ORDER BY k",
                "SELECT COUNT(DISTINCT s),SUM(DISTINCT v) FROM t WHERE k < 0",
                "SELECT k,COUNT(*) AS \"MixedCase\" FROM t GROUP BY k ORDER BY k NULLS FIRST",
                "SELECT COUNT(DISTINCT NULL),SUM(CASE WHEN v = -57 THEN 9223372036854775807 + 1 ELSE -9223372036854775807 - 1 END) AS wide FROM t WHERE v IN (-57,-56)",
            ] {
                fixture.check(sql, &options);
            }
        }
    }
}

#[test]
fn automatic_aggregate_retries_after_state_pressure() {
    let fixture = Fixture::new(16384, 8192, 8);
    for workers in [1, 4, 10] {
        let mut options = fixture.options(4 << 20, 128, true);
        options.workers = workers;
        options.spill.as_mut().unwrap().mode = assaydb::spill::SpillMode::Auto;
        let sql = "SELECT k,COUNT(*),SUM(v),MIN(s),COUNT(DISTINCT s) FROM t GROUP BY k ORDER BY k NULLS FIRST LIMIT 29";
        let actual = fixture.check(sql, &options);
        assert_eq!(actual.metrics.auto_spill_retries, 1);
        // Compact numeric groups may now fit their first partition. The
        // optimizer-off case below still exercises recursive repartitioning.
    }
}
#[test]
fn high_cardinality_repartitions_without_publishing_failed_attempts() {
    let fixture = Fixture::new(16384, 8192, 8);
    let sql = "SELECT k,COUNT(*),SUM(v),MIN(s),COUNT(DISTINCT s) FROM t GROUP BY k ORDER BY k NULLS FIRST LIMIT 29";
    for optimized in [false, true] {
        let options = fixture.options(4 << 20, 128, optimized);
        let actual = fixture.check(sql, &options);
        if !optimized {
            assert!(actual.metrics.external_aggregate_repartitions > 0);
            assert!(actual.metrics.spill.bytes_written > options.memory_bytes as u64);
        }
        let plain = Options {
            spill: None,
            ..options
        };
        assert!(
            execute(&prepare(sql, &fixture.tables, &plain).unwrap(), &plain)
                .err()
                .unwrap()
                .contains("resource-limit")
        );
    }
}

#[test]
fn high_cardinality_in_memory_output_streams_into_downstream_aggregation() {
    let fixture = Fixture::new(32768, 16384, 8);
    let footer_bytes = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(&fixture.tables["t"]).unwrap(),
    )
    .unwrap()
    .metadata()
    .memory_size();
    let sql = "SELECT COUNT(*) AS groups,SUM(n) AS rows,SUM(total) AS total FROM (SELECT k,COUNT(*) AS n,SUM(v) AS total FROM t GROUP BY k) g";
    let control = Options {
        memory_bytes: 256 << 20,
        relational_optimizations: false,
        parquet_cache_bytes: None,
        ..Default::default()
    };
    let expected = execute(&prepare(sql, &fixture.tables, &control).unwrap(), &control).unwrap();
    for batch_size in [17, 128] {
        for workers in [1, 4, 10] {
            let options = Options {
                // Keep the original working allowance, while also funding the
                // footer and helper lifetimes that used to be unaccounted.
                memory_bytes: (8 << 20) + footer_bytes + (workers - 1) * 512 * 1024,
                batch_size,
                workers,
                parquet_cache_bytes: None,
                ..Default::default()
            };
            let actual =
                execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
            assert_eq!(actual.batch, expected.batch);
            assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
            assert_eq!(actual.metrics.spill.files_created, 0);
        }
    }
}

#[test]
fn large_scalar_distinct_masks_duplicates_and_restores_checked_sum_order() {
    let fixture = Fixture::new(8192, 1, 1024);
    for batch in [32, 128] {
        for optimized in [false, true] {
            let options = fixture.options(4 << 20, batch, optimized);
            for sql in [
                "SELECT COUNT(*),COUNT(DISTINCT s),COUNT(DISTINCT v),SUM(DISTINCT v),AVG(DISTINCT v),SUM(d),SUM(DISTINCT d) FROM t",
                "SELECT f,COUNT(*),COUNT(DISTINCT s),SUM(DISTINCT v) FROM t GROUP BY f ORDER BY f NULLS FIRST",
            ] {
                let actual = fixture.check(sql, &options);
                assert!(actual.metrics.external_aggregate_distinct_passes > 0);
                assert!(actual.metrics.spill.bytes_written > options.memory_bytes as u64);
            }
        }
    }
}

#[test]
fn partitioned_aggregation_preserves_errors_and_cleans_disk_quota_failure() {
    let fixture = Fixture::new(257, 3, 16);
    let mut options = fixture.options(4 << 20, 32, true);
    for sql in [
        "SELECT k,SUM(d) FROM t GROUP BY k",
        "SELECT COUNT(DISTINCT s),SUM(d+d) FROM t",
    ] {
        for spill in [false, true] {
            let mut chosen = options.clone();
            if !spill {
                chosen.spill = None;
            }
            let error = execute(&prepare(sql, &fixture.tables, &chosen).unwrap(), &chosen)
                .err()
                .unwrap();
            assert!(error.contains("overflow"), "{error}");
            fixture.clean();
        }
    }
    options.spill.as_mut().unwrap().disk_quota_bytes = 128;
    let sql = "SELECT k,COUNT(*) FROM t GROUP BY k";
    let error = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options)
        .err()
        .unwrap();
    assert!(
        error.starts_with("resource-limit:") && error.contains("spill disk quota"),
        "{error}"
    );
    fixture.clean();
}
