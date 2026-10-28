use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
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
struct Fixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new(left: usize, right: usize, domain: usize, width: usize) -> Self {
        Self::with_text_domain(left, right, domain, width, 23)
    }
    fn with_text_domain(
        left: usize,
        right: usize,
        domain: usize,
        width: usize,
        text_domain: usize,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "assaydb-join-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("spill")).unwrap();
        let mut tables = HashMap::new();
        for (name, count) in [("a", left), ("b", right)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ]));
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values((0..count).map(|i| i as i64))),
                Arc::new(Int64Array::from_iter(
                    (0..count).map(|i| (i % 17 != 0).then_some((i % domain) as i64)),
                )),
                Arc::new(Int64Array::from_iter(
                    (0..count).map(|i| (i % 13 != 0).then_some((i % 79) as i64 - 37)),
                )),
                Arc::new(StringArray::from_iter((0..count).map(|i| {
                    (i % 11 != 0)
                        .then(|| format!("{:03}:é🦀\0{}", i % text_domain, "w".repeat(width)))
                }))),
            ];
            let path = root.join(format!("{name}.parquet"));
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
            tables.insert(name.into(), path);
        }
        Self { root, tables }
    }
    fn options(&self, batch: usize, memory: usize, optimized: bool) -> Options {
        Options {
            batch_size: batch,
            memory_bytes: memory,
            relational_optimizations: optimized,
            parquet_cache_bytes: None,
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
            spill: None,
            memory_bytes: 256 << 20,
            ..options.clone()
        };
        let expected = execute(&prepare(sql, &self.tables, &plain).unwrap(), &plain).unwrap();
        let actual = execute(&prepare(sql, &self.tables, options).unwrap(), options).unwrap();
        assert_eq!(actual.batch, expected.batch, "{sql}");
        assert!(actual.metrics.external_join_partitions > 0, "{sql}");
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
        for path in self.tables.values() {
            fs::remove_file(path).unwrap();
        }
        fs::remove_dir(self.root.join("spill")).unwrap();
        fs::remove_dir(&self.root).unwrap();
    }
}
#[test]
fn automatic_spill_releases_failed_attempt_and_retries_wide_join() {
    let fixture = Fixture::new(513, 16384, 16384, 1024);
    let sql = "SELECT a.id AS l,b.id AS r,b.s AS s FROM a LEFT JOIN b ON a.k=b.k ORDER BY l,r NULLS FIRST LIMIT 31";
    for workers in [1, 2, 4, 8, 10] {
        let mut options = fixture.options(64, 8 << 20, true);
        options.workers = workers;
        options.spill.as_mut().unwrap().mode = assaydb::spill::SpillMode::Auto;
        let actual = fixture.check(sql, &options);
        assert_eq!(actual.metrics.auto_spill_retries, 1);
        assert!(actual.metrics.auto_spill_initial_seconds > 0.0);
        assert!(actual.metrics.execution_seconds >= actual.metrics.auto_spill_initial_seconds);
        assert!(actual.metrics.auto_spill_initial_peak_reserved_bytes > 0);
        assert!(actual.metrics.auto_spill_initial_peak_reserved_bytes <= options.memory_bytes);
        assert!(
            actual
                .metrics
                .auto_spill_initial_error
                .as_ref()
                .unwrap()
                .starts_with("resource-limit: accounted memory")
        );
        assert!(actual.metrics.worker_threads_peak <= workers);
    }
    let mut options = fixture.options(64, 8 << 20, true);
    options.workers = 4;
    let spill = options.spill.as_mut().unwrap();
    spill.mode = assaydb::spill::SpillMode::Auto;
    spill.disk_quota_bytes = 128;
    let error = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options)
        .err()
        .unwrap();
    assert!(error.starts_with("resource-limit:"));
    assert!(
        error.contains("spill disk quota") && error.contains("automatic spill retry"),
        "{error}"
    );
    fixture.clean();
}
#[test]
fn automatic_spill_does_not_write_when_the_query_fits() {
    let fixture = Fixture::new(17, 23, 7, 8);
    let mut options = fixture.options(8, 8 << 20, true);
    let spill = options.spill.as_mut().unwrap();
    spill.mode = assaydb::spill::SpillMode::Auto;
    spill.disk_quota_bytes = 1;
    let sql = "SELECT COUNT(*) AS n FROM a JOIN b ON a.k=b.k";
    let actual = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
    let plain = Options {
        spill: None,
        ..options.clone()
    };
    let expected = execute(&prepare(sql, &fixture.tables, &plain).unwrap(), &plain).unwrap();
    assert_eq!(actual.batch, expected.batch);
    assert_eq!(actual.metrics.auto_spill_retries, 0);
    assert_eq!(actual.metrics.spill.files_created, 0);
    fixture.clean();
}
#[test]
fn spilled_join_modes_preserve_duplicates_nulls_composite_keys_and_residuals() {
    let fixture = Fixture::new(37, 53, 7, 8);
    for batch in [7, 32] {
        for optimized in [false, true] {
            let options = fixture.options(batch, 8 << 20, optimized);
            for sql in [
                "SELECT a.id AS l,b.id AS r FROM a JOIN b ON a.k=b.k ORDER BY l,r",
                "SELECT a.id AS l,b.id AS r,b.s AS s FROM a LEFT JOIN b ON a.k=b.k AND a.v<b.v ORDER BY l,r NULLS FIRST",
                "SELECT a.id AS l FROM a SEMI JOIN b ON a.k=b.k AND a.v<b.v ORDER BY l",
                "SELECT a.id AS l FROM a ANTI JOIN b ON a.k=b.k AND a.v<b.v ORDER BY l",
                "SELECT a.id AS l,b.id AS r FROM a LEFT JOIN b ON a.k=b.k AND a.s=b.s ORDER BY l,r NULLS FIRST",
                "SELECT a.id AS l,b.id AS r FROM a JOIN b ON a.k+1=b.k+1 AND a.v=b.v ORDER BY l,r",
                "SELECT a.id AS l,a.k IN(SELECT k FROM b) AS yes,a.k NOT IN(SELECT k FROM b) AS no FROM a ORDER BY l",
                "SELECT a.id AS l,a.k IN(SELECT k FROM b WHERE FALSE) AS yes,a.k NOT IN(SELECT k FROM b WHERE FALSE) AS no FROM a ORDER BY l",
                "SELECT a.id AS l,a.k IN(SELECT k FROM b WHERE k IS NULL) AS yes FROM a ORDER BY l",
                "SELECT COUNT(*) AS n FROM a,b",
                "SELECT a.id AS l,b.id AS r FROM a JOIN b ON a.v<b.v ORDER BY l,r",
            ] {
                fixture.check(sql, &options);
            }
        }
    }
}
#[test]
fn repartitioned_wide_build_exceeds_budget_and_matches_in_memory() {
    let fixture = Fixture::new(513, 16384, 16384, 1024);
    let options = fixture.options(64, 4 << 20, true);
    let sql = "SELECT a.id AS l,b.id AS r,b.s AS s FROM a LEFT JOIN b ON a.k=b.k ORDER BY l,r NULLS FIRST LIMIT 31";
    let actual = fixture.check(sql, &options);
    assert!(actual.metrics.external_join_repartitions > 0);
    assert!(actual.metrics.spill.bytes_written > options.memory_bytes as u64);
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

#[test]
fn repeated_cte_uses_query_owned_spilled_materialization() {
    let fixture = Fixture::new(129, 8192, 8192, 1024);
    let options = fixture.options(64, 4 << 20, true);
    let sql = "WITH x AS (SELECT id,k,s FROM b) SELECT a.id AS l,x.id AS r,x.s AS s,(SELECT COUNT(*) FROM x) AS n FROM a LEFT JOIN x ON a.k=x.k ORDER BY l,r NULLS FIRST LIMIT 17";
    let actual = fixture.check(sql, &options);
    assert!(actual.metrics.spilled_materializations > 0);
    assert!(actual.metrics.materialization_replay_batches > 0);
    let again = fixture.check(sql, &options);
    assert_eq!(again.batch, actual.batch);
}

#[test]
fn paired_wide_mark_joins_do_not_retain_unused_right_payload() {
    let fixture = Fixture::with_text_domain(513, 16384, 4096, 1024, 4096);
    let footer_bytes: usize = fixture
        .tables
        .values()
        .map(|path| {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .metadata()
            .memory_size()
        })
        .sum();
    for optimized in [false, true] {
        // Preserve the original 4 MiB operator allowance, plus the footers
        // newly charged while the two mark-join scans overlap.
        let options = fixture.options(128, (4 << 20) + footer_bytes, optimized);
        fixture.check("SELECT a.id AS l,a.s IN(SELECT s FROM b) AS yes,a.s NOT IN(SELECT s FROM b) AS no FROM a ORDER BY l", &options);
    }
}
#[test]
fn hot_key_block_fallback_retains_outer_and_existence_match_state() {
    let fixture = Fixture::new(19, 8192, 1, 1024);
    for optimized in [false, true] {
        let options = fixture.options(64, 4 << 20, optimized);
        for kind in ["LEFT", "SEMI", "ANTI"] {
            let sql = if kind == "LEFT" {
                "SELECT a.id AS l,COUNT(b.id) AS n FROM a LEFT JOIN b ON a.k=b.k AND a.s<b.s GROUP BY a.id ORDER BY l".into()
            } else {
                format!("SELECT a.id AS l FROM a {kind} JOIN b ON a.k=b.k AND a.s<b.s ORDER BY l")
            };
            let actual = fixture.check(&sql, &options);
            assert!(actual.metrics.external_join_block_fallbacks > 0);
        }
    }
}
#[test]
fn empty_inputs_and_disk_quota_cleanup() {
    for (left, right) in [(0, 0), (7, 0), (0, 7)] {
        let fixture = Fixture::new(left, right, 3, 0);
        let options = fixture.options(3, 4 << 20, true);
        for sql in [
            "SELECT a.id AS l,b.id AS r FROM a LEFT JOIN b ON a.k=b.k ORDER BY l,r",
            "SELECT a.id AS l FROM a ANTI JOIN b ON a.k=b.k ORDER BY l",
            "SELECT a.id AS l,a.k NOT IN(SELECT k FROM b) AS yes FROM a ORDER BY l",
            "SELECT COUNT(*) AS n FROM a,b",
        ] {
            fixture.check(sql, &options);
        }
    }
    let fixture = Fixture::new(17, 31, 7, 16);
    let mut options = fixture.options(7, 4 << 20, true);
    options.spill.as_mut().unwrap().disk_quota_bytes = 128;
    let sql = "SELECT a.id AS l,b.id AS r FROM a LEFT JOIN b ON a.k=b.k";
    let error = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options)
        .err()
        .unwrap();
    assert!(
        error.starts_with("resource-limit:") && error.contains("spill disk quota"),
        "{error}"
    );
    fixture.clean();
}
