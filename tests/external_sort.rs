use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::{Cancellation, SpillOptions},
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
    fn new(rows: usize, wide: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "assaydb-external-sort-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("spill")).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(18, 2), true),
            Field::new("f", DataType::Float64, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64))),
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 11 != 0).then_some(((i * 37) % 31) as i64 - 15)),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 13 != 0).then(|| {
                    let s = ["", "é🦀", "a\0b", "Ω", "z"][i % 5];
                    if wide {
                        format!("{}{:04}{}", s, i % 79, "w".repeat(1024))
                    } else {
                        s.into()
                    }
                })
            }))),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i128 * 113 - 7000)),
                )
                .with_precision_and_scale(18, 2)
                .unwrap(),
            ),
            Arc::new(Float64Array::from_iter(
                (0..rows).map(|i| (i % 17 != 0).then_some((i as f64 - 500.) / 3.)),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let path = root.join("t.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(256)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self {
            root,
            tables: HashMap::from([("t".into(), path)]),
        }
    }
    fn spilled(&self, batch_size: usize, fan: usize) -> Options {
        Options {
            batch_size,
            memory_bytes: 4 << 20,
            parquet_cache_bytes: None,
            spill: Some(SpillOptions {
                mode: Default::default(),
                directory: self.root.join("spill"),
                disk_quota_bytes: 64 << 20,
                sort_run_bytes: 1,
                merge_fan_in: fan,
            }),
            ..Options::default()
        }
    }
    fn clean(&self) {
        assert_eq!(fs::read_dir(self.root.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(self.root.join("t.parquet")).unwrap();
        self.clean();
        fs::remove_dir(self.root.join("spill")).unwrap();
        fs::remove_dir(&self.root).unwrap();
    }
}
#[test]
fn sql_external_sort_matches_in_memory_across_types_nulls_and_merge_passes() {
    let fixture = Fixture::new(257, false);
    for (batch, fan) in [(7, 2), (31, 3), (128, 4)] {
        let options = fixture.spilled(batch, fan);
        let plain = Options {
            batch_size: batch,
            parquet_cache_bytes: None,
            ..Options::default()
        };
        for sql in [
            "SELECT id,k,s,d,f FROM t ORDER BY k ASC NULLS LAST,s DESC NULLS FIRST,id",
            "SELECT id,s,d FROM t ORDER BY s ASC NULLS FIRST,d DESC NULLS LAST,id LIMIT 37",
            "SELECT id,f FROM t ORDER BY f DESC NULLS LAST,id",
            "SELECT k,COUNT(*) AS n FROM t GROUP BY k ORDER BY n DESC,k NULLS FIRST",
            "SELECT id FROM t WHERE id < 0 ORDER BY id",
        ] {
            let expected =
                execute(&prepare(sql, &fixture.tables, &plain).unwrap(), &plain).unwrap();
            let result =
                execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
            assert_eq!(result.batch, expected.batch, "{sql}, batch {batch}");
            if result.batch.num_rows() > 0 {
                assert!(result.metrics.external_sort_runs > 0);
                assert!(result.metrics.spill.bytes_written > 0);
                assert!(result.metrics.spill.bytes_read > 0);
            }
            if sql.contains("SELECT id,k") && batch == 7 {
                assert!(result.metrics.external_sort_merge_passes > 1);
            }
            assert_eq!(result.metrics.spill.live_bytes, 0);
            assert_eq!(result.metrics.spill.live_files, 0);
            assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
            fixture.clean();
        }
    }
}
#[test]
fn automatic_sort_spills_only_after_memory_pressure() {
    let fixture = Fixture::new(8192, true);
    let sql = "SELECT id,s FROM t ORDER BY s DESC NULLS FIRST,id LIMIT 17";
    let plain = Options {
        batch_size: 32,
        parquet_cache_bytes: None,
        ..Options::default()
    };
    let expected = execute(&prepare(sql, &fixture.tables, &plain).unwrap(), &plain).unwrap();
    for workers in [1, 4, 10] {
        let mut options = fixture.spilled(32, 2);
        options.memory_bytes = 8 << 20;
        options.workers = workers;
        options.spill.as_mut().unwrap().mode = assaydb::spill::SpillMode::Auto;
        let actual = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.auto_spill_retries, 1);
        assert!(actual.metrics.external_sort_merge_passes > 0);
        assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
        assert_eq!(actual.metrics.spill.live_files, 0);
        fixture.clean();
    }
}
#[test]
fn sort_input_larger_than_budget_with_small_final_result() {
    let fixture = Fixture::new(8192, true);
    let options = fixture.spilled(32, 2);
    let sql = "SELECT id,s FROM t ORDER BY s DESC NULLS FIRST,id LIMIT 17";
    let plain = Options {
        batch_size: 32,
        parquet_cache_bytes: None,
        ..Options::default()
    };
    let expected = execute(&prepare(sql, &fixture.tables, &plain).unwrap(), &plain).unwrap();
    let result = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
    assert_eq!(result.batch, expected.batch);
    assert!(result.metrics.spill.bytes_written > options.memory_bytes as u64);
    assert!(result.metrics.external_sort_runs >= 256);
    assert!(result.metrics.external_sort_merge_passes > 1);
    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
    let limited = Options {
        memory_bytes: options.memory_bytes,
        ..plain
    };
    assert!(
        execute(&prepare(sql, &fixture.tables, &limited).unwrap(), &limited)
            .err()
            .unwrap()
            .contains("resource-limit")
    );
    fixture.clean();
}
#[test]
fn sql_quota_failure_and_cancelled_query_clean_up() {
    let fixture = Fixture::new(97, false);
    let mut options = fixture.spilled(7, 2);
    options.spill.as_mut().unwrap().disk_quota_bytes = 128;
    let sql = "SELECT id,s FROM t ORDER BY s,id";
    let error = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options)
        .err()
        .unwrap();
    assert!(error.contains("spill disk quota"), "{error}");
    assert!(error.starts_with("resource-limit:"), "{error}");
    fixture.clean();
    options.spill.as_mut().unwrap().disk_quota_bytes = 64 << 20;
    let prepared = prepare(sql, &fixture.tables, &options).unwrap();
    let cancel = Cancellation::default();
    options.cancellation = cancel.clone();
    cancel.cancel();
    assert!(
        execute(&prepared, &options)
            .err()
            .unwrap()
            .contains("cancelled")
    );
    fixture.clean();
    options.cancellation = Cancellation::default();
    assert!(execute(&prepared, &options).is_ok());
    fixture.clean();
}
