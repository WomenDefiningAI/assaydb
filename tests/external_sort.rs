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

#[test]
fn top_k_matches_full_sort_across_workers_batches_and_completed_groups() {
    let fixture = Fixture::new(257, false);
    for (sql, admitted) in [
        (
            "SELECT id,k,s,d,f FROM t ORDER BY k NULLS FIRST,s DESC,id LIMIT 7",
            true,
        ),
        (
            "SELECT id,k,s FROM t ORDER BY s DESC NULLS LAST,id LIMIT 20",
            true,
        ),
        (
            "SELECT id,CASE WHEN id=0 THEN 1 ELSE 9223372036854775808 END AS v FROM t ORDER BY id LIMIT 1",
            true,
        ),
        (
            "SELECT k,COUNT(*) n FROM t GROUP BY k ORDER BY n DESC,k LIMIT 3",
            true,
        ),
        ("SELECT id,k FROM t ORDER BY k LIMIT 7", false),
        ("SELECT id,NULL n FROM t ORDER BY n LIMIT 7", false),
        ("SELECT id FROM t WHERE id<0 ORDER BY id LIMIT 7", false),
        ("SELECT id FROM t ORDER BY id LIMIT 0", false),
    ] {
        let serial = Options {
            relational_optimizations: false,
            batch_size: 1,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &fixture.tables, &serial).unwrap(), &serial).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let result =
                    execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options).unwrap();
                assert_eq!(result.batch, expected.batch, "{sql}/{workers}/{batch_size}");
                assert_eq!(
                    result.metrics.top_k_sorts > 0,
                    admitted,
                    "{sql}/{workers}/{batch_size}"
                );
            }
        }
    }
}

#[test]
fn top_k_never_hides_late_projection_or_group_errors() {
    let fixture = Fixture::new(257, false);
    for sql in [
        "SELECT id,bad FROM (SELECT id,CAST('1e308' AS REAL)*CAST(id+2 AS REAL) bad FROM t) q ORDER BY id LIMIT 0",
        "SELECT id,10000000000000000000000000000000000000 * id AS bad FROM t ORDER BY id LIMIT 1",
        "SELECT k,SUM(10000000000000000000000000000000000000 * id) AS bad FROM t GROUP BY k ORDER BY k LIMIT 1",
    ] {
        let serial = Options {
            relational_optimizations: false,
            batch_size: 1,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &fixture.tables, &serial).unwrap(), &serial)
            .err()
            .unwrap();
        assert!(expected.contains("overflow"), "{expected}");
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let error = execute(&prepare(sql, &fixture.tables, &options).unwrap(), &options)
                    .err()
                    .unwrap();
                assert_eq!(error, expected, "{sql}/{workers}/{batch_size}");
            }
        }
    }
}
#[test]
fn top_k_complete_key_ties_and_large_limits_match_the_full_sort_prefix() {
    let fixture = Fixture::new(257, false);
    for order in ["k NULLS FIRST", "s DESC NULLS LAST", "k,s", "n"] {
        let full_sql = format!("SELECT id,k,s,NULL n FROM t ORDER BY {order}");
        let serial = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        let full = execute(
            &prepare(&full_sql, &fixture.tables, &serial).unwrap(),
            &serial,
        )
        .unwrap();
        assert_eq!(full.metrics.top_k_sorts, 0);
        for limit in [7, 257, 300] {
            let sql = format!("{full_sql} LIMIT {limit}");
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let actual =
                        execute(&prepare(&sql, &fixture.tables, &options).unwrap(), &options)
                            .unwrap();
                    assert_eq!(
                        actual.batch,
                        full.batch.slice(0, limit.min(257)),
                        "{sql}/{workers}/{batch_size}"
                    );
                    assert_eq!(
                        actual.metrics.top_k_sorts, 0,
                        "{sql}/{workers}/{batch_size}"
                    );
                }
            }
        }
    }
}

#[test]
fn top_k_sort_key_errors_are_row_major_and_limit_zero_consumes_input() {
    let fixture = Fixture::new(257, false);
    // Row zero, second key fails before row one's first key. Reversing
    // expression slots on row zero also distinguishes the two error causes.
    for (order, expected) in [
        (
            "CASE WHEN id=1 THEN CAST('1e308' AS REAL)*CAST(id+1 AS REAL) ELSE 1.0 END, CASE WHEN id=0 THEN 60000000000000000000000000000000000000*(id+2) ELSE 1 END",
            "decimal precision 38 overflow",
        ),
        (
            "CAST('1e308' AS REAL)*CAST(id+2 AS REAL), 60000000000000000000000000000000000000*(id+2)",
            "floating arithmetic overflow",
        ),
        (
            "60000000000000000000000000000000000000*(id+2), CAST('1e308' AS REAL)*CAST(id+2 AS REAL)",
            "decimal precision 38 overflow",
        ),
    ] {
        let full_sql = format!("SELECT id,{order} FROM t ORDER BY 2,3");
        let serial = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        assert_eq!(
            execute(
                &prepare(&full_sql, &fixture.tables, &serial).unwrap(),
                &serial
            )
            .err()
            .unwrap(),
            expected
        );
        for limit in [0, 1, 300] {
            let sql = format!("{full_sql} LIMIT {limit}");
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let actual =
                        execute(&prepare(&sql, &fixture.tables, &options).unwrap(), &options)
                            .err()
                            .unwrap();
                    assert_eq!(actual, expected, "{sql}/{workers}/{batch_size}");
                }
            }
        }
    }
}

#[test]
fn top_k_offset_slices_completed_order_even_for_zero_offset() {
    let fixture = Fixture::new(257, false);
    for offset in [0, 1, 300] {
        let sql = format!("SELECT id FROM t ORDER BY id LIMIT 7 OFFSET {offset}");
        for relational_optimizations in [false, true] {
            let options = Options {
                relational_optimizations,
                ..Options::default()
            };
            let result =
                execute(&prepare(&sql, &fixture.tables, &options).unwrap(), &options).unwrap();
            let actual = result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let expected = (offset..257).take(7).map(|i| i as i64).collect::<Vec<_>>();
            assert_eq!(actual.values().as_ref(), expected.as_slice());
        }
    }
}

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
