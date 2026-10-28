//! Demand pruning must preserve errors in groups absent from the probe.
use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

struct Fixture(PathBuf);

impl Fixture {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("demand-errors-{}-{name}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }

    fn overflow(probes: i64) -> Self {
        let fixture = Self::new(&probes.to_string());
        fixture.write("p", (0..probes).collect(), None);
        // Input A has exactly 1,114,129 ordinary keys, then ten overflowing
        // values in a group that cannot match the probe. The small variant
        // also passed the old 65,536-member admission cap.
        let rows = probes * 17;
        fixture.write(
            "r",
            (0..rows).chain(std::iter::repeat_n(-1, 10)).collect(),
            Some(
                std::iter::repeat_n(0, rows as usize)
                    .chain(std::iter::repeat_n(10_i128.pow(37), 10))
                    .collect(),
            ),
        );
        fixture
    }

    fn write(&self, name: &str, keys: Vec<i64>, decimals: Option<Vec<i128>>) {
        let mut fields = vec![Field::new("k", DataType::Int64, false)];
        let mut arrays: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
        if let Some(values) = decimals {
            fields.push(Field::new("d", DataType::Decimal128(38, 0), false));
            arrays.push(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        self.write_batch(name, &batch, 65_539);
    }

    fn write_batch(&self, name: &str, batch: &RecordBatch, row_group: usize) {
        let mut writer = ArrowWriter::try_new(
            File::create_new(self.0.join(format!("{name}.parquet"))).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn tables(&self) -> HashMap<String, PathBuf> {
        ["p", "r"]
            .map(|name| (name.into(), self.0.join(format!("{name}.parquet"))))
            .into()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn overflow_parity(probes: i64) {
    let fixture = Fixture::overflow(probes);
    let sql = "SELECT p.k, a.s FROM p LEFT JOIN \
               (SELECT k, SUM(d) AS s FROM r GROUP BY k) a \
               ON p.k = a.k ORDER BY 1";
    let mut mismatches = Vec::new();
    for workers in [1, 2, 4, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                memory_bytes: 2 << 30,
                spill: None,
                relational_optimizations,
                ..Options::default()
            };
            let result =
                prepare(sql, &fixture.tables(), &options).and_then(|p| execute(&p, &options));
            let error = result.err();
            if error.as_deref() != Some("decimal precision 38 overflow") {
                mismatches.push((workers, relational_optimizations, error));
            }
        }
    }
    assert!(mismatches.is_empty(), "probes={probes}: {mismatches:?}");
}

#[test]
fn demand_preserves_overflow_above_old_key_cap() {
    overflow_parity(65_537);
}

#[test]
fn demand_preserves_overflow_below_old_key_cap() {
    overflow_parity(4);
}

fn edge_fixture(name: &str, empty_probe: bool, empty_build: bool) -> Fixture {
    let fixture = Fixture::new(name);
    let probe = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![
            Some(2),
            Some(1),
            None,
            Some(1),
            Some(-1),
        ]))],
    )
    .unwrap();
    fixture.write_batch("p", &probe.slice(0, if empty_probe { 0 } else { 5 }), 2);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new("d", DataType::Decimal128(15, 2), true),
        Field::new("v", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let keys = (0..513)
        .map(|i| (i % 19 != 0).then_some(i % 128))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(keys.clone())),
        Arc::new(
            Decimal128Array::from_iter(
                keys.iter()
                    .enumerate()
                    .map(|(i, k)| (k != &Some(2)).then_some((i % 9) as i128 - 4)),
            )
            .with_precision_and_scale(15, 2)
            .unwrap(),
        ),
        Arc::new(Int64Array::from(vec![i64::MAX; keys.len()])),
        Arc::new(Float64Array::from_iter_values(
            (0..keys.len()).map(|i| if i % 2 == 0 { -0.0 } else { 0.0 }),
        )),
    ];
    let build = RecordBatch::try_new(schema, arrays).unwrap();
    fixture.write_batch("r", &build.slice(0, if empty_build { 0 } else { 513 }), 17);
    fixture
}

fn assert_bits(actual: &RecordBatch, expected: &RecordBatch, sql: &str) {
    assert_eq!(actual, expected, "{sql}");
    for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
        if let Some(actual) = actual.as_any().downcast_ref::<Float64Array>() {
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..actual.len() {
                if !actual.is_null(row) {
                    assert_eq!(
                        actual.value(row).to_bits(),
                        expected.value(row).to_bits(),
                        "{sql}"
                    );
                }
            }
        }
    }
}

#[test]
fn safe_demand_preserves_nulls_duplicates_empty_sides_and_batch_boundaries() {
    for (name, empty_probe, empty_build) in [
        ("edges", false, false),
        ("empty-probe", true, false),
        ("empty-build", false, true),
    ] {
        let fixture = edge_fixture(name, empty_probe, empty_build);
        for (sql, demand) in [
            (
                "SELECT p.k,a.s,a.c,a.lo,a.hi,a.av FROM p LEFT JOIN \
             (SELECT k,SUM(d) s,COUNT(d) c,MIN(d) lo,MAX(d) hi,AVG(d) av FROM r GROUP BY k) a \
             ON p.k=a.k ORDER BY 1,2",
                1,
            ),
            (
                "SELECT p.k,a.s,a.c,a.lo,a.hi FROM p LEFT JOIN \
             (SELECT k,SUM(DISTINCT d) s,COUNT(DISTINCT d) c,MIN(f) lo,MAX(f) hi FROM r GROUP BY k) a \
             ON p.k=a.k ORDER BY 1,2",
                1,
            ),
            // The general bounded product used by a correlated SUM projection
            // (including q20) remains eligible with same-type equality keys.
            (
                "SELECT p.k,a.s FROM p LEFT JOIN \
             (SELECT k,0.5*SUM(d) s FROM r WHERE k>=0 GROUP BY k) a \
             ON p.k=a.k ORDER BY 1,2",
                1,
            ),
            // Mixed Int/Decimal equality is a residual, not a hash key. Its
            // existing fallback must keep identical values and multiplicity.
            (
                "SELECT p.k,a.s FROM p LEFT JOIN \
             (SELECT k,0.5*SUM(d) s FROM r WHERE k>=0 GROUP BY k) a \
             ON p.k*1.0=a.k ORDER BY 1,2",
                0,
            ),
            // HAVING with mixed numeric scales uses the same safe range proof.
            (
                "SELECT p.k,a.s FROM p LEFT JOIN \
             (SELECT k,SUM(d) s FROM r GROUP BY k HAVING SUM(d)>-300) a \
             ON p.k=a.k ORDER BY 1,2",
                1,
            ),
        ] {
            let serial = Options {
                relational_optimizations: false,
                ..Options::default()
            };
            let expected =
                execute(&prepare(sql, &fixture.tables(), &serial).unwrap(), &serial).unwrap();
            assert_eq!(expected.batch.num_rows(), if empty_probe { 0 } else { 5 });
            let keys = expected
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..keys.len() {
                if empty_build || keys.is_null(row) || matches!(keys.value(row), -1 | 2) {
                    assert!(expected.batch.column(1).is_null(row), "{sql}");
                }
            }
            for workers in [1, 2, 4, 10] {
                for batch_size in [3, 37, 8192] {
                    for relational_optimizations in [false, true] {
                        let options = Options {
                            workers,
                            batch_size,
                            relational_optimizations,
                            ..Options::default()
                        };
                        let actual = execute(
                            &prepare(sql, &fixture.tables(), &options).unwrap(),
                            &options,
                        )
                        .unwrap();
                        assert_bits(&actual.batch, &expected.batch, sql);
                        if relational_optimizations && !empty_probe && !empty_build {
                            assert_eq!(actual.metrics.demand_filtered_aggregates, demand, "{sql}");
                        }
                        assert_eq!(actual.metrics.auto_spill_retries, 0);
                    }
                }
            }
        }
    }
}

#[test]
fn demand_declines_fallible_expressions_and_preserves_first_error() {
    let fixture = edge_fixture("fallible", false, false);
    for sql in [
        // A direct aggregate and an unsafe aggregate must decline together.
        "SELECT p.k,a.s,a.c FROM p LEFT JOIN \
         (SELECT k,SUM(v*v) s,COUNT(*) c FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1",
        // Errors in other grouping keys, input filters, HAVING and aggregate
        // projections must not disappear along with undemanded groups.
        "SELECT p.k,a.c FROM p LEFT JOIN \
         (SELECT k,COUNT(*) c FROM r GROUP BY k,v*v*v) a ON p.k=a.k ORDER BY 1",
        "SELECT p.k,a.c FROM p LEFT JOIN \
         (SELECT k,COUNT(*) c FROM r WHERE v*v*v>0 GROUP BY k) a ON p.k=a.k ORDER BY 1",
        "SELECT p.k,a.s FROM p LEFT JOIN \
         (SELECT k,MAX(v)*MAX(v)*MAX(v) s FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1",
        "SELECT p.k,a.c FROM p LEFT JOIN \
         (SELECT k,COUNT(*) c FROM r GROUP BY k HAVING MAX(v)*MAX(v)*MAX(v)>0) a \
         ON p.k=a.k ORDER BY 1",
        // The RHS is evaluated first by the serial join; the probe's different
        // failure must not take precedence through optional pre-evaluation.
        "SELECT p.k,a.s FROM (SELECT k FROM p WHERE CAST(1 AS DECIMAL(38,0))/0>0) p LEFT JOIN \
         (SELECT k,SUM(v*v*v) s FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1",
        // Float SUM/AVG now execute, but arbitrary Float columns cannot prove
        // finite-prefix totality, so demand pruning must still decline.
        "SELECT p.k,a.s FROM p LEFT JOIN \
         (SELECT k,SUM(f) s FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1",
        "SELECT p.k,a.s FROM p LEFT JOIN \
         (SELECT k,AVG(f) s FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1",
        // Admitting safe inner joins must still inspect both child predicates
        // and residual expressions whose errors pruning could otherwise hide.
        "SELECT p.k,a.lo FROM p LEFT JOIN \
         (SELECT r.k,MIN(r.d) lo FROM r JOIN r s ON r.k=s.k AND r.v*s.v*s.v>0 \
          GROUP BY r.k) a ON p.k=a.k ORDER BY 1",
        "SELECT p.k,a.lo FROM p LEFT JOIN \
         (SELECT r.k,MIN(r.d) lo FROM r JOIN \
          (SELECT k FROM r WHERE v*v*v>0) s ON r.k=s.k \
          GROUP BY r.k) a ON p.k=a.k ORDER BY 1",
    ] {
        let serial = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        if sql.contains("SUM(f)") || sql.contains("AVG(f)") {
            let expected =
                execute(&prepare(sql, &fixture.tables(), &serial).unwrap(), &serial).unwrap();
            for workers in [1, 2, 4, 10] {
                for relational_optimizations in [false, true] {
                    let options = Options {
                        workers,
                        relational_optimizations,
                        batch_size: 37,
                        ..Options::default()
                    };
                    let actual = execute(
                        &prepare(sql, &fixture.tables(), &options).unwrap(),
                        &options,
                    )
                    .unwrap();
                    assert_bits(&actual.batch, &expected.batch, sql);
                    assert_eq!(actual.metrics.demand_filtered_aggregates, 0, "{sql}");
                }
            }
            continue;
        }
        let expected = prepare(sql, &fixture.tables(), &serial)
            .and_then(|p| execute(&p, &serial))
            .err()
            .expect(sql);
        for workers in [1, 2, 4, 10] {
            for relational_optimizations in [false, true] {
                let options = Options {
                    workers,
                    relational_optimizations,
                    batch_size: 37,
                    ..Options::default()
                };
                let actual = prepare(sql, &fixture.tables(), &options)
                    .and_then(|p| execute(&p, &options))
                    .err()
                    .expect(sql);
                assert_eq!(
                    actual, expected,
                    "workers={workers}, optimization={relational_optimizations}: {sql}"
                );
            }
        }
    }
}

#[test]
fn cancelled_demand_query_matches_serial_and_next_execution_is_clean() {
    let fixture = edge_fixture("cancel", false, false);
    let sql = "SELECT p.k,a.s FROM p LEFT JOIN (SELECT k,SUM(d) s FROM r GROUP BY k) a \
               ON p.k=a.k ORDER BY 1";
    for workers in [1, 2, 4, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                relational_optimizations,
                ..Options::default()
            };
            let prepared = prepare(sql, &fixture.tables(), &options).unwrap();
            options.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "cancelled: query execution"
            );
            let fresh = Options {
                workers,
                relational_optimizations,
                ..Options::default()
            };
            assert_eq!(execute(&prepared, &fresh).unwrap().batch.num_rows(), 5);
        }
    }
}

#[test]
fn demand_does_not_evaluate_fallible_keys_before_having() {
    let fixture = edge_fixture("key-error", false, false);
    let sql = "SELECT p.k,a.s FROM p LEFT JOIN \
               (SELECT k,SUM(d) s FROM r GROUP BY k HAVING k<=1) a \
               ON p.k=a.k*99999999999999999999999999999999999999 ORDER BY 1";
    let serial = Options {
        relational_optimizations: false,
        ..Options::default()
    };
    let expected = execute(&prepare(sql, &fixture.tables(), &serial).unwrap(), &serial).unwrap();
    assert_eq!(expected.batch.num_rows(), 5);
    assert_eq!(expected.batch.column(1).null_count(), 5);
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 37,
            ..Options::default()
        };
        let actual = execute(
            &prepare(sql, &fixture.tables(), &options).unwrap(),
            &options,
        )
        .unwrap();
        assert_bits(&actual.batch, &expected.batch, sql);
        assert_eq!(actual.metrics.demand_filtered_aggregates, 0);
    }
}

#[test]
fn demand_retains_safe_joined_extremes_and_bounded_average_projections() {
    let fixture = edge_fixture("safe-joined-average", false, false);
    for sql in [
        // Inner joins can duplicate an extreme's inputs without making its
        // arithmetic fallible; the physical Cartesian product bounds counts.
        "SELECT p.k,a.lo,a.hi FROM p LEFT JOIN \
         (SELECT r.k,MIN(r.d) lo,MAX(s.d) hi FROM r JOIN r s ON r.k=s.k \
          WHERE s.k>=0 GROUP BY r.k) a ON p.k=a.k ORDER BY 1,2",
        // AVG of exact values is finalized once, then the ordinary Float
        // expression runs unchanged. Its finite bound permits demand pruning.
        "SELECT p.k,a.av FROM p LEFT JOIN \
         (SELECT k,0.2*AVG(d) av FROM r GROUP BY k) a ON p.k=a.k ORDER BY 1,2",
    ] {
        let serial = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let expected =
            execute(&prepare(sql, &fixture.tables(), &serial).unwrap(), &serial).unwrap();
        assert_eq!(expected.batch.num_rows(), 5);
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 37,
                ..Options::default()
            };
            let actual = execute(
                &prepare(sql, &fixture.tables(), &options).unwrap(),
                &options,
            )
            .unwrap();
            assert_bits(&actual.batch, &expected.batch, sql);
            assert_eq!(actual.metrics.demand_filtered_aggregates, 1, "{sql}");
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
    }
}

// R-03: a streaming one-group join must not retain its entire probe merely
// to discover that the optional demand filter is unselective.
fn streaming_resource_parity(
    name: &str,
    rows: usize,
    batch_size: usize,
    memory_bytes: usize,
    build_keys: usize,
) {
    let fixture = Fixture::new(name);
    for (name, with_value) in [("p", true), ("r", false)] {
        let mut fields = vec![Field::new("k", DataType::Int64, false)];
        let keys = (0..batch_size)
            .map(|i| {
                if with_value {
                    0
                } else {
                    (i % build_keys) as i64
                }
            })
            .collect::<Vec<_>>();
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
        if with_value {
            fields.push(Field::new("v", DataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(vec![1; batch_size])));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(fixture.0.join(format!("{name}.parquet"))).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(batch_size)
                    .set_dictionary_enabled(false)
                    .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                    .build(),
            ),
        )
        .unwrap();
        for start in (0..rows).step_by(batch_size) {
            writer
                .write(&batch.slice(0, batch_size.min(rows - start)))
                .unwrap();
        }
        writer.close().unwrap();
    }
    let sql = "SELECT SUM(p.v), MAX(a.c) FROM p LEFT JOIN \
               (SELECT k, COUNT(*) AS c FROM r GROUP BY k) a ON p.k=a.k";
    for workers in [1, 2, 4, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                batch_size,
                memory_bytes,
                relational_optimizations,
                parquet_cache_bytes: None,
                spill: None,
                ..Options::default()
            };
            let result = prepare(sql, &fixture.tables(), &options)
                .and_then(|plan| execute(&plan, &options))
                .unwrap_or_else(|error| {
                    panic!("workers={workers}, optimized={relational_optimizations}: {error}")
                });
            assert_eq!(result.batch.num_rows(), 1);
            assert_eq!(result.batch.num_columns(), 2);
            // Handwritten: rows copies of v=1, all matching key 0; each
            // build key occurs exactly rows/build_keys times.
            for (column, expected) in result.batch.columns().iter().zip([rows, rows / build_keys]) {
                let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    vec![Some(expected as i64)]
                );
            }
            assert_eq!(result.metrics.auto_spill_retries, 0);
        }
    }
}

#[test]
fn optional_demand_streaming_resource_parity_scaled() {
    streaming_resource_parity("streaming-scaled", 262_144, 128, 2 << 20, 1);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "full 16M-row reproducer runs in the release gate"
)]
fn optional_demand_streaming_resource_parity_full() {
    streaming_resource_parity("streaming-full", 16_777_216, 8192, 64 << 20, 1);
}

#[test]
fn selective_demand_streams_a_probe_larger_than_the_budget() {
    streaming_resource_parity("streaming-selective", 262_144, 128, 2 << 20, 128);
}
