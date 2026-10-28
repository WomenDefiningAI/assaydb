//! Float aggregates: pinned references and bit-exact canonical-order controls.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
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
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-float-aggregate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::create_dir(directory.join("spill")).unwrap();
        Self(directory)
    }
    fn options(&self, workers: usize, batch_size: usize, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: 32 << 20,
            spill: spill.then(|| SpillOptions {
                directory: self.0.join("spill"),
                sort_run_bytes: 4096,
                merge_fan_in: 2,
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn write(&self, name: &str, batch: RecordBatch, row_group_size: usize) -> PathBuf {
        let path = self.0.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group_size)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }
    fn clean_spill(&self) {
        assert_eq!(fs::read_dir(self.0.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn expected() -> Value {
    serde_json::from_str(include_str!("fixtures/float_aggregate/expected.json")).unwrap()
}

fn rows(batch: &RecordBatch, bits: bool) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
                        let v = array.value(row);
                        if bits {
                            json!(v.to_bits())
                        } else if v.is_nan() {
                            json!("NaN")
                        } else if v.is_infinite() {
                            json!(if v > 0.0 { "+inf" } else { "-inf" })
                        } else {
                            json!(v)
                        }
                    } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                        json!(array.value(row))
                    } else {
                        json!(array_value_to_string(array.as_ref(), row).unwrap())
                    }
                })
                .collect()
        })
        .collect()
}

fn row_matches(actual: &[Value], wanted: &[Value]) -> bool {
    actual.len() == wanted.len()
        && actual.iter().zip(wanted).all(|(a, b)| {
            if a.is_f64() || b.is_f64() {
                a.as_f64()
                    .zip(b.as_f64())
                    .is_some_and(|(a, b)| (a - b).abs() <= 1e-12_f64.max(b.abs() * 1e-12))
            } else {
                a == b
            }
        })
}

fn compare(batch: &RecordBatch, case: &Value) {
    let actual = rows(batch, false);
    let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    assert_eq!(actual.len(), wanted.len(), "{case}");
    // Physical integer/decimal widths differ between the engines. Verify the
    // logical Float64 result explicitly; compare other values without tolerance.
    for (column, ty) in case["types"].as_array().unwrap().iter().enumerate() {
        if ty == "DOUBLE" && case["id"] != "null_untyped" {
            assert_eq!(batch.column(column).data_type(), &DataType::Float64);
        }
    }
    if case["ordered"].as_bool().unwrap_or(true) {
        assert!(
            actual.iter().zip(&wanted).all(|(a, b)| row_matches(a, b)),
            "{actual:?} != {wanted:?}: {case}"
        );
    } else {
        for row in actual {
            let index = wanted
                .iter()
                .position(|w| row_matches(&row, w))
                .unwrap_or_else(|| panic!("missing {row:?} in {wanted:?}: {case}"));
            wanted.remove(index);
        }
        assert!(wanted.is_empty());
    }
}

#[test]
fn every_inline_duckdb_case_including_explicit_refusals() {
    let reference = expected();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 52);
    let sources = include_str!("fixtures/float_aggregate/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(case["id"], metadata["id"]);
        assert_eq!(case["sql"], sql.trim());
        assert_eq!(case["ordered"], metadata["ordered"]);
        assert_eq!(case["refusal"], metadata["refusal"]);
    }
    let fixture = Fixture::new();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = fixture.options(workers, batch_size, spill);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let result = prepare(sql, &HashMap::new(), &options)
                        .and_then(|plan| execute(&plan, &options));
                    if case["id"] == "window" {
                        // Window-frame card: use the already frozen DuckDB 1.5
                        // answer; retain the historical refusal in the fixture.
                        assert_eq!(case["refusal"], "unsupported: Float/Double sum OVER");
                        assert_eq!(case["status"], "ok");
                        compare(&result.unwrap_or_else(|e| panic!("{sql}: {e}")).batch, case);
                    } else if let Some(refusal) = case["refusal"].as_str() {
                        assert_eq!(result.err().as_deref(), Some(refusal), "{sql}");
                    } else {
                        assert_eq!(case["status"], "ok");
                        compare(&result.unwrap_or_else(|e| panic!("{sql}: {e}")).batch, case);
                    }
                    fixture.clean_spill();
                }
            }
        }
    }
}

#[test]
fn nonfinite_subnormal_zero_and_overflow_reference_probes() {
    let reference = expected();
    let fixture = Fixture::new();
    let cases = reference["boundaries"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    for case in cases {
        let values = case["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                let value = v.as_str().unwrap();
                (value != "NULL").then(|| value.parse::<f64>().unwrap())
            })
            .collect::<Vec<_>>();
        let keys = Arc::new(Int64Array::from(vec![0; values.len()]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("k", DataType::Int64, false),
            ])),
            vec![Arc::new(Float64Array::from(values)), keys],
        )
        .unwrap();
        let path = fixture.write(case["id"].as_str().unwrap(), batch, 1);
        let tables = HashMap::from([("t".into(), path)]);
        let mut bits = None;
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for spill in [false, true] {
                    let options = fixture.options(workers, batch_size, spill);
                    for sql in [
                        "SELECT SUM(x),AVG(x),COUNT(x) FROM t",
                        "SELECT SUM(x),AVG(x),COUNT(x) FROM t GROUP BY k",
                    ] {
                        let plan = prepare(sql, &tables, &options).unwrap();
                        let result = execute(&plan, &options);
                        if let Some(refusal) = case["refusal"].as_str() {
                            assert_eq!(result.err().as_deref(), Some(refusal), "{case}");
                        } else {
                            let result = result.unwrap();
                            compare(&result.batch, case);
                            let actual = rows(&result.batch, true);
                            if let Some(expected) = &bits {
                                assert_eq!(&actual, expected, "{case}");
                            } else {
                                bits = Some(actual);
                            }
                            if spill && sql.contains("GROUP BY") {
                                assert!(result.metrics.external_aggregate_partitions > 0);
                            }
                        }
                        fixture.clean_spill();
                    }
                }
            }
        }
    }
}

#[test]
fn adversarial_input_is_bit_identical_across_workers_batches_spill_and_group_paths() {
    let fixture = Fixture::new();
    // Each group's canonical stream is [1e16, 1, -1e16, 3, NULL, -0].
    // Plain addition gives exactly 3, not the compensated/exact sum of 4.
    // Three interleaved groups straddle row-group and every tested batch boundary.
    let values = [
        Some(1e16),
        Some(1.0),
        Some(-1e16),
        Some(3.0),
        None,
        Some(-0.0),
    ];
    let array = (0..18).map(|row| values[row / 3]).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("x", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values((0..18).map(|row| row % 3))),
            Arc::new(StringArray::from_iter_values(
                (0..18).map(|row| ["a", "b", "c"][row % 3]),
            )),
            Arc::new(Float64Array::from(array)),
        ],
    )
    .unwrap();
    let path = fixture.write("adversarial", batch, 2);
    let tables = HashMap::from([("t".into(), path)]);
    for optimized in [false, true] {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for spill in [false, true] {
                    let mut options = fixture.options(workers, batch_size, spill);
                    options.relational_optimizations = optimized;
                    for (sql, total, count, groups) in [
                        (
                            "SELECT SUM(x),AVG(x),COUNT(x),SUM(x) FROM t",
                            9.0_f64,
                            15,
                            1,
                        ),
                        (
                            "SELECT SUM(x),AVG(x),COUNT(x),SUM(x) FROM t GROUP BY k",
                            3.0,
                            5,
                            3,
                        ),
                        (
                            "SELECT SUM(x),AVG(x),COUNT(x),SUM(x) FROM t GROUP BY s",
                            3.0,
                            5,
                            3,
                        ),
                        (
                            "SELECT SUM(x),AVG(x),COUNT(x),SUM(x) FROM t WHERE k=1",
                            3.0,
                            5,
                            1,
                        ),
                        (
                            "SELECT SUM(x),AVG(x),COUNT(x),SUM(x) FROM t GROUP BY k HAVING SUM(x)>0",
                            3.0,
                            5,
                            3,
                        ),
                    ] {
                        let result =
                            execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                        assert_eq!(
                            rows(&result.batch, true),
                            vec![
                                vec![
                                    json!(total.to_bits()),
                                    json!((total / count as f64).to_bits()),
                                    json!(count),
                                    json!(total.to_bits())
                                ];
                                groups
                            ],
                            "{sql} workers={workers} batch={batch_size} spill={spill} optimized={optimized}"
                        );
                        assert_eq!(
                            result.metrics.pipeline_producers, 0,
                            "Float partial reduction must not run"
                        );
                        if spill && sql.contains("GROUP BY") {
                            assert!(
                                result.metrics.external_aggregate_partitions > 0,
                                "{sql}: spill must be exercised"
                            );
                        }
                        fixture.clean_spill();
                    }
                }
            }
        }
    }
}

#[test]
fn constant_null_distinct_preserves_groups_counts_types_and_spill() {
    let fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let path = fixture.write(
        "distinct_null",
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2, 1, 2, 1, 2]))],
        )
        .unwrap(),
        2,
    );
    let tables = HashMap::from([("t".into(), path)]);
    for optimized in [false, true] {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for spill in [false, true] {
                    let mut options = fixture.options(workers, batch_size, spill);
                    options.relational_optimizations = optimized;
                    let sql = "SELECT k,SUM(DISTINCT CAST(NULL AS REAL)),AVG(DISTINCT -CAST(NULL AS REAL)),COUNT(*) FROM t GROUP BY k ORDER BY k";
                    let result =
                        execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                    assert_eq!(
                        rows(&result.batch, true),
                        vec![
                            vec![json!(1), Value::Null, Value::Null, json!(2)],
                            vec![json!(2), Value::Null, Value::Null, json!(3)],
                        ]
                    );
                    for column in [1, 2] {
                        assert_eq!(result.batch.column(column).data_type(), &DataType::Float64);
                    }
                    if spill {
                        assert!(result.metrics.external_aggregate_partitions > 0);
                    }
                    fixture.clean_spill();
                }
            }
        }
    }
}

#[test]
fn float_state_growth_is_funded_and_memory_refusal_cleans_up() {
    let fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("x", DataType::Float64, false),
    ]));
    let path = fixture.write(
        "growth",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..257)),
                Arc::new(Float64Array::from(vec![1.5; 257])),
            ],
        )
        .unwrap(),
        31,
    );
    let tables = HashMap::from([("t".into(), path)]);
    for spill in [false, true] {
        let mut options = fixture.options(4, 7, spill);
        let sql = "SELECT k,SUM(x),AVG(x) FROM t GROUP BY k ORDER BY k";
        let plan = prepare(sql, &tables, &options).unwrap();
        let result = execute(&plan, &options).unwrap();
        assert_eq!(result.batch.num_rows(), 257);
        for row in 0..257 {
            assert_eq!(
                rows(&result.batch.slice(row, 1), false),
                vec![vec![json!(row), json!(1.5), json!(1.5)]]
            );
        }
        options.memory_bytes = 1;
        let error = execute(&plan, &options).err().unwrap();
        assert!(error.starts_with("resource-limit:"), "{error}");
        fixture.clean_spill();
    }
}
