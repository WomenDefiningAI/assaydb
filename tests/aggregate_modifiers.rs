//! Aggregate FILTER, DISTINCT Float SUM/AVG, STRING_AGG ORDER BY and
//! ROLLUP/CUBE/GROUPING SETS against independent DuckDB 1.5.6 goldens
//! (tools/testing/aggregate_modifiers_reference.py). No Assay output feeds them.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const CASES: &str = include_str!("fixtures/aggregate_modifiers/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("aggregate_modifiers/analysis.sql"));
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-aggregate-modifiers-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::create_dir(path.join("spill")).unwrap();
        Self(path)
    }
    fn options(&self, workers: usize, batch_size: usize, spill: bool, optimized: bool) -> Options {
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            memory_bytes: 64 << 20,
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
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aggregate_modifiers")
        .join(name)
}

fn expected(name: &str) -> Value {
    if name == "analysis_expected.json" {
        return serde_json::from_str(&external_sf1::read(
            "aggregate_modifiers/analysis_expected.json",
        ))
        .unwrap();
    }
    serde_json::from_str(&fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn tables() -> HashMap<String, PathBuf> {
    ["sales", "empty_sales", "big"]
        .into_iter()
        .map(|name| (name.to_owned(), fixture_path(&format!("{name}.parquet"))))
        .collect()
}

fn float_cell(value: f64) -> Value {
    if value.is_nan() {
        json!({"float": "NaN"})
    } else if value.is_infinite() {
        json!({"float": if value > 0.0 { "+inf" } else { "-inf" }})
    } else {
        json!({"float": value})
    }
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(row) || a.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    match a.data_type() {
                        DataType::Int64 => {
                            json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
                        }
                        DataType::Utf8 => {
                            json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                        }
                        DataType::Boolean => json!(
                            a.as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Float64 => float_cell(
                            a.as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row),
                        ),
                        DataType::Date32 => {
                            let days = a.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
                            json!(
                                (chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                    + chrono::Duration::days(days.into()))
                                .to_string()
                            )
                        }
                        DataType::Decimal128(_, scale) => json!(assaydb::types::decimal_text(
                            a.as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        other => panic!("unexpected output type {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

/// DuckDB's printed type, mapped to the Assay type class it must match.
/// DuckDB widens integer SUM to HUGEINT and keeps narrower DECIMAL precisions;
/// Assay returns checked BIGINT and precision-38 DECIMAL with the same scale.
fn same_type(assay: &DataType, duckdb: &str) -> bool {
    match assay {
        DataType::Null => true,
        DataType::Int64 => matches!(duckdb, "BIGINT" | "HUGEINT" | "INTEGER"),
        DataType::Utf8 => duckdb == "VARCHAR",
        DataType::Boolean => duckdb == "BOOLEAN",
        DataType::Float64 => duckdb == "DOUBLE",
        DataType::Date32 => duckdb == "DATE",
        DataType::Decimal128(_, scale) => duckdb
            .strip_prefix("DECIMAL(")
            .and_then(|t| t.strip_suffix(')'))
            .and_then(|t| t.split_once(','))
            .is_some_and(|(_, s)| s.trim().parse::<i8>().ok() == Some(*scale)),
        _ => false,
    }
}

/// Exact comparison. Floats compare as binary64 values (DuckDB's shortest
/// repr parses back to the identical double; NaN equals NaN; the approved
/// fold from +0 makes -0.0 and 0.0 equal). `tolerance` is the approved Float
/// SUM/AVG reference tolerance, used only for the marked TPC-H case.
fn cell_matches(actual: &Value, wanted: &Value, tolerance: bool) -> bool {
    match (actual.get("float"), wanted.get("float")) {
        (Some(a), Some(b)) => {
            let parse = |v: &Value| match v {
                Value::String(s) if s == "NaN" => f64::NAN,
                Value::String(s) if s == "+inf" => f64::INFINITY,
                Value::String(s) if s == "-inf" => f64::NEG_INFINITY,
                Value::String(s) => s.parse().unwrap(),
                other => other.as_f64().unwrap(),
            };
            let (a, b) = (parse(a), parse(b));
            (a.is_nan() && b.is_nan())
                || a == b
                || (tolerance
                    && a.is_finite()
                    && b.is_finite()
                    && (a - b).abs() <= 1e-12_f64.max(b.abs() * 1e-12))
        }
        _ => actual == wanted,
    }
}

fn check_rows(batch: &RecordBatch, case: &Value, context: &str, tolerance: bool) {
    assert_eq!(case["reference_outcome"], "rows", "{context}");
    let names = batch
        .schema()
        .fields()
        .iter()
        .map(|f| json!(f.name()))
        .collect::<Vec<_>>();
    assert_eq!(json!(names), case["columns"], "{context}");
    for (field, ty) in batch
        .schema()
        .fields()
        .iter()
        .zip(case["types"].as_array().unwrap())
    {
        assert!(
            same_type(field.data_type(), ty.as_str().unwrap()),
            "{context}: {} is {} but DuckDB returned {ty}",
            field.name(),
            field.data_type()
        );
    }
    let mut actual = rows(batch);
    let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    if !case["ordered"].as_bool().unwrap() {
        actual.sort_by_key(|r| serde_json::to_string(r).unwrap());
        wanted.sort_by_key(|r| serde_json::to_string(r).unwrap());
    }
    assert_eq!(
        actual.len(),
        wanted.len(),
        "{context}: {actual:?} != {wanted:?}"
    );
    for (a, w) in actual.iter().zip(&wanted) {
        assert!(
            a.len() == w.len() && a.iter().zip(w).all(|(x, y)| cell_matches(x, y, tolerance)),
            "{context}: {actual:?} != {wanted:?}"
        );
    }
}

fn reference_cases() -> Vec<Value> {
    let expected = expected("expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    for (name, file) in expected["tables"].as_object().unwrap() {
        let size = fs::metadata(fixture_path(&format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), file["bytes"], "{name}.parquet");
    }
    let cases = expected["cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 89);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(&cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "fixture metadata {key}");
        }
        assert!(case["pg"].is_string(), "{}: PostgreSQL note", case["id"]);
        assert_eq!(case["sql"], sql.trim());
    }
    cases
}

#[test]
fn independent_duckdb_cases_match_at_all_worker_batch_and_spill_settings() {
    let cases = reference_cases();
    let tables = tables();
    let fixture = Fixture::new();
    let mut spilled = 0;
    let mut settings = Vec::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                settings.push((workers, batch_size, spill, true));
            }
        }
    }
    // Planner optimizations off as well, at one mid-size setting.
    settings.push((4, 7, false, false));
    settings.push((4, 7, true, false));
    for (workers, batch_size, spill, optimized) in settings {
        let options = fixture.options(workers, batch_size, spill, optimized);
        for case in &cases {
            let sql = case["sql"].as_str().unwrap();
            let context = format!(
                "{} workers={workers} batch={batch_size} spill={spill} optimized={optimized}",
                case["id"]
            );
            let result = prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options));
            if let Some(error) = case["assay_error"].as_str() {
                let actual = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: expected error"));
                assert!(actual.starts_with(error), "{context}: {actual}");
            } else {
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                check_rows(&result.batch, case, &context, false);
                spilled += usize::from(spill && result.metrics.external_aggregate_partitions > 0);
            }
            fixture.clean_spill();
        }
    }
    // Forced spill really routes grouped aggregation through partitions.
    assert!(spilled > 8 * 40, "only {spilled} spilled executions");
}

/// Distinct Float values are added once, at their first occurrence in canonical
/// input order, starting from +0. Each group's stream is
/// [1e16, 1, 1e16, -1e16, 3, NULL, -0.0, 0.0, 1, NaN, -NaN] without the NaNs for
/// k=0: the first occurrences 1e16, 1, -1e16, 3, -0.0 fold to exactly 3, which a
/// sorted or hash order would not produce. Three interleaved groups straddle
/// row-group and batch boundaries.
#[test]
fn distinct_float_sum_uses_first_occurrence_order_on_every_path() {
    let fixture = Fixture::new();
    let neg_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let stream = [
        Some(1e16),
        Some(1.0),
        Some(1e16),
        Some(-1e16),
        Some(3.0),
        None,
        Some(-0.0),
        Some(0.0),
        Some(1.0),
        Some(f64::NAN),
        Some(neg_nan),
    ];
    let groups = 3;
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for value in stream {
        for k in 0..groups {
            // Group 0 never sees the NaNs.
            if k == 0 && value.is_some_and(f64::is_nan) {
                continue;
            }
            keys.push(k as i64);
            values.push(value);
        }
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("x", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();
    let path = fixture.write("first_occurrence", batch, 2);
    let tables = HashMap::from([("t".to_owned(), path)]);
    let sql = "SELECT k, SUM(DISTINCT x), AVG(DISTINCT x), COUNT(DISTINCT x), SUM(x) FILTER (WHERE x > 2 AND x <= 100000000000000000) FROM t GROUP BY k ORDER BY k";
    let total = 3.0_f64;
    let wanted = vec![
        vec![
            json!(0),
            json!(total.to_bits()),
            json!((total / 5.0).to_bits()),
            json!(5),
            json!((1e16_f64 + 1e16 + 3.0).to_bits()),
        ],
        vec![
            json!(1),
            json!(f64::NAN.to_bits()),
            json!(f64::NAN.to_bits()),
            json!(6),
            json!((1e16_f64 + 1e16 + 3.0).to_bits()),
        ],
        vec![
            json!(2),
            json!(f64::NAN.to_bits()),
            json!(f64::NAN.to_bits()),
            json!(6),
            json!((1e16_f64 + 1e16 + 3.0).to_bits()),
        ],
    ];
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for spill in [false, true] {
                    let options = fixture.options(workers, batch_size, spill, optimized);
                    let result =
                        execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                    let actual = (0..result.batch.num_rows())
                        .map(|row| {
                            let column = |c: usize| result.batch.column(c);
                            let float = |c: usize| {
                                json!(
                                    column(c)
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .unwrap()
                                        .value(row)
                                        .to_bits()
                                )
                            };
                            let int = |c: usize| {
                                json!(
                                    column(c)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .value(row)
                                )
                            };
                            vec![int(0), float(1), float(2), int(3), float(4)]
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, wanted,
                        "workers={workers} batch={batch_size} spill={spill} optimized={optimized}"
                    );
                    if spill {
                        assert!(result.metrics.external_aggregate_partitions > 0);
                    }
                    fixture.clean_spill();
                }
            }
        }
    }
}

/// One large STRING_AGG group grows its entry buffer through many funded
/// reallocations; the result is independent of settings, and a tiny memory
/// limit is a clean resource-limit refusal.
#[test]
fn string_agg_growth_is_funded_deterministic_and_refuses_cleanly() {
    let fixture = Fixture::new();
    let rows = 5000;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i % 2))),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i * 7919) % rows),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 11 != 0).then(|| format!("v{}é", (i * 7919) % rows))
            }))),
        ],
    )
    .unwrap();
    let path = fixture.write("growth", batch, 333);
    let tables = HashMap::from([("t".to_owned(), path)]);
    let sql = "SELECT k, STRING_AGG(s, ',' ORDER BY id DESC NULLS LAST) AS joined, COUNT(s) AS n FROM t GROUP BY ROLLUP(k) ORDER BY GROUPING(k), k";
    // Expected value computed directly: ids are a permutation of 0..rows.
    let expected_for = |filter: &dyn Fn(i64) -> bool| {
        let mut items = (0..rows)
            .filter(|i| filter(*i) && i % 11 != 0)
            .map(|i| (i * 7919) % rows)
            .collect::<Vec<_>>();
        items.sort_unstable_by(|a, b| b.cmp(a));
        (
            items
                .iter()
                .map(|id| format!("v{id}é"))
                .collect::<Vec<_>>()
                .join(","),
            items.len() as i64,
        )
    };
    let wanted = [
        (Some(0), expected_for(&|i| i % 2 == 0)),
        (Some(1), expected_for(&|i| i % 2 == 1)),
        (None, expected_for(&|_| true)),
    ];
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = fixture.options(workers, batch_size, spill, true);
                let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                let batch = result.batch;
                assert_eq!(batch.num_rows(), 3);
                for (row, (key, (joined, n))) in wanted.iter().enumerate() {
                    let k = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(k.is_valid(row).then(|| k.value(row)), *key);
                    let s = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(s.value(row), joined);
                    let c = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(c.value(row), *n);
                }
                fixture.clean_spill();
            }
        }
    }
    for spill in [false, true] {
        let mut options = fixture.options(4, 7, spill, true);
        options.memory_bytes = 64 << 10;
        let error = prepare(sql, &tables, &options)
            .and_then(|plan| execute(&plan, &options))
            .err()
            .unwrap();
        assert!(error.starts_with("resource-limit:"), "{error}");
        fixture.clean_spill();
    }
}

fn analysis_session(directory: &Path, workers: usize, batch_size: usize) -> Session {
    let mut session = Session::new(
        directory,
        Options {
            workers,
            batch_size,
            memory_bytes: 2 << 30,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    session.set_local_files_enabled(true);
    session
}

/// TPC-H SF1 analysis examples. Set ASSAY_TPCH_SF1_DIR to the Parquet
/// directory whose sizes analysis_expected.json records; required for qualification.
#[test]
fn tpch_sf1_analysis_examples_match_duckdb() {
    let directory = std::env::var("ASSAY_TPCH_SF1_DIR")
        .expect("full qualification requires ASSAY_TPCH_SF1_DIR; SF1 coverage cannot be skipped");
    let expected = expected("analysis_expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    for (name, file) in expected["files"].as_object().unwrap() {
        let size = fs::metadata(Path::new(&directory).join(format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), file["bytes"], "{name}.parquet");
    }
    let cases = expected["cases"].as_array().unwrap();
    let sources = ANALYSIS.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    let fixture = Fixture::new();
    for (source, case) in sources.iter().zip(cases) {
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        let sql = sql.trim().replace("{tpch}", &directory.replace('\'', "''"));
        let tolerance = case.get("float_tolerance").is_some();
        for workers in [1, 4, 10] {
            for batch_size in [1000, 8192] {
                let mut session = analysis_session(&fixture.0, workers, batch_size);
                let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
                let Output::Query { execution, .. } = session
                    .execute_sql(&sql)
                    .unwrap_or_else(|error| panic!("{context}: {}", error.message))
                else {
                    panic!("{context}: expected query");
                };
                check_rows(&execution.batch, case, &context, tolerance);
            }
        }
    }
}

/// Memory pressure on one hot DISTINCT set takes the external sort-based
/// deduplication path. Its value identity must equal the in-memory sets:
/// -0.0 equals 0.0 and NaN payloads/signs are one value.
#[test]
fn distinct_float_identity_survives_sort_based_spill_deduplication() {
    let fixture = Fixture::new();
    let rows = 8192;
    let neg_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let xs = [
        Some(1.5),
        Some(-0.0),
        Some(2.25),
        Some(0.0),
        None,
        Some(-3.0),
    ];
    let ys = [Some(1.0), Some(f64::NAN), Some(neg_nan), None];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("{:08}{}", i % 4096, "w".repeat(1024))),
            )),
            Arc::new(Float64Array::from_iter((0..rows).map(|i| xs[i % xs.len()]))),
            Arc::new(Float64Array::from_iter((0..rows).map(|i| ys[i % ys.len()]))),
        ],
    )
    .unwrap();
    let path = fixture.write("hot_distinct", batch, 256);
    let tables = HashMap::from([("t".to_owned(), path)]);
    let sql = "SELECT COUNT(DISTINCT s), COUNT(DISTINCT x), SUM(DISTINCT x), AVG(DISTINCT x), COUNT(DISTINCT y) FROM t";
    // First occurrences 1.5, -0.0, 2.25, -3.0 fold from +0 to exactly 0.75.
    let wanted = vec![vec![
        json!(4096),
        json!(4),
        json!(0.75_f64.to_bits()),
        json!((0.75_f64 / 4.0).to_bits()),
        json!(2),
    ]];
    let cells = |batch: &RecordBatch| {
        let int = |c: usize| {
            json!(
                batch
                    .column(c)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            )
        };
        let float = |c: usize| {
            json!(
                batch
                    .column(c)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits()
            )
        };
        vec![vec![int(0), int(1), float(2), float(3), int(4)]]
    };
    let control = fixture.options(1, 8192, false, true);
    let control = Options {
        memory_bytes: 256 << 20,
        ..control
    };
    let result = execute(&prepare(sql, &tables, &control).unwrap(), &control).unwrap();
    assert_eq!(cells(&result.batch), wanted);
    for batch_size in [32, 128] {
        for optimized in [false, true] {
            let mut options = fixture.options(4, batch_size, true, optimized);
            options.memory_bytes = 4 << 20;
            let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
            assert_eq!(cells(&result.batch), wanted, "batch={batch_size}");
            assert!(result.metrics.external_aggregate_distinct_passes > 0);
            fixture.clean_spill();
        }
    }
}
