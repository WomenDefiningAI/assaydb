//! Ordered-set and distribution aggregates (MEDIAN, QUANTILE_CONT/DISC,
//! PERCENTILE_CONT/DISC WITHIN GROUP, MODE, APPROX_COUNT_DISTINCT) against
//! independent DuckDB 1.5.6 goldens with PostgreSQL 17 notes
//! (tools/testing/ordered_set_reference.py). No Assay output feeds them.
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
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const CASES: &str = include_str!("fixtures/ordered_set/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("ordered_set/analysis.sql"));
const TABLES: [&str; 6] = ["vals", "empty", "regions", "edge", "seq", "big"];
const CASE_COUNT: usize = 46;
/// Relative tolerance against DuckDB (the approved Float aggregate tolerance),
/// with the same absolute floor near zero. DuckDB's own build fuses one
/// multiply-add in DOUBLE interpolation, so its last bit can differ.
const DUCKDB_TOLERANCE: f64 = 1e-12;
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-ordered-set-{}-{}",
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
        .join("tests/fixtures/ordered_set")
        .join(name)
}

fn expected(name: &str) -> Value {
    if name == "analysis_expected.json" {
        return serde_json::from_str(&external_sf1::read("ordered_set/analysis_expected.json"))
            .unwrap();
    }
    serde_json::from_str(&fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn tables() -> HashMap<String, PathBuf> {
    TABLES
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

/// Exact float bits of every REAL cell, for cross-setting determinism.
fn float_bits(batch: &RecordBatch) -> Vec<u64> {
    batch
        .columns()
        .iter()
        .filter_map(|a| a.as_any().downcast_ref::<Float64Array>())
        .flat_map(|a| a.iter().map(|v| v.map_or(u64::MAX, f64::to_bits)))
        .collect()
}

/// DuckDB's printed type, mapped to the Assay type class it must match.
fn same_type(assay: &DataType, duckdb: &str) -> bool {
    match assay {
        DataType::Null => true,
        DataType::Int64 => matches!(duckdb, "BIGINT" | "INTEGER"),
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

fn parse_float(value: &Value) -> f64 {
    match value {
        Value::String(s) if s == "NaN" => f64::NAN,
        Value::String(s) if s == "+inf" => f64::INFINITY,
        Value::String(s) if s == "-inf" => f64::NEG_INFINITY,
        Value::String(s) => s.parse().unwrap(),
        other => other.as_f64().unwrap(),
    }
}

fn cell_matches(actual: &Value, wanted: &Value, worst: &mut f64) -> bool {
    match (actual.get("float"), wanted.get("float")) {
        (Some(a), Some(b)) => {
            let (a, b) = (parse_float(a), parse_float(b));
            if a.is_nan() || b.is_nan() || a.is_infinite() || b.is_infinite() {
                return a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan());
            }
            if a != b {
                *worst = worst.max((a - b).abs() / b.abs().max(1.0));
            }
            (a - b).abs() <= DUCKDB_TOLERANCE.max(b.abs() * DUCKDB_TOLERANCE)
        }
        _ => actual == wanted,
    }
}

fn check_rows(batch: &RecordBatch, case: &Value, context: &str, worst: &mut f64) {
    assert_eq!(case["reference_outcome"], "rows", "{context}");
    // Decided differences (APPROX_COUNT_DISTINCT exact; DECIMAL
    // interpolation as DOUBLE) carry their own expected rows and types.
    let assay_rows = case.get("assay_rows");
    let types = case
        .get("assay_types")
        .unwrap_or(&case["types"])
        .as_array()
        .unwrap();
    assert_eq!(types.len(), batch.num_columns(), "{context}");
    for (field, ty) in batch.schema().fields().iter().zip(types) {
        assert!(
            same_type(field.data_type(), ty.as_str().unwrap()),
            "{context}: {} is {} but the reference type is {ty}",
            field.name(),
            field.data_type()
        );
    }
    let actual = rows(batch);
    let wanted: Vec<Vec<Value>> =
        serde_json::from_value(assay_rows.unwrap_or(&case["rows"]).clone()).unwrap();
    assert_eq!(
        actual.len(),
        wanted.len(),
        "{context}: {actual:?} != {wanted:?}"
    );
    // Plain MODE(x) ties: DuckDB may return any equally frequent value; Assay
    // returns the smallest, which the generator lists first.
    let tie_column = case.get("tie_column").and_then(Value::as_u64);
    let permitted = case.get("permitted").and_then(Value::as_array);
    for (row, (a, w)) in actual.iter().zip(&wanted).enumerate() {
        assert_eq!(a.len(), w.len(), "{context}");
        for (column, (x, y)) in a.iter().zip(w).enumerate() {
            if tie_column == Some(column as u64)
                && let Some(choices) = permitted
                    .unwrap()
                    .iter()
                    .find(|p| p[0] == a[0])
                    .map(|p| p[1].as_array().unwrap())
            {
                assert!(choices.contains(x), "{context}: {x} not among {choices:?}");
                assert_eq!(x, &choices[0], "{context}: smallest tied value");
                assert!(choices.contains(y), "{context}: DuckDB {y} not permitted");
                continue;
            }
            assert!(
                cell_matches(x, y, worst),
                "{context}: row {row} column {column}: {actual:?} != {wanted:?}"
            );
        }
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
    assert_eq!(cases.len(), CASE_COUNT);
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
fn independent_reference_cases_match_at_all_worker_batch_and_spill_settings() {
    let cases = reference_cases();
    let tables = tables();
    let fixture = Fixture::new();
    let mut settings = Vec::new();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                settings.push((workers, batch_size, spill, true));
            }
        }
    }
    // Planner optimizations off as well.
    for spill in [false, true] {
        settings.push((4, 7, spill, false));
        settings.push((1, 8192, spill, false));
    }
    let mut bits: HashMap<String, Vec<u64>> = HashMap::new();
    let (mut spilled, mut worst) = (0, 0.0_f64);
    for (workers, batch_size, spill, optimized) in settings {
        let options = fixture.options(workers, batch_size, spill, optimized);
        for case in &cases {
            let sql = case["sql"].as_str().unwrap();
            // One-row batches over the 6,000-row table add time, not coverage.
            if batch_size == 1 && sql.contains("FROM big") {
                continue;
            }
            let id = case["id"].as_str().unwrap();
            let context = format!(
                "{id} workers={workers} batch={batch_size} spill={spill} optimized={optimized}"
            );
            let result = prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options));
            if let Some(error) = case["assay_error"].as_str() {
                let actual = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: expected error"));
                assert!(actual.starts_with(error), "{context}: {actual}");
            } else {
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                check_rows(&result.batch, case, &context, &mut worst);
                let observed = float_bits(&result.batch);
                let first = bits
                    .entry(id.to_owned())
                    .or_insert_with(|| observed.clone());
                assert_eq!(first, &observed, "{context}: REAL bits depend on settings");
                spilled += usize::from(spill && result.metrics.external_aggregate_partitions > 0);
            }
            fixture.clean_spill();
        }
    }
    eprintln!("max relative error vs DuckDB: {worst:e}");
    assert!(worst <= DUCKDB_TOLERANCE);
    // Forced spill really routes grouped aggregation through partitions.
    assert!(spilled > 200, "only {spilled} spilled executions");
}

/// The fixture documents the reference disagreements that Assay refuses.
#[test]
fn reference_disagreements_are_recorded() {
    let cases = reference_cases();
    let case = |id: &str| cases.iter().find(|c| c["id"] == id).unwrap().clone();
    // DuckDB truncates DECIMAL interpolation differently by declared
    // precision; Assay (maintainer decision) returns PostgreSQL's DOUBLE.
    let grouped = case("median_grouped");
    assert_eq!(
        grouped["rows"][1][2],
        json!("-1.65"),
        "DuckDB DECIMAL(12,2)"
    );
    assert_eq!(
        grouped["rows"][1][3],
        json!("-1.64"),
        "DuckDB DECIMAL(20,2)"
    );
    assert_eq!(grouped["pg_rows"][1][2], json!("-1.645"), "PostgreSQL");
    assert_eq!(grouped["assay_rows"][1][2], json!({"float": "-1.645"}));
    assert_eq!(grouped["assay_rows"][1][3], json!({"float": "-1.645"}));
    let desc = case("decimal_desc_through_subquery");
    assert_eq!(desc["rows"], json!([["3.88"]]), "DuckDB DESC truncation");
    assert_eq!(desc["assay_rows"], json!([[{"float": "3.875"}]]));
    let asc = case("decimal_asc_through_subquery");
    assert_eq!(asc["rows"], json!([["3.87"]]), "DuckDB ASC truncation");
    assert_eq!(asc["assay_rows"], json!([[{"float": "3.875"}]]));
    for c in &cases {
        if c.get("assay_rows").is_some() {
            assert_eq!(c["pg_vs_assay"], "agrees", "{}", c["id"]);
        }
    }
    let index = case("refuse_percentile_disc_binary64_index");
    assert_eq!(index["rows"], json!([[7]]));
    assert_eq!(index["pg_rows"], json!([["8"]]));
    let zero = case("refuse_zero_fraction_desc");
    assert_eq!(zero["rows"], json!([[-9]]));
    assert_eq!(zero["pg_rows"], json!([["12"]]));
    let overflow = case("refuse_real_overflow");
    assert_eq!(overflow["rows"], json!([[{"float": "0.0"}]]));
    assert_eq!(overflow["pg_rows"], json!([["Infinity"]]));
    // APPROX_COUNT_DISTINCT: DuckDB's estimate is not the exact count.
    let approx = case("approx_count_distinct_estimate_differs");
    assert_ne!(approx["rows"], approx["assay_rows"]);
}

fn run_rows(sql: &str) -> Vec<Vec<Value>> {
    let fixture = Fixture::new();
    let options = fixture.options(4, 3, false, true);
    let result = execute(&prepare(sql, &tables(), &options).unwrap(), &options)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows(&result.batch)
}

/// Hand-checked answers, independent of both the generator and DuckDB.
#[test]
fn handwritten_controls() {
    let data = "(SELECT 1 AS k, 10 AS x, CAST(1.01 AS DECIMAL(10,2)) AS d, 'b' AS s UNION ALL SELECT 1, 40, CAST(1.04 AS DECIMAL(10,2)), 'a' UNION ALL SELECT 1, 20, NULL, 'b' UNION ALL SELECT 1, 30, NULL, 'a' UNION ALL SELECT 2, NULL, NULL, NULL UNION ALL SELECT 3, 7, CAST(-2.25 AS DECIMAL(10,2)), 'z') t";
    // k=1: x = 10, 20, 30, 40. RN = 3q; p25 = 17.5, p75 = 32.5; disc p50 = 20.
    assert_eq!(
        run_rows(&format!(
            "SELECT k, MEDIAN(x), PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY x), QUANTILE_CONT(x, 0.75), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x DESC), MODE(s), MODE() WITHIN GROUP (ORDER BY s DESC) FROM {data} GROUP BY k ORDER BY k"
        )),
        vec![
            vec![
                json!(1),
                float_cell(25.0),
                float_cell(17.5),
                float_cell(32.5),
                json!(20),
                json!(30),
                json!("a"),
                json!("b"),
            ],
            vec![
                json!(2),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                json!(3),
                float_cell(7.0),
                float_cell(7.0),
                float_cell(7.0),
                json!(7),
                json!(7),
                json!("z"),
                json!("z"),
            ],
        ]
    );
    // DECIMAL(12,2) column, median of 5.50 and 2.25: exactly 3.875 as DOUBLE
    // in either direction (DuckDB: DECIMAL 3.87 ascending, 3.88 descending).
    // 0.3 of {2.25, 5.50} is 2.25 + 0.3 * 3.25 = 3.225; DISC keeps DECIMAL.
    assert_eq!(
        run_rows(
            "SELECT MEDIAN(d), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d DESC), QUANTILE_CONT(d, 0.3), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY d) FROM vals WHERE id IN (1, 2)"
        ),
        vec![vec![
            float_cell(3.875),
            float_cell(3.875),
            float_cell(3.225),
            json!("2.25")
        ]]
    );
    // Empty input: NULL quantiles and modes, APPROX_COUNT_DISTINCT 0.
    assert_eq!(
        run_rows(&format!(
            "SELECT MEDIAN(x), MODE(s), APPROX_COUNT_DISTINCT(s) FROM {data} WHERE k > 5"
        )),
        vec![vec![Value::Null, Value::Null, json!(0)]]
    );
    // REAL: -0.0 and 0.0 are one value (returned as 0.0) and NaN sorts last
    // and counts as one value.
    let reals = "(SELECT -CAST(0 AS DOUBLE) AS f UNION ALL SELECT -CAST(0 AS DOUBLE) UNION ALL SELECT CAST(0 AS DOUBLE) UNION ALL SELECT CAST(1 AS DOUBLE)) t";
    let fixture = Fixture::new();
    let options = fixture.options(1, 8192, false, true);
    let sql = format!(
        "SELECT MODE(f), MEDIAN(f), PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY f) FROM {reals}"
    );
    let result = execute(&prepare(&sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
    for column in 0..3 {
        let value = result
            .batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(value.to_bits(), 0.0_f64.to_bits(), "column {column}");
    }
    // vals id 19 holds the default NaN.
    let nans = "(SELECT f FROM vals WHERE id = 19 UNION ALL SELECT f FROM vals WHERE id = 19 UNION ALL SELECT CAST(1 AS DOUBLE) UNION ALL SELECT CAST(2 AS DOUBLE) UNION ALL SELECT CAST(2 AS DOUBLE)) t";
    let got = run_rows(&format!(
        "SELECT MODE(f), MODE() WITHIN GROUP (ORDER BY f DESC), MEDIAN(f), QUANTILE_DISC(f, 1) FROM {nans}"
    ));
    assert_eq!(
        got,
        vec![vec![
            float_cell(2.0),
            float_cell(f64::NAN),
            float_cell(2.0),
            float_cell(f64::NAN)
        ]]
    );
    // Values that need more than 64 bits after narrower ones (three-row
    // batches): the stored group widens without losing earlier values.
    let wide = "(SELECT CAST(1 AS DECIMAL(38,0)) AS v UNION ALL SELECT 2 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT CAST(100000000000000000000 AS DECIMAL(38,0)) UNION ALL SELECT CAST(100000000000000000000 AS DECIMAL(38,0)) UNION ALL SELECT CAST(100000000000000000000 AS DECIMAL(38,0))) t";
    assert_eq!(
        run_rows(&format!(
            "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v), QUANTILE_DISC(v, 1), MODE(v), MEDIAN(v), QUANTILE_DISC(v, 0) FROM {wide}"
        )),
        vec![vec![
            json!("3"),
            json!("100000000000000000000"),
            json!("100000000000000000000"),
            float_cell(3.0),
            json!("1")
        ]]
    );
    // APPROX_COUNT_DISTINCT returns exactly COUNT(DISTINCT x).
    assert_eq!(
        run_rows(&format!(
            "SELECT k, APPROX_COUNT_DISTINCT(s), COUNT(DISTINCT s), APPROX_COUNT_DISTINCT(x), COUNT(DISTINCT x) FROM {data} GROUP BY k ORDER BY k"
        )),
        vec![
            vec![json!(1), json!(2), json!(2), json!(4), json!(4)],
            vec![json!(2), json!(0), json!(0), json!(0), json!(0)],
            vec![json!(3), json!(1), json!(1), json!(1), json!(1)],
        ]
    );
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create_new(path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn budget_options(fixture: &Fixture, memory: usize, spill: bool) -> Options {
    Options {
        memory_bytes: memory,
        ..fixture.options(4, 1024, spill, true)
    }
}

/// Ordered-set state (every value of every group) is charged to the query
/// budget: a budget too small for it fails with a typed resource-limit error
/// without spill, and gives identical results through forced external
/// aggregation, which splits the groups across partitions.
#[test]
fn ordered_set_state_is_budgeted_and_spills() {
    let fixture = Fixture::new();
    let count = 120_000_i64;
    let keys: Vec<i64> = (0..count).map(|i| (i * 7919) % 20_000).collect();
    let xs: Vec<i64> = (0..count).map(|i| (i * 31) % 1000 - 500).collect();
    let fs: Vec<f64> = (0..count).map(|i| 1e6 + (i % 97) as f64 * 0.25).collect();
    let ss: Vec<String> = (0..count)
        .map(|i| format!("v{:03}", (i * 13) % 17))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(xs)),
            Arc::new(Float64Array::from(fs)),
            Arc::new(StringArray::from(ss)),
        ],
    )
    .unwrap();
    let path = fixture.0.join("groups.parquet");
    write_parquet(&path, &batch);
    let tables = HashMap::from([("t".to_owned(), path)]);
    let sql = "SELECT k, MEDIAN(x), PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY f DESC), QUANTILE_CONT(f, 0.3), MODE(s) FROM t GROUP BY k ORDER BY k";
    let run = |options: &Options| prepare(sql, &tables, options).and_then(|p| execute(&p, options));
    let reference = run(&budget_options(&fixture, 256 << 20, false)).unwrap();
    assert_eq!(reference.batch.num_rows(), 20_000);
    let tight = 8 << 20;
    let error = run(&budget_options(&fixture, tight, false))
        .err()
        .expect("values of 20,000 groups exceed the budget");
    assert!(error.starts_with("resource-limit:"), "{error}");
    fixture.clean_spill();
    let spilled = run(&budget_options(&fixture, tight, true)).unwrap();
    assert!(spilled.metrics.external_aggregate_partitions > 1);
    assert_eq!(rows(&spilled.batch), rows(&reference.batch));
    assert_eq!(float_bits(&spilled.batch), float_bits(&reference.batch));
    fixture.clean_spill();
}

/// A single group larger than the budget cannot be split by hashing: forced
/// spill reports a typed resource-limit error (grouped and ungrouped).
#[test]
fn one_group_larger_than_the_budget_fails_with_resource_limit() {
    let fixture = Fixture::new();
    let count = 300_000_i64;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1; count as usize])),
            Arc::new(Int64Array::from((0..count).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let path = fixture.0.join("hot.parquet");
    write_parquet(&path, &batch);
    let tables = HashMap::from([("t".to_owned(), path)]);
    for sql in [
        "SELECT k, MEDIAN(x) FROM t GROUP BY k",
        "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x) FROM t",
    ] {
        let ample = prepare(sql, &tables, &budget_options(&fixture, 256 << 20, false))
            .and_then(|p| execute(&p, &budget_options(&fixture, 256 << 20, false)))
            .unwrap();
        let last = ample.batch.num_columns() - 1;
        let median = rows(&ample.batch)[0][last].clone();
        assert!(
            median == float_cell(149_999.5) || median == json!(149_999),
            "{sql}: {median}"
        );
        for spill in [false, true] {
            let options = budget_options(&fixture, 3 << 20, spill);
            let error = prepare(sql, &tables, &options)
                .and_then(|p| execute(&p, &options))
                .err()
                .unwrap_or_else(|| panic!("{sql} spill={spill}: expected resource limit"));
            assert!(error.starts_with("resource-limit:"), "{sql}: {error}");
            fixture.clean_spill();
        }
    }
}

/// The command-line interface over Parquet returns the same answers.
#[test]
fn cli_over_parquet_matches_reference() {
    let cases = reference_cases();
    let fixture = Fixture::new();
    for id in [
        "median_grouped",
        "percentile_disc_types",
        "mode_within_group_ties",
        "decimal_interpolation_is_double",
    ] {
        let case = cases.iter().find(|c| c["id"] == id).unwrap();
        let output = fixture.0.join(format!("{id}.json"));
        let mut cli = Command::new(env!("CARGO_BIN_EXE_assaydb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let request = json!({"sql": case["sql"], "tables": tables(), "output": output});
        writeln!(cli.stdin.take().unwrap(), "{request}").unwrap();
        assert!(cli.wait().unwrap().success(), "{id}");
        let result: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        let actual = result["rows"].as_array().unwrap();
        let wanted = case
            .get("assay_rows")
            .unwrap_or(&case["rows"])
            .as_array()
            .unwrap();
        assert_eq!(actual.len(), wanted.len(), "{id}");
        let mut worst = 0.0;
        for (a, w) in actual.iter().zip(wanted) {
            for (x, y) in a.as_array().unwrap().iter().zip(w.as_array().unwrap()) {
                let x = match (x, y.get("float")) {
                    (Value::String(text), Some(_)) => json!({"float": text}),
                    (Value::Null, _) | (_, None) => x.clone(),
                    (other, Some(_)) => json!({"float": other}),
                };
                assert!(cell_matches(&x, y, &mut worst), "{id}: {a} != {w}");
            }
        }
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
fn tpch_sf1_analysis_examples_match_references() {
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
    let mut worst = 0.0_f64;
    for (source, case) in sources.iter().zip(cases) {
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        let sql = sql.trim().replace("{tpch}", &directory.replace('\'', "''"));
        for (workers, batch_size) in [(1, 8192), (4, 1000), (10, 8192)] {
            let mut session = analysis_session(&fixture.0, workers, batch_size);
            let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
            let Output::Query { execution, .. } = session
                .execute_sql(&sql)
                .unwrap_or_else(|error| panic!("{context}: {}", error.message))
            else {
                panic!("{context}: expected query");
            };
            check_rows(&execution.batch, case, &context, &mut worst);
        }
    }
    eprintln!("TPC-H max relative error vs reference: {worst:e}");
}
