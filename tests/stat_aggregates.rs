//! Statistical and boolean aggregates (VARIANCE/STDDEV, COVAR/CORR, REGR_*,
//! BOOL_AND/BOOL_OR, COUNT_IF, ARG_MAX/ARG_MIN) against independent DuckDB
//! 1.5.6 goldens and exact rational values
//! (tools/testing/stat_aggregates_reference.py). No Assay output feeds them.
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

const CASES: &str = include_str!("fixtures/stat_aggregates/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("stat_aggregates/analysis.sql"));
const TABLES: [&str; 7] = [
    "stats",
    "empty",
    "regions",
    "nonfinite",
    "pairs",
    "wide",
    "big",
];
/// Relative tolerance against DuckDB (the approved Float aggregate tolerance),
/// with the same absolute floor near zero.
const DUCKDB_TOLERANCE: f64 = 1e-12;
/// Against exact rational values Assay must be within a few ulps.
const EXACT_TOLERANCE: f64 = 1e-14;
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-stat-aggregates-{}-{}",
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
        .join("tests/fixtures/stat_aggregates")
        .join(name)
}

fn expected(name: &str) -> Value {
    if name == "analysis_expected.json" {
        return serde_json::from_str(&external_sf1::read(
            "stat_aggregates/analysis_expected.json",
        ))
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
/// DuckDB widens COUNT_IF to HUGEINT and REGR_COUNT is UINTEGER; Assay returns
/// its 64-bit INTEGER with the same values.
fn same_type(assay: &DataType, duckdb: &str) -> bool {
    match assay {
        DataType::Null => true,
        DataType::Int64 => matches!(duckdb, "BIGINT" | "HUGEINT" | "INTEGER" | "UINTEGER"),
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

/// Relative error of one REAL cell (0 for equal, including both zero).
fn relative_error(actual: f64, wanted: f64) -> f64 {
    if actual == wanted {
        0.0
    } else {
        (actual - wanted).abs() / wanted.abs()
    }
}

fn cell_matches(actual: &Value, wanted: &Value, exact: bool, worst: &mut f64) -> bool {
    match (actual.get("float"), wanted.get("float")) {
        (Some(a), Some(b)) => {
            let (a, b) = (parse_float(a), parse_float(b));
            if a.is_nan() || b.is_nan() || a.is_infinite() || b.is_infinite() {
                return a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan());
            }
            let error = relative_error(a, b);
            if error.is_finite() {
                *worst = worst.max(error);
            }
            if exact {
                a == b || error <= EXACT_TOLERANCE
            } else {
                (a - b).abs() <= DUCKDB_TOLERANCE.max(b.abs() * DUCKDB_TOLERANCE)
            }
        }
        _ => actual == wanted,
    }
}

fn check_rows(batch: &RecordBatch, case: &Value, context: &str, worst: &mut f64) {
    assert_eq!(case["reference_outcome"], "rows", "{context}");
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
    let actual = rows(batch);
    let exact = case.get("exact_rows").is_some();
    // Exact rational values where DuckDB's own error exceeds the tolerance;
    // PostgreSQL's answer where a recorded decision follows PostgreSQL.
    let source = if exact {
        "exact_rows"
    } else if case.get("expected_rows").is_some() {
        "expected_rows"
    } else {
        "rows"
    };
    let wanted: Vec<Vec<Value>> = serde_json::from_value(case[source].clone()).unwrap();
    assert_eq!(
        actual.len(),
        wanted.len(),
        "{context}: {actual:?} != {wanted:?}"
    );
    // Ties: DuckDB may return any tied argument; Assay returns the first in
    // canonical input order, which the generator lists first.
    let permitted = case.get("permitted").and_then(Value::as_array);
    for (row, (a, w)) in actual.iter().zip(&wanted).enumerate() {
        assert_eq!(a.len(), w.len(), "{context}");
        for (column, (x, y)) in a.iter().zip(w).enumerate() {
            if let Some(permitted) = permitted
                && column == a.len() - 1
            {
                let choices = permitted[row][1].as_array().unwrap();
                assert_eq!(permitted[row][0], a[0], "{context}: tie group order");
                assert!(choices.contains(x), "{context}: {x} not among {choices:?}");
                assert_eq!(x, &choices[0], "{context}: canonical first tie");
                assert!(choices.contains(y), "{context}: DuckDB {y} not permitted");
                continue;
            }
            assert!(
                cell_matches(x, y, exact, worst),
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
    assert_eq!(cases.len(), 72);
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
    let (mut spilled, mut parallel, mut worst_duckdb, mut worst_exact) = (0, 0, 0.0_f64, 0.0_f64);
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
                let worst = if case.get("exact_rows").is_some() {
                    &mut worst_exact
                } else {
                    &mut worst_duckdb
                };
                check_rows(&result.batch, case, &context, worst);
                let observed = float_bits(&result.batch);
                let first = bits
                    .entry(id.to_owned())
                    .or_insert_with(|| observed.clone());
                assert_eq!(first, &observed, "{context}: REAL bits depend on settings");
                spilled += usize::from(spill && result.metrics.external_aggregate_partitions > 0);
                parallel += usize::from(result.metrics.parallel_aggregate_batches > 0);
            }
            fixture.clean_spill();
        }
    }
    // Aggregate helpers own whole moment states (never fragments) in some runs.
    assert!(parallel > 0, "no run used aggregate helpers");
    eprintln!("max relative error: vs DuckDB {worst_duckdb:e}; vs exact rational {worst_exact:e}");
    assert!(worst_duckdb <= DUCKDB_TOLERANCE && worst_exact <= EXACT_TOLERANCE);
    // Forced spill really routes grouped aggregation through partitions.
    assert!(spilled > 200, "only {spilled} spilled executions");
}

/// DuckDB's own double-precision error exceeds the tolerance on the
/// ill-conditioned fixture groups; Assay is compared with exact values there.
#[test]
fn exact_rational_references_document_duckdb_error() {
    let cases = reference_cases();
    let grouped = cases
        .iter()
        .find(|c| c["id"] == "many_batches_grouped")
        .unwrap();
    let error = grouped["duckdb_max_relative_error_vs_exact"]
        .as_f64()
        .unwrap();
    assert!(error > DUCKDB_TOLERANCE && error < 1e-9, "{error}");
    let wide = cases
        .iter()
        .find(|c| c["id"] == "exact_beyond_double_precision")
        .unwrap();
    // DuckDB rounds 2^53+1, 2^53+3, 2^53+5 to DOUBLE before VAR_POP.
    assert_eq!(wide["rows"][0][0], json!({"float": "4.0"}));
    assert_eq!(
        wide["exact_rows"][0][0],
        json!({"float": "2.6666666666666665"})
    );
}

/// Hand-checked arithmetic, independent of both the generator and DuckDB.
#[test]
fn handwritten_controls() {
    let fixture = Fixture::new();
    let options = fixture.options(4, 3, false, true);
    let run = |sql: &str| {
        let result = execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
        rows(&result.batch)
    };
    let data = "(SELECT 1 AS x, 3 AS y UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 4 UNION ALL SELECT 4, 8 UNION ALL SELECT 5, 10) t";
    // x̄ = 3, ȳ = 6, Sxx = 10, Sxy = 17, Syy = 34.
    assert_eq!(
        run(&format!(
            "SELECT VAR_SAMP(x), VAR_POP(x), COVAR_SAMP(y, x), REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_SXX(y, x), REGR_SXY(y, x), REGR_SYY(y, x), REGR_AVGX(y, x), REGR_AVGY(y, x), REGR_COUNT(y, x) FROM {data}"
        )),
        vec![vec![
            float_cell(2.5),
            float_cell(2.0),
            float_cell(4.25),
            float_cell(1.7),
            float_cell(0.9),
            float_cell(10.0),
            float_cell(17.0),
            float_cell(34.0),
            float_cell(3.0),
            float_cell(6.0),
            json!(5),
        ]]
    );
    // 1e9 + {4, 7, 13, 16}: the textbook one-pass formula loses every digit.
    let shifted = "(SELECT CAST(1000000004 AS DOUBLE) AS v UNION ALL SELECT CAST(1000000007 AS DOUBLE) UNION ALL SELECT CAST(1000000013 AS DOUBLE) UNION ALL SELECT CAST(1000000016 AS DOUBLE)) t";
    assert_eq!(
        run(&format!("SELECT VAR_SAMP(v), VAR_POP(v) FROM {shifted}")),
        vec![vec![float_cell(30.0), float_cell(22.5)]]
    );
    // Booleans: false < true, NULL ignored; COUNT_IF is NULL without input.
    let flags = "(SELECT 1 AS k, TRUE AS b UNION ALL SELECT 1, NULL UNION ALL SELECT 2, FALSE UNION ALL SELECT 2, TRUE UNION ALL SELECT 3, NULL) t";
    assert_eq!(
        run(&format!(
            "SELECT k, BOOL_AND(b), BOOL_OR(b), COUNT_IF(b) FROM {flags} GROUP BY k ORDER BY k"
        )),
        vec![
            vec![json!(1), json!(true), json!(true), json!(1)],
            vec![json!(2), json!(false), json!(true), json!(1)],
            vec![json!(3), Value::Null, Value::Null, Value::Null],
        ]
    );
    // Degenerate groups (one pair; constant X) are NULL beside ordinary
    // groups, and the whole query succeeds (decision 2026-10-17).
    let mixed = "(SELECT 1 AS k, 1 AS x, 1 AS y UNION ALL SELECT 1, 2, 3 UNION ALL SELECT 2, 7, 5 UNION ALL SELECT 2, NULL, 6 UNION ALL SELECT 3, 4, 1 UNION ALL SELECT 3, 4, 2) t";
    assert_eq!(
        run(&format!(
            "SELECT k, COUNT(*), REGR_COUNT(y, x), CORR(y, x), REGR_SLOPE(y, x), REGR_INTERCEPT(y, x) FROM {mixed} GROUP BY k ORDER BY k"
        )),
        vec![
            vec![
                json!(1),
                json!(2),
                json!(2),
                float_cell(1.0),
                float_cell(2.0),
                float_cell(-1.0)
            ],
            vec![
                json!(2),
                json!(2),
                json!(1),
                Value::Null,
                Value::Null,
                Value::Null
            ],
            vec![
                json!(3),
                json!(2),
                json!(2),
                Value::Null,
                Value::Null,
                Value::Null
            ],
        ]
    );
    assert_eq!(
        run(&format!(
            "SELECT k, COUNT(*) FROM {mixed} GROUP BY k HAVING CORR(y, x) IS NULL ORDER BY k"
        )),
        vec![vec![json!(2), json!(2)], vec![json!(3), json!(2)]]
    );
    assert_eq!(
        run(&format!(
            "SELECT k, CORR(y, x) FROM {mixed} GROUP BY k HAVING REGR_COUNT(y, x) > 1 AND COUNT(*) = 2 ORDER BY k"
        )),
        vec![vec![json!(1), float_cell(1.0)], vec![json!(3), Value::Null]]
    );
    // ARG_MAX ties: the first row in input order; NULL arguments are skipped.
    let ties = "(SELECT 'a' AS s, 2 AS v UNION ALL SELECT NULL, 3 UNION ALL SELECT 'b', 2 UNION ALL SELECT 'c', 1) t";
    assert_eq!(
        run(&format!(
            "SELECT ARG_MAX(s, v), ARG_MIN(s, v), MAX_BY(v, s), MIN_BY(v, s) FROM {ties}"
        )),
        vec![vec![json!("a"), json!("c"), json!(1), json!(2)]]
    );
    // ARG_MAX returns the winning row's REAL argument bit for bit (-0.0 stays
    // -0.0, as in DuckDB), while value comparison treats -0.0 as 0.0.
    let sql = "SELECT ARG_MAX(v, k), MIN_BY(k, w) FROM (SELECT -CAST(0 AS DOUBLE) AS v, 1 AS k, CAST(0 AS DOUBLE) AS w UNION ALL SELECT CAST(5 AS DOUBLE), 0, -CAST(0 AS DOUBLE)) t";
    let result = execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
    let value = result
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(
        rows(&result.batch)[0][1],
        json!(1),
        "0.0 and -0.0 tie: first row"
    );
}

fn moment_query_options(fixture: &Fixture, memory: usize, spill: bool) -> Options {
    Options {
        memory_bytes: memory,
        ..fixture.options(4, 1024, spill, true)
    }
}

/// Moment state is charged to the query budget per group and is spill-safe:
/// a budget too small for every group's state fails without spill and gives
/// bit-identical results through forced external aggregation.
#[test]
fn moment_state_is_budgeted_and_spills() {
    let fixture = Fixture::new();
    let count = 40_000_i64;
    let keys: Vec<i64> = (0..count).map(|i| (i * 7919) % 20_000).collect();
    let xs: Vec<i64> = (0..count).map(|i| (i * 31) % 1000 - 500).collect();
    let fs: Vec<f64> = (0..count).map(|i| 1e6 + (i % 97) as f64 * 0.25).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(xs)),
            Arc::new(Float64Array::from(fs)),
        ],
    )
    .unwrap();
    let path = fixture.0.join("groups.parquet");
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create_new(&path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_owned(), path)]);
    let sql = "SELECT k, VAR_POP(x), COVAR_POP(f, x), REGR_SXY(f, f), STDDEV_POP(f), ARG_MAX(x, f) FROM t GROUP BY k ORDER BY k";
    let run = |options: &Options| prepare(sql, &tables, options).and_then(|p| execute(&p, options));
    let reference = run(&moment_query_options(&fixture, 256 << 20, false)).unwrap();
    assert_eq!(reference.batch.num_rows(), 20_000);
    let tight = 6 << 20;
    let error = run(&moment_query_options(&fixture, tight, false))
        .err()
        .expect("state for 20,000 groups exceeds the budget");
    assert!(error.starts_with("resource-limit:"), "{error}");
    fixture.clean_spill();
    let spilled = run(&moment_query_options(&fixture, tight, true)).unwrap();
    assert!(spilled.metrics.external_aggregate_partitions > 1);
    assert_eq!(rows(&spilled.batch), rows(&reference.batch));
    assert_eq!(float_bits(&spilled.batch), float_bits(&reference.batch));
    fixture.clean_spill();
}

/// The command-line interface over Parquet returns the same answers.
#[test]
fn cli_over_parquet_matches_reference() {
    let cases = reference_cases();
    let fixture = Fixture::new();
    for id in [
        "grouped_correlation",
        "boolean_grouped",
        "arg_extremes_grouped",
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
        let wanted = case["rows"].as_array().unwrap();
        assert_eq!(actual.len(), wanted.len(), "{id}");
        let mut worst = 0.0;
        for (a, w) in actual.iter().zip(wanted) {
            for (x, y) in a.as_array().unwrap().iter().zip(w.as_array().unwrap()) {
                let x = match (x, y.get("float")) {
                    (Value::String(text), Some(_)) => json!({"float": text}),
                    (Value::Null, _) | (_, None) => x.clone(),
                    (other, Some(_)) => json!({"float": other}),
                };
                assert!(cell_matches(&x, y, false, &mut worst), "{id}: {a} != {w}");
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

/// Integration candidate 2: aggregate FILTER (#100) over the statistical and
/// boolean aggregates. The first argument is lowered to `CASE WHEN c THEN x
/// END`; two-argument aggregates skip rows where any argument is NULL, so the
/// filtered rows drop out, and every other argument must be total because it is
/// still evaluated on filtered-out rows. Expected values are DuckDB 1.5.6's.
#[test]
fn filter_on_statistical_and_boolean_aggregates() {
    let fixture = Fixture::new();
    let data = "(SELECT 1 AS g, 1 AS x, 3 AS y UNION ALL SELECT 1, 2, 5 UNION ALL SELECT 1, 3, 4 UNION ALL SELECT 2, 4, 8 UNION ALL SELECT 2, 5, 10 UNION ALL SELECT 1, 6, 9 UNION ALL SELECT 2, 7, 6) t";
    for (workers, batch_size, spill) in [(1, 8192, false), (4, 1, true), (10, 3, false)] {
        let options = fixture.options(workers, batch_size, spill, true);
        let sql = format!(
            "SELECT CORR(y,x) FILTER (WHERE g=1), REGR_SLOPE(y,x) FILTER (WHERE g=1), COVAR_POP(y,x) FILTER (WHERE x>1), STDDEV_SAMP(x) FILTER (WHERE g=1), ARG_MAX(y, x) FILTER (WHERE g=2), MIN_BY(x, y) FILTER (WHERE g=2), BOOL_AND(x>3) FILTER (WHERE g=2), COUNT_IF(y>4) FILTER (WHERE g=1) FROM {data}"
        );
        let result = execute(&prepare(&sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
        let row = &rows(&result.batch)[0];
        for (index, wanted) in [
            0.938743438158172,
            1.1428571428571428,
            1.8333333333333333,
            2.160246899469287,
        ]
        .into_iter()
        .enumerate()
        {
            let actual = parse_float(&row[index]["float"]);
            assert!(
                relative_error(actual, wanted) <= 1e-12,
                "column {index}: {actual} vs DuckDB {wanted} (workers={workers} batch={batch_size})"
            );
        }
        assert_eq!(
            row[4..].to_vec(),
            vec![json!(6), json!(7), json!(true), json!(2)],
            "workers={workers} batch={batch_size}"
        );
        // A second argument that may fail is refused like a fallible first one.
        let sql =
            format!("SELECT CORR(y, CAST(x AS DECIMAL(10,2))) FILTER (WHERE g=1) FROM {data}");
        let error = prepare(&sql, &HashMap::new(), &options).err().unwrap();
        assert!(
            error.contains("aggregate FILTER over an argument that may fail"),
            "{error}"
        );
    }
}
