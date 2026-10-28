//! October 18 window extras against independent DuckDB 1.5.6 / PostgreSQL 17
//! goldens (tools/testing/window_extras_reference.py): PERCENT_RANK/CUME_DIST,
//! IGNORE/RESPECT NULLS, named WINDOW clauses, GROUPS frames, EXCLUDE, frames
//! starting FOLLOWING or ending PRECEDING, DECIMAL/REAL RANGE offsets, the
//! decided DESC NULLS LAST default, and statistical, boolean, ARG_MAX,
//! ordered-set, FILTER and DISTINCT window aggregates. No Assay output feeds
//! the goldens; every case's answer is independent of peer order.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const CASES: &str = include_str!("fixtures/window_extras/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("window_extras/analysis.sql"));
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-window-extras-{}-{}",
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
                // Small runs: buffered window partitions spill their chunks.
                sort_run_bytes: 512,
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
        .join("tests/fixtures/window_extras")
        .join(name)
}

fn tables() -> HashMap<String, PathBuf> {
    ["events", "empty_events"]
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

/// Floats within 1e-12 relative (the approved reference tolerance; DuckDB's
/// statistical folds are not always correctly rounded), NaN equal to NaN;
/// everything else exact. Decimal text compares by value.
fn cell_matches(actual: &Value, wanted: &Value) -> bool {
    let parse = |v: &Value| match v {
        Value::String(s) if s == "NaN" => Some(f64::NAN),
        Value::String(s) if s == "+inf" => Some(f64::INFINITY),
        Value::String(s) if s == "-inf" => Some(f64::NEG_INFINITY),
        Value::String(s) => s.parse().ok(),
        other => other.as_f64(),
    };
    if let (Some(a), Some(b)) = (actual.get("float"), wanted.get("float")) {
        let (Some(a), Some(b)) = (parse(a), parse(b)) else {
            return false;
        };
        return (a.is_nan() && b.is_nan())
            || a == b
            || (a.is_finite()
                && b.is_finite()
                && (a - b).abs() <= 1e-12_f64.max(a.abs().max(b.abs()) * 1e-12));
    }
    if let (Value::String(a), Value::String(b)) = (actual, wanted)
        && let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>())
        && a.contains('.')
        && b.contains('.')
    {
        // DECIMAL text: PostgreSQL may print a different number of zeros.
        return x == y;
    }
    actual == wanted
}

fn check_rows(batch: &RecordBatch, case: &Value, context: &str) {
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
    let actual = rows(batch);
    // Recorded decisions: PostgreSQL's answer, or for window MODE ties an
    // equivalent PostgreSQL query (grouped mode() per frame); DuckDB's own
    // answer stays in "rows" as the documented difference.
    let source = if case["expected_from"] == "postgresql"
        || case["expected_from"] == "postgresql_equivalent"
    {
        &case["expected_rows"]
    } else {
        &case["rows"]
    };
    let wanted: Vec<Vec<Value>> = serde_json::from_value(source.clone()).unwrap();
    assert_eq!(
        actual.len(),
        wanted.len(),
        "{context}: {actual:?} != {wanted:?}"
    );
    for (a, w) in actual.iter().zip(&wanted) {
        assert!(
            a.len() == w.len() && a.iter().zip(w).all(|(x, y)| cell_matches(x, y)),
            "{context}: {a:?} != {w:?}"
        );
    }
}

fn reference() -> Value {
    let expected: Value =
        serde_json::from_str(&fs::read_to_string(fixture_path("expected.json")).unwrap()).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["problems"], json!([]));
    assert!(
        expected["postgresql"].as_str().unwrap().starts_with("17."),
        "PostgreSQL outcomes recorded"
    );
    for (name, file) in expected["tables"].as_object().unwrap() {
        let size = fs::metadata(fixture_path(&format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), file["bytes"], "{name}.parquet");
    }
    expected
}

#[test]
fn reference_sources_match_cases() {
    let expected = reference();
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 94);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata["id"], case["id"]);
        assert_eq!(sql.trim(), case["sql"]);
        // Every PostgreSQL relation was verified by the generator.
        let pg = case["pg"].as_str().unwrap();
        assert!(
            pg == "agree" || pg == "rejects" || pg.starts_with("differs:"),
            "{}",
            case["id"]
        );
        if pg == "rejects" {
            assert_eq!(case["pg_outcome"], "error", "{}", case["id"]);
        }
        if case["expected_from"] == "postgresql_equivalent" {
            // The decision differs from DuckDB on these frames by construction.
            assert_eq!(case["reference_matches_expected"], false, "{}", case["id"]);
            assert!(case["expected_rows"].is_array(), "{}", case["id"]);
        }
    }
}

#[test]
fn independent_cases_match_at_all_worker_batch_and_spill_settings() {
    let expected = reference();
    let cases = expected["cases"].as_array().unwrap();
    let tables = tables();
    let fixture = Fixture::new();
    let mut settings = Vec::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                settings.push((workers, batch_size, spill, true));
            }
        }
    }
    settings.push((4, 7, false, false));
    settings.push((4, 7, true, false));
    let mut canonical: Vec<Option<Vec<Vec<Value>>>> = vec![None; cases.len()];
    let mut answered = 0;
    for (workers, batch_size, spill, optimized) in settings {
        let options = fixture.options(workers, batch_size, spill, optimized);
        for (index, case) in cases.iter().enumerate() {
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
                assert!(actual.contains(error), "{context}: {actual}");
                assert!(
                    !error.starts_with("unsupported:") || actual.starts_with("unsupported:"),
                    "{context}: typed refusal {actual}"
                );
            } else {
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                check_rows(&result.batch, case, &context);
                let bits = rows(&result.batch);
                match &canonical[index] {
                    None => canonical[index] = Some(bits),
                    Some(first) => assert_eq!(first, &bits, "{context}: deterministic bits"),
                }
                answered += 1;
            }
            fixture.clean_spill();
        }
    }
    assert!(answered > 14 * 60, "only {answered} answered executions");
}

/// The realistic TPC-H SF1 analyses (analysis.sql) against their frozen DuckDB
/// answers; run when the driver supplies the SF1 directory.
#[test]
#[ignore = "driver: set WINDOW_EXTRAS_SF1 to the TPC-H SF1 Parquet directory"]
fn sf1_analysis_against_frozen_reference() {
    let directory = PathBuf::from(std::env::var("WINDOW_EXTRAS_SF1").unwrap());
    let expected: Value =
        serde_json::from_str(&external_sf1::read("window_extras/analysis_expected.json")).unwrap();
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(
        cases.len(),
        ANALYSIS.split("-- case: ").skip(1).count(),
        "analysis cases"
    );
    let tables = ["lineitem", "orders", "customer", "nation"]
        .into_iter()
        .map(|name| (name.to_owned(), directory.join(format!("{name}.parquet"))))
        .collect::<HashMap<_, _>>();
    let options = Options {
        workers: 4,
        memory_bytes: 4 << 30,
        ..Options::default()
    };
    for case in cases {
        let sql = case["sql"].as_str().unwrap();
        let result = prepare(sql, &tables, &options)
            .and_then(|plan| execute(&plan, &options))
            .unwrap_or_else(|e| panic!("{}: {e}", case["id"]));
        check_rows(&result.batch, case, case["id"].as_str().unwrap());
    }
}

/// The command-line interface over Parquet returns the reference answers and
/// prints EXCLUDE frames and named windows in column names.
#[test]
fn cli_over_parquet_matches_reference() {
    let expected = reference();
    let cases = expected["cases"].as_array().unwrap();
    let fixture = Fixture::new();
    for id in [
        "named_shared",
        "exclude_group_ties_range",
        "ignore_nulls_carry_forward",
        "rank_dist_partition_desc",
        "stats_sliding",
        "distinct_windows",
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
        for (a, w) in actual.iter().zip(wanted) {
            for (x, y) in a.as_array().unwrap().iter().zip(w.as_array().unwrap()) {
                let x = match (x, y.get("float")) {
                    (Value::String(text), Some(_)) => json!({"float": text}),
                    (Value::Null, _) | (_, None) => x.clone(),
                    (other, Some(_)) => json!({"float": other}),
                };
                assert!(cell_matches(&x, y), "{id}: {a} != {w}");
            }
        }
    }
    let output = fixture.0.join("names.json");
    let mut cli = Command::new(env!("CARGO_BIN_EXE_assaydb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let request = json!({
        "sql": "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW), COUNT(*) OVER (ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES), SUM(x) OVER w FROM events WINDOW w AS (ORDER BY id) ORDER BY id LIMIT 1",
        "tables": tables(),
        "output": output
    });
    writeln!(cli.stdin.take().unwrap(), "{request}").unwrap();
    assert!(cli.wait().unwrap().success());
    let result: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    let names = result["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)",
            "COUNT(*) OVER (ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES)",
            "SUM(x) OVER (ORDER BY id)",
        ]
    );
}

/// MEDIAN/QUANTILE_CONT OVER DECIMAL interpolate in DOUBLE like the grouped
/// aggregates (maintainer decision 2026-10-17; DuckDB keeps DECIMAL(10,2)
/// and rounds 1.875 to 1.87). DuckDB's other answers are in the fixture.
#[test]
fn decimal_median_window_follows_grouped_decision() {
    let fixture = Fixture::new();
    let options = fixture.options(4, 7, true, true);
    let sql = "SELECT id, MEDIAN(d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS m FROM events WHERE id IN (1, 3, 4) ORDER BY id";
    let result = execute(&prepare(sql, &tables(), &options).unwrap(), &options).unwrap();
    assert_eq!(result.batch.column(1).data_type(), &DataType::Float64);
    assert_eq!(
        rows(&result.batch),
        vec![
            vec![json!(1), float_cell(1.5)],
            vec![json!(3), float_cell(1.875)],
            vec![json!(4), float_cell(2.25)]
        ]
    );
    fixture.clean_spill();
}

/// One 20,000-row partition, checked against naive models written here
/// (independent of the engine), with and without spilled partition chunks
/// and at several worker counts; plus the per-partition fold-work refusal.
#[test]
fn large_partition_matches_naive_models_and_fold_limit() {
    const N: i64 = 20_000;
    let fixture = Fixture::new();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("x", DataType::Int64, true),
    ]));
    let ids = (0..N).collect::<Vec<_>>();
    let keys = ids.iter().map(|i| i / 3).collect::<Vec<_>>();
    let xs = ids
        .iter()
        .map(|i| (i % 7 != 3).then_some((i * 37) % 101 - 50))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(Int64Array::from(xs.clone())),
        ],
    )
    .unwrap();
    let path = fixture.0.join("big.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("big".to_owned(), path)]);
    let n = N as usize;
    // Naive models.
    let excluded_sum = (0..n)
        .map(|i| {
            let (lo, hi) = (i.saturating_sub(5), (i + 5).min(n - 1));
            let values = (lo..=hi)
                .filter(|j| *j != i)
                .filter_map(|j| xs[j])
                .collect::<Vec<_>>();
            (!values.is_empty()).then(|| values.iter().sum::<i64>())
        })
        .collect::<Vec<_>>();
    let mut carried = Vec::with_capacity(n);
    let mut last = None;
    for x in &xs {
        last = x.or(last);
        carried.push(last);
    }
    // Peer groups of k = i / 3: GROUPS 1 PRECEDING .. 1 FOLLOWING counts.
    let mut per_key = HashMap::<i64, i64>::new();
    for key in &keys {
        *per_key.entry(*key).or_default() += 1;
    }
    let groups = keys
        .iter()
        .map(|g| {
            (g - 1..=g + 1)
                .map(|k| per_key.get(&k).copied().unwrap_or(0))
                .sum::<i64>()
        })
        .collect::<Vec<_>>();
    let percent = (0..n)
        .map(|i| (3 * keys[i]) as f64 / (n - 1) as f64)
        .collect::<Vec<_>>();
    let sql = "SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING EXCLUDE CURRENT ROW) AS s, LAST_VALUE(x IGNORE NULLS) OVER (ORDER BY id) AS carried, COUNT(*) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS g, PERCENT_RANK() OVER (ORDER BY k) AS pr, STDDEV_POP(x) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sd FROM big ORDER BY id";
    let mut first: Option<Vec<Vec<Value>>> = None;
    for (workers, batch_size, spill) in [(1, 8192, false), (4, 1000, true), (10, 7, true)] {
        let mut options = fixture.options(workers, batch_size, spill, true);
        if let Some(spill) = &mut options.spill {
            spill.sort_run_bytes = 64 << 10;
        }
        let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        let actual = rows(&result.batch);
        assert_eq!(actual.len(), n);
        for (i, row) in actual.iter().enumerate() {
            assert_eq!(row[1], json!(excluded_sum[i]), "row {i}");
            assert_eq!(row[2], json!(carried[i]), "row {i}");
            assert_eq!(row[3], json!(groups[i]), "row {i}");
            assert_eq!(row[4], float_cell(percent[i]), "row {i}");
            let window = xs[i.saturating_sub(3)..=i]
                .iter()
                .flatten()
                .map(|x| *x as f64)
                .collect::<Vec<_>>();
            let expected = (!window.is_empty()).then(|| {
                let mean = window.iter().sum::<f64>() / window.len() as f64;
                (window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / window.len() as f64)
                    .sqrt()
            });
            match (&row[5], expected) {
                (Value::Null, None) => {}
                (cell, Some(e)) => {
                    assert!(cell_matches(cell, &float_cell(e)), "row {i}: {cell} vs {e}")
                }
                (cell, None) => panic!("row {i}: {cell}"),
            }
        }
        match &first {
            None => first = Some(actual),
            Some(first) => assert_eq!(first, &actual, "deterministic bits"),
        }
        fixture.clean_spill();
    }
    // Whole-partition EXCLUDE refolds every frame: 20,000^2 frame rows exceed
    // max(2^28, 128 * rows) and are refused before any folding.
    let options = fixture.options(1, 8192, false, true);
    let error = prepare(
        "SELECT COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM big",
        &tables,
        &options,
    )
    .and_then(|plan| execute(&plan, &options))
    .err()
    .unwrap();
    assert!(
        error.starts_with(
            "unsupported: window frames need more than 268435456 frame-row accumulations"
        ),
        "{error}"
    );
}
