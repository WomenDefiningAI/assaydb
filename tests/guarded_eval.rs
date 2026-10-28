//! Guarded AND/OR evaluation: an error on a row that another operand of the
//! same AND (FALSE, or NULL in a row filter) or OR (TRUE) already decides is
//! not raised. Goldens are independent DuckDB 1.5.6 / PostgreSQL 17 outcomes
//! (tools/testing/guarded_eval_reference.py); no Assay output feeds them.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

const CASES: &str = include_str!("fixtures/guarded_eval/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-guarded-eval-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(path.join("spill")).unwrap();
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
impl Drop for Scratch {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/guarded_eval")
        .join(name)
}

fn tables() -> HashMap<String, PathBuf> {
    ["g", "h", "g_empty"]
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
        DataType::Int64 => matches!(duckdb, "BIGINT" | "HUGEINT" | "INTEGER"),
        DataType::Utf8 => duckdb == "VARCHAR",
        DataType::Boolean => duckdb == "BOOLEAN",
        DataType::Float64 => duckdb == "DOUBLE",
        DataType::Decimal128(_, scale) => duckdb
            .strip_prefix("DECIMAL(")
            .and_then(|t| t.strip_suffix(')'))
            .and_then(|t| t.split_once(','))
            .is_some_and(|(_, s)| s.trim().parse::<i8>().ok() == Some(*scale)),
        _ => false,
    }
}

/// Floats compare as binary64 values parsed from DuckDB's shortest repr,
/// except sums/averages of several terms, which use the approved Float
/// SUM/AVG reference tolerance (1e-12 relative).
fn cell_matches(actual: &Value, wanted: &Value) -> bool {
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
                || (a.is_finite() && b.is_finite() && (a - b).abs() <= b.abs() * 1e-12)
        }
        _ => actual == wanted,
    }
}

fn check_rows(batch: &RecordBatch, case: &Value, context: &str) {
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
    assert!(
        actual.len() == wanted.len()
            && actual.iter().zip(&wanted).all(
                |(a, w)| a.len() == w.len() && a.iter().zip(w).all(|(x, y)| cell_matches(x, y))
            ),
        "{context}: {actual:?} != {wanted:?}"
    );
}

fn original_reference_cases() -> Vec<Value> {
    let expected: Value =
        serde_json::from_str(&fs::read_to_string(fixture_path("expected.json")).unwrap()).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["problems"], json!([]));
    for (name, file) in expected["tables"].as_object().unwrap() {
        let size = fs::metadata(fixture_path(&format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), file["bytes"], "{name}.parquet");
    }
    let cases = expected["cases"].as_array().unwrap().clone();
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    // 100 guarded-evaluation cases plus 13 integration candidate 3 cases
    // (math domain errors, POWER overflow, DECIMAL % zero, TIMESTAMP casts).
    assert_eq!(cases.len(), 113);
    for (source, case) in sources.iter().zip(&cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "fixture metadata {key}");
        }
        assert_eq!(case["sql"], sql.trim());
        // The requirement this corpus pins: where both references answer the
        // same rows, Assay answers them too.
        if case["references"] == "both answer" {
            assert_eq!(case["expect"], "rows", "{}", case["id"]);
        }
    }
    cases
}

/// Explicit strict-cast variant of the frozen guard observations. BIGINT keeps
/// the checked conversion that these error/guard cases require; INTEGER now
/// follows the restored permissive public contract. Neither source fixture nor
/// reference rows are rewritten, and this is not an original-corpus result.
fn reference_cases() -> Vec<Value> {
    original_reference_cases()
        .into_iter()
        .map(|mut case| {
            case["sql"] = json!(
                case["sql"]
                    .as_str()
                    .unwrap()
                    .replace("AS INTEGER", "AS BIGINT")
            );
            if let Some(error) = case["error"].as_str() {
                case["error"] = json!(
                    error
                        .replace("type integer:", "type bigint:")
                        .replace("integer out of range", "bigint out of range")
                );
            }
            case
        })
        .collect()
}

#[test]
fn original_public_integer_guards_keep_their_reference_rows() {
    let cases = original_reference_cases();
    let tables = tables();
    let scratch = Scratch::new();
    // These original queries convert only selected, complete integer strings,
    // or their guards discard/decide the nonnumeric rows. Public prefix casts
    // therefore preserve their original answers. Keep the original SQL and
    // reference rows across all planner/worker/spill settings, independently of
    // the strict-BIGINT error-path variants above. The two Float-to-INTEGER row
    // cases are intentionally not in this list: truncation needs its own oracle.
    const ORIGINAL_TEXT_ROWS: [&str; 34] = [
        "where_null_conjunct_decides_filter",
        "where_cast_guard",
        "where_cast_guard_reversed",
        "where_cast_guard_two_conjuncts",
        "where_cast_or_guard",
        "where_cast_or_guard_reversed",
        "where_both_operands_fail_each_row_decided",
        "select_or_cast_guard",
        "select_not_and_cast_guard",
        "select_case_and_condition",
        "select_coalesce_guard",
        "select_coalesce_lazy_second_argument",
        "left_join_on_guard",
        "join_where_cross_table_guard",
        "having_key_cast_guard",
        "grouped_filter_predicate_cast_guard",
        "window_over_guarded_where",
        "where_null_conjunct_decides_reversed",
        "join_on_null_conjunct_decides",
        "having_null_conjunct_decides",
        "planner_in_subquery_guards_where_cast",
        "planner_not_in_subquery_guards_where_cast",
        "planner_in_subquery_value_guarded",
        "planner_not_in_subquery_value_guarded",
        "planner_join_key_guarded_on",
        "planner_join_key_guarded_on_right",
        "planner_join_key_guarded_comma",
        "planner_join_key_guarded_left_join",
        "planner_exists_key_guarded",
        "planner_not_exists_key_guarded",
        "planner_on_residual_guarded_by_where",
        "planner_on_residual_guarded_by_other_on",
        "planner_three_way_guard_on_last_table",
        "planner_or_with_subquery_guard",
    ];
    for (workers, batch_size, spill, optimized) in settings() {
        let options = scratch.options(workers, batch_size, spill, optimized);
        for id in ORIGINAL_TEXT_ROWS {
            let case = cases.iter().find(|c| c["id"] == id).unwrap();
            let sql = case["sql"].as_str().unwrap();
            assert_eq!(case["expect"], "rows", "{id}");
            assert!(sql.contains("AS INTEGER"), "{id}");
            assert!(!sql.contains("CAST(f AS INTEGER)"), "{id}");
            let result = prepare(sql, &tables, &options)
                .and_then(|p| execute(&p, &options))
                .unwrap();
            check_rows(&result.batch, case, id);
            scratch.clean_spill();
        }
    }
}

fn settings() -> Vec<(usize, usize, bool, bool)> {
    let mut settings = Vec::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 2, 3, 8192] {
            for spill in [false, true] {
                settings.push((workers, batch_size, spill, true));
            }
        }
    }
    for (workers, batch_size) in [(1, 8192), (4, 3), (10, 1)] {
        for spill in [false, true] {
            settings.push((workers, batch_size, spill, false));
        }
    }
    settings
}

#[test]
fn strict_bigint_guard_variant_matches_every_worker_batch_spill_and_optimizer_setting() {
    let cases = reference_cases();
    let tables = tables();
    let scratch = Scratch::new();
    let mut checked = (0, 0);
    for (workers, batch_size, spill, optimized) in settings() {
        let options = scratch.options(workers, batch_size, spill, optimized);
        for case in &cases {
            let sql = case["sql"].as_str().unwrap();
            let context = format!(
                "{} workers={workers} batch={batch_size} spill={spill} optimized={optimized}",
                case["id"]
            );
            let result = prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options));
            if case["expect"] == "error" {
                let actual = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: expected an error"));
                // A spilled join appends where it failed (unchanged behaviour).
                let wanted = case["error"].as_str().unwrap();
                let cause = actual
                    .split_once("; external join")
                    .map_or(actual.as_str(), |(cause, _)| cause);
                assert_eq!(cause, wanted, "{context}: {actual}");
                checked.1 += 1;
            } else {
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                check_rows(&result.batch, case, &context);
                checked.0 += 1;
            }
            scratch.clean_spill();
        }
    }
    assert_eq!(checked.0 + checked.1, settings().len() * cases.len());
}

/// The same guarded and unguarded cases through the CLI over Parquet.
#[test]
fn cli_strict_bigint_guard_variant_answers_guards_and_raises_unguarded_errors() {
    let scratch = Scratch::new();
    let cases = reference_cases();
    let pick = |id: &str| cases.iter().find(|c| c["id"] == id).unwrap().clone();
    for (id, threads, batch) in [
        ("where_decimal_zero_guard_reversed", 4, 2),
        ("where_null_conjunct_decides_filter", 1, 8192),
        ("join_where_cross_table_guard", 4, 3),
        ("having_aggregate_guard_reversed", 10, 1),
        ("select_or_cast_guard", 1, 1),
        ("where_unguarded_cast", 4, 2),
        ("where_both_operands_fail_left_error_reversed", 10, 3),
        ("filter_argument_decimal_division", 4, 8192),
    ] {
        let case = pick(id);
        let output = scratch.0.join(format!("{id}.json"));
        let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let request = json!({
            "sql": case["sql"], "tables": tables(), "output": output,
            "threads": threads, "batch_size": batch,
        });
        child
            .stdin
            .take()
            .unwrap()
            .write_all(request.to_string().as_bytes())
            .unwrap();
        let result = child.wait_with_output().unwrap();
        if case["expect"] == "error" {
            assert_eq!(result.status.code(), Some(1), "{id}");
            let failure = String::from_utf8(result.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .find(|event| event["event"] == "failure")
                .expect("failure event");
            assert_eq!(failure["message"], case["error"], "{id}");
            assert!(!output.exists(), "{id}");
        } else {
            assert!(result.status.success(), "{id}: {result:?}");
            let written: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
            let wanted = case["rows"].as_array().unwrap();
            let actual = written["rows"].as_array().unwrap();
            assert_eq!(actual.len(), wanted.len(), "{id}: {actual:?}");
            for (a, w) in actual.iter().zip(wanted) {
                for (x, y) in a.as_array().unwrap().iter().zip(w.as_array().unwrap()) {
                    // The CLI prints DECIMAL/DOUBLE as text.
                    let same = match (x, y.get("float")) {
                        (Value::String(text), Some(float)) => {
                            let wanted: f64 = float.as_str().unwrap().parse().unwrap();
                            let got: f64 = text.parse().unwrap();
                            got == wanted || (got - wanted).abs() <= wanted.abs() * 1e-12
                        }
                        _ => x == y,
                    };
                    assert!(same, "{id}: {actual:?} != {wanted:?}");
                }
            }
        }
    }
}

/// Unguarded failures raise exactly the error the eager evaluator raised
/// before guarded evaluation existed, at every setting: the first failing row
/// in input order, and on that row the left operand's error.
// INTEGER is permissive under the public conversion contract. Use the
// separately introduced checked BIGINT casts to retain error-order coverage.
#[test]
fn unguarded_errors_keep_their_first_row_and_left_operand() {
    let scratch = Scratch::new();
    let tables = tables();
    for (sql, error) in [
        ("SELECT id FROM g WHERE 10 / x > 1", "division by zero"),
        (
            "SELECT id FROM g WHERE CAST(s AS BIGINT) > 0 AND 10 / x > 0",
            "invalid input syntax for type bigint: \"abc\"",
        ),
        (
            "SELECT id FROM g WHERE 10 / x > 0 AND CAST(s AS BIGINT) > 0",
            "division by zero",
        ),
        (
            "SELECT id FROM g WHERE id > 2 AND (CAST(s AS BIGINT) > 0 OR 10 / x < 0)",
            "invalid input syntax for type bigint: \"x9\"",
        ),
        (
            "SELECT id, CAST(f AS BIGINT) > 0 AND x <> 0 AS r FROM g",
            "bigint out of range",
        ),
        (
            "SELECT k FROM g GROUP BY k HAVING 10 / sum(x) > 0 AND count(*) > 0",
            "division by zero",
        ),
    ] {
        for (workers, batch_size, spill, optimized) in settings() {
            let options = scratch.options(workers, batch_size, spill, optimized);
            let actual = prepare(sql, &tables, &options)
                .and_then(|plan| execute(&plan, &options))
                .err()
                .unwrap_or_else(|| panic!("{sql}: expected an error"));
            assert_eq!(
                actual, error,
                "{sql} workers={workers} batch={batch_size} spill={spill} optimized={optimized}"
            );
        }
    }
}
