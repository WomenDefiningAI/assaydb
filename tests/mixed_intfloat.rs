//! Mixed Int/Float promotion (Helen, October 15): reachable Int arms of a
//! Float CASE/COALESCE become DOUBLE as in DuckDB 1.5.6. Answers come from
//! tools/testing/mixed_intfloat_reference.py; Float cells compare bit for bit.
//! Cases marked `assay_refusal`/`assay_error` are the scoped consumer guards
//! (INTEGER cast of a promoted value; the REAL remainder guard was removed when
//! `%` became fmod on October 17); `assay_rows` marks
//! unpromoted controls that intentionally keep exact Int/Float comparison.
use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::Value;

const EXPECTED: &str = include_str!("fixtures/mixed_intfloat/expected.json");

fn session(workers: usize, batch_size: usize, optimized: bool, setup: &str) -> Session {
    let mut session = Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    for sql in setup.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        session.execute_sql(sql).unwrap();
    }
    session
}

fn sources(text: &str) -> Vec<(Value, String)> {
    text.split("-- case: ")
        .skip(1)
        .map(|section| {
            let (metadata, sql) = section.split_once('\n').unwrap();
            let sql = sql
                .lines()
                .filter(|line| !line.starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            (
                serde_json::from_str(metadata).unwrap(),
                sql.trim().trim_end_matches(';').trim().to_string(),
            )
        })
        .collect()
}

/// `assay_rows` are plain JSON values; convert to the reference cell encoding.
fn plain(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Bool(b) => serde_json::json!({ "bool": b }),
        Value::Number(n) if n.is_i64() => serde_json::json!({ "int": n }),
        Value::Number(n) => {
            let x = n.as_f64().unwrap();
            serde_json::json!({ "float": x.to_string(), "bits": format!("{:016x}", x.to_bits()) })
        }
        Value::String(s) => serde_json::json!({ "text": s }),
        _ => panic!("unexpected plain value {value}"),
    }
}

fn check_cell(array: &dyn Array, row: usize, expected: &Value, context: &str) {
    if expected.is_null() {
        assert!(array.is_null(row), "{context}: expected NULL");
        return;
    }
    assert!(
        !array.is_null(row),
        "{context}: unexpected NULL, want {expected}"
    );
    match array.data_type() {
        DataType::Float64 => {
            let actual = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            let bits = expected["bits"]
                .as_str()
                .unwrap_or_else(|| panic!("{context}: Float result, want {expected}"));
            assert_eq!(
                format!("{:016x}", actual.to_bits()),
                bits,
                "{context}: {actual:e} vs {expected}"
            );
        }
        DataType::Int64 => {
            let actual = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            assert_eq!(Some(actual), expected["int"].as_i64(), "{context}");
        }
        DataType::Decimal128(_, 0) => {
            let actual = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row);
            assert_eq!(
                Some(actual),
                expected["int"].as_i64().map(i128::from),
                "{context}"
            );
        }
        DataType::Boolean => {
            let actual = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            assert_eq!(Some(actual), expected["bool"].as_bool(), "{context}");
        }
        DataType::Utf8 => {
            let actual = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            assert_eq!(Some(actual), expected["text"].as_str(), "{context}");
        }
        other => panic!("{context}: unexpected result type {other:?}"),
    }
}

fn check_rows(batch: &RecordBatch, rows: &[Value], context: &str) {
    assert_eq!(batch.num_rows(), rows.len(), "{context}: row count");
    for (r, row) in rows.iter().enumerate() {
        let row = row.as_array().unwrap();
        assert_eq!(batch.num_columns(), row.len(), "{context}: column count");
        for (c, expected) in row.iter().enumerate() {
            check_cell(
                batch.column(c).as_ref(),
                r,
                expected,
                &format!("{context} row {r} column {c}"),
            );
        }
    }
}

fn run(session: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match session.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("expected a query: {sql}"),
    }
}

#[test]
fn promotion_matches_duckdb_with_scoped_consumer_guards() {
    let expected: Value = serde_json::from_str(EXPECTED).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let setup = include_str!("fixtures/mixed_intfloat/setup.sql");
    assert_eq!(expected["setup_sql"], setup.trim());
    let cases = expected["cases"].as_array().unwrap();
    let sources = sources(include_str!("fixtures/mixed_intfloat/cases.sql"));
    assert!((40..=80).contains(&cases.len()), "{}", cases.len());
    assert_eq!(sources.len(), cases.len());
    for ((metadata, sql), case) in sources.iter().zip(cases) {
        assert_eq!(case["id"], metadata["id"]);
        assert_eq!(case["sql"], sql.as_str());
    }
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                let mut s = session(workers, batch_size, optimized, setup);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let context = format!(
                        "{} workers={workers} batch={batch_size} optimized={optimized}",
                        case["id"]
                    );
                    let result = run(&mut s, sql);
                    if let Some(refusal) = case["assay_refusal"].as_str() {
                        let error = result.err().unwrap_or_else(|| panic!("{context}: ok"));
                        assert!(error.starts_with("unsupported: "), "{context}: {error}");
                        assert!(error.contains(refusal), "{context}: {error}");
                        continue;
                    }
                    if let Some(message) = case["assay_error"].as_str() {
                        let error = result.err().unwrap_or_else(|| panic!("{context}: ok"));
                        assert!(!error.starts_with("unsupported:"), "{context}: {error}");
                        assert!(error.contains(message), "{context}: {error}");
                        continue;
                    }
                    let batch = result.unwrap_or_else(|e| panic!("{context}: {e}\n{sql}"));
                    let rows = match case["assay_rows"].as_array() {
                        Some(rows) => rows
                            .iter()
                            .map(|row| {
                                Value::Array(row.as_array().unwrap().iter().map(plain).collect())
                            })
                            .collect(),
                        None => {
                            assert_eq!(case["status"], "ok", "{context}");
                            case["rows"].as_array().unwrap().clone()
                        }
                    };
                    check_rows(&batch, &rows, &context);
                }
            }
        }
    }
    // Scope: every DuckDB error in the fixture is an INTEGER cast out of range,
    // the class Assay's guard reproduces; DuckDB returns rows for the others.
    for case in cases {
        if case["status"] == "error" {
            let message = case["assay_error"].as_str().unwrap();
            if message.starts_with("INTEGER out of range") {
                assert!(
                    case["error"]
                        .as_str()
                        .unwrap()
                        .contains("out of range for the destination type INT64"),
                    "{case}"
                );
            }
        }
    }
}

#[test]
fn order_line_discount_analysis_matches_hand_fixed_answers() {
    let expected: Value = serde_json::from_str(EXPECTED).unwrap();
    let setup = include_str!("fixtures/mixed_intfloat/analysis_setup.sql");
    assert_eq!(expected["analysis_setup_sql"], setup.trim());
    let cases = expected["analysis"].as_array().unwrap();
    let sources = sources(include_str!("fixtures/mixed_intfloat/analysis.sql"));
    assert_eq!(sources.len(), cases.len());
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut s = session(workers, batch_size, true, setup);
            for ((metadata, sql), case) in sources.iter().zip(cases) {
                assert_eq!(case["id"], metadata["id"]);
                let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
                // Hand-fixed answers, converted with the same cell encoding.
                let rows = metadata["expected_rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| Value::Array(row.as_array().unwrap().iter().map(plain).collect()))
                    .collect::<Vec<_>>();
                let batch = run(&mut s, sql).unwrap_or_else(|e| panic!("{context}: {e}"));
                check_rows(&batch, &rows, &context);
            }
        }
    }
}
