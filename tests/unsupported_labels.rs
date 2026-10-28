//! Reporting-only boundaries: refused queries remain refused, successes stay exact.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    types::{Type, decimal_text},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

#[path = "support/unsupported_labels_input.rs"]
mod input;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-labels-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Keep failing test inputs and wire replies available for diagnosis.
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn cases() -> Vec<Value> {
    include_str!("fixtures/unsupported_labels/cases.sql")
        .split("-- case: ")
        .skip(1)
        .map(|section| {
            let (metadata, sql) = section.split_once('\n').unwrap();
            let mut case: Value = serde_json::from_str(metadata).unwrap();
            case["sql"] = json!(sql.trim());
            date_toolkit_transition(&mut case);
            case
        })
        .collect()
}

// Preserve the label card's original receipt while asserting the four newly
// authorized transitions against the exact DuckDB values already recorded.
fn date_toolkit_transition(case: &mut Value) {
    if matches!(
        case["id"].as_str().unwrap(),
        "date_plus_int" | "int_plus_date" | "date_minus_int" | "date_difference"
    ) {
        assert_eq!(case["assay_status"], "unsupported");
        assert_eq!(
            case["assay_message"],
            "unsupported: expected numeric type, got Date"
        );
        case["assay_status"] = json!("ok");
        let object = case.as_object_mut().unwrap();
        object.remove("assay_message");
        object.remove("difference");
    }
}

fn query_rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) {
                        return Value::Null;
                    }
                    match array.data_type() {
                        DataType::Date32 => {
                            let days = array
                                .as_any()
                                .downcast_ref::<Date32Array>()
                                .unwrap()
                                .value(row);
                            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                + chrono::Duration::days(i64::from(days));
                            json!(date.to_string())
                        }
                        DataType::Null => Value::Null,
                        DataType::Int64 => json!(
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Float64 => json!(
                            array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Boolean => json!(
                            array
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Utf8 => json!(
                            array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Decimal128(_, scale) => json!(decimal_text(
                            array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        other => panic!("unexpected fixture result {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn labels_and_successes_are_independent_of_workers_and_batches() {
    let fixture = Fixture::new();
    let cases = cases();
    assert_eq!(cases.len(), 44);
    let mut successful_rows = HashMap::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let mut session = Session::new(&fixture.0, options.clone(), Limits::default()).unwrap();
            for case in &cases {
                let sql = case["sql"].as_str().unwrap();
                let native =
                    prepare(sql, &HashMap::new(), &options).and_then(|p| execute(&p, &options));
                let reply = session.execute_sql(sql);
                if case["assay_status"] == "ok" {
                    let native = native.unwrap_or_else(|e| panic!("{sql}: {e}"));
                    let Output::Query { execution, .. } = reply.unwrap() else {
                        panic!("query expected")
                    };
                    let rows = query_rows(&native.batch);
                    assert_eq!(rows, query_rows(&execution.batch), "{sql}");
                    let baseline = successful_rows
                        .entry(case["id"].as_str().unwrap())
                        .or_insert_with(|| rows.clone());
                    assert_eq!(
                        &rows, baseline,
                        "workers={workers} batch={batch_size}: {sql}"
                    );
                } else {
                    assert_eq!(
                        native.err().unwrap(),
                        case["assay_message"].as_str().unwrap(),
                        "{sql}"
                    );
                    let error = reply.err().unwrap();
                    assert_eq!(
                        error.message,
                        case["assay_message"].as_str().unwrap(),
                        "{sql}"
                    );
                    assert_eq!(
                        error.kind,
                        if case["assay_status"] == "unsupported" {
                            "unsupported"
                        } else {
                            "engine"
                        },
                        "{sql}"
                    );
                }
            }
        }
    }
}

fn cli_failure(fixture: &Fixture, sql: &str, tables: Value) -> Value {
    let output_path = fixture.0.join("result.json");
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "{}",
        json!({"sql":sql,"tables":tables,"output":output_path})
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success(), "{sql}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    fs::write(fixture.0.join("cli-events.jsonl"), &stdout).unwrap();
    let events: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        !events
            .iter()
            .any(|e| e["event"] == "done" || e["event"] == "sample"),
        "{sql}: {events:?}"
    );
    assert!(!output_path.exists());
    let failures: Vec<_> = events
        .into_iter()
        .filter(|e| e["event"] == "failure")
        .collect();
    assert_eq!(failures.len(), 1);
    failures.into_iter().next().unwrap()
}

#[test]
fn cli_failure_and_session_reply_expose_exact_class_and_message() {
    let fixture = Fixture::new();
    let failures: Vec<_> = cases()
        .into_iter()
        .filter(|c| c["assay_status"] != "ok")
        .collect();
    // Ordinary and DISTINCT Float SUM/AVG now succeed (October 16). Keep the
    // exact CLI/session refusal contract pinned to the still-unsupported Bool
    // SUM; the two DISTINCT cases are DuckDB-answer checks below.
    for id in ["float_sum_distinct", "float_avg_distinct"] {
        assert!(failures.iter().all(|case| case["id"] != id), "{id}");
    }
    let case = failures
        .iter()
        .find(|case| case["id"] == "bool_sum_duplicates")
        .unwrap();
    assert_eq!(case["assay_status"], "unsupported");
    assert_eq!(
        case["assay_message"],
        "unsupported: expected numeric type, got Bool"
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb-session"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{}", json!({"op":"open", "directory":fixture.0})).unwrap();
    for case in &failures {
        writeln!(stdin, "{}", json!({"sql":case["sql"]})).unwrap();
    }
    // A refusal must not poison the session.
    writeln!(stdin, "{}", json!({"sql":"SELECT 7 AS value"})).unwrap();
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    fs::write(fixture.0.join("session-replies.jsonl"), &stdout).unwrap();
    let replies: Vec<Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(replies.len(), failures.len() + 2);
    assert_eq!(replies[0]["status"], "ok");
    for (case, reply) in failures.iter().zip(&replies[1..]) {
        let sql = case["sql"].as_str().unwrap();
        let event = cli_failure(&fixture, sql, json!({}));
        for response in [&event, reply] {
            assert_eq!(
                response["status"], case["assay_status"],
                "{sql}: {response}"
            );
            assert_eq!(
                response["message"], case["assay_message"],
                "{sql}: {response}"
            );
        }
    }
    assert_eq!(replies.last().unwrap()["status"], "ok");
    assert_eq!(replies.last().unwrap()["result"]["rows"], json!([[7]]));
}

#[test]
fn unsupported_parquet_columns_reach_cli_failure_and_native_session() {
    let fixture = Fixture::new();
    let mut session = Session::new(&fixture.0, Options::default(), Limits::default()).unwrap();
    session.set_local_files_enabled(true);
    for (index, ty) in input::input_types().iter().enumerate() {
        let path = fixture.0.join(format!("column-{index}.parquet"));
        input::write_input(&path, ty);
        let message = input::expected_message(ty);
        for projection in ["x", "COUNT(x)"] {
            let sql = format!("SELECT {projection} FROM t");
            let event = cli_failure(&fixture, &sql, json!({"t":path}));
            assert_eq!(event["status"], "unsupported");
            assert_eq!(event["message"], message);
            let sql = format!(
                "SELECT {projection} FROM read_parquet('{}')",
                path.to_str().unwrap().replace('\'', "''")
            );
            let error = session.execute_sql(&sql).err().unwrap();
            assert_eq!(error.kind, "unsupported");
            assert_eq!(error.message, message);
        }
    }
}

#[test]
fn independent_reference_corpus_preserves_answers_and_documents_refusals() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/unsupported_labels/expected.json"
    );
    let mut expected: Value = serde_json::from_str(&fs::read_to_string(path).expect(
        "run tools/testing/unsupported_labels_reference.py with the pinned project Python",
    ))
    .unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    for case in expected["cases"].as_array_mut().unwrap() {
        date_toolkit_transition(case);
    }
    let wanted = expected["cases"].as_array().unwrap();
    let source = cases();
    assert_eq!(wanted.len(), source.len());
    let fixture = Fixture::new();
    let mut session = Session::new(&fixture.0, Options::default(), Limits::default()).unwrap();
    for (case, reference) in source.iter().zip(wanted) {
        for (key, value) in case.as_object().unwrap() {
            assert_eq!(
                &reference[key], value,
                "stale fixture {}: {key}",
                case["id"]
            );
        }
        let sql = case["sql"].as_str().unwrap();
        let result = session.execute_sql(sql);
        if case["assay_status"] == "ok" {
            let Output::Query { execution, .. } = result.unwrap() else {
                panic!("query expected")
            };
            assert_eq!(reference["reference"]["status"], "ok");
            let columns: Vec<_> = execution
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            assert_eq!(json!(columns), reference["reference"]["columns"], "{sql}");
            let mut actual = query_rows(&execution.batch);
            let mut rows: Vec<Vec<Value>> =
                serde_json::from_value(reference["reference"]["rows"].clone()).unwrap();
            if case["ordered"] == false {
                actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
            }
            assert_eq!(actual, rows, "{sql}");
        } else {
            let error = result.err().unwrap();
            assert_eq!(
                error.message,
                case["assay_message"].as_str().unwrap(),
                "{sql}"
            );
            if case["assay_status"] == "error" {
                assert_eq!(
                    reference["reference"]["status"], "error",
                    "negative control: {sql}"
                );
            } else {
                assert!(case["difference"].as_str().is_some_and(|s| !s.is_empty()));
            }
        }
    }
}

#[test]
fn unsupported_arrow_types_have_one_prefix_without_changing_scale_errors() {
    use arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;
    let child = Arc::new(Field::new("item", DataType::Int32, true));
    let mut types = vec![
        DataType::Float16,
        DataType::List(child.clone()),
        DataType::LargeList(child.clone()),
        DataType::FixedSizeList(child.clone(), 2),
        DataType::Struct(vec![child.clone()].into()),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Decimal128(38, -1),
        DataType::Decimal128(38, 39),
        DataType::Decimal256(76, 2),
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(2),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Duration(unit));
        // Old -> new (TIMESTAMP and TIMESTAMPTZ cards, Oct 17): wall-clock
        // timestamps in every unit are TIMESTAMP, time-zone-aware ones
        // TIMESTAMPTZ (DuckDB 1.5.6 reads both).
        assert_eq!(
            Type::from_arrow(&DataType::Timestamp(unit, None)).unwrap(),
            Type::Timestamp
        );
        assert_eq!(
            Type::from_arrow(&DataType::Timestamp(unit, Some("UTC".into()))).unwrap(),
            Type::TimestampTz
        );
    }
    for unit in [
        arrow::datatypes::IntervalUnit::YearMonth,
        arrow::datatypes::IntervalUnit::DayTime,
        arrow::datatypes::IntervalUnit::MonthDayNano,
    ] {
        types.push(DataType::Interval(unit));
    }
    for ty in types {
        assert_eq!(
            Type::from_arrow(&ty).unwrap_err(),
            format!("unsupported: unsupported input type: {ty}")
        );
    }
    for ty in [
        Type::Int,
        Type::Decimal(2),
        Type::Float,
        Type::Bool,
        Type::Text,
        Type::Binary,
        Type::Date,
        Type::Timestamp,
        Type::TimestampTz,
        Type::Null,
    ] {
        assert_eq!(Type::from_arrow(&ty.arrow()).unwrap(), ty);
    }
    for ty in [Type::Float, Type::Bool, Type::Date, Type::Text] {
        assert_eq!(
            ty.scale().unwrap_err(),
            format!("expected numeric type, got {ty:?}")
        );
    }
}
