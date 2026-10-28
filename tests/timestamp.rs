//! TIMESTAMP (without time zone) against answers frozen from DuckDB 1.5.6 and
//! checked on PostgreSQL 17 before any Assay run
//! (tools/testing/timestamp_reference.py). Parquet inputs were written by
//! PyArrow in second, millisecond, microsecond, nanosecond and INT96 units.
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    spill::SpillOptions,
    types::Type,
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

const CASES: &str = include_str!("fixtures/timestamp/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-timestamp-{}-{}",
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
impl Drop for Scratch {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/timestamp")
        .join(name)
}

fn expected() -> Value {
    serde_json::from_str(&fs::read_to_string(fixture("expected.json")).unwrap()).unwrap()
}

fn tables(expected: &Value) -> HashMap<String, PathBuf> {
    expected["files"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, file)| {
            let path = fixture(&format!("{name}.parquet"));
            assert_eq!(
                json!(fs::metadata(&path).unwrap().len()),
                file["bytes"],
                "{name}.parquet"
            );
            (name.clone(), path)
        })
        .collect()
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
                        DataType::Timestamp(TimeUnit::Microsecond, None) => {
                            json!(
                                assaydb::expr::timestamp::format(
                                    a.as_any()
                                        .downcast_ref::<TimestampMicrosecondArray>()
                                        .unwrap()
                                        .value(row)
                                )
                                .unwrap()
                            )
                        }
                        DataType::Int64 => {
                            json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
                        }
                        DataType::Float64 => json!({"float":
                            a.as_any().downcast_ref::<Float64Array>().unwrap().value(row)}),
                        DataType::Utf8 => {
                            json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                        }
                        DataType::Boolean => json!(
                            a.as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Date32 => {
                            let days = a.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
                            json!(
                                (chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                    + chrono::Duration::days(days.into()))
                                .format("%Y-%m-%d")
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

fn type_name(ty: &DataType) -> String {
    match ty {
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Utf8 => "VARCHAR".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(_, s) => format!("DECIMAL(38,{s})"),
        other => panic!("unexpected output type {other}"),
    }
}

/// Floats: DuckDB's binary64 within 1e-12 relative (epoch values are exact).
fn cell_matches(actual: &Value, wanted: &Value) -> bool {
    match (actual.get("float"), wanted.get("float")) {
        (Some(a), Some(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            a == b || (a - b).abs() <= 1e-12_f64.max(b.abs() * 1e-12)
        }
        _ => actual == wanted,
    }
}

fn check(batch: &RecordBatch, case: &Value, context: &str) {
    let schema = batch.schema();
    let names = schema
        .fields()
        .iter()
        .map(|f| json!(f.name()))
        .collect::<Vec<_>>();
    assert_eq!(json!(names), case["columns"], "{context}");
    for (field, ty) in schema
        .fields()
        .iter()
        .zip(case["types"].as_array().unwrap())
    {
        let wanted = ty.as_str().unwrap();
        let actual = type_name(field.data_type());
        // DuckDB keeps DECIMAL(p,s) precision; Assay is precision 38.
        let same = actual == wanted
            || wanted.starts_with("DECIMAL(")
                && actual.ends_with(&format!(",{}", wanted.rsplit(',').next().unwrap()));
        assert!(
            same,
            "{context}: {} is {actual}, DuckDB {wanted}",
            field.name()
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
            a.len() == w.len() && a.iter().zip(w).all(|(x, y)| cell_matches(x, y)),
            "{context}: {a:?} != {w:?}"
        );
    }
}

fn reference_cases(expected: &Value) -> Vec<Value> {
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert!(
        expected["postgresql_version"]
            .as_str()
            .unwrap()
            .starts_with("17."),
        "PostgreSQL agreement recorded"
    );
    let cases = expected["cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 139);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(&cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "{}: metadata {key}", case["id"]);
        }
        assert_eq!(case["sql"], sql.trim());
        if case["assay_outcome"] == "rows" && case["pg_skip"] != true {
            assert_eq!(case["postgresql_agrees"], true, "{}", case["id"]);
        }
        if case["assay_outcome"] != "rows" {
            assert!(case["assay_error"].is_string(), "{}", case["id"]);
        }
    }
    cases
}

#[test]
fn pinned_references_at_all_worker_batch_spill_and_optimizer_settings() {
    let expected = expected();
    let cases = reference_cases(&expected);
    let tables = tables(&expected);
    let scratch = Scratch::new();
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
    let mut spilled = 0;
    for (workers, batch_size, spill, optimized) in settings {
        let options = scratch.options(workers, batch_size, spill, optimized);
        for case in &cases {
            let sql = case["sql"].as_str().unwrap();
            let context = format!(
                "{} workers={workers} batch={batch_size} spill={spill} optimized={optimized}",
                case["id"]
            );
            let result = prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options));
            if case["assay_outcome"] == "rows" {
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                check(&result.batch, case, &context);
                spilled += usize::from(spill && result.metrics.external_aggregate_partitions > 0);
            } else {
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: expected a refusal/error"));
                let prefix = case["assay_error"].as_str().unwrap();
                assert!(error.starts_with(prefix), "{context}: {error}");
                // Both references reject: an ordinary data error, never a refusal.
                assert_eq!(
                    case["assay_outcome"] == "error",
                    !error.starts_with("unsupported:"),
                    "{context}: {error}"
                );
            }
            scratch.clean_spill();
        }
    }
    // Grouped TIMESTAMP keys really route through external aggregation.
    assert!(spilled > 50, "only {spilled} spilled executions");
}

#[test]
fn parquet_units_and_arrow_types() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        assert_eq!(
            Type::from_arrow(&DataType::Timestamp(unit, None)).unwrap(),
            Type::Timestamp
        );
        // Old -> new (TIMESTAMPTZ card): time-zone-aware columns are instants.
        assert_eq!(
            Type::from_arrow(&DataType::Timestamp(unit, Some("UTC".into()))).unwrap(),
            Type::TimestampTz
        );
    }
    assert_eq!(
        Type::Timestamp.arrow(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    // Every unit file holds the same instants (s/ms truncated) and reads to
    // the same microsecond values through the decoder lease and widening.
    let expected = expected();
    let tables = tables(&expected);
    let scratch = Scratch::new();
    let options = scratch.options(4, 7, false, true);
    let read = |table: &str| {
        let plan = prepare(
            &format!(
                "SELECT event_ts FROM {table} ORDER BY event_ts NULLS LAST, user_id NULLS LAST"
            ),
            &tables,
            &options,
        )
        .unwrap();
        let batch = execute(&plan, &options).unwrap().batch;
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        rows(&batch)
    };
    let micros = read("events");
    assert_eq!(read("events_ns"), micros);
    let truncate = |rows: &[Vec<Value>], digits: usize| -> Vec<Vec<Value>> {
        rows.iter()
            .map(|r| {
                r.iter()
                    .map(|v| match v.as_str() {
                        None => Value::Null,
                        Some(text) => {
                            let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
                            let kept = fraction[..fraction.len().min(digits)].trim_end_matches('0');
                            if kept.is_empty() {
                                json!(whole)
                            } else {
                                json!(format!("{whole}.{kept}"))
                            }
                        }
                    })
                    .collect()
            })
            .collect()
    };
    let mut ms = truncate(&micros, 3);
    ms.sort_by_key(|r| serde_json::to_string(r).unwrap());
    let mut got = read("events_ms");
    got.sort_by_key(|r| serde_json::to_string(r).unwrap());
    assert_eq!(got, ms);
    let mut s = truncate(&micros, 0);
    s.sort_by_key(|r| serde_json::to_string(r).unwrap());
    let mut got = read("events_s");
    got.sort_by_key(|r| serde_json::to_string(r).unwrap());
    assert_eq!(got, s);
}

#[test]
fn session_tables_store_timestamps() {
    let scratch = Scratch::new();
    let mut session = Session::new(
        &scratch.0,
        Options {
            workers: 2,
            batch_size: 3,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE ev(id INTEGER PRIMARY KEY, at TIMESTAMP NOT NULL, note TEXT)",
        "INSERT INTO ev VALUES (1, TIMESTAMP '2024-02-29 23:59:59.5', 'a'), (2, TIMESTAMP '2024-03-01 00:00:00', 'b'), (3, TIMESTAMP '1969-12-31 23:59:59.999999', 'c')",
        "CREATE TABLE later(at TIMESTAMP WITHOUT TIME ZONE UNIQUE)",
        "INSERT INTO later SELECT at + INTERVAL '1' HOUR FROM ev WHERE id < 3",
    ] {
        session
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let query = |session: &mut Session, sql: &str| {
        let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
            panic!("{sql}");
        };
        rows(&execution.batch)
    };
    assert_eq!(
        query(
            &mut session,
            "SELECT id, at, DATE_TRUNC('day', at) AS d, EXTRACT(EPOCH FROM at) AS e FROM ev ORDER BY at"
        ),
        vec![
            vec![
                json!(3),
                json!("1969-12-31 23:59:59.999999"),
                json!("1969-12-31 00:00:00"),
                json!({"float": -0.000001})
            ],
            vec![
                json!(1),
                json!("2024-02-29 23:59:59.5"),
                json!("2024-02-29 00:00:00"),
                json!({"float": 1709251199.5})
            ],
            vec![
                json!(2),
                json!("2024-03-01 00:00:00"),
                json!("2024-03-01 00:00:00"),
                json!({"float": 1709251200.0})
            ],
        ]
    );
    assert_eq!(
        query(&mut session, "SELECT at FROM later ORDER BY at"),
        vec![
            vec![json!("2024-03-01 00:59:59.5")],
            vec![json!("2024-03-01 01:00:00")]
        ]
    );
    // UNIQUE over TIMESTAMP compares the stored microseconds.
    let error = session
        .execute_sql("INSERT INTO later VALUES (TIMESTAMP '2024-03-01 01:00:00')")
        .err()
        .unwrap();
    assert_eq!(error.kind, "semantic", "{error}");
    let error = session
        .execute_sql("INSERT INTO ev VALUES (4, TIMESTAMP '2024-02-30 00:00:00', 'x')")
        .err()
        .unwrap();
    assert_eq!(error.kind, "semantic", "{error}");
    let error = session
        .execute_sql("INSERT INTO ev VALUES (4, TIMESTAMP '2024-03-01 00:00:00+02', 'x')")
        .err()
        .unwrap();
    assert_eq!(error.kind, "unsupported", "{error}");
    // TIMESTAMPTZ columns are covered in tests/timestamptz.rs.
    for ddl in [
        "CREATE TABLE z(t TIMESTAMP(3))",
        "CREATE TABLE z(t TIMESTAMP(3) WITH TIME ZONE)",
    ] {
        let error = session.execute_sql(ddl).err().unwrap();
        assert_eq!(error.kind, "unsupported", "{ddl}: {error}");
    }
}

/// The CLI wire format over Parquet: schema type `timestamp`, text values,
/// and the unsupported class for a time-zone-aware column.
#[test]
fn cli_over_parquet() {
    let scratch = Scratch::new();
    let run = |sql: &str, table: &str| -> (bool, Value, String) {
        let output = scratch.0.join("result.json");
        let _ = fs::remove_file(&output);
        let mut cli = Command::new(env!("CARGO_BIN_EXE_assaydb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = cli.stdin.take().unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"sql": sql, "tables": {"t": fixture(&format!("{table}.parquet"))}, "output": output, "threads": 4, "batch_size": 7})
        )
        .unwrap();
        drop(stdin);
        let done = cli.wait_with_output().unwrap();
        let text = String::from_utf8_lossy(&done.stdout).to_string()
            + &String::from_utf8_lossy(&done.stderr);
        let result = fs::read(&output)
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or(Value::Null);
        (done.status.success(), result, text)
    };
    let (ok, result, text) = run(
        "SELECT DATE_TRUNC('hour', event_ts) AS hour, COUNT(*) AS n FROM t WHERE event_ts < TIMESTAMP '2024-02-27 03:00:00' GROUP BY 1 ORDER BY 1",
        "events_ns",
    );
    assert!(ok, "{text}");
    assert_eq!(result["schema"][0]["type"], "timestamp");
    let expected = expected();
    let wanted = expected["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "hourly_counts")
        .unwrap()["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r[0].as_str().is_some_and(|h| h < "2024-02-27 03:00:00"))
        .map(|r| json!([r[0], r[1]]))
        .collect::<Vec<_>>();
    assert!(!wanted.is_empty());
    assert_eq!(result["rows"], json!(wanted));
    // Old -> new (TIMESTAMPTZ card): a pandas tz-aware column reads as
    // TIMESTAMPTZ, printed in the session zone (default UTC) like DuckDB.
    let (ok, result, text) = run("SELECT t FROM t", "tz_ny");
    assert!(ok, "{text}");
    assert_eq!(result["schema"][0]["type"], "timestamptz");
    assert_eq!(result["rows"], json!([["2024-03-01 10:30:00+00"]]));
}
