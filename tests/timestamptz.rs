//! TIMESTAMPTZ and the session time zone against answers frozen from DuckDB
//! 1.5.6 and checked on PostgreSQL 17 with both engines set to the case's
//! zone (tools/testing/timestamptz_reference.py). Parquet inputs are
//! time-zone-aware PyArrow columns (pandas style) in us, ms and ns.
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    spill::SpillOptions,
    zone::SessionZone,
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

const CASES: &str = include_str!("fixtures/timestamptz/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-timestamptz-{}-{}",
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
        .join("tests/fixtures/timestamptz")
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
                        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                            json!(assaydb::zone::format_labelled(a.as_ref(), row).unwrap())
                        }
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
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
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
            .starts_with("17.")
    );
    let cases = expected["cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 118);
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
    }
    cases
}

/// Execute and label TIMESTAMPTZ columns with the session zone, as the
/// session, CLI and Python results are.
fn run(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
) -> assaydb::Result<RecordBatch> {
    let plan = prepare(sql, tables, options)?;
    let execution = execute(&plan, options)?;
    assaydb::zone::relabel(&execution.batch, &options.time_zone)
}

#[test]
fn pinned_references_in_each_zone_at_all_settings() {
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
        for case in &cases {
            let mut options = scratch.options(workers, batch_size, spill, optimized);
            options.time_zone = SessionZone::parse(case["zone"].as_str().unwrap()).unwrap();
            let sql = case["sql"].as_str().unwrap();
            let context = format!(
                "{} [{}] workers={workers} batch={batch_size} spill={spill} optimized={optimized}",
                case["id"], case["zone"]
            );
            if case["assay_outcome"] == "rows" {
                let plan =
                    prepare(sql, &tables, &options).unwrap_or_else(|e| panic!("{context}: {e}"));
                let execution =
                    execute(&plan, &options).unwrap_or_else(|e| panic!("{context}: {e}"));
                spilled +=
                    usize::from(spill && execution.metrics.external_aggregate_partitions > 0);
                let batch = assaydb::zone::relabel(&execution.batch, &options.time_zone).unwrap();
                check(&batch, case, &context);
            } else {
                // As the CLI wire does, print every TIMESTAMPTZ value.
                let error = run(sql, &tables, &options)
                    .and_then(|batch| {
                        for column in batch.columns() {
                            if matches!(column.data_type(), DataType::Timestamp(_, Some(_))) {
                                for row in 0..column.len() {
                                    if column.is_valid(row) {
                                        assaydb::zone::format_labelled(column.as_ref(), row)?;
                                    }
                                }
                            }
                        }
                        Ok(batch)
                    })
                    .err()
                    .unwrap_or_else(|| panic!("{context}: expected a refusal/error"));
                let prefix = case["assay_error"].as_str().unwrap();
                assert!(error.starts_with(prefix), "{context}: {error}");
                assert_eq!(
                    case["assay_outcome"] == "error",
                    !error.starts_with("unsupported:"),
                    "{context}: {error}"
                );
            }
            scratch.clean_spill();
        }
    }
    assert!(spilled > 20, "only {spilled} spilled executions");
}

/// NOW()/CURRENT_TIMESTAMP are one instant per statement; with a fixed clock
/// every worker/batch/spill setting returns exactly it, and CURRENT_DATE /
/// LOCALTIMESTAMP are its date/wall clock in the session zone.
#[test]
fn fixed_clock_is_deterministic() {
    let scratch = Scratch::new();
    let tables = tables(&expected());
    let clock = assaydb::zone::fixed_clock("2024-03-10 03:00:00+00").unwrap();
    assert_eq!(clock, 1_710_039_600_000_000);
    for (zone, date, wall) in [
        ("UTC", "2024-03-10", "2024-03-10 03:00:00"),
        ("America/Los_Angeles", "2024-03-09", "2024-03-09 19:00:00"),
        ("Asia/Kolkata", "2024-03-10", "2024-03-10 08:30:00"),
    ] {
        for (workers, batch, spill) in [(1, 1, false), (4, 7, true), (10, 8192, false)] {
            let mut options = scratch.options(workers, batch, spill, true);
            options.time_zone = SessionZone::parse(zone).unwrap();
            options.fixed_clock = Some(clock);
            let batch = run(
                "SELECT NOW() AS a, CURRENT_TIMESTAMP AS b, CURRENT_DATE AS c, LOCALTIMESTAMP AS d, COUNT(*) AS n, MIN(event_at) <= NOW() AS e FROM events WHERE event_at < NOW()",
                &tables,
                &options,
            )
            .unwrap();
            let now = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(now.value(0), clock);
            assert_eq!(now.timezone(), Some(zone));
            let got = rows(&batch);
            assert_eq!(got[0][1], got[0][0]);
            assert_eq!(got[0][2], json!(date), "{zone}");
            assert_eq!(got[0][3], json!(wall), "{zone}");
            assert_eq!(got[0][4], json!(62));
            assert_eq!(got[0][5], json!(true));
            scratch.clean_spill();
        }
    }
    // Without a fixed clock the statement still sees one instant.
    let options = scratch.options(4, 7, false, true);
    let got = rows(
        &run(
            "SELECT NOW() = CURRENT_TIMESTAMP AS a, NOW() > TIMESTAMPTZ '2026-01-01 00:00:00+00' AS b",
            &tables,
            &options,
        )
        .unwrap(),
    );
    assert_eq!(got, vec![vec![json!(true), json!(true)]]);
}

#[test]
fn session_time_zone_tables_and_set() {
    let scratch = Scratch::new();
    let mut session = Session::new(&scratch.0, Options::default(), Limits::default()).unwrap();
    let query = |session: &mut Session, sql: &str| {
        let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
            panic!("{sql}");
        };
        rows(&execution.batch)
    };
    for sql in [
        "SET TimeZone = 'America/New_York'",
        "CREATE TABLE ev(id INTEGER PRIMARY KEY, at TIMESTAMPTZ NOT NULL, wall TIMESTAMP)",
        "INSERT INTO ev VALUES (1, TIMESTAMPTZ '2024-03-10 02:30:00', TIMESTAMP '2024-03-10 02:30:00'), (2, TIMESTAMPTZ '2024-11-03 01:30:00', NULL), (3, TIMESTAMP WITH TIME ZONE '2024-07-01 12:00:00+05:30', NULL)",
    ] {
        session
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(session.time_zone().name(), "America/New_York");
    // DST gap and repeated hour take the later instant, as in both references.
    let ny = query(
        &mut session,
        "SELECT id, at, CAST(at AS DATE) AS d FROM ev ORDER BY id",
    );
    assert_eq!(
        ny,
        vec![
            vec![
                json!(1),
                json!("2024-03-10 03:30:00-04"),
                json!("2024-03-10")
            ],
            vec![
                json!(2),
                json!("2024-11-03 01:30:00-05"),
                json!("2024-11-03")
            ],
            vec![
                json!(3),
                json!("2024-07-01 02:30:00-04"),
                json!("2024-07-01")
            ],
        ]
    );
    session.execute_sql("SET TIME ZONE 'Asia/Kolkata'").unwrap();
    assert_eq!(
        query(&mut session, "SELECT at FROM ev WHERE id = 3"),
        vec![vec![json!("2024-07-01 12:00:00+05:30")]]
    );
    for (sql, kind) in [
        ("SET TimeZone = 'PST'", "unsupported"),
        ("SET TimeZone = '+05:30'", "unsupported"),
        ("SET TimeZone = 'Mars/Olympus'", "semantic"),
        ("SET search_path = 'x'", "unsupported"),
        (
            "INSERT INTO ev VALUES (4, TIMESTAMPTZ '2024-03-10 06:30:00 PST', NULL)",
            "unsupported",
        ),
        (
            "CREATE TABLE z(t TIMESTAMP(3) WITH TIME ZONE)",
            "unsupported",
        ),
    ] {
        let error = session.execute_sql(sql).err().unwrap();
        assert_eq!(error.kind, kind, "{sql}: {error}");
    }
    assert_eq!(session.time_zone().name(), "Asia/Kolkata");
    session
        .execute_sql("SET assay_fixed_clock = '2024-03-10 03:00:00+00'")
        .unwrap();
    assert_eq!(
        query(
            &mut session,
            "SELECT CAST(NOW() AS VARCHAR) AS t, CURRENT_DATE AS d"
        ),
        vec![vec![
            json!("2024-03-10 08:30:00+05:30"),
            json!("2024-03-10")
        ]]
    );
}

/// CLI: `timezone` and `fixed_clock` request fields, wire type
/// `timestamptz`, values printed in the session zone.
#[test]
fn cli_time_zone_and_clock() {
    let scratch = Scratch::new();
    let output = scratch.0.join("result.json");
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
        json!({"sql": "SELECT CAST(event_at AS DATE) AS day, COUNT(*) AS n, MIN(event_at) AS first_at, CURRENT_DATE AS today FROM t WHERE event_at IS NOT NULL GROUP BY 1 ORDER BY 1 LIMIT 2",
               "tables": {"t": fixture("events.parquet")}, "output": output, "threads": 4,
               "timezone": "America/Los_Angeles", "fixed_clock": "2024-03-10 03:00:00+00"})
    )
    .unwrap();
    drop(stdin);
    let done = cli.wait_with_output().unwrap();
    assert!(
        done.status.success(),
        "{}",
        String::from_utf8_lossy(&done.stdout)
    );
    let result: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    assert_eq!(result["schema"][2]["type"], "timestamptz");
    let expected = expected();
    let wanted = expected["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "daily_active_America/Los_Angeles")
        .unwrap()["rows"]
        .clone();
    assert_eq!(result["rows"][0][0], wanted[0][0]);
    assert_eq!(result["rows"][0][1], wanted[0][2]);
    assert!(result["rows"][0][2].as_str().unwrap().ends_with("-08"));
    assert_eq!(result["rows"][0][3], "2024-03-09");
}
