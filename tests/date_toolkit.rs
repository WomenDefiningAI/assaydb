//! DATE toolkit: pinned references, handwritten boundaries, no runtime oracle.
#[path = "support/external_sf1.rs"]
mod external_sf1;
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const SETUP: &str = include_str!("fixtures/date_toolkit/setup.sql");
const CASES: &str = include_str!("fixtures/date_toolkit/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-date-toolkit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn session(&self, workers: usize, batch_size: usize, optimized: bool) -> Session {
        let mut session = Session::new(
            &self.0,
            Options {
                workers,
                batch_size,
                relational_optimizations: optimized,
                memory_bytes: 64 << 20,
                ..Options::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap();
        for statement in SETUP.split(';').filter(|s| !s.trim().is_empty()) {
            session.execute_sql(statement).unwrap();
        }
        session
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected query: {sql}");
    };
    execution.batch.clone()
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    match array.data_type() {
                        DataType::Date32 => {
                            use chrono::Datelike;
                            let d = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                + chrono::Duration::days(i64::from(
                                    array
                                        .as_any()
                                        .downcast_ref::<Date32Array>()
                                        .unwrap()
                                        .value(row),
                                ));
                            json!(format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day()))
                        }
                        DataType::Int64 => {
                            json!(
                                array
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(row)
                            )
                        }
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
                        DataType::Decimal128(_, scale) => {
                            let value = array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row);
                            let scale = u32::try_from(*scale).unwrap();
                            if scale == 0 {
                                return json!(value.to_string());
                            }
                            let factor = 10_u128.pow(scale);
                            let magnitude = value.unsigned_abs();
                            let sign = if value < 0 { "-" } else { "" };
                            json!(format!(
                                "{sign}{}.{:0width$}",
                                magnitude / factor,
                                magnitude % factor,
                                width = scale as usize
                            ))
                        }
                        other => panic!("unexpected DATE fixture output {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

fn compare(batch: &RecordBatch, case: &Value) {
    assert_eq!(json!(rows(batch)), case["rows"], "{}", case["id"]);
    let schema = batch.schema();
    let columns = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
    assert_eq!(json!(columns), case["columns"], "{}", case["id"]);
    let types = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Date32 => "DATE".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Utf8 => "VARCHAR".to_string(),
            DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
            t => panic!("unexpected result type {t}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(json!(types), case["types"], "{}", case["id"]);
}

#[test]
fn pinned_duckdb_corpus_across_workers_batches_and_optimizer() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/date_toolkit/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["setup_sql"], SETUP);
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 77);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value);
        }
        assert_eq!(case["sql"], sql.trim());
    }
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for optimized in [false, true] {
                let mut session = fixture.session(workers, batch, optimized);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    if case["assay_outcome"] == "rows" {
                        compare(&query(&mut session, sql), case);
                    } else {
                        let error = session.execute_sql(sql).err().expect(sql);
                        assert!(
                            error
                                .message
                                .starts_with(case["assay_error"].as_str().unwrap()),
                            "{sql}: {error}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn handwritten_calendar_clamping_and_null_overloads() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, true);
    let sql = "SELECT DATE '1900-01-31'+INTERVAL '1' MONTH AS a,DATE '2000-01-31'+INTERVAL '1' MONTH AS b,DATE '2000-02-29'+INTERVAL '4' YEAR AS c,DATE '2024-03-31'-INTERVAL '1' MONTH AS d,DATE_TRUNC('week',DATE '2016-01-03') AS e,DATE '2000-03-01'-DATE '2000-02-28' AS f,DATE '1900-03-01'-DATE '1900-02-28' AS g";
    assert_eq!(
        rows(&query(&mut session, sql)),
        vec![vec![
            json!("1900-02-28"),
            json!("2000-02-29"),
            json!("2004-02-29"),
            json!("2024-02-29"),
            json!("2015-12-28"),
            json!(2),
            json!(1)
        ]]
    );
    let batch = query(
        &mut session,
        "SELECT DATE '2024-01-01'-NULL AS a,NULL+DATE '2024-01-01' AS b,CAST(NULL AS DATE)-DATE '2024-01-01' AS c",
    );
    assert_eq!(rows(&batch), vec![vec![Value::Null; 3]]);
    assert_eq!(batch.column(0).data_type(), &DataType::Date32);
    assert_eq!(batch.column(1).data_type(), &DataType::Date32);
    assert_eq!(batch.column(2).data_type(), &DataType::Int64);
    // Old -> new (session clock policy, maintainer decision Oct 17): the
    // clock functions used to refuse pending a policy. They now read the
    // statement-start instant (fixed here) in the session zone, as DuckDB
    // 1.5.6 and PostgreSQL 17 do (tests/timestamptz.rs has the references).
    session
        .execute_sql("SET assay_fixed_clock = '2024-03-10 03:00:00+00'")
        .unwrap();
    session
        .execute_sql("SET TimeZone = 'America/Los_Angeles'")
        .unwrap();
    assert_eq!(
        rows(&query(&mut session, "SELECT CURRENT_DATE AS d")),
        vec![vec![json!("2024-03-09")]]
    );
    for sql in ["SELECT NOW() AS t", "SELECT CURRENT_TIMESTAMP AS t"] {
        let batch = query(&mut session, sql);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), 1_710_039_600_000_000, "{sql}");
        assert_eq!(values.timezone(), Some("America/Los_Angeles"), "{sql}");
    }
    session.execute_sql("SET TimeZone = 'UTC'").unwrap();
    session
        .execute_sql("SET assay_fixed_clock = 'system'")
        .unwrap();
    for sql in [
        "SELECT DATE_TRUNC('month',NULL)",
        "SELECT DATE_TRUNC('month',d) FROM empty_dates GROUP BY d HAVING DATE_TRUNC('hour',d)=d",
    ] {
        let error = session.execute_sql(sql).err().expect(sql);
        assert!(error.message.starts_with("unsupported:"), "{sql}: {error}");
    }
    // DDL classifies directly; query errors retain the binder's prefix in
    // their message. Verify the stored-type refusal through the DDL contract.
    let error = session
        .execute_sql("CREATE TABLE interval_values(i INTERVAL)")
        .err()
        .unwrap();
    assert_eq!(error.kind, "unsupported");
    assert_eq!(error.message, "column type: INTERVAL");
}

#[test]
fn date32_extremes_refuse_without_panicking_or_leaking() {
    use assaydb::{
        expr::{Expr, Input, Kind, Op, Value as Funded},
        memory::Budget,
        types::Type,
    };
    let budget = Budget::new(1 << 20);
    let value = Funded::adopt(
        std::sync::Arc::new(Date32Array::from(vec![i32::MIN, i32::MAX])),
        &budget,
    )
    .unwrap();
    let input = Input {
        columns: vec![Some(value)],
        len: 2,
    };
    let date = Expr {
        ty: Type::Date,
        kind: Kind::Column(0),
    };
    let expr = Expr {
        ty: Type::Int,
        kind: Kind::Binary(Op::Sub, Box::new(date.clone()), Box::new(date)),
    };
    assert!(
        expr.eval(&input, &budget)
            .err()
            .unwrap()
            .starts_with("unsupported:")
    );
    drop(input);
    assert_eq!(budget.used(), 0);
}

// Opt-in local SF1 check: the driver supplies this immutable dataset. It is
// intentionally separate from portable all-target tests and is not a timing gate.
#[test]
#[ignore = "requires ASSAY_DATE_TOOLKIT_SF1 and the pinned SF1 Parquet files"]
fn sf1_analysis_and_strict_analyst_q01() {
    let root = PathBuf::from(std::env::var_os("ASSAY_DATE_TOOLKIT_SF1").expect("SF1 dataset path"));
    let expected: Value =
        serde_json::from_str(&external_sf1::read("date_toolkit/analysis_expected.json")).unwrap();
    let tables: HashMap<String, PathBuf> = expected["tables"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(n, v)| (n.clone(), root.join(v["file"].as_str().unwrap())))
        .collect();
    let source_text = external_sf1::read("date_toolkit/analysis.sql");
    let source = source_text.split("-- case: ").skip(1).collect::<Vec<_>>();
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(source.len(), cases.len());
    for (s, c) in source.iter().zip(cases) {
        assert_eq!(s.split_once('\n').unwrap().1.trim(), c["sql"]);
    }
    for workers in [1, 4, 10] {
        let options = Options {
            workers,
            batch_size: 8192,
            ..Options::default()
        };
        for case in cases {
            let context = || {
                format!(
                    "case={} workers={} memory_bytes={} batch_size={}",
                    case["id"], options.workers, options.memory_bytes, options.batch_size
                )
            };
            let plan = prepare(case["sql"].as_str().unwrap(), &tables, &options)
                .unwrap_or_else(|error| panic!("prepare {}: {error}", context()));
            let result = execute(&plan, &options)
                .unwrap_or_else(|error| panic!("execute {}: {error}", context()));
            compare(&result.batch, case);
        }
    }
}
