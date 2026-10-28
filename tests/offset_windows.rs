//! Exact reference goldens plus handwritten frame, error and type boundaries.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
    types::decimal_text,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-offset-window-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn session(&self, workers: usize, batch_size: usize, spill: bool) -> Session {
        Session::new(
            &self.0,
            Options {
                workers,
                batch_size,
                memory_bytes: 64 << 20,
                spill: spill.then(|| SpillOptions {
                    directory: self.0.clone(),
                    sort_run_bytes: 512,
                    merge_fan_in: 2,
                    ..Default::default()
                }),
                ..Default::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap()
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
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("expected query")
    };
    execution.batch.clone()
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows()).map(|row| batch.columns().iter().map(|a| {
        if a.is_null(row) || a.data_type() == &DataType::Null { return Value::Null; }
        match a.data_type() {
            DataType::Int64 => json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
            DataType::Decimal128(_, scale) => {
                let text = decimal_text(a.as_any().downcast_ref::<Decimal128Array>().unwrap().value(row), *scale);
                json!({"decimal": if text.contains('.') { text.trim_end_matches('0').trim_end_matches('.') } else { &text }})
            }
            DataType::Float64 => json!({"float_bits": format!("{:016x}", a.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_bits())}),
            DataType::Date32 => json!({"date": (chrono::NaiveDate::from_ymd_opt(1970,1,1).unwrap() + chrono::Duration::days(i64::from(a.as_any().downcast_ref::<Date32Array>().unwrap().value(row)))).to_string()}),
            DataType::Utf8 => json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(row)),
            DataType::Boolean => json!(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(row)),
            other => panic!("unexpected window output {other}"),
        }
    }).collect()).collect()
}

#[test]
fn independent_reference_cases_across_workers_batches_and_spill() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/offset_windows/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 80);
    let sources = include_str!("fixtures/offset_windows/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata["id"], case["id"]);
        assert_eq!(metadata["ordered"], case["ordered"]);
        assert_eq!(sql.trim(), case["sql"]);
    }
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch_size, spill);
                for case in expected["integer_width_controls"].as_array().unwrap() {
                    assert_eq!(
                        session
                            .execute_sql(case["sql"].as_str().unwrap())
                            .err()
                            .unwrap()
                            .message,
                        case["assay_error"].as_str().unwrap()
                    );
                }
                for case in cases {
                    if let Some(error) = case["assay_error"].as_str() {
                        assert_eq!(
                            session
                                .execute_sql(case["sql"].as_str().unwrap())
                                .err()
                                .unwrap()
                                .message,
                            error
                        );
                        continue;
                    }
                    let result = query(&mut session, case["sql"].as_str().unwrap());
                    for (field, expected) in result
                        .schema()
                        .fields()
                        .iter()
                        .zip(case["types"].as_array().unwrap())
                    {
                        let expected = expected.as_str().unwrap();
                        if expected.starts_with("DECIMAL(") {
                            let scale: i8 = expected
                                .trim_end_matches(')')
                                .rsplit(',')
                                .next()
                                .unwrap()
                                .parse()
                                .unwrap();
                            assert_eq!(
                                field.data_type(),
                                &DataType::Decimal128(38, scale),
                                "{}",
                                case["id"]
                            );
                        } else if expected == "DOUBLE" {
                            assert_eq!(field.data_type(), &DataType::Float64);
                        }
                    }
                    let mut actual = rows(&result);
                    let mut wanted: Vec<Vec<Value>> =
                        serde_json::from_value(case["rows"].clone()).unwrap();
                    if !case["ordered"].as_bool().unwrap() {
                        actual.sort_by_key(|r| serde_json::to_string(r).unwrap());
                        wanted.sort_by_key(|r| serde_json::to_string(r).unwrap());
                    }
                    assert_eq!(
                        actual, wanted,
                        "{} workers={workers} batch={batch_size} spill={spill}",
                        case["id"]
                    );
                }
            }
        }
    }
}

#[test]
fn handwritten_ties_defaults_frames_and_refusals() {
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            let mut session = fixture.session(workers, batch, false);
            session
                .execute_sql("CREATE TABLE t(id INTEGER,k INTEGER,v INTEGER)")
                .unwrap();
            session
                .execute_sql("INSERT INTO t VALUES (1,1,10),(2,1,NULL),(3,1,30),(4,2,40)")
                .unwrap();
            let actual = query(
                &mut session,
                "SELECT id,ROW_NUMBER() OVER (ORDER BY k) AS n,LAG(v,1,-9) OVER (ORDER BY k) AS a,LEAD(v,1,-9) OVER (ORDER BY k) AS b,LAG(v,0,-9) OVER (ORDER BY k ROWS CURRENT ROW) AS c FROM t ORDER BY id",
            );
            assert_eq!(
                rows(&actual),
                vec![
                    vec![json!(1), json!(1), json!(-9), Value::Null, json!(10)],
                    vec![json!(2), json!(2), json!(10), json!(30), Value::Null],
                    vec![json!(3), json!(3), Value::Null, json!(40), json!(30)],
                    vec![json!(4), json!(4), json!(30), json!(-9), json!(40)]
                ]
            );
            for (sql, message) in [
                (
                    "SELECT LAG(v,-1) OVER (ORDER BY id) FROM t",
                    "unsupported: window offset must be a non-negative integer literal",
                ),
                (
                    "SELECT LEAD(v,id) OVER (ORDER BY id) FROM t",
                    "unsupported: window offset must be a non-negative integer literal",
                ),
                (
                    "SELECT LAG(v,1.0) OVER (ORDER BY id) FROM t",
                    "unsupported: window offset must be a non-negative integer literal",
                ),
                (
                    "SELECT LAG(v,NULL) OVER (ORDER BY id) FROM t",
                    "unsupported: window offset must be a non-negative integer literal",
                ),
                (
                    "SELECT LAG(v,1,id) OVER (ORDER BY id) FROM t",
                    "unsupported: window default must be a literal or NULL",
                ),
                (
                    "SELECT LAG(v,1,'bad') OVER (ORDER BY id) FROM t",
                    "unsupported: window default requires a compatible type",
                ),
                (
                    "SELECT LAG(v) IGNORE NULLS OVER (ORDER BY id) FROM t",
                    "unsupported: offset window arguments/modifiers (including NULL treatment)",
                ),
                (
                    "SELECT LEAD(v) RESPECT NULLS OVER (ORDER BY id) FROM t",
                    "unsupported: offset window arguments/modifiers (including NULL treatment)",
                ),
                (
                    "SELECT ROUND(v,id) FROM t",
                    "unsupported: ROUND precision must be an integer literal or NULL",
                ),
                (
                    "SELECT ROUND(v,2147483648) FROM t",
                    "unsupported: ROUND precision must fit INTEGER",
                ),
                (
                    "SELECT ROUND('x',2)",
                    "unsupported: ROUND requires Int, Decimal or Float",
                ),
            ] {
                assert_eq!(
                    session.execute_sql(sql).err().unwrap().message,
                    message,
                    "{sql}"
                );
            }
            assert_eq!(
                rows(&query(
                    &mut session,
                    "SELECT LAG(v) OVER (ORDER BY id) FROM t QUALIFY TRUE"
                )),
                rows(&query(
                    &mut session,
                    "SELECT LAG(v) OVER (ORDER BY id) FROM t"
                ))
            );
            assert_eq!(
                rows(&query(
                    &mut session,
                    "SELECT FIRST_VALUE(v) OVER(ORDER BY id),LAST_VALUE(v) OVER(ORDER BY id),NTH_VALUE(v,1) OVER(ORDER BY id),NTILE(2) OVER(ORDER BY id) FROM t ORDER BY id"
                )),
                vec![
                    vec![json!(10), json!(10), json!(10), json!(1)],
                    vec![json!(10), Value::Null, json!(10), json!(1)],
                    vec![json!(10), json!(30), json!(10), json!(2)],
                    vec![json!(10), json!(40), json!(10), json!(2)]
                ]
            );
            // October 18 window extras (old -> new): named windows and
            // PERCENT_RANK/CUME_DIST answer; values from DuckDB 1.5.6 and
            // PostgreSQL 17 (which agree).
            assert_eq!(
                rows(&query(
                    &mut session,
                    "SELECT LAG(v) OVER w FROM t WINDOW w AS (ORDER BY id) ORDER BY id"
                )),
                vec![
                    vec![Value::Null],
                    vec![json!(10)],
                    vec![Value::Null],
                    vec![json!(30)]
                ]
            );
            let ranks = rows(&query(
                &mut session,
                "SELECT PERCENT_RANK() OVER (ORDER BY id),CUME_DIST() OVER (ORDER BY id) FROM t ORDER BY id",
            ));
            let bits = |x: f64| json!({ "float_bits": format!("{:016x}", x.to_bits()) });
            assert_eq!(
                ranks,
                vec![
                    vec![bits(0.0), bits(0.25)],
                    vec![bits(1.0 / 3.0), bits(0.5)],
                    vec![bits(2.0 / 3.0), bits(0.75)],
                    vec![bits(1.0), bits(1.0)]
                ]
            );
        }
    }
}
