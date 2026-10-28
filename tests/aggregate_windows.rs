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
            "assay-aggregate-window-{}-{}",
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
        fs::remove_dir_all(&self.0).unwrap();
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
        serde_json::from_str(include_str!("fixtures/aggregate_windows/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 66);
    let sources = include_str!("fixtures/aggregate_windows/cases.sql")
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
                for case in cases {
                    let mut actual = rows(&query(&mut session, case["sql"].as_str().unwrap()));
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
fn handwritten_ties_nulls_and_plain_aggregate_type_identity() {
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch, spill);
                session.execute_sql("CREATE TABLE t(id INTEGER,k INTEGER,v INTEGER,d DECIMAL(20,2),s TEXT,b BOOLEAN,day DATE)").unwrap();
                session.execute_sql("INSERT INTO t VALUES(1,1,NULL,NULL,NULL,NULL,NULL),(2,1,4,4.50,'z',TRUE,DATE '2024-01-02'),(3,2,2,2.50,'a',FALSE,DATE '2024-01-01'),(4,2,NULL,NULL,NULL,NULL,NULL)").unwrap();
                let result = query(
                    &mut session,
                    "SELECT id,SUM(v) OVER (ORDER BY k) AS range_sum,SUM(v) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING) AS row_sum,COUNT(v) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING) AS n,ROW_NUMBER() OVER (ORDER BY k) AS r FROM t ORDER BY id",
                );
                assert_eq!(
                    rows(&result),
                    vec![
                        vec![json!(1), json!(4), Value::Null, json!(0), json!(1)],
                        vec![json!(2), json!(4), json!(4), json!(1), json!(2)],
                        vec![json!(3), json!(6), json!(6), json!(2), json!(3)],
                        vec![json!(4), json!(6), json!(6), json!(2), json!(4)]
                    ]
                );
                for aggregate in [
                    "SUM(v)", "SUM(d)", "COUNT(*)", "COUNT(v)", "AVG(v)", "AVG(d)", "MIN(v)",
                    "MAX(d)", "MIN(s)", "MAX(s)", "MIN(b)", "MAX(b)", "MIN(day)", "MAX(day)",
                ] {
                    let ordinary = query(&mut session, &format!("SELECT {aggregate} FROM t"));
                    let window = query(&mut session, &format!("SELECT {aggregate} OVER () FROM t"));
                    assert_eq!(
                        window.schema().field(0).data_type(),
                        ordinary.schema().field(0).data_type(),
                        "{aggregate}"
                    );
                    assert_eq!(
                        rows(&window),
                        vec![rows(&ordinary)[0].clone(); 4],
                        "{aggregate}"
                    );
                }
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT COUNT(*) OVER (ROWS UNBOUNDED PRECEDING) FROM t"
                    )),
                    vec![
                        vec![json!(1)],
                        vec![json!(2)],
                        vec![json!(3)],
                        vec![json!(4)]
                    ]
                );
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT COUNT(COUNT(*)) OVER (),SUM(SUM(v)) OVER () FROM t WHERE id<0"
                    )),
                    vec![vec![json!(1), Value::Null]]
                );
                assert!(
                    rows(&query(
                        &mut session,
                        "SELECT k,SUM(SUM(v)) OVER () FROM t GROUP BY k HAVING COUNT(*)<0"
                    ))
                    .is_empty()
                );
                let joined = query(
                    &mut session,
                    "SELECT a.id AS ai,b.id AS bi,SUM(a.v) OVER () AS total,SUM(a.v) OVER (ORDER BY a.k ROWS UNBOUNDED PRECEDING) AS running FROM t a JOIN t b ON a.k=b.k ORDER BY ai,bi",
                );
                assert_eq!(
                    rows(&joined),
                    vec![
                        vec![json!(1), json!(1), json!(12), Value::Null],
                        vec![json!(1), json!(2), json!(12), Value::Null],
                        vec![json!(2), json!(1), json!(12), json!(4)],
                        vec![json!(2), json!(2), json!(12), json!(8)],
                        vec![json!(3), json!(3), json!(12), json!(10)],
                        vec![json!(3), json!(4), json!(12), json!(12)],
                        vec![json!(4), json!(3), json!(12), json!(12)],
                        vec![json!(4), json!(4), json!(12), json!(12)],
                    ]
                );
                session.execute_sql("DROP TABLE t").unwrap();
            }
        }
    }
}

#[test]
fn exact_overflow_completion_and_refusals() {
    let fixture = Fixture::new();
    for spill in [false, true] {
        let mut session = fixture.session(4, 1, spill);
        let limit = "99999999999999999999999999999999999999";
        let input = format!(
            "(SELECT 1 AS id,CAST('{limit}' AS DECIMAL(38,0)) AS v UNION ALL SELECT 2,CAST(1 AS DECIMAL(38,0)) UNION ALL SELECT 3,CAST(-1 AS DECIMAL(38,0))) t"
        );
        for frame in ["", "ORDER BY 1"] {
            let sql = format!("SELECT SUM(v) OVER ({frame}) FROM {input}");
            assert_eq!(
                rows(&query(&mut session, &sql)),
                vec![vec![json!({"decimal":limit})]; 3]
            );
        }
        for sql in [
            format!("SELECT SUM(v) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM {input}"),
            format!("SELECT SUM(v) OVER () FROM {input} WHERE id<3"),
            format!("SELECT SUM(v) OVER () FROM {input} WHERE id<3 LIMIT 1"),
            "SELECT SUM(v) OVER () FROM (SELECT 9223372036854775807 AS v UNION ALL SELECT 1) t"
                .into(),
            "SELECT SUM(v*99999999999999999999999999999999999999) OVER () FROM (SELECT 2 AS v) t".into(),
            "SELECT COUNT(*) OVER (ORDER BY v*99999999999999999999999999999999999999) FROM (SELECT 2 AS v) t".into(),
        ] {
            let error = session
                .execute_sql(&sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            assert!(
                error.message.contains("overflow") || error.message.contains("zero"),
                "{sql}: {error}"
            );
        }
        assert_eq!(
            rows(&query(
                &mut session,
                "SELECT SUM(v) OVER ()-9223372036854775807 FROM (SELECT 9223372036854775807 AS v UNION ALL SELECT 1) t"
            )),
            vec![vec![json!(1)]; 2]
        );
        for sql in [
            "SELECT SUM(CAST(1 AS REAL)) OVER ()",
            "SELECT AVG(CAST(1 AS DOUBLE PRECISION)) OVER ()",
            "SELECT AVG(CAST(1 AS REAL)) OVER ()",
            "SELECT SUM(1) OVER (ROWS 1 PRECEDING)",
            "SELECT SUM(1) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
            "SELECT SUM(1) OVER (ORDER BY 1 RANGE 1 PRECEDING)",
            "SELECT SUM(1) OVER () QUALIFY TRUE",
            "SELECT NTILE(2) OVER ()",
            "SELECT FIRST_VALUE(1) OVER ()",
            "SELECT LAST_VALUE(1) OVER ()",
        ] {
            let expected = if sql.contains("CAST") {
                json!({"float_bits":"3ff0000000000000"})
            } else {
                json!(1)
            };
            assert_eq!(rows(&query(&mut session, sql)), vec![vec![expected]]);
        }
        // October 18 window extras (old -> new): DISTINCT (DuckDB-only),
        // FILTER, EXCLUDE, named windows and the decided DESC NULLS LAST
        // default now answer; values checked against DuckDB 1.5.6 and, where
        // it accepts the form, PostgreSQL 17.
        for (sql, expected) in [
            ("SELECT SUM(DISTINCT 1) OVER ()", json!(1)),
            ("SELECT COUNT(DISTINCT 1) OVER ()", json!(1)),
            ("SELECT SUM(1) FILTER (WHERE TRUE) OVER ()", json!(1)),
            (
                "SELECT SUM(1) OVER (ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)",
                Value::Null,
            ),
            ("SELECT SUM(1) OVER w WINDOW w AS ()", json!(1)),
            (
                "SELECT SUM(1) OVER w FROM (SELECT 1 AS id) t WINDOW w AS ()",
                json!(1),
            ),
            ("SELECT SUM(1) OVER (ORDER BY 1 DESC)", json!(1)),
        ] {
            assert_eq!(
                rows(&query(&mut session, sql)),
                vec![vec![expected]],
                "{sql}"
            );
        }
        for sql in [
            "SELECT SUM('x') OVER ()",
            "SELECT AVG(TRUE) OVER ()",
            "SELECT SUM(1) OVER (GROUPS UNBOUNDED PRECEDING)",
            "SELECT LAG(1) OVER ()",
            "SELECT LEAD(1) OVER ()",
            "SELECT SUM(SUM(1) OVER ()) OVER ()",
            "SELECT SUM(*) OVER ()",
            "SELECT SUM(1) OVER () WHERE COUNT(*) OVER ()>0",
        ] {
            let error = session
                .execute_sql(sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            assert_eq!(error.kind, "unsupported", "{sql}: {error}");
            assert!(
                error.to_string().starts_with("unsupported:"),
                "{sql}: {error}"
            );
            // Parser errors carry the refusal prefix in Error.kind/Display;
            // bound-window errors also retain it in the raw message.
            assert!(
                error.message.starts_with("unsupported:")
                    || error.message.starts_with("SQL parser:"),
                "{sql}: {error}"
            );
        }
    }
}

#[test]
fn float_keys_equate_signed_zeros_and_all_nan_payloads() {
    use arrow::datatypes::{Field, Schema};
    use assaydb::execute::{execute, prepare};
    use std::{collections::HashMap, sync::Arc};
    let fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Float64, false),
    ]));
    for (name, keys) in [
        ("finite", vec![-0.0, 0.0]),
        ("nan", vec![f64::NAN, f64::from_bits(0x7ff8000000000002)]),
    ] {
        let path = fixture.0.join(format!("{name}.parquet"));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(keys.clone())),
            ],
        )
        .unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            fs::File::create_new(&path).unwrap(),
            schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let tables = HashMap::from([("t".into(), path)]);
        let options = Options {
            batch_size: 1,
            ..Default::default()
        };
        for sql in [
            "SELECT SUM(id) OVER (ORDER BY k) FROM t",
            "SELECT SUM(id) OVER (PARTITION BY k) FROM t",
        ] {
            let result = execute(&prepare(sql, &tables, &options).unwrap(), &options);
            assert_eq!(rows(&result.unwrap().batch), vec![vec![json!(3)]; 2]);
        }
        for sql in [
            "SELECT SUM(k) OVER () FROM t",
            "SELECT AVG(k) OVER () FROM t",
        ] {
            let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
            let values = result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.len(), 2);
            assert_eq!(values.null_count(), 0, "{sql}");
            for value in values.values().iter() {
                if keys.iter().any(|key| key.is_nan()) {
                    // Every NaN input payload yields the one documented NaN
                    // encoding (aggregation/float.rs), never an input payload.
                    assert_eq!(value.to_bits(), f64::NAN.to_bits(), "{sql}");
                } else {
                    // -0.0 and 0.0 folded from +0.0 in either peer order give
                    // +0.0 bits; DuckDB 1.5.6 returns 0x0000000000000000 for
                    // SUM and AVG in both orders as well.
                    assert_eq!(value.to_bits(), 0_u64, "{sql}");
                }
            }
        }
        let count = execute(
            &prepare("SELECT COUNT(k) OVER () FROM t", &tables, &options).unwrap(),
            &options,
        )
        .unwrap();
        assert_eq!(rows(&count.batch), vec![vec![json!(2)]; 2]);
    }
}
