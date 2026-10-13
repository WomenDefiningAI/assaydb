//! Exact48-query oracle retained from the found-and-fixed CAST boundary probe.
//! Expected results are pinned SQLite3.54 values, including binary64 zero signs.
use arrow::{
    array::{BooleanArray, Float64Array, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use serde_json::Value;
use std::collections::HashMap;

fn check(batch: &RecordBatch, expected: &Value, sql: &str) {
    assert_eq!(batch.num_rows(), 1, "{sql}");
    let a = batch.column(0);
    if expected["class"] == "null" {
        assert!(assaydb::expr::is_sql_null(a.as_ref(), 0), "{sql}");
    } else if expected["class"] == "int" {
        let value = if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
            a.value(0)
        } else {
            i64::from(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(0))
        };
        assert_eq!(
            value.to_string(),
            expected["value"].as_str().unwrap(),
            "{sql}"
        );
    } else {
        let bits = u64::from_str_radix(expected["bits"].as_str().unwrap(), 16).unwrap();
        assert_eq!(
            a.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            bits,
            "{sql}"
        );
    }
}

#[test]
fn all_48_pinned_numeric_cast_queries_keep_exact_values_and_zero_signs() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/sqlite-numeric-cast-boundaries.json")).unwrap();
    assert_eq!(fixture["sqlite_version"], "3.54.0");
    assert_eq!(
        fixture["amalgamation_sha256"],
        "b739c477092f587417d402fe227eb13c0a83f7534e87082f4a1b80d429a57931"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 48);
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                ..Options::default()
            };
            let mut session =
                Session::new(&std::env::temp_dir(), options.clone(), Limits::default()).unwrap();
            for row in cases {
                let sql = row["sql"].as_str().unwrap();
                let result =
                    execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
                check(&result.batch, &row["expected"], sql);
                let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
                    panic!("query")
                };
                check(&execution.batch, &row["expected"], sql);
            }
        }
    }
}

#[test]
fn decimal_float_consumers_match_pinned_sqlite_sweep() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/sqlite-decimal-float-sweep.json")).unwrap();
    assert_eq!(
        fixture["amalgamation_sha256"],
        "b739c477092f587417d402fe227eb13c0a83f7534e87082f4a1b80d429a57931"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 45);
    for workers in [1, 10] {
        for optimized in [false, true] {
            for row in cases {
                let options = Options {
                    workers,
                    relational_optimizations: optimized,
                    ..Options::default()
                };
                let mut session =
                    Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
                for setup in row["setup"].as_array().unwrap() {
                    session.execute_sql(setup.as_str().unwrap()).unwrap();
                }
                let sql = row["sql"].as_str().unwrap();
                let Output::Query { execution, .. } = session
                    .execute_sql(sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                else {
                    panic!("query")
                };
                check(&execution.batch, &row["expected"], sql);
            }
        }
    }
}
