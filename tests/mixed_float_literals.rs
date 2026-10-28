//! Exact literal promotion, with independent reference values and schema checks.
#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};

fn session(workers: usize, batch_size: usize, optimized: bool) -> Session {
    Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap()
}

fn query(session: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match session.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("expected SELECT: {sql}"),
    }
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
                        DataType::Float64 => json!({"float64": format!("{:016x}",
                            array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row).to_bits()
                        )}),
                        DataType::Int64 => json!(
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
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
                        DataType::Utf8 | DataType::Decimal128(..) | DataType::Date32 => json!(
                            arrow::util::display::array_value_to_string(array.as_ref(), row)
                                .unwrap()
                        ),
                        other => panic!("unhandled result type {other:?}"),
                    }
                })
                .collect()
        })
        .collect()
}

fn check(session: &mut Session, case: &Value) {
    let sql = case["sql"].as_str().unwrap();
    // Old: cases with status "unsupported" asserted the row-dependent mixed
    // Int/Float refusal. New (Helen, Oct 15): they promote to DOUBLE and must
    // equal the DuckDB 1.5.6 answers this fixture always retained for them.
    let batch = query(session, sql).unwrap_or_else(|e| panic!("{}: {e}\n{sql}", case["id"]));
    let types = case["types"].as_array().unwrap();
    assert_eq!(batch.num_columns(), types.len());
    for (column, expected) in batch.columns().iter().zip(types) {
        let matches = match expected.as_str().unwrap() {
            "DOUBLE" => column.data_type() == &DataType::Float64,
            "INTEGER" | "BIGINT" | "HUGEINT" => column.data_type() == &DataType::Int64,
            "BOOLEAN" => column.data_type() == &DataType::Boolean,
            "VARCHAR" => column.data_type() == &DataType::Utf8,
            "DATE" => column.data_type() == &DataType::Date32,
            "DECIMAL(10,2)" => column.data_type() == &DataType::Decimal128(38, 2),
            other => panic!("unhandled reference type {other}"),
        };
        assert!(
            matches,
            "{}: {:?} vs {expected}",
            case["id"],
            column.data_type()
        );
    }
    let mut actual = rows(&batch);
    let mut expected: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    if case["ordered"] == false {
        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
    }
    assert_eq!(actual, expected, "{}: {sql}", case["id"]);
}

fn corpus() -> Value {
    serde_json::from_str(include_str!("fixtures/mixed_float_literals/expected.json")).unwrap()
}

#[test]
fn independent_reference_corpus_at_all_worker_and_batch_sizes() {
    let expected = corpus();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 52);
    let sources: Vec<_> = include_str!("fixtures/mixed_float_literals/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for key in ["id", "ordered", "status"] {
            assert_eq!(case[key], metadata[key]);
        }
        assert_eq!(case["sql"], sql.trim());
    }
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                let mut session = session(workers, batch_size, optimized);
                for case in cases.iter().filter(|case| case["status"] != "cv6") {
                    check(&mut session, case);
                }
            }
        }
    }
}

#[test]
fn float_sum_after_literal_promotion() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = session(workers, batch_size, true);
            for case in corpus()["cases"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|case| case["status"] == "cv6")
            {
                check(&mut session, case);
            }
        }
    }
}

#[test]
fn errors_laziness_and_remaining_refusals() {
    let promoted = promoted_reference::group("literal_consumers");
    let mut session = session(4, 7, true);
    session
        .execute_sql("CREATE TABLE t(f DOUBLE, flag BOOLEAN)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (2.5,TRUE),(NULL,FALSE)")
        .unwrap();
    for expression in [
        "COALESCE(f,9007199254740994)",
        "COALESCE(f,-9007199254740994)",
        "CASE WHEN flag THEN 0+0 ELSE f END",
    ] {
        for consumer in [
            format!("{expression}+1"),
            format!("CAST({expression} AS REAL)"),
            format!("ABS({expression})"),
            format!("MIN({expression})"),
            format!("COUNT({expression})"),
            format!("NULLIF({expression},0)"),
            format!("{expression} IN (0,1)"),
        ] {
            // Old: refused. New: DOUBLE promotion, independent DuckDB answer.
            let sql = format!("SELECT {consumer} FROM t");
            promoted_reference::check(&mut session, &promoted, &sql, "literal consumers");
        }
    }
    for sql in [
        "SELECT COALESCE(f,missing) FROM t",
        "SELECT CASE WHEN flag THEN f ELSE missing END FROM t",
    ] {
        assert!(
            query(&mut session, sql)
                .unwrap_err()
                .contains("unknown or ambiguous column")
        );
    }
    assert!(
        query(&mut session, "SELECT FACTORIAL(COALESCE(f,0)) FROM t")
            .unwrap_err()
            .starts_with("unsupported:")
    );
    let error = query(
        &mut session,
        "SELECT CASE WHEN flag THEN CAST(1.0/0 AS REAL) ELSE 0 END FROM t",
    )
    .unwrap_err();
    assert!(error.contains("division by zero"), "{error}");
    let batch = query(&mut session, "SELECT COALESCE(f,0),CASE WHEN FALSE THEN CAST(1.0/0 AS REAL) ELSE COALESCE(f,0) END FROM t ORDER BY 1").unwrap();
    assert_eq!(
        rows(&batch),
        vec![
            vec![
                json!({"float64":"0000000000000000"}),
                json!({"float64":"0000000000000000"})
            ],
            vec![
                json!({"float64":"4004000000000000"}),
                json!({"float64":"4004000000000000"})
            ]
        ]
    );
}
