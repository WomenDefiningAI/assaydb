//! VALUES query relations: ordered rows, typed coercion and query composition.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{self, Options},
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::collections::HashMap;

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    match session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    {
        Output::Query { execution, .. } => execution.batch,
        _ => panic!("expected query: {sql}"),
    }
}

fn rows(batch: &RecordBatch) -> Value {
    json!(
        (0..batch.num_rows())
            .map(|row| batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    match array.data_type() {
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
                        DataType::Decimal128(_, scale) => json!(assaydb::types::decimal_text(
                            array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        _ => json!(
                            arrow::util::display::array_value_to_string(array.as_ref(), row)
                                .unwrap()
                        ),
                    }
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    )
}

#[test]
fn ordered_values_and_relational_composition() {
    let cases = [
        (
            "VALUES (3,'three'),(1,'one'),(2,NULL)",
            json!([[3, "three"], [1, "one"], [2, null]]),
        ),
        (
            "VALUES (3),(1),(2) ORDER BY col0 LIMIT 2 OFFSET 1",
            json!([[2], [3]]),
        ),
        ("VALUES (3),(1),(2) ORDER BY 1 DESC", json!([[3], [2], [1]])),
        (
            "SELECT x+10 AS y FROM (VALUES (3),(1),(2)) t(x) WHERE x>1 ORDER BY y",
            json!([[12], [13]]),
        ),
        (
            "SELECT SUM(x),COUNT(*),COUNT(x) FROM (VALUES (3),(NULL),(2)) t(x)",
            json!([[5, 3, 2]]),
        ),
        (
            "VALUES (1),(1),(2) UNION VALUES (2),(3) ORDER BY 1",
            json!([[1], [2], [3]]),
        ),
        (
            "VALUES (1),(2) UNION ALL VALUES (3),(4)",
            json!([[1], [2], [3], [4]]),
        ),
        (
            "VALUES (1),(1),(2) INTERSECT ALL VALUES (1),(1),(3) ORDER BY 1",
            json!([[1], [1]]),
        ),
        (
            "SELECT a.x,b.y FROM (VALUES (1),(2)) a(x) JOIN (VALUES (2),(3)) b(y) ON a.x=b.y",
            json!([[2, 2]]),
        ),
        (
            "SELECT x,ROW_NUMBER() OVER (ORDER BY x) AS n FROM (VALUES (3),(1),(2)) t(x) ORDER BY x",
            json!([[1, 1], [2, 2], [3, 3]]),
        ),
        ("VALUES ((SELECT 3)),((SELECT 1))", json!([[3], [1]])),
        (
            "WITH t AS (VALUES (1),(2)) SELECT SUM(col0) FROM t",
            json!([[3]]),
        ),
        (
            "VALUES (CASE WHEN FALSE THEN 1/0 ELSE 4 END),(5)",
            json!([[4], [5]]),
        ),
        (
            "SELECT SUM(col0) FROM (VALUES (9223372036854775807+1),(-9223372036854775807)) t",
            json!([[1]]),
        ),
        ("VALUES (1),(2) LIMIT 0", json!([])),
    ];
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
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
                for (sql, expected) in &cases {
                    assert_eq!(
                        rows(&query(&mut session, sql)),
                        *expected,
                        "{sql}; workers={workers}, batch={batch_size}, optimized={optimized}"
                    );
                }
                let names = query(&mut session, "VALUES (1,'a')").schema();
                assert_eq!(names.field(0).name(), "col0");
                assert_eq!(names.field(1).name(), "col1");
            }
        }
    }
}

#[test]
fn duckdb_values_type_inference_and_strict_text_coercion() {
    // Checked against DuckDB 2.0.0. VALUES resolves column types left-to-right;
    // unlike an explicit compatibility INTEGER cast, bad text must not become 0.
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    for (sql, expected) in [
        ("VALUES (NULL),(1)", json!([[null], [1]])),
        ("VALUES (NULL),(NULL)", json!([[null], [null]])),
        ("VALUES (1),('2')", json!([[1], [2]])),
        ("VALUES (1),('2.5')", json!([[1], [3]])),
        ("VALUES (1),(TRUE)", json!([[1], [1]])),
        ("VALUES (TRUE),(2)", json!([[1], [2]])),
        ("VALUES (1),(2.5)", json!([["1.0"], ["2.5"]])),
        ("VALUES (1),(CAST(2.5 AS DOUBLE))", json!([[1.0], [2.5]])),
        ("VALUES ('a'),(NULL),('b')", json!([["a"], [null], ["b"]])),
    ] {
        assert_eq!(rows(&query(&mut session, sql)), expected, "{sql}");
    }
    let dates = query(
        &mut session,
        "VALUES (DATE '2020-01-01'),(TIMESTAMP '2020-01-02')",
    );
    assert!(matches!(
        dates.column(0).data_type(),
        DataType::Timestamp(_, None)
    ));
    let timestamps = dates
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(timestamps.value(0), 1577836800000000);
    assert_eq!(timestamps.value(1), 1577923200000000);
    for sql in [
        "VALUES ('2'),(1)",
        "VALUES (NULL),('2'),(1)",
        "VALUES (1),(CAST('2' AS VARCHAR))",
        "VALUES (1.5),(TRUE)",
        "VALUES (1),(NULL),('bad')",
        "VALUES (1),(1,2)",
        "VALUES (1),('9223372036854775808')",
        "VALUES (unknown_column)",
        "VALUES (SUM(1))",
        "VALUES ()",
    ] {
        assert!(session.execute_sql(sql).is_err(), "must reject {sql}");
    }
    assert_eq!(
        rows(&query(&mut session, "SELECT CAST('bad' AS INTEGER)")),
        json!([[0]])
    );
}

#[test]
fn wide_values_use_balanced_plans_and_existing_resource_controls() {
    let sql = format!(
        "SELECT COUNT(*),SUM(col0) FROM (VALUES {}) t",
        (0..1025)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    assert_eq!(rows(&query(&mut session, &sql)), json!([[1025, 524800]]));
    let options = Options {
        memory_bytes: 16,
        ..Options::default()
    };
    let result = execute::prepare(&sql, &HashMap::new(), &options)
        .and_then(|plan| execute::execute(&plan, &options));
    assert!(result.is_err(), "tiny memory budget must not be ignored");
    let cancelled = Options::default();
    cancelled.cancellation.cancel();
    assert!(
        execute::prepare(&sql, &HashMap::new(), &cancelled)
            .and_then(|plan| execute::execute(&plan, &cancelled))
            .is_err()
    );
}
