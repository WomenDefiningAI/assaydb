//! Positional CTE names: DuckDB 2.0 controls and scope/materialization coverage.
//! Name-collision and alias-count goldens were independently queried in DuckDB
//! 2.0.0; the CTE body values are unchanged by alias binding.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected query: {sql}")
    };
    execution.batch
}

fn rows(batch: &RecordBatch) -> Value {
    Value::Array(
        (0..batch.num_rows())
            .map(|row| {
                Value::Array(
                    batch
                        .columns()
                        .iter()
                        .map(|array| {
                            if array.is_null(row) {
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
                                DataType::Boolean => json!(
                                    array
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .unwrap()
                                        .value(row)
                                ),
                                other => panic!("unexpected test type {other}"),
                            }
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

fn configurations(mut test: impl FnMut(&mut Session)) {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for relational_optimizations in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
                        relational_optimizations,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                test(&mut session);
            }
        }
    }
}

#[test]
fn duckdb_column_names_and_alias_counts() {
    configurations(|session| {
        for (sql, names, expected) in [
            (
                "WITH c(a) AS (SELECT 1 AS x,2 AS y) SELECT * FROM c",
                vec!["a", "y"],
                json!([[1, 2]]),
            ),
            (
                "WITH c(a,b,c) AS (SELECT 1 AS x,2 AS y) SELECT * FROM c",
                vec!["a", "b"],
                json!([[1, 2]]),
            ),
            (
                "WITH c(a,a) AS (SELECT 1 AS x,2 AS y) SELECT * FROM c",
                vec!["a", "a_1"],
                json!([[1, 2]]),
            ),
            (
                "WITH c(a,A) AS (SELECT 1 AS x,2 AS y) SELECT * FROM c",
                vec!["a", "A_1"],
                json!([[1, 2]]),
            ),
            (
                "WITH c(a,a_1,a) AS (SELECT 1,2,3) SELECT * FROM c",
                vec!["a", "a_1", "a_2"],
                json!([[1, 2, 3]]),
            ),
            (
                "WITH c(a,a,a_1) AS (SELECT 1,2,3) SELECT * FROM c",
                vec!["a", "a_1", "a_1_1"],
                json!([[1, 2, 3]]),
            ),
            (
                "WITH c(a,a,A_1,a) AS (SELECT 1,2,3,4) SELECT * FROM c",
                vec!["a", "a_1", "A_1_1", "a_2"],
                json!([[1, 2, 3, 4]]),
            ),
            (
                "WITH c(a) AS (SELECT 1 AS x,2 AS a) SELECT * FROM c",
                vec!["a", "a_1"],
                json!([[1, 2]]),
            ),
            (
                "WITH c(\"Renamed Value\") AS (SELECT 42 AS x) SELECT * FROM c",
                vec!["Renamed Value"],
                json!([[42]]),
            ),
            (
                "WITH c(\"Renamed Value\") AS (SELECT 42 AS x) SELECT \"Renamed Value\" AS value FROM c",
                vec!["value"],
                json!([[42]]),
            ),
        ] {
            let batch = query(session, sql);
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                names,
                "{sql}"
            );
            assert_eq!(rows(&batch), expected, "{sql}");
        }
    });
}

#[test]
fn aliases_preserve_scope_values_types_and_reused_bodies() {
    configurations(|session| {
        session
            .execute_sql("CREATE TABLE t(x INTEGER,y INTEGER)")
            .unwrap();
        session
            .execute_sql("INSERT INTO t VALUES (1,10),(2,NULL),(2,20)")
            .unwrap();
        for (sql, expected) in [
            (
                "WITH c(a,b) AS (SELECT x,y FROM t) SELECT l.a,l.b,r.b FROM c l JOIN c r ON l.a=r.a WHERE l.a=2 ORDER BY l.b NULLS FIRST,r.b NULLS FIRST",
                json!([[2, null, null], [2, null, 20], [2, 20, null], [2, 20, 20]]),
            ),
            (
                "WITH c(a) AS (SELECT x,y FROM t), d(b,c) AS (SELECT a,y FROM c) SELECT b,c FROM d ORDER BY b,c NULLS FIRST",
                json!([[1, 10], [2, null], [2, 20]]),
            ),
            (
                "WITH c(a) AS (SELECT 1) SELECT a,(WITH c(a) AS (SELECT 2) SELECT a FROM c) AS inner_value FROM c",
                json!([[1, 2]]),
            ),
            (
                "WITH c(a) AS (SELECT x FROM t), d(b) AS (SELECT a+1 FROM c) SELECT b FROM d ORDER BY b",
                json!([[2], [3], [3]]),
            ),
            (
                "WITH c(a,b) AS (SELECT x,SUM(y) FROM t GROUP BY x) SELECT a,b FROM c ORDER BY a",
                json!([[1, 10], [2, 20]]),
            ),
            (
                "WITH c(a,b) AS (SELECT x,y FROM t WHERE FALSE) SELECT COUNT(*) FROM c",
                json!([[0]]),
            ),
            (
                "WITH t(a,b) AS (SELECT x,y FROM t) SELECT a,b FROM t ORDER BY a,b NULLS FIRST",
                json!([[1, 10], [2, null], [2, 20]]),
            ),
            (
                "WITH c(a,b) AS (SELECT x,y FROM t) SELECT renamed.n FROM c AS renamed(n,m) WHERE renamed.m=20",
                json!([[2]]),
            ),
            (
                "WITH c(a) AS (SELECT 9223372036854775807+1) SELECT a>9223372036854775807 FROM c",
                json!([[true]]),
            ),
            (
                "WITH c(a) AS (SELECT CAST(1.25 AS DECIMAL(10,2))) SELECT a=CAST(1.25 AS DECIMAL(10,2)) FROM c",
                json!([[true]]),
            ),
            (
                "WITH c(a,a) AS (SELECT 1,2) SELECT a_1,a FROM c",
                json!([[2, 1]]),
            ),
        ] {
            assert_eq!(rows(&query(session, sql)), expected, "{sql}");
        }
    });
}

#[test]
fn renamed_columns_do_not_leak_old_or_nonexistent_names() {
    configurations(|session| {
        for (sql, name) in [
            (
                "WITH c(a) AS (SELECT 1 AS old_name) SELECT old_name FROM c",
                "old_name",
            ),
            ("WITH c(a,b) AS (SELECT 1) SELECT b FROM c", "b"),
            (
                "WITH c(a) AS (SELECT 1) SELECT c.a FROM c AS renamed",
                "c.a",
            ),
        ] {
            let error = session
                .execute_sql(sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            // Keep the established unknown-name classification; alias support
            // does not redefine existing engine error categories.
            assert_eq!(error.kind, "engine", "{sql}: {error}");
            assert_eq!(
                error.message,
                format!("unknown or ambiguous column: {name}")
            );
        }
        // Explicit materialization hints preserve alias binding and results.
        for sql in [
            "WITH c(a) AS MATERIALIZED (SELECT 1) SELECT a FROM c",
            "WITH c(a) AS NOT MATERIALIZED (SELECT 1) SELECT a FROM c",
        ] {
            assert_eq!(rows(&query(session, sql)), json!([[1]]), "{sql}");
        }
        // This broader identifier-representation limitation remains explicit;
        // accepting it and stripping the dot would return the wrong schema.
        let error = session
            .execute_sql("WITH c(\"a.b\") AS (SELECT 1) SELECT * FROM c")
            .err()
            .unwrap();
        assert_eq!(error.kind, "unsupported");
        assert!(error.message.contains("dotted output names"));
    });
}
