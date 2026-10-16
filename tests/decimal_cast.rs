//! Additive explicit DECIMAL phase-1 controls; frozen corpora are unchanged.
use arrow::{array::Decimal128Array, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;

fn coefficients(batch: &RecordBatch) -> Vec<Option<i128>> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .collect()
}
fn controls(run: &mut impl FnMut(&str) -> Result<RecordBatch, String>) {
    for (source, p, s, expected) in [
        ("'1.25'", 3, 2, 125),
        ("1.245", 5, 2, 125),
        ("-1.245", 5, 2, -125),
        ("1.244", 5, 2, 124),
        ("-1.244", 5, 2, -124),
        ("1.246", 5, 2, 125),
        ("-1.246", 5, 2, -125),
        ("0.005", 2, 2, 1),
        ("-0.005", 2, 2, -1),
        ("0.004", 2, 2, 0),
        ("-0.004", 2, 2, 0),
        ("9.49", 1, 0, 9),
        ("-9.49", 1, 0, -9),
        ("12", 6, 3, 12000),
        ("+12", 6, 3, 12000),
        ("1.2", 6, 3, 1200),
        ("1 + 2", 4, 2, 300),
        ("CAST(1.245 AS DECIMAL(5,3))", 5, 2, 125),
        ("9223372036854775807 + 1", 38, 0, 9223372036854775808),
        (
            "99999999999999999999999999999999999999",
            38,
            0,
            99999999999999999999999999999999999999,
        ),
        ("0.000000000000000001", 18, 18, 1),
        ("NULLIF(1, 2)", 3, 1, 10),
    ] {
        for target in ["DECIMAL", "NUMERIC", "DEC"] {
            for expression in [
                format!("CAST(({source}) AS {target}({p},{s}))"),
                format!("({source})::{target}({p},{s})"),
            ] {
                let sql = format!("SELECT {expression} AS result");
                let batch = run(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
                assert_eq!(coefficients(&batch), vec![Some(expected)], "{sql}");
            }
        }
    }
    for sql in [
        "SELECT CAST(NULL AS DECIMAL(3,2))",
        "SELECT (NULL + 1)::NUMERIC(3,2)",
        "SELECT CAST(NULLIF(1,1) AS DEC(3,2))",
    ] {
        assert_eq!(coefficients(&run(sql).unwrap()), vec![None], "{sql}");
    }
    for source in [
        "9.5",
        "-9.5",
        "10",
        "-10",
        "99.9",
        "CAST(10 AS DECIMAL(3,0))",
    ] {
        let sql = format!("SELECT CAST({source} AS DECIMAL(1,0))");
        assert!(
            run(&sql)
                .unwrap_err()
                .contains("decimal cast precision overflow"),
            "{sql}"
        );
    }
    for sql in [
        "SELECT CAST(9.995 AS DECIMAL(3,2))",
        "SELECT CAST(-9.995 AS DECIMAL(3,2))",
        "SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,1))",
        "SELECT CASE WHEN TRUE THEN CAST(10 AS DECIMAL(1,0)) ELSE 0 END",
    ] {
        assert!(
            run(sql)
                .unwrap_err()
                .contains("decimal cast precision overflow"),
            "{sql}"
        );
    }
    for sql in [
        "SELECT CAST(NULL AS DECIMAL(0,0))",
        "SELECT CASE WHEN FALSE THEN CAST(1 AS DECIMAL(0,0)) ELSE 0 END",
        "SELECT CAST(1 AS DECIMAL(0,0))",
        "SELECT CAST(1 AS NUMERIC(39,0))",
        "SELECT CAST(1 AS DEC(20,19))",
        "SELECT CAST(1 AS DECIMAL(2,3))",
        "SELECT CAST(1 AS DECIMAL)",
        "SELECT 1::NUMERIC(5)",
        "SELECT CAST(CAST(1 AS REAL) AS DECIMAL(4,2))",
    ] {
        assert!(run(sql).is_err(), "{sql}");
    }
    for sql in [
        "SELECT CASE WHEN FALSE THEN CAST(10 AS DECIMAL(1,0)) ELSE CAST(7 AS DECIMAL(1,0)) END",
        "SELECT CASE WHEN TRUE THEN CAST(7 AS DECIMAL(1,0)) ELSE CAST(10 AS DECIMAL(1,0)) END",
    ] {
        assert_eq!(coefficients(&run(sql).unwrap()), vec![Some(7)], "{sql}");
    }
    // Retain lossless literal text behavior, including rejecting scale loss.
    assert_eq!(
        coefficients(&run("SELECT CAST('1.25' AS DECIMAL(3,2))").unwrap()),
        vec![Some(125)]
    );
    assert!(run("SELECT CAST('1.255' AS DECIMAL(3,2))").is_err());
}

#[test]
fn explicit_decimal_scalar_contract() {
    for optimized in [false, true] {
        let options = Options {
            relational_optimizations: optimized,
            ..Options::default()
        };
        controls(&mut |sql| {
            prepare(sql, &HashMap::new(), &options)
                .and_then(|p| execute(&p, &options))
                .map(|e| e.batch.clone())
        });
        let mut session = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
        controls(
            &mut |sql| match session.execute_sql(sql).map_err(|e| e.to_string())? {
                Output::Query { execution, .. } => Ok(execution.batch.clone()),
                _ => Err("expected query".into()),
            },
        );
    }
}

#[test]
fn explicit_decimal_columns_aggregates_and_reached_rows() {
    for optimized in [false, true] {
        for workers in [1, 4] {
            let options = Options {
                relational_optimizations: optimized,
                workers,
                ..Options::default()
            };
            let mut session =
                Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            session
                .execute_sql("CREATE TABLE dc (id INTEGER, d DECIMAL(8,3), t TEXT)")
                .unwrap();
            session.execute_sql("INSERT INTO dc VALUES (1, 1.245, '1'), (2, -1.245, '2'), (3, NULL, '3'), (10, 9.995, '4')").unwrap();
            for (sql, expected) in [
                (
                    "SELECT CAST(d AS DECIMAL(5,2)), id FROM dc ORDER BY id",
                    vec![Some(125), Some(-125), None, Some(1000)],
                ),
                (
                    "SELECT id::NUMERIC(4,1) AS a, id FROM dc ORDER BY id",
                    vec![Some(10), Some(20), Some(30), Some(100)],
                ),
                (
                    "SELECT CAST(d + 0.005 AS DECIMAL(5,2)), id FROM dc ORDER BY id",
                    vec![Some(125), Some(-124), None, Some(1000)],
                ),
                (
                    "SELECT CAST(id AS DECIMAL(4,1)), COUNT(*), id FROM dc GROUP BY id ORDER BY id",
                    vec![Some(10), Some(20), Some(30), Some(100)],
                ),
                (
                    "SELECT CAST(SUM(d) AS DECIMAL(6,2)) FROM dc",
                    vec![Some(1000)],
                ),
                (
                    "SELECT SUM(CAST(d AS DECIMAL(6,2))) FROM dc",
                    vec![Some(1000)],
                ),
                (
                    "SELECT CAST(x AS DECIMAL(5,2)) FROM (SELECT d AS x FROM dc) q",
                    vec![Some(125), Some(-125), None, Some(1000)],
                ),
                (
                    "SELECT CASE WHEN id < 10 THEN CAST(id AS DECIMAL(1,0)) ELSE CAST(0 AS DECIMAL(1,0)) END, id FROM dc ORDER BY id",
                    vec![Some(1), Some(2), Some(3), Some(0)],
                ),
                (
                    "SELECT CAST(id AS DECIMAL(1,0)), id FROM dc WHERE id < 10 ORDER BY id",
                    vec![Some(1), Some(2), Some(3)],
                ),
                (
                    "SELECT SUM(CAST(id AS DECIMAL(3,0))) + SUM(CAST(id AS DECIMAL(3,0))) FROM dc",
                    vec![Some(32)],
                ),
            ] {
                let Output::Query { execution, .. } = session
                    .execute_sql(sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                else {
                    panic!("expected query")
                };
                assert_eq!(coefficients(&execution.batch), expected, "{sql}");
            }
            for sql in [
                "SELECT CAST(d AS DECIMAL(3,2)) FROM dc",
                "SELECT SUM(CAST(id AS DECIMAL(1,0))) FROM dc",
                "SELECT SUM(CAST(id AS DECIMAL(3,0))) + SUM(CAST(id AS DECIMAL(1,0))) FROM dc",
            ] {
                let error = session
                    .execute_sql(sql)
                    .err()
                    .expect("expected cast overflow");
                assert!(
                    error.message.contains("decimal cast precision overflow"),
                    "{sql}: {error}"
                );
            }
            let error = session
                .execute_sql("SELECT CAST(t AS DECIMAL(4,2)) FROM dc")
                .err()
                .expect("expected unsupported text source");
            assert!(error.message.contains("exact numeric source"), "{error}");
        }
    }
}
