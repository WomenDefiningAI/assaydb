// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Conditional reachability preserves the chosen integer type before division.
use arrow::{
    array::{Array, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn query(s: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match s.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("query"),
    }
}
fn ints(batch: &RecordBatch) -> Vec<i64> {
    (0..batch.num_columns())
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("chosen integer type")
                .value(0)
        })
        .collect()
}
#[test]
fn all_fifty_three_original_wrong_answers() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/conditional-type-wrong-answers.json")).unwrap();
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let mut s = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    relational_optimizations,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            for case in cases.as_array().unwrap() {
                let sql = case["sql"].as_str().unwrap();
                let expected: Vec<i64> = case["expected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n.as_i64().unwrap())
                    .collect();
                let b = query(&mut s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
                assert_eq!(b.num_rows(), 1, "{sql}");
                assert_eq!(ints(&b), expected, "{sql}");
            }
        }
    }
}
#[test]
fn lazy_errors_binding_checks_and_aggregate_work_remain() {
    for relational_optimizations in [false, true] {
        let mut s = Session::new(
            &std::env::temp_dir(),
            Options {
                relational_optimizations,
                ..Options::default()
            },
            Limits::default(),
        )
        .unwrap();
        for (sql, expected) in [
            ("SELECT COALESCE(3,CAST(4 AS REAL))/2", 1),
            (
                "SELECT COALESCE(CAST(NULL AS INTEGER),-3,CAST(4 AS REAL))/2",
                -1,
            ),
            (
                "SELECT CASE WHEN FALSE THEN CAST(4 AS REAL) ELSE 3 END/2",
                1,
            ),
            ("SELECT CASE -60 WHEN 58 THEN -AVG(51) ELSE 29 END/2", 14),
            ("SELECT COALESCE(SUM(3),AVG(4))/2", 1),
            ("SELECT COALESCE(AVG(CAST(NULL AS INTEGER)),3)/2", 1),
            ("SELECT CASE COUNT(*) WHEN 48 THEN AVG(4) ELSE 3 END/2", 1),
            ("SELECT COALESCE(7,CAST(1.0/0 AS INTEGER))/2", 3),
            (
                "SELECT CASE WHEN TRUE THEN 7 ELSE CAST(1.0/0 AS INTEGER) END/2",
                3,
            ),
            (
                "SELECT CASE WHEN FALSE THEN CAST(1.0/0 AS INTEGER) ELSE 7 END/2",
                3,
            ),
            ("SELECT 10-COALESCE(NULLIF(3,4),AVG(5))/2", 9),
            ("SELECT COALESCE(NULLIF(3,3),3,AVG(5))/2", 1),
            (
                "SELECT CASE WHEN NULLIF(3,3) IS NULL THEN 3 ELSE AVG(5) END/2",
                1,
            ),
            ("SELECT COALESCE(SUM(NULLIF(3,4)),AVG(5))/2", 1),
            ("SELECT COALESCE(AVG(NULLIF(3,3)),3)/2", 1),
            (
                "SELECT COALESCE(3,NULLIF(CAST(1.0/0 AS INTEGER),2),AVG(5))",
                3,
            ),
            ("SELECT 10-COALESCE(ABS(-3),AVG(5))/2", 9),
            ("SELECT 10-COALESCE(FACTORIAL(3),AVG(5))/4", 9),
        ] {
            assert_eq!(ints(&query(&mut s, sql).unwrap()), vec![expected], "{sql}");
        }
        for sql in [
            "SELECT COALESCE(7,missing)",
            "SELECT CASE WHEN FALSE THEN missing ELSE 7 END",
            "SELECT COALESCE(7,LENGTH(1))",
        ] {
            assert!(query(&mut s, sql).is_err(), "{sql}");
        }
        for sql in [
            "SELECT COALESCE(CAST(1.0/0 AS INTEGER),7)",
            "SELECT CASE WHEN CAST(1.0/0 AS INTEGER)=0 THEN 7 ELSE 9 END",
            "SELECT COALESCE(7,AVG(CAST(1.0/0 AS INTEGER)))",
            "SELECT NULLIF(3,CAST(1.0/0 AS INTEGER))",
            "SELECT COALESCE(3,AVG(NULLIF(CAST(1.0/0 AS INTEGER),2)))",
        ] {
            let e = query(&mut s, sql).unwrap_err();
            assert!(e.contains("division by zero"), "{sql}: {e}");
        }
        s.execute_sql("CREATE TABLE t(v INT)").unwrap();
        s.execute_sql("INSERT INTO t VALUES(3),(NULL),(7)").unwrap();
        let b = query(&mut s, "SELECT COALESCE(v,9)/2 AS n FROM t ORDER BY n").unwrap();
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 3, 4]
        );
        let b = query(&mut s, "SELECT SUM(v) FROM t WHERE FALSE").unwrap();
        assert!(b.column(0).is_null(0));
        // A filtered One source is not an unconditional one-row aggregate fact.
        let b = query(&mut s, "SELECT COUNT(*) WHERE FALSE").unwrap();
        assert_eq!(ints(&b), vec![0]);
    }
}
