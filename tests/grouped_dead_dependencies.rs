// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Unavailable source fields may validate dead arms, never supply grouped values.
use arrow::{array::Int64Array, record_batch::RecordBatch};
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
fn ints(b: &RecordBatch, col: usize) -> Vec<Option<i64>> {
    b.column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn bound_dead_arms_do_not_require_bare_group_representatives() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let mut s = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    relational_optimizations,
                    batch_size: 2,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            s.execute_sql("CREATE TABLE t(k INT,v INT)").unwrap();
            s.execute_sql("INSERT INTO t VALUES(1,9),(1,0),(2,4),(NULL,7)")
                .unwrap();
            for sql in [
                "SELECT COALESCE(37,v/k) AS n FROM t GROUP BY k ORDER BY n",
                "SELECT CASE WHEN TRUE THEN 37 ELSE v END AS n FROM t GROUP BY k ORDER BY n",
                "SELECT CASE 1 WHEN 2 THEN v ELSE 37 END AS n FROM t GROUP BY k ORDER BY n",
                "SELECT COALESCE(37,t.v/t.k) AS n FROM t GROUP BY t.k ORDER BY n",
                "SELECT COALESCE(37,v/(k-k)) AS n FROM t GROUP BY k ORDER BY n",
            ] {
                assert_eq!(
                    ints(&query(&mut s, sql).unwrap(), 0),
                    vec![Some(37); 3],
                    "{sql}"
                );
            }
            let b=query(&mut s,"SELECT k AS key_alias FROM t GROUP BY k HAVING COALESCE(1,v)=1 ORDER BY key_alias NULLS FIRST").unwrap();
            assert_eq!(ints(&b, 0), vec![None, Some(1), Some(2)]);
            for sql in [
                "SELECT COALESCE(37,missing) FROM t GROUP BY k",
                "SELECT COALESCE(37,LENGTH(v)) FROM t GROUP BY k",
                "SELECT CASE WHEN TRUE THEN 37 ELSE missing END FROM t GROUP BY k",
                "SELECT v FROM t GROUP BY k",
                "SELECT COALESCE(v,37) FROM t GROUP BY k",
                "SELECT k FROM t GROUP BY k HAVING v>0",
            ] {
                assert!(query(&mut s, sql).is_err(), "{sql}");
            }
            let error = query(
                &mut s,
                "SELECT COALESCE(CAST(1.0/0 AS INTEGER),v) FROM t GROUP BY k",
            )
            .unwrap_err();
            assert!(error.contains("unknown or ambiguous column"), "{error}");
            let error = query(
                &mut s,
                "SELECT COALESCE(CAST(1.0/0 AS INTEGER),k) FROM t GROUP BY k",
            )
            .unwrap_err();
            assert!(error.contains("division by zero"), "{error}");
            s.execute_sql("CREATE TABLE u(k INT,v INT)").unwrap();
            s.execute_sql("INSERT INTO u VALUES(1,2)").unwrap();
            assert!(
                query(
                    &mut s,
                    "SELECT COALESCE(37,v) FROM t CROSS JOIN u GROUP BY t.k"
                )
                .is_err()
            );
        }
    }
}
#[test]
fn having_aliases_preserve_input_priority_and_scope() {
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
        s.execute_sql("CREATE TABLE t(k INT,v INT)").unwrap();
        s.execute_sql("INSERT INTO t VALUES(1,9),(1,8),(2,7)")
            .unwrap();
        let b=query(&mut s,"SELECT k AS key_alias,COUNT(*) AS n FROM t GROUP BY k HAVING key_alias>1 ORDER BY key_alias").unwrap();
        assert_eq!(ints(&b, 0), vec![Some(2)]);
        let b = query(
            &mut s,
            "SELECT k,SUM(v) AS total FROM t GROUP BY k HAVING total>10 ORDER BY k",
        )
        .unwrap();
        assert_eq!(ints(&b, 0), vec![Some(1)]);
        assert_eq!(ints(&b, 1), vec![Some(17)]);
        // Input k wins over the SELECT alias k (whose value is always100).
        let b = query(&mut s, "SELECT 100 AS k FROM t GROUP BY t.k HAVING k>1").unwrap();
        assert_eq!(ints(&b, 0), vec![Some(100)]);
        // Input v also wins when it is unavailable after grouping; do not use
        // alias k instead to manufacture a representative value.
        assert!(query(&mut s, "SELECT k AS v FROM t GROUP BY k HAVING v>1").is_err());
        assert!(
            query(
                &mut s,
                "SELECT k AS z FROM t GROUP BY k HAVING (SELECT z)>0"
            )
            .is_err()
        );
    }
}
