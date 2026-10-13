//! Grouped source-column identity; no arbitrary bare-column selection.
use arrow::{array::Int64Array, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn query(s: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
        panic!("query")
    };
    execution.batch.clone()
}
fn values(b: &RecordBatch, column: usize) -> Vec<Option<i64>> {
    b.column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn qualified_group_columns_share_identity_in_projection_and_having() {
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
            s.execute_sql("CREATE TABLE t(a INT,b INT)").unwrap();
            s.execute_sql("INSERT INTO t VALUES(1,10),(1,20),(2,30),(NULL,40)")
                .unwrap();
            for sql in [
                "SELECT t.a AS k FROM t GROUP BY a ORDER BY k NULLS FIRST",
                "SELECT a AS k FROM t GROUP BY t.a ORDER BY k NULLS FIRST",
                "SELECT +a AS k FROM t GROUP BY t.a ORDER BY k NULLS FIRST",
                "SELECT a AS k FROM t GROUP BY +t.a ORDER BY k NULLS FIRST",
                "SELECT ((a)) AS k FROM t GROUP BY (t.a) ORDER BY k NULLS FIRST",
                "SELECT cor.a AS k FROM t AS cor GROUP BY a,cor.a ORDER BY k NULLS FIRST",
                "SELECT a AS k FROM t AS cor GROUP BY cor.a,a ORDER BY k NULLS FIRST",
            ] {
                assert_eq!(
                    values(&query(&mut s, sql), 0),
                    vec![None, Some(1), Some(2)],
                    "{sql}"
                );
            }
            let b = query(
                &mut s,
                "SELECT t.a AS k,SUM(b) AS n FROM t GROUP BY a HAVING t.a IS NOT NULL ORDER BY k",
            );
            assert_eq!(values(&b, 0), vec![Some(1), Some(2)]);
            assert_eq!(values(&b, 1), vec![Some(30), Some(30)]);
            assert_eq!(
                values(
                    &query(
                        &mut s,
                        "SELECT -t.a + a AS k FROM t GROUP BY a ORDER BY k NULLS FIRST"
                    ),
                    0
                ),
                vec![None, Some(0), Some(0)]
            );
            // A SELECT alias is a GROUP BY fallback only when no input name wins.
            assert_eq!(
                values(
                    &query(
                        &mut s,
                        "SELECT a AS renamed FROM t GROUP BY renamed ORDER BY renamed NULLS FIRST"
                    ),
                    0
                ),
                vec![None, Some(1), Some(2)]
            );
            assert_eq!(
                values(
                    &query(
                        &mut s,
                        "SELECT t.a AS b FROM t GROUP BY a ORDER BY b NULLS FIRST"
                    ),
                    0
                ),
                vec![None, Some(1), Some(2)]
            );
            s.execute_sql("CREATE TABLE u(a INT)").unwrap();
            s.execute_sql("INSERT INTO u VALUES(9)").unwrap();
            // Rewriting cannot cross a subquery's independent name scope.
            let b = query(
                &mut s,
                "SELECT (SELECT a FROM u) AS v,COUNT(*) AS n FROM t GROUP BY t.a ORDER BY n",
            );
            assert_eq!(values(&b, 0), vec![Some(9), Some(9), Some(9)]);
            assert_eq!(values(&b, 1), vec![Some(1), Some(1), Some(2)]);
            for sql in [
                "SELECT b FROM t GROUP BY a",
                "SELECT t.b FROM t GROUP BY a",
                "SELECT a AS b FROM t GROUP BY b",
                "SELECT a FROM t CROSS JOIN u GROUP BY t.a",
                "SELECT u.a FROM t CROSS JOIN u GROUP BY t.a",
                "SELECT missing.a FROM t GROUP BY t.a",
                "SELECT a FROM t GROUP BY a+1",
                "SELECT b,MAX(a) FROM t",
            ] {
                assert!(
                    s.execute_sql(sql).is_err(),
                    "must not manufacture a grouped or bare column: {sql}"
                );
            }
        }
    }
}
