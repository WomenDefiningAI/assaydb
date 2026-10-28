// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Error-aware NULL facts do not invent grouped representatives or swallow errors.
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
#[test]
fn null_facts_keep_binding_errors_and_cardinality() {
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
            s.execute_sql("INSERT INTO t VALUES(1,9),(1,0),(2,NULL),(NULL,-9223372036854775808)")
                .unwrap();
            for sql in [
                "SELECT k FROM t GROUP BY k HAVING NULL=v",
                "SELECT k FROM t GROUP BY k HAVING NOT NULL>=-v",
                "SELECT k FROM t GROUP BY k HAVING NULL BETWEEN v*17 AND NULL",
                "SELECT k FROM t GROUP BY k HAVING 48 BETWEEN 73 AND v*28",
                "SELECT k FROM t GROUP BY k HAVING NULL=NULLIF(NULL,v+1)",
            ] {
                assert_eq!(query(&mut s, sql).unwrap().num_rows(), 0, "{sql}");
            }
            for sql in [
                "SELECT CASE WHEN NULL=v THEN CAST(1.0/0 AS INTEGER) ELSE k END AS x FROM t GROUP BY k ORDER BY x NULLS FIRST",
                "SELECT CASE v WHEN CAST(NULL AS INTEGER) THEN CAST(1.0/0 AS INTEGER) ELSE k END AS x FROM t GROUP BY k ORDER BY x NULLS FIRST",
            ] {
                let b = query(&mut s, sql).unwrap();
                assert_eq!(
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, Some(1), Some(2)],
                    "{sql}"
                );
            }
            let b = query(
                &mut s,
                "SELECT NULLIF(CAST(NULL AS INTEGER),v*17) AS x FROM t GROUP BY k",
            )
            .unwrap();
            assert_eq!(b.num_rows(), 3);
            assert_eq!(b.column(0).null_count(), 3);
            // Approved dead-arm typing: the unreachable REAL result cannot
            // promote the surviving grouped Int or change integer division.
            let sql =
                "SELECT CASE WHEN NULL=v THEN CAST(1 AS REAL) ELSE k END / 2 FROM t GROUP BY k";
            let result = query(&mut s, sql).unwrap();
            assert_eq!(
                result.column(0).data_type(),
                &arrow::datatypes::DataType::Int64,
                "{sql}"
            );
            let mut actual = result
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            actual.sort();
            assert_eq!(actual, vec![None, Some(0), Some(1)], "{sql}");
            for sql in [
                "SELECT k FROM t GROUP BY k HAVING NULL=missing",
                "SELECT k FROM t GROUP BY k HAVING NULL=LENGTH(v)",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/0",
                "SELECT k FROM t GROUP BY k HAVING NULL=v*v*v",
                "SELECT NULLIF(CAST(NULL AS INTEGER),v/0) FROM t GROUP BY k",
                "SELECT CASE WHEN NULL=v/0 THEN k ELSE k END FROM t GROUP BY k",
            ] {
                assert!(query(&mut s, sql).is_err(), "{sql}");
            }
            // New facts cannot hide an upstream aggregate error even when HAVING
            // rejects all groups, nor change the error of an already legal plan.
            let e = query(
                &mut s,
                "SELECT SUM(CAST(1.0/0 AS INTEGER)) FROM t GROUP BY k HAVING NULL=v",
            )
            .unwrap_err();
            assert!(e.contains("division by zero"), "{e}");
            let e = query(
                &mut s,
                "SELECT k FROM t GROUP BY k HAVING NULL=(CAST(k/0.0 AS INTEGER))",
            )
            .unwrap_err();
            assert!(e.contains("division by zero"), "{e}");
            // Option B keeps the cast exact; returning its wide INTEGER errors.
            let e=query(&mut s,"SELECT CASE WHEN NULL=v THEN 0 ELSE CAST(9223372036854775808 AS INTEGER) END FROM t GROUP BY k").unwrap_err();
            assert_eq!(e, "engine: BIGINT overflow");
            let e=query(&mut s,"SELECT CASE WHEN NULL=v THEN 0 ELSE CAST(9223372036854775808 AS BIGINT) END FROM t GROUP BY k").unwrap_err();
            assert_eq!(e, "engine: bigint out of range");
            s.execute_sql("CREATE TABLE d(k INT,v DECIMAL(38,1),w DECIMAL(38,0))")
                .unwrap();
            s.execute_sql("INSERT INTO d VALUES(1,1.0,99999999999999999999999999999999999999)")
                .unwrap();
            assert_eq!(
                query(&mut s, "SELECT k FROM d GROUP BY k HAVING NULL=v")
                    .unwrap()
                    .num_rows(),
                0
            );
            assert!(query(&mut s, "SELECT k FROM d GROUP BY k HAVING NULL=(v=w)").is_err());
            let e = query(&mut s, "SELECT k FROM d GROUP BY k,v,w HAVING NULL=(v=w)").unwrap_err();
            assert!(e.contains("overflow"), "{e}");
            assert!(query(&mut s,"SELECT q.k FROM (SELECT 1 AS k,X'80' AS b) q GROUP BY q.k HAVING NULL=CAST(q.b AS TEXT)").is_err());
            s.execute_sql("CREATE TABLE empty_t(k INT,v INT)").unwrap();
            assert_eq!(
                query(
                    &mut s,
                    "SELECT NULLIF(CAST(NULL AS INTEGER),v) FROM empty_t GROUP BY k"
                )
                .unwrap()
                .num_rows(),
                0
            );
        }
    }
}
