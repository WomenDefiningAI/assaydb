// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Interval facts prove all values, including NULL and signed division edges.
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
fn total_same_int_proofs_keep_unsafe_dependencies_live() {
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
            s.execute_sql("INSERT INTO t VALUES(1,-9223372036854775808),(1,9223372036854775807),(2,0),(2,NULL),(NULL,9)").unwrap();
            for sql in [
                "SELECT k FROM t GROUP BY k HAVING NULL=NULLIF(v,58)",
                "SELECT k FROM t GROUP BY k HAVING NULL=-NULLIF(v,k*9)+92",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/2",
                // Option B makes these divisions total over BIGINT inputs.
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(-1)",
                "SELECT k FROM t GROUP BY k HAVING NULL=(-v)/1",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(-2)",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(CASE WHEN k=1 THEN 2 ELSE 3 END)",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(CASE WHEN k=1 THEN CAST(NULL AS INTEGER) ELSE 2 END)",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(CASE WHEN NULL=v THEN CAST(1.0/0 AS INTEGER) ELSE 2 END)",
            ] {
                assert_eq!(
                    query(&mut s, sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"))
                        .num_rows(),
                    0,
                    "{sql}"
                );
            }
            for sql in [
                "SELECT CASE k WHEN CAST(NULL AS INTEGER) THEN v WHEN 60 THEN NULL ELSE NULL END AS x FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER),-27*NULLIF(-v,k*9)) AS x FROM t GROUP BY k",
            ] {
                let b = query(&mut s, sql).unwrap();
                assert_eq!(b.num_rows(), 3);
                assert_eq!(b.column(0).null_count(), 3, "{sql}");
            }
            // Approved dead-arm typing: the unreachable REAL result cannot
            // promote the surviving grouped Int or change integer division.
            let sql = "SELECT CASE WHEN NULL=v THEN CAST(1 AS REAL) ELSE k END/2 FROM t GROUP BY k";
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
                "SELECT k FROM t GROUP BY k HAVING NULL=v/0",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/v",
                "SELECT k FROM t GROUP BY k HAVING NULL=v/(CASE WHEN k=1 THEN 0 ELSE 2 END)",
                "SELECT k FROM t GROUP BY k HAVING NULL=NULLIF(v,v*v*v)",
                "SELECT k FROM t GROUP BY k HAVING NULL=NULLIF(v,missing)",
                "SELECT k FROM t GROUP BY k HAVING NULL=NULLIF(v,'text')",
                "SELECT CASE k WHEN 60 THEN CAST(NULL AS INTEGER) ELSE NULLIF(CAST(NULL AS INTEGER),v/0) END FROM t GROUP BY k",
            ] {
                assert!(query(&mut s, sql).is_err(), "{sql}");
            }
            let e = query(
                &mut s,
                "SELECT SUM(CAST(1.0/0 AS INTEGER)) FROM t GROUP BY k HAVING NULL=v/2",
            )
            .unwrap_err();
            assert!(e.contains("division by zero"), "{e}");
            assert_eq!(
                query(&mut s, "SELECT k FROM t GROUP BY k,v HAVING NULL=v/(-1)")
                    .unwrap()
                    .num_rows(),
                0
            );
            // A live grouped quotient retains integer division and original order.
            let b = query(
                &mut s,
                "SELECT k/2 AS q FROM t GROUP BY k ORDER BY q NULLS FIRST",
            )
            .unwrap();
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(0), Some(1)]
            );
        }
    }
}

#[test]
fn grouped_column_range_evidence_follows_identity_not_wide_storage_or_names() {
    let mut s = Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    s.execute_sql("CREATE TABLE t(k INT,v INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,2),(1,3)").unwrap();
    // A direct-column derived projection changes physical storage but preserves
    // both grouped and validation-only raw INTEGER value ranges.
    for sql in [
        "SELECT k FROM (SELECT k,v FROM t) AS d GROUP BY k HAVING NULL=-NULLIF(v,k*9)+92",
        "WITH d AS (SELECT k,v FROM t) SELECT k FROM d GROUP BY k HAVING NULL=-NULLIF(v,k*9)+92",
    ] {
        assert_eq!(
            query(&mut s, sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .num_rows(),
            0
        );
    }
    // The same names and precision-38 physical fields are not evidence that
    // derived arithmetic values fit BIGINT. k*9 can overflow precision38 here,
    // so the nongrouped dependency must stay live and binding must refuse.
    let sql = "SELECT k FROM (SELECT 9223372036854775807*9223372036854775807 AS k,v FROM t) AS d GROUP BY k HAVING NULL=-NULLIF(v,k*9)+92";
    assert!(query(&mut s, sql).is_err(), "{sql}");
}
