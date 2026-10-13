// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Error-aware finite bounds, without changing Float evaluation or result types.
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn query(session: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match session.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("query"),
    }
}

#[test]
fn finite_cast_proofs_preserve_typed_null_groups_and_eager_errors() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let mut session = Session::new(
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
            session
                .execute_sql("CREATE TABLE t(k INT,v INT,f DOUBLE)")
                .unwrap();
            session.execute_sql("INSERT INTO t VALUES (1,-9223372036854775808,1.0),(1,9223372036854775807,2.0),(2,0,3.0),(2,NULL,NULL),(NULL,9007199254740993,4.0)").unwrap();
            for rhs in [
                "CAST(46 AS INTEGER) + -CAST(v AS REAL)",
                "-(CAST(v AS REAL) - CAST(v AS REAL)) + CAST(v AS REAL)",
                // Successful Int intermediates can exceed the public i64 range.
                "CAST(v*v AS REAL) - CAST(v+v AS REAL)",
                "CASE WHEN 1=1 THEN CAST(v AS REAL) ELSE CAST(CAST(1.0/0 AS INTEGER) AS REAL) END",
            ] {
                let sql =
                    format!("SELECT -NULLIF(CAST(NULL AS INTEGER),{rhs}) AS n FROM t GROUP BY k");
                let result = query(&mut session, &sql).unwrap();
                assert_eq!(result.num_rows(), 3, "{sql}");
                assert_eq!(result.column(0).data_type(), &DataType::Int64, "{sql}");
                assert_eq!(result.column(0).null_count(), 3, "{sql}");
                let sql = format!("SELECT k FROM t GROUP BY k HAVING NULL={rhs}");
                assert_eq!(query(&mut session, &sql).unwrap().num_rows(), 0, "{sql}");
            }
            // Approved dead-arm typing: the unreachable REAL result cannot
            // promote the surviving grouped Int or change integer division.
            let sql = "SELECT CASE WHEN NULL=v THEN CAST(1 AS REAL) ELSE k END/2 FROM t GROUP BY k";
            let result = query(&mut session, sql).unwrap();
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
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v/0.0 AS REAL)) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v*v*v AS REAL)) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v AS REAL)+f) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), -f) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v AS REAL)*CAST(v AS REAL)) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v AS REAL)/CAST(v AS REAL)) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(missing AS REAL)) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CASE WHEN 1=1 THEN CAST(v AS REAL) ELSE CAST(missing AS REAL) END) FROM t GROUP BY k",
                "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v AS REAL)+'text') FROM t GROUP BY k",
            ] {
                assert!(query(&mut session, sql).is_err(), "{sql}");
            }
            // Successful binding bypasses the grouped proof. Original eager
            // division/overflow and left-to-right operand failures still occur.
            for (sql, error) in [
                (
                    "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v/0.0 AS REAL)) FROM t GROUP BY k,v",
                    "division by zero",
                ),
                (
                    "SELECT NULLIF(CAST(NULL AS INTEGER), CAST(v*v*v AS REAL)) FROM t GROUP BY k,v",
                    "overflow",
                ),
                (
                    "SELECT SUM(CAST(1.0/0 AS INTEGER)) FROM t GROUP BY k HAVING NULL=CAST(v AS REAL)+46",
                    "division by zero",
                ),
                (
                    "SELECT NULLIF(CAST(NULL AS INTEGER),CAST(CAST(1.0/0 AS INTEGER) AS REAL)+CAST(FACTORIAL(21) AS REAL)) FROM t GROUP BY k",
                    "division by zero",
                ),
                (
                    "SELECT NULLIF(CAST(NULL AS INTEGER),CAST(FACTORIAL(21) AS REAL)+CAST(CAST(1.0/0 AS INTEGER) AS REAL)) FROM t GROUP BY k",
                    "FACTORIAL",
                ),
            ] {
                let actual = query(&mut session, sql).unwrap_err();
                assert!(actual.contains(error), "{sql}: {actual}");
            }
        }
    }
}
