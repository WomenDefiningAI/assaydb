// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
use arrow::{
    array::{BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!()
    };
    execution.batch.clone()
}
fn bools(batch: &RecordBatch, col: usize) -> Vec<Option<bool>> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn setup(workers: usize, optimize: bool) -> Session {
    let mut session = Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            relational_optimizations: optimize,
            batch_size: 2,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES(1),(1),(2),(NULL)")
        .unwrap();
    session.execute_sql("CREATE TABLE empty_t(x INT)").unwrap();
    session
}
#[test]
fn empty_in_subqueries_bind_without_runtime_or_cardinality_evaluation() {
    for workers in [1, 10] {
        for optimize in [false, true] {
            let mut s = setup(workers, optimize);
            for (sql, expected, rows) in [
                ("SELECT (SELECT CAST(1.0/0 AS INTEGER)) IN ()", false, 1),
                ("SELECT (SELECT CAST(1.0/0 AS INTEGER)) NOT IN ()", true, 1),
                ("SELECT (SELECT x FROM t) IN ()", false, 1),
                ("SELECT (SELECT x FROM t) NOT IN () FROM t", true, 4),
                (
                    "SELECT (EXISTS(SELECT x FROM t WHERE CAST(1.0/0 AS INTEGER)=0)) IN ()",
                    false,
                    1,
                ),
                (
                    "SELECT (1 IN (SELECT CAST(1.0/0 AS INTEGER))) NOT IN ()",
                    true,
                    1,
                ),
                (
                    "SELECT ((SELECT CAST(1.0/0 AS INTEGER)) IN ()) IN ()",
                    false,
                    1,
                ),
            ] {
                let b = query(&mut s, sql);
                assert_eq!(bools(&b, 0), vec![Some(expected); rows], "{sql}");
            }
            let b = query(
                &mut s,
                "SELECT x FROM t WHERE (SELECT CAST(1.0/0 AS INTEGER)) NOT IN ()",
            );
            assert_eq!(b.num_rows(), 4);
            let b = query(&mut s, "SELECT (SELECT CAST(1.0/0 AS INTEGER)) IN ()");
            assert_eq!(
                b.schema().field(0).name(),
                "(SELECT CAST(1.0 / 0 AS INTEGER)) IN ()"
            );
            for sql in [
                "SELECT (SELECT missing FROM t) IN ()",
                "SELECT (SELECT x,x FROM t) IN ()",
                "SELECT (SELECT x FROM missing_table) NOT IN ()",
                "SELECT ((SELECT x FROM t) + 'text') IN ()",
            ] {
                assert!(s.execute_sql(sql).is_err(), "must bind {sql}");
            }
        }
    }
}
#[test]
fn dead_aggregate_inputs_preserve_scalar_and_group_cardinality() {
    for workers in [1, 10] {
        for optimize in [false, true] {
            let mut s = setup(workers, optimize);
            for table in ["t", "empty_t"] {
                let sql = format!(
                    "SELECT COUNT(CAST(1.0/0 AS INTEGER)) IN (), COUNT(CAST(1.0/0 AS INTEGER)) NOT IN (), SUM(99999999999999999999999999999999999999*2) IN () FROM {table}"
                );
                let b = query(&mut s, &sql);
                assert_eq!(b.num_rows(), 1);
                assert_eq!(bools(&b, 0), vec![Some(false)]);
                assert_eq!(bools(&b, 1), vec![Some(true)]);
                assert_eq!(bools(&b, 2), vec![Some(false)]);
            }
            let b = query(
                &mut s,
                "SELECT x AS k, COUNT(CAST(1.0/0 AS INTEGER)) IN () AS unused FROM t GROUP BY x ORDER BY k NULLS LAST",
            );
            assert_eq!(b.num_rows(), 3);
            assert_eq!(bools(&b, 1), vec![Some(false); 3]);
            let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(
                keys.iter().collect::<Vec<_>>(),
                vec![Some(1), Some(2), None]
            );
            let b = query(
                &mut s,
                "SELECT x AS k FROM t GROUP BY x HAVING COUNT(CAST(1.0/0 AS INTEGER)) NOT IN () ORDER BY k NULLS LAST",
            );
            assert_eq!(b.num_rows(), 3);
            let b = query(
                &mut s,
                "SELECT x AS k,COUNT(CAST(1.0/0 AS INTEGER)) NOT IN () FROM empty_t GROUP BY x",
            );
            assert_eq!(b.num_rows(), 0);
            let b = query(
                &mut s,
                "SELECT COUNT(*) AS n,COUNT(CAST(1.0/0 AS INTEGER)) IN () FROM t",
            );
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                4
            );
            // A shared aggregate used anywhere outside an empty operand is live.
            for sql in [
                "SELECT COUNT(CAST(1.0/0 AS INTEGER)),COUNT(CAST(1.0/0 AS INTEGER)) IN () FROM t",
                "SELECT COUNT(CAST(1.0/0 AS INTEGER)) IN (),COUNT(CAST(1.0/0 AS INTEGER)) FROM t",
                "SELECT COUNT(CAST(1.0/0 AS INTEGER)) IN () FROM t HAVING COUNT(CAST(1.0/0 AS INTEGER))>0",
            ] {
                assert!(
                    s.execute_sql(sql)
                        .err()
                        .unwrap()
                        .message
                        .contains("division by zero"),
                    "{sql}"
                );
            }
            for sql in [
                "SELECT COUNT(missing) IN () FROM t",
                "SELECT (COUNT(*)+'text') IN () FROM t",
                "SELECT x,COUNT(CAST(1.0/0 AS INTEGER)) IN () FROM t",
                "SELECT SUM('text') NOT IN () FROM t",
            ] {
                assert!(s.execute_sql(sql).is_err(), "must bind {sql}");
            }
        }
    }
}
