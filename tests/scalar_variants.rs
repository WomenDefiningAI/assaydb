// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Supplemental SQLite NULLIF/unary-plus contracts; immutable upstream SLT unchanged.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;

fn ints(batch: &RecordBatch) -> Vec<Option<i64>> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn contract(run: &mut impl FnMut(&str) -> Result<RecordBatch, assaydb::Error>) {
    for (sql, expected) in [
        ("SELECT NULLIF(20,-9)", Some(20)),
        ("SELECT 36+-25*-20*NULLIF(20,-+9)", Some(10036)),
        ("SELECT NULLIF(89,+74*-44)-63", Some(26)),
        ("SELECT NULLIF(7,7)", None),
        ("SELECT NULLIF(7,NULL)", Some(7)),
        ("SELECT NULLIF(CAST(NULL AS INT),7)", None),
        ("SELECT NULLIF(NULL,7)+1", None),
        ("SELECT NULLIF(7,7.00)", None),
        (
            "SELECT NULLIF(9007199254740993,CAST('9007199254740992' AS REAL))",
            Some(9007199254740993),
        ),
        (
            "SELECT NULLIF(9007199254740992,CAST('9007199254740992' AS REAL))",
            None,
        ),
        (
            "SELECT NULLIF(9223372036854775807,CAST('9223372036854775808' AS REAL))",
            Some(i64::MAX),
        ),
        (
            "SELECT NULLIF(-9223372036854775808,CAST('-9223372036854775808' AS REAL))",
            None,
        ),
        (
            "SELECT CASE WHEN FALSE THEN NULLIF(CAST(1.0/0 AS INTEGER),2) ELSE 9 END",
            Some(9),
        ),
        ("SELECT COALESCE(NULLIF(1,1),9)", Some(9)),
        ("SELECT +CAST(NULL AS INTEGER)", None),
    ] {
        assert_eq!(
            ints(&run(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))),
            vec![expected],
            "{sql}"
        );
    }
    for (sql, expected) in [
        ("SELECT NULLIF('é','é')", None),
        ("SELECT NULLIF('é','e')", Some("é")),
        ("SELECT +('raw text')", Some("raw text")),
        ("SELECT NULLIF('a','A')", Some("a")),
        ("SELECT NULLIF('',NULL)", Some("")),
    ] {
        let b = run(sql).unwrap();
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![expected],
            "{sql}"
        );
    }
    for sql in [
        "SELECT NULLIF(X'00ff',X'00ff')",
        "SELECT NULLIF(CAST(NULL AS TEXT),'a')",
    ] {
        assert!(run(sql).unwrap().column(0).is_null(0), "{sql}");
    }
    let b = run("SELECT +X'00ff',NULLIF(X'00ff',X'00fe'),+TRUE").unwrap();
    for i in 0..2 {
        assert_eq!(
            b.column(i)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            [0, 255]
        );
    }
    assert!(
        b.column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let b=run("SELECT NULLIF(CAST('1.25' AS DECIMAL(10,2)),1),NULLIF(CAST('1.25' AS DECIMAL(10,2)),1.250)").unwrap();
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        125
    );
    assert_eq!(b.column(0).data_type(), &DataType::Decimal128(38, 2));
    assert!(b.column(1).is_null(0));
    for (sql, error) in [
        (
            "SELECT NULLIF(CAST(1.0/0 AS INTEGER),2)",
            "division by zero",
        ),
        (
            "SELECT NULLIF(1,CAST(1.0/0 AS INTEGER))",
            "division by zero",
        ),
        (
            "SELECT NULLIF(NULL,CAST(1.0/0 AS INTEGER))+1",
            "division by zero",
        ),
        (
            "SELECT NULLIF(CAST(1.0/0 AS INTEGER),-9223372036854775808/-1)",
            "division by zero",
        ),
        (
            "SELECT NULLIF(-9223372036854775808/-1,CAST(1.0/0 AS INTEGER))",
            // The first argument is now valid exact INTEGER; the second errors.
            "division by zero",
        ),
        (
            "SELECT NULLIF(1,'1')",
            "comparison requires compatible types",
        ),
        (
            "SELECT NULLIF('a',X'61')",
            "comparison requires compatible types",
        ),
        ("SELECT NULLIF(1)", "exactly two"),
        ("SELECT NULLIF(1,2,3)", "exactly two"),
        ("SELECT NULLIF(DISTINCT 1,2)", "unsupported"),
        ("SELECT +missing", "unknown or ambiguous column"),
    ] {
        let e = run(sql).unwrap_err();
        assert!(e.contains(error), "{sql}: {e}");
    }
}
#[test]
fn scalar_variants_public_query_and_session() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 3,
                ..Options::default()
            };
            contract(&mut |sql| {
                prepare(sql, &HashMap::new(), &options)
                    .and_then(|p| execute(&p, &options))
                    .map(|r| r.batch.clone())
            });
            let mut session =
                Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            contract(
                &mut |sql| match session.execute_sql(sql).map_err(|e| e.message)? {
                    Output::Query { execution, .. } => Ok(execution.batch.clone()),
                    _ => Err("expected query".into()),
                },
            );
            session
                .execute_sql("CREATE TABLE t(id INT,n INT,s TEXT)")
                .unwrap();
            session
                .execute_sql("INSERT INTO t VALUES(1,7,'seven'),(2,7,NULL),(3,NULL,'x'),(4,8,'x')")
                .unwrap();
            let mut query = |sql| match session.execute_sql(sql).unwrap() {
                Output::Query { execution, .. } => execution.batch.clone(),
                _ => panic!("expected query"),
            };
            assert_eq!(
                ints(&query("SELECT NULLIF(n,7) AS v,id FROM t ORDER BY id")),
                vec![None, None, None, Some(8)]
            );
            assert_eq!(
                ints(&query("SELECT id FROM t WHERE +(s) IS NULL")),
                vec![Some(2)]
            );
            assert_eq!(
                ints(&query("SELECT COUNT(NULLIF(n,7)) FROM t")),
                vec![Some(1)]
            );
            assert_eq!(
                ints(&query("SELECT SUM(NULLIF(n,7)) FROM t")),
                vec![Some(8)]
            );
            assert_eq!(ints(&query("SELECT NULLIF(COUNT(*),4) FROM t")), vec![None]);
            // Repeated expression compilation and projection remapping preserve both
            // operands; filtered rows must not evaluate the zero divisor.
            assert_eq!(
                ints(&query("SELECT SUM(NULLIF(n,7))+COUNT(NULLIF(n,7)) FROM t")),
                vec![Some(9)]
            );
            assert_eq!(
                ints(&query(
                    "SELECT NULLIF(CAST(1.0/(id-1) AS INTEGER),0),id FROM t WHERE id>1 ORDER BY id"
                )),
                vec![Some(1), None, None]
            );
        }
    }
}
