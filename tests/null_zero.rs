//! Approved SQLite NULL results and INTEGER/REAL zero arithmetic.
//! DECIMAL zero division remains a separate checked extension.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::{Expr, Input, Kind, Op, Value},
    memory::Budget,
    session::{Limits, Output, Session},
    spill::SpillOptions,
    types::Type,
};
use std::{collections::HashMap, sync::Arc};

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    match session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {}", e.message))
    {
        Output::Query { execution, .. } => execution.batch.clone(),
        _ => panic!("query expected"),
    }
}
fn ints(batch: &RecordBatch, index: usize) -> Vec<Option<i64>> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn all_null(batch: &RecordBatch, index: usize, rows: usize, ty: DataType) {
    assert_eq!(batch.num_rows(), rows);
    assert_eq!(batch.column(index).data_type(), &ty);
    assert!((0..rows).all(|i| assaydb::expr::is_sql_null(batch.column(index).as_ref(), i)));
}

#[test]
fn scalar_null_zero_and_remainder_preserve_types_and_checked_errors() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                ..Options::default()
            };
            for sql in [
                "SELECT NULL",
                "SELECT -NULL",
                "SELECT NULL+NULL",
                "SELECT NULL/NULL",
                "SELECT NULL%NULL",
                "SELECT COALESCE(NULL,NULL)",
                "SELECT CASE WHEN TRUE THEN NULL ELSE NULL END",
            ] {
                let plan = prepare(sql, &HashMap::new(), &options).unwrap();
                let result = execute(&plan, &options).unwrap();
                all_null(&result.batch, 0, 1, DataType::Null);
            }
            let mut session =
                Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            for sql in [
                "SELECT 1/0",
                "SELECT -1/-0",
                "SELECT 0/0",
                "SELECT 1%0",
                "SELECT -NULL+1",
                "SELECT (NULL+NULL)+1",
                "SELECT NULL%1",
                "SELECT 1%NULL",
                // Option B: these wide quotients are valid intermediates.
                "SELECT NULL+(-9223372036854775808/-1)",
                "SELECT (1/0)+(-9223372036854775808/-1)",
                "SELECT (-9223372036854775808/-1)%0",
            ] {
                all_null(&query(&mut session, sql), 0, 1, DataType::Int64);
            }
            for sql in [
                "SELECT CAST(1 AS REAL)/0",
                "SELECT 1/CAST('-0' AS REAL)",
                "SELECT CAST(1 AS REAL)%0",
                "SELECT 1%CAST('0.9' AS REAL)",
            ] {
                all_null(&query(&mut session, sql), 0, 1, DataType::Float64);
            }
            for (sql, expected) in [
                ("SELECT 7%3", 1),
                ("SELECT -7%3", -1),
                ("SELECT 7%-3", 1),
                ("SELECT -7%-3", -1),
                ("SELECT -9223372036854775808%-1", 0),
                ("SELECT 9007199254740993%2", 1),
                ("SELECT (9223372036854775807+1)%3", 2),
                ("SELECT COALESCE(1/0,7)", 7),
                ("SELECT CASE WHEN 1/0 IS NULL THEN 9 ELSE 8 END", 9),
            ] {
                assert_eq!(
                    ints(&query(&mut session, sql), 0),
                    vec![Some(expected)],
                    "{sql}"
                );
            }
            for (sql, expected) in [
                ("SELECT CAST('7.9' AS REAL)%3", 1.0),
                ("SELECT -7%CAST('3.9' AS REAL)", -1.0),
                ("SELECT CAST('1e100' AS REAL)%2", 1.0),
                ("SELECT CAST('-1e100' AS REAL)%3", -2.0),
                ("SELECT 9007199254740993%CAST(2 AS REAL)", 1.0),
            ] {
                let batch = query(&mut session, sql);
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    expected,
                    "{sql}"
                );
            }
            for (sql, expected) in [
                ("SELECT -9223372036854775808/-1", "BIGINT overflow"),
                ("SELECT 7.0/0", "division by zero"),
                ("SELECT 7/CAST('0' AS DECIMAL(10,0))", "division by zero"),
                ("SELECT 7.0%3", "exact DECIMAL remainder"),
                ("SELECT '7'%3", "remainder requires"),
                ("SELECT CASE WHEN TRUE THEN NULL ELSE missing END", "column"),
            ] {
                let e = session.execute_sql(sql).err().expect(sql);
                assert!(e.message.contains(expected), "{sql}: {}", e.message);
            }
        }
    }
}

#[test]
fn null_columns_cardinality_grouping_distinct_order_join_and_materialization() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 2,
                ..Options::default()
            };
            let mut s = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            s.execute_sql("CREATE TABLE t(id INT,n INT,d INT)").unwrap();
            s.execute_sql(
                "INSERT INTO t VALUES (1,7,3),(2,-7,0),(3,NULL,0),(4,7,-3),(5,0,NULL),(6,-7,-3)",
            )
            .unwrap();
            all_null(
                &query(&mut s, "SELECT NULL AS z FROM t ORDER BY z"),
                0,
                6,
                DataType::Null,
            );
            all_null(
                &query(&mut s, "SELECT NULL FROM t WHERE FALSE"),
                0,
                0,
                DataType::Null,
            );
            all_null(
                &query(&mut s, "SELECT DISTINCT NULL AS z FROM t ORDER BY z"),
                0,
                1,
                DataType::Null,
            );
            all_null(
                &query(
                    &mut s,
                    "SELECT -NULL AS z,COUNT(*) FROM t GROUP BY -NULL ORDER BY z",
                ),
                0,
                1,
                DataType::Null,
            );
            assert_eq!(
                ints(
                    &query(&mut s, "SELECT COUNT(NULL),COUNT(DISTINCT NULL) FROM t"),
                    0
                ),
                vec![Some(0)]
            );
            for sql in [
                "SELECT SUM(NULL),AVG(NULL),MIN(NULL),MAX(NULL) FROM t",
                "SELECT SUM(NULL),AVG(NULL),MIN(NULL),MAX(NULL) FROM t WHERE FALSE",
                "SELECT SUM(DISTINCT NULL),AVG(DISTINCT NULL),MIN(NULL),MAX(NULL) FROM t",
            ] {
                let b = query(&mut s, sql);
                for i in 0..4 {
                    all_null(&b, i, 1, DataType::Null);
                }
            }
            let b = query(&mut s, "SELECT n/d,n%d,id FROM t ORDER BY id");
            assert_eq!(
                ints(&b, 0),
                vec![Some(2), None, None, Some(-2), None, Some(2)]
            );
            assert_eq!(
                ints(&b, 1),
                vec![Some(1), None, None, Some(1), None, Some(-1)]
            );
            let b = query(
                &mut s,
                "WITH q AS (SELECT id,NULL AS z FROM t) SELECT q.id,r.z FROM q LEFT JOIN q r ON q.z=r.z ORDER BY 1",
            );
            all_null(&b, 1, 6, DataType::Null);
            let b = query(
                &mut s,
                "WITH q AS (SELECT id,NULL AS z FROM t) SELECT q.z FROM q INNER JOIN q r ON q.z=r.z",
            );
            all_null(&b, 0, 0, DataType::Null);
            all_null(
                &query(&mut s, "SELECT NULL UNION SELECT NULL"),
                0,
                1,
                DataType::Null,
            );
            all_null(
                &query(&mut s, "SELECT NULL UNION ALL SELECT NULL"),
                0,
                2,
                DataType::Null,
            );
            all_null(
                &query(&mut s, "SELECT NULL INTERSECT SELECT NULL"),
                0,
                1,
                DataType::Null,
            );
            all_null(
                &query(&mut s, "SELECT NULL EXCEPT SELECT NULL"),
                0,
                0,
                DataType::Null,
            );
            all_null(&query(&mut s, "SELECT (SELECT NULL)"), 0, 1, DataType::Null);
            s.execute_sql("UPDATE t SET n=n/d WHERE id=2").unwrap();
            assert_eq!(
                ints(&query(&mut s, "SELECT n FROM t WHERE id=2"), 0),
                vec![None]
            );
        }
    }
}

#[test]
fn zero_remainder_budget_is_retained_and_released_with_output() {
    let budget = Budget::new(1 << 20);
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(7), Some(-7), None, Some(9)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let values = values.slice(1, 3);
    let input = Input {
        columns: vec![Some(Value::adopt(values, &budget).unwrap())],
        len: 3,
    };
    let e = Expr {
        ty: Type::Int,
        kind: Kind::Binary(
            Op::Mod,
            Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Column(0),
            }),
            Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Literal(assaydb::expr::Literal::Num(0)),
            }),
        ),
    };
    let base = budget.used();
    let value = e.eval(&input, &budget).unwrap();
    assert!(budget.used() > base);
    assert!((0..3).all(|i| assaydb::expr::is_sql_null(value.array.as_ref(), i)));
    drop(value);
    assert_eq!(budget.used(), base);
    drop(input);
    assert_eq!(budget.used(), 0);
    let tiny = Budget::new(1);
    assert!(
        e.eval(
            &Input {
                columns: vec![],
                len: 3
            },
            &tiny
        )
        .err()
        .unwrap()
        .contains("accounted memory")
    );
    assert_eq!(tiny.used(), 0);
}

#[test]
fn null_group_and_remainder_force_spill_and_cleanup() {
    let dir = std::env::temp_dir().join(format!("assaydb-null-zero-spill-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    {
        let options = Options {
            workers: 2,
            batch_size: 2,
            spill: Some(SpillOptions {
                directory: dir.clone(),
                sort_run_bytes: 1024,
                ..SpillOptions::default()
            }),
            ..Options::default()
        };
        let mut s = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
        s.execute_sql("CREATE TABLE t(id INT,v INT)").unwrap();
        s.execute_sql("INSERT INTO t VALUES(1,7),(2,7),(3,NULL),(4,-7),(5,0)")
            .unwrap();
        all_null(
            &query(&mut s, "SELECT DISTINCT NULL AS z FROM t ORDER BY z"),
            0,
            1,
            DataType::Null,
        );
        let b = query(
            &mut s,
            "SELECT z,SUM(z),AVG(z),COUNT(v%0) FROM (SELECT NULL AS z,v FROM t) q GROUP BY z ORDER BY z",
        );
        for i in 0..3 {
            all_null(&b, i, 1, DataType::Null);
        }
        assert_eq!(ints(&b, 3), vec![Some(0)]);
        let b = query(&mut s, "SELECT id,v%0 AS z FROM t ORDER BY z,id");
        all_null(&b, 1, 5, DataType::Int64);
    }
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir(&dir).unwrap();
}
