//! Approved mathematical Int/Float comparisons; DECIMAL remains its own contract.
use arrow::array::{ArrayRef, BooleanArray, Decimal128Array, Float64Array};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::{Expr, Input, Kind, Op, Value},
    memory::Budget,
    session::{Limits, Output, Session},
    types::Type,
};
use std::{collections::HashMap, sync::Arc};

fn cases() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/exact-numeric-comparison.json")).unwrap()
}
fn expected(cmp: i64) -> [bool; 6] {
    [cmp == 0, cmp != 0, cmp < 0, cmp <= 0, cmp > 0, cmp >= 0]
}
fn query(sql: &str, o: &Options) -> arrow::record_batch::RecordBatch {
    execute(&prepare(sql, &HashMap::new(), o).unwrap(), o)
        .unwrap()
        .batch
}
fn check(batch: &arrow::record_batch::RecordBatch, values: &[Option<bool>]) {
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), values.len());
    for (column, value) in batch.columns().iter().zip(values) {
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![*value]
        );
    }
}
const MIXED_IN: &str = "mixed Int/Float IN subquery comparison";
fn refused(sql: &str, options: &Options, session: &mut Session) {
    let error = prepare(sql, &HashMap::new(), options)
        .err()
        .expect("mixed IN must not bind");
    assert!(error.contains(MIXED_IN), "{sql}: {error}");
    let error = session
        .execute_sql(sql)
        .err()
        .expect("mixed IN must not execute");
    assert_eq!(error.kind, "unsupported", "{sql}: {error}");
    assert!(error.message.contains(MIXED_IN), "{sql}: {error}");
}

#[test]
fn public_comparison_and_membership_matrix() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                relational_optimizations,
                batch_size: 2,
                ..Options::default()
            };
            let mut session =
                Session::new(&std::env::temp_dir(), options.clone(), Limits::default()).unwrap();
            for case in cases()["cases"].as_array().unwrap() {
                let a = case["integer"].as_str().unwrap();
                let b = format!("CAST('{}' AS REAL)", case["real"].as_str().unwrap());
                let cmp = case["comparison"].as_i64().unwrap();
                for (left, right, order) in [
                    (a.to_string(), b.clone(), cmp),
                    (b.clone(), a.to_string(), -cmp),
                ] {
                    let projection = ["=", "<>", "<", "<=", ">", ">="]
                        .into_iter()
                        .map(|op| format!("({left}){op}({right})"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT {projection},({left}) IN ({right}),({left}) IN (SELECT {right}),({left}) NOT IN (SELECT {right}),NULLIF({left},{right}) IS NULL"
                    );
                    // Keep every original 46-query oracle SQL record as an
                    // explicit refusal. Direct/list/NULLIF controls are additive.
                    refused(&sql, &options, &mut session);
                    let sql = format!(
                        "SELECT {projection},({left}) IN ({right}),({left}) NOT IN ({right}),NULLIF({left},{right}) IS NULL"
                    );
                    let values = expected(order)
                        .into_iter()
                        .chain([order == 0, order != 0, order == 0])
                        .map(Some)
                        .collect::<Vec<_>>();
                    check(&query(&sql, &options), &values);
                    let Output::Query { execution, .. } = session.execute_sql(&sql).unwrap() else {
                        panic!()
                    };
                    check(&execution.batch, &values);
                }
            }
            for sql in [
                "SELECT CAST(NULL AS INTEGER)=CAST('2' AS REAL)",
                "SELECT CAST('2' AS REAL)<CAST(NULL AS INTEGER)",
            ] {
                check(&query(sql, &options), &[None]);
            }
            for sql in [
                "SELECT 9007199254740993 IN(SELECT CAST('9007199254740992' AS REAL) UNION ALL SELECT NULL)",
                "SELECT CAST('9007199254740992' AS REAL) NOT IN(SELECT 9007199254740993 UNION ALL SELECT NULL)",
                "SELECT 9007199254740993 IN(SELECT CAST('9007199254740992' AS REAL) WHERE FALSE),NULL IN(SELECT CAST('1' AS REAL) WHERE FALSE)",
            ] {
                refused(sql, &options, &mut session);
            }
            check(
                &query(
                    "SELECT NULL IN(SELECT CAST('1' AS REAL) WHERE FALSE)",
                    &options,
                ),
                &[Some(false)],
            );
            // DECIMAL/Float retains its prior conversion relation, unlike Int/Float.
            check(
                &query(
                    "SELECT CAST('9007199254740993' AS DECIMAL(38,0))=CAST('9007199254740992' AS REAL)",
                    &options,
                ),
                &[Some(true)],
            );
            // Wide Int intermediate can be compared before public narrowing.
            check(
                &query(
                    "SELECT (9223372036854775807+1)=CAST('9223372036854775808' AS REAL),(9223372036854775807+2)>CAST('9223372036854775808' AS REAL)",
                    &options,
                ),
                &[Some(true), Some(true)],
            );
            for sql in [
                "SELECT (CAST(1.0/0 AS INTEGER))=CAST('1' AS REAL)",
                "SELECT CAST('1' AS REAL)=(CAST(1.0/0 AS INTEGER))",
            ] {
                assert!(
                    execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options)
                        .err()
                        .unwrap()
                        .contains("division by zero")
                );
            }
            check(
                &query(
                    "SELECT CASE WHEN FALSE THEN CAST(1.0/0 AS INTEGER)=CAST('1' AS REAL) ELSE TRUE END",
                    &options,
                ),
                &[Some(true)],
            );
        }
    }
}
#[test]
fn table_filter_join_and_membership_preserve_original_rows() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let o = Options {
                workers,
                relational_optimizations,
                batch_size: 1,
                ..Options::default()
            };
            let mut s = Session::new(&std::env::temp_dir(), o, Limits::default()).unwrap();
            for sql in [
                "CREATE TABLE l(id INTEGER,n INTEGER)",
                "CREATE TABLE r(x REAL)",
                "INSERT INTO l VALUES(1,9007199254740992),(2,9007199254740993),(3,NULL)",
                // Assignment to REAL converts these exactly representable 2^53
                // integer literals; VALUES does not accept CAST expressions.
                "INSERT INTO r VALUES(9007199254740992),(9007199254740992),(NULL)",
            ] {
                s.execute_sql(sql).unwrap();
            }
            for sql in [
                "SELECT COUNT(*)=1 FROM l WHERE n IN(SELECT x FROM r)",
                "SELECT COUNT(*)=1 FROM l WHERE n NOT IN(SELECT x FROM r WHERE x IS NOT NULL)",
            ] {
                let error = s.execute_sql(sql).err().expect("mixed IN refusal");
                assert_eq!(error.kind, "unsupported");
                assert!(error.message.contains(MIXED_IN), "{sql}: {error}");
            }
            for sql in [
                "SELECT COUNT(*)=1 FROM l WHERE n=CAST('9007199254740992' AS REAL)",
                "SELECT COUNT(*)=2 FROM l JOIN r ON l.n=r.x",
            ] {
                let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
                    panic!()
                };
                check(&execution.batch, &[Some(true)]);
            }
        }
    }
}
#[test]
fn sliced_arrays_nan_and_budget_lifetimes() {
    let budget = Budget::new(32 * 1024);
    let ints: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(0),
            Some(9007199254740993),
            Some(0),
            None,
            Some(1),
        ])
        .with_precision_and_scale(38, 0)
        .unwrap(),
    );
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(9007199254740992.0),
        Some(-0.0),
        Some(1.0),
        Some(f64::NAN),
    ]));
    let input = Input {
        len: 4,
        columns: vec![
            Some(Value::adopt(ints.slice(1, 4), &budget).unwrap()),
            Some(Value::adopt(floats.slice(1, 4), &budget).unwrap()),
        ],
    };
    let make = |op| Expr {
        ty: Type::Bool,
        kind: Kind::Binary(
            op,
            Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Column(0),
            }),
            Box::new(Expr {
                ty: Type::Float,
                kind: Kind::Column(1),
            }),
        ),
    };
    let value = make(Op::Eq).eval(&input, &budget).unwrap();
    assert_eq!(
        value
            .array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), Some(true), None, Some(false)]
    );
    assert!(
        make(Op::Ne)
            .eval(&input, &budget)
            .unwrap()
            .array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(3)
    );
    let tiny = Budget::new(1);
    assert!(make(Op::Eq).eval(&input, &tiny).is_err());
    assert_eq!(tiny.used(), 0);
    drop(input);
    assert!(budget.used() >= value.array.get_array_memory_size());
    drop(value);
    assert_eq!(budget.used(), 0);
}
