use arrow::{
    array::{Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn session(workers: usize, optimized: bool) -> Session {
    let mut s = Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            relational_optimizations: optimized,
            batch_size: 1,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    s.execute_sql("CREATE TABLE t(id INTEGER NOT NULL,v INTEGER)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(9007199254740993,NULL),(2,7)")
        .unwrap();
    s
}
fn query(s: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match s.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("query"),
    }
}
fn integers(s: &mut Session, sql: &str, expected: &[Option<i64>]) {
    let batch = query(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(batch.column(0).data_type(), &DataType::Int64, "{sql}");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), expected, "{sql}");
}
#[test]
fn unreachable_alternatives_do_not_round_or_change_ancestor_integer_division() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for expression in [
                "COALESCE(id,CAST(5 AS REAL))",
                "COALESCE(id,5.0)",
                "CASE WHEN id IS NULL THEN CAST(5 AS REAL) ELSE id END",
                "CASE WHEN id IS NOT NULL THEN id ELSE CAST(5 AS REAL) END",
                "CAST(COALESCE(id,CAST(5 AS REAL)) AS INTEGER)",
                "ABS(COALESCE(id,CAST(5 AS REAL)))",
                "NULLIF(COALESCE(id,CAST(5 AS REAL)),CAST(0 AS REAL))",
                "COALESCE(NULLIF(id,NULL),CAST(5 AS REAL))",
            ] {
                integers(
                    &mut s,
                    &format!("SELECT {expression} FROM t ORDER BY 1"),
                    &[Some(2), Some(9007199254740993)],
                );
            }
            for fallback in ["CAST(5 AS REAL)", "5.0"] {
                integers(
                    &mut s,
                    &format!("SELECT COALESCE(id/2,{fallback})/2 FROM t ORDER BY 1"),
                    &[Some(0), Some(2251799813685248)],
                );
            }
            integers(
                &mut s,
                "SELECT MIN(COALESCE(id,CAST(5 AS REAL))) FROM t",
                &[Some(2)],
            );
            integers(
                &mut s,
                "SELECT MAX(COALESCE(id,CAST(5 AS REAL))) FROM t",
                &[Some(9007199254740993)],
            );
        }
    }
}
#[test]
fn aliases_ctes_materialization_and_grouped_facts_preserve_reachable_type() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for sql in [
                "SELECT x FROM (SELECT COALESCE(id,CAST(5 AS REAL)) AS x FROM t) q ORDER BY 1",
                "WITH q AS(SELECT id AS x FROM t) SELECT COALESCE(x,CAST(5 AS REAL)) FROM q ORDER BY 1",
                "WITH q AS(SELECT id AS x FROM t) SELECT COALESCE(a.x,CAST(5 AS REAL)) FROM q a JOIN q b ON a.x=b.x ORDER BY 1",
                "SELECT COALESCE(MIN(id),CAST(5 AS REAL)) FROM t GROUP BY id ORDER BY 1",
                "SELECT COALESCE(SUM(id),CAST(5 AS REAL)) FROM t GROUP BY id ORDER BY 1",
            ] {
                integers(&mut s, sql, &[Some(2), Some(9007199254740993)]);
            }
            integers(
                &mut s,
                "SELECT COALESCE(AVG(CAST(NULL AS INTEGER)),3)/2 FROM t",
                &[Some(1)],
            );
            integers(
                &mut s,
                "WITH q AS(SELECT AVG(CAST(NULL AS INTEGER)) AS n FROM t) SELECT COALESCE(n,3)/2 FROM q",
                &[Some(1)],
            );
            integers(
                &mut s,
                "SELECT (CASE WHEN COUNT(NULL)=0 THEN 3 ELSE CAST(5 AS REAL) END)/2 FROM t",
                &[Some(1)],
            );
        }
    }
}
#[test]
fn outer_join_and_empty_scalar_keep_null_extension() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            // Old: refused as row-dependent mixed Int/Float. New (Helen, Oct 15):
            // the NULL-extended Int arm is promoted to DOUBLE. DuckDB 1.5.6
            // returns 5.0 and 9007199254740992.0 (2^53+1 rounded) for both.
            for sql in [
                "SELECT COALESCE(b.id,CAST(5 AS REAL)) FROM t a LEFT JOIN t b ON a.id=b.id AND b.id>2 ORDER BY 1",
                "SELECT CASE WHEN b.p THEN a.id ELSE CAST(5 AS REAL) END FROM t a LEFT JOIN (SELECT id,TRUE AS p FROM t) b ON a.id=b.id AND b.id>2 ORDER BY 1",
            ] {
                let batch = query(&mut s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(
                    values
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .collect::<Vec<_>>(),
                    vec![
                        Some(5.0_f64.to_bits()),
                        Some(9007199254740992.0_f64.to_bits())
                    ],
                    "{sql}"
                );
            }
            // RIGHT/FULL are supported now (tests/joins_sets.rs). Their
            // NULL-extended Int side is promoted to DOUBLE like LEFT's (Helen,
            // Oct 15: mixed Int/Float conditionals promote like DuckDB; this
            // integration replaces the joins card's base-era refusal check).
            // DuckDB 1.5.6 answers RIGHT 5.0, 2^53 and FULL 2.0, 5.0, 2^53
            // (2^53+1 rounded); the integer form answers like DuckDB too.
            for (join, wanted, floats) in [
                (
                    "RIGHT",
                    [Some(3), Some(9007199254740993)].as_slice(),
                    vec![Some(5.0_f64), Some(9007199254740992.0)],
                ),
                (
                    "FULL",
                    [Some(2), Some(3), Some(9007199254740993)].as_slice(),
                    vec![Some(2.0_f64), Some(5.0), Some(9007199254740992.0)],
                ),
            ] {
                let sql = format!(
                    "SELECT COALESCE(a.id,CAST(5 AS REAL)) FROM t a {join} JOIN t b ON a.id=b.id AND a.id>2 ORDER BY 1"
                );
                let batch = query(&mut s, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(
                    values
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .collect::<Vec<_>>(),
                    floats
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .collect::<Vec<_>>(),
                    "{sql}"
                );
                integers(
                    &mut s,
                    &format!(
                        "SELECT COALESCE(a.id,3) FROM t a {join} JOIN t b ON a.id=b.id AND a.id>2 ORDER BY 1"
                    ),
                    wanted,
                );
            }
            integers(
                &mut s,
                "SELECT COALESCE((SELECT id FROM t WHERE FALSE),3)/2",
                &[Some(1)],
            );
            let b=query(&mut s,"SELECT CASE WHEN (SELECT TRUE FROM t WHERE FALSE) THEN CAST(1 AS REAL) ELSE CAST(5 AS REAL) END").unwrap();
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                5.0
            );
            integers(
                &mut s,
                "SELECT COALESCE(b.id,3)/2 FROM t a LEFT JOIN t b ON a.id=b.id AND b.id>2 ORDER BY 1",
                &[Some(1), Some(4503599627370496)],
            );
        }
    }
}
#[test]
fn kleene_conditions_are_not_arithmetic_null_propagation() {
    let mut s = session(1, true);
    for condition in ["(id>0 OR NULL) IS NULL", "(id<0 AND NULL) IS NULL"] {
        let sql = format!(
            "SELECT CASE WHEN {condition} THEN CAST(1 AS REAL) ELSE CAST(5 AS REAL) END FROM t"
        );
        let batch = query(&mut s, &sql).unwrap();
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![Some(5.0), Some(5.0)],
            "{sql}"
        );
    }
}
#[test]
fn dead_values_bind_normally_and_live_conditions_keep_errors() {
    let mut s = session(1, true);
    for sql in [
        "SELECT COALESCE(id,missing) FROM t",
        "SELECT CASE WHEN id IS NULL THEN missing ELSE id END FROM t",
        "SELECT COALESCE(id,CAST(5 AS REAL)),missing FROM t",
    ] {
        assert!(
            query(&mut s, sql)
                .unwrap_err()
                .contains("unknown or ambiguous column"),
            "{sql}"
        );
    }
    assert!(
        query(&mut s, "SELECT COALESCE(id,'text') FROM t")
            .unwrap_err()
            .contains("incompatible CASE")
    );
    // The NULL comparison proves the branch unselected only upon successful
    // evaluation. Its eager Decimal zero-divisor error must still be evaluated.
    let sql = "SELECT CASE WHEN (id/CAST('0' AS DECIMAL(10,0))) IS NULL THEN CAST(5 AS REAL) ELSE id END FROM t";
    let error = query(&mut s, sql).unwrap_err();
    assert!(error.contains("division by zero"), "{sql}: {error}");
    let sql = "SELECT COALESCE(NULLIF(id*99999999999999999999999999999999999999,id*99999999999999999999999999999999999999),3) FROM t";
    let error = query(&mut s, sql).unwrap_err();
    assert!(error.contains("overflow"), "{sql}: {error}");
    // Float reflexivity cannot suppress evaluation errors in its operands.
    // Use a homogeneous Float fallback to inspect its eager Decimal error.
    let sql = "SELECT COALESCE(NULLIF(id/CAST('0' AS DECIMAL(10,0)),id/CAST('0' AS DECIMAL(10,0))),CAST(3 AS REAL)) FROM t";
    let error = query(&mut s, sql).unwrap_err();
    assert!(error.contains("division by zero"), "{sql}: {error}");
}
#[test]
fn reachable_decimal_and_homogeneous_float_contracts_stay_intact() {
    let mut s = session(1, true);
    let b = query(&mut s, "SELECT COALESCE(v,5.0)/2 FROM t ORDER BY 1").unwrap();
    // Decimal-involved division retains the existing Float result contract.
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2.5), Some(3.5)]
    );
    let b = query(&mut s, "SELECT COALESCE(v,5.0) FROM t ORDER BY 1").unwrap();
    assert!(matches!(
        b.column(0).data_type(),
        DataType::Decimal128(_, 1)
    ));
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(50), Some(70)]
    );
    let b = query(
        &mut s,
        "SELECT COALESCE(CAST(id AS REAL),CAST(5 AS REAL))/2 FROM t ORDER BY 1",
    )
    .unwrap();
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );
}

#[test]
fn normalization_limit_fails_closed_only_when_type_repair_is_needed() {
    let mut s = session(1, true);
    let zeros = (0..7).fold("0".to_string(), |e, _| format!("({e}+{e})"));
    let large = format!("(id+{zeros})");
    let error = query(
        &mut s,
        &format!("SELECT COALESCE({large},CAST(5 AS REAL)) FROM t"),
    )
    .unwrap_err();
    assert!(
        error.contains("conditional reachable-type normalization proof limit"),
        "{error}"
    );
    integers(
        &mut s,
        &format!("SELECT COALESCE({large},5) FROM t ORDER BY 1"),
        &[Some(2), Some(9007199254740993)],
    );
    let error = query(
        &mut s,
        &format!("SELECT COALESCE({large},CAST(5 AS REAL)),missing FROM t"),
    )
    .unwrap_err();
    assert!(error.contains("unknown or ambiguous column"), "{error}");
}
