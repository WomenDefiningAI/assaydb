// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Approved IN-subquery comparisons retain existing equality rules; the mixed
//! Int/Float pair, formerly refused, compares in DOUBLE (Helen, October 15).
//! Explicit decimal division preserves Float coverage after approved Int/Int truncation.
#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::array::{BooleanArray, Int64Array};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;
fn session(workers: usize, optimized: bool) -> Session {
    Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            relational_optimizations: optimized,
            batch_size: 2,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap()
}
fn bools(session: &mut Session, sql: &str) -> Vec<Option<bool>> {
    let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
        panic!()
    };
    execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn ids(session: &mut Session, sql: &str) -> Vec<i64> {
    let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
        panic!()
    };
    execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
#[test]
fn null_and_mixed_numeric_membership_agree_in_projection_where_and_not() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            s.execute_sql("CREATE TABLE t(id INT, n INT)").unwrap();
            s.execute_sql("INSERT INTO t VALUES(1,1),(2,2),(3,NULL),(4,3)")
                .unwrap();
            s.execute_sql("CREATE TABLE r(n DECIMAL(8,2))").unwrap();
            for expression in ["NULL", "1", "1.23", "(1.0/1)"] {
                assert_eq!(
                    bools(&mut s, &format!("SELECT {expression} IN(SELECT n FROM r)")),
                    vec![Some(false)]
                );
                assert_eq!(
                    bools(
                        &mut s,
                        &format!("SELECT {expression} NOT IN(SELECT n FROM r)")
                    ),
                    vec![Some(true)]
                );
            }
            s.execute_sql("INSERT INTO r VALUES(1.00),(1.00),(NULL),(2.50)")
                .unwrap();
            assert_eq!(
                bools(
                    &mut s,
                    "SELECT n IN(SELECT n FROM r), id FROM t ORDER BY id"
                ),
                vec![Some(true), None, None, None]
            );
            assert_eq!(
                bools(
                    &mut s,
                    "SELECT n NOT IN(SELECT n FROM r), id FROM t ORDER BY id"
                ),
                vec![Some(false), None, None, None]
            );
            assert_eq!(
                ids(
                    &mut s,
                    "SELECT id FROM t WHERE n IN(SELECT n FROM r) ORDER BY id"
                ),
                vec![1]
            );
            assert!(
                ids(
                    &mut s,
                    "SELECT id FROM t WHERE n NOT IN(SELECT n FROM r) ORDER BY id"
                )
                .is_empty()
            );
            s.execute_sql("DELETE FROM r WHERE n IS NULL").unwrap();
            assert_eq!(
                ids(
                    &mut s,
                    "SELECT id FROM t WHERE n NOT IN(SELECT n FROM r) ORDER BY id"
                ),
                vec![2, 4]
            );
            for expression in [
                "NULL",
                "(1.0/1)",
                "1",
                "1.23",
                "'text'",
                "TRUE",
                "DATE '2024-01-01'",
                "X'00ff'",
            ] {
                assert_eq!(
                    bools(&mut s, &format!("SELECT {expression} IN(SELECT NULL)")),
                    vec![None]
                );
                assert_eq!(
                    bools(&mut s, &format!("SELECT {expression} NOT IN(SELECT NULL)")),
                    vec![None]
                );
                assert_eq!(
                    bools(
                        &mut s,
                        &format!("SELECT {expression} IN(SELECT NULL WHERE FALSE)")
                    ),
                    vec![Some(false)]
                );
                assert_eq!(
                    bools(
                        &mut s,
                        &format!("SELECT {expression} NOT IN(SELECT NULL WHERE FALSE)")
                    ),
                    vec![Some(true)]
                );
                assert!(
                    ids(
                        &mut s,
                        &format!("SELECT id FROM t WHERE {expression} IN(SELECT NULL)")
                    )
                    .is_empty()
                );
                assert_eq!(
                    ids(
                        &mut s,
                        &format!(
                            "SELECT id FROM t WHERE {expression} NOT IN(SELECT NULL WHERE FALSE) ORDER BY id"
                        )
                    ),
                    vec![1, 2, 3, 4]
                );
            }
            for sql in [
                "SELECT (n+1) IS NULL FROM (SELECT NULL AS n) AS raw_null",
                "SELECT (n AND TRUE) IS NULL FROM (SELECT NULL AS n) AS raw_null",
                "SELECT (n=1) IS NULL FROM (SELECT NULL AS n) AS raw_null",
                "SELECT 1 IN(SELECT n+1 FROM (SELECT NULL AS n) AS raw_null) IS NULL",
            ] {
                assert_eq!(bools(&mut s, sql), vec![Some(true)]);
            }
            let child_error = s
                .execute_sql("SELECT (n+1) IS NULL FROM (SELECT NULL AS n, CAST(1.0/0 AS INTEGER) AS bad) AS d")
                .err()
                .expect("computed child error must not be pruned");
            assert!(
                child_error.message.contains("division by zero"),
                "{}",
                child_error.message
            );
            assert!(
                bools(
                    &mut s,
                    "SELECT (n+1) IS NULL FROM (SELECT NULL AS n WHERE FALSE) AS d"
                )
                .is_empty()
            );
            for sql in [
                "SELECT NULL",
                "SELECT n FROM (SELECT NULL AS n) AS raw_null",
            ] {
                // Approved untyped NULL output preserves its true Arrow type.
                let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
                    panic!()
                };
                assert_eq!(execution.batch.num_rows(), 1);
                assert_eq!(
                    execution.batch.column(0).data_type(),
                    &arrow::datatypes::DataType::Null
                );
            }
            assert_eq!(bools(&mut s, "SELECT NULL IN(SELECT X'ff')"), vec![None]);
            assert_eq!(
                bools(&mut s, "SELECT X'00ff' IN(SELECT X'00FF')"),
                vec![Some(true)]
            );
        }
    }
}
#[test]
fn numeric_pair_rules_match_ordinary_equality_without_eager_overflow() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for (a, b) in [
                ("1", "1.00"),
                ("1.23", "1"),
                ("-1.20", "-1.200"),
                ("(1.23/1)", "1.23"),
            ] {
                let expected = bools(&mut s, &format!("SELECT {a}={b}"));
                assert_eq!(
                    bools(&mut s, &format!("SELECT {a} IN(SELECT {b})")),
                    expected,
                    "{a} / {b}"
                );
                assert_eq!(
                    bools(&mut s, &format!("SELECT {a} NOT IN(SELECT {b})")),
                    expected.iter().map(|v| v.map(|b| !b)).collect::<Vec<_>>()
                );
            }
            // Same original mixed pairs. Old: the blanket refusal applied even
            // to exactly representable values and signed zero. New (Helen,
            // Oct 15): DOUBLE comparison, independent DuckDB 1.5.6 answers.
            let group = promoted_reference::group("membership_pairs");
            for (a, b) in [
                ("(9007199254740992.0/1)", "9007199254740993"),
                ("9007199254740993", "(9007199254740992.0/1)"),
                ("(0.0/(-1))", "0"),
            ] {
                for operator in ["IN", "NOT IN"] {
                    let sql = format!("SELECT {a} {operator}(SELECT {b})");
                    promoted_reference::check(&mut s, &group, &sql, "membership pair");
                }
            }
            let huge = "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))";
            assert!(s.execute_sql(&format!("SELECT {huge}=0.1")).is_err());
            let error = s
                .execute_sql(&format!("SELECT {huge} IN(SELECT 0.1)"))
                .err()
                .unwrap();
            assert!(error.message.contains("overflow"));
            assert_eq!(
                bools(&mut s, &format!("SELECT {huge} IN(SELECT 0.1 WHERE FALSE)")),
                vec![Some(false)]
            );
            assert_eq!(
                bools(
                    &mut s,
                    &format!("SELECT {huge} IN(SELECT CAST(NULL AS DECIMAL(38,1)))")
                ),
                vec![None]
            );
        }
    }
}
#[test]
fn incompatible_types_bind_fail_on_empty_sets_and_arity_is_semantic() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            s.execute_sql("CREATE TABLE e(n INT)").unwrap();
            for sql in [
                "SELECT 'hello' IN(SELECT n FROM e)",
                "SELECT 'hello' NOT IN(SELECT n FROM e)",
                "SELECT n FROM e WHERE 'hello' IN(SELECT n FROM e)",
                "SELECT X'31' IN(SELECT n FROM e)",
                "SELECT TRUE IN(SELECT n FROM e)",
            ] {
                assert_eq!(
                    s.execute_sql(sql).err().unwrap().kind,
                    "unsupported",
                    "{sql}"
                );
            }
            for sql in [
                "SELECT 1 IN(SELECT 1,2)",
                "SELECT 1 NOT IN(SELECT 1,2)",
                "SELECT n FROM e WHERE n IN(SELECT n,n FROM e)",
                "SELECT n FROM e WHERE n NOT IN(SELECT n,n FROM e)",
            ] {
                let error = s.execute_sql(sql).err().unwrap();
                assert_eq!(error.kind, "semantic", "{sql}: {error}");
                assert!(error.message.contains("exactly one column"));
            }
        }
    }
}

#[test]
fn float_nan_signed_zero_preserve_equality_and_mixed_membership_uses_double() {
    use arrow::{
        array::{ArrayRef, Float64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use std::{
        fs::{self, File},
        sync::Arc,
    };
    let path = std::env::temp_dir().join(format!(
        "assaydb-in-float-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])),
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(-0.0),
            Some(0.0),
            Some(9007199254740992.0),
            Some(f64::INFINITY),
            None,
        ])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path.clone())]);
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 2,
                ..Options::default()
            };
            for (sql, expected) in [
                (
                    "SELECT f IN(SELECT f FROM t WHERE id=0), id FROM t ORDER BY id",
                    vec![
                        Some(true),
                        Some(false),
                        Some(false),
                        Some(false),
                        Some(false),
                        None,
                    ],
                ),
                (
                    "SELECT f IN(SELECT f FROM t WHERE id IN(1,5)), id FROM t ORDER BY id",
                    vec![None, Some(true), Some(true), None, None, None],
                ),
            ] {
                let out = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    out.batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            // Preserve the original mixed-pair SQL/Parquet values. Old: refused
            // before execution. New: DOUBLE membership; DuckDB 1.5.6 answers
            // false for NaN, -0.0, 0.0 and inf, true for 2^53, NULL for NULL.
            let sql = "SELECT f IN(SELECT 9007199254740993), id FROM t ORDER BY id";
            let out = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
            assert_eq!(
                out.batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                    None
                ],
                "{sql}"
            );
            let out = execute(
                &prepare(
                    "SELECT id FROM t WHERE f IN(SELECT f FROM t WHERE id=0)",
                    &tables,
                    &options,
                )
                .unwrap(),
                &options,
            )
            .unwrap();
            assert_eq!(out.batch.num_rows(), 1);
            assert_eq!(
                out.batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
    }
    fs::remove_file(path).unwrap();
}

#[test]
fn planner_selects_typed_membership_and_preserves_hash_mark_cte_reuse() {
    fn operators(plan: &serde_json::Value, name: &str) -> usize {
        match plan {
            serde_json::Value::Object(fields) => {
                usize::from(
                    fields
                        .get("op")
                        .and_then(|op| op.as_object())
                        .is_some_and(|op| op.contains_key(name)),
                ) + fields
                    .values()
                    .map(|value| operators(value, name))
                    .sum::<usize>()
            }
            serde_json::Value::Array(values) => {
                values.iter().map(|value| operators(value, name)).sum()
            }
            _ => 0,
        }
    }
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                ..Options::default()
            };
            for (sql, typed, hash) in [
                ("SELECT 1 IN(SELECT 1)", 0, 1),
                ("SELECT 1 NOT IN(SELECT 1)", 0, 1),
                ("SELECT X'00ff' IN(SELECT X'00ff')", 0, 1),
                ("SELECT 1.23 IN(SELECT 1)", 1, 0),
                ("SELECT 1 IN(SELECT NULL)", 1, 0),
                ("SELECT NULL IN(SELECT 1)", 1, 0),
                ("SELECT (1.0/1) IN(SELECT (1.0/1))", 1, 0),
                ("SELECT (1/1) IN(SELECT (1/1))", 0, 1),
                ("SELECT 1 WHERE 1 IN(SELECT 1.23)", 1, 0),
                ("SELECT 1 WHERE 1 IN(SELECT 1)", 0, usize::from(!optimized)),
            ] {
                let prepared = prepare(sql, &HashMap::new(), &options).unwrap();
                let serialized =
                    serde_json::to_value(prepared.relational.as_ref().unwrap()).unwrap();
                assert_eq!(operators(&serialized, "TypedMark"), typed, "{sql}");
                assert_eq!(operators(&serialized, "Mark"), hash, "{sql}");
            }
            let prepared = prepare(
                "WITH r AS (SELECT 1 AS n) SELECT 1.00 IN(SELECT n FROM r), 1 IN(SELECT n FROM r)",
                &HashMap::new(),
                &options,
            )
            .unwrap();
            let serialized = serde_json::to_value(prepared.relational.as_ref().unwrap()).unwrap();
            assert_eq!(operators(&serialized, "TypedMark"), 1);
            assert_eq!(operators(&serialized, "Mark"), 1);
            let result = execute(&prepared, &options).unwrap();
            for array in result.batch.columns() {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(true)]
                );
            }
            // Planner::query only creates CTE materialization boundaries with
            // relational optimizations enabled; the disabled plan expands both
            // references independently, so no cache access is expected.
            assert_eq!(
                operators(&serialized, "Materialize"),
                2 * usize::from(optimized)
            );
            assert_eq!(result.metrics.cte_materializations, usize::from(optimized));
            assert_eq!(result.metrics.cte_reuses, usize::from(optimized));
        }
    }
}
