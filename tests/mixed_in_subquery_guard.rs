//! Mixed Int/Float IN subqueries: the former explicit refusal is replaced by
//! DOUBLE comparison (Helen, October 15: promote like DuckDB 1.5.6). No SQLite
//! affinity emulation. Old refusal assertions now check DuckDB answers.
#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::array::BooleanArray;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn session(workers: usize, optimized: bool) -> Session {
    Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            relational_optimizations: optimized,
            batch_size: 1,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap()
}
#[test]
fn both_orders_and_contexts_match_duckdb_even_empty_or_all_null_inputs() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            s.execute_sql("CREATE TABLE ints(n INTEGER)").unwrap();
            s.execute_sql("CREATE TABLE reals(x REAL)").unwrap();
            for populated in [false, true] {
                let group = promoted_reference::group(if populated {
                    "in_matrix_populated"
                } else {
                    "in_matrix_empty"
                });
                let context =
                    format!("workers={workers} optimized={optimized} populated={populated}");
                // Old: every form below was refused. New: DuckDB's answer.
                let refused = |s: &mut Session, sql: &str| {
                    promoted_reference::check(s, &group, sql, &context)
                };
                if populated {
                    s.execute_sql("INSERT INTO ints VALUES(1),(9007199254740993),(NULL)")
                        .unwrap();
                    s.execute_sql("INSERT INTO reals VALUES(1),(9007199254740992),(NULL)")
                        .unwrap();
                }
                for (left, from, right, right_from) in
                    [("n", "ints", "x", "reals"), ("x", "reals", "n", "ints")]
                {
                    for operator in ["IN", "NOT IN"] {
                        for filter in [
                            "",
                            " WHERE FALSE",
                            " WHERE NULL IS NULL",
                            " WHERE NULL IS NOT NULL",
                        ] {
                            let membership = format!(
                                "{left} {operator}(SELECT {right} FROM {right_from}{filter})"
                            );
                            refused(&mut s, &format!("SELECT {membership} FROM {from}"));
                            refused(
                                &mut s,
                                &format!("SELECT {left} FROM {from} WHERE {membership}"),
                            );
                            refused(
                                &mut s,
                                &format!("SELECT {membership} FROM {from} WHERE FALSE"),
                            );
                        }
                    }
                }
                for sql in [
                    "SELECT CAST(NULL AS INTEGER) IN(SELECT CAST(NULL AS REAL))",
                    "SELECT CAST(NULL AS REAL) NOT IN(SELECT CAST(NULL AS INTEGER))",
                    "SELECT 1 IN(SELECT x FROM reals WHERE x IS NULL)",
                    "SELECT CAST(1 AS REAL) IN(SELECT n FROM ints WHERE n IS NULL)",
                    "WITH q AS(SELECT x FROM reals) SELECT n IN(SELECT x FROM q) FROM ints",
                    "SELECT n NOT IN(SELECT x FROM (SELECT x FROM reals) q) FROM ints",
                    "SELECT NULLIF(1,2) IN(SELECT CAST(1 AS REAL))",
                    "SELECT COALESCE(1,2) NOT IN(SELECT CAST(1 AS REAL))",
                ] {
                    refused(&mut s, sql);
                }
            }
        }
    }
}
#[test]
fn original_eight_oracle_disagreements_remain_identical_sql_and_membership_matches_duckdb() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mixed-in-original-wrong.json")).unwrap();
    assert_eq!(fixture["record_count"], 8);
    let group = promoted_reference::group("in_original8");
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for (record, reference) in fixture["records"]
                .as_array()
                .unwrap()
                .iter()
                .zip(group["entries"].as_array().unwrap())
            {
                assert_eq!(record["original_outcome"]["status"], "wrong-answer");
                let sql = record["sql"].as_str().unwrap();
                assert_eq!(reference["sql"], sql);
                // Old: refused. New: the IN/NOT IN subquery columns (7, 8)
                // compare in DOUBLE and equal DuckDB 1.5.6. Direct, IN-list and
                // NULLIF columns keep the exact Int/Float relation, as before.
                let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
                    panic!("query")
                };
                let actual = promoted_reference::cells(&execution.batch);
                let expected = &reference["rows"][0];
                for column in [7, 8] {
                    assert_eq!(
                        actual[0][column], expected[column],
                        "{sql}: column {column}"
                    );
                }
            }
        }
    }
}
#[test]
fn normal_binding_and_other_membership_contracts_remain() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for sql in [
                "SELECT 1 IN(SELECT CAST(1 AS REAL),2)",
                "SELECT CAST(1 AS REAL) NOT IN(SELECT 1,2)",
            ] {
                let error = s.execute_sql(sql).err().unwrap();
                assert_eq!(error.kind, "semantic", "{sql}: {error}");
                assert!(
                    error.message.contains("exactly one column"),
                    "{sql}: {error}"
                );
            }
            for sql in [
                "SELECT missing IN(SELECT CAST(1 AS REAL))",
                "SELECT 1 IN(SELECT CAST(missing AS REAL))",
            ] {
                let error = s.execute_sql(sql).err().unwrap();
                assert!(
                    error.message.contains("unknown or ambiguous column"),
                    "{sql}: {error}"
                );
            }
            let sql = "SELECT 1=CAST(1 AS REAL),1 IN(CAST(1 AS REAL)),1 IN(SELECT 1),CAST(1 AS REAL) IN(SELECT CAST(1 AS REAL)),1 IN(SELECT 1.0),CAST(1 AS REAL) IN(SELECT 1.0),NULL IN(SELECT CAST(1 AS REAL) WHERE FALSE)";
            let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
                panic!("query")
            };
            for (i, expected) in [true, true, true, true, true, true, false]
                .into_iter()
                .enumerate()
            {
                let actual = execution
                    .batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                assert_eq!(actual, vec![Some(expected)], "column{i}");
            }
        }
    }
}
