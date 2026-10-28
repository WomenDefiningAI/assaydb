//! Names/rows below are captured DuckDB 2.0.0 derived-alias controls.
//! Candidate execution is required independently; dotted names remain a gap.
use arrow::util::display::array_value_to_string;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

#[test]
fn derived_aliases_deduplicate_after_partial_positional_names() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for relational_optimizations in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
                        relational_optimizations,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                for (sql, names, expected) in [
                    (
                        "SELECT * FROM (VALUES (1,2)) t(a)",
                        vec!["a", "col1"],
                        vec!["1", "2"],
                    ),
                    (
                        "SELECT * FROM (VALUES (1,2)) t(a,a)",
                        vec!["a", "a_1"],
                        vec!["1", "2"],
                    ),
                    (
                        "SELECT * FROM (VALUES (1,2,3)) t(a,a,a_1)",
                        vec!["a", "a_1", "a_1_1"],
                        vec!["1", "2", "3"],
                    ),
                    (
                        r#"SELECT * FROM (VALUES (1,2)) t("A",a)"#,
                        vec!["A", "a_1"],
                        vec!["1", "2"],
                    ),
                    (
                        "SELECT * FROM (SELECT 1 AS x,2 AS y) t(a)",
                        vec!["a", "y"],
                        vec!["1", "2"],
                    ),
                    (
                        "SELECT * FROM (SELECT 1 AS x,2 AS x) t",
                        vec!["x", "x_1"],
                        vec!["1", "2"],
                    ),
                    (
                        "SELECT t.a_1 FROM (VALUES (1,2)) t(a,a)",
                        vec!["a_1"],
                        vec!["2"],
                    ),
                ] {
                    let Output::Query { execution, .. } = session
                        .execute_sql(sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    else {
                        panic!("query expected")
                    };
                    let batch = execution.batch;
                    assert_eq!(
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>(),
                        names,
                        "{sql}"
                    );
                    assert_eq!(batch.num_rows(), 1, "{sql}");
                    assert_eq!(
                        batch
                            .columns()
                            .iter()
                            .map(|a| array_value_to_string(a.as_ref(), 0).unwrap())
                            .collect::<Vec<_>>(),
                        expected,
                        "{sql}"
                    );
                }
                for sql in [
                    "SELECT * FROM (VALUES (1,2)) t(a,b,c)",
                    "SELECT * FROM (SELECT 1 AS x,2 AS y) t(a,b,c)",
                ] {
                    let error = session
                        .execute_sql(sql)
                        .err()
                        .expect("excess alias list must error");
                    assert!(
                        error
                            .to_string()
                            .contains("derived table alias column count"),
                        "{error}"
                    );
                }
                // Existing wildcard dotted-name refusal remains unchanged. Newly
                // admitted partial lists must not silently strip a dotted remaining name.
                for sql in [
                    r#"SELECT * FROM (VALUES (1,2)) t("a.b",c)"#,
                    r#"SELECT * FROM (VALUES (1,2)) t("a.b")"#,
                ] {
                    let error = session
                        .execute_sql(sql)
                        .err()
                        .expect("dotted name boundary");
                    assert!(error.to_string().contains("unsupported:"), "{error}");
                }
                // Regression controls: these old shapes do not demand dotted columns.
                for sql in [
                    "SELECT COUNT(*) FROM (SELECT 1.5) t",
                    "SELECT COUNT(*) FROM (SELECT 1.5,1.5) t",
                    r#"SELECT COUNT(*) FROM (SELECT 1) t("a.b")"#,
                    r#"SELECT COUNT(*) FROM (SELECT 1,2) t("a.b",c)"#,
                    "SELECT COUNT(*) FROM (SELECT 1.5) t(n)",
                ] {
                    let Output::Query { execution, .. } = session
                        .execute_sql(sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    else {
                        panic!("query expected")
                    };
                    assert_eq!(
                        array_value_to_string(execution.batch.column(0).as_ref(), 0).unwrap(),
                        "1",
                        "{sql}"
                    );
                }
            }
        }
    }
}

#[test]
fn derived_aliases_compose_without_changing_cte_excess_alias_contract() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    // Supplemental composition controls; these exact queries need an oracle
    // receipt as well as candidate execution before being counted as parity.
    for (sql, expected) in [
        ("SELECT t.a_1 FROM (SELECT 1 AS x,2 AS a) t(a)", "2"),
        (
            "SELECT q.a_1 FROM (SELECT * FROM (VALUES(1,2)) t(a,a)) q",
            "2",
        ),
        (
            "WITH t AS (SELECT * FROM (VALUES(1,2)) q(a,a)) SELECT x.a_1 FROM t x JOIN t y ON x.a=y.a",
            "2",
        ),
        ("WITH t(a,b,c) AS (SELECT 1,2) SELECT b FROM t", "2"),
    ] {
        let Output::Query { execution, .. } = session
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
        else {
            panic!("query expected")
        };
        assert_eq!(execution.batch.num_rows(), 1);
        assert_eq!(
            array_value_to_string(execution.batch.column(0).as_ref(), 0).unwrap(),
            expected,
            "{sql}"
        );
    }
}
