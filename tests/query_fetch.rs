//! FETCH ONLY preserves the same completed-query boundary as LIMIT/OFFSET.
use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn rows(session: &mut Session, sql: &str) -> Vec<i64> {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("query required")
    };
    execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .map(Option::unwrap)
        .collect()
}

#[test]
fn fetch_only_forms_and_completed_query_composition() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
                        relational_optimizations: optimized,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                for (sql, wanted) in [
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 FETCH FIRST ROW ONLY",
                        vec![1],
                    ),
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY",
                        vec![2, 3],
                    ),
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 FETCH FIRST 0 ROWS ONLY",
                        vec![],
                    ),
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 FETCH FIRST NULL ROWS ONLY",
                        vec![1, 2, 3],
                    ),
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 FETCH FIRST 2.5 ROWS ONLY",
                        vec![1, 2, 3],
                    ),
                    (
                        "VALUES (3),(1),(2) ORDER BY col0 FETCH FIRST '2' ROWS ONLY",
                        vec![1, 2],
                    ),
                    (
                        "VALUES (1),(2) UNION ALL VALUES (3),(4) ORDER BY 1 DESC FETCH FIRST 2 ROWS ONLY",
                        vec![4, 3],
                    ),
                    (
                        "SELECT DISTINCT col0 FROM (VALUES (2),(1),(1),(3)) t ORDER BY col0 FETCH FIRST 2 ROWS ONLY",
                        vec![1, 2],
                    ),
                    (
                        "WITH x AS (VALUES (3),(1),(2) ORDER BY 1 FETCH FIRST 2 ROWS ONLY) SELECT col0 FROM x ORDER BY col0 DESC",
                        vec![2, 1],
                    ),
                    (
                        "SELECT col0 FROM (VALUES (3),(1),(2) ORDER BY 1 FETCH FIRST ROW ONLY) t",
                        vec![1],
                    ),
                    (
                        "SELECT COUNT(*) FROM (VALUES (1),(2),(3)) t FETCH FIRST ROW ONLY",
                        vec![3],
                    ),
                    ("SELECT 7 FETCH FIRST ROW ONLY", vec![7]),
                ] {
                    assert_eq!(
                        rows(&mut session, sql),
                        wanted,
                        "{sql}/{workers}/{batch_size}/{optimized}"
                    );
                }
            }
        }
    }
}

#[test]
fn expressions_nested_fetch_and_lexical_boundaries() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    for (sql, wanted) in [
        (
            "VALUES (3),(1),(2) ORDER BY 1 FETCH FIRST (1+1) ROWS ONLY",
            vec![1, 2],
        ),
        (
            "VALUES (3),(1),(2) ORDER BY 1 FETCH FIRST (CASE WHEN TRUE THEN 2 ELSE 1 END) ROWS ONLY",
            vec![1, 2],
        ),
        (
            "WITH t AS (VALUES (3),(1),(2) ORDER BY 1 FETCH FIRST (1+1) ROWS ONLY) SELECT col0 FROM t ORDER BY col0 DESC FETCH FIRST (2-1) ROWS ONLY",
            vec![2],
        ),
        (
            "SELECT (SELECT col0 FROM (VALUES (3),(1)) t ORDER BY col0 FETCH FIRST (2-1) ROWS ONLY)",
            vec![1],
        ),
        (
            "SELECT 8 AS \"FETCH\" FETCH FIRST /* quantity */ (1+0) /* end */ ROWS ONLY",
            vec![8],
        ),
        (
            "SELECT LENGTH('FETCH FIRST (1+1) ROWS ONLY') FETCH FIRST (1+0) ROWS ONLY",
            vec![27],
        ),
    ] {
        assert_eq!(rows(&mut session, sql), wanted, "{sql}");
    }
    session.execute_sql("CREATE VIEW fetch_view AS SELECT col0 FROM (VALUES (3),(1),(2)) t ORDER BY col0 FETCH FIRST (1+1) ROWS ONLY").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT col0 FROM fetch_view ORDER BY col0"),
        vec![1, 2]
    );
    for sql in [
        "SELECT 1 FETCH FIRST 1 ROW WITH TIES",
        "SELECT 1 FETCH FIRST 50 PERCENT ROWS ONLY",
        "SELECT 1 LIMIT 2 FETCH FIRST 1 ROW ONLY",
        "SELECT 1 FETCH FIRST 'bad' ROWS ONLY",
        "SELECT 1 FETCH FIRST '__assay_fetch_quantity_0' ROWS ONLY",
        "SELECT 1 FETCH FIRST (-1) ROWS ONLY",
        "SELECT 1 FETCH FIRST (unknown_column) ROWS ONLY",
    ] {
        assert!(session.execute_sql(sql).is_err(), "must reject {sql}");
    }
    // Existing Assay integer division by zero yields NULL. FETCH NULL is
    // unbounded, so this returns the row. DuckDB 2.0 instead evaluates 1/0 as
    // positive infinity and rejects its conversion to a FETCH count. Preserve
    // the public arithmetic contract and keep this semantic parity gap visible.
    assert_eq!(
        rows(&mut session, "SELECT 1 FETCH FIRST (1/0) ROWS ONLY"),
        vec![1]
    );
    // Existing LIMIT syntax/semantics is not broadened by FETCH evaluation.
    assert!(session.execute_sql("SELECT 1 LIMIT (1+1)").is_err());
}
