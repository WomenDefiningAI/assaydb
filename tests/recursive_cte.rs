//! Recursive CTE execution, normalization and resource regression tests.
use arrow::record_batch::RecordBatch;
use assaydb::{
    execute::{self, Options},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;
fn rows(batch: &RecordBatch) -> Vec<Vec<String>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(row) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(a.as_ref(), row).unwrap()
                    }
                })
                .collect()
        })
        .collect()
}
#[test]
fn recursive_relations_preserve_iteration_scopes_and_union_semantics() {
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+0.6 FROM x WHERE n<3) SELECT n FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT CAST(n+1 AS VARCHAR) FROM x WHERE n<3) SELECT n FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT n FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS NOT MATERIALIZED (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT n FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n,phase) AS (SELECT 1,0 UNION ALL SELECT CASE WHEN phase=0 THEN CAST(NULL AS BIGINT) ELSE 2 END,phase+1 FROM x WHERE phase=0 OR (n IS NULL AND phase=1)) SELECT n,phase FROM x ORDER BY phase",
            &[&["1", "0"], &["NULL", "1"], &["2", "2"]],
        ),
        (
            r#"WITH RECURSIVE "x"(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM "x" WHERE n<3) SELECT * FROM "x" ORDER BY n"#,
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM (WITH early AS (SELECT * FROM x), x AS (SELECT 8 AS n) SELECT * FROM early) t WHERE n<3) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"], &["4"]],
        ),
        (
            "WITH RECURSIVE x(n) AS ((VALUES (1),(1)) UNION SELECT n+1 FROM x WHERE n<3) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS ((VALUES (1),(1)) UNION ALL SELECT n+1 FROM x WHERE n<2) SELECT * FROM x ORDER BY n",
            &[&["1"], &["1"], &["2"], &["2"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 WHERE false UNION ALL SELECT n+1 FROM x WHERE n<4) SELECT * FROM x",
            &[],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT CAST(NULL AS BIGINT) UNION SELECT n FROM x) SELECT * FROM x",
            &[&["NULL"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (VALUES (7)) SELECT * FROM x",
            &[&["7"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT a.n+b.n FROM x a,x b WHERE a.n+b.n<=4) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["4"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT a.n,b.n FROM x a JOIN x b ON a.n=b.n ORDER BY a.n",
            &[&["1", "1"], &["2", "2"], &["3", "3"]],
        ),
        (
            "WITH RECURSIVE edges(a,b) AS (VALUES (1,2),(1,3),(2,4),(3,4),(4,1)), reach(n) AS (SELECT 1 UNION SELECT e.b FROM reach r JOIN edges e ON r.n=e.a) SELECT * FROM reach ORDER BY n",
            &[&["1"], &["2"], &["3"], &["4"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3), y AS (WITH x AS (SELECT 8 AS n) SELECT * FROM x) SELECT * FROM y",
            &[&["8"]],
        ),
    ];
    for optimize in [false, true] {
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        relational_optimizations: optimize,
                        batch_size,
                        workers,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                for (sql, expected) in cases {
                    let output = session
                        .execute_sql(sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"));
                    let Output::Query { execution, .. } = output else {
                        panic!("not query")
                    };
                    let expected: Vec<Vec<String>> = expected
                        .iter()
                        .map(|r| r.iter().map(|s| s.to_string()).collect())
                        .collect();
                    assert_eq!(rows(&execution.batch), expected, "{sql}");
                }
            }
        }
    }
}
#[test]
fn recursive_pre_cancel_and_shape_errors_do_not_publish_partial_results() {
    let sql =
        "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<10) SELECT * FROM x";
    let options = Options::default();
    options.cancellation.cancel();
    assert!(
        execute::prepare(sql, &HashMap::new(), &options)
            .and_then(|p| execute::execute(&p, &options))
            .is_err()
    );
    let options = Options::default();
    let sql =
        "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1,2 FROM x WHERE n<3) SELECT * FROM x";
    assert!(execute::prepare(sql, &HashMap::new(), &options).is_err());
}

#[test]
fn wide_values_frontier_normalizes_computed_integer_arrays() {
    // VALUES produces Int64 while the recursive arithmetic uses internal
    // Decimal128. Every iteration must convert with live funded buffers.
    let anchor = (0..2048)
        .map(|n| format!("({n},0)"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "WITH RECURSIVE x(n,k) AS (VALUES {anchor} UNION ALL SELECT n+1,k+1 FROM x WHERE k<2) SELECT COUNT(*),SUM(n),MAX(k) FROM x"
    );
    for optimized in [false, true] {
        let options = Options {
            relational_optimizations: optimized,
            batch_size: 7,
            ..Options::default()
        };
        let plan = execute::prepare(&sql, &HashMap::new(), &options).unwrap();
        let result = execute::execute(&plan, &options).unwrap();
        let sum = (3 * (2047 * 2048 / 2) + 3 * 2048).to_string();
        assert_eq!(
            rows(&result.batch),
            vec![vec!["6144".to_owned(), sum, "2".to_owned()]]
        );
    }
}
