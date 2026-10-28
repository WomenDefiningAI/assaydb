//! Accumulated recursive relation regression coverage.
use arrow::record_batch::RecordBatch;
use assaydb::{
    execute::{self, Options},
    spill::SpillOptions,
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
fn recurring_prefixes_duplicates_nulls_and_multiple_readers() {
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3 AND (WITH RECURSIVE x(m) AS (SELECT MAX(n) FROM recurring.x UNION ALL SELECT m+10 FROM x WHERE FALSE) SELECT MAX(m) FROM x)<3) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3 AND (WITH x AS (SELECT 99 AS n) SELECT MAX(n) FROM recurring.x)<3) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3 AND (WITH RECURSIVE x(m) AS (SELECT 10 UNION SELECT m+1 FROM recurring.x WHERE m<11) SELECT MAX(m) FROM x)=11) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM (WITH recurring AS (SELECT 99) SELECT * FROM recurring.x) z WHERE n<3) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "WITH RECURSIVE x(n,c) AS (VALUES(1,0) UNION ALL SELECT n+1,(SELECT COUNT(*) FROM recurring.x) FROM x WHERE n<4) SELECT * FROM x ORDER BY n",
            &[&["1", "0"], &["2", "1"], &["3", "2"], &["4", "3"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT n+1 FROM recurring.x WHERE n<4) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"], &["4"]],
        ),
        (
            "WITH RECURSIVE x(n,k) AS (VALUES(1,0),(1,0) UNION ALL SELECT n,k+1 FROM x WHERE k<1 AND (SELECT COUNT(*) FROM recurring.x)=2) SELECT n,k FROM x ORDER BY k,n",
            &[&["1", "0"], &["1", "0"], &["1", "1"], &["1", "1"]],
        ),
        (
            "WITH RECURSIVE x(n,k) AS (SELECT CAST(NULL AS INTEGER),0 UNION ALL SELECT n,k+1 FROM x WHERE k<2 AND EXISTS(SELECT 1 FROM recurring.x r WHERE r.n IS NULL)) SELECT n,k FROM x ORDER BY k",
            &[&["NULL", "0"], &["NULL", "1"], &["NULL", "2"]],
        ),
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION SELECT a.n+b.n FROM recurring.x a,recurring.x b WHERE a.n+b.n<=4) SELECT * FROM x ORDER BY n",
            &[&["1"], &["2"], &["3"], &["4"]],
        ),
        (
            r#"WITH RECURSIVE "x"(n) AS (SELECT 1 UNION SELECT n+1 FROM "recurring"."x" WHERE n<3) SELECT * FROM "x" ORDER BY n"#,
            &[&["1"], &["2"], &["3"]],
        ),
    ];
    for spill in [false, true] {
        for optimize in [false, true] {
            for batch_size in [1, 7] {
                for workers in [1, 2] {
                    let options = Options {
                        relational_optimizations: optimize,
                        batch_size,
                        workers,
                        spill: spill.then(SpillOptions::default),
                        ..Options::default()
                    };
                    for (sql, expected) in cases {
                        let plan = execute::prepare(sql, &HashMap::new(), &options)
                            .unwrap_or_else(|e| panic!("{sql}: {e}"));
                        let result = execute::execute(&plan, &options)
                            .unwrap_or_else(|e| panic!("{sql}: {e}"));
                        let expected: Vec<Vec<String>> = expected
                            .iter()
                            .map(|r| r.iter().map(|s| s.to_string()).collect())
                            .collect();
                        assert_eq!(rows(&result.batch), expected, "{sql}");
                    }
                }
            }
        }
    }
}
#[test]
fn accumulated_names_are_available_only_inside_recursive_scope() {
    for sql in [
        "WITH x AS (SELECT 1 AS n) SELECT * FROM recurring.x",
        "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT * FROM recurring.x",
    ] {
        assert!(execute::prepare(sql, &HashMap::new(), &Options::default()).is_err());
    }
}
