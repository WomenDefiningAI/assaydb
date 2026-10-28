//! Integer table-function controls checked against saved DuckDB 2.0 probes.
//! Full-domain inclusive cardinality remains an explicitly recorded divergence.
use arrow::util::display::array_value_to_string;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
fn query(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!()
    };
    let b = &execution.batch;
    (0..b.num_rows())
        .map(|r| {
            b.columns()
                .iter()
                .map(|a| array_value_to_string(a.as_ref(), r).unwrap())
                .collect()
        })
        .collect()
}
fn expected(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}
#[test]
fn integer_series_relational_composition_and_bounded_limit() {
    for workers in [1, 4] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                for spill in [false, true] {
                    let mut s = Session::new(
                        &std::env::temp_dir(),
                        Options {
                            workers,
                            batch_size,
                            relational_optimizations: optimized,
                            spill: spill.then(|| SpillOptions {
                                directory: std::env::temp_dir(),
                                sort_run_bytes: 128,
                                ..SpillOptions::default()
                            }),
                            ..Options::default()
                        },
                        Limits::default(),
                    )
                    .unwrap();
                    assert_eq!(
                        query(&mut s, "SELECT * FROM range('3.5')"),
                        expected(&[&["0"], &["1"], &["2"], &["3"]])
                    );
                    for sql in [
                        "SELECT * FROM range(CAST('bad' AS BIGINT)) LIMIT 0",
                        "SELECT * FROM range('bad') LIMIT 0",
                        "SELECT * FROM range(TRUE) LIMIT 0",
                        "SELECT * FROM range(CAST('3' AS VARCHAR))",
                        "SELECT * FROM range('1'||'0')",
                    ] {
                        assert!(s.execute_sql(sql).is_err(), "{sql}");
                    }
                    for sql in [
                        "SELECT * FROM (SELECT * FROM range(0,5,0) LIMIT 10) q LIMIT 0",
                        "SELECT * FROM (SELECT * FROM range(0,5,0) LIMIT 0) q LIMIT 10",
                        "SELECT * FROM range(0,5,0) WHERE range>1 LIMIT 0",
                        "SELECT * FROM range(0,5,0) ORDER BY range LIMIT 0",
                        "SELECT CAST(CASE WHEN range=0 THEN 'bad' ELSE '1' END AS BIGINT) FROM range(10) LIMIT 0",
                        "WITH r AS (SELECT * FROM range(0,5,0)) SELECT * FROM r LIMIT 0",
                        "WITH r AS MATERIALIZED (SELECT * FROM range(0,5,0)) SELECT * FROM r LIMIT 0",
                    ] {
                        assert!(query(&mut s, sql).is_empty(), "{sql}");
                    }
                    for (sql, expected_names) in [
                        ("SELECT * FROM range(3) g", vec!["g"]),
                        (
                            "SELECT * FROM range(3) WITH ORDINALITY g",
                            vec!["g", "ordinality"],
                        ),
                        ("SELECT g.range FROM range(3) g", vec!["range"]),
                        (
                            "SELECT * FROM (SELECT g.range FROM range(3) g) q",
                            vec!["range"],
                        ),
                    ] {
                        let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
                            panic!()
                        };
                        let names = execution
                            .batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.name().clone())
                            .collect::<Vec<_>>();
                        assert_eq!(names, expected_names, "{sql}");
                        assert_eq!(execution.batch.num_rows(), 3, "{sql}");
                    }
                    assert_eq!(
                        query(&mut s, "SELECT * FROM range('3')"),
                        expected(&[&["0"], &["1"], &["2"]])
                    );
                    for sql in [
                        "SELECT * FROM range(3::HUGEINT)",
                        "SELECT * FROM range(3::UBIGINT)",
                        "SELECT * FROM range(3.0::DOUBLE)",
                        "SELECT * FROM range(3.0::DECIMAL(2,1))",
                        "SELECT * FROM range(TRUE)",
                        "SELECT * FROM range()",
                        "SELECT * FROM range(0,3,1,1)",
                        "SELECT * FROM range(stop := 3)",
                    ] {
                        assert!(s.execute_sql(sql).is_err(), "{sql}");
                    }
                    for sql in [
                        "SELECT * FROM range(NULL::BIGINT,5,0)",
                        "SELECT * FROM generate_series(NULL::BIGINT,5,0)",
                        "SELECT * FROM range(0,NULL::BIGINT,0)",
                        "SELECT * FROM range(0,5,NULL::BIGINT)",
                        "SELECT * FROM range(2,8,-1)",
                        "SELECT * FROM generate_series(8,2,1)",
                    ] {
                        assert!(query(&mut s, sql).is_empty(), "{sql}");
                    }
                    assert_eq!(
                        query(&mut s, "SELECT * FROM range(2,8,3)"),
                        expected(&[&["2"], &["5"]])
                    );
                    assert_eq!(
                        query(&mut s, "SELECT * FROM generate_series(2,8,3)"),
                        expected(&[&["2"], &["5"], &["8"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT v FROM range(9223372036854775807) AS r(v) LIMIT 2 OFFSET 3"
                        ),
                        expected(&[&["3"], &["4"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 1000000000000 OFFSET 20) q LIMIT 1 OFFSET 2"
                        ),
                        expected(&[&["22"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "WITH r AS (SELECT * FROM range(9223372036854775807)) SELECT * FROM r LIMIT 1"
                        ),
                        expected(&[&["0"]])
                    );
                    assert!(query(&mut s,"SELECT * FROM range(9223372036854775807) LIMIT 0 OFFSET 9223372036854775807").is_empty());
                    assert!(query(&mut s,"SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 2 OFFSET 9223372036854775807) q LIMIT 0").is_empty());
                    assert!(query(&mut s,"SELECT * FROM (SELECT * FROM range(9223372036854775807) LIMIT 0 OFFSET 9223372036854775807) q LIMIT 2").is_empty());
                    assert!(
                        query(
                            &mut s,
                            "SELECT * FROM range(0,5,0) LIMIT 0 OFFSET 9223372036854775807"
                        )
                        .is_empty()
                    );
                    assert!(s.execute_sql("SELECT v FROM (SELECT CAST(CASE WHEN range=9 THEN 'bad' ELSE '1' END AS BIGINT) AS v FROM range(10) LIMIT 10) q LIMIT 1").is_err());
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT * FROM range(10) WITH ORDINALITY LIMIT 2 OFFSET 3"
                        ),
                        expected(&[&["3", "4"], &["4", "5"]])
                    );
                    assert_eq!(
                        query(&mut s, "SELECT COUNT(*) FROM range(4097)"),
                        expected(&[&["4097"]])
                    );
                    assert_eq!(
                        query(&mut s, "SELECT * FROM range(4100) LIMIT 2 OFFSET 4097"),
                        expected(&[&["4097"], &["4098"]])
                    );
                    assert_eq!(
                        query(&mut s, "SELECT 1 FROM range(9223372036854775807) LIMIT 1"),
                        expected(&[&["1"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT * FROM generate_series(8,2,-3) WITH ORDINALITY AS g(v,n)"
                        ),
                        expected(&[&["8", "1"], &["5", "2"], &["2", "3"]])
                    );
                    assert_eq!(
                        query(&mut s, "SELECT SUM(range),COUNT(*) FROM range(10)"),
                        expected(&[&["45", "10"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT range FROM range(5) WHERE range>1 ORDER BY range DESC LIMIT 2"
                        ),
                        expected(&[&["4"], &["3"]])
                    );
                    assert_eq!(
                        query(
                            &mut s,
                            "SELECT a.range,b.range FROM range(2) a JOIN range(3) b ON a.range=b.range ORDER BY a.range"
                        ),
                        expected(&[&["0", "0"], &["1", "1"]])
                    );
                    assert!(query(&mut s, "SELECT * FROM range(NULL::BIGINT)").is_empty());
                    assert!(s.execute_sql("SELECT * FROM range(0,5,0)").is_err());
                    assert!(query(&mut s, "SELECT * FROM range(0,5,0) LIMIT 0").is_empty());
                    // The cap cannot cross fallible computed projection or hide its tail.
                    assert!(s.execute_sql("SELECT CAST(CASE WHEN range=9 THEN 'bad' ELSE '1' END AS BIGINT) FROM range(10) LIMIT 1").is_err());
                }
            }
        }
    }
}
