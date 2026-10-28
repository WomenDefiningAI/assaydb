//! UNION BY NAME controls checked against DuckDB 2.0.0; unordered sets are
//! explicitly sorted in SQL. Public numeric conversion semantics are retained.
use arrow::array::Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

#[test]
fn union_name_alignment_and_set_semantics() {
    let cases = [
        (
            "SELECT 1 AS a,2 AS b UNION ALL BY NAME SELECT 3 AS b,4 AS a",
            vec!["a", "b"],
            vec![vec![Some("1"), Some("2")], vec![Some("4"), Some("3")]],
        ),
        (
            "SELECT 1 AS a UNION BY NAME SELECT 2 AS b ORDER BY a NULLS LAST",
            vec!["a", "b"],
            vec![vec![Some("1"), None], vec![None, Some("2")]],
        ),
        (
            "SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS A",
            vec!["a"],
            vec![vec![Some("1")], vec![Some("2")]],
        ),
        (
            "SELECT 1 AS a UNION DISTINCT BY NAME SELECT 1 AS a",
            vec!["a"],
            vec![vec![Some("1")]],
        ),
        (
            "SELECT NULL AS a UNION BY NAME SELECT 1 AS b ORDER BY b NULLS FIRST",
            vec!["a", "b"],
            vec![vec![None, None], vec![None, Some("1")]],
        ),
        (
            "SELECT 'left' AS a UNION ALL BY NAME SELECT 'right' AS b",
            vec!["a", "b"],
            vec![vec![Some("left"), None], vec![None, Some("right")]],
        ),
        (
            "(SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS b) UNION ALL BY NAME SELECT 3 AS c ORDER BY a NULLS LAST,b NULLS LAST LIMIT 2",
            vec!["a", "b", "c"],
            vec![vec![Some("1"), None, None], vec![None, Some("2"), None]],
        ),
        (
            "SELECT 9007199254740993 AS a UNION ALL BY NAME SELECT 7 AS b",
            vec!["a", "b"],
            vec![vec![Some("9007199254740993"), None], vec![None, Some("7")]],
        ),
        (
            "SELECT 1 AS a WHERE FALSE UNION ALL BY NAME SELECT 2 AS b",
            vec!["a", "b"],
            vec![vec![None, Some("2")]],
        ),
    ];
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
                for (sql, names, expected) in &cases {
                    let Output::Query { execution, .. } = session
                        .execute_sql(sql)
                        .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    else {
                        panic!("expected query")
                    };
                    let b = &execution.batch;
                    assert_eq!(
                        b.schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>(),
                        *names,
                        "{sql}"
                    );
                    let actual = (0..b.num_rows())
                        .map(|r| {
                            b.columns()
                                .iter()
                                .map(|a| {
                                    if a.is_null(r)
                                        || a.data_type() == &arrow::datatypes::DataType::Null
                                    {
                                        None
                                    } else {
                                        Some(
                                            arrow::util::display::array_value_to_string(
                                                a.as_ref(),
                                                r,
                                            )
                                            .unwrap(),
                                        )
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    let expected = expected
                        .iter()
                        .map(|r| r.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "{sql}; workers={workers}, batch={batch_size}, optimized={optimized}"
                    );
                }
                for sql in [
                    "SELECT 1 AS a,2 AS a UNION BY NAME SELECT 3 AS a",
                    "SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a,3 AS A",
                ] {
                    assert!(
                        session
                            .execute_sql(sql)
                            .err()
                            .unwrap()
                            .message
                            .contains("duplicate column name"),
                        "{sql}"
                    );
                }
            }
        }
    }
}
