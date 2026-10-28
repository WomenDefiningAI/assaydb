//! Goldens checked against captured DuckDB 2.0.0 results. These tests still
//! require a candidate execution receipt; oracle answers alone are not a pass.
//! Existing Boolean CAST/CONCAT controls deliberately retain public behavior.
use arrow::array::{Array, StringArray};
use assaydb::{
    execute::{self, Options},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;

fn result(session: &mut Session, sql: &str) -> Vec<Option<String>> {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("query expected");
    };
    let values = execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..values.len())
        .map(|row| values.is_valid(row).then(|| values.value(row).to_owned()))
        .collect()
}
#[test]
fn concat_ws_translate_values_and_composition() {
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
                for (sql, expected) in [
                    (
                        "SELECT concat_ws(', ', 'Banana', 'Apple', 'Melon')",
                        Some("Banana, Apple, Melon"),
                    ),
                    ("SELECT concat_ws('-', 'a', NULL, '', 'b')", Some("a--b")),
                    ("SELECT concat_ws('-', NULL, NULL)", Some("")),
                    ("SELECT concat_ws(NULL, 'a', 'b')", None),
                    ("SELECT concat_ws('', 'é', '🦆')", Some("é🦆")),
                    (
                        "SELECT concat_ws('|', 42, 2.50, TRUE, FALSE, DATE '2024-02-29')",
                        Some("42|2.50|true|false|2024-02-29"),
                    ),
                    ("SELECT translate('12345', '143', 'ax')", Some("a2x5")),
                    ("SELECT translate('ababa', 'aaab', 'XYZQ')", Some("XQXQX")),
                    ("SELECT translate('é🦆é', 'é🦆́', '界!')", Some("界!e")),
                    ("SELECT translate('abc', '', 'xyz')", Some("abc")),
                    ("SELECT translate('abc', 'abc', '')", Some("")),
                    ("SELECT translate('abc','aba','X')", Some("Xc")),
                    ("SELECT translate('🦆🦆','🦆🦆','ab')", Some("aa")),
                    ("SELECT translate('', 'abc', 'xyz')", Some("")),
                    ("SELECT translate(NULL, 'a', 'b')", None),
                    ("SELECT translate('a', NULL, 'b')", None),
                    ("SELECT translate('a', 'a', NULL)", None),
                ] {
                    assert_eq!(
                        result(&mut session, sql),
                        vec![expected.map(str::to_owned)],
                        "{sql}"
                    );
                }
                assert_eq!(
                    result(
                        &mut session,
                        "SELECT concat_ws(':', id, translate(s,'ab','XY')) FROM (VALUES (1,'aba'),(2,NULL),(3,'')) t(id,s) ORDER BY id"
                    ),
                    vec![Some("1:XYX".into()), Some("2".into()), Some("3:".into())]
                );
                // Preserved-baseline controls: VARCHAR already returns true text;
                // CONCAT(Boolean) remains refused (a documented DuckDB difference).
                assert_eq!(
                    result(&mut session, "SELECT CAST(TRUE AS VARCHAR)"),
                    vec![Some("true".into())]
                );
                assert!(session.execute_sql("SELECT CONCAT(TRUE)").is_err());
                for sql in [
                    "SELECT concat_ws()",
                    "SELECT concat_ws('-')",
                    "SELECT concat_ws(42,'a')",
                    "SELECT translate('a','b')",
                    "SELECT translate(123,'1','x')",
                    "SELECT concat_ws(NULL, CAST('bad' AS BIGINT))",
                ] {
                    assert!(session.execute_sql(sql).is_err(), "{sql}");
                }
            }
        }
    }
}
#[test]
fn concat_ws_translate_resource_and_cancellation_controls() {
    for sql in [
        "SELECT concat_ws('-', 'abcdef', 'ghijkl')",
        "SELECT translate('aaaa', 'a', '🦆')",
    ] {
        let options = Options {
            memory_bytes: 16,
            ..Options::default()
        };
        assert!(
            execute::prepare(sql, &HashMap::new(), &options)
                .and_then(|p| execute::execute(&p, &options))
                .is_err()
        );
        let options = Options::default();
        options.cancellation.cancel();
        assert!(
            execute::prepare(sql, &HashMap::new(), &options)
                .and_then(|p| execute::execute(&p, &options))
                .is_err()
        );
    }
}
