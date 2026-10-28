//! NULL-only Float arms must not manufacture mixed numeric provenance.
#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::array::{Array, Float64Array};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::Value;

#[test]
fn null_float_arms_keep_the_pre_promotion_outcomes() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/mixed_float_nulls/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let setup = include_str!("fixtures/mixed_float_nulls/setup.sql");
    assert_eq!(expected["setup_sql"], setup.trim());
    let cases = expected["cases"].as_array().unwrap();
    let sources: Vec<_> = include_str!("fixtures/mixed_float_nulls/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect();
    assert_eq!(cases.len(), 10);
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(case["id"], metadata["id"]);
        assert_eq!(case["sql"], sql.trim());
    }
    let promoted = promoted_reference::group("null_arms");
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
                for sql in setup
                    .split(';')
                    .map(str::trim)
                    .filter(|sql| !sql.is_empty())
                {
                    session.execute_sql(sql).unwrap();
                }
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let context = format!(
                        "{} workers={workers} batch={batch_size} optimized={optimized}",
                        case["id"]
                    );
                    // MySQL's CAST AS SIGNED stays refused exactly as on main: its rounding of
                    // non-integers differs from Assay's truncating INTEGER cast (pending decision).
                    // DuckDB's answer (with SIGNED read as BIGINT) is retained in the fixture.
                    if case["id"] == "slt_good_83_decimal_signed" {
                        let error = session.execute_sql(sql).err().map(|e| e.to_string());
                        assert!(
                            error
                                .as_deref()
                                .is_some_and(|e| e.starts_with("unsupported:")
                                    && e.ends_with("CAST AS SIGNED")),
                            "{context}: {error:?}"
                        );
                        continue;
                    }
                    let Output::Query { execution, .. } = session
                        .execute_sql(sql)
                        .unwrap_or_else(|e| panic!("{context}: {e}\n{sql}"))
                    else {
                        panic!("{context}: expected a query")
                    };
                    let batch = execution.batch;
                    let rows = case["rows"].as_array().unwrap();
                    assert_eq!(batch.num_rows(), rows.len(), "{context}");
                    assert_eq!(batch.num_columns(), 1, "{context}");
                    let array = batch.column(0);
                    for (row, expected) in rows.iter().enumerate() {
                        if expected[0].is_null() {
                            assert!(array.is_null(row), "{context}: row {row}");
                        } else {
                            assert!(!array.is_null(row), "{context}: row {row}");
                            let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
                            assert_eq!(
                                floats.value(row).to_bits(),
                                expected[0].as_f64().unwrap().to_bits(),
                                "{context}: row {row}"
                            );
                        }
                    }
                }
                // Old: a real Float alternative made a computed Int fallback a
                // row-dependent mixed refusal. New (Helen, Oct 15): the Int arm
                // is promoted to DOUBLE; independent DuckDB 1.5.6 answers.
                for expression in [
                    "COALESCE(CASE WHEN flag THEN CAST(n AS REAL) ELSE -21 END, n + 67)",
                    "CASE WHEN flag THEN CAST(n AS REAL) ELSE n END",
                    "CAST(CASE WHEN flag THEN CAST(n AS REAL) ELSE n END AS INTEGER)",
                    "CASE WHEN flag THEN CAST(NULL AS REAL) ELSE CASE WHEN flag THEN CAST(n AS REAL) ELSE n END END",
                ] {
                    let sql =
                        format!("SELECT ({expression}) + CAST(NULL AS INTEGER) FROM null_arms");
                    promoted_reference::check(
                        &mut session,
                        &promoted,
                        &sql,
                        &format!("{workers}/{batch_size}/{optimized}"),
                    );
                }
            }
        }
    }
}
