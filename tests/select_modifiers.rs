//! Independent SELECT ordinal/order/pagination goldens and error preservation.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
use serde_json::{Value, json};

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected rows")
    };
    execution.batch.clone()
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) {
                        return Value::Null;
                    }
                    match array.data_type() {
                        DataType::Int64 => json!(
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Decimal128(_, scale) => json!(assaydb::types::decimal_text(
                            array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        DataType::Utf8 => json!(
                            array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Date32 => json!(
                            arrow::util::display::array_value_to_string(array.as_ref(), row)
                                .unwrap()
                        ),
                        other => panic!("unexpected fixture type {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn independent_duckdb_cases_across_workers_and_batches() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/select_modifiers/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    let sources = include_str!("fixtures/select_modifiers/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 97);
    assert_eq!(sources.len(), cases.len());
    for (case, source) in cases.iter().zip(sources) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata["id"], case["id"]);
        assert_eq!(metadata["ordered"], case["ordered"]);
        assert_eq!(metadata["assay_error"], case["assay_error"]);
        assert_eq!(metadata["assay_columns"], case["assay_columns"]);
        assert_eq!(case["sql"], sql.trim());
    }
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
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let context = format!("{}/{workers}/{batch_size}/{optimized}", case["id"]);
                    if let Some(error) = case["assay_error"].as_str() {
                        assert_eq!(
                            session.execute_sql(sql).err().expect(&context).message,
                            error,
                            "{context}"
                        );
                        continue;
                    }
                    let batch = query(&mut session, sql);
                    let names = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect::<Vec<_>>();
                    // Unaliased qualified columns historically retain their SQL
                    // spelling in Assay. Pin that display-only difference for
                    // the additive cases; values/types/order stay reference-exact.
                    let columns = case.get("assay_columns").unwrap_or(&case["columns"]);
                    assert_eq!(json!(names), *columns, "{context}: output schema");
                    let types = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| match f.data_type() {
                            DataType::Int64 => "integer",
                            DataType::Decimal128(_, _) => "decimal",
                            DataType::Utf8 => "text",
                            DataType::Date32 => "date",
                            other => panic!("unexpected {other}"),
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(json!(types), case["types"], "{context}: types");
                    let mut actual = rows(&batch)
                        .into_iter()
                        .map(|r| json!(r))
                        .collect::<Vec<_>>();
                    let mut wanted = case["rows"].as_array().unwrap().clone();
                    if !case["ordered"].as_bool().unwrap() {
                        actual.sort_by_key(Value::to_string);
                        wanted.sort_by_key(Value::to_string);
                    }
                    assert_eq!(actual, wanted, "{context}");
                }
            }
        }
    }
}

#[test]
fn pagination_preserves_plan_order_and_late_errors_in_memory_and_spill() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
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
                session
                    .execute_sql("CREATE TABLE t(id INT, k INT, x TEXT)")
                    .unwrap();
                session.execute_sql("INSERT INTO t VALUES(3,1,'Bee'),(1,1,'ant'),(4,NULL,NULL),(2,1,'ant'),(5,2,'CAT'),(6,2,'cat'),(7,2,'DOG'),(8,2,'dog'),(9,2,'elk')").unwrap();
                let full = query(&mut session, "SELECT id FROM t");
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT id FROM t ORDER BY k NULLS LAST"
                    )),
                    [3, 1, 2, 5, 6, 7, 8, 9, 4].map(|id| vec![json!(id)])
                );
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT id FROM t ORDER BY LOWER(x) NULLS FIRST LIMIT 4 OFFSET 1"
                    )),
                    [1, 2, 3, 5].map(|id| vec![json!(id)])
                );
                for (limit, offset) in [
                    (0, 0),
                    (3, 0),
                    (3, 1),
                    (3, 7),
                    (3, 9),
                    (3, 99),
                    (i64::MAX as usize, 1),
                    (1, i64::MAX as usize),
                ] {
                    let actual = query(
                        &mut session,
                        &format!("SELECT id FROM t LIMIT {limit} OFFSET {offset}"),
                    );
                    let start = offset.min(full.num_rows());
                    assert_eq!(
                        actual,
                        full.slice(start, limit.min(full.num_rows() - start))
                    );
                }
                for sql in [
                    "SELECT id FROM t ORDER BY LOWER(x) NULLS FIRST, id",
                    "SELECT id FROM t ORDER BY k NULLS LAST, id",
                    "SELECT k, COUNT(*) AS n FROM t GROUP BY 1 ORDER BY COUNT(*) DESC, k NULLS LAST",
                ] {
                    let full = query(&mut session, sql);
                    let actual = query(&mut session, &format!("{sql} LIMIT 2 OFFSET 1"));
                    assert_eq!(actual, full.slice(1, 2));
                }
                // Errors on skipped rows and LIMIT 0 remain observable. Hidden
                // keys keep wide integer intermediates private, but still check
                // precision-38 arithmetic before any final pagination.
                let overflow = "99999999999999999999999999999999999999+id";
                for sql in [
                    format!("SELECT {overflow} AS n FROM t LIMIT 0 OFFSET 99"),
                    format!("SELECT id FROM t ORDER BY {overflow} LIMIT 0"),
                    format!("SELECT id FROM t ORDER BY {overflow} LIMIT 2 OFFSET 99"),
                ] {
                    let error = session
                        .execute_sql(&sql)
                        .err()
                        .expect("error must not be hidden");
                    assert!(error.message.contains("overflow"), "{sql}: {error}");
                }
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT id FROM t ORDER BY 9223372036854775807+id DESC LIMIT 2 OFFSET 1"
                    )),
                    vec![vec![json!(8)], vec![json!(7)]]
                );
            }
        }
    }
}

#[test]
fn unsupported_modifiers_keep_specific_refusal_prefixes() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    for sql in [
        "SELECT 1 LIMIT 1, 2",
        "SELECT 1 LIMIT -1",
        "SELECT 1 OFFSET -1",
        "SELECT 1 LIMIT 1.5",
        "SELECT 1 OFFSET 1+1",
        "SELECT 1 GROUP BY 'text'",
        "SELECT 1 GROUP BY NULL",
        "SELECT 1 ORDER BY -1.5",
        "SELECT LOWER(123)",
        "SELECT UPPER(TRUE)",
    ] {
        let error = session.execute_sql(sql).err().expect(sql);
        assert!(error.message.starts_with("unsupported:"), "{sql}: {error}");
    }
    // Newly supported syntax gets positive result assertions; every other
    // refusal above keeps its established classification and prefix.
    for sql in [
        "SELECT 1 FETCH FIRST 1 ROW ONLY",
        "SELECT DISTINCT ON (1) 1 ORDER BY 2+2",
    ] {
        assert_eq!(
            rows(&query(&mut session, sql)),
            vec![vec![json!(1)]],
            "{sql}"
        );
    }
    // Compound ORDER BY/LIMIT were refused before Oct 16; DuckDB 1.5.6 answers
    // [[1], [2]] and one row (tests/fixtures/joins_sets covers the forms).
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 DESC"
        )),
        vec![vec![json!(2)], vec![json!(1)]]
    );
    assert_eq!(
        query(&mut session, "SELECT 1 UNION ALL SELECT 2 LIMIT 1").num_rows(),
        1
    );
    for sql in ["SELECT 1 ORDER BY -1", "SELECT 1 ORDER BY (-2)"] {
        assert_eq!(
            session.execute_sql(sql).err().unwrap().message,
            "ORDER BY ordinal out of range"
        );
    }
    for sql in [
        "SELECT SUM(1) OVER () GROUP BY 1",
        "SELECT LAG(1) OVER () GROUP BY 1",
        "SELECT ROW_NUMBER() OVER (ORDER BY 1 ROWS UNBOUNDED PRECEDING) GROUP BY 1",
    ] {
        assert_eq!(
            session.execute_sql(sql).err().unwrap().message,
            "GROUP BY ordinal refers to a window expression"
        );
    }
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT LOWER('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), UPPER('abcdefghijklmnopqrstuvwxyz'), LOWER(NULL), UPPER('')"
        )),
        vec![vec![
            json!("abcdefghijklmnopqrstuvwxyz"),
            json!("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            Value::Null,
            json!("")
        ]]
    );
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT CASE WHEN 1=1 THEN LOWER('ABC') ELSE LOWER('É') END"
        )),
        vec![vec![json!("abc")]]
    );
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT id FROM (SELECT 1 AS id, 'ABC' AS x UNION ALL SELECT 2, 'É') q WHERE id=1 ORDER BY LOWER(x)"
        )),
        vec![vec![json!(1)]]
    );
    // Formerly `unsupported: non-ASCII LOWER/UPPER requires a locale policy`;
    // the Oct 16 decision follows DuckDB 1.5.6's mapping (independent answers
    // from tests/fixtures/string_functions/expected.json).
    for (sql, answer) in [("SELECT LOWER('É')", "é"), ("SELECT UPPER('ß')", "ẞ")] {
        assert_eq!(rows(&query(&mut session, sql)), vec![vec![json!(answer)]]);
    }
}
