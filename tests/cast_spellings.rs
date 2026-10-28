//! Analyst CAST aliases, exact rounding, formatting and conservative refusals.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    expr::{Expr, Input, Kind, Value},
    memory::Budget,
    session::{Limits, Output, Session},
    types::Type,
};
use serde_json::{Value as Json, json};
use std::{fs, sync::Arc};

fn session(workers: usize, batch_size: usize, optimized: bool) -> Session {
    Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap()
}

fn query(session: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match session.execute_sql(sql).map_err(|e| {
        if e.message.starts_with("unsupported:") {
            e.message
        } else {
            e.to_string()
        }
    })? {
        Output::Query { execution, .. } => Ok(execution.batch.clone()),
        _ => Err("expected a query".into()),
    }
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Json>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Json::Null;
                    }
                    match array.data_type() {
                        DataType::Float64 => json!({"f64": format!(
                            "{:016x}",
                            array.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_bits()
                        )}),
                        DataType::Utf8 => json!(
                            array.as_any().downcast_ref::<StringArray>().unwrap().value(row)
                        ),
                        DataType::Int64 => json!(
                            array.as_any().downcast_ref::<Int64Array>().unwrap().value(row)
                        ),
                        DataType::Decimal128(_, scale) => json!({"decimal": assaydb::types::decimal_text(
                            array.as_any().downcast_ref::<Decimal128Array>().unwrap().value(row),
                            *scale
                        )}),
                        other => panic!("unexpected result type: {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

fn reference() -> Json {
    serde_json::from_str(include_str!(
        "fixtures/cast_spellings/expected-strings-distinct-float.json"
    ))
    .unwrap()
}

fn assert_reference_success(session: &mut Session, sql: &str, reference: &Json) {
    let case = reference["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["sql"].as_str().unwrap().trim_end_matches(';') == sql)
        .unwrap_or_else(|| panic!("missing independent reference: {sql}"));
    assert!(case.get("assay_error").is_none(), "{sql}");
    assert_eq!(
        json!(rows(&query(session, sql).unwrap())),
        case["rows"],
        "{sql}"
    );
}

fn assert_reference_success_unordered(session: &mut Session, sql: &str, reference: &Json) {
    let case = reference["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["sql"].as_str().unwrap().trim_end_matches(';') == sql)
        .unwrap_or_else(|| panic!("missing independent reference: {sql}"));
    assert!(case.get("assay_error").is_none(), "{sql}");
    let mut actual = rows(&query(session, sql).unwrap());
    let mut wanted: Vec<Vec<Json>> = serde_json::from_value(case["rows"].clone()).unwrap();
    actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
    wanted.sort_by_key(|row| serde_json::to_string(row).unwrap());
    assert_eq!(actual, wanted, "{sql}");
}

#[test]
fn numeric_aliases_round_once_and_preserve_nulls_and_float_bits() {
    let reference = reference();
    for optimized in [false, true] {
        let mut session = session(1, 7, optimized);
        for target in ["DOUBLE", "DOUBLE PRECISION", "FLOAT8"] {
            for source in [
                "0",
                "9007199254740993",
                "9007199254740995",
                "-9007199254740993",
                "9223372036854775807",
                "-9223372036854775808",
                "0.1",
                "-0.1",
                "12345678901234567890.123456789012345678",
                "0.000000000000000001",
                "1.000000000000000111",
                "1.000000000000000112",
            ] {
                let expected = source.parse::<f64>().unwrap().to_bits();
                for expr in [
                    format!("CAST({source} AS {target})"),
                    format!("({source})::{target}"),
                ] {
                    let sql = format!("SELECT {expr}");
                    let batch = query(&mut session, &sql).unwrap();
                    assert_eq!(
                        rows(&batch),
                        vec![vec![json!({"f64": format!("{expected:016x}")})]],
                        "{sql}"
                    );
                    assert_eq!(batch.column(0).data_type(), &DataType::Float64);
                }
            }
            for source in [
                "NULL",
                "CAST(NULL AS INTEGER)",
                "CAST(NULL AS DECIMAL(5,2))",
                "CAST(NULL AS REAL)",
            ] {
                let batch =
                    query(&mut session, &format!("SELECT CAST({source} AS {target})")).unwrap();
                assert_eq!(batch.column(0).data_type(), &DataType::Float64);
                assert!(batch.column(0).is_null(0));
            }
            let batch = query(
                &mut session,
                &format!("SELECT CAST(CAST('-0.0' AS REAL) AS {target})"),
            )
            .unwrap();
            assert_eq!(rows(&batch), vec![vec![json!({"f64":"8000000000000000"})]]);
            for source in [
                "'1.25'",
                "'1.25tail'",
                "''",
                "TRUE",
                "X'31'",
                "DATE '2024-01-01'",
                "CAST(NULL AS TEXT)",
            ] {
                let sql = format!("SELECT CAST({source} AS {target})");
                match source {
                    // TEXT to DOUBLE was refused on the base. Complete numeric
                    // text now converts as in DuckDB 1.5.6 (1.25 and NULL);
                    // incomplete text is the data error both references raise.
                    "'1.25'" => assert_eq!(
                        rows(&query(&mut session, &sql).unwrap()),
                        vec![vec![json!({"f64":"3ff4000000000000"})]]
                    ),
                    "CAST(NULL AS TEXT)" => {
                        let batch = query(&mut session, &sql).unwrap();
                        assert_eq!(batch.column(0).data_type(), &DataType::Float64);
                        assert!(batch.column(0).is_null(0));
                    }
                    "'1.25tail'" => assert_eq!(
                        query(&mut session, &sql).unwrap_err(),
                        r#"engine: invalid input syntax for type double precision: "1.25tail""#
                    ),
                    "''" => assert_eq!(
                        query(&mut session, &sql).unwrap_err(),
                        r#"engine: invalid input syntax for type double precision: """#
                    ),
                    _ => {
                        let error = query(&mut session, &sql).unwrap_err();
                        assert!(error.starts_with("unsupported:"), "{error}");
                    }
                }
            }
        }
        // REAL retains its public numeric-prefix conversion; the checked
        // DOUBLE spellings above still reject incomplete numeric text.
        assert_eq!(
            rows(&query(&mut session, "SELECT CAST('1.25tail' AS REAL)").unwrap()),
            vec![vec![json!({"f64":"3ff4000000000000"})]]
        );
        for sql in [
            "SELECT CAST(1 AS FLOAT)",
            "SELECT (9007199254740995)::FLOAT",
            "SELECT CAST(NULL AS FLOAT)",
            "SELECT CAST(1 AS FLOAT4)",
            "SELECT CAST(1 AS FLOAT(24))",
        ] {
            assert!(
                query(&mut session, sql)
                    .unwrap_err()
                    .starts_with("unsupported:"),
                "{sql}"
            );
        }
        for sql in [
            "SELECT CAST(1 AS DECIMAL)",
            "SELECT CAST(1 AS NUMERIC)",
            "SELECT CAST(CAST(1 AS DOUBLE) AS DECIMAL(5,2))",
        ] {
            assert_reference_success(&mut session, sql, &reference);
        }
    }
}

#[test]
fn double_aggregate_inputs_use_float_semantics_including_distinct() {
    let reference = reference();
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let mut session = session(workers, batch_size, optimized);
                session
                    .execute_sql("CREATE TABLE discounts (k INTEGER, l_discount DECIMAL(4,2))")
                    .unwrap();
                session
                    .execute_sql(
                        "INSERT INTO discounts VALUES (1,0.05),(1,0.05),(2,-0.02),(2,NULL)",
                    )
                    .unwrap();
                for target in ["DOUBLE", "DOUBLE PRECISION", "FLOAT8"] {
                    for source in [
                        format!("CAST(l_discount AS {target})"),
                        format!("((l_discount::{target}))"),
                    ] {
                        for function in ["AVG", "SUM"] {
                            for sql in [
                                format!(
                                    "SELECT CAST({function}({source}) AS DECIMAL(10,4)) FROM discounts"
                                ),
                                format!(
                                    "SELECT k,{function}(DISTINCT {source}) FROM discounts GROUP BY k"
                                ),
                                format!("SELECT {function}({source}) FROM discounts WHERE k < 0"),
                                format!(
                                    "SELECT {function}({source}) FROM discounts WHERE l_discount IS NULL"
                                ),
                            ] {
                                // DISTINCT now follows the approved Float DISTINCT
                                // rule; its grouped rows are unordered.
                                assert_reference_success_unordered(&mut session, &sql, &reference);
                            }
                        }
                    }
                }
                assert_reference_success(
                    &mut session,
                    "SELECT CAST(AVG(l_discount) AS DECIMAL(10,4)) FROM discounts",
                    &reference,
                );
            }
        }
    }
}

#[test]
fn text_aliases_preserve_scale_dates_and_legacy_sources() {
    for optimized in [false, true] {
        let mut session = session(1, 7, optimized);
        for target in [
            "TEXT",
            "VARCHAR",
            "STRING",
            "CHARACTER VARYING",
            "CHAR VARYING",
        ] {
            for (source, expected) in [
                ("1.50", "1.50"),
                ("-1.50", "-1.50"),
                ("0.00", "0.00"),
                ("-CAST(0 AS DECIMAL(3,2))", "0.00"),
                ("-0.05", "-0.05"),
                ("CAST(12 AS DECIMAL(4,0))", "12"),
                ("0.000000000000000001", "0.000000000000000001"),
                ("-9223372036854775808", "-9223372036854775808"),
                ("DATE '0001-01-01'", "0001-01-01"),
                ("DATE '1969-12-31'", "1969-12-31"),
                ("DATE '2024-02-29'", "2024-02-29"),
                ("DATE '9999-12-31'", "9999-12-31"),
                ("''", ""),
                ("' é😀 '", " é😀 "),
                ("X'6100c3a9'", "a\0é"),
                // Owner decision, October 15, 2026: DuckDB 1.5.6 text.
                ("TRUE", "true"),
                ("FALSE", "false"),
            ] {
                for expr in [
                    format!("CAST({source} AS {target})"),
                    format!("({source})::{target}"),
                ] {
                    let sql = format!("SELECT {expr}");
                    assert_eq!(
                        rows(&query(&mut session, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"))),
                        vec![vec![json!(expected)]],
                        "{sql}"
                    );
                }
            }
            for source in ["NULL", "CAST(NULL AS DECIMAL(4,2))", "CAST(NULL AS DATE)"] {
                let batch =
                    query(&mut session, &format!("SELECT CAST({source} AS {target})")).unwrap();
                assert_eq!(batch.column(0).data_type(), &DataType::Utf8);
                assert!(batch.column(0).is_null(0));
            }
            // REAL (binary64 in Assay) to text was refused until Oct 16; it now
            // matches the DuckDB receipt's `float_text_refusal` answer, 1.25.
            assert_eq!(
                rows(
                    &query(
                        &mut session,
                        &format!("SELECT CAST(CAST(1.25 AS REAL) AS {target})")
                    )
                    .unwrap()
                ),
                vec![vec![json!("1.25")]]
            );
            // -0.00 is a negative-zero REAL literal: still refused.
            for source in ["-0.00", "X'ff'", "DATE '0000-01-01'"] {
                assert!(
                    query(&mut session, &format!("SELECT CAST({source} AS {target})"))
                        .unwrap_err()
                        .starts_with("unsupported:")
                );
            }
        }
    }
}

#[test]
fn bounded_varchar_checks_characters_on_reached_rows_at_every_batch_setting() {
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let mut session = session(workers, batch_size, optimized);
                session
                    .execute_sql("CREATE TABLE cc (id INTEGER, d DECIMAL(6,2), dt DATE, t TEXT)")
                    .unwrap();
                session.execute_sql("INSERT INTO cc VALUES (1,1.50,DATE '2024-02-29','a'), (2,-1.50,DATE '1969-12-31','é😀'), (3,0.00,DATE '1970-01-01',''), (4,NULL,NULL,NULL), (5,1.50,DATE '2024-02-29','a')").unwrap();
                let batch = query(&mut session, "SELECT CAST(d AS VARCHAR(5)), CAST(dt AS VARCHAR(10)), CAST(t AS VARCHAR(2)), id FROM cc ORDER BY id").unwrap();
                assert_eq!(
                    rows(&batch),
                    vec![
                        vec![json!("1.50"), json!("2024-02-29"), json!("a"), json!(1)],
                        vec![json!("-1.50"), json!("1969-12-31"), json!("é😀"), json!(2)],
                        vec![json!("0.00"), json!("1970-01-01"), json!(""), json!(3)],
                        vec![Json::Null, Json::Null, Json::Null, json!(4)],
                        vec![json!("1.50"), json!("2024-02-29"), json!("a"), json!(5)],
                    ]
                );
                assert_eq!(
                    rows(
                        &query(&mut session, "SELECT CAST(SUM(d) AS VARCHAR(4)) FROM cc").unwrap()
                    ),
                    vec![vec![json!("1.50")]]
                );
                assert_eq!(
                    rows(
                        &query(
                            &mut session,
                            "SELECT CAST(d AS DOUBLE), id FROM cc ORDER BY id"
                        )
                        .unwrap()
                    ),
                    vec![
                        vec![json!({"f64": "3ff8000000000000"}), json!(1)],
                        vec![json!({"f64": "bff8000000000000"}), json!(2)],
                        vec![json!({"f64": "0000000000000000"}), json!(3)],
                        vec![Json::Null, json!(4)],
                        vec![json!({"f64": "3ff8000000000000"}), json!(5)],
                    ]
                );
                assert_eq!(
                    rows(
                        &query(
                            &mut session,
                            "SELECT CAST(t AS VARCHAR(1)) FROM cc WHERE id=1"
                        )
                        .unwrap()
                    ),
                    vec![vec![json!("a")]]
                );
                assert_eq!(
                    query(
                        &mut session,
                        "SELECT CAST(d AS DOUBLE), CAST(t AS VARCHAR(1)) FROM cc WHERE id<0"
                    )
                    .unwrap()
                    .num_rows(),
                    0
                );
                for target in ["VARCHAR", "CHARACTER VARYING", "CHAR VARYING"] {
                    for (source, limit) in [
                        ("'é😀'", 2),
                        ("'é'", 2),
                        ("''", 1),
                        ("NULL", 1),
                        ("-1.50", 5),
                    ] {
                        query(
                            &mut session,
                            &format!("SELECT CAST({source} AS {target}({limit}))"),
                        )
                        .unwrap();
                    }
                    for source in ["'é😀'", "'a '", "12", "1.50", "DATE '2024-01-01'"] {
                        let error = query(
                            &mut session,
                            &format!("SELECT CAST({source} AS {target}(1))"),
                        )
                        .unwrap_err();
                        assert!(error.starts_with("unsupported: VARCHAR(1)"), "{error}");
                    }
                }
                let error = query(
                    &mut session,
                    "SELECT CAST(t AS VARCHAR(1)), id FROM cc ORDER BY id",
                )
                .unwrap_err();
                assert!(error.starts_with("unsupported: VARCHAR(1)"), "{error}");
                for sql in [
                    "SELECT CASE WHEN TRUE THEN 'ok' ELSE CAST('long' AS VARCHAR(1)) END",
                    "SELECT COALESCE('ok', CAST('long' AS VARCHAR(1)))",
                ] {
                    assert_eq!(
                        rows(&query(&mut session, sql).unwrap()),
                        vec![vec![json!("ok")]]
                    );
                }
                for target in [
                    "VARCHAR(0)",
                    "VARCHAR(2 OCTETS)",
                    "VARCHAR(MAX)",
                    "STRING(2)",
                ] {
                    assert!(
                        query(&mut session, &format!("SELECT CAST('a' AS {target})"))
                            .unwrap_err()
                            .starts_with("unsupported:")
                    );
                }
            }
        }
    }
}

#[test]
fn text_cast_reservations_release_on_length_and_date_errors() {
    let budget = Budget::new(1 << 20);
    for (array, ty, limit, error) in [
        (
            Arc::new(
                Decimal128Array::from(vec![Some(-150), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ) as ArrayRef,
            Type::Decimal(2),
            4,
            "unsupported: VARCHAR(4)",
        ),
        (
            Arc::new(Date32Array::from(vec![Some(i32::MAX), None])) as ArrayRef,
            Type::Date,
            10,
            "unsupported: DATE to TEXT",
        ),
        (
            Arc::new(StringArray::from(vec![Some("é😀"), None])) as ArrayRef,
            Type::Text,
            1,
            "unsupported: VARCHAR(1)",
        ),
    ] {
        let input = Input {
            columns: vec![Some(Value::adopt(array, &budget).unwrap())],
            len: 2,
        };
        let cast = Expr {
            kind: Kind::Cast(Box::new(Expr {
                kind: Kind::Column(0),
                ty,
            })),
            ty: Type::Text,
        };
        let expr = Expr {
            kind: Kind::CastVarchar(Box::new(cast), limit),
            ty: Type::Text,
        };
        let before = budget.used();
        assert!(expr.eval(&input, &budget).err().unwrap().starts_with(error));
        assert_eq!(budget.used(), before);
        drop(input);
        assert_eq!(budget.used(), 0);
    }
    let input = Input {
        columns: vec![Some(
            Value::adopt(
                Arc::new(
                    Decimal128Array::from(vec![Some(-150), None])
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ),
                &budget,
            )
            .unwrap(),
        )],
        len: 2,
    };
    let expr = Expr {
        kind: Kind::CastVarchar(
            Box::new(Expr {
                kind: Kind::Cast(Box::new(Expr {
                    kind: Kind::Column(0),
                    ty: Type::Decimal(2),
                })),
                ty: Type::Text,
            }),
            5,
        ),
        ty: Type::Text,
    };
    let before = budget.used();
    let blocker = budget.reserve(budget.limit() - before).unwrap();
    assert!(
        expr.eval(&input, &budget)
            .err()
            .unwrap()
            .starts_with("resource-limit:")
    );
    drop(blocker);
    assert_eq!(budget.used(), before);
    let output = expr.eval(&input, &budget).unwrap();
    drop(input);
    assert_eq!(
        output
            .array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "-1.50"
    );
    assert!(output.array.is_null(1));
    assert!(budget.used() > 0);
    drop(output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn every_independent_reference_case_is_compared_exactly() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/cast_spellings/expected-strings-distinct-float.json"
    );
    let expected: Json = serde_json::from_str(&fs::read_to_string(path).expect(
        "generate expected.json using tools/testing/cast_spellings_reference.py and DuckDB 1.5.6",
    ))
    .unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    let sources: Vec<_> = include_str!("fixtures/cast_spellings/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect();
    assert_eq!(cases.len(), 137);
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Json = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value);
        }
        assert_eq!(case["sql"], sql.trim());
    }
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let mut session = session(workers, batch_size, optimized);
                for sql in expected["setup"].as_array().unwrap() {
                    session.execute_sql(sql.as_str().unwrap()).unwrap();
                }
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let result = query(&mut session, sql);
                    // TEXT to DOUBLE, refused on the base, now parses complete
                    // input (DuckDB's rows above) and raises the data error both
                    // references raise for a text prefix (owner decision,
                    // October 16, 2026).
                    if case["id"] == "invalid_text_double_refusal" {
                        assert_eq!(case["reference_error"], true);
                        assert_eq!(
                            result.unwrap_err(),
                            r#"engine: invalid input syntax for type double precision: "1.25tail""#
                        );
                        continue;
                    }
                    if let Some(prefix) = case["assay_error"]
                        .as_str()
                        .filter(|_| case["id"] != "text_double_refusal")
                    {
                        assert!(case["difference"].is_string());
                        let error = result.unwrap_err();
                        assert!(error.starts_with(prefix), "{sql}: {error}");
                        continue;
                    }
                    let batch = result.unwrap_or_else(|e| panic!("{sql}: {e}"));
                    let types: Vec<_> = batch
                        .columns()
                        .iter()
                        .map(|a| match a.data_type() {
                            DataType::Float64 => "DOUBLE".to_string(),
                            DataType::Utf8 => "VARCHAR".to_string(),
                            DataType::Int64 => "BIGINT".to_string(),
                            DataType::Decimal128(_, scale) => format!("DECIMAL(scale={scale})"),
                            ty => panic!("unexpected fixture type: {ty}"),
                        })
                        .collect();
                    let reference_types: Vec<_> = case["types"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| match t.as_str().unwrap() {
                            "INTEGER" => "BIGINT".to_string(), // Assay's integral result boundary is always BIGINT.
                            t if t.starts_with("DECIMAL(") => format!(
                                "DECIMAL(scale={})",
                                t.split_once(',').unwrap().1.trim_end_matches(')')
                            ),
                            t => t.to_string(),
                        })
                        .collect();
                    assert_eq!(types, reference_types, "{sql}");
                    let mut actual = rows(&batch);
                    // bool_text_legacy's 1/0 override is superseded by the
                    // October 15 Boolean text decision: DuckDB's rows apply.
                    let wanted = case
                        .get("assay_rows")
                        .filter(|_| case["id"] != "bool_text_legacy")
                        .unwrap_or(&case["rows"]);
                    let mut wanted: Vec<Vec<Json>> =
                        serde_json::from_value(wanted.clone()).unwrap();
                    if !case["ordered"].as_bool().unwrap() {
                        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                        wanted.sort_by_key(|row| serde_json::to_string(row).unwrap());
                    }
                    assert_eq!(
                        actual, wanted,
                        "{} workers={workers} batch={batch_size} optimized={optimized}: {sql}",
                        case["id"]
                    );
                }
            }
        }
    }
}
