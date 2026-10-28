//! Exact binary64 -> DECIMAL and bare CAST defaults, independent of execution shape.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::{Expr, Input, Kind, Value as ExprValue},
    memory::Budget,
    session::{Limits, Output, Session},
    types::{Type, decimal_text},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/decimal_float_cast/expected.json")).unwrap()
}

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected query: {sql}")
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
                    if array.is_null(row) || array.data_type() == &DataType::Null {
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
                        DataType::Decimal128(_, scale) => json!(decimal_text(
                            array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale,
                        )),
                        other => panic!("unexpected fixture output: {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn pinned_sql_corpus_across_workers_batches_and_optimizers() {
    let reference = fixture();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 62);
    let sources = include_str!("fixtures/decimal_float_cast/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "fixture metadata: {key}");
        }
        assert_eq!(case["sql"], sql.trim());
    }
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations: optimized,
                    ..Options::default()
                };
                let mut session =
                    Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    // TEXT columns, refused on the base, now convert complete
                    // input and are compared with DuckDB's rows below.
                    let text_column = case["id"] == "text_column_stays_refused";
                    if let Some(error) = case["assay_error"].as_str().filter(|_| !text_column) {
                        let actual = session.execute_sql(sql).err().expect("expected error");
                        assert_eq!(actual.message, error, "{sql}");
                        continue;
                    }
                    let batch = query(&mut session, sql);
                    // Arrow uses precision 38 internally; validate SQL scale and
                    // numeric kind, without treating storage width as precision.
                    let types = case["duckdb_types"].as_array().unwrap();
                    assert_eq!(batch.num_columns(), types.len(), "{sql}");
                    for (field, expected) in batch.schema().fields().iter().zip(types) {
                        let expected = expected.as_str().unwrap();
                        if expected.starts_with("DECIMAL(") {
                            let scale = expected.split_once(',').unwrap().1.trim_end_matches(')');
                            assert_eq!(
                                field.data_type(),
                                &DataType::Decimal128(38, scale.parse().unwrap()),
                                "{sql}"
                            );
                        } else {
                            assert!(
                                matches!(expected, "INTEGER" | "BIGINT" | "HUGEINT"),
                                "{expected}"
                            );
                            assert_eq!(field.data_type(), &DataType::Int64, "{sql}");
                        }
                    }
                    let mut actual = rows(&batch);
                    let mut expected: Vec<Vec<Value>> = serde_json::from_value(if text_column {
                        case["duckdb_rows"].clone()
                    } else {
                        case["rows"].clone()
                    })
                    .unwrap();
                    if !case["ordered"].as_bool().unwrap() {
                        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
                    }
                    assert_eq!(
                        actual, expected,
                        "{}: workers={workers} batch={batch_size} optimized={optimized}: {sql}",
                        case["id"]
                    );
                }
            }
        }
    }
}

#[test]
fn exact_fraction_bit_boundaries_and_failed_conversion_release_memory() {
    let reference = fixture();
    let cases = reference["boundaries"].as_array().unwrap();
    assert!(cases.len() > 2000);
    let budget = Budget::new(1 << 20);
    for case in cases {
        let bits = u64::from_str_radix(case["bits"].as_str().unwrap(), 16).unwrap();
        let scale = case["scale"].as_i64().unwrap() as i8;
        let precision = case["precision"].as_u64().unwrap() as u8;
        let input = Input {
            columns: vec![Some(
                ExprValue::adopt(
                    Arc::new(Float64Array::from(vec![
                        None,
                        Some(f64::from_bits(bits)),
                        None,
                    ])),
                    &budget,
                )
                .unwrap(),
            )],
            len: 3,
        };
        let expr = Expr {
            kind: Kind::CastDecimal(
                Box::new(Expr {
                    kind: Kind::Column(0),
                    ty: Type::Float,
                }),
                precision,
                scale,
            ),
            ty: Type::Decimal(scale),
        };
        let before = budget.used();
        let result = expr.eval(&input, &budget);
        if let Some(error) = case["error"].as_str() {
            assert_eq!(result.err().unwrap(), error, "{case}");
        } else {
            let result = result.unwrap_or_else(|error| panic!("{case}: {error}"));
            let values = result
                .array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let coefficient = case["coefficient"]
                .as_str()
                .unwrap()
                .parse::<i128>()
                .unwrap();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                vec![None, Some(coefficient), None],
                "{case}"
            );
            assert!(budget.used() > before);
        }
        assert_eq!(
            budget.used(),
            before,
            "conversion must release reservations: {case}"
        );
        drop(input);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn bare_ddl_defaults_and_float_result_types_are_unchanged() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    session
        .execute_sql("CREATE TABLE defaults (d DECIMAL, n NUMERIC, c DEC)")
        .unwrap();
    session
        .execute_sql("INSERT INTO defaults VALUES (1000000000000000, 2, 3)")
        .unwrap();
    let batch = query(&mut session, "SELECT d, n, c FROM defaults");
    for field in batch.schema().fields() {
        assert_eq!(field.data_type(), &DataType::Decimal128(38, 0));
    }
    assert_eq!(
        rows(&batch),
        vec![vec![json!("1000000000000000"), json!("2"), json!("3")]]
    );
    for sql in [
        "SELECT CAST(d AS DECIMAL) FROM defaults",
        "SELECT d::NUMERIC FROM defaults",
        "SELECT d::DEC FROM defaults",
    ] {
        assert_eq!(
            session.execute_sql(sql).err().unwrap().message,
            "decimal cast precision overflow"
        );
    }
    let batch = query(&mut session, "SELECT AVG(n), AVG(c) FROM defaults");
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Float64)
    );
    let batch = query(&mut session, "SELECT n / c FROM defaults");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    let error = session
        .execute_sql("SELECT CAST(n / 0 AS DECIMAL) FROM defaults")
        .err()
        .unwrap();
    assert!(error.message.contains("division by zero"), "{error}");
}

struct FloatFile(PathBuf);
impl Drop for FloatFile {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

#[test]
fn scanned_float_columns_nonfinite_errors_and_masks() {
    let file = FloatFile(std::env::temp_dir().join(format!(
        "assay-decimal-float-{}.parquet",
        std::process::id()
    )));
    let values = vec![
        None,
        Some(2.675),
        Some(-2.675),
        Some(1.125),
        Some(-1.125),
        Some(-0.0),
        Some(f64::from_bits(1)),
        Some(2.675),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::MAX),
        Some(-f64::MAX),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..values.len() as i64)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_size(3)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create_new(&file.0).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("floats".into(), file.0.clone())]);
    for optimized in [false, true] {
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations: optimized,
                    ..Options::default()
                };
                let run = |sql: &str| {
                    prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options))
                };
                for target in ["DECIMAL(10,2)", "NUMERIC(10,2)", "DEC(10,2)"] {
                    let sql = format!(
                        "SELECT id, CAST(f AS {target}) AS v FROM floats WHERE id < 8 ORDER BY id"
                    );
                    let result = run(&sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
                    assert_eq!(
                        rows(&result.batch),
                        vec![
                            vec![json!(0), Value::Null],
                            vec![json!(1), json!("2.67")],
                            vec![json!(2), json!("-2.67")],
                            vec![json!(3), json!("1.13")],
                            vec![json!(4), json!("-1.13")],
                            vec![json!(5), json!("0.00")],
                            vec![json!(6), json!("0.00")],
                            vec![json!(7), json!("2.67")],
                        ],
                        "workers={workers} batch={batch_size} optimized={optimized}: {sql}"
                    );
                }
                for id in 8..13 {
                    let sql = format!("SELECT CAST(f AS DECIMAL) AS v FROM floats WHERE id = {id}");
                    let error = run(&sql).err().expect("expected nonfinite/overflow error");
                    assert_eq!(
                        error,
                        if id < 11 {
                            "decimal cast nonfinite source"
                        } else {
                            "decimal cast precision overflow"
                        },
                        "{sql}"
                    );
                }
                let sql = "SELECT CASE WHEN id < 8 THEN CAST(f AS DECIMAL) ELSE CAST(0 AS DECIMAL) END AS v FROM floats WHERE id >= 8";
                assert_eq!(
                    rows(&run(sql).unwrap().batch),
                    vec![vec![json!("0.000")]; 5]
                );
            }
        }
    }
}
