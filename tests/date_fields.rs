//! DATE calendar fields and strict literal comparisons. No runtime oracle.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const FIELDS: [&str; 9] = [
    "YEAR", "QUARTER", "MONTH", "DAY", "DOW", "ISODOW", "DOY", "WEEK", "ISOYEAR",
];
const SETUP: &str = include_str!("fixtures/date_fields/setup.sql");
const CASES: &str = include_str!("fixtures/date_fields/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-date-fields-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn session(&self, workers: usize, batch_size: usize, optimized: bool) -> Session {
        let mut session = Session::new(
            &self.0,
            Options {
                workers,
                batch_size,
                relational_optimizations: optimized,
                memory_bytes: 64 << 20,
                ..Options::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap();
        for statement in SETUP.split(';').filter(|s| !s.trim().is_empty()) {
            session.execute_sql(statement).unwrap();
        }
        session
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected query: {sql}");
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
                        DataType::Int64 => {
                            json!(
                                array
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(row)
                            )
                        }
                        DataType::Boolean => json!(
                            array
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Utf8 => json!(
                            array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Decimal128(_, scale) => {
                            let value = array
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row);
                            let scale = u32::try_from(*scale).unwrap();
                            if scale == 0 {
                                return json!(value.to_string());
                            }
                            let factor = 10_u128.pow(scale);
                            let magnitude = value.unsigned_abs();
                            let sign = if value < 0 { "-" } else { "" };
                            json!(format!(
                                "{sign}{}.{:0width$}",
                                magnitude / factor,
                                magnitude % factor,
                                width = scale as usize
                            ))
                        }
                        other => panic!("unexpected DATE fixture output {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn handwritten_leap_days_and_iso_year_edges() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, true);
    // These expectations are handwritten, not derived from the implementation
    // or a second engine run. Sunday is 0/7; ISO week 1 can start in December.
    for (date, expected) in [
        ("1900-03-01", [1900, 1, 3, 1, 4, 4, 60, 9, 1900]),
        ("2000-02-29", [2000, 1, 2, 29, 2, 2, 60, 9, 2000]),
        ("2014-12-28", [2014, 4, 12, 28, 0, 7, 362, 52, 2014]),
        ("2014-12-29", [2014, 4, 12, 29, 1, 1, 363, 1, 2015]),
        ("2014-12-30", [2014, 4, 12, 30, 2, 2, 364, 1, 2015]),
        ("2014-12-31", [2014, 4, 12, 31, 3, 3, 365, 1, 2015]),
        ("2015-01-01", [2015, 1, 1, 1, 4, 4, 1, 1, 2015]),
        ("2015-01-02", [2015, 1, 1, 2, 5, 5, 2, 1, 2015]),
        ("2015-01-03", [2015, 1, 1, 3, 6, 6, 3, 1, 2015]),
        ("2016-01-01", [2016, 1, 1, 1, 5, 5, 1, 53, 2015]),
        ("2016-01-03", [2016, 1, 1, 3, 0, 7, 3, 53, 2015]),
        ("2016-01-04", [2016, 1, 1, 4, 1, 1, 4, 1, 2016]),
        ("2024-02-29", [2024, 1, 2, 29, 4, 4, 60, 9, 2024]),
        ("2024-04-01", [2024, 2, 4, 1, 1, 1, 92, 14, 2024]),
    ] {
        for function_form in [false, true] {
            let expressions = FIELDS
                .iter()
                .map(|field| {
                    if function_form {
                        format!("date_part('{field}',DATE '{date}')")
                    } else {
                        format!("EXTRACT({field} FROM DATE '{date}')")
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            let batch = query(&mut session, &format!("SELECT {expressions}"));
            assert_eq!(
                rows(&batch),
                vec![expected.map(Value::from).to_vec()],
                "{date}"
            );
            for field in batch.schema().fields() {
                assert_eq!(field.data_type(), &DataType::Int64);
            }
        }
    }
    for field in FIELDS {
        let sql =
            format!("SELECT EXTRACT({field} FROM NULL),date_part('{field}',CAST(NULL AS DATE))");
        let batch = query(&mut session, &sql);
        assert_eq!(rows(&batch), vec![vec![Value::Null; 2]]);
        for column in batch.columns() {
            assert_eq!(column.data_type(), &DataType::Int64);
        }
    }
}

#[test]
fn invalid_literals_fail_during_prepare_even_without_rows() {
    // prepare alone proves these errors are not deferred until evaluation.
    for invalid in [
        "2024-02-30",
        "2023-02-29",
        "1900-02-29",
        "2024-13-01",
        "2024-00-01",
        "2024-01-00",
        "2024-1-5",
        "2024-01-5",
        "24-01-01",
        "2024-01-01 ",
        " 2024-01-01",
        "2024/01/01",
        "0000-01-01",
        "abc",
        "",
        "２０２４-01-01",
    ] {
        let date = "CAST(NULL AS DATE)";
        let mut expressions = vec![
            format!("{date} BETWEEN '{invalid}' AND '2024-12-31'"),
            format!("'{invalid}' BETWEEN {date} AND '2024-12-31'"),
            format!("{date} IN ('2024-01-01','{invalid}')"),
            format!("'{invalid}' NOT IN ('2024-01-01',{date})"),
        ];
        for op in ["=", "<>", "<", "<=", ">", ">="] {
            expressions.push(format!("{date} {op} '{invalid}'"));
            expressions.push(format!("'{invalid}' {op} {date}"));
        }
        for expression in expressions {
            let sql = format!("SELECT {expression} WHERE FALSE");
            let error = prepare(&sql, &HashMap::new(), &Options::default())
                .err()
                .unwrap_or_else(|| panic!("bind unexpectedly succeeded: {sql}"));
            assert!(error.starts_with("invalid DATE literal"), "{sql}: {error}");
        }
    }
}

#[test]
fn date_literal_comparisons_and_preserved_casts_have_handwritten_results() {
    let sql = "SELECT DATE '2024-01-01' = '2024-01-01', '2023-12-31' < DATE '2024-01-01', DATE '2024-02-29' BETWEEN '2024-01-01' AND '2024-12-31', '2024-02-29' BETWEEN DATE '2024-01-01' AND '2024-12-31', '2024-02-29' IN ('2024-02-29',DATE '2000-01-01'), DATE '2024-02-29' NOT IN ('2024-01-01',NULL), CAST(NULL AS DATE) = '2024-01-01', CAST('2024-01-01' AS DATE) = DATE '2024-01-01', '2024-01-01' = (DATE '2024-01-01'), 'abc' < 'xyz'";
    let options = Options::default();
    let result = execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options).unwrap();
    assert_eq!(
        rows(&result.batch),
        vec![vec![
            json!(true),
            json!(true),
            json!(true),
            json!(true),
            json!(true),
            Value::Null,
            Value::Null,
            json!(true),
            json!(true),
            json!(true),
        ]]
    );
}

#[test]
fn unsupported_forms_remain_explicit() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, true);
    for sql in [
        "SELECT d=t FROM dates",
        "SELECT t<d FROM empty_dates",
        "SELECT d BETWEEN t AND '2025-01-01' FROM dates",
        "SELECT d IN ('2024-02-29',t) FROM dates",
        "SELECT t IN ('2024-02-29',d) FROM dates",
        "SELECT DATE '2024-01-01'=CAST('2024-01-01' AS TEXT)",
        "SELECT DATE '2024-01-01'=COALESCE('2024-01-01','2025-01-01')",
        "SELECT DATE '2024-01-01'=1",
        "SELECT DATE '2024-01-01'=1.25",
        "SELECT EXTRACT(MONTH FROM 1)",
        "SELECT EXTRACT(DAY FROM 1.25)",
        "SELECT EXTRACT(YEAR FROM '2024-01-01')",
        "SELECT EXTRACT(EPOCH FROM d) FROM dates",
        "SELECT EXTRACT(HOUR FROM d) FROM empty_dates",
        "SELECT EXTRACT(MINUTE FROM d) FROM dates",
        "SELECT EXTRACT(SECOND FROM d) FROM dates",
        "SELECT date_part('epoch',d) FROM dates",
        "SELECT date_part('hour',d) FROM dates",
        "SELECT date_part(t,d) FROM dates",
        "SELECT date_part('month',1)",
        "SELECT date_part('month')",
        "SELECT date_part('month',d,1) FROM dates",
        "SELECT year(d) FROM dates",
        "SELECT month(d) FROM dates",
        "SELECT day(d) FROM dates",
        "SELECT EXTRACT(ISOYEAR FROM DATE '0000-01-01')",
    ] {
        let error = session.execute_sql(sql).err().expect(sql);
        assert!(error.message.starts_with("unsupported:"), "{sql}: {error}");
    }
}

#[test]
fn independent_duckdb_cases_match_at_all_worker_and_batch_sizes() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/date_fields/expected.json"
    );
    let expected: Value = serde_json::from_str(&fs::read_to_string(path).expect(
        "generate DATE goldens with the project Python and tools/testing/date_fields_reference.py",
    ))
    .unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    assert_eq!(expected["setup_sql"], SETUP);
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 67);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "fixture metadata {key}");
        }
        assert_eq!(case["sql"], sql.trim());
    }
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                let mut session = fixture.session(workers, batch_size, optimized);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let context = format!(
                        "{} workers={workers} batch={batch_size} optimized={optimized}: {sql}",
                        case["id"]
                    );
                    // October 15 toolkit decision: the original d059 refusal
                    // becomes DATE output. Retain its original DuckDB receipt.
                    if case["id"] == "d059" {
                        assert_eq!(case["assay_outcome"], "unsupported");
                        let batch = query(&mut session, sql);
                        assert_eq!(batch.schema().field(0).name(), "x");
                        let dates = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap();
                        let actual: Vec<Value> = dates
                            .iter()
                            .map(|d| {
                                d.map_or(Value::Null, |d| {
                                    let d = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                        + chrono::Duration::days(i64::from(d));
                                    json!(format!("{d}T00:00:00"))
                                })
                            })
                            .collect();
                        let wanted: Vec<Value> = case["rows"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|r| r[0].clone())
                            .collect();
                        assert_eq!(actual, wanted, "{context}");
                        continue;
                    }
                    if case["assay_outcome"] != "rows" {
                        let error = session.execute_sql(sql).err().expect(&context);
                        assert!(
                            error
                                .message
                                .starts_with(case["assay_error"].as_str().unwrap()),
                            "{context}: {error}"
                        );
                        continue;
                    }
                    assert_eq!(case["reference_outcome"], "rows", "{context}");
                    let batch = query(&mut session, sql);
                    let names = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| json!(f.name()))
                        .collect::<Vec<_>>();
                    assert_eq!(json!(names), case["columns"], "{context}");
                    let types = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| match f.data_type() {
                            DataType::Int64 => "BIGINT".to_string(),
                            DataType::Boolean => "BOOLEAN".to_string(),
                            DataType::Utf8 => "VARCHAR".to_string(),
                            DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
                            other => panic!("{context}: unexpected {other}"),
                        })
                        .collect::<Vec<_>>();
                    // COUNT/SUM of INTEGER may use HUGEINT in DuckDB; Assay's
                    // checked returned INTEGER remains BIGINT. Values stay exact.
                    let wanted_types = case["types"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| match t.as_str().unwrap() {
                            "HUGEINT" => "BIGINT",
                            other => other,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(types, wanted_types, "{context}");
                    let mut actual = rows(&batch);
                    let mut wanted: Vec<Vec<Value>> =
                        serde_json::from_value(case["rows"].clone()).unwrap();
                    if !case["ordered"].as_bool().unwrap() {
                        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                        wanted.sort_by_key(|row| serde_json::to_string(row).unwrap());
                    }
                    assert_eq!(actual, wanted, "{context}");
                }
            }
        }
    }
}
