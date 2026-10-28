//! String functions against independent DuckDB 1.5.6 goldens
//! (tools/testing/string_functions_reference.py). No Assay output feeds them.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    expr::{Call, Expr, Input, Kind, Value as ExprValue, text},
    memory::Budget,
    session::{Limits, Output, Session},
    types::Type,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const SETUP: &str = include_str!("fixtures/string_functions/setup.sql");
const CASES: &str = include_str!("fixtures/string_functions/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("string_functions/analysis.sql"));
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-string-functions-{}-{}",
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
                memory_bytes: 256 << 20,
                ..Options::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap();
        for statement in SETUP.split(";\n").filter(|s| !s.trim().is_empty()) {
            session.execute_sql(statement).unwrap();
        }
        session
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn expected(name: &str) -> Value {
    if name == "analysis_expected.json" {
        return serde_json::from_str(&external_sf1::read(
            "string_functions/analysis_expected.json",
        ))
        .unwrap();
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/string_functions")
        .join(name);
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
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
                .map(|a| {
                    if a.is_null(row) || a.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    match a.data_type() {
                        DataType::Int64 => {
                            json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
                        }
                        DataType::Utf8 => {
                            json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                        }
                        DataType::Boolean => {
                            json!(
                                a.as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .unwrap()
                                    .value(row)
                            )
                        }
                        DataType::Decimal128(_, scale) => json!(assaydb::types::decimal_text(
                            a.as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        other => panic!("unexpected output type {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Int64 => "BIGINT".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Utf8 => "VARCHAR".into(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        other => panic!("unexpected output type {other}"),
    }
}

/// Run one reference case and compare names, types and exact values.
fn check_case(session: &mut Session, case: &Value, sql: &str, context: &str) {
    if let Some(error) = case["assay_error"].as_str() {
        let actual = session.execute_sql(sql).err().expect(context);
        assert!(
            actual.message.starts_with(error),
            "{context}: {}",
            actual.message
        );
        return;
    }
    assert_eq!(case["reference_outcome"], "rows", "{context}");
    let batch = query(session, sql);
    let names = batch
        .schema()
        .fields()
        .iter()
        .map(|f| json!(f.name()))
        .collect::<Vec<_>>();
    assert_eq!(json!(names), case["columns"], "{context}");
    if case.get("null_result_type").is_none() {
        let types = batch
            .schema()
            .fields()
            .iter()
            .map(|f| type_name(f.data_type()))
            .collect::<Vec<_>>();
        // DuckDB widens COUNT/SUM of integers to HUGEINT; Assay's checked
        // returned INTEGER is BIGINT. Values stay exact.
        let wanted = case["types"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| match t.as_str().unwrap() {
                "HUGEINT" => "BIGINT",
                other => other,
            })
            .collect::<Vec<_>>();
        assert_eq!(types, wanted, "{context}");
    }
    let mut actual = rows(&batch);
    let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    if !case["ordered"].as_bool().unwrap() {
        actual.sort_by_key(|r| serde_json::to_string(r).unwrap());
        wanted.sort_by_key(|r| serde_json::to_string(r).unwrap());
    }
    assert_eq!(actual, wanted, "{context}");
}

#[test]
fn independent_duckdb_cases_match_at_all_worker_and_batch_sizes() {
    let expected = expected("expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    assert_eq!(expected["setup_sql"], SETUP);
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 90);
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        for (key, value) in metadata.as_object().unwrap() {
            assert_eq!(&case[key], value, "fixture metadata {key}");
        }
        assert!(case["pg"].is_string(), "{}: PostgreSQL column", case["id"]);
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
                    check_case(&mut session, case, sql, &context);
                }
                for control in expected["controls"].as_array().unwrap() {
                    let mut control = control.clone();
                    control["reference_outcome"] = json!("rows");
                    control["ordered"] = json!(true);
                    let sql = control["sql"].as_str().unwrap().to_string();
                    if control["assay_error"].is_null() {
                        let batch = query(&mut session, &sql);
                        assert_eq!(json!(rows(&batch)), control["rows"], "{sql}");
                    } else {
                        check_case(&mut session, &control, &sql, &sql);
                    }
                }
            }
        }
    }
}

fn column_input(array: ArrayRef, budget: &Budget) -> Input {
    let len = array.len();
    Input {
        columns: vec![Some(ExprValue::adopt(array, budget).unwrap())],
        len,
    }
}
fn column(ty: Type) -> Expr {
    Expr {
        kind: Kind::Column(0),
        ty,
    }
}
fn scalars() -> impl Iterator<Item = char> {
    (0..=0x10FFFF).filter_map(char::from_u32)
}

#[test]
fn case_mapping_matches_duckdb_for_every_code_point() {
    let expected = expected("expected.json");
    let budget = Budget::new(1 << 30);
    let input = column_input(
        Arc::new(StringArray::from_iter_values(scalars().map(String::from))),
        &budget,
    );
    for name in ["upper", "lower"] {
        let mut wanted: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        for pair in expected["case_mapping"][name].as_array().unwrap() {
            wanted.insert(
                pair[0].as_u64().unwrap() as u32,
                pair[1].as_u64().unwrap() as u32,
            );
        }
        let expr = Expr {
            kind: Kind::Function(name.into(), Box::new(column(Type::Text))),
            ty: Type::Text,
        };
        let value = expr.eval(&input, &budget).unwrap();
        let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
        for (row, c) in scalars().enumerate() {
            let mapped = wanted.get(&(c as u32)).copied().unwrap_or(c as u32);
            assert_eq!(
                array.value(row),
                char::from_u32(mapped).unwrap().to_string(),
                "{name} U+{:04X}",
                c as u32
            );
        }
    }
}

#[test]
fn trim_space_set_and_reverse_ranges_match_duckdb() {
    let expected = expected("expected.json");
    let spaces = expected["trim_spaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap() as u32)
        .collect::<Vec<_>>();
    let ours = scalars()
        .filter(|c| *c == ' ' || text::duckdb_trim_space(*c))
        .map(|c| c as u32)
        .collect::<Vec<_>>();
    assert_eq!(ours, spaces);
    let budget = Budget::new(1 << 30);
    let trim = Expr {
        kind: Kind::Call(
            Call::Trim {
                leading: true,
                trailing: false,
            },
            vec![column(Type::Text)],
        ),
        ty: Type::Text,
    };
    // Every DuckDB-only space refuses; every other character is kept.
    for c in scalars().filter(|c| *c != ' ') {
        let one = column_input(
            Arc::new(StringArray::from(vec![format!("{c}a{c}")])),
            &budget,
        );
        match trim.eval(&one, &budget) {
            Err(error) => {
                assert!(text::duckdb_trim_space(c), "U+{:04X}", c as u32);
                assert_eq!(error, text::TRIM_SPACES);
            }
            Ok(value) => {
                assert!(!text::duckdb_trim_space(c));
                let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(array.value(0), format!("{c}a{c}"));
            }
        }
    }
    let ranges = expected["reverse_safe"]["ranges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r[0].as_u64().unwrap() as u32, r[1].as_u64().unwrap() as u32))
        .collect::<Vec<_>>();
    assert_eq!(ranges, text::REVERSE_SAFE.to_vec());
    let reverse = Expr {
        kind: Kind::Call(Call::Reverse, vec![column(Type::Text)]),
        ty: Type::Text,
    };
    for c in scalars() {
        // A non-ASCII neighbor selects the grapheme-sensitive path.
        let one = column_input(Arc::new(StringArray::from(vec![format!("é{c}")])), &budget);
        let safe = ranges
            .iter()
            .any(|(lo, hi)| (*lo..=*hi).contains(&(c as u32)));
        match reverse.eval(&one, &budget) {
            Ok(value) => {
                assert!(safe, "U+{:04X}", c as u32);
                let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(array.value(0), format!("{c}é"));
            }
            Err(error) => {
                assert!(!safe, "U+{:04X}", c as u32);
                assert_eq!(error, text::REVERSE_JOINING);
            }
        }
    }
}

#[test]
fn double_to_varchar_matches_duckdb_bits_controls() {
    let expected = expected("expected.json");
    let controls = expected["double_to_varchar"].as_array().unwrap();
    assert!(controls.len() > 3000);
    let values = controls
        .iter()
        .map(|c| f64::from_bits(u64::from_str_radix(c[0].as_str().unwrap(), 16).unwrap()))
        .collect::<Vec<_>>();
    let budget = Budget::new(1 << 26);
    let cast = Expr {
        kind: Kind::Cast(Box::new(column(Type::Float))),
        ty: Type::Text,
    };
    let mut rejected = 0;
    for (value, control) in values.iter().zip(controls) {
        let input = column_input(Arc::new(Float64Array::from(vec![*value])), &budget);
        match cast.eval(&input, &budget) {
            Ok(text) => {
                let array = text.array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(array.value(0), control[1].as_str().unwrap(), "{value:e}");
            }
            Err(error) => {
                // Only negative zero is refused among finite values.
                assert!(
                    *value == 0.0 && value.is_sign_negative(),
                    "{value:e}: {error}"
                );
                assert!(error.starts_with("unsupported: negative zero DOUBLE to TEXT"));
                rejected += 1;
            }
        }
    }
    assert_eq!(rejected, 1);
    for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let input = column_input(Arc::new(Float64Array::from(vec![value])), &budget);
        let error = cast.eval(&input, &budget).err().unwrap();
        assert!(error.starts_with("unsupported: non-finite DOUBLE to TEXT"));
    }
    let input = column_input(Arc::new(Float64Array::from(vec![None, Some(1.0)])), &budget);
    let value = cast.eval(&input, &budget).unwrap();
    let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(array.is_null(0));
    assert_eq!(array.value(1), "1.0");
}

/// Assay refusals keep a single error kind across settings, and statements
/// that both references reject are errors rather than answers.
#[test]
fn refusals_and_errors_are_explicit() {
    let fixture = Fixture::new();
    let mut session = fixture.session(4, 7, true);
    for (sql, prefix) in [
        (
            "SELECT LPAD('a', 3000000000, 'x')",
            "LPAD/RPAD length out of range",
        ),
        (
            "SELECT RIGHT('abc', -3000000000)",
            "unsupported: LEFT/RIGHT count outside the 32-bit range",
        ),
        (
            "SELECT SPLIT_PART('a', ',', 3000000000)",
            "unsupported: SPLIT_PART field outside the 32-bit range",
        ),
        (
            "SELECT LEFT(1.5, 1)",
            "unsupported: LEFT requires TEXT arguments",
        ),
        (
            "SELECT LEFT('a', 1.5)",
            "unsupported: LEFT requires an INTEGER count",
        ),
        ("SELECT 1 || 2", "unsupported: || requires a TEXT operand"),
        (
            "SELECT 'a' || x'00'",
            "unsupported: Binary in string concatenation",
        ),
        (
            "SELECT 'a' ILIKE 'A' ESCAPE 'a'",
            "unsupported: ILIKE ESCAPE with a cased letter",
        ),
        (
            "SELECT 'a' LIKE 'a' ESCAPE 1",
            "unsupported: LIKE ESCAPE requires a string literal",
        ),
        ("SELECT REVERSE('a', 'b')", "unsupported: REVERSE arity"),
    ] {
        let error = session.execute_sql(sql).err().unwrap();
        assert!(
            error.message.starts_with(prefix),
            "{sql}: {}",
            error.message
        );
    }
    // A runtime refusal in a later row is reported identically everywhere.
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = fixture.session(workers, batch_size, true);
            session
                .execute_sql("CREATE TABLE r(id BIGINT, t TEXT)")
                .unwrap();
            session
                .execute_sql(
                    "INSERT INTO r VALUES (1, 'ok'), (2, 'fine'), (3, 'é\u{301}'), (4, 'x')",
                )
                .unwrap();
            let error = session
                .execute_sql("SELECT id, REVERSE(t) FROM r ORDER BY id")
                .err()
                .unwrap();
            assert_eq!(error.message, text::REVERSE_JOINING);
            let ok = query(
                &mut session,
                "SELECT id, REVERSE(t) AS r FROM r WHERE id <> 3 ORDER BY id",
            );
            assert_eq!(
                rows(&ok),
                vec![
                    vec![json!(1), json!("ko")],
                    vec![json!(2), json!("enif")],
                    vec![json!(4), json!("x")]
                ]
            );
        }
    }
}

/// TPC-H SF1 analysis examples. Set ASSAY_TPCH_SF1_DIR to the Parquet
/// directory whose sizes analysis_expected.json records; required for qualification.
#[test]
fn tpch_sf1_analysis_examples_match_duckdb() {
    let directory = std::env::var("ASSAY_TPCH_SF1_DIR")
        .expect("full qualification requires ASSAY_TPCH_SF1_DIR; SF1 coverage cannot be skipped");
    let expected = expected("analysis_expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    for (name, file) in expected["files"].as_object().unwrap() {
        let size = fs::metadata(Path::new(&directory).join(format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), file["bytes"], "{name}.parquet");
    }
    let cases = expected["cases"].as_array().unwrap();
    let sources = ANALYSIS.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    let fixture = Fixture::new();
    for (source, case) in sources.iter().zip(cases) {
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        let sql = sql.trim().replace("{tpch}", &directory.replace('\'', "''"));
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let mut session = fixture.session(workers, batch_size, true);
                session.set_local_files_enabled(true);
                let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
                check_case(&mut session, case, &sql, &context);
            }
        }
    }
}

/// Integration of string functions with mixed Int/Float promotion (Helen,
/// Oct 15: promote like DuckDB). A promoted DOUBLE operand of `||`/CONCAT is
/// formatted like DuckDB 1.5.6 (2^53 + 1 rounds to 9007199254740992.0); a
/// promoted value is never narrowed into an INTEGER count argument.
#[test]
fn promoted_mixed_conditionals_in_text_functions() {
    let fixture = Fixture::new();
    for (workers, batch_size, optimized) in [(1, 1, false), (4, 7, true)] {
        let mut session = fixture.session(workers, batch_size, optimized);
        session
            .execute_sql("CREATE TABLE m(id INTEGER, s TEXT)")
            .unwrap();
        session
            .execute_sql("INSERT INTO m VALUES (9007199254740993, 'a'), (2, 'b')")
            .unwrap();
        for (sql, wanted) in [
            (
                "SELECT s || ':' || CASE WHEN id > 5 THEN id ELSE CAST(0.5 AS REAL) END AS v FROM m ORDER BY s",
                [["a:9007199254740992.0"], ["b:0.5"]],
            ),
            (
                "SELECT CONCAT(s, CASE WHEN id > 5 THEN id ELSE CAST(0.5 AS REAL) END) AS v FROM m ORDER BY s",
                [["a9007199254740992.0"], ["b0.5"]],
            ),
        ] {
            assert_eq!(
                rows(&query(&mut session, sql)),
                wanted
                    .iter()
                    .map(|row| row.iter().map(|v| json!(v)).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        }
        let sql = "SELECT LEFT(s, CASE WHEN id > 5 THEN id ELSE CAST(0.5 AS REAL) END) FROM m";
        let error = session.execute_sql(sql).err().unwrap();
        assert!(
            error.message.contains("requires an INTEGER count"),
            "{sql}: {error}"
        );
    }
}
