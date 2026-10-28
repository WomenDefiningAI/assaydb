//! Regular-expression functions against independent DuckDB 1.5.6 and
//! PostgreSQL 17 references (tools/testing/regexp_reference.py). Every
//! required Assay outcome is derived in the generator from the references
//! and the named admission rules; none is taken from Assay.
use arrow::{
    array::*,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::regexp,
    session::{Limits, Session},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

const CASES: &str = include_str!("fixtures/regexp/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("regexp/analysis.sql"));
static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/regexp")
}
fn load(name: &str) -> Value {
    if name == "analysis_expected.json" {
        return serde_json::from_str(&external_sf1::read("regexp/analysis_expected.json")).unwrap();
    }
    serde_json::from_str(&fs::read_to_string(fixture_dir().join(name)).unwrap()).unwrap()
}
fn tables() -> HashMap<String, PathBuf> {
    HashMap::from([
        ("r".into(), fixture_dir().join("r.parquet")),
        ("empty".into(), fixture_dir().join("empty.parquet")),
        ("t".into(), fixture_dir().join("texts.parquet")),
    ])
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-regexp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(directory.join("spill")).unwrap();
        Self(directory)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn options(workers: usize, batch_size: usize, optimized: bool, spill: Option<&Scratch>) -> Options {
    Options {
        workers,
        batch_size,
        relational_optimizations: optimized,
        memory_bytes: 256 << 20,
        spill: spill.map(|s| SpillOptions {
            directory: s.0.join("spill"),
            sort_run_bytes: 4096,
            merge_fan_in: 2,
            ..SpillOptions::default()
        }),
        ..Options::default()
    }
}

fn settings() -> Vec<(usize, usize, bool)> {
    let mut all = Vec::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                all.push((workers, batch_size, optimized));
            }
        }
    }
    all
}

fn run(sql: &str, options: &Options) -> Result<RecordBatch, String> {
    prepare(sql, &tables(), options)
        .and_then(|plan| execute(&plan, options))
        .map(|e| e.batch)
        .map_err(|e| e.as_str().to_string())
}

fn encode(array: &dyn Array, row: usize) -> Value {
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
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                + chrono::Duration::days(i64::from(days));
            json!(date.format("%Y-%m-%d").to_string())
        }
        DataType::Decimal128(_, scale) => json!(assaydb::types::decimal_text(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row),
            *scale
        )),
        other => panic!("unexpected output type {other}"),
    }
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|c| encode(c.as_ref(), row))
                .collect()
        })
        .collect()
}

fn type_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int64 => "BIGINT".into(),
            DataType::Boolean => "BOOLEAN".into(),
            DataType::Utf8 => "VARCHAR".into(),
            DataType::Date32 => "DATE".into(),
            DataType::Null => "NULL".into(),
            DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
            other => panic!("unexpected output type {other}"),
        })
        .collect()
}

/// DuckDB widens integer sums to HUGEINT and keeps INTEGER 32-bit; Assay's
/// INTEGER is 64-bit (maintainer decision, October 17).
fn duckdb_type(name: &str) -> &str {
    match name {
        "HUGEINT" | "INTEGER" => "BIGINT",
        other => other,
    }
}

fn check_case(case: &Value, sql: &str, options: &Options, context: &str) {
    let result = run(sql, options);
    if let Some(prefix) = case["assay_error"].as_str() {
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{context}: answered"));
        assert!(error.starts_with(prefix), "{context}: {error}");
        return;
    }
    let batch = result.unwrap_or_else(|e| panic!("{context}: {e}"));
    assert_eq!(case["reference_outcome"], "rows", "{context}");
    let names = batch
        .schema()
        .fields()
        .iter()
        .map(|f| json!(f.name()))
        .collect::<Vec<_>>();
    assert_eq!(json!(names), case["columns"], "{context}");
    let wanted_types = case["types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| duckdb_type(t.as_str().unwrap()).to_string())
        .collect::<Vec<_>>();
    assert_eq!(type_names(&batch.schema()), wanted_types, "{context}");
    let mut actual = rows(&batch);
    let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    if !case["ordered"].as_bool().unwrap_or(false) {
        actual.sort_by_key(|r| serde_json::to_string(r).unwrap());
        wanted.sort_by_key(|r| serde_json::to_string(r).unwrap());
    }
    assert_eq!(actual, wanted, "{context}");
}

#[test]
fn reference_cases_match_at_all_worker_batch_and_optimizer_settings() {
    let expected = load("expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert!(
        expected["postgresql_version"]
            .as_str()
            .unwrap()
            .starts_with("17.")
    );
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    let cases = expected["cases"].as_array().unwrap();
    let sources = CASES.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    assert_eq!(cases.len(), 83);
    for (source, case) in sources.iter().zip(cases) {
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        if case["assay_error"]
            .as_str()
            .is_some_and(|e| e.starts_with("unsupported:"))
        {
            assert!(case["difference"].is_string(), "{}", case["id"]);
        }
    }
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        for case in cases {
            let sql = case["sql"].as_str().unwrap();
            let context = format!(
                "{} workers={workers} batch={batch_size} optimized={optimized}",
                case["id"]
            );
            check_case(case, sql, &options, &context);
        }
    }
}

/// Grouping on regular-expression results, with spill enabled.
#[test]
fn grouped_results_with_spill() {
    let expected = load("expected.json");
    let scratch = Scratch::new();
    for case in expected["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        if ![
            "group_counts",
            "group_by_extracted_word",
            "distinct_and_order",
        ]
        .contains(&id)
        {
            continue;
        }
        for (workers, batch_size) in [(1, 1), (4, 7), (10, 8192)] {
            let mut options = options(workers, batch_size, true, Some(&scratch));
            options.memory_bytes = 64 << 20;
            check_case(case, case["sql"].as_str().unwrap(), &options, id);
        }
    }
    assert_eq!(fs::read_dir(scratch.0.join("spill")).unwrap().count(), 0);
}

fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The exact message each row rule produces.
fn rule_messages(rule: &str) -> Vec<&'static str> {
    match rule {
        "case_fold" => vec![regexp::CASE_FOLD],
        "class_non_ascii" => vec![regexp::CLASS_NON_ASCII],
        "vertical_tab" => vec![regexp::VERTICAL_TAB],
        "newline" => vec![regexp::NEWLINE],
        "extent" => vec![regexp::EXTENT, regexp::EMPTY_AFTER],
        other => panic!("unknown rule {other}"),
    }
}

/// One text's value from a compacted list: `{"default": v, "except":
/// {"<text index>": value}}`.
fn reference(values: &Value, row: usize) -> &Value {
    values["except"]
        .get(row.to_string())
        .unwrap_or(&values["default"])
}

fn check_grid_entry(
    entry: &Value,
    template: &str,
    texts: usize,
    options: &Options,
    tally: &mut HashMap<String, usize>,
) {
    let pattern = entry["pattern"].as_str().unwrap();
    let mut expr = template.replace("{p}", &sql_literal(pattern));
    if let Some(replacement) = entry["replacement"].as_str() {
        expr = expr.replace("'<>'", &sql_literal(replacement));
    }
    let context = format!("{} {pattern:?} -> {expr}", entry["family"]);
    let outcomes: Vec<String> = match &entry["assay"] {
        Value::String(one) => vec![one.clone(); texts],
        packed => (0..texts)
            .map(|row| reference(packed, row).as_str().unwrap().to_string())
            .collect(),
    };
    for outcome in &outcomes {
        *tally.entry(outcome.clone()).or_default() += 1;
    }
    let all = format!("SELECT id, {expr} AS r FROM t ORDER BY id");
    match outcomes[0].as_str() {
        "refuse:static" | "error" | "resource" if outcomes.iter().all(|o| *o == outcomes[0]) => {
            let error = run(&all, options).expect_err(&context);
            let ok = match outcomes[0].as_str() {
                "refuse:static" => error.starts_with("unsupported:"),
                "error" => error.starts_with("invalid regular expression"),
                _ => error.starts_with("resource-limit:"),
            };
            assert!(ok, "{context}: {} vs {error}", outcomes[0]);
            return;
        }
        _ => {}
    }
    let answers = (0..texts)
        .filter(|row| outcomes[*row] == "answer")
        .collect::<Vec<_>>();
    if !answers.is_empty() {
        let ids = answers
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT id, {expr} AS r FROM t WHERE id IN ({ids}) ORDER BY id");
        let batch = run(&sql, options).unwrap_or_else(|e| panic!("{context}: {e}"));
        let got = rows(&batch);
        assert_eq!(got.len(), answers.len(), "{context}");
        for (row, values) in answers.iter().zip(got) {
            assert_eq!(
                &values[1],
                reference(&entry["duckdb"], *row),
                "{context} text {row}"
            );
            match entry.get("postgresql") {
                None => {}
                Some(Value::String(same)) => assert_eq!(same, "same", "{context}"),
                Some(pg) => assert_eq!(
                    &values[1],
                    reference(pg, *row),
                    "{context} text {row} (PostgreSQL)"
                ),
            }
        }
    }
    for (row, outcome) in outcomes.iter().enumerate() {
        let Some(rule) = outcome.strip_prefix("refuse:") else {
            continue;
        };
        assert_ne!(rule, "static", "{context}: mixed static outcome");
        let sql = format!("SELECT {expr} AS r FROM t WHERE id = {row}");
        let error = run(&sql, options).expect_err(&format!("{context} text {row}"));
        assert!(
            rule_messages(rule).contains(&error.as_str()),
            "{context} text {row}: {rule} vs {error}"
        );
    }
}

/// Every pattern x text cell of the reference grid: the admitted cells equal
/// DuckDB (and PostgreSQL where it defines the function), every other cell
/// is refused or rejected exactly as the rules require.
#[test]
fn reference_grid_cells_match_the_required_outcome() {
    let grid = load("grid.json");
    assert_eq!(grid["duckdb_version"], "1.5.6");
    let texts = grid["texts"].as_array().unwrap().len();
    let families = grid["families"].as_object().unwrap();
    for (workers, batch_size, optimized) in [(1, 1, false), (10, 7, true)] {
        let options = options(workers, batch_size, optimized, None);
        let mut tally = HashMap::new();
        for entry in grid["grid"].as_array().unwrap() {
            let family = entry["family"].as_str().unwrap();
            let template = families[family]["assay"].as_str().unwrap();
            check_grid_entry(entry, template, texts, &options, &mut tally);
        }
        for entry in grid["refs"].as_array().unwrap() {
            let template = match entry["family"].as_str().unwrap() {
                "replace" => "regexp_replace(s, {p}, '<>')",
                _ => "regexp_replace(s, {p}, '<>', 'g')",
            };
            check_grid_entry(entry, template, texts, &options, &mut tally);
        }
        let counts = grid["outcome_counts"].as_object().unwrap();
        for (outcome, count) in counts {
            assert_eq!(
                tally.get(outcome).copied().unwrap_or(0),
                count.as_u64().unwrap() as usize,
                "{outcome}"
            );
        }
    }
}

/// A data-dependent refusal is reported for the first failing row in input
/// order at every setting.
#[test]
fn row_refusals_report_the_first_row_everywhere() {
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        let context = format!("workers={workers} batch={batch_size} optimized={optimized}");
        // Text 19 ('a\nb') refuses '.', text 23 ('é') the class.
        assert_eq!(
            run("SELECT regexp_like(s, '\\w.') AS m FROM t", &options).err(),
            Some(regexp::NEWLINE.to_string()),
            "{context}"
        );
        assert_eq!(
            run(
                "SELECT regexp_like(s, '\\w.') AS m FROM t WHERE id > 21",
                &options
            )
            .err(),
            Some(regexp::CLASS_NON_ASCII.to_string()),
            "{context}"
        );
        let batch = run(
            "SELECT id, regexp_like(s, '[a-z]\\.') AS m FROM t WHERE id IN (2, 14) ORDER BY id",
            &options,
        )
        .unwrap();
        assert_eq!(
            rows(&batch),
            vec![vec![json!(2), json!(false)], vec![json!(14), json!(true)]],
            "{context}"
        );
    }
}

fn cli(sql: &str) -> Result<Value, String> {
    let scratch = Scratch::new();
    let output = scratch.0.join("out.json");
    let table_json: serde_json::Map<String, Value> =
        tables().into_iter().map(|(k, v)| (k, json!(v))).collect();
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "{}",
        json!({"sql": sql, "tables": table_json, "output": output})
    )
    .unwrap();
    let result = child.wait_with_output().unwrap();
    if !result.status.success() {
        let stdout = String::from_utf8(result.stdout).unwrap();
        return Err(stdout
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find(|e| e["event"] == "failure")
            .map(|e| e["message"].as_str().unwrap().to_string())
            .unwrap_or(stdout));
    }
    Ok(
        serde_json::from_str::<Value>(&fs::read_to_string(&output).unwrap()).unwrap()["rows"]
            .clone(),
    )
}

#[test]
fn cli_over_parquet() {
    let expected = load("expected.json");
    let case = |id: &str| {
        expected["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == id)
            .unwrap()
            .clone()
    };
    for id in [
        "replace_first_and_global",
        "like_table",
        "extract_groups",
        "similar_to",
    ] {
        let case = case(id);
        assert_eq!(
            cli(case["sql"].as_str().unwrap()).unwrap(),
            case["rows"],
            "{id}"
        );
    }
    let refused = case("newline_dot_like_refused");
    assert_eq!(
        cli(refused["sql"].as_str().unwrap()).err().as_deref(),
        Some(regexp::NEWLINE)
    );
    let tilde = cli("SELECT 'abc' ~ 'b' AS m").unwrap_err();
    assert!(tilde.starts_with("unsupported: ~ "), "{tilde}");
}

/// TPC-H SF1 analysis examples. Set ASSAY_TPCH_SF1_DIR to the Parquet
/// directory whose sizes analysis_expected.json records; required for qualification.
#[test]
fn tpch_sf1_analysis_examples_match_the_references() {
    let directory = std::env::var("ASSAY_TPCH_SF1_DIR")
        .expect("full qualification requires ASSAY_TPCH_SF1_DIR; SF1 coverage cannot be skipped");
    let expected = load("analysis_expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    for (name, table) in expected["tables"].as_object().unwrap() {
        let size = fs::metadata(Path::new(&directory).join(format!("{name}.parquet")))
            .unwrap()
            .len();
        assert_eq!(json!(size), table["bytes"], "{name}.parquet");
    }
    let cases = expected["cases"].as_array().unwrap();
    let sources = ANALYSIS.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    let scratch = Scratch::new();
    for (source, case) in sources.iter().zip(cases) {
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        let sql = sql.trim().replace("{tpch}", &directory.replace('\'', "''"));
        for workers in [1, 4, 10] {
            for batch_size in [7, 8192] {
                let mut session = Session::new(
                    &scratch.0,
                    options(workers, batch_size, true, None),
                    Limits::default(),
                )
                .unwrap();
                session.set_local_files_enabled(true);
                let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
                let result = session.execute_sql(&sql);
                if let Some(prefix) = case["assay_error"].as_str() {
                    let error = result.err().expect(&context);
                    assert!(error.message.starts_with(prefix), "{context}");
                    continue;
                }
                let assaydb::session::Output::Query { execution, .. } =
                    result.unwrap_or_else(|e| panic!("{context}: {}", e.message))
                else {
                    panic!("{context}: expected a query");
                };
                assert_eq!(json!(rows(&execution.batch)), case["rows"], "{context}");
            }
        }
    }
}

/// Integration candidate 4: guarded AND/OR (candidate 3) over regular
/// expressions. The column pattern of row 5 is invalid ('(?:'). A row whose
/// result another operand decides skips the failing pattern; an undecided row
/// raises it, and a resource limit (`regexp::TOO_LARGE`) is never a row error.
/// DuckDB 1.5.6 answers the guard-first spelling [1, 4, 7, 8, 10] and raises on
/// the others (it evaluates the pattern first); PostgreSQL 17 raises on all
/// (its planner evaluates regexp_like before the IN list). Assay answers in
/// any operand order and never returns a value computed from a failed step.
#[test]
fn guarded_invalid_column_pattern_is_skipped_and_unguarded_raises() {
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        let context = format!("workers={workers} batch={batch_size} optimized={optimized}");
        for sql in [
            "SELECT id FROM r WHERE id IN (1, 4, 7, 8, 10) AND regexp_matches(s, p) ORDER BY id",
            "SELECT id FROM r WHERE regexp_matches(s, p) AND id IN (1, 4, 7, 8, 10) ORDER BY id",
        ] {
            let batch = run(sql, &options).unwrap_or_else(|e| panic!("{sql}: {e} {context}"));
            assert_eq!(
                rows(&batch),
                [1, 4, 7, 8, 10]
                    .iter()
                    .map(|id| vec![json!(id)])
                    .collect::<Vec<_>>(),
                "{sql} {context}"
            );
        }
        let batch = run(
            "SELECT id, id <> 5 AND regexp_matches(s, p) AS m FROM r WHERE id IN (1, 5, 7) ORDER BY id",
            &options,
        )
        .unwrap();
        assert_eq!(
            rows(&batch),
            vec![
                vec![json!(1), json!(true)],
                vec![json!(5), json!(false)],
                vec![json!(7), json!(true)]
            ],
            "{context}"
        );
        let error = run(
            "SELECT id FROM r WHERE id IN (5, 7) AND regexp_matches(s, p) ORDER BY id",
            &options,
        )
        .unwrap_err();
        assert!(error.starts_with(regexp::INVALID), "{error} {context}");
    }
    assert!(!regexp::RUNTIME_ERRORS.contains(&regexp::TOO_LARGE));
    assert!(regexp::TOO_LARGE.starts_with("resource-limit:"));
}

/// TIMESTAMP inputs: DuckDB 1.5.6 (no regexp_matches(TIMESTAMP, ...)) and
/// PostgreSQL 17 (no regexp_like(timestamp, ...)/timestamp ~ text) both reject
/// them at binding; Assay refuses like its other TEXT-only functions.
#[test]
fn timestamp_regex_inputs_are_refused() {
    let options = options(1, 8192, true, None);
    for sql in [
        "SELECT regexp_matches(TIMESTAMP '2024-03-10 01:00:00', '2024') AS m",
        "SELECT regexp_replace(TIMESTAMP '2024-03-10 01:00:00', '2024', 'x') AS m",
    ] {
        let error = run(sql, &options).unwrap_err();
        assert!(
            error.starts_with("unsupported:") && error.contains("requires TEXT arguments"),
            "{sql}: {error}"
        );
    }
}
