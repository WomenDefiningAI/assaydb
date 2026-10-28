//! Scalar math functions and GREATEST/LEAST against pinned DuckDB 1.5.6 /
//! PostgreSQL 17 references (tests/fixtures/scalar_math). Every expected Assay
//! outcome is derived in the generator from the references plus a named rule;
//! none is taken from Assay.
//!
//! Float values are compared bit for bit on macOS, where the references were
//! generated and both engines call the same platform libm. Elsewhere a libm
//! may round a transcendental result differently in the last place, so finite
//! values may differ by at most 4 ulp there; nonfinite values and signed zeros
//! stay exact everywhere.
use arrow::{
    array::*,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

const CASES: &str = include_str!("fixtures/scalar_math/cases.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/scalar_math")
}

fn expected() -> Value {
    serde_json::from_str(include_str!("fixtures/scalar_math/expected.json")).unwrap()
}

fn tables() -> HashMap<String, PathBuf> {
    HashMap::from([
        ("u".into(), fixture_dir().join("values.parquet")),
        ("v".into(), fixture_dir().join("pairs.parquet")),
        ("n".into(), fixture_dir().join("nums.parquet")),
        ("empty".into(), fixture_dir().join("empty.parquet")),
        ("rf".into(), fixture_dir().join("remainder_float.parquet")),
        ("rd".into(), fixture_dir().join("remainder_decimal.parquet")),
    ])
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-scalar-math-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(directory.join("spill")).unwrap();
        Self(directory)
    }
    fn clean(&self) {
        assert_eq!(fs::read_dir(self.0.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn options(workers: usize, batch_size: usize, optimized: bool, spill: Option<&Scratch>) -> Options {
    Options {
        workers,
        batch_size,
        relational_optimizations: optimized,
        memory_bytes: 32 << 20,
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

fn run(sql: &str, options: &Options) -> Result<assaydb::execute::Execution, String> {
    prepare(sql, &tables(), options)
        .and_then(|plan| execute(&plan, options))
        .map_err(|e| e.as_str().to_string())
}

/// Reference encoding: NaN is "NaN", infinities "+inf"/"-inf", zeros "0"/"-0",
/// finite Floats shortest round-trip text (compared after parsing), DECIMAL
/// exact text at its scale, INTEGER a JSON number.
fn encode(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) || array.data_type() == &DataType::Null {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            if v.is_nan() {
                json!("NaN")
            } else if v.is_infinite() {
                json!(if v > 0.0 { "+inf" } else { "-inf" })
            } else if v == 0.0 {
                json!(if v.is_sign_negative() { "-0" } else { "0" })
            } else {
                json!(format!("{v:?}"))
            }
        }
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
        DataType::Decimal128(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row);
            let scale = u32::try_from(*scale).unwrap();
            let sign = if value < 0 { "-" } else { "" };
            let magnitude = value.unsigned_abs();
            if scale == 0 {
                return json!(format!("{sign}{magnitude}"));
            }
            let factor = 10_u128.pow(scale);
            json!(format!(
                "{sign}{}.{:0width$}",
                magnitude / factor,
                magnitude % factor,
                width = scale as usize
            ))
        }
        other => panic!("unexpected scalar math output {other}"),
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

fn types(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int64 => "BIGINT".into(),
            DataType::Float64 => "DOUBLE".into(),
            DataType::Boolean => "BOOLEAN".into(),
            DataType::Utf8 => "VARCHAR".into(),
            DataType::Date32 => "DATE".into(),
            DataType::Null => "NULL".into(),
            DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
            other => panic!("unexpected type {other}"),
        })
        .collect()
}

/// Assay has one 64-bit INTEGER type and precision-38 DECIMALs: DuckDB's
/// narrower integers map to BIGINT and DECIMAL(p,s) to DECIMAL(38,s).
fn reference_type(ty: &str) -> String {
    match ty {
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" => "BIGINT".into(),
        decimal if decimal.starts_with("DECIMAL(") => {
            let scale = decimal
                .trim_end_matches(')')
                .rsplit(',')
                .next()
                .unwrap()
                .to_string();
            format!("DECIMAL(38,{scale})")
        }
        other => other.into(),
    }
}

fn float(value: &Value) -> Option<f64> {
    match value.as_str()? {
        "NaN" => Some(f64::NAN),
        "+inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        "-0" => Some(-0.0),
        text => text.parse().ok(),
    }
}

fn ulps(a: f64, b: f64) -> u64 {
    let key = |x: f64| {
        let bits = x.to_bits() as i64;
        if bits < 0 { i64::MIN - bits } else { bits }
    };
    key(a).abs_diff(key(b))
}

/// `tolerance` is a relative bound for Float aggregates (fixed-order sums).
fn same(actual: &Value, wanted: &Value, tolerance: Option<f64>) -> bool {
    if actual == wanted {
        return true;
    }
    // Integers and DECIMAL text compare exactly; only Float text parses here.
    let (Some(a), Some(b)) = (float(actual), float(wanted)) else {
        return false;
    };
    if !(actual.is_string() && wanted.is_string()) {
        return false;
    }
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if !a.is_finite() || !b.is_finite() || a == 0.0 || b == 0.0 {
        return a.to_bits() == b.to_bits();
    }
    if let Some(tolerance) = tolerance {
        return (a - b).abs() <= b.abs() * tolerance;
    }
    if cfg!(target_os = "macos") {
        a.to_bits() == b.to_bits()
    } else {
        ulps(a, b) <= 4
    }
}

fn assert_rows(
    actual: &[Vec<Value>],
    wanted: &[Vec<Value>],
    tolerance: Option<f64>,
    context: &str,
) {
    assert_eq!(
        actual.len(),
        wanted.len(),
        "{context}: {actual:?} != {wanted:?}"
    );
    for (a, w) in actual.iter().zip(wanted) {
        assert_eq!(a.len(), w.len(), "{context}");
        for (a, w) in a.iter().zip(w) {
            assert!(
                same(a, w, tolerance),
                "{context}: got {a}, want {w}\n{actual:?}\n!= {wanted:?}"
            );
        }
    }
}

/// Bulk queries over every reference-successful row (contiguous id ranges,
/// at most eight per query to keep the predicate shallow), so NaN/infinite
/// rows cross batch and worker boundaries, plus one single-row query per
/// expected error.
fn check_cells(table: &str, cells: &[&Value], expression: &str) {
    let ok: Vec<&Value> = cells
        .iter()
        .copied()
        .filter(|c| c["assay"]["status"] == "ok")
        .collect();
    let errors: Vec<&Value> = cells
        .iter()
        .copied()
        .filter(|c| c["assay"]["status"] == "error")
        .collect();
    assert_eq!(ok.len() + errors.len(), cells.len(), "{expression}");
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    for cell in &ok {
        let id = cell["id"].as_i64().unwrap();
        match ranges.last_mut() {
            Some((_, high)) if *high + 1 == id => *high = id,
            _ => ranges.push((id, id)),
        }
    }
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        let mut checked = 0;
        for chunk in ranges.chunks(8) {
            let filter = chunk
                .iter()
                .map(|(low, high)| format!("id BETWEEN {low} AND {high}"))
                .collect::<Vec<_>>()
                .join(" OR ");
            let bulk = format!("SELECT id, {expression} FROM {table} WHERE {filter} ORDER BY id");
            let context =
                format!("{bulk} workers={workers} batch={batch_size} optimized={optimized}");
            let result = run(&bulk, &options).unwrap_or_else(|e| panic!("{context}: {e}"));
            let actual = rows(&result.batch);
            let wanted = &ok[checked..checked + actual.len()];
            checked += actual.len();
            for (row, cell) in actual.iter().zip(wanted) {
                assert_eq!(row[0], cell["id"], "{context}");
                assert!(
                    same(&row[1], &cell["assay"]["value"], None),
                    "{context}: id {} ({cell}): got {}, want {} [{}]",
                    cell["id"],
                    row[1],
                    cell["assay"]["value"],
                    cell["rule"]
                );
            }
        }
        assert_eq!(checked, ok.len(), "{expression}");
        if !matches!(
            (workers, batch_size, optimized),
            (1, 8192, true) | (10, 1, false)
        ) {
            continue;
        }
        for cell in &errors {
            let sql = format!("SELECT {expression} FROM {table} WHERE id = {}", cell["id"]);
            assert_eq!(
                run(&sql, &options).err().as_deref(),
                cell["assay"]["error"].as_str(),
                "{sql} workers={workers} [{}]",
                cell["rule"]
            );
        }
    }
}

#[test]
fn fixture_is_pinned_and_every_cell_has_a_rule() {
    let reference = expected();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    assert!(
        reference["postgresql_version"]
            .as_str()
            .unwrap()
            .starts_with("17.")
    );
    assert_eq!(reference["unexpected_reference_outcomes"], json!([]));
    let unary = reference["unary"].as_array().unwrap();
    let binary = reference["binary"].as_array().unwrap();
    assert_eq!(unary.len(), 23 * 30);
    assert_eq!(binary.len(), 6 * 18 * 18);
    let mut agreeing = 0;
    for cell in unary.iter().chain(binary) {
        let rule = cell["rule"].as_str().unwrap();
        assert!(!rule.is_empty());
        assert!(matches!(
            cell["assay"]["status"].as_str(),
            Some("ok" | "error")
        ));
        if rule.starts_with("references agree") {
            agreeing += 1;
            assert_eq!(cell["duckdb"]["status"], "ok");
            assert_eq!(cell["postgresql"]["status"], "ok");
            assert_eq!(cell["assay"], cell["duckdb"]);
        }
    }
    assert_eq!(agreeing, 2193);
    let remainder = reference["remainder"].as_array().unwrap();
    assert_eq!(remainder.len(), 2 * 15 * 15 + 4 * 10 * 10);
    let mut agreeing = 0;
    for cell in remainder {
        assert!(!cell["rule"].as_str().unwrap().is_empty());
        if cell["rule"]
            .as_str()
            .unwrap()
            .starts_with("references agree")
        {
            agreeing += 1;
            assert_eq!(cell["assay"], cell["duckdb"]);
        }
    }
    assert_eq!(agreeing, 676);
    // DEGREES is refused because the references differ in the last bit.
    let degrees = reference["degrees"].as_array().unwrap();
    assert!(
        degrees
            .iter()
            .any(|c| c["duckdb"]["status"] == "ok" && c["duckdb"] != c["postgresql"])
    );
}

#[test]
fn unary_functions_over_nonfinite_and_boundary_values() {
    let reference = expected();
    let mut expressions: Vec<&str> = Vec::new();
    for cell in reference["unary"].as_array().unwrap() {
        let expr = cell["expr"].as_str().unwrap();
        if !expressions.contains(&expr) {
            expressions.push(expr);
        }
    }
    assert_eq!(expressions.len(), 23);
    for expression in expressions {
        let cells: Vec<&Value> = reference["unary"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["expr"] == expression)
            .collect();
        check_cells("u", &cells, expression);
    }
}

#[test]
fn binary_functions_over_ordered_pairs() {
    let reference = expected();
    let mut expressions: Vec<&str> = Vec::new();
    for cell in reference["binary"].as_array().unwrap() {
        let expr = cell["expr"].as_str().unwrap();
        if !expressions.contains(&expr) {
            expressions.push(expr);
        }
    }
    assert_eq!(expressions.len(), 6);
    for expression in expressions {
        let cells: Vec<&Value> = reference["binary"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["expr"] == expression)
            .collect();
        check_cells("v", &cells, expression);
    }
}

#[test]
fn remainder_over_float_and_decimal_pairs() {
    let reference = expected();
    let cells = reference["remainder"].as_array().unwrap();
    let mut groups: Vec<(&str, &str)> = Vec::new();
    for cell in cells {
        let key = (
            cell["table"].as_str().unwrap(),
            cell["expr"].as_str().unwrap(),
        );
        if !groups.contains(&key) {
            groups.push(key);
        }
    }
    assert_eq!(groups.len(), 6);
    for (table, expression) in groups {
        let group: Vec<&Value> = cells
            .iter()
            .filter(|c| c["table"] == table && c["expr"] == expression)
            .collect();
        check_cells(table, &group, expression);
    }
}

#[test]
fn duckdb_cases_match_at_all_worker_batch_and_spill_settings() {
    let reference = expected();
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 62);
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
    let scratch = Scratch::new();
    for case in cases {
        let sql = case["sql"].as_str().unwrap();
        let spill_settings: &[bool] = if case["spill"] == true {
            &[false, true]
        } else {
            &[false]
        };
        for (workers, batch_size, optimized) in settings() {
            for &spill in spill_settings {
                let options = options(workers, batch_size, optimized, spill.then_some(&scratch));
                let context = format!(
                    "{} workers={workers} batch={batch_size} optimized={optimized} spill={spill}: {sql}",
                    case["id"]
                );
                let result = run(sql, &options);
                if case["assay_outcome"] != "rows" {
                    let error = result.err().expect(&context);
                    assert!(
                        error.starts_with(case["assay_error"].as_str().unwrap()),
                        "{context}: {error}"
                    );
                    scratch.clean();
                    continue;
                }
                assert_eq!(case["reference_outcome"], "rows", "{context}");
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                let schema = result.batch.schema();
                let names = schema
                    .fields()
                    .iter()
                    .map(|f| json!(f.name()))
                    .collect::<Vec<_>>();
                assert_eq!(json!(names), case["columns"], "{context}");
                let wanted_types = match case.get("assay_types") {
                    Some(types) => types
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| t.as_str().unwrap().to_string())
                        .collect::<Vec<_>>(),
                    None => case["types"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| reference_type(t.as_str().unwrap()))
                        .collect(),
                };
                assert_eq!(types(&schema), wanted_types, "{context}");
                let mut actual = rows(&result.batch);
                // A documented difference (approved Assay rule) names its rows.
                let mut wanted: Vec<Vec<Value>> =
                    serde_json::from_value(case.get("assay_rows").unwrap_or(&case["rows"]).clone())
                        .unwrap();
                if !case["ordered"].as_bool().unwrap() {
                    actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                    wanted.sort_by_key(|row| serde_json::to_string(row).unwrap());
                }
                assert_rows(&actual, &wanted, case["float_tolerance"].as_f64(), &context);
                if spill && sql.contains("GROUP BY") {
                    assert!(
                        result.metrics.external_aggregate_partitions > 0,
                        "{context}: spill must be exercised"
                    );
                }
                scratch.clean();
            }
        }
    }
}

#[test]
fn refused_and_invalid_forms_stay_explicit() {
    let options = Options::default();
    for (sql, prefix) in [
        ("SELECT degrees(x) FROM u", "unsupported: DEGREES"),
        ("SELECT degrees(NULL)", "unsupported: DEGREES"),
        ("SELECT t % 2 FROM n", "remainder requires numeric inputs"),
        (
            "SELECT mod(dt, 2) FROM n",
            "remainder requires numeric inputs",
        ),
        ("SELECT trunc(x, id) FROM u", "unsupported: TRUNC precision"),
        (
            "SELECT trunc(x, 99999999999) FROM u",
            "unsupported: TRUNC precision must fit INTEGER",
        ),
        ("SELECT floor(x TO DAY) FROM u", "unsupported:"),
        ("SELECT ceil(x, 1) FROM u", "unsupported:"),
        (
            "SELECT sqrt(t) FROM n",
            "unsupported: SQRT requires a numeric",
        ),
        (
            "SELECT power(dt, 2) FROM n",
            "unsupported: POWER requires a numeric",
        ),
        (
            "SELECT sign(i > 0) FROM n",
            "unsupported: SIGN requires a numeric",
        ),
        (
            "SELECT greatest(i, dt) FROM n",
            "unsupported: DATE comparison requires",
        ),
        (
            "SELECT greatest(i, t) FROM n",
            "GREATEST arguments have incompatible",
        ),
        (
            "SELECT least(t, 1) FROM n",
            "LEAST arguments have incompatible",
        ),
        (
            "SELECT greatest()",
            "GREATEST requires at least one argument",
        ),
        ("SELECT power(2)", "POWER requires 2 argument(s)"),
        ("SELECT log(1, 2, 3)", "LOG requires 1 or 2 argument(s)"),
        ("SELECT trunc(1, 2, 3)", "TRUNC requires 1 or 2 argument(s)"),
        ("SELECT sqrt(x => 4)", "unsupported: SQRT requires unnamed"),
    ] {
        let error = prepare(sql, &tables(), &options)
            .and_then(|p| execute(&p, &options))
            .err()
            .unwrap_or_else(|| panic!("{sql} unexpectedly succeeded"));
        assert!(error.as_str().starts_with(prefix), "{sql}: {error}");
    }
}

/// Error replay picks the first failing row in input order whatever the
/// worker count, batch size or which row a worker reached first.
#[test]
fn competing_domain_errors_report_the_first_row() {
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        for (sql, wanted) in [
            // Row 0 is negative, row 2 is zero.
            (
                "SELECT ln(d) FROM n",
                "cannot take logarithm of a negative number",
            ),
            (
                "SELECT ln(d) FROM n WHERE id >= 2",
                "cannot take logarithm of zero",
            ),
            // u: id 3 is 0 (LN fails), id 9 is 2 (ASIN fails); id 1 is +inf.
            (
                "SELECT CASE WHEN id = 3 THEN ln(x) ELSE asin(x) END FROM u WHERE id IN (3, 9)",
                "cannot take logarithm of zero",
            ),
            (
                "SELECT CASE WHEN id = 9 THEN ln(x) ELSE sin(x) END FROM u WHERE id IN (1, 3, 9)",
                "SIN input is out of range",
            ),
        ] {
            assert_eq!(
                run(sql, &options).err().as_deref(),
                Some(wanted),
                "{sql} workers={workers} batch={batch_size} optimized={optimized}"
            );
        }
    }
}

fn cli(sql: &str) -> Result<(Vec<Value>, Vec<Vec<Value>>), String> {
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
        let message = stdout
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find(|e| e["event"] == "failure")
            .map(|e| e["message"].as_str().unwrap().to_string())
            .unwrap_or(stdout);
        return Err(message);
    }
    let value: Value = serde_json::from_str(&fs::read_to_string(&output).unwrap()).unwrap();
    let types = value["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["type"].clone())
        .collect();
    Ok((
        types,
        serde_json::from_value(value["rows"].clone()).unwrap(),
    ))
}

#[test]
fn cli_over_parquet() {
    // nums.parquet row 1: i 7, d 2.55, e 1.235, f 2.5; row 6: f = 1e15 + 0.5.
    assert_eq!(
        cli(
            "SELECT floor(d), ceil(e), trunc(d, 1), sign(f), greatest(i, d), least(i, f), power(2, i), sqrt(16) FROM n WHERE id = 1"
        ),
        Ok((
            [
                "decimal", "decimal", "decimal", "int", "decimal", "float", "float", "float"
            ]
            .map(|t| json!(t))
            .to_vec(),
            vec![vec![
                json!("2"),
                json!("2"),
                json!("2.5"),
                json!(1),
                json!("7.00"),
                json!("2.5"),
                json!("128"),
                json!("4")
            ]]
        ))
    );
    // values.parquet: id 0 NaN, 1 +inf, 2 -inf, 4 -0.
    assert_eq!(
        cli("SELECT id, sqrt(x), exp(-x), greatest(x, 0.5), floor(x) FROM u WHERE id IN (0, 1, 4) ORDER BY id")
            .map(|(_, rows)| rows),
        Ok(vec![
            vec![json!(0), json!("NaN"), json!("NaN"), json!("NaN"), json!("NaN")],
            vec![json!(1), json!("inf"), json!("0"), json!("inf"), json!("inf")],
            vec![json!(4), json!("-0"), json!("1"), json!("0.5"), json!("-0")],
        ])
    );
    // Remainder: exact DECIMAL, fmod for REAL, INTEGER unchanged.
    assert_eq!(
        cli("SELECT d % 1, f % 2, i % 3, mod(d, 0.5), -f % 2 FROM n WHERE id = 1"),
        Ok((
            ["decimal", "float", "int", "decimal", "float"]
                .map(|t| json!(t))
                .to_vec(),
            vec![vec![
                json!("0.55"),
                json!("0.5"),
                json!(1),
                json!("0.05"),
                json!("-0.5")
            ]]
        ))
    );
    assert_eq!(
        cli("SELECT exp(f) FROM n WHERE id = 6"),
        Err("floating arithmetic overflow".to_string())
    );
    assert_eq!(
        cli("SELECT power(x, 0.5) FROM u WHERE id = 6"),
        Err("unsupported: POWER of a negative number to a non-integer exponent (DuckDB returns NaN, PostgreSQL raises an error)".to_string())
    );
}
