//! Nonfinite Float arithmetic and overflowing Float SUM/AVG against pinned
//! DuckDB 1.5.6 / PostgreSQL 17 references (tests/fixtures/nonfinite_arith).
//! Every expected Assay outcome is derived in the generator from the two
//! references plus a named rule; none is taken from Assay.
use arrow::{
    array::{Array, Float64Array, Int64Array},
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

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/nonfinite_arith")
}

fn expected() -> Value {
    serde_json::from_str(include_str!("fixtures/nonfinite_arith/expected.json")).unwrap()
}

fn tables() -> HashMap<String, PathBuf> {
    HashMap::from([
        ("v".into(), fixture_dir().join("pairs.parquet")),
        ("u".into(), fixture_dir().join("values.parquet")),
        ("a".into(), fixture_dir().join("aggregate.parquet")),
    ])
}

struct Spill(PathBuf);
impl Spill {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-nonfinite-arith-{}-{}",
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
impl Drop for Spill {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn options(workers: usize, batch_size: usize, optimized: bool, spill: Option<&Spill>) -> Options {
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
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                all.push((workers, batch_size, optimized));
            }
        }
    }
    all
}

/// Reference JSON encoding: NaN of any sign/payload is "NaN"; infinities are
/// "+inf"/"-inf"; zeros are "0"/"-0" (sign is exact); finite values are numbers.
fn encode(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        let v = array.value(row);
        return if v.is_nan() {
            json!("NaN")
        } else if v.is_infinite() {
            json!(if v > 0.0 { "+inf" } else { "-inf" })
        } else if v == 0.0 {
            json!(if v.is_sign_negative() { "-0" } else { "0" })
        } else {
            json!(v)
        };
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return json!(array.value(row));
    }
    panic!("unexpected result type {:?}", array.data_type())
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

/// Bits of every Float cell, with NaNs collapsed (payload is not a SQL value).
fn bits(batch: &RecordBatch) -> Vec<Vec<Option<u64>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|c| {
                    if c.is_null(row) {
                        None
                    } else if let Some(f) = c.as_any().downcast_ref::<Float64Array>() {
                        let v = f.value(row);
                        Some(if v.is_nan() { u64::MAX } else { v.to_bits() })
                    } else {
                        Some(c.as_any().downcast_ref::<Int64Array>().unwrap().value(row) as u64)
                    }
                })
                .collect()
        })
        .collect()
}

/// Finite reference floats are shortest round-trip strings ("1e+308"), parsed
/// here with Rust's correctly rounded parser; integers are JSON numbers. The
/// labels NaN, +inf, -inf, 0 and -0 compare as labels (zero sign is exact).
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if !matches!(s.as_str(), "NaN" | "+inf" | "-inf" | "0" | "-0") => {
            Some(s.parse().expect("finite reference float"))
        }
        _ => None,
    }
}

/// Arithmetic is exact IEEE binary64 in every engine, so finite values compare
/// exactly; Int/Float spellings (2 vs 2.0) compare by value.
fn same(actual: &Value, wanted: &Value) -> bool {
    match (number(actual), number(wanted)) {
        (Some(a), Some(b)) => a == b,
        _ => actual == wanted,
    }
}

fn run(sql: &str, options: &Options) -> Result<RecordBatch, String> {
    prepare(sql, &tables(), options)
        .and_then(|plan| execute(&plan, options))
        .map(|r| r.batch)
        .map_err(|e| e.as_str().to_string())
}

/// One bulk query per expression over all reference-successful rows (so batch
/// and worker boundaries split NaN/infinite rows) plus one single-row query
/// per expected error, in every setting.
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
    let excluded = errors
        .iter()
        .map(|c| c["id"].to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let filter = if excluded.is_empty() {
        String::new()
    } else {
        format!(" WHERE id NOT IN ({excluded})")
    };
    let bulk = format!("SELECT id, {expression} FROM {table}{filter} ORDER BY id");
    let mut first_bits = None;
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        let context = format!("{bulk} workers={workers} batch={batch_size} optimized={optimized}");
        let batch = run(&bulk, &options).unwrap_or_else(|e| panic!("{context}: {e}"));
        let actual = rows(&batch);
        assert_eq!(actual.len(), ok.len(), "{context}");
        for (row, cell) in actual.iter().zip(&ok) {
            assert_eq!(row[0], cell["id"], "{context}");
            assert!(
                same(&row[1], &cell["assay"]["value"]),
                "{context}: id {} ({}): got {}, want {} [{}]",
                cell["id"],
                cell,
                row[1],
                cell["assay"]["value"],
                cell["rule"]
            );
        }
        let b = bits(&batch);
        match &first_bits {
            Some(first) => assert_eq!(&b, first, "{context}"),
            None => first_bits = Some(b),
        }
        for cell in &errors {
            let sql = format!("SELECT {expression} FROM {table} WHERE id = {}", cell["id"]);
            assert_eq!(
                run(&sql, &options).err().as_deref(),
                cell["assay"]["error"].as_str(),
                "{sql} workers={workers} batch={batch_size} optimized={optimized} [{}]",
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
    assert_eq!(reference["values"].as_array().unwrap().len(), 12);
    assert_eq!(reference["binary"].as_array().unwrap().len(), 12 * 12 * 4);
    assert_eq!(reference["unary"].as_array().unwrap().len(), 12 * 16);
    assert_eq!(reference["aggregates"].as_array().unwrap().len(), 8);
    let mut agreeing = 0;
    for cell in reference["binary"]
        .as_array()
        .unwrap()
        .iter()
        .chain(reference["unary"].as_array().unwrap())
        .chain(reference["aggregates"].as_array().unwrap())
    {
        assert!(cell["rule"].as_str().is_some_and(|r| !r.is_empty()));
        if cell["rule"] == "references agree" {
            agreeing += 1;
            assert_eq!(cell["duckdb"]["status"], "ok");
            assert_eq!(cell["postgresql"]["status"], "ok");
        }
    }
    assert_eq!(agreeing, 680);
}

#[test]
fn binary_operators_over_nonfinite_parquet_values() {
    let reference = expected();
    for op in ["+", "-", "*", "/"] {
        let cells: Vec<&Value> = reference["binary"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["op"] == op)
            .collect();
        assert_eq!(cells.len(), 144);
        check_cells("v", &cells, &format!("x {op} y"));
    }
}

#[test]
fn unary_literal_and_function_expressions_over_nonfinite_values() {
    let reference = expected();
    let mut expressions: Vec<&str> = Vec::new();
    for cell in reference["unary"].as_array().unwrap() {
        let expr = cell["expr"].as_str().unwrap();
        if !expressions.contains(&expr) {
            expressions.push(expr);
        }
    }
    assert_eq!(expressions.len(), 16);
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
fn float_sum_avg_overflow_and_nan_across_workers_batches_and_spill() {
    let reference = expected();
    let spill_dir = Spill::new();
    for case in reference["aggregates"].as_array().unwrap() {
        let sql = case["sql"].as_str().unwrap();
        let mut first_bits = None;
        for (workers, batch_size, optimized) in settings() {
            for spill in [false, true] {
                let options = options(workers, batch_size, optimized, spill.then_some(&spill_dir));
                let context = format!(
                    "{sql} workers={workers} batch={batch_size} optimized={optimized} spill={spill} [{}]",
                    case["rule"]
                );
                let result = prepare(sql, &tables(), &options).and_then(|p| execute(&p, &options));
                if case["assay"]["status"] == "error" {
                    assert_eq!(
                        result.err().as_ref().map(assaydb::Error::as_str),
                        case["assay"]["error"].as_str(),
                        "{context}"
                    );
                    spill_dir.clean();
                    continue;
                }
                let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                let actual = rows(&result.batch);
                let wanted = case["assay"]["rows"].as_array().unwrap();
                assert_eq!(actual.len(), wanted.len(), "{context}: {actual:?}");
                for (a, w) in actual.iter().zip(wanted) {
                    let w = w.as_array().unwrap();
                    assert_eq!(a.len(), w.len(), "{context}");
                    for (a, w) in a.iter().zip(w) {
                        // Sums of finite non-overflowing values stay within the
                        // approved 1e-12 relative Float aggregate tolerance.
                        let ok = match (number(a), number(w)) {
                            (Some(a), Some(w)) => (a - w).abs() <= 1e-12_f64.max(w.abs() * 1e-12),
                            _ => a == w,
                        };
                        assert!(ok, "{context}: {actual:?} != {wanted:?}");
                    }
                }
                let b = bits(&result.batch);
                match &first_bits {
                    Some(first) => assert_eq!(&b, first, "{context}"),
                    None => first_bits = Some(b),
                }
                if spill && sql.contains("GROUP BY") {
                    assert!(
                        result.metrics.external_aggregate_partitions > 0,
                        "{context}: spill must be exercised"
                    );
                }
                spill_dir.clean();
            }
        }
    }
}

fn cli(sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let spill = Spill::new();
    let output = spill.0.join("out.json");
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
    Ok(serde_json::from_value(value["rows"].clone()).unwrap())
}

#[test]
fn cli_over_parquet_reports_nan_infinity_and_overflow() {
    // Row ids in pairs.parquet are 12 * index(x) + index(y); values order:
    // NaN, -NaN, +inf, -inf, 0, -0, 1.5, -2, 1e308, -1e308, 1e-308, NULL.
    let nan_plus_inf = 2;
    let inf_minus_inf = 2 * 12 + 3;
    assert_eq!(
        cli(&format!(
            "SELECT x + y, x * y, x / y, x - 1 FROM v WHERE id = {nan_plus_inf}"
        )),
        Ok(vec![vec![
            json!("NaN"),
            json!("NaN"),
            json!("NaN"),
            json!("NaN")
        ]])
    );
    assert_eq!(
        cli(&format!(
            "SELECT x + y, x - y, x * 2, 1 / x FROM v WHERE id = {inf_minus_inf}"
        )),
        Ok(vec![vec![
            json!("NaN"),
            json!("inf"),
            json!("inf"),
            json!("0")
        ]])
    );
    assert_eq!(
        cli("SELECT g, SUM(x), AVG(x) FROM a WHERE g IN (1, 2, 4, 6) GROUP BY g ORDER BY g"),
        Ok(vec![
            vec![json!(1), json!("inf"), json!("inf")],
            vec![json!(2), json!("-inf"), json!("-inf")],
            vec![json!(4), json!("NaN"), json!("NaN")],
            vec![json!(6), json!("4"), json!("2")],
        ])
    );
    // Finite overflow keeps its error (PostgreSQL raises; DuckDB returns inf).
    assert_eq!(
        cli("SELECT x * 10 FROM u WHERE id = 8"),
        Err("floating arithmetic overflow".to_string())
    );
}

/// The planner folds a quotient with a constant zero divisor to NULL. NaN / 0
/// is NaN (decision 2026-10-17), so only a Float dividend loses that fold:
/// `COALESCE(x / 0, 7)` is DOUBLE 7.0 (as in DuckDB 1.5.6), while Int dividends
/// keep the fold and their INTEGER result.
#[test]
fn zero_divisor_fold_typing() {
    for (sql, float_result) in [
        ("SELECT COALESCE(x / 0, 7) FROM u WHERE id = 6", true),
        ("SELECT COALESCE(x / 0, 7) FROM u WHERE id = 0", true),
        ("SELECT COALESCE(id / 0, 7) FROM u WHERE id = 6", false),
        (
            "SELECT COALESCE(id / CAST(0 AS REAL), 7) FROM u WHERE id = 6",
            false,
        ),
        (
            "SELECT CASE WHEN id / CAST(0 AS REAL) IS NULL THEN 7 ELSE 2.5 END FROM u WHERE id = 6",
            false,
        ),
    ] {
        for (workers, batch_size, optimized) in settings() {
            let batch = run(sql, &options(workers, batch_size, optimized, None))
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            let column = batch.column(0);
            let value = encode(column.as_ref(), 0);
            if float_result {
                assert!(column.as_any().is::<Float64Array>(), "{sql}");
                let want = if sql.ends_with("id = 0") {
                    json!("NaN")
                } else {
                    json!(7.0)
                };
                assert_eq!(value, want, "{sql}");
            } else {
                assert!(
                    !column.as_any().is::<Float64Array>(),
                    "{sql}: {:?}",
                    column.data_type()
                );
                assert_eq!(value, json!(7), "{sql}");
            }
        }
    }
}

/// Alignment after integration candidate 2: `NaN % 0` is NaN, as `NaN / 0`
/// (decision 2026-10-17), for REAL and DECIMAL zeros and for MOD. Both
/// references agree: DuckDB 1.5.6 returns NaN for `x % 0`, `x % CAST(0 AS
/// DOUBLE)`, `x % 0.0` and `mod(x, 0)` over these NaN rows, and PostgreSQL 17
/// numeric returns NaN for `'NaN' % 0`, `mod('NaN', 0.0)`. Nonfinite dividends
/// with a nonzero divisor are NaN in both (REAL fmod; PostgreSQL numeric mod).
/// A non-NaN dividend keeps the approved zero-divisor rules (NULL / error).
#[test]
fn nan_remainder_by_zero_is_nan() {
    for (workers, batch_size, optimized) in settings() {
        let options = options(workers, batch_size, optimized, None);
        let sql = "SELECT x % 0, x % CAST(0 AS REAL), x % 0.0, MOD(x, 0), x % 2, MOD(x, -3.5) FROM u WHERE id IN (0, 1) ORDER BY id";
        let batch = run(sql, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows(&batch), vec![vec![json!("NaN"); 6]; 2], "{sql}");
        let sql = "SELECT x % 2, MOD(x, -3.5) FROM u WHERE id IN (2, 3) ORDER BY id";
        let batch = run(sql, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows(&batch), vec![vec![json!("NaN"); 2]; 2], "{sql}");
        // Zero-divisor folding keeps a NaN dividend reachable (DOUBLE result).
        for (sql, want) in [
            (
                "SELECT COALESCE(x % 0, 7) FROM u WHERE id = 0",
                json!("NaN"),
            ),
            ("SELECT COALESCE(x % 0, 7) FROM u WHERE id = 6", json!(7.0)),
        ] {
            let batch = run(sql, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(batch.column(0).as_any().is::<Float64Array>(), "{sql}");
            assert_eq!(encode(batch.column(0).as_ref(), 0), want, "{sql}");
        }
        // Non-NaN dividends keep the approved rules: REAL zero -> NULL,
        // DECIMAL zero -> division by zero.
        let sql = "SELECT x % 0 FROM u WHERE id = 6";
        let batch = run(sql, &options).unwrap();
        assert_eq!(encode(batch.column(0).as_ref(), 0), Value::Null, "{sql}");
        let error = run("SELECT x % 0.0 FROM u WHERE id = 6", &options).unwrap_err();
        assert_eq!(error, "division by zero");
    }
}
