//! RIGHT/FULL joins, USING/NATURAL, INTERSECT ALL/EXCEPT ALL and compound
//! ORDER BY/LIMIT against independent DuckDB 1.5.6 goldens
//! (tools/testing/joins_sets_reference.py) and a brute-force Rust oracle.
//! No Assay output feeds any expected value.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const SETUP: &str = include_str!("fixtures/joins_sets/setup.sql");
const CASES: &str = include_str!("fixtures/joins_sets/cases.sql");
#[path = "support/external_sf1.rs"]
mod external_sf1;
static ANALYSIS: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| external_sf1::read("joins_sets/analysis.sql"));
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-joins-sets-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(path.join("spill")).unwrap();
        Self(path)
    }
    fn options(&self, workers: usize, batch_size: usize, optimized: bool, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            memory_bytes: 256 << 20,
            spill: spill.then(|| SpillOptions {
                directory: self.0.join("spill"),
                sort_run_bytes: 4 << 10,
                merge_fan_in: 2,
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn session(&self, options: Options) -> Session {
        let mut session = Session::new(
            &self.0,
            options,
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
    fn assert_no_spill_files(&self) {
        assert_eq!(fs::read_dir(self.0.join("spill")).unwrap().count(), 0);
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
        return serde_json::from_str(&external_sf1::read("joins_sets/analysis_expected.json"))
            .unwrap();
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/joins_sets")
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
                        DataType::Boolean => json!(
                            a.as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Float64 => {
                            let value = a
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row);
                            json!({"float_bits": hex(value.to_be_bytes())})
                        }
                        DataType::Date32 => json!(
                            arrow::util::display::array_value_to_string(a.as_ref(), row).unwrap()
                        ),
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

fn hex(bytes: [u8; 8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Logical type: DuckDB widens COUNT/SUM of integers to HUGEINT where Assay's
/// checked INTEGER is BIGINT, and DECIMAL precision follows each engine's
/// own result-precision rule; names, scales and values stay exact.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Int64 => "BIGINT".into(),
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Utf8 => "VARCHAR".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(_, s) => format!("DECIMAL(scale {s})"),
        other => panic!("unexpected output type {other}"),
    }
}
fn reference_type(name: &str) -> String {
    match name {
        "HUGEINT" | "INTEGER" => "BIGINT".into(),
        other if other.starts_with("DECIMAL(") => {
            let scale = other.trim_end_matches(')').rsplit(',').next().unwrap();
            format!("DECIMAL(scale {scale})")
        }
        other => other.into(),
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
    // Unaliased qualified columns keep their SQL spelling (`a.cust`) in Assay;
    // `assay_columns` pins that existing display-only difference.
    let columns = case.get("assay_columns").unwrap_or(&case["columns"]);
    assert_eq!(json!(names), *columns, "{context}");
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|f| type_name(f.data_type()))
        .collect::<Vec<_>>();
    let wanted = case["types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| reference_type(t.as_str().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(types, wanted, "{context}");
    let mut actual = rows(&batch);
    let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    if !case["ordered"].as_bool().unwrap() {
        actual.sort_by_key(|r| serde_json::to_string(r).unwrap());
        wanted.sort_by_key(|r| serde_json::to_string(r).unwrap());
    }
    assert_eq!(actual, wanted, "{context}");
}

#[test]
fn independent_duckdb_cases_match_at_every_worker_batch_optimizer_and_spill_setting() {
    let expected = expected("expected.json");
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["unexpected_reference_outcomes"], json!([]));
    assert_eq!(expected["setup_sql"], SETUP);
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 84);
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
    // Nested set operations recurse once per plan node during execution; an
    // unoptimized test build's frames exceed the 2 MiB test-thread stack for
    // a three-branch EXCEPT ALL (and, before this change, for a four-branch
    // EXCEPT). Run the matrix on a thread with an ordinary 8 MiB main stack.
    let cases = cases.clone();
    std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(move || run_cases(&cases))
        .unwrap()
        .join()
        .unwrap();
}

fn run_cases(cases: &[Value]) {
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for (optimized, spill) in [(false, false), (true, false), (true, true), (false, true)] {
                let mut session =
                    fixture.session(fixture.options(workers, batch_size, optimized, spill));
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let context = format!(
                        "{} workers={workers} batch={batch_size} optimized={optimized} spill={spill}: {sql}",
                        case["id"]
                    );
                    check_case(&mut session, case, sql, &context);
                }
                fixture.assert_no_spill_files();
            }
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
    let cases = expected["cases"].as_array().unwrap().clone();
    let sources = ANALYSIS.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    // Same 8 MiB stack as the reference corpus: scalar subqueries over
    // INTERSECT ALL nest deeper than the 2 MiB test thread in a debug build.
    std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(move || run_analysis(&directory, &sources, &cases))
        .unwrap()
        .join()
        .unwrap();
}

fn run_analysis(directory: &str, sources: &[&str], cases: &[Value]) {
    let fixture = Fixture::new();
    let only = std::env::var("ASSAY_TPCH_CASE").ok();
    for (source, case) in sources.iter().zip(cases) {
        if only.as_deref().is_some_and(|id| case["id"] != id) {
            continue;
        }
        let (_, sql) = source.split_once('\n').unwrap();
        assert_eq!(case["sql"], sql.trim());
        let sql = sql.trim().replace("{tpch}", &directory.replace('\'', "''"));
        eprintln!("analysis case {}", case["id"]);
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let mut options = fixture.options(workers, batch_size, true, false);
                options.memory_bytes = 4 << 30;
                let mut session = fixture.session(options);
                session.set_local_files_enabled(true);
                let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
                check_case(&mut session, case, &sql, &context);
            }
        }
    }
}

/// Previously refused forms now answer like DuckDB (old assertions in
/// tests/engine.rs and tests/dead_arm_types.rs); see SEMANTICS-LOG.md.
#[test]
fn formerly_refused_forms_return_duckdb_answers() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let fixture = Fixture::new();
            let mut s = Session::new(
                &fixture.0,
                fixture.options(workers, 1, optimized, false),
                Limits::default(),
            )
            .unwrap();
            assert_eq!(
                rows(&query(&mut s, "SELECT 1 INTERSECT ALL SELECT 1")),
                vec![vec![json!(1)]]
            );
            assert_eq!(query(&mut s, "SELECT 1 EXCEPT ALL SELECT 1").num_rows(), 0);
            s.execute_sql("CREATE TABLE t(id INTEGER NOT NULL,v INTEGER)")
                .unwrap();
            s.execute_sql("INSERT INTO t VALUES(9007199254740993,NULL),(2,7)")
                .unwrap();
            for (join, wanted) in [
                ("RIGHT", vec![3_i64, 9007199254740993]),
                ("FULL", vec![2_i64, 3, 9007199254740993]),
            ] {
                let sql = format!(
                    "SELECT COALESCE(a.id,3) AS v FROM t a {join} JOIN t b ON a.id=b.id AND a.id>2 ORDER BY 1"
                );
                assert_eq!(
                    rows(&query(&mut s, &sql)),
                    wanted
                        .into_iter()
                        .map(|v| vec![json!(v)])
                        .collect::<Vec<_>>(),
                    "{sql}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FULL/RIGHT over Parquet at workers x batch x forced spill, checked against a
// brute-force nested-loop oracle written independently of the engine.

type Row = (i64, Option<i64>, Option<i64>, Option<String>);
type Matches = Box<dyn Fn(&Row, &Row) -> Option<bool>>;

struct Tables {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
    a: Vec<Row>,
    b: Vec<Row>,
}
impl Tables {
    fn new(left: usize, right: usize, domain: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "assay-outer-join-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("spill")).unwrap();
        let generate = |count: usize, salt: usize| -> Vec<Row> {
            (0..count)
                .map(|i| {
                    (
                        i as i64,
                        (!(i + salt).is_multiple_of(17))
                            .then_some(((i * 7 + salt) % domain) as i64),
                        (!(i + salt).is_multiple_of(13))
                            .then_some(((i * 5 + salt) % 79) as i64 - 37),
                        (!(i + salt).is_multiple_of(11))
                            .then(|| format!("{:03}:é{}", (i + salt) % 23, "w".repeat(i % 5))),
                    )
                })
                .collect()
        };
        let a = generate(left, 0);
        let b = generate(right, 3);
        let mut tables = HashMap::new();
        for (name, data) in [("a", &a), ("b", &b)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ]));
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(data.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter(data.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter(data.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter(data.iter().map(|r| r.3.clone()))),
            ];
            let path = root.join(format!("{name}.parquet"));
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(128)
                        .build(),
                ),
            )
            .unwrap();
            writer
                .write(&RecordBatch::try_new(schema, arrays).unwrap())
                .unwrap();
            writer.close().unwrap();
            tables.insert(name.into(), path);
        }
        Self { root, tables, a, b }
    }
    fn options(&self, workers: usize, batch_size: usize, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: if spill { 8 << 20 } else { 256 << 20 },
            parquet_cache_bytes: None,
            spill: spill.then(|| SpillOptions {
                directory: self.root.join("spill"),
                disk_quota_bytes: 256 << 20,
                sort_run_bytes: 64 << 10,
                merge_fan_in: 2,
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn run(
        &self,
        sql: &str,
        options: &Options,
    ) -> (Vec<Vec<Option<i64>>>, assaydb::execute::Metrics) {
        let result = execute(&prepare(sql, &self.tables, options).unwrap(), options)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            result.metrics.peak_reserved_bytes <= options.memory_bytes,
            "{sql}: peak {} > budget {}",
            result.metrics.peak_reserved_bytes,
            options.memory_bytes
        );
        assert_eq!(result.metrics.spill.live_bytes, 0);
        assert_eq!(result.metrics.spill.live_files, 0);
        assert_eq!(fs::read_dir(self.root.join("spill")).unwrap().count(), 0);
        let batch = result.batch;
        let mut out = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        (!values.is_null(row)).then(|| values.value(row))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        out.sort();
        (out, result.metrics)
    }
}
impl Drop for Tables {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}

/// Nested-loop outer join: (left id, right id) pairs plus unmatched rows.
fn oracle(
    a: &[Row],
    b: &[Row],
    full: bool,
    matches: impl Fn(&Row, &Row) -> Option<bool>,
) -> Vec<Vec<Option<i64>>> {
    let mut out = Vec::new();
    let mut right_matched = vec![false; b.len()];
    for l in a {
        let mut matched = false;
        for (j, r) in b.iter().enumerate() {
            if matches(l, r) == Some(true) {
                matched = true;
                right_matched[j] = true;
                out.push(vec![Some(l.0), Some(r.0)]);
            }
        }
        if full && !matched {
            out.push(vec![Some(l.0), None]);
        }
    }
    for (j, r) in b.iter().enumerate() {
        if !right_matched[j] {
            out.push(vec![None, Some(r.0)]);
        }
    }
    out.sort();
    out
}
fn eq<T: PartialEq>(x: &Option<T>, y: &Option<T>) -> Option<bool> {
    Some(x.as_ref()? == y.as_ref()?)
}

#[test]
fn full_and_right_joins_match_oracle_at_every_worker_batch_and_spill_setting() {
    let t = Tables::new(1500, 1100, 97);
    let cases: Vec<(&str, bool, Matches)> = vec![
        (
            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b ON a.k = b.k",
            true,
            Box::new(|l, r| eq(&l.1, &r.1)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a RIGHT JOIN b ON a.k = b.k",
            false,
            Box::new(|l, r| eq(&l.1, &r.1)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b ON a.k = b.k AND a.v < b.v",
            true,
            Box::new(|l, r| Some(eq(&l.1, &r.1)? && l.2? < r.2?)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a RIGHT JOIN b ON a.k = b.k AND a.s < b.s",
            false,
            Box::new(|l, r| Some(eq(&l.1, &r.1)? && l.3.as_ref()? < r.3.as_ref()?)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b ON a.k = b.k AND a.s = b.s",
            true,
            Box::new(|l, r| Some(eq(&l.1, &r.1)? && eq(&l.3, &r.3)?)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b ON a.v - 70 > b.v",
            true,
            Box::new(|l, r| Some(l.2? - 70 > r.2?)),
        ),
        (
            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b USING (k)",
            true,
            Box::new(|l, r| eq(&l.1, &r.1)),
        ),
    ];
    let expected = cases
        .iter()
        .map(|(_, full, matches)| oracle(&t.a, &t.b, *full, matches))
        .collect::<Vec<_>>();
    // The oracle exercises duplicates, NULL keys and unmatched rows on both sides.
    assert!(expected[0].iter().any(|r| r[0].is_none()));
    assert!(expected[0].iter().any(|r| r[1].is_none()));
    assert!(expected[0].len() > 10_000);
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = t.options(workers, batch_size, spill);
                for ((sql, _, _), wanted) in cases.iter().zip(&expected) {
                    let context =
                        format!("{sql} workers={workers} batch={batch_size} spill={spill}");
                    let (actual, metrics) = t.run(sql, &options);
                    assert_eq!(actual.len(), wanted.len(), "{context}");
                    assert!(actual == *wanted, "{context}");
                    if spill {
                        assert!(metrics.external_join_partitions > 0, "{context}");
                    }
                }
                // Aggregates over the merged USING column (COALESCE of both keys).
                let sql = "SELECT k, COUNT(*) AS n, COUNT(a.id) AS na, COUNT(b.id) AS nb FROM a FULL JOIN b USING (k) GROUP BY k";
                let (actual, _) = t.run(sql, &options);
                let mut wanted: HashMap<Option<i64>, [i64; 3]> = HashMap::new();
                for row in &expected[0] {
                    let key = row[0]
                        .map(|l| t.a[l as usize].1)
                        .unwrap_or_else(|| t.b[row[1].unwrap() as usize].1);
                    let entry = wanted.entry(key).or_default();
                    entry[0] += 1;
                    entry[1] += i64::from(row[0].is_some());
                    entry[2] += i64::from(row[1].is_some());
                }
                let mut wanted = wanted
                    .into_iter()
                    .map(|(k, [n, na, nb])| vec![k, Some(n), Some(na), Some(nb)])
                    .collect::<Vec<_>>();
                wanted.sort();
                assert_eq!(
                    actual, wanted,
                    "{sql} workers={workers} batch={batch_size} spill={spill}"
                );
            }
        }
    }
}

#[test]
fn full_join_with_empty_sides_at_every_setting() {
    for (left, right) in [(0, 0), (9, 0), (0, 9)] {
        let t = Tables::new(left, right, 3);
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for spill in [false, true] {
                    let options = t.options(workers, batch_size, spill);
                    for (sql, full) in [
                        (
                            "SELECT a.id AS l, b.id AS r FROM a FULL JOIN b ON a.k = b.k",
                            true,
                        ),
                        (
                            "SELECT a.id AS l, b.id AS r FROM a RIGHT JOIN b ON a.k = b.k",
                            false,
                        ),
                    ] {
                        let (actual, _) = t.run(sql, &options);
                        assert_eq!(
                            actual,
                            oracle(&t.a, &t.b, full, |l, r| eq(&l.1, &r.1)),
                            "{sql} {left}x{right} workers={workers} batch={batch_size} spill={spill}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn intersect_and_except_all_match_multiset_oracle_in_memory_and_spill() {
    let t = Tables::new(700, 650, 11);
    let count = |rows: &[Row]| {
        let mut counts: HashMap<Option<i64>, i64> = HashMap::new();
        for row in rows {
            *counts.entry(row.1).or_default() += 1;
        }
        counts
    };
    let (ca, cb) = (count(&t.a), count(&t.b));
    let keys = ca
        .keys()
        .chain(cb.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let expand = |f: &dyn Fn(i64, i64) -> i64| {
        let mut out = Vec::new();
        for k in &keys {
            let n = f(*ca.get(k).unwrap_or(&0), *cb.get(k).unwrap_or(&0));
            out.extend((0..n).map(|_| vec![*k]));
        }
        out.sort();
        out
    };
    let intersect = expand(&|m, n| m.min(n));
    let except = expand(&|m, n| (m - n).max(0));
    assert!(intersect.iter().any(|r| r[0].is_none()));
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = t.options(workers, batch_size, spill);
                for (sql, wanted) in [
                    ("SELECT k FROM a INTERSECT ALL SELECT k FROM b", &intersect),
                    ("SELECT k FROM a EXCEPT ALL SELECT k FROM b", &except),
                ] {
                    let (actual, _) = t.run(sql, &options);
                    assert_eq!(
                        &actual, wanted,
                        "{sql} workers={workers} batch={batch_size} spill={spill}"
                    );
                }
            }
        }
    }
}
