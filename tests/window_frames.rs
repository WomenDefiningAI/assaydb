//! Exact reference goldens plus handwritten frame, error and type boundaries.
#[path = "support/external_sf1.rs"]
mod external_sf1;
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
    types::decimal_text,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-window-frames-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn session(&self, workers: usize, batch_size: usize, spill: bool) -> Session {
        Session::new(
            &self.0,
            Options {
                workers,
                batch_size,
                memory_bytes: 64 << 20,
                spill: spill.then(|| SpillOptions {
                    directory: self.0.clone(),
                    sort_run_bytes: 512,
                    merge_fan_in: 2,
                    ..Default::default()
                }),
                ..Default::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("expected query")
    };
    execution.batch.clone()
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows()).map(|row| batch.columns().iter().map(|a| {
        if a.is_null(row) || a.data_type() == &DataType::Null { return Value::Null; }
        match a.data_type() {
            DataType::Int64 => json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
            DataType::Decimal128(_, scale) => {
                let text = decimal_text(a.as_any().downcast_ref::<Decimal128Array>().unwrap().value(row), *scale);
                json!({"decimal": if text.contains('.') { text.trim_end_matches('0').trim_end_matches('.') } else { &text }})
            }
            DataType::Float64 => json!({"float_bits": format!("{:016x}", a.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_bits())}),
            DataType::Date32 => json!({"date": (chrono::NaiveDate::from_ymd_opt(1970,1,1).unwrap() + chrono::Duration::days(i64::from(a.as_any().downcast_ref::<Date32Array>().unwrap().value(row)))).to_string()}),
            DataType::Utf8 => json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(row)),
            DataType::Boolean => json!(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(row)),
            other => panic!("unexpected window output {other}"),
        }
    }).collect()).collect()
}

fn same(actual: &Value, expected: &Value) -> bool {
    if let (Some(a), Some(b)) = (
        actual.get("float_bits").and_then(Value::as_str),
        expected.get("float_bits").and_then(Value::as_str),
    ) {
        let a = f64::from_bits(u64::from_str_radix(a, 16).unwrap());
        let b = f64::from_bits(u64::from_str_radix(b, 16).unwrap());
        return a == b
            || (a.is_nan() && b.is_nan())
            || (a - b).abs() <= 1e-12
            || (a - b).abs() <= 1e-12 * a.abs().max(b.abs());
    }
    actual == expected
}

#[test]
fn frozen_duckdb_frames_workers_batches_and_spill() {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/window_frames/expected.json")).unwrap();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let siblings: Value = serde_json::from_str(include_str!(
        "fixtures/window_frames/siblings_expected.json"
    ))
    .unwrap();
    let ties: Value =
        serde_json::from_str(include_str!("fixtures/window_frames/ties_reference.json")).unwrap();
    assert_eq!(siblings["duckdb_version"], "1.5.6");
    assert_eq!(ties["duckdb_version"], "1.5.6");
    assert_eq!(reference["cases"].as_array().unwrap().len(), 80);
    assert_eq!(siblings["cases"].as_array().unwrap().len(), 3);
    assert_eq!(ties["cases"].as_array().unwrap().len(), 19);
    let cases = reference["cases"]
        .as_array()
        .unwrap()
        .iter()
        .chain(siblings["cases"].as_array().unwrap())
        .collect::<Vec<_>>();
    let sources = [
        include_str!("fixtures/window_frames/cases.sql"),
        include_str!("fixtures/window_frames/siblings.sql"),
    ]
    .join("\n");
    let sources = sources.split("-- case: ").skip(1).collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(&cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata["id"], case["id"]);
        assert_eq!(sql.trim(), case["sql"]);
    }
    let fixture = Fixture::new();
    let mut canonical = vec![None; cases.len()];
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch, spill);
                for (index, case) in cases.iter().enumerate() {
                    let sql = case["sql"].as_str().unwrap();
                    if let Some(error) = case["assay_error"].as_str() {
                        let actual = session.execute_sql(sql).err().unwrap();
                        assert!(actual.message.contains(error), "{sql}: {actual}");
                        // wf080's frame total 2^63 fits precision 38 but not the
                        // public BIGINT result: the same engine-class error and
                        // message as every other BIGINT output overflow.
                        assert_eq!(case["id"], "wf080");
                        assert_eq!(
                            (actual.kind, actual.message.as_str()),
                            ("engine", "BIGINT overflow"),
                            "{sql}"
                        );
                        continue;
                    }
                    let actual = rows(&query(&mut session, sql));
                    let modeled = ties["cases"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|tie| tie["id"] == case["id"]);
                    let wanted = if let Some(tie) = modeled {
                        assert_eq!(tie["sql"], case["sql"]);
                        assert_eq!(tie["duckdb_rows"], case["rows"]);
                        let permitted = tie["permitted_rows"].as_array().unwrap();
                        assert!(permitted.contains(&case["rows"]));
                        assert!(
                            permitted.contains(&json!(actual)),
                            "{}: result outside complete SQL-permitted set",
                            case["id"]
                        );
                        assert_eq!(
                            json!(actual),
                            tie["canonical_rows"],
                            "{}: documented canonical input order",
                            case["id"]
                        );
                        if permitted.len() == 1 {
                            assert_eq!(tie["canonical_rows"], case["rows"]);
                        } else {
                            assert_eq!(tie["classification"], "order-dependent per SQL (ties)");
                        }
                        &tie["canonical_rows"]
                    } else {
                        &case["rows"]
                    };
                    let wanted: Vec<Vec<Value>> = serde_json::from_value(wanted.clone()).unwrap();
                    assert_eq!(
                        actual.len(),
                        wanted.len(),
                        "{} workers={workers} batch={batch} spill={spill}",
                        case["id"]
                    );
                    for (a, b) in actual.iter().zip(&wanted) {
                        assert_eq!(a.len(), b.len());
                        for (a, b) in a.iter().zip(b) {
                            assert!(
                                same(a, b),
                                "{} workers={workers} batch={batch} spill={spill}: {a} != {b}",
                                case["id"]
                            );
                        }
                    }
                    if workers == 1 && batch == 1 && !spill {
                        canonical[index] = Some(actual);
                    } else {
                        assert_eq!(
                            Some(&actual),
                            canonical[index].as_ref(),
                            "deterministic bits: {}",
                            case["id"]
                        );
                    }
                }
            }
        }
    }
}

/// Handwritten controls whose SQL answer is peer-order dependent. The complete
/// permitted set and canonical answer come from the independent model in
/// `window_frames_reference.py --handwritten`; DuckDB's answer is recorded.
fn handwritten_tie(id: &str) -> Value {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/window_frames/handwritten_ties.json")).unwrap();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    reference["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == id)
        .unwrap_or_else(|| panic!("missing handwritten tie {id}"))
        .clone()
}

fn assert_tie_answer(case: &Value, sql: &str, actual: &[Vec<Value>]) {
    assert_eq!(case["sql"], sql);
    assert_eq!(case["compare"], "ordered");
    let permitted = case["permitted_rows"].as_array().unwrap();
    assert!(
        permitted.len() > 1,
        "{}: control is order-dependent",
        case["id"]
    );
    assert!(permitted.contains(&case["canonical_rows"]));
    assert!(permitted.contains(&case["duckdb_rows"]));
    assert!(
        permitted.contains(&json!(actual)),
        "{}: result outside complete SQL-permitted set",
        case["id"]
    );
    assert_eq!(
        json!(actual),
        case["canonical_rows"],
        "{}: documented canonical input order",
        case["id"]
    );
}

#[test]
fn handwritten_peer_end_nulls_and_float_fold_bits() {
    let fixture = Fixture::new();
    let values = handwritten_tie("hw-frames-value-peers");
    let fold = handwritten_tie("hw-frames-float-fold-order");
    let setup: Value =
        serde_json::from_str(include_str!("fixtures/window_frames/handwritten_ties.json")).unwrap();
    let setup = setup["frames_setup"].as_array().unwrap().clone();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch, spill);
                let create = "CREATE TABLE t(id INTEGER,k INTEGER,v INTEGER,f DOUBLE)";
                let insert =
                    "INSERT INTO t VALUES(1,1,2,1e16),(2,1,NULL,1),(3,3,4,-1e16),(4,NULL,8,1)";
                assert_eq!(setup, vec![json!(create), json!(insert)]);
                session.execute_sql(create).unwrap();
                session.execute_sql(insert).unwrap();
                let sql = "SELECT id,LAST_VALUE(v) OVER(ORDER BY k),FIRST_VALUE(v) OVER(ORDER BY k ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),NTH_VALUE(v,2) OVER(ORDER BY k) FROM t ORDER BY id";
                let actual = rows(&query(&mut session, sql));
                // LAST/NTH_VALUE see the k=1 peers (ids 1, 2) in either order:
                // the permitted set also holds the ids-2,1 answer (2 throughout).
                assert_tie_answer(&values, sql, &actual);
                assert_eq!(
                    actual,
                    vec![
                        vec![json!(1), Value::Null, json!(2), Value::Null],
                        vec![json!(2), Value::Null, Value::Null, Value::Null],
                        vec![json!(3), json!(4), json!(4), Value::Null],
                        vec![json!(4), json!(8), json!(8), Value::Null],
                    ]
                );
                let sql = "SELECT SUM(f) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING),SUM(f) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),AVG(f) OVER() FROM t ORDER BY id";
                let result = query(&mut session, sql);
                // AVG(f) OVER() folds four peers: 0.25 canonically, 0 or 0.5 in
                // other peer orders; the recorded set holds all three.
                assert_tie_answer(&fold, sql, &rows(&result));
                let columns = result
                    .columns()
                    .iter()
                    .map(|a| a.as_any().downcast_ref::<Float64Array>().unwrap())
                    .collect::<Vec<_>>();
                for (column, expected) in columns.iter().zip([
                    [1e16, 1e16, 0.0, 1.0],
                    [1e16, 1e16, 0.0, -1e16],
                    [0.25; 4],
                ]) {
                    assert_eq!(
                        column
                            .iter()
                            .map(|x| x.unwrap().to_bits())
                            .collect::<Vec<_>>(),
                        expected.map(f64::to_bits)
                    );
                }
                assert_eq!(
                    rows(&query(
                        &mut session,
                        "SELECT id,NTILE(3) OVER(ORDER BY id) FROM t QUALIFY id>1 ORDER BY id"
                    )),
                    vec![
                        vec![json!(2), json!(1)],
                        vec![json!(3), json!(2)],
                        vec![json!(4), json!(3)]
                    ]
                );
            }
        }
    }
}

#[test]
fn refused_frames_modifiers_and_completion_errors() {
    let fixture = Fixture::new();
    for spill in [false, true] {
        let mut session = fixture.session(4, 1, spill);
        let boundaries: Value = serde_json::from_str(include_str!(
            "fixtures/window_frames/boundary_reference.json"
        ))
        .unwrap();
        assert_eq!(boundaries["duckdb_version"], "1.5.6");
        assert_eq!(boundaries["controls"].as_array().unwrap().len(), 4);
        for case in boundaries["controls"].as_array().unwrap() {
            let error = session
                .execute_sql(case["sql"].as_str().unwrap())
                .err()
                .unwrap();
            assert_eq!(
                error.message,
                "unsupported: RANGE boundary outside common reference domain"
            );
        }
        let ambiguous = session.execute_sql("SELECT a.id AS id,ROW_NUMBER() OVER() AS r FROM (SELECT 1 AS id) a CROSS JOIN (SELECT 2 AS id) b QUALIFY id=1").err().unwrap();
        assert!(
            ambiguous.message.to_lowercase().contains("ambiguous"),
            "{ambiguous}"
        );
        for sql in [
            "SELECT 1 qualify",
            "SELECT 1 AS qualify",
            "SELECT 1 qualify FROM (SELECT 1) t",
            "SELECT 1 qualify ORDER BY 1",
        ] {
            assert_eq!(rows(&query(&mut session, sql)), vec![vec![json!(1)]]);
        }
        // October 18 window extras (old -> new): GROUPS, EXCLUDE, REAL and
        // DECIMAL RANGE offsets, the decided DESC NULLS LAST default and
        // named windows answer; DuckDB 1.5.6 and PostgreSQL 17 agree on each.
        for (sql, expected) in [
            (
                "SELECT SUM(1) OVER(ORDER BY 1 GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)",
                json!(1),
            ),
            (
                "SELECT SUM(1) OVER(ORDER BY 1 ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)",
                Value::Null,
            ),
            (
                "SELECT SUM(1) OVER(ORDER BY CAST(1 AS DOUBLE) RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
                json!(1),
            ),
            (
                "SELECT SUM(1) OVER(ORDER BY CAST(1 AS DECIMAL(9,2)) RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
                json!(1),
            ),
            ("SELECT LAST_VALUE(1) OVER(ORDER BY 1 DESC)", json!(1)),
            ("SELECT FIRST_VALUE(1) OVER w WINDOW w AS ()", json!(1)),
        ] {
            assert_eq!(
                rows(&query(&mut session, sql)),
                vec![vec![expected]],
                "{sql}"
            );
        }
        for sql in [
            "SELECT FIRST_VALUE(1) IGNORE NULLS OVER()",
            "SELECT NTH_VALUE(1,0) OVER()",
            "SELECT NTILE(0) OVER()",
            "SELECT NTILE(1+1) OVER()",
            "SELECT SUM(v) OVER(ORDER BY id ROWS BETWEEN id PRECEDING AND CURRENT ROW) FROM (SELECT 1 AS id,1 AS v) t",
            "SELECT SUM(1) OVER(ORDER BY 'x' RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            "SELECT SUM(1) OVER(ORDER BY DATE '2024-01-01' RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            "SELECT SUM(1) OVER(ORDER BY DATE '2024-01-01' RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW)",
            "SELECT SUM(1) OVER(ORDER BY 1,2 RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            "SELECT SUM(1) OVER(ROWS BETWEEN -1 PRECEDING AND CURRENT ROW)",
            "SELECT 1 QUALIFY TRUE",
        ] {
            let error = session
                .execute_sql(sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            assert_eq!(error.kind, "unsupported", "{sql}: {error}");
        }
        let input = "(SELECT 1 AS id,CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS v UNION ALL SELECT 2,CAST(1 AS DECIMAL(38,0))) t";
        for tail in ["", " LIMIT 1", " QUALIFY id=1"] {
            let sql = format!(
                "SELECT id,SUM(v) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {input}{tail}"
            );
            // Exact precision-38 completion error, as for grouped SUM.
            let error = session.execute_sql(&sql).err().unwrap();
            assert_eq!(
                (error.kind, error.message.as_str()),
                ("engine", "decimal precision 38 overflow"),
                "{sql}"
            );
        }
        session
            .execute_sql("CREATE TABLE finite_overflow(id INTEGER,v DOUBLE)")
            .unwrap();
        session
            .execute_sql("INSERT INTO finite_overflow VALUES(1,1e308),(2,1e308)")
            .unwrap();
        for f in ["SUM", "AVG"] {
            let sql = format!(
                "SELECT {f}(v) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (SELECT 1 AS id,CAST(1e308 AS DOUBLE) AS v UNION ALL SELECT 2,CAST(1e308 AS DOUBLE)) t"
            );
            let error = session.execute_sql(&sql).err().unwrap();
            assert_eq!(error.kind, "unsupported");
            assert_eq!(error.message, "unsupported numeric literal: 1e308");
            // SELECT's exact numeric literal binder rejects exponent notation.
            // Typed INSERT accepts finite Float literals and reaches summation.
            // Integration candidate 2: a running Float total that overflows
            // is +/-infinity like DuckDB 1.5.6 (maintainer decision
            // 2026-10-17), in window frames as in grouped SUM/AVG. DuckDB
            // answers [1e308, inf] for both SUM and AVG here.
            let sql = format!(
                "SELECT id,{f}(v) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM finite_overflow ORDER BY id"
            );
            let bits = |x: f64| json!({"float_bits": format!("{:016x}", x.to_bits())});
            assert_eq!(
                rows(&query(&mut session, &sql)),
                vec![
                    vec![json!(1), bits(1e308)],
                    vec![json!(2), bits(f64::INFINITY)]
                ],
                "{sql}"
            );
        }
    }
}

/// SF1 is a separate driver acceptance workload, never a default debug test.
/// Its independent reference must already exist; this test cannot generate it.
#[test]
#[ignore = "driver: supply WINDOW_FRAMES_SF1 and hash-verified external references under shared lock"]
fn sf1_analysis_against_frozen_reference() {
    let directory = PathBuf::from(std::env::var("WINDOW_FRAMES_SF1").unwrap());
    let reference: Value =
        serde_json::from_slice(external_sf1::read("window_frames/sf1_expected.json").as_bytes())
            .unwrap();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let tables = ["orders", "customer", "lineitem"]
        .into_iter()
        .map(|name| (name.to_string(), directory.join(format!("{name}.parquet"))))
        .collect();
    let fixture = Fixture::new();
    for workers in [1, 4, 10] {
        let options = Options {
            workers,
            memory_bytes: 4 << 30,
            ..Default::default()
        };
        for case in reference["cases"].as_array().unwrap() {
            let prepared =
                assaydb::execute::prepare(case["sql"].as_str().unwrap(), &tables, &options)
                    .unwrap();
            let actual = rows(
                &assaydb::execute::execute(&prepared, &options)
                    .unwrap()
                    .batch,
            );
            let wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
            assert_eq!(actual.len(), wanted.len(), "{}", case["id"]);
            for (a, b) in actual.iter().zip(&wanted) {
                assert_eq!(a.len(), b.len());
                for (a, b) in a.iter().zip(b) {
                    assert!(same(a, b), "{}: {a} != {b}", case["id"]);
                }
            }
        }
    }
    drop(fixture);
}

fn integer_cells(array: &ArrayRef) -> Vec<Option<i128>> {
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        a.iter().map(|v| v.map(i128::from)).collect()
    } else {
        let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(a.scale(), 0);
        a.iter().collect()
    }
}

/// One 200,000-row partition: suffix (CURRENT ROW .. UNBOUNDED FOLLOWING),
/// RANGE offset and default-frame LAST_VALUE must be linear, including with
/// spilled partition chunks under a small budget. Expected answers are simple
/// independent computations over the generated columns, never Assay output.
#[test]
fn large_single_partition_frames_are_linear_with_and_without_spill() {
    use arrow::datatypes::{Field, Schema};
    use assaydb::execute::{execute, prepare};
    use std::{collections::HashMap, sync::Arc, time::Instant};
    const N: usize = 200_000;
    let fixture = Fixture::new();
    // A permutation of ids so the input is not already in window order.
    let ids = (0..N).map(|i| ((i * 7919) % N) as i64).collect::<Vec<_>>();
    let value = |id: i64| (id % 17 != 0).then(|| (id * 2_654_435_761) % 2001 - 1000);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|id| id / 4))),
            Arc::new(Int64Array::from_iter(ids.iter().map(|id| value(*id)))),
            Arc::new(Float64Array::from_iter(
                ids.iter().map(|id| value(*id).map(|v| v as f64)),
            )),
        ],
    )
    .unwrap();
    let path = fixture.0.join("large.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create_new(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);

    // Independent expectations in id order (ids are exactly 0..N).
    let v = (0..N as i64).map(value).collect::<Vec<_>>();
    let mut suffix = vec![None; N + 1];
    let mut suffix_max = vec![None; N + 1];
    for i in (0..N).rev() {
        suffix[i] = match (v[i], suffix[i + 1]) {
            (None, s) => s,
            (Some(x), None) => Some(i128::from(x)),
            (Some(x), Some(s)) => Some(s + i128::from(x)),
        };
        suffix_max[i] = match (v[i], suffix_max[i + 1]) {
            (None, s) => s,
            (Some(x), s) => Some(s.map_or(x, |s: i64| s.max(x))),
        };
    }
    // RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING over k = id / 4.
    let keys = N / 4;
    let range = |k: usize| {
        let ids = (k.saturating_sub(3) * 4)..((k + 3).min(keys) * 4);
        let present = ids.filter_map(|id| v[id]).collect::<Vec<_>>();
        (
            (!present.is_empty()).then(|| present.iter().map(|x| i128::from(*x)).sum::<i128>()),
            present.len() as i128,
            present.iter().min().copied().map(i128::from),
        )
    };
    let ranges = (0..keys).map(range).collect::<Vec<_>>();
    // Float SUM over exactly representable integers: each IEEE addition is
    // exact, so the canonical left-to-right fold equals the integer sum.
    // Confirm that claim directly on sampled suffixes with a real fold.
    for start in (0..N).step_by(N / 16).chain([N - 1]) {
        let mut fold = 0.0_f64;
        for x in v[start..].iter().flatten() {
            fold += *x as f64;
        }
        assert_eq!(
            fold.to_bits(),
            (suffix[start].unwrap() as f64).to_bits(),
            "sequential fold at {start}"
        );
    }

    let queries = [
        "SELECT id,SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),MAX(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),SUM(f) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t ORDER BY id",
        "SELECT id,SUM(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING),COUNT(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING),MIN(v) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING) FROM t ORDER BY id",
        "SELECT id,LAST_VALUE(v) OVER (ORDER BY id) FROM t ORDER BY id",
    ];
    for (workers, batch_size, spill, memory) in
        [(1, 8192, false, 256_usize << 20), (4, 1000, true, 64 << 20)]
    {
        let options = Options {
            workers,
            batch_size,
            memory_bytes: memory,
            spill: spill.then(|| SpillOptions {
                directory: fixture.0.clone(),
                sort_run_bytes: 256 << 10,
                merge_fan_in: 8,
                ..Default::default()
            }),
            ..Default::default()
        };
        for (index, sql) in queries.iter().enumerate() {
            let started = Instant::now();
            let prepared = prepare(sql, &tables, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let execution = execute(&prepared, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let elapsed = started.elapsed();
            let io = &execution.metrics.spill;
            eprintln!(
                "large frame query {index} workers={workers} spill={spill} memory={memory}: {:.2}s (spill files={} written={} read={} peak_reserved={})",
                elapsed.as_secs_f64(),
                io.files_created,
                io.bytes_written,
                io.bytes_read,
                execution.metrics.peak_reserved_bytes
            );
            if spill {
                // Partition chunks really spill, and each spilled chunk is read
                // a bounded number of times (not once per row or probe).
                assert!(io.files_created > 0 && io.bytes_written > 0);
                assert!(io.bytes_read <= io.bytes_written * 16, "{io:?}");
            }
            // The previous per-row frame recomputation needed about 2e10 row
            // visits here; a linear pass finishes far inside this bound.
            assert!(elapsed.as_secs() < 120, "{sql}: {elapsed:?}");
            let result = execution.batch;
            assert_eq!(result.num_rows(), N);
            let id = integer_cells(result.column(0));
            assert!(id.iter().enumerate().all(|(i, x)| *x == Some(i as i128)));
            match index {
                0 => {
                    assert_eq!(integer_cells(result.column(1)), suffix[..N]);
                    assert_eq!(
                        integer_cells(result.column(2)),
                        suffix_max[..N]
                            .iter()
                            .map(|x| x.map(i128::from))
                            .collect::<Vec<_>>()
                    );
                    let floats = result
                        .column(3)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    for (i, expected) in suffix[..N].iter().enumerate() {
                        assert_eq!(
                            floats.is_valid(i).then(|| floats.value(i).to_bits()),
                            expected.map(|x| (x as f64).to_bits()),
                            "SUM(f) row {i}"
                        );
                    }
                }
                1 => {
                    let expected = (0..N).map(|id| ranges[id / 4]).collect::<Vec<_>>();
                    assert_eq!(
                        integer_cells(result.column(1)),
                        expected.iter().map(|x| x.0).collect::<Vec<_>>()
                    );
                    assert_eq!(
                        integer_cells(result.column(2)),
                        expected.iter().map(|x| Some(x.1)).collect::<Vec<_>>()
                    );
                    assert_eq!(
                        integer_cells(result.column(3)),
                        expected.iter().map(|x| x.2).collect::<Vec<_>>()
                    );
                }
                _ => assert_eq!(
                    integer_cells(result.column(1)),
                    v.iter().map(|x| x.map(i128::from)).collect::<Vec<_>>()
                ),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum B {
    UnboundedPreceding,
    Preceding(usize),
    Current,
    Following(usize),
    UnboundedFollowing,
}

struct ModelRow {
    p: i64,
    id: i64,
    k: Option<i64>,
    v: Option<i64>,
    f: Option<f64>,
    s: Option<String>,
}

fn model_rows() -> Vec<ModelRow> {
    let mut rows = Vec::new();
    let mut id = 0;
    // p0: exactly summable Floats plus signed zero, infinities and NaN.
    // p1: inexact Floats with catastrophic cancellation. p2: one NULL row.
    // p3: inexact Floats with NaN and infinity. p4: all-NULL values.
    for (p, n) in [(0_i64, 40_usize), (1, 40), (2, 1), (3, 25), (4, 3)] {
        for i in 0..n {
            id += 1;
            let f = match p {
                0 => Some(match i {
                    17 => f64::INFINITY,
                    29 => f64::NEG_INFINITY,
                    33 => f64::NAN,
                    11 => -0.0,
                    _ if i % 6 == 0 => ((i * 37) % 101) as f64 * 0.25 - 12.5,
                    _ => ((i * 37) % 101) as f64 - 50.0,
                })
                .filter(|_| i % 9 != 4),
                1 => match i {
                    5 | 20 => Some(1e16),
                    6 | 21 => Some(-1e16),
                    _ if i % 7 == 3 => None,
                    _ => Some(i as f64 * 0.1 + 0.07),
                },
                3 => match i {
                    10 => Some(f64::NAN),
                    15 => Some(f64::INFINITY),
                    _ => Some(i as f64 * 0.1),
                },
                _ => None,
            };
            rows.push(ModelRow {
                p,
                id,
                k: (i % 8 != 5 && p != 4).then_some(((i * 5) % 9) as i64),
                v: (i % 5 != 2 && p != 4 && p != 2).then_some(((i * 13) % 41) as i64 - 20),
                f,
                s: (i % 6 != 1 && p != 4)
                    .then(|| format!("s{:02}{}", (i * 7) % 23, "x".repeat(i % 4))),
            });
        }
    }
    rows
}

fn model_frame(
    keys: &[Option<i64>],
    rows: bool,
    descending: bool,
    nulls_first: bool,
    (start, end): (B, B),
    r: usize,
) -> Vec<usize> {
    let n = keys.len();
    if rows {
        let b = match start {
            B::UnboundedPreceding => 0,
            B::Preceding(c) => r.saturating_sub(c),
            B::Current => r,
            _ => unreachable!(),
        };
        let e = match end {
            B::Current => r + 1,
            B::Following(c) => (r + c + 1).min(n),
            B::UnboundedFollowing => n,
            _ => unreachable!(),
        };
        return (b..e).collect();
    }
    // SQL RANGE membership by keys: a row is in the frame unless it sorts
    // strictly before the start bound or strictly after the end bound.
    let order = |a: Option<i64>, b: Option<i64>| match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) if nulls_first => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) if nulls_first => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(a), Some(b)) if descending => b.cmp(&a),
        (Some(a), Some(b)) => a.cmp(&b),
    };
    let current = keys[r];
    let bound =
        |b: B, sign: i64| -> Option<Option<i64>> {
            match b {
                B::UnboundedPreceding | B::UnboundedFollowing => None,
                B::Current => Some(current),
                B::Preceding(c) | B::Following(c) => Some(current.map(|k| {
                    k + sign * c as i64 * if matches!(b, B::Preceding(_)) { -1 } else { 1 }
                })),
            }
        };
    let direction = if descending { -1 } else { 1 };
    let low = bound(start, direction);
    let high = bound(end, direction);
    (0..n)
        .filter(|&j| {
            low.is_none_or(|low| order(keys[j], low).is_ge())
                && high.is_none_or(|high| order(keys[j], high).is_le())
        })
        .collect()
}

fn fold_cell(values: &[f64], average: bool) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let mut sum = 0.0_f64;
    for value in values {
        let next = sum + value;
        assert!(!(next.is_infinite() && sum.is_finite() && value.is_finite()));
        sum = if next.is_nan() { f64::NAN } else { next };
    }
    let result = if average {
        sum / values.len() as f64
    } else {
        sum
    };
    json!({"float_bits": format!("{:016x}", result.to_bits())})
}

/// Every aggregate method and frame class against a naive per-frame model:
/// O(n * w) frame evaluation, canonical left-to-right Float folds from +0.0.
#[test]
fn frame_methods_match_naive_per_frame_model() {
    use arrow::datatypes::{Field, Schema};
    use assaydb::execute::{execute, prepare};
    use std::{collections::HashMap, sync::Arc};
    let fixture = Fixture::new();
    let data = model_rows();
    let schema = Arc::new(Schema::new(vec![
        Field::new("p", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    // Store rows in a scrambled order; ids give the canonical order.
    let order = (0..data.len())
        .map(|i| (i * 31) % data.len())
        .collect::<Vec<_>>();
    assert_eq!(
        order
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        data.len()
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                order.iter().map(|&i| data[i].p),
            )),
            Arc::new(Int64Array::from_iter_values(
                order.iter().map(|&i| data[i].id),
            )),
            Arc::new(Int64Array::from_iter(order.iter().map(|&i| data[i].k))),
            Arc::new(Int64Array::from_iter(order.iter().map(|&i| data[i].v))),
            Arc::new(Float64Array::from_iter(order.iter().map(|&i| data[i].f))),
            Arc::new(StringArray::from_iter(
                order.iter().map(|&i| data[i].s.as_deref()),
            )),
        ],
    )
    .unwrap();
    let path = fixture.0.join("model.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create_new(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);

    let int = |n: i128| json!(i64::try_from(n).unwrap());
    let text = |s: Option<&String>| s.map_or(Value::Null, |s| json!(s));
    let rows_frames = [
        (
            "BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            (B::Current, B::UnboundedFollowing),
        ),
        (
            "BETWEEN 2 PRECEDING AND 1 FOLLOWING",
            (B::Preceding(2), B::Following(1)),
        ),
        (
            "BETWEEN 3 PRECEDING AND CURRENT ROW",
            (B::Preceding(3), B::Current),
        ),
        (
            "BETWEEN CURRENT ROW AND 2 FOLLOWING",
            (B::Current, B::Following(2)),
        ),
        (
            "BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING",
            (B::UnboundedPreceding, B::Following(1)),
        ),
        (
            "BETWEEN 0 PRECEDING AND 0 FOLLOWING",
            (B::Preceding(0), B::Following(0)),
        ),
        (
            "BETWEEN 50 PRECEDING AND 50 FOLLOWING",
            (B::Preceding(50), B::Following(50)),
        ),
    ];
    let range_frames = [
        (
            "BETWEEN 2 PRECEDING AND 1 FOLLOWING",
            (B::Preceding(2), B::Following(1)),
        ),
        (
            "BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            (B::Current, B::UnboundedFollowing),
        ),
        (
            "BETWEEN 1 PRECEDING AND CURRENT ROW",
            (B::Preceding(1), B::Current),
        ),
        (
            "BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING",
            (B::UnboundedPreceding, B::Following(3)),
        ),
        (
            "BETWEEN 0 PRECEDING AND 0 FOLLOWING",
            (B::Preceding(0), B::Following(0)),
        ),
    ];
    let mut cases = Vec::new();
    for (frame, bounds) in rows_frames {
        let over = format!("OVER (PARTITION BY p ORDER BY id ROWS {frame})");
        let sql = format!(
            "SELECT id,SUM(f) {over},AVG(f) {over},SUM(v) {over},COUNT(v) {over},COUNT(*) {over},MIN(s) {over},MAX(v) {over},MAX(s) {over},FIRST_VALUE(s) {over},LAST_VALUE(s) {over},NTH_VALUE(s,2) {over} FROM t ORDER BY id"
        );
        let mut expected = Vec::new();
        for p in 0..5 {
            let part = data.iter().filter(|r| r.p == p).collect::<Vec<_>>();
            let keys = part.iter().map(|r| Some(r.id)).collect::<Vec<_>>();
            for r in 0..part.len() {
                let frame = model_frame(&keys, true, false, false, bounds, r)
                    .into_iter()
                    .map(|j| part[j])
                    .collect::<Vec<_>>();
                let floats = frame.iter().filter_map(|x| x.f).collect::<Vec<_>>();
                let ints = frame.iter().filter_map(|x| x.v).collect::<Vec<_>>();
                let texts = frame
                    .iter()
                    .filter_map(|x| x.s.as_ref())
                    .collect::<Vec<_>>();
                expected.push(vec![
                    json!(part[r].id),
                    fold_cell(&floats, false),
                    fold_cell(&floats, true),
                    if ints.is_empty() {
                        Value::Null
                    } else {
                        int(ints.iter().map(|x| i128::from(*x)).sum())
                    },
                    json!(ints.len()),
                    json!(frame.len()),
                    text(texts.iter().min().copied()),
                    ints.iter().max().map_or(Value::Null, |x| json!(x)),
                    text(texts.iter().max().copied()),
                    text(frame.first().and_then(|x| x.s.as_ref())),
                    text(frame.last().and_then(|x| x.s.as_ref())),
                    text(frame.get(1).and_then(|x| x.s.as_ref())),
                ]);
            }
        }
        cases.push((sql, expected));
    }
    for (key_sql, descending, nulls_first) in [
        ("k", false, false),
        ("k DESC NULLS FIRST", true, true),
        ("k NULLS FIRST", false, true),
    ] {
        for (frame, bounds) in range_frames {
            let over = format!("OVER (PARTITION BY p ORDER BY {key_sql} RANGE {frame})");
            let sql = format!(
                "SELECT id,SUM(v) {over},COUNT(v) {over},COUNT(*) {over},MIN(s) {over},MAX(v) {over} FROM t ORDER BY id"
            );
            let mut expected = Vec::new();
            for p in 0..5 {
                let part = data.iter().filter(|r| r.p == p).collect::<Vec<_>>();
                let keys = part.iter().map(|r| r.k).collect::<Vec<_>>();
                for r in 0..part.len() {
                    let frame = model_frame(&keys, false, descending, nulls_first, bounds, r)
                        .into_iter()
                        .map(|j| part[j])
                        .collect::<Vec<_>>();
                    let ints = frame.iter().filter_map(|x| x.v).collect::<Vec<_>>();
                    let texts = frame
                        .iter()
                        .filter_map(|x| x.s.as_ref())
                        .collect::<Vec<_>>();
                    expected.push(vec![
                        json!(part[r].id),
                        if ints.is_empty() {
                            Value::Null
                        } else {
                            int(ints.iter().map(|x| i128::from(*x)).sum())
                        },
                        json!(ints.len()),
                        json!(frame.len()),
                        text(texts.iter().min().copied()),
                        ints.iter().max().map_or(Value::Null, |x| json!(x)),
                    ]);
                }
            }
            cases.push((sql, expected));
        }
    }
    // Single-row batches split every chunk; 512-byte runs spill every chunk.
    for (workers, batch_size, spill) in [(1, 8192, false), (10, 1, false), (4, 7, true)] {
        let options = Options {
            workers,
            batch_size,
            memory_bytes: 64 << 20,
            spill: spill.then(|| SpillOptions {
                directory: fixture.0.clone(),
                sort_run_bytes: 512,
                merge_fan_in: 2,
                ..Default::default()
            }),
            ..Default::default()
        };
        for (sql, expected) in &cases {
            let prepared = prepare(sql, &tables, &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let actual = rows(
                &execute(&prepared, &options)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .batch,
            );
            assert_eq!(actual.len(), expected.len(), "{sql}");
            for (a, b) in actual.iter().zip(expected) {
                assert_eq!(
                    a, b,
                    "workers={workers} batch={batch_size} spill={spill}: {sql}"
                );
            }
        }
    }
}

/// INTEGER SUM/AVG keep the grouped rule in every frame class: each running
/// total of the frame, folded from its first row, stays within precision 38
/// (DECIMAL checks only the frame total). Expected outcomes come from that
/// rule applied naively to each frame, never from Assay output.
#[test]
fn integer_running_totals_and_decimal_totals_in_sliding_frames() {
    const LIMIT: i128 = 10_i128.pow(38) - 1;
    // X = (2^63 - 1)^2, an INTEGER intermediate with 2X > 10^38 - 1.
    const X: i128 = 9_223_372_036_854_775_807_i128 * 9_223_372_036_854_775_807_i128;
    let fixture = Fixture::new();
    let frames = [
        (
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            (B::Current, B::UnboundedFollowing),
        ),
        (
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            (B::Preceding(1), B::Following(1)),
        ),
        (
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING",
            (B::UnboundedPreceding, B::Following(1)),
        ),
        (
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
            (B::Preceding(2), B::Current),
        ),
    ];
    let inputs: [&[i64]; 5] = [
        &[1, -1, 1, -1],
        &[1, 1, -1, -1],
        &[-1, 1, 1],
        &[1, -1, 1],
        &[-1, 1, -1, 1, 1, -1],
    ];
    let mut outcomes = [0; 4];
    for spill in [false, true] {
        let mut session = fixture.session(4, 1, spill);
        for values in inputs {
            let source = values
                .iter()
                .enumerate()
                .map(|(i, v)| format!("SELECT {} AS id,{v} AS v", i + 1))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            for (frame, bounds) in frames {
                let keys = vec![None; values.len()];
                let frames = (0..values.len())
                    .map(|r| model_frame(&keys, true, false, false, bounds, r))
                    .collect::<Vec<_>>();
                // Integer: every running total; Decimal: frame total only.
                let integer_ok = frames.iter().all(|frame| {
                    let mut total = 0_i128;
                    frame.iter().all(|&j| {
                        total += i128::from(values[j]) * X;
                        total.abs() <= LIMIT
                    })
                });
                let totals = frames
                    .iter()
                    .map(|frame| frame.iter().map(|&j| i128::from(values[j])).sum::<i128>())
                    .collect::<Vec<_>>();
                let expected = totals
                    .iter()
                    .enumerate()
                    .map(|(i, total)| vec![json!(i + 1), json!(*total as i64)])
                    .collect::<Vec<_>>();
                let product = "v*9223372036854775807*9223372036854775807";
                for (function, scale) in [
                    ("SUM", "/ 9223372036854775807 / 9223372036854775807"),
                    ("AVG", ""),
                ] {
                    let sql = format!(
                        "SELECT id,{function}({product}) OVER (ORDER BY id {frame}){scale} FROM ({source}) t ORDER BY id"
                    );
                    match session.execute_sql(&sql) {
                        Ok(Output::Query { execution, .. }) => {
                            assert!(
                                integer_ok,
                                "{sql}: accepted a running total above precision 38"
                            );
                            outcomes[0] += 1;
                            if function == "SUM" {
                                assert_eq!(rows(&execution.batch), expected, "{sql}");
                            } else {
                                let averages = execution
                                    .batch
                                    .column(1)
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap()
                                    .clone();
                                for (i, frame) in frames.iter().enumerate() {
                                    let wanted = (totals[i] * X) as f64 / frame.len() as f64;
                                    assert!(
                                        (averages.value(i) - wanted).abs() <= wanted.abs() * 1e-15,
                                        "{sql}"
                                    );
                                }
                            }
                        }
                        Ok(_) => panic!("expected query"),
                        Err(error) => {
                            assert!(!integer_ok, "{sql}: {error}");
                            outcomes[1] += 1;
                            assert_eq!(
                                (error.kind, error.message.as_str()),
                                ("engine", "decimal precision 38 overflow"),
                                "{sql}"
                            );
                        }
                    }
                }
                let sql = format!(
                    "SELECT id,SUM(CAST({product} AS DECIMAL(38,0))) OVER (ORDER BY id {frame}) FROM ({source}) t ORDER BY id"
                );
                let decimal_ok = totals.iter().all(|total| (total * X).abs() <= LIMIT);
                match session.execute_sql(&sql) {
                    Ok(Output::Query { execution, .. }) => {
                        assert!(decimal_ok, "{sql}");
                        outcomes[2] += 1;
                        assert_eq!(
                            rows(&execution.batch),
                            totals
                                .iter()
                                .enumerate()
                                .map(|(i, total)| vec![
                                    json!(i + 1),
                                    json!({"decimal": (total * X).to_string()})
                                ])
                                .collect::<Vec<_>>(),
                            "{sql}"
                        );
                    }
                    Ok(_) => panic!("expected query"),
                    Err(error) => {
                        assert!(!decimal_ok, "{sql}: {error}");
                        assert_eq!(
                            (error.kind, error.message.as_str()),
                            ("engine", "decimal precision 38 overflow"),
                            "{sql}"
                        );
                    }
                }
                // A frame that DECIMAL accepts despite an out-of-range running
                // total, which INTEGER refuses.
                if decimal_ok && !integer_ok {
                    outcomes[3] += 1;
                }
            }
        }
    }
    // Both outcomes occur, including errors only after the frame start moves.
    assert!(outcomes.iter().all(|n| *n > 0), "{outcomes:?}");
}

/// Inexact Float frames that do not start at UNBOUNDED PRECEDING fold each
/// frame left to right; a partition whose total folded frame rows exceed
/// max(2^28, 128 * rows) is refused before any of that work.
#[test]
fn inexact_float_frames_fold_canonically_or_refuse_before_quadratic_work() {
    use arrow::datatypes::{Field, Schema};
    use assaydb::execute::{execute, prepare};
    use std::{collections::HashMap, sync::Arc, time::Instant};
    const N: usize = 24_000;
    let fixture = Fixture::new();
    let f = (0..N)
        .map(|i| (i % 10 != 3).then_some((i % 97) as f64 * 0.1 + 0.01))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..N as i64)),
            Arc::new(Float64Array::from(f.clone())),
        ],
    )
    .unwrap();
    let path = fixture.0.join("inexact.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create_new(&path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);
    for spill in [false, true] {
        let options = Options {
            workers: 4,
            batch_size: 1000,
            memory_bytes: 64 << 20,
            spill: spill.then(|| SpillOptions {
                directory: fixture.0.clone(),
                sort_run_bytes: 64 << 10,
                merge_fan_in: 4,
                ..Default::default()
            }),
            ..Default::default()
        };
        // N(N+1)/2 = 288,012,000 > 2^28 = 268,435,456 > 128 * N.
        let started = Instant::now();
        let sql = "SELECT SUM(f) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t";
        let error = execute(&prepare(sql, &tables, &options).unwrap(), &options)
            .err()
            .unwrap();
        assert_eq!(
            error,
            "unsupported: Float SUM/AVG window frames need more than 268435456 sequential frame-row additions in one partition (frame not starting at UNBOUNDED PRECEDING over inexactly summable Float values)"
        );
        assert!(
            started.elapsed().as_secs() < 60,
            "refusal precedes the folds"
        );
        for (sql, low, high, average) in [
            (
                "SELECT id,SUM(f) OVER (ORDER BY id ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM t ORDER BY id",
                100,
                100,
                false,
            ),
            (
                "SELECT id,AVG(f) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) FROM t ORDER BY id",
                N,
                5,
                true,
            ),
        ] {
            let result = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .batch;
            let values = result
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..N {
                let frame = &f[row.saturating_sub(low)..(row + high + 1).min(N)];
                let present = frame.iter().flatten().copied().collect::<Vec<_>>();
                assert_eq!(
                    json!({"float_bits": format!("{:016x}", values.value(row).to_bits())}),
                    fold_cell(&present, average),
                    "{sql} row {row}"
                );
            }
        }
    }
}

/// An ORDER BY key textually identical to the QUALIFY predicate must not bind
/// to QUALIFY's hidden projection column, which is removed after filtering.
/// Answers were produced by DuckDB 1.5.6 on the same table before this check.
#[test]
fn order_by_expression_identical_to_qualify_predicate() {
    let fixture = Fixture::new();
    for (workers, batch, spill) in [(1, 8192, false), (4, 1, true)] {
        let mut session = fixture.session(workers, batch, spill);
        session
            .execute_sql("CREATE TABLE q(id INTEGER,v INTEGER)")
            .unwrap();
        session
            .execute_sql("INSERT INTO q VALUES(1,10),(2,30),(3,20),(4,40)")
            .unwrap();
        for (sql, duckdb) in [
            (
                "SELECT id,v FROM q QUALIFY ROW_NUMBER() OVER (ORDER BY id) > 1 ORDER BY ROW_NUMBER() OVER (ORDER BY id) > 1, v DESC",
                vec![
                    vec![json!(4), json!(40)],
                    vec![json!(2), json!(30)],
                    vec![json!(3), json!(20)],
                ],
            ),
            (
                "SELECT id,v FROM q QUALIFY ROW_NUMBER() OVER (ORDER BY id) > 1 ORDER BY ROW_NUMBER() OVER (ORDER BY id) > 1 DESC, v",
                vec![
                    vec![json!(3), json!(20)],
                    vec![json!(2), json!(30)],
                    vec![json!(4), json!(40)],
                ],
            ),
            (
                "SELECT id FROM q QUALIFY ROW_NUMBER() OVER (ORDER BY v) > 1 ORDER BY ROW_NUMBER() OVER (ORDER BY v) > 1, id DESC",
                vec![vec![json!(4)], vec![json!(3)], vec![json!(2)]],
            ),
        ] {
            assert_eq!(rows(&query(&mut session, sql)), duckdb, "{sql}");
        }
    }
}

/// Integration candidate 2: Float SUM/AVG OVER frames whose running totals
/// overflow return +/-infinity like DuckDB 1.5.6 (maintainer decision
/// 2026-10-17). The frames cover the prefix fold, the sequential per-frame fold
/// (sliding frames whose later totals are finite again), the aggregate prefix
/// path, RANGE and partitions; the exact scaled-sum method never applies here
/// because the partition total exceeds binary64. Expected rows are DuckDB's
/// (tools/testing/window_float_overflow_reference.py).
#[test]
fn float_sum_avg_window_overflow_matches_duckdb() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/window_float_overflow/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 16);
    let fixture = Fixture::new();
    for (workers, batch_size, spill) in [(1, 8192, false), (4, 1, true), (10, 3, false)] {
        let mut session = fixture.session(workers, batch_size, spill);
        for sql in expected["setup"].as_array().unwrap() {
            session.execute_sql(sql.as_str().unwrap()).unwrap();
        }
        for case in cases {
            let sql = case["sql"].as_str().unwrap();
            assert_eq!(
                json!(rows(&query(&mut session, sql))),
                case["rows"],
                "{} workers={workers} batch={batch_size} spill={spill}",
                case["id"]
            );
        }
    }
}
