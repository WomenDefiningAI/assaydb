//! Exact external widening against answers frozen before the first Assay run.
use arrow::{
    array::*, datatypes::DataType, record_batch::RecordBatch, util::display::array_value_to_string,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
    spill::SpillOptions,
    types::Type,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/parquet_types")
}
fn reference() -> Value {
    serde_json::from_str(include_str!("fixtures/parquet_types/expected.json")).unwrap()
}
fn tables(reference: &Value) -> HashMap<String, PathBuf> {
    reference["files"]
        .as_object()
        .unwrap()
        .keys()
        .map(|name| (name.clone(), root().join(format!("{name}.parquet"))))
        .collect()
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows()).map(|row| batch.columns().iter().map(|array| {
        if array.is_null(row) { return Value::Null; }
        match array.data_type() {
            DataType::Float64 => json!({"f64_bits": array.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_bits()}),
            DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
            DataType::Null => Value::Null,
            _ => json!(array_value_to_string(array.as_ref(), row).unwrap()),
        }
    }).collect()).collect()
}
fn compare(batch: &RecordBatch, case: &Value) {
    let mut actual = rows(batch);
    let mut expected: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
    for (index, ty) in case["types"].as_array().unwrap().iter().enumerate() {
        let wanted = match ty.as_str().unwrap() {
            "FLOAT" | "DOUBLE" => DataType::Float64,
            "VARCHAR" => DataType::Utf8,
            "DATE" => DataType::Date32,
            _ => DataType::Int64,
        };
        assert_eq!(batch.column(index).data_type(), &wanted, "{}", case["id"]);
    }
    if !case["ordered"].as_bool().unwrap() {
        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
    }
    assert_eq!(actual, expected, "{}: {}", case["id"], case["sql"]);
}

fn check_case(case: &Value, tables: &HashMap<String, PathBuf>, options: &Options) {
    let sql = case["sql"].as_str().unwrap();
    let result = prepare(sql, tables, options).and_then(|plan| execute(&plan, options));
    if case["id"] == "uint64_aggregate" {
        // DuckDB's frozen HUGEINT SUM is 2^63 + 4. Assay's existing public
        // BIGINT result boundary is independent of exact input widening.
        assert_eq!(case["rows"][0][0], json!(9_223_372_036_854_775_812_u64));
        assert_eq!(result.err().as_deref(), Some("BIGINT overflow"));
    } else if case["id"] == "float32_groups" {
        // The input holds -0.0 (and no +0.0). DuckDB returns that group's
        // first-seen key, -0.0. Assay's existing GROUP BY contract treats signed
        // zeros as equal and emits the canonical +0.0 key (the same holds for a
        // native Float64 column; see SEMANTICS-LOG "existing GROUP BY
        // contract"). The frozen reference is kept verbatim; only that one key
        // cell is mapped -0.0 -> +0.0, and every other cell must match exactly.
        // Bit-exact widening of -0.0 itself is pinned by float32_values and
        // float_special_bits.
        let negative_zero = json!({"f64_bits": (-0.0_f64).to_bits()});
        let mut expected = case.clone();
        let rows = expected["rows"].as_array_mut().unwrap();
        let zero_keys = rows.iter().filter(|row| row[0] == negative_zero).count();
        assert_eq!(zero_keys, 1, "reference keeps DuckDB's -0.0 group key");
        for row in rows {
            if row[0] == negative_zero {
                row[0] = json!({"f64_bits": 0.0_f64.to_bits()});
            }
        }
        let result = result.unwrap_or_else(|e| panic!("{sql}: {e}"));
        compare(&result.batch, &expected);
    } else {
        let result = result.unwrap_or_else(|e| panic!("{sql}: {e}"));
        compare(&result.batch, case);
    }
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-parquet-types-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn options(&self, workers: usize, batch_size: usize, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: 32 << 20,
            spill: spill.then(|| SpillOptions {
                directory: self.0.clone(),
                sort_run_bytes: 128,
                merge_fan_in: 2,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[test]
fn exact_duckdb_answers_across_workers_batches_and_spill() {
    let reference = reference();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let tables = tables(&reference);
    let scratch = Scratch::new();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let options = scratch.options(workers, batch, spill);
                for case in reference["cases"].as_array().unwrap() {
                    check_case(case, &tables, &options);
                }
                assert_eq!(fs::read_dir(&scratch.0).unwrap().count(), 0);
            }
        }
    }
}

#[test]
fn cached_predicates_and_unoptimized_scans_keep_widened_values() {
    let reference = reference();
    let tables = tables(&reference);
    let scratch = Scratch::new();
    for cache in [None, Some(0), Some(1 << 16)] {
        for optimizations in [false, true] {
            let mut options = scratch.options(4, 7, false);
            options.parquet_cache_bytes = cache;
            options.relational_optimizations = optimizations;
            for case in reference["cases"].as_array().unwrap() {
                check_case(case, &tables, &options);
            }
        }
    }
}

#[test]
fn checked_conversion_refusals_name_the_column_and_keep_unsupported_label() {
    let tables = tables(&reference());
    let scratch = Scratch::new();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let options = scratch.options(workers, batch, spill);
                for (table, message) in [
                    (
                        "uint64_overflow",
                        "unsupported: column 'v': UInt64 value exceeds Int64 maximum",
                    ),
                    (
                        "date64_non_midnight",
                        "unsupported: column 'v': Date64 value is not exact midnight",
                    ),
                    (
                        "date64_out_of_range",
                        "unsupported: column 'v': Date64 day exceeds Date range",
                    ),
                ] {
                    for expression in ["v", "COUNT(v)"] {
                        for predicate in ["", " WHERE v IS NOT NULL"] {
                            let sql = format!("SELECT {expression} FROM {table}{predicate}");
                            let error = prepare(&sql, &tables, &options)
                                .and_then(|plan| execute(&plan, &options))
                                .err()
                                .unwrap();
                            assert_eq!(error, message, "{workers}/{batch}/{spill} {sql}");
                        }
                    }
                }
                // Old -> new (TIMESTAMP card, Oct 17): the microsecond
                // Timestamp file used to refuse here; it now reads as
                // TIMESTAMP. DuckDB 1.5.6 over the same file returns
                // 1970-01-01 00:00:00, NULL, 1970-01-01 00:00:00.000001 and
                // COUNT(v) = 2. Time-zone-aware timestamps stay refused
                // (tests/timestamp.rs).
                let sql = "SELECT v, COUNT(v) OVER () AS n FROM timestamp_us";
                let result = prepare(sql, &tables, &options)
                    .and_then(|plan| execute(&plan, &options))
                    .unwrap()
                    .batch;
                let values = result
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    [Some(0), None, Some(1)],
                    "{workers}/{batch}/{spill}"
                );
                assert_eq!(
                    assaydb::expr::timestamp::format(1).unwrap(),
                    "1970-01-01 00:00:00.000001"
                );
                let counts = result
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                assert!(
                    counts.iter().all(|n| n == Some(2)),
                    "{workers}/{batch}/{spill}"
                );
            }
        }
    }
}

#[test]
fn session_local_sales_and_float4_stays_refused() {
    let scratch = Scratch::new();
    let mut session = Session::new(&scratch.0, Options::default(), Limits::default()).unwrap();
    session.set_local_files_enabled(true);
    // REAL DDL already maps to binary64. FLOAT4 is binary32 in DuckDB and
    // PostgreSQL, and CAST AS FLOAT4 is deliberately refused, so FLOAT4 DDL is
    // not an exact alias and stays refused; scan-time Float32 widening is a
    // different (exact) conversion.
    let error = session
        .execute_sql("CREATE TABLE prices (single FLOAT4)")
        .err()
        .unwrap();
    assert_eq!(error.kind, "unsupported");
    assert_eq!(error.message, "column type: FLOAT4");
    session
        .execute_sql("CREATE TABLE prices (real_value REAL)")
        .unwrap();
    session
        .execute_sql("INSERT INTO prices VALUES (16777217), (NULL)")
        .unwrap();
    let Output::Query { execution, .. } = session.execute_sql("SELECT * FROM prices").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rows(&execution.batch),
        vec![
            vec![json!({"f64_bits": 16777217_f64.to_bits()})],
            vec![Value::Null]
        ]
    );
    let reference = reference();
    let case = reference["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == "sales_rows")
        .unwrap();
    let sql = format!(
        "SELECT * FROM read_parquet('{}') ORDER BY sale_id",
        root().join("sales.parquet").display()
    );
    let Output::Query { execution, .. } = session.execute_sql(&sql).unwrap() else {
        panic!()
    };
    compare(&execution.batch, case);
}

#[test]
fn newly_supported_arrow_types_have_the_expected_logical_type() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        assert_eq!(Type::from_arrow(&ty).unwrap(), Type::Int);
    }
    for ty in [
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
    ] {
        assert_eq!(Type::from_arrow(&ty).unwrap(), Type::Text);
    }
    assert_eq!(Type::from_arrow(&DataType::Float32).unwrap(), Type::Float);
    assert_eq!(Type::from_arrow(&DataType::Date64).unwrap(), Type::Date);
}
