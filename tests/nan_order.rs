//! Independent DuckDB goldens, exact random differential, and forced-spill matrix.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-nan-order-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        fs::create_dir(directory.join("spill")).unwrap();
        fs::write(
            directory.join("readings.parquet"),
            include_bytes!("fixtures/nan_order/readings.parquet"),
        )
        .unwrap();
        Self(directory)
    }
    fn tables(&self) -> HashMap<String, PathBuf> {
        HashMap::from([("t".into(), self.0.join("readings.parquet"))])
    }
    fn options(&self, workers: usize, batch_size: usize, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: 64 << 20,
            spill: spill.then(|| SpillOptions {
                directory: self.0.join("spill"),
                sort_run_bytes: 256,
                merge_fan_in: 2,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn clean(&self) {
        assert_eq!(fs::read_dir(self.0.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows()).map(|row| batch.columns().iter().map(|array| {
        if array.is_null(row) || array.data_type() == &DataType::Null { return Value::Null; }
        match array.data_type() {
            DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
            DataType::Boolean => json!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row)),
            DataType::Float64 => {
                let value = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
                json!({"float": if value.is_nan() { "NaN".into() } else { format!("{:016x}", if value == 0.0 { 0 } else { value.to_bits() }) }})
            }
            DataType::Utf8 => json!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row)),
            other => panic!("unexpected NaN fixture output {other}"),
        }
    }).collect()).collect()
}
fn check(batch: &RecordBatch, case: &Value, options: &Options) {
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int64 => "integer",
            DataType::Float64 => "float",
            DataType::Boolean => "boolean",
            DataType::Utf8 => "text",
            other => panic!("unexpected output type {other}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(json!(types), case["types"], "schema: {}", case["sql"]);
    assert_eq!(
        json!(rows(batch)),
        case["rows"],
        "{} workers={} batch={} spill={}",
        case["sql"],
        options.workers,
        options.batch_size,
        options.spill.is_some()
    );
}

#[test]
fn reference_matrix_workers_batches_spill() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/nan_order/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 77);
    for (source, case) in include_str!("fixtures/nan_order/cases.sql")
        .split("-- case: ")
        .skip(1)
        .zip(cases)
    {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata["id"], case["id"]);
        assert_eq!(sql.trim(), case["sql"].as_str().unwrap());
    }
    let fixture = Fixture::new();
    let tables = fixture.tables();
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let options = fixture.options(workers, batch, spill);
                let (mut sorts, mut joins, mut aggregates) = (0, 0, 0);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let result =
                        prepare(sql, &tables, &options).and_then(|p| execute(&p, &options));
                    if let Some(refusal) = case["refusal"].as_str() {
                        let error = result.err().expect("existing syntax boundary must refuse");
                        assert!(error.contains(refusal), "{sql}: {error}");
                    } else {
                        let result = result.unwrap_or_else(|e| panic!("{sql}: {e}"));
                        check(&result.batch, case, &options);
                        assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                        assert!(result.metrics.worker_threads_peak <= workers);
                        sorts += result.metrics.external_sort_runs;
                        joins += result.metrics.external_join_partitions;
                        aggregates += result.metrics.external_aggregate_partitions;
                    }
                    fixture.clean();
                }
                if spill {
                    assert!(
                        sorts > 1 && joins > 0 && aggregates > 0,
                        "must exercise external sort/join/aggregation: {sorts}/{joins}/{aggregates}"
                    );
                }
            }
        }
    }
}

#[test]
fn thousand_unique_random_differentials_with_and_without_forced_spill() {
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/nan_order/random.json")).unwrap();
    assert_eq!(reference["seed"], 20261016);
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 1000);
    let unique = cases
        .iter()
        .map(|c| c["sql"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), 1000);
    let fixture = Fixture::new();
    let tables = fixture.tables();
    for spill in [false, true] {
        let mut sorts = 0;
        for (i, case) in cases.iter().enumerate() {
            let mut options = fixture.options([1, 4, 10][i % 3], [1, 7, 8192][i / 3 % 3], spill);
            // Exercise optimized and generic paths over the same reference SQL.
            options.relational_optimizations = i % 2 == 0;
            let sql = case["sql"].as_str().unwrap();
            let result = execute(
                &prepare(sql, &tables, &options).unwrap_or_else(|e| panic!("{sql}: {e}")),
                &options,
            )
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
            check(&result.batch, case, &options);
            sorts += result.metrics.external_sort_runs;
            assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
            fixture.clean();
        }
        if spill {
            assert!(sorts > 1, "random suite must spill");
        }
    }
}

#[test]
fn parquet_statistics_do_not_hide_nan_or_change_source_bits() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let fixture = Fixture::new();
    let tables = fixture.tables();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&tables["t"]).unwrap()).unwrap();
    // Group zero contains a negative NaN, but the Parquet statistics omit it.
    let stats = builder
        .metadata()
        .row_group(0)
        .column(2)
        .statistics()
        .unwrap();
    let parquet::file::statistics::Statistics::Double(stats) = stats else {
        panic!("expected DOUBLE stats")
    };
    assert_eq!(stats.max_opt(), Some(&-0.25));
    let source = builder
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let bits = source
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for spill in [false, true] {
        let options = fixture.options(4, 7, spill);
        let sql = "SELECT id,reading FROM t ORDER BY reading DESC NULLS LAST,id";
        let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        let ids = result
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = result
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for (id, value) in ids.iter().zip(values.iter()) {
            assert_eq!(value.map(f64::to_bits), bits[id.unwrap() as usize]);
        }
        let sql = "SELECT id FROM t WHERE reading>100 AND id<4";
        let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        assert_eq!(rows(&result.batch), vec![vec![json!(0)]]);
        fixture.clean();
    }
}

#[test]
fn float32_input_keeps_nan_order_and_session_assignment_boundary_is_explicit() {
    use assaydb::session::{Limits, Session};
    let fixture = Fixture::new();
    fs::write(
        fixture.0.join("float32.parquet"),
        include_bytes!("fixtures/nan_order/float32.parquet"),
    )
    .unwrap();
    let tables = HashMap::from([("t".into(), fixture.0.join("float32.parquet"))]);
    let options = fixture.options(1, 7, false);
    // Integration (#91 exact Parquet widening + #101 NaN order): the Float32
    // column widens to binary64 and keeps NaN equality and order. Expected
    // answers are DuckDB 1.5.6 over the same file (in-memory table and
    // read_parquet agree): 40 rows, 38 non-NULL, 10 distinct values (all 15
    // NaNs one value, signed zeros one value), 25 rows > 0 (15 NaN + 10
    // positive), MAX = NaN and MIN = -inf.
    for (sql, expected) in [
        (
            "SELECT count(*) AS n, count(reading) AS c, count(DISTINCT reading) AS d FROM t",
            vec![json!(40), json!(38), json!(10)],
        ),
        (
            "SELECT count(*) AS n FROM t WHERE reading > 0",
            vec![json!(25)],
        ),
        (
            "SELECT max(reading) AS mx, min(reading) AS mn FROM t",
            vec![
                json!({"float": "NaN"}),
                json!({"float": "fff0000000000000"}),
            ],
        ),
    ] {
        let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        assert_eq!(rows(&result.batch), vec![expected], "{sql}");
    }
    let mut session = Session::new(
        &fixture.0,
        options,
        Limits {
            max_catalog_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .unwrap();
    session.set_local_files_enabled(true);
    session
        .execute_sql("CREATE TABLE readings(x DOUBLE)")
        .unwrap();
    let sql = format!(
        "INSERT INTO readings SELECT reading FROM read_parquet('{}') WHERE id=0",
        fixture.0.join("readings.parquet").display()
    );
    let error = session.execute_sql(&sql).err().unwrap();
    assert!(
        error.message.contains("non-finite float assignment"),
        "{error}"
    );
}
