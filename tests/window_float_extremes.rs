//! Float MIN/MAX OVER every supported frame with NaN, signed zeros, infinities
//! and NULLs (integration of the NaN total order with window frames). Expected
//! rows are DuckDB 1.5.6's over an in-memory copy of the NaN fixture; see
//! tests/fixtures/window_float_extremes/README.md.
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
            "assay-window-float-extremes-{}-{}",
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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) {
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
                        DataType::Float64 => {
                            let value = array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row);
                            json!({"float": if value.is_nan() {
                                "NaN".to_string()
                            } else {
                                format!("{:016x}", if value == 0.0 { 0 } else { value.to_bits() })
                            }})
                        }
                        other => panic!("unexpected output type {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn float_min_max_over_frames_match_duckdb() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/window_float_extremes/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    let fixture = Fixture::new();
    let tables = HashMap::from([("t".to_string(), fixture.0.join("readings.parquet"))]);
    for workers in [1, 4, 10] {
        for batch in [1, 7, 8192] {
            for spill in [false, true] {
                let options = fixture.options(workers, batch, spill);
                for case in cases {
                    let sql = case["sql"].as_str().unwrap();
                    let result = prepare(sql, &tables, &options)
                        .and_then(|p| execute(&p, &options))
                        .unwrap_or_else(|e| panic!("{}: {sql}: {e}", case["id"]));
                    assert_eq!(
                        json!(rows(&result.batch)),
                        case["rows"],
                        "{} workers={workers} batch={batch} spill={spill}",
                        case["id"]
                    );
                    assert_eq!(fs::read_dir(fixture.0.join("spill")).unwrap().count(), 0);
                }
            }
        }
    }
}
