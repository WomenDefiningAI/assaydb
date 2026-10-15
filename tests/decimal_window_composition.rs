//! Additive composition probes; requires both DECIMAL phase1 and W1 ranking.
//! The hand-derived fixture is independent of both frozen feature suites.
use arrow::{
    array::{Array, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::SpillOptions,
    types::decimal_text,
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use std::{collections::HashMap, fs, sync::Arc};

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        return Value::Null;
                    }
                    match column.data_type() {
                        DataType::Int64 => json!(
                            column
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Decimal128(_, scale) => json!(decimal_text(
                            column
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .value(row),
                            *scale
                        )),
                        other => panic!("unexpected composition result type: {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn decimal_window_composition_exact_values_errors_and_activated_paths() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/decimal_window_phase1/cases.json")).unwrap();
    let directory =
        std::env::temp_dir().join(format!("assay-decimal-window-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Decimal128(38, 2), true),
        Field::new("p", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(124),
                    Some(125),
                    Some(126),
                    Some(-125),
                    None,
                    Some(125),
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1, 2])),
        ],
    )
    .unwrap();
    let path = directory.join("dw.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, None).unwrap();
    // Six genuine row groups allow the parallel-scan activation control.
    for row in 0..6 {
        writer.write(&batch.slice(row, 1)).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let tables = HashMap::from([("dw".to_string(), path)]);
    for workers in [1, 4] {
        for batch_size in [1, 7] {
            for spill in [false, true] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 16 << 20,
                    spill: spill.then(|| SpillOptions {
                        directory: directory.clone(),
                        sort_run_bytes: 64 << 10,
                        merge_fan_in: 2,
                        ..SpillOptions::default()
                    }),
                    ..Options::default()
                };
                for case in cases["cases"].as_array().unwrap() {
                    let sql = case["sql"].as_str().unwrap();
                    let result =
                        prepare(sql, &tables, &options).and_then(|plan| execute(&plan, &options));
                    if let Some(error) = case["error_contains"].as_str() {
                        let actual = result.err().expect("expected reached cast overflow");
                        assert!(
                            actual.contains(error),
                            "{workers}/{batch_size}/{spill}: {sql}: {actual}"
                        );
                        continue;
                    }
                    let actual = result.unwrap_or_else(|error| {
                        panic!("{workers}/{batch_size}/{spill}: {sql}: {error}")
                    });
                    let expected: Vec<Vec<Value>> =
                        serde_json::from_value(case["rows"].clone()).unwrap();
                    assert_eq!(
                        rows(&actual.batch),
                        expected,
                        "{workers}/{batch_size}/{spill}: {sql}"
                    );
                    assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                    if case["activation_control"] == true {
                        if workers > 1 {
                            assert!(
                                actual.metrics.parallel_scan_tasks > 0,
                                "parallel scan must activate"
                            );
                        }
                        if spill {
                            assert!(
                                actual.metrics.external_sort_runs > 0,
                                "external sort must activate"
                            );
                        }
                    }
                }
            }
        }
    }
}
