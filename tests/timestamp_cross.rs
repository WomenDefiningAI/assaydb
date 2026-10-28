//! TIMESTAMP/TIMESTAMPTZ combined with the candidate 2 features (integration
//! candidate 3): GREATEST/LEAST, MIN/MAX, ARG_MAX family, COUNT(DISTINCT),
//! FILTER and window frames answer DuckDB 1.5.6's rows; ordered-set aggregates
//! over TIMESTAMP and STRING_AGG ordered by TIMESTAMP refuse explicitly;
//! statistical aggregates reject TIMESTAMP as DuckDB's binder does.
//! Integration candidate 4 adds the window-extras frames over TIMESTAMP keys
//! (GROUPS, EXCLUDE, FOLLOWING starts, named windows, window FILTER/ARG_MAX,
//! PERCENT_RANK/CUME_DIST) and the refused window MEDIAN over TIMESTAMP.
//! Goldens: tools/testing/timestamp_cross_reference.py.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::execute::{Options, execute, prepare};
use serde_json::{Value, json};
use std::{collections::HashMap, path::PathBuf};

fn cell(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
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
        DataType::Float64 => {
            json!(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Boolean => {
            json!(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Timestamp(..) => json!({"micros": array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row)}),
        DataType::Decimal128(_, 0) => json!(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row) as i64
        ),
        other => panic!("unexpected output type {other}"),
    }
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|c| cell(c.as_ref(), row))
                .collect()
        })
        .collect()
}

#[test]
fn timestamps_with_candidate_two_features_match_duckdb_or_refuse() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/timestamp_cross/expected.json")).unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    let tables = HashMap::from([(
        "t".to_string(),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/timestamp_cross/ts.parquet"),
    )]);
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 26);
    for (workers, batch_size) in [(1, 8192), (4, 1), (10, 3)] {
        let options = Options {
            workers,
            batch_size,
            ..Options::default()
        };
        for case in cases {
            let sql = case["sql"].as_str().unwrap();
            let result = prepare(sql, &tables, &options).and_then(|p| execute(&p, &options));
            let context = format!("{} workers={workers} batch={batch_size}", case["id"]);
            if let Some(error) = case["assay_error"].as_str() {
                let actual = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: answered"));
                assert!(actual.starts_with(error), "{context}: {actual}");
                continue;
            }
            let batch = result.unwrap_or_else(|e| panic!("{context}: {e}")).batch;
            assert_eq!(json!(rows(&batch)), case["duckdb_rows"], "{context}");
        }
    }
}
