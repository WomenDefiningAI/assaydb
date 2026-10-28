//! Old refusal assertions migrated to independent DuckDB 1.5.6 answers after
//! Helen's October 15 Int/Float promotion decision (old refusal -> new answer).
//! Answers: tools/testing/mixed_intfloat_migrated_reference.py. Float cells
//! compare bit for bit; unordered results compare as sorted row multisets.
#![allow(dead_code)]
use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use assaydb::session::{Output, Session};
use serde_json::{Value, json};

const MIGRATED: &str = include_str!("../fixtures/mixed_intfloat/migrated.json");

pub fn group(name: &str) -> Value {
    let all: Value = serde_json::from_str(MIGRATED).unwrap();
    assert_eq!(all["duckdb_version"], "1.5.6");
    let group = all["groups"][name].clone();
    assert!(group.is_object(), "missing migrated group {name}");
    group
}

fn entry<'a>(group: &'a Value, sql: &str) -> &'a Value {
    group["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sql"] == sql)
        .unwrap_or_else(|| panic!("no migrated reference for {sql}"))
}

fn plain(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Bool(b) => json!({ "bool": b }),
        Value::Number(n) if n.is_i64() => json!({ "int": n }),
        Value::Number(n) => {
            let x = n.as_f64().unwrap();
            json!({ "bits": format!("{:016x}", x.to_bits()) })
        }
        Value::String(s) => json!({ "text": s }),
        _ => panic!("unexpected value {value}"),
    }
}

/// Normalize a reference cell to the comparable subset produced by `cells`.
fn reference(value: &Value) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    if let Some(bits) = value.get("bits") {
        return json!({ "bits": bits });
    }
    value.clone()
}

pub fn cells(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    let any = array.as_any();
                    match array.data_type() {
                        DataType::Float64 => json!({ "bits": format!("{:016x}",
                            any.downcast_ref::<Float64Array>().unwrap().value(row).to_bits()) }),
                        DataType::Int64 => {
                            json!({ "int": any.downcast_ref::<Int64Array>().unwrap().value(row) })
                        }
                        DataType::Decimal128(_, 0) => json!({ "int":
                            any.downcast_ref::<Decimal128Array>().unwrap().value(row) as i64 }),
                        DataType::Boolean => {
                            json!({ "bool": any.downcast_ref::<BooleanArray>().unwrap().value(row) })
                        }
                        DataType::Utf8 => {
                            json!({ "text": any.downcast_ref::<StringArray>().unwrap().value(row) })
                        }
                        other => panic!("unhandled result type {other:?}"),
                    }
                })
                .collect()
        })
        .collect()
}

fn run(session: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match session.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("expected a query: {sql}"),
    }
}

/// Run `sql` (after the entry's recorded `pre` statements) and compare it with
/// the migrated reference: DuckDB rows, or a documented Assay override.
pub fn check(session: &mut Session, group: &Value, sql: &str, context: &str) {
    let entry = entry(group, sql);
    for statement in entry["pre"].as_array().unwrap() {
        session
            .execute_sql(statement.as_str().unwrap())
            .unwrap_or_else(|e| panic!("{context}: {statement}: {e}"));
    }
    // The immutable migrated record explicitly preserves the former public
    // answer in its reason. INTEGER is again exact on integer intermediates:
    // 2^63 != 2^63-1, so both rows take the Float-typed ELSE zero. Keep the
    // DuckDB observation intact, and check its strict BIGINT variant separately.
    if sql
        == "WITH q AS (SELECT CAST(9223372036854775808 AS INTEGER) AS x,v FROM t) SELECT CASE WHEN x=9223372036854775807 THEN COALESCE(v,CAST(5 AS REAL))/2 ELSE 0 END FROM q"
    {
        assert!(entry["reason"].as_str().unwrap().contains("[[0.0], [0.0]]"));
        let batch = run(session, sql).unwrap();
        assert_eq!(
            cells(&batch),
            vec![vec![json!({"bits":"0000000000000000"})]; 2],
            "{context}: {sql}"
        );
        let strict = sql.replace("AS INTEGER", "AS BIGINT");
        assert_eq!(
            run(session, &strict).unwrap_err(),
            "engine: bigint out of range"
        );
        return;
    }
    let result = run(session, sql);
    if let Some(refusal) = entry["assay_refusal"].as_str() {
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{context}: {sql}: ok"));
        assert!(
            error.starts_with("unsupported: ") && error.contains(refusal),
            "{context}: {sql}: {error}"
        );
        return;
    }
    if let Some(message) = entry["assay_error"].as_str() {
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{context}: {sql}: ok"));
        assert!(
            !error.starts_with("unsupported:") && error.contains(message),
            "{context}: {sql}: {error}"
        );
        return;
    }
    let batch = result.unwrap_or_else(|e| panic!("{context}: {sql}: {e}"));
    let mut expected: Vec<Vec<Value>> = match entry["assay_rows"].as_array() {
        Some(rows) => rows
            .iter()
            .map(|row| row.as_array().unwrap().iter().map(plain).collect())
            .collect(),
        None => {
            assert_eq!(entry["status"], "ok", "{context}: {sql}");
            entry["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row.as_array().unwrap().iter().map(reference).collect())
                .collect()
        }
    };
    let mut actual = cells(&batch);
    if entry["ordered"] != true {
        actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
    }
    assert_eq!(actual, expected, "{context}: {sql}");
}

/// Every entry of a group, in recorded order.
pub fn sqls(group: &Value) -> Vec<String> {
    group["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["sql"].as_str().unwrap().to_string())
        .collect()
}
