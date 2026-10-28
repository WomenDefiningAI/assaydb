//! Original, hand-derived controls for the public INTEGER/INT/REAL contract.
//! The private 593-record replay is separate; these fixtures contain none of it.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

fn rows(batch: &RecordBatch) -> Value {
    json!(
        (0..batch.num_rows())
            .map(|i| batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(i) {
                        return Value::Null;
                    }
                    if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
                        return json!(a.value(i));
                    }
                    if let Some(a) = a.as_any().downcast_ref::<Float64Array>() {
                        return json!(a.value(i));
                    }
                    panic!("unexpected type {:?}", a.data_type());
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    )
}

fn check(session: &mut Session, sql: &str, expected: Value) {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("query expected");
    };
    assert_eq!(rows(&execution.batch), expected, "{sql}");
}

#[test]
fn public_conversions_preserve_prefixes_truncation_saturation_and_nulls() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for relational_optimizations in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
                        relational_optimizations,
                        memory_bytes: 256 << 20,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                for target in ["INTEGER", "INT"] {
                    for (expression, expected) in [
                        ("''", json!(0)),
                        ("'not a number'", json!(0)),
                        ("'  -12.75tail'", json!(-12)),
                        ("'+7e2'", json!(7)),
                        ("'0x10'", json!(0)),
                        ("'+'", json!(0)),
                        ("'9223372036854775808'", json!(i64::MAX)),
                        ("'-9223372036854775809'", json!(i64::MIN)),
                        ("CAST('9223372036854775806' AS REAL)", json!(i64::MAX)),
                        ("CAST('9223372036854775807' AS REAL)", json!(i64::MAX)),
                        ("CAST('-9223372036854777856' AS REAL)", json!(i64::MIN)),
                        ("CAST('12.75' AS REAL)", json!(12)),
                        ("CAST('-12.75' AS REAL)", json!(-12)),
                        ("CAST('3.5' AS REAL)", json!(3)),
                        ("CAST('-3.5' AS REAL)", json!(-3)),
                        ("CAST('3.5' AS DECIMAL(8,1))", json!(3)),
                        ("CAST('-3.5' AS DECIMAL(8,1))", json!(-3)),
                        ("NULL", Value::Null),
                    ] {
                        check(
                            &mut session,
                            &format!("SELECT CAST({expression} AS {target})"),
                            json!([[expected]]),
                        );
                    }
                }
                for (expression, expected) in [
                    ("''", json!(0.0)),
                    ("'not a number'", json!(0.0)),
                    ("'  -12.75tail'", json!(-12.75)),
                    ("'+7e2tail'", json!(700.0)),
                    ("'0x10'", json!(0.0)),
                    ("'NaN'", json!(0.0)),
                    ("'Infinity'", json!(0.0)),
                    ("NULL", Value::Null),
                ] {
                    check(
                        &mut session,
                        &format!("SELECT CAST({expression} AS REAL)"),
                        json!([[expected]]),
                    );
                }
                session
                    .execute_sql("CREATE TABLE c(id INTEGER,s TEXT,f REAL,d DECIMAL(8,1))")
                    .unwrap();
                session.execute_sql("INSERT INTO c VALUES (0,'bad',3.5,3.5),(1,' -8tail',-3.5,-3.5),(2,NULL,NULL,NULL)").unwrap();
                check(
                    &mut session,
                    "SELECT id, CAST(s AS INTEGER), CAST(f AS INTEGER), CAST(d AS INTEGER) FROM c ORDER BY id",
                    json!([[0, 0, 3, 3], [1, -8, -3, -3], [2, null, null, null]]),
                );
                check(
                    &mut session,
                    "SELECT id, CAST(s AS REAL) FROM c ORDER BY id",
                    json!([[0, 0.0], [1, -8.0], [2, null]]),
                );
                // A permissive zero participates normally in guards; it does
                // not become a fabricated parse error, even on selected rows.
                check(
                    &mut session,
                    "SELECT id FROM c WHERE CAST(s AS INTEGER)=0 OR id=1 ORDER BY id",
                    json!([[0], [1]]),
                );
                check(
                    &mut session,
                    "SELECT CASE WHEN id=0 THEN CAST(s AS INTEGER) ELSE 9 END FROM c ORDER BY id",
                    json!([[0], [9], [9]]),
                );
                check(
                    &mut session,
                    "SELECT id FROM c WHERE FALSE AND CAST(s AS INTEGER)>0",
                    json!([]),
                );
                check(
                    &mut session,
                    "SELECT id FROM c WHERE TRUE AND CAST(s AS INTEGER)=0 ORDER BY id",
                    json!([[0]]),
                );
                check(
                    &mut session,
                    "SELECT id FROM c WHERE NULL AND CAST(s AS INTEGER)>0",
                    json!([]),
                );
                check(
                    &mut session,
                    "SELECT id FROM c WHERE CAST(s AS INTEGER)=0 OR NULL ORDER BY id",
                    json!([[0]]),
                );
                // New checked dialect spellings are not silently broadened.
                assert_eq!(
                    session
                        .execute_sql("SELECT CAST('bad' AS BIGINT)")
                        .err()
                        .unwrap()
                        .message,
                    "invalid input syntax for type bigint: \"bad\""
                );
                assert_eq!(
                    session
                        .execute_sql("SELECT CAST('bad' AS DOUBLE)")
                        .err()
                        .unwrap()
                        .message,
                    "invalid input syntax for type double precision: \"bad\""
                );
                assert_eq!(
                    session
                        .execute_sql("SELECT CAST('1e999' AS REAL)")
                        .err()
                        .unwrap()
                        .message,
                    "unsupported: REAL cast nonfinite result"
                );
                assert_eq!(
                    session
                        .execute_sql("SELECT 10.0 / CAST('bad' AS INTEGER)")
                        .err()
                        .unwrap()
                        .message,
                    "division by zero"
                );
            }
        }
    }
}

/// These are actual IEEE Float64 source values, not text spellings such as
/// 'NaN' that the permissive public REAL parser converts to numeric zero.
#[test]
fn actual_float64_nonfinite_columns_keep_public_conversion_boundaries() {
    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                std::fs::remove_file(&self.0).unwrap();
            }
        }
    }
    let path = std::env::temp_dir().join(format!(
        "assay-public-cast-nonfinite-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(3.75),
                None,
            ])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let fixture = Fixture(path);
    let tables = HashMap::from([("t".to_owned(), fixture.0.clone())]);
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for relational_optimizations in [false, true] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations,
                    memory_bytes: 256 << 20,
                    ..Options::default()
                };
                let run =
                    |sql: &str| prepare(sql, &tables, &options).and_then(|p| execute(&p, &options));
                for target in ["INTEGER", "INT"] {
                    let sql = format!("SELECT CAST(f AS {target}) FROM t WHERE id>0 ORDER BY id");
                    assert_eq!(
                        rows(&run(&sql).unwrap().batch),
                        json!([[i64::MAX], [i64::MIN], [3], [null]]),
                        "{sql}"
                    );
                    let sql = format!("SELECT CAST(f AS {target}) FROM t WHERE id=0");
                    assert_eq!(
                        run(&sql).err().unwrap(),
                        "unsupported: cast from NaN",
                        "{sql}"
                    );
                }
                assert_eq!(
                    run("SELECT CAST(f AS REAL) FROM t WHERE id=0")
                        .err()
                        .unwrap(),
                    "unsupported: cast from NaN"
                );
                for id in [1, 2] {
                    assert_eq!(
                        run(&format!("SELECT CAST(f AS REAL) FROM t WHERE id={id}"))
                            .err()
                            .unwrap(),
                        "unsupported: REAL cast nonfinite result"
                    );
                    assert_eq!(
                        run(&format!("SELECT CAST(f AS BIGINT) FROM t WHERE id={id}"))
                            .err()
                            .unwrap(),
                        "bigint out of range"
                    );
                }
                assert_eq!(rows(&run("SELECT CASE WHEN id=0 THEN 17 ELSE CAST(f AS INTEGER) END FROM t ORDER BY id").unwrap().batch), json!([[17],[i64::MAX],[i64::MIN],[3],[null]]));
                assert_eq!(
                    run("SELECT CAST(f AS INTEGER) FROM t WHERE FALSE")
                        .unwrap()
                        .batch
                        .num_rows(),
                    0
                );
            }
        }
    }
}
