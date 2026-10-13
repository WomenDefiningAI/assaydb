// Zero-NULL approval: deliberate error/laziness markers use checked DECIMAL
// division, with an INTEGER cast where the original scalar type matters.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::{Expr, Input, Kind, Literal, Value},
    memory::Budget,
    session::{Limits, Output, Session},
    types::{Type, binary_literal},
};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
fn path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "assaydb-binary-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = path("input.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Binary, true),
        ]));
        // Slice offsets and arbitrary non-UTF8 bytes are part of the contract.
        let bytes = BinaryArray::from(vec![
            Some(&b"discard"[..]),
            None,
            Some(&b""[..]),
            Some(&b"\xff"[..]),
            Some(&b"\x00\xff"[..]),
            Some(&b"\xff"[..]),
            Some(&b"\xc3\x28"[..]),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])),
                Arc::new(bytes.slice(1, 6)),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn tables(&self) -> HashMap<String, PathBuf> {
        HashMap::from([("t".into(), self.0.clone())])
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}
fn query(sql: &str, tables: &HashMap<String, PathBuf>, options: &Options) -> RecordBatch {
    execute(&prepare(sql, tables, options).unwrap(), options)
        .unwrap()
        .batch
        .clone()
}
fn bytes(batch: &RecordBatch, col: usize) -> Vec<Option<Vec<u8>>> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|x| x.map(<[u8]>::to_vec))
        .collect()
}

#[test]
fn binary_literals_decode_bytes_compare_and_preserve_nulls() {
    let b = query(
        "SELECT X'', X'00ffC328', X'ff'=x'FF', X'00'<X'ff', X'' IS NULL, X'ff'=NULL, COALESCE(NULL,X'ff'), CASE WHEN FALSE THEN X'00' ELSE NULL END",
        &HashMap::new(),
        &Options::default(),
    );
    assert_eq!(bytes(&b, 0), vec![Some(vec![])]);
    assert_eq!(bytes(&b, 1), vec![Some(vec![0, 255, 195, 40])]);
    for (col, expected) in [
        (2, Some(true)),
        (3, Some(true)),
        (4, Some(false)),
        (5, None),
    ] {
        assert_eq!(
            b.column(col)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![expected]
        );
    }
    assert_eq!(bytes(&b, 6), vec![Some(vec![255])]);
    assert_eq!(bytes(&b, 7), vec![None]);
    for malformed in ["f", "xyz", "gg", " 1", "é"] {
        assert!(binary_literal(malformed).is_err());
    }
    for sql in [
        "SELECT X'f'",
        "SELECT X'zz'",
        "SELECT X'31'='1'",
        "SELECT X'31'=1",
        "SELECT MIN(X'00')",
        "SELECT MAX(X'00')",
    ] {
        assert!(
            prepare(sql, &HashMap::new(), &Options::default()).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn binary_keys_group_distinct_and_membership_keep_empty_ff_and_null_separate() {
    let fixture = Fixture::new();
    for workers in [1, 4] {
        for optimize in [false, true] {
            let options = Options {
                workers,
                batch_size: 2,
                relational_optimizations: optimize,
                ..Options::default()
            };
            let b = query(
                "SELECT b AS k,COUNT(*) AS n FROM t GROUP BY b ORDER BY k NULLS FIRST",
                &fixture.tables(),
                &options,
            );
            assert_eq!(
                bytes(&b, 0),
                vec![
                    None,
                    Some(vec![]),
                    Some(vec![0, 255]),
                    Some(vec![195, 40]),
                    Some(vec![255])
                ]
            );
            assert_eq!(
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1, 1, 1, 1, 2]
            );
            let count = query(
                "SELECT COUNT(b),COUNT(DISTINCT b) FROM t",
                &fixture.tables(),
                &options,
            );
            assert_eq!(
                count
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                5
            );
            assert_eq!(
                count
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                4
            );
            let b = query(
                "SELECT i,b IN (SELECT b FROM t WHERE i<=2),b NOT IN (SELECT b FROM t WHERE i<=2) FROM t ORDER BY i",
                &fixture.tables(),
                &options,
            );
            for (col, matched) in [(1, true), (2, false)] {
                assert_eq!(
                    b.column(col)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![
                        None,
                        Some(matched),
                        Some(matched),
                        None,
                        Some(matched),
                        None
                    ]
                );
            }
            let b = query(
                "SELECT COUNT(*) FROM t WHERE b IN (SELECT b FROM t WHERE i<=2)",
                &fixture.tables(),
                &options,
            );
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                3
            );
            let b = query(
                "SELECT i,COALESCE(b,X'00') FROM t ORDER BY i",
                &fixture.tables(),
                &options,
            );
            assert_eq!(
                bytes(&b, 1),
                vec![
                    Some(vec![0]),
                    Some(vec![]),
                    Some(vec![255]),
                    Some(vec![0, 255]),
                    Some(vec![255]),
                    Some(vec![195, 40])
                ]
            );
        }
    }
}

#[test]
fn binary_literal_non_power_of_two_payload_is_fully_funded() {
    let budget = Budget::new(13_000);
    let literal = Expr {
        ty: Type::Binary,
        kind: Kind::Literal(Literal::Binary(vec![255; 4096])),
    };
    let value = literal
        .eval(
            &Input {
                len: 3,
                columns: vec![],
            },
            &budget,
        )
        .unwrap();
    assert!(budget.used() >= value.array.get_array_memory_size());
    let array = value.array.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(array.len(), 3);
    assert!(
        array
            .iter()
            .all(|value| value == Some(&vec![255; 4096][..]))
    );
    drop(value);
    assert_eq!(budget.used(), 0);

    // The selected arm has three rows, rather than the old power-of-two
    // fixture. Its payload stays funded while the other arm and concat run.
    let budget = Budget::new(120_000);
    let input = Input {
        len: 4,
        columns: vec![Some(
            Value::adopt(
                Arc::new(BooleanArray::from(vec![true, false, true, true])),
                &budget,
            )
            .unwrap(),
        )],
    };
    let entry = budget.used();
    let expr = Expr {
        ty: Type::Binary,
        kind: Kind::Case(
            vec![(
                Expr {
                    ty: Type::Bool,
                    kind: Kind::Column(0),
                },
                literal,
            )],
            Box::new(Expr {
                ty: Type::Binary,
                kind: Kind::Literal(Literal::Binary(vec![0; 4096])),
            }),
        ),
    };
    let value = expr.eval(&input, &budget).unwrap();
    assert!(budget.used() >= entry + value.array.get_array_memory_size());
    let array = value.array.as_any().downcast_ref::<BinaryArray>().unwrap();
    for (row, byte) in [255, 0, 255, 255].into_iter().enumerate() {
        assert_eq!(array.value(row), vec![byte; 4096]);
    }
    drop(value);
    assert_eq!(budget.used(), entry);
    drop(input);
    assert_eq!(budget.used(), 0);
}

#[test]
fn binary_case_retains_payload_credit_and_failure_releases_it() {
    let overflow = Expr {
        ty: Type::Binary,
        kind: Kind::Literal(Literal::Binary(vec![0, 255])),
    };
    let budget = Budget::new(4096);
    let error = overflow
        .eval(
            &Input {
                len: i32::MAX as usize,
                columns: vec![],
            },
            &budget,
        )
        .err()
        .unwrap();
    assert!(error.contains("32-bit offsets"));
    assert_eq!(budget.used(), 0);
    assert!(Type::from_arrow(&DataType::LargeBinary).is_err());
    let b = query(
        "SELECT CASE WHEN TRUE THEN X'ff' ELSE CASE WHEN CAST(1.0/0 AS INTEGER)=0 THEN X'00' ELSE X'01' END END",
        &HashMap::new(),
        &Options::default(),
    );
    assert_eq!(bytes(&b, 0), vec![Some(vec![255])]);
    for limit in [120_000, 8_192] {
        let budget = Budget::new(limit);
        let input = Input {
            len: 4,
            columns: vec![Some(
                Value::adopt(
                    Arc::new(BooleanArray::from(vec![true, false, true, false])),
                    &budget,
                )
                .unwrap(),
            )],
        };
        let entry = budget.used();
        let literal = |byte| Expr {
            ty: Type::Binary,
            kind: Kind::Literal(Literal::Binary(vec![byte; 4096])),
        };
        let expr = Expr {
            ty: Type::Binary,
            kind: Kind::Case(
                vec![(
                    Expr {
                        ty: Type::Bool,
                        kind: Kind::Column(0),
                    },
                    literal(255),
                )],
                Box::new(literal(0)),
            ),
        };
        let result = expr.eval(&input, &budget);
        if limit > 10_000 {
            let value = result.unwrap();
            assert!(budget.used() >= entry + value.array.get_array_memory_size());
            let array = value.array.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(array.value(0), vec![255; 4096]);
            assert_eq!(array.value(1), vec![0; 4096]);
            drop(value);
        } else {
            assert!(result.err().unwrap().starts_with("resource-limit:"));
        }
        assert_eq!(budget.used(), entry);
        drop(input);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn binary_catalog_and_mixed_in_remain_unsupported_without_mutation() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    assert_eq!(
        session
            .execute_sql("CREATE TABLE blobs(b BLOB)")
            .err()
            .unwrap()
            .kind,
        "unsupported"
    );
    session.execute_sql("CREATE TABLE t(i INTEGER)").unwrap();
    for sql in [
        "SELECT X'303132' IN (SELECT i FROM t)",
        "SELECT X'303132' NOT IN (SELECT i FROM t)",
    ] {
        assert_eq!(session.execute_sql(sql).err().unwrap().kind, "unsupported");
    }
    session
        .execute_sql("CREATE TABLE text_values(v TEXT)")
        .unwrap();
    for sql in [
        "INSERT INTO text_values VALUES (X'ff')",
        "INSERT INTO text_values SELECT X'ff'",
    ] {
        assert!(session.execute_sql(sql).is_err());
    }
    let Output::Query { execution, .. } = session
        .execute_sql("SELECT COUNT(*) FROM text_values")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        execution
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[test]
fn binary_wire_is_typed_hex_in_both_executables() {
    let directory = path("protocol");
    fs::create_dir(&directory).unwrap();
    let mut session = Command::new(env!("CARGO_BIN_EXE_assaydb-session"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = session.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"op":"open","directory":directory})
    )
    .unwrap();
    let sql = "SELECT X'00FFC328',X'',CASE WHEN FALSE THEN X'00' ELSE NULL END";
    writeln!(stdin, "{}", serde_json::json!({"sql":sql})).unwrap();
    drop(stdin);
    let output = session.wait_with_output().unwrap();
    assert!(output.status.success());
    let replies = String::from_utf8(output.stdout).unwrap();
    let result: serde_json::Value = serde_json::from_str(replies.lines().nth(1).unwrap()).unwrap();
    assert_eq!(result["status"], "ok");
    let expected = serde_json::json!([["00ffc328", "", null]]);
    assert_eq!(result["result"]["rows"], expected);
    for c in result["result"]["schema"].as_array().unwrap() {
        assert_eq!(c["type"], "binary");
    }
    let result_path = directory.join("result.json");
    let mut cli = Command::new(env!("CARGO_BIN_EXE_assaydb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = cli.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"sql":sql,"output":result_path})
    )
    .unwrap();
    drop(stdin);
    assert!(cli.wait().unwrap().success());
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(result_path).unwrap()).unwrap();
    assert_eq!(result["rows"], expected);
    for c in result["schema"].as_array().unwrap() {
        assert_eq!(c["type"], "binary");
    }
    fs::remove_dir_all(directory).unwrap();
}
