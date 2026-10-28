// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
//! Supplemental approved casts; upstream corpus and TPC-H SQL remain immutable.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    expr::{Expr, Input, Kind, Value, normalize_input},
    memory::Budget,
    session::{Limits, Output, Session},
    types::Type,
};
use parquet::arrow::ArrowWriter;
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

fn ints(b: &RecordBatch) -> Vec<Option<i64>> {
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn contract(run: &mut impl FnMut(&str) -> Result<RecordBatch, assaydb::Error>) {
    // Restore the retained pre-dialect public numeric contract assertions
    // (03d483cb), per the owner's October 21 compatibility decision.
    for (sql, expected) in [
        ("SELECT CAST('  +123e+5junk' AS INTEGER)", 123),
        ("SELECT CAST('-123.99' AS INTEGER)", -123),
        ("SELECT CAST('.9' AS INTEGER)", 0),
        ("SELECT CAST('0x12' AS INTEGER)", 0),
        ("SELECT CAST('not a number' AS INTEGER)", 0),
        ("SELECT CAST('9223372036854775808999' AS INTEGER)", i64::MAX),
        (
            "SELECT CAST('-9223372036854775808999' AS INTEGER)",
            i64::MIN,
        ),
        ("SELECT CAST(-7.9 AS INTEGER)", -7),
        ("SELECT CAST(CAST('-7.9' AS REAL) AS INTEGER)", -7),
        ("SELECT CAST(CAST('1e100' AS REAL) AS INTEGER)", i64::MAX),
        ("SELECT CAST(CAST('-1e100' AS REAL) AS INTEGER)", i64::MIN),
        ("SELECT CAST(9007199254740993 AS INTEGER)", 9007199254740993),
        // Option B: INTEGER-to-INTEGER is identity, not saturation.
        (
            "SELECT CAST(9223372036854775807+1 AS INTEGER)-9223372036854775807",
            1,
        ),
        ("SELECT CAST(TRUE AS INTEGER)", 1),
        ("SELECT CAST(FALSE AS INT)", 0),
        ("SELECT CAST(X'20202d31323300616263ff' AS INTEGER)", -123),
        ("SELECT CAST('12' AS INTEGER)+1", 13),
        (
            "SELECT CASE WHEN FALSE THEN CAST(CAST(1.0/0 AS INTEGER) AS INTEGER) ELSE 4 END",
            4,
        ),
        (
            "SELECT COALESCE(CAST('7tail' AS INTEGER),CAST(1.0/0 AS INTEGER))",
            7,
        ),
    ] {
        assert_eq!(ints(&run(sql).unwrap()), vec![Some(expected)], "{sql}");
    }
    let error = run("SELECT CAST(9223372036854775807+1 AS INTEGER)").unwrap_err();
    assert!(error.contains("BIGINT overflow"), "{error}");
    for sql in ["SELECT CAST(NULL AS INTEGER)", "SELECT CAST(NULL AS INT)"] {
        assert_eq!(ints(&run(sql).unwrap()), vec![None]);
    }
    for (text, expected) in [
        ("  +123e+5junk", 12300000.0),
        ("-.125tail", -0.125),
        ("1.e2tail", 100.0),
        ("12e+tail", 12.0),
        ("12.5e-tail", 12.5),
        ("no", 0.0),
        ("0x12", 0.0),
        ("+", 0.0),
        (".e2", 0.0),
        ("9007199254740993", 9007199254740992.0),
        ("1e-999", 0.0),
    ] {
        let b = run(&format!("SELECT CAST('{text}' AS REAL)")).unwrap();
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            expected,
            "{text}"
        );
    }
    for (sql, expected) in [
        ("SELECT CAST(X'2d2e356532ff' AS REAL)", -50.0),
        ("SELECT CAST(7 AS REAL)", 7.0),
        ("SELECT CAST(1.25 AS REAL)", 1.25),
        ("SELECT CAST(TRUE AS REAL)", 1.0),
    ] {
        let b = run(sql).unwrap();
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    for (sql, expected) in [
        ("SELECT CAST(9007199254740993 AS TEXT)", "9007199254740993"),
        (
            "SELECT CAST(-9223372036854775808 AS TEXT)",
            "-9223372036854775808",
        ),
        ("SELECT CAST(FALSE AS TEXT)", "false"),
        ("SELECT CAST(1.25 AS TEXT)", "1.25"),
        ("SELECT CAST('  é ' AS TEXT)", "  é "),
        ("SELECT CAST(X'6100c3a9' AS TEXT)", "a\0é"),
    ] {
        let b = run(sql).unwrap();
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            expected
        );
    }
    for (sql, ty) in [
        ("SELECT CAST(NULL AS REAL)", DataType::Float64),
        ("SELECT CAST(NULL AS TEXT)", DataType::Utf8),
    ] {
        let b = run(sql).unwrap();
        assert_eq!(b.column(0).data_type(), &ty);
        assert!(b.column(0).is_null(0));
    }
    // REAL-to-TEXT was refused until Oct 16; DuckDB 1.5.6 answers 1.25 for
    // CAST(CAST('1.25' AS DOUBLE) AS VARCHAR) (and for REAL), now matched.
    assert_eq!(
        run("SELECT CAST(CAST('1.25' AS REAL) AS TEXT)")
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "1.25"
    );
    for sql in ["SELECT CAST(X'ff' AS TEXT)", "SELECT CAST('1e999' AS REAL)"] {
        assert!(run(sql).unwrap_err().contains("unsupported:"), "{sql}");
    }
    assert!(
        run("SELECT CAST(CAST(1.0/0 AS INTEGER) AS INTEGER)")
            .unwrap_err()
            .contains("division by zero")
    );
    // Existing unapproved casts keep their strict contracts.
    assert!(run("SELECT CAST('12tail' AS BIGINT)").is_err());
    // Phase 1 now supports exact DECIMAL expressions; this supplemental
    // capability control keeps its SQL and checks the exact scaled value.
    assert_eq!(
        run("SELECT CAST(1.2 AS DECIMAL(10,1))")
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        12
    );
    let b = run("SELECT CAST('1.25' AS DECIMAL(10,2))").unwrap();
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        125
    );
}
#[test]
fn scalar_casts_query_and_session_preserve_types_masks_and_errors() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                ..Options::default()
            };
            contract(&mut |sql| {
                prepare(sql, &HashMap::new(), &options)
                    .and_then(|p| execute(&p, &options))
                    .map(|e| e.batch.clone())
            });
            let mut s = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            contract(&mut |sql| match s.execute_sql(sql).map_err(|error| {
                // Shared data errors are ordinary execution errors, never
                // refusals; compare the message with the direct path.
                if error.message.starts_with("invalid input syntax")
                    || error.message.ends_with("out of range")
                {
                    assert_eq!(error.kind, "engine", "{sql}: {error}");
                }
                error.message
            })? {
                Output::Query { execution, .. } => Ok(execution.batch.clone()),
                _ => Err("expected query".into()),
            });
        }
    }
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("assaydb-casts-{}.parquet", std::process::id()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    Some("12tail"),
                    Some(" -7.9"),
                    None,
                    Some("\t\n\u{000b}\u{000c}\r 9"),
                    Some("999999999999999999999999"),
                    Some("bad"),
                ])),
                Arc::new(BinaryArray::from(vec![
                    Some(&b"+3x"[..]),
                    Some(&b"-8e2"[..]),
                    None,
                    Some(&b"\xff"[..]),
                    Some(&b"17\0rest"[..]),
                    Some(&b"2.5!"[..]),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(7.9),
                    Some(-7.9),
                    None,
                    Some(1e100),
                    Some(-1e100),
                    Some(-0.0),
                ])),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
#[test]
fn column_casts_pruning_case_and_real_input_saturation() {
    let f = Fixture::new();
    let tables = HashMap::from([("t".into(), f.0.clone())]);
    for workers in [1, 10] {
        for optimized in [false, true] {
            let o = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 2,
                ..Options::default()
            };
            for (expr, expected) in [
                (
                    "CAST(s AS INTEGER)",
                    vec![Some(12), Some(-7), None, Some(9), Some(i64::MAX), Some(0)],
                ),
                (
                    "CAST(b AS INTEGER)",
                    vec![Some(3), Some(-8), None, Some(0), Some(17), Some(2)],
                ),
                (
                    "CAST(f AS INTEGER)",
                    vec![
                        Some(7),
                        Some(-7),
                        None,
                        Some(i64::MAX),
                        Some(i64::MIN),
                        Some(0),
                    ],
                ),
            ] {
                let p =
                    prepare(&format!("SELECT {expr},id FROM t ORDER BY id"), &tables, &o).unwrap();
                assert_eq!(ints(&execute(&p, &o).unwrap().batch), expected);
            }
            let p=prepare("SELECT CASE WHEN id=4 THEN 'masked' ELSE CAST(b AS TEXT) END,id FROM t ORDER BY id",&tables,&o).unwrap();
            let b = execute(&p, &o).unwrap().batch;
            assert_eq!(
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(3),
                "masked"
            );
            let p = prepare(
                "SELECT CAST(s AS INTEGER) FROM t WHERE CAST(s AS INTEGER)=12",
                &tables,
                &o,
            )
            .unwrap();
            assert_eq!(ints(&execute(&p, &o).unwrap().batch), vec![Some(12)]);
        }
    }
}
#[test]
fn cast_funding_retains_output_and_releases_failed_utf8_or_credit() {
    let budget = Budget::new(2 << 20);
    let source: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"valid"[..]),
        Some(&b"\xff"[..]),
    ]));
    let input = Input {
        columns: vec![Some(Value::adopt(source, &budget).unwrap())],
        len: 2,
    };
    let expr = Expr {
        kind: Kind::Cast(Box::new(Expr {
            kind: Kind::Column(0),
            ty: Type::Binary,
        })),
        ty: Type::Text,
    };
    let before = budget.used();
    assert!(expr.eval(&input, &budget).err().unwrap().contains("UTF-8"));
    assert_eq!(budget.used(), before);
    drop(input);
    assert_eq!(budget.used(), 0);
    let text = "é".repeat(4096);
    let source: ArrayRef = Arc::new(BinaryArray::from(vec![Some(text.as_bytes()); 3]));
    let input = Input {
        columns: vec![Some(Value::adopt(source, &budget).unwrap())],
        len: 3,
    };
    let before = budget.used();
    let blocker = budget.reserve(budget.limit() - before - 1024).unwrap();
    assert!(
        expr.eval(&input, &budget)
            .err()
            .unwrap()
            .contains("resource-limit: accounted memory")
    );
    drop(blocker);
    assert_eq!(budget.used(), before);
    let output = expr.eval(&input, &budget).unwrap();
    drop(input);
    assert!(budget.used() >= output.array.get_array_memory_size());
    assert_eq!(
        output
            .array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(2),
        text
    );
    drop(output);
    assert_eq!(budget.used(), 0);
    // Option B: normalized INTEGER-to-INTEGER conversion is identity.
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![i128::from(i64::MAX) + 1])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let input = Input {
        columns: vec![Some(normalize_input(&values, &Type::Int, &budget).unwrap())],
        len: 1,
    };
    let e = Expr {
        kind: Kind::Cast(Box::new(Expr {
            kind: Kind::Column(0),
            ty: Type::Int,
        })),
        ty: Type::Int,
    };
    let output = e.eval(&input, &budget).unwrap();
    assert_eq!(
        output
            .array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        i128::from(i64::MAX) + 1
    );
    drop(output);
    drop(input);
    assert_eq!(budget.used(), 0);
}
