// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
use arrow::{
    array::{ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-select-distinct-{}.parquet",
            std::process::id()
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Decimal128(12, 2), true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..8)),
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(1),
                Some(2),
                None,
                None,
                Some(1),
                Some(3),
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                Some("same"),
                Some("a"),
                Some("same"),
                None,
                None,
                Some("A"),
                Some(""),
                Some(""),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&b"\xff"[..]),
                Some(&b"\0"[..]),
                Some(&b"\xff"[..]),
                None,
                None,
                Some(&b"\0"[..]),
                Some(&b""[..]),
                Some(&b""[..]),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(1.0),
                Some(1.0),
                None,
                None,
                Some(2.0),
                Some(2.0),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(100),
                    Some(100),
                    Some(200),
                    Some(200),
                    None,
                    None,
                    Some(0),
                    Some(0),
                ])
                .with_precision_and_scale(12, 2)
                .unwrap(),
            ),
        ];
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn run(&self, sql: &str, options: &Options) -> Result<RecordBatch, String> {
        let tables = HashMap::from([("t".into(), self.0.clone())]);
        Ok(execute(&prepare(sql, &tables, options)?, options)?
            .batch
            .clone())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}
fn ints(batch: &RecordBatch) -> Vec<Option<i64>> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn distinct_projection_grouping_order_limit_and_typed_equality() {
    let fixture = Fixture::new();
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            for batch_size in [2, 3] {
                let options = Options {
                    workers,
                    relational_optimizations,
                    batch_size,
                    ..Options::default()
                };
                for (sql, expected) in [
                    (
                        "SELECT DISTINCT k FROM t ORDER BY k NULLS FIRST",
                        vec![None, Some(1), Some(2), Some(3)],
                    ),
                    (
                        "SELECT DISTINCT k FROM t ORDER BY k DESC NULLS FIRST LIMIT 2",
                        vec![None, Some(3)],
                    ),
                    ("SELECT DISTINCT 7 AS x FROM t LIMIT 5", vec![Some(7)]),
                    (
                        "SELECT DISTINCT k*0 AS z FROM t ORDER BY z NULLS FIRST",
                        vec![None, Some(0)],
                    ),
                    (
                        "SELECT DISTINCT COUNT(*) AS n FROM t GROUP BY k HAVING COUNT(*)>1",
                        vec![Some(2)],
                    ),
                    ("SELECT DISTINCT COUNT(*) FROM t WHERE FALSE", vec![Some(0)]),
                    ("SELECT DISTINCT k FROM t WHERE FALSE", vec![]),
                    ("SELECT DISTINCT CAST(NULL AS BIGINT) FROM t", vec![None]),
                    ("SELECT (SELECT DISTINCT k FROM t WHERE k=2)", vec![Some(2)]),
                    (
                        "SELECT DISTINCT CASE WHEN FALSE THEN CAST(1.0/0 AS INTEGER) ELSE 7 END FROM t",
                        vec![Some(7)],
                    ),
                ] {
                    assert_eq!(
                        ints(&fixture.run(sql, &options).unwrap()),
                        expected,
                        "{sql}"
                    );
                }
                let ordinary = fixture
                    .run("SELECT k FROM t ORDER BY k NULLS FIRST", &options)
                    .unwrap();
                assert_eq!(
                    fixture
                        .run("SELECT ALL k FROM t ORDER BY k NULLS FIRST", &options)
                        .unwrap(),
                    ordinary
                );
                assert_eq!(ordinary.num_rows(), 8);
                let aliases = fixture
                    .run("SELECT DISTINCT k AS x,s AS x FROM t", &options)
                    .unwrap();
                assert_eq!(aliases.num_rows(), 5);
                assert_eq!(aliases.schema().field(0).name(), "x");
                assert_eq!(aliases.schema().field(1).name(), "x");
                let text = fixture
                    .run("SELECT DISTINCT s FROM t ORDER BY s NULLS FIRST", &options)
                    .unwrap();
                assert_eq!(
                    text.column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, Some(""), Some("A"), Some("a"), Some("same")]
                );
                let binary = fixture
                    .run("SELECT DISTINCT b FROM t ORDER BY b NULLS FIRST", &options)
                    .unwrap();
                assert_eq!(
                    binary
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, Some(&b""[..]), Some(&b"\0"[..]), Some(&b"\xff"[..])]
                );
                let floats = fixture
                    .run("SELECT DISTINCT f FROM t ORDER BY f NULLS FIRST", &options)
                    .unwrap();
                assert_eq!(
                    floats
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, Some(0.0), Some(1.0), Some(2.0)]
                );
                let decimals = fixture
                    .run("SELECT DISTINCT d FROM t ORDER BY d NULLS FIRST", &options)
                    .unwrap();
                assert_eq!(
                    decimals
                        .column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, Some(0), Some(100), Some(200)]
                );
                // DISTINCT and LIMIT must not hide a later input expression error.
                let error = fixture
                    .run(
                        "SELECT DISTINCT CAST(1.0/(7-id) AS INTEGER) FROM t LIMIT 1",
                        &options,
                    )
                    .unwrap_err();
                assert!(error.contains("division by zero"), "{error}");
            }
        }
    }
    // DISTINCT ON now has its own key-selection path. Keep a concrete NULL/
    // duplicate result check here alongside the original DISTINCT coverage.
    let on = fixture
        .run(
            "SELECT DISTINCT ON(k) k FROM t ORDER BY k NULLS FIRST",
            &Options::default(),
        )
        .unwrap();
    assert_eq!(
        on.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(1), Some(2), Some(3)]
    );
    assert!(
        fixture
            .run("SELECT DISTINCT missing FROM t", &Options::default())
            .is_err()
    );
    // Approved public untyped NULL output also participates in DISTINCT.
    let nulls = fixture
        .run("SELECT DISTINCT NULL FROM t", &Options::default())
        .unwrap();
    assert_eq!(nulls.num_rows(), 1);
    assert_eq!(nulls.column(0).data_type(), &DataType::Null);
    let cancelled = Options::default();
    cancelled.cancellation.cancel();
    assert!(
        fixture
            .run("SELECT DISTINCT k FROM t", &cancelled)
            .unwrap_err()
            .contains("cancel")
    );
    assert!(
        fixture
            .run(
                "SELECT DISTINCT k FROM t",
                &Options {
                    memory_bytes: 1,
                    ..Options::default()
                }
            )
            .is_err()
    );
}

#[test]
fn native_session_distinct_is_available_in_insert_select_and_scalar_queries() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    for sql in [
        "CREATE TABLE t(k INTEGER)",
        "INSERT INTO t VALUES(1),(1),(NULL),(NULL),(2)",
        "CREATE TABLE u(k INTEGER)",
        "INSERT INTO u SELECT DISTINCT k FROM t",
    ] {
        session.execute_sql(sql).unwrap();
    }
    let Output::Query { execution, .. } = session
        .execute_sql("SELECT k FROM u ORDER BY k NULLS FIRST")
        .unwrap()
    else {
        panic!("query");
    };
    assert_eq!(ints(&execution.batch), vec![None, Some(1), Some(2)]);
    let Output::Query { execution, .. } = session.execute_sql("SELECT DISTINCT 7").unwrap() else {
        panic!("query");
    };
    assert_eq!(ints(&execution.batch), vec![Some(7)]);
}
