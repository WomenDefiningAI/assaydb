//! Approved Int/Int truncation; Int/Float zero divisors now produce NULL.
use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use parquet::arrow::ArrowWriter;
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

fn ints(batch: &RecordBatch, column: usize) -> Vec<Option<i64>> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer division must expose Int64")
        .iter()
        .collect()
}
fn scalar_contract(run: &mut impl FnMut(&str) -> Result<RecordBatch, assaydb::Error>) {
    // Original pilot SQL, unchanged: expr:29836 and expr:3684.
    for (sql, expected) in [
        ("SELECT 12 / + 16 + - 6", -6),
        ("SELECT 62 + - 49 / + 39", 61),
        ("SELECT 7/3", 2),
        ("SELECT -7/3", -2),
        ("SELECT 7/-3", -2),
        ("SELECT -7/-3", 2),
        ("SELECT -1/2", 0),
        ("SELECT 9007199254740993/1", 9007199254740993),
        ("SELECT 9223372036854775807/1", i64::MAX),
        ("SELECT -9223372036854775808/1", i64::MIN),
        // Existing wider Int intermediates must not be narrowed before division.
        ("SELECT (9223372036854775807+1)/2", 4611686018427387904),
        // Option B: a wide quotient may feed an outer operation.
        ("SELECT (-9223372036854775808/-1)/2", 4611686018427387904),
        ("SELECT CASE WHEN FALSE THEN 1/0 ELSE -7/3 END", -2),
        ("SELECT COALESCE(7/3,1/0)", 2),
    ] {
        assert_eq!(ints(&run(sql).unwrap(), 0), vec![Some(expected)], "{sql}");
    }
    for sql in [
        "SELECT NULL/0",
        "SELECT 1/0",
        "SELECT 1/NULL",
        "SELECT CAST(NULL AS BIGINT)/CAST(NULL AS BIGINT)",
        "SELECT (1/0)+(-9223372036854775808/-1)",
        "SELECT (-9223372036854775808/-1)+(1/0)",
    ] {
        assert_eq!(ints(&run(sql).unwrap(), 0), vec![None], "{sql}");
    }
    {
        // Only the actually returned INTEGER must fit BIGINT.
        let (sql, expected) = ("SELECT -9223372036854775808/-1", "BIGINT overflow");
        let error = run(sql).unwrap_err();
        assert!(error.contains(expected), "{sql}: {error}");
    }
    for sql in [
        "SELECT 7.0/3",
        "SELECT 7/3.0",
        "SELECT CAST('7' AS DECIMAL(10,0))/3",
        "SELECT 7/CAST('3' AS DECIMAL(10,0))",
    ] {
        let batch = run(sql).unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 7.0 / 3.0, "{sql}");
    }
}

fn column_contract(run: &mut impl FnMut(&str) -> Result<RecordBatch, assaydb::Error>) {
    let expected = vec![
        Some(2),
        Some(-2),
        Some(-2),
        Some(2),
        Some(9007199254740993),
        Some(i64::MIN),
        None,
        None,
    ];
    let batch = run("SELECT n/d,id FROM t WHERE id<=8 ORDER BY id").unwrap();
    assert_eq!(ints(&batch, 0), expected);
    let batch =
        run("SELECT CASE WHEN id>=9 THEN NULL ELSE n/d END AS q,id FROM t ORDER BY id").unwrap();
    let mut masked = expected;
    masked.extend([None, None]);
    assert_eq!(ints(&batch, 0), masked);
    {
        let (sql, expected) = ("SELECT n/d FROM t WHERE id=10", "BIGINT overflow");
        let error = run(sql).unwrap_err();
        assert!(error.contains(expected), "{sql}: {error}");
    }
    assert_eq!(
        ints(&run("SELECT n/d FROM t WHERE id=9").unwrap(), 0),
        vec![None]
    );
    // Actual Float and declared scale-zero Decimal columns retain floating division.
    let batch = run("SELECT f/d,n/f,z/d,c/d FROM t WHERE id=1").unwrap();
    for (column, expected) in [7.0 / 3.0, 1.0, 7.0 / 3.0, 7.0 / 3.0]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    let batch = run("SELECT AVG(n) FROM t WHERE id=1 OR id=3").unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        7.0
    );
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-integer-division-{}.parquet",
            std::process::id()
        ));
        let n = vec![
            Some(7),
            Some(-7),
            Some(7),
            Some(-7),
            Some(9007199254740993),
            Some(i64::MIN),
            None,
            Some(7),
            Some(7),
            Some(i64::MIN),
        ];
        let d = vec![
            Some(3),
            Some(3),
            Some(-3),
            Some(-3),
            Some(1),
            Some(1),
            Some(0),
            None,
            Some(0),
            Some(-1),
        ];
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(1..=10)) as ArrayRef,
            ),
            ("n", Arc::new(Int64Array::from(n))),
            ("d", Arc::new(Int64Array::from(d))),
            ("f", Arc::new(Float64Array::from(vec![7.0; 10]))),
            (
                "z",
                Arc::new(
                    Decimal128Array::from(vec![7; 10])
                        .with_precision_and_scale(10, 0)
                        .unwrap(),
                ),
            ),
            (
                "c",
                Arc::new(
                    Decimal128Array::from(vec![700; 10])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), batch.schema(), None).unwrap();
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
fn query_integer_division_scalar_and_column_contract() {
    let fixture = Fixture::new();
    let tables = HashMap::from([("t".into(), fixture.0.clone())]);
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 3,
                ..Options::default()
            };
            let mut run = |sql: &str| {
                prepare(sql, &tables, &options)
                    .and_then(|p| execute(&p, &options))
                    .map(|r| r.batch.clone())
            };
            scalar_contract(&mut run);
            column_contract(&mut run);
        }
    }
}
#[test]
fn native_session_integer_division_and_atomic_update() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let options = Options {
                workers,
                relational_optimizations: optimized,
                batch_size: 3,
                ..Options::default()
            };
            let mut session =
                Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            session
                .execute_sql(
                    "CREATE TABLE t(id INT,n INT,d INT,f DOUBLE,z DECIMAL(10,0),c DECIMAL(10,2))",
                )
                .unwrap();
            session.execute_sql("INSERT INTO t VALUES(1,7,3,7,7,7),(2,-7,3,7,7,7),(3,7,-3,7,7,7),(4,-7,-3,7,7,7),(5,9007199254740993,1,7,7,7),(6,-9223372036854775808,1,7,7,7),(7,NULL,0,7,7,7),(8,7,NULL,7,7,7),(9,7,0,7,7,7),(10,-9223372036854775808,-1,7,7,7)").unwrap();
            {
                let mut run =
                    |sql: &str| match session.execute_sql(sql).map_err(|error| error.message)? {
                        Output::Query { execution, .. } => Ok(execution.batch.clone()),
                        _ => Err("expected query".into()),
                    };
                scalar_contract(&mut run);
                column_contract(&mut run);
            }
            assert!(
                session
                    .execute_sql("UPDATE t SET n=n/d")
                    .err()
                    .unwrap()
                    .message
                    .contains("integer assignment overflow")
            );
            let Output::Query { execution, .. } =
                session.execute_sql("SELECT n FROM t WHERE id=1").unwrap()
            else {
                panic!()
            };
            assert_eq!(ints(&execution.batch, 0), vec![Some(7)]);
            session
                .execute_sql("UPDATE t SET n=n/d WHERE id<=8")
                .unwrap();
            let Output::Query { execution, .. } =
                session.execute_sql("SELECT n FROM t WHERE id=1").unwrap()
            else {
                panic!()
            };
            assert_eq!(ints(&execution.batch, 0), vec![Some(2)]);
        }
    }
}
