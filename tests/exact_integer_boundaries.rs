//! Option B: exact precision-38 SQL integer values, BIGINT only at return/store.
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn query(s: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match s.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("expected query: {sql}"),
    }
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Option<i64>>> {
    (0..batch.num_rows())
        .map(|r| {
            batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(r) {
                        None
                    } else if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
                        Some(a.value(r))
                    } else {
                        Some(i64::from(
                            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(r),
                        ))
                    }
                })
                .collect()
        })
        .collect()
}
#[test]
fn wide_values_survive_every_internal_consumer() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let mut s = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    relational_optimizations,
                    batch_size: 2,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            s.execute_sql("CREATE TABLE t(a INTEGER)").unwrap();
            s.execute_sql("INSERT INTO t VALUES(2),(1),(2),(NULL)")
                .unwrap();
            let m = "9223372036854775807";
            for (sql, expected) in [
                (format!("SELECT ({m}+2)-{m}"), vec![vec![Some(2)]]),
                (format!("SELECT (({m}+1)/1)-{m}"), vec![vec![Some(1)]]),
                (format!("SELECT (({m}+1)%({m}+2))-{m}"), vec![vec![Some(1)]]),
                (
                    "SELECT (-9223372036854775808/-1)/2".into(),
                    vec![vec![Some(4611686018427387904)]],
                ),
                (
                    "SELECT NULL+(-9223372036854775808/-1),(-9223372036854775808/-1)%0".into(),
                    vec![vec![None, None]],
                ),
                (
                    format!("SELECT CAST({m}+2 AS INTEGER)-{m}"),
                    vec![vec![Some(2)]],
                ),
                (
                    format!(
                        "SELECT x-{m} FROM (SELECT {m}+a AS x FROM t) AS u ORDER BY 1 NULLS FIRST"
                    ),
                    vec![vec![None], vec![Some(1)], vec![Some(2)], vec![Some(2)]],
                ),
                (
                    format!(
                        "SELECT x/{m} FROM (SELECT {m}+a AS x FROM t) AS u WHERE x>{m} ORDER BY 1 NULLS FIRST"
                    ),
                    vec![vec![Some(1)], vec![Some(1)], vec![Some(1)]],
                ),
                (
                    format!("SELECT SUM({m}+a)-3*{m} FROM t"),
                    vec![vec![Some(5)]],
                ),
                (
                    format!("SELECT MIN({m}+a)-{m},MAX({m}+a)-{m} FROM t"),
                    vec![vec![Some(1), Some(2)]],
                ),
                (
                    format!(
                        "SELECT {m}+a-{m},COUNT(*) FROM t GROUP BY {m}+a ORDER BY 1 NULLS FIRST"
                    ),
                    vec![
                        vec![None, Some(1)],
                        vec![Some(1), Some(1)],
                        vec![Some(2), Some(2)],
                    ],
                ),
                (
                    format!(
                        "SELECT x-{m} FROM (SELECT DISTINCT {m}+a AS x FROM t) AS u ORDER BY 1 NULLS FIRST"
                    ),
                    vec![vec![None], vec![Some(1)], vec![Some(2)]],
                ),
                (
                    format!(
                        "SELECT x-{m} FROM (SELECT {m}+a AS x FROM t ORDER BY x DESC LIMIT 1) AS u"
                    ),
                    vec![vec![Some(2)]],
                ),
                (
                    format!("SELECT a FROM t WHERE {m}+a>{m} ORDER BY a NULLS FIRST"),
                    vec![vec![Some(1)], vec![Some(2)], vec![Some(2)]],
                ),
                (
                    format!("SELECT CASE WHEN {m}+2>{m} THEN ({m}+2)/1-{m} ELSE 0 END"),
                    vec![vec![Some(2)]],
                ),
                (
                    format!("SELECT COALESCE(NULL,({m}+2)/1)-{m}"),
                    vec![vec![Some(2)]],
                ),
                (format!("SELECT ABS(-({m}+2))-{m}"), vec![vec![Some(2)]]),
                (
                    format!("SELECT NULLIF({m}+2,{m}+1)-{m}"),
                    vec![vec![Some(2)]],
                ),
                (
                    format!(
                        "SELECT l.x-{m} FROM (SELECT {m}+a AS x FROM t) AS l INNER JOIN (SELECT {m}+1 AS y) AS r ON l.x=r.y"
                    ),
                    vec![vec![Some(1)]],
                ),
                (
                    format!(
                        "SELECT l.x-{m} FROM (SELECT {m}+a AS x FROM t) AS l CROSS JOIN (SELECT 1 AS y) AS r ORDER BY 1 NULLS FIRST"
                    ),
                    vec![vec![None], vec![Some(1)], vec![Some(2)], vec![Some(2)]],
                ),
                (
                    format!("SELECT COUNT(DISTINCT {m}+a) FROM t"),
                    vec![vec![Some(2)]],
                ),
                (
                    format!("SELECT ({m}+2) IN (SELECT {m}+a FROM t)"),
                    vec![vec![Some(1)]],
                ),
            ] {
                let batch = query(&mut s, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
                assert_eq!(
                    rows(&batch),
                    expected,
                    "workers={workers} optimized={relational_optimizations}: {sql}"
                );
            }
            // Existing syntax limitation remains a declared refusal. The
            // derived-table ORDER BY x case above exercises supported wide sorting.
            let unsupported = query(
                &mut s,
                &format!("SELECT a FROM t ORDER BY {m}+a DESC NULLS LAST LIMIT 2"),
            )
            .unwrap_err();
            assert!(
                unsupported.contains("ORDER BY must name an output or ordinal"),
                "{unsupported}"
            );
            for sql in [
                format!("SELECT {m}+1"),
                format!("SELECT SUM({m}+a) FROM t"),
                "SELECT (99999999999999999999999999999999999999+1)-1".into(),
                "SELECT (99999999999999999999999999999999999999*2)/2".into(),
            ] {
                let e = query(&mut s, &sql).unwrap_err();
                assert!(e.contains("overflow"), "{sql}: {e}");
            }
            // Integer aggregate prefixes cannot leave precision 38 and later
            // be rescued by cancellation, even when the final result fits.
            s.execute_sql("CREATE TABLE huge(x INTEGER)").unwrap();
            s.execute_sql("INSERT INTO huge VALUES(1),(1),(-1),(-1)")
                .unwrap();
            for name in ["SUM", "AVG"] {
                let sql =
                    format!("SELECT {name}(x*99999999999999999999999999999999999999) FROM huge");
                assert!(
                    query(&mut s, &sql).unwrap_err().contains("overflow"),
                    "{sql}"
                );
            }
            // Storage is a genuine BIGINT boundary, and failure is atomic.
            for sql in [
                format!("INSERT INTO t SELECT {m}+1"),
                format!("UPDATE t SET a={m}+a"),
            ] {
                assert!(
                    s.execute_sql(&sql)
                        .err()
                        .expect("storage overflow")
                        .to_string()
                        .contains("overflow"),
                    "{sql}"
                );
                assert_eq!(
                    rows(&query(&mut s, "SELECT a FROM t ORDER BY a NULLS FIRST").unwrap()),
                    vec![vec![None], vec![Some(1)], vec![Some(2)], vec![Some(2)]]
                );
            }
        }
    }
}

#[test]
fn external_decimal_metadata_cannot_impersonate_internal_integer() {
    use arrow::array::Float64Array;
    use assaydb::execute::{execute, prepare};
    use std::collections::HashMap;
    let path = std::env::temp_dir().join(format!(
        "assay-external-wide-marker-{}.parquet",
        std::process::id()
    ));
    // Exact retained file from the unmerged-candidate before/after reproduction.
    std::fs::write(
        &path,
        include_bytes!("fixtures/external-integer-marker.parquet"),
    )
    .unwrap();
    let tables = HashMap::from([("t".into(), path.clone())]);
    for sql in [
        "SELECT x/2 FROM t",
        "SELECT y/2 FROM (SELECT x AS y FROM t) AS u",
    ] {
        for relational_optimizations in [false, true] {
            let options = Options {
                relational_optimizations,
                ..Options::default()
            };
            let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
            assert_eq!(
                result
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                1.5,
                "{sql}"
            );
        }
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn insert_select_converts_exact_source_at_the_target_storage_boundary() {
    let mut s = Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    s.execute_sql("CREATE TABLE d(x DECIMAL(38,0),f REAL)")
        .unwrap();
    s.execute_sql("INSERT INTO d SELECT 9223372036854775807+1,9223372036854775807+1")
        .unwrap();
    let b=query(&mut s,"SELECT CAST(x-9223372036854775807 AS INTEGER),f=CAST(9223372036854775807+1 AS REAL) FROM d").unwrap();
    assert_eq!(rows(&b), vec![vec![Some(1), Some(1)]]);
    s.execute_sql("UPDATE d SET x=9223372036854775807+2,f=9223372036854775807+2")
        .unwrap();
    let b=query(&mut s,"SELECT CAST(x-9223372036854775807 AS INTEGER),f=CAST(9223372036854775807+2 AS REAL) FROM d").unwrap();
    assert_eq!(rows(&b), vec![vec![Some(2), Some(1)]]);
}
