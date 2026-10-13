//! Nested DML must be refused before it changes either INSERT catalog snapshot.
use arrow::{
    array::{Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use std::fs;

fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    match session.execute_sql(sql).unwrap() {
        Output::Query { execution, .. } => execution.batch.clone(),
        _ => panic!("expected query output"),
    }
}

fn count(session: &mut Session, table: &str) -> i64 {
    query(session, &format!("SELECT COUNT(*) FROM {table}"))
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn contents(session: &mut Session, table: &str) -> Vec<(Option<i64>, Option<String>)> {
    let batch = query(session, &format!("SELECT x,y FROM {table} ORDER BY x"));
    let xs = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ys = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                (!xs.is_null(row)).then(|| xs.value(row)),
                (!ys.is_null(row)).then(|| ys.value(row).to_owned()),
            )
        })
        .collect()
}

fn catalog_bytes(session: &Session) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(session.directory())
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                fs::read(path).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn exact_minimal_nested_insert_refuses_without_mutation_or_connection_loss() {
    let mut session = session();
    let statements = include_str!("fixtures/nested-insert/minimal.sql")
        .split(';')
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(statements.len(), 2);
    session.execute_sql(statements[0]).unwrap();
    let before = catalog_bytes(&session);
    let error = session.execute_sql(statements[1]).err().unwrap();
    assert_eq!(error.kind, "unsupported");
    assert!(error.message.contains("INSERT source must be read-only"));
    assert_eq!(catalog_bytes(&session), before);
    assert_eq!(count(&mut session, "t1"), 0);
    session
        .execute_sql("INSERT INTO t1 VALUES(1,'alive')")
        .unwrap();
    assert_eq!(
        contents(&mut session, "t1"),
        vec![(Some(1), Some("alive".into()))]
    );
}

#[test]
fn nested_insert_update_delete_replace_sources_preserve_both_tables() {
    let mut session = session();
    session
        .execute_sql("CREATE TABLE t1(x INT PRIMARY KEY,y TEXT)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE dst(x INT,y TEXT)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES(1,'original'),(2,'second')")
        .unwrap();
    session
        .execute_sql("INSERT INTO dst VALUES(9,'destination')")
        .unwrap();
    let before_files = catalog_bytes(&session);
    let before_memory = session.catalog_bytes();
    let source_rows = contents(&mut session, "t1");
    let destination_rows = contents(&mut session, "dst");
    for sql in [
        "INSERT INTO t1 INSERT INTO t1 VALUES(3,'nested')",
        "INSERT INTO dst INSERT INTO t1 VALUES(3,'nested')",
        "INSERT INTO dst UPDATE t1 SET y='changed' WHERE x=1",
        "INSERT INTO dst DELETE FROM t1",
        "INSERT INTO dst (INSERT INTO t1 VALUES(3,'nested'))",
        "INSERT INTO dst WITH unused AS (SELECT 1) INSERT INTO t1 VALUES(3,'nested')",
        "INSERT INTO dst WITH changed AS (DELETE FROM t1) SELECT x,y FROM t1",
        "INSERT OR REPLACE INTO t1 INSERT OR REPLACE INTO t1 VALUES(1,'replaced')",
        "REPLACE INTO t1 INSERT INTO t1 VALUES(3,'nested')",
        "INSERT INTO dst INSERT INTO dst INSERT INTO t1 VALUES(3,'nested')",
        "INSERT INTO dst SELECT x,y FROM t1 UNION ALL (INSERT INTO t1 VALUES(3,'nested'))",
    ] {
        let error = session.execute_sql(sql).err().unwrap();
        assert_eq!(error.kind, "unsupported", "{sql}: {error}");
        if error.message.starts_with("SQL parser:") {
            // Some statement-shaped variants are refused by the pinned parser
            // before source validation. Preserve that distinction; do not add
            // parser support merely to make them reach the new guard.
            assert!(
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect, sql,)
                    .is_err()
            );
        } else {
            assert!(
                error.message.contains("INSERT source must be read-only"),
                "{sql}: {error}"
            );
        }
        assert_eq!(session.catalog_bytes(), before_memory, "{sql}");
        assert_eq!(catalog_bytes(&session), before_files, "{sql}");
        assert_eq!(contents(&mut session, "t1"), source_rows, "{sql}");
        assert_eq!(contents(&mut session, "dst"), destination_rows, "{sql}");
    }
}

#[test]
fn adjacent_query_cte_subquery_and_ctas_routes_do_not_execute_dml() {
    let mut session = session();
    session
        .execute_sql("CREATE TABLE t1(x INT,y TEXT)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES(1,'original')")
        .unwrap();
    let before = catalog_bytes(&session);
    for sql in [
        "WITH changed AS (INSERT INTO t1 VALUES(2,'nested')) SELECT * FROM t1",
        "SELECT * FROM (INSERT INTO t1 VALUES(2,'nested')) q",
        "SELECT (DELETE FROM t1)",
        "SELECT 1 UNION ALL (DELETE FROM t1)",
        "CREATE TABLE copied AS INSERT INTO t1 VALUES(2,'nested')",
        "INSERT INTO t1 SELECT * FROM (DELETE FROM t1) q",
        "INSERT INTO t1 VALUES((DELETE FROM t1),'nested')",
    ] {
        let error = session.execute_sql(sql).err().unwrap();
        assert_eq!(error.kind, "unsupported", "{sql}: {error}");
        assert_eq!(catalog_bytes(&session), before, "{sql}");
        assert_eq!(
            contents(&mut session, "t1"),
            vec![(Some(1), Some("original".into()))],
            "{sql}"
        );
    }
}

#[test]
fn ordinary_insert_values_select_cte_union_and_self_snapshot_keep_working() {
    let mut session = session();
    session
        .execute_sql("CREATE TABLE t1(x INT,y TEXT)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES(1,'first')")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 SELECT x+1,y FROM t1")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 WITH q AS (SELECT x+10 AS x,y FROM t1) SELECT x,y FROM q")
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 SELECT 100,'a' UNION ALL SELECT 101,'b'")
        .unwrap();
    assert_eq!(count(&mut session, "t1"), 6);
    session
        .execute_sql("INSERT INTO t1 SELECT * FROM t1")
        .unwrap();
    assert_eq!(count(&mut session, "t1"), 12);
}

#[test]
fn original_eight_request_fuzz_artifact_no_longer_panics_and_cleans_up() {
    let bytes = include_bytes!("fixtures/nested-insert/original-fuzz-input.bin");
    assert_eq!(bytes.len(), 212);
    let flags = bytes[0];
    let options = Options {
        workers: 1,
        batch_size: 8,
        memory_bytes: 16 * 1024 * 1024,
        parquet_cache_bytes: Some(0),
        projection_pushdown: flags & 1 == 0,
        relational_optimizations: flags & 2 == 0,
        ..Options::default()
    };
    let limits = Limits {
        max_catalog_bytes: 4 * 1024 * 1024,
        max_tables: 4,
        max_columns: 16,
        max_rows: 64,
    };
    let mut session = Session::new(&std::env::temp_dir(), options, limits).unwrap();
    let directory = session.directory().to_owned();
    session
        .execute_sql("CREATE TABLE t(id INTEGER NOT NULL,n INTEGER,s VARCHAR)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES(0,NULL,''),(1,0,'a'),(2,-1,'b'),(3,2,'a')")
        .unwrap();
    let requests = bytes[1..]
        .split(|byte| *byte == 0)
        .take(8)
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 8);
    for (index, request) in requests.iter().enumerate() {
        let sql = std::str::from_utf8(request).unwrap();
        let before = catalog_bytes(&session);
        let result = session.execute_sql(sql);
        match index {
            0 | 1 | 3 | 5 => {
                result.unwrap();
            }
            2 => {
                let error = result.err().unwrap();
                assert_eq!(error.kind, "unsupported");
                assert!(error.message.contains("INSERT source must be read-only"));
                assert_eq!(catalog_bytes(&session), before);
                assert_eq!(
                    contents(&mut session, "t1"),
                    vec![(Some(1), Some("true".into()))]
                );
                assert_eq!(count(&mut session, "t"), 4);
            }
            4 | 6 | 7 => {
                assert!(result.is_err(), "{sql}");
            }
            _ => unreachable!(),
        }
    }
    drop(session);
    assert!(!directory.exists());
}
