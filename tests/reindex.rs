use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, MAX_SQL_BYTES, Output, Session},
};

fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn values(s: &mut Session) -> Vec<Option<i64>> {
    let Output::Query { execution, .. } = s.execute_sql("SELECT x FROM t").unwrap() else {
        panic!()
    };
    execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn reindex_validates_catalog_without_changing_storage_or_encounter_order() {
    let mut s = session();
    s.execute_sql("REINDEX").unwrap();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(3),(1),(3),(NULL)")
        .unwrap();
    s.execute_sql("CREATE INDEX i ON t(x DESC)").unwrap();
    let expected = values(&mut s);
    let catalog = s.catalog_bytes();
    let files = std::fs::read_dir(s.directory())
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    for sql in [
        "REINDEX",
        "REINDEX;",
        "reindex T",
        "REINDEX I;",
        "/* a /* nested */ b */ -- c\nREINDEX /* target */ i; -- trailing",
    ] {
        assert!(
            matches!(
                s.execute_sql(sql).unwrap(),
                Output::Statement { affected_rows: 0 }
            ),
            "{sql}"
        );
        assert_eq!(values(&mut s), expected);
        assert_eq!(s.catalog_bytes(), catalog);
        assert_eq!(
            std::fs::read_dir(s.directory()).unwrap().count(),
            files.len()
        );
        for (path, bytes) in &files {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
    }
    s.execute_sql("DROP INDEX i").unwrap();
    assert_eq!(s.execute_sql("REINDEX i").err().unwrap().kind, "semantic");
    s.execute_sql("REINDEX t").unwrap();
    s.execute_sql("DROP TABLE t").unwrap();
    assert_eq!(s.execute_sql("REINDEX t").err().unwrap().kind, "semantic");
    s.execute_sql("REINDEX").unwrap();
}

#[test]
fn invalid_reindex_forms_and_unknown_targets_do_not_mutate() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(9)").unwrap();
    s.execute_sql("CREATE VIEW v AS SELECT x FROM t").unwrap();
    let before = s.catalog_bytes();
    for sql in ["REINDEX missing", "REINDEX v"] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic", "{sql}");
    }
    for sql in [
        "REINDEX t; DELETE FROM t",
        "REINDEX; REINDEX",
        "REINDEX;;",
        "REINDEX t extra",
        "REINDEX t, t",
        "REINDEX main.t",
        "REINDEX \"t\"",
        "REINDEX `t`",
        "REINDEX 't'",
        "REINDEX (VERBOSE) t",
        "REINDEX TABLE t",
        "REINDEX INDEX i",
        "REINDEX CONCURRENTLY t",
        "REINDEX IF EXISTS t",
        "REINDEX t /* unterminated",
    ] {
        assert_eq!(
            s.execute_sql(sql).err().unwrap().kind,
            "unsupported",
            "{sql}"
        );
        assert_eq!(values(&mut s), vec![Some(9)]);
        assert_eq!(s.catalog_bytes(), before);
    }
    let oversized = format!("REINDEX{}", " ".repeat(MAX_SQL_BYTES));
    assert_eq!(s.execute_sql(&oversized).err().unwrap().kind, "resource");
    assert_eq!(s.catalog_bytes(), before);
}

#[test]
fn cancelled_reindex_is_not_reported_as_success() {
    let options = Options::default();
    let cancel = options.cancellation.clone();
    let mut s = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    let before = s.catalog_bytes();
    cancel.cancel();
    assert_eq!(s.execute_sql("REINDEX t").err().unwrap().kind, "resource");
    assert_eq!(s.catalog_bytes(), before);
}
