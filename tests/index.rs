use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn ints(session: &mut Session) -> Vec<Option<i64>> {
    let Output::Query { execution, .. } = session.execute_sql("SELECT x FROM t").unwrap() else {
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
fn nonunique_indexes_retain_lifecycle_without_changing_data_or_query_order() {
    let mut session = session();
    session.execute_sql("CREATE TABLE t(x INT,y INT)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES(2,1),(1,1),(2,2),(NULL,NULL)")
        .unwrap();
    let expected = ints(&mut session);
    let bytes = session.catalog_bytes();
    let paths = std::fs::read_dir(session.directory())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    let before = std::fs::read(&paths[0]).unwrap();
    session
        .execute_sql("CREATE INDEX i ON t(y DESC,x ASC,x)")
        .unwrap();
    assert!(session.catalog_bytes() > bytes);
    let indexed_bytes = session.catalog_bytes();
    assert_eq!(ints(&mut session), expected);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), before);
    assert_eq!(std::fs::read_dir(session.directory()).unwrap().count(), 1);
    assert_eq!(
        session
            .execute_sql("CREATE INDEX i ON t(x)")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    session
        .execute_sql("CREATE INDEX IF NOT EXISTS i ON t(x)")
        .unwrap();
    assert_eq!(session.catalog_bytes(), indexed_bytes);
    session.execute_sql("DROP INDEX i").unwrap();
    assert_eq!(session.catalog_bytes(), bytes);
    assert_eq!(
        session.execute_sql("DROP INDEX i").err().unwrap().kind,
        "semantic"
    );
    session.execute_sql("DROP INDEX IF EXISTS i").unwrap();
    assert_eq!(ints(&mut session), expected);
}
#[test]
fn index_target_columns_and_unimplemented_modes_are_not_silently_accepted() {
    let mut session = session();
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    let bytes = session.catalog_bytes();
    for sql in [
        "CREATE INDEX i ON missing(x)",
        "CREATE INDEX i ON t(missing)",
    ] {
        assert_eq!(
            session.execute_sql(sql).err().unwrap().kind,
            "semantic",
            "{sql}"
        );
    }
    for sql in [
        "CREATE INDEX i ON t(x+1)",
        "CREATE INDEX i ON t(x) WHERE x>0",
        "CREATE INDEX i ON t(x NULLS FIRST)",
        "CREATE INDEX i ON t(x COLLATE nocase)",
        "CREATE INDEX i ON t USING HASH(x)",
        "CREATE INDEX i ON t(x) INCLUDE(x)",
        "CREATE INDEX CONCURRENTLY i ON t(x)",
    ] {
        assert!(
            session.execute_sql(sql).is_err(),
            "unsupported index accepted: {sql}"
        );
        assert_eq!(session.catalog_bytes(), bytes, "{sql}");
    }
    session.execute_sql("CREATE INDEX i ON t(x)").unwrap();
    for sql in ["DROP INDEX i CASCADE", "DROP INDEX i, missing"] {
        assert!(session.execute_sql(sql).is_err(), "{sql}");
    }
    session.execute_sql("DROP INDEX i").unwrap();
    assert_eq!(session.catalog_bytes(), bytes);
}
#[test]
fn index_names_share_catalog_namespace_and_table_drop_removes_definitions() {
    let mut session = session();
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    session
        .execute_sql("CREATE VIEW v AS SELECT x FROM t")
        .unwrap();
    assert_eq!(
        session
            .execute_sql("CREATE INDEX t ON t(x)")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    assert_eq!(
        session
            .execute_sql("CREATE INDEX v ON t(x)")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    assert_eq!(
        session
            .execute_sql("CREATE INDEX i ON v(x)")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    session.execute_sql("CREATE INDEX i ON t(x)").unwrap();
    assert!(session.execute_sql("CREATE TABLE i(x INT)").is_err());
    assert!(
        session
            .execute_sql("CREATE VIEW i AS SELECT x FROM t")
            .is_err()
    );
    session.execute_sql("DROP VIEW v").unwrap();
    session.execute_sql("DROP TABLE t").unwrap();
    assert_eq!(session.catalog_bytes(), 0);
    assert_eq!(
        session.execute_sql("DROP INDEX i").err().unwrap().kind,
        "semantic"
    );
    session.execute_sql("CREATE TABLE i(x INT)").unwrap();
    session.execute_sql("CREATE INDEX t ON i(x)").unwrap();
    session.execute_sql("DROP TABLE i").unwrap();
    assert_eq!(session.catalog_bytes(), 0);
}
#[test]
fn index_count_limit_is_separate_and_released_on_drop() {
    let mut session = Session::new(
        &std::env::temp_dir(),
        Options::default(),
        Limits {
            max_tables: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    session.execute_sql("CREATE INDEX i ON t(x)").unwrap();
    assert_eq!(
        session
            .execute_sql("CREATE INDEX j ON t(x)")
            .err()
            .unwrap()
            .kind,
        "resource"
    );
    session.execute_sql("DROP INDEX i").unwrap();
    session.execute_sql("CREATE INDEX j ON t(x)").unwrap();
    session.execute_sql("DROP TABLE t").unwrap();
    assert_eq!(session.catalog_bytes(), 0);
}
