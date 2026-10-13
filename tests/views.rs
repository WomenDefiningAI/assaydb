use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn values(s: &mut Session, sql: &str) -> Vec<i64> {
    let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
        panic!("query")
    };
    execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
#[test]
fn views_are_dynamic_read_only_and_drop_preserves_table() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT,y INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,0),(2,1)").unwrap();
    s.execute_sql("CREATE VIEW v AS SELECT x FROM t WHERE y=1")
        .unwrap();
    assert_eq!(values(&mut s, "SELECT x FROM v"), [2]);
    s.execute_sql("UPDATE t SET y=1 WHERE x=1").unwrap();
    assert_eq!(values(&mut s, "SELECT x FROM v ORDER BY x"), [1, 2]);
    for sql in [
        "INSERT INTO v VALUES(3)",
        "UPDATE v SET x=4",
        "DELETE FROM v",
        "CREATE VIEW v AS SELECT 1",
        "DROP TABLE v",
        "DROP TABLE IF EXISTS v",
    ] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic", "{sql}");
    }
    s.execute_sql("CREATE VIEW IF NOT EXISTS v AS SELECT 42")
        .unwrap();
    s.execute_sql("DELETE FROM t WHERE x=2").unwrap();
    assert_eq!(values(&mut s, "SELECT x FROM v"), [1]);
    s.execute_sql("DROP VIEW v").unwrap();
    assert_eq!(values(&mut s, "SELECT x FROM t"), [1]);
    assert_eq!(s.execute_sql("DROP VIEW v").err().unwrap().kind, "semantic");
    s.execute_sql("DROP VIEW IF EXISTS v").unwrap();
}
#[test]
fn view_names_nested_aliases_and_cte_scopes() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(7)").unwrap();
    s.execute_sql("CREATE VIEW v(a) AS SELECT x FROM t")
        .unwrap();
    s.execute_sql("CREATE VIEW w AS SELECT a+1 AS b FROM v")
        .unwrap();
    assert_eq!(values(&mut s, "SELECT b FROM w"), [8]);
    assert_eq!(
        values(&mut s, "WITH t AS (SELECT 99 AS x) SELECT a FROM v"),
        [7]
    );
    assert_eq!(
        values(&mut s, "WITH v AS (SELECT 99 AS a) SELECT a FROM v"),
        [99]
    );
    assert_eq!(values(&mut s, "SELECT q.a FROM v AS q"), [7]);
    s.execute_sql("CREATE TABLE dst(x INT)").unwrap();
    s.execute_sql("INSERT INTO dst SELECT a FROM v").unwrap();
    assert_eq!(values(&mut s, "SELECT x FROM dst"), [7]);
    for sql in [
        "CREATE TABLE v(x INT)",
        "CREATE VIEW t AS SELECT 1",
        "DROP VIEW IF EXISTS t",
    ] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic", "{sql}");
    }
}
#[test]
fn unused_invalid_views_and_cycles_are_isolated_and_accounted() {
    let mut s = session();
    let initial = s.catalog_bytes();
    s.execute_sql("CREATE VIEW later AS SELECT x FROM absent")
        .unwrap();
    assert!(s.catalog_bytes() > initial);
    assert_eq!(values(&mut s, "SELECT 3"), [3]);
    assert!(s.execute_sql("SELECT * FROM later").is_err());
    s.execute_sql("CREATE TABLE absent(x INT)").unwrap();
    s.execute_sql("INSERT INTO absent VALUES(5)").unwrap();
    assert_eq!(values(&mut s, "SELECT * FROM later"), [5]);
    s.execute_sql("DROP TABLE absent").unwrap();
    s.execute_sql("DROP VIEW later").unwrap();
    assert_eq!(s.catalog_bytes(), initial);
    s.execute_sql("CREATE VIEW a AS SELECT * FROM b").unwrap();
    s.execute_sql("CREATE VIEW b AS SELECT * FROM a").unwrap();
    assert_eq!(
        s.execute_sql("SELECT * FROM a").err().unwrap().kind,
        "semantic"
    );
    assert_eq!(values(&mut s, "SELECT 4"), [4]);
}

#[test]
fn unused_views_preserve_option_validation_and_named_cycles_are_semantic() {
    use assaydb::spill::SpillOptions;
    for spill in [
        SpillOptions {
            sort_run_bytes: 0,
            ..SpillOptions::default()
        },
        SpillOptions {
            merge_fan_in: 1,
            ..SpillOptions::default()
        },
    ] {
        let mut s = Session::new(
            &std::env::temp_dir(),
            Options {
                spill: Some(spill),
                ..Options::default()
            },
            Limits::default(),
        )
        .unwrap();
        let before = s.execute_sql("SELECT 1").err().unwrap();
        s.execute_sql("CREATE VIEW unused AS SELECT 2").unwrap();
        let after = s.execute_sql("SELECT 1").err().unwrap();
        assert_eq!(before.message, "invalid external sort settings");
        assert_eq!(before.message, after.message);
    }
    let mut s = session();
    s.execute_sql("CREATE VIEW memory AS SELECT * FROM memory")
        .unwrap();
    assert_eq!(
        s.execute_sql("SELECT * FROM memory").err().unwrap().kind,
        "semantic"
    );
    s.execute_sql("CREATE VIEW owned AS WITH source AS (SELECT 5 AS x) SELECT x FROM source")
        .unwrap();
    assert_eq!(
        values(
            &mut s,
            "WITH source AS (SELECT 99 AS x) SELECT x FROM owned"
        ),
        [5]
    );
}
