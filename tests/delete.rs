use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn affected(session: &mut Session, sql: &str) -> usize {
    let Output::Statement { affected_rows } = session.execute_sql(sql).unwrap() else {
        panic!("statement")
    };
    affected_rows
}
fn ints(session: &mut Session, sql: &str) -> Vec<Option<i64>> {
    let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
        panic!("query")
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
fn snapshot(session: &Session) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files = std::fs::read_dir(session.directory())
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

#[test]
fn delete_where_true_only_preserves_nulls_order_and_affected_counts() {
    let mut session = session();
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES(3),(NULL),(1),(2),(NULL)")
        .unwrap();
    let original = snapshot(&session);
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE NULL"), 0);
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE FALSE"), 0);
    assert_eq!(snapshot(&session), original);
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE t.x >= 2"), 2);
    assert_eq!(
        ints(&mut session, "SELECT x FROM t"),
        vec![None, Some(1), None]
    );
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE x IS NULL"), 2);
    assert_eq!(ints(&mut session, "SELECT x FROM t"), vec![Some(1)]);
    assert_eq!(affected(&mut session, "DELETE FROM t"), 1);
    assert_eq!(ints(&mut session, "SELECT COUNT(*) FROM t"), vec![Some(0)]);
    assert_eq!(affected(&mut session, "DELETE FROM t"), 0);
    assert_eq!(std::fs::read_dir(session.directory()).unwrap().count(), 1);
    session.execute_sql("INSERT INTO t VALUES(9)").unwrap();
    assert_eq!(ints(&mut session, "SELECT x FROM t"), vec![Some(9)]);
}

#[test]
fn delete_validates_unknown_targets_empty_predicates_and_all_modifiers() {
    let mut session = session();
    assert_eq!(
        session
            .execute_sql("DELETE FROM missing")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    session.execute_sql("CREATE TABLE t(x INT)").unwrap();
    for sql in [
        "DELETE FROM t WHERE 1",
        "DELETE FROM t WHERE x",
        "DELETE FROM t WHERE missing=1",
    ] {
        assert_eq!(
            session.execute_sql(sql).err().unwrap().kind,
            "semantic",
            "{sql}"
        );
    }
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE x>0"), 0);
    session.execute_sql("INSERT INTO t VALUES(1),(2)").unwrap();
    for sql in [
        "DELETE FROM t RETURNING x",
        "DELETE FROM t LIMIT 1",
        "DELETE FROM t ORDER BY x",
        "DELETE FROM t AS alias WHERE alias.x=1",
        "DELETE FROM t USING t AS other WHERE t.x=other.x",
        "DELETE FROM t, t",
        "DELETE t FROM t",
    ] {
        let before = snapshot(&session);
        assert!(
            session.execute_sql(sql).is_err(),
            "modifier silently accepted: {sql}"
        );
        assert_eq!(snapshot(&session), before, "{sql}");
    }
    assert_eq!(
        ints(&mut session, "SELECT x FROM t"),
        vec![Some(1), Some(2)]
    );
}

#[test]
fn delete_late_predicate_error_rolls_back_all_prior_slices() {
    let mut session = session();
    session
        .execute_sql("CREATE TABLE t(x INT PRIMARY KEY, y INT)")
        .unwrap();
    let values = (0..9000)
        .map(|i| format!("({i},{})", usize::from(i != 8999)))
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute_sql(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let before = snapshot(&session);
    let bytes = session.catalog_bytes();
    let error = session
        .execute_sql("DELETE FROM t WHERE 10.0/y > 0")
        .err()
        .unwrap();
    assert_eq!(error.kind, "semantic");
    assert!(error.message.contains("division by zero"));
    assert_eq!(snapshot(&session), before);
    assert_eq!(session.catalog_bytes(), bytes);
    assert_eq!(
        ints(&mut session, "SELECT COUNT(*) FROM t"),
        vec![Some(9000)]
    );
    assert_eq!(
        ints(&mut session, "SELECT SUM(x) FROM t"),
        vec![Some(40_495_500)]
    );
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE x < 4500"), 4500);
    assert_eq!(
        ints(&mut session, "SELECT COUNT(*) FROM t"),
        vec![Some(4500)]
    );
    assert_eq!(ints(&mut session, "SELECT MIN(x) FROM t"), vec![Some(4500)]);
    assert_eq!(std::fs::read_dir(session.directory()).unwrap().count(), 1);
}

#[test]
fn delete_preserves_native_survivor_types_and_constraints() {
    let mut session = session();
    session.execute_sql("CREATE TABLE t(x INT PRIMARY KEY, d DECIMAL(8,2), text VARCHAR(1), flag BOOLEAN, day DATE, f DOUBLE, unique_value INT UNIQUE)").unwrap();
    session.execute_sql("INSERT INTO t VALUES(1,1.25,'猫 full text',TRUE,DATE '2024-02-29',1.5,NULL),(2,NULL,NULL,NULL,NULL,NULL,2),(3,-2.50,' tail ',FALSE,DATE '2025-01-01',-3.25,NULL)").unwrap();
    let Output::Query {
        execution: expected,
        ..
    } = session
        .execute_sql("SELECT * FROM t WHERE x<>2 ORDER BY x")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(affected(&mut session, "DELETE FROM t WHERE x=2"), 1);
    let Output::Query {
        execution: actual, ..
    } = session.execute_sql("SELECT * FROM t ORDER BY x").unwrap()
    else {
        panic!()
    };
    assert_eq!(actual.batch, expected.batch);
    // Freed keys can be reused; surviving primary/unique constraints remain.
    session
        .execute_sql("INSERT INTO t(x,unique_value) VALUES(2,2)")
        .unwrap();
    assert!(
        session
            .execute_sql("INSERT INTO t(x,unique_value) VALUES(4,2)")
            .is_err()
    );
    assert_eq!(
        affected(&mut session, "UPDATE t SET unique_value=4 WHERE x=2"),
        1
    );
    assert_eq!(
        affected(&mut session, "DELETE FROM t WHERE unique_value=4"),
        1
    );
    assert_eq!(
        ints(&mut session, "SELECT x FROM t ORDER BY x"),
        vec![Some(1), Some(3)]
    );
}
