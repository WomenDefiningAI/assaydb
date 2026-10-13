//! SQLite-compatible UNIQUE index enforcement, independent of access paths.
use arrow::array::{Array, Int64Array};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn ints(s: &mut Session, sql: &str) -> Vec<Option<i64>> {
    let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
        panic!()
    };
    let a = execution
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..a.len())
        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
        .collect()
}
fn constraint(s: &mut Session, sql: &str) {
    let e = s
        .execute_sql(sql)
        .err()
        .expect("must reject duplicate index entry");
    assert_eq!(e.kind, "semantic", "{sql}: {}", e.message);
    assert!(e.message.contains("UNIQUE"), "{sql}: {}", e.message);
}
#[test]
fn unique_creation_is_atomic_and_checks_existing_rows() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT,x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,7),(2,7),(3,NULL),(4,NULL)")
        .unwrap();
    let before = s.catalog_bytes();
    constraint(&mut s, "CREATE UNIQUE INDEX u ON t(x DESC)");
    assert_eq!(s.catalog_bytes(), before);
    assert!(s.execute_sql("DROP INDEX u").is_err());
    s.execute_sql("DELETE FROM t WHERE id=2").unwrap();
    s.execute_sql("CREATE UNIQUE INDEX u ON t(x DESC,x ASC)")
        .unwrap();
    let indexed = s.catalog_bytes();
    s.execute_sql("CREATE UNIQUE INDEX IF NOT EXISTS u ON absent(missing)")
        .unwrap();
    assert_eq!(s.catalog_bytes(), indexed);
    constraint(&mut s, "INSERT INTO t VALUES(5,7),(6,9)");
    assert_eq!(
        ints(&mut s, "SELECT id FROM t ORDER BY id"),
        vec![Some(1), Some(3), Some(4)]
    );
    s.execute_sql("DROP INDEX u").unwrap();
    s.execute_sql("INSERT INTO t VALUES(5,7)").unwrap();
    s.execute_sql("DROP TABLE t").unwrap();
    assert_eq!(s.catalog_bytes(), 0);
}
#[test]
fn composite_nulls_text_and_replacement_use_every_unique_index() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY,x INT,y TEXT)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,10,'A'),(2,20,'B')")
        .unwrap();
    s.execute_sql("CREATE UNIQUE INDEX ux ON t(x)").unwrap();
    s.execute_sql("CREATE UNIQUE INDEX uy ON t(y)").unwrap();
    s.execute_sql("INSERT OR REPLACE INTO t VALUES(3,10,'B')")
        .unwrap();
    assert_eq!(ints(&mut s, "SELECT id FROM t"), vec![Some(3)]);
    s.execute_sql("INSERT INTO t VALUES(4,NULL,NULL),(5,NULL,NULL),(6,11,'b')")
        .unwrap();
    constraint(&mut s, "INSERT INTO t VALUES(7,10,'new')");
    s.execute_sql("DROP INDEX ux").unwrap();
    s.execute_sql("INSERT INTO t VALUES(7,10,'new')").unwrap();
    constraint(&mut s, "INSERT INTO t VALUES(8,30,'B')");
    s.execute_sql("DROP INDEX uy").unwrap();
    // Dropping explicit indexes must not remove the declared primary key.
    constraint(&mut s, "INSERT INTO t VALUES(3,99,'other')");
    s.execute_sql("CREATE UNIQUE INDEX pair ON t(x,y)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(8,10,'B2'),(9,NULL,'B'),(10,NULL,'B')")
        .unwrap();
    constraint(&mut s, "INSERT INTO t VALUES(11,10,'B')");
}
#[test]
fn update_checks_transient_conflicts_and_rolls_back_the_whole_statement() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT,x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,1),(2,2),(3,NULL)")
        .unwrap();
    s.execute_sql("CREATE UNIQUE INDEX u ON t(x)").unwrap();
    let before = s.catalog_bytes();
    constraint(&mut s, "UPDATE t SET x=3-x WHERE id<=2");
    assert_eq!(
        ints(&mut s, "SELECT x,id FROM t ORDER BY id"),
        vec![Some(1), Some(2), None]
    );
    assert_eq!(s.catalog_bytes(), before);
    constraint(&mut s, "UPDATE t SET x=9 WHERE id<=2");
    assert_eq!(
        ints(&mut s, "SELECT x,id FROM t ORDER BY id"),
        vec![Some(1), Some(2), None]
    );
    s.execute_sql("UPDATE t SET x=x-1 WHERE id<=2").unwrap();
    assert_eq!(
        ints(&mut s, "SELECT x,id FROM t ORDER BY id"),
        vec![Some(0), Some(1), None]
    );
    s.execute_sql("UPDATE t SET x=NULL").unwrap();
    assert_eq!(
        ints(&mut s, "SELECT x,id FROM t ORDER BY id"),
        vec![None, None, None]
    );
}
#[test]
fn floating_signed_zero_conflicts_and_other_tables_remain_independent() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT,x DOUBLE)").unwrap();
    s.execute_sql("CREATE UNIQUE INDEX u ON t(x)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,0.0)").unwrap();
    constraint(&mut s, "INSERT INTO t VALUES(2,-0.0)");
    s.execute_sql("CREATE TABLE other(x INT)").unwrap();
    s.execute_sql("INSERT INTO other VALUES(1),(1)").unwrap();
    s.execute_sql("DROP TABLE t").unwrap();
    s.execute_sql("CREATE INDEX u ON other(x)").unwrap();
    assert_eq!(ints(&mut s, "SELECT COUNT(*) FROM other"), vec![Some(2)]);
}

#[test]
fn replace_insert_select_uses_original_snapshot_and_incoming_order_across_indexes() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT,x INT,y TEXT)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,10,'A'),(2,20,'B'),(3,30,'C')")
        .unwrap();
    s.execute_sql("CREATE UNIQUE INDEX ux ON t(x)").unwrap();
    s.execute_sql("CREATE UNIQUE INDEX uy ON t(y)").unwrap();
    // Incoming (11,20,A) deletes original rows 1 and 2; the next two rows
    // still come from the original snapshot. Incoming (13,20,C) then deletes
    // the earlier incoming row 11. Stale entries for original row 3 must not
    // delete unrelated live rows when its y=C key is encountered again.
    let Output::Statement { affected_rows } = s.execute_sql(
        "INSERT OR REPLACE INTO t SELECT id+10 AS next_id,CASE WHEN id=2 THEN 30 ELSE 20 END,y FROM t ORDER BY next_id",
    ).unwrap() else { panic!("statement"); };
    assert_eq!(affected_rows, 3);
    assert_eq!(
        ints(&mut s, "SELECT id FROM t ORDER BY id"),
        vec![Some(12), Some(13)]
    );
    assert_eq!(
        ints(&mut s, "SELECT x,id FROM t ORDER BY id"),
        vec![Some(30), Some(20)]
    );
    assert_eq!(ints(&mut s, "SELECT id FROM t WHERE y='B'"), vec![Some(12)]);
    assert_eq!(ints(&mut s, "SELECT id FROM t WHERE y='C'"), vec![Some(13)]);
    constraint(&mut s, "INSERT INTO t VALUES(14,20,'new')");
    constraint(&mut s, "INSERT INTO t VALUES(14,40,'B')");
}
