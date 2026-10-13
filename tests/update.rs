// Zero-NULL approval: deliberate error/laziness markers use checked DECIMAL
// division, with an INTEGER cast where the original scalar type matters.
use arrow::array::{Array, Int64Array, StringArray};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn ints(s: &mut Session, sql: &str) -> Vec<Option<i64>> {
    let Output::Query { execution, .. } = s.execute_sql(sql).unwrap() else {
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
fn affected(s: &mut Session, sql: &str) -> usize {
    let Output::Statement { affected_rows } = s.execute_sql(sql).unwrap() else {
        panic!("statement")
    };
    affected_rows
}
#[test]
fn update_simultaneous_rhs_predicate_and_nulls() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT,y INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,10),(2,20),(NULL,30)")
        .unwrap();
    assert_eq!(affected(&mut s, "UPDATE t SET x=y,y=x WHERE x > 0"), 2);
    assert_eq!(
        ints(&mut s, "SELECT x FROM t ORDER BY x NULLS LAST"),
        vec![Some(10), Some(20), None]
    );
    assert_eq!(
        ints(&mut s, "SELECT y FROM t ORDER BY y"),
        vec![Some(1), Some(2), Some(30)]
    );
    assert_eq!(affected(&mut s, "UPDATE t SET x=7 WHERE NULL"), 0);
    assert_eq!(affected(&mut s, "UPDATE t SET x=NULL WHERE y=1"), 1);
    assert_eq!(ints(&mut s, "SELECT COUNT(x) FROM t"), vec![Some(1)]);
}
#[test]
fn update_masks_rhs_errors_and_rolls_back_whole_statement() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT NOT NULL,y VARCHAR(2),f DOUBLE)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(0,'a',0),(2,'b',0)")
        .unwrap();
    // Keep this DOUBLE assignment/masking fixture floating after Int/Int approval.
    assert_eq!(affected(&mut s, "UPDATE t SET f=10.0/x WHERE x<>0"), 1);
    for sql in [
        "UPDATE t SET f=10.0/x",
        "UPDATE t SET x=NULL",
        "UPDATE t SET x='bad'",
        "UPDATE t SET x=x+9223372036854775807",
        "UPDATE t SET missing=1 WHERE FALSE",
        "UPDATE t SET x=1 RETURNING x",
        "UPDATE t AS a SET x=1",
        "UPDATE t SET x=1 WHERE 1",
    ] {
        assert!(s.execute_sql(sql).is_err(), "{sql}");
        assert_eq!(
            ints(&mut s, "SELECT x FROM t ORDER BY x"),
            vec![Some(0), Some(2)],
            "{sql}"
        );
    }
    assert_eq!(affected(&mut s, "UPDATE t SET x=x+1"), 2);
    assert_eq!(
        ints(&mut s, "SELECT x FROM t ORDER BY x"),
        vec![Some(1), Some(3)]
    );
    assert_eq!(std::fs::read_dir(s.directory()).unwrap().count(), 1);
}
#[test]
fn update_validates_empty_tables_and_assigns_typed_values() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT,d DECIMAL(5,2),b BOOLEAN,day DATE,y VARCHAR(2))")
        .unwrap();
    assert!(s.execute_sql("UPDATE t SET x='bad'").is_err());
    assert!(s.execute_sql("UPDATE t SET x=missing").is_err());
    assert_eq!(affected(&mut s, "UPDATE t SET x=x+1"), 0);
    s.execute_sql("INSERT INTO t VALUES(1,1.25,FALSE,DATE '2024-01-01','a')")
        .unwrap();
    assert_eq!(
        affected(
            &mut s,
            "UPDATE t SET d=x,b=TRUE,day=DATE '2024-02-29',y='猫'"
        ),
        1
    );
    let Output::Query { execution, .. } = s.execute_sql("SELECT y FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(
        execution
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "猫"
    );
    assert!(s.execute_sql("UPDATE t SET d=0.001").is_err());
    assert!(s.execute_sql("UPDATE t SET d=1000").is_err());
    assert_eq!(affected(&mut s, "UPDATE t SET y=NULL"), 1);
    let Output::Query { execution, .. } = s.execute_sql("SELECT y FROM t").unwrap() else {
        panic!()
    };
    assert!(execution.batch.column(0).is_null(0));
}

#[test]
fn update_keys_validate_final_state_and_roll_back_across_slices() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT PRIMARY KEY,y INT NOT NULL)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    assert_eq!(affected(&mut s, "UPDATE t SET x=3-x"), 2);
    assert_eq!(
        ints(&mut s, "SELECT y FROM t ORDER BY y"),
        vec![Some(10), Some(20)]
    );
    assert_eq!(ints(&mut s, "SELECT x FROM t WHERE y=10"), vec![Some(2)]);
    for sql in ["UPDATE t SET x=1", "UPDATE t SET y=NULL"] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic");
        assert_eq!(ints(&mut s, "SELECT x FROM t WHERE y=10"), vec![Some(2)]);
    }
    s.execute_sql("CREATE TABLE big(x INT PRIMARY KEY,y INT)")
        .unwrap();
    let values = (0..9000)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    s.execute_sql(&format!("INSERT INTO big VALUES {values}"))
        .unwrap();
    let directory_before = std::fs::read_dir(s.directory()).unwrap().count();
    let bytes_before = s.catalog_bytes();
    assert_eq!(
        s.execute_sql("UPDATE big SET y=y+1,x=CASE WHEN x=8999 THEN 0 ELSE x END")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    assert_eq!(
        ints(&mut s, "SELECT SUM(y) FROM big"),
        vec![Some(40_495_500)]
    );
    assert_eq!(
        ints(&mut s, "SELECT x FROM big WHERE y=8999"),
        vec![Some(8999)]
    );
    assert_eq!(s.catalog_bytes(), bytes_before);
    assert_eq!(
        std::fs::read_dir(s.directory()).unwrap().count(),
        directory_before
    );
    assert_eq!(affected(&mut s, "UPDATE big SET y=y+1"), 9000);
    assert_eq!(
        ints(&mut s, "SELECT SUM(y) FROM big"),
        vec![Some(40_504_500)]
    );
}

#[test]
fn duplicate_update_uses_last_rhs_and_ignores_earlier_rhs() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT,y INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    // Earlier duplicates are ignored before binding or evaluation, including
    // wrong types, unknown columns and arithmetic errors. Winners read old rows.
    assert_eq!(
        affected(
            &mut s,
            "UPDATE t SET x=missing,y=x,x='bad',x=CAST(1.0/0 AS INTEGER),x=y+1 WHERE x=1"
        ),
        1
    );
    assert_eq!(
        ints(&mut s, "SELECT x FROM t ORDER BY x"),
        vec![Some(2), Some(11)]
    );
    assert_eq!(ints(&mut s, "SELECT y FROM t WHERE x=11"), vec![Some(1)]);
    assert!(s.execute_sql("UPDATE t SET x=1,x=missing").is_err());
    assert_eq!(
        ints(&mut s, "SELECT x FROM t ORDER BY x"),
        vec![Some(2), Some(11)]
    );
    s.execute_sql("CREATE TABLE empty(x INT)").unwrap();
    assert_eq!(affected(&mut s, "UPDATE empty SET x=missing,x=7"), 0);
    // Exact duplicate-target statement behind pinned evidence line 97's lost pass.
    s.execute_sql("CREATE TABLE evidence(x INT)").unwrap();
    s.execute_sql("INSERT INTO evidence VALUES(4),(4),(4)")
        .unwrap();
    assert_eq!(affected(&mut s, "UPDATE evidence SET x=3, x=4, x=5"), 3);
    assert_eq!(
        ints(&mut s, "SELECT COUNT(*) FROM evidence WHERE x=3"),
        vec![Some(0)]
    );
    assert_eq!(
        ints(&mut s, "SELECT COUNT(*) FROM evidence WHERE x=4"),
        vec![Some(0)]
    );
    assert_eq!(
        ints(&mut s, "SELECT COUNT(*) FROM evidence WHERE x=5"),
        vec![Some(3)]
    );
}
