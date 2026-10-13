use arrow::array::{Array, Int64Array, StringArray};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

fn session() -> Session {
    Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
}
fn query(s: &mut Session, sql: &str) -> arrow::record_batch::RecordBatch {
    match s.execute_sql(sql).unwrap() {
        Output::Query { execution, .. } => execution.batch.clone(),
        _ => panic!("expected query"),
    }
}
fn first_int(s: &mut Session, sql: &str) -> i64 {
    query(s, sql)
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn native_create_insert_select_permuted_columns_and_nulls() {
    let mut s = session();
    s.execute_sql("CREATE TABLE Things (x INTEGER NOT NULL, label VARCHAR(8), optional BIGINT)")
        .unwrap();
    s.execute_sql("INSERT INTO things(label,x) VALUES ('hello',1), (NULL,-2), ('猫',3)")
        .unwrap();
    assert_eq!(first_int(&mut s, "SELECT SUM(x) FROM things"), 2);
    let b = query(&mut s, "SELECT x,label,optional FROM things ORDER BY x");
    assert_eq!(b.num_rows(), 3);
    assert!(b.column(1).is_null(0));
    assert_eq!(b.column(2).null_count(), 3);
    assert_eq!(
        b.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(2),
        "猫"
    );
    s.execute_sql("CREATE TABLE IF NOT EXISTS things(x INT)")
        .unwrap();
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM things"), 3);
}

#[test]
fn rejected_ddl_and_insert_never_mutate_existing_state() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t (x INT NOT NULL, y VARCHAR(2))")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES (1,'a')").unwrap();
    for sql in [
        "CREATE TABLE t(x INT)",
        "CREATE TABLE t2(x INT, X INT)",
        "CREATE TABLE t2(x TEXT DEFAULT 'a')",
        "CREATE TABLE t2(x BLOB)",
        "CREATE TEMP TABLE t2(x INT)",
        "INSERT INTO t VALUES (2,'b'),(NULL,'c')",
        "INSERT INTO t VALUES (2,'ok'),('wrong','a')",
        "INSERT INTO t(y) VALUES ('a')",
        "INSERT INTO t(x,x) VALUES (2,3)",
        "INSERT INTO t(z) VALUES (2)",
        "INSERT INTO t VALUES (2)",
        "INSERT INTO t VALUES (2,'b') RETURNING x",
        "INSERT OR IGNORE INTO t VALUES (2,'b')",
        "INSERT INTO t VALUES (2,'b'); INSERT INTO t VALUES(3,'c')",
    ] {
        assert!(s.execute_sql(sql).is_err(), "unexpected success: {sql}");
        assert_eq!(
            first_int(&mut s, "SELECT COUNT(*) FROM t"),
            1,
            "mutated after {sql}"
        );
        assert_eq!(first_int(&mut s, "SELECT SUM(x) FROM t"), 1);
    }
    s.execute_sql("CREATE TABLE t2(x INT)").unwrap();
}

#[test]
fn strict_types_decimal_date_boolean_and_integer_boundaries() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(i BIGINT,d DECIMAL(5,2),f DOUBLE,b BOOLEAN,day DATE)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES (-9223372036854775808,-12.25,1.5,TRUE,DATE '2024-02-29')")
        .unwrap();
    assert_eq!(first_int(&mut s, "SELECT i FROM t"), i64::MIN);
    for sql in [
        "INSERT INTO t(i) VALUES (9223372036854775808)",
        "INSERT INTO t(d) VALUES (1000)",
        "INSERT INTO t(b) VALUES (1)",
        "INSERT INTO t(i) VALUES ('4')",
        "INSERT INTO t(day) VALUES (DATE '2023-02-29')",
    ] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic", "{sql}");
    }
    assert_eq!(
        s.execute_sql("INSERT INTO t(d) VALUES (0.001)")
            .err()
            .unwrap()
            .kind,
        "unsupported"
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn drop_empty_tables_safe_paths_and_owned_cleanup() {
    let mut s = session();
    let directory = s.directory().to_path_buf();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 0);
    assert!(s.execute_sql("CREATE TABLE \"../outside\"(x INT)").is_err());
    s.execute_sql("DROP TABLE t").unwrap();
    assert!(s.execute_sql("SELECT x FROM t").is_err());
    assert_eq!(
        s.execute_sql("DROP TABLE t").err().unwrap().kind,
        "semantic"
    );
    s.execute_sql("DROP TABLE IF EXISTS t").unwrap();
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    let unrelated = directory.join("caller-owned.txt");
    fs::write(&unrelated, "preserve").unwrap();
    drop(s);
    assert_eq!(fs::read_to_string(&unrelated).unwrap(), "preserve");
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    fs::remove_file(unrelated).unwrap();
    fs::remove_dir(directory).unwrap();
    let s = session();
    let directory = s.directory().to_path_buf();
    drop(s);
    assert!(!directory.exists());
}

#[test]
fn native_resource_failures_preserve_catalog() {
    let limits = Limits {
        max_catalog_bytes: 2048,
        max_rows: 2,
        max_tables: 1,
        ..Limits::default()
    };
    let mut s = Session::new(&std::env::temp_dir(), Options::default(), limits).unwrap();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    assert_eq!(
        s.execute_sql("CREATE TABLE u(x INT)").err().unwrap().kind,
        "resource"
    );
    s.execute_sql("INSERT INTO t VALUES (1),(2)").unwrap();
    assert_eq!(
        s.execute_sql("INSERT INTO t VALUES (3)")
            .err()
            .unwrap()
            .kind,
        "resource"
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(
        s.execute_sql(&" ".repeat(1024 * 1024 + 1))
            .err()
            .unwrap()
            .kind,
        "resource"
    );
    let s = Session::new(
        &std::env::temp_dir(),
        Options::default(),
        Limits {
            max_catalog_bytes: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let mut s = s;
    assert_eq!(
        s.execute_sql("CREATE TABLE t(x INT)").err().unwrap().kind,
        "resource"
    );
    assert_eq!(fs::read_dir(s.directory()).unwrap().count(), 0);
}

#[test]
fn small_memory_preflights_insert_before_catalog_growth() {
    let options = Options {
        memory_bytes: 8192,
        ..Options::default()
    };
    let limits = Limits {
        max_catalog_bytes: 2048,
        ..Limits::default()
    };
    let mut s = Session::new(&std::env::temp_dir(), options, limits).unwrap();
    s.execute_sql("CREATE TABLE t(x TEXT)").unwrap();
    let before = s.catalog_bytes();
    let sql = format!("INSERT INTO t VALUES ('{}')", "a".repeat(4096));
    assert_eq!(s.execute_sql(&sql).err().unwrap().kind, "resource");
    assert_eq!(s.catalog_bytes(), before);
    assert_eq!(fs::read_dir(s.directory()).unwrap().count(), 1);
    s.execute_sql("INSERT INTO t VALUES ('ok')").unwrap();
    // Resource failure did not retain a failed reservation or consume row state.
    assert!(s.catalog_bytes() > before);
    s.execute_sql("DROP TABLE t").unwrap();
    assert_eq!(s.catalog_bytes(), 0);
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
}

#[test]
fn failed_snapshot_creation_does_not_replace_table_or_unrelated_file() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(x INT)").unwrap();
    s.execute_sql("INSERT INTO t VALUES (1)").unwrap();
    let collision = s.directory().join("table-2.parquet");
    fs::write(&collision, "caller-owned").unwrap();
    assert_eq!(
        s.execute_sql("INSERT INTO t VALUES (2)")
            .err()
            .unwrap()
            .kind,
        "internal"
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(fs::read_to_string(&collision).unwrap(), "caller-owned");
    s.execute_sql("INSERT INTO t VALUES (3)").unwrap();
    assert_eq!(first_int(&mut s, "SELECT SUM(x) FROM t"), 4);
    fs::remove_file(collision).unwrap();
}

#[test]
fn oversized_protocol_request_is_drained_without_mutation() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb-session"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(
        input,
        "{}",
        json!({"op":"open","directory":std::env::temp_dir()})
    )
    .unwrap();
    writeln!(input, "{}", "x".repeat(1024 * 1024 + 1)).unwrap();
    writeln!(input, "{}", json!({"sql":"SELECT 7"})).unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let replies = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(replies[1]["error_kind"], "resource");
    assert_eq!(replies[2]["result"]["rows"], json!([[7]]));
    assert!(!std::path::Path::new(replies[0]["directory"].as_str().unwrap()).exists());
}

#[test]
fn json_lines_protocol_retains_state_recovers_and_cleans_up() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb-session"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    for request in [
        json!({"op":"open","directory":std::env::temp_dir()}),
        json!({"sql":"CREATE TABLE t(x INT)"}),
        json!({"sql":"INSERT INTO t VALUES (7)"}),
        json!({"sql":"INSERT INTO t VALUES ('bad')"}),
        json!({"sql":"SELECT x FROM t"}),
        json!({"op":"open","directory":std::env::temp_dir()}),
    ] {
        writeln!(input, "{request}").unwrap();
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let replies = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), 6);
    assert_eq!(replies[3]["error_kind"], "semantic");
    assert_eq!(replies[4]["result"]["rows"], json!([[7]]));
    assert_eq!(replies[5]["error_kind"], "protocol");
    assert!(!std::path::Path::new(replies[0]["directory"].as_str().unwrap()).exists());
}

#[test]
fn native_primary_unique_and_insert_select_are_atomic() {
    let mut s = session();
    s.execute_sql("CREATE TABLE src(id INTEGER PRIMARY KEY, label TEXT UNIQUE)")
        .unwrap();
    s.execute_sql("INSERT INTO src VALUES (1,'a'),(2,'b'),(3,NULL),(4,NULL)")
        .unwrap();
    for sql in [
        "INSERT INTO src VALUES (1,'z')",
        "INSERT INTO src VALUES (5,'a')",
        "INSERT INTO src VALUES (NULL,'z')",
        "INSERT INTO src VALUES (5,'z'),(6,'z')",
        "INSERT INTO src SELECT id,label FROM src",
    ] {
        assert_eq!(s.execute_sql(sql).err().unwrap().kind, "semantic", "{sql}");
        assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM src"), 4);
    }
    s.execute_sql("CREATE TABLE dst(id INT PRIMARY KEY,label TEXT UNIQUE)")
        .unwrap();
    s.execute_sql("INSERT INTO dst(label,id) SELECT label,id+10 FROM src")
        .unwrap();
    assert_eq!(first_int(&mut s, "SELECT SUM(id) FROM dst"), 50);
    s.execute_sql("CREATE TABLE copies(id INT,label TEXT)")
        .unwrap();
    s.execute_sql("INSERT INTO copies SELECT * FROM src")
        .unwrap();
    s.execute_sql("INSERT INTO copies SELECT * FROM copies")
        .unwrap();
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM copies"), 8);
    s.execute_sql("CREATE TABLE IF NOT EXISTS src(id INT UNIQUE)")
        .unwrap();
    assert!(s.execute_sql("INSERT INTO src VALUES (NULL,'z')").is_err());
}

#[test]
fn native_composite_constraints_and_strict_query_assignment() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(a INT,b TEXT,c INT,CONSTRAINT pk PRIMARY KEY(a,b),UNIQUE(b,c))")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES (1,'x',2),(2,'x',3),(3,'x',NULL),(4,'x',NULL)")
        .unwrap();
    for sql in [
        "INSERT INTO t VALUES (1,'x',4)",
        "INSERT INTO t VALUES (9,'x',2)",
        "INSERT INTO t VALUES (NULL,'z',5)",
    ] {
        assert!(s.execute_sql(sql).is_err(), "{sql}");
        assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 4);
    }
    for sql in [
        "CREATE TABLE bad(a INT PRIMARY KEY,b INT PRIMARY KEY)",
        "CREATE TABLE bad(a INT,UNIQUE(z))",
        "CREATE TABLE bad(a INT,PRIMARY KEY(a,a))",
        "CREATE TABLE bad(a INT, UNIQUE(a DESC))",
        "CREATE TABLE bad(a INT, UNIQUE(a WITH FILL))",
    ] {
        assert!(s.execute_sql(sql).is_err(), "{sql}");
    }
    s.execute_sql("CREATE TABLE d(a INT NOT NULL,b VARCHAR(1),c DECIMAL(3,1))")
        .unwrap();
    s.execute_sql("INSERT INTO d SELECT 1,'x',2").unwrap();
    for sql in [
        "INSERT INTO d SELECT '1','x',2",
        "INSERT INTO d SELECT 2,'x',100",
        "INSERT INTO d SELECT 2,'x',1.23",
    ] {
        assert!(s.execute_sql(sql).is_err(), "{sql}");
        assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM d"), 1);
    }
    s.execute_sql("CREATE TABLE floats(x DOUBLE UNIQUE)")
        .unwrap();
    s.execute_sql("INSERT INTO floats VALUES (-0.0)").unwrap();
    assert!(s.execute_sql("INSERT INTO floats VALUES (0.0)").is_err());
}

#[test]
fn native_query_insert_checks_empty_types_and_missing_not_null_columns() {
    let mut s = session();
    s.execute_sql("CREATE TABLE src(x TEXT)").unwrap();
    s.execute_sql("CREATE TABLE dst(x INT NOT NULL,y TEXT)")
        .unwrap();
    assert_eq!(
        s.execute_sql("INSERT INTO dst(x) SELECT x FROM src")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    s.execute_sql("INSERT INTO src VALUES ('a')").unwrap();
    assert_eq!(
        s.execute_sql("INSERT INTO dst(y) SELECT x FROM src")
            .err()
            .unwrap()
            .kind,
        "semantic"
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM dst"), 0);
}

fn affected(s: &mut Session, sql: &str) -> usize {
    let Output::Statement { affected_rows } = s.execute_sql(sql).unwrap() else {
        panic!("statement")
    };
    affected_rows
}

#[test]
fn replace_deletes_all_key_conflicts_and_processes_new_rows_in_order() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY,a TEXT UNIQUE,b INT UNIQUE)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,'a',10),(2,'b',20)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(3,'c',30)").unwrap();
    // Three distinct rows conflict through three different keys.
    assert_eq!(affected(&mut s, "REPLACE INTO t VALUES(1,'b',30)"), 1);
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(first_int(&mut s, "SELECT b FROM t"), 30);
    assert_eq!(
        affected(
            &mut s,
            "INSERT OR REPLACE INTO t VALUES(2,'b',40),(3,'c',40),(4,'b',50)"
        ),
        3
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(first_int(&mut s, "SELECT SUM(id) FROM t"), 7);
    // Stale indexes for a deleted row must never resurrect/delete an unrelated row.
    assert_eq!(affected(&mut s, "REPLACE INTO t VALUES(5,'a',30)"), 1);
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(std::fs::read_dir(s.directory()).unwrap().count(), 1);
}

#[test]
fn replace_query_uses_one_old_snapshot_and_preserves_null_unique_semantics() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY,a TEXT,b INT,UNIQUE(a,b))")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,'a',1),(2,'b',2),(3,NULL,1),(4,NULL,1)")
        .unwrap();
    assert_eq!(
        affected(
            &mut s,
            "INSERT OR REPLACE INTO t(id,a,b) SELECT id+1 AS next_id,a,b FROM t ORDER BY next_id"
        ),
        4
    );
    // Snapshot source rows are all processed once even when earlier replacements
    // delete later source rows; UNIQUE tuples containing NULL do not conflict.
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 4);
    assert_eq!(first_int(&mut s, "SELECT SUM(id) FROM t"), 14);
    assert_eq!(
        affected(&mut s, "REPLACE INTO t VALUES(6,NULL,1),(7,NULL,1)"),
        2
    );
    assert_eq!(
        first_int(&mut s, "SELECT COUNT(*) FROM t WHERE a IS NULL"),
        4
    );
    assert_eq!(
        affected(
            &mut s,
            "INSERT OR REPLACE INTO t(b,id,a) SELECT b,id,a FROM t"
        ),
        6
    );
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 6);
    s.execute_sql("CREATE TABLE floats(x DOUBLE UNIQUE,id INT)")
        .unwrap();
    s.execute_sql("INSERT INTO floats VALUES(-0.0,1)").unwrap();
    s.execute_sql("REPLACE INTO floats VALUES(0.0,2)").unwrap();
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM floats"), 1);
    assert_eq!(first_int(&mut s, "SELECT id FROM floats"), 2);
    s.execute_sql("CREATE TABLE plain(x INT)").unwrap();
    s.execute_sql("REPLACE INTO plain VALUES(1),(1)").unwrap();
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM plain"), 2);
}

#[test]
fn replace_statement_errors_roll_back_all_deletions_and_insertions() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY,a VARCHAR(2) NOT NULL UNIQUE)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,'a'),(2,'b')")
        .unwrap();
    let before = query(&mut s, "SELECT * FROM t ORDER BY id");
    let bytes = s.catalog_bytes();
    for sql in [
        "REPLACE INTO t VALUES(1,'b'),(3,NULL)",
        "REPLACE INTO t VALUES(1,'b'),(NULL,'z')",
        "REPLACE INTO t VALUES(1,'b'),('bad','z')",
        "INSERT OR REPLACE INTO t(id) SELECT id FROM t",
        "REPLACE INTO t SELECT id,CASE WHEN id=2 THEN NULL ELSE 'b' END FROM t",
        "REPLACE INTO t VALUES(1,'z') RETURNING id",
        "INSERT OR IGNORE INTO t VALUES(1,'z')",
    ] {
        assert!(s.execute_sql(sql).is_err(), "{sql}");
        assert_eq!(
            query(&mut s, "SELECT * FROM t ORDER BY id"),
            before,
            "{sql}"
        );
        assert_eq!(s.catalog_bytes(), bytes);
        assert_eq!(std::fs::read_dir(s.directory()).unwrap().count(), 1);
    }
}

#[test]
fn replace_applies_row_limit_to_final_snapshot() {
    let mut s = Session::new(
        &std::env::temp_dir(),
        Options::default(),
        Limits {
            max_rows: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(1),(2)").unwrap();
    assert_eq!(affected(&mut s, "REPLACE INTO t VALUES(1),(1),(2)"), 3);
    assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(
        s.execute_sql("REPLACE INTO t VALUES(1),(3)")
            .err()
            .unwrap()
            .kind,
        "resource"
    );
    assert_eq!(first_int(&mut s, "SELECT SUM(id) FROM t"), 3);
}

#[test]
fn declared_character_widths_never_truncate_pad_or_reject_text() {
    // Approved semantic change: former length-error/unsupported-CHAR cases
    // become positive tests. Numeric, type and NOT NULL policies stay strict.
    for ty in [
        "VARCHAR(1)",
        "CHAR(1)",
        "CHARACTER(1)",
        "CHARACTER VARYING(1)",
        "CHAR VARYING(1)",
        "VARCHAR(0)",
        "CHAR(0)",
        "VARCHAR",
        "CHAR",
        "CHAR(100)",
    ] {
        let mut s = session();
        s.execute_sql(&format!(
            "CREATE TABLE t(id INT PRIMARY KEY,txt {ty} NOT NULL UNIQUE)"
        ))
        .unwrap();
        s.execute_sql("INSERT INTO t VALUES(1,'é雪-long'),(2,'é雪-other'),(3,'q'),(4,'')")
            .unwrap();
        let texts = |s: &mut Session| {
            query(s, "SELECT id,txt FROM t ORDER BY id")
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|x| x.unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(texts(&mut s), ["é雪-long", "é雪-other", "q", ""], "{ty}");
        s.execute_sql("REPLACE INTO t VALUES(1,'insert or replace (new)')")
            .unwrap();
        assert_eq!(texts(&mut s)[0], "insert or replace (new)", "{ty}");
        // Exact whole strings remain the UNIQUE keys, not a declared-width prefix.
        s.execute_sql("REPLACE INTO t VALUES(7,'insert or replace (new)')")
            .unwrap();
        assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t WHERE id=1"), 0);
        s.execute_sql("INSERT OR REPLACE INTO t SELECT 9,txt FROM t WHERE id=7")
            .unwrap();
        assert_eq!(first_int(&mut s, "SELECT COUNT(*) FROM t WHERE id=7"), 0);
        s.execute_sql("UPDATE t SET txt='  updated é雪 with spaces  ' WHERE id=2")
            .unwrap();
        assert_eq!(texts(&mut s)[0], "  updated é雪 with spaces  ", "{ty}");
        s.execute_sql(&format!("CREATE TABLE copied(id INT,txt {ty})"))
            .unwrap();
        s.execute_sql("INSERT INTO copied SELECT id,txt FROM t")
            .unwrap();
        assert_eq!(
            query(&mut s, "SELECT id,txt FROM copied ORDER BY id"),
            query(&mut s, "SELECT id,txt FROM t ORDER BY id")
        );
        let before = query(&mut s, "SELECT id,txt FROM t ORDER BY id");
        for sql in [
            "INSERT INTO t VALUES(10,10)",
            "INSERT INTO t VALUES(10,NULL)",
            "UPDATE t SET txt=10",
            "UPDATE t SET txt=NULL",
            "REPLACE INTO t VALUES(9,NULL)",
            "INSERT INTO t SELECT 10,10",
        ] {
            assert!(s.execute_sql(sql).is_err(), "{ty}: {sql}");
            assert_eq!(query(&mut s, "SELECT id,txt FROM t ORDER BY id"), before);
        }
    }
}

#[test]
fn pinned_replace_width_examples_store_the_complete_values() {
    let mut s = session();
    s.execute_sql("CREATE TABLE t1(x INTEGER PRIMARY KEY,y VARCHAR(16))")
        .unwrap();
    for sql in [
        "INSERT INTO t1 VALUES(2,'insert')",
        "INSERT OR REPLACE INTO t1 VALUES(2,'insert or replace')",
        "REPLACE INTO t1 VALUES(2,'replace')",
        "INSERT OR REPLACE INTO t1 VALUES(3,'insert or replace (new)')",
        "REPLACE INTO t1 VALUES(4,'replace (new)')",
    ] {
        s.execute_sql(sql).unwrap();
    }
    let batch = query(&mut s, "SELECT x,y FROM t1 ORDER BY x");
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("replace"),
            Some("insert or replace (new)"),
            Some("replace (new)")
        ]
    );
}
