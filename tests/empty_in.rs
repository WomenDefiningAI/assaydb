// Zero-NULL approval: deliberate error/laziness markers use checked DECIMAL
// division, with an INTEGER cast where the original scalar type matters.
use arrow::{
    array::{BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    session::{Limits, Output, Session},
};
use std::collections::HashMap;

fn booleans(batch: &RecordBatch, column: usize) -> Vec<Option<bool>> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
        panic!()
    };
    execution.batch.clone()
}

#[test]
fn empty_in_binds_left_but_does_not_evaluate_it() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                relational_optimizations,
                ..Options::default()
            };
            let tables = HashMap::new();
            let sql = "SELECT 1 IN (), 1 NOT IN (), NULL IN (), NULL NOT IN (), 'text' IN (), X'ff' NOT IN (), (CAST(1.0 / 0 AS INTEGER)) IN (), (CAST(1.0 / 0 AS INTEGER)) NOT IN (), NOT (NULL IN ()), CASE WHEN NULL NOT IN () THEN 7 ELSE 8 END";
            let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                .unwrap()
                .batch;
            for (column, expected) in [false, true, false, true, false, true, false, true, true]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    booleans(&actual, column),
                    vec![Some(expected)],
                    "{workers}/{relational_optimizations}/{column}"
                );
            }
            assert_eq!(
                actual
                    .column(9)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                7
            );
            assert!(prepare("SELECT missing IN ()", &tables, &options).is_err());
            assert!(prepare("SELECT ('text' + 1) NOT IN ()", &tables, &options).is_err());
            assert!(prepare("SELECT unsupported_function(1) IN ()", &tables, &options).is_err());
        }
    }
}

#[test]
fn empty_in_survives_native_statements_views_and_predicates() {
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                relational_optimizations,
                ..Options::default()
            };
            let mut session =
                Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
            session.execute_sql("CREATE TABLE t(x INT)").unwrap();
            session
                .execute_sql("INSERT INTO t VALUES(1),(NULL),(2)")
                .unwrap();
            let batch = query(&mut session, "SELECT x IN (), x NOT IN () FROM t");
            assert_eq!(booleans(&batch, 0), vec![Some(false); 3]);
            assert_eq!(booleans(&batch, 1), vec![Some(true); 3]);
            assert_eq!(
                query(&mut session, "SELECT x FROM t WHERE x IN ()").num_rows(),
                0
            );
            assert_eq!(
                query(&mut session, "SELECT x FROM t WHERE x NOT IN ()").num_rows(),
                3
            );
            assert!(session.execute_sql("SELECT missing IN () FROM t").is_err());
            session
                .execute_sql("CREATE VIEW v AS SELECT x FROM t WHERE x NOT IN ()")
                .unwrap();
            assert_eq!(query(&mut session, "SELECT * FROM v").num_rows(), 3);
            assert!(matches!(
                session
                    .execute_sql("UPDATE t SET x=4 WHERE x IN ()")
                    .unwrap(),
                Output::Statement { affected_rows: 0 }
            ));
            assert!(matches!(
                session.execute_sql("DELETE FROM t WHERE x IN ()").unwrap(),
                Output::Statement { affected_rows: 0 }
            ));
            assert!(matches!(
                session
                    .execute_sql("DELETE FROM t WHERE x NOT IN ()")
                    .unwrap(),
                Output::Statement { affected_rows: 3 }
            ));
            assert_eq!(query(&mut session, "SELECT x FROM t").num_rows(), 0);
        }
    }
}

#[test]
fn empty_in_is_structural_and_preserves_nonempty_and_generic_syntax() {
    let cases = [
        "SELECT 1 IN (1,2), NULL IN (1), 2 NOT IN (1,3)",
        "SELECT 'IN ()' AS s, X'00ff', 1 /* IN () */ + 2",
        "SELECT CASE WHEN 1 IN (1) THEN 2 ELSE 3 END",
        "SELECT * FROM t WHERE x IN (SELECT x FROM t)",
        "SELECT /* outer /* nested */ comment */ 1",
    ];
    for sql in cases {
        let old =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect, sql).unwrap();
        let sqlparser::ast::Statement::Query(expected) = &old[0] else {
            panic!()
        };
        assert_eq!(assaydb::bind::parse(sql).unwrap(), **expected);
    }
    let options = Options::default();
    let sql = "SELECT 1 IN /* spacing */ (/* empty */), 1 NOT IN ( -- line\n ), 'IN ()' = 'IN ()', 2 IN (1,2), NULL NOT IN (1)";
    let batch = execute(&prepare(sql, &HashMap::new(), &options).unwrap(), &options)
        .unwrap()
        .batch;
    for (i, expected) in [Some(false), Some(true), Some(true), Some(true), None]
        .into_iter()
        .enumerate()
    {
        assert_eq!(booleans(&batch, i), vec![expected]);
    }
    for invalid in [
        "SELECT 1 IN ( , )",
        "SELECT 1 IN (1,,2)",
        "SELECT 1 IN (SELECT)",
    ] {
        assert!(
            prepare(invalid, &HashMap::new(), &options).is_err(),
            "{invalid}"
        );
    }
}
