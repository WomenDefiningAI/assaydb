use arrow::{array::Int64Array, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("expected query: {sql}");
    };
    execution.batch
}

fn rows(batch: &RecordBatch) -> Vec<Vec<Option<i64>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| {
                    use arrow::array::Array;
                    let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!values.is_null(row)).then(|| values.value(row))
                })
                .collect()
        })
        .collect()
}

fn session(optimized: bool) -> Session {
    let mut session = Session::new(
        &std::env::temp_dir(),
        Options {
            relational_optimizations: optimized,
            projection_pushdown: optimized,
            batch_size: 2,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE a(id INTEGER, x INTEGER)",
        "INSERT INTO a VALUES(1,10),(1,11),(2,NULL),(3,30)",
        "CREATE TABLE b(id INTEGER, y INTEGER)",
        "INSERT INTO b VALUES(1,100),(1,101),(2,NULL),(4,400)",
        "CREATE TABLE c(id INTEGER)",
        "INSERT INTO c VALUES(1),(4)",
        "CREATE TABLE empty(id INTEGER)",
    ] {
        session
            .execute_sql(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    session
}

#[test]
fn cross_and_unconstrained_inner_joins_preserve_duplicates_nulls_and_empty_inputs() {
    for optimized in [false, true] {
        let mut s = session(optimized);
        let expected = rows(&query(
            &mut s,
            "SELECT a.id,a.x,b.id,b.y FROM a,b ORDER BY 1,2,3,4 NULLS FIRST",
        ));
        assert_eq!(expected.len(), 16);
        for spelling in ["CROSS JOIN", "JOIN", "INNER JOIN"] {
            assert_eq!(
                rows(&query(
                    &mut s,
                    &format!(
                        "SELECT a.id,a.x,b.id,b.y FROM a {spelling} b ORDER BY 1,2,3,4 NULLS FIRST"
                    )
                )),
                expected
            );
        }
        assert_eq!(
            query(&mut s, "SELECT a.id FROM a CROSS JOIN empty").num_rows(),
            0
        );
        assert_eq!(
            query(&mut s, "SELECT a.id FROM empty CROSS JOIN a").num_rows(),
            0
        );
        assert_eq!(
            rows(&query(
                &mut s,
                "SELECT COUNT(*) FROM a CROSS JOIN b WHERE a.id=b.id"
            )),
            vec![vec![Some(5)]]
        );
        assert_eq!(
            rows(&query(
                &mut s,
                "SELECT a.id, COUNT(*) FROM a CROSS JOIN b GROUP BY a.id ORDER BY 1"
            )),
            vec![
                vec![Some(1), Some(8)],
                vec![Some(2), Some(4)],
                vec![Some(3), Some(4)]
            ]
        );
        assert_eq!(
            rows(&query(
                &mut s,
                "SELECT a.id,empty.id FROM a LEFT JOIN empty ORDER BY 1"
            )),
            vec![
                vec![Some(1), None],
                vec![Some(1), None],
                vec![Some(2), None],
                vec![Some(3), None]
            ]
        );
    }
}

#[test]
fn parenthesized_joins_preserve_association_qualifiers_and_alias_scope() {
    for optimized in [false, true] {
        let mut s = session(optimized);
        assert_eq!(
            rows(&query(
                &mut s,
                "SELECT a.id,b.id,c.id FROM a LEFT JOIN (b INNER JOIN c ON b.id=c.id) ON a.id=b.id ORDER BY 1,2,3"
            )),
            vec![vec![Some(1), Some(1), Some(1)]; 4]
                .into_iter()
                .chain([vec![Some(2), None, None], vec![Some(3), None, None]])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rows(&query(
                &mut s,
                "SELECT a.id,b.id,c.id FROM (a LEFT JOIN b ON a.id=b.id) INNER JOIN c ON b.id=c.id ORDER BY 1,2,3"
            )),
            vec![vec![Some(1), Some(1), Some(1)]; 4]
        );
        let nested = query(&mut s, "SELECT * FROM ((a CROSS JOIN c)) ORDER BY 1,2,3");
        assert_eq!(nested.num_columns(), 3);
        assert_eq!(nested.num_rows(), 8);
        for sql in [
            "SELECT j.x FROM (a CROSS JOIN b) AS j",
            "SELECT a.x FROM (a CROSS JOIN b) AS j",
            "SELECT j.* FROM (a CROSS JOIN b) AS j(\"dotted.name\",x,k,y)",
        ] {
            assert!(s.execute_sql(sql).is_err());
        }
        assert!(s.execute_sql("SELECT id FROM (a CROSS JOIN b)").is_err());
        assert_eq!(rows(&query(&mut s,"SELECT j.x,k.y FROM (SELECT x FROM a) AS j CROSS JOIN (SELECT y FROM b) AS k WHERE 1=0")).len(),0);
    }
}
