//! GROUP BY ALL inference. Values and rejection boundaries are independent
//! DuckDB 2.0.0 controls; existing explicit GROUP BY semantics are not changed.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};

fn rows(batch: &RecordBatch) -> Value {
    Value::Array(
        (0..batch.num_rows())
            .map(|row| {
                Value::Array(
                    batch
                        .columns()
                        .iter()
                        .map(|array| {
                            if array.is_null(row) {
                                return Value::Null;
                            }
                            match array.data_type() {
                                DataType::Null => Value::Null,
                                DataType::Int64 => json!(
                                    array
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .value(row)
                                ),
                                DataType::Boolean => json!(
                                    array
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .unwrap()
                                        .value(row)
                                ),
                                other => panic!("unexpected test type {other}"),
                            }
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}
fn query(session: &mut Session, sql: &str) -> Value {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("query required")
    };
    rows(&execution.batch)
}
fn configurations(mut f: impl FnMut(&mut Session)) {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for relational_optimizations in [false, true] {
                let mut session = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        batch_size,
                        relational_optimizations,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                session
                    .execute_sql("CREATE TABLE t(x INTEGER,y INTEGER)")
                    .unwrap();
                session
                    .execute_sql("INSERT INTO t VALUES(1,10),(1,20),(2,30)")
                    .unwrap();
                f(&mut session);
            }
        }
    }
}

#[test]
fn visible_expressions_define_groups_without_hidden_columns() {
    configurations(|session| {
        for (sql, expected) in [
            (
                "SELECT x,SUM(y) FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 30], [2, 30]]),
            ),
            (
                "SELECT x%2 AS a,COUNT(*) FROM t GROUP BY ALL ORDER BY a",
                json!([[0, 1], [1, 2]]),
            ),
            (
                "SELECT x AS a,a+1 AS b,SUM(y) FROM t GROUP BY ALL ORDER BY a",
                json!([[1, 2, 30], [2, 3, 30]]),
            ),
            (
                "SELECT SUM(y) AS a,a+1 FROM t GROUP BY ALL",
                json!([[60, 61]]),
            ),
            (
                "SELECT x AS a,SUM(a) FROM t GROUP BY ALL ORDER BY a",
                json!([[1, 2], [2, 2]]),
            ),
            (
                "SELECT x AS y,SUM(y) FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 30], [2, 30]]),
            ),
            (
                "SELECT x,x,COUNT(*) FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 1, 2], [2, 2, 1]]),
            ),
            (
                "SELECT x AS a,t.x AS a,COUNT(*) FROM t GROUP BY ALL ORDER BY a",
                json!([[1, 1, 2], [2, 2, 1]]),
            ),
            (
                "SELECT * FROM t GROUP BY ALL ORDER BY x,y",
                json!([[1, 10], [1, 20], [2, 30]]),
            ),
            (
                "SELECT t.*,COUNT(*) FROM t GROUP BY ALL ORDER BY x,y",
                json!([[1, 10, 1], [1, 20, 1], [2, 30, 1]]),
            ),
            (
                "SELECT x,SUM(y) FILTER(WHERE y>10) FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 20], [2, 30]]),
            ),
            (
                "SELECT x FROM t GROUP BY ALL HAVING SUM(y)>20 ORDER BY x",
                json!([[1], [2]]),
            ),
            (
                "SELECT x FROM t GROUP BY ALL ORDER BY SUM(y),x",
                json!([[1], [2]]),
            ),
            (
                "SELECT x,ROW_NUMBER() OVER() FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 1], [2, 2]]),
            ),
            (
                "SELECT SUM(y),ROW_NUMBER() OVER() FROM t GROUP BY ALL",
                json!([[60, 1]]),
            ),
            (
                "SELECT DISTINCT x,COUNT(*) FROM t GROUP BY ALL ORDER BY x",
                json!([[1, 2], [2, 1]]),
            ),
        ] {
            assert_eq!(query(session, sql), expected, "{sql}");
        }
    });
}

#[test]
fn constants_empty_inputs_and_trivial_scalar_scopes() {
    configurations(|session| {
        for (sql, expected) in [
            ("SELECT 1 FROM t GROUP BY ALL", json!([[1], [1], [1]])),
            (
                "SELECT NULL FROM t GROUP BY ALL",
                json!([[null], [null], [null]]),
            ),
            ("SELECT 1 FROM t WHERE FALSE GROUP BY ALL", json!([])),
            ("SELECT 1,COUNT(*) FROM t GROUP BY ALL", json!([[1, 3]])),
            (
                "SELECT 1,COUNT(*) FROM t WHERE FALSE GROUP BY ALL",
                json!([[1, 0]]),
            ),
            (
                "SELECT x,COUNT(*) FROM t WHERE FALSE GROUP BY ALL",
                json!([]),
            ),
            (
                "SELECT (SELECT 1),SUM(y) FROM t GROUP BY ALL",
                json!([[1, 60]]),
            ),
            (
                "SELECT (SELECT x),COUNT(*) FROM t GROUP BY ALL ORDER BY 1",
                json!([[1, 2], [2, 1]]),
            ),
            (
                "SELECT (SELECT 1)+x,COUNT(*) FROM t GROUP BY ALL ORDER BY 1",
                json!([[2, 2], [3, 1]]),
            ),
        ] {
            assert_eq!(query(session, sql), expected, "{sql}");
        }
    });
}

#[test]
fn invalid_group_composition_is_not_silently_inferred() {
    configurations(|session| {
        for (sql, message) in [
            (
                "SELECT x+SUM(y) FROM t GROUP BY ALL",
                "cannot mix aggregates",
            ),
            (
                "SELECT x,x+SUM(y) FROM t GROUP BY ALL",
                "cannot mix aggregates",
            ),
            (
                "SELECT x+SUM(y) FROM t GROUP BY ALL HAVING x>0",
                "cannot mix aggregates",
            ),
            (
                "SELECT x+ROW_NUMBER() OVER() FROM t GROUP BY ALL",
                "cannot group on a window clause",
            ),
            (
                "SELECT ROW_NUMBER() OVER(ORDER BY y) FROM t GROUP BY ALL",
                "cannot group on a window clause",
            ),
            (
                "SELECT x,SUM(y) OVER() FROM t GROUP BY ALL",
                "cannot group on a window clause",
            ),
        ] {
            let error = session
                .execute_sql(sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            assert_eq!(error.kind, "semantic", "{sql}: {error}");
            assert!(error.message.contains(message), "{sql}: {error}");
        }
        assert!(
            session
                .execute_sql("SELECT x FROM t GROUP BY ALL ORDER BY y")
                .is_err()
        );
        let error = session
            .execute_sql("SELECT x AS a,y AS a FROM t GROUP BY ALL ORDER BY a")
            .err()
            .unwrap();
        assert_eq!(
            error.message,
            "ORDER BY name must identify one output column"
        );
        // Nontrivial scalar subqueries remain a recorded composition gap. In
        // particular, do not inline predicates/LIMIT or change volatile calls'
        // evaluation counts by pretending every scalar SELECT is one literal.
        for sql in [
            "SELECT (SELECT 1 WHERE FALSE),COUNT(*) FROM t GROUP BY ALL",
            "SELECT (SELECT 1 LIMIT 0),COUNT(*) FROM t GROUP BY ALL",
            "SELECT (SELECT MAX(y) FROM t),COUNT(*) FROM t GROUP BY ALL",
        ] {
            let error = session
                .execute_sql(sql)
                .err()
                .unwrap_or_else(|| panic!("accepted {sql}"));
            assert_eq!(error.kind, "unsupported", "{sql}: {error}");
            assert!(
                error.message.contains("GROUP BY ALL subquery composition"),
                "{sql}: {error}"
            );
        }
    });
}

#[test]
fn alias_expansion_is_bounded_before_copying_exponential_trees() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    session
        .execute_sql("CREATE TABLE t(x INTEGER,y INTEGER)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES(1,10),(1,20),(2,30)")
        .unwrap();
    let chain = |last: usize| {
        let mut items = vec!["x AS a0".to_owned()];
        for i in 1..=last {
            items.push(format!("a{}+a{} AS a{i}", i - 1, i - 1));
        }
        format!(
            "SELECT {},COUNT(*) FROM t GROUP BY ALL ORDER BY a0",
            items.join(",")
        )
    };
    // Nine doublings produce a substantial but bounded expression tree.
    let expected = Value::Array(
        [1_i64, 2]
            .into_iter()
            .map(|x| {
                let mut row = (0..=9).map(|shift| json!(x << shift)).collect::<Vec<_>>();
                row.push(json!(if x == 1 { 2 } else { 1 }));
                Value::Array(row)
            })
            .collect(),
    );
    assert_eq!(query(&mut session, &chain(9)), expected);
    let error = session.execute_sql(&chain(30)).err().unwrap();
    assert_eq!(error.kind, "resource");
    assert!(error.message.contains("alias expansion budget exceeded"));

    // Counting nodes alone cannot bound aliases containing large text values.
    let payload = "x".repeat(40_000);
    let sql = format!(
        "SELECT '{payload}' AS a,a||a AS b,b||b AS c,c||c AS d,d||d AS e,e||e AS f FROM t GROUP BY ALL"
    );
    let error = session.execute_sql(&sql).err().unwrap();
    assert_eq!(error.kind, "resource");
    assert!(error.message.contains("alias expansion budget exceeded"));
}
