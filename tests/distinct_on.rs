//! DISTINCT ON goldens include actual DuckDB 2.0.0 oracle probes. Ordering
//! assertions use unique final keys; unspecified peer order is tested for local
//! determinism separately, not treated as an independent DuckDB guarantee.
use arrow::{record_batch::RecordBatch, util::display::array_value_to_string};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
};

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("expected query");
    };
    let batch: &RecordBatch = &execution.batch;
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) {
                        "NULL".into()
                    } else {
                        array_value_to_string(array.as_ref(), row).unwrap()
                    }
                })
                .collect()
        })
        .collect()
}
fn check(session: &mut Session, sql: &str, expected: &[&[&str]]) {
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect();
    assert_eq!(rows(session, sql), expected, "{sql}");
}

#[test]
fn completed_projection_distinct_on_across_execution_modes() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for optimized in [false, true] {
                for spill in [false, true] {
                    let mut session = Session::new(
                        &std::env::temp_dir(),
                        Options {
                            workers,
                            batch_size,
                            relational_optimizations: optimized,
                            spill: spill.then(|| SpillOptions {
                                directory: std::env::temp_dir(),
                                sort_run_bytes: 128,
                                ..SpillOptions::default()
                            }),
                            ..Options::default()
                        },
                        Limits::default(),
                    )
                    .unwrap();
                    session
                        .execute_sql("CREATE TABLE t(a INT,b INT,c TEXT)")
                        .unwrap();
                    session.execute_sql("INSERT INTO t VALUES (1,20,'x'),(1,10,'y'),(2,30,'z'),(NULL,40,'n'),(NULL,5,'m')").unwrap();
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a) c FROM t ORDER BY b",
                        &[&["m"], &["y"], &["z"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(1) a,b FROM t ORDER BY b",
                        &[&["NULL", "5"], &["1", "10"], &["2", "30"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(b) a AS b,c FROM t ORDER BY c",
                        &[&["NULL", "m"], &["1", "x"], &["2", "z"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a+1) c FROM t ORDER BY b",
                        &[&["m"], &["y"], &["z"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a) a,ROW_NUMBER() OVER(ORDER BY b) r FROM t QUALIFY r>1 ORDER BY b",
                        &[&["1", "2"], &["2", "4"], &["NULL", "5"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(1+0) a,b FROM t ORDER BY b",
                        &[&["NULL", "5"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(x+0) a AS x,c FROM t ORDER BY c",
                        &[&["NULL", "m"], &["1", "x"], &["2", "z"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(b+0) a AS b,c FROM t ORDER BY c",
                        &[
                            &["NULL", "m"],
                            &["NULL", "n"],
                            &["1", "x"],
                            &["1", "y"],
                            &["2", "z"],
                        ],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(x) a AS x,b AS x,c FROM t ORDER BY c",
                        &[
                            &["NULL", "5", "m"],
                            &["NULL", "40", "n"],
                            &["1", "20", "x"],
                            &["1", "10", "y"],
                            &["2", "30", "z"],
                        ],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a) c FROM t ORDER BY b DESC LIMIT 1 OFFSET 1",
                        &[&["z"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a) a,SUM(b) s FROM t GROUP BY a HAVING SUM(b)>20 ORDER BY s,a",
                        &[&["1", "30"], &["2", "30"], &["NULL", "45"]],
                    );
                    check(
                        &mut session,
                        "SELECT DISTINCT ON(a,b) c FROM t WHERE b<0 ORDER BY b",
                        &[],
                    );
                    check(
                        &mut session,
                        "SELECT c FROM (SELECT DISTINCT ON(a) c,b FROM t ORDER BY b) q ORDER BY b",
                        &[&["m"], &["y"], &["z"]],
                    );
                    for sql in [
                        "SELECT DISTINCT ON(NULL) a FROM t",
                        "SELECT DISTINCT ON(1.0) a FROM t",
                        "SELECT DISTINCT ON(0) a FROM t",
                        "SELECT DISTINCT ON(-1) a FROM t",
                        "SELECT DISTINCT ON(2) a FROM t ORDER BY b",
                    ] {
                        assert!(session.execute_sql(sql).is_err(), "{sql}");
                    }
                }
            }
        }
    }
}

#[test]
fn distinct_on_alias_expansion_has_planning_limits() {
    let mut session =
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
    check(
        &mut session,
        "SELECT DISTINCT ON(length(a)+length(a)) 'abc' AS a",
        &[&["abc"]],
    );
    // Original SQL is small; expanding each alias occurrence would copy a
    // large string repeatedly. Reject before the expanded payload is allocated.
    let key = vec!["length(a)"; 40].join("+");
    let sql = format!("SELECT DISTINCT ON({key}) '{}' AS a", "x".repeat(40000));
    let error = session
        .execute_sql(&sql)
        .err()
        .expect("expansion must refuse");
    assert!(
        error
            .to_string()
            .contains("DISTINCT ON alias expansion budget"),
        "{error}"
    );
    // A broad alias expression repeated in separate keys also consumes the
    // total node allowance, even if each individual key is shallow.
    let alias = vec!["1"; 100].join("+");
    let keys = (0..50)
        .map(|i| format!("a+{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("SELECT DISTINCT ON({keys}) {alias} AS a");
    let error = session
        .execute_sql(&sql)
        .err()
        .expect("node expansion must refuse");
    assert!(
        error
            .to_string()
            .contains("DISTINCT ON alias expansion budget"),
        "{error}"
    );
}

#[test]
fn distinct_on_alias_expansion_rechecks_nesting() {
    // Direct parser checks require the same stack boundary as public execution.
    assaydb::engine_stack::run(|| {
        let mut session =
            Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
        // Each separate expression is safely below the 250-level nesting fence.
        // Substitution joins the paths into >250 levels, while fewer than 300
        // expression nodes and 2KiB remain well below the node and byte budgets.
        let levels = assaydb::nesting::LIMIT / 2 + 10;
        let wrap =
            |inner: &str| format!("{}{}{}", "ABS(".repeat(levels), inner, ")".repeat(levels));
        let key = wrap("a");
        let alias = wrap("1");
        let sql = format!("SELECT DISTINCT ON({key}) {alias} AS a");
        assert!(
            assaydb::nesting::measure(&sql).unwrap().is_some(),
            "original SQL must pass the fence"
        );
        assert!(
            assaydb::nesting::parse(&sql).is_ok(),
            "original SQL must parse"
        );
        let expanded = format!("SELECT {}", wrap(&alias));
        assert!(
            assaydb::nesting::measure(&expanded).unwrap().is_none(),
            "expanded SQL must exceed nesting"
        );
        let error = session
            .execute_sql(&sql)
            .err()
            .expect("expanded nesting must refuse");
        assert!(
            error
                .to_string()
                .contains("DISTINCT ON expanded expression exceeds nesting limits"),
            "{error}"
        );
    })
    .unwrap();
}
