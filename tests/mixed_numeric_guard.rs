#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::record_batch::RecordBatch;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

fn query(s: &mut Session, sql: &str) -> Result<RecordBatch, String> {
    match s.execute_sql(sql).map_err(|e| e.to_string())? {
        Output::Query { execution, .. } => Ok(execution.batch),
        _ => panic!("query"),
    }
}
fn session(workers: usize, optimized: bool) -> Session {
    let mut s = Session::new(
        &std::env::temp_dir(),
        Options {
            workers,
            relational_optimizations: optimized,
            batch_size: 1,
            ..Options::default()
        },
        Limits::default(),
    )
    .unwrap();
    s.execute_sql("CREATE TABLE t(id INTEGER NOT NULL,v INTEGER,f DOUBLE)")
        .unwrap();
    s.execute_sql("INSERT INTO t VALUES(1,3,3.0),(2,NULL,5.0)")
        .unwrap();
    s
}
#[test]
fn conditional_integer_float_consumers_now_promote_like_duckdb() {
    let group = promoted_reference::group("guard_t");
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for sql in [
                // Formerly refused for every consumer (broadened guard). Each
                // form now promotes the reachable Int arm to DOUBLE.
                "SELECT CASE WHEN CAST('1' AS REAL)=CAST('1' AS REAL) THEN 9007199254740993 ELSE CAST(5 AS REAL) END",
                "SELECT COALESCE(NULLIF(CAST('5' AS INTEGER),5),CAST(2 AS REAL))",
                "SELECT COALESCE(v,CAST(5 AS REAL)) FROM t",
                "SELECT COALESCE(v/CAST('2' AS DECIMAL(10,0)),9007199254740993) FROM t",
                "SELECT CASE WHEN v IS NULL THEN 9007199254740993 ELSE v/CAST('2' AS DECIMAL(10,0)) END FROM t",
                "SELECT CASE WHEN v IS NULL THEN CAST(5 AS REAL) ELSE v END FROM t",
                "SELECT MIN(COALESCE(v,CAST(5 AS REAL))),MAX(COALESCE(v,CAST(5 AS REAL))) FROM t",
                "SELECT NULLIF(v,COALESCE(v,CAST(5 AS REAL)))/2 FROM t",
                "SELECT CAST(COALESCE(v,CAST(5 AS REAL)) AS INTEGER)/2 FROM t",
                "SELECT CAST(COALESCE(v,CAST(5 AS REAL)) AS REAL)/2 FROM t",
                "SELECT (COALESCE(v,CAST(5 AS REAL))+CAST(1 AS REAL))/2 FROM t",
                "SELECT FACTORIAL(CAST(COALESCE(v,CAST(5 AS REAL)) AS INTEGER)) FROM t",
                "SELECT ABS(COALESCE(v,CAST(5 AS REAL))) FROM t",
                "SELECT COALESCE(v,CAST(5 AS REAL))=v FROM t",
                "SELECT COALESCE(v,CAST(5 AS REAL))+1 FROM t",
                "SELECT COALESCE(v,CAST(5 AS REAL))*0 FROM t",
                "SELECT COUNT(COALESCE(v,CAST(5 AS REAL))) FROM t",
                // These remainder fixtures require the independently approved
                // Op::Mod integration; final validation is the combined gate.
                "SELECT COALESCE(v,CAST(5 AS REAL))%2 FROM t",
                "WITH q AS (SELECT COALESCE(v,CAST(5 AS REAL)) AS x FROM t) SELECT x%2 FROM q",
                // Both exact two-row examples from the approved proposal.
                "SELECT id,COALESCE(v,CAST(5 AS REAL))/2 AS result FROM t ORDER BY id",
                "SELECT id,(CASE WHEN v IS NULL THEN CAST(5 AS REAL) ELSE v END)/2 AS result FROM t ORDER BY id",
                "SELECT NULLIF(COALESCE(v,CAST(5 AS REAL)),0)/2 FROM t",
                "SELECT ABS(-COALESCE(v,CAST(5 AS REAL)))/2 FROM t",
                "SELECT (COALESCE(v,CAST(5 AS REAL))+1)*3/2 FROM t",
                "SELECT CAST(COALESCE(v,CAST(5 AS REAL))/2 AS REAL) FROM t",
                "SELECT MAX(COALESCE(v,CAST(5 AS REAL)))/2 FROM t",
                "SELECT COALESCE(MIN(3),CAST(5 AS REAL))/2 FROM t",
                "SELECT MIN(CASE WHEN v IS NULL THEN CAST(5 AS REAL) ELSE v END)/2 FROM t",
                "SELECT x/2 FROM (SELECT COALESCE(v,CAST(5 AS REAL)) AS x FROM t) q",
                "WITH q AS (SELECT COALESCE(v,CAST(5 AS REAL)) AS x FROM t) SELECT x/2 FROM q",
                "WITH q AS (SELECT id,COALESCE(v,CAST(5 AS REAL)) AS x FROM t) SELECT a.x/2 FROM q a JOIN q b ON a.id=b.id",
                "SELECT id FROM t WHERE COALESCE(v,CAST(5 AS REAL))/2>0",
                "SELECT COUNT(*) FROM t GROUP BY COALESCE(v,CAST(5 AS REAL))/2",
                "SELECT COALESCE(v,CAST(5 AS REAL))/2 AS x FROM t ORDER BY x",
                "SELECT a.id FROM t a JOIN t b ON COALESCE(a.v,CAST(5 AS REAL))/2=b.f",
                "SELECT MAX(x)/2 FROM (SELECT COALESCE(v,CAST(5 AS REAL)) AS x FROM t) q",
                "WITH q AS (SELECT ABS(-1) AS x,v FROM t) SELECT CASE WHEN x=1 THEN COALESCE(v,CAST(5 AS REAL))/2 ELSE 0 END FROM q",
                "WITH q AS (SELECT CAST(9223372036854775808 AS INTEGER) AS x,v FROM t) SELECT CASE WHEN x=9223372036854775807 THEN COALESCE(v,CAST(5 AS REAL))/2 ELSE 0 END FROM q",
            ] {
                // Old: refused as row-dependent mixed Int/Float conditional.
                // New (Helen, Oct 15): DOUBLE promotion; independent DuckDB answer.
                promoted_reference::check(&mut s, &group, sql, &format!("{workers}/{optimized}"));
            }
        }
    }
}
#[test]
fn exact_literal_alternative_to_integer_avg_is_now_float() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for sql in [
                "SELECT COALESCE(AVG(v),3)/2 FROM t",
                "SELECT x/2 FROM (SELECT COALESCE(AVG(v),3) AS x FROM t) q",
            ] {
                let batch = query(&mut s, sql).unwrap();
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(1.5)], "{sql}");
            }
        }
    }
}
#[test]
fn homogeneous_casts_nullif_and_proven_dead_branches_stay_accepted() {
    for workers in [1, 10] {
        for optimized in [false, true] {
            let mut s = session(workers, optimized);
            for sql in [
                "SELECT CASE WHEN CAST('1' AS REAL)=CAST('1' AS REAL) THEN 9007199254740993 ELSE 5 END",
                "SELECT NULLIF(v,CAST(5 AS REAL))/2 FROM t",
                "SELECT NULLIF(v,CAST(5 AS REAL))%2 FROM t",
                "SELECT COALESCE(v%CAST(0.9 AS REAL),3)/2 FROM t",
                "SELECT COALESCE(CAST(v AS REAL),CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(v,5)/2 FROM t",
                "SELECT COALESCE(3,CAST(v AS REAL))/2 FROM t",
                "SELECT CASE WHEN TRUE THEN 3 ELSE CAST(v AS REAL) END/2 FROM t",
                "SELECT CASE WHEN FALSE THEN CAST(v AS REAL) ELSE 3 END/2 FROM t",
                "SELECT CASE WHEN TRUE THEN 3 ELSE COALESCE(v,CAST(5 AS REAL))/2 END FROM t",
                "SELECT COALESCE(id,CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(NULLIF(id,NULL),CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(NULLIF(v,v),CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(id/2,CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(SUM(CAST(NULL AS INTEGER)),CAST(5 AS REAL))/2 FROM t",
                "SELECT COALESCE(AVG(CAST(NULL AS INTEGER)),3)/2 FROM t",
                "SELECT COALESCE(MIN(CAST(NULL AS INTEGER)),CAST(5 AS REAL))/2 FROM t",
                "SELECT (CASE WHEN COUNT(NULL)=0 THEN 3 ELSE CAST(5 AS REAL) END)/2 FROM t",
                "SELECT COALESCE(AVG(v),3)/2 FROM t WHERE FALSE",
                "SELECT COALESCE(MIN(id),CAST(5 AS REAL))/2 FROM t GROUP BY id",
                "SELECT COALESCE(SUM(id),CAST(5 AS REAL))/2 FROM t GROUP BY id",
                "SELECT COALESCE(AVG(id),3)/2 FROM t GROUP BY id",
                "SELECT COALESCE(AVG(v),3)/2 FROM (SELECT v FROM t LIMIT 0) q",
            ] {
                query(&mut s, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            }
            let error = query(
                &mut s,
                "SELECT CASE WHEN TRUE THEN 3 ELSE missing END/2 FROM t",
            )
            .unwrap_err();
            assert!(error.contains("unknown or ambiguous column"), "{error}");
            let error = query(
                &mut s,
                "SELECT COALESCE(v,CAST(5 AS REAL))/2,missing FROM t",
            )
            .unwrap_err();
            assert!(error.contains("unknown or ambiguous column"), "{error}");
            let error = query(
                &mut s,
                "SELECT FACTORIAL(COALESCE(v,CAST(5 AS REAL))) FROM t",
            )
            .unwrap_err();
            assert!(error.contains("unsupported"), "{error}");
        }
    }
}
#[test]
fn promoted_mutations_match_duckdb() {
    // Old: each mutation was refused and left the snapshot intact. New: the
    // promoted expression is applied; the resulting table matches DuckDB.
    let group = promoted_reference::group("guard_mutations");
    for entry in group["entries"].as_array().unwrap() {
        let mut s = session(1, true);
        let sql = entry["sql"].as_str().unwrap();
        promoted_reference::check(&mut s, &group, sql, entry["pre"][0].as_str().unwrap());
        s.execute_sql("UPDATE t SET f=CAST(v AS REAL)").unwrap();
    }
}

#[test]
fn all_214_original_conditional_hunt_disagreements_now_match_duckdb() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mixed-conditional-hunt.json")).unwrap();
    assert_eq!(fixture["selected_count"], 214);
    for workers in [1, 10] {
        for relational_optimizations in [false, true] {
            let mut checked = 0;
            for dataset in fixture["datasets"].as_array().unwrap() {
                let mut s = Session::new(
                    &std::env::temp_dir(),
                    Options {
                        workers,
                        relational_optimizations,
                        batch_size: 1,
                        ..Options::default()
                    },
                    Limits::default(),
                )
                .unwrap();
                for setup in dataset["setup"].as_array().unwrap() {
                    s.execute_sql(setup.as_str().unwrap()).unwrap();
                }
                // Old: every record refused. New: DuckDB 1.5.6 answer (these
                // records are SQLite disagreements: DuckDB-only typing), or the
                // scoped INTEGER-cast guard error where DuckDB also errors.
                let group = promoted_reference::group(&format!("hunt214/{}", dataset["dataset"]));
                for record in dataset["queries"].as_array().unwrap() {
                    let sql = record["sql"].as_str().unwrap();
                    let context = format!("dataset{} index{}", dataset["dataset"], record["index"]);
                    promoted_reference::check(&mut s, &group, sql, &context);
                    checked += 1;
                }
            }
            assert_eq!(checked, 214);
        }
    }
}
