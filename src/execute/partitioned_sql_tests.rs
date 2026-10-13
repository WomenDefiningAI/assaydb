//! SQL-facing parity tests for the serial partition representation. No production
//! dispatch or scheduler is enabled by this correctness stage.
use super::*;
use crate::session::{Limits, Output as SessionOutput, Session};

fn fixture(options: Options) -> Session {
    let mut session = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
    session
        .execute_sql("CREATE TABLE t(k INT,s TEXT,v DECIMAL(38,0))")
        .unwrap();
    session.execute_sql("INSERT INTO t VALUES (3,'é',10),(1,'a',20),(3,'é',NULL),(NULL,'a',-10),(1,NULL,5),(3,'é',4),(2,'',NULL),(NULL,'a',10)").unwrap();
    session
}
fn query(session: &mut Session, sql: &str) -> Result<RecordBatch> {
    match session.execute_sql(sql).map_err(|e| e.message)? {
        SessionOutput::Query { execution, .. } => Ok(execution.batch.clone()),
        _ => Err("expected query".into()),
    }
}

#[test]
fn forced_partition_sql_matches_direct_relational_and_ordered_output() {
    let queries = [
        "SELECT k,SUM(v),AVG(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
        "SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k",
        "SELECT k,SUM(v) FROM t WHERE v IS NOT NULL GROUP BY k ORDER BY k DESC",
        "SELECT k,SUM(v) AS total FROM t GROUP BY k HAVING SUM(v)>0 ORDER BY total",
        "SELECT k,COUNT(*) FROM (SELECT k,v FROM t WHERE v>0) x GROUP BY k",
        "SELECT a.k,COUNT(*),SUM(a.v) FROM t a JOIN t b ON a.k=b.k GROUP BY a.k",
        "WITH grouped AS (SELECT k,SUM(v) AS total FROM t GROUP BY k) SELECT COUNT(*),SUM(total) FROM grouped",
        "SELECT k,SUM(v),COUNT(*) FROM t WHERE v>1000 GROUP BY k",
        // These exercise explicit fallback, not reduced query coverage.
        "SELECT SUM(v),COUNT(*) FROM t",
        "SELECT k,MIN(v),MAX(v),COUNT(DISTINCT s) FROM t GROUP BY k",
    ];
    for workers in [1, 4] {
        for batch_size in [1, 3, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let mut expected = fixture(options.clone());
            let mut actual = fixture(Options {
                test_aggregate_partitions: Some(4),
                ..options
            });
            for sql in queries {
                assert_eq!(
                    query(&mut actual, sql).unwrap(),
                    query(&mut expected, sql).unwrap(),
                    "workers={workers},batch={batch_size}: {sql}"
                );
            }
        }
    }
}

#[test]
fn forced_partition_is_selected_only_for_eligible_sql_and_never_default() {
    // Zero partitions deliberately makes admission observable: eligible queries
    // fail construction, while unsupported states retain the ordinary backend.
    let mut session = fixture(Options {
        test_aggregate_partitions: Some(0),
        ..Options::default()
    });
    assert!(
        query(&mut session, "SELECT k,SUM(v) FROM t GROUP BY k")
            .unwrap_err()
            .contains("unsupported partitioned aggregate contract")
    );
    for sql in [
        "SELECT SUM(v) FROM t",
        "SELECT k,MIN(v) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT s) FROM t GROUP BY k",
    ] {
        assert!(query(&mut session, sql).is_ok(), "{sql}");
    }
    let mut ordinary = fixture(Options::default());
    assert!(query(&mut ordinary, "SELECT k,SUM(v) FROM t GROUP BY k").is_ok());
}

#[test]
fn forced_partition_preserves_wide_cancellation_and_input_error_priority() {
    for batch_size in [1, 2, 5] {
        let mut sessions = [None,Some(3)].map(|test_aggregate_partitions| {
            let mut s = fixture(Options {batch_size,test_aggregate_partitions,..Options::default()});
            s.execute_sql("CREATE TABLE wide(k INT,v DECIMAL(38,0),d INT)").unwrap();
            s.execute_sql("INSERT INTO wide VALUES (1,99999999999999999999999999999999999999,1),(1,99999999999999999999999999999999999999,1),(1,-99999999999999999999999999999999999999,1),(1,-99999999999999999999999999999999999999,1),(2,1,0)").unwrap();
            s
        });
        for sql in [
            "SELECT k,SUM(v),AVG(v),COUNT(*) FROM wide GROUP BY k",
            "SELECT k,AVG(v) FROM wide WHERE v>0 GROUP BY k",
            "SELECT k,SUM(v) FROM wide WHERE v>0 GROUP BY k",
            "SELECT k,SUM(v),SUM(1/d) FROM wide WHERE v>0 GROUP BY k",
            "SELECT k+9223372036854775807,SUM(v) FROM wide GROUP BY k+9223372036854775807",
        ] {
            let expected = query(&mut sessions[0], sql);
            let actual = query(&mut sessions[1], sql);
            if !sql.contains("WHERE") && !sql.contains("9223372036854775807")
                || sql.contains("SELECT k,AVG(v)")
            {
                assert!(
                    expected.is_ok(),
                    "expected valid wide-state query: {expected:?}"
                );
            }
            assert_eq!(actual, expected, "batch={batch_size}: {sql}");
        }
    }
}

#[test]
fn serial_partition_stream_output_keeps_chunks_and_releases_query_credits() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", arrow::datatypes::DataType::Int64, true),
        Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]));
    let plan = bind::bind(
        &bind::parse("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k").unwrap(),
        &schema,
        None,
        true,
    )
    .unwrap();
    let prepared = Prepared {
        internal_output: false,
        plan,
        schema: schema.clone(),
        relational: None,
    };
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(3),
                Some(1),
                None,
                Some(3),
                Some(2),
            ])),
            Arc::new(Int64Array::from(vec![2, 3, 4, 5, 6])),
        ],
    )
    .unwrap();
    let mut expected = None;
    for count in [None, Some(1), Some(4)] {
        let budget = Budget::new(8 << 20);
        let options = Options {
            batch_size: 2,
            test_aggregate_partitions: count,
            ..Options::default()
        };
        let mut chunks = Vec::new();
        execute_aggregate_stream(
            &prepared,
            &options,
            budget.clone(),
            |send| {
                send(batch.slice(0, 2))?;
                send(batch.slice(2, 3))
            },
            &mut |b| {
                chunks.push(b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![2, 2]
        );
        assert_eq!(budget.used(), 0);
        if let Some(expected) = &expected {
            assert_eq!(&chunks, expected);
        } else {
            expected = Some(chunks);
        }
    }
    let budget = Budget::new(1024);
    let options = Options {
        test_aggregate_partitions: Some(4),
        ..Options::default()
    };
    assert!(
        execute_stream(&prepared, &options, budget.clone(), |send| send(
            batch.clone()
        ))
        .err()
        .unwrap()
        .contains("resource-limit")
    );
    assert_eq!(budget.used(), 0);
}
