use arrow::array::Int64Array;
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};

#[test]
fn explicit_hints_control_cte_boundary_and_keep_results() {
    for optimized in [false, true] {
        let mut session = Session::new(
            &std::env::temp_dir(),
            Options {
                relational_optimizations: optimized,
                ..Options::default()
            },
            Limits::default(),
        )
        .unwrap();
        for (hint, expected_materializations) in [("MATERIALIZED", 1), ("NOT MATERIALIZED", 0)] {
            for uses in [1, 2] {
                let body = if uses == 1 {
                    "SELECT x FROM c"
                } else {
                    "SELECT x FROM c UNION ALL SELECT x FROM c"
                };
                let sql = format!("WITH c(x) AS {hint} (SELECT 7) {body}");
                // Run twice to detect a boundary that accidentally shares across executions.
                for _ in 0..2 {
                    let Output::Query { execution, .. } = session.execute_sql(&sql).unwrap() else {
                        panic!("query");
                    };
                    assert_eq!(execution.batch.num_rows(), uses);
                    let values = execution
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert!(values.values().iter().all(|n| *n == 7));
                    assert_eq!(
                        execution.metrics.cte_materializations, expected_materializations,
                        "{sql}, optimized={optimized}"
                    );
                    assert_eq!(
                        execution.metrics.cte_reuses,
                        if hint == "MATERIALIZED" { uses - 1 } else { 0 },
                        "{sql}"
                    );
                }
            }
        }
    }
}
