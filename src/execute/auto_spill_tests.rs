use super::*;
use crate::spill::{SpillMode, SpillOptions};
use std::cell::Cell;
#[path = "../../tests/support/auto_spill.rs"]
mod support;
use support::{Fixture, QUERIES, assert_bits};

#[test]
fn interrupted_decoder_retries_through_public_execute_and_joins_workers() {
    let fixture = Fixture::new();
    let sql = "SELECT k,SUM(v),SUM(d),COUNT(*) FROM t GROUP BY k ORDER BY k";
    for workers in [2, 4] {
        let interruption = Arc::new(std::sync::Mutex::new(PipelineInterruption::default()));
        let options = Options {
            workers,
            batch_size: 64,
            memory_bytes: 64 << 20,
            parquet_cache_bytes: Some(0),
            test_pipeline_default: true,
            test_pipeline_interruption: Some(interruption.clone()),
            spill: Some(SpillOptions {
                mode: SpillMode::Auto,
                directory: fixture.root.join("spill"),
                sort_run_bytes: 64 << 10,
                ..SpillOptions::default()
            }),
            ..Options::default()
        };
        let prepared = prepare(sql, &fixture.tables, &options).unwrap();
        assert!(prepared.auto_simple.is_some());
        let relational = crate::rel::prepare(sql, &fixture.tables, &options).unwrap();
        let expected = execute(&relational, &options).unwrap();
        let actual = execute(&prepared, &options).unwrap();
        assert_bits(&actual.batch, &expected.batch, sql);
        assert_eq!(actual.metrics.auto_spill_retries, 1);
        assert!(actual.metrics.spill.files_created > 0);
        assert!(
            actual
                .metrics
                .auto_spill_initial_error
                .as_ref()
                .unwrap()
                .starts_with("resource-limit: accounted memory ")
        );
        let interruption = interruption.lock().unwrap();
        assert!(interruption.aggregated_rows > 0);
        assert!(interruption.queued_sources > 0);
        let abandoned = interruption.budget.as_ref().expect("interrupted pipeline");
        assert!(abandoned.peak() > 0);
        assert!(abandoned.peak_workers() > 0);
        assert_eq!(abandoned.used(), 0);
        assert_eq!(abandoned.active_workers(), 0);
        let report = interruption
            .attempt_report
            .as_ref()
            .expect("interrupted direct attempt report");
        assert_eq!(report.candidate_attempts, 1);
        assert_eq!(report.fallback_attempts, 1);
        assert!(
            report
                .candidate_error
                .as_ref()
                .unwrap()
                .starts_with("resource-limit: accounted memory ")
        );
        fixture.assert_clean();
    }
}

#[test]
fn accounted_failure_retries_retained_relational_plan_once_without_leaks() {
    let fixture = Fixture::new();
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 64,
            memory_bytes: 64 << 20,
            parquet_cache_bytes: Some(0),
            test_pipeline_default: true,
            spill: Some(SpillOptions {
                mode: SpillMode::Auto,
                directory: fixture.root.join("spill"),
                sort_run_bytes: 64 << 10,
                ..SpillOptions::default()
            }),
            ..Options::default()
        };
        for sql in QUERIES.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let prepared = prepare(sql, &fixture.tables, &options).unwrap();
            assert!(prepared.auto_simple.is_some(), "{sql}");
            let relational = crate::rel::prepare(sql, &fixture.tables, &options).unwrap();
            let expected = execute(&relational, &options).unwrap();
            let mut forced_options = options.clone();
            forced_options.spill.as_mut().unwrap().mode = SpillMode::Force;
            let forced = execute(&relational, &forced_options).unwrap();
            let calls = Cell::new(0);
            let actual = execution_policy(&options, |attempt_options, budget| {
                calls.set(calls.get() + 1);
                let result = execute_in_budget(&prepared, attempt_options, budget.clone())?;
                if calls.get() == 1 {
                    assert_bits(&result.batch, &expected.batch, sql);
                    assert!(attempt_options.spill.is_none());
                    assert_eq!(
                        result.metrics.pipeline_candidate_attempts,
                        usize::from(workers > 1),
                        "{sql}"
                    );
                    // Deterministic pressure at the unpublished result boundary:
                    // use the real Budget refusal, then drop the whole attempt.
                    // This exercises policy/dispatch without tuning a fragile
                    // fixture size to a platform-dependent allocation threshold.
                    let error = budget.reserve(options.memory_bytes + 1).unwrap_err();
                    drop(result);
                    assert_eq!(budget.used(), 0, "{sql}");
                    assert_eq!(budget.active_workers(), 0, "{sql}");
                    Err(error)
                } else {
                    assert_eq!(calls.get(), 2);
                    assert_eq!(
                        attempt_options.spill.as_ref().unwrap().mode,
                        SpillMode::Force
                    );
                    assert_bits(&result.batch, &forced.batch, sql);
                    assert_eq!(result.metrics.pipeline_candidate_attempts, 0, "{sql}");
                    if result.batch.num_rows() > 0 {
                        assert!(result.metrics.spill.files_created > 0, "{sql}");
                    }
                    Ok(result)
                }
            })
            .unwrap();
            assert_eq!(calls.get(), 2);
            assert_eq!(actual.metrics.auto_spill_retries, 1);
            assert!(
                actual
                    .metrics
                    .auto_spill_initial_error
                    .as_ref()
                    .unwrap()
                    .starts_with("resource-limit: accounted memory")
            );
            assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
            assert_bits(&actual.batch, &forced.batch, sql);
            fixture.assert_clean();
        }
    }
}
