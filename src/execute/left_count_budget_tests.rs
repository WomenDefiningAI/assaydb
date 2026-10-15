//! These SQL tests need the private executor entry point to inspect its budget.
use super::{Execution, Options, execute, execute_in_budget};
use crate::{memory::Budget, rel};
use arrow::array::{Array, Int64Array};
use std::sync::Arc;

#[path = "../../tests/left_join_count/fixture.rs"]
mod fixture;
use fixture::{Fixture, custom_fixture_with_nullability};

impl Fixture {
    fn run(&self, sql: &str, options: &Options) -> Result<Execution, String> {
        execute(&rel::prepare(sql, &self.tables, options)?, options)
    }

    // The same prepared plan runs with an observable budget; callers disable spill.
    fn run_in_budget(
        &self,
        sql: &str,
        options: &Options,
        budget: Budget,
    ) -> Result<Execution, String> {
        assert!(options.spill.is_none());
        execute_in_budget(&rel::prepare(sql, &self.tables, options)?, options, budget)
    }
}

#[test]
fn inner_join_aggregation_bypasses_counted_retry() {
    let f = Fixture::new(67, 79, false);
    let options = Options {
        workers: 1,
        batch_size: 13,
        spill: None,
        parquet_cache_bytes: None,
        ..Options::default()
    };
    // Ordinary joined aggregates must retain their original execution path,
    // including COUNT, computed SUM and first-seen groups across batches.
    for sql in [
        "SELECT l.g,COUNT(r.v) FROM l JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,SUM(l.g * r.v),COUNT(*) FROM l JOIN r ON l.k=r.k GROUP BY l.g",
    ] {
        let expected = f.run(sql, &options).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = f
                .run(
                    sql,
                    &Options {
                        workers,
                        ..options.clone()
                    },
                )
                .unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}, workers={workers}");
            assert_eq!(actual.metrics.fused_left_count_joins, 0);
            assert_eq!(actual.metrics.fused_left_count_serial_retries, 0);
        }
    }
}

#[test]
fn counted_probe_review_example_fits_sixteen_mib_at_every_worker_count() {
    let rows = 1_310_720;
    let batch_size = 131_072;
    let f = custom_fixture_with_nullability(
        vec![
            ("k", Arc::new(Int64Array::from(vec![1; rows]))),
            ("g", Arc::new(Int64Array::from(vec![1; rows]))),
        ],
        vec![
            ("k", Arc::new(Int64Array::from(vec![1]))),
            ("v", Arc::new(Int64Array::from(vec![1]))),
        ],
        batch_size,
        false,
    );
    let sql = "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
    let options = Options {
        workers: 1,
        batch_size,
        memory_bytes: 16 << 20,
        spill: None,
        parquet_cache_bytes: None,
        relational_optimizations: true,
        ..Options::default()
    };
    let expected = f.run(sql, &options).unwrap();
    assert_eq!(expected.batch.num_rows(), 1);
    assert_eq!(
        expected
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        expected
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        rows as i64
    );
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(options.memory_bytes);
        let actual = f
            .run_in_budget(
                sql,
                &Options {
                    workers,
                    ..options.clone()
                },
                budget.clone(),
            )
            .unwrap();
        assert_eq!(actual.batch, expected.batch, "workers={workers}");
        assert_eq!(actual.metrics.fused_left_count_joins, 1);
        assert_eq!(actual.metrics.fused_left_count_rows, rows);
        assert_eq!(
            actual.metrics.parallel_joins, 0,
            "tight budget must probe serially"
        );
        assert_eq!(
            actual.metrics.fused_left_count_serial_retries, 0,
            "headroom admission must handle the review case directly"
        );
        assert_eq!(actual.metrics.auto_spill_retries, 0);
        assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
        drop(actual);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn counted_join_tight_budgets_keep_serial_results_and_resource_errors() {
    // Multiple count-build owners and many distinct downstream groups exercise
    // both sides of the serial retry boundary, including failures before probe.
    let f = Fixture::new(521, 907, true);
    let sql = "SELECT l.k,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.k";
    let options = Options {
        workers: 1,
        batch_size: 31,
        memory_bytes: 64 << 20,
        spill: None,
        parquet_cache_bytes: None,
        ..Options::default()
    };
    let reference = f.run(sql, &options).unwrap();
    let peak = reference.metrics.peak_reserved_bytes;
    for memory_bytes in [8192, peak / 2, peak, peak + 65536] {
        let serial_options = Options {
            memory_bytes,
            ..options.clone()
        };
        let expected = f.run(sql, &serial_options);
        for workers in [1, 2, 4, 10] {
            let budget = Budget::new(memory_bytes);
            let actual = f.run_in_budget(
                sql,
                &Options {
                    workers,
                    ..serial_options.clone()
                },
                budget.clone(),
            );
            match (&expected, &actual) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(actual.batch, expected.batch, "{memory_bytes}/{workers}");
                    assert!(
                        actual.metrics.fused_left_count_serial_retries <= usize::from(workers > 1)
                    );
                }
                (Err(expected), Err(actual)) => {
                    assert_eq!(actual, expected, "{memory_bytes}/{workers}")
                }
                _ => panic!("worker-dependent outcome at {memory_bytes}/{workers}"),
            }
            drop(actual);
            assert_eq!(budget.used(), 0);
        }
    }
}
