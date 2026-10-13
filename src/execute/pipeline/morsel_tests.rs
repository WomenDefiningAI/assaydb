use super::tests::{Fixture, test_producer};
use super::*;

#[test]
fn physical_row_group_morsels_preserve_sql_batches_and_reduce_dispatches() {
    let fixture = Fixture::new(64, 16, false);
    for workers in [1, 2, 4] {
        let old = Options {
            workers,
            batch_size: 2,
            ..Options::default()
        };
        let new = Options {
            test_pipeline_partitions: Some(4),
            ..old.clone()
        };
        let prepared = fixture.prepared("SELECT k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k", &old);
        let expected = crate::execute::execute(&prepared, &old).unwrap();
        let budget = Budget::new(new.memory_bytes);
        let actual = execute(&prepared, &new, budget.clone()).unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.pipeline_source_batches, 32);
        assert_eq!(actual.metrics.pipeline_step_jobs, 4);
        assert_eq!(actual.metrics.pipeline_runtime_submitted[0], 4);
        assert_eq!(
            actual.metrics.pipeline_runtime_submitted,
            actual.metrics.pipeline_runtime_completed
        );
        if workers == 1 {
            assert_eq!(actual.metrics.pipeline_runtime_background, [0; 3]);
        }
        drop(actual);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn decoded_row_quantum_never_splits_an_original_batch() {
    let fixture = Fixture::new(250_001, 250_001, false);
    let options = Options {
        batch_size: 100_000,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let producer = test_producer(&fixture, &options, &budget);
    let mut sizes = Vec::new();
    let mut previous = 0;
    let ProducerStep::Continue(producer) = producer
        .step_morsel(&mut |next| {
            sizes.push(next.metrics.rows_read - previous);
            previous = next.metrics.rows_read;
        })
        .unwrap()
    else {
        panic!("row quantum must yield before group EOF");
    };
    assert_eq!(sizes, vec![100_000, 100_000]);
    assert_eq!(producer.metrics.pipeline_source_batches, 2);
    assert_eq!(producer.metrics.pipeline_step_jobs, 1);
    assert!(producer.cursor.is_some());
    assert_eq!(producer.metrics.rows_read, 200_000);
    drop(producer);
    assert_eq!(budget.used(), 0);
}

#[test]
fn batch_quantum_bounds_filtered_empty_and_zero_row_batches() {
    let fixture = Fixture::new(100, 100, false);
    let options = Options {
        batch_size: 1,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let mut producer = test_producer(&fixture, &options, &budget);
    // Same group/value expressions and projection; only the exact WHERE differs.
    producer.plan = Arc::new(
        fixture
            .prepared(
                "SELECT k,SUM(v),COUNT(*) FROM t WHERE k>100 GROUP BY k",
                &options,
            )
            .plan,
    );
    let ProducerStep::Continue(producer) = producer.step().unwrap() else {
        panic!("batch quantum must yield inside the group");
    };
    assert_eq!(producer.metrics.pipeline_source_batches, MORSEL_BATCHES);
    assert_eq!(producer.metrics.rows_read, MORSEL_BATCHES);
    assert_eq!(producer.metrics.rows_after_filter, 0);
    assert_eq!(producer.state.as_ref().unwrap().group_count, 0);
    assert!(producer.cursor.is_some());
    drop(producer);
    assert_eq!(budget.used(), 0);
    // Shared-source integration can deliver original zero-row batches. They
    // consume the same batch quantum even without any decoded-row progress.
    let mut metrics = Metrics::default();
    let limit = MorselLimit::new(&metrics);
    for _ in 0..MORSEL_BATCHES - 1 {
        metrics.pipeline_source_batches += 1;
        assert!(!limit.reached(&metrics));
    }
    metrics.pipeline_source_batches += 1;
    assert!(limit.reached(&metrics));
    let empty = Fixture::new(0, 8, false);
    let output = test_producer(&empty, &options, &budget).step().unwrap();
    let ProducerStep::Finished(output) = output else {
        panic!("empty source must finish");
    };
    assert_eq!(output.metrics.pipeline_source_batches, 0);
    assert_eq!(output.metrics.pipeline_step_jobs, 1);
    drop(output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn cancellation_between_original_batches_drops_live_morsel_state() {
    let fixture = Fixture::new(100, 100, false);
    let options = Options {
        batch_size: 1,
        test_pipeline_slice_bytes: Some(1 << 20),
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let producer = test_producer(&fixture, &options, &budget);
    let mut calls = 0;
    let error = producer
        .step_morsel(&mut |next| {
            calls += 1;
            assert!(next.state.as_ref().unwrap().group_count > 0);
            assert!(next.backing.is_some());
            if calls == 3 {
                options.cancellation.cancel();
            }
        })
        .err()
        .unwrap();
    assert!(error.contains("cancel"), "{error}");
    assert_eq!(calls, 3);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn quantum_returns_caller_so_queued_partition_work_can_progress() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let fixture = Fixture::new(100, 100, false);
    let options = Options {
        batch_size: 1,
        ..Options::default()
    };
    let budget = Budget::new(options.memory_bytes);
    let producer = test_producer(&fixture, &options, &budget);
    let mut runtime =
        Runtime::<ProducerStep>::new(&budget, 1, 2, options.cancellation.clone()).unwrap();
    let producer_id = runtime
        .try_submit(JobKind::Morsel, move |_| producer.step())
        .ok()
        .unwrap();
    let progressed = Arc::new(AtomicBool::new(false));
    let observed = progressed.clone();
    let partition_id = runtime
        .try_submit(JobKind::Partition, move |cancel| {
            cancel.check()?;
            observed.store(true, Ordering::Release);
            Err("partition progress marker".into())
        })
        .ok()
        .unwrap();
    assert!(runtime.help_one()); // FIFO: caller executes only this source quantum.
    assert!(!progressed.load(Ordering::Acquire));
    let done = runtime.try_completion().unwrap();
    assert_eq!(done.id, producer_id);
    let ProducerStep::Continue(producer) = done.result.ok().unwrap() else {
        panic!("expected quantum yield");
    };
    assert_eq!(producer.metrics.pipeline_source_batches, MORSEL_BATCHES);
    assert!(producer.cursor.is_some());
    assert!(runtime.help_one()); // No need to finish the remaining original group.
    let done = runtime.try_completion().unwrap();
    assert_eq!(done.id, partition_id);
    assert_eq!(done.result.err().unwrap(), "partition progress marker");
    assert!(progressed.load(Ordering::Acquire));
    assert_eq!(runtime.statistics().caller_started, [1, 1, 0]);
    drop(producer);
    drop(runtime);
    assert_eq!(budget.used(), 0);
    assert!(options.cancellation.check().is_ok());
}
