//! Actual generic-runtime handoffs, still excluded from production compilation.
use super::super::tests::{plan, state};
use super::*;
use crate::{
    pipeline_runtime::{JobKind, Runtime},
    spill::Cancellation,
};
use std::{fs::File, sync::mpsc, time::Duration};

fn jobs(
    owner: &mut OwnedPartitions,
    p: &Plan,
    e: &crate::expr_program::ExpressionProgram,
    rows: &[super::super::tests::Row],
    budget: &Budget,
) -> Vec<OwnedJob> {
    let raw = state(p, e, rows, budget)
        .into_raw(
            RawFunding::SharedQuery(budget.clone()),
            owner.next_sequence(),
        )
        .unwrap();
    owner
        .submit(p, e, Arc::new(raw), &Cancellation::default())
        .unwrap()
}
#[test]
fn partition_runtime_runs_after_real_parquet_eof_and_uses_caller_only_at_one_worker() {
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ArrowReaderMetadata};
    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = std::env::temp_dir().join(format!(
        "assaydb-owned-runtime-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let fixture = Fixture(path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Decimal128(38, 0), true),
        Field::new("s", DataType::Utf8, true),
        Field::new("v", DataType::Decimal128(38, 2), true),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                Decimal128Array::from_iter((0..32).map(|i| Some(i % 7)))
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter((0..32).map(|_| Some("é")))),
            Arc::new(
                Decimal128Array::from_iter((0..32).map(Some))
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create_new(&fixture.0).unwrap(), schema, None).unwrap();
    writer.write(&input).unwrap();
    writer.close().unwrap();
    drop(input);
    let (p, e, output_schema) = plan("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", false);
    let p = Arc::new(p);
    let e = Arc::new(e);
    for workers in [1, 3] {
        let budget = Budget::new(16 << 20);
        let cancel = Cancellation::default();
        let mut runtime = Runtime::new(&budget, workers, 4, cancel.clone()).unwrap();
        let file = File::open(&fixture.0).unwrap();
        let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
        let options = Options {
            workers: 1,
            batch_size: 3,
            parquet_cache_bytes: None,
            ..Options::default()
        };
        let mut reader = crate::parquet_scan::Reader::new(
            file,
            metadata,
            ProjectionMask::all(),
            None,
            &options,
            &budget,
        )
        .unwrap();
        let mut producer = Aggregation::new(&p, &e, budget.clone(), true).unwrap();
        producer.disable_dense();
        let mut last_batch = 0;
        for (source_batch, b) in reader.by_ref().enumerate() {
            let b = b.unwrap();
            let mut input = crate::expr::Input {
                len: b.num_rows(),
                columns: b
                    .columns()
                    .iter()
                    .map(|a| {
                        crate::expr::normalize_input(
                            a,
                            &Type::from_arrow(a.data_type()).unwrap(),
                            &budget,
                        )
                        .map(Some)
                    })
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
            };
            let (keys, values) = e.evaluate(&mut input, &budget).unwrap();
            let tokens = (0..b.num_rows())
                .map(|row| EncounterToken::new(0, source_batch as u64, row))
                .collect::<Vec<_>>();
            producer
                .update_with_encounters(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    EvaluatedBatch {
                        rows: b.num_rows(),
                        keys,
                        values,
                        prepared_keys: None,
                    },
                    &tokens,
                )
                .unwrap();
            last_batch = source_batch;
        }
        assert!(reader.next().is_none());
        drop(reader);
        assert!(last_batch > 0);
        let expected_credit = budget
            .reserve(
                producer
                    .output_bytes(&p, &e, 0..producer.group_count)
                    .unwrap(),
            )
            .unwrap();
        let expected = producer
            .output_batch(&p, &e, &output_schema, 0..producer.group_count)
            .unwrap();
        let raw = Arc::new(
            producer
                .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
                .unwrap(),
        );
        let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), 4).unwrap();
        let owned_jobs = owner.submit(&p, &e, raw, &cancel).unwrap();
        let count = owned_jobs.len();
        let (sent, received) = mpsc::channel();
        for job in owned_jobs {
            let p = p.clone();
            let e = e.clone();
            let sent = sent.clone();
            assert!(
                runtime
                    .try_submit(JobKind::Partition, move |cancel| {
                        let completion = job.run(&p, &e, cancel);
                        let thread = std::thread::current().id();
                        sent.send(thread).unwrap();
                        Ok((completion, thread))
                    })
                    .is_ok()
            );
        }
        let caller = std::thread::current().id();
        if workers > 1 {
            // No caller assistance has happened: this proves useful helper work
            // after the actual source reader exhausted and released its buffers.
            assert_ne!(
                received.recv_timeout(Duration::from_secs(2)).unwrap(),
                caller
            );
        }
        let mut done = 0;
        while let Some(completion) = runtime.next_completion() {
            let (completion, thread) = completion.result.unwrap();
            if workers == 1 {
                assert_eq!(thread, caller);
            }
            owner.acknowledge(completion).unwrap();
            done += 1;
        }
        assert_eq!(done, count);
        drop(runtime);
        assert!(cancel.check().is_ok());
        owner.finish(&cancel).unwrap();
        let output_credit = budget
            .reserve(owner.output_bytes(&p, 0..owner.group_count()).unwrap())
            .unwrap();
        assert_eq!(
            owner
                .output_batch(&p, &e, &output_schema, 0..owner.group_count())
                .unwrap(),
            expected
        );
        drop(output_credit);
        drop(expected);
        drop(expected_credit);
        drop(owner);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak_workers(), workers - 1);
    }
}

#[test]
fn runtime_higher_error_finishes_first_without_cancelling_lower_indexed_job() {
    let (p, e, _) = plan("SELECT SUM(v),COUNT(*) FROM t GROUP BY k", false);
    let p = Arc::new(p);
    let e = Arc::new(e);
    let rows = (0..20)
        .map(|i| (Some(i), None, Some(1)))
        .collect::<Vec<_>>();
    let budget = Budget::new(8 << 20);
    let cancel = Cancellation::default();
    let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), 2).unwrap();
    owner
        .routing
        .set_hash(ahash::RandomState::with_seeds(1, 2, 3, 4));
    for job in jobs(&mut owner, &p, &e, &rows, &budget) {
        owner.acknowledge(job.run(&p, &e, &cancel)).unwrap();
    }
    let count_slot = owner
        .slots
        .iter()
        .position(|slot| slot.as_ref().unwrap().state.numeric_map.get(0).is_some())
        .unwrap();
    let sum_slot = 1 - count_slot;
    let count_group = owner.slots[count_slot]
        .as_ref()
        .unwrap()
        .state
        .numeric_map
        .get(0)
        .unwrap();
    owner.slots[count_slot].as_mut().unwrap().state.states[1].counts[count_group] = u64::MAX;
    owner.slots[sum_slot].as_mut().unwrap().state.states[0]
        .sums
        .as_mut()
        .unwrap()[0] = i256::MAX;
    let mut owned_jobs = jobs(&mut owner, &p, &e, &rows, &budget);
    let lower = owned_jobs.remove(owned_jobs.iter().position(|j| j.slot == sum_slot).unwrap());
    let higher = owned_jobs.pop().unwrap();
    assert!(owned_jobs.is_empty());
    let mut runtime = Runtime::new(&budget, 3, 2, cancel.clone()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let lp = p.clone();
    let le = e.clone();
    assert!(
        runtime
            .try_submit(JobKind::Partition, move |cancel| {
                started_tx.send(std::thread::current().id()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                Ok(lower.run(&lp, &le, cancel))
            })
            .is_ok()
    );
    assert_ne!(
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        std::thread::current().id()
    );
    assert!(
        runtime
            .try_submit(JobKind::Partition, move |cancel| Ok(
                higher.run(&p, &e, cancel)
            ))
            .is_ok()
    );
    let higher = runtime.next_completion().unwrap().result.unwrap();
    assert!(
        higher
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("count overflow")
    );
    owner.acknowledge(higher).unwrap();
    assert!(cancel.check().is_ok());
    release_tx.send(()).unwrap();
    let lower = runtime.next_completion().unwrap().result.unwrap();
    let error = owner.acknowledge(lower).unwrap_err();
    assert!(error.contains("accumulator overflow"));
    assert!(runtime.next_completion().is_none());
    drop(runtime);
    assert_eq!(owner.finish(&Cancellation::default()).unwrap_err(), error);
    drop(owner);
    assert_eq!(budget.used(), 0);
}

#[test]
fn runtime_abort_joins_transferred_partition_before_releasing_base_backing() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    let p = Arc::new(p);
    let e = Arc::new(e);
    let budget = Budget::new(4 << 20);
    let cancel = Cancellation::default();
    let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), 1).unwrap();
    let weak = Arc::downgrade(&owner.inner._metadata);
    let job = jobs(&mut owner, &p, &e, &[(Some(1), None, None)], &budget)
        .pop()
        .unwrap();
    let mut runtime = Runtime::new(&budget, 2, 1, cancel.clone()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    assert!(
        runtime
            .try_submit(JobKind::Partition, move |cancel| {
                started_tx.send(()).unwrap();
                while cancel.check().is_ok() {
                    std::thread::yield_now();
                }
                Ok(job.run(&p, &e, cancel))
            })
            .is_ok()
    );
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(owner);
    assert!(weak.upgrade().is_some());
    drop(runtime);
    assert!(cancel.check().is_err());
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    let restored = budget.workers(1, 1);
    assert_eq!(restored.count, 1);
    drop(restored);
}
