use super::super::tests::{keys, options, stored};
use super::*;
use arrow::datatypes::DataType;
use std::sync::{
    Barrier, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn parallel(
    prepared: &Prepared,
    options: &Options,
    budget: &Budget,
    storage: Arc<crate::rel::Materialized>,
    hook: Option<Hook>,
) -> Result<(Option<Proof>, Report)> {
    let mut runtime = None;
    let mut report = Report::default();
    let result = prove(
        prepared,
        options,
        budget,
        storage,
        &mut runtime,
        &mut report,
        hook,
    );
    if let Some(runtime) = runtime {
        runtime.shutdown_for_retry()?;
    }
    result.map(|proof| (proof, report))
}
type DomainSignature = (Range<usize>, Option<(i128, i128)>, Option<(i128, usize)>);
type ProofSignature = (u64, usize, Vec<DomainSignature>);
fn signature(proof: &Option<Proof>) -> Option<ProofSignature> {
    proof.as_ref().map(|p| {
        (
            p.rows,
            p.dense_bytes,
            p.domains
                .iter()
                .map(|d| (d.batches.clone(), d.bounds, d.window))
                .collect(),
        )
    })
}
#[test]
fn parallel_proof_matches_original_serial_admission_and_windows() {
    let cases = [
        keys(&[&[3, 1, 2], &[3, 4], &[], &[4, 6, 5], &[7, 8], &[8, 9]]),
        keys(&[&[9, 8], &[8, 7], &[], &[6, 4, 5], &[4, 3], &[2, 1]]),
        keys(&[&[1], &[1], &[1], &[1]]),
        keys(&[&[], &[], &[], &[]]),
        keys(&[&[0, 2], &[1, 3]]),
        keys(&[&[0, 100_000_000], &[100_000_001]]),
        vec![
            vec![(None, Some(1), Some(1))],
            vec![(Some(2), Some(1), Some(1))],
        ],
    ];
    for physical in [
        DataType::Int32,
        DataType::Int64,
        DataType::Decimal128(38, 0),
        DataType::Decimal128(38, 2),
    ] {
        for chunks in &cases {
            for workers in [1, 2, 4, 10] {
                let budget = Budget::new(64 << 20);
                let (prepared, storage) = stored(
                    chunks,
                    physical.clone(),
                    "SELECT k,SUM(v) FROM t GROUP BY k",
                    &budget,
                );
                let entry = budget.used();
                let opt = options(workers);
                let expected = serial_proof(&prepared, &opt, &budget, &storage).unwrap();
                let (actual, _) =
                    parallel(&prepared, &opt, &budget, storage.clone(), None).unwrap();
                assert_eq!(signature(&actual), signature(&expected));
                drop(actual);
                drop(expected);
                assert_eq!(budget.used(), entry);
                assert_eq!(budget.active_workers(), 0);
                assert!(opt.cancellation.check().is_ok());
                assert_eq!(Arc::strong_count(&storage), 1);
                drop(storage);
                assert_eq!(budget.used(), 0);
            }
        }
    }
}
#[test]
fn proof_runs_off_caller_and_reduces_in_source_order() {
    let budget = Budget::new(8 << 20);
    let (prepared, storage) = stored(
        &keys(&[&[1], &[2], &[3], &[4]]),
        DataType::Int64,
        "SELECT k,SUM(v) FROM t GROUP BY k",
        &budget,
    );
    let entry = budget.used();
    let barrier = Arc::new(Barrier::new(2));
    let background = Arc::new(AtomicUsize::new(0));
    let hook: Hook = Arc::new({
        let barrier = barrier.clone();
        let background = background.clone();
        move |_, batch| {
            if batch == 0 || batch == 2 {
                if std::thread::current()
                    .name()
                    .is_some_and(|n| n.starts_with("assaydb-pipeline-"))
                {
                    background.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait();
            }
            Ok(())
        }
    });
    let (proof, report) =
        parallel(&prepared, &options(2), &budget, storage.clone(), Some(hook)).unwrap();
    assert_eq!(proof.as_ref().unwrap().rows, 4);
    assert!(background.load(Ordering::Relaxed) > 0);
    assert!(report.proof_background_jobs > 0);
    assert_eq!(report.proof_jobs, 2);
    drop(proof);
    assert_eq!(budget.used(), entry);
    assert_eq!(budget.active_workers(), 0);
}
#[test]
fn proof_quantum_bounds_empty_batches_and_original_row_batches() {
    for large in [false, true] {
        let budget = Budget::new(64 << 20);
        let rows = if large { 65536 } else { 0 };
        let chunks = vec![vec![(Some(1), Some(1), Some(1)); rows]; if large { 5 } else { 40 }];
        let (prepared, storage) = stored(
            &chunks,
            DataType::Int64,
            "SELECT k,SUM(v) FROM t GROUP BY k",
            &budget,
        );
        let visits = Arc::new(AtomicUsize::new(0));
        let input = Arc::new(Input {
            column: 0,
            columns: prepared.plan.columns.clone(),
            schema: prepared.schema,
            hook: Some(Arc::new({
                let visits = visits.clone();
                move |_, _| {
                    visits.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            })),
            storage,
            _metadata: Arc::new(budget.reserve(8192).unwrap()),
        });
        let mut producer = ProofProducer {
            rank: 0,
            batches: 0..chunks.len(),
            next: 0..chunks.len(),
            bounds: None,
            rows: 0,
            declined: false,
            input,
        };
        producer = producer.step(&Cancellation::default()).unwrap();
        assert_eq!(visits.load(Ordering::Relaxed), if large { 2 } else { 16 });
        assert_eq!(producer.rows, if large { 128 * 1024 } else { 0 });
        while !producer.next.is_empty() {
            producer = producer.step(&Cancellation::default()).unwrap();
        }
        assert_eq!(visits.load(Ordering::Relaxed), chunks.len());
        assert_eq!(producer.rows, (rows * chunks.len()) as u64);
        drop(producer);
        assert_eq!(budget.used(), 0);
    }
}
fn release(signal: &(Mutex<bool>, Condvar)) {
    *signal.0.lock().unwrap() = true;
    signal.1.notify_all();
}
fn await_later(signal: &(Mutex<bool>, Condvar)) {
    let guard = signal.0.lock().unwrap();
    let (guard, timed) = signal
        .1
        .wait_timeout_while(guard, std::time::Duration::from_secs(5), |ready| !*ready)
        .unwrap();
    assert!(
        *guard && !timed.timed_out(),
        "later source proof did not run"
    );
}
#[test]
fn reversed_proof_errors_decline_and_panic_preserve_precedence_and_cleanup() {
    for mode in 0..3 {
        let budget = Budget::new(8 << 20);
        let mut chunks = keys(&[&[1], &[2]]);
        if mode != 0 {
            chunks[0][0].0 = None;
        }
        let (prepared, storage) = stored(
            &chunks,
            DataType::Int64,
            "SELECT k,SUM(v) FROM t GROUP BY k",
            &budget,
        );
        let entry = budget.used();
        let signal = Arc::new((Mutex::new(false), Condvar::new()));
        let hook: Hook = Arc::new(move |rank, _| {
            if rank == 0 {
                await_later(&signal);
                if mode == 0 {
                    return Err("earlier proof error".into());
                }
            } else {
                release(&signal);
                if mode == 2 {
                    panic!("injected proof panic");
                }
                return Err("later proof error".into());
            }
            Ok(())
        });
        let opt = options(2);
        let result = parallel(&prepared, &opt, &budget, storage.clone(), Some(hook));
        match mode {
            0 => assert_eq!(result.err().unwrap(), "earlier proof error"),
            1 => assert!(result.unwrap().0.is_none()),
            _ => assert!(result.err().unwrap().contains("panicked; retry forbidden")),
        }
        assert_eq!(budget.used(), entry);
        assert_eq!(budget.active_workers(), 0);
        assert!(opt.cancellation.check().is_ok());
    }
}
#[test]
fn cancelled_parallel_proof_and_speculative_budget_denial_restore_owners() {
    let budget = Budget::new(8 << 20);
    let (prepared, storage) = stored(
        &keys(&[&[1], &[2], &[3], &[4]]),
        DataType::Int64,
        "SELECT k,SUM(v) FROM t GROUP BY k",
        &budget,
    );
    let entry = budget.used();
    let opt = options(2);
    let cancel = opt.cancellation.clone();
    let hook: Hook = Arc::new(move |_, _| {
        cancel.cancel();
        Ok(())
    });
    assert!(
        parallel(&prepared, &opt, &budget, storage.clone(), Some(hook))
            .err()
            .unwrap()
            .contains("cancel")
    );
    assert!(opt.cancellation.check().is_err());
    assert_eq!(budget.used(), entry);
    assert_eq!(budget.active_workers(), 0);
    for available in [0, 8192, 32 * 1024, 64 * 1024] {
        let block = budget.reserve(budget.limit() - entry - available).unwrap();
        let used = budget.used();
        let opt = options(2);
        let result = super::super::try_execute(
            &prepared,
            &opt,
            budget.clone(),
            storage.clone(),
            Arc::new(Attempt::default()),
            &mut |_| Ok(()),
        );
        // Depending on pressure, either speculative proof declines cleanly or
        // admitted state allocation returns the existing retryable resource.
        if let Err(error) = result {
            assert!(
                error.starts_with("resource-limit: accounted memory ")
                    || error == "pipeline closure memory limit"
            );
        }
        assert_eq!(budget.used(), used);
        assert_eq!(budget.active_workers(), 0);
        assert!(opt.cancellation.check().is_ok());
        drop(block);
    }
    assert_eq!(budget.used(), entry);
}

// Frozen pre-change serial proof, retained as an admission oracle only.
fn serial_proof(
    prepared: &Prepared,
    options: &Options,
    budget: &Budget,
    storage: &crate::rel::Materialized,
) -> Result<Option<Proof>> {
    options.cancellation.check()?;
    if !eligible(prepared, options) || budget.active_workers() != 0 || storage.batches.len() < 2 {
        return Ok(None);
    }
    if !storage.budget.same_domain(budget) {
        return Err("source ownership funding domain mismatch".into());
    }
    let Kind::Column(column) = prepared.plan.groups[0].kind else {
        unreachable!()
    };
    let count = options.workers.min(storage.batches.len());
    // Covers proof/range vectors, retained owners, completion bookkeeping,
    // immutable plan/program headers and batch descriptor envelopes separately.
    let per_owner = prepared
        .plan
        .groups
        .len()
        .checked_add(prepared.plan.aggregates.len())
        .and_then(|n| n.checked_mul(256))
        .and_then(|n| n.checked_add(8192))
        .ok_or("source owner state headers overflow")?;
    let shared_headers = prepared
        .plan
        .columns
        .len()
        .checked_add(prepared.plan.aggregates.len())
        .and_then(|n| n.checked_add(prepared.plan.output.len()))
        .and_then(|n| n.checked_add(prepared.schema.fields().len()))
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_mul(1024))
        .ok_or("source shared headers overflow")?;
    let names = prepared.plan.names.iter().try_fold(0usize, |n, name| {
        n.checked_add(name.len())
            .ok_or("source plan names overflow")
    })?;
    let metadata = Arc::new(
        budget.reserve(
            count
                .checked_mul(per_owner)
                .and_then(|n| n.checked_add(shared_headers))
                .and_then(|n| {
                    names
                        .checked_mul(4)
                        .and_then(|strings| n.checked_add(strings))
                })
                .ok_or("source ownership descriptors overflow")?,
        )?,
    );
    let mut domains = Vec::with_capacity(count);
    let mut rows = 0u64;
    let mut dense_bytes = 0usize;
    for owner in 0..count {
        let first = storage
            .batches
            .len()
            .checked_mul(owner)
            .ok_or("source owner range overflow")?
            / count;
        let last = storage
            .batches
            .len()
            .checked_mul(owner + 1)
            .ok_or("source owner range overflow")?
            / count;
        let mut bounds: Option<(i128, i128)> = None;
        let mut owner_rows = 0u64;
        for batch in &storage.batches[first..last] {
            options.cancellation.check()?;
            owner_rows = owner_rows
                .checked_add(batch.num_rows() as u64)
                .ok_or("source owner rows overflow")?;
            if batch.num_columns() != prepared.schema.fields().len()
                || prepared.plan.columns.iter().any(|&index| {
                    batch.column(index).data_type() != prepared.schema.field(index).data_type()
                        || Type::from_arrow(batch.column(index).data_type()).is_err()
                })
            {
                return Ok(None);
            }
            let array = batch.column(column);
            if array.null_count() != 0 {
                return Ok(None);
            }
            let physical = array.data_type();
            if !matches!(
                physical,
                arrow::datatypes::DataType::Int32
                    | arrow::datatypes::DataType::Int64
                    | arrow::datatypes::DataType::Decimal128(_, 0..=38)
            ) {
                return Ok(None);
            }
            match physical {
                arrow::datatypes::DataType::Int32 => extend_bounds(
                    &mut bounds,
                    array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                        .map(i128::from),
                    &options.cancellation,
                )?,
                arrow::datatypes::DataType::Int64 => extend_bounds(
                    &mut bounds,
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                        .map(i128::from),
                    &options.cancellation,
                )?,
                _ => extend_bounds(
                    &mut bounds,
                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                    &options.cancellation,
                )?,
            }
        }
        // Every local group ID fits the compact sentinel representation.
        if owner_rows >= u64::from(u32::MAX) {
            return Ok(None);
        }
        rows = rows
            .checked_add(owner_rows)
            .filter(|rows| *rows <= i64::MAX as u64)
            .ok_or("source owned row count exceeds i64::MAX")?;
        let window = if let Some((lo, hi)) = bounds {
            let Some(len) = hi
                .checked_sub(lo)
                .and_then(|n| n.checked_add(1))
                .and_then(|n| usize::try_from(n).ok())
            else {
                return Ok(None);
            };
            let Some(bytes) = len
                .checked_mul(std::mem::size_of::<u32>())
                .and_then(|n| n.checked_add(256))
            else {
                return Ok(None);
            };
            let Some(total) = dense_bytes
                .checked_add(bytes)
                .filter(|n| *n <= DENSE_ALLOWANCE)
            else {
                return Ok(None);
            };
            dense_bytes = total;
            Some((lo, len))
        } else {
            None
        };
        domains.push(Domain {
            batches: first..last,
            bounds,
            window,
        });
    }
    let mut previous = None;
    let mut ascending = true;
    let mut descending = true;
    for domain in &domains {
        if let Some((lo, hi)) = domain.bounds {
            if let Some((old_lo, old_hi)) = previous {
                ascending &= old_hi <= lo;
                descending &= old_lo >= hi;
            }
            previous = Some((lo, hi));
        }
    }
    if !ascending && !descending {
        return Ok(None);
    }
    Ok(Some(Proof {
        domains,
        rows,
        dense_bytes,
        _metadata: metadata,
    }))
}
