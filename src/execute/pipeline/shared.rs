//! Replay source; original immutable batches are never rechunked.
use super::*;
pub(super) struct Source {
    pub(super) next: Range<usize>,
    pub(super) storage: Arc<crate::rel::Materialized>,
}
pub(super) fn step(mut producer: Producer) -> AttemptResult<ProducerStep> {
    let source = producer.shared.as_mut().expect("shared source");
    let Some(index) = source.next.next() else {
        return producer.finish();
    };
    // Only the descriptor clone is new; the source owner retains original payload credit.
    let descriptor_count = producer
        .schema
        .fields()
        .len()
        .checked_add(producer.plan.groups.len())
        .and_then(|n| n.checked_add(producer.plan.aggregates.len()))
        .and_then(|n| n.checked_add(producer.expressions.bindings.len()))
        .ok_or("shared batch descriptor count overflow")?;
    let envelope = producer.budget.reserve(
        descriptor_count
            .checked_mul(std::mem::size_of::<Option<Value>>() * 4)
            .and_then(|n| n.checked_add(2048))
            .ok_or("shared batch envelope overflow")?,
    )?;
    let batch = source.storage.batches[index].clone();
    let rows = batch.num_rows();
    producer.metrics.rows_read = producer
        .metrics
        .rows_read
        .checked_add(rows)
        .ok_or("shared source row counter overflow")?;
    producer.metrics.batches += 1;
    producer.metrics.pipeline_source_batches += 1;
    let mut input = Input {
        len: rows,
        columns: vec![None; producer.schema.fields().len()],
    };
    // Unlike a Parquet ProjectionMask, a stored batch retains bound local indices.
    for &column in &producer.plan.columns {
        input.columns[column] = Some(normalize_input(
            batch.column(column),
            &Type::from_field(producer.schema.field(column))?,
            &producer.budget,
        )?);
    }
    producer.metrics.rows_after_filter += rows;
    let start = Instant::now();
    let (keys, values) = producer
        .expressions
        .evaluate(&mut input, &producer.budget)
        .map_err(Failure::sql)?;
    producer.metrics.expression_seconds += start.elapsed().as_secs_f64();
    producer.metrics.shared_expression_evaluations += producer.expressions.bindings.len();
    let prepared_keys = if rows > 0 {
        Some(crate::keys::Keys::new(&keys, rows, &producer.budget)?)
    } else {
        None
    };
    producer.pending = Some(PendingBatch {
        batch: EvaluatedBatch {
            rows,
            keys,
            values,
            prepared_keys,
        },
        source: SourceBatch::new(0, index as u64),
        _envelope: envelope,
    });
    drop(input);
    drop(batch);
    producer.apply_pending()
}

#[cfg(test)]
pub(crate) fn execute(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    storage: Arc<crate::rel::Materialized>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    execute_with_attempt(
        prepared,
        options,
        budget,
        storage,
        Arc::new(Attempt::default()),
        emit,
    )
}
pub(crate) fn execute_with_attempt(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    storage: Arc<crate::rel::Materialized>,
    attempt: Arc<Attempt>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    options.cancellation.check()?;
    if !storage.budget.same_domain(&budget) {
        return Err("shared storage funding domain mismatch".into());
    }
    let start = Instant::now();
    let rows = storage
        .batches
        .iter()
        .try_fold(0u64, |n, b| {
            n.checked_add(b.num_rows() as u64)
                .filter(|n| *n <= i64::MAX as u64)
        })
        .ok_or("shared source row count exceeds i64::MAX")?;
    let producers = options.workers.min(storage.batches.len()).max(1);
    #[cfg(test)]
    let partitions_count = options.test_pipeline_partitions.unwrap_or(producers);
    #[cfg(not(test))]
    let partitions_count = producers;
    attempt
        .admitted
        .store(true, std::sync::atomic::Ordering::Release);
    let descriptors = budget.reserve(
        producers
            .checked_mul(4096 + 2 * std::mem::size_of::<Exchange>())
            .ok_or("shared producer descriptors overflow")?,
    )?;
    // Later declaration drops/joins runtime before descriptor credit on unwind.
    let mut runtime_owner = None;
    let result = (|| {
        let plan = Arc::new(prepared.plan.clone());
        let mut executor = super::super::executor(prepared, budget.clone(), true)?;
        let expressions = executor.expressions.clone();
        let mut partitions =
            OwnedPartitions::new(&plan, &expressions, budget.clone(), partitions_count)?;
        runtime_owner = Some(Runtime::<JobOutput>::new(
            &budget,
            options.workers,
            producers
                .checked_add(partitions_count)
                .ok_or("shared runtime capacity overflow")?,
            options.cancellation.clone(),
        )?);
        let default_cap = ((budget.limit() - budget.used()) / (4 * producers)).min(64 << 20);
        #[cfg(test)]
        let (cap, forced) = options
            .test_pipeline_slice_bytes
            .map(|cap| (cap, true))
            .unwrap_or((default_cap, false));
        #[cfg(not(test))]
        let (cap, forced) = (default_cap, false);
        if !forced && cap < 64 << 10 {
            return Err(
                attempt.record(usize::MAX, Failure::Resource(ResourceReason::SliceTooSmall))
            );
        }
        let mut backing = (0..producers)
            .map(|_| budget.reserve(cap))
            .collect::<Result<Vec<_>>>()?;
        let runtime = runtime_owner.as_mut().unwrap();
        let mut active = vec![None; producers];
        for (lane, slot) in active.iter_mut().enumerate() {
            let mut local_options = options.clone();
            local_options.workers = 1;
            let first = storage
                .batches
                .len()
                .checked_mul(lane)
                .ok_or("shared batch range overflow")?
                / producers;
            let last = storage
                .batches
                .len()
                .checked_mul(lane + 1)
                .ok_or("shared batch range overflow")?
                / producers;
            let producer = Producer {
                attempt: attempt.clone(),
                source: lane,
                routing: partitions.routing_plan(),
                path: PathBuf::new(),
                metadata: None,
                plan: plan.clone(),
                expressions: expressions.clone(),
                schema: prepared.schema.clone(),
                options: local_options,
                slice_bytes: Some(cap),
                budget: budget.clone(),
                groups: 0..0,
                cursor: None,
                progress: None,
                state: None,
                backing: backing.pop(),
                pending: None,
                metrics: Metrics::default(),
                background: false,
                last_worker: None,
                shared: Some(Source {
                    next: first..last,
                    storage: storage.clone(),
                }),
            };
            *slot = Some(
                runtime
                    .try_submit(JobKind::Morsel, move |_| {
                        producer.step().map(JobOutput::Producer)
                    })
                    .map_err(|e| attempt.rejection(e.reason))?,
            );
        }
        drain_bounded(
            runtime,
            &mut active,
            &mut partitions,
            &plan,
            &expressions,
            options,
            &mut executor.metrics,
            &attempt,
        )?;
        if executor.metrics.rows_read as u64 != rows {
            return Err("shared source delivery mismatch".into());
        }
        let stats = runtime.statistics();
        executor.metrics.pipeline_runtime_submitted = stats.submitted;
        executor.metrics.pipeline_runtime_caller = stats.caller_started;
        executor.metrics.pipeline_runtime_background = stats.background_started;
        executor.metrics.pipeline_runtime_completed = stats.completed;
        executor.metrics.pipeline_runtime_peak_outstanding = stats.peak_outstanding;
        executor.metrics.pipeline_runtime_peak_queued = stats.peak_queued;
        // No caller cancellation poison and no nested worker permits at callbacks.
        runtime_owner.take().unwrap().shutdown_for_retry()?;
        let executor = executor.with_aggregation(PartitionOutput::new(
            partitions,
            options.cancellation.clone(),
        ));
        // Preserve original per-chunk finalization and downstream error chronology.
        // No local replay is allowed after this callback boundary.
        executor
            .finish_batches(start, options, emit)
            .map_err(|error| attempt.record(usize::MAX, Failure::sql(error)))
    })();
    let cleanup = runtime_owner
        .take()
        .map(|runtime| runtime.shutdown_for_retry())
        .transpose();
    drop(descriptors);
    if let Err(error) = cleanup {
        return Err(attempt.record(usize::MAX, Failure::Terminal(error)));
    }
    if let Err(error) = &result {
        attempt.returned(error.clone());
    }
    result
}
