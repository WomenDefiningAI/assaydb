//! Speculative metadata ownership: no pruning, no publication before validation.
use super::*;
use crate::execute::aggregation::clustered::{ClusteredBatch, Having};

#[cfg(test)]
mod tests;

/// Admit only total column updates, internal wide outputs and a total optional HAVING.
fn eligible(
    prepared: &Prepared,
    options: &Options,
    having: Option<&crate::expr::Expr>,
) -> Option<Having> {
    let plan = &prepared.plan;
    if !(1..=64).contains(&options.workers)
        || !shape(prepared, options)
        || !prepared.internal_output
        || plan.groups[0].ty != Type::Int
        || plan
            .aggregates
            .iter()
            .any(|a| !matches!(a.function, Aggregate::Count | Aggregate::Sum))
    {
        return None;
    }
    Having::compile(having, plan)
}

/// Complete, non-NULL INT intervals may share endpoints, but never overlap in their interiors.
fn domains(
    metadata: &ArrowReaderMetadata,
    leaf: usize,
    workers: usize,
) -> Option<Vec<(i128, i128)>> {
    let mut result = Vec::with_capacity(metadata.metadata().num_row_groups());
    let mut rows = 0i64;
    let mut previous = None;
    for group in metadata.metadata().row_groups() {
        if group.num_rows() <= 0 || group.num_rows() >= i64::from(u32::MAX) {
            return None;
        }
        rows = rows.checked_add(group.num_rows())?;
        let stats = group.column(leaf).statistics()?;
        if stats.null_count_opt()? != 0 {
            return None;
        }
        let (lo, hi) = match stats {
            Statistics::Int32(s) => (i128::from(*s.min_opt()?), i128::from(*s.max_opt()?)),
            Statistics::Int64(s) => (i128::from(*s.min_opt()?), i128::from(*s.max_opt()?)),
            _ => return None,
        };
        let span = hi.checked_sub(lo)?.checked_add(1)?;
        if lo > hi
            || previous.is_some_and(|last| last > lo)
            || span > i128::from(group.num_rows()) * 4
            || span > ((DENSE_ALLOWANCE / workers).saturating_sub(256) / 4) as i128
        {
            return None;
        }
        result.push((lo, hi));
        previous = Some(hi);
    }
    (rows == metadata.metadata().file_metadata().num_rows() && !result.is_empty()).then_some(result)
}

/// Validate every decoded key before an update, so even forged bounds cannot split a group.
pub(super) fn contains(array: &ArrayRef, (lo, hi): (i128, i128)) -> bool {
    if array.null_count() != 0 {
        return false;
    }
    match array.data_type() {
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .all(|&v| lo <= i128::from(v) && i128::from(v) <= hi),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .iter()
            .all(|&v| lo <= i128::from(v) && i128::from(v) <= hi),
        _ => false,
    }
}

struct Finished {
    output: ClusteredBatch,
    metrics: Metrics,
}

/// A job owns exactly one physical row group and funds its dense window before allocation.
fn lane(source: Arc<Source>, rank: usize, bounds: (i128, i128)) -> Result<Lane> {
    let width =
        usize::try_from(bounds.1 - bounds.0 + 1).map_err(|_| "clustered window overflow")?;
    let mut state = new_state(
        &source.plan,
        &source.expressions,
        &source.budget,
        Some((bounds.0, width)),
    )?;
    state.disable_dense();
    state.enable_numeric_slack()?;
    Ok(Lane {
        source,
        groups: rank..rank + 1,
        cursor: None,
        progress: None,
        state: Some(state),
        bounds: Some(bounds),
        order: RunOrder::default(),
        runs: false,
        rows: 0,
        declined: false,
        metrics: Metrics::default(),
        decoder_funding: None,
    })
}

struct Pending {
    rank: usize,
    lane: Lane,
    skip: Option<usize>,
}

/// Decode one validated interval; retain raw states so an endpoint is never finalized early.
fn decode(mut lane: Lane, rank: usize, cancel: &crate::spill::Cancellation) -> Result<Pending> {
    while !lane.done() {
        lane = lane.step(cancel)?;
    }
    if lane.declined {
        return Err("clustered metadata bounds mismatch".into());
    }
    lane.state.as_mut().unwrap().finish_updates()?;
    Ok(Pending {
        rank,
        lane,
        skip: None,
    })
}

/// A completed boundary permits encounter-order finalization; nothing is published here.
fn finish(
    mut pending: Pending,
    having: Having,
    schema: &SchemaRef,
    cancel: &crate::spill::Cancellation,
) -> Result<Finished> {
    let lane = &mut pending.lane;
    let source = &lane.source;
    let output_start = Instant::now();
    let output = lane.state.as_mut().unwrap().clustered_batch(
        &source.plan,
        &source.expressions,
        schema,
        having,
        pending.skip,
        cancel,
    )?;
    lane.metrics.output_sort_seconds += output_start.elapsed().as_secs_f64();
    Ok(Finished {
        output,
        metrics: std::mem::take(&mut lane.metrics),
    })
}

/// Keep the earliest owner until its shared key completes, even through singleton interval chains.
fn carry(
    mut current: Pending,
    held: &mut Option<Pending>,
    output: &mut Vec<Finished>,
    having: Having,
    schema: &SchemaRef,
    cancel: &crate::spill::Cancellation,
) -> Result<()> {
    cancel.check()?;
    if let Some(mut prior) = held.take() {
        let high = prior.lane.bounds.unwrap().1;
        if high == current.lane.bounds.unwrap().0 {
            current.skip = prior
                .lane
                .state
                .as_mut()
                .unwrap()
                .clustered_merge_boundary(
                    current.lane.state.as_ref().unwrap(),
                    high,
                    &prior.lane.source.expressions,
                )?;
        }
        if current.skip.is_some() && current.lane.state.as_ref().unwrap().group_count == 1 {
            // This interval adds no new groups. Its only key still belongs to
            // the held owner; retain that owner's first-seen order unchanged.
            prior.lane.metrics.scan_decode_seconds += current.lane.metrics.scan_decode_seconds;
            merge_metrics(&mut prior.lane.metrics, current.lane.metrics);
            *held = Some(prior);
            return Ok(());
        }
        output.push(finish(prior, having, schema, cancel)?);
    }
    *held = Some(current);
    Ok(())
}

/// Reconstruct the old aggregate chunk boundaries, including gaps removed by HAVING.
fn emit_ordered(
    output: &[Finished],
    schema: &SchemaRef,
    options: &Options,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let mut parts = Vec::with_capacity(output.len());
    let mut group_offset = 0usize;
    let mut chunk = None;
    let flush = |parts: &mut Vec<RecordBatch>| -> Result<()> {
        options.cancellation.check()?;
        if parts.is_empty() {
            return Ok(());
        }
        // The caller holds a full-chunk replacement envelope through delivery.
        let batch = concat_batches(schema, parts.iter()).map_err(|e| e.to_string())?;
        emit(batch)?;
        parts.clear();
        Ok(())
    };
    let mut flush = flush;
    for finished in output {
        let output = &finished.output;
        let mut first = 0;
        while first < output.selected.len() {
            let ordinal = group_offset
                .checked_add(output.selected[first])
                .ok_or("clustered ordinal overflow")?;
            let current = ordinal / options.batch_size;
            if chunk != Some(current) {
                flush(&mut parts)?;
                chunk = Some(current);
            }
            let mut end = first + 1;
            while end < output.selected.len()
                && (group_offset + output.selected[end]) / options.batch_size == current
            {
                end += 1;
            }
            parts.push(output.batch.slice(first, end - first));
            first = end;
        }
        group_offset = group_offset
            .checked_add(output.groups)
            .ok_or("clustered ordinal overflow")?;
    }
    flush(&mut parts)
}

/// Bound outstanding jobs by workers; start decoding before acquiring runtime permits.
fn collect(
    source: Arc<Source>,
    bounds: &[(i128, i128)],
    having: Having,
    schema: SchemaRef,
) -> Result<Option<Vec<Finished>>> {
    let options = &source.options;
    let budget = &source.budget;
    let first = match lane(source.clone(), 0, bounds[0])
        .and_then(|lane| lane.step(&options.cancellation))
    {
        Ok(lane) => lane,
        Err(_) => {
            options.cancellation.check()?;
            return Ok(None);
        }
    };
    let slots = options.workers.min(bounds.len());
    let mut runtime = match Runtime::new(
        budget,
        options.workers.min(bounds.len()),
        slots,
        options.cancellation.clone(),
    ) {
        Err(error) if error.starts_with("resource-limit: accounted memory ") => return Ok(None),
        other => other?,
    };
    let result = (|| -> Result<Vec<Finished>> {
        runtime
            .try_submit(JobKind::Morsel, move |cancel| decode(first, 0, cancel))
            .map_err(|rejected| rejected.reason.to_owned())?;
        let mut next = 1;
        let mut ordered = 0;
        let mut output = Vec::with_capacity(bounds.len());
        // Completed states count against the same window as running jobs:
        // a slow early group cannot retain unbounded later aggregate states.
        let mut ready: Vec<Option<Pending>> = (0..slots).map(|_| None).collect();
        let mut held = None;
        loop {
            while next < bounds.len() && next - ordered < slots {
                let rank = next;
                let domain = bounds[rank];
                let source = source.clone();
                runtime
                    .try_submit(JobKind::Morsel, move |cancel| {
                        decode(lane(source, rank, domain)?, rank, cancel)
                    })
                    .map_err(|rejected| rejected.reason.to_owned())?;
                next += 1;
            }
            let Some(done) = runtime.next_completion() else {
                break;
            };
            let done = done.result?;
            let index = done.rank % slots;
            ready[index] = Some(done);
            while let Some(current) = ready[ordered % slots].take() {
                carry(
                    current,
                    &mut held,
                    &mut output,
                    having,
                    &schema,
                    &options.cancellation,
                )?;
                ordered += 1;
            }
        }
        if ordered != bounds.len() {
            return Err("clustered completion missing".into());
        }
        if let Some(last) = held {
            output.push(finish(last, having, &schema, &options.cancellation)?);
        }
        Ok(output)
    })();
    // Always join and drop speculative work before returning a fresh-path decline.
    let stats = runtime.shutdown_for_retry()?;
    options.cancellation.check()?;
    let Ok(mut output) = result else {
        return Ok(None);
    };
    let metrics = &mut output[0].metrics;
    metrics.pipeline_runtime_submitted = stats.submitted;
    metrics.pipeline_runtime_caller = stats.caller_started;
    metrics.pipeline_runtime_background = stats.background_started;
    metrics.pipeline_runtime_completed = stats.completed;
    metrics.pipeline_runtime_peak_outstanding = stats.peak_outstanding;
    metrics.pipeline_runtime_peak_queued = stats.peak_queued;
    Ok(Some(output))
}

/// Metadata only selects ownership; validated rows, ordinary updates and fresh replay decide results.
pub(crate) fn try_execute(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    path: &Path,
    columns: &[usize],
    having: Option<&crate::expr::Expr>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    options.cancellation.check()?;
    let Some(having) = eligible(prepared, options, having) else {
        return Ok(None);
    };
    if budget.active_workers() != 0 {
        return Ok(None);
    }
    let start = Instant::now();
    let Ok(mut metadata_funding) = budget.reserve(64 << 10) else {
        return Ok(None);
    };
    let identity = match fallback::Source::capture_checked(path, &|| options.cancellation.check()) {
        Ok(identity) => identity,
        Err(_) => {
            options.cancellation.check()?;
            return Ok(None);
        }
    };
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    file.seek(SeekFrom::End(-8)).map_err(|e| e.to_string())?;
    let mut trailer = [0u8; 8];
    file.read_exact(&mut trailer).map_err(|e| e.to_string())?;
    let footer_bytes = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
    let Some(parsing_bytes) = footer_bytes.checked_mul(64) else {
        return Ok(None);
    };
    if !metadata_funding.try_grow(parsing_bytes) {
        return Ok(None);
    }
    let metadata = crate::parquet_scan::external_metadata(&file)?;
    let Kind::Column(key) = prepared.plan.groups[0].kind else {
        unreachable!()
    };
    if columns.len() != prepared.schema.fields().len()
        || key >= columns.len()
        || columns.windows(2).any(|w| w[0] >= w[1])
        || columns.iter().enumerate().any(|(i, &column)| {
            metadata
                .schema()
                .fields()
                .get(column)
                .is_none_or(|field| field.data_type() != prepared.schema.field(i).data_type())
        })
        || !matches!(
            prepared.schema.field(key).data_type(),
            DataType::Int32 | DataType::Int64
        )
    {
        return Ok(None);
    }
    let projection = ProjectionMask::roots(metadata.parquet_schema(), columns.iter().copied());
    let key_projection = ProjectionMask::roots(metadata.parquet_schema(), [columns[key]]);
    let Some(key_leaf) = (0..metadata.parquet_schema().num_columns())
        .find(|&leaf| key_projection.leaf_included(leaf))
    else {
        return Ok(None);
    };
    // Prepay domains, completion slots, retained batch descriptors, plans and schemas.
    let group_headers = std::mem::size_of::<Option<Finished>>()
        + 2 * std::mem::size_of::<Finished>()
        + 512
        + prepared.plan.output.len() * 512;
    let headers = metadata
        .metadata()
        .memory_size()
        .checked_mul(2)
        .and_then(|n| {
            n.checked_add(
                metadata
                    .metadata()
                    .num_row_groups()
                    .checked_mul(group_headers)?,
            )
        })
        .and_then(|n| n.checked_add(options.workers * 32768))
        .and_then(|n| {
            n.checked_add(
                (prepared.schema.fields().len()
                    + prepared.plan.aggregates.len()
                    + prepared.plan.output.len()
                    + 1)
                    * 4096,
            )
        })
        .and_then(|n| {
            n.checked_add(
                prepared
                    .plan
                    .names
                    .iter()
                    .map(|name| name.len() * 4)
                    .sum::<usize>(),
            )
        })
        .ok_or("clustered descriptors overflow")?;
    let Ok(funding) = budget.reserve(headers) else {
        return Ok(None);
    };
    let Some(bounds) = domains(&metadata, key_leaf, options.workers) else {
        return Ok(None);
    };
    let executor = match super::super::super::executor(prepared, budget.clone(), true) {
        Err(error) if error.starts_with("resource-limit: accounted memory ") => return Ok(None),
        other => other?,
    };
    let schema = executor.output_schema.clone();
    let delivery_bytes = options
        .batch_size
        .checked_mul((schema.fields().len() + 1) * 128)
        .and_then(|n| n.checked_add(8192 + schema.fields().len() * 1024))
        .ok_or("clustered delivery size overflow")?;
    let Ok(_delivery) = budget.reserve(delivery_bytes) else {
        return Ok(None);
    };
    let mut metrics = Metrics {
        source_domain_proof_seconds: start.elapsed().as_secs_f64(),
        shared_aggregate_states: executor.metrics.shared_aggregate_states,
        ..Metrics::default()
    };
    metrics.selected_column_compressed_bytes = metadata
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|group| group.columns().iter().enumerate())
        .filter(|(leaf, _)| projection.leaf_included(*leaf))
        .map(|(_, column)| column.compressed_size())
        .sum();
    let source = Arc::new(Source {
        path: path.to_path_buf(),
        metadata,
        projection,
        key_projection,
        plan: Arc::new(prepared.plan.clone()),
        expressions: executor.expressions.clone(),
        schema: prepared.schema.clone(),
        options: options.clone(),
        budget: budget.clone(),
        dense: budget.source_dense_allowance(DENSE_ALLOWANCE),
        validate_bounds: true,
        #[cfg(test)]
        after_batch: None,
        _metadata_funding: metadata_funding,
        funding: Arc::new(funding),
    });
    drop(executor);
    if source.dense.bytes < DENSE_ALLOWANCE {
        return Ok(None);
    }
    let output = collect(source.clone(), &bounds, having, schema.clone())?;
    identity.verify_checked(path, &|| options.cancellation.check())?;
    let Some(output) = output else {
        return Ok(None);
    };
    metrics.pipeline_producers = options.workers.min(bounds.len());
    metrics.clustered_row_groups = bounds.len();
    emit_ordered(&output, &schema, options, emit)?;
    for finished in output {
        let m = &finished.metrics;
        for i in 0..3 {
            metrics.pipeline_runtime_submitted[i] += m.pipeline_runtime_submitted[i];
            metrics.pipeline_runtime_caller[i] += m.pipeline_runtime_caller[i];
            metrics.pipeline_runtime_background[i] += m.pipeline_runtime_background[i];
            metrics.pipeline_runtime_completed[i] += m.pipeline_runtime_completed[i];
        }
        metrics.pipeline_runtime_peak_outstanding = metrics
            .pipeline_runtime_peak_outstanding
            .max(m.pipeline_runtime_peak_outstanding);
        metrics.pipeline_runtime_peak_queued = metrics
            .pipeline_runtime_peak_queued
            .max(m.pipeline_runtime_peak_queued);
        metrics.scan_decode_seconds += m.scan_decode_seconds;
        metrics.output_sort_seconds += m.output_sort_seconds;
        merge_metrics(&mut metrics, finished.metrics);
    }
    metrics.execution_seconds = start.elapsed().as_secs_f64();
    metrics.peak_reserved_bytes = budget.peak();
    metrics.workers = options.workers;
    metrics.worker_threads_peak = budget.peak_threads();
    Ok(Some(metrics))
}
