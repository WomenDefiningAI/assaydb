//! Bounded two-pass source ownership for plain numeric Parquet leaves.
//! Statistics can reject a shape, but only decoded keys authorize ownership.
use super::*;
use crate::expr::Kind;
use aggregation::source_owned::{SourceOwned, new_state};
use arrow::datatypes::DataType;
use parquet::{arrow::ProjectionMask, file::statistics::Statistics};
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};

const DENSE_ALLOWANCE: usize = 32 << 20;

pub(crate) mod clustered;

#[cfg(test)]
mod tests;

/// The existing two-pass path retains its worker threshold and unchanged SQL shape.
pub(crate) fn eligible(prepared: &Prepared, options: &Options) -> bool {
    (2..=64).contains(&options.workers) && shape(prepared, options)
}

/// Shared leaf restrictions keep expression evaluation and INTEGER updates unchanged.
fn shape(prepared: &Prepared, options: &Options) -> bool {
    #[cfg(test)]
    if options.test_aggregate_partitions.is_some()
        || options.test_pipeline_partitions.is_some()
        || options.test_shared_batch_pipeline
        || options.test_source_owned_pipeline
    {
        // Explicit test backends retain their original dispatch and assertions.
        return false;
    }
    let p = &prepared.plan;
    (1..=131072).contains(&options.batch_size)
        && options.relational_optimizations
        && options.spill.is_none()
        && p.aggregation
        && p.groups.len() == 1
        && p.filter.is_none()
        && p.order.is_empty()
        && matches!(p.groups[0].kind, Kind::Column(_))
        && matches!(p.groups[0].ty, Type::Int | Type::Decimal(_))
        && integer_totals_reorder_safe(p, &prepared.schema)
        && prepared.schema.fields().iter().all(|field| {
            matches!(
                field.data_type(),
                DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, 0..=38)
            )
        })
        && p.aggregates.iter().all(|a| {
            !a.distinct
                && matches!(
                    a.function,
                    Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                )
                && (a.input.is_some() || matches!(a.function, Aggregate::Count))
                && a.input.as_ref().is_none_or(|e| {
                    matches!(e.kind, Kind::Column(_))
                        && matches!(e.ty, Type::Int | Type::Decimal(_))
                })
        })
        && p.output
            .iter()
            .all(|o| matches!(o, Output::Group(0) | Output::Aggregate(_)))
}

fn separated(bounds: impl IntoIterator<Item = Option<(i128, i128)>>) -> bool {
    let mut previous = None;
    let mut ascending = true;
    let mut descending = true;
    for (lo, hi) in bounds.into_iter().flatten() {
        if lo > hi {
            return false;
        }
        if let Some((old_lo, old_hi)) = previous {
            ascending &= old_hi <= lo;
            descending &= old_lo >= hi;
        }
        previous = Some((lo, hi));
    }
    ascending || descending
}

struct RunOrder {
    last: Option<i128>,
    ascending: bool,
    descending: bool,
}

impl Default for RunOrder {
    /// An empty prefix is monotone in both directions.
    fn default() -> Self {
        Self {
            last: None,
            ascending: true,
            descending: true,
        }
    }
}

impl RunOrder {
    /// Each surviving direction describes every decoded adjacent pair, including batch/page boundaries.
    fn observe(&mut self, value: i128) {
        if let Some(last) = self.last {
            self.ascending &= last <= value;
            self.descending &= last >= value;
        }
        self.last = Some(value);
    }
}

/// Local monotonicity plus ordered extrema proves that no completed key can reappear.
fn ordered_runs(lanes: &[Lane]) -> bool {
    let mut previous = None;
    let mut ascending = true;
    let mut descending = true;
    for lane in lanes {
        ascending &= lane.order.ascending;
        descending &= lane.order.descending;
        if let Some((lo, hi)) = lane.bounds {
            if let Some((old_lo, old_hi)) = previous {
                ascending &= old_hi <= lo;
                descending &= old_lo >= hi;
            }
            previous = Some((lo, hi));
        }
    }
    ascending || descending
}

struct Source {
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    projection: ProjectionMask,
    key_projection: ProjectionMask,
    plan: Arc<Plan>,
    expressions: Arc<crate::expr_program::ExpressionProgram>,
    schema: SchemaRef,
    options: Options,
    budget: Budget,
    dense: crate::memory::SourceDenseAllowance,
    // Metadata ownership is speculative until every decoded key is checked.
    validate_bounds: bool,
    #[cfg(test)]
    after_batch: Option<AfterBatch>,
    _metadata_funding: Arc<Reservation>,
    // Plans, cursors, lane/result descriptors and metadata drop before credit.
    funding: Arc<Reservation>,
}

#[cfg(test)]
type AfterBatch = Arc<dyn Fn(&Lane, &crate::spill::Cancellation) -> Result<()> + Send + Sync>;

struct Lane {
    source: Arc<Source>,
    groups: Range<usize>,
    cursor: Option<ScanCursor>,
    progress: Option<admission::GroupProgress>,
    state: Option<Aggregation>,
    bounds: Option<(i128, i128)>,
    order: RunOrder,
    runs: bool,
    rows: u64,
    declined: bool,
    metrics: Metrics,
}

impl Lane {
    fn done(&self) -> bool {
        self.declined || (self.groups.is_empty() && self.cursor.is_none())
    }

    /// Validate metadata-owned keys before the unchanged serial aggregate updates; never prune rows.
    fn step(mut self, cancel: &crate::spill::Cancellation) -> Result<Self> {
        if self.done() {
            return Ok(self);
        }
        let start_rows = self.rows;
        let mut batches = 0;
        self.metrics.pipeline_step_jobs += 1;
        while batches < MORSEL_BATCHES && self.rows - start_rows < MORSEL_ROWS as u64 {
            cancel.check()?;
            if self.cursor.is_none() {
                let Some(group) = self.groups.next() else {
                    break;
                };
                let source = &self.source;
                let projection = if self.state.is_some() {
                    source.projection.clone()
                } else {
                    source.key_projection.clone()
                };
                let metadata = source.metadata.metadata().row_group(group);
                // ScanCursor acquires the shared decoder lease before construction.
                self.progress = Some(admission::GroupProgress::new(metadata.num_rows())?);
                let factory: FilterFactory = Arc::new(|_| None);
                let start = Instant::now();
                self.cursor = Some(ScanCursor::new(
                    File::open(&source.path).map_err(|e| e.to_string())?,
                    source.metadata.clone(),
                    projection,
                    &factory,
                    None,
                    &source.options,
                    &source.budget,
                    group,
                )?);
                self.metrics.scan_decode_seconds += start.elapsed().as_secs_f64();
            }
            let start = Instant::now();
            let decoded = self.cursor.as_mut().unwrap().next();
            self.metrics.scan_decode_seconds += start.elapsed().as_secs_f64();
            let Some(decoded) = decoded else {
                let cursor = self.cursor.take().unwrap();
                self.progress.take().unwrap().finish()?;
                let (buffered, cached, uncached, bytes) = cursor.statistics();
                self.metrics.buffered_parquet_scans += buffered;
                self.metrics.parquet_cached_column_rows += cached;
                self.metrics.parquet_uncached_column_rows += uncached;
                self.metrics.buffered_compressed_bytes_read += bytes;
                drop(cursor);
                continue;
            };
            let decoded = decoded?;
            let rows = decoded.batch.num_rows();
            self.progress.as_mut().unwrap().observe(rows)?;
            self.rows = self
                .rows
                .checked_add(rows as u64)
                .ok_or("numeric leaf rows overflow")?;
            batches += 1;
            self.metrics.decoded_arrow_bytes_total += decoded.batch.get_array_memory_size();
            if self.source.validate_bounds {
                let Kind::Column(key) = self.source.plan.groups[0].kind else {
                    unreachable!()
                };
                if !clustered::contains(decoded.batch.column(key), self.bounds.unwrap()) {
                    self.declined = true;
                    break;
                }
            }
            if let Some(state) = &mut self.state {
                let source = &self.source;
                let descriptors = source.schema.fields().len()
                    + source.plan.groups.len()
                    + source.plan.aggregates.len()
                    + source.expressions.bindings.len();
                let _envelope = source.budget.reserve(
                    descriptors
                        .checked_mul(std::mem::size_of::<Option<Value>>() * 4)
                        .and_then(|n| n.checked_add(2048))
                        .ok_or("numeric leaf batch envelope overflow")?,
                )?;
                let mut input = Input {
                    len: rows,
                    columns: vec![None; source.schema.fields().len()],
                };
                let start = Instant::now();
                for (column, value) in input.columns.iter_mut().enumerate() {
                    if !source.options.projection_pushdown || source.plan.columns.contains(&column)
                    {
                        *value = Some(normalize_input(
                            decoded.batch.column(column),
                            &Type::from_field(source.schema.field(column))?,
                            &source.budget,
                        )?);
                    }
                }
                let (keys, values) = source.expressions.evaluate(&mut input, &source.budget)?;
                self.metrics.expression_seconds += start.elapsed().as_secs_f64();
                self.metrics.shared_expression_evaluations += source.expressions.bindings.len();
                self.metrics.rows_read += rows;
                self.metrics.rows_after_filter += rows;
                self.metrics.batches += 1;
                self.metrics.pipeline_source_batches += 1;
                let batch = EvaluatedBatch {
                    rows,
                    keys,
                    values,
                    prepared_keys: None,
                };
                if self.runs {
                    state.update_runs(
                        &source.plan,
                        &source.expressions,
                        &mut self.metrics,
                        batch,
                    )?;
                } else {
                    state.update(&source.plan, &source.expressions, &mut self.metrics, batch)?;
                }
            } else {
                let array = decoded.batch.column(0);
                if array.null_count() != 0 {
                    self.declined = true;
                    break;
                }
                let mut observe = |value: i128| {
                    self.order.observe(value);
                    self.bounds = Some(
                        self.bounds
                            .map_or((value, value), |(lo, hi)| (lo.min(value), hi.max(value))),
                    );
                };
                match array.data_type() {
                    DataType::Int32 => array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .for_each(|v| observe(i128::from(*v))),
                    DataType::Int64 => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .for_each(|v| observe(i128::from(*v))),
                    DataType::Decimal128(_, _) => array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .for_each(|v| observe(*v)),
                    _ => return Err("numeric leaf key type changed".into()),
                }
            }
            #[cfg(test)]
            if let Some(after_batch) = &self.source.after_batch {
                after_batch(&self, cancel)?;
            }
        }
        Ok(self)
    }
}

// Run only leaf work: no producer closure can wait for a blocking child operator.
// The first caller step starts upstream decoding before any permits are reserved.
fn run_lanes(
    mut lanes: Vec<Lane>,
    options: &Options,
    budget: &Budget,
) -> Result<Option<Vec<Lane>>> {
    let first = lanes.remove(0).step(&options.cancellation);
    let first = match first {
        Ok(first) => first,
        Err(_) => {
            options.cancellation.check()?;
            return Ok(None);
        }
    };
    lanes.insert(0, first);
    let mut runtime = match Runtime::new(
        budget,
        options.workers,
        lanes.len(),
        options.cancellation.clone(),
    ) {
        Err(error) if error.starts_with("resource-limit: accounted memory ") => return Ok(None),
        other => other?,
    };
    let result: Result<Option<Vec<Lane>>> = (|| {
        let mut active = vec![None; lanes.len()];
        let mut completed: Vec<Option<Result<Lane>>> = (0..lanes.len()).map(|_| None).collect();
        for (rank, lane) in lanes.into_iter().enumerate() {
            active[rank] = Some(
                runtime
                    .try_submit(JobKind::Morsel, move |cancel| lane.step(cancel))
                    .map_err(|rejected| rejected.reason.to_owned())?,
            );
        }
        while let Some(done) = runtime.next_completion() {
            let rank = active
                .iter()
                .position(|id| *id == Some(done.id))
                .ok_or("numeric leaf completion identity")?;
            active[rank] = None;
            match done.result {
                Ok(lane) if !lane.done() => {
                    active[rank] = Some(
                        runtime
                            .try_submit(JobKind::Morsel, move |cancel| lane.step(cancel))
                            .map_err(|rejected| rejected.reason.to_owned())?,
                    );
                }
                result => completed[rank] = Some(result),
            }
        }
        let mut output = Vec::with_capacity(completed.len());
        for receipt in completed {
            match receipt.ok_or("numeric leaf result missing")? {
                Ok(lane) => output.push(lane),
                // No output is visible yet. The ordinary path arbitrates any
                // speculative decoder/resource errors in its original chronology.
                Err(_) => return Ok(None),
            }
        }
        Ok(Some(output))
    })();
    // Explicit shutdown also on rejection: do not poison query cancellation.
    // Panics/cancellation are terminal and cannot disappear behind a decline.
    let stats = runtime.shutdown_for_retry()?;
    let mut output = match result {
        Err(error) if error == "pipeline closure memory limit" => return Ok(None),
        other => other?,
    };
    if let Some(lanes) = &mut output {
        let metrics = &mut lanes[0].metrics;
        metrics.pipeline_runtime_submitted = stats.submitted;
        metrics.pipeline_runtime_caller = stats.caller_started;
        metrics.pipeline_runtime_background = stats.background_started;
        metrics.pipeline_runtime_completed = stats.completed;
        metrics.pipeline_runtime_peak_outstanding = stats.peak_outstanding;
        metrics.pipeline_runtime_peak_queued = stats.peak_queued;
    }
    Ok(output)
}

pub(crate) fn try_execute(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    path: &Path,
    columns: &[usize],
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    try_execute_having(prepared, options, budget, path, columns, None, emit)
}

/// Source ownership and decoder/permit bounds are unchanged by output selection.
pub(crate) fn try_execute_having(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    path: &Path,
    columns: &[usize],
    having: Option<&super::super::Having>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    if !eligible(prepared, options) || budget.active_workers() != 0 {
        return Ok(None);
    }
    let start = Instant::now();
    // Prepay footer parsing as well as the retained reader metadata. Thrift
    // fields and Arrow schema descriptors can expand beyond serialized bytes.
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
    metadata_funding.release_excess(0);
    let metadata_funding = crate::parquet_scan::decoder_metadata_lease(&metadata, &budget)?;
    let group_count = metadata.metadata().num_row_groups();
    let Kind::Column(key) = prepared.plan.groups[0].kind else {
        unreachable!()
    };
    if group_count < 2
        || metadata.metadata().file_metadata().num_rows() < 1024
        || columns.len() != prepared.schema.fields().len()
        || key >= columns.len()
        || columns.windows(2).any(|w| w[0] >= w[1])
        || columns.iter().enumerate().any(|(i, &column)| {
            metadata
                .schema()
                .fields()
                .get(column)
                .is_none_or(|field| field.data_type() != prepared.schema.field(i).data_type())
        })
    {
        return Ok(None);
    }
    let Ok(Some(admission)) = admission::SourceAdmission::for_direct_scan(
        metadata.metadata().file_metadata().num_rows(),
        metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|group| group.num_rows()),
        options.batch_size,
        options.workers,
        0,
    ) else {
        return Ok(None);
    };
    // At most i64::MAX physical rows, each INTEGER input <= 2^63 in
    // magnitude: every prefix/subset total is <2^126<10^38. DECIMAL's
    // i256 bound is <2^190. Thus counts and updates cannot raise SQL errors,
    // even after boundary reassociation; finalization keeps original order.
    let count = options.workers.min(group_count);
    let ranges = (0..count)
        .map(|rank| group_count * rank / count..group_count * (rank + 1) / count)
        .collect::<Vec<_>>();
    // Cheap rejection only. Missing/incorrect statistics can never prove a
    // domain: every admitted key is checked by the following key-only scan.
    let projection = ProjectionMask::roots(metadata.parquet_schema(), columns.iter().copied());
    let key_projection = ProjectionMask::roots(metadata.parquet_schema(), [columns[key]]);
    // Eligibility above restricts projected roots to primitive numeric fields.
    let Some(key_leaf) = (0..metadata.parquet_schema().num_columns())
        .find(|&leaf| key_projection.leaf_included(leaf))
    else {
        return Ok(None);
    };
    let mut hints = Vec::with_capacity(count);
    for range in &ranges {
        let mut bounds = None;
        for group in range.clone() {
            let column = metadata.metadata().row_group(group).column(key_leaf);
            let value = match column
                .statistics()
                .filter(|_| crate::cardinality::signed_integer_statistics(column))
            {
                Some(Statistics::Int32(s)) => s
                    .min_opt()
                    .zip(s.max_opt())
                    .map(|(a, b)| (i128::from(*a), i128::from(*b))),
                Some(Statistics::Int64(s)) => s
                    .min_opt()
                    .zip(s.max_opt())
                    .map(|(a, b)| (i128::from(*a), i128::from(*b))),
                _ => None,
            };
            if let Some((a, b)) = value {
                bounds =
                    Some(bounds.map_or((a, b), |(lo, hi): (i128, i128)| (lo.min(a), hi.max(b))));
            }
        }
        hints.push(bounds);
    }
    if !separated(hints) {
        return Ok(None);
    }
    let headers = count
        .checked_mul(16384)
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
                    .map(|s| s.len() * 4)
                    .sum::<usize>(),
            )
        })
        .ok_or("numeric leaf descriptors overflow")?;
    let Ok(funding) = budget.reserve(headers) else {
        return Ok(None);
    };
    let funding = Arc::new(funding);
    let mut executor = super::super::executor(prepared, budget.clone(), true)?;
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
        validate_bounds: false,
        #[cfg(test)]
        after_batch: None,
        _metadata_funding: metadata_funding,
        funding,
    });
    let lanes = ranges
        .iter()
        .map(|groups| Lane {
            source: source.clone(),
            groups: groups.clone(),
            cursor: None,
            progress: None,
            state: None,
            bounds: None,
            order: RunOrder::default(),
            runs: false,
            rows: 0,
            declined: false,
            metrics: Metrics::default(),
        })
        .collect();
    let Some(proof) = run_lanes(lanes, options, &budget)? else {
        identity.verify_checked(path, &|| options.cancellation.check())?;
        return Ok(None);
    };
    identity.verify_checked(path, &|| options.cancellation.check())?;
    if proof.iter().any(|lane| lane.declined)
        || proof.iter().map(|lane| lane.rows).sum::<u64>() != admission.total_rows()
        || !separated(proof.iter().map(|lane| lane.bounds))
    {
        return Ok(None);
    }
    let bounds = proof.iter().map(|lane| lane.bounds).collect::<Vec<_>>();
    let runs = ordered_runs(&proof);
    executor.metrics.source_domain_proof_seconds = start.elapsed().as_secs_f64();
    // Count physical proof work, but count logical input rows only in the
    // aggregate pass. Both runtimes are included in task/decode telemetry.
    for lane in &proof {
        for i in 0..3 {
            executor.metrics.pipeline_runtime_submitted[i] +=
                lane.metrics.pipeline_runtime_submitted[i];
            executor.metrics.pipeline_runtime_caller[i] += lane.metrics.pipeline_runtime_caller[i];
            executor.metrics.pipeline_runtime_background[i] +=
                lane.metrics.pipeline_runtime_background[i];
            executor.metrics.pipeline_runtime_completed[i] +=
                lane.metrics.pipeline_runtime_completed[i];
        }
        executor.metrics.pipeline_step_jobs += lane.metrics.pipeline_step_jobs;
        executor.metrics.scan_decode_seconds += lane.metrics.scan_decode_seconds;
        executor.metrics.decoded_arrow_bytes_total += lane.metrics.decoded_arrow_bytes_total;
        executor.metrics.pipeline_runtime_peak_outstanding = executor
            .metrics
            .pipeline_runtime_peak_outstanding
            .max(lane.metrics.pipeline_runtime_peak_outstanding);
        executor.metrics.pipeline_runtime_peak_queued = executor
            .metrics
            .pipeline_runtime_peak_queued
            .max(lane.metrics.pipeline_runtime_peak_queued);
    }
    executor.metrics.selected_column_compressed_bytes = source
        .metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|group| {
            group.column(key_leaf).compressed_size()
                + group
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(leaf, _)| source.projection.leaf_included(*leaf))
                    .map(|(_, column)| column.compressed_size())
                    .sum::<i64>()
        })
        .sum();
    let mut dense_bytes = 0;
    let mut lanes = Vec::with_capacity(count);
    for (lane, groups) in proof.into_iter().zip(ranges) {
        let window = lane.bounds.filter(|_| !runs).and_then(|(lo, hi)| {
            let span = hi
                .checked_sub(lo)?
                .checked_add(1)
                .and_then(|n| usize::try_from(n).ok())?;
            let max_slots = ((source.dense.bytes / count).saturating_sub(256) / 4)
                .min(usize::try_from(lane.rows).ok()?.saturating_mul(4));
            (span <= max_slots).then_some((lo, span))
        });
        let mut state = match new_state(&source.plan, &source.expressions, &budget, window) {
            Err(error) if error.starts_with("resource-limit: accounted memory ") => {
                return Ok(None);
            }
            other => other?,
        };
        // Runs need no index; other large/sparse domains use exact hashing.
        // Disable adaptive growth so N owners cannot each acquire the cap.
        state.disable_dense();
        state.enable_numeric_slack()?;
        dense_bytes += window.map_or(0, |(_, len)| len * 4 + 256);
        lanes.push(Lane {
            source: source.clone(),
            groups,
            cursor: None,
            progress: None,
            state: Some(state),
            bounds: lane.bounds,
            order: lane.order,
            runs,
            rows: 0,
            declined: false,
            metrics: Metrics::default(),
        });
    }
    let Some(owners) = run_lanes(lanes, options, &budget)? else {
        identity.verify_checked(path, &|| options.cancellation.check())?;
        return Ok(None);
    };
    identity.verify_checked(path, &|| options.cancellation.check())?;
    let mut states = Vec::with_capacity(count);
    for owner in owners {
        for i in 0..3 {
            executor.metrics.pipeline_runtime_submitted[i] +=
                owner.metrics.pipeline_runtime_submitted[i];
            executor.metrics.pipeline_runtime_caller[i] += owner.metrics.pipeline_runtime_caller[i];
            executor.metrics.pipeline_runtime_background[i] +=
                owner.metrics.pipeline_runtime_background[i];
            executor.metrics.pipeline_runtime_completed[i] +=
                owner.metrics.pipeline_runtime_completed[i];
        }
        executor.metrics.pipeline_runtime_peak_outstanding = executor
            .metrics
            .pipeline_runtime_peak_outstanding
            .max(owner.metrics.pipeline_runtime_peak_outstanding);
        executor.metrics.pipeline_runtime_peak_queued = executor
            .metrics
            .pipeline_runtime_peak_queued
            .max(owner.metrics.pipeline_runtime_peak_queued);
        executor.metrics.scan_decode_seconds += owner.metrics.scan_decode_seconds;
        merge_metrics(&mut executor.metrics, owner.metrics);
        states.push(owner.state.ok_or("numeric leaf state missing")?);
    }
    executor.metrics.pipeline_producers = count;
    executor.metrics.dense_group_index_peak_bytes = dense_bytes;
    let boundary = Instant::now();
    let output = SourceOwned::finish(
        states,
        &bounds,
        &source.plan,
        &source.expressions,
        &options.cancellation,
        source.funding.clone(),
    )?;
    executor.metrics.source_boundary_merge_seconds = boundary.elapsed().as_secs_f64();
    executor.metrics.source_boundary_merges = output.boundary_merges;
    executor
        .with_aggregation(output)
        .finish_having_batches(start, options, having, emit)
        .map(Some)
}
