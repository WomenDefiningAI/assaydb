//! Direct scan pipeline correctness stage: persistent producer state across
//! contiguous row-group ranges, followed by partition-owned final aggregation.
//! Portable parallel admission; private slices hand off on conservative memory pressure.
mod admission;
pub(crate) mod fallback;
pub(crate) mod leaf_owned;
#[cfg(test)]
pub(crate) mod shared;
pub(crate) mod source_owned;
use super::*;
use crate::{
    parquet_scan::{FilterFactory, cursor::ScanCursor},
    pipeline_runtime::{JobKind, Runtime},
};
use aggregation::{
    Aggregation, EvaluatedBatch, OwnedCompletion, OwnedPartitions, PartitionOutput, RawFunding,
    RoutedPartial, RoutingPlan, SourceBatch,
};
use fallback::{Attempt, AttemptResult, Failure, ResourceReason};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::{collections::VecDeque, ops::Range};

pub(crate) fn integer_totals_reorder_safe(plan: &Plan, schema: &Schema) -> bool {
    plan.aggregates.iter().all(|a| {
        if !matches!(a.function, Aggregate::Sum | Aggregate::Avg) {
            return true;
        }
        let Some(e) = &a.input else {
            return true;
        };
        if e.ty != Type::Int {
            return true;
        }
        let crate::expr::Kind::Column(column) = e.kind else {
            return false;
        };
        schema.fields().get(column).is_some_and(|f| {
            matches!(
                f.data_type(),
                arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
            )
        })
    })
}

pub(super) fn eligible(prepared: &Prepared, options: &Options) -> bool {
    let p = &prepared.plan;
    #[cfg(test)]
    if options.test_pipeline_partitions.is_none() && !options.test_pipeline_default {
        return false;
    }
    #[cfg(test)]
    let forced = options.test_pipeline_partitions.is_some();
    #[cfg(not(test))]
    let forced = false;
    (forced || options.workers > 1)
        && options.spill.is_none()
        && options.relational_optimizations
        && prepared.relational.is_none()
        && p.path.is_some()
        && p.aggregation
        && integer_totals_reorder_safe(p, &prepared.schema)
        && !p.groups.is_empty()
        && p.aggregates.iter().all(|a| {
            !a.distinct
                && matches!(
                    a.function,
                    Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                )
                && (matches!(a.function, Aggregate::Count)
                    || a.input
                        .as_ref()
                        .is_some_and(|e| matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Null)))
        })
}
struct Producer {
    attempt: Arc<Attempt>,
    source: usize,
    routing: RoutingPlan,
    path: PathBuf,
    metadata: Option<ArrowReaderMetadata>,
    plan: Arc<Plan>,
    expressions: Arc<crate::expr_program::ExpressionProgram>,
    schema: SchemaRef,
    options: Options,
    slice_bytes: Option<usize>,
    budget: Budget,
    groups: Range<usize>,
    cursor: Option<ScanCursor>,
    progress: Option<admission::GroupProgress>,
    state: Option<Aggregation>,
    backing: Option<Reservation>,
    pending: Option<PendingBatch>,
    metrics: Metrics,
    background: bool,
    last_worker: Option<std::thread::ThreadId>,
    // Last: cached arrays remain funded through parked input destruction.
    #[cfg(test)]
    shared: Option<shared::Source>,
}
struct PendingBatch {
    batch: EvaluatedBatch,
    source: SourceBatch,
    _envelope: Reservation,
}
struct Produced {
    routed: RoutedPartial,
    metrics: Metrics,
    background: bool,
}
#[allow(clippy::large_enum_variant)] // Inline result storage is prepaid per runtime slot.
enum JobOutput {
    Producer(ProducerStep),
    Partition(OwnedCompletion),
}
#[allow(clippy::large_enum_variant)] // Exclusively owned continuation, funded inline in runtime slots.
enum ProducerStep {
    Continue(Producer),
    Flush {
        producer: Producer,
        routed: RoutedPartial,
    },
    Finished(Produced),
}
// A scheduling quantum, never a decode/evaluation batch size. Whole original
// batches may cross the row ceiling; empty batches still consume the batch cap.
const MORSEL_ROWS: usize = 128 * 1024;
const MORSEL_BATCHES: usize = 16;
struct MorselLimit {
    rows: usize,
    batches: usize,
}
impl MorselLimit {
    fn new(metrics: &Metrics) -> Self {
        Self {
            rows: metrics.rows_read,
            batches: metrics.pipeline_source_batches,
        }
    }
    fn reached(&self, metrics: &Metrics) -> bool {
        metrics.rows_read - self.rows >= MORSEL_ROWS
            || metrics.pipeline_source_batches - self.batches >= MORSEL_BATCHES
    }
}
impl Producer {
    #[inline(always)]
    fn metadata(&self) -> &ArrowReaderMetadata {
        self.metadata.as_ref().expect("direct source metadata")
    }
    #[inline(always)]
    fn step(self) -> Result<ProducerStep> {
        self.step_morsel(
            #[cfg(test)]
            &mut |_| {},
        )
    }
    fn step_morsel(
        self,
        #[cfg(test)] after_batch: &mut dyn FnMut(&Producer),
    ) -> Result<ProducerStep> {
        self.step_quantum(
            false,
            #[cfg(test)]
            after_batch,
        )
    }
    fn step_quantum(
        mut self,
        batch_only: bool,
        #[cfg(test)] after_batch: &mut dyn FnMut(&Producer),
    ) -> Result<ProducerStep> {
        let attempt = self.attempt.clone();
        let source = self.source;
        let limit = MorselLimit::new(&self.metrics);
        // These count actual runtime dispatches, not batches inside a dispatch.
        self.metrics.pipeline_step_jobs += 1;
        let worker = std::thread::current().id();
        if self.last_worker.is_some_and(|previous| previous != worker) {
            self.metrics.pipeline_worker_handoffs += 1;
        }
        self.last_worker = Some(worker);
        self.background |= std::thread::current()
            .name()
            .is_some_and(|n| n.starts_with("assaydb-pipeline-"));
        loop {
            match self
                .step_attempt()
                .map_err(|failure| attempt.record(source, failure))?
            {
                ProducerStep::Continue(next) => {
                    #[cfg(test)]
                    after_batch(&next);
                    // EOF closes this cursor, even if more groups remain. Keep
                    // aggregate state but let the scheduler assign the next group.
                    #[cfg(test)]
                    let shared = next.shared.is_some();
                    #[cfg(not(test))]
                    let shared = false;
                    if batch_only
                        || (!shared && next.cursor.is_none())
                        || limit.reached(&next.metrics)
                    {
                        return Ok(ProducerStep::Continue(next));
                    }
                    self = next;
                }
                output => return Ok(output), // Pressure never waits inside a job.
            }
        }
    }
    fn step_attempt(mut self) -> AttemptResult<ProducerStep> {
        self.options.cancellation.check()?;
        if self.state.is_none() {
            let state_budget = if let Some(cap) = self.slice_bytes {
                // Also covers export of an empty producer, which has no successful
                // batch preflight. Never fall back to unbacked private allocations.
                if cap < 4096 {
                    return Err(Failure::Resource(ResourceReason::SliceTooSmall));
                }
                if self.backing.is_none() {
                    self.backing = Some(self.budget.reserve(cap)?);
                }
                Budget::new(cap)
            } else {
                self.budget.clone()
            };
            let mut state = Aggregation::new(&self.plan, &self.expressions, state_budget, true)?;
            state.disable_dense();
            if self.slice_bytes.is_some() {
                state.enable_numeric_slack()?;
            }
            self.state = Some(state);
        }
        if self.pending.is_some() {
            return self.apply_pending();
        }
        #[cfg(test)]
        if self.shared.is_some() {
            return shared::step(self);
        }
        if self.cursor.is_none() {
            let Some(group) = self.groups.next() else {
                return self.finish();
            };
            let projection =
                ProjectionMask::roots(self.metadata().parquet_schema(), self.plan.columns.clone());
            let factory: FilterFactory = Arc::new(|_| None);
            let file = File::open(&self.path).map_err(|e| e.to_string())?;
            self.progress = Some(admission::GroupProgress::new(
                self.metadata().metadata().row_group(group).num_rows(),
            )?);
            self.cursor = Some(ScanCursor::new(
                file,
                self.metadata().clone(),
                projection,
                &factory,
                None,
                &self.options,
                &self.budget,
                group,
            )?);
        }
        let next = self.cursor.as_mut().expect("opened cursor").next();
        let Some(decoded) = next else {
            let cursor = self.cursor.take().expect("completed cursor");
            self.progress.take().expect("admitted group").finish()?;
            let (buffered, cached, uncached, bytes) = cursor.statistics();
            self.metrics.buffered_parquet_scans += buffered;
            self.metrics.parquet_cached_column_rows += cached;
            self.metrics.parquet_uncached_column_rows += uncached;
            self.metrics.buffered_compressed_bytes_read += bytes;
            drop(cursor);
            return if self.groups.is_empty() {
                self.finish()
            } else {
                Ok(ProducerStep::Continue(self))
            };
        };
        let decoded = decoded?;
        let rows = decoded.batch.num_rows();
        self.progress
            .as_mut()
            .expect("admitted group")
            .observe(rows)?;
        self.metrics.rows_read = self
            .metrics
            .rows_read
            .checked_add(rows)
            .ok_or("pipeline row counter overflow")?;
        self.metrics.batches += 1;
        self.metrics.pipeline_source_batches += 1;
        self.metrics.decoded_arrow_bytes_total += decoded.batch.get_array_memory_size();
        // Prepay descriptors before constructing the schema-width input and
        // expression vectors. This envelope follows evaluated values if parked.
        let descriptor_count = self
            .schema
            .fields()
            .len()
            .checked_add(self.plan.groups.len())
            .and_then(|n| n.checked_add(self.plan.aggregates.len()))
            .and_then(|n| n.checked_add(self.expressions.bindings.len()))
            .ok_or("pipeline descriptor count overflow")?;
        let envelope_bytes = descriptor_count
            .checked_mul(std::mem::size_of::<Option<Value>>() * 4)
            .and_then(|n| n.checked_add(1024))
            .ok_or("pipeline batch envelope overflow")?;
        let envelope = self.budget.reserve(envelope_bytes)?;
        let source = SourceBatch::new(decoded.row_group as u64, decoded.batch_in_group);
        let mut input = Input {
            len: rows,
            columns: vec![None; self.schema.fields().len()],
        };
        for (local, &original) in self.plan.columns.iter().enumerate() {
            input.columns[original] = Some(normalize_input(
                decoded.batch.column(local),
                &Type::from_field(self.schema.field(original))?,
                &self.budget,
            )?);
        }
        let start = Instant::now();
        if let Some(predicate) = &self.plan.filter {
            let mask = predicate.eval(&input, &self.budget).map_err(Failure::sql)?;
            let mask = boolean(&mask.array);
            input = filter_input(&input, mask, &self.budget, true)?;
        }
        self.metrics.filter_seconds += start.elapsed().as_secs_f64();
        self.metrics.rows_after_filter += input.len;
        drop(decoded.batch);
        drop(decoded.lease);
        let start = Instant::now();
        let (keys, values) = self
            .expressions
            .evaluate(&mut input, &self.budget)
            .map_err(Failure::sql)?;
        self.metrics.expression_seconds += start.elapsed().as_secs_f64();
        self.metrics.shared_expression_evaluations += self.expressions.bindings.len();
        let prepared_keys = if self.slice_bytes.is_some() && input.len > 0 {
            Some(crate::keys::Keys::new(&keys, input.len, &self.budget)?)
        } else {
            None
        };
        self.pending = Some(PendingBatch {
            batch: EvaluatedBatch {
                rows: input.len,
                keys,
                values,
                prepared_keys,
            },
            source,
            _envelope: envelope,
        });
        drop(input);
        self.apply_pending()
    }
    fn apply_pending(mut self) -> AttemptResult<ProducerStep> {
        let pending = self.pending.as_ref().expect("evaluated pending batch");
        if pending.batch.rows == 0 {
            self.pending = None;
            return Ok(ProducerStep::Continue(self));
        }
        let state = self.state.as_ref().expect("initialized state");
        if self.slice_bytes.is_some()
            && !state.batch_export_fits(
                &self.plan,
                &self.expressions,
                &pending.batch,
                &self.budget,
            )?
        {
            if state.group_count == 0 {
                return Err(Failure::Resource(ResourceReason::OversizedBatch));
            }
            self.metrics.pipeline_pressure_flushes += 1;
            let routed = self.export()?;
            // Pending expressions/source prefix stay unchanged. A driver must
            // merge and drop this raw backing before it resubmits the producer.
            return Ok(ProducerStep::Flush {
                producer: self,
                routed,
            });
        }
        let PendingBatch {
            batch,
            source,
            _envelope,
        } = self.pending.take().unwrap();
        self.state.as_mut().unwrap().update_ordered_source(
            &self.plan,
            &self.expressions,
            &mut self.metrics,
            batch,
            source,
        )?; // Never retry this mutation in place; controller may replay the whole query.
        drop(_envelope);
        self.options.cancellation.check()?;
        Ok(ProducerStep::Continue(self))
    }
    fn export(&mut self) -> AttemptResult<RoutedPartial> {
        let state = self.state.take().expect("initialized state");
        let groups = state.group_count;
        let funding = match self.backing.take() {
            Some(backing) => RawFunding::PrepaidPrivate(backing),
            None => RawFunding::SharedQuery(self.budget.clone()),
        };
        let raw = Arc::new(state.into_raw_with_encounters(funding)?);
        // Hash and construct partition-local ID fragments on this source job,
        // before handing the exclusive routed owner to the coordinator.
        let routed = self.routing.route(raw, &self.options.cancellation)?;
        self.metrics.pipeline_source_routes += 1;
        self.metrics.pipeline_source_routed_groups += groups;
        self.metrics.pipeline_background_routes += usize::from(
            std::thread::current()
                .name()
                .is_some_and(|n| n.starts_with("assaydb-pipeline-")),
        );
        Ok(routed)
    }
    fn finish(mut self) -> AttemptResult<ProducerStep> {
        self.options.cancellation.check()?;
        let routed = self.export()?;
        Ok(ProducerStep::Finished(Produced {
            routed,
            metrics: self.metrics,
            background: self.background,
        }))
    }
}
struct Exchange {
    source: usize,
    resume: Option<Producer>,
    routed: RoutedPartial,
}
struct Flight {
    source: usize,
    resume: Option<Producer>,
    remaining: usize,
}
fn submit_producer(
    runtime: &mut Runtime<JobOutput>,
    active: &mut [Option<crate::pipeline_runtime::JobId>],
    source: usize,
    producer: Producer,
) -> Result<()> {
    let attempt = producer.attempt.clone();
    // Keep the deterministic decoder-refusal injection at its original
    // coordinator boundary; ordinary continuations never make that round trip.
    #[cfg(test)]
    if producer.options.test_pipeline_interruption.is_some() {
        active[source] = Some(
            runtime
                .try_submit(JobKind::Morsel, move |_| {
                    producer.step().map(JobOutput::Producer)
                })
                .map_err(|e| attempt.rejection(e.reason))?,
        );
        return Ok(());
    }
    let mut owner = Some(producer);
    active[source] = Some(
        runtime
            .try_submit_continuation(JobKind::Morsel, move |_, background| {
                let producer = owner.take().expect("exclusive producer continuation");
                #[cfg(test)]
                let hooks = producer.options.test_pipeline_continuation.clone();
                #[cfg(test)]
                let source = producer.source;
                #[cfg(test)]
                let producer = {
                    let mut producer = producer;
                    if let Some(hooks) = &hooks {
                        hooks.before(&mut producer, background);
                    }
                    producer
                };
                let step = if background {
                    producer.step()
                } else {
                    // Return to completion draining after one original batch.
                    producer.step_quantum(
                        true,
                        #[cfg(test)]
                        &mut |_| {},
                    )
                };
                #[cfg(test)]
                if let Some(hooks) = &hooks {
                    hooks.after(source, &step);
                }
                match step? {
                    ProducerStep::Continue(producer) => {
                        owner = Some(producer);
                        Ok(None)
                    }
                    output => Ok(Some(JobOutput::Producer(output))),
                }
            })
            .map_err(|e| attempt.rejection(e.reason))?,
    );
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn drain_bounded(
    runtime: &mut Runtime<JobOutput>,
    active: &mut [Option<crate::pipeline_runtime::JobId>],
    partitions: &mut OwnedPartitions,
    plan: &Arc<Plan>,
    expressions: &Arc<crate::expr_program::ExpressionProgram>,
    options: &Options,
    metrics: &mut Metrics,
    attempt: &Arc<Attempt>,
) -> Result<()> {
    // Each source owns at most one active step OR parked exchange. Runtime
    // capacity additionally includes every partition slot, so waiting sources
    // cannot consume the slots needed to release their own private backing.
    let mut queue = VecDeque::<Exchange>::with_capacity(active.len());
    let mut flight: Option<Flight> = None;
    let mut first_error: Option<(usize, String)> = None;
    loop {
        if flight.as_ref().is_some_and(|f| f.remaining == 0) {
            let finished = flight.take().unwrap();
            // Every job/raw Arc has dropped before its receipt is acknowledged.
            // Resume allocates the next private slice only after that release.
            if let Some(producer) = finished.resume {
                submit_producer(runtime, active, finished.source, producer)?;
            }
        }
        if flight.is_none()
            && let Some(exchange) = queue.pop_front()
        {
            let jobs = partitions.submit_routed(exchange.routed, &options.cancellation)?;
            let remaining = jobs.len();
            flight = Some(Flight {
                source: exchange.source,
                resume: exchange.resume,
                remaining,
            });
            for job in jobs {
                let plan = plan.clone();
                let expressions = expressions.clone();
                let latch = attempt.clone();
                runtime
                    .try_submit(JobKind::Partition, move |cancel| {
                        let receipt = job.run(&plan, &expressions, cancel);
                        if let Some(error) = receipt.error_message() {
                            latch.record(usize::MAX, Failure::from(error));
                        }
                        Ok(JobOutput::Partition(receipt))
                    })
                    .map_err(|e| attempt.rejection(e.reason))?;
            }
            if remaining == 0 {
                continue;
            }
        }
        if active.iter().all(Option::is_none) && queue.is_empty() && flight.is_none() {
            break;
        }
        let completion = runtime
            .next_completion()
            .ok_or("pipeline stalled without runnable jobs")?;
        if completion.kind == JobKind::Partition {
            let JobOutput::Partition(receipt) = completion.result? else {
                return Err("partition job returned a producer".into());
            };
            partitions.acknowledge(receipt)?;
            flight.as_mut().expect("partition flight").remaining -= 1;
            continue;
        }
        let source = active
            .iter()
            .position(|id| *id == Some(completion.id))
            .expect("active source completion");
        active[source] = None;
        match completion.result {
            Ok(JobOutput::Producer(ProducerStep::Continue(producer))) => {
                #[cfg(test)]
                if let Some(target) = &options.test_pipeline_interruption {
                    let mut report = target.lock().unwrap();
                    if report.budget.is_none() {
                        // Resume a live partial aggregate at the next decoder
                        // step, while the other sources still own outstanding
                        // jobs/completions. Do not let closure funding consume
                        // the refusal before the decoder requests its buffers.
                        report.queued_sources = active.iter().flatten().count() + queue.len();
                        report.aggregated_rows = producer.metrics.rows_after_filter;
                        report.budget = Some(producer.budget.clone());
                        producer.budget.test_refuse_reservations();
                        drop(report);
                        return match producer.step() {
                            Err(error) => Err(error),
                            Ok(_) => panic!("expected decoder reservation refusal"),
                        };
                    }
                }
                submit_producer(runtime, active, source, producer)?;
            }
            Ok(JobOutput::Producer(ProducerStep::Flush { producer, routed })) => {
                queue.push_back(Exchange {
                    source,
                    resume: Some(producer),
                    routed,
                });
            }
            Ok(JobOutput::Producer(ProducerStep::Finished(output))) => {
                metrics.pipeline_producers += 1;
                metrics.pipeline_background_producers += usize::from(output.background);
                merge_metrics(metrics, output.metrics);
                queue.push_back(Exchange {
                    source,
                    resume: None,
                    routed: output.routed,
                });
            }
            Ok(JobOutput::Partition(_)) => unreachable!("source job"),
            Err(error) => {
                // Later source failure never suppresses lower-source input work.
                if first_error.as_ref().is_none_or(|(rank, _)| source < *rank) {
                    first_error = Some((source, error));
                }
            }
        }
        metrics.pipeline_exchange_peak = metrics
            .pipeline_exchange_peak
            .max(queue.len() + usize::from(flight.is_some()));
        debug_assert!(queue.len() + usize::from(flight.is_some()) <= active.len());
    }
    if let Some((_, error)) = first_error {
        return Err(error);
    }
    Ok(())
}

fn merge_metrics(dst: &mut Metrics, src: Metrics) {
    dst.rows_read += src.rows_read;
    dst.batches += src.batches;
    dst.rows_after_filter += src.rows_after_filter;
    dst.decoded_arrow_bytes_total += src.decoded_arrow_bytes_total;
    dst.filter_seconds += src.filter_seconds;
    dst.expression_seconds += src.expression_seconds;
    dst.aggregate_seconds += src.aggregate_seconds;
    dst.shared_expression_evaluations += src.shared_expression_evaluations;
    dst.bounded_sum_batches += src.bounded_sum_batches;
    dst.short_group_cache_batches += src.short_group_cache_batches;
    dst.short_group_cache_hits += src.short_group_cache_hits;
    dst.short_group_cache_misses += src.short_group_cache_misses;
    dst.short_group_cache_bypasses += src.short_group_cache_bypasses;
    dst.short_group_cache_peak_bytes = dst
        .short_group_cache_peak_bytes
        .max(src.short_group_cache_peak_bytes);
    dst.dense_group_batches += src.dense_group_batches;
    dst.dense_group_rebuilds += src.dense_group_rebuilds;
    dst.dense_group_index_peak_bytes = dst
        .dense_group_index_peak_bytes
        .max(src.dense_group_index_peak_bytes);
    dst.pipeline_source_batches += src.pipeline_source_batches;
    dst.pipeline_step_jobs += src.pipeline_step_jobs;
    dst.pipeline_pressure_flushes += src.pipeline_pressure_flushes;
    dst.pipeline_worker_handoffs += src.pipeline_worker_handoffs;
    dst.pipeline_source_routes += src.pipeline_source_routes;
    dst.pipeline_source_routed_groups += src.pipeline_source_routed_groups;
    dst.pipeline_background_routes += src.pipeline_background_routes;
    dst.buffered_parquet_scans += src.buffered_parquet_scans;
    dst.parquet_cached_column_rows += src.parquet_cached_column_rows;
    dst.parquet_uncached_column_rows += src.parquet_uncached_column_rows;
    dst.buffered_compressed_bytes_read += src.buffered_compressed_bytes_read;
}

pub(super) fn execute(prepared: &Prepared, options: &Options, budget: Budget) -> Result<Execution> {
    #[cfg(test)]
    if options.test_pipeline_partitions.is_some() && !options.test_pipeline_fallback {
        return execute_with_flush_order(prepared, options, budget, false);
    }
    fallback::execute(prepared, options, budget)
}
#[cfg(test)]
fn execute_with_flush_order(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    reverse_source_flush: bool,
) -> Result<Execution> {
    execute_attempt(
        prepared,
        options,
        budget,
        reverse_source_flush,
        Arc::new(Attempt::default()),
    )
}
fn execute_attempt(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    reverse_source_flush: bool,
    attempt: Arc<Attempt>,
) -> Result<Execution> {
    // Source continuations can outlive the coordinator's fallible closure.
    // Keep their descriptor funding until every runtime job has joined/dropped.
    let mut descriptors_owner = None;
    let mut runtime_owner: Option<Runtime<JobOutput>> = None;
    let result = (|| {
        let start = Instant::now();
        let plan = Arc::new(prepared.plan.clone());
        let path = plan.path.clone().expect("pipeline admitted path");
        let file = File::open(&path).map_err(|e| e.to_string())?;
        let metadata = crate::parquet_scan::external_metadata(&file)?;
        drop(file);
        // Match baseline physical-group batches only when its parallel reader is
        // guaranteed; otherwise preserve serial cross-group batch chronology.
        let Some(admission) = admission::SourceAdmission::for_direct_scan(
            metadata.metadata().file_metadata().num_rows(),
            metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|g| g.num_rows()),
            options.batch_size,
            options.workers,
            budget.active_workers(),
        )?
        else {
            return super::execute_baseline(prepared, options, budget);
        };
        for &column in &plan.columns {
            Type::from_field(prepared.schema.field(column))?;
        }
        let groups = metadata.metadata().num_row_groups();
        let producers = options.workers.min(groups).max(1);
        #[cfg(test)]
        let forced = options.test_pipeline_partitions;
        #[cfg(not(test))]
        let forced: Option<usize> = None;
        if forced.is_none()
            && (producers < 2
                || admission.total_rows()
                    < options
                        .batch_size
                        .saturating_mul(producers)
                        .saturating_mul(2)
                        .max(1024) as u64)
        {
            return super::execute_baseline(prepared, options, budget);
        }
        #[cfg(test)]
        let partition_count = forced.unwrap_or(producers);
        #[cfg(not(test))]
        let partition_count = producers;
        #[cfg(test)]
        let mut slice_bytes = if forced.is_some() {
            options.test_pipeline_slice_bytes
        } else {
            Some(0)
        };
        #[cfg(not(test))]
        let mut slice_bytes = Some(0);
        attempt
            .admitted
            .store(true, std::sync::atomic::Ordering::Release);
        let mut executor = super::executor(prepared, budget.clone(), true)?;
        let expressions = executor.expressions.clone();
        let mut partitions =
            OwnedPartitions::new(&plan, &expressions, budget.clone(), partition_count)?;
        // Prepay source, parked exchange and flight descriptors. Bounded mode
        // retains at most one raw fragment or running continuation per source.
        descriptors_owner = Some(
            budget.reserve(
                producers
                    .checked_mul(4096 + 2 * std::mem::size_of::<Exchange>())
                    .ok_or("pipeline producer metadata overflow")?,
            )?,
        );
        runtime_owner = Some(Runtime::<JobOutput>::new(
            &budget,
            options.workers,
            if slice_bytes.is_some() {
                producers + partition_count
            } else {
                producers.max(partition_count)
            },
            options.cancellation.clone(),
        )?);
        let runtime = runtime_owner.as_mut().unwrap();
        let mut initial_backing = Vec::new();
        if forced.is_none() {
            let cap = ((budget.limit() - budget.used()) / (4 * producers)).min(64 * 1024 * 1024);
            if cap < 64 * 1024 {
                return Err(attempt.record(
                    usize::MAX,
                    Failure::Resource(fallback::ResourceReason::SliceTooSmall),
                ));
            }
            slice_bytes = Some(cap);
            initial_backing = (0..producers)
                .map(|_| budget.reserve(cap))
                .collect::<Result<Vec<_>>>()?;
        }
        let mut active = vec![None; producers];
        for lane in 0..producers {
            let mut producer_options = options.clone();
            producer_options.workers = 1;
            producer_options.parquet_cache_bytes =
                options.parquet_cache_bytes.map(|n| n / producers);
            let producer = Producer {
                attempt: attempt.clone(),
                source: lane,
                routing: partitions.routing_plan(),
                path: path.clone(),
                metadata: Some(metadata.clone()),
                plan: plan.clone(),
                expressions: expressions.clone(),
                schema: prepared.schema.clone(),
                options: producer_options,
                slice_bytes,
                budget: budget.clone(),
                groups: groups * lane / producers..groups * (lane + 1) / producers,
                cursor: None,
                progress: None,
                state: None,
                backing: initial_backing.pop(),
                pending: None,
                metrics: Metrics::default(),
                background: false,
                last_worker: None,
                #[cfg(test)]
                shared: None,
            };
            submit_producer(runtime, &mut active, lane, producer)?;
        }
        #[cfg(test)]
        if let Some(hooks) = &options.test_pipeline_continuation {
            hooks.submitted(runtime);
        }
        let mut partials = Vec::with_capacity(producers);
        let mut first_error: Option<(usize, String)> = None;
        if slice_bytes.is_some() {
            if reverse_source_flush {
                return Err("bounded exchange cannot defer all source flushes".into());
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
        } else {
            while let Some(completion) = runtime.next_completion() {
                // A continuation retains its job ID; flushed/resumed sources can
                // receive new IDs, but their contiguous range identity never changes.
                let source = active
                    .iter()
                    .position(|id| *id == Some(completion.id))
                    .expect("active producer completion");
                active[source] = None;
                match completion.result {
                    Ok(JobOutput::Producer(ProducerStep::Continue(producer))) => {
                        submit_producer(runtime, &mut active, source, producer)?;
                    }
                    Ok(JobOutput::Producer(ProducerStep::Finished(output))) => {
                        executor.metrics.pipeline_producers += 1;
                        executor.metrics.pipeline_background_producers +=
                            usize::from(output.background);
                        merge_metrics(&mut executor.metrics, output.metrics);
                        partials.push((source, output.routed));
                    }
                    Ok(JobOutput::Partition(_))
                    | Ok(JobOutput::Producer(ProducerStep::Flush { .. })) => {
                        unreachable!("unbounded producer phase")
                    }
                    Err(error) => {
                        if first_error.as_ref().is_none_or(|(id, _)| source < *id) {
                            first_error = Some((source, error));
                        }
                    }
                }
            }
        }
        if let Some((_, error)) = first_error {
            return Err(error);
        }
        if executor.metrics.rows_read as u64 != admission.total_rows() {
            return Err("pipeline decoded total differs from admitted source".into());
        }
        // Arrival order may differ from source order. Original per-group encounter
        // minima, not this order or partition admission generation, determine output.
        if reverse_source_flush {
            partials.sort_by_key(|(source, _)| std::cmp::Reverse(*source));
        }
        for (_, routed) in partials {
            let jobs = partitions.submit_routed(routed, &options.cancellation)?;
            for job in jobs {
                let plan = plan.clone();
                let expressions = expressions.clone();
                let latch = attempt.clone();
                runtime
                    .try_submit(JobKind::Partition, move |cancel| {
                        let receipt = job.run(&plan, &expressions, cancel);
                        if let Some(error) = receipt.error_message() {
                            latch.record(usize::MAX, Failure::from(error));
                        }
                        Ok(JobOutput::Partition(receipt))
                    })
                    .map_err(|e| attempt.rejection(e.reason))?;
            }
            while let Some(completion) = runtime.next_completion() {
                match completion.result? {
                    JobOutput::Partition(receipt) => partitions.acknowledge(receipt)?,
                    JobOutput::Producer(_) => unreachable!("partition phase"),
                }
            }
        }
        let runtime_stats = runtime.statistics();
        executor.metrics.pipeline_runtime_submitted = runtime_stats.submitted;
        executor.metrics.pipeline_runtime_caller = runtime_stats.caller_started;
        executor.metrics.pipeline_runtime_background = runtime_stats.background_started;
        executor.metrics.pipeline_runtime_completed = runtime_stats.completed;
        executor.metrics.pipeline_runtime_peak_outstanding = runtime_stats.peak_outstanding;
        executor.metrics.pipeline_runtime_peak_queued = runtime_stats.peak_queued;
        drop(runtime_owner.take()); // All query threads join before result materialization.
        let executor = executor.with_aggregation(PartitionOutput::new(
            partitions,
            options.cancellation.clone(),
        ));
        let mut output = executor
            .finish(start, None, options)
            .map_err(|e| attempt.record(usize::MAX, Failure::sql(e)))?;
        output.metrics.workers = options.workers;
        output.metrics.worker_threads_peak = budget.peak_threads();
        Ok(output)
    })();
    if let Err(error) = &result {
        attempt.returned(error.clone());
    }
    if let Some(runtime) = runtime_owner.take()
        && let Err(error) = runtime.shutdown_for_retry()
    {
        return Err(attempt.record(usize::MAX, Failure::Terminal(error)));
    }
    drop(descriptors_owner);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    pub(super) struct Fixture(pub(super) PathBuf);
    impl Fixture {
        pub(super) fn new(rows: usize, group_size: usize, wide: bool) -> Self {
            let path = std::env::temp_dir().join(format!(
                "assaydb-pipeline-{}-{}.parquet",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", Type::Int.arrow(), true),
                Field::new("s", Type::Text.arrow(), true),
                Field::new("v", Type::Decimal(0).arrow(), true),
                Field::new("d", Type::Int.arrow(), false),
            ]));
            let limit = 10_i128.pow(38) - 1;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        (0..rows)
                            .map(|i| {
                                [
                                    Some(3),
                                    Some(1),
                                    None,
                                    Some(2),
                                    Some(3),
                                    Some(1),
                                    Some(2),
                                    None,
                                ][i % 8]
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        (0..rows)
                            .map(|i| [Some("é"), Some(""), None, Some("a")][i % 4])
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(
                        Decimal128Array::from(
                            (0..rows)
                                .map(|i| {
                                    Some(if wide {
                                        if i < rows / 2 { limit } else { -limit }
                                    } else {
                                        i as i128 - 5
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                    ),
                    Arc::new(Int64Array::from(
                        (0..rows)
                            .map(|i| if i + 1 == rows { 1 } else { 0 })
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let file = File::create_new(&path).unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                schema,
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(group_size)
                        .build(),
                ),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            Self(path)
        }
        pub(super) fn prepared(&self, sql: &str, options: &Options) -> Prepared {
            prepare(sql, &HashMap::from([("t".into(), self.0.clone())]), options).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
    fn result(prepared: &Prepared, options: &Options) -> Result<RecordBatch> {
        crate::execute::execute(prepared, options).map(|r| r.batch)
    }

    #[test]
    fn source_lookup_metrics_are_propagated_with_ordered_encounters() {
        let fixture = Fixture::new(256, 128, false);
        let options = Options {
            workers: 2,
            batch_size: 64,
            test_pipeline_partitions: Some(2),
            test_pipeline_slice_bytes: Some(2 << 20),
            ..Options::default()
        };
        let prepared = fixture.prepared("SELECT s,COUNT(*) FROM t GROUP BY s", &options);
        let result = crate::execute::execute(&prepared, &options).unwrap();
        assert_eq!(result.metrics.pipeline_producers, 2);
        assert!(result.metrics.short_group_cache_batches > 0);
        assert!(result.metrics.short_group_cache_hits > 0);
        assert!(result.metrics.short_group_cache_peak_bytes > 0);
        assert_eq!(result.metrics.dense_group_batches, 0);
    }

    #[test]
    fn candidate_resource_replays_once_with_original_options_and_releases_owners() {
        let fixture = Fixture::new(32, 4, false);
        for workers in [1, 2, 4] {
            for cap in [1, 4096] {
                let baseline = Options {
                    workers,
                    batch_size: 2,
                    ..Options::default()
                };
                let report = Arc::new(std::sync::Mutex::new(PipelineAttemptReport::default()));
                let options = Options {
                    test_pipeline_partitions: Some(4),
                    test_pipeline_slice_bytes: Some(cap),
                    test_pipeline_fallback: true,
                    test_pipeline_report: Some(report.clone()),
                    ..baseline.clone()
                };
                let prepared = fixture.prepared(
                    "SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k ORDER BY k",
                    &baseline,
                );
                let expected = result(&prepared, &baseline).unwrap();
                let budget = Budget::new(options.memory_bytes);
                let held = budget.reserve(512).unwrap();
                let actual = super::execute(&prepared, &options, budget.clone()).unwrap();
                assert_eq!(actual.batch, expected);
                assert_eq!(actual.metrics.workers, workers);
                assert_eq!(actual.metrics.pipeline_candidate_attempts, 1);
                assert_eq!(actual.metrics.pipeline_fallbacks, 1);
                assert!(
                    actual.metrics.execution_seconds
                        >= actual.metrics.pipeline_candidate_seconds
                            + actual.metrics.pipeline_fallback_seconds
                );
                assert!(
                    actual.metrics.peak_reserved_bytes
                        >= actual.metrics.pipeline_candidate_peak_reserved_bytes
                );
                assert_eq!(report.lock().unwrap().fallback_attempts, 1);
                assert!(options.cancellation.check().is_ok());
                drop(actual);
                assert_eq!(budget.used(), 512);
                assert_eq!(budget.active_workers(), 0);
                drop(held);
            }
        }
    }

    #[test]
    fn sql_only_never_retries_but_mixed_source_sql_and_resource_uses_baseline_once() {
        let fixture = Fixture::new(32, 4, false);
        let baseline = Options {
            workers: 2,
            batch_size: 2,
            ..Options::default()
        };
        let prepared = fixture.prepared(
            "SELECT k,COUNT(CAST(1.0 / CASE WHEN v < 0 THEN 0 ELSE 1 END AS INTEGER)) FROM t GROUP BY k",
            &baseline,
        );
        let expected = result(&prepared, &baseline).unwrap_err();
        assert_eq!(expected, "division by zero");
        for (cap, retries) in [(128 * 1024, 0), (4096, 1)] {
            let report = Arc::new(std::sync::Mutex::new(PipelineAttemptReport::default()));
            let options = Options {
                test_pipeline_partitions: Some(4),
                test_pipeline_slice_bytes: Some(cap),
                test_pipeline_fallback: true,
                test_pipeline_report: Some(report.clone()),
                ..baseline.clone()
            };
            let budget = Budget::new(options.memory_bytes);
            let error = super::execute(&prepared, &options, budget.clone())
                .err()
                .unwrap();
            assert_eq!(error, expected);
            let report = report.lock().unwrap();
            assert_eq!(report.candidate_attempts, 1);
            assert_eq!(report.fallback_attempts, retries);
            assert_eq!(
                report.candidate_sql_error.as_deref(),
                Some(expected.as_str())
            );
            assert_eq!(report.candidate_error.is_some(), retries != 0);
            assert!(options.cancellation.check().is_ok());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }

    #[test]
    fn candidate_cancellation_is_terminal_and_nonadmission_is_not_a_retry() {
        let fixture = Fixture::new(21, 3, false);
        let report = Arc::new(std::sync::Mutex::new(PipelineAttemptReport::default()));
        let options = Options {
            workers: 1,
            batch_size: 2,
            test_pipeline_partitions: Some(2),
            test_pipeline_slice_bytes: Some(1),
            test_pipeline_fallback: true,
            test_pipeline_report: Some(report.clone()),
            ..Options::default()
        };
        let prepared = fixture.prepared("SELECT k,COUNT(*) FROM t GROUP BY k", &options);
        let actual = crate::execute::execute(&prepared, &options).unwrap();
        assert_eq!(actual.metrics.pipeline_candidate_attempts, 0);
        assert_eq!(report.lock().unwrap().fallback_attempts, 0);
        options.cancellation.cancel();
        let budget = Budget::new(options.memory_bytes);
        assert!(
            super::execute(&prepared, &options, budget.clone())
                .err()
                .unwrap()
                .contains("cancel")
        );
        assert_eq!(report.lock().unwrap().fallback_attempts, 0);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }

    #[test]
    fn persistent_direct_sql_matches_original_across_workers_filters_and_order() {
        let fixture = Fixture::new(32, 4, false);
        for workers in [1, 2, 4] {
            for batch_size in [1, 2, 4] {
                let old = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let new = Options {
                    test_pipeline_partitions: Some(4),
                    ..old.clone()
                };
                for sql in [
                    "SELECT k,SUM(v),AVG(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
                    "SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k",
                    "SELECT k,SUM(v) FROM t WHERE k IS NOT NULL GROUP BY k ORDER BY k DESC",
                    "SELECT k,COUNT(*) FROM t WHERE v>1000 GROUP BY k",
                    "SELECT k,COUNT(*) FROM t WHERE v>15 GROUP BY k",
                    "SELECT k,COUNT(*) FROM t WHERE k>100 GROUP BY k",
                ] {
                    let prepared = fixture.prepared(sql, &old);
                    assert!(eligible(&prepared, &new));
                    let expected = result(&prepared, &old).unwrap();
                    let actual = crate::execute::execute(&prepared, &new).unwrap();
                    assert_eq!(
                        actual.batch, expected,
                        "workers={workers},batch={batch_size}: {sql}"
                    );
                    assert_eq!(actual.metrics.pipeline_producers, workers);
                    assert_eq!(actual.metrics.pipeline_source_batches, 32 / batch_size);
                    assert!(
                        actual.metrics.pipeline_source_batches > actual.metrics.pipeline_producers
                    );
                    assert!(actual.metrics.worker_threads_peak <= workers);
                    assert_eq!(
                        actual.metrics.pipeline_runtime_submitted[0],
                        actual.metrics.pipeline_step_jobs
                    );
                    assert_eq!(
                        actual.metrics.pipeline_runtime_submitted,
                        actual.metrics.pipeline_runtime_completed
                    );
                    for kind in 0..3 {
                        assert_eq!(
                            actual.metrics.pipeline_runtime_submitted[kind],
                            actual.metrics.pipeline_runtime_caller[kind]
                                + actual.metrics.pipeline_runtime_background[kind]
                        );
                    }
                    assert!(actual.metrics.pipeline_runtime_peak_outstanding <= workers.max(4));
                    assert!(
                        actual.metrics.pipeline_runtime_peak_queued
                            <= actual.metrics.pipeline_runtime_peak_outstanding
                    );
                }
            }
        }
    }
    #[test]
    fn wide_states_and_input_errors_match_before_finalization() {
        let fixture = Fixture::new(32, 4, true);
        for workers in [1, 2, 4] {
            let old = Options {
                workers,
                batch_size: 2,
                ..Options::default()
            };
            let new = Options {
                test_pipeline_partitions: Some(3),
                ..old.clone()
            };
            for sql in [
                "SELECT k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k",
                "SELECT k,SUM(v) FROM t WHERE v>0 GROUP BY k",
                "SELECT k,SUM(v),COUNT(CAST(1.0/(1-d) AS INTEGER)) FROM t WHERE v>0 OR d=1 GROUP BY k",
                "SELECT k,COUNT(v+v),COUNT(1/d) FROM t GROUP BY k",
            ] {
                let prepared = fixture.prepared(sql, &old);
                let expected = result(&prepared, &old);
                if sql.contains("1-d") {
                    assert!(
                        expected
                            .as_ref()
                            .err()
                            .unwrap()
                            .contains("division by zero")
                    );
                }
                assert_eq!(
                    result(&prepared, &new),
                    expected,
                    "workers={workers}: {sql}"
                );
                let bounded = Options {
                    test_pipeline_slice_bytes: Some(128 * 1024),
                    ..new.clone()
                };
                assert_eq!(
                    result(&prepared, &bounded),
                    expected,
                    "bounded workers={workers}: {sql}"
                );
            }
        }
    }
    #[test]
    fn reverse_source_flush_retains_original_groups_and_uses_background_workers() {
        let fixture = Fixture::new(8192, 512, false);
        let old = Options {
            workers: 4,
            batch_size: 64,
            ..Options::default()
        };
        let new = Options {
            test_pipeline_partitions: Some(4),
            ..old.clone()
        };
        for sql in [
            "SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k",
            "SELECT s,k,SUM(v),AVG(v) FROM t WHERE v>0 GROUP BY s,k",
        ] {
            let prepared = fixture.prepared(sql, &old);
            let expected = result(&prepared, &old).unwrap();
            let actual =
                execute_with_flush_order(&prepared, &new, Budget::new(new.memory_bytes), true)
                    .unwrap();
            assert_eq!(actual.batch, expected);
            assert_eq!(actual.metrics.pipeline_producers, 4);
            assert!(actual.metrics.pipeline_background_producers > 0);
            assert_eq!(actual.metrics.pipeline_source_batches, 128);
            assert!(actual.metrics.worker_threads_peak <= 4);
        }
    }

    #[test]
    fn fallback_precedes_consumption_and_unaligned_layout_keeps_old_batching() {
        let fixture = Fixture::new(6, 2, false);
        let old = Options {
            batch_size: 3,
            ..Options::default()
        };
        let new = Options {
            test_pipeline_partitions: Some(4),
            ..old.clone()
        };
        let prepared = fixture.prepared("SELECT k,COUNT(*) FROM t GROUP BY k", &old);
        let original = crate::execute::execute(&prepared, &old).unwrap();
        let actual = crate::execute::execute(&prepared, &new).unwrap();
        assert_eq!(actual.batch, original.batch);
        assert_eq!(actual.metrics.pipeline_producers, 0);
        assert_eq!(actual.metrics.batches, original.metrics.batches);
        for sql in [
            "SELECT SUM(v) FROM t",
            "SELECT k,MIN(v) FROM t GROUP BY k",
            "SELECT k,COUNT(DISTINCT s) FROM t GROUP BY k",
        ] {
            let prepared = fixture.prepared(sql, &old);
            assert!(!eligible(&prepared, &new));
            assert_eq!(result(&prepared, &new), result(&prepared, &old));
        }
    }
    pub(super) fn test_producer(fixture: &Fixture, options: &Options, budget: &Budget) -> Producer {
        let prepared = fixture.prepared("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", options);
        let metadata =
            ArrowReaderMetadata::load(&File::open(&fixture.0).unwrap(), Default::default())
                .unwrap();
        let groups = 0..metadata.metadata().num_row_groups();
        let expressions = Arc::new(crate::expr_program::ExpressionProgram::new(
            &prepared.plan,
            prepared.schema.fields().len(),
            true,
        ));
        let routing = OwnedPartitions::new(&prepared.plan, &expressions, budget.clone(), 4)
            .unwrap()
            .routing_plan();
        Producer {
            attempt: Arc::new(Attempt::default()),
            source: 0,
            routing,
            path: fixture.0.clone(),
            metadata: Some(metadata),
            plan: Arc::new(prepared.plan),
            expressions,
            schema: prepared.schema,
            options: options.clone(),
            slice_bytes: options.test_pipeline_slice_bytes,
            budget: budget.clone(),
            groups,
            cursor: None,
            progress: None,
            state: None,
            backing: None,
            pending: None,
            metrics: Metrics::default(),
            background: false,
            last_worker: None,
            shared: None,
        }
    }

    #[test]
    fn bounded_slices_merge_repeated_keys_without_repeating_expressions() {
        let fixture = Fixture::new(256, 64, false);
        for workers in [1, 2, 4] {
            let old = Options {
                workers,
                batch_size: 8,
                ..Options::default()
            };
            let reference = Options {
                test_pipeline_partitions: Some(4),
                ..old.clone()
            };
            let bounded = Options {
                test_pipeline_slice_bytes: Some(128 * 1024),
                ..reference.clone()
            };
            for sql in [
                "SELECT k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k",
                "SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k",
                "SELECT v,COUNT(*) FROM t GROUP BY v",
            ] {
                let prepared = fixture.prepared(sql, &old);
                let expected = result(&prepared, &old).unwrap();
                let reference = crate::execute::execute(&prepared, &reference).unwrap();
                let budget = Budget::new(bounded.memory_bytes);
                let actual = execute(&prepared, &bounded, budget.clone()).unwrap();
                assert_eq!(actual.batch, expected, "workers={workers}: {sql}");
                assert!(actual.metrics.pipeline_pressure_flushes > workers);
                assert_eq!(
                    actual.metrics.pipeline_source_routes,
                    actual.metrics.pipeline_pressure_flushes + actual.metrics.pipeline_producers
                );
                assert!(
                    actual.metrics.pipeline_source_routed_groups
                        >= actual.metrics.pipeline_source_routes
                );
                assert_eq!(actual.metrics.pipeline_source_batches, 32);
                assert_eq!(
                    actual.metrics.shared_expression_evaluations,
                    reference.metrics.shared_expression_evaluations
                );
                assert!(actual.metrics.pipeline_exchange_peak <= workers);
                assert!(actual.metrics.pipeline_runtime_peak_outstanding <= workers + 4);
                assert_eq!(
                    actual.metrics.pipeline_runtime_submitted,
                    actual.metrics.pipeline_runtime_completed
                );
                assert!(actual.metrics.worker_threads_peak <= workers);
                drop(actual);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn bounded_slice_keeps_pending_batch_and_releases_backing_before_resume() {
        let fixture = Fixture::new(32, 8, false);
        let options = Options {
            batch_size: 8,
            test_pipeline_slice_bytes: Some(128 * 1024),
            ..Options::default()
        };
        let budget = Budget::new(options.memory_bytes);
        let mut producer = test_producer(&fixture, &options, &budget);
        let (producer, raw) = loop {
            match producer.step().unwrap() {
                ProducerStep::Continue(next) => producer = next,
                ProducerStep::Flush { producer, routed } => break (producer, routed),
                ProducerStep::Finished(_) => panic!("expected pressure flush"),
            }
        };
        assert!(producer.state.is_none() && producer.backing.is_none());
        assert_eq!(producer.pending.as_ref().unwrap().batch.rows, 8);
        let source_batches = producer.metrics.pipeline_source_batches;
        let evaluations = producer.metrics.shared_expression_evaluations;
        let live = budget.used();
        drop(raw);
        assert!(live - budget.used() >= options.test_pipeline_slice_bytes.unwrap());
        let ProducerStep::Continue(producer) = producer.step().unwrap() else {
            panic!("empty slice must accept retained batch");
        };
        assert!(producer.pending.is_none());
        assert_eq!(producer.metrics.pipeline_source_batches, source_batches);
        assert_eq!(producer.metrics.shared_expression_evaluations, evaluations);
        options.cancellation.cancel();
        assert!(producer.step().err().unwrap().contains("cancel"));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn bounded_slices_handle_empty_oversized_and_competing_error_inputs() {
        let fixture = Fixture::new(32, 4, false);
        for workers in [1, 2, 4] {
            let old = Options {
                workers,
                batch_size: 1,
                ..Options::default()
            };
            let bounded = Options {
                test_pipeline_partitions: Some(4),
                test_pipeline_slice_bytes: Some(128 * 1024),
                ..old.clone()
            };
            for sql in [
                "SELECT k,COUNT(*) FROM t WHERE k>100 GROUP BY k",
                "SELECT k,COUNT(CAST(1.0/v AS INTEGER)),COUNT(CASE WHEN v=11 THEN v+99999999999999999999999999999999999999 ELSE 0 END) FROM t GROUP BY k",
            ] {
                let prepared = fixture.prepared(sql, &old);
                let budget = Budget::new(bounded.memory_bytes);
                let expected = result(&prepared, &old);
                let actual = execute(&prepared, &bounded, budget.clone()).map(|r| r.batch);
                assert_eq!(actual, expected);
                assert_eq!(budget.used(), 0);
            }
            let tiny = Options {
                test_pipeline_slice_bytes: Some(4096),
                ..bounded
            };
            let prepared = fixture.prepared("SELECT k,COUNT(*) FROM t GROUP BY k", &old);
            let budget = Budget::new(tiny.memory_bytes);
            assert!(
                execute(&prepared, &tiny, budget.clone())
                    .err()
                    .unwrap()
                    .contains("cannot fit empty pipeline slice")
            );
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn source_export_routes_on_helper_before_coordinator_receives_fragment() {
        use std::{sync::mpsc, time::Duration};
        let fixture = Fixture::new(8, 4, false);
        let options = Options {
            batch_size: 2,
            ..Options::default()
        };
        let budget = Budget::new(options.memory_bytes);
        let mut producer = test_producer(&fixture, &options, &budget);
        let mut runtime =
            Runtime::<ProducerStep>::new(&budget, 2, 1, options.cancellation.clone()).unwrap();
        let (finished, wait_finished) = mpsc::channel();
        let output = loop {
            let finished = finished.clone();
            assert!(
                runtime
                    .try_submit(JobKind::Morsel, move |_| {
                        let result = producer.step();
                        finished.send(()).unwrap();
                        result
                    })
                    .is_ok()
            );
            // Do not call help_one/next_completion until the helper has completed
            // the step. This observes actual background execution, not a likely
            // race between caller and helper.
            wait_finished.recv_timeout(Duration::from_secs(5)).unwrap();
            match runtime.next_completion().unwrap().result.ok().unwrap() {
                ProducerStep::Continue(next) => producer = next,
                ProducerStep::Finished(output) => break output,
                ProducerStep::Flush { .. } => panic!("unbounded test has no pressure flush"),
            }
        };
        assert_eq!(output.metrics.pipeline_source_routes, 1);
        assert_eq!(output.metrics.pipeline_background_routes, 1);
        assert_eq!(output.metrics.pipeline_source_routed_groups, 4);
        assert_eq!(runtime.statistics().caller_started[0], 0);
        assert_eq!(
            runtime.statistics().background_started[0],
            output.metrics.pipeline_step_jobs
        );
        let retained = budget.used();
        drop(output);
        assert!(budget.used() < retained);
        drop(runtime);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }

    #[test]
    fn continuation_moves_from_caller_to_idle_helper_and_cancels_with_live_state() {
        use std::{sync::mpsc, time::Duration};
        let fixture = Fixture::new(32, 4, false);
        let options = Options {
            batch_size: 1,
            ..Options::default()
        };
        let budget = Budget::new(options.memory_bytes);
        let producer = test_producer(&fixture, &options, &budget);
        let mut runtime =
            Runtime::<ProducerStep>::new(&budget, 2, 2, options.cancellation.clone()).unwrap();
        let (started, wait_started) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        assert!(
            runtime
                .try_submit(JobKind::Finalize, move |_| {
                    started.send(()).unwrap();
                    gate.recv().unwrap();
                    Err("test-held-worker".into())
                })
                .is_ok()
        );
        wait_started.recv_timeout(Duration::from_secs(5)).unwrap();
        let id = runtime
            .try_submit(JobKind::Morsel, move |_| producer.step())
            .ok()
            .unwrap();
        assert!(runtime.help_one()); // Only the producer is queued; helper is held.
        let done = runtime.try_completion().unwrap();
        assert_eq!(done.id, id);
        let ProducerStep::Continue(producer) = done.result.ok().unwrap() else {
            panic!("expected continuation")
        };
        assert_eq!(producer.metrics.pipeline_source_batches, 4);
        assert_eq!(producer.metrics.pipeline_step_jobs, 1);
        assert_eq!(producer.state.as_ref().unwrap().group_count, 4);
        assert!(!producer.background);
        release.send(()).unwrap();
        assert_eq!(
            runtime.next_completion().unwrap().result.err().unwrap(),
            "test-held-worker"
        );
        let (finished, wait_finished) = mpsc::channel();
        assert!(
            runtime
                .try_submit(JobKind::Morsel, move |_| {
                    let result = producer.step();
                    finished.send(()).unwrap();
                    result
                })
                .is_ok()
        );
        // No caller help: the idle background worker must run this same owner.
        wait_finished.recv_timeout(Duration::from_secs(5)).unwrap();
        let ProducerStep::Continue(producer) =
            runtime.next_completion().unwrap().result.ok().unwrap()
        else {
            panic!("expected continuation")
        };
        assert_eq!(producer.metrics.pipeline_source_batches, 8);
        assert_eq!(producer.metrics.pipeline_step_jobs, 2);
        assert_eq!(producer.state.as_ref().unwrap().group_count, 4);
        assert_eq!(producer.metrics.pipeline_worker_handoffs, 1);
        assert!(producer.background);
        options.cancellation.cancel();
        assert!(
            runtime
                .try_submit(JobKind::Morsel, move |_| producer.step())
                .is_ok()
        );
        assert!(
            runtime
                .next_completion()
                .unwrap()
                .result
                .err()
                .unwrap()
                .contains("cancel")
        );
        drop(runtime);
        assert_eq!(budget.used(), 0);
        let permits = budget.workers(1, 1);
        assert_eq!(permits.count, 1);
        drop(permits);
    }

    #[test]
    fn resubmitted_producer_errors_follow_source_rank_not_new_job_ids() {
        let fixture = Fixture::new(32, 4, false);
        // Earlier source fails in its second row-group morsel; a later source
        // fails in its first, with a smaller runtime ID than the failing resume.
        let sql = "SELECT k,COUNT(CAST(1.0/v AS INTEGER)),COUNT(CASE WHEN v=11 THEN v+99999999999999999999999999999999999999 ELSE 0 END) FROM t GROUP BY k";
        for workers in [2, 4] {
            let old = Options {
                workers,
                batch_size: 1,
                ..Options::default()
            };
            let new = Options {
                test_pipeline_partitions: Some(4),
                ..old.clone()
            };
            let prepared = fixture.prepared(sql, &old);
            let expected = result(&prepared, &old).err().unwrap();
            assert!(expected.contains("division by zero"));
            let budget = Budget::new(new.memory_bytes);
            let actual = super::execute(&prepared, &new, budget.clone())
                .err()
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn pipeline_budget_and_cancel_failures_release_query_ownership() {
        let fixture = Fixture::new(32, 4, false);
        let options = Options {
            workers: 4,
            batch_size: 2,
            test_pipeline_partitions: Some(4),
            ..Options::default()
        };
        let prepared = fixture.prepared("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", &options);
        let tiny = Budget::new(1024);
        assert!(super::execute(&prepared, &options, tiny.clone()).is_err());
        assert_eq!(tiny.used(), 0);
        let budget = Budget::new(options.memory_bytes);
        options.cancellation.cancel();
        assert!(
            super::execute(&prepared, &options, budget.clone())
                .err()
                .unwrap()
                .contains("cancel")
        );
        assert_eq!(budget.used(), 0);
        let available = budget.workers(3, 3);
        assert_eq!(available.count, 3);
        drop(available);
        let fresh = Options {
            cancellation: crate::spill::Cancellation::default(),
            ..options
        };
        let output = super::execute(&prepared, &fresh, budget.clone()).unwrap();
        assert_eq!(output.metrics.pipeline_producers, 4);
        drop(output);
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
pub(crate) mod continuation_tests;
#[cfg(test)]
mod morsel_tests;
