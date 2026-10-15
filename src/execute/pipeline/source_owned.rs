//! Source ownership for exhaustively proved numeric key domains.
//! The proof reads immutable cached input; no file statistics or sort assumption.
use super::*;
use crate::{expr::Kind, pipeline_runtime::JobId};
use aggregation::source_owned::{SourceOwned, new_state};

const DENSE_ALLOWANCE: usize = 32 << 20;
/// Internal staging diagnostics; absent from production Options and Metrics.
#[derive(Default, Clone, Debug)]
pub(crate) struct Report {
    pub proof_seconds: f64,
    pub owners_seconds: f64,
    pub boundary_seconds: f64,
    pub output_seconds: f64,
    pub admitted: bool,
    pub dense_bytes: usize,
    pub boundary_merges: usize,
    pub proof_jobs: usize,
    pub proof_background_jobs: usize,
}
struct ReportOwner {
    report: Report,
    #[cfg(test)]
    target: Option<Arc<std::sync::Mutex<Report>>>,
}
impl Drop for ReportOwner {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(target) = &self.target {
            *target.lock().unwrap() = self.report.clone();
        }
    }
}
struct Domain {
    batches: Range<usize>,
    bounds: Option<(i128, i128)>,
    window: Option<(i128, usize)>,
}
struct Proof {
    domains: Vec<Domain>,
    rows: u64,
    dense_bytes: usize,
    _metadata: Arc<Reservation>,
}
fn eligible(prepared: &Prepared, options: &Options) -> bool {
    let plan = &prepared.plan;
    #[cfg(test)]
    if !options.test_source_owned_pipeline {
        return false;
    }
    (2..=64).contains(&options.workers)
        && options.batch_size > 0
        && options.relational_optimizations
        && options.spill.is_none()
        && plan.aggregation
        && super::integer_totals_reorder_safe(plan, &prepared.schema)
        && plan.groups.len() == 1
        && plan.filter.is_none()
        && plan.order.is_empty()
        && matches!(plan.groups[0].kind, Kind::Column(_))
        && matches!(plan.groups[0].ty, Type::Int | Type::Decimal(_))
        && plan.aggregates.iter().all(|aggregate| {
            !aggregate.distinct
                && matches!(
                    aggregate.function,
                    Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                )
                && (aggregate.input.is_some() || matches!(aggregate.function, Aggregate::Count))
                && aggregate.input.as_ref().is_none_or(|input| {
                    matches!(input.kind, Kind::Column(_))
                        && (matches!(aggregate.function, Aggregate::Count)
                            || matches!(input.ty, Type::Int | Type::Decimal(_) | Type::Null))
                })
        })
}
mod proof;
use proof::prove;

// One physical runtime survives both the immutable proof and aggregate phases.
// Queue slots pay for the larger continuation before either phase starts.
#[allow(clippy::large_enum_variant)]
enum SourceJob {
    Proof(proof::ProofProducer),
    Aggregate(Producer),
}

struct Producer {
    attempt: Arc<Attempt>,
    rank: usize,
    next: Range<usize>,
    state: Aggregation,
    plan: Arc<Plan>,
    expressions: Arc<crate::expr_program::ExpressionProgram>,
    schema: SchemaRef,
    budget: Budget,
    metrics: Metrics,
    // Input arrays and their query leases outlive all batch evaluation/state.
    storage: Arc<crate::rel::Materialized>,
    // Jobs can outlive lexical Proof on an early dispatch/completion error.
    // Keep plan/program/state-header credit until every producer field drops.
    _metadata: Arc<Reservation>,
}
impl Producer {
    fn step(self, cancellation: &crate::spill::Cancellation) -> Result<Self> {
        let attempt = self.attempt.clone();
        let rank = self.rank;
        self.step_inner(cancellation)
            .map_err(|error| attempt.record(rank, Failure::sql(error)))
    }
    fn step_inner(mut self, cancellation: &crate::spill::Cancellation) -> Result<Self> {
        self.metrics.pipeline_step_jobs += 1;
        let start_rows = self.metrics.rows_read;
        let mut batches = 0;
        while batches < 16 && self.metrics.rows_read - start_rows < 128 * 1024 {
            cancellation.check()?;
            let Some(index) = self.next.next() else {
                break;
            };
            let batch = &self.storage.batches[index];
            let rows = batch.num_rows();
            let descriptors = self
                .schema
                .fields()
                .len()
                .checked_add(self.plan.groups.len())
                .and_then(|n| n.checked_add(self.plan.aggregates.len()))
                .and_then(|n| n.checked_add(self.expressions.bindings.len()))
                .ok_or("source batch envelope overflow")?;
            let _envelope = self.budget.reserve(
                descriptors
                    .checked_mul(std::mem::size_of::<Option<Value>>() * 4)
                    .and_then(|n| n.checked_add(2048))
                    .ok_or("source batch envelope overflow")?,
            )?;
            let mut input = Input {
                len: rows,
                columns: vec![None; self.schema.fields().len()],
            };
            for &column in &self.plan.columns {
                input.columns[column] = Some(normalize_input(
                    batch.column(column),
                    &Type::from_field(self.schema.field(column))?,
                    &self.budget,
                )?);
            }
            let start = Instant::now();
            let (keys, values) = self.expressions.evaluate(&mut input, &self.budget)?;
            self.metrics.expression_seconds += start.elapsed().as_secs_f64();
            self.metrics.shared_expression_evaluations += self.expressions.bindings.len();
            self.metrics.rows_read += rows;
            self.metrics.rows_after_filter += rows;
            self.metrics.batches += 1;
            self.metrics.pipeline_source_batches += 1;
            self.state.update(
                &self.plan,
                &self.expressions,
                &mut self.metrics,
                EvaluatedBatch {
                    rows,
                    keys,
                    values,
                    prepared_keys: None,
                },
            )?;
            batches += 1;
        }
        Ok(self)
    }
}

pub(crate) fn try_execute(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    storage: Arc<crate::rel::Materialized>,
    attempt: Arc<Attempt>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Metrics>> {
    let start = Instant::now();
    // Cached and streaming owners share one optional index allowance, even
    // when downstream execution starts while earlier output still owns state.
    let dense_allowance = budget.source_dense_allowance(DENSE_ALLOWANCE);
    if dense_allowance.bytes < DENSE_ALLOWANCE {
        return Ok(None);
    }
    let mut report = ReportOwner {
        report: Report::default(),
        #[cfg(test)]
        target: options.test_source_owned_report.clone(),
    };
    // Always explicitly retire the runtime, even speculative proof failures.
    // Its ordinary Drop intentionally aborts the caller, which decline must not.
    let mut runtime_owner = None;
    let result = (|| {
        let proof_result = prove(
            prepared,
            options,
            &budget,
            storage.clone(),
            &mut runtime_owner,
            &mut report.report,
            #[cfg(test)]
            None,
        );
        report.report.proof_seconds = start.elapsed().as_secs_f64();
        let proof_result = match proof_result {
            Err(error)
                if error.starts_with("resource-limit: accounted memory ")
                    || error == "pipeline closure memory limit" =>
            {
                // Speculative proof has consumed no aggregate input. Its owners
                // have dropped; preserve baseline memory fitting without a replay.
                options.cancellation.check()?;
                return Ok(None);
            }
            other => other,
        };
        let Some(proof) = proof_result? else {
            return Ok(None);
        };
        report.report.admitted = true;
        report.report.dense_bytes = proof.dense_bytes;
        attempt
            .admitted
            .store(true, std::sync::atomic::Ordering::Release);
        let count = proof.domains.len();
        // Proof and aggregate jobs retain their cache/descriptor backing independently.

        let owners_start = Instant::now();
        let mut executor = super::super::executor(prepared, budget.clone(), true)?;
        let plan = Arc::new(prepared.plan.clone());
        let expressions = executor.expressions.clone();
        // Prepay/install every exact dense window before dispatch. All are
        // charged to this query, and their TOTAL is <=32MiB including headers.
        let mut producers = proof
            .domains
            .iter()
            .enumerate()
            .map(|(rank, domain)| {
                Ok(Producer {
                    attempt: attempt.clone(),
                    rank,
                    next: domain.batches.clone(),
                    state: new_state(&plan, &expressions, &budget, domain.window)?,
                    plan: plan.clone(),
                    expressions: expressions.clone(),
                    schema: prepared.schema.clone(),
                    budget: budget.clone(),
                    metrics: Metrics::default(),
                    storage: storage.clone(),
                    _metadata: proof._metadata.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let runtime = runtime_owner.as_mut().unwrap();
        let mut active: Vec<Option<JobId>> = vec![None; count];
        let mut completed: Vec<Option<Aggregation>> = (0..count).map(|_| None).collect();
        for producer in producers.drain(..) {
            let rank = producer.rank;
            active[rank] = Some(
                runtime
                    .try_submit(JobKind::Morsel, move |cancel| {
                        producer.step(cancel).map(SourceJob::Aggregate)
                    })
                    .map_err(|error| attempt.rejection(error.reason))?,
            );
        }
        let mut error: Option<(usize, String)> = None;
        while let Some(done) = runtime.next_completion() {
            let rank = active
                .iter()
                .position(|id| *id == Some(done.id))
                .ok_or("source owner completion identity")?;
            active[rank] = None;
            match done.result {
                Err(message) => {
                    let message = attempt.record(rank, Failure::sql(message));
                    if error.as_ref().is_none_or(|(old, _)| rank < *old) {
                        error = Some((rank, message));
                    }
                }
                Ok(SourceJob::Proof(_)) => {
                    return Err("proof result after aggregate dispatch".into());
                }
                Ok(SourceJob::Aggregate(producer)) if !producer.next.is_empty() => {
                    active[rank] = Some(
                        runtime
                            .try_submit(JobKind::Morsel, move |cancel| {
                                producer.step(cancel).map(SourceJob::Aggregate)
                            })
                            .map_err(|error| attempt.rejection(error.reason))?,
                    );
                }
                Ok(SourceJob::Aggregate(producer)) => {
                    merge_metrics(&mut executor.metrics, producer.metrics);
                    completed[rank] = Some(producer.state);
                }
            }
        }
        let stats = runtime.statistics();
        runtime_owner.take().unwrap().shutdown_for_retry()?;
        report.report.owners_seconds = owners_start.elapsed().as_secs_f64();
        if let Some((_, message)) = error {
            return Err(message);
        }
        if executor.metrics.rows_read as u64 != proof.rows {
            return Err("source owner delivered row count mismatch".into());
        }
        executor.metrics.pipeline_producers = count;
        executor.metrics.pipeline_runtime_submitted = stats.submitted;
        executor.metrics.pipeline_runtime_caller = stats.caller_started;
        executor.metrics.pipeline_runtime_background = stats.background_started;
        executor.metrics.pipeline_runtime_completed = stats.completed;
        executor.metrics.pipeline_runtime_peak_outstanding = stats.peak_outstanding;
        executor.metrics.pipeline_runtime_peak_queued = stats.peak_queued;
        executor.metrics.dense_group_index_peak_bytes = proof.dense_bytes;
        let states = completed
            .into_iter()
            .map(|state| state.ok_or_else(|| "source owner result missing".into()))
            .collect::<Result<Vec<_>>>()?;
        let domains = proof
            .domains
            .iter()
            .map(|domain| domain.bounds)
            .collect::<Vec<_>>();
        let boundary_start = Instant::now();
        let output = SourceOwned::finish(
            states,
            &domains,
            &plan,
            &expressions,
            &options.cancellation,
            proof._metadata.clone(),
        )?;
        report.report.boundary_seconds = boundary_start.elapsed().as_secs_f64();
        report.report.boundary_merges = output.boundary_merges;
        debug_assert!(output.boundary_merges < count);
        executor.metrics.source_domain_proof_seconds = report.report.proof_seconds;
        executor.metrics.source_boundary_merge_seconds = report.report.boundary_seconds;
        executor.metrics.source_boundary_merges = output.boundary_merges;
        let executor = executor.with_aggregation(output);
        let output_start = Instant::now();
        let result = executor
            .finish_batches(start, options, emit)
            .map_err(|error| attempt.record(usize::MAX, Failure::sql(error)))
            .map(Some);
        report.report.output_seconds = output_start.elapsed().as_secs_f64();
        result
    })();
    if let Some(runtime) = runtime_owner.take() {
        // No runtime Drop poisoning, including resource failure during dispatch.
        if let Err(error) = runtime.shutdown_for_retry() {
            return Err(attempt.record(usize::MAX, Failure::Terminal(error)));
        }
    }
    if let Err(error) = &result {
        attempt.returned(error.clone());
    }
    result
}

#[cfg(test)]
mod tests;
