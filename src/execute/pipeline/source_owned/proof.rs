//! Exhaustive, bounded owner-domain proof on the query's existing task runtime.
//! Jobs never evaluate SQL or mutate aggregates; the source-ranked fold below
//! preserves the serial admission decision independently of completion order.
use super::*;
use crate::spill::Cancellation;

#[cfg(test)]
type Hook = Arc<dyn Fn(usize, usize) -> Result<()> + Send + Sync>;
struct Input {
    column: usize,
    columns: Vec<usize>,
    schema: SchemaRef,
    #[cfg(test)]
    hook: Option<Hook>,
    // Source buffers and plan descriptors retain their exact funding owners.
    storage: Arc<crate::rel::Materialized>,
    _metadata: Arc<Reservation>,
}
pub(super) struct ProofProducer {
    rank: usize,
    batches: Range<usize>,
    next: Range<usize>,
    bounds: Option<(i128, i128)>,
    rows: u64,
    declined: bool,
    input: Arc<Input>,
}
impl ProofProducer {
    fn step(mut self, cancellation: &Cancellation) -> Result<Self> {
        let start_rows = self.rows;
        let mut batches = 0;
        while batches < MORSEL_BATCHES && self.rows - start_rows < MORSEL_ROWS as u64 {
            cancellation.check()?;
            let Some(index) = self.next.next() else {
                break;
            };
            #[cfg(test)]
            if let Some(hook) = &self.input.hook {
                hook(self.rank, index)?;
            }
            let batch = &self.input.storage.batches[index];
            self.rows = self
                .rows
                .checked_add(batch.num_rows() as u64)
                .ok_or("source owner rows overflow")?;
            if batch.num_columns() != self.input.schema.fields().len()
                || self.input.columns.iter().any(|&column| {
                    batch.column(column).data_type() != self.input.schema.field(column).data_type()
                        || Type::from_arrow(batch.column(column).data_type()).is_err()
                })
            {
                self.declined = true;
                break;
            }
            let array = batch.column(self.input.column);
            if array.null_count() != 0 {
                self.declined = true;
                break;
            }
            match array.data_type() {
                arrow::datatypes::DataType::Int32 => extend_bounds(
                    &mut self.bounds,
                    array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                        .map(i128::from),
                    cancellation,
                )?,
                arrow::datatypes::DataType::Int64 => extend_bounds(
                    &mut self.bounds,
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                        .map(i128::from),
                    cancellation,
                )?,
                arrow::datatypes::DataType::Decimal128(_, 0..=38) => extend_bounds(
                    &mut self.bounds,
                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                    cancellation,
                )?,
                _ => {
                    self.declined = true;
                    break;
                }
            }
            batches += 1;
        }
        Ok(self)
    }
}
fn extend_bounds(
    bounds: &mut Option<(i128, i128)>,
    values: impl Iterator<Item = i128>,
    cancellation: &Cancellation,
) -> Result<()> {
    for (row, value) in values.enumerate() {
        if row % 4096 == 0 {
            cancellation.check()?;
        }
        *bounds = Some(bounds.map_or((value, value), |(lo, hi)| (lo.min(value), hi.max(value))));
    }
    Ok(())
}

pub(super) fn prove(
    prepared: &Prepared,
    options: &Options,
    budget: &Budget,
    storage: Arc<crate::rel::Materialized>,
    runtime_owner: &mut Option<Runtime<SourceJob>>,
    report: &mut Report,
    #[cfg(test)] hook: Option<Hook>,
) -> Result<Option<Proof>> {
    options.cancellation.check()?;
    if !eligible(prepared, options) || budget.active_workers() != 0 || storage.batches.len() < 2 {
        return Ok(None);
    }
    if !storage.budget.same_domain(budget) {
        return Err("source ownership funding domain mismatch".into());
    }
    if runtime_owner.is_some() {
        return Err("source proof runtime already exists".into());
    }
    let Kind::Column(column) = prepared.plan.groups[0].kind else {
        unreachable!()
    };
    let count = options.workers.min(storage.batches.len());
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
    let input = Arc::new(Input {
        column,
        columns: prepared.plan.columns.clone(),
        schema: prepared.schema.clone(),
        #[cfg(test)]
        hook,
        storage,
        _metadata: metadata.clone(),
    });
    let mut producers = Vec::with_capacity(count);
    for rank in 0..count {
        let first = input
            .storage
            .batches
            .len()
            .checked_mul(rank)
            .ok_or("source owner range overflow")?
            / count;
        let last = input
            .storage
            .batches
            .len()
            .checked_mul(rank + 1)
            .ok_or("source owner range overflow")?
            / count;
        producers.push(ProofProducer {
            rank,
            batches: first..last,
            next: first..last,
            bounds: None,
            rows: 0,
            declined: false,
            input: input.clone(),
        });
    }
    *runtime_owner = Some(Runtime::new(
        budget,
        options.workers,
        count,
        options.cancellation.clone(),
    )?);
    let runtime = runtime_owner.as_mut().unwrap();
    let mut active = vec![None; count];
    let mut completed: Vec<Option<Result<ProofProducer>>> = (0..count).map(|_| None).collect();
    for producer in producers {
        let rank = producer.rank;
        active[rank] = Some(submit(runtime, producer)?);
    }
    while let Some(done) = runtime.next_completion() {
        let rank = active
            .iter()
            .position(|id| *id == Some(done.id))
            .ok_or("source proof completion identity")?;
        active[rank] = None;
        match done.result {
            Ok(SourceJob::Proof(producer)) if producer.rank != rank => {
                return Err("source proof owner identity".into());
            }
            Ok(SourceJob::Proof(producer)) if !producer.declined && !producer.next.is_empty() => {
                active[rank] = Some(submit(runtime, producer)?);
            }
            Ok(SourceJob::Proof(producer)) => completed[rank] = Some(Ok(producer)),
            Ok(SourceJob::Aggregate(_)) => {
                return Err("aggregate result before source proof".into());
            }
            Err(error) => completed[rank] = Some(Err(error)),
        }
    }
    let stats = runtime.statistics();
    report.proof_jobs = stats.submitted[0];
    report.proof_background_jobs = stats.background_started[0];
    options.cancellation.check()?;
    // A sticky runtime panic must not disappear behind an earlier proof decline.
    // The caller always invokes shutdown_for_retry on every nonadmission/error.
    // Successful admission also joins before any output is published.
    let mut domains = Vec::with_capacity(count);
    let mut rows = 0u64;
    let mut dense_bytes = 0usize;
    for receipt in completed {
        let producer = receipt.ok_or("source proof result missing")??;
        if producer.declined {
            return Ok(None);
        }
        let ProofProducer {
            rows: owner_rows,
            bounds,
            batches,
            ..
        } = producer;
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
            batches,
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
fn submit(runtime: &mut Runtime<SourceJob>, producer: ProofProducer) -> Result<JobId> {
    runtime
        .try_submit(JobKind::Morsel, move |cancel| {
            producer.step(cancel).map(SourceJob::Proof)
        })
        .map_err(|rejected| rejected.reason.to_owned())
}
#[cfg(test)]
mod tests;
