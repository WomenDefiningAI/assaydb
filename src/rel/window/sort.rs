//! Same typed/ordinal ordering in memory and on disk. Every retained batch and
//! construction workspace is reserved before ownership/allocation changes.
use super::*;
use crate::external_sort::{ExternalSort, SortMetrics};

pub(super) enum Sorter {
    External(ExternalSort),
    Memory {
        schema: SchemaRef,
        order: Vec<bind::Sort>,
        batches: Vec<RecordBatch>,
        leases: Vec<Reservation>,
        bytes: usize,
        budget: Budget,
        cancel: crate::spill::Cancellation,
        batch_rows: usize,
        _metadata: Reservation,
    },
}

impl Sorter {
    pub(super) fn new(
        schema: SchemaRef,
        order: Vec<bind::Sort>,
        ctx: &Context<'_>,
    ) -> Result<Self> {
        if let Some(store) = &ctx.spill {
            let settings = ctx.options.spill.as_ref().unwrap();
            return Ok(Self::External(ExternalSort::new(
                schema,
                order,
                store.clone(),
                ctx.budget.clone(),
                ctx.options.cancellation.clone(),
                settings.sort_run_bytes,
                settings.merge_fan_in,
                ctx.options.batch_size,
            )?));
        }
        let metadata = ctx
            .budget
            .reserve(order.len().saturating_mul(1024).saturating_add(4096))?;
        Ok(Self::Memory {
            schema,
            order,
            batches: Vec::new(),
            leases: Vec::new(),
            bytes: 0,
            budget: ctx.budget.clone(),
            cancel: ctx.options.cancellation.clone(),
            batch_rows: ctx.options.batch_size,
            _metadata: metadata,
        })
    }

    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            Self::External(sorter) => sorter.push(batch),
            Self::Memory {
                batches,
                leases,
                bytes,
                budget,
                cancel,
                ..
            } => {
                cancel.check()?;
                let size = batch
                    .get_array_memory_size()
                    .saturating_add(batch.num_columns().saturating_mul(256))
                    .saturating_add(1024);
                let lease = budget.reserve(size)?;
                *bytes = bytes.checked_add(size).ok_or("window sort size overflow")?;
                // The per-batch metadata allowance covers both growing vectors.
                batches.push(batch);
                leases.push(lease);
                Ok(())
            }
        }
    }

    pub(super) fn finish(
        self,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<SortMetrics> {
        match self {
            Self::External(sorter) => sorter.finish(emit),
            Self::Memory {
                schema,
                order,
                batches,
                leases,
                bytes,
                budget,
                cancel,
                batch_rows,
                _metadata,
            } => {
                cancel.check()?;
                let start = Instant::now();
                let rows = batches.iter().try_fold(0_usize, |n, b| {
                    n.checked_add(b.num_rows())
                        .ok_or("window sort row count overflow")
                })?;
                if rows > u32::MAX as usize {
                    return Err(
                        "resource-limit: in-memory window sort exceeds 32-bit indices".into(),
                    );
                }
                // Input, concatenation, Arrow sorting workspace, indices and
                // one compact take coexist through downstream consumption.
                let work = bytes
                    .checked_mul(4)
                    .and_then(|n| rows.checked_mul(64).and_then(|r| n.checked_add(r)))
                    .and_then(|n| n.checked_add(4096))
                    .ok_or("window sort size overflow")?;
                let _workspace = budget.reserve(work)?;
                let batch = concat_batches(&schema, &batches)
                    .map_err(|e| crate::Error::from(e.to_string()))?;
                let columns = order
                    .iter()
                    .map(|key| SortColumn {
                        values: batch.column(key.column).clone(),
                        options: Some(SortOptions {
                            descending: key.descending,
                            nulls_first: key.nulls_first,
                        }),
                    })
                    .collect();
                let indices = crate::external_sort::sort_indices(columns, rows, &budget)?;
                drop(batches);
                drop(leases);
                for offset in (0..rows).step_by(batch_rows) {
                    cancel.check()?;
                    let selected = indices.slice(offset, batch_rows.min(rows - offset));
                    let arrays = batch
                        .columns()
                        .iter()
                        .map(|array| {
                            take(array, &selected, None)
                                .map_err(|e| crate::Error::from(e.to_string()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    emit(
                        RecordBatch::try_new(schema.clone(), arrays)
                            .map_err(|e| crate::Error::from(e.to_string()))?,
                    )?;
                }
                Ok(SortMetrics {
                    seconds: start.elapsed().as_secs_f64(),
                    ..SortMetrics::default()
                })
            }
        }
    }
}

pub(super) fn merge_metrics(metrics: &mut Metrics, sorted: SortMetrics) {
    metrics.external_sort_runs += sorted.runs;
    metrics.external_sort_merge_passes += sorted.merge_passes;
    metrics.external_sort_rows += sorted.rows;
    metrics.output_sort_seconds += sorted.seconds;
}
