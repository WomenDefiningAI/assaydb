//! Completed peers/partitions are buffered by the existing spillable sorter.
//! ROWS frames emit one finalized value per row; RANGE finalizes only at peer end.
use super::*;
use crate::{execute::WindowAccumulator, expr::Value};

struct Pending {
    sorter: Sorter,
}

impl Pending {
    fn new(schema: SchemaRef, ordinal: usize, ctx: &Context<'_>) -> Result<Self> {
        Ok(Self {
            sorter: Sorter::new(
                schema,
                vec![bind::Sort {
                    column: ordinal,
                    descending: false,
                    nulls_first: false,
                }],
                ctx,
            )?,
        })
    }

    fn finish(
        self,
        value: &ArrayRef,
        restore: &mut Sorter,
        schema: &SchemaRef,
        width: usize,
        ctx: &mut Context<'_>,
    ) -> Result<()> {
        let metrics = self.sorter.finish(&mut |batch| {
            ctx.options.cancellation.check()?;
            let _credit = ctx.budget.reserve(
                batch
                    .num_rows()
                    .saturating_mul(value.get_array_memory_size().saturating_add(32))
                    .saturating_add((width + 2) * 1024),
            )?;
            let indices = UInt64Array::from(vec![0; batch.num_rows()]);
            let result =
                take(value, &indices, None).map_err(|e| crate::Error::from(e.to_string()))?;
            push_result(&batch, result, restore, schema, width)
        })?;
        sort::merge_metrics(&mut ctx.metrics, metrics);
        Ok(())
    }
}

fn push_result(
    batch: &RecordBatch,
    result: ArrayRef,
    restore: &mut Sorter,
    schema: &SchemaRef,
    width: usize,
) -> Result<()> {
    let mut arrays = batch.columns()[..width].to_vec();
    arrays.push(result);
    arrays.push(batch.column(batch.num_columns() - 1).clone());
    restore.push(
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| crate::Error::from(e.to_string()))?,
    )
}

fn update(
    state: &mut WindowAccumulator,
    aggregate: &bind::Agg,
    batch: &RecordBatch,
    start: usize,
    len: usize,
    argument: usize,
    budget: &Budget,
) -> Result<()> {
    let value = aggregate
        .input
        .as_ref()
        .map(|_| Value::adopt(batch.column(argument).slice(start, len), budget))
        .transpose()?;
    state.update(aggregate, value.as_ref(), len)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    node: &Node,
    spec: &Specification,
    aggregate: &bind::Agg,
    sorter: Sorter,
    mut restore: Sorter,
    keyed_schema: SchemaRef,
    restore_schema: SchemaRef,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let width = node.schema.fields().len() - 1;
    let argument = width + spec.partition.len() + spec.order.len();
    let ordinal = keyed_schema.fields().len() - 1;
    let budget = ctx.budget.clone();
    let mut state = WindowAccumulator::new(aggregate, &budget)?;
    let mut previous: Option<(RecordBatch, Reservation)> = None;
    let mut pending: Option<Pending> = None;
    let metrics = sorter.finish(&mut |batch| {
        ctx.options.cancellation.check()?;
        let _metadata = budget.reserve(
            batch
                .num_rows()
                .saturating_mul(64)
                .saturating_add(batch.num_columns() * 1024),
        )?;
        let mut results = Vec::new();
        let mut credits = Vec::new();
        let mut start = 0;
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                ctx.options.cancellation.check()?;
            }
            let old = if row == 0 {
                previous.as_ref().map(|(b, _)| (b, 0))
            } else {
                Some((&batch, row - 1))
            };
            let same_partition = old.is_some_and(|(old, i)| {
                equal_keys(old, i, &batch, row, width, spec.partition.len())
            });
            let peer = same_partition
                && old.is_some_and(|(old, i)| {
                    equal_keys(
                        old,
                        i,
                        &batch,
                        row,
                        width + spec.partition.len(),
                        spec.order.len(),
                    )
                });
            if spec.frame == Frame::Rows {
                if !same_partition {
                    state = WindowAccumulator::new(aggregate, &budget)?;
                }
                update(&mut state, aggregate, &batch, row, 1, argument, &budget)?;
                let (value, credit) = state.finish(aggregate)?;
                results.push(value);
                credits.push(credit);
            } else if !same_partition || (spec.frame == Frame::Range && !peer) {
                if row > start {
                    update(
                        &mut state,
                        aggregate,
                        &batch,
                        start,
                        row - start,
                        argument,
                        &budget,
                    )?;
                    pending
                        .as_mut()
                        .unwrap()
                        .sorter
                        .push(batch.slice(start, row - start))?;
                }
                if let Some(old) = pending.take() {
                    let (value, _credit) = state.finish(aggregate)?;
                    old.finish(&value, &mut restore, &restore_schema, width, ctx)?;
                }
                if !same_partition {
                    state = WindowAccumulator::new(aggregate, &budget)?;
                }
                pending = Some(Pending::new(keyed_schema.clone(), ordinal, ctx)?);
                start = row;
            }
        }
        if spec.frame == Frame::Rows && !results.is_empty() {
            let bytes = results.iter().fold(0_usize, |sum, array| {
                sum.saturating_add(array.get_array_memory_size())
            });
            let _credit = budget.reserve(bytes.saturating_mul(2).saturating_add(1024))?;
            let refs = results.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let result =
                arrow::compute::concat(&refs).map_err(|e| crate::Error::from(e.to_string()))?;
            push_result(&batch, result, &mut restore, &restore_schema, width)?;
        } else if start < batch.num_rows() {
            update(
                &mut state,
                aggregate,
                &batch,
                start,
                batch.num_rows() - start,
                argument,
                &budget,
            )?;
            pending
                .as_mut()
                .unwrap()
                .sorter
                .push(batch.slice(start, batch.num_rows() - start))?;
        }
        if batch.num_rows() != 0 {
            // A compact final key row avoids retaining a complete sort run.
            let mut credit = budget.reserve(
                batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(batch.num_columns() * 1024),
            )?;
            let indices = UInt64Array::from(vec![(batch.num_rows() - 1) as u64]);
            let arrays = batch
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| crate::Error::from(e.to_string())))
                .collect::<Result<Vec<_>>>()?;
            let last = RecordBatch::try_new(keyed_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            credit.release_excess(
                last.get_array_memory_size()
                    .saturating_add(batch.num_columns() * 256),
            );
            previous = Some((last, credit));
        }
        Ok(())
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    drop(previous);
    if let Some(pending) = pending {
        let (value, _credit) = state.finish(aggregate)?;
        pending.finish(&value, &mut restore, &restore_schema, width, ctx)?;
    }
    drop(state);
    let metrics = restore.finish(&mut |batch| {
        ctx.options.cancellation.check()?;
        let _metadata = budget.reserve((width + 1) * 256 + 1024)?;
        emit(
            RecordBatch::try_new(node.schema.clone(), batch.columns()[..width + 1].to_vec())
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}
