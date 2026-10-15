//! Scan-local count owners and ordered waves of complete serial probe batches.
use super::*;
use crate::parquet_scan::{BatchTransform, FilterFactory, Reader, TransformedBatch};
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT_POOL: AtomicUsize = AtomicUsize::new(1);
thread_local! {
    static OWNER: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

pub(super) struct Tables {
    pub counts: Vec<Counts>,
    // Includes the table headers and scan closure descriptors, and outlives them.
    _funding: Reservation,
}
impl Tables {
    pub fn single(counts: Counts, budget: &Budget) -> Result<Self> {
        let funding = budget.reserve(std::mem::size_of::<Counts>())?;
        Ok(Self {
            counts: vec![counts],
            _funding: funding,
        })
    }
}

// A projection/rename changes column positions, never the numeric key itself.
fn scan_column(node: &Node, column: usize) -> Option<(&Node, usize)> {
    match &node.op {
        RelOp::Scan { .. } => Some((node, column)),
        RelOp::Rename { input } | RelOp::Filter { input, .. } => scan_column(input, column),
        RelOp::Project { input, columns } => scan_column(input, *columns.get(column)?),
        _ => None,
    }
}

fn required_counts(right: &Node, columns: &[Option<usize>]) -> bool {
    columns.iter().all(|column| {
        column.is_none_or(|column| {
            scan_column(right, column).is_some_and(|(scan, column)| {
                let field = scan.schema.field(column);
                !field.is_nullable() && field.data_type() != &arrow::datatypes::DataType::Null
            })
        })
    })
}

fn expression_bytes(expr: &Expr) -> Option<usize> {
    let children = match &expr.kind {
        Kind::Binary(_, a, b) | Kind::Like(a, b, _) => {
            expression_bytes(a)?.checked_add(expression_bytes(b)?)?
        }
        Kind::Not(a) | Kind::IsNull(a, _) => expression_bytes(a)?,
        Kind::Literal(Literal::Text(text)) => text.len(),
        Kind::Column(_) | Kind::Literal(_) => 0,
        _ => return None,
    };
    children.checked_add(512)
}

fn source_bytes(node: &Node, options: &Options) -> Option<usize> {
    let own = node
        .schema
        .fields()
        .len()
        .checked_mul(512)?
        .checked_add(1024)?;
    let child = match &node.op {
        RelOp::Scan { .. } => 0,
        RelOp::Rename { input } | RelOp::Project { input, .. } => source_bytes(input, options)?,
        RelOp::Filter { predicate, input } => {
            // Keep today's selective RowFilter path, including which payload
            // pages it legitimately skips. This path moves post-decode work only.
            if options.parquet_cache_bytes.is_some_and(|bytes| bytes > 0)
                && node.estimated_rows <= input.estimated_rows / 16
                && crate::scan_predicate::total(predicate)
            {
                return None;
            }
            source_bytes(input, options)?.checked_add(expression_bytes(predicate)?)?
        }
        _ => return None,
    };
    own.checked_add(child)
}

fn source_filters(node: &Node, filters: &mut Vec<Expr>) -> Result<()> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } => source_filters(input, filters),
        RelOp::Filter { input, predicate } => {
            source_filters(input, filters)?;
            let mapping = (0..input.schema.fields().len())
                .map(|column| scan_column(input, column).map(|(_, column)| column))
                .collect::<Vec<_>>();
            let mut predicate = predicate.clone();
            predicate.remap_columns(&mapping)?;
            filters.push(predicate);
            Ok(())
        }
        _ => Ok(()),
    }
}

struct Owner {
    counts: Counts,
    rows: usize,
    selected: usize,
    bytes: usize,
    seconds: f64,
    batches: usize,
}

struct Pool {
    id: usize,
    next: AtomicUsize,
    slots: Vec<Mutex<Owner>>,
}
impl Pool {
    fn owner(&self) -> Result<std::sync::MutexGuard<'_, Owner>> {
        let index = OWNER.with(|cached| {
            let (id, index) = cached.get();
            if id == self.id {
                return index;
            }
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            cached.set((self.id, index));
            index
        });
        // Each decoder thread (including the caller) retains its own slot.
        // There is no lock wait, count-table sharing, or scheduling-dependent
        // reassignment between batches. Query IDs cannot alias a prior pool.
        self.slots
            .get(index)
            .ok_or("internal: counted scan owner unavailable")?
            .try_lock()
            .map_err(|_| "internal: counted scan owner busy".into())
    }
}

pub(super) fn build(
    right: &Node,
    keys: &[Expr],
    columns: &[Option<usize>],
    ctx: &mut Context<'_>,
) -> Result<Option<Tables>> {
    // The parallel build admits one direct INTEGER key with a cheap bounded
    // storage hint. Other key layouts retain the existing counted build.
    let [
        Expr {
            kind: Kind::Column(key),
            ty: Type::Int,
        },
    ] = keys
    else {
        return Ok(None);
    };
    let Some((scan, key)) = scan_column(right, *key) else {
        return Ok(None);
    };
    let RelOp::Scan {
        path,
        columns: physical,
        statistics,
        ..
    } = &scan.op
    else {
        unreachable!()
    };
    if !matches!(
        scan.schema.field(key).data_type(),
        arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
    ) {
        return Ok(None);
    }
    let Some((base, high)) = statistics.get(key).and_then(|s| s.bounds) else {
        return Ok(None);
    };
    let Some(len) = high
        .checked_sub(base)
        .and_then(|n| n.checked_add(1))
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
    else {
        return Ok(None);
    };
    let Some(descriptors) = source_bytes(right, ctx.options) else {
        return Ok(None);
    };
    let budget = &ctx.budget;
    let file = File::open(path).map_err(|e| e.to_string())?;
    let metadata = crate::parquet_scan::external_metadata(&file)?;
    let owners = ctx
        .options
        .workers
        .min(metadata.metadata().num_row_groups().max(1));
    let collapsed = required_counts(right, columns);
    let row_bound = metadata.metadata().file_metadata().num_rows();
    let width = if collapsed { 1 } else { columns.len() + 1 };
    let cell_bytes = if collapsed {
        Counts::compact_cell_bytes(row_bound)
    } else {
        8
    };
    // Optional storage admission, never a key-domain assumption. Bound the
    // total replicated window, not just one owner's allocation.
    let Some(bytes) = len
        .checked_mul(width)
        .and_then(|n| n.checked_mul(cell_bytes))
        .and_then(|n| n.checked_mul(owners))
        .filter(|n| *n <= (budget.limit() / 4).min(512 << 20))
    else {
        return Ok(None);
    };
    let Some(headers) = descriptors
        .checked_mul(owners + 2)
        .and_then(|n| n.checked_add(owners * (std::mem::size_of::<Owner>() + 512)))
        .and_then(|n| n.checked_add(metadata.metadata().memory_size() * 2))
    else {
        return Ok(None);
    };
    // Fund all owners before consuming any input; optional refusal is clean.
    let Ok(mut funding) = budget.reserve(bytes.saturating_add(headers)) else {
        return Ok(None);
    };
    let mut slots = Vec::with_capacity(owners);
    for _ in 0..owners {
        let lease = funding.split_off(len * width * cell_bytes)?;
        let counts = if collapsed {
            Counts::with_compact_window(base, len, row_bound, lease)?
        } else {
            Counts::with_window(width, base, len, lease)?
        };
        slots.push(Mutex::new(Owner {
            counts,
            rows: 0,
            selected: 0,
            bytes: 0,
            seconds: 0.,
            batches: 0,
        }));
    }
    let id = NEXT_POOL
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| "counted scan pool ID exhausted")?;
    let slots = Arc::new(Pool {
        id,
        next: AtomicUsize::new(0),
        slots,
    });
    let mut filters = Vec::new();
    source_filters(right, &mut filters)?;
    let columns = columns
        .iter()
        .map(|column| column.map(|column| scan_column(right, column).unwrap().1))
        .collect::<Vec<_>>();
    let keys = vec![Expr {
        kind: Kind::Column(key),
        ty: Type::Int,
    }];
    let cancellation = ctx.options.cancellation.clone();
    let transform: BatchTransform = {
        let slots = slots.clone();
        let budget = budget.clone();
        let empty_schema = Arc::new(Schema::empty());
        Arc::new(move |batch| {
            cancellation.check()?;
            let start = Instant::now();
            let bytes = batch.get_array_memory_size();
            let _decoded = budget.reserve(bytes)?;
            let _workspace = budget.reserve(descriptors)?;
            let input = input(&batch, &budget, filters.iter())?;
            let masks = filters
                .iter()
                .map(|filter| filter.eval(&input, &budget))
                .collect::<Result<Vec<_>>>()?;
            let keys = BatchKeys::new(&batch, &keys, &budget)?;
            let mut owner = slots.owner()?;
            let selected =
                owner
                    .counts
                    .add(&keys, &batch, &columns, &masks, &budget, &cancellation)?;
            owner.rows = owner
                .rows
                .checked_add(batch.num_rows())
                .ok_or("resource-limit: join build indices")?;
            owner.selected = owner
                .selected
                .checked_add(selected)
                .ok_or("resource-limit: join build indices")?;
            owner.bytes += bytes;
            owner.batches += 1;
            owner.seconds += start.elapsed().as_secs_f64();
            drop(owner);
            drop(keys);
            drop(masks);
            drop(input);
            drop(batch); // Free decoded backing before its reservation leaves.
            // Only an empty completion token crosses the reader's ordered
            // queue. Its existing source-error order and cancellation remain.
            let lease = budget.reserve(256)?;
            Ok(TransformedBatch {
                batch: RecordBatch::new_empty(empty_schema.clone()),
                lease,
                preserve_empty: true,
            })
        })
    };
    let projection = ProjectionMask::roots(metadata.parquet_schema(), physical.iter().copied());
    ctx.metrics.selected_column_compressed_bytes += metadata
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|group| group.columns().iter().enumerate())
        .filter(|(leaf, _)| projection.leaf_included(*leaf))
        .map(|(_, column)| column.compressed_size())
        .sum::<i64>();
    let factory: FilterFactory = Arc::new(|_| None);
    let start = Instant::now();
    let mut reader = Reader::with_transform(
        path,
        file,
        metadata,
        projection,
        factory,
        Some(transform),
        ctx.options,
        budget,
    )?;
    for batch in reader.by_ref() {
        ctx.options.cancellation.check()?;
        batch?;
    }
    ctx.metrics.scan_decode_seconds += start.elapsed().as_secs_f64();
    if let Some(stats) = reader.parallel_statistics() {
        ctx.metrics.parallel_scan_tasks += stats.tasks;
        ctx.metrics.parallel_scan_batches += stats.batches;
        ctx.metrics.parallel_scan_pending_batches_peak = ctx
            .metrics
            .parallel_scan_pending_batches_peak
            .max(stats.pending_peak);
        ctx.metrics.parallel_scan_worker_seconds += stats.worker_seconds;
        ctx.metrics.parallel_scan_caller_seconds += stats.caller_seconds;
        ctx.metrics.parallel_scan_caller_tasks += stats.caller_tasks;
        ctx.metrics.parallel_scan_queue_slots_max = ctx
            .metrics
            .parallel_scan_queue_slots_max
            .max(stats.queue_slots);
        ctx.metrics.parallel_scan_backpressure_seconds += stats.backpressure_seconds;
        ctx.metrics.parallel_scan_pending_bytes_peak = ctx
            .metrics
            .parallel_scan_pending_bytes_peak
            .max(stats.pending_bytes_peak);
    }
    let (buffered, cached, uncached, bytes) = reader.statistics();
    ctx.metrics.buffered_parquet_scans += buffered;
    ctx.metrics.parquet_cached_column_rows += cached;
    ctx.metrics.parquet_uncached_column_rows += uncached;
    ctx.metrics.buffered_compressed_bytes_read += bytes;
    drop(reader); // Join every callback before publishing immutable tables.
    let slots = Arc::try_unwrap(slots).map_err(|_| "internal: live counted scan owner")?;
    let mut counts = Vec::with_capacity(owners);
    let mut rows = 0usize;
    for slot in slots.slots {
        let owner = slot
            .into_inner()
            .map_err(|_| "internal: poisoned counted scan owner")?;
        rows = rows
            .checked_add(owner.selected)
            .ok_or("resource-limit: join build indices")?;
        ctx.metrics.rows_read += owner.rows;
        ctx.metrics.decoded_arrow_bytes_total += owner.bytes;
        ctx.metrics.join_seconds += owner.seconds;
        ctx.metrics.post_decode_filter_batches += owner.batches;
        ctx.metrics.post_decode_filter_input_rows += owner.rows;
        ctx.metrics.post_decode_filter_output_rows += owner.selected;
        counts.push(owner.counts);
    }
    if rows > u32::MAX as usize {
        return Err("resource-limit: join build indices".into());
    }
    ctx.metrics.join_build_rows += rows;
    Ok(Some(Tables {
        counts,
        _funding: funding,
    }))
}

pub(super) struct Probe<'a> {
    pub tables: &'a Tables,
    pub keys: &'a [Expr],
    pub groups: &'a [Expr],
    pub filters: &'a [Expr],
    pub columns: &'a [Option<usize>],
    pub descriptors: usize,
    pub budget: &'a Budget,
    pub options: &'a Options,
}
struct Weighted {
    input: Input,
    weights: Vec<Value>,
    rows: usize,
    seconds: f64,
    _workspace: Reservation,
}
impl Probe<'_> {
    /// Admit only funded scalar waves; uncertain layouts use the serial batch path.
    fn wave_limit(&self, batch: &RecordBatch) -> usize {
        let estimate = || -> Option<usize> {
            if !self.filters.is_empty()
                || !self.keys.iter().chain(self.groups).all(|e| {
                    matches!(e.kind, Kind::Column(_))
                        && matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Date | Type::Bool)
                })
            {
                return None;
            }
            // Include raw backing, normalization/selection replacement peaks,
            // weights, key encoding and worst-case new COUNT groups, not just
            // the raw Pending batch. Leave two current live sets for aggregate
            // capacity replacement and the scan's retained/current-next batch.
            // This is optional admission, not prepaid credit: every allocation
            // still reserves normally, and an unpublished resource failure is
            // replayed serially by try_execute (also covering decoder growth).
            let width = self
                .keys
                .len()
                .checked_add(self.groups.len())?
                .checked_add(self.columns.len())?;
            let per_row = width.checked_mul(128)?.checked_add(1024)?;
            let slot = batch
                .num_rows()
                .checked_mul(per_row)?
                .checked_add(batch.get_array_memory_size().checked_mul(2)?)?
                .checked_add(self.descriptors.checked_mul(4)?)?
                .checked_add(65536 + 8192)?;
            let available = self
                .budget
                .limit()
                .saturating_sub(self.budget.used().checked_mul(3)?);
            Some((available / slot).clamp(1, self.options.workers.max(1)))
        };
        estimate().unwrap_or(1)
    }

    fn run(&self, batch: &RecordBatch) -> Result<Weighted> {
        self.options.cancellation.check()?;
        let start = Instant::now();
        let width = self.columns.len() + 1;
        let bytes = width
            .checked_mul(16)
            .and_then(|n| n.checked_add(4))
            .and_then(|n| n.checked_mul(batch.num_rows()))
            .and_then(|n| n.checked_add(self.descriptors))
            .ok_or("count join batch size overflow")?;
        let workspace = self.budget.reserve(bytes)?;
        let keys = BatchKeys::new(batch, self.keys, self.budget)?;
        let mut totals = vec![0u64; batch.num_rows() * width];
        for counts in &self.tables.counts {
            for row in 0..batch.num_rows() {
                if row % 1024 == 0 {
                    self.options.cancellation.check()?;
                }
                if let Some(group) = keys.group(row, counts) {
                    for (slot, total) in totals[row * width..(row + 1) * width]
                        .iter_mut()
                        .enumerate()
                    {
                        // Nonnegative local counts sum to at most the checked
                        // u32 build cardinality; still check every addition.
                        *total = total
                            .checked_add(counts.value(group, slot))
                            .ok_or("resource-limit: join build indices")?;
                    }
                }
            }
        }
        let values = input(batch, self.budget, self.groups.iter().chain(self.filters))?;
        let masks = self
            .filters
            .iter()
            .map(|e| e.eval(&values, self.budget))
            .collect::<Result<Vec<_>>>()?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        // Weighted encounters follow probe order, including misses in place.
        // Wave::flush then delivers complete batches in that same input order.
        rows.extend((0..batch.num_rows()).filter_map(|row| {
            masks
                .iter()
                .all(|mask| {
                    !is_sql_null(mask.array.as_ref(), row) && boolean(&mask.array).value(row)
                })
                .then_some(row as u32)
        }));
        let weights = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                let array = UInt64Array::from_iter_values(rows.iter().map(|row| {
                    let row = *row as usize;
                    if column.is_none() {
                        totals[row * width].max(1)
                    } else {
                        totals[row * width + i + 1]
                    }
                }));
                Value::adopt(Arc::new(array), self.budget)
            })
            .collect::<Result<Vec<_>>>()?;
        let values = if rows.len() == batch.num_rows()
            && rows.iter().enumerate().all(|(i, row)| i == *row as usize)
        {
            values
        } else {
            select_input(values, &UInt32Array::from(rows), self.budget)?
        };
        Ok(Weighted {
            input: values,
            weights,
            rows: batch.num_rows(),
            seconds: start.elapsed().as_secs_f64(),
            _workspace: workspace,
        })
    }
}

struct Pending {
    batch: RecordBatch,
    _lease: Reservation,
}
pub(super) struct Wave {
    pending: Vec<Pending>,
    limit: usize,
    pub rows: usize,
    pub seconds: f64,
    pub tasks: usize,
    pub background: usize,
    _metadata: Reservation,
}
impl Wave {
    /// Start with exactly the serial owner's allocation, regardless of workers.
    pub fn new(_options: &Options, budget: &Budget) -> Result<Self> {
        let metadata = budget.reserve(8192)?;
        Ok(Self {
            pending: Vec::with_capacity(1),
            limit: 1,
            rows: 0,
            seconds: 0.,
            tasks: 0,
            background: 0,
            _metadata: metadata,
        })
    }
    /// Admit only the current headroom's wave, delivering predecessors first.
    pub fn push(
        &mut self,
        batch: RecordBatch,
        probe: &Probe<'_>,
        send: &mut dyn FnMut(Input, Vec<Value>) -> Result<()>,
    ) -> Result<()> {
        // Recheck at every source boundary: batch widths and the downstream
        // group's retained state can grow between waves. Flush predecessors
        // before admitting an input which no longer fits the current wave.
        let limit = probe.wave_limit(&batch);
        if !self.pending.is_empty() && self.pending.len() >= limit {
            self.flush(probe, send)?;
        }
        if self.pending.is_empty() {
            let limit = probe.wave_limit(&batch);
            let extra = (limit - 1).checked_mul(8192);
            if extra.is_some_and(|extra| self._metadata.try_grow(extra)) {
                // Fund replacement overlap before Vec grows. The 8-KiB slot
                // covers Pending, task/handle storage and the old tiny vector.
                self.pending.reserve_exact(limit);
                self.limit = limit;
            }
        } else {
            self.limit = self.limit.min(limit);
        }
        let lease = probe.budget.reserve(batch.get_array_memory_size())?;
        self.pending.push(Pending {
            batch,
            _lease: lease,
        });
        if self.pending.len() == self.limit {
            self.flush(probe, send)?;
        }
        Ok(())
    }

    /// Consume one complete batch in serial encounter/error order.
    fn deliver(
        &mut self,
        result: Result<Weighted>,
        send: &mut dyn FnMut(Input, Vec<Value>) -> Result<()>,
    ) -> Result<()> {
        let weighted = result?;
        self.rows += weighted.rows;
        self.seconds += weighted.seconds;
        send(weighted.input, weighted.weights)
    }

    /// Join and deliver each predecessor before awaiting its successors.
    pub fn flush(
        &mut self,
        probe: &Probe<'_>,
        send: &mut dyn FnMut(Input, Vec<Value>) -> Result<()>,
    ) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let count = self.pending.len();
        self.tasks += count;
        if count == 1 {
            // Exactly the former one-worker path: no handles or result queue.
            let pending = self.pending.pop().unwrap();
            let result = probe.run(&pending.batch);
            drop(pending);
            let result = self.deliver(result, send);
            self.pending = Vec::with_capacity(1);
            self.limit = 1;
            self._metadata.release_excess(8192);
            return result;
        }
        enum Task<'scope> {
            Inline(Pending),
            Running(crate::memory::Joined<std::thread::ScopedJoinHandle<'scope, Result<Weighted>>>),
            Failed(String),
        }
        let pending = std::mem::replace(&mut self.pending, Vec::with_capacity(1));
        // All input is present before any permit is acquired. Workers do only
        // this batch's probe, never recursive Node::run or upstream waiting.
        let result = std::thread::scope(|scope| -> Result<()> {
            let mut pending = pending.into_iter();
            let first = pending.next().unwrap();
            let mut tasks = Vec::with_capacity(count - 1);
            for pending in pending {
                let permit = probe.budget.workers(1, probe.options.workers - 1);
                let task = if permit.count == 0 {
                    Task::Inline(pending)
                } else {
                    self.background += 1;
                    match crate::memory::Joined::spawn_scoped(
                        scope,
                        "assaydb-count-probe",
                        permit,
                        move || {
                            let result = probe.run(&pending.batch);
                            // Capture the whole owner, not just its batch field:
                            // the source lease must survive worker execution.
                            drop(pending);
                            result
                        },
                    ) {
                        Ok(handle) => Task::Running(handle),
                        Err(error) => Task::Failed(format!("count probe worker spawn: {error}")),
                    }
                };
                tasks.push(task);
            }
            let first_result = probe.run(&first.batch);
            drop(first);
            let mut status = self.deliver(first_result, send);
            for task in tasks {
                // Even after an earlier error, join every running owner before
                // returning its funding; never let a later panic replace it.
                let result = match task {
                    Task::Inline(pending) if status.is_ok() => probe.run(&pending.batch),
                    Task::Inline(_) => continue,
                    Task::Running(handle) => handle
                        .join()
                        .unwrap_or_else(|_| Err("count probe worker panicked".into())),
                    Task::Failed(error) => Err(error),
                };
                if status.is_ok() {
                    status = self.deliver(result, send);
                }
            }
            status
        });
        self.limit = 1;
        self._metadata.release_excess(8192);
        result
    }
}

#[cfg(test)]
mod tests;
