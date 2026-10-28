//! Bounded-run external merge sort. SQL ordering comes from Arrow sort/row
//! kernels; input order breaks ties between runs. No query-specific dispatch.
use crate::{
    Result,
    bind::Sort,
    memory::{Budget, Reservation},
    spill::{Cancellation, SpillBatch, SpillFile, SpillStore},
};
use arrow::{
    array::UInt32Array,
    compute::{
        SortColumn, SortOptions, concat_batches, interleave_record_batch, lexsort_to_indices, take,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use std::{cmp::Ordering, collections::VecDeque, time::Instant};
#[cfg(test)]
mod top_k_tests;

/// Select a completed sort's prefix with O(k) indices. The max heap is ordered
/// by (the same Arrow typed comparator, input ordinal); no input is skipped.
/// Arrow's legacy full sort is unstable on ties. Until that separate contract
/// changes, decline if ties in/at the prefix could make its bytes differ from
/// the stable selection. Ties strictly after the prefix cannot affect it.
pub(crate) fn try_top_k_indices(
    columns: &[SortColumn],
    rows: usize,
    k: usize,
    budget: &Budget,
    cancel: &Cancellation,
) -> Result<Option<(UInt32Array, Reservation)>> {
    use arrow::{compute::kernels::sort::LexicographicalComparator, datatypes::DataType};
    cancel.check()?;
    if k == 0 || k > 4096 || k.saturating_mul(4) >= rows || rows > u32::MAX as usize {
        return Ok(None);
    }
    // Flat arrays share their payloads/null buffers when the comparator is
    // constructed. Do not admit nested/dictionary types with hidden workspace.
    if columns.iter().any(|column| {
        column.values.len() != rows
            || !matches!(
                column.values.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Date32
                    | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
                    | DataType::Utf8
                    | DataType::Binary
            )
    }) {
        return Ok(None);
    }
    let Some(bytes) = columns
        .len()
        .checked_mul(1024)
        .and_then(|n| k.checked_mul(16).and_then(|k| n.checked_add(k)))
        .and_then(|n| n.checked_add(1024))
    else {
        return Ok(None);
    };
    // Reservation denial is an optimization miss. The existing full-sort
    // workspace remains available, and no partial output has escaped.
    let Ok(credit) = budget.reserve(bytes) else {
        return Ok(None);
    };
    let columns = columns
        .iter()
        .filter(|column| column.values.data_type() != &DataType::Null)
        .cloned()
        .collect::<Vec<_>>();
    let Ok((columns, _float_credit)) = canonical_columns(columns, budget) else {
        return Ok(None);
    };
    let comparator = LexicographicalComparator::try_new(&columns)
        .map_err(|e| crate::Error::from(e.to_string()))?;
    let compare = |a: u32, b: u32| {
        comparator
            .compare(a as usize, b as usize)
            .then_with(|| a.cmp(&b))
    };
    let mut heap = Vec::<u32>::new();
    if heap.try_reserve_exact(k).is_err() {
        return Ok(None);
    }
    let mut best_discarded: Option<u32> = None;
    for row in 0..rows as u32 {
        #[cfg(test)]
        top_k_tests::checkpoint(row, cancel);
        if row % 1024 == 0 {
            cancel.check()?;
        }
        if heap.len() < k {
            heap.push(row);
            let mut child = heap.len() - 1;
            while child > 0 {
                let parent = (child - 1) / 2;
                if compare(heap[parent], heap[child]) != Ordering::Less {
                    break;
                }
                heap.swap(parent, child);
                child = parent;
            }
        } else {
            let discarded = if compare(row, heap[0]) == Ordering::Less {
                let discarded = std::mem::replace(&mut heap[0], row);
                sift_down(&mut heap, &compare);
                discarded
            } else {
                row
            };
            if best_discarded.is_none_or(|old| compare(discarded, old) == Ordering::Less) {
                best_discarded = Some(discarded);
            }
        }
    }
    heap.sort_unstable_by(|a, b| compare(*a, *b));
    if heap
        .windows(2)
        .any(|pair| comparator.compare(pair[0] as usize, pair[1] as usize) == Ordering::Equal)
        || best_discarded.is_some_and(|discarded| {
            comparator.compare(heap[k - 1] as usize, discarded as usize) == Ordering::Equal
        })
    {
        return Ok(None);
    }
    cancel.check()?;
    // Charge both the index vector and Arrow's conversion/replacement peak.
    Ok(Some((UInt32Array::from(heap), credit)))
}

/// Restores a max heap after replacing its root; length/capacity never change.
fn sift_down(heap: &mut [u32], compare: &impl Fn(u32, u32) -> Ordering) {
    let mut parent = 0;
    loop {
        let left = parent * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child = if right < heap.len() && compare(heap[left], heap[right]) == Ordering::Less {
            right
        } else {
            left
        };
        if compare(heap[parent], heap[child]) != Ordering::Less {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
}

/// Arrow's comparator does not order NullArray. A statically NULL sort key
/// ties every row, so omit it without changing any remaining key's priority.
/// Consume the already funded descriptors; the caller's existing sort workspace
/// covers the indices, including the all-tied identity permutation.
pub(crate) fn sort_indices(
    columns: Vec<SortColumn>,
    rows: usize,
    budget: &Budget,
) -> Result<UInt32Array> {
    let (mut columns, _float_credit) = canonical_columns(columns, budget)?;
    columns.retain(|column| column.values.data_type() != &arrow::datatypes::DataType::Null);
    if columns.is_empty() {
        let rows =
            u32::try_from(rows).map_err(|_| "resource-limit: sort exceeds 32-bit indices")?;
        Ok(UInt32Array::from_iter_values(0..rows))
    } else {
        Ok(lexsort_to_indices(&columns, None).map_err(|e| crate::Error::from(e.to_string()))?)
    }
}

/// Reserve before copying; projections keep their original floating bits.
fn canonical_columns(
    mut columns: Vec<SortColumn>,
    budget: &Budget,
) -> Result<(Vec<SortColumn>, Option<Reservation>)> {
    let bytes = columns.iter().try_fold(0usize, |bytes, column| {
        bytes
            .checked_add(crate::expr::numeric_compare::canonical_bytes(
                &column.values,
            )?)
            .ok_or_else(|| "float sort key size overflow".to_string())
    })?;
    let credit = (bytes != 0).then(|| budget.reserve(bytes)).transpose()?;
    if credit.is_some() {
        for column in &mut columns {
            column.values = crate::expr::numeric_compare::canonical_array(&column.values);
        }
    }
    Ok((columns, credit))
}

#[derive(Default)]
pub(crate) struct SortMetrics {
    pub runs: usize,
    pub merge_passes: usize,
    pub rows: usize,
    pub seconds: f64,
}
struct Run(VecDeque<SpillFile>);
struct Cursor {
    files: VecDeque<SpillFile>,
    batch: Option<SpillBatch>,
    rows: Option<Rows>,
    row: usize,
    keys_lease: Option<Reservation>,
}
impl Cursor {
    fn load(&mut self, converter: &RowConverter, order: &[Sort], budget: &Budget) -> Result<()> {
        self.batch = None;
        self.rows = None;
        self.keys_lease = None;
        self.row = 0;
        if let Some(file) = self.files.pop_front() {
            let batch = file.read_batch()?;
            let lease = budget.reserve(workspace(
                batch.batch.get_array_memory_size(),
                batch.batch.num_rows(),
            )?)?;
            // Repeated ORDER BY expressions can copy the same physical column
            // several times. Fund every private copy before encoding merge keys.
            let float_bytes = order.iter().try_fold(0usize, |bytes, s| {
                bytes
                    .checked_add(crate::expr::numeric_compare::canonical_bytes(
                        batch.batch.column(s.column),
                    )?)
                    .ok_or_else(|| "float merge key size overflow".to_string())
            })?;
            let _float_credit = (float_bytes != 0)
                .then(|| budget.reserve(float_bytes))
                .transpose()?;
            let columns = order
                .iter()
                .map(|s| {
                    crate::expr::numeric_compare::canonical_array(batch.batch.column(s.column))
                })
                .collect::<Vec<_>>();
            let rows = converter
                .convert_columns(&columns)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            self.batch = Some(batch);
            self.rows = Some(rows);
            self.keys_lease = Some(lease);
        }
        Ok(())
    }
}
fn workspace(bytes: usize, rows: usize) -> Result<usize> {
    bytes
        .checked_mul(4)
        .and_then(|n| rows.checked_mul(64).and_then(|r| n.checked_add(r)))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| "external sort workspace overflow".into())
}
struct BufferedOutput<'a> {
    schema: SchemaRef,
    budget: Budget,
    emit: &'a mut dyn FnMut(RecordBatch) -> Result<()>,
    batches: Vec<RecordBatch>,
    leases: Vec<Reservation>,
    rows: usize,
    target: usize,
}
impl BufferedOutput<'_> {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.leases.push(
            self.budget.reserve(
                batch
                    .get_array_memory_size()
                    .checked_add(256)
                    .ok_or("merge buffer overflow")?,
            )?,
        );
        self.rows += batch.num_rows();
        self.batches.push(batch);
        if self.rows == self.target {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let bytes = self
            .batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .try_fold(0_usize, |a, b| {
                a.checked_add(b).ok_or("merge buffer overflow")
            })?;
        let _lease = self.budget.reserve(workspace(bytes, self.rows)?)?;
        let batch = concat_batches(&self.schema, &self.batches)
            .map_err(|e| crate::Error::from(e.to_string()))?;
        (self.emit)(batch)?;
        self.batches.clear();
        self.leases.clear();
        self.rows = 0;
        Ok(())
    }
}

pub(crate) struct ExternalSort {
    schema: SchemaRef,
    order: Vec<Sort>,
    store: SpillStore,
    budget: Budget,
    cancel: Cancellation,
    run_bytes: usize,
    fan_in: usize,
    batch_rows: usize,
    replay_bytes: usize,
    pending: Vec<RecordBatch>,
    leases: Vec<Reservation>,
    pending_bytes: usize,
    runs: Vec<Run>,
    metrics: SortMetrics,
    _metadata: Reservation,
}
impl ExternalSort {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        order: Vec<Sort>,
        store: SpillStore,
        budget: Budget,
        cancel: Cancellation,
        run_bytes: usize,
        fan_in: usize,
        batch_rows: usize,
    ) -> Result<Self> {
        if run_bytes == 0
            || !(2..=32).contains(&fan_in)
            || !(1..=131072).contains(&batch_rows)
            || order.is_empty()
            || order.iter().any(|s| s.column >= schema.fields().len())
        {
            return Err("invalid external sort settings".into());
        }
        let metadata = budget.reserve(4096 + order.len() * 128)?;
        // Leave room for simultaneous replay, encoded keys, output, codec
        // scratch and downstream reservations. Row count alone is insufficient
        // for wide strings at low budgets.
        let run_bytes = run_bytes.min((budget.limit() / 16).max(1));
        let replay_bytes = (budget.limit() / (32 * (fan_in + 2))).max(1);
        Ok(Self {
            schema,
            order,
            store,
            budget,
            cancel,
            run_bytes,
            fan_in,
            batch_rows,
            replay_bytes,
            pending: Vec::new(),
            leases: Vec::new(),
            pending_bytes: 0,
            runs: Vec::new(),
            metrics: SortMetrics::default(),
            _metadata: metadata,
        })
    }
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.cancel.check()?;
        if batch.schema() != self.schema {
            return Err("external sort schema mismatch".into());
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let start = Instant::now();
        let bytes = batch
            .get_array_memory_size()
            .checked_add(256)
            .ok_or("sort size overflow")?;
        let row_bytes = bytes.div_ceil(batch.num_rows()).max(1);
        self.batch_rows = self.batch_rows.min((self.replay_bytes / row_bytes).max(1));
        if !self.pending.is_empty() && self.pending_bytes.saturating_add(bytes) > self.run_bytes {
            self.flush()?;
        }
        // Oversized individual input batches are sorted alone, not silently
        // sliced while retaining an unaccounted parent buffer. Reservations
        // still fail explicitly if one batch cannot fit the query budget.
        self.leases.push(self.budget.reserve(bytes)?);
        self.pending_bytes = self
            .pending_bytes
            .checked_add(bytes)
            .ok_or("sort size overflow")?;
        self.metrics.rows += batch.num_rows();
        self.pending.push(batch);
        if self.pending_bytes >= self.run_bytes {
            self.flush()?;
        }
        self.metrics.seconds += start.elapsed().as_secs_f64();
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.cancel.check()?;
        let rows = self
            .pending
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        if rows > u32::MAX as usize {
            return Err("resource-limit: sort run exceeds 32-bit indices".into());
        }
        let _workspace = self.budget.reserve(workspace(self.pending_bytes, rows)?)?;
        let combined = concat_batches(&self.schema, &self.pending)
            .map_err(|e| crate::Error::from(e.to_string()))?;
        let columns = self
            .order
            .iter()
            .map(|s| SortColumn {
                values: combined.column(s.column).clone(),
                options: Some(SortOptions {
                    descending: s.descending,
                    nulls_first: s.nulls_first,
                }),
            })
            .collect::<Vec<_>>();
        let indices = sort_indices(columns, rows, &self.budget)?;
        let mut files = VecDeque::new();
        // Take directly from the combined run into compact replay batches.
        // Stored memory estimates must not retain an entire sliced parent.
        let mut offset = 0;
        while offset < rows {
            self.cancel.check()?;
            let mut count = self.batch_rows.min(rows - offset);
            let batch = loop {
                let selected: UInt32Array = indices.slice(offset, count);
                let arrays = combined
                    .columns()
                    .iter()
                    .map(|a| {
                        take(a, &selected, None).map_err(|e| crate::Error::from(e.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let batch = RecordBatch::try_new(self.schema.clone(), arrays)
                    .map_err(|e| crate::Error::from(e.to_string()))?;
                if batch.get_array_memory_size() <= self.replay_bytes || count == 1 {
                    break batch;
                }
                count = count.div_ceil(2);
            };
            files.push_back(self.store.write_batch(&batch)?);
            offset += count;
        }
        self.runs.push(Run(files));
        self.metrics.runs += 1;
        self.pending.clear();
        self.leases.clear();
        self.pending_bytes = 0;
        Ok(())
    }
    fn merge(&self, runs: Vec<Run>, emit: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        self.cancel.check()?;
        let _metadata = self
            .budget
            .reserve(4096 + self.batch_rows * 32 + self.fan_in * 256)?;
        let converter = RowConverter::new(
            self.order
                .iter()
                .map(|s| {
                    SortField::new_with_options(
                        self.schema.field(s.column).data_type().clone(),
                        SortOptions {
                            descending: s.descending,
                            nulls_first: s.nulls_first,
                        },
                    )
                })
                .collect(),
        )
        .map_err(|e| crate::Error::from(e.to_string()))?;
        let mut cursors = runs
            .into_iter()
            .map(|run| Cursor {
                files: run.0,
                batch: None,
                rows: None,
                row: 0,
                keys_lease: None,
            })
            .collect::<Vec<_>>();
        for cursor in &mut cursors {
            cursor.load(&converter, &self.order, &self.budget)?;
        }
        let mut selected = Vec::with_capacity(self.batch_rows);
        // Coalesce compact fragments before writing a new run. Otherwise every
        // exhausted source could create another tiny file at every merge pass.
        let mut output = BufferedOutput {
            schema: self.schema.clone(),
            budget: self.budget.clone(),
            emit,
            batches: Vec::new(),
            leases: Vec::new(),
            rows: 0,
            target: self.batch_rows,
        };
        loop {
            self.cancel.check()?;
            let winner = (0..cursors.len())
                .filter(|i| cursors[*i].batch.is_some())
                .min_by(|a, b| {
                    let ca = &cursors[*a];
                    let cb = &cursors[*b];
                    ca.rows
                        .as_ref()
                        .unwrap()
                        .row(ca.row)
                        .cmp(&cb.rows.as_ref().unwrap().row(cb.row))
                        .then_with(|| a.cmp(b))
                });
            let Some(winner) = winner else {
                break;
            };
            let cursor = &mut cursors[winner];
            selected.push((winner, cursor.row));
            cursor.row += 1;
            let exhausted = cursor.row == cursor.batch.as_ref().unwrap().batch.num_rows();
            // Flush before replacing any selected source buffer, keeping at
            // most one replay batch per run live regardless of output order.
            if selected.len() == self.batch_rows - output.rows || exhausted {
                let empty = RecordBatch::new_empty(self.schema.clone());
                let batches = cursors
                    .iter()
                    .map(|c| c.batch.as_ref().map(|b| &b.batch).unwrap_or(&empty))
                    .collect::<Vec<_>>();
                let bytes = batches
                    .iter()
                    .map(|b| b.get_array_memory_size())
                    .try_fold(0_usize, |a, b| {
                        a.checked_add(b).ok_or("merge output size overflow")
                    })?;
                let _output = self.budget.reserve(workspace(bytes, selected.len())?)?;
                let batch = interleave_record_batch(&batches, &selected)
                    .map_err(|e| crate::Error::from(e.to_string()))?;
                output.push(batch)?;
                selected.clear();
            }
            if exhausted {
                cursors[winner].load(&converter, &self.order, &self.budget)?;
            }
        }
        debug_assert!(selected.is_empty());
        output.flush()
    }
    pub fn finish(
        mut self,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<SortMetrics> {
        let start = Instant::now();
        self.flush()?;
        while self.runs.len() > self.fan_in {
            let old = std::mem::take(&mut self.runs);
            let mut iter = old.into_iter();
            let mut next = Vec::new();
            loop {
                let group = iter.by_ref().take(self.fan_in).collect::<Vec<_>>();
                if group.is_empty() {
                    break;
                }
                let mut files = VecDeque::new();
                self.merge(group, &mut |batch| {
                    files.push_back(self.store.write_batch(&batch)?);
                    Ok(())
                })?;
                next.push(Run(files));
            }
            self.runs = next;
            self.metrics.merge_passes += 1;
        }
        let runs = std::mem::take(&mut self.runs);
        if !runs.is_empty() {
            self.merge(runs, emit)?;
            self.metrics.merge_passes += 1;
        }
        self.metrics.seconds += start.elapsed().as_secs_f64();
        Ok(self.metrics)
    }
}
