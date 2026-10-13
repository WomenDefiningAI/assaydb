//! Partitioned hash joins with a bounded block fallback for unsplittable keys.
use crate::{
    Result,
    execute::{Metrics, Options},
    expr::{Expr, Input, Value, boolean, is_sql_null, normalize_input},
    keys::Keys,
    memory::{Budget, Reservation},
    rel::JoinKind,
    spill::{SpillFile, SpillStore},
    spill_partition::{FANOUT, Partition, PartitionWriter},
    types::Type,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, StringArray, UInt32Array},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use std::{collections::HashMap, sync::Arc, time::Instant};

#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Rows(JoinKind),
    Mark { negated: bool },
}

pub(crate) struct Partitioned {
    parts: [Partition; FANOUT],
    payload: SchemaRef,
    schema: SchemaRef,
    keys: usize,
    rows: usize,
    has_null: bool,
}
pub(crate) struct Side<'a> {
    writer: PartitionWriter<'a>,
    payload: SchemaRef,
    schema: SchemaRef,
    keys: Vec<Expr>,
    budget: &'a Budget,
    options: &'a Options,
    rows: usize,
    has_null: bool,
}
impl<'a> Side<'a> {
    pub fn new(
        payload: SchemaRef,
        keys: Vec<Expr>,
        store: &'a SpillStore,
        budget: &'a Budget,
        options: &'a Options,
    ) -> Result<Self> {
        let mut fields = keys
            .iter()
            .enumerate()
            .map(|(i, e)| {
                Field::new(
                    format!("spill_join_key_{i}"),
                    if e.ty == Type::Int {
                        DataType::Decimal128(38, 0)
                    } else {
                        e.ty.arrow()
                    },
                    true,
                )
            })
            .collect::<Vec<_>>();
        fields.extend(payload.fields().iter().map(|f| f.as_ref().clone()));
        Ok(Self {
            writer: PartitionWriter::new(store, budget, options, keys.len(), 0)?,
            payload,
            schema: Arc::new(Schema::new(fields)),
            keys,
            budget,
            options,
            rows: 0,
            has_null: false,
        })
    }
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.options.cancellation.check()?;
        let values = evaluate(&batch, &self.keys, self.budget)?;
        if values.len() == 1 {
            self.has_null |=
                (0..batch.num_rows()).any(|i| is_sql_null(values[0].array.as_ref(), i));
        }
        let arrays = values
            .iter()
            .map(|v| v.array.clone())
            .chain(
                batch
                    .columns()
                    .iter()
                    .take(self.payload.fields().len())
                    .cloned(),
            )
            .collect();
        let projected = make_batch(self.schema.clone(), arrays, batch.num_rows())?;
        let _lease = self.budget.reserve(projected.get_array_memory_size())?;
        self.rows = self
            .rows
            .checked_add(batch.num_rows())
            .ok_or("join input rows overflow")?;
        self.writer.push(&projected)
    }
    pub fn finish(self) -> Result<Partitioned> {
        Ok(Partitioned {
            parts: self.writer.finish()?,
            payload: self.payload,
            schema: self.schema,
            keys: self.keys.len(),
            rows: self.rows,
            has_null: self.has_null,
        })
    }
}
fn evaluate(batch: &RecordBatch, expressions: &[Expr], budget: &Budget) -> Result<Vec<Value>> {
    let mut needed = std::collections::BTreeSet::new();
    for e in expressions {
        e.columns(&mut needed);
    }
    let mut input = Input {
        columns: vec![None; batch.num_columns()],
        len: batch.num_rows(),
    };
    for i in needed {
        input.columns[i] = Some(normalize_input(
            batch.column(i),
            &Type::from_field(batch.schema().field(i))?,
            budget,
        )?);
    }
    expressions.iter().map(|e| e.eval(&input, budget)).collect()
}
fn make_batch(schema: SchemaRef, arrays: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| e.to_string())
}
fn encode(batch: &RecordBatch, keys: usize, budget: &Budget, optimized: bool) -> Result<Keys> {
    let values = batch.columns()[..keys]
        .iter()
        .map(|a| Value::adopt(a.clone(), budget))
        .collect::<Result<Vec<_>>>()?;
    Keys::with_text_encoding(&values, batch.num_rows(), budget, optimized)
}
struct HeldBatch {
    batch: RecordBatch,
    _lease: Reservation,
}
impl HeldBatch {
    fn read(file: &SpillFile, budget: &Budget) -> Result<Self> {
        let replay = file.read_batch()?;
        let lease = budget.reserve(
            replay
                .batch
                .get_array_memory_size()
                .checked_add(256)
                .ok_or("join replay retention overflow")?,
        )?;
        Ok(Self {
            batch: replay.batch.clone(),
            _lease: lease,
        })
    }
}
struct Build {
    held: HeldBatch,
    map: HashMap<Box<[u8]>, u32, ahash::RandomState>,
    next: Vec<u32>,
    _index: Reservation,
}
impl Build {
    fn new(held: HeldBatch, keys: usize, budget: &Budget, options: &Options) -> Result<Self> {
        let rows = held.batch.num_rows();
        if rows > u32::MAX as usize {
            return Err("resource-limit: join build indices".into());
        }
        let encoded = encode(&held.batch, keys, budget, options.relational_optimizations)?;
        let key_bytes = (0..rows).try_fold(0_usize, |n, row| {
            n.checked_add(encoded.row(row).len())
                .ok_or("join key bytes overflow")
        })?;
        let lease = budget.reserve(
            rows.checked_mul(128)
                .and_then(|n| key_bytes.checked_mul(2).and_then(|k| n.checked_add(k)))
                .and_then(|n| n.checked_add(4096))
                .ok_or("join index allocation overflow")?,
        )?;
        let mut map = HashMap::with_capacity_and_hasher(rows, ahash::RandomState::new());
        let mut next = vec![u32::MAX; rows];
        for (row, link) in next.iter_mut().enumerate() {
            if row % 8192 == 0 {
                options.cancellation.check()?;
            }
            if !encoded.valid(row) {
                continue;
            }
            if let Some(prior) = map.insert(Box::<[u8]>::from(encoded.row(row)), row as u32) {
                *link = prior;
            }
        }
        Ok(Self {
            held,
            map,
            next,
            _index: lease,
        })
    }
}
struct Join<'a> {
    left: SchemaRef,
    right: SchemaRef,
    right_stored: SchemaRef,
    output: SchemaRef,
    keys: usize,
    mode: Mode,
    right_empty: bool,
    right_has_null: bool,
    residual: Option<&'a Expr>,
    options: &'a Options,
    budget: Budget,
    store: SpillStore,
    emit: &'a mut dyn FnMut(RecordBatch) -> Result<()>,
    metrics: Metrics,
}
impl Join<'_> {
    fn build(&self, partition: &Partition) -> Result<Build> {
        // Keep probe/output headroom while deciding whether the build fits.
        // A failed build has emitted nothing and can safely be repartitioned.
        let available = self.budget.limit().saturating_sub(self.budget.used());
        let _headroom = self.budget.reserve(available / 2)?;
        let mut held = Vec::new();
        for file in &partition.files {
            self.options.cancellation.check()?;
            held.push(HeldBatch::read(file, &self.budget)?);
        }
        let bytes = held.iter().try_fold(4096_usize, |n, b| {
            n.checked_add(b.batch.get_array_memory_size())
                .ok_or("join concat overflow")
        })?;
        let lease = self.budget.reserve(bytes)?;
        let batch = if self.right_stored.fields().is_empty() {
            make_batch(self.right_stored.clone(), vec![], partition.rows)?
        } else {
            concat_batches(&self.right_stored, held.iter().map(|h| &h.batch))
                .map_err(|e| e.to_string())?
        };
        drop(held);
        Build::new(
            HeldBatch {
                batch,
                _lease: lease,
            },
            self.keys,
            &self.budget,
            self.options,
        )
    }
    fn partition(&mut self, mut left: Partition, mut right: Partition, depth: usize) -> Result<()> {
        self.options.cancellation.check()?;
        self.metrics.external_join_partitions += 1;
        if left.rows == 0 {
            return Ok(());
        }
        match self.build(&right) {
            Ok(build) => {
                drop(right);
                while let Some(file) = left.files.pop_front() {
                    let probe = HeldBatch::read(&file, &self.budget)?;
                    let _matched = self.budget.reserve(
                        probe
                            .batch
                            .num_rows()
                            .checked_add(256)
                            .ok_or("join matches overflow")?,
                    )?;
                    let mut matched = vec![false; probe.batch.num_rows()];
                    self.probe(&probe.batch, &build, &mut matched)
                        .map_err(|e| format!("{e}; external join probe"))?;
                    self.finish_probe(&probe.batch, &matched)
                        .map_err(|e| format!("{e}; external join unmatched/mark output"))?;
                }
                Ok(())
            }
            Err(error) if error.starts_with("resource-limit: accounted memory") => {
                if depth < 31 && right.mixed_hashes {
                    let mut lw = PartitionWriter::new(
                        &self.store,
                        &self.budget,
                        self.options,
                        self.keys,
                        depth + 1,
                    )?;
                    while let Some(file) = left.files.pop_front() {
                        let b = file.read_batch()?;
                        lw.push(&b.batch)?;
                    }
                    let lp = lw.finish()?;
                    let mut rw = PartitionWriter::new(
                        &self.store,
                        &self.budget,
                        self.options,
                        self.keys,
                        depth + 1,
                    )?;
                    while let Some(file) = right.files.pop_front() {
                        let b = file.read_batch()?;
                        rw.push(&b.batch)?;
                    }
                    let rp = rw.finish()?;
                    self.metrics.external_join_repartitions += 1;
                    for (l, r) in lp.into_iter().zip(rp) {
                        self.partition(l, r, depth + 1)?;
                    }
                    Ok(())
                } else {
                    self.metrics.external_join_block_fallbacks += 1;
                    // Match bits live for one probe batch across all build blocks.
                    while let Some(file) = left.files.pop_front() {
                        let probe = HeldBatch::read(&file, &self.budget)?;
                        let _matched = self.budget.reserve(
                            probe
                                .batch
                                .num_rows()
                                .checked_add(256)
                                .ok_or("join matches overflow")?,
                        )?;
                        let mut matched = vec![false; probe.batch.num_rows()];
                        for file in &right.files {
                            self.options.cancellation.check()?;
                            let build = Build::new(
                                HeldBatch::read(file, &self.budget)?,
                                self.keys,
                                &self.budget,
                                self.options,
                            )?;
                            self.probe(&probe.batch, &build, &mut matched)?;
                        }
                        self.finish_probe(&probe.batch, &matched)?;
                    }
                    Ok(())
                }
            }
            Err(error) => Err(error),
        }
    }
    fn payload(&self, batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
        make_batch(
            schema.clone(),
            batch.columns()[self.keys..].to_vec(),
            batch.num_rows(),
        )
    }
    fn probe(&mut self, stored: &RecordBatch, build: &Build, matched: &mut [bool]) -> Result<()> {
        self.options.cancellation.check()?;
        let encoded = encode(
            stored,
            self.keys,
            &self.budget,
            self.options.relational_optimizations,
        )?;
        let left = self.payload(stored, &self.left)?;
        let right = self.payload(&build.held.batch, &self.right)?;
        let available = self.budget.limit().saturating_sub(self.budget.used());
        let width = max_row_bytes(&left)?
            .checked_add(max_row_bytes(&right)?)
            .ok_or("join row width overflow")?;
        let rows = self
            .options
            .batch_size
            .min((available / 4 / width.saturating_mul(8).max(1)).max(1));
        let _indices = self.budget.reserve(
            rows.checked_mul(32)
                .and_then(|n| n.checked_add(1024))
                .ok_or("join indices overflow")?,
        )?;
        let mut li = Vec::with_capacity(rows);
        let mut ri = Vec::with_capacity(rows);
        let existence = matches!(
            self.mode,
            Mode::Mark { .. } | Mode::Rows(JoinKind::Semi | JoinKind::Anti)
        );
        for row in 0..stored.num_rows() {
            if row % 8192 == 0 {
                self.options.cancellation.check()?;
            }
            if !encoded.valid(row) || (existence && self.residual.is_none() && matched[row]) {
                continue;
            }
            if let Some(first) = build.map.get(encoded.row(row)) {
                let mut r = *first;
                while r != u32::MAX {
                    li.push(row as u32);
                    ri.push(r);
                    if li.len() == rows {
                        self.candidates(&left, &right, &mut li, &mut ri, matched)?;
                    }
                    if existence && self.residual.is_none() {
                        break;
                    }
                    r = build.next[r as usize];
                }
            }
        }
        self.candidates(&left, &right, &mut li, &mut ri, matched)
    }
    fn candidates(
        &mut self,
        left: &RecordBatch,
        right: &RecordBatch,
        li: &mut Vec<u32>,
        ri: &mut Vec<u32>,
        matched: &mut [bool],
    ) -> Result<()> {
        if li.is_empty() {
            return Ok(());
        }
        self.options.cancellation.check()?;
        if let Some(residual) = self.residual {
            let schema = Schema::new(
                left.schema()
                    .fields()
                    .iter()
                    .chain(right.schema().fields())
                    .cloned()
                    .collect::<Vec<_>>(),
            );
            let lidx = UInt32Array::from(li.clone());
            let ridx = UInt32Array::from(ri.clone());
            let (mask, bytes) = if self.options.relational_optimizations {
                crate::join_filter::JoinFilter::new(residual, &schema)?.eval(
                    left,
                    right,
                    &lidx,
                    &ridx,
                    &self.budget,
                )?
            } else {
                let mut arrays = Vec::new();
                let mut leases = Vec::new();
                for (batch, indices) in [(left, &lidx), (right, &ridx)] {
                    for array in batch.columns() {
                        let (array, lease) =
                            crate::join_filter::gather(array, indices, &self.budget)?;
                        arrays.push(array);
                        leases.push(lease);
                    }
                }
                let batch = make_batch(Arc::new(schema), arrays, li.len())?;
                let bytes = batch.get_array_memory_size();
                let mask = evaluate(&batch, std::slice::from_ref(residual), &self.budget)?
                    .pop()
                    .ok_or("missing join predicate result")?;
                (mask, bytes)
            };
            self.metrics.residual_candidate_rows += li.len();
            self.metrics.residual_gathered_bytes += bytes;
            let mut keep = 0;
            for i in 0..li.len() {
                if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i) {
                    li[keep] = li[i];
                    ri[keep] = ri[i];
                    keep += 1;
                }
            }
            li.truncate(keep);
            ri.truncate(keep);
        }
        for i in li.iter() {
            matched[*i as usize] = true;
        }
        if matches!(self.mode, Mode::Rows(JoinKind::Inner | JoinKind::Left)) && !li.is_empty() {
            let li = UInt32Array::from(li.clone());
            let ri = UInt32Array::from(ri.clone());
            let mut arrays = Vec::new();
            let mut leases = Vec::new();
            for (batch, indices) in [(left, &li), (right, &ri)] {
                for array in batch.columns() {
                    let (array, lease) = crate::join_filter::gather(array, indices, &self.budget)?;
                    arrays.push(array);
                    leases.push(lease);
                }
            }
            let batch = make_batch(self.output.clone(), arrays, li.len())?;
            self.metrics.join_output_rows += batch.num_rows();
            (self.emit)(batch)?;
        }
        li.clear();
        ri.clear();
        Ok(())
    }
    fn finish_probe(&mut self, stored: &RecordBatch, matched: &[bool]) -> Result<()> {
        let _workspace = self.budget.reserve(
            stored
                .num_rows()
                .checked_mul(32)
                .and_then(|n| n.checked_add(4096))
                .ok_or("join unmatched indices overflow")?,
        )?;
        let left = self.payload(stored, &self.left)?;
        let indices = matched
            .iter()
            .enumerate()
            .filter(|(_, yes)| match self.mode {
                Mode::Rows(JoinKind::Inner) => false,
                Mode::Rows(JoinKind::Left | JoinKind::Anti) => !**yes,
                Mode::Rows(JoinKind::Semi) => **yes,
                Mode::Mark { .. } => true,
            })
            .map(|(i, _)| i as u32)
            .collect::<Vec<_>>();
        if indices.is_empty() {
            return Ok(());
        }
        let indices = UInt32Array::from(indices);
        let mut arrays = Vec::new();
        let mut leases = Vec::new();
        for array in left.columns() {
            let (array, lease) = crate::join_filter::gather(array, &indices, &self.budget)?;
            arrays.push(array);
            leases.push(lease);
        }
        match self.mode {
            Mode::Rows(JoinKind::Left) => {
                let empty = RecordBatch::new_empty(self.right.clone());
                let nulls = UInt32Array::from(vec![None; indices.len()]);
                for array in empty.columns() {
                    let (array, lease) = crate::join_filter::gather(array, &nulls, &self.budget)?;
                    arrays.push(array);
                    leases.push(lease);
                }
            }
            Mode::Mark { negated } => {
                let keys = encode(
                    stored,
                    self.keys,
                    &self.budget,
                    self.options.relational_optimizations,
                )?;
                let mask = BooleanArray::from_iter((0..stored.num_rows()).map(|i| {
                    let value = if self.right_empty {
                        Some(false)
                    } else if !keys.valid(i) {
                        None
                    } else if matched[i] {
                        Some(true)
                    } else if self.right_has_null {
                        None
                    } else {
                        Some(false)
                    };
                    value.map(|v| v != negated)
                }));
                arrays.push(Arc::new(mask));
            }
            _ => {}
        }
        let batch = make_batch(self.output.clone(), arrays, indices.len())?;
        self.metrics.join_output_rows += batch.num_rows();
        (self.emit)(batch)
    }
}
fn max_row_bytes(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(32_usize, |n, a| {
        let bytes = if let Some(a) = a.as_any().downcast_ref::<StringArray>() {
            a.value_offsets()
                .windows(2)
                .map(|p| (p[1] - p[0]) as usize)
                .max()
                .unwrap_or(0)
                .saturating_add(8)
        } else {
            17
        };
        n.checked_add(bytes)
            .ok_or_else(|| "join row width overflow".into())
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    left: Partitioned,
    right: Partitioned,
    output: SchemaRef,
    mode: Mode,
    residual: Option<&Expr>,
    options: &Options,
    budget: Budget,
    store: SpillStore,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    let start = Instant::now();
    if left.keys != right.keys {
        return Err("spill join key count mismatch".into());
    }
    let mut join = Join {
        left: left.payload,
        right: right.payload,
        right_stored: right.schema,
        output,
        keys: left.keys,
        mode,
        right_empty: right.rows == 0,
        right_has_null: right.has_null,
        residual,
        options,
        budget,
        store,
        emit,
        metrics: Metrics {
            join_build_rows: right.rows,
            external_join_input_rows: left.rows + right.rows,
            ..Metrics::default()
        },
    };
    for (l, r) in left.parts.into_iter().zip(right.parts) {
        join.partition(l, r, 0)?;
    }
    join.metrics.external_join_seconds = start.elapsed().as_secs_f64();
    Ok(join.metrics)
}
