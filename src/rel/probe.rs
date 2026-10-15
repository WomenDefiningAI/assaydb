//! Probe rows retain build insertion order; LEFT misses occupy their probe position.
use super::{JoinKind, input};
use crate::{
    Result,
    expr::{Expr, boolean, is_sql_null},
    join_index::{ProbeCounts, View},
    memory::{Budget, Reservation},
};
use arrow::{
    array::{UInt32Array, new_null_array},
    compute::{filter_record_batch, take},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::time::Instant;

mod lane_keys;
pub(super) use lane_keys::{Numbers, ProbeKeys};
mod left;
pub(super) mod owned;
pub(super) mod parallel;
mod unique;

/// Static dispatch preserves the existing probe loop for borrowed and owned maps.
pub(super) trait Index: Copy {
    fn head(&self, key: &[u8], counts: &mut ProbeCounts) -> Option<u32>;
}
impl Index for View<'_, '_> {
    fn head(&self, key: &[u8], counts: &mut ProbeCounts) -> Option<u32> {
        self.get(key, counts).copied()
    }
}
#[derive(Default)]
pub(super) struct Statistics {
    pub seconds: f64,
    pub output_rows: usize,
    pub residual_rows: usize,
    pub residual_bytes: usize,
    pub probes: usize,
    pub rejected: usize,
}
impl Statistics {
    pub fn merge(&mut self, other: Self) {
        self.seconds += other.seconds;
        self.output_rows += other.output_rows;
        self.residual_rows += other.residual_rows;
        self.residual_bytes += other.residual_bytes;
        self.probes += other.probes;
        self.rejected += other.rejected;
    }
}
impl super::ordered_batches::Report for Statistics {
    fn merge(&mut self, other: Self) {
        Statistics::merge(self, other);
    }
}
pub(super) struct Kernel<'v, I> {
    pub map: I,
    pub build: &'v RecordBatch,
    pub next: &'v [u32],
    pub keys: &'v [(Expr, Expr)],
    pub kind: JoinKind,
    pub residual: Option<&'v Expr>,
    pub residual_program: Option<&'v crate::join_filter::JoinFilter<'v>>,
    pub combined_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub budget: &'v Budget,
    pub compact_text: bool,
    pub batch_size: usize,
    pub reserve_gathers: bool,
}
impl<I: Index> Kernel<'_, I> {
    pub fn run(
        &self,
        probe: RecordBatch,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Statistics> {
        self.run_with_credits(probe, &mut |batch, _credits| emit(batch), check)
    }
    pub(super) fn run_with_credits(
        &self,
        probe: RecordBatch,
        emit: &mut dyn FnMut(RecordBatch, Vec<Reservation>) -> Result<()>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Statistics> {
        let budget = self.budget;
        let compact_text = self.compact_text;
        let keys = self.keys;
        let build = self.build;
        let map = self.map;
        let next = self.next;
        let batch_size = self.batch_size;
        let kind = self.kind;
        let residual = self.residual;
        let residual_program = self.residual_program;
        let combined_schema = &self.combined_schema;
        let output_schema = &self.output_schema;
        let right_schema = &self.right_schema;
        let mut output_rows = 0;
        let mut residual_rows = 0;
        let mut residual_bytes = 0;
        let mut counts = ProbeCounts::default();
        let start = Instant::now();
        check()?;
        let probe_input = input(&probe, budget, keys.iter().map(|(e, _)| e))?;
        let probe_keys = keys
            .iter()
            .map(|(e, _)| e.eval(&probe_input, budget))
            .collect::<Result<Vec<_>>>()?;
        let probe_encoded = crate::keys::Keys::with_text_encoding(
            &probe_keys,
            probe.num_rows(),
            budget,
            compact_text,
        )?;
        if kind == JoinKind::Left {
            let mut stats = self.run_left(&probe, &probe_encoded, emit, check)?;
            stats.seconds = start.elapsed().as_secs_f64();
            return Ok(stats);
        }
        let mut li = Vec::with_capacity(batch_size);
        let mut ri = Vec::with_capacity(batch_size);
        let mut matched = vec![false; probe.num_rows()];
        // Matched candidates always have a build row. LEFT misses are
        // emitted separately with explicit NULL columns below.
        let mut flush = |li: &mut Vec<u32>, ri: &mut Vec<u32>| -> Result<()> {
            check()?;
            if li.is_empty() {
                return Ok(());
            }
            if residual.is_none() && matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                for l in li.iter() {
                    matched[*l as usize] = true;
                }
                li.clear();
                ri.clear();
                return Ok(());
            }
            let lidx = UInt32Array::from(li.clone());
            let ridx = UInt32Array::from(ri.clone());
            if let Some(program) = &residual_program {
                let (mask, bytes) = program.eval(&probe, build, &lidx, &ridx, budget)?;
                residual_rows += li.len();
                residual_bytes += bytes;
                let mut kept = 0;
                for i in 0..li.len() {
                    if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i) {
                        matched[li[i] as usize] = true;
                        li[kept] = li[i];
                        ri[kept] = ri[i];
                        kept += 1;
                    }
                }
                li.truncate(kept);
                ri.truncate(kept);
                if kept > 0 && matches!(kind, JoinKind::Inner | JoinKind::Left) {
                    let lidx = UInt32Array::from(li.clone());
                    let ridx = UInt32Array::from(ri.clone());
                    let mut arrays = Vec::with_capacity(combined_schema.fields().len());
                    let mut leases = Vec::with_capacity(arrays.capacity());
                    for (batch, indices) in [(&probe, &lidx), (build, &ridx)] {
                        for array in batch.columns() {
                            let (array, lease) =
                                crate::join_filter::gather(array, indices, budget)?;
                            arrays.push(array);
                            leases.push(lease);
                        }
                    }
                    let batch = RecordBatch::try_new(combined_schema.clone(), arrays)
                        .map_err(|e| e.to_string())?;
                    output_rows += kept;
                    emit(batch, leases)?;
                }
                li.clear();
                ri.clear();
                return Ok(());
            }
            let mut arrays = Vec::with_capacity(combined_schema.fields().len());
            let mut leases = Vec::new();
            for (batch, indices) in [(&probe, &lidx), (build, &ridx)] {
                for array in batch.columns() {
                    if self.reserve_gathers {
                        let (array, lease) = crate::join_filter::gather(array, indices, budget)?;
                        arrays.push(array);
                        leases.push(lease);
                    } else {
                        arrays.push(take(array, indices, None).map_err(|e| e.to_string())?);
                    }
                }
            }
            let batch =
                RecordBatch::try_new(combined_schema.clone(), arrays).map_err(|e| e.to_string())?;
            if !self.reserve_gathers {
                leases.push(budget.reserve(batch.get_array_memory_size())?);
            }
            let mask = residual
                .map(|e| e.eval(&input(&batch, budget, [e])?, budget))
                .transpose()?;
            for (i, l) in li.iter().enumerate() {
                if mask
                    .as_ref()
                    .is_none_or(|m| !is_sql_null(m.array.as_ref(), i) && boolean(&m.array).value(i))
                {
                    matched[*l as usize] = true;
                }
            }
            if matches!(kind, JoinKind::Inner | JoinKind::Left) {
                let batch = if let Some(mask) = mask {
                    filter_record_batch(&batch, boolean(&mask.array)).map_err(|e| e.to_string())?
                } else {
                    batch
                };
                output_rows += batch.num_rows();
                emit(batch, leases)?;
            }
            li.clear();
            ri.clear();
            Ok(())
        };
        for row in 0..probe.num_rows() {
            if row % 1024 == 0 {
                check()?;
            }
            if !probe_encoded.valid(row) {
                continue;
            }
            if let Some(first) = map.head(probe_encoded.row(row), &mut counts) {
                let mut r = first;
                while r != u32::MAX {
                    li.push(row as u32);
                    ri.push(r);
                    if li.len() == batch_size {
                        flush(&mut li, &mut ri)?;
                    }
                    // Existence needs one matching key, not every duplicate
                    // and not materialized build-side values.
                    if residual.is_none() && matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                        break;
                    }
                    r = next[r as usize];
                }
            }
        }
        flush(&mut li, &mut ri)?;
        let keep = matched
            .iter()
            .enumerate()
            .filter(|(_, yes)| match kind {
                JoinKind::Inner => false,
                JoinKind::Left | JoinKind::Anti => !**yes,
                JoinKind::Semi => **yes,
            })
            .map(|(i, _)| i as u32)
            .collect::<Vec<_>>();
        if !keep.is_empty() {
            check()?;
            let idx = UInt32Array::from(keep);
            let mut arrays = Vec::with_capacity(output_schema.fields().len());
            let mut leases = Vec::new();
            for array in probe.columns() {
                if self.reserve_gathers {
                    let (array, lease) = crate::join_filter::gather(array, &idx, budget)?;
                    arrays.push(array);
                    leases.push(lease);
                } else {
                    arrays.push(take(array, &idx, None).map_err(|e| e.to_string())?);
                }
            }
            if kind == JoinKind::Left {
                for field in right_schema.fields() {
                    if self.reserve_gathers {
                        let bytes = idx
                            .len()
                            .checked_mul(17)
                            .and_then(|n| n.checked_add(256))
                            .ok_or("join null gather size overflow")?;
                        leases.push(budget.reserve(bytes)?);
                    }
                    arrays.push(new_null_array(field.data_type(), idx.len()));
                }
            }
            let b =
                RecordBatch::try_new(output_schema.clone(), arrays).map_err(|e| e.to_string())?;
            output_rows += b.num_rows();
            emit(b, leases)?;
        }
        Ok(Statistics {
            seconds: start.elapsed().as_secs_f64(),
            output_rows,
            residual_rows,
            residual_bytes,
            probes: counts.probes,
            rejected: counts.rejected,
        })
    }
}
