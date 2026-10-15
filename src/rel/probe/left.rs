//! Bounded, funded index buffers interleave LEFT matches and misses without sorting.
use super::{Index, Kernel, Statistics};
use crate::{
    Result,
    expr::{boolean, is_sql_null},
    join_index::ProbeCounts,
    keys::Keys,
    memory::Reservation,
};
use arrow::{
    array::{BinaryArray, StringArray, UInt32Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};

impl<I: Index> Kernel<'_, I> {
    pub(super) fn run_left(
        &self,
        probe: &RecordBatch,
        keys: &Keys,
        emit: &mut dyn FnMut(RecordBatch, Vec<Reservation>) -> Result<()>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Statistics> {
        if probe.num_rows() == 0 {
            return Ok(Statistics::default());
        }
        // A row emits at most all build rows or one miss. Small/empty builds
        // therefore need no full-size scratch allocation for tiny probe batches.
        let batch_size = self.batch_size.min(
            probe
                .num_rows()
                .saturating_mul(self.build.num_rows().max(1)),
        );
        // Covers both index vectors, candidate indices, simultaneous Arrow
        // index copies/validity, and gather descriptors before any allocation.
        // Output payloads have separate leases retained through delivery.
        let bytes = batch_size
            .checked_mul(64)
            .and_then(|n| n.checked_add(self.combined_schema.fields().len().checked_mul(256)?))
            .and_then(|n| n.checked_add(2048))
            .ok_or("join candidate size overflow")?;
        let _workspace = self.budget.reserve(bytes)?;
        let mut li = Vec::with_capacity(batch_size);
        let mut ri = Vec::with_capacity(batch_size);
        let mut candidates = Vec::with_capacity(if self.residual.is_some() {
            batch_size
        } else {
            0
        });
        // Even with relational optimizations disabled, validation must gather
        // only the columns used by ON, never the unselected output payload.
        // The descriptor storage is covered by the workspace above.
        let fallback_program = if self.residual_program.is_none() {
            self.residual
                .map(|e| crate::join_filter::JoinFilter::new(e, &self.combined_schema))
                .transpose()?
        } else {
            None
        };
        let mut stats = Statistics::default();
        let mut counts = ProbeCounts::default();
        for row in 0..probe.num_rows() {
            if row % 1024 == 0 {
                check()?;
            }
            let mut next = if keys.valid(row) {
                self.map
                    .head(keys.row(row), &mut counts)
                    .unwrap_or(u32::MAX)
            } else {
                u32::MAX
            };
            let mut matched = false;
            if let Some(program) = self.residual_program.or(fallback_program.as_ref()) {
                // Hold at most one funded batch of matches until EVERY
                // candidate for this row succeeds. A downstream error must not
                // hide a later candidate's residual error, even at batch size 1.
                // If that buffer fills, remember the first unbuffered match and
                // validate the rest without retaining it. Then emit the prefix
                // and replay only the suffix over immutable inputs. This bounds
                // scratch independently of duplicate-chain length, without a
                // larger reservation that could refuse a previously fitting row.
                // No vector grows beyond the capacity prepaid above, even when
                // all build rows match. Payload chunks have an independent byte
                // bound in flush_left; funded IDs alone cannot fund wide values.
                let mut deferred = u32::MAX;
                let mut replaying = false;
                loop {
                    while next != u32::MAX {
                        check()?;
                        candidates.clear();
                        while next != u32::MAX && candidates.len() < batch_size {
                            candidates.push(next);
                            next = self.next[next as usize];
                        }
                        let lidx = UInt32Array::from_iter_values(std::iter::repeat_n(
                            row as u32,
                            candidates.len(),
                        ));
                        let ridx = UInt32Array::from_iter_values(candidates.iter().copied());
                        let (mask, bytes) =
                            program.eval(probe, self.build, &lidx, &ridx, self.budget)?;
                        // These counters track the enabled residual optimization;
                        // the fallback shares its evaluator only for bounded gathers.
                        if self.residual_program.is_some() {
                            stats.residual_rows += candidates.len();
                            stats.residual_bytes += bytes;
                        }
                        for (i, &right) in candidates.iter().enumerate() {
                            if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i)
                            {
                                matched = true;
                                if !replaying && li.len() == batch_size {
                                    if deferred == u32::MAX {
                                        deferred = right;
                                    }
                                } else {
                                    li.push(row as u32);
                                    ri.push(Some(right));
                                }
                            }
                        }
                        if replaying {
                            // Every residual already succeeded in the first
                            // pass. The empty-at-entry buffers cannot grow past
                            // this candidate chunk, and replay preserves order.
                            stats.output_rows +=
                                self.flush_left(probe, &mut li, &mut ri, emit, check)?;
                        }
                    }
                    if replaying || deferred == u32::MAX {
                        break;
                    }
                    stats.output_rows += self.flush_left(probe, &mut li, &mut ri, emit, check)?;
                    next = deferred;
                    replaying = true;
                }
            } else {
                // The common equijoin path stays vectorized across probe rows.
                while next != u32::MAX {
                    matched = true;
                    li.push(row as u32);
                    ri.push(Some(next));
                    next = self.next[next as usize];
                    if li.len() == batch_size {
                        stats.output_rows +=
                            self.flush_left(probe, &mut li, &mut ri, emit, check)?;
                    }
                }
            }
            if !matched {
                li.push(row as u32);
                ri.push(None);
            }
            if li.len() == batch_size || self.residual.is_some() {
                stats.output_rows += self.flush_left(probe, &mut li, &mut ri, emit, check)?;
            }
        }
        stats.output_rows += self.flush_left(probe, &mut li, &mut ri, emit, check)?;
        stats.probes = counts.probes;
        stats.rejected = counts.rejected;
        Ok(stats)
    }

    fn flush_left(
        &self,
        probe: &RecordBatch,
        li: &mut Vec<u32>,
        ri: &mut Vec<Option<u32>>,
        emit: &mut dyn FnMut(RecordBatch, Vec<Reservation>) -> Result<()>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<usize> {
        if li.is_empty() {
            return Ok(0);
        }
        let rows = li.len();
        let mut start = 0;
        while start < rows {
            check()?;
            // Keep room for downstream evaluation, as in the external join's
            // per-batch policy. A single wide row may exceed this soft target;
            // its actual gathers still have to reserve before allocating.
            let available = self.budget.limit().saturating_sub(self.budget.used());
            let count = self.left_output_rows(probe, &li[start..], &ri[start..], available / 4)?;
            let end = start + count;
            let lidx = UInt32Array::from_iter_values(li[start..end].iter().copied());
            // A NULL index gathers NULL even from an empty build. It never
            // invents a candidate on which to evaluate ON.
            let ridx = UInt32Array::from_iter(ri[start..end].iter().copied());
            let mut arrays = Vec::with_capacity(self.output_schema.fields().len());
            let mut leases = Vec::with_capacity(arrays.capacity());
            for (batch, indices) in [(probe, &lidx), (self.build, &ridx)] {
                for array in batch.columns() {
                    let (array, lease) = crate::join_filter::gather(array, indices, self.budget)?;
                    arrays.push(array);
                    leases.push(lease);
                }
            }
            let batch = RecordBatch::try_new(self.output_schema.clone(), arrays)
                .map_err(|e| e.to_string())?;
            emit(batch, leases)?;
            start = end;
        }
        li.clear();
        ri.clear();
        Ok(rows)
    }

    /// Bound the next gather by its selected bytes, including repeated strings
    /// and binary values. These are the same flat-type bounds used by gather:
    /// 256 bytes per array plus 17 per row plus non-NULL variable-width data.
    /// This only sizes output; it never changes which residuals are demanded.
    fn left_output_rows(
        &self,
        probe: &RecordBatch,
        li: &[u32],
        ri: &[Option<u32>],
        byte_limit: usize,
    ) -> Result<usize> {
        let columns = self.output_schema.fields().len();
        let base = columns
            .checked_mul(256)
            .ok_or("join gather size overflow")?;
        let per_row = columns.checked_mul(17).ok_or("join gather size overflow")?;
        let rows = li
            .len()
            .min(self.batch_size)
            .min((byte_limit.saturating_sub(base) / per_row.max(1)).max(1));
        if !probe
            .columns()
            .iter()
            .chain(self.build.columns())
            .any(|a| matches!(a.data_type(), DataType::Utf8 | DataType::Binary))
        {
            return Ok(rows);
        }
        let mut bytes = base;
        for (i, (&left, &right)) in li.iter().zip(ri).take(rows).enumerate() {
            let mut next = bytes
                .checked_add(per_row)
                .ok_or("join gather size overflow")?;
            for (batch, row) in [(probe, Some(left)), (self.build, right)] {
                if let Some(row) = row {
                    let row = row as usize;
                    for array in batch.columns() {
                        let payload = if array.is_null(row) {
                            0
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).len()
                        } else if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
                            a.value(row).len()
                        } else {
                            0
                        };
                        next = next
                            .checked_add(payload)
                            .ok_or("join gather size overflow")?;
                    }
                }
            }
            if i > 0 && next > byte_limit {
                return Ok(i);
            }
            bytes = next;
        }
        Ok(rows)
    }
}
