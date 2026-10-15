//! Nonexpanding equality probes for an admitted scan-lane stage.
use super::*;

impl<I: Index> Kernel<'_, I> {
    /// With unique build keys and no residual, one lookup emits at most one row in probe order.
    pub(super) fn unique(
        &self,
        probe: RecordBatch,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<(RecordBatch, Reservation, Statistics)> {
        debug_assert!(self.residual.is_none());
        debug_assert!(matches!(
            self.kind,
            JoinKind::Inner | JoinKind::Semi | JoinKind::Anti
        ));
        let start = Instant::now();
        check()?;
        // Numeric columns use the identical equality bytes without temporary
        // widened arrays. Other types retain the ordinary funded key encoder.
        let encoded =
            super::lane_keys::ProbeKeys::new(&probe, self.keys, self.budget, self.compact_text)?;
        let mut left = Vec::with_capacity(probe.num_rows());
        let mut right = if self.kind == JoinKind::Inner {
            Vec::with_capacity(probe.num_rows())
        } else {
            Vec::new()
        };
        let mut counts = ProbeCounts::default();
        let mut key = [0; 34];
        for row in 0..probe.num_rows() {
            if row % 1024 == 0 {
                check()?;
            }
            let head = encoded
                .key(row, &mut key)
                .and_then(|key| self.map.head(key, &mut counts));
            match (self.kind, head) {
                (JoinKind::Inner, Some(build)) => {
                    left.push(row as u32);
                    right.push(build);
                }
                (JoinKind::Semi, Some(_)) | (JoinKind::Anti, None) => left.push(row as u32),
                _ => {}
            }
        }
        drop(encoded);
        check()?;
        let rows = left.len();
        let li = UInt32Array::from(left);
        let ri = UInt32Array::from(right);
        let sources = [(&probe, &li), (self.build, &ri)];
        let sources = &sources[..if self.kind == JoinKind::Inner { 2 } else { 1 }];
        let mut bytes = 256usize;
        // Reserve all gathers once, before Arrow allocation, using exactly the
        // ordinary funded kernel's bound for every selected column and string.
        for (batch, indices) in sources {
            for array in batch.columns() {
                bytes = bytes
                    .checked_add(crate::join_filter::gather_bytes(array, indices)?)
                    .ok_or("join gather size overflow")?;
            }
        }
        let lease = self.budget.reserve(bytes)?;
        let mut arrays = Vec::with_capacity(self.output_schema.fields().len());
        for (batch, indices) in sources {
            for array in batch.columns() {
                check()?;
                arrays.push(take(array, *indices, None).map_err(|e| e.to_string())?);
            }
        }
        let batch = RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| e.to_string())?;
        Ok((
            batch,
            lease,
            Statistics {
                seconds: start.elapsed().as_secs_f64(),
                output_rows: rows,
                probes: counts.probes,
                rejected: counts.rejected,
                ..Statistics::default()
            },
        ))
    }
}
