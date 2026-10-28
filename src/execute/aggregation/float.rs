//! Plain IEEE addition in canonical input-row order, with no partial reduction.
//!
//! Scan delivery, relational inputs and external aggregate replay preserve each
//! group's row order. Batches only suspend this accumulator; they never reset it.
//! Aggregate helpers may own different columns, never fragments of one state.
use super::*;

impl States {
    pub(super) fn update_float(
        &mut self,
        array: &ArrayRef,
        groups: &[usize],
        scalar: bool,
    ) -> Result<()> {
        let numbers = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("bound Float aggregate input");
        for (row, number) in numbers.iter().enumerate() {
            let Some(number) = number else { continue };
            let group = if scalar { 0 } else { groups[row] };
            self.add_float(group, number)?;
        }
        Ok(())
    }

    /// One sequential IEEE addition, shared by plain and DISTINCT Float sums.
    pub(super) fn add_float(&mut self, group: usize, number: f64) -> Result<()> {
        let sums = self.float_sums.as_mut().expect("Float aggregate state");
        // A finite prefix that overflows yields +/-infinity like DuckDB
        // 1.5.6 (PostgreSQL float8_pl errors): maintainer decision of
        // 2026-10-17. Later terms keep IEEE propagation in fixed order.
        let sum = sums[group] + number;
        // NaN payload/sign is not a SQL value distinction. Fix the encoding
        // too, so input IPC/Parquet and arithmetic NaNs yield identical bits.
        sums[group] = if sum.is_nan() { f64::NAN } else { sum };
        self.counts[group] = self.counts[group]
            .checked_add(1)
            .ok_or("aggregate count overflow")?;
        Ok(())
    }

    pub(super) fn float_output(&self, range: std::ops::Range<usize>, average: bool) -> ArrayRef {
        let sums = self.float_sums.as_ref().expect("Float aggregate state");
        Arc::new(Float64Array::from_iter(range.map(|group| {
            let count = self.counts[group];
            (count != 0).then(|| {
                if average {
                    sums[group] / count as f64
                } else {
                    sums[group]
                }
            })
        })))
    }
}
