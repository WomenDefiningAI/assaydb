//! Funded integer-series cursor used by SQL table-function execution.
//! NULL/zero-step precedence and direction follow the saved DuckDB 2.0 probes.
use crate::{
    Result,
    memory::{Budget, Reservation},
    spill::Cancellation,
};
#[cfg(test)]
use std::ops::ControlFlow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Endpoint {
    Exclusive,
    Inclusive,
}

/// A funded, finite batch. Keep the lease with its values through consumption;
/// never extract the Vec without transferring its reservation to the consumer.
#[derive(Debug)]
pub(crate) struct IntegerBatch {
    values: Vec<i64>,
    _lease: Reservation,
}
impl IntegerBatch {
    #[cfg(test)]
    pub(crate) fn values(&self) -> &[i64] {
        &self.values
    }
    pub(crate) fn into_parts(self) -> (Vec<i64>, Reservation) {
        (self.values, self._lease)
    }
}

/// Counts and current values use i128/u128: inclusive MIN..MAX contains 2^64
/// values, and advancing past the final i64 endpoint must not overflow.
#[derive(Debug, Clone)]
pub(crate) struct IntegerSeries {
    next: i128,
    step: i128,
    remaining: u128,
}
impl IntegerSeries {
    pub(crate) fn new(
        start: Option<i64>,
        stop: Option<i64>,
        step: Option<i64>,
        endpoint: Endpoint,
        cancellation: &Cancellation,
    ) -> Result<Self> {
        cancellation.check()?;
        // DuckDB 2.0: any NULL argument yields no rows, including NULL/zero.
        let (Some(start), Some(stop), Some(step)) = (start, stop, step) else {
            return Ok(Self {
                next: 0,
                step: 1,
                remaining: 0,
            });
        };
        if step == 0 {
            return Err("interval cannot be 0!".into());
        }
        let start = i128::from(start);
        let stop = i128::from(stop);
        let step = i128::from(step);
        let distance = if step > 0 { stop - start } else { start - stop };
        let stride = step.abs(); // abs(i64::MIN) is safe in i128.
        let remaining = if distance < 0 || (distance == 0 && endpoint == Endpoint::Exclusive) {
            0
        } else if endpoint == Endpoint::Inclusive {
            (distance / stride + 1) as u128
        } else {
            // Positive distance: ceil(distance / stride), without floating point.
            ((distance - 1) / stride + 1) as u128
        };
        Ok(Self {
            next: start,
            step,
            remaining,
        })
    }

    pub(crate) fn remaining(&self) -> u128 {
        self.remaining
    }

    /// Allocation is bounded by requested rows and actual remaining rows, never
    /// the full conceptual sequence. Failed funding/allocation/cancellation does
    /// not advance the cursor, making failure/retry behavior explicit.
    pub(crate) fn next_batch(
        &mut self,
        max_rows: usize,
        budget: &Budget,
        cancellation: &Cancellation,
    ) -> Result<Option<IntegerBatch>> {
        cancellation.check()?;
        if max_rows == 0 {
            return Err("series batch size must be positive".into());
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let count = self.remaining.min(max_rows as u128) as usize;
        let bytes = count
            .checked_mul(std::mem::size_of::<i64>())
            .and_then(|n| n.checked_add(1024))
            .ok_or("series batch allocation size overflow")?;
        let lease = budget.reserve(bytes)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|e| crate::Error::from(format!("series allocation failed: {e}")))?;
        let mut next = self.next;
        for index in 0..count {
            if index % 256 == 0 {
                cancellation.check()?;
            }
            values.push(
                i64::try_from(next)
                    .map_err(|_| crate::Error::from("internal: series value outside i64"))?,
            );
            next += self.step;
        }
        cancellation.check()?;
        self.next = next;
        self.remaining -= count as u128;
        Ok(Some(IntegerBatch {
            values,
            _lease: lease,
        }))
    }

    /// A bounded consumer can stop normally without draining a huge series.
    /// This draft-local control flow must be integrated with the relational
    /// execution contract before SQL LIMIT can use it.
    #[cfg(test)]
    pub(crate) fn emit_batches(
        &mut self,
        max_rows: usize,
        budget: &Budget,
        cancellation: &Cancellation,
        emit: &mut dyn FnMut(IntegerBatch) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        while let Some(batch) = self.next_batch(max_rows, budget, cancellation)? {
            if let ControlFlow::Break(()) = emit(batch)? {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "integer_series_tests.rs"]
mod tests;
