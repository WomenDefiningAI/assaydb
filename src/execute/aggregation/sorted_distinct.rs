//! Optional COUNT(DISTINCT) over exact encounter IDs and direct INTEGER input.
//! The caller owns a no-output fallback boundary; neither a cap nor a wide
//! INTEGER refusal is a SQL error. Only sorted pairs move, never output groups.
use super::*;
use crate::{expr::Kind, spill::Cancellation};

pub(crate) const DECLINED: &str = "internal: sorted distinct admission declined";

pub(super) struct Pairs {
    // Both capacities are prepaid. Drop buffers before their common credit.
    pairs: Vec<u128>,
    scratch: Vec<u128>,
    _credit: Reservation,
    row_limit: usize,
    rows: usize,
    varying: u128,
    cancellation: Cancellation,
}

impl Pairs {
    /// A checked physical-row cap funds both full capacities before allocation;
    /// refusal leaves the existing hash state untouched and reserves no workers.
    fn new(budget: &Budget, row_limit: usize, cancellation: Cancellation) -> Option<Self> {
        let bytes = row_limit.checked_mul(2 * std::mem::size_of::<u128>())?;
        if row_limit == 0
            || row_limit > i64::MAX as usize
            || bytes > isize::MAX as usize
            || bytes > budget.limit().saturating_sub(budget.used()) / 4
        {
            return None;
        }
        let credit = budget.reserve(bytes).ok()?;
        let pairs = Vec::with_capacity(row_limit);
        let scratch = Vec::with_capacity(row_limit);
        assert_eq!(pairs.capacity(), row_limit);
        assert_eq!(scratch.capacity(), row_limit);
        Some(Self {
            pairs,
            scratch,
            _credit: credit,
            row_limit,
            rows: 0,
            varying: 0,
            cancellation,
        })
    }

    /// Each non-NULL row appends one exact pair without growth. Counts of all
    /// rows enforce the cap even for all-NULL values; wide integers decline.
    pub(super) fn append(&mut self, groups: &[usize], value: &Value) -> Result<usize> {
        self.cancellation.check()?;
        let rows = self.rows.checked_add(groups.len()).ok_or(DECLINED)?;
        if rows > self.row_limit {
            return Err(DECLINED.into());
        }
        let before = self.pairs.len();
        if value.array.data_type() != &arrow::datatypes::DataType::Null {
            let values = numeric(&value.array);
            debug_assert_eq!(groups.len(), values.len());
            for (row, &group) in groups.iter().enumerate() {
                if row % 8192 == 0 {
                    self.cancellation.check()?;
                }
                if values.is_null(row) {
                    continue;
                }
                let value = i64::try_from(values.value(row)).map_err(|_| DECLINED)?;
                let group = u64::try_from(group).map_err(|_| DECLINED)?;
                let pair = (u128::from(group) << 64) | u128::from((value as u64) ^ (1_u64 << 63));
                self.varying |= self.pairs.first().map_or(0, |first| first ^ pair);
                self.pairs.push(pair);
            }
        }
        self.rows = rows;
        Ok(self.pairs.len() - before)
    }

    /// Stable byte passes use safe indexes and a checked prefix sum. Constant
    /// digits need no pass; scratch initializes only the actually retained rows.
    fn sort(&mut self) -> Result<()> {
        self.cancellation.check()?;
        self.scratch.resize(self.pairs.len(), 0);
        for shift in (0..128).step_by(8) {
            if (self.varying >> shift) & 255 == 0 {
                continue;
            }
            let mut offsets = [0usize; 256];
            for chunk in self.pairs.chunks(8192) {
                self.cancellation.check()?;
                for &pair in chunk {
                    // At most row_limit <= isize::MAX/32 entries in any bucket.
                    offsets[((pair >> shift) & 255) as usize] += 1;
                }
            }
            let mut total = 0usize;
            for offset in &mut offsets {
                let count = *offset;
                *offset = total;
                total = total.checked_add(count).ok_or(DECLINED)?;
            }
            debug_assert_eq!(total, self.pairs.len());
            for chunk in self.pairs.chunks(8192) {
                self.cancellation.check()?;
                for &pair in chunk {
                    let bucket = ((pair >> shift) & 255) as usize;
                    self.scratch[offsets[bucket]] = pair;
                    offsets[bucket] += 1;
                }
            }
            std::mem::swap(&mut self.pairs, &mut self.scratch);
        }
        Ok(())
    }

    /// Adjacent equal pairs count once into the original encounter-ID vector.
    /// Each count is bounded by row_limit <= i64::MAX; absent/NULL-only groups
    /// keep zero. Taking ownership releases all sort storage before emission.
    pub(super) fn finish(mut self, counts: &mut [u64]) -> Result<()> {
        self.sort()?;
        let mut previous = None;
        for chunk in self.pairs.chunks(8192) {
            self.cancellation.check()?;
            for &pair in chunk {
                if previous != Some(pair) {
                    counts[(pair >> 64) as usize] += 1;
                    previous = Some(pair);
                }
            }
        }
        Ok(())
    }
}

impl Aggregation {
    /// Admit exactly one COUNT(DISTINCT direct_integer) with a direct integer
    /// group ID. No mixed aggregates, expression errors, scalar or helper state
    /// can have their evaluation/finalization chronology changed here.
    pub(in crate::execute) fn enable_sorted_distinct(
        &mut self,
        p: &Plan,
        row_limit: usize,
        options: &Options,
    ) -> bool {
        if !options.relational_optimizations
            || options.error_replay.canonical()
            || options.spill.is_some()
            || !p.aggregation
            || p.filter.is_some()
            || !p.order.is_empty()
            || p.groups.len() != 1
            || p.groups[0].ty != Type::Int
            || !matches!(p.groups[0].kind, Kind::Column(_))
            || p.aggregates.len() != 1
            || !p.aggregates[0].distinct
            || !matches!(p.aggregates[0].function, Aggregate::Count)
            || !p.aggregates[0]
                .input
                .as_ref()
                .is_some_and(|e| e.ty == Type::Int && matches!(e.kind, Kind::Column(_)))
            || self.group_count != 0
            || self.aggregate_workers.is_some()
            || self.sorted_distinct.is_some()
        {
            return false;
        }
        let Some(pairs) = Pairs::new(&self.budget, row_limit, options.cancellation.clone()) else {
            return false;
        };
        self.states[0].distinct = None;
        self.sorted_distinct = Some(pairs);
        true
    }
}

#[cfg(test)]
mod tests;
