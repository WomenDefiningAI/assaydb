//! Stack-only deltas, used only after the caller proves all prefixes fit precision 38.
use super::States;
use super::{Aggregate, Value, bind};
use arrow::array::{Array, Decimal128Array};
use arrow::datatypes::i256;

pub(super) const GROUPS: usize = 64;

pub(super) struct NarrowDeltas {
    sums: [i128; GROUPS],
    counts: [u64; GROUPS],
}

const COLUMNS: usize = 8;

/// Admit only non-NULL exact inputs and safe states, without allocating or
/// mutating anything. Aliased SUM/AVG states retain their canonical owner.
pub(super) fn update_many(
    states: &mut [States],
    aggregates: &[bind::Agg],
    sources: &[usize],
    values: &[Option<Value>],
    groups: &[usize],
) -> Option<usize> {
    let rows = groups.len();
    if rows == 0 {
        return None;
    }
    let mut inputs: [&[i128]; COLUMNS] = [&[]; COLUMNS];
    let mut sum_indices = [0; COLUMNS];
    let mut count_indices = [0; COLUMNS];
    let mut sums = 0;
    let mut counts = 0;
    for (index, agg) in aggregates.iter().enumerate() {
        if sources[index] != index {
            continue;
        }
        let state = &states[index];
        if agg.distinct || state.counts.is_empty() || state.counts.len() > GROUPS {
            return None;
        }
        match agg.function {
            Aggregate::Sum | Aggregate::Avg => {
                if sums == COLUMNS || !state.narrow_state_is_safe(rows) {
                    return None;
                }
                let numbers = values[index]
                    .as_ref()?
                    .array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()?;
                if numbers.len() != rows || numbers.null_count() != 0 {
                    return None;
                }
                inputs[sums] = numbers.values();
                sum_indices[sums] = index;
                sums += 1;
            }
            Aggregate::Count => {
                if counts == COLUMNS
                    || state.counts.iter().any(|n| *n > u64::MAX - rows as u64)
                    || values[index].as_ref().is_some_and(|value| {
                        value.array.len() != rows || value.array.logical_null_count() != 0
                    })
                {
                    return None;
                }
                count_indices[counts] = index;
                counts += 1;
            }
            _ => return None,
        }
    }
    if sums + counts < 2 {
        return None;
    }
    // Specializing the small column count keeps the inner loop bounded and
    // lets each row reuse its group ID and hot, contiguous local sum cells.
    let count_indices = &count_indices[..counts];
    let applied = match sums {
        1 => reduce_many::<1>(states, &inputs, &sum_indices, count_indices, groups),
        2 => reduce_many::<2>(states, &inputs, &sum_indices, count_indices, groups),
        3 => reduce_many::<3>(states, &inputs, &sum_indices, count_indices, groups),
        4 => reduce_many::<4>(states, &inputs, &sum_indices, count_indices, groups),
        5 => reduce_many::<5>(states, &inputs, &sum_indices, count_indices, groups),
        6 => reduce_many::<6>(states, &inputs, &sum_indices, count_indices, groups),
        7 => reduce_many::<7>(states, &inputs, &sum_indices, count_indices, groups),
        8 => reduce_many::<8>(states, &inputs, &sum_indices, count_indices, groups),
        _ => false,
    };
    applied.then_some(sums)
}

/// Temporary operands are always signed i64, even on rejection. Thus every
/// local prefix is bounded by rows * 2^63 <= 10^38-1 < i128::MAX. Only publish
/// after every scaled operand equals its temporary and all states passed the
/// same prefix/count margin; otherwise the original error path runs unchanged.
fn reduce_many<const N: usize>(
    states: &mut [States],
    inputs: &[&[i128]; COLUMNS],
    sum_indices: &[usize; COLUMNS],
    count_indices: &[usize],
    groups: &[usize],
) -> bool {
    // All scratch is stack-only. The group-ID Vec is reserved by the caller.
    let mut deltas = [[0_i128; N]; GROUPS];
    let mut counts = [0_u64; GROUPS];
    let mut fits = true;
    for (row, &group) in groups.iter().enumerate() {
        counts[group] += 1;
        for (column, delta) in deltas[group].iter_mut().enumerate() {
            let value = inputs[column][row];
            let narrow = i128::from(value as i64);
            fits &= narrow == value;
            *delta += narrow;
        }
    }
    if !fits {
        return false;
    }
    for (column, &index) in sum_indices[..N].iter().enumerate() {
        let state = &mut states[index];
        for (group, (sum, count)) in state
            .sums
            .as_mut()
            .unwrap()
            .iter_mut()
            .zip(&mut state.counts)
            .enumerate()
        {
            *sum += i256::from_i128(deltas[group][column]);
            *count += counts[group];
        }
    }
    for &index in count_indices {
        for (count, delta) in states[index].counts.iter_mut().zip(counts) {
            *count += delta;
        }
    }
    true
}
impl NarrowDeltas {
    /// Zero deltas preserve the existing states and require no heap reservation.
    pub(super) fn new() -> Self {
        Self {
            sums: [0; GROUPS],
            counts: [0; GROUPS],
        }
    }

    /// Admission bounds |input| by 2^63 and batch growth by precision 38, including every prefix.
    #[inline]
    pub(super) fn add(&mut self, group: usize, value: i128) {
        self.sums[group] += value;
        self.counts[group] += 1;
    }

    /// The admitted prefix/count margins make one wide merge equal all ordered additions.
    pub(super) fn merge(self, state: &mut States) {
        let sums = state.sums.as_mut().unwrap();
        for (group, (sum, count)) in sums
            .iter_mut()
            .zip(&mut state.counts)
            .take(GROUPS)
            .enumerate()
        {
            *sum += i256::from_i128(self.sums[group]);
            *count += self.counts[group];
        }
    }
}
