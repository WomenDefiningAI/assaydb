//! Completed partitions for lookahead frames and positional value functions.
//! The input sort fixes ORDER BY + canonical ordinal. Large partitions retain
//! query-owned IPC chunks. Every supported frame start and end only moves
//! forward as the current row advances, so the frame bounds, entering rows,
//! leaving rows and selected values each use their own one-chunk cursor: a
//! spilled chunk is read O(1) times per cursor per partition, never per row.
//! Every frame is finalized before the restore sort can expose any output.
//!
//! Work per partition of n rows (w = frame width):
//! - COUNT, Int/Decimal SUM/AVG: add entering and subtract leaving rows, O(n).
//!   Int keeps its every-prefix precision-38 rule with monotone min/max queues
//!   of running totals, used only when the partition's absolute total could
//!   exceed that limit.
//! - MIN/MAX: running extreme (prefix frames) or a monotone queue, O(n).
//! - Float SUM/AVG: prefix frames extend one sequential fold, O(n). Other frames
//!   use exact scaled-integer totals when every partial sum is provably exact
//!   (each IEEE addition is then exact, so the bits equal the canonical fold),
//!   O(n). Otherwise each frame is folded left to right, O(n * w), and a
//!   partition whose total folded frame rows would exceed FLOAT_FOLD_BASE or
//!   FLOAT_FOLD_PER_ROW * n is refused as unsupported. Never subtract a Float.
//! - FIRST/LAST/NTH_VALUE and NTILE: O(n).
use super::*;
use crate::{
    aggregate_sum,
    bind::Aggregate,
    execute::{Key, WindowAccumulator, count_array, keys_array, physical_output_type},
    expr::{Value, numeric},
    spill::{SpillBatch, SpillFile},
};
use arrow::datatypes::i256;
use frames::{Amount, Bound, Exclude, Unit, ValueKind};
use std::collections::VecDeque;

/// Non-prefix inexact Float frames may fold at most max(BASE, PER_ROW * n)
/// frame rows per partition before a typed refusal (SEMANTICS-LOG.md).
pub(super) const FLOAT_FOLD_BASE: u64 = 1 << 28;
pub(super) const FLOAT_FOLD_PER_ROW: u64 = 128;
const PRECISION_LIMIT: i128 = 10_i128.pow(38) - 1;
/// 10^0 ..= 10^22, each exactly representable in binary64.
const POWERS_OF_TEN: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

#[derive(Clone)]
enum Loaded {
    Memory(RecordBatch, Arc<Reservation>),
    Disk(SpillBatch),
}
impl Loaded {
    fn batch(&self) -> &RecordBatch {
        match self {
            Self::Memory(batch, _credit) => batch,
            Self::Disk(b) => &b.batch,
        }
    }
}
enum Storage {
    Memory(Loaded),
    Disk(SpillFile),
}
struct Chunk {
    start: usize,
    rows: usize,
    data: Storage,
    _metadata: Reservation,
}

/// Bit range of the partition's finite non-zero Float aggregate inputs.
#[derive(Default, Clone, Copy)]
struct FloatRange {
    count: u64,
    low: i32,
    high: i32,
}

#[derive(Default)]
struct Partition {
    chunks: Vec<Chunk>,
    rows: usize,
    memory: usize,
    integer_magnitude: u128,
    floats: FloatRange,
}
impl Partition {
    fn push(
        &mut self,
        batch: RecordBatch,
        argument: Option<(usize, &Type)>,
        ctx: &Context<'_>,
    ) -> Result<()> {
        let _workspace = ctx.budget.reserve(
            batch
                .get_array_memory_size()
                .saturating_mul(2)
                .saturating_add(batch.num_rows().saturating_mul(16))
                .saturating_add(batch.num_columns() * 1024),
        )?;
        let indices = UInt64Array::from_iter_values(0..batch.num_rows() as u64);
        let compact = RecordBatch::try_new(
            batch.schema(),
            batch
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| crate::Error::from(e.to_string())))
                .collect::<Result<Vec<_>>>()?,
        )
        .map_err(|e| e.to_string())?;
        if let Some((column, ty)) = argument {
            self.observe(compact.column(column), ty);
        }
        let metadata = ctx.budget.reserve(512)?;
        let bytes = compact
            .get_array_memory_size()
            .saturating_add(compact.num_columns() * 256);
        let data = if let Some(store) = &ctx.spill
            && self.memory.saturating_add(bytes)
                > ctx.options.spill.as_ref().unwrap().sort_run_bytes
        {
            Storage::Disk(store.write_batch(&compact)?)
        } else {
            let credit = ctx.budget.reserve(bytes)?;
            self.memory = self.memory.saturating_add(bytes);
            Storage::Memory(Loaded::Memory(compact, Arc::new(credit)))
        };
        let start = self.rows;
        self.rows = self
            .rows
            .checked_add(batch.num_rows())
            .ok_or("window partition size overflow")?;
        self.chunks.push(Chunk {
            start,
            rows: batch.num_rows(),
            data,
            _metadata: metadata,
        });
        Ok(())
    }

    /// Facts that select an exact O(n) aggregate algorithm, gathered while the
    /// chunk is still in memory. They never change a result, only the method.
    fn observe(&mut self, array: &ArrayRef, ty: &Type) {
        match ty {
            Type::Int => {
                for value in numeric(array).iter().flatten() {
                    self.integer_magnitude =
                        self.integer_magnitude.saturating_add(value.unsigned_abs());
                }
            }
            Type::Float => {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for value in values.iter().flatten() {
                    if value.is_finite() && value != 0.0 {
                        let (_, low, high) = float_parts(value);
                        let range = &mut self.floats;
                        if range.count == 0 {
                            (range.low, range.high) = (low, high);
                        } else {
                            range.low = range.low.min(low);
                            range.high = range.high.max(high);
                        }
                        range.count += 1;
                    }
                }
            }
            _ => {}
        }
    }

    fn read(&self, index: usize) -> Result<Loaded> {
        Ok(match &self.chunks[index].data {
            Storage::Memory(value) => value.clone(),
            Storage::Disk(file) => Loaded::Disk(file.read_batch()?),
        })
    }
}

/// One forward reader holding a single chunk (spilled chunks stay funded by
/// their own lease while held).
#[derive(Default)]
struct Cursor {
    loaded: Option<(usize, Loaded)>,
}
impl Cursor {
    fn at(&mut self, partition: &Partition, row: usize) -> Result<(usize, &Loaded, usize)> {
        let hit = self.loaded.as_ref().is_some_and(|(index, _)| {
            let chunk = &partition.chunks[*index];
            chunk.start <= row && row < chunk.start + chunk.rows
        });
        if !hit {
            let index = partition.chunks.partition_point(|chunk| chunk.start <= row) - 1;
            self.loaded = None;
            self.loaded = Some((index, partition.read(index)?));
        }
        let (index, loaded) = self.loaded.as_ref().unwrap();
        Ok((*index, loaded, row - partition.chunks[*index].start))
    }
}

/// A frame bound as a forward-only scan. Each supported bound is monotone in
/// the current row (ROWS arithmetic, RANGE offsets and peers in sort order,
/// NULL peers at their sorted end), so the scan restarts where the previous
/// row's bound ended and visits each row O(1) times per partition.
#[derive(Default)]
struct Seeker {
    cursor: Cursor,
    position: usize,
}
impl Seeker {
    fn seek(
        &mut self,
        partition: &Partition,
        low: usize,
        high: usize,
        mut before: impl FnMut(&RecordBatch, usize) -> bool,
    ) -> Result<usize> {
        let mut row = self.position.max(low).min(high);
        while row < high {
            let (_, loaded, local) = self.cursor.at(partition, row)?;
            if !before(loaded.batch(), local) {
                break;
            }
            row += 1;
        }
        self.position = row;
        Ok(row)
    }
}

/// Peer groups of one partition: the first row of each group, in order.
struct Peers {
    starts: Vec<usize>,
    credit: Reservation,
    /// The current row's group.
    group: usize,
}
impl Peers {
    fn build(
        partition: &Partition,
        spec: &Specification,
        width: usize,
        ctx: &Context<'_>,
    ) -> Result<Self> {
        let mut peers = Self {
            starts: vec![0],
            credit: ctx.budget.reserve(1024)?,
            group: 0,
        };
        let column = width + spec.partition.len();
        let keys = spec.order.len();
        if keys == 0 || partition.rows == 0 {
            return Ok(peers);
        }
        let (mut previous, mut current) = (Cursor::default(), Cursor::default());
        for row in 1..partition.rows {
            ctx.options.cancellation.check()?;
            let (_, before, before_local) = previous.at(partition, row - 1)?;
            let (_, after, after_local) = current.at(partition, row)?;
            if !equal_keys(
                before.batch(),
                before_local,
                after.batch(),
                after_local,
                column,
                keys,
            ) {
                if peers.starts.len() == peers.starts.capacity() {
                    let grow = peers.starts.capacity().max(64);
                    peers
                        .credit
                        .absorb(ctx.budget.reserve(grow * std::mem::size_of::<usize>())?);
                    peers.starts.reserve_exact(grow);
                }
                peers.starts.push(row);
            }
        }
        Ok(peers)
    }
    fn advance(&mut self, row: usize) {
        while self.group + 1 < self.starts.len() && self.starts[self.group + 1] <= row {
            self.group += 1;
        }
    }
    /// First row of group `index`, or the partition end past the last group.
    fn start(&self, index: usize, rows: usize) -> usize {
        self.starts.get(index).copied().unwrap_or(rows)
    }
}

/// The RANGE offset bound of a non-NULL current key, compared in the key's
/// own type: exact integers (INTEGER, DATE days), exact scaled DECIMAL, or
/// IEEE binary64 with the SQL total order (NaN above +infinity) for REAL.
enum Target {
    Integer(i128),
    Decimal { target: i256, factor: i256 },
    Real(f64),
}
impl Target {
    fn compare(&self, array: &ArrayRef, index: usize) -> std::cmp::Ordering {
        match self {
            Self::Integer(target) => integer_key(array, index).cmp(target),
            Self::Decimal { target, factor } => {
                (i256::from_i128(integer_key(array, index)) * *factor).cmp(target)
            }
            Self::Real(target) => crate::expr::numeric_compare::real_order(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(index),
                *target,
            ),
        }
    }
}

fn range_target(
    spec: &Specification,
    amount: Amount,
    following: bool,
    current: &ArrayRef,
    local: usize,
) -> Result<Target> {
    let descending = spec.order[0].1;
    let upward = following ^ descending;
    let outside =
        || crate::Error::from("unsupported: RANGE boundary outside common reference domain");
    Ok(match (&spec.order[0].0.ty, amount) {
        (Type::Float, amount) => {
            let offset = match amount {
                Amount::Count(n) => n as f64,
                // Both references convert the literal with one correctly
                // rounded operation; admitted literals are exact integers
                // over an exact power of ten (frames::validate).
                Amount::Exact { digits, scale } => digits as f64 / POWERS_OF_TEN[scale as usize],
                Amount::Days(_) | Amount::Micros(_) => unreachable!("validated RANGE offset"),
            };
            let value = current
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(local);
            Target::Real(if upward {
                value + offset
            } else {
                value - offset
            })
        }
        (Type::Decimal(key_scale), amount) => {
            let (digits, scale) = match amount {
                Amount::Count(n) => (n as i128, 0),
                Amount::Exact { digits, scale } => (digits, scale),
                Amount::Days(_) | Amount::Micros(_) => unreachable!("validated RANGE offset"),
            };
            let common = (*key_scale).max(scale).max(0);
            let power = |n: i8| i256::from_i128(10).wrapping_pow((common - n).max(0) as u32);
            let factor = power(*key_scale);
            let value = i256::from_i128(integer_key(current, local)) * factor;
            let offset = i256::from_i128(digits) * power(scale);
            let target = if upward {
                value + offset
            } else {
                value - offset
            };
            // Stay within the key type's DECIMAL(38, scale) domain, where
            // both references compute the bound exactly.
            let limit = i256::from_i128(PRECISION_LIMIT) * factor;
            if target > limit || target < -limit {
                return Err(outside());
            }
            Target::Decimal { target, factor }
        }
        (_, amount) => {
            let timestamp = matches!(spec.order[0].0.ty, Type::Timestamp | Type::TimestampTz);
            let count = match amount {
                // TIMESTAMP keys are microseconds: a DAY is 24 hours.
                Amount::Days(n) if timestamp => n as i128 * 86_400_000_000,
                Amount::Count(n) | Amount::Days(n) | Amount::Micros(n) => n as i128,
                Amount::Exact { .. } => unreachable!("validated RANGE offset"),
            };
            let value = integer_key(current, local);
            let target = value
                .checked_add(if upward { count } else { -count })
                .ok_or("unsupported: RANGE boundary overflow")?;
            let domain = if spec.order[0].0.ty == Type::Date {
                // DATE +/- INTERVAL is timestamp arithmetic in both references.
                // PG timestamp.h: Julian day 0, less UNIX_EPOCH_JDATE=2440588.
                // DuckDB's upper limit is signed i64 microseconds since Unix epoch.
                -2_440_588..=(i64::MAX as i128 / 86_400_000_000)
            } else if timestamp {
                // The calendar range where both references print the same values.
                i128::from(crate::expr::timestamp::MIN)..=i128::from(crate::expr::timestamp::MAX)
            } else {
                i64::MIN as i128..=i64::MAX as i128
            };
            if !(domain.contains(&value) && domain.contains(&target)) {
                return Err(outside());
            }
            Target::Integer(target)
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn boundary(
    seeker: &mut Seeker,
    partition: &Partition,
    bound: Bound,
    end: bool,
    unit: Unit,
    row: usize,
    current: &RecordBatch,
    local: usize,
    spec: &Specification,
    width: usize,
    peers: Option<&Peers>,
) -> Result<usize> {
    if bound == Bound::Unbounded {
        return Ok(if end { partition.rows } else { 0 });
    }
    let (amount, following) = match bound {
        Bound::Offset { amount, following } => (Some(amount), following),
        _ => (None, false),
    };
    let count = match amount {
        Some(Amount::Count(count)) => usize::try_from(count).unwrap_or(usize::MAX),
        _ => 0,
    };
    let rows = partition.rows;
    match unit {
        Unit::Rows => {
            return Ok(match (end, following) {
                (true, true) => row.saturating_add(count).saturating_add(1).min(rows),
                (true, false) => (row + 1).saturating_sub(count),
                (false, true) => row.saturating_add(count).min(rows),
                (false, false) => row.saturating_sub(count),
            });
        }
        Unit::Groups => {
            let peers = peers.expect("GROUPS frames build peer groups");
            let group = peers.group;
            // [first row of group start, first row after group end)
            return Ok(match (end, following) {
                (false, true) => peers.start(group.saturating_add(count), rows),
                (false, false) => peers.start(group.saturating_sub(count), rows),
                (true, true) => peers.start(group.saturating_add(count).saturating_add(1), rows),
                (true, false) => {
                    if group >= count {
                        peers.start(group - count + 1, rows)
                    } else {
                        0
                    }
                }
            });
        }
        Unit::Range => {}
    }
    let column = width + spec.partition.len();
    let keys = spec.order.len();
    if bound == Bound::Current || is_sql_null(current.column(column).as_ref(), local) {
        // Peers: rows before `row` that differ precede the peer group; rows
        // after `row` that are equal belong to it. A NULL key's offset bounds
        // are its NULL peer group in both references.
        return if end {
            seeker.seek(partition, row, partition.rows, |batch, index| {
                equal_keys(batch, index, current, local, column, keys)
            })
        } else {
            seeker.seek(partition, 0, row, |batch, index| {
                !equal_keys(batch, index, current, local, column, keys)
            })
        };
    }
    let descending = spec.order[0].1;
    let nulls_first = spec.order[0].2;
    let target = range_target(
        spec,
        amount.expect("RANGE offset bound"),
        following,
        current.column(column),
        local,
    )?;
    seeker.seek(partition, 0, partition.rows, |batch, index| {
        let array = batch.column(column);
        if is_sql_null(array.as_ref(), index) {
            nulls_first
        } else {
            let order = target.compare(array, index);
            if descending {
                if end { order.is_ge() } else { order.is_gt() }
            } else if end {
                order.is_le()
            } else {
                order.is_lt()
            }
        }
    })
}

/// The frame [begin, end) of each row, in row order.
struct Bounds {
    unit: Unit,
    start: Bound,
    end: Bound,
    lower: Seeker,
    upper: Seeker,
}
impl Bounds {
    #[allow(clippy::too_many_arguments)]
    fn frame(
        &mut self,
        partition: &Partition,
        row: usize,
        batch: &RecordBatch,
        local: usize,
        spec: &Specification,
        width: usize,
        peers: Option<&Peers>,
    ) -> Result<(usize, usize)> {
        let peers_only = spec.order.is_empty() && self.unit != Unit::Rows;
        let begin = if peers_only && self.start == Bound::Current {
            0
        } else {
            boundary(
                &mut self.lower,
                partition,
                self.start,
                false,
                self.unit,
                row,
                batch,
                local,
                spec,
                width,
                peers,
            )?
        };
        let end = if peers_only && self.end == Bound::Current {
            partition.rows
        } else {
            boundary(
                &mut self.upper,
                partition,
                self.end,
                true,
                self.unit,
                row,
                batch,
                local,
                spec,
                width,
                peers,
            )?
        };
        // A frame whose start follows its end is empty. Both bounds are
        // monotone in the row, so their minimum is too.
        Ok((begin.min(end), end))
    }
}

/// The frame of one row after EXCLUDE, as up to three ordered row ranges
/// (before the excluded rows, the kept current row of EXCLUDE TIES, after
/// them). Each range's bounds are monotone in the row.
fn segments(
    begin: usize,
    end: usize,
    row: usize,
    exclude: Exclude,
    peers: Option<&Peers>,
    rows: usize,
) -> [(usize, usize); 3] {
    let (low, high) = match exclude {
        Exclude::NoOthers => return [(begin, end), (row, row), (end, end)],
        Exclude::CurrentRow => (row, row + 1),
        Exclude::Group | Exclude::Ties => {
            let peers = peers.expect("EXCLUDE GROUP/TIES build peer groups");
            (
                peers.start(peers.group, rows),
                peers.start(peers.group + 1, rows),
            )
        }
    };
    let first = (begin, low.min(end).max(begin));
    let kept = if exclude == Exclude::Ties && begin <= row && row < end {
        (row, row + 1)
    } else {
        (row, row)
    };
    let last = (high.max(begin).min(end), end);
    [first, kept, last]
}

fn float_fold_refusal(limit: u64) -> String {
    format!(
        "unsupported: Float SUM/AVG window frames need more than {limit} sequential frame-row additions in one partition (frame not starting at UNBOUNDED PRECEDING over inexactly summable Float values)"
    )
}

fn fold_refusal(limit: u64) -> String {
    format!(
        "unsupported: window frames need more than {limit} frame-row accumulations in one partition (statistical, DISTINCT, ordered-set or EXCLUDE frames are folded per frame)"
    )
}

/// A FIFO whose allocated capacity and owned payload stay funded.
struct Queue<T> {
    items: VecDeque<T>,
    credit: Reservation,
    capacity_bytes: usize,
    payload: fn(&T) -> usize,
}
impl<T> Queue<T> {
    fn new(budget: &Budget, payload: fn(&T) -> usize) -> Result<Self> {
        Ok(Self {
            items: VecDeque::new(),
            credit: budget.reserve(0)?,
            capacity_bytes: 0,
            payload,
        })
    }
    fn push(&mut self, item: T) -> Result<()> {
        let budget = self.credit.budget();
        if self.items.len() == self.items.capacity() {
            let grow = self.items.capacity().max(16);
            let extra = budget.reserve(grow.saturating_mul(std::mem::size_of::<T>()))?;
            self.items.reserve_exact(grow);
            self.credit.absorb(extra);
            self.capacity_bytes = self
                .capacity_bytes
                .saturating_add(grow * std::mem::size_of::<T>());
            let actual = self.items.capacity() * std::mem::size_of::<T>();
            if actual > self.capacity_bytes {
                self.credit
                    .absorb(budget.reserve(actual - self.capacity_bytes)?);
                self.capacity_bytes = actual;
            }
        }
        let payload = (self.payload)(&item);
        if payload != 0 {
            self.credit.absorb(budget.reserve(payload)?);
        }
        self.items.push_back(item);
        Ok(())
    }
    fn release(&mut self, item: T) {
        let payload = (self.payload)(&item);
        drop(item);
        if payload != 0 {
            let kept = self.credit.bytes().saturating_sub(payload);
            self.credit.release_excess(kept);
        }
    }
    fn pop_front(&mut self) {
        if let Some(item) = self.items.pop_front() {
            self.release(item);
        }
    }
    fn pop_back(&mut self) {
        if let Some(item) = self.items.pop_back() {
            self.release(item);
        }
    }
}

fn no_payload<T>(_: &T) -> usize {
    0
}
fn key_payload(item: &(usize, Key)) -> usize {
    item.1.payload_bytes()
}

/// Decompose a finite non-zero Float: |v| = odd * 2^low and |v| < 2^high.
fn float_parts(value: f64) -> (u64, i32, i32) {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, power) = if exponent == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), exponent - 1075)
    };
    let zeros = mantissa.trailing_zeros() as i32;
    (
        mantissa >> zeros,
        power + zeros,
        power + 64 - mantissa.leading_zeros() as i32,
    )
}

/// The exact scale 2^low when every partial sum of every frame is provably an
/// exactly representable finite double: all inputs are multiples of 2^low and
/// the partition's absolute total is below 2^(low+53) and below 2^1023.
fn exact_float_scale(range: FloatRange) -> Option<i32> {
    if range.count == 0 {
        return Some(0);
    }
    let log = 64 - (range.count - 1).leading_zeros() as i32;
    (log + range.high <= 53 + range.low && log + range.high <= 1023).then_some(range.low)
}

fn power_of_two(exponent: i32) -> f64 {
    if exponent >= -1022 {
        f64::from_bits(((exponent + 1023) as u64) << 52)
    } else {
        f64::from_bits(1 << (exponent + 1074))
    }
}

fn extreme_key(array: &ArrayRef, ty: &Type, row: usize) -> Key {
    match ty {
        Type::Int | Type::Decimal(_) => Key::Num(numeric(array).value(row)),
        Type::Text => Key::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .into(),
        ),
        Type::Bool => Key::Bool(crate::expr::boolean(array).value(row)),
        Type::Date => Key::Date(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row),
        ),
        Type::Timestamp | Type::TimestampTz => {
            Key::Timestamp(crate::expr::timestamp::value(array.as_ref(), row))
        }
        // The engine's canonical Float key (signed zeros equal, one NaN),
        // ordered by `extreme_order` exactly like grouped MIN/MAX.
        Type::Float => Key::Float(
            crate::expr::numeric_compare::canonical(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            )
            .to_bits(),
        ),
        // Binary MIN/MAX windows are refused at bind time.
        _ => unreachable!("bound MIN/MAX window type"),
    }
}

/// MIN/MAX order: SQL Float total order (NaN above +infinity, as grouped
/// MIN/MAX and ORDER BY use); other key types compare as before.
fn extreme_order(a: &Key, b: &Key) -> std::cmp::Ordering {
    match (a, b) {
        (Key::Float(a), Key::Float(b)) => {
            crate::expr::numeric_compare::real_order(f64::from_bits(*a), f64::from_bits(*b))
        }
        _ => a.cmp(b),
    }
}

/// Running INTEGER totals after each row, (row + 1, total), for the frame's
/// maximum (first) and minimum (second) running totals.
type Peaks = Queue<(usize, i256)>;

enum Method {
    Count,
    /// Int/Decimal (and all-NULL) SUM/AVG as exact wide totals.
    Exact {
        peaks: Option<(Peaks, Peaks)>,
    },
    /// Prefix frames: the canonical sequential fold itself.
    FloatPrefix {
        sum: f64,
    },
    /// Exactly summable inputs as integer multiples of 2^scale.
    FloatExact {
        scale: i32,
    },
    /// Inexact non-prefix frames: retained values folded per frame.
    FloatFold {
        values: Queue<(usize, f64)>,
        work: u64,
        limit: u64,
    },
    Extreme {
        minimum: bool,
        queue: Queue<(usize, Key)>,
    },
    /// Every other aggregate (DISTINCT, statistical, ARG_MAX/ARG_MIN,
    /// ordered-set) and every EXCLUDE frame: the grouped accumulator itself.
    /// Prefix frames extend one state; other frames refold each frame's rows
    /// in frame order, under the same work limit as inexact Float frames.
    General {
        prefix: bool,
        state: Box<WindowAccumulator>,
        aggregate: Box<bind::Agg>,
        work: u64,
        limit: u64,
    },
}

enum Output {
    Counts(Vec<u64>),
    Decimals(Vec<Option<i128>>),
    Floats(Vec<Option<f64>>),
    Keys(Vec<Key>, Reservation),
    Arrays(Vec<ArrayRef>, Vec<Reservation>),
}

/// True when the aggregate needs the general accumulator.
fn general(aggregate: &bind::Agg) -> bool {
    aggregate.distinct
        || matches!(
            aggregate.function,
            Aggregate::Moment { .. }
                | Aggregate::ArgExtreme { .. }
                | Aggregate::Quantile(_)
                | Aggregate::Mode { .. }
        )
}

/// Running totals for the rows that entered ([0]) and left ([1]) the frame.
struct Totals {
    method: Method,
    prefix: bool,
    average: bool,
    scale: i8,
    ty: Type,
    input: Option<Type>,
    count: [u64; 2],
    sum: [i256; 2],
    scaled: [i128; 2],
    // NaN, +Infinity, -Infinity
    special: [[u64; 3]; 2],
    output: Output,
    budget: Budget,
}
impl Totals {
    fn new(
        aggregate: &bind::Agg,
        partition: &Partition,
        prefix: bool,
        exclude: Exclude,
        budget: &Budget,
    ) -> Result<Self> {
        let input = aggregate.input.as_ref().map(|e| e.ty.clone());
        let float = input.as_ref() == Some(&Type::Float);
        let average = matches!(aggregate.function, Aggregate::Avg);
        let method = match aggregate.function {
            _ if general(aggregate) || exclude != Exclude::NoOthers => Method::General {
                prefix: prefix
                    && exclude == Exclude::NoOthers
                    && !WindowAccumulator::consuming(aggregate),
                state: Box::new(WindowAccumulator::new(aggregate, budget)?),
                aggregate: Box::new(aggregate.clone()),
                work: 0,
                limit: FLOAT_FOLD_BASE
                    .max(FLOAT_FOLD_PER_ROW.saturating_mul(partition.rows as u64)),
            },
            Aggregate::Count => Method::Count,
            Aggregate::Min | Aggregate::Max => Method::Extreme {
                minimum: matches!(aggregate.function, Aggregate::Min),
                queue: Queue::new(budget, key_payload)?,
            },
            // STRING_AGG and statistical aggregates OVER are refused when the
            // window is bound.
            Aggregate::StringAgg(_) => {
                return Err("internal: STRING_AGG is not a window aggregate".into());
            }
            Aggregate::Moment { .. }
            | Aggregate::ArgExtreme { .. }
            | Aggregate::Quantile(_)
            | Aggregate::Mode { .. } => unreachable!("general window aggregate"),
            Aggregate::Sum | Aggregate::Avg if float => {
                if prefix {
                    Method::FloatPrefix { sum: 0.0 }
                } else if let Some(scale) = exact_float_scale(partition.floats) {
                    Method::FloatExact { scale }
                } else {
                    Method::FloatFold {
                        values: Queue::new(budget, no_payload)?,
                        work: 0,
                        limit: FLOAT_FOLD_BASE
                            .max(FLOAT_FOLD_PER_ROW.saturating_mul(partition.rows as u64)),
                    }
                }
            }
            Aggregate::Sum | Aggregate::Avg => Method::Exact {
                // Every running INTEGER total of a frame must stay within
                // precision 38. No prefix can leave it unless the partition's
                // absolute total does.
                peaks: (input.as_ref() == Some(&Type::Int)
                    && partition.integer_magnitude > PRECISION_LIMIT as u128)
                    .then(|| -> Result<_> {
                        Ok((
                            Queue::new(budget, no_payload)?,
                            Queue::new(budget, no_payload)?,
                        ))
                    })
                    .transpose()?,
            },
        };
        let ty = physical_output_type(&aggregate.ty, true);
        let output = match &method {
            Method::General { .. } => Output::Arrays(Vec::new(), Vec::new()),
            Method::Count => Output::Counts(Vec::new()),
            Method::Extreme { .. } => Output::Keys(Vec::new(), budget.reserve(0)?),
            Method::Exact { .. } if !average => Output::Decimals(Vec::new()),
            _ => Output::Floats(Vec::new()),
        };
        Ok(Self {
            method,
            prefix,
            average,
            scale: match &input {
                Some(Type::Decimal(scale)) => *scale,
                _ => 0,
            },
            ty,
            input,
            count: [0; 2],
            sum: [i256::ZERO; 2],
            scaled: [0; 2],
            special: [[0; 3]; 2],
            output,
            budget: budget.clone(),
        })
    }

    fn needs_leaving_values(&self) -> bool {
        matches!(
            self.method,
            Method::Count | Method::Exact { .. } | Method::FloatExact { .. }
        )
    }

    /// Fold rows [local, local+n) of one chunk; `base` is the partition row of
    /// `local`. Side 0 enters the frame, side 1 leaves it (inverse methods).
    fn absorb(
        &mut self,
        array: Option<&ArrayRef>,
        local: usize,
        n: usize,
        base: usize,
        side: usize,
    ) -> Result<()> {
        if let Method::General {
            state, aggregate, ..
        } = &mut self.method
        {
            // Prefix frames only: entering rows extend the one state.
            let value = array
                .map(|a| Value::view(a.slice(local, n), &self.budget))
                .transpose()?;
            return state.update(aggregate, value.as_ref(), n);
        }
        let Some(array) = array else {
            // COUNT(*)
            self.count[side] += n as u64;
            return Ok(());
        };
        if array.data_type() == &arrow::datatypes::DataType::Null {
            return Ok(());
        }
        match &mut self.method {
            Method::Count => {
                let valid = array
                    .logical_nulls()
                    .map_or(n, |nulls| n - nulls.slice(local, n).null_count());
                self.count[side] += valid as u64;
            }
            Method::Exact { peaks } => {
                let numbers = numeric(array);
                for index in local..local + n {
                    if numbers.is_null(index) {
                        continue;
                    }
                    self.sum[side] = aggregate_sum::add(self.sum[side], numbers.value(index))?;
                    self.count[side] += 1;
                    if side == 0
                        && let Some((high, low)) = peaks
                    {
                        let entry = (base + index - local + 1, self.sum[0]);
                        while high.items.back().is_some_and(|old| old.1 <= entry.1) {
                            high.pop_back();
                        }
                        high.push(entry)?;
                        while low.items.back().is_some_and(|old| old.1 >= entry.1) {
                            low.pop_back();
                        }
                        low.push(entry)?;
                    }
                }
            }
            Method::FloatPrefix { sum } => {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for index in local..local + n {
                    if values.is_null(index) {
                        continue;
                    }
                    let number = values.value(index);
                    // A running total that overflows is +/-infinity like
                    // DuckDB 1.5.6 and grouped SUM/AVG (decision 2026-10-17).
                    let next = *sum + number;
                    *sum = if next.is_nan() { f64::NAN } else { next };
                    self.count[0] += 1;
                }
            }
            Method::FloatExact { scale } => {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for index in local..local + n {
                    if values.is_null(index) {
                        continue;
                    }
                    let number = values.value(index);
                    self.count[side] += 1;
                    if number.is_nan() {
                        self.special[side][0] += 1;
                    } else if number.is_infinite() {
                        self.special[side][if number > 0.0 { 1 } else { 2 }] += 1;
                    } else if number != 0.0 {
                        let (odd, low, _) = float_parts(number);
                        let magnitude = i128::from(odd) << (low - *scale);
                        self.scaled[side] += if number < 0.0 { -magnitude } else { magnitude };
                    }
                }
            }
            Method::FloatFold { values: queue, .. } => {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for index in local..local + n {
                    if values.is_valid(index) {
                        queue.push((base + index - local, values.value(index)))?;
                    }
                }
            }
            Method::General { .. } => unreachable!("handled above"),
            Method::Extreme { minimum, queue } => {
                let ty = self.input.as_ref().unwrap();
                for index in local..local + n {
                    if is_sql_null(array.as_ref(), index) {
                        continue;
                    }
                    let key = extreme_key(array, ty, index);
                    let better = |a: &Key, b: &Key| {
                        let order = extreme_order(a, b);
                        if *minimum {
                            order.is_lt()
                        } else {
                            order.is_gt()
                        }
                    };
                    if self.prefix {
                        // Same strict replacement as the grouped accumulator.
                        if queue.items.front().is_none_or(|old| better(&key, &old.1)) {
                            queue.pop_front();
                            queue.push((base + index - local, key))?;
                        }
                        continue;
                    }
                    while queue.items.back().is_some_and(|old| !better(&old.1, &key)) {
                        queue.pop_back();
                    }
                    queue.push((base + index - local, key))?;
                }
            }
        }
        Ok(())
    }

    /// Drop retained entries that precede the frame start.
    fn expire(&mut self, begin: usize) {
        match &mut self.method {
            Method::Exact {
                peaks: Some((high, low)),
            } => {
                // Running totals after rows (begin, end] bound the frame's prefixes.
                while high.items.front().is_some_and(|old| old.0 <= begin) {
                    high.pop_front();
                }
                while low.items.front().is_some_and(|old| old.0 <= begin) {
                    low.pop_front();
                }
            }
            Method::FloatFold { values, .. } => {
                while values.items.front().is_some_and(|old| old.0 < begin) {
                    values.pop_front();
                }
            }
            Method::Extreme { queue, .. } => {
                while queue.items.front().is_some_and(|old| old.0 < begin) {
                    queue.pop_front();
                }
            }
            _ => {}
        }
    }

    fn emit(&mut self, width: usize, ctx: &Context<'_>) -> Result<()> {
        let count = self.count[0] - self.count[1];
        match (&mut self.method, &mut self.output) {
            (Method::Count, Output::Counts(out)) => out.push(count),
            (Method::Exact { peaks }, out) => {
                if let Some((high, low)) = peaks {
                    // A running total of this frame is (total after row j) less
                    // (total before the frame), exactly as the from-start fold.
                    let before = self.sum[1];
                    let limit = i256::from_i128(PRECISION_LIMIT);
                    if high
                        .items
                        .front()
                        .is_some_and(|peak| peak.1 - before > limit)
                        || low
                            .items
                            .front()
                            .is_some_and(|peak| before - peak.1 > limit)
                    {
                        return Err("decimal precision 38 overflow".into());
                    }
                }
                let sum = self.sum[0] - self.sum[1];
                match out {
                    Output::Floats(out) => out.push(if count == 0 {
                        None
                    } else {
                        Some(crate::numeric_float::average(sum, self.scale, count)?)
                    }),
                    Output::Decimals(out) => out.push(if count == 0 {
                        None
                    } else {
                        Some(aggregate_sum::finish(sum)?)
                    }),
                    _ => unreachable!(),
                }
            }
            (Method::FloatPrefix { sum }, Output::Floats(out)) => {
                out.push((count != 0).then(|| {
                    if self.average {
                        *sum / count as f64
                    } else {
                        *sum
                    }
                }))
            }
            (Method::FloatExact { scale }, Output::Floats(out)) => {
                let special = [0, 1, 2].map(|i| self.special[0][i] - self.special[1][i]);
                let sum = if special[0] != 0 || (special[1] != 0 && special[2] != 0) {
                    f64::NAN
                } else if special[1] != 0 {
                    f64::INFINITY
                } else if special[2] != 0 {
                    f64::NEG_INFINITY
                } else {
                    // |scaled| < 2^53 and the product is representable: exact.
                    (self.scaled[0] - self.scaled[1]) as f64 * power_of_two(*scale)
                };
                out.push((count != 0).then(|| {
                    if self.average {
                        sum / count as f64
                    } else {
                        sum
                    }
                }));
            }
            (
                Method::FloatFold {
                    values,
                    work,
                    limit,
                },
                Output::Floats(out),
            ) => {
                // The pre-pass already refused partitions above the limit.
                *work = work.saturating_add(width as u64);
                if *work > *limit {
                    return Err(float_fold_refusal(*limit).into());
                }
                let mut sum = 0.0_f64;
                for (step, (_, number)) in values.items.iter().enumerate() {
                    if step % 65_536 == 65_535 {
                        ctx.options.cancellation.check()?;
                    }
                    // Overflow is +/-infinity (decision 2026-10-17); each
                    // frame refolds, so a later frame can be finite again.
                    let next = sum + number;
                    sum = if next.is_nan() { f64::NAN } else { next };
                }
                let count = values.items.len();
                out.push((count != 0).then(|| {
                    if self.average {
                        sum / count as f64
                    } else {
                        sum
                    }
                }));
            }
            (
                Method::General {
                    state, aggregate, ..
                },
                Output::Arrays(out, credits),
            ) => {
                let (array, credit) = state.finish(aggregate)?;
                out.push(array);
                credits.push(credit);
            }
            (Method::Extreme { queue, .. }, Output::Keys(out, credit)) => {
                let key = queue
                    .items
                    .front()
                    .map_or(Key::Null, |entry| entry.1.clone());
                let payload = key.payload_bytes();
                if payload != 0 {
                    credit.absorb(ctx.budget.reserve(payload)?);
                }
                out.push(key);
            }
            _ => unreachable!("window method/output pairing"),
        }
        Ok(())
    }

    /// One output array for a whole chunk.
    fn array(&mut self, rows: usize, ctx: &Context<'_>) -> Result<(ArrayRef, Reservation)> {
        let ty = self.ty.clone();
        if ty == Type::Null {
            self.clear();
            return Ok((new_null_array(&ty.arrow(), rows), ctx.budget.reserve(0)?));
        }
        Ok(match &mut self.output {
            Output::Counts(out) => {
                let credit = ctx
                    .budget
                    .reserve(rows.saturating_mul(16).saturating_add(1024))?;
                (count_array(out.drain(..), &ty)?, credit)
            }
            Output::Decimals(out) => {
                let credit = ctx
                    .budget
                    .reserve(rows.saturating_mul(16).saturating_add(1024))?;
                let array = Decimal128Array::from(std::mem::take(out))
                    .with_precision_and_scale(38, ty.scale()?)
                    .map_err(|e| e.to_string())?;
                (Arc::new(array) as ArrayRef, credit)
            }
            Output::Floats(out) => {
                let credit = ctx
                    .budget
                    .reserve(rows.saturating_mul(8).saturating_add(1024))?;
                (
                    Arc::new(Float64Array::from(std::mem::take(out))) as ArrayRef,
                    credit,
                )
            }
            Output::Arrays(out, credits) => {
                let bytes = out
                    .iter()
                    .fold(0_usize, |n, a| n.saturating_add(a.get_array_memory_size()));
                let mut credit = ctx
                    .budget
                    .reserve(bytes.saturating_mul(2).saturating_add(1024))?;
                let refs = out.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                let array = if refs.is_empty() {
                    new_null_array(&ty.arrow(), 0)
                } else {
                    arrow::compute::concat(&refs).map_err(|e| e.to_string())?
                };
                out.clear();
                credits.clear();
                credit.release_excess(array.get_array_memory_size().saturating_add(1024));
                (array, credit)
            }
            Output::Keys(out, payload) => {
                let bytes = payload.bytes();
                let credit = ctx.budget.reserve(
                    bytes
                        .saturating_add(rows.saturating_mul(16))
                        .saturating_add(1024),
                )?;
                let array = keys_array(out, &ty)?;
                out.clear();
                payload.release_excess(0);
                (array, credit)
            }
        })
    }

    fn clear(&mut self) {
        match &mut self.output {
            Output::Counts(out) => out.clear(),
            Output::Decimals(out) => out.clear(),
            Output::Floats(out) => out.clear(),
            Output::Arrays(out, credits) => {
                out.clear();
                credits.clear();
            }
            Output::Keys(out, payload) => {
                out.clear();
                payload.release_excess(0);
            }
        }
    }
}

/// A frame's aggregate, updated as its monotone start/end advance.
struct Window {
    totals: Totals,
    argument: Option<usize>,
    entering: Cursor,
    leaving: Cursor,
    entered: usize,
    left: usize,
}
impl Window {
    fn advance(
        &mut self,
        partition: &Partition,
        begin: usize,
        end: usize,
        ctx: &Context<'_>,
    ) -> Result<()> {
        while self.entered < end {
            ctx.options.cancellation.check()?;
            let (_, loaded, local) = self.entering.at(partition, self.entered)?;
            let batch = loaded.batch();
            let n = (end - self.entered).min(batch.num_rows() - local);
            let array = self.argument.map(|column| batch.column(column));
            self.totals.absorb(array, local, n, self.entered, 0)?;
            self.entered += n;
        }
        if self.totals.prefix {
            return Ok(());
        }
        if self.totals.needs_leaving_values() {
            while self.left < begin {
                ctx.options.cancellation.check()?;
                let (_, loaded, local) = self.leaving.at(partition, self.left)?;
                let batch = loaded.batch();
                let n = (begin - self.left).min(batch.num_rows() - local);
                let array = self.argument.map(|column| batch.column(column));
                self.totals.absorb(array, local, n, self.left, 1)?;
                self.left += n;
            }
        }
        self.left = self.left.max(begin);
        self.totals.expire(begin);
        Ok(())
    }

    fn refolds(&self) -> bool {
        matches!(self.totals.method, Method::General { prefix: false, .. })
    }

    /// Fold one frame's rows (EXCLUDE segments in order) into a fresh
    /// accumulator and finalize it.
    fn refold(
        &mut self,
        partition: &Partition,
        segments: &[(usize, usize)],
        pool: &mut Pool,
        ctx: &Context<'_>,
    ) -> Result<()> {
        let Method::General {
            state,
            aggregate,
            work,
            limit,
            ..
        } = &mut self.totals.method
        else {
            unreachable!("refold requires the general accumulator")
        };
        let rows = segments
            .iter()
            .fold(0_u64, |n, (a, b)| n.saturating_add((b - a) as u64));
        // The pre-pass already refused partitions above the limit.
        *work = work.saturating_add(rows);
        if *work > *limit {
            return Err(fold_refusal(*limit).into());
        }
        **state = WindowAccumulator::new(aggregate, &ctx.budget)?;
        for &(first, last) in segments {
            let mut row = first;
            while row < last {
                ctx.options.cancellation.check()?;
                let (loaded, local) = pool.at(partition, row)?;
                let batch = loaded.batch();
                let n = (last - row).min(batch.num_rows() - local);
                let value = self
                    .argument
                    .map(|column| Value::view(batch.column(column).slice(local, n), &ctx.budget))
                    .transpose()?;
                state.update(aggregate, value.as_ref(), n)?;
                row += n;
            }
        }
        self.totals.emit(0, ctx)
    }
}

/// A few chunk readers for frames refolded row by row: a frame spanning
/// adjacent chunks keeps each of them loaded across consecutive rows.
#[derive(Default)]
struct Pool {
    cursors: [Cursor; 3],
    clock: [u64; 3],
    tick: u64,
}
impl Pool {
    fn at(&mut self, partition: &Partition, row: usize) -> Result<(&Loaded, usize)> {
        self.tick += 1;
        let hit = self.cursors.iter().position(|cursor| {
            cursor.loaded.as_ref().is_some_and(|(index, _)| {
                let chunk = &partition.chunks[*index];
                chunk.start <= row && row < chunk.start + chunk.rows
            })
        });
        let slot = hit.unwrap_or_else(|| {
            (0..self.cursors.len())
                .min_by_key(|&i| self.clock[i])
                .unwrap()
        });
        self.clock[slot] = self.tick;
        let (_, loaded, local) = self.cursors[slot].at(partition, row)?;
        Ok((loaded, local))
    }
}

/// Non-NULL rows of one monotone frame segment, for IGNORE NULLS.
struct NonNulls {
    queue: Queue<usize>,
    cursor: Cursor,
    entered: usize,
}
impl NonNulls {
    fn new(budget: &Budget) -> Result<Self> {
        Ok(Self {
            queue: Queue::new(budget, no_payload)?,
            cursor: Cursor::default(),
            entered: 0,
        })
    }
    fn advance(
        &mut self,
        partition: &Partition,
        (first, last): (usize, usize),
        argument: usize,
        ctx: &Context<'_>,
    ) -> Result<()> {
        self.entered = self.entered.max(first);
        while self.entered < last {
            ctx.options.cancellation.check()?;
            let (_, loaded, local) = self.cursor.at(partition, self.entered)?;
            let array = loaded.batch().column(argument);
            let n = (last - self.entered).min(array.len() - local);
            for index in 0..n {
                if !is_sql_null(array.as_ref(), local + index) {
                    self.queue.push(self.entered + index)?;
                }
            }
            self.entered += n;
        }
        while self.queue.items.front().is_some_and(|row| *row < first) {
            self.queue.pop_front();
        }
        Ok(())
    }
}

/// The row a FIRST_VALUE/LAST_VALUE/NTH_VALUE selects from the frame segments.
fn choose(
    kind: ValueKind,
    segments: &[(usize, usize); 3],
    non_nulls: Option<&[NonNulls; 3]>,
) -> Option<usize> {
    if let Some(non_nulls) = non_nulls {
        return match kind {
            ValueKind::First => non_nulls
                .iter()
                .find_map(|n| n.queue.items.front().copied()),
            ValueKind::Last => non_nulls
                .iter()
                .rev()
                .find_map(|n| n.queue.items.back().copied()),
            ValueKind::Nth(nth) => {
                let mut skip = nth as usize - 1;
                for n in non_nulls {
                    if skip < n.queue.items.len() {
                        return Some(n.queue.items[skip]);
                    }
                    skip -= n.queue.items.len();
                }
                None
            }
            _ => unreachable!("positional value function"),
        };
    }
    match kind {
        ValueKind::First => segments.iter().find(|(a, b)| b > a).map(|(a, _)| *a),
        ValueKind::Last => segments
            .iter()
            .rev()
            .find(|(a, b)| b > a)
            .map(|(_, b)| b - 1),
        ValueKind::Nth(nth) => {
            let mut skip = nth as usize - 1;
            for (a, b) in segments {
                if skip < b - a {
                    return Some(a + skip);
                }
                skip -= b - a;
            }
            None
        }
        _ => unreachable!("positional value function"),
    }
}

/// Selected FIRST/LAST/NTH values for one output chunk, gathered with a single
/// interleave. The selected row is monotone, so its cursor reads forward.
#[derive(Default)]
struct Picker {
    cursor: Cursor,
    sources: Vec<Loaded>,
    current: Option<usize>,
    picks: Vec<(usize, usize)>,
    payload: usize,
}
impl Picker {
    fn select(&mut self, partition: &Partition, row: Option<usize>, argument: usize) -> Result<()> {
        let Some(row) = row else {
            self.picks.push((0, 0));
            return Ok(());
        };
        let (chunk, loaded, local) = self.cursor.at(partition, row)?;
        if self.current != Some(chunk) {
            self.sources.push(loaded.clone());
            self.current = Some(chunk);
        }
        let array = loaded.batch().column(argument);
        self.payload = self.payload.saturating_add(
            if let Some(text) = array.as_any().downcast_ref::<StringArray>() {
                text.value_length(local) as usize
            } else if let Some(bytes) = array.as_any().downcast_ref::<BinaryArray>() {
                bytes.value_length(local) as usize
            } else {
                0
            },
        );
        self.picks.push((self.sources.len(), local));
        Ok(())
    }

    fn array(
        &mut self,
        argument: usize,
        data_type: &arrow::datatypes::DataType,
        ctx: &Context<'_>,
    ) -> Result<(ArrayRef, Reservation)> {
        let mut credit = ctx.budget.reserve(
            self.payload
                .saturating_add(self.picks.len().saturating_mul(40))
                .saturating_add(4096),
        )?;
        let null = new_null_array(data_type, 1);
        let mut arrays: Vec<&dyn Array> = vec![null.as_ref()];
        arrays.extend(
            self.sources
                .iter()
                .map(|source| source.batch().column(argument).as_ref()),
        );
        let result = arrow::compute::interleave(&arrays, &self.picks).map_err(|e| e.to_string())?;
        credit.release_excess(result.get_array_memory_size().saturating_add(256));
        self.sources.clear();
        self.current = None;
        self.picks.clear();
        self.payload = 0;
        Ok((result, credit))
    }
}

impl Partition {
    fn finish(
        self,
        spec: &Specification,
        restore: &mut Sorter,
        schema: &SchemaRef,
        width: usize,
        ctx: &Context<'_>,
    ) -> Result<()> {
        let argument = width + spec.partition.len() + spec.order.len();
        let (unit, start, end, exclude) = match spec.frame {
            Frame::Partition => (
                Unit::Range,
                Bound::Unbounded,
                Bound::Unbounded,
                Exclude::NoOthers,
            ),
            Frame::Range => (
                Unit::Range,
                Bound::Unbounded,
                Bound::Current,
                Exclude::NoOthers,
            ),
            Frame::Rows => (
                Unit::Rows,
                Bound::Unbounded,
                Bound::Current,
                Exclude::NoOthers,
            ),
            Frame::Bounded {
                unit,
                start,
                end,
                exclude,
            } => (unit, start, end, exclude),
        };
        let value_kind = spec.value.as_ref().map(|value| value.kind);
        let ranked = matches!(
            value_kind,
            Some(ValueKind::PercentRank | ValueKind::CumeDist)
        );
        let ntile = match value_kind {
            Some(ValueKind::Ntile(buckets)) => Some(buckets),
            _ => None,
        };
        // NTILE, PERCENT_RANK and CUME_DIST ignore frames, like the references.
        let framed = ntile.is_none() && !ranked;
        let mut peers = if ranked
            || (framed
                && (unit == Unit::Groups || matches!(exclude, Exclude::Group | Exclude::Ties)))
        {
            Some(Peers::build(&self, spec, width, ctx)?)
        } else {
            None
        };
        let mut window = spec
            .aggregate
            .as_ref()
            .map(|aggregate| -> Result<Window> {
                Ok(Window {
                    totals: Totals::new(
                        aggregate,
                        &self,
                        start == Bound::Unbounded,
                        exclude,
                        &ctx.budget,
                    )?,
                    argument: aggregate.input.as_ref().map(|_| argument),
                    entering: Cursor::default(),
                    leaving: Cursor::default(),
                    entered: 0,
                    left: 0,
                })
            })
            .transpose()?;
        let mut bounds = Bounds {
            unit,
            start,
            end,
            lower: Seeker::default(),
            upper: Seeker::default(),
        };
        let fold_limit = match window.as_ref().map(|w| &w.totals.method) {
            Some(Method::FloatFold { limit, .. }) => Some((*limit, true)),
            Some(Method::General {
                prefix: false,
                limit,
                ..
            }) => Some((*limit, false)),
            _ => None,
        };
        if let Some((limit, float)) = fold_limit {
            // Refuse before any quadratic work: one forward pass sums widths.
            let mut probe = Bounds {
                unit,
                start,
                end,
                lower: Seeker::default(),
                upper: Seeker::default(),
            };
            let mut reader = Cursor::default();
            let mut work = 0_u64;
            for row in 0..self.rows {
                ctx.options.cancellation.check()?;
                if let Some(peers) = &mut peers {
                    peers.advance(row);
                }
                let (_, loaded, local) = reader.at(&self, row)?;
                let (begin, finish) = probe.frame(
                    &self,
                    row,
                    loaded.batch(),
                    local,
                    spec,
                    width,
                    peers.as_ref(),
                )?;
                let parts = segments(begin, finish, row, exclude, peers.as_ref(), self.rows);
                work = parts
                    .iter()
                    .fold(work, |n, (a, b)| n.saturating_add((b - a) as u64));
                if work > limit {
                    return Err(if float {
                        float_fold_refusal(limit)
                    } else {
                        fold_refusal(limit)
                    }
                    .into());
                }
            }
            if let Some(peers) = &mut peers {
                peers.group = 0;
            }
        }
        let mut non_nulls = if spec.value.as_ref().is_some_and(|v| v.ignore_nulls) {
            Some([
                NonNulls::new(&ctx.budget)?,
                NonNulls::new(&ctx.budget)?,
                NonNulls::new(&ctx.budget)?,
            ])
        } else {
            None
        };
        let mut pool = Pool::default();
        let mut picker = Picker::default();
        let mut output = Cursor::default();
        for chunk in 0..self.chunks.len() {
            let first = self.chunks[chunk].start;
            let batch = output.at(&self, first)?.1.batch().clone();
            let length = batch.num_rows();
            let _metadata = ctx
                .budget
                .reserve(length.saturating_mul(48).saturating_add(2048))?;
            let mut tiles = Vec::new();
            let mut fractions = Vec::new();
            for local in 0..length {
                ctx.options.cancellation.check()?;
                let row = first + local;
                if let Some(peers) = &mut peers {
                    peers.advance(row);
                }
                // NTILE is partition-wide and, like the references, ignores a frame.
                if let Some(buckets) = ntile {
                    let n = self.rows as u64;
                    let row = row as u64;
                    let small = n / buckets;
                    let large_rows = (n % buckets) * (small + 1);
                    let tile = if row < large_rows {
                        row / (small + 1) + 1
                    } else {
                        (n % buckets) + (row - large_rows) / small + 1
                    };
                    tiles.push(tile as i64);
                    continue;
                }
                if ranked {
                    // PERCENT_RANK = (rank - 1) / (rows - 1), 0 for one row;
                    // CUME_DIST = rows through the last peer / rows (both
                    // references compute these in binary64).
                    let peers = peers.as_ref().unwrap();
                    let n = self.rows;
                    fractions.push(if value_kind == Some(ValueKind::CumeDist) {
                        peers.start(peers.group + 1, n) as f64 / n as f64
                    } else if n > 1 {
                        peers.start(peers.group, n) as f64 / (n - 1) as f64
                    } else {
                        0.0
                    });
                    continue;
                }
                let (begin, finish) =
                    bounds.frame(&self, row, &batch, local, spec, width, peers.as_ref())?;
                let parts = segments(begin, finish, row, exclude, peers.as_ref(), self.rows);
                if let Some(window) = &mut window {
                    if window.refolds() {
                        window.refold(&self, &parts, &mut pool, ctx)?;
                    } else {
                        window.advance(&self, begin, finish, ctx)?;
                        window.totals.emit(finish - begin, ctx)?;
                    }
                } else {
                    let value = spec.value.as_ref().unwrap();
                    if let Some(non_nulls) = &mut non_nulls {
                        for (tracker, part) in non_nulls.iter_mut().zip(parts) {
                            tracker.advance(&self, part, argument, ctx)?;
                        }
                    }
                    let selected = choose(value.kind, &parts, non_nulls.as_ref());
                    picker.select(&self, selected, argument)?;
                }
            }
            let (values, _credit) = if ntile.is_some() {
                let credit = ctx
                    .budget
                    .reserve(length.saturating_mul(8).saturating_add(1024))?;
                (Arc::new(Int64Array::from(tiles)) as ArrayRef, credit)
            } else if ranked {
                let credit = ctx
                    .budget
                    .reserve(length.saturating_mul(8).saturating_add(1024))?;
                (Arc::new(Float64Array::from(fractions)) as ArrayRef, credit)
            } else if let Some(window) = &mut window {
                window.totals.array(length, ctx)?
            } else {
                picker.array(argument, batch.column(argument).data_type(), ctx)?
            };
            let mut arrays = batch.columns()[..width].to_vec();
            arrays.push(values);
            arrays.push(batch.column(batch.num_columns() - 1).clone());
            restore
                .push(RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?)?;
        }
        Ok(())
    }
}

fn integer_key(array: &ArrayRef, row: usize) -> i128 {
    if let Some(a) = array.as_any().downcast_ref::<Decimal128Array>() {
        a.value(row)
    } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        a.value(row) as i128
    } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        a.value(row) as i128
    } else if let Some(a) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        a.value(row) as i128
    } else {
        array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row) as i128
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    node: &Node,
    spec: &Specification,
    sorter: Sorter,
    mut restore: Sorter,
    keyed_schema: SchemaRef,
    restore_schema: SchemaRef,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let width = node.schema.fields().len() - 1;
    let budget = ctx.budget.clone();
    let observed = spec
        .aggregate
        .as_ref()
        .and_then(|aggregate| aggregate.input.as_ref())
        .filter(|input| matches!(input.ty, Type::Int | Type::Float))
        .map(|input| {
            (
                width + spec.partition.len() + spec.order.len(),
                input.ty.clone(),
            )
        });
    let observed = observed.as_ref().map(|(column, ty)| (*column, ty));
    let mut partition = Partition::default();
    let mut previous: Option<(RecordBatch, Reservation)> = None;
    let metrics = sorter.finish(&mut |batch| {
        let mut first = 0;
        for row in 0..batch.num_rows() {
            ctx.options.cancellation.check()?;
            let old = if row == 0 {
                previous.as_ref().map(|(b, _)| (b, 0))
            } else {
                Some((&batch, row - 1))
            };
            if old.is_some_and(|(old, index)| {
                !equal_keys(old, index, &batch, row, width, spec.partition.len())
            }) {
                if row > first {
                    partition.push(batch.slice(first, row - first), observed, ctx)?;
                }
                std::mem::take(&mut partition).finish(
                    spec,
                    &mut restore,
                    &restore_schema,
                    width,
                    ctx,
                )?;
                first = row;
            }
        }
        if first < batch.num_rows() {
            partition.push(batch.slice(first, batch.num_rows() - first), observed, ctx)?;
            let mut credit = budget.reserve(
                batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(batch.num_columns() * 1024),
            )?;
            let indices = UInt64Array::from(vec![(batch.num_rows() - 1) as u64]);
            let arrays = batch
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| crate::Error::from(e.to_string())))
                .collect::<Result<Vec<_>>>()?;
            let last =
                RecordBatch::try_new(keyed_schema.clone(), arrays).map_err(|e| e.to_string())?;
            credit.release_excess(
                last.get_array_memory_size()
                    .saturating_add(batch.num_columns() * 256),
            );
            previous = Some((last, credit));
        }
        Ok(())
    })?;
    drop(previous);
    sort::merge_metrics(&mut ctx.metrics, metrics);
    partition.finish(spec, &mut restore, &restore_schema, width, ctx)?;
    let metrics = restore.finish(&mut |batch| {
        ctx.options.cancellation.check()?;
        let _metadata = budget.reserve((width + 1) * 256 + 1024)?;
        emit(
            RecordBatch::try_new(node.schema.clone(), batch.columns()[..width + 1].to_vec())
                .map_err(|e| e.to_string())?,
        )
    })?;
    sort::merge_metrics(&mut ctx.metrics, metrics);
    Ok(())
}
