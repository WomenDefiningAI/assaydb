//! Ordered-set aggregate state.
//!
//! MEDIAN/QUANTILE/PERCENTILE keep every non-NULL input value of each group
//! (8 bytes per INTEGER/DECIMAL value while the values fit 64 bits); MODE keeps
//! one count per distinct value. Both results depend only on the multiset of a
//! group's values, so they are identical for every worker count, batch size
//! and spill partitioning. All storage is charged to the query budget before
//! it is allocated; the external aggregation path splits groups across
//! partitions when a budget is too small, and a single group larger than the
//! budget fails with a typed `resource-limit:` error. Value groups are sorted
//! once, after the last update.
use super::*;
use crate::bind::Quantile;
use arrow::datatypes::i256;
use std::{borrow::Borrow, hash::Hash};

/// Keep this much released storage credit for later growth.
const SLACK: usize = 64 * 1024;

type Map<T> = hashbrown::HashMap<T, u64, ahash::RandomState>;
type Counted<T> = Vec<Option<Box<Map<T>>>>;

/// Every value of each group, for quantiles.
enum Values {
    /// INTEGER/DECIMAL while every value fits 64 bits.
    Small(Vec<Vec<i64>>),
    Num(Vec<Vec<i128>>),
    Float(Vec<Vec<f64>>),
    Date(Vec<Vec<i32>>),
    Bool(Vec<Vec<bool>>),
    Text(Vec<Vec<String>>),
}

/// Occurrences of each distinct value of each group, for MODE.
enum Counts {
    Num(Counted<i128>),
    /// Canonical REAL bits (one NaN, -0.0 stored as 0.0).
    Float(Counted<u64>),
    Date(Counted<i32>),
    /// [false, true] counts.
    Bool(Vec<[u64; 2]>),
    Text(Counted<String>),
}

enum Store {
    Values(Values),
    Counts(Counts),
}

pub(super) struct Cells {
    store: Store,
    // Unused, still reserved credit inside the caller's retained lease.
    slack: usize,
    finished: bool,
    // A REAL input held a NaN with a non-default payload (sign or bits).
    // DuckDB's MODE counts such NaNs as different values; PostgreSQL has a
    // single NaN. MODE refuses a group containing NaN after this.
    nan_payloads: bool,
}

/// DuckDB/PostgreSQL value order within one type: NaN after +infinity, -0.0
/// equal to 0.0 (both are canonicalized on input), text by bytes. The
/// canonical form is the engine's one (`numeric_compare::canonical`), so
/// `total_cmp` on canonical values is the SQL Float order.
fn canonical(n: f64) -> f64 {
    crate::expr::numeric_compare::canonical(n)
}

/// Funding helper shared by value vectors and count tables.
struct Funds<'a> {
    retained: &'a mut Reservation,
    slack: &'a mut usize,
}
impl Funds<'_> {
    /// Take `bytes` of credit, reserving a chunk from the budget if needed.
    fn take(&mut self, bytes: usize) -> Result<()> {
        if *self.slack < bytes {
            let budget = self.retained.budget();
            let lease = match budget.reserve(bytes.max(SLACK)) {
                Ok(lease) => lease,
                Err(_) => budget.reserve(bytes - *self.slack)?,
            };
            *self.slack += lease.bytes();
            self.retained.absorb(lease);
        }
        *self.slack -= bytes;
        Ok(())
    }
    /// Return credit of freed (or unused) storage, keeping a bounded slack.
    fn give(&mut self, bytes: usize) {
        *self.slack += bytes;
        if *self.slack > 4 * SLACK {
            let keep = self.retained.bytes() - (*self.slack - SLACK);
            self.retained.release_excess(keep);
            *self.slack = SLACK;
        }
    }
}

/// Append with capacity funded first; the old buffer's credit is returned.
fn append<T>(cell: &mut Vec<T>, value: T, funds: &mut Funds<'_>) -> Result<()> {
    if cell.len() == cell.capacity() {
        let size = std::mem::size_of::<T>();
        let old = cell.capacity() * size;
        let capacity = cell.capacity().saturating_mul(2).max(4);
        let bytes = capacity
            .checked_mul(size)
            .ok_or("ordered-set capacity overflow")?;
        funds.take(bytes)?;
        cell.reserve_exact(capacity - cell.len());
        let actual = cell.capacity() * size;
        if actual > bytes {
            return Err("internal: ordered-set capacity exceeds its funding".into());
        }
        funds.give(old + bytes - actual);
    }
    cell.push(value);
    Ok(())
}

/// Bytes of a hashbrown table with room for `entries` (see heap.rs).
fn table_bytes(entries: usize, entry: usize) -> Result<usize> {
    let buckets = if entries < 4 {
        4
    } else if entries < 8 {
        8
    } else if entries < 15 {
        16
    } else {
        entries
            .checked_mul(8)
            .and_then(|n| (n / 7).checked_next_power_of_two())
            .ok_or("ordered-set capacity overflow")?
    };
    buckets
        .checked_mul(entry + 1)
        .and_then(|n| n.checked_add(16))
        .ok_or_else(|| "ordered-set capacity overflow".into())
}

/// Count one occurrence; a new distinct value funds its table slot and copy.
#[allow(clippy::box_collection)]
fn count<T, Q>(
    slot: &mut Option<Box<Map<T>>>,
    value: &Q,
    owned: impl Fn(&Q) -> T,
    payload: usize,
    funds: &mut Funds<'_>,
) -> Result<()>
where
    T: Hash + Eq + Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    if let Some(n) = slot.as_mut().and_then(|map| map.get_mut(value)) {
        *n += 1;
        return Ok(());
    }
    if slot.as_ref().is_none_or(|map| map.len() == map.capacity()) {
        let entries = slot.as_ref().map_or(1, |map| map.len() + 1);
        let boxed = if slot.is_none() {
            std::mem::size_of::<Map<T>>()
        } else {
            0
        };
        let bytes = table_bytes(entries, std::mem::size_of::<(T, u64)>())? + boxed;
        let old = slot.as_ref().map_or(0, |map| map.allocation_size());
        funds.take(bytes)?;
        let hasher = slot
            .as_ref()
            .map_or_else(ahash::RandomState::new, |map| map.hasher().clone());
        let mut next = Map::with_capacity_and_hasher(entries, hasher);
        let actual = next.allocation_size() + boxed;
        if actual > bytes {
            return Err("internal: ordered-set table exceeds its funding".into());
        }
        if let Some(map) = slot {
            next.extend(map.drain());
            **map = next; // The old buckets drop here.
        } else {
            *slot = Some(Box::new(next));
        }
        funds.give(old + bytes - actual);
    }
    if payload > 0 {
        funds.take(payload)?;
    }
    slot.as_mut().unwrap().insert(owned(value), 1);
    Ok(())
}

impl Cells {
    pub(super) fn new(function: &Aggregate, ty: &Type) -> Self {
        let store = if matches!(function, Aggregate::Mode { .. }) {
            Store::Counts(match ty {
                Type::Float => Counts::Float(Vec::new()),
                Type::Date => Counts::Date(Vec::new()),
                Type::Bool => Counts::Bool(Vec::new()),
                Type::Text => Counts::Text(Vec::new()),
                // Int, Decimal and an all-NULL input.
                _ => Counts::Num(Vec::new()),
            })
        } else {
            Store::Values(match ty {
                Type::Float => Values::Float(Vec::new()),
                Type::Date => Values::Date(Vec::new()),
                Type::Bool => Values::Bool(Vec::new()),
                Type::Text => Values::Text(Vec::new()),
                _ => Values::Small(Vec::new()),
            })
        };
        Self {
            store,
            slack: 0,
            finished: false,
            nan_payloads: false,
        }
    }
    pub(super) fn bytes_per_group(&self) -> usize {
        match &self.store {
            Store::Values(_) => std::mem::size_of::<Vec<u8>>(),
            Store::Counts(Counts::Bool(_)) => std::mem::size_of::<[u64; 2]>(),
            Store::Counts(_) => std::mem::size_of::<Option<Box<Map<u8>>>>(),
        }
    }
    pub(super) fn push(&mut self) {
        match &mut self.store {
            Store::Values(Values::Small(v)) => v.push(Vec::new()),
            Store::Values(Values::Num(v)) => v.push(Vec::new()),
            Store::Values(Values::Float(v)) => v.push(Vec::new()),
            Store::Values(Values::Date(v)) => v.push(Vec::new()),
            Store::Values(Values::Bool(v)) => v.push(Vec::new()),
            Store::Values(Values::Text(v)) => v.push(Vec::new()),
            Store::Counts(Counts::Num(v)) => v.push(None),
            Store::Counts(Counts::Float(v)) => v.push(None),
            Store::Counts(Counts::Date(v)) => v.push(None),
            Store::Counts(Counts::Bool(v)) => v.push([0, 0]),
            Store::Counts(Counts::Text(v)) => v.push(None),
        }
    }

    /// Store one batch's non-NULL values for their groups.
    pub(super) fn update(
        &mut self,
        value: &Value,
        groups: &[usize],
        retained: &mut Reservation,
    ) -> Result<()> {
        if self.finished {
            return Err("internal: ordered-set update after finalization".into());
        }
        let array = value.array.as_ref();
        if array.data_type() == &arrow::datatypes::DataType::Null {
            return Ok(());
        }
        let mut funds = Funds {
            retained,
            slack: &mut self.slack,
        };
        let valid = |row: usize| array.is_valid(row);
        match &mut self.store {
            Store::Values(values) => {
                if let Values::Small(cells) = values {
                    let numbers = numeric(&value.array);
                    if (0..groups.len())
                        .any(|row| valid(row) && i64::try_from(numbers.value(row)).is_err())
                    {
                        *values = Values::Num(widen(cells, &mut funds)?);
                    }
                }
                match values {
                    Values::Small(cells) => {
                        let numbers = numeric(&value.array);
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                // Checked above: every value of the batch fits.
                                let n = numbers.value(row) as i64;
                                append(&mut cells[*group], n, &mut funds)?;
                            }
                        }
                    }
                    Values::Num(cells) => {
                        let numbers = numeric(&value.array);
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                append(&mut cells[*group], numbers.value(row), &mut funds)?;
                            }
                        }
                    }
                    Values::Float(cells) => {
                        let numbers = reals(array)?;
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                let n = numbers.value(row);
                                append(&mut cells[*group], canonical(n), &mut funds)?;
                            }
                        }
                    }
                    Values::Date(cells) => {
                        let dates = dates(array)?;
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                append(&mut cells[*group], dates.value(row), &mut funds)?;
                            }
                        }
                    }
                    Values::Bool(cells) => {
                        let flags = crate::expr::boolean(&value.array);
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                append(&mut cells[*group], flags.value(row), &mut funds)?;
                            }
                        }
                    }
                    Values::Text(cells) => {
                        let text = texts(array)?;
                        for (row, group) in groups.iter().enumerate() {
                            if valid(row) {
                                let s = text.value(row);
                                // String::from allocates exactly the payload.
                                funds.take(s.len())?;
                                append(&mut cells[*group], String::from(s), &mut funds)?;
                            }
                        }
                    }
                }
            }
            Store::Counts(counts) => match counts {
                Counts::Num(cells) => {
                    let numbers = numeric(&value.array);
                    for (row, group) in groups.iter().enumerate() {
                        if valid(row) {
                            let n = numbers.value(row);
                            count(&mut cells[*group], &n, |n| *n, 0, &mut funds)?;
                        }
                    }
                }
                Counts::Float(cells) => {
                    let numbers = reals(array)?;
                    for (row, group) in groups.iter().enumerate() {
                        if valid(row) {
                            let n = numbers.value(row);
                            self.nan_payloads |= n.is_nan() && n.to_bits() != f64::NAN.to_bits();
                            let bits = canonical(n).to_bits();
                            count(&mut cells[*group], &bits, |n| *n, 0, &mut funds)?;
                        }
                    }
                }
                Counts::Date(cells) => {
                    let dates = dates(array)?;
                    for (row, group) in groups.iter().enumerate() {
                        if valid(row) {
                            let d = dates.value(row);
                            count(&mut cells[*group], &d, |d| *d, 0, &mut funds)?;
                        }
                    }
                }
                Counts::Bool(cells) => {
                    let flags = crate::expr::boolean(&value.array);
                    for (row, group) in groups.iter().enumerate() {
                        if valid(row) {
                            cells[*group][usize::from(flags.value(row))] += 1;
                        }
                    }
                }
                Counts::Text(cells) => {
                    let text = texts(array)?;
                    for (row, group) in groups.iter().enumerate() {
                        if valid(row) {
                            let s = text.value(row);
                            count(
                                &mut cells[*group],
                                s,
                                |s: &str| s.to_owned(),
                                s.len(),
                                &mut funds,
                            )?;
                        }
                    }
                }
            },
        }
        Ok(())
    }

    /// Sort every value group once, in place, after the last update.
    pub(super) fn finish(&mut self) {
        if self.finished {
            return;
        }
        if let Store::Values(values) = &mut self.store {
            match values {
                Values::Small(v) => v.iter_mut().for_each(|g| g.sort_unstable()),
                Values::Num(v) => v.iter_mut().for_each(|g| g.sort_unstable()),
                Values::Float(v) => v
                    .iter_mut()
                    .for_each(|g| g.sort_unstable_by(f64::total_cmp)),
                Values::Date(v) => v.iter_mut().for_each(|g| g.sort_unstable()),
                Values::Bool(v) => v.iter_mut().for_each(|g| g.sort_unstable()),
                Values::Text(v) => v.iter_mut().for_each(|g| g.sort_unstable()),
            }
        }
        self.finished = true;
    }

    /// Upper bound of the output payload bytes for groups in `range`.
    pub(super) fn output_payload(&self, range: std::ops::Range<usize>) -> usize {
        let longest = |lengths: &mut dyn Iterator<Item = usize>| lengths.max().unwrap_or(0);
        match &self.store {
            Store::Values(Values::Text(groups)) => groups[range]
                .iter()
                .map(|g| longest(&mut g.iter().map(String::len)))
                .fold(0usize, usize::saturating_add),
            Store::Counts(Counts::Text(groups)) => groups[range]
                .iter()
                .flatten()
                .map(|m| longest(&mut m.keys().map(String::len)))
                .fold(0usize, usize::saturating_add),
            _ => 0,
        }
    }

    pub(super) fn output(
        &self,
        function: &Aggregate,
        input: &Type,
        ty: &Type,
        range: std::ops::Range<usize>,
    ) -> Result<ArrayRef> {
        if !self.finished {
            return Err("internal: ordered-set output before finalization".into());
        }
        if *ty == Type::Null {
            return Ok(arrow::array::new_null_array(&ty.arrow(), range.len()));
        }
        let keys = match (function, &self.store) {
            (Aggregate::Mode { descending }, Store::Counts(counts)) => {
                self.modes(counts, *descending, range)
            }
            (Aggregate::Quantile(q), Store::Values(values)) if !q.continuous => {
                select(values, range, |n| discrete_index(q, n))
            }
            (Aggregate::Quantile(q), Store::Values(values)) => {
                return interpolate(values, q, input, range);
            }
            _ => return Err("internal: ordered-set state mismatch".into()),
        };
        keys_array(&keys?, ty)
    }

    /// MODE: the most frequent value; ties go to the first value in the
    /// ordering (the smallest, or the largest when descending).
    fn modes(
        &self,
        counts: &Counts,
        descending: bool,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Key>> {
        fn best<T>(
            groups: &[Option<Box<Map<T>>>],
            descending: bool,
            order: impl Fn(&T, &T) -> std::cmp::Ordering,
            key: impl Fn(&T) -> Key,
        ) -> Vec<Key> {
            groups
                .iter()
                .map(|map| {
                    map.as_ref()
                        .and_then(|map| {
                            map.iter().max_by(|(a, m), (b, n)| {
                                m.cmp(n).then_with(|| {
                                    let first = order(a, b);
                                    if descending { first } else { first.reverse() }
                                })
                            })
                        })
                        .map_or(Key::Null, |(value, _)| key(value))
                })
                .collect()
        }
        Ok(match counts {
            Counts::Num(g) => best(&g[range], descending, i128::cmp, |v| Key::Num(*v)),
            Counts::Float(g) => {
                let groups = &g[range];
                if self.nan_payloads
                    && groups
                        .iter()
                        .flatten()
                        .any(|map| map.contains_key(&f64::NAN.to_bits()))
                {
                    return Err("unsupported: MODE over REAL values with different NaN payloads (DuckDB counts each payload as a separate value; PostgreSQL has one NaN)".into());
                }
                best(
                    groups,
                    descending,
                    |a, b| f64::from_bits(*a).total_cmp(&f64::from_bits(*b)),
                    |v| Key::Float(*v),
                )
            }
            Counts::Date(g) => best(&g[range], descending, i32::cmp, |v| Key::Date(*v)),
            Counts::Text(g) => best(&g[range], descending, String::cmp, |v| Key::Text(v.clone())),
            Counts::Bool(g) => g[range]
                .iter()
                .map(|[f, t]| match (*f, *t) {
                    (0, 0) => Key::Null,
                    (f, t) if f > t || (f == t && !descending) => Key::Bool(false),
                    _ => Key::Bool(true),
                })
                .collect(),
        })
    }
}

fn reals(array: &dyn Array) -> Result<&Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| "internal: ordered-set REAL input".into())
}
fn dates(array: &dyn Array) -> Result<&Date32Array> {
    array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| "internal: ordered-set DATE input".into())
}
fn texts(array: &dyn Array) -> Result<&StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "internal: ordered-set text input".into())
}

/// Convert 64-bit value groups to 128 bits when a wider value arrives.
fn widen(cells: &mut [Vec<i64>], funds: &mut Funds<'_>) -> Result<Vec<Vec<i128>>> {
    let mut wide = Vec::with_capacity(cells.len());
    for cell in cells {
        let bytes = cell
            .len()
            .checked_mul(std::mem::size_of::<i128>())
            .ok_or("ordered-set capacity overflow")?;
        funds.take(bytes)?;
        let mut next = Vec::with_capacity(cell.len());
        next.extend(cell.iter().map(|v| i128::from(*v)));
        let old = cell.capacity() * std::mem::size_of::<i64>();
        *cell = Vec::new();
        funds.give(old + bytes - next.capacity() * std::mem::size_of::<i128>());
        wide.push(next);
    }
    Ok(wide)
}

/// One existing value per group, by its index in the ascending group.
fn select(
    values: &Values,
    range: std::ops::Range<usize>,
    index: impl Fn(usize) -> Result<Option<usize>>,
) -> Result<Vec<Key>> {
    fn keys<T>(
        groups: &[Vec<T>],
        index: &dyn Fn(usize) -> Result<Option<usize>>,
        key: impl Fn(&T) -> Key,
    ) -> Result<Vec<Key>> {
        groups
            .iter()
            .map(|g| Ok(index(g.len())?.map_or(Key::Null, |i| key(&g[i]))))
            .collect()
    }
    match values {
        Values::Small(g) => keys(&g[range], &index, |v| Key::Num(i128::from(*v))),
        Values::Num(g) => keys(&g[range], &index, |v| Key::Num(*v)),
        Values::Float(g) => keys(&g[range], &index, |v| Key::Float(v.to_bits())),
        Values::Date(g) => keys(&g[range], &index, |v| Key::Date(*v)),
        Values::Bool(g) => keys(&g[range], &index, |v| Key::Bool(*v)),
        Values::Text(g) => keys(&g[range], &index, |v| Key::Text(v.clone())),
    }
}

fn interpolate(
    values: &Values,
    q: &Quantile,
    input: &Type,
    range: std::ops::Range<usize>,
) -> Result<ArrayRef> {
    fn each<T, V>(
        groups: &[Vec<T>],
        q: &Quantile,
        f: impl Fn(&Interpolation, &T, &T) -> Result<V>,
    ) -> Result<Vec<Option<V>>> {
        groups
            .iter()
            .map(|g| {
                interpolation(q, g.len())?
                    .map(|p| f(&p, &g[p.low], &g[p.high]))
                    .transpose()
            })
            .collect()
    }
    let real =
        |reals: Vec<Option<f64>>| -> Result<ArrayRef> { Ok(Arc::new(Float64Array::from(reals))) };
    match (values, input) {
        (Values::Float(g), _) => real(each(&g[range], q, |p, lo, hi| p.real(q, *lo, *hi))?),
        // DuckDB casts INTEGER to DOUBLE before interpolating.
        (Values::Small(g), Type::Int) => real(each(&g[range], q, |p, lo, hi| {
            p.real(q, *lo as f64, *hi as f64)
        })?),
        (Values::Num(g), Type::Int) => real(each(&g[range], q, |p, lo, hi| {
            p.real(q, *lo as f64, *hi as f64)
        })?),
        (Values::Small(g), Type::Decimal(scale)) => real(each(&g[range], q, |p, lo, hi| {
            p.decimal(i128::from(*lo), i128::from(*hi), *scale)
        })?),
        (Values::Num(g), Type::Decimal(scale)) => {
            real(each(&g[range], q, |p, lo, hi| p.decimal(*lo, *hi, *scale))?)
        }
        _ => Err("internal: interpolated ordered-set input type".into()),
    }
}

/// The ascending index of a group position counted in the ORDER BY direction.
fn ascending(q: &Quantile, n: usize, position: usize) -> usize {
    if q.descending {
        n - 1 - position
    } else {
        position
    }
}

/// PERCENTILE_DISC/QUANTILE_DISC: the first value whose cumulative position
/// reaches the fraction. DuckDB computes ceil(n * q) exactly for a DECIMAL
/// fraction and in binary64 otherwise.
fn discrete_index(q: &Quantile, n: usize) -> Result<Option<usize>> {
    if n == 0 {
        return Ok(None);
    }
    let floored: usize = match q.fraction.exact {
        Some((digits, scale)) if scale > 0 => {
            let scaling = crate::types::factor(scale)?;
            let n = i128::try_from(n).map_err(|_| "ordered-set group size overflow")?;
            let scaled_n = n.checked_mul(scaling).ok_or("quantile index overflow")?;
            let scaled_q = n.checked_mul(digits).ok_or("quantile index overflow")?;
            usize::try_from((scaled_n - scaled_q) / scaling).map_err(|_| "quantile index range")?
        }
        _ => (n as f64 - n as f64 * q.fraction.value).floor() as usize,
    };
    let position = n.saturating_sub(floored).max(1) - 1;
    if q.postgres {
        // PostgreSQL: rownum = ceil(percentile * rowcount) in binary64.
        let rownum = (q.fraction.value * n as f64).ceil();
        let postgres = if rownum <= 1.0 {
            0
        } else {
            rownum as usize - 1
        };
        if postgres != position {
            return Err(format!(
                "unsupported: PERCENTILE_DISC({}) over {n} rows: DuckDB selects row {} and PostgreSQL row {} (binary64 rounding of the fraction)",
                q.fraction.value,
                position + 1,
                postgres + 1
            )
            .into());
        }
    }
    Ok(Some(ascending(q, n, position)))
}

/// Positions of the two neighbours and the interpolation weight.
struct Interpolation {
    low: usize,
    high: usize,
    weight: f64,
}
fn interpolation(q: &Quantile, n: usize) -> Result<Option<Interpolation>> {
    if n == 0 {
        return Ok(None);
    }
    // Both references: RN = (n - 1) * q in binary64, FRN = floor, CRN = ceil.
    let rn = (n - 1) as f64 * q.fraction.value;
    let (frn, crn) = (rn.floor(), rn.ceil());
    Ok(Some(Interpolation {
        low: ascending(q, n, frn as usize),
        high: ascending(q, n, crn as usize),
        weight: rn - frn,
    }))
}
impl Interpolation {
    /// DOUBLE result (REAL input, or INTEGER cast to DOUBLE first).
    fn real(&self, q: &Quantile, lo: f64, hi: f64) -> Result<f64> {
        if self.low == self.high {
            return Ok(lo);
        }
        let d = self.weight;
        // DuckDB 1.5.6 InterpolateOperator<double>.
        let duckdb = lo * (1.0 - d) + hi * d;
        if q.postgres {
            // PostgreSQL float8_lerp: lo + d * (hi - lo). The two differ in
            // the last bits only, except where one of them overflows.
            let postgres = lo + d * (hi - lo);
            let class = |v: f64| {
                (
                    v.is_nan(),
                    v.is_infinite() && v > 0.0,
                    v.is_infinite() && v < 0.0,
                )
            };
            if class(duckdb) != class(postgres) {
                return Err(format!(
                    "unsupported: PERCENTILE_CONT between {lo} and {hi}: DuckDB returns {duckdb} and PostgreSQL {postgres}"
                )
                .into());
            }
        }
        Ok(duckdb)
    }

    /// DOUBLE result of a DECIMAL interpolation (unscaled integers at
    /// `scale`): lo + d * (hi - lo) computed exactly from the decimal values
    /// and the binary64 weight d, then rounded once to the nearest DOUBLE
    /// (ties to even). PostgreSQL instead converts each value to DOUBLE first
    /// and interpolates in binary64, so its last bit can differ.
    fn decimal(&self, lo: i128, hi: i128, scale: i8) -> Result<f64> {
        if self.low == self.high {
            return decimal_ratio(i256::from_i128(lo), scale, 0)
                .ok_or_else(|| "internal: DECIMAL to DOUBLE conversion".into());
        }
        if let Some(exact) = exact_interpolation(lo, hi, self.weight, scale) {
            return Ok(exact);
        }
        // Only weights with more than 120 fractional bits (fractions below
        // about 1e-20) reach this: PostgreSQL's binary64 interpolation.
        let to_real = |n: i128| {
            decimal_ratio(i256::from_i128(n), scale, 0)
                .ok_or_else(|| crate::Error::from("internal: DECIMAL to DOUBLE conversion"))
        };
        let (l, h) = (to_real(lo)?, to_real(hi)?);
        Ok(l + self.weight * (h - l))
    }
}

/// Exact lo + d * (hi - lo) over 10^scale, rounded once to binary64. `None`
/// when d has more than 120 fractional bits or the result would be subnormal.
fn exact_interpolation(lo: i128, hi: i128, d: f64, scale: i8) -> Option<f64> {
    if !(d > 0.0 && d < 1.0 && d.is_normal()) {
        return None;
    }
    // d = m * 2^-e with m odd.
    let bits = d.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = (bits & ((1 << 52) - 1)) | (1 << 52);
    let zeros = mantissa.trailing_zeros();
    let m = mantissa >> zeros;
    let e = u32::try_from(1075 - exponent - zeros as i32).ok()?;
    if e > 120 {
        return None;
    }
    // N = lo * 2^e + m * (hi - lo); value = N / (10^scale * 2^e).
    let delta = i256::from_i128(hi).checked_sub(i256::from_i128(lo))?;
    let n = i256::from_i128(lo)
        .checked_mul(i256::from_i128(1) << (e as u8))?
        .checked_add(i256::from_i128(i128::from(m)).checked_mul(delta)?)?;
    decimal_ratio(n, scale, e)
}

/// Correctly rounded (to nearest, ties to even) binary64 value of
/// n / 10^scale / 2^shift. `None` if the result would not be a normal number.
fn decimal_ratio(n: i256, scale: i8, shift: u32) -> Option<f64> {
    if n == i256::ZERO {
        return Some(0.0);
    }
    let negative = n.is_negative();
    let a = n.checked_abs()?;
    let p = i256::from_i128(crate::types::factor(scale).ok()?);
    let length = |x: i256| 256 - x.leading_zeros();
    // Scale so the integer quotient has at least 55 significant bits.
    let k = (56 + length(p)).saturating_sub(length(a));
    let a = if k > 0 { a << (k as u8) } else { a };
    let (q, r) = (a.checked_div(p)?, a.checked_rem(p)?);
    let drop = length(q).checked_sub(54)?;
    let top = q >> (drop as u8);
    let sticky = (top << (drop as u8)) != q || r != i256::ZERO;
    let top = top.to_i128()? as u64;
    let (mut mantissa, round) = (top >> 1, top & 1 == 1);
    if round && (sticky || mantissa & 1 == 1) {
        mantissa += 1;
    }
    // value = mantissa * 2^power, with mantissa <= 2^53.
    let power = drop as i64 + 1 - i64::from(k) - i64::from(shift);
    if !(-1022..=1023 - 53).contains(&power) {
        return None;
    }
    let scale = f64::from_bits(((power + 1023) as u64) << 52);
    let value = mantissa as f64 * scale;
    Some(if negative { -value } else { value })
}
