//! Ordered row selections over funded decoded buffers. Kernels are compiled
//! against each batch's physical arrays, without normalizing or copying values.
use crate::{
    Result,
    expr::{Expr, Kind, Literal, Op},
    memory::{Budget, Reservation},
    parquet_scan::TransformedBatch,
    spill::Cancellation,
    types::Type,
};
use arrow::{array::*, datatypes::Schema, record_batch::RecordBatch};
use std::{cmp::Ordering, sync::Arc};

pub(crate) struct Rows {
    pub indices: Vec<u32>,
    _lease: Reservation,
}

pub(crate) struct Batch {
    pub batch: RecordBatch,
    pub rows: Option<Rows>,
    pub lease: Reservation,
    pub preserve_empty: bool,
}
impl Batch {
    pub fn adopt(batch: RecordBatch, budget: &Budget) -> Result<Self> {
        let lease = budget.reserve(batch.get_array_memory_size())?;
        Ok(Self {
            batch,
            rows: None,
            lease,
            preserve_empty: false,
        })
    }
    pub fn from_output(output: TransformedBatch) -> Self {
        Self {
            batch: output.batch,
            rows: None,
            lease: output.lease,
            preserve_empty: output.preserve_empty,
        }
    }
    pub fn len(&self) -> usize {
        self.rows
            .as_ref()
            .map_or(self.batch.num_rows(), |r| r.indices.len())
    }
    /// Only the materialization boundary gathers. Parent and index reservations
    /// survive every allocation, including a failure partway through gathering.
    pub fn materialize(self, budget: &Budget, cancel: &Cancellation) -> Result<TransformedBatch> {
        cancel.check()?;
        let Some(rows) = self.rows else {
            return Ok(TransformedBatch {
                batch: self.batch,
                lease: self.lease,
                preserve_empty: self.preserve_empty,
            });
        };
        let indices = UInt32Array::from(rows.indices);
        let bytes = self.batch.columns().iter().try_fold(256usize, |n, a| {
            n.checked_add(crate::join_filter::gather_bytes(a, &indices)?)
                .ok_or_else(|| "selection gather size overflow".to_string())
        })?;
        let lease = budget.reserve(bytes)?;
        let arrays = self
            .batch
            .columns()
            .iter()
            .map(|a| {
                cancel.check()?;
                arrow::compute::take(a, &indices, None)
                    .map_err(|e| crate::Error::from(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.batch.schema(),
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )
        .map_err(|e| crate::Error::from(e.to_string()))?;
        Ok(TransformedBatch {
            batch,
            lease,
            preserve_empty: self.preserve_empty,
        })
    }
}

// This deliberately excludes arithmetic on columns, casts, floating conversions,
// general LIKE and mixed-scale column comparisons. Those keep Expr::eval intact.
fn operand(e: &Expr, schema: &Schema, scale: Option<i8>) -> bool {
    if let Some(scale) = scale {
        if matches!(e.kind, Kind::Literal(Literal::Null)) {
            return true;
        }
        if let Kind::Column(i) = e.kind {
            return e.ty.scale().ok() == Some(scale)
                && schema
                    .fields()
                    .get(i)
                    .is_some_and(|f| Type::from_field(f).ok().as_ref() == Some(&e.ty));
        }
        return crate::scan_predicate::numeric_constant(e).is_some_and(|n| {
            e.ty.scale()
                .ok()
                .is_some_and(|old| crate::types::rescale(n, old, scale).is_ok())
        });
    }
    match &e.kind {
        Kind::Column(i) => {
            matches!(e.ty, Type::Date | Type::Text | Type::Bool | Type::Null)
                && schema
                    .fields()
                    .get(*i)
                    .is_some_and(|f| Type::from_field(f).ok().as_ref() == Some(&e.ty))
        }
        Kind::Literal(Literal::Date(_) | Literal::Text(_) | Literal::Bool(_) | Literal::Null) => {
            true
        }
        Kind::Substring(a, _, _) => {
            e.ty == Type::Text && a.ty == Type::Text && operand(a, schema, None)
        }
        _ => false,
    }
}
fn comparison_scale(a: &Expr, b: &Expr) -> Option<i8> {
    a.ty.scale()
        .ok()
        .zip(b.ty.scale().ok())
        .map(|(a, b)| a.max(b))
}
pub(crate) fn supported(e: &Expr, schema: &Schema) -> bool {
    match &e.kind {
        Kind::Binary(Op::And | Op::Or, a, b) => supported(a, schema) && supported(b, schema),
        Kind::Not(a) => supported(a, schema),
        Kind::Binary(Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge, a, b) => {
            let scale = comparison_scale(a, b);
            (scale.is_some() || a.ty == b.ty)
                && operand(a, schema, scale)
                && operand(b, schema, scale)
        }
        Kind::IsNull(a, _) => operand(a, schema, a.ty.scale().ok()),
        Kind::Like(a, b, _) => {
            a.ty == Type::Text
                && operand(a, schema, None)
                && matches!(&b.kind, Kind::Literal(Literal::Text(s)) if !s.contains('_') && s.strip_suffix('%').is_some_and(|s| !s.contains('%')))
        }
        _ => e.ty == Type::Bool && operand(e, schema, None),
    }
}
fn descriptors(e: &Expr) -> Option<usize> {
    let child = match &e.kind {
        Kind::Binary(_, a, b) | Kind::Like(a, b, _) => {
            descriptors(a)?.checked_add(descriptors(b)?)?
        }
        Kind::Not(a) | Kind::IsNull(a, _) | Kind::Substring(a, _, _) | Kind::Neg(a) => {
            descriptors(a)?
        }
        Kind::Literal(Literal::Text(s)) => s.len(),
        _ => 0,
    };
    child.checked_add(256)
}
pub(crate) struct Program {
    terms: Vec<Term>,
    workspace: usize,
    _lease: Reservation,
}

enum Term {
    Expression(Expr),
    // Inclusive bounds use i64 during compilation so DATE's extreme values
    // and strict comparisons never overflow when intersecting ranges.
    DateRange { column: usize, low: i64, high: i64 },
}

fn date_bound(e: &Expr) -> Option<(usize, i64, i64)> {
    let Kind::Binary(op, a, b) = &e.kind else {
        return None;
    };
    let (Kind::Column(column), Kind::Literal(Literal::Date(value))) = (&a.kind, &b.kind) else {
        return None;
    };
    if a.ty != Type::Date {
        return None;
    }
    let (min, max, value) = (i64::from(i32::MIN), i64::from(i32::MAX), i64::from(*value));
    let (low, high) = match op {
        Op::Eq => (value, value),
        Op::Lt => (min, value - 1),
        Op::Le => (min, value),
        Op::Gt => (value + 1, max),
        Op::Ge => (value, max),
        _ => return None,
    };
    Some((*column, low, high))
}

/// An immediate gather pays for selections only when a cheap date guard can
/// reduce subsequent predicate work. Other standalone filters keep Arrow's
/// existing vector evaluator; lanes can benefit from deferred gathers as well.
pub(crate) fn has_date_guard(e: &Expr) -> bool {
    match &e.kind {
        Kind::Binary(Op::And, a, b) => has_date_guard(a) || has_date_guard(b),
        _ => date_bound(e).is_some(),
    }
}

fn append_terms(e: &Expr, terms: &mut Vec<Term>) {
    if let Kind::Binary(Op::And, a, b) = &e.kind {
        append_terms(a, terms);
        append_terms(b, terms);
    } else if let Some((column, low, high)) = date_bound(e) {
        if let Some(Term::DateRange {
            low: old_low,
            high: old_high,
            ..
        }) = terms
            .iter_mut()
            .find(|t| matches!(t, Term::DateRange { column: old, .. } if *old == column))
        {
            *old_low = (*old_low).max(low);
            *old_high = (*old_high).min(high);
        } else {
            terms.push(Term::DateRange { column, low, high });
        }
    } else {
        terms.push(Term::Expression(e.clone()));
    }
}

fn term_count(e: &Expr) -> Option<usize> {
    match &e.kind {
        Kind::Binary(Op::And, a, b) => term_count(a)?.checked_add(term_count(b)?),
        _ => Some(1),
    }
}

impl Program {
    pub fn new(expression: &Expr, schema: &Schema, budget: &Budget) -> Result<Option<Self>> {
        if !supported(expression, schema) {
            return Ok(None);
        }
        let count = term_count(expression).ok_or("selection term count overflow")?;
        let workspace = count
            .checked_mul(std::mem::size_of::<Term>())
            .and_then(|bytes| bytes.checked_add(descriptors(expression)?))
            .ok_or("selection descriptor size overflow")?;
        let lease = budget.reserve(workspace)?;
        let mut terms = Vec::with_capacity(count);
        append_terms(expression, &mut terms);
        // All terms have passed totality admission. Reordering top-level AND
        // cannot change WHERE's TRUE set, including NULLs, or expose an error.
        // Do not reorder inside OR/NOT, whose full Kleene value is required.
        terms.sort_unstable_by_key(|term| match term {
            Term::DateRange { .. } => 0,
            Term::Expression(Expr {
                kind: Kind::Binary(_, a, _),
                ..
            }) if matches!(a.ty, Type::Int | Type::Decimal(_) | Type::Date) => 1,
            _ => 2,
        });
        Ok(Some(Self {
            terms,
            workspace,
            _lease: lease,
        }))
    }
    pub fn select(
        &self,
        mut batch: Batch,
        budget: &Budget,
        cancel: &Cancellation,
    ) -> Result<Batch> {
        cancel.check()?;
        if batch.batch.num_rows() > u32::MAX as usize {
            return Err("selection exceeds u32 rows".into());
        }
        let _workspace = budget.reserve(self.workspace)?;
        let mut initial = batch.rows.is_none();
        let mut rows = match batch.rows.take() {
            Some(rows) => rows,
            None => {
                let lease = budget.reserve(
                    batch
                        .batch
                        .num_rows()
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(256))
                        .ok_or("selection size overflow")?,
                )?;
                Rows {
                    indices: Vec::with_capacity(batch.batch.num_rows()),
                    _lease: lease,
                }
            }
        };
        for term in &self.terms {
            let kernel = match term {
                Term::Expression(e) => Kernel::compile(e, &batch.batch),
                Term::DateRange { column, low, high } => Kernel::DateRange(
                    batch.batch.column(*column).as_any().downcast_ref().unwrap(),
                    *low,
                    *high,
                ),
            };
            if initial {
                kernel.select_all(batch.batch.num_rows(), &mut rows.indices, cancel)?;
                initial = false;
            } else {
                kernel.retain(&mut rows.indices, cancel)?;
            }
            if rows.indices.is_empty() {
                break;
            }
        }
        if rows.indices.len() != batch.batch.num_rows() {
            batch.rows = Some(rows);
        }
        Ok(batch)
    }
}

// All references honor Arrow's slice offsets and validity. Text values remain
// borrowed, including SUBSTRING: character boundaries are shared with Expr::eval.
enum Value<'a> {
    Small(&'a Int32Array),
    Int(&'a Int64Array),
    Wide(&'a Decimal128Array),
    Date(&'a Date32Array),
    Text(&'a StringArray),
    Bool(&'a BooleanArray),
    Number(i128),
    String(&'a str),
    Boolean(bool),
    Null,
    Substring(Box<Self>, usize, usize),
}
impl<'a> Value<'a> {
    fn bind(e: &'a Expr, batch: &'a RecordBatch, scale: Option<i8>) -> Self {
        if let Kind::Column(i) = e.kind {
            let a = batch.column(i);
            use arrow::datatypes::DataType;
            return match a.data_type() {
                DataType::Int32 => Self::Small(a.as_any().downcast_ref().unwrap()),
                DataType::Int64 => Self::Int(a.as_any().downcast_ref().unwrap()),
                DataType::Decimal128(_, _) => Self::Wide(a.as_any().downcast_ref().unwrap()),
                DataType::Date32 => Self::Date(a.as_any().downcast_ref().unwrap()),
                DataType::Utf8 => Self::Text(a.as_any().downcast_ref().unwrap()),
                DataType::Boolean => Self::Bool(a.as_any().downcast_ref().unwrap()),
                DataType::Null => Self::Null,
                _ => unreachable!("admitted selection column"),
            };
        }
        if matches!(e.kind, Kind::Literal(Literal::Null)) {
            return Self::Null;
        }
        if let Some(scale) = scale {
            let n = crate::scan_predicate::numeric_constant(e).unwrap();
            return Self::Number(crate::types::rescale(n, e.ty.scale().unwrap(), scale).unwrap());
        }
        match &e.kind {
            Kind::Literal(Literal::Date(d)) => Self::Number(i128::from(*d)),
            Kind::Literal(Literal::Text(s)) => Self::String(s),
            Kind::Literal(Literal::Bool(b)) => Self::Boolean(*b),
            Kind::Substring(a, start, len) => {
                Self::Substring(Box::new(Self::bind(a, batch, None)), *start, *len)
            }
            _ => unreachable!("admitted selection operand"),
        }
    }
    fn number(&self, row: usize) -> Option<i128> {
        match self {
            Self::Small(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            Self::Int(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            Self::Wide(a) => a.is_valid(row).then(|| a.value(row)),
            Self::Date(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            Self::Number(n) => Some(*n),
            _ => None,
        }
    }
    fn text(&self, row: usize) -> Option<&str> {
        match self {
            Self::Text(a) => a.is_valid(row).then(|| a.value(row)),
            Self::String(s) => Some(s),
            Self::Substring(a, start, len) => {
                a.text(row).map(|s| crate::expr::substring(s, *start, *len))
            }
            _ => None,
        }
    }
    fn boolean(&self, row: usize) -> Option<bool> {
        match self {
            Self::Bool(a) => a.is_valid(row).then(|| a.value(row)),
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Text(_) | Self::String(_) | Self::Substring(..) => self.text(row).is_none(),
            Self::Bool(_) | Self::Boolean(_) => self.boolean(row).is_none(),
            _ => self.number(row).is_none(),
        }
    }
}
enum Kernel<'a> {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Compare(Op, Value<'a>, Value<'a>, Type),
    Membership(Value<'a>, Vec<&'a str>),
    Prefix(Value<'a>, &'a str, bool),
    IsNull(Value<'a>, bool),
    Bool(Value<'a>),
    DateRange(&'a Date32Array, i64, i64),
}
impl<'a> Kernel<'a> {
    fn compile(e: &'a Expr, batch: &'a RecordBatch) -> Self {
        if matches!(e.kind, Kind::Binary(Op::Or, _, _)) {
            let mut common = None;
            let mut count = 0;
            if crate::expr::string_terms(e, &mut common, &mut |_| count += 1) && count >= 2 {
                let mut members = Vec::with_capacity(count);
                crate::expr::string_terms(e, &mut None, &mut |s| members.push(s));
                return Self::Membership(Value::bind(common.unwrap(), batch, None), members);
            }
        }
        match &e.kind {
            Kind::Binary(Op::And, a, b) => Self::And(
                Box::new(Self::compile(a, batch)),
                Box::new(Self::compile(b, batch)),
            ),
            Kind::Binary(Op::Or, a, b) => Self::Or(
                Box::new(Self::compile(a, batch)),
                Box::new(Self::compile(b, batch)),
            ),
            Kind::Not(a) => Self::Not(Box::new(Self::compile(a, batch))),
            Kind::Binary(op, a, b) => Self::Compare(
                *op,
                Value::bind(a, batch, comparison_scale(a, b)),
                Value::bind(b, batch, comparison_scale(a, b)),
                a.ty.clone(),
            ),
            Kind::IsNull(a, negated) => {
                Self::IsNull(Value::bind(a, batch, a.ty.scale().ok()), *negated)
            }
            Kind::Like(a, b, negated) => {
                let Kind::Literal(Literal::Text(s)) = &b.kind else {
                    unreachable!()
                };
                Self::Prefix(
                    Value::bind(a, batch, None),
                    s.strip_suffix('%').unwrap(),
                    *negated,
                )
            }
            _ => Self::Bool(Value::bind(e, batch, None)),
        }
    }
    fn eval(&self, row: usize) -> Option<bool> {
        match self {
            Self::And(a, b) => match a.eval(row) {
                Some(false) => Some(false),
                Some(true) => b.eval(row),
                None => (b.eval(row) == Some(false)).then_some(false),
            },
            Self::Or(a, b) => match a.eval(row) {
                Some(true) => Some(true),
                Some(false) => b.eval(row),
                None => (b.eval(row) == Some(true)).then_some(true),
            },
            Self::Not(a) => a.eval(row).map(|b| !b),
            Self::Bool(a) => a.boolean(row),
            Self::DateRange(a, low, high) => a.is_valid(row).then(|| {
                let value = i64::from(a.value(row));
                *low <= value && value <= *high
            }),
            Self::IsNull(a, negated) => Some(a.is_null(row) != *negated),
            Self::Prefix(a, prefix, negated) => {
                a.text(row).map(|s| s.starts_with(prefix) != *negated)
            }
            Self::Membership(a, members) => a.text(row).map(|s| members.contains(&s)),
            Self::Compare(op, a, b, ty) => {
                let order = match ty {
                    Type::Text => a.text(row)?.cmp(b.text(row)?),
                    Type::Bool => a.boolean(row)?.cmp(&b.boolean(row)?),
                    _ => a.number(row)?.cmp(&b.number(row)?),
                };
                Some(match op {
                    Op::Eq => order == Ordering::Equal,
                    Op::Ne => order != Ordering::Equal,
                    Op::Lt => order == Ordering::Less,
                    Op::Le => order != Ordering::Greater,
                    Op::Gt => order == Ordering::Greater,
                    Op::Ge => order != Ordering::Less,
                    _ => unreachable!(),
                })
            }
        }
    }
    fn select_all(&self, len: usize, rows: &mut Vec<u32>, cancel: &Cancellation) -> Result<()> {
        if let Self::And(a, b) = self {
            a.select_all(len, rows, cancel)?;
            return b.retain(rows, cancel);
        }
        self.apply(rows, Some(len), cancel)
    }
    fn retain(&self, rows: &mut Vec<u32>, cancel: &Cancellation) -> Result<()> {
        // WHERE needs only TRUE. Top-level conjunctions may discard FALSE/NULL
        // immediately, but nested OR/NOT must retain the complete Kleene value.
        if let Self::And(a, b) = self {
            a.retain(rows, cancel)?;
            return b.retain(rows, cancel);
        }
        self.apply(rows, None, cancel)
    }
    fn apply(
        &self,
        rows: &mut Vec<u32>,
        initial: Option<usize>,
        cancel: &Cancellation,
    ) -> Result<()> {
        // Bind common primitive comparisons outside the row loop. The original
        // typed Arrow values are read directly; no enum interpreter, widening
        // array or boolean mask is needed for date ranges/column comparisons.
        match self {
            Self::DateRange(a, low, high) => {
                if low > high {
                    rows.clear();
                    return cancel.check();
                }
                // Bounds were intersected with DATE's domain at compilation.
                let (low, high) = (*low as i32, *high as i32);
                let values = a.values();
                if a.null_count() == 0 {
                    select_rows(rows, initial, cancel, |r| {
                        low <= values[r] && values[r] <= high
                    })
                } else {
                    select_rows(rows, initial, cancel, |r| {
                        a.is_valid(r) && low <= values[r] && values[r] <= high
                    })
                }
            }
            Self::Compare(op, Value::Date(a), Value::Date(b), _) => {
                select_arrays(rows, initial, cancel, *op, a, b)
            }
            Self::Compare(op, Value::Date(a), Value::Number(b), _) => {
                select_scalar(rows, initial, cancel, *op, a, *b, i128::from)
            }
            Self::Compare(op, Value::Int(a), Value::Int(b), _) => {
                select_arrays(rows, initial, cancel, *op, a, b)
            }
            Self::Compare(op, Value::Int(a), Value::Number(b), _) => {
                select_scalar(rows, initial, cancel, *op, a, *b, i128::from)
            }
            Self::Compare(op, Value::Wide(a), Value::Number(b), _) => {
                select_scalar(rows, initial, cancel, *op, a, *b, |n| n)
            }
            _ => select_rows(rows, initial, cancel, |r| self.eval(r) == Some(true)),
        }
    }
}

fn select_arrays<T: arrow::datatypes::ArrowPrimitiveType>(
    rows: &mut Vec<u32>,
    initial: Option<usize>,
    cancel: &Cancellation,
    op: Op,
    a: &PrimitiveArray<T>,
    b: &PrimitiveArray<T>,
) -> Result<()>
where
    T::Native: Ord,
{
    let (left, right) = (a.values(), b.values());
    if a.null_count() == 0 && b.null_count() == 0 {
        select_comparison(
            rows,
            initial,
            cancel,
            op,
            |r| left[r],
            |r| right[r],
            |_| true,
        )
    } else {
        select_comparison(
            rows,
            initial,
            cancel,
            op,
            |r| left[r],
            |r| right[r],
            |r| a.is_valid(r) && b.is_valid(r),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn select_scalar<T: arrow::datatypes::ArrowPrimitiveType>(
    rows: &mut Vec<u32>,
    initial: Option<usize>,
    cancel: &Cancellation,
    op: Op,
    a: &PrimitiveArray<T>,
    b: i128,
    widen: impl Fn(T::Native) -> i128,
) -> Result<()> {
    let values = a.values();
    if a.null_count() == 0 {
        select_comparison(
            rows,
            initial,
            cancel,
            op,
            |r| widen(values[r]),
            |_| b,
            |_| true,
        )
    } else {
        select_comparison(
            rows,
            initial,
            cancel,
            op,
            |r| widen(values[r]),
            |_| b,
            |r| a.is_valid(r),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn select_comparison<T: Ord>(
    rows: &mut Vec<u32>,
    initial: Option<usize>,
    cancel: &Cancellation,
    op: Op,
    a: impl Fn(usize) -> T,
    b: impl Fn(usize) -> T,
    valid: impl Fn(usize) -> bool,
) -> Result<()> {
    // Dispatch once per batch, so the row loop contains only the typed comparison.
    match op {
        Op::Eq => select_rows(rows, initial, cancel, |r| valid(r) && a(r) == b(r)),
        Op::Ne => select_rows(rows, initial, cancel, |r| valid(r) && a(r) != b(r)),
        Op::Lt => select_rows(rows, initial, cancel, |r| valid(r) && a(r) < b(r)),
        Op::Le => select_rows(rows, initial, cancel, |r| valid(r) && a(r) <= b(r)),
        Op::Gt => select_rows(rows, initial, cancel, |r| valid(r) && a(r) > b(r)),
        Op::Ge => select_rows(rows, initial, cancel, |r| valid(r) && a(r) >= b(r)),
        _ => unreachable!("selection comparison"),
    }
}
fn select_rows(
    rows: &mut Vec<u32>,
    initial: Option<usize>,
    cancel: &Cancellation,
    test: impl Fn(usize) -> bool,
) -> Result<()> {
    cancel.check()?;
    let mut kept = 0;
    if let Some(len) = initial {
        // Initialize only the funded index buffer. Branchless compaction avoids
        // an unpredictable push branch for every decoded row; order is stable.
        rows.resize(len, 0);
        for start in (0..len).step_by(1024) {
            cancel.check()?;
            for row in start..(start + 1024).min(len) {
                rows[kept] = row as u32;
                kept += usize::from(test(row));
            }
        }
    } else {
        let len = rows.len();
        for start in (0..len).step_by(1024) {
            cancel.check()?;
            for i in start..(start + 1024).min(len) {
                let row = rows[i];
                rows[kept] = row;
                kept += usize::from(test(row as usize));
            }
        }
    }
    rows.truncate(kept);
    Ok(())
}

pub(crate) type Transform = Arc<dyn Fn(Batch) -> Result<Batch> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn fixture() -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(99),
                Some(i64::MIN),
                None,
                Some(2),
                Some(2),
                Some(5),
                Some(i64::MAX),
                Some(99),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(-1),
                Some(1),
                Some(0),
                Some(2),
                None,
                Some(i32::MAX),
                Some(0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("padding"),
                Some("éab"),
                None,
                Some("é🙂x"),
                Some("é🙂x"),
                Some(""),
                Some("a\0b"),
                Some("padding"),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(true),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(0),
                    Some(-100),
                    None,
                    Some(125),
                    Some(125),
                    Some(1000),
                    Some(10_i128.pow(37)),
                    Some(0),
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(i32::MIN),
                None,
                Some(2),
                Some(2),
                Some(5),
                Some(i32::MAX),
                Some(0),
            ])),
            Arc::new(NullArray::new(8)),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .zip(["k", "d", "s", "b", "n", "j", "z"])
                .map(|(a, n)| Field::new(n, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, arrays).unwrap().slice(1, 6)
    }
    fn predicate(sql: &str, schema: &Schema) -> Expr {
        crate::bind::bind(
            &crate::bind::parse(&format!("SELECT k WHERE {sql}")).unwrap(),
            schema,
            None,
            true,
        )
        .unwrap()
        .filter
        .unwrap()
    }
    fn baseline(batch: &RecordBatch, expression: &Expr, budget: &Budget) -> RecordBatch {
        let mut input = crate::expr::Input {
            columns: vec![None; batch.num_columns()],
            len: batch.num_rows(),
        };
        let mut needed = std::collections::BTreeSet::new();
        expression.columns(&mut needed);
        for i in needed {
            input.columns[i] = Some(
                crate::expr::normalize_input(
                    batch.column(i),
                    &Type::from_field(batch.schema().field(i)).unwrap(),
                    budget,
                )
                .unwrap(),
            );
        }
        let mask = expression.eval(&input, budget).unwrap();
        arrow::compute::filter_record_batch(batch, crate::expr::boolean(&mask.array)).unwrap()
    }
    #[test]
    fn kernels_match_eager_values_order_nulls_slices_and_empty_batches() {
        let batch = fixture();
        for sql in [
            "k >= 2 AND k < 10 AND j = k",
            "d >= DATE '1969-12-31' AND d < DATE '1970-01-03'",
            "s IN ('éab','é🙂x') AND d < DATE '1970-01-03' AND k >= 2 AND d >= DATE '1970-01-01'",
            "d >= DATE '1970-01-03' AND d < DATE '1970-01-01'",
            "d < d OR d = d",
            "n >= 1 AND n <= 1 + 10",
            "k IN (2,5,2)",
            "k NOT IN (2,5)",
            "s LIKE 'é%'",
            "s NOT LIKE '%'",
            "s LIKE '%' AND s <> ''",
            "SUBSTRING(s,2,1) IN ('🙂','a','🙂')",
            "SUBSTRING(s,2,0) IN ('','a')",
            "SUBSTRING(s,99,2) = ''",
            "NOT (b OR k = 2)",
            "NOT (b AND k = 2)",
            "(b AND k = 2) OR (NOT b AND k < 0)",
            "s IS NULL OR n IS NOT NULL",
            "z IS NULL",
            "b",
            "false",
            "true",
        ] {
            for source in [batch.clone(), batch.slice(2, 0), batch.slice(2, 2)] {
                let budget = Budget::new(1 << 22);
                let expression = predicate(sql, &source.schema());
                let expected = baseline(&source, &expression, &budget);
                let program = Program::new(&expression, &source.schema(), &budget)
                    .unwrap()
                    .unwrap_or_else(|| panic!("not supported: {sql}"));
                let parent = source.column(2).clone();
                let selected = program
                    .select(
                        Batch::adopt(source, &budget).unwrap(),
                        &budget,
                        &Cancellation::default(),
                    )
                    .unwrap();
                assert!(
                    Arc::ptr_eq(selected.batch.column(2), &parent),
                    "selection copied payload: {sql}"
                );
                let output = selected
                    .materialize(&budget, &Cancellation::default())
                    .unwrap();
                assert_eq!(output.batch, expected, "{sql}");
                drop(output);
                drop(program);
                assert_eq!(budget.used(), 0, "{sql}");
            }
        }
    }
    #[test]
    fn unsupported_and_fallible_expressions_keep_eager_path() {
        let batch = fixture();
        for sql in [
            "k + 1 > 0",
            "n / 0 > 0",
            "k * k * k > 0",
            "false AND k * k * k > 0",
            "d < DATE '1970-01-01' AND k * k * k > 0",
            "n < 1.001",
            "s LIKE '%x'",
            "s LIKE '_%'",
            "CAST(s AS INTEGER) = 2",
        ] {
            let expression = predicate(sql, &batch.schema());
            let budget = Budget::new(1 << 20);
            assert!(
                Program::new(&expression, &batch.schema(), &budget)
                    .unwrap()
                    .is_none(),
                "{sql}"
            );
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn date_guards_intersect_before_text_without_changing_extreme_dates() {
        let budget = Budget::new(1 << 22);
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("d", DataType::Date32, true),
                Field::new("s", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Date32Array::from(vec![
                    None,
                    Some(i32::MIN),
                    Some(0),
                    Some(i32::MAX),
                ])),
                Arc::new(StringArray::from(vec!["keep"; 4])),
            ],
        )
        .unwrap();
        for (op, bound, expected) in [
            (Op::Lt, i32::MIN, vec![]),
            (Op::Le, i32::MIN, vec![1]),
            (Op::Gt, i32::MAX, vec![]),
            (Op::Ge, i32::MAX, vec![3]),
            (Op::Eq, 0, vec![2]),
        ] {
            let date = Expr {
                ty: Type::Bool,
                kind: Kind::Binary(
                    op,
                    Box::new(Expr {
                        ty: Type::Date,
                        kind: Kind::Column(0),
                    }),
                    Box::new(Expr {
                        ty: Type::Date,
                        kind: Kind::Literal(Literal::Date(bound)),
                    }),
                ),
            };
            let text = Expr {
                ty: Type::Bool,
                kind: Kind::Binary(
                    Op::Eq,
                    Box::new(Expr {
                        ty: Type::Text,
                        kind: Kind::Column(1),
                    }),
                    Box::new(Expr {
                        ty: Type::Text,
                        kind: Kind::Literal(Literal::Text("keep".into())),
                    }),
                ),
            };
            let expression = Expr {
                ty: Type::Bool,
                kind: Kind::Binary(Op::And, Box::new(text), Box::new(date)),
            };
            assert!(has_date_guard(&expression));
            let program = Program::new(&expression, &source.schema(), &budget)
                .unwrap()
                .unwrap();
            assert!(matches!(program.terms[0], Term::DateRange { .. }));
            let selected = program
                .select(
                    Batch::adopt(source.clone(), &budget).unwrap(),
                    &budget,
                    &Cancellation::default(),
                )
                .unwrap();
            assert_eq!(selected.rows.as_ref().unwrap().indices, expected);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn compacted_selections_cross_cancellation_chunks_in_parent_order() {
        let budget = Budget::new(1 << 20);
        for len in [0, 1, 7, 1023, 1024, 1025, 8192] {
            let source = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)])),
                vec![Arc::new(Date32Array::from_iter_values(
                    (0..len).map(|i| i % 11),
                ))],
            )
            .unwrap();
            let expression = crate::bind::bind(
                &crate::bind::parse(
                    "SELECT d WHERE d >= DATE '1970-01-04' AND d < DATE '1970-01-08'",
                )
                .unwrap(),
                &source.schema(),
                None,
                true,
            )
            .unwrap()
            .filter
            .unwrap();
            let program = Program::new(&expression, &source.schema(), &budget)
                .unwrap()
                .unwrap();
            assert_eq!(program.terms.len(), 1);
            let selected = program
                .select(
                    Batch::adopt(source, &budget).unwrap(),
                    &budget,
                    &Cancellation::default(),
                )
                .unwrap();
            let expected: Vec<u32> = (0..len)
                .filter(|i| (3..7).contains(&(i % 11)))
                .map(|i| i as u32)
                .collect();
            assert_eq!(
                selected
                    .rows
                    .as_ref()
                    .map(|r| r.indices.as_slice())
                    .unwrap_or(&[]),
                expected
            );
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn chained_selections_hold_parents_and_release_on_denial_or_cancellation() {
        let batch = fixture();
        let budget = Budget::new(1 << 20);
        let expr = predicate("k >= 2", &batch.schema());
        let second = predicate("s LIKE 'é%'", &batch.schema());
        let first = Program::new(&expr, &batch.schema(), &budget)
            .unwrap()
            .unwrap();
        let second = Program::new(&second, &batch.schema(), &budget)
            .unwrap()
            .unwrap();
        for fail in [false, true] {
            let cancel = Cancellation::default();
            let selected = first
                .select(
                    Batch::adopt(batch.clone(), &budget).unwrap(),
                    &budget,
                    &cancel,
                )
                .unwrap();
            assert_eq!(selected.rows.as_ref().unwrap().indices, vec![2, 3, 4, 5]);
            assert!(budget.used() >= batch.get_array_memory_size());
            let held = budget.reserve(budget.limit() - budget.used()).unwrap();
            if fail {
                cancel.cancel();
            }
            let result = second.select(selected, &budget, &cancel);
            let error = match result {
                Err(e) => e,
                Ok(_) => panic!("expected failure"),
            };
            assert!(
                error.contains(if fail { "cancelled" } else { "resource-limit" }),
                "{error}"
            );
            drop(held);
        }
        let cancel = Cancellation::default();
        let selected = first
            .select(
                Batch::adopt(batch.clone(), &budget).unwrap(),
                &budget,
                &cancel,
            )
            .unwrap();
        let selected = second.select(selected, &budget, &cancel).unwrap();
        assert_eq!(selected.rows.as_ref().unwrap().indices, vec![2, 3]);
        let held = budget.reserve(budget.limit() - budget.used()).unwrap();
        assert!(selected.materialize(&budget, &cancel).is_err());
        drop(held);
        drop(first);
        drop(second);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn all_null_and_wide_integer_columns_are_not_reinterpreted() {
        let schema = Schema::new(vec![
            Type::Int.field("k", true),
            Field::new("d", DataType::Decimal128(38, 0), true),
        ]);
        let expression = predicate("k > 9223372036854775807", &schema);
        let source = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(i128::from(i64::MAX) + 1), None])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(0), None])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let budget = Budget::new(1 << 20);
        let program = Program::new(&expression, &source.schema(), &budget)
            .unwrap()
            .unwrap();
        let selected = program
            .select(
                Batch::adopt(source, &budget).unwrap(),
                &budget,
                &Cancellation::default(),
            )
            .unwrap();
        assert_eq!(selected.rows.as_ref().unwrap().indices, vec![0]);
        drop(selected);
        drop(program);
        assert_eq!(budget.used(), 0);
    }
}
