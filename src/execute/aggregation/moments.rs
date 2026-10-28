//! Per-group second-moment and ARG_MAX/ARG_MIN states.
//!
//! Like Float SUM, each group's state is updated in canonical input-row order
//! and is never merged or partially reduced: scan delivery, relational inputs
//! and external aggregate replay preserve each group's row order, and helpers
//! never own fragments of one state. Results are therefore bit-identical for
//! every worker count, batch size and spill setting.
use super::*;
use crate::{
    bind::Moment,
    expr::pack,
    moments::{Exact1, Exact2, Finished, Real1, Real2, evaluate},
};

pub(super) enum Cells {
    Exact1(Vec<Exact1>),
    Exact2(Vec<Exact2>),
    Real1(Vec<Real1>),
    Real2(Vec<Real2>),
}

fn exact(ty: &Type) -> bool {
    matches!(ty, Type::Int | Type::Decimal(_) | Type::Null)
}
fn scale(ty: &Type) -> i8 {
    ty.scale().unwrap_or(0)
}

impl Cells {
    pub(super) fn new(inputs: &[Type]) -> Self {
        match inputs {
            [x] if exact(x) => Self::Exact1(Vec::new()),
            [_] => Self::Real1(Vec::new()),
            [y, x] if exact(y) && exact(x) => Self::Exact2(Vec::new()),
            _ => Self::Real2(Vec::new()),
        }
    }
    pub(super) fn bytes_per_group(&self) -> usize {
        match self {
            Self::Exact1(_) => std::mem::size_of::<Exact1>(),
            Self::Exact2(_) => std::mem::size_of::<Exact2>(),
            Self::Real1(_) => std::mem::size_of::<Real1>(),
            Self::Real2(_) => std::mem::size_of::<Real2>(),
        }
    }
    pub(super) fn push(&mut self) {
        match self {
            Self::Exact1(cells) => cells.push(Exact1::default()),
            Self::Exact2(cells) => cells.push(Exact2::default()),
            Self::Real1(cells) => cells.push(Real1::default()),
            Self::Real2(cells) => cells.push(Real2::default()),
        }
    }
}

impl States {
    /// One non-NULL row for `group` (its count is maintained by the caller).
    #[inline]
    fn moment_row(
        &mut self,
        inputs: &[Type],
        array: &dyn Array,
        group: usize,
        row: usize,
    ) -> Result<()> {
        match self.moments.as_mut().expect("moment state") {
            Cells::Exact1(cells) => {
                let numbers = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("bound exact moment input");
                cells[group].update(numbers.value(row));
            }
            Cells::Real1(cells) => {
                let numbers = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("bound REAL moment input");
                cells[group].update(numbers.value(row))?;
            }
            Cells::Exact2(cells) => {
                let bytes = binary(array).value(row);
                let (y, x) = bytes.split_at(16);
                cells[group].update(pack::exact(y), pack::exact(x));
            }
            Cells::Real2(cells) => {
                let bytes = binary(array).value(row);
                let (y, x) = pack::split(bytes, &inputs[0]);
                cells[group].update(pack::as_f64(y, &inputs[0])?, pack::as_f64(x, &inputs[1])?)?;
            }
        }
        Ok(())
    }

    /// Ordinary (non-DISTINCT) accumulation of a whole batch.
    pub(super) fn update_moments(
        &mut self,
        inputs: &[Type],
        value: &Value,
        groups: &[usize],
        rows: usize,
        scalar: bool,
    ) -> Result<()> {
        let array = value.array.as_ref();
        if array.data_type() == &arrow::datatypes::DataType::Null {
            return Ok(());
        }
        let nulls = array.logical_nulls();
        let group_of = |row: usize| if scalar { 0 } else { groups[row] };
        for row in 0..rows {
            if nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                continue;
            }
            let group = group_of(row);
            self.counts[group] = self.counts[group]
                .checked_add(1)
                .ok_or("aggregate count overflow")?;
            self.moment_row(inputs, array, group, row)?;
        }
        Ok(())
    }

    /// A row admitted by the general (DISTINCT) path, already counted.
    pub(super) fn update_moment_row(
        &mut self,
        inputs: &[Type],
        value: &Value,
        group: usize,
        row: usize,
    ) -> Result<()> {
        self.moment_row(inputs, value.array.as_ref(), group, row)
    }

    pub(super) fn moment_output(
        &self,
        function: Moment,
        inputs: &[Type],
        range: std::ops::Range<usize>,
    ) -> Result<ArrayRef> {
        let cells = self.moments.as_ref().expect("moment state");
        let mut values = Vec::with_capacity(range.len());
        for group in range {
            let n = self.counts[group];
            let finished = match cells {
                Cells::Exact1(cells) => Finished::exact1(n, &cells[group], scale(&inputs[0])),
                Cells::Real1(cells) => Finished::real1(n, &cells[group]),
                Cells::Exact2(cells) => {
                    Finished::exact2(n, &cells[group], scale(&inputs[0]), scale(&inputs[1]))
                }
                Cells::Real2(cells) => Finished::real2(n, &cells[group]),
            };
            values.push(evaluate(function, &finished)?);
        }
        Ok(Arc::new(Float64Array::from(values)))
    }

    /// ARG_MAX/ARG_MIN: keep the packed row with the strictly best value; the
    /// first row in canonical order wins among equal values.
    pub(super) fn update_arg_extreme(
        &mut self,
        minimum: bool,
        arg: &Type,
        value_type: &Type,
        value: &Value,
        group: usize,
        row: usize,
    ) -> Result<()> {
        let bytes = binary(value.array.as_ref()).value(row);
        let slot = &mut self.extremes.as_mut().expect("ARG_MAX state")[group];
        if let Some(Key::Binary(old)) = slot.as_ref() {
            let candidate = pack::split(bytes, arg).1;
            let incumbent = pack::split(old, arg).1;
            let cmp = pack::compare(candidate, incumbent, value_type);
            if !(if minimum { cmp.is_lt() } else { cmp.is_gt() }) {
                return Ok(());
            }
        }
        heap::replace_binary(slot, self.heap_lease.as_mut().expect("heap lease"), bytes)
    }

    pub(super) fn arg_extreme_output(
        &self,
        arg: &Type,
        ty: &Type,
        range: std::ops::Range<usize>,
    ) -> Result<ArrayRef> {
        let keys = self.extremes.as_ref().expect("ARG_MAX state")[range]
            .iter()
            .map(|slot| match slot {
                Some(Key::Binary(bytes)) => pack::key(pack::split(bytes, arg).0, arg),
                _ => Key::Null,
            })
            .collect::<Vec<_>>();
        keys_array(&keys, ty)
    }
}

fn binary(array: &dyn Array) -> &BinaryArray {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("packed aggregate arguments")
}
