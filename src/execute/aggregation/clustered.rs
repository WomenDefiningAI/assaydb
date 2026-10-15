//! Finalize every group before applying a total HAVING; build only surviving rows.
use super::*;
use crate::expr::{Expr, Kind, Literal, Op};

#[derive(Clone, Copy)]
pub(in crate::execute) enum Having {
    All(bool),
    Compare {
        column: usize,
        op: Op,
        literal: i128,
    },
}

pub(in crate::execute) struct ClusteredBatch {
    pub batch: RecordBatch,
    pub selected: Box<[usize]>,
    pub groups: usize,
    // Buffers and selection indices drop before their reservations.
    _lease: Reservation,
    _selection: Reservation,
}

impl Having {
    /// Only same-scale column comparisons (or safe literal rescaling) are total.
    pub(in crate::execute) fn compile(expr: Option<&Expr>, plan: &Plan) -> Option<Self> {
        let Some(expr) = expr else {
            return Some(Self::All(true));
        };
        if let Kind::Literal(Literal::Bool(value)) = expr.kind {
            return Some(Self::All(value));
        }
        let Kind::Binary(op, a, b) = &expr.kind else {
            return None;
        };
        if !matches!(op, Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge) {
            return None;
        }
        let reverse = match op {
            Op::Lt => Op::Gt,
            Op::Le => Op::Ge,
            Op::Gt => Op::Lt,
            Op::Ge => Op::Le,
            other => *other,
        };
        for (a, b, op) in [(a, b, *op), (b, a, reverse)] {
            if let (Kind::Column(column), Kind::Literal(Literal::Num(literal))) = (&a.kind, &b.kind)
            {
                if plan.types.get(*column) != Some(&a.ty) {
                    continue;
                }
                let scale = a.ty.scale().ok()?;
                let old = b.ty.scale().ok()?;
                if old > scale {
                    continue;
                }
                let literal = crate::types::rescale(*literal, old, scale).ok()?;
                return Some(Self::Compare {
                    column: *column,
                    op,
                    literal,
                });
            }
        }
        None
    }

    /// NULL comparisons reject the row, exactly like a SQL HAVING mask.
    fn accepts(self, value: Option<i128>) -> bool {
        match self {
            Self::All(keep) => keep,
            Self::Compare { op, literal, .. } => value.is_some_and(|value| match op {
                Op::Eq => value == literal,
                Op::Ne => value != literal,
                Op::Lt => value < literal,
                Op::Le => value <= literal,
                Op::Gt => value > literal,
                Op::Ge => value >= literal,
                _ => unreachable!(),
            }),
        }
    }
}

impl Aggregation {
    /// Admitted COUNT/SUM slots use the ordinary exact finalizers without narrowing INTEGER.
    fn clustered_value(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        output: &Output,
        group: usize,
    ) -> Result<Option<i128>> {
        match output {
            Output::Group(0) => Ok(Some(self.numeric_group_keys.as_ref().unwrap()[group])),
            Output::Aggregate(index) => {
                let state = &self.states[expressions.state_sources[*index]];
                match plan.aggregates[*index].function {
                    Aggregate::Count => Ok(Some(i128::from(state.counts[group]))),
                    Aggregate::Sum if state.counts[group] == 0 => Ok(None),
                    Aggregate::Sum => {
                        crate::aggregate_sum::finish(state.sums.as_ref().unwrap()[group]).map(Some)
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    /// Only a shared endpoint can repeat; merge its raw slots into its first owner in file order.
    pub(in crate::execute) fn clustered_merge_boundary(
        &mut self,
        source: &Self,
        key: i128,
        expressions: &crate::expr_program::ExpressionProgram,
    ) -> Result<Option<usize>> {
        let (Some(to), Some(from)) = (self.numeric_map.get(key), source.numeric_map.get(key))
        else {
            return Ok(None);
        };
        for (slot, target) in self.states.iter_mut().enumerate() {
            if expressions.state_sources[slot] != slot {
                continue;
            }
            let source = &source.states[slot];
            target.counts[to] = target.counts[to]
                .checked_add(source.counts[from])
                .ok_or("aggregate count overflow")?;
            if let Some(sums) = &mut target.sums {
                sums[to] = sums[to]
                    .checked_add(source.sums.as_ref().unwrap()[from])
                    .ok_or("aggregate accumulator overflow")?;
                // Int32/64 inputs and <= i64::MAX file rows bound every possible
                // INTEGER prefix by 2^126 < 10^38, including this boundary merge.
                if target.integer_sum {
                    crate::aggregate_sum::finish(sums[to])?;
                }
            }
        }
        Ok(Some(from))
    }

    /// All groups finalize in encounter/slot order, including HAVING rejects; errors cause fresh replay.
    pub(in crate::execute) fn clustered_batch(
        &mut self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        having: Having,
        skip: Option<usize>,
        cancel: &crate::spill::Cancellation,
    ) -> Result<ClusteredBatch> {
        self.finish_updates()?;
        let mut selection = self.budget.reserve(
            self.group_count
                .checked_mul(2 * std::mem::size_of::<usize>())
                .and_then(|n| n.checked_add(256))
                .ok_or("clustered selection size overflow")?,
        )?;
        let mut selected = Vec::with_capacity(self.group_count);
        for group in 0..self.group_count {
            if group % 1024 == 0 {
                cancel.check()?;
            }
            if skip == Some(group) {
                continue;
            }
            // Finalize hidden aggregate slots too: HAVING never suppresses SUM errors.
            for slot in 0..plan.aggregates.len() {
                self.clustered_value(plan, expressions, &Output::Aggregate(slot), group)?;
            }
            let value = match having {
                Having::Compare { column, .. } => {
                    self.clustered_value(plan, expressions, &plan.output[column], group)?
                }
                Having::All(_) => None,
            };
            if having.accepts(value) {
                selected.push(group);
            }
        }
        let bytes = selected
            .len()
            .checked_mul((plan.output.len() + 1) * 128)
            .and_then(|n| n.checked_add(4096 + plan.output.len() * 1024))
            .ok_or("clustered output size overflow")?;
        let mut lease = self.budget.reserve(bytes)?;
        let mut arrays = Vec::with_capacity(plan.output.len());
        for (column, output) in plan.output.iter().enumerate() {
            cancel.check()?;
            let ty = physical_output_type(
                &plan.types[column],
                Type::is_wide_integer(schema.field(column)),
            );
            let mut builder = Decimal128Builder::with_capacity(selected.len());
            for &group in &selected {
                builder.append_option(self.clustered_value(plan, expressions, output, group)?);
            }
            let values: ArrayRef = Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(38, ty.scale()?)
                    .map_err(|e| e.to_string())?,
            );
            arrays.push(output_array(
                materialize(&values, &ty)?,
                schema.field(column),
            )?);
        }
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?;
        lease.release_excess(
            batch
                .get_array_memory_size()
                .saturating_add(1024 + plan.output.len() * 256),
        );
        if let Some(skip) = skip {
            for group in &mut selected {
                *group -= usize::from(*group > skip);
            }
        }
        let selected = selected.into_boxed_slice();
        selection.release_excess(std::mem::size_of_val(selected.as_ref()) + 256);
        Ok(ClusteredBatch {
            batch,
            selected,
            groups: self.group_count - usize::from(skip.is_some()),
            _lease: lease,
            _selection: selection,
        })
    }
}
