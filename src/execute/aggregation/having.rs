//! Total scalar HAVING selection after all canonical output checks in a chunk.
use super::*;
use crate::expr::{Expr, Kind, Literal, Op};

/// Admission proves comparison needs no column rescaling or fallible evaluation.
#[derive(Clone, Copy)]
pub(crate) struct Having {
    column: usize,
    literal: i128,
    op: Op,
    reversed: bool,
}
impl Having {
    /// Admit only numeric keys, COUNT/SUM outputs and a total column/literal comparison.
    pub(crate) fn for_plan(plan: &Plan, predicate: &Expr) -> Option<Self> {
        if !plan.aggregation
            || !plan.order.is_empty()
            || plan.groups.len() > 1
            || plan
                .groups
                .iter()
                .any(|g| !matches!(g.ty, Type::Int | Type::Decimal(0..=38)))
            || plan.output.len() != plan.types.len()
            || plan.output.iter().zip(&plan.types).any(|(output, ty)| {
                !matches!(ty, Type::Int | Type::Decimal(0..=38) | Type::Null)
                    || match output {
                        Output::Group(i) => *i != 0 || plan.groups.is_empty(),
                        Output::Aggregate(i) => plan.aggregates.get(*i).is_none_or(|a| {
                            !matches!(a.function, Aggregate::Count | Aggregate::Sum) || a.distinct
                        }),
                        Output::Expr(_) => true,
                    }
            })
        {
            return None;
        }
        let Kind::Binary(op @ (Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge), a, b) =
            &predicate.kind
        else {
            return None;
        };
        let (column, literal, reversed) = match (&a.kind, &b.kind) {
            (Kind::Column(_), Kind::Literal(Literal::Num(_))) => (a, b, false),
            (Kind::Literal(Literal::Num(_)), Kind::Column(_)) => (b, a, true),
            _ => return None,
        };
        let Kind::Column(index) = column.kind else {
            unreachable!()
        };
        let Kind::Literal(Literal::Num(value)) = literal.kind else {
            unreachable!()
        };
        if predicate.ty != Type::Bool || plan.types.get(index)? != &column.ty {
            return None;
        }
        let from = literal.ty.scale().ok()?;
        let to = column.ty.scale().ok()?;
        if from > to {
            // The existing evaluator would rescale the column, which can fail.
            return None;
        }
        Some(Self {
            column: index,
            literal: crate::types::rescale(value, from, to).ok()?,
            op: *op,
            reversed,
        })
    }

    /// SQL UNKNOWN rejects a row; admitted numeric operands compare exactly.
    fn accepts(&self, value: Option<i128>) -> bool {
        let Some(value) = value else { return false };
        let order = value.cmp(&self.literal);
        let order = if self.reversed {
            order.reverse()
        } else {
            order
        };
        match self.op {
            Op::Eq => order.is_eq(),
            Op::Ne => !order.is_eq(),
            Op::Lt => order.is_lt(),
            Op::Le => !order.is_gt(),
            Op::Gt => order.is_gt(),
            Op::Ge => !order.is_lt(),
            _ => unreachable!("admitted comparison"),
        }
    }

    /// Other output owners retain canonical materialization before the same total filter.
    pub(super) fn filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let array = batch.column(self.column);
        let mask = if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
            BooleanArray::from_iter(values.iter().map(|v| Some(self.accepts(v))))
        } else {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("admitted numeric output");
            BooleanArray::from_iter(values.iter().map(|v| Some(self.accepts(v.map(i128::from)))))
        };
        Ok(arrow::compute::filter_record_batch(&batch, &mask).map_err(|e| e.to_string())?)
    }
}

/// Read the exact canonical output scalar, including NULL SUM and shared states.
fn value(
    state: &Aggregation,
    group: usize,
    output: &Output,
    ty: &Type,
    plan: &Plan,
    expressions: &crate::expr_program::ExpressionProgram,
) -> Result<Option<i128>> {
    if *ty == Type::Null {
        return Ok(None);
    }
    match output {
        Output::Group(index) => Ok(if let Some(keys) = &state.numeric_group_keys {
            (state.null_group != Some(group)).then_some(keys[group])
        } else {
            match state.group_keys[*index][group] {
                Key::Num(value) => Some(value),
                Key::Null => None,
                _ => unreachable!("admitted numeric key"),
            }
        }),
        Output::Aggregate(index) => {
            let cells = &state.states[expressions.state_sources[*index]];
            match plan.aggregates[*index].function {
                Aggregate::Count => Ok(Some(i128::from(cells.counts[group]))),
                Aggregate::Sum if cells.counts[group] == 0 => Ok(None),
                Aggregate::Sum => {
                    crate::aggregate_sum::finish(cells.sums.as_ref().unwrap()[group]).map(Some)
                }
                _ => unreachable!("admitted aggregate"),
            }
        }
        Output::Expr(_) => unreachable!("admitted output"),
    }
}

/// Check every column over the original chunk before building only first-seen survivors.
/// The caller funds the ordinary 128 bytes/(column+1)/row envelope: this covers
/// the 16-byte row references, Arrow builders, validity, narrowing and descriptors.
pub(super) fn batch<'a, I>(
    rows: impl Fn() -> I,
    count: usize,
    plan: &Plan,
    expressions: &crate::expr_program::ExpressionProgram,
    schema: &SchemaRef,
    having: &Having,
) -> Result<RecordBatch>
where
    I: Iterator<Item = (&'a Aggregation, usize)>,
{
    let mut selected = Vec::with_capacity(count);
    for (column, (output, ty)) in plan.output.iter().zip(&plan.types).enumerate() {
        let narrow = *ty == Type::Int && !Type::is_wide_integer(schema.field(column));
        let mut overflow = false;
        for (state, group) in rows() {
            let scalar = value(state, group, output, ty, plan, expressions)?;
            // SUM's precision checks for the whole column precede BIGINT
            // materialization, even when an earlier group cannot narrow.
            overflow |= narrow && scalar.is_some_and(|n| i64::try_from(n).is_err());
            if column == having.column && having.accepts(scalar) {
                selected.push((state, group));
            }
        }
        if overflow {
            return Err("BIGINT overflow".into());
        }
    }
    let mut arrays = Vec::with_capacity(plan.output.len());
    for (column, (output, ty)) in plan.output.iter().zip(&plan.types).enumerate() {
        let physical = physical_output_type(ty, Type::is_wide_integer(schema.field(column)));
        let array = if physical == Type::Null {
            arrow::array::new_null_array(&physical.arrow(), selected.len())
        } else {
            let mut builder = Decimal128Builder::with_capacity(selected.len());
            for &(state, group) in &selected {
                builder.append_option(value(state, group, output, ty, plan, expressions)?);
            }
            let array: ArrayRef = Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(38, physical.scale()?)
                    .map_err(|e| e.to_string())?,
            );
            materialize(&array, &physical)?
        };
        arrays.push(output_array(array, schema.field(column))?);
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?)
}

#[cfg(test)]
mod tests;
