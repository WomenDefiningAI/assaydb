//! Streaming output over proved source-owned numeric domains.
//! No copied partial maps, global group descriptors, or encounter-token sort.
use super::*;
use crate::spill::Cancellation;

/// A monotone owner's extrema are its endpoint groups; indexed owners retain exact lookup otherwise.
fn endpoint_group(state: &Aggregation, key: i128) -> Option<usize> {
    let keys = state.numeric_group_keys.as_ref()?;
    if keys.first() == Some(&key) {
        Some(0)
    } else if keys.last() == Some(&key) {
        Some(keys.len() - 1)
    } else {
        state.numeric_map.get(key)
    }
}

pub(in crate::execute) fn new_state(
    plan: &Plan,
    expressions: &crate::expr_program::ExpressionProgram,
    budget: &Budget,
    window: Option<(i128, usize)>,
) -> Result<Aggregation> {
    let mut state = Aggregation::new(plan, expressions, budget.clone(), true)?;
    if let Some((base, len)) = window {
        state.numeric_map =
            crate::group_index::NumericGroups::with_proven_window(base, len, budget)?;
    }
    Ok(state)
}

struct Owner {
    state: Aggregation,
    // Strict ordered domains permit only one incoming duplicate key. A key
    // repeated through an equality chain is merged into its earliest owner.
    skip: Option<usize>,
    start: usize,
    end: usize,
}
pub(in crate::execute) struct SourceOwned {
    owners: Vec<Owner>,
    groups: usize,
    pub(in crate::execute) boundary_merges: usize,
    // Retain descriptor backing past every owner and its state buffers.
    _metadata: Arc<Reservation>,
}
impl SourceOwned {
    /// Exact endpoint IDs merge boundary duplicates into their first owner; global finalization order is unchanged.
    pub(in crate::execute) fn finish(
        states: Vec<Aggregation>,
        domains: &[Option<(i128, i128)>],
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        cancellation: &Cancellation,
        metadata: Arc<Reservation>,
    ) -> Result<Self> {
        // The owner exists before any validation can fail; descriptors and
        // buffers always drop before their final metadata credit.
        let mut output = Self {
            owners: states
                .into_iter()
                .map(|state| Owner {
                    state,
                    skip: None,
                    start: 0,
                    end: 0,
                })
                .collect(),
            groups: 0,
            boundary_merges: 0,
            _metadata: metadata,
        };
        if output.owners.len() != domains.len()
            || plan.groups.len() != 1
            || expressions.state_sources.len() != plan.aggregates.len()
            || plan.aggregates.iter().any(|a| {
                a.distinct
                    || !matches!(
                        a.function,
                        Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                    )
            })
            || plan.output.iter().any(|o| match o {
                Output::Group(index) => *index != 0,
                Output::Aggregate(index) => *index >= plan.aggregates.len(),
                Output::Expr(_) => true,
            })
        {
            return Err("source owned finalization shape mismatch".into());
        }
        let mut ascending = true;
        let mut descending = true;
        let mut previous = None;
        for (owner, domain) in output.owners.iter().zip(domains) {
            cancellation.check()?;
            let state = &owner.state;
            if state.null_group.is_some()
                || state.states.len() != plan.aggregates.len()
                || state.numeric_group_keys.as_ref().map_or(0, Vec::len) != state.group_count
            {
                return Err("source owned state shape mismatch".into());
            }
            if let Some((lo, hi)) = *domain {
                if lo > hi
                    || state.group_count == 0
                    || endpoint_group(state, lo).is_none_or(|id| id >= state.group_count)
                    || endpoint_group(state, hi).is_none_or(|id| id >= state.group_count)
                {
                    return Err("source owned domain endpoints mismatch".into());
                }
                if let Some((old_lo, old_hi)) = previous {
                    ascending &= old_hi <= lo;
                    descending &= old_lo >= hi;
                }
                previous = Some((lo, hi));
            } else if state.group_count != 0 {
                return Err("empty source domain produced groups".into());
            }
            for (index, aggregate) in plan.aggregates.iter().enumerate() {
                let canonical = expressions.state_sources[index];
                if canonical > index
                    || expressions.state_sources[canonical] != canonical
                    || matches!(aggregate.function, Aggregate::Count)
                        != matches!(plan.aggregates[canonical].function, Aggregate::Count)
                {
                    return Err("source owned canonical state mismatch".into());
                }
                let cells = &state.states[index];
                let len = if canonical == index {
                    state.group_count
                } else {
                    0
                };
                let sums = matches!(aggregate.function, Aggregate::Sum | Aggregate::Avg);
                if cells.counts.len() != len
                    || cells.sums.is_some() != sums
                    || cells
                        .sums
                        .as_ref()
                        .is_some_and(|values| values.len() != len)
                    || cells.extremes.is_some()
                    || cells.distinct.is_some()
                {
                    return Err("source owned aggregate state mismatch".into());
                }
            }
        }
        if !ascending && !descending {
            return Err("source owned domains overlap".into());
        }
        // Resolve every boundary ID before changing a destination state. This
        // is O(owners^2), with <=64 owners, independent of group cardinality.
        let mut transfers = Vec::with_capacity(output.owners.len());
        for current in 0..output.owners.len() {
            let Some((lo, hi)) = domains[current] else {
                continue;
            };
            for (prior, domain) in domains.iter().enumerate().take(current) {
                let Some((old_lo, old_hi)) = *domain else {
                    continue;
                };
                let key = if old_hi == lo {
                    lo
                } else if old_lo == hi {
                    hi
                } else {
                    continue;
                };
                let to = endpoint_group(&output.owners[prior].state, key)
                    .ok_or("source boundary missing in earliest owner")?;
                let from = endpoint_group(&output.owners[current].state, key)
                    .ok_or("source boundary missing in later owner")?;
                if to >= output.owners[prior].state.group_count
                    || from >= output.owners[current].state.group_count
                {
                    return Err("source boundary group index mismatch".into());
                }
                transfers.push((current, from, prior, to));
                break;
            }
        }
        for owner in &mut output.owners {
            owner.state.finish_updates()?;
        }
        for (current, from, prior, to) in transfers {
            cancellation.check()?;
            let (before, after) = output.owners.split_at_mut(current);
            let dst = &mut before[prior].state;
            let src = &mut after[0];
            for (index, target) in dst.states.iter_mut().enumerate() {
                if expressions.state_sources[index] != index {
                    continue;
                }
                let source = &src.state.states[index];
                target.counts[to] = target.counts[to]
                    .checked_add(source.counts[from])
                    .ok_or("aggregate count overflow")?;
                if let Some(a) = &mut target.sums {
                    a[to] = a[to]
                        .checked_add(source.sums.as_ref().unwrap()[from])
                        .ok_or("aggregate accumulator overflow")?;
                    if target.integer_sum {
                        crate::aggregate_sum::finish(a[to])?;
                    }
                }
            }
            src.skip = Some(from);
            output.boundary_merges += 1;
        }
        let mut groups = 0usize;
        for owner in &mut output.owners {
            owner.start = groups;
            groups = groups
                .checked_add(owner.state.group_count - usize::from(owner.skip.is_some()))
                .ok_or("source owned group count overflow")?;
            owner.end = groups;
        }
        output.groups = groups;
        Ok(output)
    }
    pub(in crate::execute) fn group_count(&self) -> usize {
        self.groups
    }

    fn rows(&self, range: std::ops::Range<usize>) -> impl Iterator<Item = (&Aggregation, usize)> {
        self.owners.iter().flat_map(move |owner| {
            let first = range.start.max(owner.start).min(owner.end) - owner.start;
            let last = range.end.min(owner.end).max(owner.start) - owner.start;
            (first..last).map(move |visible| {
                let group = visible + usize::from(owner.skip.is_some_and(|skip| visible >= skip));
                (&owner.state, group)
            })
        })
    }
    pub(in crate::execute) fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        if range.start > range.end || range.end > self.groups {
            return Err("source owned output range".into());
        }
        let mut arrays = Vec::with_capacity(plan.output.len());
        // Complete each canonical output column across the GLOBAL chunk before
        // the next column. Finalizing each owner first would change SQL errors.
        for (output, ty) in plan.output.iter().zip(&plan.types) {
            let physical_ty =
                physical_output_type(ty, Type::is_wide_integer(schema.field(arrays.len())));
            let ty = &physical_ty;
            let array: ArrayRef = match output {
                Output::Group(_) => {
                    let values = self
                        .rows(range.clone())
                        .map(|(state, group)| {
                            state
                                .numeric_group_keys
                                .as_ref()
                                .expect("proved numeric key")[group]
                        })
                        .collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(38, ty.scale()?)
                            .map_err(|e| e.to_string())?,
                    );
                    materialize(&array, ty)?
                }
                Output::Aggregate(_) if *ty == Type::Null => {
                    arrow::array::new_null_array(&ty.arrow(), range.len())
                }
                Output::Aggregate(index) => {
                    let aggregate = &plan.aggregates[*index];
                    let states = || {
                        self.rows(range.clone()).map(|(state, group)| {
                            (&state.states[expressions.state_sources[*index]], group)
                        })
                    };
                    match aggregate.function {
                        Aggregate::Count => {
                            count_array(states().map(|(state, group)| state.counts[group]), ty)?
                        }
                        Aggregate::Sum => {
                            let values = states()
                                .map(|(state, group)| {
                                    if state.counts[group] == 0 {
                                        Ok(None)
                                    } else {
                                        crate::aggregate_sum::finish(
                                            state.sums.as_ref().unwrap()[group],
                                        )
                                        .map(Some)
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let array: ArrayRef = Arc::new(
                                Decimal128Array::from(values)
                                    .with_precision_and_scale(38, ty.scale()?)
                                    .map_err(|e| e.to_string())?,
                            );
                            materialize(&array, ty)?
                        }
                        Aggregate::Avg => {
                            let scale = aggregate.input.as_ref().unwrap().ty.scale()?;
                            Arc::new(Float64Array::from(
                                states()
                                    .map(|(state, group)| {
                                        if state.counts[group] == 0 {
                                            Ok(None)
                                        } else {
                                            crate::numeric_float::average(
                                                state.sums.as_ref().unwrap()[group],
                                                scale,
                                                state.counts[group],
                                            )
                                            .map(Some)
                                        }
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            ))
                        }
                        _ => return Err("source owned unsupported aggregate".into()),
                    }
                }
                Output::Expr(_) => return Err("source owned unsupported output expression".into()),
            };
            arrays.push(output_array(array, schema.field(arrays.len()))?);
        }
        Ok(RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?)
    }
    pub(in crate::execute) fn output_bytes(
        &self,
        plan: &Plan,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        if range.start > range.end || range.end > self.groups {
            return Err("source owned output range".into());
        }
        range
            .len()
            .checked_mul((plan.output.len() + 1) * 128)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| "output size overflow".into())
    }
}

impl super::AggregateOutput for SourceOwned {
    fn finish_updates(&mut self) -> Result<()> {
        Ok(())
    }
    fn group_count(&self) -> usize {
        SourceOwned::group_count(self)
    }
    fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
    ) -> Result<RecordBatch> {
        SourceOwned::output_batch(self, plan, expressions, schema, range)
    }
    /// Global chunk traversal preserves boundary skips and column/error order.
    fn having_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: std::ops::Range<usize>,
        having: &having::Having,
    ) -> Result<RecordBatch> {
        if range.start > range.end || range.end > self.groups {
            return Err("source owned output range".into());
        }
        having::batch(
            || self.rows(range.clone()),
            range.len(),
            plan,
            expressions,
            schema,
            having,
        )
    }
    fn output_bytes(
        &self,
        plan: &Plan,
        _expressions: &crate::expr_program::ExpressionProgram,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        SourceOwned::output_bytes(self, plan, range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    #[test]
    fn source_owned_canonical_columns_cross_owner_boundaries_before_finalization() {
        let budget = Budget::new(1 << 20);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", arrow::datatypes::DataType::Int64, false),
            Field::new("v", arrow::datatypes::DataType::Decimal128(38, 0), false),
        ]));
        let plan = bind::bind(
            &bind::parse("SELECT COUNT(*),SUM(v) FROM t GROUP BY k").unwrap(),
            &schema,
            None,
            true,
        )
        .unwrap();
        let program = crate::expr_program::ExpressionProgram::new(&plan, 2, true);
        let metadata = Arc::new(budget.reserve(32768).unwrap());
        let mut states = Vec::new();
        for key in [0, 1] {
            let mut state = new_state(&plan, &program, &budget, Some((key, 1))).unwrap();
            state.group_count = 1;
            state.numeric_group_keys = Some(vec![key]);
            state.numeric_map.insert(key, 0);
            for cells in &mut state.states {
                cells.push();
            }
            state.states[0].counts[0] = if key == 0 { 1 } else { u64::MAX };
            state.states[1].counts[0] = 1;
            state.states[1].sums.as_mut().unwrap()[0] = if key == 0 {
                i256::from_i128(10_i128.pow(38))
            } else {
                i256::ONE
            };
            states.push(state);
        }
        // Deliberately injected impossible input counts distinguish COUNT's
        // final error from SUM's error without changing any engine semantics.
        let output = SourceOwned::finish(
            states,
            &[Some((0, 0)), Some((1, 1))],
            &plan,
            &program,
            &Cancellation::default(),
            metadata,
        )
        .unwrap();
        let output_schema = Arc::new(Schema::new(
            plan.names
                .iter()
                .zip(&plan.types)
                .map(|(name, ty)| Field::new(name, ty.arrow(), true))
                .collect::<Vec<_>>(),
        ));
        assert_eq!(
            output
                .output_batch(&plan, &program, &output_schema, 0..2)
                .unwrap_err(),
            "BIGINT overflow"
        );
        assert_eq!(
            output
                .output_batch(&plan, &program, &output_schema, 0..1)
                .unwrap_err(),
            "decimal precision 38 overflow"
        );
        drop(output);
        assert_eq!(budget.used(), 0);
    }
}
