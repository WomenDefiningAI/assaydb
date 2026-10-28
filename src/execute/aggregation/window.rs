//! A single window frame or partition uses the same states and exact kernels
//! as GROUP BY: every aggregate admitted with OVER (including DISTINCT,
//! statistical, ARG_MAX/ARG_MIN and ordered-set aggregates) is accumulated row
//! by row in frame order and finalized like one grouped group.
use super::*;

pub(crate) struct WindowAccumulator {
    states: States,
    budget: Budget,
    _credit: Reservation,
}

impl WindowAccumulator {
    pub(crate) fn new(aggregate: &bind::Agg, budget: &Budget) -> Result<Self> {
        let mut states = States::new(aggregate);
        let credit = budget.reserve(states.bytes_per_group().saturating_mul(4) + 1024)?;
        states.push();
        states.heap_lease = Some(budget.reserve(0)?);
        Ok(Self {
            states,
            budget: budget.clone(),
            _credit: credit,
        })
    }

    /// True when finalization consumes the state (ordered-set aggregates sort
    /// their values once): such a state cannot be updated after `finish`.
    pub(crate) fn consuming(aggregate: &bind::Agg) -> bool {
        aggregate.function.is_ordered_set()
    }

    pub(crate) fn update(
        &mut self,
        aggregate: &bind::Agg,
        value: Option<&Value>,
        rows: usize,
    ) -> Result<()> {
        if !aggregate.distinct
            && matches!(
                aggregate.function,
                Aggregate::Count | Aggregate::Sum | Aggregate::Avg
            )
        {
            self.states
                .update_plain(&aggregate.function, value, &[], rows, true, false)?;
            return Ok(());
        }
        if !aggregate.distinct
            && let Aggregate::Moment { inputs, .. } = &aggregate.function
        {
            let value = value.ok_or("internal: moment aggregate without input")?;
            return self.states.update_moments(inputs, value, &[], rows, true);
        }
        if aggregate.function.is_ordered_set() {
            let value = value.ok_or("internal: ordered-set aggregate without input")?;
            let groups = vec![0_usize; rows];
            let States {
                ordered,
                heap_lease,
                ..
            } = &mut self.states;
            return ordered.as_mut().expect("ordered-set state").update(
                value,
                &groups,
                heap_lease.as_mut().expect("window heap lease"),
            );
        }
        // MIN/MAX, DISTINCT and ARG_MAX/ARG_MIN: the grouped typed-key path.
        let state = &mut self.states;
        for row in 0..rows {
            if value.is_some_and(|v| is_sql_null(v.array.as_ref(), row)) {
                continue;
            }
            if aggregate.distinct {
                let value = value.ok_or("unsupported: DISTINCT wildcard")?;
                if !heap::insert_distinct(
                    &mut state.distinct.as_mut().unwrap()[0],
                    state.heap_lease.as_mut().unwrap(),
                    value,
                    &aggregate.input.as_ref().unwrap().ty,
                    row,
                )? {
                    continue;
                }
            }
            // Float sums count inside add_float.
            if state.float_sums.is_none() {
                state.counts[0] = state.counts[0]
                    .checked_add(1)
                    .ok_or("aggregate count overflow")?;
            }
            match &aggregate.function {
                Aggregate::Count => {}
                _ if state.float_sums.is_some() => {
                    let number = value
                        .unwrap()
                        .array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("bound Float aggregate input")
                        .value(row);
                    state.add_float(0, number)?;
                }
                Aggregate::Moment { inputs, .. } => {
                    state.update_moment_row(inputs, value.unwrap(), 0, row)?;
                }
                Aggregate::ArgExtreme {
                    minimum,
                    arg,
                    value: value_type,
                } => {
                    state.update_arg_extreme(*minimum, arg, value_type, value.unwrap(), 0, row)?;
                }
                Aggregate::Min | Aggregate::Max => {
                    heap::update_extreme(
                        &mut state.extremes.as_mut().unwrap()[0],
                        state.heap_lease.as_mut().unwrap(),
                        value.expect("bound MIN/MAX input"),
                        &aggregate.input.as_ref().unwrap().ty,
                        row,
                        matches!(aggregate.function, Aggregate::Min),
                    )?;
                }
                Aggregate::Sum | Aggregate::Avg => {
                    let v = numeric(&value.unwrap().array).value(row);
                    let sum = &mut state.sums.as_mut().unwrap()[0];
                    *sum = if state.integer_sum {
                        crate::aggregate_sum::add_integer(*sum, v)?
                    } else {
                        crate::aggregate_sum::add(*sum, v)?
                    };
                }
                Aggregate::StringAgg(_) | Aggregate::Quantile(_) | Aggregate::Mode { .. } => {
                    return Err("internal: unsupported window aggregate state".into());
                }
            }
        }
        Ok(())
    }

    /// Finalize only an actual frame, never a partial peer group/partition.
    /// INTEGER stays precision-38 internally, as it does between grouped stages.
    /// A `consuming` aggregate must not be updated afterwards.
    pub(crate) fn finish(&mut self, aggregate: &bind::Agg) -> Result<(ArrayRef, Reservation)> {
        let payload = self
            .states
            .extremes
            .as_ref()
            .and_then(|v| v[0].as_ref())
            .map_or(0, Key::payload_bytes)
            .saturating_add(
                self.states
                    .ordered
                    .as_ref()
                    .map_or(0, |ordered| ordered.output_payload(0..1)),
            );
        let credit = self
            .budget
            .reserve(payload.saturating_mul(3).saturating_add(2048))?;
        let ty = physical_output_type(&aggregate.ty, true);
        let count = self.states.counts[0];
        let array = if ty == Type::Null {
            new_null_array(&ty.arrow(), 1)
        } else {
            match &aggregate.function {
                Aggregate::Count => count_array(std::iter::once(count), &ty)?,
                Aggregate::Sum | Aggregate::Avg if self.states.float_sums.is_some() => self
                    .states
                    .float_output(0..1, matches!(aggregate.function, Aggregate::Avg)),
                Aggregate::Sum => Arc::new(
                    Decimal128Array::from(vec![if count == 0 {
                        None
                    } else {
                        Some(crate::aggregate_sum::finish(
                            self.states.sums.as_ref().unwrap()[0],
                        )?)
                    }])
                    .with_precision_and_scale(38, ty.scale()?)
                    .map_err(|e| e.to_string())?,
                ) as ArrayRef,
                Aggregate::Avg => Arc::new(Float64Array::from(vec![if count == 0 {
                    None
                } else {
                    Some(crate::numeric_float::average(
                        self.states.sums.as_ref().unwrap()[0],
                        aggregate.input.as_ref().unwrap().ty.scale()?,
                        count,
                    )?)
                }])),
                Aggregate::StringAgg(_) => {
                    return Err("internal: STRING_AGG is not a window aggregate".into());
                }
                Aggregate::Moment { function, inputs } => {
                    self.states.moment_output(*function, inputs, 0..1)?
                }
                Aggregate::ArgExtreme { arg, .. } => {
                    self.states.arg_extreme_output(arg, &ty, 0..1)?
                }
                Aggregate::Quantile(_) | Aggregate::Mode { .. } => {
                    let ordered = self.states.ordered.as_mut().expect("ordered-set state");
                    ordered.finish();
                    ordered.output(
                        &aggregate.function,
                        &aggregate.input.as_ref().expect("ordered-set input").ty,
                        &ty,
                        0..1,
                    )?
                }
                Aggregate::Min | Aggregate::Max => keys_array(
                    &[self.states.extremes.as_ref().unwrap()[0]
                        .clone()
                        .unwrap_or(Key::Null)],
                    &ty,
                )?,
            }
        };
        Ok((array, credit))
    }
}
