//! Index-free states for a decoded, non-NULL monotone numeric leaf.
use super::*;
use std::ops::Range;

impl States {
    /// A run updates exactly its original rows, with the ordinary per-add checks and NULL counts.
    fn update_run(
        &mut self,
        function: &Aggregate,
        value: Option<&Value>,
        rows: Range<usize>,
        group: usize,
    ) -> Result<()> {
        let count = match function {
            Aggregate::Count => {
                let nulls = value.and_then(|v| v.array.logical_nulls());
                match nulls {
                    None => rows.len(),
                    Some(nulls) => rows.filter(|row| nulls.is_valid(*row)).count(),
                }
            }
            Aggregate::Sum | Aggregate::Avg => {
                let array = &value.expect("bound run aggregate input").array;
                if array.data_type() == &arrow::datatypes::DataType::Null {
                    return Ok(());
                }
                let numbers = numeric(array);
                let mut sum = self.sums.as_ref().unwrap()[group];
                let mut count = 0;
                for row in rows {
                    if numbers.is_valid(row) {
                        sum = if self.integer_sum {
                            crate::aggregate_sum::add_integer(sum, numbers.value(row))?
                        } else {
                            crate::aggregate_sum::add(sum, numbers.value(row))?
                        };
                        count += 1;
                    }
                }
                self.sums.as_mut().unwrap()[group] = sum;
                count
            }
            _ => return Err("internal: unsupported run aggregate".into()),
        };
        self.counts[group] = self.counts[group]
            .checked_add(count as u64)
            .ok_or("aggregate count overflow")?;
        Ok(())
    }
}

impl Aggregation {
    /// The minimum retained canonical-vector capacity bounds allocation-free run appends.
    fn run_capacity(&self, expressions: &crate::expr_program::ExpressionProgram) -> usize {
        let mut capacity = self.numeric_group_keys.as_ref().map_or(0, Vec::capacity);
        for (index, state) in self.states.iter().enumerate() {
            if expressions.state_sources[index] != index {
                continue;
            }
            debug_assert!(state.extremes.is_none() && state.distinct.is_none());
            capacity = capacity.min(state.counts.capacity());
            if let Some(sums) = &state.sums {
                capacity = capacity.min(sums.capacity());
            }
        }
        capacity
    }

    /// For a proved monotone leaf with total updates, append first-seen runs and retain wide states until ordinary emission.
    pub(in crate::execute) fn update_runs(
        &mut self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
    ) -> Result<()> {
        // Only leaf_owned's actual-key proof admits this entry: one numeric
        // key, COUNT/SUM/AVG columns, no DISTINCT or float, i64-bounded INTEGER
        // inputs and <=i64::MAX source rows. Thus update errors cannot be moved
        // between aggregates/runs. No ordinary indexed update may follow it.
        debug_assert!(self.precise_numeric && self.aggregate_workers.is_none());
        debug_assert_eq!(p.groups.len(), 1);
        debug_assert_eq!(self.numeric_map.hash_len(), 0);
        debug_assert_eq!(self.numeric_map.dense_bytes(), 0);
        self.encounters_valid = false;
        let start = Instant::now();
        let keys = numeric(&batch.keys[0].array);
        debug_assert_eq!(keys.null_count(), 0);
        let mut capacity = self.run_capacity(expressions);
        let mut first = 0;
        while first < batch.rows {
            let key = keys.value(first);
            let mut end = first + 1;
            while end < batch.rows && keys.value(end) == key {
                end += 1;
            }
            let continuing = self
                .numeric_group_keys
                .as_ref()
                .is_some_and(|keys| keys.last() == Some(&key));
            let group = if continuing {
                self.group_count - 1
            } else {
                // Capacity stays valid until growth: updates only append to
                // canonical vectors. Avoid walking their allocation metadata
                // for every run when existing funded storage has room.
                if self.group_count >= capacity {
                    // None requests vector growth only, with no hash insertion.
                    // Existing accounting still funds replacement peaks first.
                    self.grow_numeric(expressions, None)?;
                    capacity = self.run_capacity(expressions);
                }
                let group = self.group_count;
                self.numeric_group_keys.as_mut().unwrap().push(key);
                for (index, state) in self.states.iter_mut().enumerate() {
                    if expressions.state_sources[index] == index {
                        state.push();
                    }
                }
                self.group_count += 1;
                group
            };
            for (index, aggregate) in p.aggregates.iter().enumerate() {
                if expressions.state_sources[index] == index {
                    self.states[index].update_run(
                        &aggregate.function,
                        batch.values[index].as_ref(),
                        first..end,
                        group,
                    )?;
                }
            }
            first = end;
        }
        metrics.aggregate_seconds += start.elapsed().as_secs_f64();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_program::ExpressionProgram;
    use arrow::datatypes::{DataType, Field, Schema};

    fn setup(budget: &Budget) -> (Plan, ExpressionProgram, Aggregation) {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]);
        let plan = bind::bind(
            &bind::parse("SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k").unwrap(),
            &schema,
            None,
            false,
        )
        .unwrap();
        let expressions = ExpressionProgram::new(&plan, 2, true);
        let state = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
        (plan, expressions, state)
    }

    fn input(plan: &Plan, budget: &Budget, key: i128, rows: usize) -> EvaluatedBatch {
        let column = |number| {
            Value::adopt(
                Arc::new(
                    Decimal128Array::from(vec![number; rows])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                budget,
            )
            .unwrap()
        };
        EvaluatedBatch {
            rows,
            keys: vec![column(Some(key))],
            values: plan
                .aggregates
                .iter()
                .map(|a| a.input.as_ref().map(|_| column(Some(1))))
                .collect(),
            prepared_keys: None,
        }
    }

    #[test]
    fn run_state_uses_no_index_and_funds_growth_before_allocating() {
        let budget = Budget::new(1 << 20);
        let (plan, expressions, mut state) = setup(&budget);
        let mut metrics = Metrics::default();
        for key in 0..4 {
            state
                .update_runs(
                    &plan,
                    &expressions,
                    &mut metrics,
                    input(&plan, &budget, key, 3),
                )
                .unwrap();
        }
        // Continuation across a batch boundary appends no group or index entry.
        state
            .update_runs(
                &plan,
                &expressions,
                &mut metrics,
                input(&plan, &budget, 3, 2),
            )
            .unwrap();
        assert_eq!(state.numeric_group_keys.as_ref().unwrap(), &[0, 1, 2, 3]);
        assert_eq!(state.states[0].counts, [3, 3, 3, 5]);
        assert_eq!(
            state.states[0].sums.as_ref().unwrap()[3],
            i256::from_i128(5)
        );
        assert_eq!(state.numeric_map.hash_len(), 0);
        assert_eq!(state.numeric_map.dense_bytes(), 0);
        let capacity = state.numeric_group_keys.as_ref().unwrap().capacity();
        assert_eq!(capacity, 4);
        let batch = input(&plan, &budget, 4, 1);
        let denial = budget.reserve(budget.limit() - budget.used()).unwrap();
        let error = state
            .update_runs(&plan, &expressions, &mut metrics, batch)
            .unwrap_err();
        assert!(error.starts_with("resource-limit: accounted memory "));
        assert_eq!(state.group_count, 4);
        assert_eq!(
            state.numeric_group_keys.as_ref().unwrap().capacity(),
            capacity
        );
        drop(denial);
        drop(state);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn run_appends_reuse_funded_capacity_with_no_spare_budget() {
        let budget = Budget::new(1 << 20);
        let (plan, expressions, mut state) = setup(&budget);
        let mut metrics = Metrics::default();
        state
            .update_runs(
                &plan,
                &expressions,
                &mut metrics,
                input(&plan, &budget, 0, 3),
            )
            .unwrap();
        let retained = budget.used();
        assert_eq!(state.run_capacity(&expressions), 4);
        let mut batch = input(&plan, &budget, 0, 5);
        batch.keys[0] = Value::adopt(
            Arc::new(
                Decimal128Array::from(vec![0, 1, 2, 3, 3])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            &budget,
        )
        .unwrap();
        let denial = budget.reserve(budget.limit() - budget.used()).unwrap();
        state
            .update_runs(&plan, &expressions, &mut metrics, batch)
            .unwrap();
        assert_eq!(state.numeric_group_keys.as_ref().unwrap(), &[0, 1, 2, 3]);
        assert_eq!(state.states[0].counts, [4, 1, 1, 2]);
        assert_eq!(state.states[1].counts, [4, 1, 1, 2]);
        assert_eq!(
            state.states[0].sums.as_ref().unwrap(),
            &vec![4, 1, 1, 2]
                .into_iter()
                .map(i256::from_i128)
                .collect::<Vec<_>>()
        );
        assert_eq!(state.run_capacity(&expressions), 4);
        drop(denial);
        assert_eq!(budget.used(), retained);
        drop(state);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn run_sum_retains_integer_prefix_checks_and_decimal_cancellation() {
        let budget = Budget::new(1 << 20);
        let (plan, _, _) = setup(&budget);
        let high = 10_i128.pow(38) - 1;
        let value = Value::adopt(
            Arc::new(
                Decimal128Array::from(vec![Some(high), Some(1), Some(-1)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            &budget,
        )
        .unwrap();
        for integer_sum in [false, true] {
            let mut state = States::new(&plan.aggregates[0]);
            state.integer_sum = integer_sum;
            state.push();
            let result = state.update_run(&Aggregate::Sum, Some(&value), 0..3, 0);
            if integer_sum {
                assert_eq!(result.unwrap_err(), "decimal precision 38 overflow");
            } else {
                result.unwrap();
                assert_eq!(state.sums.unwrap()[0], i256::from_i128(high));
                assert_eq!(state.counts, [3]);
            }
        }
        drop(value);
        assert_eq!(budget.used(), 0);
    }
}
