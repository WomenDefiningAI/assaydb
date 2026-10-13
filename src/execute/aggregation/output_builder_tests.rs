use super::*;
use crate::expr_program::ExpressionProgram;
use arrow::datatypes::{DataType, Field, Schema};

fn fixture(sql: &str, budget: &Budget) -> (Plan, ExpressionProgram, SchemaRef, Aggregation) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]);
    let plan = bind::bind(&bind::parse(sql).unwrap(), &schema, None, false).unwrap();
    let expressions = ExpressionProgram::new(&plan, 2, true);
    let output = Arc::new(Schema::new(
        plan.names
            .iter()
            .zip(&plan.types)
            .map(|(name, ty)| Field::new(name, ty.arrow(), true))
            .collect::<Vec<_>>(),
    ));
    let mut state = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
    // Synthetic final states, all vector storage precharged before allocation.
    state.state_leases.push(budget.reserve(4096).unwrap());
    state.group_count = 3;
    state.numeric_group_keys = Some(vec![11, 0, 33]);
    state.null_group = Some(1);
    state.states[0].counts = vec![1, 0, 1];
    state.states[0].sums = Some(vec![i256::from_i128(5), i256::MAX, i256::from_i128(-8)]);
    (plan, expressions, output, state)
}

#[test]
fn direct_output_builder_handles_offset_nulls_empty_ranges_and_credit() {
    let budget = Budget::new(1 << 20);
    {
        let (plan, expressions, schema, state) =
            fixture("SELECT k,SUM(v) FROM t GROUP BY k", &budget);
        for range in [0..3, 1..3, 1..2, 0..0] {
            let before = budget.used();
            let lease = budget
                .reserve(
                    state
                        .output_bytes(&plan, &expressions, range.clone())
                        .unwrap(),
                )
                .unwrap();
            let batch = state
                .output_batch(&plan, &expressions, &schema, range.clone())
                .unwrap();
            assert!(batch.get_array_memory_size() <= budget.used() - before);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(
                keys.iter().collect::<Vec<_>>(),
                [Some(11), None, Some(33)][range.clone()]
            );
            assert_eq!(
                sums.iter().collect::<Vec<_>>(),
                [Some(5), None, Some(-8)][range]
            );
            drop(batch);
            drop(lease);
            assert_eq!(budget.used(), before);
        }
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn direct_output_builder_keeps_column_then_materialization_error_order() {
    let budget = Budget::new(1 << 20);
    {
        let (plan, expressions, schema, mut state) =
            fixture("SELECT SUM(v),k FROM t GROUP BY k", &budget);
        // First group fits Decimal38 but cannot narrow to BIGINT. Later group
        // exceeds Decimal38. Complete SUM finalization must report the latter
        // before materialize checks the earlier BIGINT value.
        state.states[0].counts = vec![1, 1, 1];
        state.states[0].sums = Some(vec![
            i256::from_i128(i64::MAX as i128 + 1),
            i256::MAX,
            i256::ZERO,
        ]);
        let before = budget.used();
        {
            let _lease = budget
                .reserve(state.output_bytes(&plan, &expressions, 0..3).unwrap())
                .unwrap();
            assert_eq!(
                state
                    .output_batch(&plan, &expressions, &schema, 0..3)
                    .unwrap_err(),
                "decimal precision 38 overflow"
            );
        }
        assert_eq!(budget.used(), before);
        state.states[0].sums.as_mut().unwrap()[1] = i256::ZERO;
        let _lease = budget
            .reserve(state.output_bytes(&plan, &expressions, 0..3).unwrap())
            .unwrap();
        assert!(
            state
                .output_batch(&plan, &expressions, &schema, 0..3)
                .unwrap_err()
                .contains("BIGINT overflow")
        );
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn direct_output_builder_does_not_move_errors_across_columns() {
    let budget = Budget::new(1 << 20);
    {
        let (plan, expressions, schema, mut state) =
            fixture("SELECT k,SUM(v) FROM t GROUP BY k", &budget);
        state.numeric_group_keys.as_mut().unwrap()[0] = i64::MAX as i128 + 1;
        state.states[0].sums.as_mut().unwrap()[0] = i256::MAX;
        let _lease = budget
            .reserve(state.output_bytes(&plan, &expressions, 0..3).unwrap())
            .unwrap();
        assert_eq!(
            state
                .output_batch(&plan, &expressions, &schema, 0..3)
                .unwrap_err(),
            "BIGINT overflow"
        );
    }
    assert_eq!(budget.used(), 0);
}
