use super::*;
use crate::expr_program::ExpressionProgram;
use arrow::datatypes::{DataType, Field, Schema};

fn plan(sql: &str) -> (Plan, ExpressionProgram) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]);
    let plan = bind::bind(&bind::parse(sql).unwrap(), &schema, None, false).unwrap();
    let expressions = ExpressionProgram::new(&plan, 2, true);
    (plan, expressions)
}

fn value(values: Vec<Option<i128>>, budget: &Budget) -> Value {
    Value::adopt(
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
        budget,
    )
    .unwrap()
}

#[test]
fn sorted_pairs_keep_encounter_order_null_groups_and_cross_batch_duplicates() {
    let (plan, expressions) = plan("SELECT k,COUNT(DISTINCT v) FROM t GROUP BY k");
    let pattern = [
        (Some(7), Some(i128::from(i64::MIN))),
        (None, Some(i128::from(i64::MAX))),
        (Some(-3), None),
        (Some(7), Some(-1)),
        (Some(4), Some(0)),
        (Some(7), Some(i128::from(i64::MAX))),
        (Some(4), None),
        (None, Some(i128::from(i64::MIN))),
        (Some(7), Some(i128::from(i64::MIN))),
    ];
    let rows: Vec<_> = pattern.into_iter().cycle().take(18_513).collect();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 127, 8192] {
            let budget = Budget::new(32 << 20);
            {
                let mut sorted =
                    Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
                let mut serial =
                    Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
                let options = Options {
                    workers,
                    ..Options::default()
                };
                assert!(sorted.enable_sorted_distinct(&plan, rows.len(), &options));
                let mut metrics = Metrics::default();
                for batch in rows.chunks(batch_size) {
                    for state in [&mut sorted, &mut serial] {
                        state
                            .update(
                                &plan,
                                &expressions,
                                &mut metrics,
                                EvaluatedBatch {
                                    rows: batch.len(),
                                    keys: vec![value(batch.iter().map(|r| r.0).collect(), &budget)],
                                    values: vec![Some(value(
                                        batch.iter().map(|r| r.1).collect(),
                                        &budget,
                                    ))],
                                    prepared_keys: None,
                                },
                            )
                            .unwrap();
                    }
                    let pairs = sorted.sorted_distinct.as_ref().unwrap();
                    assert_eq!(
                        pairs._credit.bytes(),
                        (pairs.pairs.capacity() + pairs.scratch.capacity()) * 16
                    );
                    assert!(budget.used() >= pairs._credit.bytes());
                    assert_eq!(budget.active_workers(), 0);
                }
                let before = budget.used();
                sorted.finish_updates().unwrap();
                assert_eq!(budget.used(), before - rows.len() * 32);
                serial.finish_updates().unwrap();
                assert_eq!(sorted.states[0].counts, [3, 2, 0, 1]);
                assert_eq!(sorted.states[0].counts, serial.states[0].counts);
                assert_eq!(sorted.numeric_group_keys, serial.numeric_group_keys);
                assert_eq!(sorted.numeric_group_keys.as_ref().unwrap(), &[7, 0, -3, 4]);
                assert_eq!(sorted.null_group, Some(1));
                // Completion can be called twice without applying contributions twice.
                sorted.finish_updates().unwrap();
                assert_eq!(sorted.states[0].counts, [3, 2, 0, 1]);
            }
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn radix_matches_ordered_set_oracle_for_skew_and_all_digits() {
    let budget = Budget::new(16 << 20);
    let mut pairs = Pairs::new(&budget, 20_000, Cancellation::default()).unwrap();
    let mut seed = 7_u64;
    let mut expected = std::collections::BTreeSet::new();
    for i in 0..20_000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let group = if i % 8 == 0 { i % 513 } else { 0 };
        let number = if i % 3 == 0 { 0 } else { seed as i64 };
        let input = value(vec![Some(i128::from(number))], &budget);
        pairs.append(&[group], &input).unwrap();
        expected.insert((group, number));
    }
    pairs.sort().unwrap();
    assert!(pairs.pairs.windows(2).all(|w| w[0] <= w[1]));
    let mut counts = vec![0; 513];
    pairs.finish(&mut counts).unwrap();
    let mut oracle = vec![0; 513];
    for (group, _) in expected {
        oracle[group] += 1;
    }
    assert_eq!(counts, oracle);
    assert_eq!(budget.used(), 0);
}

#[test]
fn funding_caps_wide_values_empty_nulls_and_cancellation_release_every_buffer() {
    let budget = Budget::new(4096);
    assert!(Pairs::new(&budget, usize::MAX, Cancellation::default()).is_none());
    assert!(Pairs::new(&budget, 33, Cancellation::default()).is_none());
    assert_eq!(budget.used(), 0);
    {
        let input = value(vec![None, Some(4)], &budget);
        let mut pairs = Pairs::new(&budget, 2, Cancellation::default()).unwrap();
        assert_eq!(pairs.append(&[1, 0], &input).unwrap(), 1);
        let live = budget.used();
        assert_eq!(pairs.append(&[1, 0], &input).unwrap_err(), DECLINED);
        assert_eq!(budget.used(), live);
        assert_eq!(pairs.pairs.len(), 1);
        let mut counts = [0, 0];
        pairs.finish(&mut counts).unwrap();
        assert_eq!(counts, [1, 0]);
    }
    assert_eq!(budget.used(), 0);
    for wide in [i128::from(i64::MIN) - 1, i128::from(i64::MAX) + 1] {
        let input = value(vec![Some(1), Some(wide)], &budget);
        let mut pairs = Pairs::new(&budget, 2, Cancellation::default()).unwrap();
        assert_eq!(pairs.append(&[0, 0], &input).unwrap_err(), DECLINED);
        assert_eq!(pairs.pairs.len(), 1);
    }
    assert_eq!(budget.used(), 0);
    for nulls in [false, true] {
        let mut pairs = Pairs::new(&budget, 2, Cancellation::default()).unwrap();
        if nulls {
            let input = Value::adopt(Arc::new(NullArray::new(2)), &budget).unwrap();
            pairs.append(&[0, 1], &input).unwrap();
        }
        let mut counts = [0, 0];
        pairs.finish(&mut counts).unwrap();
        assert_eq!(counts, [0, 0]);
    }
    assert_eq!(budget.used(), 0);
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            ..Options::default()
        };
        let input = value(vec![Some(4), Some(1)], &budget);
        let mut pairs = Pairs::new(&budget, 2, options.cancellation.clone()).unwrap();
        pairs.append(&[1, 0], &input).unwrap();
        options.cancellation.cancel();
        assert_eq!(
            pairs.finish(&mut [0, 0]).unwrap_err(),
            "cancelled: query execution"
        );
        drop(input);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn mixed_fallible_coerced_and_scalar_plans_decline_without_changing_hash_states() {
    for sql in [
        "SELECT k,COUNT(DISTINCT v),COUNT(*) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT v),SUM(v) FROM t GROUP BY k",
        "SELECT k,SUM(DISTINCT v) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT (v+1)) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT CAST(v AS REAL)) FROM t GROUP BY k",
        "SELECT COUNT(DISTINCT v) FROM t",
    ] {
        let (plan, expressions) = plan(sql);
        let budget = Budget::new(1 << 20);
        let mut state = Aggregation::new(&plan, &expressions, budget.clone(), true).unwrap();
        let live = budget.used();
        assert!(
            !state.enable_sorted_distinct(&plan, 100, &Options::default()),
            "{sql}"
        );
        assert_eq!(budget.used(), live);
        assert!(state.states[0].distinct.is_some());
        drop(state);
        assert_eq!(budget.used(), 0);
    }
}
