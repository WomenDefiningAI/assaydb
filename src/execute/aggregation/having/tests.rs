use super::*;
use crate::{
    execute::{self, Options, Prepared},
    expr_program::ExpressionProgram,
    spill::Cancellation,
};
use arrow::datatypes::{DataType, Field, Schema};

fn predicate(column: usize, ty: Type, op: Op, literal: i128, literal_ty: Type) -> Expr {
    Expr {
        ty: Type::Bool,
        kind: Kind::Binary(
            op,
            Box::new(Expr {
                ty,
                kind: Kind::Column(column),
            }),
            Box::new(Expr {
                ty: literal_ty,
                kind: Kind::Literal(Literal::Num(literal)),
            }),
        ),
    }
}

fn prepared(sql: &str, internal_output: bool) -> Prepared {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 2), true),
    ]));
    Prepared {
        plan: bind::bind(&bind::parse(sql).unwrap(), &schema, None, true).unwrap(),
        schema,
        internal_output,
        auto_simple: None,
        relational: None,
    }
}

fn output_schema(p: &Prepared) -> SchemaRef {
    Arc::new(Schema::new(
        p.plan
            .names
            .iter()
            .zip(&p.plan.types)
            .map(|(n, ty)| ty.field(n, p.internal_output))
            .collect::<Vec<_>>(),
    ))
}

fn state(p: &Prepared, budget: &Budget) -> (Aggregation, ExpressionProgram) {
    let program = ExpressionProgram::new(&p.plan, 3, true);
    let mut state = Aggregation::new(&p.plan, &program, budget.clone(), true).unwrap();
    state.state_leases.push(budget.reserve(16384).unwrap());
    state.group_count = 3;
    state.numeric_group_keys = Some(vec![9, 0, 4]);
    state.null_group = Some(1);
    for (index, cells) in state.states.iter_mut().enumerate() {
        if program.state_sources[index] != index {
            continue;
        }
        cells.counts = vec![1, 0, 1];
        if let Some(sums) = &mut cells.sums {
            *sums = vec![i256::from_i128(7), i256::MAX, i256::from_i128(-5)];
        }
    }
    (state, program)
}

#[test]
fn selection_matches_canonical_filter_for_nulls_offsets_and_empty_chunks() {
    for internal in [false, true] {
        let budget = Budget::new(1 << 20);
        {
            let p = prepared(
                "SELECT k,SUM(v),COUNT(v),SUM(v) FROM t GROUP BY k",
                internal,
            );
            let schema = output_schema(&p);
            let (state, program) = state(&p, &budget);
            for op in [Op::Eq, Op::Ne, Op::Lt, Op::Le, Op::Gt, Op::Ge] {
                let having =
                    Having::for_plan(&p.plan, &predicate(1, Type::Int, op, 0, Type::Int)).unwrap();
                for range in [0..3, 1..3, 1..2, 0..0] {
                    let expected = having
                        .filter(
                            state
                                .output_batch(&p.plan, &program, &schema, range.clone())
                                .unwrap(),
                        )
                        .unwrap();
                    let before = budget.used();
                    let lease = budget
                        .reserve(
                            state
                                .output_bytes(&p.plan, &program, range.clone())
                                .unwrap(),
                        )
                        .unwrap();
                    let actual = state
                        .having_batch(&p.plan, &program, &schema, range, &having)
                        .unwrap();
                    assert_eq!(actual, expected);
                    assert!(actual.get_array_memory_size() <= budget.used() - before);
                    drop(actual);
                    drop(lease);
                    assert_eq!(budget.used(), before);
                }
            }
        }
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn rejected_groups_keep_precision_before_narrowing_and_column_error_order() {
    let budget = Budget::new(1 << 20);
    for sql in [
        "SELECT SUM(v),k FROM t GROUP BY k",
        "SELECT k,SUM(v) FROM t GROUP BY k",
    ] {
        let p = prepared(sql, false);
        let schema = output_schema(&p);
        let (mut state, program) = state(&p, &budget);
        state.states[0].counts = vec![1, 1, 1];
        state.states[0].sums = Some(vec![
            i256::from_i128(i64::MAX as i128 + 1),
            i256::MAX,
            i256::ZERO,
        ]);
        state.numeric_group_keys.as_mut().unwrap()[0] = i64::MAX as i128 + 1;
        let column = usize::from(sql.starts_with("SELECT k"));
        let having =
            Having::for_plan(&p.plan, &predicate(column, Type::Int, Op::Lt, 0, Type::Int)).unwrap();
        let _lease = budget
            .reserve(state.output_bytes(&p.plan, &program, 0..3).unwrap())
            .unwrap();
        for range in [0..3, 0..1, 1..3] {
            let expected = state
                .output_batch(&p.plan, &program, &schema, range.clone())
                .unwrap_err();
            let actual = state
                .having_batch(&p.plan, &program, &schema, range, &having)
                .unwrap_err();
            assert_eq!(actual, expected);
        }
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn admission_declines_fallible_scaling_and_unsupported_outputs() {
    for sql in [
        "SELECT k,AVG(v) FROM t GROUP BY k",
        "SELECT k,MIN(v) FROM t GROUP BY k",
        "SELECT k,SUM(DISTINCT v) FROM t GROUP BY k",
        "SELECT k,v,COUNT(*) FROM t GROUP BY k,v",
    ] {
        let p = prepared(sql, true);
        assert!(
            Having::for_plan(&p.plan, &predicate(0, Type::Int, Op::Gt, 0, Type::Int)).is_none()
        );
    }
    let p = prepared("SELECT k,SUM(v),SUM(d) FROM t GROUP BY k", true);
    assert!(
        Having::for_plan(
            &p.plan,
            &predicate(1, Type::Int, Op::Gt, 1, Type::Decimal(1))
        )
        .is_none()
    );
    assert!(
        Having::for_plan(
            &p.plan,
            &predicate(2, Type::Decimal(2), Op::Gt, 10_i128.pow(37), Type::Int)
        )
        .is_none()
    );
    assert!(Having::for_plan(&p.plan, &predicate(1, Type::Int, Op::Add, 0, Type::Int)).is_none());
    let having = Having::for_plan(
        &p.plan,
        &predicate(2, Type::Decimal(2), Op::Gt, 3, Type::Int),
    )
    .unwrap();
    assert!(!having.accepts(None));
    assert!(!having.accepts(Some(300)));
    assert!(having.accepts(Some(301)));
    let mut reversed = predicate(2, Type::Decimal(2), Op::Lt, 3, Type::Int);
    if let Kind::Binary(_, a, b) = &mut reversed.kind {
        std::mem::swap(a, b);
    }
    let having = Having::for_plan(&p.plan, &reversed).unwrap();
    assert!(having.accepts(Some(301)));
    assert!(!having.accepts(Some(300)));
}

#[test]
fn owned_selection_checks_global_columns_and_merges_boundary_groups_once() {
    let budget = Budget::new(1 << 20);
    {
        let p = prepared("SELECT k,SUM(v),COUNT(*) FROM t GROUP BY k", true);
        let schema = output_schema(&p);
        let program = ExpressionProgram::new(&p.plan, 3, true);
        let mut states = Vec::new();
        let metadata = Arc::new(budget.reserve(65536).unwrap());
        for (keys, sums) in [(vec![1, 3], [1, 4]), (vec![3, 4], [6, 8])] {
            let mut state = source_owned::new_state(&p.plan, &program, &budget, None).unwrap();
            state.state_leases.push(budget.reserve(16384).unwrap());
            state.group_count = 2;
            for (index, key) in keys.iter().enumerate() {
                state.numeric_map.insert(*key, index);
            }
            state.numeric_group_keys = Some(keys);
            for cells in &mut state.states {
                cells.counts = vec![1, 1];
                if let Some(values) = &mut cells.sums {
                    *values = sums.iter().map(|n| i256::from_i128(*n)).collect();
                }
            }
            states.push(state);
        }
        let output = source_owned::SourceOwned::finish(
            states,
            &[Some((1, 3)), Some((3, 4))],
            &p.plan,
            &program,
            &Cancellation::default(),
            metadata,
        )
        .unwrap();
        assert_eq!(output.boundary_merges, 1);
        assert_eq!(output.group_count(), 3);
        let having =
            Having::for_plan(&p.plan, &predicate(1, Type::Int, Op::Ge, 8, Type::Int)).unwrap();
        for range in [0..3, 0..2, 1..3, 2..3, 0..0] {
            let _lease = budget
                .reserve(output.output_bytes(&p.plan, range.clone()).unwrap())
                .unwrap();
            let expected = having
                .filter(
                    output
                        .output_batch(&p.plan, &program, &schema, range.clone())
                        .unwrap(),
                )
                .unwrap();
            let actual = output
                .having_batch(&p.plan, &program, &schema, range, &having)
                .unwrap();
            assert_eq!(actual, expected);
        }
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn selection_cancellation_denial_and_consumer_error_release_all_credits() {
    for workers in [1, 2, 4, 10] {
        for failure in ["cancel", "deny", "consumer"] {
            let budget = Budget::new(1 << 20);
            let options = Options {
                workers,
                batch_size: 2,
                ..Options::default()
            };
            let p = prepared("SELECT k,SUM(v) FROM t GROUP BY k", true);
            let having =
                Having::for_plan(&p.plan, &predicate(1, Type::Int, Op::Gt, 0, Type::Int)).unwrap();
            let input = RecordBatch::try_new(
                p.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(
                        Decimal128Array::from(vec![0; 5])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap();
            let mut blocker = None;
            let mut emitted = 0;
            let result = execute::execute_having_stream(
                &p,
                &options,
                budget.clone(),
                Some(&having),
                |send| {
                    send(input)?;
                    if failure == "deny" {
                        blocker = Some(budget.reserve(budget.limit() - budget.used()).unwrap());
                    }
                    Ok(())
                },
                &mut |_| {
                    emitted += 1;
                    if failure == "cancel" {
                        options.cancellation.cancel();
                    }
                    if failure == "consumer" {
                        return Err("injected consumer failure".into());
                    }
                    Ok(())
                },
            );
            let error = result.unwrap_err();
            if failure == "deny" {
                assert!(error.starts_with("resource-limit:"));
                assert_eq!(emitted, 0);
            } else {
                assert_eq!(emitted, 1);
                assert_eq!(
                    error,
                    if failure == "cancel" {
                        "cancelled: query execution"
                    } else {
                        "injected consumer failure"
                    }
                );
            }
            drop(blocker);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
