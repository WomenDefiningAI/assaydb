use super::super::super::tests::{Row, plan, state};
use super::*;
use crate::{expr_program::ExpressionProgram, spill::Cancellation};

fn raw(
    p: &Plan,
    e: &ExpressionProgram,
    rows: &[Row],
    budget: &Budget,
    sequence: u64,
) -> Arc<RawPartial> {
    Arc::new(
        state(p, e, rows, budget)
            .into_raw(RawFunding::SharedQuery(budget.clone()), sequence)
            .unwrap(),
    )
}

#[test]
fn producer_routes_match_original_seed_and_preserve_parallel_merge_order() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<RoutingPlan>();
    send_sync::<RoutedPartial>();
    let rows = vec![
        (Some(i128::MIN), Some("é\0"), Some(17)),
        (None, Some("é\0"), None),
        (Some(i128::MAX), None, Some(-7)),
        (Some(-1), Some(""), Some(3)),
        (Some(i128::MIN), Some("é\0"), Some(5)),
        (None, Some("é\0"), Some(9)),
    ];
    for sql in [
        "SELECT k,SUM(v),AVG(v),AVG(v),COUNT(v),COUNT(*) FROM t GROUP BY k",
        "SELECT k,s,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k,s",
    ] {
        let (p, e, schema) = plan(sql, false);
        for count in [1, 4, 8] {
            let budget = Budget::new(16 << 20);
            let mut owner = OwnedPartitions::new(&p, &e, budget.clone(), count).unwrap();
            let expected = state(&p, &e, &rows, &budget);
            for (sequence, chunk) in rows.chunks(2).enumerate() {
                let partial = raw(&p, &e, chunk, &budget, sequence as u64);
                let routing = owner.routing_plan();
                let permits = budget.workers(1, 1);
                assert_eq!(permits.count, 1);
                let routed = std::thread::scope(|scope| {
                    scope
                        .spawn(move || routing.route(partial, &Cancellation::default()))
                        .join()
                        .unwrap()
                })
                .unwrap();
                drop(permits);
                assert_eq!(
                    routed.groups.iter().map(Vec::len).sum::<usize>(),
                    routed.raw.group_count
                );
                for (slot, groups) in routed.groups.iter().enumerate() {
                    assert!(groups.windows(2).all(|pair| pair[0] < pair[1]));
                    for &local in groups {
                        // Independent routing oracle uses the original owner's
                        // seed and explicit NULL discriminator, not route().
                        let mut hash = owner.inner.hash.build_hasher();
                        if let Some(keys) = &routed.raw.numeric_group_keys {
                            (routed.raw.null_group != Some(local)).hash(&mut hash);
                            if routed.raw.null_group != Some(local) {
                                keys[local].hash(&mut hash);
                            }
                        } else {
                            routed.raw.encoded[local].as_ref().unwrap().hash(&mut hash);
                        }
                        assert_eq!(slot, (hash.finish() % count as u64) as usize);
                    }
                }
                let mut jobs = owner
                    .submit_routed(routed, &Cancellation::default())
                    .unwrap();
                jobs.reverse();
                for job in jobs {
                    owner
                        .acknowledge(job.run(&p, &e, &Cancellation::default()))
                        .unwrap();
                }
            }
            owner.finish(&Cancellation::default()).unwrap();
            assert_eq!(
                owner
                    .output_batch(&p, &e, &schema, 0..expected.group_count)
                    .unwrap(),
                expected
                    .output_batch(&p, &e, &schema, 0..expected.group_count)
                    .unwrap()
            );
            drop(owner);
            drop(expected);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn routing_handles_empty_all_null_and_private_backing_after_owner_drop() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    for rows in [vec![], vec![(None, None, None), (None, None, None)]] {
        let query = Budget::new(8 << 20);
        let owner = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
        let routing = owner.routing_plan();
        let private = Budget::new(1 << 20);
        let backing = query.reserve(private.limit()).unwrap();
        let partial = Arc::new(
            state(&p, &e, &rows, &private)
                .into_raw(RawFunding::PrepaidPrivate(backing), 0)
                .unwrap(),
        );
        let weak_raw = Arc::downgrade(&partial);
        let weak_plan = Arc::downgrade(&routing.0);
        let weak_base = Arc::downgrade(&owner.inner._metadata);
        let routed = routing.route(partial, &Cancellation::default()).unwrap();
        drop(owner);
        drop(routing);
        assert!(weak_raw.upgrade().is_some());
        assert!(weak_plan.upgrade().is_some());
        assert!(weak_base.upgrade().is_some());
        assert!(query.used() >= private.limit());
        drop(routed);
        assert!(weak_raw.upgrade().is_none());
        assert!(weak_plan.upgrade().is_none());
        assert!(weak_base.upgrade().is_none());
        assert_eq!(private.used(), 0);
        assert_eq!(query.used(), 0);
    }
}

#[test]
fn routed_wrong_owner_is_rejected_even_with_same_budget_and_layout() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    let query = Budget::new(8 << 20);
    let producer_owner = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
    let mut wrong = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
    let routed = producer_owner
        .routing_plan()
        .route(
            raw(&p, &e, &[(Some(1), None, None)], &query, 0),
            &Cancellation::default(),
        )
        .unwrap();
    let error = wrong
        .submit_routed(routed, &Cancellation::default())
        .err()
        .unwrap();
    assert!(error.contains("wrong-owner"));
    assert!(
        wrong
            .slots
            .iter()
            .all(|slot| slot.as_ref().unwrap().state.group_count == 0)
    );
    assert_eq!(wrong.finish(&Cancellation::default()).unwrap_err(), error);
    drop(wrong);
    drop(producer_owner);
    assert_eq!(query.used(), 0);
}

#[test]
fn producer_routing_rejects_foreign_funding_bad_shapes_and_cancellation_without_leaks() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    let query = Budget::new(8 << 20);
    let owner = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
    let routing = owner.routing_plan();
    let base = query.used();
    for mode in [
        "funding",
        "encounters",
        "numeric",
        "states",
        "sums",
        "cancel",
        "metadata",
    ] {
        let source = if mode == "funding" {
            Budget::new(4 << 20)
        } else {
            query.clone()
        };
        let mut partial = raw(&p, &e, &[(Some(1), None, None)], &source, 0);
        let cancel = Cancellation::default();
        match mode {
            "encounters" => {
                Arc::get_mut(&mut partial).unwrap().encounters.clear();
            }
            "numeric" => {
                Arc::get_mut(&mut partial)
                    .unwrap()
                    .numeric_group_keys
                    .as_mut()
                    .unwrap()
                    .clear();
            }
            "states" => {
                Arc::get_mut(&mut partial).unwrap().states[0].counts.clear();
            }
            "sums" => {
                Arc::get_mut(&mut partial).unwrap().states[0].sums = Some(vec![i256::ZERO]);
            }
            "cancel" => cancel.cancel(),
            _ => (),
        }
        let blocker =
            (mode == "metadata").then(|| query.reserve(query.limit() - query.used()).unwrap());
        assert!(routing.route(partial, &cancel).is_err(), "{mode}");
        drop(blocker);
        assert_eq!(query.used(), base, "{mode}");
        if mode == "funding" {
            assert_eq!(source.used(), 0);
        }
    }
    let (other, oe, _) = plan("SELECT k,SUM(v) FROM t GROUP BY k", false);
    assert!(routing.check_layout(&other, &oe).is_err());
    assert!(
        routing
            .route(
                raw(&other, &oe, &[(Some(1), None, Some(2))], &query, 0),
                &Cancellation::default()
            )
            .is_err()
    );
    drop(routing);
    drop(owner);
    assert_eq!(query.used(), 0);
}

#[test]
fn routed_destination_preflight_never_moves_slots_on_late_credit_failure() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    let query = Budget::new(8 << 20);
    let mut owner = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
    let rows = (0..40).map(|k| (Some(k), None, None)).collect::<Vec<_>>();
    let partial = raw(&p, &e, &rows, &query, 0);
    let routed = owner
        .routing_plan()
        .route(partial.clone(), &Cancellation::default())
        .unwrap();
    assert!(routed.growth.iter().filter(|&&n| n > 0).count() > 1);
    let first = *routed.growth.iter().find(|&&n| n > 0).unwrap();
    let metadata = routed.credit.bytes();
    let blocker = query.reserve(query.limit() - query.used() - first).unwrap();
    let before = query.used();
    assert!(
        owner
            .submit_routed(routed, &Cancellation::default())
            .err()
            .unwrap()
            .contains("resource-limit")
    );
    assert!(
        owner
            .slots
            .iter()
            .all(|slot| slot.as_ref().unwrap().state.group_count == 0)
    );
    assert_eq!(query.used(), before - metadata);
    drop(blocker);
    drop(partial);
    drop(owner);
    assert_eq!(query.used(), 0);
}

#[test]
fn empty_routed_input_advances_generation_and_worker_layout_errors_stay_sticky() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k", false);
    let query = Budget::new(8 << 20);
    let mut owner = OwnedPartitions::new(&p, &e, query.clone(), 1).unwrap();
    let empty = owner
        .routing_plan()
        .route(raw(&p, &e, &[], &query, 0), &Cancellation::default())
        .unwrap();
    assert!(
        owner
            .submit_routed(empty, &Cancellation::default())
            .unwrap()
            .is_empty()
    );
    assert_eq!(owner.next_sequence(), 1);
    let routed = owner
        .routing_plan()
        .route(
            raw(&p, &e, &[(Some(1), None, None)], &query, 1),
            &Cancellation::default(),
        )
        .unwrap();
    let job = owner
        .submit_routed(routed, &Cancellation::default())
        .unwrap()
        .pop()
        .unwrap();
    let (wrong, wrong_e, _) = plan("SELECT k,SUM(v) FROM t GROUP BY k", false);
    let completion = job.run(&wrong, &wrong_e, &Cancellation::default());
    let error = completion.error_message().unwrap().to_string();
    assert!(error.contains("layout mismatch"));
    assert_eq!(owner.acknowledge(completion).unwrap_err(), error);
    assert_eq!(owner.finish(&Cancellation::default()).unwrap_err(), error);
    drop(owner);
    assert_eq!(query.used(), 0);
}

#[test]
fn routed_fragments_can_wait_across_generations_without_reordering_group_encounters() {
    let (p, e, schema) = plan("SELECT s,COUNT(*) FROM t GROUP BY s", false);
    let query = Budget::new(8 << 20);
    let mut owner = OwnedPartitions::new(&p, &e, query.clone(), 4).unwrap();
    let rows = [(None, Some("first"), None), (None, Some("last"), None)];
    let expected = state(&p, &e, &rows, &query);
    let routing = owner.routing_plan();
    let first = routing
        .route(raw(&p, &e, &rows[..1], &query, 0), &Cancellation::default())
        .unwrap();
    let second = routing
        .route(raw(&p, &e, &rows[1..], &query, 1), &Cancellation::default())
        .unwrap();
    // Reverse arrival is legal. The owner binds generation only at admission;
    // producer source tokens, not arrival/generation, control result ordering.
    for fragment in [second, first] {
        for job in owner
            .submit_routed(fragment, &Cancellation::default())
            .unwrap()
        {
            owner
                .acknowledge(job.run(&p, &e, &Cancellation::default()))
                .unwrap();
        }
    }
    assert_eq!(owner.next_sequence(), 2);
    owner.finish(&Cancellation::default()).unwrap();
    assert_eq!(
        owner.output_batch(&p, &e, &schema, 0..2).unwrap(),
        expected.output_batch(&p, &e, &schema, 0..2).unwrap()
    );
    drop(expected);
    drop(owner);
    drop(routing);
    assert_eq!(query.used(), 0);
}
