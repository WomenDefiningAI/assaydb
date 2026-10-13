use super::*;
use crate::{
    expr::{Kind, Value},
    expr_program::ExpressionProgram,
    spill::Cancellation,
};
use arrow::datatypes::{DataType, Field, Schema};
type Row = (Option<i128>, Option<&'static str>, Option<i128>);
fn plan(sql: &str) -> (Plan, ExpressionProgram, SchemaRef) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Decimal128(38, 0), true),
        Field::new("s", DataType::Utf8, true),
        Field::new("v", DataType::Decimal128(38, 0), true),
    ]);
    let p = bind::bind(&bind::parse(sql).unwrap(), &schema, None, true).unwrap();
    let e = ExpressionProgram::new(&p, 3, true);
    let output = Arc::new(Schema::new(
        p.names
            .iter()
            .zip(&p.types)
            .map(|(name, ty)| Field::new(name, ty.arrow(), true))
            .collect::<Vec<_>>(),
    ));
    (p, e, output)
}
fn batch(p: &Plan, rows: &[Row], query: &Budget) -> EvaluatedBatch {
    let numeric = |values: Vec<Option<i128>>| {
        Value::adopt(
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            query,
        )
        .unwrap()
    };
    let columns = [
        numeric(rows.iter().map(|r| r.0).collect::<Vec<_>>()),
        Value::adopt(
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
            query,
        )
        .unwrap(),
        numeric(rows.iter().map(|r| r.2).collect::<Vec<_>>()),
    ];
    let value = |e: &crate::expr::Expr| {
        let Kind::Column(i) = e.kind else {
            panic!("column fixture")
        };
        columns[i].clone()
    };
    let keys = p.groups.iter().map(value).collect::<Vec<_>>();
    let prepared_keys = Some(crate::keys::Keys::new(&keys, rows.len(), query).unwrap());
    EvaluatedBatch {
        rows: rows.len(),
        keys,
        values: p
            .aggregates
            .iter()
            .map(|a| a.input.as_ref().map(value))
            .collect(),
        prepared_keys,
    }
}
fn state(p: &Plan, e: &ExpressionProgram, budget: &Budget) -> Aggregation {
    let mut state = Aggregation::new(p, e, budget.clone(), true).unwrap();
    state.disable_dense();
    state
}
fn rows() -> [Vec<Row>; 2] {
    let limit = 10_i128.pow(38) - 1;
    [
        vec![
            (Some(3), Some("a"), Some(limit)),
            (Some(1), Some("b"), Some(7)),
            (Some(3), Some("a"), Some(1)),
            (None, None, Some(2)),
        ],
        vec![
            (Some(3), Some("a"), Some(-limit)),
            (Some(2), Some("c"), Some(3)),
            (Some(1), Some("b"), Some(-2)),
        ],
    ]
}

#[test]
fn first_group_occurrence_preserves_stable_filtered_order_and_wide_values() {
    for sql in [
        "SELECT k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY k",
        "SELECT s,k,SUM(v),AVG(v),COUNT(*) FROM t GROUP BY s,k",
    ] {
        let (p, e, schema) = plan(sql);
        let budget = Budget::new(8 << 20);
        let mut ordered = state(&p, &e, &budget);
        let mut arbitrary = state(&p, &e, &budget);
        let input = rows();
        for (index, physical_rows) in [vec![2, 5, 9, 12], vec![0, 4, 8]].iter().enumerate() {
            let prefix = SourceBatch::new(0, index as u64 * 2);
            ordered
                .update_ordered_source(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    batch(&p, &input[index], &budget),
                    prefix,
                )
                .unwrap();
            let tokens = physical_rows
                .iter()
                .map(|row| prefix.token(*row))
                .collect::<Vec<_>>();
            arbitrary
                .update_with_encounters(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    batch(&p, &input[index], &budget),
                    &tokens,
                )
                .unwrap();
        }
        assert_eq!(ordered.group_count, 4);
        assert_eq!(
            ordered.encounters,
            vec![
                SourceBatch::new(0, 0).token(0),
                SourceBatch::new(0, 0).token(1),
                SourceBatch::new(0, 0).token(3),
                SourceBatch::new(0, 2).token(1)
            ]
        );
        for a in 0..4 {
            for b in 0..4 {
                assert_eq!(
                    ordered.encounters[a].cmp(&ordered.encounters[b]),
                    arbitrary.encounters[a].cmp(&arbitrary.encounters[b])
                );
            }
        }
        assert_eq!(
            ordered.output_batch(&p, &e, &schema, 0..4).unwrap(),
            arbitrary.output_batch(&p, &e, &schema, 0..4).unwrap()
        );
        drop(ordered);
        drop(arbitrary);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn repeated_or_reversed_prefix_and_arbitrary_history_reject_before_mutation() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k");
    let budget = Budget::new(4 << 20);
    let mut ordered = state(&p, &e, &budget);
    let input = [(Some(3), None, None), (Some(1), None, None)];
    ordered
        .update_ordered_source(
            &p,
            &e,
            &mut Metrics::default(),
            batch(&p, &input, &budget),
            SourceBatch::new(4, 2),
        )
        .unwrap();
    for prefix in [
        SourceBatch::new(4, 2),
        SourceBatch::new(4, 1),
        SourceBatch::new(3, 999),
    ] {
        let used = budget.used();
        assert!(
            ordered
                .update_ordered_source(
                    &p,
                    &e,
                    &mut Metrics::default(),
                    batch(&p, &input, &budget),
                    prefix
                )
                .is_err()
        );
        assert_eq!(ordered.states[0].counts, vec![1, 1]);
        assert_eq!(ordered.group_count, 2);
        assert!(ordered.encounters_valid);
        assert_eq!(budget.used(), used);
    }
    // An empty input is skipped by the producer after evaluating expressions;
    // this API rejects it rather than changing its source-prefix contract.
    assert!(
        ordered
            .update_ordered_source(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, &[], &budget),
                SourceBatch::new(4, 3)
            )
            .is_err()
    );
    ordered
        .update_with_encounters(
            &p,
            &e,
            &mut Metrics::default(),
            batch(&p, &input, &budget),
            &[EncounterToken::new(0, 0, 4), EncounterToken::new(0, 0, 2)],
        )
        .unwrap();
    assert_eq!(
        ordered.encounters,
        vec![EncounterToken::new(0, 0, 4), EncounterToken::new(0, 0, 2)]
    );
    assert!(
        ordered
            .update_ordered_source(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, &input, &budget),
                SourceBatch::new(9, 9)
            )
            .is_err()
    );
    drop(ordered);
    assert_eq!(budget.used(), 0);
}

#[test]
fn reversed_pressure_fragments_merge_duplicate_keys_by_minimum_source_coordinate() {
    let (p, e, schema) = plan("SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k");
    let query = Budget::new(16 << 20);
    let mut fragments = Vec::new();
    let mut expected = state(&p, &e, &query);
    for (index, input) in rows().iter().enumerate() {
        let prefix = SourceBatch::new(index as u64, 0);
        let backing = query.reserve(512 << 10).unwrap();
        let private = Budget::new(512 << 10);
        let mut producer = state(&p, &e, &private);
        let input_batch = batch(&p, input, &query);
        assert!(
            producer
                .batch_export_fits(&p, &e, &input_batch, &query)
                .unwrap()
        );
        producer
            .update_ordered_source(&p, &e, &mut Metrics::default(), input_batch, prefix)
            .unwrap();
        fragments.push(Arc::new(
            producer
                .into_raw_with_encounters(RawFunding::PrepaidPrivate(backing))
                .unwrap(),
        ));
        expected
            .update_ordered_source(
                &p,
                &e,
                &mut Metrics::default(),
                batch(&p, input, &query),
                prefix,
            )
            .unwrap();
    }
    let cancel = Cancellation::default();
    let mut owner = OwnedPartitions::new(&p, &e, query.clone(), 3).unwrap();
    for raw in fragments.into_iter().rev() {
        let routed = owner.routing_plan().route(raw, &cancel).unwrap();
        let jobs = owner.submit_routed(routed, &cancel).unwrap();
        for job in jobs.into_iter().rev() {
            owner.acknowledge(job.run(&p, &e, &cancel)).unwrap();
        }
    }
    owner.finish(&cancel).unwrap();
    assert_eq!(owner.group_count(), 4);
    assert_eq!(
        owner.output_batch(&p, &e, &schema, 0..4).unwrap(),
        expected.output_batch(&p, &e, &schema, 0..4).unwrap()
    );
    drop(owner);
    drop(expected);
    assert_eq!(query.used(), 0);
}

#[test]
fn failed_ordered_update_retains_grown_encounter_credit_and_blocks_export() {
    let (p, e, _) = plan("SELECT k,COUNT(*) FROM t GROUP BY k");
    let budget = Budget::new(4 << 20);
    let mut producer = state(&p, &e, &budget);
    producer
        .update_ordered_source(
            &p,
            &e,
            &mut Metrics::default(),
            batch(&p, &[(Some(0), None, None)], &budget),
            SourceBatch::new(0, 0),
        )
        .unwrap();
    // Inject an otherwise unreachable raw count to force failure after new
    // groups and their first-encounter entries have already been appended.
    producer.states[0].counts[0] = u64::MAX;
    let old_capacity = producer.encounters.capacity();
    let input = (1..64)
        .chain(std::iter::once(0))
        .map(|key| (Some(key), None, None))
        .collect::<Vec<_>>();
    let error = producer
        .update_ordered_source(
            &p,
            &e,
            &mut Metrics::default(),
            batch(&p, &input, &budget),
            SourceBatch::new(0, 1),
        )
        .unwrap_err();
    assert_eq!(error, "aggregate count overflow");
    assert_eq!(producer.group_count, 64);
    assert_eq!(producer.encounters.len(), 64);
    assert_eq!(producer.encounters[1], SourceBatch::new(0, 1).token(0));
    let added =
        (producer.encounters.capacity() - old_capacity) * std::mem::size_of::<EncounterToken>();
    assert!(added > 0);
    assert_eq!(producer.state_leases.last().unwrap().bytes(), added);
    assert!(!producer.encounters_valid);
    assert!(
        producer
            .into_raw_with_encounters(RawFunding::SharedQuery(budget.clone()))
            .is_err()
    );
    assert_eq!(budget.used(), 0);
}
