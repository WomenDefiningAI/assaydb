use super::*;

fn cursor(start: i64, stop: i64, step: i64, endpoint: Endpoint) -> IntegerSeries {
    IntegerSeries::new(
        Some(start),
        Some(stop),
        Some(step),
        endpoint,
        &Cancellation::default(),
    )
    .unwrap()
}
fn collect(mut cursor: IntegerSeries, batch: usize) -> Vec<i64> {
    let budget = Budget::new(64 * 1024);
    let mut output = Vec::new();
    cursor
        .emit_batches(batch, &budget, &Cancellation::default(), &mut |chunk| {
            assert!(chunk.values().len() <= batch);
            output.extend_from_slice(chunk.values());
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(budget.used(), 0);
    output
}
#[test]
fn endpoint_and_direction_matrix() {
    for batch in [1, 2, 7, 8192] {
        assert_eq!(
            collect(cursor(0, 5, 1, Endpoint::Exclusive), batch),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(collect(cursor(2, 8, 3, Endpoint::Exclusive), batch), [2, 5]);
        assert_eq!(
            collect(cursor(2, 8, 3, Endpoint::Inclusive), batch),
            [2, 5, 8]
        );
        assert_eq!(
            collect(cursor(8, 2, -3, Endpoint::Exclusive), batch),
            [8, 5]
        );
        assert_eq!(
            collect(cursor(8, 2, -3, Endpoint::Inclusive), batch),
            [8, 5, 2]
        );
        assert_eq!(
            collect(cursor(2, 9, 3, Endpoint::Inclusive), batch),
            [2, 5, 8]
        );
        assert!(collect(cursor(2, 8, -1, Endpoint::Inclusive), batch).is_empty());
        assert!(collect(cursor(8, 2, 1, Endpoint::Exclusive), batch).is_empty());
        assert!(collect(cursor(2, 2, 1, Endpoint::Exclusive), batch).is_empty());
        assert_eq!(collect(cursor(2, 2, -1, Endpoint::Inclusive), batch), [2]);
    }
}
#[test]
fn extreme_endpoints_and_stride_never_wrap() {
    assert_eq!(
        collect(cursor(i64::MAX - 1, i64::MAX, 1, Endpoint::Inclusive), 7),
        [i64::MAX - 1, i64::MAX]
    );
    assert_eq!(
        collect(cursor(i64::MIN + 1, i64::MIN, -1, Endpoint::Inclusive), 7),
        [i64::MIN + 1, i64::MIN]
    );
    assert_eq!(
        collect(cursor(i64::MAX, i64::MIN, i64::MIN, Endpoint::Inclusive), 7),
        [i64::MAX, -1]
    );
    assert_eq!(
        collect(cursor(i64::MIN, i64::MAX, i64::MAX, Endpoint::Inclusive), 7),
        [i64::MIN, -1, i64::MAX - 1]
    );
    assert_eq!(
        cursor(i64::MIN, i64::MAX, 1, Endpoint::Inclusive).remaining(),
        1u128 << 64
    );
}
#[test]
fn huge_sequence_stops_after_one_funded_batch() {
    let budget = Budget::new(4096);
    let mut series = cursor(i64::MIN, i64::MAX, 1, Endpoint::Inclusive);
    let mut calls = 0;
    series
        .emit_batches(7, &budget, &Cancellation::default(), &mut |chunk| {
            calls += 1;
            assert_eq!(chunk.values().len(), 7);
            assert!(budget.used() >= 7 * 8);
            Ok(ControlFlow::Break(()))
        })
        .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(series.remaining(), (1u128 << 64) - 7);
    assert_eq!(budget.used(), 0);
}
#[test]
fn failures_preserve_cursor_and_release_credit() {
    let mut series = cursor(0, 100, 1, Endpoint::Exclusive);
    let tiny = Budget::new(1);
    assert!(
        series
            .next_batch(7, &tiny, &Cancellation::default())
            .is_err()
    );
    assert_eq!(series.remaining(), 100);
    assert_eq!(tiny.used(), 0);
    let budget = Budget::new(4096);
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(series.next_batch(7, &budget, &cancel).is_err());
    assert_eq!(series.remaining(), 100);
    assert_eq!(budget.used(), 0);
    assert!(
        series
            .next_batch(0, &budget, &Cancellation::default())
            .is_err()
    );
    let error = series.emit_batches(7, &budget, &Cancellation::default(), &mut |_| {
        Err("consumer failed".into())
    });
    assert!(error.is_err());
    assert_eq!(budget.used(), 0);
}
#[test]
fn duckdb2_null_zero_contract_is_explicit() {
    let cancel = Cancellation::default();
    for (start, stop, step) in [
        (None, Some(3), Some(1)),
        (Some(0), None, Some(1)),
        (Some(0), Some(3), None),
        (None, Some(3), Some(0)),
    ] {
        assert_eq!(
            IntegerSeries::new(start, stop, step, Endpoint::Exclusive, &cancel)
                .unwrap()
                .remaining(),
            0
        );
    }
    assert_eq!(
        IntegerSeries::new(Some(0), Some(0), Some(0), Endpoint::Inclusive, &cancel)
            .unwrap_err()
            .as_str(),
        "interval cannot be 0!"
    );
}
