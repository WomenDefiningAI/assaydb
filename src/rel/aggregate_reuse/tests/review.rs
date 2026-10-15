use super::*;

// Check the retained plan, its actual rewritten attempt (even on error), and
// whole-query fallback independently. A successful fallback alone cannot prove
// that an overflow fixture exercised reuse.
fn check_matrix(root: &Node, admitted: bool, expected: Result<RecordBatch>) {
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let attempt = candidate(root, &budget, &options);
            assert_eq!(attempt.is_some(), admitted, "{workers}/{batch_size}");
            if let Some(attempt) = attempt {
                assert_eq!(
                    baseline(&attempt.root, &options, budget.clone()).map(|e| e.batch),
                    expected,
                    "rewritten attempt: {workers}/{batch_size}"
                );
                drop(attempt);
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
            assert_eq!(
                baseline(root, &options, budget.clone()).map(|e| e.batch),
                expected,
                "reuse disabled: {workers}/{batch_size}"
            );
            let actual = execute_once(root, &options, budget.clone(), None, false);
            if let Ok(output) = &actual {
                assert_eq!(output.metrics.aggregate_scan_reuses, usize::from(admitted));
            }
            assert_eq!(
                actual.map(|e| e.batch),
                expected,
                "whole query: {workers}/{batch_size}"
            );
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

fn serial_batch(root: &Node) -> RecordBatch {
    let options = Options {
        workers: 1,
        batch_size: 1,
        ..Options::default()
    };
    baseline(root, &options, Budget::new(options.memory_bytes))
        .unwrap()
        .batch
}

#[test]
fn admitted_integer_sums_check_public_bigint_boundaries_after_reuse() {
    for mut values in [
        vec![i64::MAX, 1, -1],
        vec![i64::MIN, -1, 1],
        vec![i64::MAX, 1, 0],
    ] {
        let mut fixture = Fixture::new(false, false, false, 15);
        // Keep the fact side larger than the dimension, so the retained
        // physical plan puts the fact scan on AG1's required probe path.
        values.resize(12, 0);
        fixture.write(
            "i",
            &["k", "v"],
            vec![
                Arc::new(Int64Array::from(vec![1; values.len()])),
                Arc::new(Int64Array::from(values)),
            ],
        );
        // k=1 has two dimension matches in label a. The first two cases
        // fit BIGINT in the producer but overflow after duplicate expansion;
        // the third is already wide in the producer and must remain internal.
        let sql = SQL
            .replace("FROM d,f", "FROM d,i")
            .replace("f.", "i.")
            .replace("FROM f GROUP", "FROM i GROUP")
            .replace("SUM(v)>0", "SUM(v) IS NOT NULL");
        check_matrix(&fixture.plan(&sql), true, Err("BIGINT overflow".into()));
    }
}

#[test]
fn mixed_sign_decimal_boundaries_admit_only_proven_outer_prefixes() {
    for precision in [36, 37] {
        let magnitude = 10_i128.pow(u32::from(precision)) - 1;
        for last in [1, magnitude] {
            let mut fixture = Fixture::new(false, false, false, 15);
            fixture.write(
                "wide",
                &["k", "v"],
                vec![
                    Arc::new(Int64Array::from(vec![1; 8])),
                    Arc::new(
                        Decimal128Array::from(vec![
                            magnitude, magnitude, -magnitude, -magnitude, last, last, 0, 0,
                        ])
                        .with_precision_and_scale(precision, 2)
                        .unwrap(),
                    ),
                ],
            );
            fixture.write(
                "copies",
                &["k", "label"],
                vec![
                    Arc::new(Int64Array::from(vec![1; 6])),
                    Arc::new(StringArray::from(vec!["a"; 6])),
                ],
            );
            let sql = SQL
                .replace("FROM d,f", "FROM copies d,wide f")
                .replace("SELECT k FROM f", "SELECT k FROM wide");
            let root = fixture.plan(&sql);
            // Producer bound is 8*(10^p-1), outer bound 48*(10^p-1).
            // Only p=36 proves every old/new prefix <10^38. At p=37
            // retain recomputation, whether cancellation rescues the total
            // or duplicate expansion makes the completed total overflow.
            let expected = if precision == 37 && last == magnitude {
                Err("decimal precision 38 overflow".into())
            } else {
                let batch = serial_batch(&root);
                assert_eq!(batch.num_rows(), 1);
                let totals = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                assert_eq!(totals.values().as_ref(), &[12 * last]);
                assert_eq!(totals.null_count(), 0);
                Ok(batch)
            };
            check_matrix(&root, precision == 36, expected);
        }
    }
}

#[test]
fn admitted_downstream_errors_keep_expression_order() {
    let fixture = Fixture::new(false, false, false, 15);
    let inner = SQL.replace(" ORDER BY 2,1 LIMIT 100", "");
    let divide = "total / 0.00";
    let overflow = "total * 99999999999999999999999999999999999999";
    // Each expression fails independently; swapping their slots must swap
    // the selected error. Both depend on completed reused aggregate output.
    for (projection, error) in [
        (divide.to_owned(), "division by zero"),
        (overflow.to_owned(), "numeric overflow"),
        (format!("{divide}, {overflow}"), "division by zero"),
        (format!("{overflow}, {divide}"), "numeric overflow"),
    ] {
        let root = fixture.plan(&format!("SELECT {projection} FROM ({inner}) q"));
        check_matrix(&root, true, Err(error.into()));
    }
}

#[test]
fn tied_order_limit_one_and_truncating_limits_preserve_selected_groups() {
    let fixture = Fixture::new(false, false, false, 15);
    // The two k=1 groups tie on the sole ordering key. LIMIT 1 must keep
    // the same first-seen occurrence as recomputation, not merely the bag.
    for (suffix, labels, totals) in [
        (" ORDER BY 2 LIMIT 1", vec!["b"], vec![600]),
        (" ORDER BY 2 LIMIT 2", vec!["b", "a"], vec![600, 1200]),
        (" ORDER BY 2,1 LIMIT 2", vec!["a", "b"], vec![1200, 600]),
    ] {
        let root = fixture.plan(&SQL.replace(" ORDER BY 2,1 LIMIT 100", suffix));
        let batch = serial_batch(&root);
        assert_eq!(batch.num_rows(), labels.len());
        let actual_labels = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            actual_labels.iter().collect::<Vec<_>>(),
            labels.into_iter().map(Some).collect::<Vec<_>>()
        );
        let actual_totals = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(actual_totals.values().as_ref(), totals.as_slice());
        check_matrix(&root, true, Ok(batch));
    }
}
