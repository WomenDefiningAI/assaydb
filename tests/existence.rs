use arrow::{
    array::*,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    rel::{JoinKind, Node, RelOp},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new(repeat: usize, narrow: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-existence-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let values = [
            None,
            Some(7),
            None,
            Some(7),
            Some(-9),
            None,
            Some(7),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
            Some(0),
            Some(-9),
        ];
        let n = values.len() * repeat;
        let num: Vec<_> = (0..n).map(|i| values[i % values.len()]).collect();
        let text: Vec<_> = num
            .iter()
            .map(|v| {
                v.map(|v| match v {
                    7 => "é雪\0",
                    -9 => "é雪",
                    0 => "",
                    _ => "longer",
                })
            })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| {
                        if i % 12 == 9 {
                            None
                        } else {
                            Some((i % 3) as i64)
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| {
                        if i % 12 == 5 {
                            None
                        } else {
                            Some(if i % 2 == 0 { "a" } else { "b" })
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(num.clone())),
            if narrow {
                Arc::new(Int32Array::from(
                    num.iter().map(|v| v.map(|v| v as i32)).collect::<Vec<_>>(),
                ))
            } else {
                Arc::new(Int64Array::from(
                    num.iter()
                        .map(|v| v.map(|v| i64::from(v as i32)))
                        .collect::<Vec<_>>(),
                ))
            },
            Arc::new(
                Decimal128Array::from(num.iter().map(|v| v.map(i128::from)).collect::<Vec<_>>())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(
                num.iter().map(|v| v.map(|v| v as i32)).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                num.iter().map(|v| v.map(|v| v > 0)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(text)),
            Arc::new(Float64Array::from(
                num.iter().map(|v| v.map(|v| v as f64)).collect::<Vec<_>>(),
            )),
        ];
        let names = [
            "id", "k", "k2", "n", "small", "d", "day", "flag", "txt", "f",
        ];
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .zip(&arrays)
                .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(5)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
fn orientation(node: &mut Node, reverse: bool) {
    match &mut node.op {
        RelOp::Join {
            left, right, kind, ..
        } => {
            orientation(left, reverse);
            orientation(right, reverse);
            if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                left.estimated_rows = if reverse { 0 } else { 100 };
                right.estimated_rows = 1;
            }
        }
        RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Project { input, .. }
        | RelOp::Rename { input }
        | RelOp::Limit { input, .. } => orientation(input, reverse),
        _ => {}
    }
}

// Change only cost estimates at existence nodes; physical Scan row bounds and
// the scalar/aggregate plans remain untouched.
fn scalar_orientation(node: &mut Node, reverse: bool, actual_cost_miss: bool) {
    match &mut node.op {
        RelOp::Join {
            left, right, kind, ..
        } => {
            if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                left.estimated_rows = if reverse { 0 } else { usize::MAX / 4 };
                right.estimated_rows = if actual_cost_miss { 1 } else { 1_000_000 };
            }
            scalar_orientation(left, reverse, actual_cost_miss);
            scalar_orientation(right, reverse, actual_cost_miss);
        }
        RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Project { input, .. }
        | RelOp::Rename { input }
        | RelOp::Limit { input, .. } => scalar_orientation(input, reverse, actual_cost_miss),
        _ => {}
    }
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn scalar_candidate_existence_preserves_ordinals_duplicates_nulls_and_empty_inputs() {
    let l = Fixture::new(1, true);
    let r = Fixture::new(8, false);
    let empty = Fixture::new(0, false);
    let tables = HashMap::from([
        ("l".into(), l.0.clone()),
        ("r".into(), r.0.clone()),
        ("empty".into(), empty.0.clone()),
    ]);
    for (source, predicate, anti, semi) in [
        ("r", "b.id<2", vec![8, 9, 11], vec![6, 7, 10]),
        ("r", "b.k IS NULL", vec![6, 7, 8, 9, 10, 11], vec![]),
        ("r", "b.id<0", vec![6, 7, 8, 9, 10, 11], vec![]),
        ("empty", "b.id<2", vec![6, 7, 8, 9, 10, 11], vec![]),
    ] {
        for (not, expected) in [("NOT ", anti), ("", semi)] {
            // The candidate has duplicate keys and a NULL key (id=9); the
            // larger source has duplicate keys and NULLs too. AVG completes
            // before the candidate comparison and before any membership work.
            let sql = format!(
                "SELECT a.id FROM l a WHERE a.id > (SELECT AVG(id) FROM l) AND {not}EXISTS(SELECT 1 FROM {source} b WHERE a.k=b.k AND {predicate})"
            );
            for batch_size in [1, 7, 8192] {
                for workers in [1, 2, 4, 10] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    for (reverse, cost_miss) in [(false, false), (true, false), (true, true)] {
                        let mut plan = prepare(&sql, &tables, &options).unwrap();
                        scalar_orientation(plan.relational.as_mut().unwrap(), reverse, cost_miss);
                        let result = execute(&plan, &options).unwrap();
                        assert_eq!(
                            ids(&result.batch),
                            expected,
                            "{sql}, workers={workers}, batch={batch_size}, reverse={reverse}, miss={cost_miss}"
                        );
                        assert_eq!(
                            result.metrics.reverse_existence_joins,
                            usize::from(reverse && !cost_miss),
                            "{sql}"
                        );
                        assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn scalar_candidate_errors_keep_scalar_completion_and_build_precedence() {
    let l = Fixture::new(1, true);
    let r = Fixture::new(8, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for (condition, probe, expected) in [
        (
            "a.id > (SELECT id FROM l)",
            "b.id<0",
            "scalar subquery returned multiple rows",
        ),
        (
            "a.id<0 AND a.id > (SELECT id FROM l)",
            "b.id<2",
            "scalar subquery returned multiple rows",
        ),
        (
            "a.id > (SELECT MAX(f*CAST('1e308' AS REAL)) FROM l)",
            "b.id<0",
            "floating arithmetic overflow",
        ),
        // The ordinary RHS build must still win when it is fallible. This
        // shape is refused by reversal even though its estimate is favorable.
        (
            "a.id > (SELECT id FROM l)",
            "b.f*CAST('1e308' AS REAL)>0",
            "floating arithmetic overflow",
        ),
        // Two scalar consumers keep the existing right-before-left order.
        (
            "a.id > (SELECT id FROM l) AND a.id > (SELECT MAX(f*CAST('1e308' AS REAL)) FROM l)",
            "b.id<2",
            "floating arithmetic overflow",
        ),
    ] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {condition} AND {not}EXISTS(SELECT 1 FROM r b WHERE a.k=b.k AND {probe})"
            );
            for batch_size in [1, 7, 8192] {
                for workers in [1, 2, 4, 10] {
                    for reverse in [false, true] {
                        let options = Options {
                            workers,
                            batch_size,
                            ..Options::default()
                        };
                        let mut plan = prepare(&sql, &tables, &options).unwrap();
                        scalar_orientation(plan.relational.as_mut().unwrap(), reverse, false);
                        assert_eq!(
                            execute(&plan, &options).err().unwrap(),
                            expected,
                            "{sql}, workers={workers}, batch={batch_size}, reverse={reverse}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn scalar_candidate_budget_spill_and_cancellation_match_original_path() {
    use assaydb::spill::{SpillMode, SpillOptions};
    let l = Fixture::new(1, true);
    let r = Fixture::new(8, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    let spill_dir = l.0.with_extension("spill");
    std::fs::create_dir(&spill_dir).unwrap();
    for (not, expected) in [("NOT ", vec![8, 9, 11]), ("", vec![6, 7, 10])] {
        let sql = format!(
            "SELECT a.id FROM l a WHERE a.id > (SELECT AVG(id) FROM l) AND {not}EXISTS(SELECT 1 FROM r b WHERE a.k=b.k AND b.id<2)"
        );
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                for mode in [None, Some(SpillMode::Force), Some(SpillMode::Auto)] {
                    let mut successes = 0;
                    let mut failures = 0;
                    for memory_bytes in [1024, 32 << 10, 256 << 10, 4 << 20] {
                        let options = Options {
                            workers,
                            batch_size,
                            memory_bytes,
                            parquet_cache_bytes: Some(0),
                            spill: mode.map(|mode| SpillOptions {
                                mode,
                                directory: spill_dir.clone(),
                                sort_run_bytes: 8192,
                                ..SpillOptions::default()
                            }),
                            ..Options::default()
                        };
                        // These controls exercise execution/cancellation of a
                        // retained plan. Fund its planning metadata separately
                        // from the deliberately tiny execution budgets.
                        let planning = Options {
                            memory_bytes: 4 << 20,
                            ..options.clone()
                        };
                        let mut plan = prepare(&sql, &tables, &planning).unwrap();
                        scalar_orientation(plan.relational.as_mut().unwrap(), true, false);
                        match execute(&plan, &options) {
                            Ok(result) => {
                                successes += 1;
                                assert_eq!(ids(&result.batch), expected, "{sql}, {mode:?}");
                                assert!(result.metrics.peak_reserved_bytes <= memory_bytes);
                                if mode == Some(SpillMode::Force) {
                                    assert_eq!(result.metrics.reverse_existence_joins, 0);
                                }
                            }
                            Err(error) => {
                                failures += 1;
                                assert!(error.starts_with("resource-limit:"), "{error}");
                            }
                        }
                        assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
                        options.cancellation.cancel();
                        assert_eq!(
                            execute(&plan, &options).err().unwrap(),
                            "cancelled: query execution"
                        );
                    }
                    assert!(successes > 0 && failures > 0);
                }
            }
        }
    }
    std::fs::remove_dir(spill_dir).unwrap();
}

#[test]
fn scalar_candidate_natural_costing_decimal_average_and_empty_candidates() {
    let l = Fixture::new(1, true);
    let r = Fixture::new(16, false);
    let empty = Fixture::new(0, false);
    let tables = HashMap::from([
        ("l".into(), l.0.clone()),
        ("r".into(), r.0.clone()),
        ("empty".into(), empty.0.clone()),
    ]);
    for (filter, anti, semi) in [
        (
            "SUBSTRING(a.txt,1,1) IN ('é','x') AND a.d > (SELECT AVG(d) FROM l)",
            vec![],
            vec![1, 3, 6],
        ),
        ("a.id > (SELECT AVG(id) FROM empty)", vec![], vec![]),
        ("a.id<0 AND a.id > (SELECT AVG(id) FROM l)", vec![], vec![]),
    ] {
        for (not, expected) in [("NOT ", anti), ("", semi)] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {filter} AND {not}EXISTS(SELECT 1 FROM r b WHERE a.k=b.k)"
            );
            let mut baseline = prepare(&sql, &tables, &Options::default()).unwrap();
            scalar_orientation(baseline.relational.as_mut().unwrap(), false, false);
            let baseline = execute(&baseline, &Options::default()).unwrap().batch;
            assert_eq!(ids(&baseline), expected, "{sql}");
            for batch_size in [1, 7, 8192] {
                for workers in [1, 2, 4, 10] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let plan = prepare(&sql, &tables, &options).unwrap();
                    let result = execute(&plan, &options).unwrap();
                    assert_eq!(
                        result.batch, baseline,
                        "{sql}, workers={workers}, batch={batch_size}"
                    );
                    assert_eq!(
                        result.metrics.reverse_existence_joins, 1,
                        "natural admission: {sql}; plan={:?}",
                        plan.relational
                    );
                }
            }
        }
    }
}

#[test]
fn inequality_existence_preserves_types_nulls_duplicates_order_and_orientation() {
    let l = Fixture::new(2, true);
    let r = Fixture::new(3, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for column in ["n", "small", "d", "day", "flag", "txt"] {
        for key in ["a.k=b.k", "a.k=b.k AND a.k2=b.k2"] {
            for not in ["", "NOT "] {
                for swapped in [false, true] {
                    let residual = if swapped {
                        format!("b.{column}<>a.{column}")
                    } else {
                        format!("a.{column}<>b.{column}")
                    };
                    let sql = format!(
                        "SELECT a.id FROM l a WHERE {not}EXISTS (SELECT 1 FROM r b WHERE {key} AND {residual})"
                    );
                    for batch_size in [1, 7, 128] {
                        let off = Options {
                            batch_size,
                            relational_optimizations: false,
                            ..Options::default()
                        };
                        let expected = execute(&prepare(&sql, &tables, &off).unwrap(), &off)
                            .unwrap()
                            .batch;
                        for reverse in [false, true] {
                            let on = Options {
                                relational_optimizations: true,
                                workers: 4,
                                ..off.clone()
                            };
                            let mut p = prepare(&sql, &tables, &on).unwrap();
                            orientation(p.relational.as_mut().unwrap(), reverse);
                            let actual = execute(&p, &on).unwrap();
                            assert_eq!(
                                actual.metrics.reverse_existence_joins,
                                usize::from(reverse),
                                "{sql}"
                            );
                            assert_eq!(
                                actual.batch, expected,
                                "{sql}, reverse={reverse}, batch={batch_size}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_and_computed_residuals_preserve_results_and_errors() {
    let l = Fixture::new(1, true);
    let tables = HashMap::from([("l".into(), l.0.clone())]);
    for residual in [
        "a.f<>b.f",
        "a.d<>b.n",
        "a.n+0<>b.n",
        "a.n<>b.n AND a.flag<>b.flag",
        "a.n<b.n",
        "a.n<>a.small",
    ] {
        let sql = format!(
            "SELECT a.id FROM l a WHERE EXISTS(SELECT 1 FROM l b WHERE a.k=b.k AND {residual})"
        );
        let off = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(&sql, &tables, &off).unwrap(), &off).map(|r| r.batch);
        for reverse in [false, true] {
            let on = Options::default();
            let mut p = prepare(&sql, &tables, &on).unwrap();
            orientation(p.relational.as_mut().unwrap(), reverse);
            let actual = execute(&p, &on);
            assert_eq!(actual.map(|r| r.batch), expected, "{sql}");
        }
    }
}

#[test]
fn later_child_errors_are_not_hidden_by_existence_matches() {
    let l = Fixture::new(1, true);
    let r = Fixture::new(4, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for not in ["", "NOT "] {
        for key in ["a.k=b.k", "a.k=b.k + CAST(1.0/(b.id-30) AS INTEGER)"] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS(SELECT 1 FROM r b WHERE {key} AND a.n<>b.n AND (b.id<24 OR CAST(1.0/(b.id-30) AS INTEGER)>0))"
            );
            for reverse in [false, true] {
                let on = Options {
                    batch_size: 1,
                    ..Options::default()
                };
                let mut p = prepare(&sql, &tables, &on).unwrap();
                orientation(p.relational.as_mut().unwrap(), reverse);
                let error = execute(&p, &on)
                    .err()
                    .expect("later right error must be evaluated");
                assert!(error.contains("division by zero"), "{error}: {sql}");
            }
        }
    }
}

#[test]
fn empty_and_all_null_sides_preserve_anti_rows() {
    let l = Fixture::new(1, true);
    let tables = HashMap::from([("l".into(), l.0.clone())]);
    for filter in ["b.id<0", "b.n IS NULL", "b.id<4", "b.id<8"] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS(SELECT 1 FROM l b WHERE a.k=b.k AND a.n<>b.n AND {filter})"
            );
            let off = Options {
                relational_optimizations: false,
                batch_size: 1,
                ..Options::default()
            };
            let expected = execute(&prepare(&sql, &tables, &off).unwrap(), &off)
                .unwrap()
                .batch;
            for reverse in [false, true] {
                let on = Options {
                    relational_optimizations: true,
                    ..off.clone()
                };
                let mut p = prepare(&sql, &tables, &on).unwrap();
                orientation(p.relational.as_mut().unwrap(), reverse);
                assert_eq!(
                    execute(&p, &on).unwrap().batch,
                    expected,
                    "{sql}; reverse={reverse}"
                );
            }
        }
    }
}

#[test]
fn no_equality_key_and_small_budgets_keep_exact_results_or_resource_errors() {
    let l = Fixture::new(1, true);
    let tables = HashMap::from([("l".into(), l.0.clone())]);
    for key in ["", "a.k=b.k AND "] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS(SELECT 1 FROM l b WHERE {key}a.n<>b.n)"
            );
            let off = Options {
                relational_optimizations: false,
                ..Options::default()
            };
            let expected = execute(&prepare(&sql, &tables, &off).unwrap(), &off)
                .unwrap()
                .batch;
            for reverse in [false, true] {
                let mut successes = 0;
                let mut failures = 0;
                for memory_bytes in [1024, 8192, 32768, 65536, 262144, 1048576] {
                    let on = Options {
                        memory_bytes,
                        batch_size: 7,
                        ..Options::default()
                    };
                    let result = prepare(&sql, &tables, &on).and_then(|mut p| {
                        orientation(p.relational.as_mut().unwrap(), reverse);
                        execute(&p, &on)
                    });
                    match result {
                        Ok(result) => {
                            successes += 1;
                            assert_eq!(result.batch, expected);
                        }
                        Err(error) => {
                            failures += 1;
                            assert!(error.starts_with("resource-limit:"), "{error}");
                        }
                    }
                }
                assert!(
                    successes > 0 && failures > 0,
                    "expected both resource rejection and successful execution"
                );
            }
        }
    }
}

#[test]
fn reverse_lane_membership_preserves_exact_order_types_nulls_and_duplicates() {
    let l = Fixture::new(2, true);
    let r = Fixture::new(4, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for key in [
        "a.k=b.k",
        "a.k=b.k AND a.k2=b.k2",
        "a.small=b.small",
        "a.d=b.d",
        "a.day=b.day",
        "a.flag=b.flag",
        "a.txt=b.txt",
        "a.f=b.f",
    ] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS (SELECT 1 FROM r b WHERE {key} AND b.id<19)"
            );
            let off = Options {
                relational_optimizations: false,
                ..Options::default()
            };
            let expected = execute(&prepare(&sql, &tables, &off).unwrap(), &off)
                .unwrap()
                .batch;
            for batch_size in [1, 7, 8192] {
                let mut plan = prepare(&sql, &tables, &Options::default()).unwrap();
                orientation(plan.relational.as_mut().unwrap(), true);
                for workers in [1, 2, 4, 10] {
                    let on = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let actual = execute(&plan, &on).unwrap();
                    assert_eq!(
                        actual.batch, expected,
                        "{sql}, workers={workers}, batch={batch_size}"
                    );
                    assert_eq!(actual.metrics.reverse_existence_joins, 1, "{sql}");
                    let direct = matches!(key, "a.k=b.k" | "a.small=b.small" | "a.d=b.d");
                    assert_eq!(
                        actual.metrics.reverse_existence_lane_joins,
                        usize::from(workers > 1 && direct),
                        "{sql}"
                    );
                    if workers > 1 && direct {
                        assert!(actual.metrics.reverse_existence_lane_batches > 0, "{sql}");
                    }
                    assert!(actual.metrics.worker_threads_peak <= workers);
                }
            }
        }
    }
}

#[test]
fn reverse_text_keys_decline_lanes_under_a_tight_budget() {
    let l = Fixture::new(2, true);
    let r = Fixture::new(4, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for not in ["", "NOT "] {
        let sql = format!(
            "SELECT a.id FROM l a WHERE {not}EXISTS (SELECT 1 FROM r b WHERE a.txt=b.txt AND b.id<19)"
        );
        for batch_size in [1, 7, 8192] {
            let serial = Options {
                batch_size,
                // Include the two footer/decoder lifetimes. This remains too
                // small for a helper and keeps the serial lane-decline check.
                memory_bytes: 128 << 10,
                parquet_cache_bytes: None,
                ..Options::default()
            };
            let mut plan = prepare(&sql, &tables, &serial).unwrap();
            orientation(plan.relational.as_mut().unwrap(), true);
            let expected = execute(&plan, &serial).unwrap();
            // Keep room for parallel reader bookkeeping, but use the same tight
            // budget as the successful serial probe at every worker count.
            let memory_bytes = serial.memory_bytes;
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    memory_bytes,
                    ..serial.clone()
                };
                let actual = execute(&plan, &options).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}, workers={workers}");
                assert_eq!(actual.metrics.reverse_existence_joins, 1);
                assert_eq!(actual.metrics.reverse_existence_lane_joins, 0);
                assert_eq!(actual.metrics.reverse_existence_lane_batches, 0);
                assert!(actual.metrics.peak_reserved_bytes <= memory_bytes);
            }
        }
    }
}

#[test]
fn reverse_lane_empty_inputs_coercions_and_errors_match_serial() {
    let l = Fixture::new(1, true);
    let r = Fixture::new(3, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for condition in [
        "a.k=b.k AND b.id<0", // Empty probe after filtering.
        "a.k=b.k AND a.id<0", // Empty retained side after filtering.
        "a.k=b.k AND b.n IS NULL",
        "a.small=b.n", // Physical Int32/Int64 normalize to the same integer keys.
        "a.d=b.n",     // Mixed scales retain their existing coercion path.
        "a.k+1=b.k",   // Computed keys decline lane admission.
        "a.k=b.k AND a.n<>b.n", // Residuals retain the serial reverse path.
        "a.k=b.k AND b.f*CAST('1e308' AS REAL)>0.0", // Required child overflow.
    ] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS(SELECT 1 FROM r b WHERE {condition})"
            );
            let mut plan = prepare(&sql, &tables, &Options::default()).unwrap();
            orientation(plan.relational.as_mut().unwrap(), true);
            let expected = execute(
                &plan,
                &Options {
                    batch_size: 1,
                    ..Options::default()
                },
            )
            .map(|r| r.batch);
            for batch_size in [1, 7, 8192] {
                for workers in [1, 2, 4, 10] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let actual = execute(&plan, &options);
                    if condition.contains("1e308") {
                        assert_eq!(
                            actual.as_ref().err().map(assaydb::Error::as_str),
                            Some("floating arithmetic overflow"),
                            "{sql}"
                        );
                    }
                    assert_eq!(
                        actual.map(|r| r.batch),
                        expected,
                        "{sql}, workers={workers}, batch={batch_size}"
                    );
                }
            }
        }
    }
}

#[test]
fn reverse_lane_source_completion_precedes_fallible_output() {
    let l = Fixture::new(2, true);
    let r = Fixture::new(3, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    let sql =
        "SELECT a.f*CAST('1e308' AS REAL) FROM l a WHERE EXISTS(SELECT 1 FROM r b WHERE a.k=b.k)";
    let mut plan = prepare(sql, &tables, &Options::default()).unwrap();
    orientation(plan.relational.as_mut().unwrap(), true);
    let expected = execute(&plan, &Options::default()).err().unwrap();
    assert_eq!(expected, "floating arithmetic overflow");
    for batch_size in [1, 7, 8192] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            assert_eq!(execute(&plan, &options).err().unwrap(), expected);
        }
    }
}

#[test]
fn reverse_numeric_bitmap_dispatch_preserves_semi_and_anti_occurrences() {
    let l = Fixture::new(2, true);
    let r = Fixture::new(4, false);
    let tables = HashMap::from([("l".into(), l.0.clone()), ("r".into(), r.0.clone())]);
    for (condition, bounded) in [
        ("a.k=b.k AND b.id<19", true),
        ("a.n=b.n AND b.id<19", false), // i64 extrema decline the bounded range.
        ("a.k=b.k AND a.k2=b.k2 AND b.id<19", false),
    ] {
        for not in ["", "NOT "] {
            let sql = format!(
                "SELECT a.id FROM l a WHERE {not}EXISTS(SELECT 1 FROM r b WHERE {condition})"
            );
            let mut plan = prepare(&sql, &tables, &Options::default()).unwrap();
            orientation(plan.relational.as_mut().unwrap(), true);
            let expected = execute(&plan, &Options::default()).unwrap().batch;
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let result = execute(&plan, &options).unwrap();
                    assert_eq!(result.batch, expected, "{sql}: {workers}/{batch_size}");
                    assert_eq!(
                        result.metrics.reverse_existence_numeric_lane_joins,
                        usize::from(bounded && workers > 1),
                        "{sql}"
                    );
                }
            }
        }
    }
}
