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
                    let mut p = prepare(&sql, &tables, &on).unwrap();
                    orientation(p.relational.as_mut().unwrap(), reverse);
                    match execute(&p, &on) {
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
                memory_bytes: 64 << 10,
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
                            actual.as_ref().err().map(String::as_str),
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
