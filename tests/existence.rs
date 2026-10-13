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
