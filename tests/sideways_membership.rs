use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    expr::{Expr, Kind},
    rel::{self, JoinKind, Node, RelOp},
    types::Type,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture {
    paths: Vec<PathBuf>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for path in &self.paths {
            std::fs::remove_file(path).unwrap();
        }
    }
}
impl Fixture {
    fn scan(
        &mut self,
        name: &str,
        keys: Vec<Option<i64>>,
        links: Vec<Option<i64>>,
        estimate: usize,
        domain: usize,
    ) -> Node {
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{name}.k"), DataType::Int64, true),
            Field::new(format!("{name}.n"), DataType::Int64, true),
            Field::new(format!("{name}.v"), DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(links)),
                Arc::new(Int64Array::from_iter_values(0..keys.len() as i64)),
            ],
        )
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "assaydb-sideways-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(7)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        self.paths.push(path.clone());
        Node {
            schema,
            estimated_rows: estimate,
            op: RelOp::Scan {
                path,
                columns: vec![0, 1, 2],
                distinct: vec![domain; 3],
                statistics: vec![],
            },
        }
    }
    fn plan(empty: bool, null_only: bool, empty_probe: bool) -> (Self, Node) {
        let mut files = Self { paths: vec![] };
        let facts = if empty_probe { 0 } else { 43 };
        // Repetition, skew, NULLs and non-sorted encounter order are deliberate.
        let fact = files.scan(
            "f",
            (0..facts)
                .map(|i| (i % 11 != 0).then_some((i * 7) % 64))
                .collect(),
            (0..facts).map(|i| Some(((i * 7) % 64) % 16)).collect(),
            100_000,
            64,
        );
        let dim_keys = if empty {
            vec![]
        } else if null_only {
            vec![None, None]
        } else {
            vec![Some(3), None, Some(1), Some(3)]
        };
        let dim = files.scan("d", dim_keys.clone(), vec![Some(0); dim_keys.len()], 4, 16);
        let customers = files.scan(
            "c",
            (0..128).map(|i| (i % 23 != 0).then_some(i % 64)).collect(),
            (0..128).map(|i| (i % 29 != 0).then_some(i % 16)).collect(),
            10_000,
            64,
        );
        let orders = files.scan(
            "o",
            (0..64).map(Some).collect(),
            (0..64).map(Some).collect(),
            20_000,
            64,
        );
        let with_dim = join(fact, dim, &[(1, 0)]);
        let with_orders = join(with_dim, orders, &[(0, 0)]);
        // The third equality is redundant but is retained by transitive closure.
        // It forces propagation beyond the ordinary one/two-key index layout.
        (
            files,
            join(with_orders, customers, &[(7, 0), (3, 1), (1, 1)]),
        )
    }
}
fn col(i: usize) -> Expr {
    Expr {
        kind: Kind::Column(i),
        ty: Type::Int,
    }
}
fn join(left: Node, right: Node, pairs: &[(usize, usize)]) -> Node {
    let schema = Arc::new(Schema::new(
        left.schema
            .fields()
            .iter()
            .chain(right.schema.fields())
            .cloned()
            .collect::<Vec<_>>(),
    ));
    Node {
        schema,
        estimated_rows: 100_000,
        op: RelOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: JoinKind::Inner,
            keys: pairs.iter().map(|(a, b)| (col(*a), col(*b))).collect(),
            residual: None,
        },
    }
}

#[test]
fn sideways_retains_exact_order_duplicates_nulls_and_empty_inputs() {
    for (empty, null_only, empty_probe) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let (_files, plan) = Fixture::plan(empty, null_only, empty_probe);
        let baseline = rel::execute(
            &plan,
            &Options {
                workers: 1,
                batch_size: 7,
                relational_optimizations: false,
                ..Options::default()
            },
        )
        .unwrap();
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let actual = rel::execute(&plan, &options).unwrap();
                assert_eq!(
                    actual.batch, baseline.batch,
                    "workers={workers} batch={batch_size} empty={empty} null={null_only} probe={empty_probe}"
                );
                assert_eq!(actual.metrics.sideways_prepared_builds, 1);
                if !empty && !null_only {
                    assert_eq!(actual.metrics.sideways_scan_filters, 1);
                    assert!(actual.metrics.join_build_rows < baseline.metrics.join_build_rows);
                    assert_eq!(actual.metrics.join_build_rows, 28);
                }
                options.cancellation.cancel();
                assert_eq!(
                    rel::execute(&plan, &options).err().unwrap(),
                    options.cancellation.check().unwrap_err()
                );
            }
        }
    }
}

#[test]
fn estimated_source_rows_do_not_replace_completed_key_cardinality() {
    for selective in [true, false] {
        let (mut files, mut plan) = Fixture::plan(false, false, false);
        let RelOp::Join { left, right, .. } = &mut plan.op else {
            unreachable!()
        };
        let RelOp::Scan { distinct, .. } = &mut right.op else {
            unreachable!()
        };
        distinct[1] = 25;
        let RelOp::Join { left, .. } = &mut left.op else {
            unreachable!()
        };
        let RelOp::Join { right, .. } = &mut left.op else {
            unreachable!()
        };
        // Both estimates miss the former 4x gate (10 * 4 >= 25).
        // Only the completed two-key source meets it; seven keys must decline.
        let keys = if selective {
            vec![Some(3), None, Some(1), Some(3)]
        } else {
            (0..7).map(Some).collect()
        };
        **right = files.scan("d", keys.clone(), vec![Some(0); keys.len()], 10, 25);
        let baseline = rel::execute(
            &plan,
            &Options {
                workers: 1,
                batch_size: 1,
                relational_optimizations: false,
                ..Options::default()
            },
        )
        .unwrap();
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let actual = rel::execute(
                    &plan,
                    &Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    },
                )
                .unwrap();
                assert_eq!(
                    actual.batch, baseline.batch,
                    "selective={selective} workers={workers} batch={batch_size}"
                );
                assert_eq!(actual.metrics.sideways_prepared_builds, 1);
                assert_eq!(actual.metrics.sideways_scan_filters, usize::from(selective));
                if selective {
                    // Customer and order builds both shrink, with duplicate
                    // occurrences and their encounter order still intact.
                    assert_eq!(actual.metrics.join_build_rows, 28);
                    assert!(actual.metrics.join_build_rows < baseline.metrics.join_build_rows);
                }
            }
        }
    }
}

#[test]
fn sideways_declines_fallible_off_key_work_and_preserves_earliest_error() {
    let (_files, mut plan) = Fixture::plan(false, false, false);
    let RelOp::Join { right, .. } = &mut plan.op else {
        unreachable!()
    };
    let input = right.clone();
    let huge = Expr {
        kind: Kind::Literal(assaydb::expr::Literal::Num(10_i128.pow(37))),
        ty: Type::Int,
    };
    let product = Expr {
        kind: Kind::Binary(assaydb::expr::Op::Mul, Box::new(col(2)), Box::new(huge)),
        ty: Type::Int,
    };
    let predicate = Expr {
        kind: Kind::Binary(assaydb::expr::Op::Gt, Box::new(product), Box::new(col(2))),
        ty: Type::Bool,
    };
    right.op = RelOp::Filter { input, predicate };
    let expected = rel::execute(
        &plan,
        &Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        },
    )
    .err()
    .unwrap();
    for batch_size in [1, 7, 8192] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            assert_eq!(rel::execute(&plan, &options).err().unwrap(), expected);
        }
    }
}

#[test]
fn oversized_optional_source_retries_the_unchanged_plan_without_retention() {
    let mut files = Fixture { paths: vec![] };
    let fact = files.scan(
        "f",
        (0..21).rev().map(Some).collect(),
        vec![Some(0); 21],
        100_000,
        1000,
    );
    // Deliberately inaccurate admission estimates must affect only cost.
    let dim = files.scan(
        "d",
        (0..4097).map(Some).collect(),
        vec![Some(0); 4097],
        2,
        1000,
    );
    let build = files.scan(
        "b",
        (0..64).map(Some).collect(),
        vec![Some(0); 64],
        10_000,
        1000,
    );
    let plan = join(join(fact, dim, &[(0, 0)]), build, &[(0, 0)]);
    let expected = rel::execute(
        &plan,
        &Options {
            workers: 1,
            relational_optimizations: false,
            ..Options::default()
        },
    )
    .unwrap();
    for workers in [1, 2, 4, 10] {
        let actual = rel::execute(
            &plan,
            &Options {
                workers,
                ..Options::default()
            },
        )
        .unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.sideways_prepared_builds, 0);
        assert_eq!(actual.metrics.sideways_scan_filters, 0);
    }
}
