//! Per-reference transforms over fully populated, immutable shared batches.
use super::{Node, RelOp, ScanKeyFilter, ordered_batches, scan_key_mask};
use crate::{
    Result,
    execute::Options,
    expr::Expr,
    memory::Budget,
    parquet_scan::{BatchTransform, FilterStats, exact_filter},
};
use arrow::{compute::filter_record_batch, datatypes::Schema, record_batch::RecordBatch};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

/// Do not cross expressions, cardinality barriers, or another filter. Each
/// reference is identified by its node, never by the shared storage ID.
pub(super) fn target(node: &Node, mut predicate: Expr) -> Result<Option<(usize, Expr)>> {
    match &node.op {
        RelOp::Materialize {
            shared_scan: true, ..
        } => Ok(Some((node as *const Node as usize, predicate))),
        RelOp::Rename { input } => target(input, predicate),
        RelOp::Project { input, columns } => {
            predicate.remap_columns(&columns.iter().map(|i| Some(*i)).collect::<Vec<_>>())?;
            target(input, predicate)
        }
        _ => Ok(None),
    }
}
#[derive(Default)]
struct Work {
    seconds: f64,
    rows: usize,
}
impl ordered_batches::Report for Work {
    fn merge(&mut self, other: Self) {
        self.seconds += other.seconds;
        self.rows += other.rows;
    }
}
#[derive(Default)]
pub(super) struct Statistics {
    pub parallel: bool,
    pub tasks: usize,
    pub batches: usize,
    pub pending_peak: usize,
    pub pending_bytes_peak: usize,
    pub filter_seconds: f64,
    pub input_rows: usize,
    pub exact_batches: usize,
}
fn configuration(
    options: &Options,
    budget: &Budget,
    batches: &[RecordBatch],
) -> Option<ordered_batches::Configuration> {
    let wanted = options.workers.saturating_sub(1).min(4);
    if wanted < 2 || batches.len() < wanted * 4 {
        return None;
    }
    // Raw storage is already charged. Admit optional parallelism only when
    // several full input/workspace/output allowances fit beyond that storage.
    let largest = batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .max()?;
    let rows = batches.iter().map(RecordBatch::num_rows).max()?;
    let per_lane = largest
        .checked_mul(8)?
        .checked_add(rows.checked_mul(96)?)?
        .checked_add(8192)?;
    let affordable = budget.limit().saturating_sub(budget.used()) / per_lane;
    let permits = budget.workers(wanted.min(affordable), options.workers.saturating_sub(1));
    if permits.count < 2 {
        return None;
    }
    ordered_batches::Configuration::with_permits(budget, permits)
}
fn transform(
    batch: RecordBatch,
    filter: Option<&ScanKeyFilter>,
    exact: Option<&BatchTransform>,
    budget: &Budget,
    check: &dyn Fn() -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Work> {
    check()?;
    let start = Instant::now();
    let rows = batch.num_rows();
    let _membership_lease;
    let batch = if let Some(filter) = filter {
        _membership_lease = Some(
            budget.reserve(
                batch
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(256),
            )?,
        );
        let mask = scan_key_mask(&batch, filter, budget)?;
        filter_record_batch(&batch, &mask).map_err(|e| e.to_string())?
    } else {
        _membership_lease = None;
        batch
    };
    check()?;
    if let Some(exact) = exact {
        // Even an empty membership result must evaluate the exact expression:
        // scalar errors on empty input are observable on the serial path.
        let output = exact(batch)?;
        let seconds = start.elapsed().as_secs_f64();
        check()?;
        let _lease = output.lease;
        if output.batch.num_rows() > 0 {
            emit(output.batch)?;
        }
        Ok(Work { seconds, rows })
    } else {
        let seconds = start.elapsed().as_secs_f64();
        check()?;
        // Membership-only replay preserves delivery of empty batches.
        emit(batch)?;
        Ok(Work { seconds, rows })
    }
}
/// The owner of `batches` and its raw-buffer leases outlive this entire call,
/// including cancellation, queue disconnection and every worker join.
pub(super) fn run(
    batches: &[RecordBatch],
    schema: &Schema,
    filter: Option<&ScanKeyFilter>,
    predicate: Option<Expr>,
    options: &Options,
    budget: &Budget,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Statistics> {
    if batches.is_empty() {
        return Ok(Statistics::default());
    }
    let exact_stats = Arc::new(Mutex::new(FilterStats::default()));
    let exact = predicate
        .map(|p| {
            exact_filter(
                p,
                schema,
                budget,
                &options.cancellation,
                false,
                exact_stats.clone(),
            )
        })
        .transpose()?;
    let mut stats = Statistics::default();
    let work = if let Some(config) = configuration(options, budget, batches) {
        let result = ordered_batches::run(
            config,
            budget,
            &options.cancellation,
            |batch, send, check| transform(batch, filter, exact.as_ref(), budget, check, send),
            |send| batches.iter().try_for_each(|batch| send(batch.clone())),
            emit,
        )?;
        stats.parallel = true;
        stats.tasks = result.tasks;
        stats.batches = result.batches;
        stats.pending_peak = result.pending_peak;
        stats.pending_bytes_peak = result.pending_bytes_peak;
        result.report
    } else {
        let mut result = Work::default();
        for batch in batches {
            let work = transform(
                batch.clone(),
                filter,
                exact.as_ref(),
                budget,
                &|| options.cancellation.check(),
                &mut |batch| {
                    stats.batches += 1;
                    emit(batch)
                },
            )?;
            result.seconds += work.seconds;
            result.rows += work.rows;
        }
        result
    };
    stats.filter_seconds = work.seconds;
    stats.input_rows = work.rows;
    stats.exact_batches = exact_stats.lock().unwrap().batches;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bind,
        expr::{Kind, Literal},
        types::Type,
    };
    use arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::{DataType, Field},
    };
    use std::collections::{HashMap, HashSet};

    fn batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(values)) as ArrayRef)])
            .unwrap()
    }
    fn predicate(sql: &str, schema: &Schema) -> Expr {
        bind::bind(&bind::parse(sql).unwrap(), schema, None, true)
            .unwrap()
            .filter
            .unwrap()
    }
    fn membership(budget: &Budget, values: &[i128]) -> ScanKeyFilter {
        let members: HashSet<Box<[u8]>, ahash::RandomState> = values
            .iter()
            .map(|n| n.to_ne_bytes().to_vec().into_boxed_slice())
            .collect();
        ScanKeyFilter {
            expressions: vec![Expr {
                kind: Kind::Column(0),
                ty: Type::Int,
            }],
            members: Arc::new(members),
            numeric: None,
            numeric_pair: None,
            _lease: Arc::new(budget.reserve(0).unwrap()),
        }
    }
    fn clean(budget: &Budget) {
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.workers(9, 9).count, 9);
    }
    #[test]
    fn parallel_and_occupied_permit_replay_preserve_order_and_raw_aliases() {
        let raw = batch(vec![Some(99), Some(1), Some(3), None, Some(5), Some(99)]);
        let batches = vec![raw.slice(1, 4); 24];
        let original = batches.clone();
        let budget = Budget::new(8 << 20);
        let filter = membership(&budget, &[1, 3, 5]);
        for sql in ["SELECT x FROM t WHERE x > 2", "SELECT x FROM t WHERE x < 4"] {
            let expr = predicate(sql, &raw.schema());
            let mut expected = vec![];
            run(
                &batches,
                &raw.schema(),
                Some(&filter),
                Some(expr.clone()),
                &Options::default(),
                &budget,
                &mut |b| {
                    expected.push(b);
                    Ok(())
                },
            )
            .unwrap();
            for workers in [2, 4, 10] {
                for occupied in [false, true] {
                    let permits = occupied.then(|| budget.workers(workers - 1, workers - 1));
                    let options = Options {
                        workers,
                        batch_size: 4,
                        ..Options::default()
                    };
                    let mut actual = vec![];
                    let stats = run(
                        &batches,
                        &raw.schema(),
                        Some(&filter),
                        Some(expr.clone()),
                        &options,
                        &budget,
                        &mut |b| {
                            actual.push(b);
                            Ok(())
                        },
                    )
                    .unwrap();
                    assert_eq!(actual, expected);
                    assert_eq!(stats.parallel, workers > 2 && !occupied);
                    assert_eq!(stats.exact_batches, 24);
                    assert!(stats.pending_peak <= 8);
                    assert_eq!(batches, original);
                    drop(permits);
                    clean(&budget);
                }
            }
        }
    }
    #[test]
    fn empty_membership_still_evaluates_exact_and_keeps_membership_only_delivery() {
        let budget = Budget::new(8 << 20);
        let filter = membership(&budget, &[]);
        let raw = batch(vec![Some(1), None]);
        let batches = vec![raw.clone(); 24];
        let exact: BatchTransform = Arc::new(|b| {
            assert_eq!(b.num_rows(), 0);
            Err("injected exact error on empty input".into())
        });
        assert_eq!(
            transform(
                raw.clone(),
                Some(&filter),
                Some(&exact),
                &budget,
                &|| Ok(()),
                &mut |_| panic!("no output")
            )
            .err()
            .unwrap(),
            "injected exact error on empty input"
        );
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                ..Options::default()
            };
            let mut delivered = 0;
            run(
                &batches,
                &raw.schema(),
                Some(&filter),
                None,
                &options,
                &budget,
                &mut |b| {
                    assert_eq!(b.num_rows(), 0);
                    delivered += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(delivered, 24);
            let expr = predicate("SELECT x FROM t WHERE x > 0", &raw.schema());
            let stats = run(
                &batches,
                &raw.schema(),
                Some(&filter),
                Some(expr),
                &options,
                &budget,
                &mut |_| panic!("exact empty output is suppressed"),
            )
            .unwrap();
            assert_eq!(stats.exact_batches, 24);
            clean(&budget);
        }
    }
    #[test]
    fn earlier_callback_failure_precedes_later_exact_failure_and_cleans_up() {
        let budget = Budget::new(8 << 20);
        let mut batches = vec![batch(vec![Some(1)]); 24];
        batches[1] = batch(vec![Some(0)]);
        let expr = predicate("SELECT x FROM t WHERE 1.0 / x > 0", &batches[0].schema());
        for workers in [2, 4, 10] {
            for fail_callback in [false, true] {
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let mut delivered = 0;
                let error = run(
                    &batches,
                    &batches[0].schema(),
                    None,
                    Some(expr.clone()),
                    &options,
                    &budget,
                    &mut |_| {
                        delivered += 1;
                        if fail_callback {
                            Err("earlier callback".into())
                        } else {
                            Ok(())
                        }
                    },
                )
                .err()
                .unwrap();
                assert_eq!(delivered, 1);
                assert_eq!(
                    error,
                    if fail_callback {
                        "earlier callback"
                    } else {
                        "division by zero"
                    }
                );
                clean(&budget);
            }
        }
    }
    #[test]
    fn cancellation_and_low_budget_release_replay_resources() {
        let raw = batch(vec![Some(1); 64]);
        let batches = vec![raw.clone(); 24];
        let expr = predicate("SELECT x FROM t WHERE x > 0", &raw.schema());
        let budget = Budget::new(8 << 20);
        let options = Options {
            workers: 10,
            ..Options::default()
        };
        let error = run(
            &batches,
            &raw.schema(),
            None,
            Some(expr.clone()),
            &options,
            &budget,
            &mut |_| {
                options.cancellation.cancel();
                Ok(())
            },
        )
        .err()
        .unwrap();
        assert!(error.contains("cancelled"));
        clean(&budget);
        let tiny = Budget::new(1024);
        let error = run(
            &batches,
            &raw.schema(),
            None,
            Some(expr),
            &Options {
                workers: 10,
                ..Options::default()
            },
            &tiny,
            &mut |_| panic!("unreserved output"),
        )
        .err()
        .unwrap();
        assert!(error.contains("resource-limit"), "{error}");
        clean(&tiny);
    }
    #[test]
    fn targeting_uses_each_reference_and_remaps_project_ordinals() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));
        let leaf = || Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op: RelOp::Materialize {
                id: 7,
                shared_scan: true,
                input: Box::new(Node {
                    schema: schema.clone(),
                    estimated_rows: 1,
                    op: RelOp::Empty,
                }),
            },
        };
        let first = leaf();
        let second = leaf();
        let p = Expr {
            kind: Kind::Column(0),
            ty: Type::Int,
        };
        assert_ne!(
            target(&first, p.clone()).unwrap().unwrap().0,
            target(&second, p.clone()).unwrap().unwrap().0
        );
        let projected = Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op: RelOp::Project {
                columns: vec![1, 1],
                input: Box::new(first),
            },
        };
        let renamed = Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op: RelOp::Rename {
                input: Box::new(projected),
            },
        };
        assert_eq!(
            target(&renamed, p.clone()).unwrap().unwrap().1.kind,
            Kind::Column(1)
        );
        for op in [
            RelOp::Filter {
                predicate: Expr {
                    ty: Type::Bool,
                    kind: Kind::Literal(Literal::Bool(true)),
                },
                input: Box::new(leaf()),
            },
            RelOp::Limit {
                count: 1,
                input: Box::new(leaf()),
            },
            RelOp::Scalar {
                input: Box::new(leaf()),
            },
            RelOp::Materialize {
                id: 7,
                shared_scan: false,
                input: Box::new(leaf()),
            },
        ] {
            assert!(
                target(
                    &Node {
                        schema: schema.clone(),
                        estimated_rows: 1,
                        op
                    },
                    p.clone()
                )
                .unwrap()
                .is_none()
            );
        }
    }
    #[test]
    fn materialization_child_error_precedes_any_replay_delivery() {
        use super::super::Context;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let node = |op| Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op,
        };
        let plan = bind::bind(
            &bind::parse("SELECT 1 AS x").unwrap(),
            &Schema::empty(),
            None,
            true,
        )
        .unwrap();
        let first = node(RelOp::Unary {
            plan,
            input: Box::new(Node {
                schema: Arc::new(Schema::empty()),
                estimated_rows: 1,
                op: RelOp::One,
            }),
        });
        let source = node(RelOp::UnionAll {
            left: Box::new(first),
            right: Box::new(node(RelOp::Scan {
                path: std::env::temp_dir()
                    .join(format!("absent-replay-source-{}", std::process::id())),
                columns: vec![0],
                distinct: vec![],
                statistics: vec![],
            })),
        });
        let materialized = node(RelOp::Materialize {
            id: 0,
            shared_scan: true,
            input: Box::new(source),
        });
        let root = node(RelOp::Filter {
            predicate: Expr {
                ty: Type::Bool,
                kind: Kind::Literal(Literal::Bool(true)),
            },
            input: Box::new(materialized),
        });
        let options = Options {
            workers: 10,
            ..Options::default()
        };
        let budget = Budget::new(8 << 20);
        let mut ctx = Context {
            shared_attempt: None,
            options: &options,
            scan_filters: HashMap::new(),
            static_filters: HashMap::new(),
            scan_transforms: HashMap::new(),
            replay_transforms: HashMap::new(),
            materialized: HashMap::new(),
            spill: None,
            budget: budget.clone(),
            metrics: Default::default(),
        };
        assert!(
            root.run(&mut ctx, &mut |_| panic!(
                "raw population must finish first"
            ))
            .is_err()
        );
        assert!(
            ctx.metrics
                .operator_profiles
                .iter()
                .any(|p| p["operator"] == "ProjectSort" && p["output_rows"] == 1)
        );
        assert!(ctx.materialized.is_empty());
        assert!(ctx.replay_transforms.is_empty());
        clean(&budget);
    }
}
