//! Unique-build probe chains run in ordered scan lanes without expanding batches.
use super::{Context, JoinKind, Node, RelOp, column_projection, membership_safety, probe};
use crate::{
    Result,
    execute::Options,
    expr::{Expr, Kind},
    memory::Reservation,
    parquet_scan::{BatchTransform, FilterStats, TransformedBatch},
};
use arrow::{
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

mod predicate_memory;
use predicate_memory::PredicateMemory;

pub(super) struct Suffix {
    transform: BatchTransform,
    lanes: Vec<Arc<AtomicBool>>,
}
impl Suffix {
    /// A receipt is marked only when every stage reaches the actual scan dispatch.
    pub(super) fn prepend(
        transform: BatchTransform,
        downstream: Option<Self>,
    ) -> (Self, Arc<AtomicBool>) {
        let reached_scan = Arc::new(AtomicBool::new(false));
        let (transform, mut lanes) = match downstream {
            Some(suffix) => (
                compose(Some(transform), Some(suffix.transform)).unwrap(),
                suffix.lanes,
            ),
            None => (transform, Vec::new()),
        };
        lanes.push(reached_scan.clone());
        (Self { transform, lanes }, reached_scan)
    }
}

/// Admit only total streaming probe chains; build dependencies keep their old order.
pub(super) fn admitted(node: &Node, ctx: &Context<'_>) -> Result<bool> {
    if !ctx.options.relational_optimizations || ctx.options.workers < 2 || ctx.spill.is_some() {
        return Ok(false);
    }
    if !node.schema.fields().iter().all(|field| {
        matches!(
            field.data_type(),
            DataType::Null
                | DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Date32
                | DataType::Decimal128(_, _)
                | DataType::Utf8
        )
    }) {
        return Ok(false);
    }
    match &node.op {
        RelOp::Scan { .. } => Ok(!super::filtered_reuse::needs_ordered_scan(node, ctx)),
        RelOp::Rename { input } | RelOp::Project { input, .. } => admitted(input, ctx),
        RelOp::Filter { input, predicate } => {
            Ok(membership_safety::can_move_membership_across(node)
                && PredicateMemory::for_expression(predicate).is_some()
                && admitted(input, ctx)?)
        }
        RelOp::Unary { input, plan } => Ok(ctx.options.projection_pushdown
            && membership_safety::can_move_membership_across(node)
            && column_projection::columns(plan, &input.schema, &node.schema).is_some()
            && admitted(input, ctx)?),
        RelOp::Join {
            left,
            right,
            kind,
            keys,
            residual,
        } => {
            if !matches!(kind, JoinKind::Inner | JoinKind::Semi | JoinKind::Anti)
                || residual.is_some() || keys.is_empty()
                || !membership_safety::join_conditions_are_total(node)
                || !keys.iter().all(|(left, right)| matches!(left.kind, Kind::Column(_))
                    && matches!(right.kind, Kind::Column(_)) && left.ty == right.ty)
                // Preserve the reverse-existence and cached-probe dispatches.
                || (matches!(kind, JoinKind::Semi | JoinKind::Anti)
                    && left.estimated_rows.saturating_mul(4) < right.estimated_rows)
                || (matches!(kind, JoinKind::Inner)
                    && left.estimated_rows <= right.estimated_rows
                    && super::aggregate_scan_target(right, keys.iter().map(|(_, e)| e.clone()).collect())?.is_some())
            {
                return Ok(false);
            }
            admitted(left, ctx)
        }
        _ => Ok(false),
    }
}

/// Hold each intermediate reservation until the next total transform has consumed it.
pub(super) fn compose(
    first: Option<BatchTransform>,
    second: Option<BatchTransform>,
) -> Option<BatchTransform> {
    match (first, second) {
        (Some(first), Some(second)) => Some(Arc::new(move |batch| {
            let intermediate = first(batch)?;
            let result = second(intermediate.batch);
            drop(intermediate.lease);
            result
        })),
        (first, second) => first.or(second),
    }
}

/// Fallback delivery applies the same pending total suffix on the coordinator.
pub(super) fn deliver(
    batch: RecordBatch,
    transform: &Option<Suffix>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    if let Some(transform) = transform {
        let output = (transform.transform)(batch)?;
        if output.batch.num_rows() > 0 {
            emit(output.batch)?;
        }
        Ok(())
    } else {
        emit(batch)
    }
}

/// Unique keys bound output by input cardinality; one funded gather preserves probe order.
#[allow(clippy::too_many_arguments)]
pub(super) fn probe_transform(
    build: probe::owned::OwnedBuild,
    keys: Vec<(Expr, Expr)>,
    kind: JoinKind,
    output_schema: SchemaRef,
    options: &Options,
    stats: Arc<Mutex<probe::Statistics>>,
    descriptors: Reservation,
) -> BatchTransform {
    let cancellation = options.cancellation.clone();
    Arc::new(move |batch| {
        let _descriptors = &descriptors;
        let (batch, lease, report) =
            build.probe_unique(batch, &keys, kind, output_schema.clone(), &cancellation)?;
        stats.lock().unwrap().merge(report);
        Ok(TransformedBatch {
            batch,
            lease,
            preserve_empty: false,
        })
    })
}

/// Push a total suffix to its scan, building nested joins in the original sequence.
pub(super) fn run(
    node: &Node,
    ctx: &mut Context<'_>,
    mut suffix: Suffix,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    // Prepay closures, column vectors and total-expression clones before creating them.
    let descriptor_bytes = node
        .schema
        .fields()
        .len()
        .checked_mul(256)
        .and_then(|n| n.checked_add(2048))
        .ok_or("resource-limit: lane descriptor size overflow")?;
    let (_descriptors, scratch, static_candidate) = match &node.op {
        RelOp::Filter { predicate, input } => {
            let memory = PredicateMemory::for_expression(predicate)
                .ok_or("resource-limit: unbounded lane predicate")?;
            // Totality was checked by admission. Preserve selective reader
            // filtering for LIKE/SUBSTRING too, when their workspace can be
            // prepaid without inspecting the decoded payload.
            let static_candidate = ctx
                .options
                .parquet_cache_bytes
                .is_some_and(|bytes| bytes > 0)
                && node.estimated_rows <= input.estimated_rows / 16
                && memory.scratch.row_bound(ctx.options.batch_size).is_some();
            // Exact filter, installed predicate, reader factory, and one clone
            // per live reader. All survive until the recursive scan has joined.
            let exact_bytes = memory
                .cloned_bytes
                .checked_add(descriptor_bytes)
                .ok_or("resource-limit: lane predicate size overflow")?;
            let prepaid = (|| {
                let copies = if static_candidate {
                    ctx.options.workers.checked_add(3)?
                } else {
                    1
                };
                // At most `workers` readers evaluate concurrently. Within each
                // reader, the Arrow predicate and post-decode filter execute
                // sequentially; queued output contains no predicate scratch.
                let workspace = memory
                    .scratch
                    .row_bound(ctx.options.batch_size)?
                    .checked_mul(ctx.options.workers.max(1))?;
                let bytes = memory
                    .cloned_bytes
                    .checked_mul(copies)?
                    .checked_add(descriptor_bytes)?
                    .checked_add(workspace)?;
                ctx.budget.reserve(bytes).ok()
            })();
            if let Some(prepaid) = prepaid {
                (prepaid, Default::default(), static_candidate)
            } else {
                // Prepayment is optional: a tight budget or input-dependent
                // workspace retains exact per-batch funding after decoding.
                (ctx.budget.reserve(exact_bytes)?, memory.scratch, false)
            }
        }
        _ => (
            ctx.budget.reserve(descriptor_bytes)?,
            Default::default(),
            false,
        ),
    };
    match &node.op {
        RelOp::Scan { .. } => {
            let id = node as *const Node as usize;
            for receipt in &suffix.lanes {
                receipt.store(true, Ordering::Relaxed);
            }
            let previous = ctx.lane_transforms.insert(id, suffix.transform);
            let result = node.run(ctx, emit);
            ctx.lane_transforms.remove(&id);
            if let Some(previous) = previous {
                ctx.lane_transforms.insert(id, previous);
            }
            result
        }
        RelOp::Filter { input, predicate } => {
            let target = if static_candidate {
                crate::scan_predicate::target(input, predicate.clone())?
            } else {
                None
            };
            let stats = Arc::new(Mutex::new(FilterStats::default()));
            let transform = crate::parquet_scan::exact_filter(
                predicate.clone(),
                &input.schema,
                &ctx.budget,
                &ctx.options.cancellation,
                false,
                stats.clone(),
            )?;
            let transform = if scratch.is_empty() {
                transform
            } else {
                let budget = ctx.budget.clone();
                let cancellation = ctx.options.cancellation.clone();
                Arc::new(move |batch: RecordBatch| {
                    cancellation.check()?;
                    // Fund compiled patterns, character matching and substring
                    // buffers before evaluation allocates any of them.
                    let _scratch = budget.reserve(scratch.bytes(&batch)?)?;
                    transform(batch)
                }) as BatchTransform
            };
            let installed = target.and_then(|(id, expression)| {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    ctx.static_filters.entry(id)
                {
                    entry.insert(expression);
                    Some(id)
                } else {
                    None
                }
            });
            suffix.transform = compose(Some(transform), Some(suffix.transform)).unwrap();
            let result = run(input, ctx, suffix, emit);
            if let Some(id) = installed {
                ctx.static_filters.remove(&id);
            }
            ctx.metrics.filter_seconds += stats.lock().unwrap().seconds;
            result
        }
        RelOp::Rename { input } | RelOp::Project { input, .. } | RelOp::Unary { input, .. } => {
            let columns = match &node.op {
                RelOp::Project { columns, .. } => columns.clone(),
                RelOp::Unary { plan, .. } => {
                    column_projection::columns(plan, &input.schema, &node.schema)
                        .ok_or("internal: non-total lane projection")?
                }
                _ => (0..input.schema.fields().len()).collect(),
            };
            let schema = node.schema.clone();
            let budget = ctx.budget.clone();
            let transform: BatchTransform = Arc::new(move |batch| {
                let (batch, lease) =
                    column_projection::project(&batch, &columns, schema.clone(), &budget)?;
                Ok(TransformedBatch {
                    batch,
                    lease,
                    preserve_empty: false,
                })
            });
            suffix.transform = compose(Some(transform), Some(suffix.transform)).unwrap();
            run(input, ctx, suffix, emit)
        }
        RelOp::Join {
            left,
            right,
            kind,
            keys,
            residual,
        } => node.hash_join(
            ctx,
            emit,
            left,
            right,
            *kind,
            keys,
            residual.as_ref(),
            Some(suffix),
        ),
        _ => Err("internal: non-streaming lane input".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Kind, memory::Budget, spill::Cancellation, types::Type};
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
    };

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap()
    }

    fn stage(
        budget: &Budget,
        options: &Options,
        input_schema: &SchemaRef,
        stats: Arc<Mutex<probe::Statistics>>,
    ) -> BatchTransform {
        let column = Expr {
            ty: Type::Int,
            kind: Kind::Column(0),
        };
        let build = probe::owned::OwnedBuild::new(
            batch(),
            std::slice::from_ref(&column),
            budget,
            true,
            &options.cancellation,
        )
        .unwrap();
        let output = super::super::combined(input_schema, &batch().schema());
        probe_transform(
            build,
            vec![(column.clone(), column)],
            JoinKind::Inner,
            output,
            options,
            stats,
            budget.reserve(2048).unwrap(),
        )
    }

    #[test]
    fn cancelled_stage_checks_before_requesting_scratch_credit() {
        let budget = Budget::new(1 << 20);
        let options = Options::default();
        let transform = stage(&budget, &options, &batch().schema(), Arc::default());
        let held = budget.reserve(budget.limit() - budget.used()).unwrap();
        options.cancellation.cancel();
        let error = match transform(batch()) {
            Err(error) => error,
            Ok(_) => panic!("expected cancellation"),
        };
        assert!(error.contains("cancel"), "{error}");
        drop(transform);
        drop(held);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn suffix_denial_and_cancellation_release_every_lane_owner() {
        for cancel in [false, true] {
            let budget = Budget::new(1 << 20);
            let options = Options {
                batch_size: 7,
                cancellation: Cancellation::default(),
                ..Options::default()
            };
            let first_stats = Arc::new(Mutex::new(probe::Statistics::default()));
            let first = stage(&budget, &options, &batch().schema(), first_stats.clone());
            let schema = super::super::combined(&batch().schema(), &batch().schema());
            let second = stage(&budget, &options, &schema, Arc::default());
            let denial_budget = budget.clone();
            let cancellation = options.cancellation.clone();
            let suffix: BatchTransform = Arc::new(move |batch| {
                if cancel {
                    cancellation.cancel();
                    second(batch)
                } else {
                    let _denied =
                        denial_budget.reserve(denial_budget.limit() - denial_budget.used())?;
                    second(batch)
                }
            });
            let transform = compose(Some(first), Some(suffix)).unwrap();
            let error = match transform(batch()) {
                Err(error) => error,
                Ok(_) => panic!("expected failure"),
            };
            assert_eq!(
                first_stats.lock().unwrap().output_rows,
                2,
                "must fail after the first join"
            );
            assert!(
                error.contains(if cancel { "cancel" } else { "resource-limit" }),
                "{error}"
            );
            drop(transform);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn predicate_clones_are_funded_before_opening_the_scan() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let literal = "x".repeat(60_000);
        for sql in [
            format!("SELECT s WHERE s LIKE '{literal}'"),
            format!("SELECT s WHERE s LIKE SUBSTRING('{literal}' FROM 1 FOR 60000)"),
        ] {
            let predicate =
                crate::bind::bind(&crate::bind::parse(&sql).unwrap(), &schema, None, true)
                    .unwrap()
                    .filter
                    .unwrap();
            let options = Options {
                workers: 2,
                ..Options::default()
            };
            let budget = Budget::new(literal.len() - 1);
            let scan = Node {
                schema: schema.clone(),
                estimated_rows: 1,
                op: RelOp::Scan {
                    path: "/unused/unopened-lane-predicate.parquet".into(),
                    columns: vec![0],
                    distinct: vec![1],
                    statistics: vec![],
                },
            };
            let filter = Node {
                schema: schema.clone(),
                estimated_rows: 1,
                op: RelOp::Filter {
                    input: Box::new(scan),
                    predicate,
                },
            };
            let mut ctx = Context {
                filtered_reuse: None,
                build_groups: None,
                demand_cache: None,
                shared_attempt: None,
                options: &options,
                scan_filters: Default::default(),
                static_filters: Default::default(),
                scan_transforms: Default::default(),
                lane_transforms: Default::default(),
                replay_transforms: Default::default(),
                materialized: Default::default(),
                sideways_registry_lease: None,
                spill: None,
                budget: budget.clone(),
                metrics: Default::default(),
            };
            assert!(admitted(&filter, &ctx).unwrap());
            let (suffix, reached) = Suffix::prepend(
                Arc::new(|_| panic!("unfunded predicate must not execute")),
                None,
            );
            let error = run(&filter, &mut ctx, suffix, &mut |_| {
                panic!("unexpected output")
            })
            .unwrap_err();
            assert!(
                error.starts_with("resource-limit: accounted memory "),
                "{error}"
            );
            assert!(!reached.load(Ordering::Relaxed));
            assert!(ctx.lane_transforms.is_empty());
            assert!(ctx.static_filters.is_empty());
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }

    #[test]
    fn like_filtered_fused_chain_fits_tight_budget_and_releases_every_reservation() {
        use arrow::array::StringArray;
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

        let path = std::env::temp_dir().join(format!(
            "assaydb-lane-like-budget-{}.parquet",
            std::process::id()
        ));
        let literal = "x".repeat(60_000);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ])),
            vec![
                batch().column(0).clone(),
                Arc::new(StringArray::from(vec![
                    Some(literal.as_str()),
                    None,
                    Some("other"),
                ])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create_new(&path).unwrap(),
            input.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(1)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&input).unwrap();
        writer.close().unwrap();
        for pattern in [&literal, &format!("_{}", &literal[1..])] {
            let sql = format!(
                "SELECT k WHERE s LIKE '{pattern}' AND SUBSTRING(s FROM 1 FOR 2) = 'xx' AND -k <= -1"
            );
            let predicate = crate::bind::bind(
                &crate::bind::parse(&sql).unwrap(),
                &input.schema(),
                None,
                true,
            )
            .unwrap()
            .filter
            .unwrap();
            let options = Options {
                workers: 2,
                batch_size: 1,
                memory_bytes: 4 << 20,
                parquet_cache_bytes: None,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let scan = Node {
                schema: input.schema(),
                estimated_rows: 3,
                op: RelOp::Scan {
                    path: path.clone(),
                    columns: vec![0, 1],
                    distinct: vec![2, 2],
                    statistics: vec![],
                },
            };
            let filter = Node {
                schema: input.schema(),
                estimated_rows: 1,
                op: RelOp::Filter {
                    input: Box::new(scan),
                    predicate,
                },
            };
            let mut ctx = Context {
                filtered_reuse: None,
                build_groups: None,
                demand_cache: None,
                shared_attempt: None,
                options: &options,
                scan_filters: Default::default(),
                static_filters: Default::default(),
                scan_transforms: Default::default(),
                lane_transforms: Default::default(),
                replay_transforms: Default::default(),
                materialized: Default::default(),
                sideways_registry_lease: None,
                spill: None,
                budget: budget.clone(),
                metrics: Default::default(),
            };
            assert!(admitted(&filter, &ctx).unwrap());
            let first_stats = Arc::new(Mutex::new(probe::Statistics::default()));
            let second_stats = Arc::new(Mutex::new(probe::Statistics::default()));
            let first = stage(&budget, &options, &input.schema(), first_stats.clone());
            let intermediate = super::super::combined(&input.schema(), &batch().schema());
            let second = stage(&budget, &options, &intermediate, second_stats.clone());
            let (suffix, second_reached) = Suffix::prepend(second, None);
            let (suffix, first_reached) = Suffix::prepend(first, Some(suffix));
            let mut keys = Vec::new();
            run(&filter, &mut ctx, suffix, &mut |batch| {
                assert!(
                    budget.used() >= literal.len(),
                    "predicate clone must stay funded"
                );
                keys.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter(),
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(keys, vec![Some(1)]);
            assert!(first_reached.load(Ordering::Relaxed));
            assert!(second_reached.load(Ordering::Relaxed));
            assert_eq!(first_stats.lock().unwrap().output_rows, 1);
            assert_eq!(second_stats.lock().unwrap().output_rows, 1);
            assert!(ctx.lane_transforms.is_empty());
            assert!(ctx.static_filters.is_empty());
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
            assert!(budget.peak() <= budget.limit());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bounded_string_predicates_keep_reader_pushdown_and_fund_fused_lanes() {
        use arrow::array::StringArray;
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

        let path = std::env::temp_dir().join(format!(
            "assaydb-lane-bounded-reader-{}.parquet",
            std::process::id()
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..64)),
                Arc::new(StringArray::from_iter((0..64).map(|i| match i {
                    0 => None,
                    1 => Some("😀😀xx"),
                    _ => Some("other"),
                }))),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(16)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&input).unwrap();
        writer.close().unwrap();
        let predicate = crate::bind::bind(
            &crate::bind::parse(
                "SELECT k WHERE s LIKE '%xx%' AND SUBSTRING(s FROM 1 FOR 2) = '😀😀' AND -k < 0",
            )
            .unwrap(),
            &schema,
            None,
            true,
        )
        .unwrap()
        .filter
        .unwrap();
        assert!(!crate::scan_predicate::total(&predicate));
        assert!(
            PredicateMemory::for_expression(&predicate)
                .unwrap()
                .scratch
                .row_bound(7)
                .is_some()
        );
        let filter = Node {
            schema: schema.clone(),
            estimated_rows: 1,
            op: RelOp::Filter {
                input: Box::new(Node {
                    schema: schema.clone(),
                    estimated_rows: 64,
                    op: RelOp::Scan {
                        path: path.clone(),
                        columns: vec![0, 1],
                        distinct: vec![64, 3],
                        statistics: vec![],
                    },
                }),
                predicate,
            },
        };
        for workers in [2, 4] {
            // The final case cannot prepay workers * batch_size workspace,
            // but the actual 16-row groups fit. It must retain per-batch funding.
            for (batch_size, cached) in [(1, true), (7, true), (1_000_000, false)] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 2 << 20,
                    parquet_cache_bytes: Some(64 << 10),
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let mut ctx = Context {
                    filtered_reuse: None,
                    build_groups: None,
                    demand_cache: None,
                    shared_attempt: None,
                    options: &options,
                    scan_filters: Default::default(),
                    static_filters: Default::default(),
                    scan_transforms: Default::default(),
                    lane_transforms: Default::default(),
                    replay_transforms: Default::default(),
                    materialized: Default::default(),
                    sideways_registry_lease: None,
                    spill: None,
                    budget: budget.clone(),
                    metrics: Default::default(),
                };
                assert!(admitted(&filter, &ctx).unwrap());
                let first_stats = Arc::new(Mutex::new(probe::Statistics::default()));
                let second_stats = Arc::new(Mutex::new(probe::Statistics::default()));
                let first = stage(&budget, &options, &schema, first_stats.clone());
                let intermediate = super::super::combined(&schema, &batch().schema());
                let second = stage(&budget, &options, &intermediate, second_stats.clone());
                let (suffix, second_reached) = Suffix::prepend(second, None);
                let (suffix, first_reached) = Suffix::prepend(first, Some(suffix));
                let mut keys = Vec::new();
                run(&filter, &mut ctx, suffix, &mut |batch| {
                    keys.extend(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter(),
                    );
                    Ok(())
                })
                .unwrap();
                assert_eq!(keys, vec![Some(1)]);
                assert!(first_reached.load(Ordering::Relaxed));
                assert!(second_reached.load(Ordering::Relaxed));
                assert_eq!(first_stats.lock().unwrap().output_rows, 1);
                assert_eq!(second_stats.lock().unwrap().output_rows, 1);
                assert_eq!(ctx.metrics.static_scan_filters, usize::from(cached));
                assert!(ctx.lane_transforms.is_empty());
                assert!(ctx.static_filters.is_empty());
                drop(ctx);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
                assert!(budget.peak() <= budget.limit());
            }
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cancelled_scan_lanes_join_workers_and_release_pipeline_state() {
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
        let path = std::env::temp_dir().join(format!(
            "assaydb-lane-cancel-{}.parquet",
            std::process::id()
        ));
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create_new(&path).unwrap(),
            batch().schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(1)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch()).unwrap();
        writer.close().unwrap();
        let scan = Node {
            schema: batch().schema(),
            estimated_rows: 3,
            op: RelOp::Scan {
                path: path.clone(),
                columns: vec![0],
                distinct: vec![2],
                statistics: vec![],
            },
        };
        let column = Expr {
            ty: Type::Int,
            kind: Kind::Column(0),
        };
        let root = Node {
            schema: super::super::combined(&scan.schema, &scan.schema),
            estimated_rows: 2,
            op: RelOp::Join {
                left: Box::new(scan.clone()),
                right: Box::new(scan),
                kind: JoinKind::Inner,
                keys: vec![(column.clone(), column)],
                residual: None,
            },
        };
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size: 1,
                ..Options::default()
            };
            let budget = Budget::new(8 << 20);
            let mut ctx = Context {
                filtered_reuse: None,
                build_groups: None,
                demand_cache: None,
                shared_attempt: None,
                options: &options,
                scan_filters: Default::default(),
                static_filters: Default::default(),
                scan_transforms: Default::default(),
                lane_transforms: Default::default(),
                replay_transforms: Default::default(),
                materialized: Default::default(),
                sideways_registry_lease: None,
                spill: None,
                budget: budget.clone(),
                metrics: Default::default(),
            };
            let result = root.run(&mut ctx, &mut |_| {
                options.cancellation.cancel();
                Err("consumer stopped".into())
            });
            assert_eq!(result.unwrap_err(), "consumer stopped");
            assert!(ctx.lane_transforms.is_empty());
            assert!(ctx.scan_filters.is_empty());
            drop(ctx);
            assert_eq!(budget.used(), 0);
            assert_eq!(
                budget.workers(3, 3).count,
                3,
                "all scan permits must return"
            );
        }
        std::fs::remove_file(path).unwrap();
    }
}
