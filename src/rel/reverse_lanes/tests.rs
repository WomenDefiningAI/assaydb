use super::*;
use crate::{
    execute::{Metrics, Options},
    expr::Value,
    keys::Keys,
    types::Type,
};
use arrow::{
    array::{Decimal128Array, Int64Array},
    datatypes::{DataType, Field},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{collections::HashMap, fs::File, path::PathBuf};

fn column() -> Expr {
    Expr {
        kind: Kind::Column(0),
        ty: Type::Int,
    }
}

fn context<'a>(options: &'a Options, budget: &Budget) -> Context<'a> {
    Context {
        build_groups: None,
        demand_cache: None,
        filtered_reuse: None,
        sideways_registry_lease: None,
        shared_attempt: None,
        options,
        scan_filters: HashMap::new(),
        static_filters: HashMap::new(),
        scan_transforms: HashMap::new(),
        lane_transforms: HashMap::new(),
        replay_transforms: HashMap::new(),
        materialized: HashMap::new(),
        spill: None,
        budget: budget.clone(),
        metrics: Metrics::default(),
    }
}

fn source(path: PathBuf) -> Node {
    Node {
        schema: Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        estimated_rows: 16395,
        op: RelOp::Scan {
            path,
            columns: vec![0],
            distinct: vec![4],
            statistics: vec![],
        },
    }
}

fn index(budget: &Budget, dense: bool) -> OwnedJoinIndex {
    let values = [Value::adopt(
        Arc::new(Decimal128Array::from(vec![
            None,
            Some(3),
            Some(3),
            Some(1),
            Some(8),
            Some(3),
            Some(-9),
        ])),
        budget,
    )
    .unwrap()];
    let keys = Arc::new(Keys::new(&values, 7, budget).unwrap());
    OwnedJoinIndex::new(keys, 7, budget, dense, &Cancellation::default()).unwrap()
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "assaydb-reverse-lanes-{}-{}.parquet",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = source(path.clone()).schema;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..16395)
                    .map(|i| match i % 8193 {
                        0 => None,
                        8191 => Some(-9),
                        8192 => Some(2),
                        _ => Some(3),
                    })
                    .collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(8193)
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

#[test]
fn scan_lanes_merge_skewed_heads_after_all_source_batches_and_release_credit() {
    let file = Fixture::new();
    let node = source(file.0.clone());
    let keys = [(column(), column())];
    for workers in [2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for dense in [false, true] {
                let budget = Budget::new(64 << 20);
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let mut ctx = context(&options, &budget);
                let (prepared, credit) = Prepared::new(&node, &keys, 7, &ctx).unwrap().unwrap();
                let mut matched = vec![false; 7];
                let report = prepared
                    .run(index(&budget, dense), &node, &mut ctx, &mut matched)
                    .unwrap();
                assert_eq!(matched, [false, true, true, false, false, true, true]);
                assert!(report.batches >= 3);
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(
                    budget.used(),
                    credit.bytes(),
                    "only the returned flags remain funded"
                );
                assert!(ctx.lane_transforms.is_empty());
                drop(matched);
                drop(credit);
                drop(ctx);
                assert_eq!(budget.used(), 0);
            }
        }
    }
}

#[test]
fn admission_requires_one_or_two_direct_numeric_probe_columns() {
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            ..Options::default()
        };
        let budget = Budget::new(1 << 20);
        let ctx = context(&options, &budget);
        for (ty, physical, direct) in [
            (Type::Int, DataType::Int32, true),
            (Type::Int, DataType::Int64, true),
            (Type::Int, DataType::Decimal128(38, 0), true),
            (Type::Decimal(2), DataType::Decimal128(38, 2), true),
            (Type::Decimal(2), DataType::Decimal128(38, 3), false),
            (Type::Int, DataType::Decimal128(38, 2), false),
            (Type::Text, DataType::Utf8, false),
            (Type::Float, DataType::Float64, false),
            (Type::Bool, DataType::Boolean, false),
            (Type::Date, DataType::Date32, false),
            (Type::Null, DataType::Null, false),
            (Type::Binary, DataType::Binary, false),
        ] {
            // A missing file verifies that every decision precedes source I/O.
            let mut node = source(PathBuf::from("missing-key-admission-source"));
            node.schema = Arc::new(Schema::new(vec![Field::new("k", physical, true)]));
            let expression = Expr {
                kind: Kind::Column(0),
                ty,
            };
            for count in 0..=3 {
                let keys = vec![(expression.clone(), expression.clone()); count];
                let prepared = Prepared::new(&node, &keys, 7, &ctx).unwrap();
                assert_eq!(
                    prepared.is_some(),
                    workers > 1 && direct && matches!(count, 1 | 2),
                    "{keys:?}, workers={workers}"
                );
                drop(prepared);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn optional_bitmap_denial_does_not_consume_source_or_index() {
    let budget = Budget::new(1 << 20);
    let options = Options {
        workers: 10,
        ..Options::default()
    };
    let ctx = context(&options, &budget);
    // Missing source is intentional: admission must not perform I/O on denial.
    let node = source(PathBuf::from("missing-reverse-lane-source"));
    let index = index(&budget, false);
    let before = budget.used();
    let held = budget.reserve(budget.limit() - before).unwrap();
    assert!(
        Prepared::new(&node, &[(column(), column())], 7, &ctx)
            .unwrap()
            .is_none()
    );
    assert_eq!(index.head(&3_i128.to_ne_bytes()), Some(5));
    assert_eq!(index.head(&8_i128.to_ne_bytes()), Some(4));
    assert_eq!(budget.used(), budget.limit());
    drop(held);
    assert_eq!(budget.used(), before);
    drop(index);
    drop(ctx);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}

#[test]
fn cancellation_after_private_matches_cannot_publish_flags_or_leak_owners() {
    for workers in [1, 2, 4, 10] {
        let budget = Budget::new(1 << 20);
        let cancel = Cancellation::default();
        // Prepared's production admission intentionally retains today's one-worker path.
        let options = Options {
            workers: workers.max(2),
            ..Options::default()
        };
        let ctx = context(&options, &budget);
        let (prepared, credit) =
            Prepared::new(&source(PathBuf::new()), &[(column(), column())], 7, &ctx)
                .unwrap()
                .unwrap();
        let Prepared {
            lanes,
            keys,
            funding,
        } = prepared;
        let pool = Arc::new(Pool {
            id: NEXT_POOL.fetch_add(1, Ordering::Relaxed),
            next: AtomicUsize::new(0),
            lanes,
            keys,
            index: index(&budget, true).into(),
            _funding: funding,
        });
        let batch = RecordBatch::try_new(
            source(PathBuf::new()).schema,
            vec![Arc::new(Int64Array::from(vec![Some(3), None, Some(-9)]))],
        )
        .unwrap();
        let payload = budget.reserve(batch.get_array_memory_size()).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let (pool, batch, budget, cancel) = (&pool, &batch, &budget, &cancel);
                scope.spawn(move || pool.probe(batch, budget, cancel).unwrap());
            }
        });
        // Cancellation occurs after successful work in every participating lane.
        cancel.cancel();
        assert_eq!(
            pool.probe(&batch, &budget, &cancel).unwrap_err(),
            "cancelled: query execution"
        );
        let mut pool = Arc::try_unwrap(pool).ok().unwrap();
        let mut matched = vec![false; 7];
        assert_eq!(
            pool.finish(&mut matched, &cancel).err().unwrap(),
            "cancelled: query execution"
        );
        assert_eq!(matched, [false; 7]);
        drop(matched);
        drop(credit);
        drop(pool);
        drop(batch);
        drop(payload);
        drop(ctx);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn failed_or_cancelled_source_drops_lane_index_without_exposing_matches() {
    for cancel in [false, true] {
        let budget = Budget::new(1 << 20);
        let options = Options {
            workers: 4,
            ..Options::default()
        };
        let mut ctx = context(&options, &budget);
        let node = source(PathBuf::from("missing-reverse-lane-source"));
        let (prepared, credit) = Prepared::new(&node, &[(column(), column())], 7, &ctx)
            .unwrap()
            .unwrap();
        if cancel {
            options.cancellation.cancel();
        }
        let mut matched = vec![false; 7];
        // Build before observing the query's cancellation, as in a completed retained phase.
        let error = prepared
            .run(index(&budget, true), &node, &mut ctx, &mut matched)
            .err()
            .unwrap();
        if cancel {
            assert_eq!(error, "cancelled: query execution");
        } else {
            assert!(error.contains("No such file"), "{error}");
        }
        assert_eq!(matched, [false; 7]);
        assert!(ctx.lane_transforms.is_empty());
        assert_eq!(budget.active_workers(), 0);
        assert_eq!(budget.used(), credit.bytes());
        drop(matched);
        drop(credit);
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn ranked_numeric_scan_matches_serial_membership_at_every_batch_and_worker_setting() {
    let file = Fixture::new();
    let node = source(file.0.clone());
    for workers in [2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let budget = Budget::new(64 << 20);
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let mut ctx = context(&options, &budget);
            let values = [Value::adopt(
                Arc::new(Decimal128Array::from(vec![
                    None,
                    Some(3),
                    Some(3),
                    Some(1),
                    Some(8),
                    Some(3),
                    Some(-9),
                ])),
                &budget,
            )
            .unwrap()];
            let encoded = Arc::new(Keys::new(&values, 7, &budget).unwrap());
            drop(values);
            let (prepared, index, credit) =
                Prepared::numeric(&node, &[(column(), column())], &encoded, &ctx)
                    .unwrap()
                    .unwrap();
            drop(encoded);
            let mut matched = vec![false; 7];
            let report = prepared.run(index, &node, &mut ctx, &mut matched).unwrap();
            assert_eq!(matched, [false, true, true, false, false, true, true]);
            assert!(report.batches >= 3);
            assert_eq!(budget.active_workers(), 0);
            assert_eq!(budget.used(), credit.bytes());
            assert!(ctx.lane_transforms.is_empty());
            drop(matched);
            drop(credit);
            drop(ctx);
            assert_eq!(budget.used(), 0);
        }
    }
}

// These controls run the complete reverse join, including its output callback.
// A source-only test would miss an accidental early call to that callback.
fn retained_fixture() -> Fixture {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "assaydb-reverse-barrier-{}-{}.parquet",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let schema = source(path.clone()).schema;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            None,
            Some(3),
            Some(3),
            Some(1),
            Some(8),
            Some(3),
            Some(-9),
        ]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Fixture(path)
}

fn barrier_join(
    left: &Fixture,
    right: &Fixture,
    numeric: bool,
    kind: crate::rel::JoinKind,
) -> Node {
    let mut left = source(left.0.clone());
    left.estimated_rows = 7;
    Node {
        schema: left.schema.clone(),
        estimated_rows: 7,
        op: RelOp::Join {
            left: Box::new(left),
            right: Box::new(source(right.0.clone())),
            kind,
            // Two exact INTEGER columns force the hash fallback; the single
            // bounded INTEGER column admits the numeric lane index.
            keys: vec![(column(), column()); if numeric { 1 } else { 2 }],
            residual: None,
        },
    }
}

fn assert_reader_cleanup(ctx: &Context<'_>, budget: &Budget) {
    assert!(ctx.lane_transforms.is_empty());
    assert!(ctx.scan_transforms.is_empty());
    assert!(ctx.replay_transforms.is_empty());
    assert!(ctx.scan_filters.is_empty());
    assert!(ctx.static_filters.is_empty());
    assert_eq!(budget.active_workers(), 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn late_corrupt_source_precedes_every_reverse_join_emission() {
    use crate::{parquet_scan::test_support::Observer, rel::JoinKind};
    use std::io::{Seek, SeekFrom, Write};

    let left = retained_fixture();
    let healthy = Fixture::new();
    let corrupt = Fixture::new();
    // Retain the footer and the complete first row group. Only the second
    // group's page bytes are invalid, so this is a real late decoder failure.
    let metadata =
        crate::parquet_scan::external_metadata(&File::open(&corrupt.0).unwrap()).unwrap();
    let (offset, length) = metadata.metadata().row_group(1).column(0).byte_range();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&corrupt.0)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&vec![0xff; length as usize]).unwrap();
    drop(file);

    // Obtain the canonical source error independently of join dispatch and
    // prove that the intact prefix really is readable before that error.
    // One-row delivery makes the intact-prefix assertion independent of a
    // reader coalescing its last row with the corrupt following row group.
    let serial = Options {
        batch_size: 1,
        ..Options::default()
    };
    let budget = Budget::new(64 << 20);
    let mut ctx = context(&serial, &budget);
    let mut prefix_rows = 0;
    let expected_error = source(corrupt.0.clone())
        .run(&mut ctx, &mut |batch| {
            prefix_rows += batch.num_rows();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(prefix_rows, 8193);
    assert!(!expected_error.is_empty());
    assert_reader_cleanup(&ctx, &budget);

    for numeric in [true, false] {
        for kind in [JoinKind::Semi, JoinKind::Anti] {
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..Options::default()
                    };
                    let budget = Budget::new(64 << 20);
                    let mut ctx = context(&options, &budget);
                    let mut values = Vec::new();
                    barrier_join(&left, &healthy, numeric, kind)
                        .run(&mut ctx, &mut |batch| {
                            values.extend(
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
                    let expected = if kind == JoinKind::Semi {
                        vec![Some(3), Some(3), Some(3), Some(-9)]
                    } else {
                        vec![None, Some(1), Some(8)]
                    };
                    assert_eq!(values, expected);
                    assert_eq!(ctx.metrics.reverse_existence_joins, 1);
                    assert_eq!(
                        ctx.metrics.reverse_existence_lane_joins,
                        usize::from(workers > 1)
                    );
                    assert_eq!(
                        ctx.metrics.reverse_existence_numeric_lane_joins,
                        usize::from(workers > 1 && numeric)
                    );
                    assert_reader_cleanup(&ctx, &budget);

                    let observer = Arc::new(Observer::new(corrupt.0.clone()));
                    let options = Options {
                        test_scan_observer: Some(observer.clone()),
                        ..options
                    };
                    let mut ctx = context(&options, &budget);
                    let mut emissions = 0;
                    let error = barrier_join(&left, &corrupt, numeric, kind)
                        .run(&mut ctx, &mut |_| {
                            emissions += 1;
                            Err("downstream must not run before source completion".into())
                        })
                        .unwrap_err();
                    assert_eq!(
                        error, expected_error,
                        "numeric={numeric}, {kind:?}, workers={workers}, batch={batch_size}"
                    );
                    assert_eq!(emissions, 0);
                    assert_eq!(ctx.metrics.reverse_existence_joins, 1);
                    assert!(observer.matching_batches.load(Ordering::SeqCst) > 0);
                    assert_eq!(
                        observer.lane_batches.load(Ordering::SeqCst) > 0,
                        workers > 1
                    );
                    assert_reader_cleanup(&ctx, &budget);
                    drop(ctx);
                    drop(options);
                    assert_eq!(
                        Arc::strong_count(&observer),
                        1,
                        "all reader observers dropped"
                    );
                }
            }
        }
    }
}

#[test]
fn cancellation_inside_live_reverse_reader_releases_lanes_and_emits_nothing() {
    use crate::{parquet_scan::test_support::Observer, rel::JoinKind};
    use std::{sync::mpsc, time::Duration};

    let left = retained_fixture();
    let right = Fixture::new();
    for numeric in [true, false] {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let (started, wait_started) = mpsc::sync_channel(1);
                let (resume, wait_resume) = mpsc::sync_channel(1);
                let mut observer = Observer::new(right.0.clone());
                observer.pause = Some((started, Mutex::new(wait_resume)));
                let observer = Arc::new(observer);
                let options = Options {
                    workers,
                    batch_size,
                    test_scan_observer: Some(observer.clone()),
                    ..Options::default()
                };
                let budget = Budget::new(64 << 20);
                let join = barrier_join(&left, &right, numeric, JoinKind::Semi);
                std::thread::scope(|scope| {
                    let handle = scope.spawn(|| {
                        let mut ctx = context(&options, &budget);
                        let mut emissions = 0;
                        let error = join
                            .run(&mut ctx, &mut |_| {
                                emissions += 1;
                                Err("downstream must not run before source completion".into())
                            })
                            .unwrap_err();
                        assert_eq!(error, "cancelled: query execution");
                        assert_eq!(emissions, 0);
                        assert_eq!(ctx.metrics.reverse_existence_joins, 1);
                        assert_reader_cleanup(&ctx, &budget);
                    });
                    let paused_on_worker =
                        wait_started.recv_timeout(Duration::from_secs(10)).unwrap();
                    // The reader is paused in its first matching transform,
                    // before returning that batch; unread source work remains.
                    assert!(!handle.is_finished());
                    assert!(observer.matching_batches.load(Ordering::SeqCst) > 0);
                    assert_eq!(
                        observer.lane_batches.load(Ordering::SeqCst) > 0,
                        workers > 1
                    );
                    // Dynamic claiming also lets the caller reach this hook
                    // after background lanes finish or release their permits.
                    // A paused background reader still owns its decode permit.
                    let active_workers = budget.active_workers();
                    assert!(active_workers < workers);
                    if paused_on_worker {
                        assert!(active_workers > 0);
                    }
                    assert!(budget.used() > 0);
                    options.cancellation.cancel();
                    resume.send(()).unwrap();
                    handle.join().unwrap();
                });
                drop(options);
                assert_eq!(Arc::strong_count(&observer), 1);
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(budget.used(), 0);
            }
        }
    }
}
