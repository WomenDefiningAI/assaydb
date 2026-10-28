use super::*;
use arrow::array::StructArray;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new(batch: &RecordBatch, group_rows: usize) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "assaydb-leaf-review-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(group_rows)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_statistics_enabled(EnabledStatistics::None)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }

    fn metadata(&self) -> ArrowReaderMetadata {
        crate::parquet_scan::external_metadata(&File::open(&self.0).unwrap()).unwrap()
    }

    fn prepared(&self, options: &Options) -> Prepared {
        crate::execute::prepare(
            "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k",
            &HashMap::from([("t".into(), self.0.clone())]),
            options,
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}

fn plain_fixture() -> Fixture {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values((0..40960).map(|i| i / 7))),
            Arc::new(Int64Array::from_iter(
                (0..40960).map(|i| (i % 5 != 0).then_some(i - 20000)),
            )),
        ],
    )
    .unwrap();
    Fixture::new(&batch, 4096)
}

#[test]
fn run_order_remembers_direction_across_empty_and_nonempty_chunks() {
    for (chunks, ascending, descending) in [
        (vec![vec![], vec![-3, -3], vec![], vec![0, 7]], true, false),
        (vec![vec![7, 7], vec![0], vec![-3, -3]], false, true),
        (vec![vec![7], vec![], vec![7, 7]], true, true),
        (vec![vec![0, 1, 2], vec![1]], false, false),
        (vec![vec![0, 2], vec![1, 3]], false, false),
        (vec![vec![i128::MIN], vec![i128::MAX]], true, false),
    ] {
        let mut order = RunOrder::default();
        for chunk in chunks {
            for value in chunk {
                order.observe(value);
            }
        }
        assert_eq!(order.ascending, ascending);
        assert_eq!(order.descending, descending);
    }
}

#[test]
fn nested_roots_fund_the_decoded_physical_pages() {
    let rows = 32768;
    let fields = vec![
        Arc::new(Field::new("a", DataType::Int64, false)),
        Arc::new(Field::new("b", DataType::Int64, true)),
    ];
    let unused = StructArray::new(
        fields.clone().into(),
        vec![
            Arc::new(Int64Array::from(vec![0; rows])),
            Arc::new(Int64Array::from(vec![None::<i64>; rows])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Struct(fields.into()), false),
            Field::new("k", DataType::Int64, false),
        ])),
        vec![
            Arc::new(unused),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        ],
    )
    .unwrap();
    let fixture = Fixture::new(&batch, rows);
    let metadata = fixture.metadata();
    let schema = metadata.parquet_schema();
    let group = metadata.metadata().row_group(0);
    let key = ProjectionMask::roots(schema, [1]);
    assert!(!key.leaf_included(1));
    assert!(key.leaf_included(2));
    assert_eq!(schema.get_column_root_idx(2), 1);
    let page_bytes = group.column(2).byte_range().1 as usize;
    assert_eq!(page_bytes, group.column(2).compressed_size() as usize);
    assert!(page_bytes > 100 * group.column(1).compressed_size() as usize);
    let expected = page_bytes
        + group.column(2).uncompressed_size() as usize
        + 8192
        + 2 * 37 * (16 + 12)
        + 1024;
    let budget = Budget::new(expected);
    let funding = budget
        .reserve({
            let estimate =
                crate::parquet_scan::DecoderEstimate::reader(&metadata, &key, None, None, 37)
                    .unwrap();
            estimate.workspace + estimate.batch
        })
        .unwrap();
    assert_eq!(budget.used(), expected);
    assert!(budget.used() >= page_bytes);
    // The helper sums every physical leaf, including multi-leaf roots.
    let all = ProjectionMask::roots(schema, [0, 1]);
    let all_expected = group
        .columns()
        .iter()
        .map(|column| {
            column.compressed_size() as usize
                + column.uncompressed_size() as usize
                + 8192
                + 2 * 37 * (16 + 12)
                + 1024
        })
        .sum::<usize>();
    let all_estimate =
        crate::parquet_scan::DecoderEstimate::reader(&metadata, &all, None, None, 37).unwrap();
    assert_eq!(all_estimate.workspace + all_estimate.batch, all_expected);
    drop(funding);
    assert_eq!(budget.used(), 0);
}

#[test]
fn live_leaf_cancellation_and_failure_release_all_reservations_and_permits() {
    let fixture = plain_fixture();
    for workers in [1, 2, 4, 10] {
        for (aggregate, runs) in [(false, false), (true, false), (true, true)] {
            for cancel in [false, true] {
                let options = Options {
                    workers,
                    batch_size: 37,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let prepared = fixture.prepared(&options);
                let executor =
                    super::super::super::executor(&prepared, budget.clone(), true).unwrap();
                let metadata = fixture.metadata();
                let fired = Arc::new(AtomicUsize::new(0));
                let observed = fired.clone();
                let after_batch: AfterBatch = Arc::new(move |lane, cancellation| {
                    // The first caller quantum must finish before injection:
                    // exercise active runtime permits and already-live decoders.
                    if lane.rows <= (MORSEL_BATCHES * 37) as u64 {
                        return Ok(());
                    }
                    assert!(lane.cursor.is_some());
                    assert!(lane.cursor.as_ref().unwrap().decoder_funded());
                    assert!(lane.source.budget.used() > 0);
                    assert_eq!(lane.source.budget.active_workers(), workers - 1);
                    if aggregate {
                        assert!(lane.state.as_ref().unwrap().group_count > 0);
                    }
                    observed.fetch_add(1, Ordering::Relaxed);
                    if cancel {
                        cancellation.cancel();
                        cancellation.check()
                    } else {
                        Err("injected numeric leaf failure".into())
                    }
                });
                let source = Arc::new(Source {
                    path: fixture.0.clone(),
                    projection: ProjectionMask::roots(metadata.parquet_schema(), [0, 1]),
                    key_projection: ProjectionMask::roots(metadata.parquet_schema(), [0]),
                    metadata,
                    plan: Arc::new(prepared.plan.clone()),
                    expressions: executor.expressions.clone(),
                    schema: prepared.schema.clone(),
                    options: options.clone(),
                    budget: budget.clone(),
                    dense: budget.source_dense_allowance(DENSE_ALLOWANCE),
                    validate_bounds: false,
                    after_batch: Some(after_batch),
                    _metadata_funding: budget.reserve(64 << 10).unwrap().into(),
                    funding: Arc::new(budget.reserve(64 << 10).unwrap()),
                });
                drop(executor);
                let lanes = (0..workers)
                    .map(|rank| Lane {
                        source: source.clone(),
                        groups: 10 * rank / workers..10 * (rank + 1) / workers,
                        cursor: None,
                        progress: None,
                        state: aggregate.then(|| {
                            new_state(&source.plan, &source.expressions, &budget, None).unwrap()
                        }),
                        bounds: None,
                        order: RunOrder::default(),
                        runs,
                        rows: 0,
                        declined: false,
                        metrics: Metrics::default(),
                    })
                    .collect();
                drop(source);
                let result = run_lanes(lanes, &options, &budget);
                assert!(fired.load(Ordering::Relaxed) > 0);
                if cancel {
                    assert_eq!(result.err().unwrap(), "cancelled: query execution");
                } else {
                    assert!(result.unwrap().is_none());
                    assert!(options.cancellation.check().is_ok());
                }
                assert_eq!(
                    budget.used(),
                    0,
                    "workers={workers}, aggregate={aggregate}, cancel={cancel}"
                );
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(budget.peak_workers(), workers - 1);
                let permits = budget.workers(workers - 1, workers - 1);
                assert_eq!(permits.count, workers - 1);
                drop(permits);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn leaf_emit_failure_is_terminal_and_releases_the_finished_states() {
    let fixture = plain_fixture();
    for workers in [2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 37,
            ..Options::default()
        };
        let prepared = fixture.prepared(&options);
        let budget = Budget::new(options.memory_bytes);
        let mut emitted = 0;
        let result = try_execute(
            &prepared,
            &options,
            budget.clone(),
            &fixture.0,
            &[0, 1],
            &mut |_| {
                emitted += 1;
                assert_eq!(budget.active_workers(), 0);
                Err("injected downstream failure".into())
            },
        );
        assert_eq!(result.err().unwrap(), "injected downstream failure");
        assert_eq!(emitted, 1);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
        assert!(options.cancellation.check().is_ok());
    }
}

#[test]
fn nested_leaf_declines_before_reserving_or_starting_upstream_work() {
    let fixture = plain_fixture();
    for workers in [2, 4, 10] {
        let options = Options {
            workers,
            ..Options::default()
        };
        let prepared = fixture.prepared(&options);
        let budget = Budget::new(options.memory_bytes);
        let upstream = budget.workers(1, workers - 1);
        let result = try_execute(
            &prepared,
            &options,
            budget.clone(),
            &fixture.0,
            &[0, 1],
            &mut |_| panic!("nested leaf must not start while upstream holds a permit"),
        )
        .unwrap();
        assert!(result.is_none());
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak(), 0);
        assert_eq!(budget.active_workers(), 1);
        assert_eq!(budget.peak_workers(), 1);
        drop(upstream);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn having_leaf_cancellation_releases_selected_output_and_all_owners() {
    let fixture = plain_fixture();
    for workers in [2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 37,
            ..Options::default()
        };
        let prepared = fixture.prepared(&options);
        let predicate = crate::expr::Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                crate::expr::Op::Gt,
                Box::new(crate::expr::Expr {
                    ty: Type::Int,
                    kind: Kind::Column(1),
                }),
                Box::new(crate::expr::Expr {
                    ty: Type::Int,
                    kind: Kind::Literal(crate::expr::Literal::Num(0)),
                }),
            ),
        };
        let having = crate::execute::Having::for_plan(&prepared.plan, &predicate).unwrap();
        let budget = Budget::new(options.memory_bytes);
        let mut emitted = 0;
        let result = try_execute_having(
            &prepared,
            &options,
            budget.clone(),
            &fixture.0,
            &[0, 1],
            Some(&having),
            &mut |batch| {
                assert!(batch.num_rows() > 0);
                assert_eq!(budget.active_workers(), 0);
                emitted += 1;
                options.cancellation.cancel();
                Ok(())
            },
        );
        assert_eq!(result.err().unwrap(), "cancelled: query execution");
        assert_eq!(emitted, 1);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}
