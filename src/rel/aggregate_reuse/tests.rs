use super::*;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::atomic::{AtomicUsize, Ordering};

mod review;

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    tables: HashMap<String, PathBuf>,
}

impl Fixture {
    fn new(empty_fact: bool, empty_dimension: bool, skew: bool, precision: u8) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-aggregate-reuse-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut fixture = Self {
            directory,
            tables: HashMap::new(),
        };
        let mut keys = vec![
            Some(3),
            Some(1),
            Some(3),
            None,
            Some(2),
            Some(4),
            Some(1),
            Some(5),
            Some(6),
            Some(6),
            Some(1),
            Some(3),
        ];
        let mut values = vec![
            Some(300),
            Some(200),
            Some(-100),
            Some(9900),
            None,
            Some(-100),
            None,
            Some(0),
            Some(900),
            Some(-900),
            Some(400),
            Some(500),
        ];
        if skew {
            keys.extend(std::iter::repeat_n(Some(3), 8201));
            values.extend(std::iter::repeat_n(Some(1), 8201));
        }
        if empty_fact {
            keys.clear();
            values.clear();
        }
        fixture.write(
            "f",
            &["k", "v"],
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, 2)
                        .unwrap(),
                ),
            ],
        );
        let dimension = RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(3),
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(8),
                ])) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec![
                    "a", "c", "a", "b", "n", "null-key", "absent",
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        fixture.write(
            "d",
            &["k", "label"],
            dimension
                .columns()
                .iter()
                .map(|a| {
                    if empty_dimension {
                        a.slice(0, 0)
                    } else {
                        a.clone()
                    }
                })
                .collect(),
        );
        fixture
    }

    fn write(&mut self, name: &str, names: &[&str], arrays: Vec<ArrayRef>) {
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .zip(&arrays)
                .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let path = self.directory.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(if arrays[0].len() > 8192 { 4093 } else { 7 })
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        self.tables.insert(name.into(), path);
    }

    fn plan(&self, sql: &str) -> Node {
        *prepare(sql, &self.tables, &Options::default())
            .unwrap()
            .relational
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

const SQL: &str = "SELECT d.label,d.k,SUM(f.v) AS total FROM d,f WHERE d.k=f.k AND d.k IN (SELECT k FROM f GROUP BY k HAVING SUM(v)>0) GROUP BY d.label,d.k ORDER BY 2,1 LIMIT 100";

fn baseline(root: &Node, options: &Options, budget: Budget) -> Result<Execution> {
    execute_once_with_demand_cache(root, options, budget, None, false, None)
}

#[test]
fn duplicate_multiplicities_nulls_order_and_batch_boundaries() {
    for skew in [false, true] {
        let fixture = Fixture::new(false, false, skew, 15);
        for sql in [
            SQL.to_owned(),
            SQL.replace("SUM(v)>0", "SUM(v) IS NULL OR SUM(v)>0"),
            SQL.replace(" ORDER BY 2,1 LIMIT 100", ""),
        ] {
            let root = fixture.plan(&sql);
            let serial = Options {
                workers: 1,
                batch_size: 1,
                ..Options::default()
            };
            let expected = baseline(&root, &serial, Budget::new(serial.memory_bytes))
                .unwrap()
                .batch;
            if !skew && sql == SQL {
                assert_eq!(expected.num_rows(), 3);
                let totals = expected
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                assert_eq!(totals.values().as_ref(), &[1200, 600, 700]);
            }
            if sql.contains("IS NULL") {
                assert_eq!(expected.column(2).null_count(), 1);
            }
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..serial.clone()
                    };
                    let budget = Budget::new(options.memory_bytes);
                    let actual =
                        execute_once(&root, &options, budget.clone(), None, false).unwrap();
                    assert_eq!(actual.batch, expected, "{workers}/{batch_size}: {sql}");
                    assert_eq!(
                        actual.metrics.aggregate_scan_reuses,
                        1,
                        "{}",
                        serde_json::to_string_pretty(&root).unwrap()
                    );
                    assert!(budget.peak() <= options.memory_bytes);
                    drop(actual);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
    }
}

#[test]
fn empty_inputs_and_filtered_empty_qualification() {
    for (fact_empty, dimension_empty) in
        [(true, false), (false, true), (true, true), (false, false)]
    {
        let fixture = Fixture::new(fact_empty, dimension_empty, false, 15);
        let sql = if fact_empty || dimension_empty {
            SQL.to_owned()
        } else {
            SQL.replace("SUM(v)>0", "SUM(v)>1000000")
        };
        let root = fixture.plan(&sql);
        let expected = baseline(&root, &Options::default(), Budget::new(64 << 20))
            .unwrap()
            .batch;
        assert_eq!(expected.num_rows(), 0);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                assert_eq!(
                    execute_once(
                        &root,
                        &options,
                        Budget::new(options.memory_bytes),
                        None,
                        false
                    )
                    .unwrap()
                    .batch,
                    expected
                );
            }
        }
    }
}

#[test]
fn different_rows_expressions_float_distinct_and_fallible_shapes_decline() {
    let fixture = Fixture::new(false, false, false, 15);
    for sql in [
        SQL.replace("SUM(f.v)", "SUM(f.v+1)"),
        SQL.replace("SUM(f.v)", "SUM(DISTINCT f.v)"),
        SQL.replace("SUM(f.v)", "SUM(CAST(f.v AS REAL))"),
        SQL.replace("SUM(f.v)", "AVG(f.v)"),
        SQL.replace("GROUP BY k HAVING", "WHERE v>0 GROUP BY k HAVING"),
        SQL.replace("WHERE d.k=f.k", "WHERE f.v>0 AND d.k=f.k"),
        SQL.replace("SUM(v)>0", "SUM(v)/0.00>0"),
        SQL.replace("SELECT k FROM f", "SELECT k+0 FROM f"),
        SQL.replace("d.k=f.k", "CAST(d.k AS REAL)=f.k"),
        SQL.replace("d.k IN", "d.k NOT IN"),
    ] {
        let prepared = prepare(&sql, &fixture.tables, &Options::default());
        if let Err(error) = &prepared {
            assert!(sql.contains("AS REAL"), "{sql}: {error}");
            assert_eq!(error, "expected numeric type, got Float");
            continue;
        }
        let root = *prepared.unwrap().relational.unwrap();
        assert!(
            candidate(&root, &Budget::new(64 << 20), &Options::default()).is_none(),
            "{sql}"
        );
        let serial = Options {
            workers: 1,
            batch_size: 1,
            ..Options::default()
        };
        let expected = baseline(&root, &serial, Budget::new(serial.memory_bytes)).map(|e| e.batch);
        if sql.contains("/0.00") {
            assert_eq!(expected.as_ref().unwrap_err(), "division by zero");
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..serial.clone()
                };
                let actual = execute_once(
                    &root,
                    &options,
                    Budget::new(options.memory_bytes),
                    None,
                    false,
                )
                .map(|e| e.batch);
                assert_eq!(actual, expected, "{workers}/{batch_size}: {sql}");
            }
        }
    }
    let wide = Fixture::new(false, false, false, 38);
    assert!(candidate(&wide.plan(SQL), &Budget::new(64 << 20), &Options::default()).is_none());
}

#[test]
fn denied_plan_credit_falls_back_and_cancellation_releases_memory() {
    let fixture = Fixture::new(false, false, false, 15);
    let root = fixture.plan(SQL);
    let bytes = build_groups::node_bytes(&root) * 16 + 65536;
    // This budget fits the unchanged query but refuses the optional plan clone.
    let budget = Budget::new(bytes - 1);
    assert!(candidate(&root, &budget, &Options::default()).is_none());
    assert_eq!(budget.used(), 0);
    let options = Options {
        workers: 1,
        batch_size: 1,
        memory_bytes: bytes - 1,
        ..Options::default()
    };
    let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
        .unwrap()
        .batch;
    let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
    assert_eq!(actual.batch, expected);
    assert_eq!(actual.metrics.aggregate_scan_reuses, 0);
    drop(actual);
    assert_eq!(budget.used(), 0);
    // Admission succeeds, then the first real reader cannot coexist with the
    // retained plan. The original query still fits after complete cleanup.
    let options = Options {
        memory_bytes: bytes + 16 * 1024,
        ..options
    };
    let budget = Budget::new(options.memory_bytes);
    let admitted = candidate(&root, &budget, &options).unwrap();
    assert!(baseline(&admitted.root, &options, budget.clone()).is_err());
    drop(admitted);
    assert_eq!(budget.used(), 0);
    let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
    assert_eq!(actual.batch, expected);
    assert_eq!(actual.metrics.aggregate_scan_reuses, 0);
    drop(actual);
    assert_eq!(budget.used(), 0);
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            options.cancellation.cancel();
            let budget = Budget::new(options.memory_bytes);
            assert_eq!(
                execute_once(&root, &options, budget.clone(), None, false)
                    .err()
                    .unwrap(),
                "cancelled: query execution"
            );
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn live_reader_cancellation_drops_the_retained_candidate_and_every_reservation() {
    use crate::parquet_scan::test_support::Observer;
    use std::{sync::mpsc, time::Duration};
    let fixture = Fixture::new(false, false, false, 15);
    let root = fixture.plan(SQL);
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let (started, wait_started) = mpsc::sync_channel(1);
            let (resume, wait_resume) = mpsc::sync_channel(1);
            let mut observer = Observer::new(fixture.tables["f"].clone());
            observer.pause = Some((started, Mutex::new(wait_resume)));
            let observer = Arc::new(observer);
            let options = Options {
                workers,
                batch_size,
                test_scan_observer: Some(observer.clone()),
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            assert!(candidate(&root, &budget, &options).is_some());
            std::thread::scope(|scope| {
                let task =
                    scope.spawn(|| execute_once(&root, &options, budget.clone(), None, false));
                wait_started.recv_timeout(Duration::from_secs(10)).unwrap();
                assert!(!task.is_finished());
                assert!(budget.used() > 0);
                assert!(observer.matching_batches.load(Ordering::SeqCst) > 0);
                options.cancellation.cancel();
                resume.send(()).unwrap();
                assert_eq!(
                    task.join().unwrap().err().unwrap(),
                    "cancelled: query execution"
                );
            });
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}

#[test]
fn qualification_through_a_dimension_join_and_integer_sum_keep_multiplicity() {
    let mut fixture = Fixture::new(false, false, true, 15);
    fixture.write(
        "c",
        &["name"],
        vec![Arc::new(StringArray::from(vec!["a", "a", "b", "c"]))],
    );
    fixture.write(
        "i",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from_iter(
                (0..100).map(|i| (i % 11 != 0).then_some(i % 4)),
            )),
            Arc::new(Int64Array::from_iter(
                (0..100).map(|i| (i % 7 != 0).then_some(i - 40)),
            )),
        ],
    );
    let joined = SQL
        .replace("FROM d,f", "FROM c,d,f")
        .replace("WHERE d.k=f.k", "WHERE c.name=d.label AND d.k=f.k");
    let integer = joined
        .replace("FROM c,d,f", "FROM c,d,i")
        .replace("f.", "i.")
        .replace("FROM f GROUP", "FROM i GROUP");
    for sql in [joined, integer] {
        let root = fixture.plan(&sql);
        let serial = Options {
            workers: 1,
            batch_size: 1,
            ..Options::default()
        };
        let expected = baseline(&root, &serial, Budget::new(serial.memory_bytes))
            .unwrap()
            .batch;
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..serial.clone()
                };
                let actual = execute_once(
                    &root,
                    &options,
                    Budget::new(options.memory_bytes),
                    None,
                    false,
                )
                .unwrap();
                assert_eq!(actual.batch, expected, "{sql}: {workers}/{batch_size}");
                assert_eq!(actual.metrics.aggregate_scan_reuses, 1, "{sql}");
            }
        }
    }
}

// Increase only proof bounds in a test plan: these remain conservative for
// the tiny physical fixture and never authorize pruning or change its SQL.
fn enlarge_scan_bounds(node: &mut Node, bound: usize) {
    match &mut node.op {
        RelOp::Scan { .. } => node.estimated_rows = bound,
        RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Materialize { input, .. }
        | RelOp::Limit { input, .. } => enlarge_scan_bounds(input, bound),
        RelOp::Join { left, right, .. } => {
            enlarge_scan_bounds(left, bound);
            enlarge_scan_bounds(right, bound);
        }
        _ => (),
    }
}

#[test]
fn large_cross_products_require_a_completed_build_count_barrier() {
    let mut fixture = Fixture::new(false, false, false, 15);
    fixture.write(
        "c",
        &["name"],
        vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
    );
    let sql = SQL
        .replace("FROM d,f", "FROM c,d,f")
        .replace("WHERE d.k=f.k", "WHERE c.name=d.label AND d.k=f.k");
    let mut root = fixture.plan(&sql);
    let options = Options::default();
    let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
        .unwrap()
        .batch;
    enlarge_scan_bounds(&mut root, 20_000_000);
    let budget = Budget::new(options.memory_bytes);
    let admitted = candidate(&root, &budget, &options).unwrap();
    assert_eq!(max_id(&admitted.root), BUILD_ID);
    let actual = baseline(&admitted.root, &options, budget.clone()).unwrap();
    assert_eq!(actual.batch, expected);
    drop(actual);
    drop(admitted);
    assert_eq!(budget.used(), 0);

    // Tighten this private barrier's count proof to one allowed build row.
    // It must fail before any fact probe can consume a partially built side.
    let mut admitted = candidate(&root, &budget, &options).unwrap();
    enlarge_scan_bounds(&mut admitted.root, usize::MAX);
    let error = baseline(&admitted.root, &options, budget.clone())
        .err()
        .unwrap();
    assert_eq!(error, "aggregate reuse build exceeds proven count bound");
    drop(admitted);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.active_workers(), 0);
}
