use super::*;
use crate::execute::{self, Options};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

struct Failure {
    stage: &'static str,
    skip: usize,
    action: &'static str,
    fired: bool,
    visits: usize,
}

struct WideFixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}

impl WideFixture {
    fn new(name: &str, empty_probe: bool, empty_build: bool) -> Self {
        Self::with_probe_payload(name, empty_probe, empty_build, 0)
    }

    fn with_probe_payload(
        name: &str,
        empty_probe: bool,
        empty_build: bool,
        payload: usize,
    ) -> Self {
        use arrow::array::{ArrayRef, Int32Array, StringArray};
        let root = std::env::temp_dir().join(format!("wide-demand-{}-{name}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let mut probe_columns = vec![(
            "k",
            Arc::new(Int64Array::from_iter(
                (0..550)
                    .map(Some)
                    .chain([Some(2), Some(2), None, Some(-1)])
                    .chain(std::iter::repeat_n(Some(2), 128)),
            )) as ArrayRef,
        )];
        if payload > 0 {
            probe_columns.push((
                "pad",
                Arc::new(StringArray::from_iter_values(
                    (0..682).map(|i| format!("{i:04}{}", "p".repeat(payload))),
                )) as ArrayRef,
            ));
        }
        let probe = RecordBatch::try_from_iter(probe_columns).unwrap();
        let build = RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int32Array::from_iter(
                    (0..4096).map(|i| (i % 23 != 0).then_some(i / 2)),
                )) as ArrayRef,
            ),
            (
                "v",
                Arc::new(Int64Array::from_iter_values(0..4096)) as ArrayRef,
            ),
            (
                "a",
                Arc::new(StringArray::from_iter_values(
                    (0..4096).map(|i| format!("a{i}")),
                )) as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringArray::from_iter(
                    (0..4096).map(|i| (i % 7 != 0).then_some("wide é payload")),
                )) as ArrayRef,
            ),
            (
                "c",
                Arc::new(StringArray::from(vec!["c"; 4096])) as ArrayRef,
            ),
            (
                "d",
                Arc::new(StringArray::from(vec!["d"; 4096])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut tables = HashMap::new();
        for (name, batch, empty) in [("p", probe, empty_probe), ("r", build, empty_build)] {
            let path = root.join(format!("{name}.parquet"));
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                batch.schema(),
                Some(
                    parquet::file::properties::WriterProperties::builder()
                        .set_max_row_group_size(257)
                        .build(),
                ),
            )
            .unwrap();
            writer
                .write(&batch.slice(0, if empty { 0 } else { batch.num_rows() }))
                .unwrap();
            writer.close().unwrap();
            tables.insert(name.into(), path);
        }
        Self { root, tables }
    }
}
impl Drop for WideFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

const WIDE_SQL: &str = "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM p LEFT JOIN r ON p.k=r.k";

#[test]
fn wide_probe_uses_existing_completed_build_membership_without_an_extra_pass() {
    let fixture = WideFixture::new("wide-probe", false, false);
    let options = Options::default();
    let sql = "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM r JOIN p ON r.k=p.k";
    let prepared = execute::prepare(sql, &fixture.tables, &options).unwrap();
    let expected = execute::execute(
        &prepared,
        &Options {
            relational_optimizations: false,
            batch_size: 1,
            ..options.clone()
        },
    )
    .unwrap();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let actual = execute::execute(
                &prepared,
                &Options {
                    workers,
                    batch_size,
                    ..options.clone()
                },
            )
            .unwrap();
            assert_eq!(actual.batch, expected.batch, "{workers}/{batch_size}");
            assert!(
                actual.metrics.bitmap_scan_filters > 0,
                "{workers}/{batch_size}"
            );
            assert_eq!(actual.metrics.demand_filtered_scans, 0);
            assert!(actual.metrics.rows_read <= expected.metrics.rows_read);
        }
    }
}

#[test]
fn wide_scan_demand_keeps_duplicates_nulls_coercions_and_probe_order() {
    for (name, empty_probe, empty_build) in [
        ("full", false, false),
        ("empty-probe", true, false),
        ("empty-build", false, true),
    ] {
        let fixture = WideFixture::new(name, empty_probe, empty_build);
        let serial = Options {
            relational_optimizations: false,
            batch_size: 1,
            ..Options::default()
        };
        let prepared = execute::prepare(WIDE_SQL, &fixture.tables, &Options::default()).unwrap();
        let root = prepared.relational.as_ref().unwrap();
        let expected = execute::execute(&prepared, &serial).unwrap();
        if !empty_probe && !empty_build {
            // Exact non-NULL duplicate multiplication plus one row for each miss.
            assert!(expected.batch.num_rows() > 1000);
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let result =
                    super::super::execute_in_budget(root, &options, budget.clone(), false).unwrap();
                assert_eq!(
                    result.batch, expected.batch,
                    "{name}/{workers}/{batch_size}"
                );
                if !empty_probe && !empty_build {
                    assert_eq!(
                        result.metrics.demand_filtered_scans, 1,
                        "{workers}/{batch_size}"
                    );
                }
                drop(result);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn wide_scan_demand_refusal_and_cancellation_leave_no_partial_state() {
    let fixture = WideFixture::new("refusals", false, false);
    let options = Options {
        batch_size: 7,
        ..Options::default()
    };
    let prepared = execute::prepare(WIDE_SQL, &fixture.tables, &options).unwrap();
    let root = prepared.relational.as_ref().unwrap();
    let expected = execute::execute(
        &prepared,
        &Options {
            relational_optimizations: false,
            ..options.clone()
        },
    )
    .unwrap();
    for workers in [1, 2, 4, 10] {
        for stage in ["membership", "filtered-build", "cached-build-index"] {
            for action in ["reservation", "cancellation"] {
                let options = Options {
                    workers,
                    cancellation: Default::default(),
                    ..options.clone()
                };
                let budget = Budget::new(options.memory_bytes);
                let injection = Inject::new(stage, 0, action);
                let actual = super::super::execute_in_budget(root, &options, budget.clone(), false);
                injection.assert_fired();
                if action == "cancellation" {
                    assert_eq!(actual.err().unwrap(), "cancelled: query execution");
                } else {
                    assert_eq!(actual.unwrap().batch, expected.batch, "{stage}/{workers}");
                }
                drop(injection);
                assert_eq!(budget.used(), 0, "{stage}/{action}/{workers}");
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn wide_scan_demand_declines_fallible_subtrees_and_computed_or_mixed_keys() {
    let fixture = WideFixture::new("barriers", false, false);
    for sql in [
        "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM p LEFT JOIN r ON CAST(p.k AS REAL)=r.k",
        "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM p LEFT JOIN r ON p.k+1=r.k",
        "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM p LEFT JOIN (SELECT k,10000000000000000000000000000000000000*v AS v,a,b,c,d FROM r) r ON p.k=r.k",
        "SELECT p.k,r.v,r.a,r.b,r.c,r.d FROM (SELECT 10000000000000000000000000000000000000*k AS k FROM p) p LEFT JOIN r ON p.k=r.k",
    ] {
        let options = Options::default();
        let prepared = execute::prepare(sql, &fixture.tables, &options).unwrap();
        let expected = execute::execute(
            &prepared,
            &Options {
                relational_optimizations: false,
                batch_size: 1,
                ..options.clone()
            },
        );
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let result = execute::execute(
                    &prepared,
                    &Options {
                        workers,
                        batch_size,
                        ..options.clone()
                    },
                );
                match (&expected, result) {
                    (Ok(expected), Ok(actual)) => {
                        assert_eq!(actual.batch, expected.batch, "{sql}");
                        assert_eq!(actual.metrics.demand_filtered_scans, 0);
                    }
                    (Err(expected), Err(actual)) => assert_eq!(&actual, expected, "{sql}"),
                    _ => panic!("status changed: {sql}"),
                }
            }
        }
    }
}

// Keep the wide RHS as the build of this fixed INNER plan. The SQL planner
// normally reverses this simple join to build the smaller table; that tests
// build-to-probe filtering instead of the probe-first path under review.
fn retained_wide_inner(node: &mut super::super::Node) {
    use super::super::{JoinKind, RelOp};
    match &mut node.op {
        RelOp::Join { kind, .. } => {
            assert_eq!(*kind, JoinKind::Left);
            *kind = JoinKind::Inner;
        }
        RelOp::Unary { input, .. } | RelOp::Project { input, .. } | RelOp::Rename { input } => {
            retained_wide_inner(input)
        }
        _ => panic!("expected the fixture's single join"),
    }
}

#[test]
fn wide_inner_probe_first_demand_runs_and_matches_the_unfiltered_plan() {
    for (name, empty_probe, empty_build) in [
        ("inner-full", false, false),
        ("inner-empty-probe", true, false),
        ("inner-empty-build", false, true),
    ] {
        let fixture = WideFixture::new(name, empty_probe, empty_build);
        let mut prepared =
            execute::prepare(WIDE_SQL, &fixture.tables, &Options::default()).unwrap();
        let root = prepared.relational.as_mut().unwrap();
        retained_wide_inner(root);
        let serial = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = super::super::execute(root, &serial).unwrap();
        assert_eq!(
            expected.batch.num_rows(),
            if empty_probe || empty_build { 0 } else { 1312 }
        );
        if !empty_probe && !empty_build {
            let keys = expected
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(keys.null_count(), 0);
            assert_eq!(keys.iter().filter(|k| *k == Some(2)).count(), 262);
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let budget = Budget::new(options.memory_bytes);
                let actual =
                    super::super::execute_in_budget(root, &options, budget.clone(), false).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "{name}/{workers}/{batch_size}"
                );
                if !empty_probe && !empty_build {
                    assert!(
                        actual.metrics.demand_filtered_scans > 0,
                        "{workers}/{batch_size}"
                    );
                }
                drop(actual);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn wide_cache_cap_declines_without_changing_results_or_leaking() {
    let fixture = WideFixture::with_probe_payload("cache-cap", false, false, 8192);
    let options = Options {
        batch_size: 1,
        memory_bytes: 64 << 20,
        ..Options::default()
    };
    let prepared = execute::prepare(
        "SELECT p.k,p.pad,r.v,r.a,r.b,r.c,r.d FROM p LEFT JOIN r ON p.k=r.k",
        &fixture.tables,
        &options,
    )
    .unwrap();
    let root = prepared.relational.as_ref().unwrap();
    let expected = super::super::execute(
        root,
        &Options {
            relational_optimizations: false,
            ..options.clone()
        },
    )
    .unwrap();
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            ..options.clone()
        };
        let budget = Budget::new(options.memory_bytes);
        // Observe all cache callbacks: a strict nonempty prefix proves admission
        // and retention before the cap declines the remaining probe.
        let injection = Inject::new("probe-caching", 0, "observe");
        let actual =
            super::super::execute_in_budget(root, &options, budget.clone(), false).unwrap();
        injection.assert_fired();
        assert_eq!(actual.metrics.demand_filtered_scans, 0);
        assert!((2..682).contains(&injection.visits()));
        assert_eq!(
            actual.metrics.rows_read,
            expected.metrics.rows_read + injection.visits()
        );
        assert_eq!(actual.batch, expected.batch);
        drop(actual);
        drop(injection);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn wide_cached_output_failure_retries_without_retention_and_preserves_errors() {
    let fixture = WideFixture::new("late-cache", false, false);
    let bad_sql = "SELECT p.k,r.v,r.a,r.b,r.c,r.d,CASE WHEN p.k=549 THEN CAST('1e308' AS REAL)*CAST(p.k AS REAL) ELSE 1.0 END bad FROM p LEFT JOIN r ON p.k=r.k";
    for sql in [WIDE_SQL, bad_sql] {
        let options = Options {
            batch_size: 7,
            ..Options::default()
        };
        let prepared = execute::prepare(sql, &fixture.tables, &options).unwrap();
        let root = prepared.relational.as_ref().unwrap();
        let expected = super::super::execute(
            root,
            &Options {
                workers: 1,
                relational_optimizations: false,
                ..options.clone()
            },
        );
        if sql == bad_sql {
            assert_eq!(
                expected.as_ref().err().unwrap(),
                "floating arithmetic overflow"
            );
        }
        for workers in [1, 2, 4, 10] {
            for action in ["reservation", "construction", "cancellation"] {
                let options = Options {
                    workers,
                    cancellation: Default::default(),
                    ..options.clone()
                };
                let budget = Budget::new(options.memory_bytes);
                // The hook runs AFTER a batch reaches the downstream consumer.
                // Skip one callback so a nonempty prefix has already escaped
                // the join, but must not escape the whole-query retry buffer.
                let injection = Inject::new("cached-probe-output", 1, action);
                let actual = super::super::execute_in_budget(root, &options, budget.clone(), false);
                injection.assert_fired();
                if action == "cancellation" {
                    assert_eq!(actual.err().unwrap(), "cancelled: query execution");
                } else {
                    match (&expected, actual) {
                        (Ok(expected), Ok(actual)) => {
                            assert_eq!(actual.batch, expected.batch, "{workers}/{action}");
                            assert_eq!(actual.metrics.demand_filtered_scans, 0);
                            assert_eq!(actual.metrics.rows_read, 4096 + 682);
                            assert_eq!(actual.metrics.auto_spill_retries, 0);
                        }
                        (Err(expected), Err(actual)) => assert_eq!(&actual, expected),
                        _ => panic!("retry changed query status"),
                    }
                }
                drop(injection);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

thread_local! {
    static FAILURE: std::cell::RefCell<Option<Failure>> = const { std::cell::RefCell::new(None) };
}

// Thread-local, one-shot injection cannot affect another query/test. Exercise
// actual Budget refusal as well as a non-resource construction error.
pub(crate) fn checkpoint(stage: &str, budget: &Budget, options: &Options) -> crate::Result<()> {
    FAILURE.with_borrow_mut(|failure| {
        let Some(failure) = failure else {
            return Ok(());
        };
        if failure.stage != stage {
            return Ok(());
        }
        failure.visits += 1;
        if failure.fired {
            return Ok(());
        }
        if failure.skip > 0 {
            failure.skip -= 1;
            return Ok(());
        }
        failure.fired = true;
        match failure.action {
            "observe" => Ok(()),
            "reservation" => {
                let _held = budget.reserve(budget.limit() - budget.used())?;
                budget.reserve(1).map(|_| ())
            }
            "construction" => Err("injected demand construction failure".into()),
            "cancellation" => {
                options.cancellation.cancel();
                options.cancellation.check()
            }
            _ => unreachable!(),
        }
    })
}

struct Inject;
impl Inject {
    fn new(stage: &'static str, skip: usize, action: &'static str) -> Self {
        FAILURE.with_borrow_mut(|failure| {
            assert!(failure.is_none());
            *failure = Some(Failure {
                stage,
                skip,
                action,
                fired: false,
                visits: 0,
            });
        });
        Self
    }
    fn visits(&self) -> usize {
        FAILURE.with_borrow(|failure| failure.as_ref().unwrap().visits)
    }
    fn assert_fired(&self) {
        FAILURE.with_borrow(|failure| assert!(failure.as_ref().unwrap().fired));
    }
}
impl Drop for Inject {
    fn drop(&mut self) {
        FAILURE.with_borrow_mut(|failure| *failure = None);
    }
}

#[test]
fn membership_refusal_preserves_accounting_and_duplicates_need_no_credit() {
    assert!(Members::new(&Budget::new(4095)).is_none());
    let budget = Budget::new(8192);
    let mut members = Members::new(&budget).unwrap();
    assert!(members.insert(&0_i64.to_le_bytes()));
    let used = budget.used();
    assert!(members.insert(&0_i64.to_le_bytes()));
    assert_eq!(budget.used(), used);
    assert!(!members.insert(&1_i64.to_le_bytes())); // Exceeds the 1/16 share.
    assert_eq!(budget.used(), used);
    assert_eq!(members.members.len(), 1);
    drop(members);
    assert_eq!(budget.used(), 0);

    let mut members = Members::new(&budget).unwrap();
    let other = budget.reserve(budget.limit() - budget.used()).unwrap();
    assert!(Members::new(&budget).is_none());
    assert!(!members.insert(&0_i64.to_le_bytes())); // Shared budget is exhausted.
    assert_eq!(members.lease.bytes(), 256);
    assert!(members.members.is_empty());
    assert_eq!(budget.used(), budget.limit());
    drop(other);
    assert!(!members.lease.try_grow(usize::MAX)); // Accounting overflow declines.
    assert_eq!(budget.used(), 256);
    assert!(members.insert(&0_i64.to_le_bytes()));
    assert_eq!(members.lease.bytes(), budget.used());
    drop(members);
    assert_eq!(budget.used(), 0);
}

struct Fixture(PathBuf);
impl Fixture {
    fn new(name: &str, rows: i64, probe: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "demand-members-{}-{name}.parquet",
            std::process::id()
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
        // A duplicate group/probe and a NULL key exercise exact membership;
        // the probe also has a key absent from the aggregate input.
        let extra = if probe {
            vec![Some(0), None, Some(-1)]
        } else {
            vec![Some(0), None]
        };
        let keys = (0..rows).map(Some).chain(extra).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(
                    keys.iter().map(|k| k.map(|k| k % 5)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    keys.iter()
                        .map(|k| k.unwrap_or(0) % 7 - 3)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
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
fn selective_composite_demand_above_old_cap_matches_unfiltered_left_join() {
    const PROBES: i64 = 65_537;
    let probe = Fixture::new("probe", PROBES, true);
    let build = Fixture::new("build", PROBES * 17, false);
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("r".into(), build.0.clone())]);
    let options = Options {
        memory_bytes: 2 << 30,
        workers: 1,
        ..Options::default()
    };
    let sql = "SELECT p.k,p.s,a.total FROM p LEFT JOIN \
               (SELECT k,s,SUM(v) AS total FROM r GROUP BY k,s) a \
               ON p.k=a.k AND p.s=a.s ORDER BY 1,2";
    let prepared = execute::prepare(sql, &tables, &options).unwrap();
    let filtered = execute::execute(&prepared, &options).unwrap();
    assert_eq!(filtered.metrics.demand_filtered_aggregates, 1);
    assert_eq!(filtered.metrics.auto_spill_retries, 0);
    let unfiltered = execute::execute(
        &prepared,
        &Options {
            relational_optimizations: false,
            ..options
        },
    )
    .unwrap();
    assert_eq!(unfiltered.metrics.demand_filtered_aggregates, 0);
    assert_eq!(filtered.batch, unfiltered.batch);
    assert_eq!(filtered.batch.num_rows(), PROBES as usize + 3);
    let keys = filtered
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = filtered
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row in 0..keys.len() {
        if keys.is_null(row) || keys.value(row) == -1 {
            assert!(totals.is_null(row));
        } else {
            assert!(!totals.is_null(row));
            let key = keys.value(row);
            assert_eq!(totals.value(row), if key == 0 { -6 } else { key % 7 - 3 });
        }
    }
}

#[test]
fn cancellation_during_demand_probe_releases_members_batches_and_workers() {
    let probe = Fixture::new("cancel-probe", 4, true);
    let build = Fixture::new("cancel-build", 128, false);
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("r".into(), build.0.clone())]);
    let sql = "SELECT p.k,a.total FROM p LEFT JOIN \
               (SELECT k,SUM(v) total FROM r GROUP BY k) a ON p.k=a.k";
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 3,
            ..Options::default()
        };
        let prepared = execute::prepare(sql, &tables, &options).unwrap();
        let budget = Budget::new(options.memory_bytes);
        let mut context = super::super::Context {
            filtered_reuse: None,
            build_groups: None,
            demand_cache: None,
            shared_attempt: None,
            options: &options,
            scan_filters: HashMap::new(),
            static_filters: HashMap::new(),
            scan_transforms: HashMap::new(),
            lane_transforms: Default::default(),
            replay_transforms: HashMap::new(),
            materialized: HashMap::new(),
            recursive_bindings: None,
            sideways_registry_lease: None,
            spill: None,
            budget: budget.clone(),
            metrics: execute::Metrics::default(),
        };
        let mut emitted = false;
        let error = prepared
            .relational
            .as_ref()
            .unwrap()
            .run(&mut context, &mut |batch| {
                assert!(batch.num_rows() > 0);
                assert!(budget.used() > 0);
                emitted = true;
                options.cancellation.cancel();
                options.cancellation.check()
            })
            .unwrap_err();
        assert!(emitted);
        assert_eq!(error, "cancelled: query execution");
        assert_eq!(context.metrics.demand_filtered_aggregates, 1);
        assert!(context.scan_filters.is_empty());
        drop(context);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.active_workers(), 0);
    }
}

#[test]
fn every_preparation_refusal_replays_from_start_and_releases_state() {
    let probe = Fixture::new("refusal-probe", 4, true);
    let build = Fixture::new("refusal-build", 128, false);
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("r".into(), build.0.clone())]);
    let sql = "SELECT SUM(p.v), MAX(a.c) FROM p LEFT JOIN \
               (SELECT k,COUNT(*) c FROM r GROUP BY k) a ON p.k=a.k";
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 3,
            ..Options::default()
        };
        let prepared = execute::prepare(sql, &tables, &options).unwrap();
        let root = prepared.relational.as_ref().unwrap();
        let serial = execute::execute(
            &prepared,
            &Options {
                relational_optimizations: false,
                workers: 1,
                ..options.clone()
            },
        )
        .unwrap();
        // p.v is [-3,-2,-1,0,-3,-3,-4]. Build key 0 has two
        // occurrences; all other matched keys have one. NULL never matches.
        for (column, expected) in serial.batch.columns().iter().zip([-16, 2]) {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(expected)]
            );
        }
        for stage in [
            "probe-caching",
            "normalization",
            "evaluation",
            "encoding",
            "membership",
            "filtered-build",
        ] {
            for skip in [0, 1] {
                if stage == "filtered-build" && skip != 0 {
                    continue;
                }
                for action in ["reservation", "construction", "cancellation"] {
                    let options = Options {
                        cancellation: Default::default(),
                        ..options.clone()
                    };
                    let budget = Budget::new(options.memory_bytes);
                    let injection = Inject::new(stage, skip, action);
                    let actual =
                        super::super::execute_in_budget(root, &options, budget.clone(), false);
                    injection.assert_fired();
                    if action == "cancellation" {
                        assert_eq!(actual.err().unwrap(), "cancelled: query execution");
                    } else {
                        let actual = actual
                            .unwrap_or_else(|e| panic!("{stage}/{skip}/{action}/{workers}: {e}"));
                        assert_eq!(
                            actual.batch, serial.batch,
                            "{stage}/{skip}/{action}/{workers}"
                        );
                        assert_eq!(actual.metrics.auto_spill_retries, 0);
                        assert_eq!(
                            actual.metrics.demand_filtered_aggregates,
                            usize::from(stage == "filtered-build")
                        );
                    }
                    drop(injection);
                    assert_eq!(budget.used(), 0, "{stage}/{skip}/{action}/{workers}");
                    assert_eq!(budget.active_workers(), 0);
                    let fresh = Options {
                        cancellation: Default::default(),
                        ..options.clone()
                    };
                    let next = super::super::execute_in_budget(root, &fresh, budget.clone(), false)
                        .unwrap();
                    assert_eq!(next.batch, serial.batch);
                    assert_eq!(next.metrics.demand_filtered_aggregates, 1);
                    drop(next);
                    assert_eq!(budget.used(), 0);
                    assert_eq!(budget.active_workers(), 0);
                }
            }
        }
    }
}

#[test]
fn probe_cache_is_bounded_funded_and_released_on_refusal() {
    let budget = Budget::new(64 << 10);
    let batch = RecordBatch::try_from_iter([(
        "k",
        Arc::new(Int64Array::from(vec![0; 128])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let mut cache = ProbeCache::new(&budget).unwrap();
    assert!(cache.push(batch.clone()).unwrap());
    let retained = budget.used();
    assert!(retained >= batch.get_array_memory_size());
    let pressure = budget.reserve(budget.limit() - budget.used()).unwrap();
    assert!(cache.push(batch.clone()).is_err());
    drop(pressure);
    assert_eq!(budget.used(), retained);
    assert_eq!(cache.batches.len(), 1);
    while cache.push(batch.clone()).unwrap() {
        assert!(budget.used() <= budget.limit() / 16);
    }
    assert!(budget.used() <= budget.limit() / 16);
    drop(cache);
    assert_eq!(budget.used(), 0);
}

#[test]
fn bounded_probe_reuse_and_late_failure_replay_preserve_complete_results() {
    let probe = Fixture::new("cache-probe", 4, true);
    let build = Fixture::new("cache-build", 128, false);
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("r".into(), build.0.clone())]);
    let sql = "SELECT p.k,a.c FROM p LEFT JOIN \
               (SELECT k,COUNT(*) c FROM r GROUP BY k) a ON p.k=a.k";
    let serial = Options {
        relational_optimizations: false,
        batch_size: 3,
        ..Options::default()
    };
    let expected =
        execute::execute(&execute::prepare(sql, &tables, &serial).unwrap(), &serial).unwrap();
    for (column, values) in expected.batch.columns().iter().zip([
        vec![Some(0), Some(1), Some(2), Some(3), Some(0), None, Some(-1)],
        vec![Some(2), Some(1), Some(1), Some(1), Some(2), None, None],
    ]) {
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            values
        );
    }
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            batch_size: 3,
            ..Options::default()
        };
        let prepared = execute::prepare(sql, &tables, &options).unwrap();
        let root = prepared.relational.as_ref().unwrap();
        let budget = Budget::new(options.memory_bytes);
        let actual =
            super::super::execute_in_budget(root, &options, budget.clone(), false).unwrap();
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.demand_filtered_aggregates, 1);
        // Seven probe rows and 130 build rows: no second probe scan.
        assert_eq!(actual.metrics.rows_read, 137);
        drop(actual);
        assert_eq!(budget.used(), 0);
        for (stage, skip) in [
            ("cached-build-index", 0),
            ("cached-probe-output", 0),
            ("cached-probe-output", 1),
        ] {
            for action in ["reservation", "construction", "cancellation"] {
                let options = Options {
                    cancellation: Default::default(),
                    ..options.clone()
                };
                let injection = Inject::new(stage, skip, action);
                let actual = super::super::execute_in_budget(root, &options, budget.clone(), false);
                injection.assert_fired();
                if action == "cancellation" {
                    assert_eq!(actual.err().unwrap(), "cancelled: query execution");
                } else {
                    let actual = actual.unwrap_or_else(|e| panic!("{stage}/{skip}/{action}: {e}"));
                    assert_eq!(actual.batch, expected.batch);
                    assert_eq!(actual.metrics.auto_spill_retries, 0);
                    // The fresh attempt streams the probe with no retention.
                    assert_eq!(actual.metrics.rows_read, 144);
                }
                drop(injection);
                assert_eq!(budget.used(), 0, "{stage}/{skip}/{action}");
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn correlated_extremum_average_and_sum_reuse_the_prepared_probe() {
    let probe = Fixture::new("correlated-cache-probe", 4, true);
    let build = Fixture::new("correlated-cache-build", 128, false);
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("r".into(), build.0.clone())]);
    for (sql, expected) in [
        (
            "SELECT p.k FROM p WHERE p.v = \
             (SELECT MIN(s.v) FROM r JOIN r s ON r.k=s.k WHERE r.k=p.k) ORDER BY 1",
            vec![0, 0, 1, 2, 3],
        ),
        (
            "SELECT p.k FROM p WHERE p.v >= \
             (SELECT 0.2*AVG(r.v) FROM r WHERE r.k=p.k) ORDER BY 1",
            vec![3],
        ),
        (
            "SELECT p.k FROM p WHERE p.v >= \
             (SELECT 0.5*SUM(r.v) FROM r WHERE r.k=p.k) ORDER BY 1",
            vec![0, 0, 3],
        ),
    ] {
        for workers in [1, 2, 4, 10] {
            for relational_optimizations in [false, true] {
                let options = Options {
                    workers,
                    relational_optimizations,
                    batch_size: 3,
                    ..Options::default()
                };
                let plan = execute::prepare(sql, &tables, &options).unwrap();
                let injection = Inject::new("cached-probe-output", 0, "observe");
                let actual = execute::execute(&plan, &options).unwrap();
                assert_eq!(
                    actual
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected.iter().copied().map(Some).collect::<Vec<_>>(),
                    "{sql}, workers={workers}, optimized={relational_optimizations}"
                );
                if relational_optimizations {
                    injection.assert_fired();
                }
                drop(injection);
            }
        }
    }
}
