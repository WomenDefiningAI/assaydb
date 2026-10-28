use super::*;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture {
    directory: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new(empty: bool) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "subplan-reuse-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut f = Self {
            directory,
            tables: HashMap::new(),
        };
        let keys = [
            Some(3),
            Some(1),
            Some(3),
            None,
            Some(2),
            Some(4),
            Some(1),
            Some(5),
        ];
        let len = if empty { 0 } else { 192 };
        f.write(
            "f",
            vec![
                (
                    "k",
                    Arc::new(Int64Array::from(
                        (0..len).map(|i| keys[i % 8]).collect::<Vec<_>>(),
                    )),
                ),
                (
                    "v",
                    Arc::new(Int64Array::from(
                        (0..len)
                            .map(|i| {
                                if i % 7 == 0 {
                                    None
                                } else {
                                    Some(i as i64 - 80)
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ),
            ],
        );
        f.write(
            "d",
            vec![
                (
                    "k",
                    Arc::new(Int64Array::from(
                        (0..64)
                            .map(|i| match i {
                                0 | 2 => Some(1),
                                1 => Some(3),
                                4 => None,
                                _ => Some(i + 8),
                            })
                            .collect::<Vec<_>>(),
                    )),
                ),
                (
                    "flag",
                    Arc::new(Int64Array::from(
                        (0..64).map(|i| i64::from(i < 3)).collect::<Vec<_>>(),
                    )),
                ),
                (
                    "label",
                    Arc::new(Int64Array::from((0..64).collect::<Vec<i64>>())),
                ),
            ],
        );
        f
    }
    fn write(&mut self, name: &str, arrays: Vec<(&str, ArrayRef)>) {
        let batch = RecordBatch::try_from_iter(arrays).unwrap();
        let path = self.directory.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(17)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        self.tables.insert(name.to_owned(), path);
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
const JOIN: &str = "SELECT f.k,f.v,d.label FROM f JOIN d ON f.k=d.k WHERE d.flag=1";
fn baseline(root: &Node, options: &Options, budget: Budget) -> Result<Execution> {
    execute_once_with_demand_cache(root, options, budget, None, false, None)
}

#[test]
fn duplicate_null_empty_rows_and_consumer_order() {
    for empty in [false, true] {
        let fixture = Fixture::new(empty);
        let sql = format!(
            "{JOIN} UNION ALL {}",
            JOIN.replace("f.", "a.")
                .replace("FROM f JOIN", "FROM f a JOIN")
        );
        let root = fixture.plan(&sql);
        let serial = Options {
            workers: 1,
            batch_size: 1,
            ..Options::default()
        };
        let expected = baseline(&root, &serial, Budget::new(serial.memory_bytes))
            .unwrap()
            .batch;
        assert_eq!(expected.num_rows(), if empty { 0 } else { 288 });
        assert_eq!(expected.column(0).null_count(), 0);
        if !empty {
            assert!(expected.column(1).null_count() > 0);
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..serial.clone()
                };
                let budget = Budget::new(options.memory_bytes);
                let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
                assert_eq!(actual.batch, expected, "{empty}/{workers}/{batch_size}");
                if !empty {
                    assert_eq!(actual.metrics.subplan_materializations, 1);
                    assert_eq!(actual.metrics.subplan_reuses, 1);
                    assert_eq!(actual.metrics.subplan_rows, 144);
                }
                assert!(budget.peak() <= options.memory_bytes);
                drop(actual);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn narrower_first_consumer_and_independent_aggregates() {
    let fixture = Fixture::new(false);
    for sql in [
        "SELECT f.k,SUM(f.v) FROM f JOIN d ON f.k=d.k WHERE d.flag=1 GROUP BY f.k HAVING SUM(f.v) > (SELECT SUM(a.v)/2 FROM f a JOIN d b ON a.k=b.k WHERE b.flag=1) ORDER BY 1",
        "SELECT f.k,f.v,d.label FROM f JOIN d ON f.k=d.k WHERE d.flag=1 AND f.v=(SELECT MIN(a.v) FROM f a JOIN d b ON a.k=b.k WHERE b.flag=1 AND a.k=f.k) ORDER BY 1,2,3",
    ] {
        let root = fixture.plan(sql);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
                    .unwrap()
                    .batch;
                let budget = Budget::new(options.memory_bytes);
                let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
                assert_eq!(actual.batch, expected, "{workers}/{batch_size}: {sql}");
                assert_eq!(actual.metrics.subplan_materializations, 1, "{sql}");
                assert!(actual.metrics.subplan_reuses > 0);
                drop(actual);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn distinct_predicates_keys_and_fallible_subtrees_do_not_match() {
    let fixture = Fixture::new(false);
    let options = Options::default();
    for other in [
        JOIN.replace("d.flag=1", "d.flag=0"),
        JOIN.replace("f.k=d.k", "f.v=d.k"),
        JOIN.replace("d.flag=1", "CAST(d.flag AS TEXT)='1'"),
    ] {
        let root = fixture.plan(&format!("{JOIN} UNION ALL {other}"));
        let budget = Budget::new(options.memory_bytes);
        assert!(candidate(&root, &budget, &options).is_none());
        assert_eq!(budget.used(), 0);
    }
    // Both copies fail inside the filter, so their common subtree is ineligible.
    let bad = JOIN.replace("d.flag=1", "f.v*99999999999999999999999999999999999999 > 0");
    let root = fixture.plan(&format!("{bad} UNION ALL {bad}"));
    let budget = Budget::new(options.memory_bytes);
    assert!(candidate(&root, &budget, &options).is_none());
    assert_eq!(budget.used(), 0);
}

#[test]
fn errors_in_either_consumer_keep_baseline_error_and_release_state() {
    let fixture = Fixture::new(false);
    let bad = JOIN.replace("f.v,", "f.v*99999999999999999999999999999999999999,");
    for sql in [
        format!("{JOIN} UNION ALL {bad}"),
        format!("{bad} UNION ALL {JOIN}"),
    ] {
        let root = fixture.plan(&sql);
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let error = baseline(&root, &options, Budget::new(options.memory_bytes))
                    .err()
                    .unwrap();
                let budget = Budget::new(options.memory_bytes);
                assert!(candidate(&root, &budget, &options).is_some());
                assert_eq!(
                    execute_once(&root, &options, budget.clone(), None, false)
                        .err()
                        .unwrap(),
                    error
                );
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn low_budget_and_forced_spill_decline_without_changing_results() {
    let fixture = Fixture::new(false);
    std::fs::create_dir(fixture.directory.join("spill")).unwrap();
    let joined = JOIN.replace("f.k,f.v,d.label", "f.k AS k,f.v AS v,d.label AS label");
    let root = fixture.plan(&format!(
        "SELECT k,v,label FROM ({joined} UNION ALL {joined}) joined ORDER BY 1,2,3"
    ));
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for forced in [false, true] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: if forced { 32 << 20 } else { 2 << 20 },
                    spill: forced.then(|| crate::spill::SpillOptions {
                        mode: crate::spill::SpillMode::Force,
                        directory: fixture.directory.join("spill"),
                        disk_quota_bytes: 64 << 20,
                        sort_run_bytes: 16384,
                        merge_fan_in: 2,
                    }),
                    ..Options::default()
                };
                let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
                    .unwrap()
                    .batch;
                let budget = Budget::new(options.memory_bytes);
                let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
                assert_eq!(actual.batch, expected);
                assert_eq!(actual.metrics.subplan_materializations, 0);
                drop(actual);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
                assert_eq!(
                    std::fs::read_dir(fixture.directory.join("spill"))
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    }
}

#[test]
fn q02_and_q11_reuse_completed_joins_with_original_sql() {
    // Small, independent inputs: only parts 1/2 have a nonzero supply cost;
    // suppliers 1/3 are German, 2 is French, and all others are outside Europe.
    let mut fixture = Fixture::new(true);
    let ints = |values: Vec<i64>| Arc::new(Int64Array::from(values)) as ArrayRef;
    let text = |values: Vec<&str>| Arc::new(StringArray::from(values)) as ArrayRef;
    let money = |values: Vec<i128>| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(15, 2)
                .unwrap(),
        ) as ArrayRef
    };
    fixture.write(
        "region",
        vec![
            ("r_regionkey", ints((1..=5).collect())),
            (
                "r_name",
                text(vec!["EUROPE", "ASIA", "AFRICA", "AMERICA", "OTHER"]),
            ),
        ],
    );
    fixture.write(
        "nation",
        vec![
            ("n_nationkey", ints((1..=25).collect())),
            (
                "n_regionkey",
                ints(
                    (1..=25)
                        .map(|i| if i <= 2 { 1 } else { (i - 3) % 4 + 2 })
                        .collect(),
                ),
            ),
            (
                "n_name",
                text(
                    (1..=25)
                        .map(|i| match i {
                            1 => "GERMANY",
                            2 => "FRANCE",
                            _ => "OTHER",
                        })
                        .collect(),
                ),
            ),
        ],
    );
    fixture.write(
        "supplier",
        vec![
            ("s_suppkey", ints((1..=64).collect())),
            (
                "s_nationkey",
                ints(
                    (1..=64)
                        .map(|i| match i {
                            1 | 3 => 1,
                            2 => 2,
                            _ => (i - 4) % 23 + 3,
                        })
                        .collect(),
                ),
            ),
            ("s_acctbal", money((1..=64).map(|i| i * 10000).collect())),
            ("s_name", text(vec!["supplier"; 64])),
            ("s_address", text(vec!["address"; 64])),
            ("s_phone", text(vec!["phone"; 64])),
            ("s_comment", text(vec!["comment"; 64])),
        ],
    );
    fixture.write(
        "part",
        vec![
            ("p_partkey", ints((1..=128).collect())),
            (
                "p_size",
                ints((1..=128).map(|i| if i <= 2 { 15 } else { 0 }).collect()),
            ),
            ("p_type", text(vec!["BRASS"; 128])),
            ("p_mfgr", text(vec!["maker"; 128])),
        ],
    );
    fixture.write(
        "partsupp",
        vec![
            ("ps_partkey", ints((0..1024).map(|i| i / 8 + 1).collect())),
            // Cover the dimension's key domain so the small fixture has the
            // same selective join shape as the original queries. Nonzero
            // costs and the handwritten answers still use only suppliers 1-8.
            (
                "ps_suppkey",
                ints(
                    (0..1024)
                        .map(|i| if i < 16 { i % 8 + 1 } else { (i - 16) % 64 + 1 })
                        .collect(),
                ),
            ),
            ("ps_availqty", ints(vec![10; 1024])),
            (
                "ps_supplycost",
                money(
                    (0..1024)
                        .map(|i| match (i / 8 + 1, i % 8 + 1) {
                            (1, 1) => 200,
                            (2, 1) | (1 | 2, 2) => 100,
                            (1 | 2, _) => 300,
                            _ => 0,
                        })
                        .collect(),
                ),
            ),
        ],
    );
    // TPC-H query text is repository-only benchmark data and is deliberately not packaged
    // (crate/sdist ship no TPC-derived queries), so read it at run time and skip outside the repo.
    let benchmarks = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("benchmarks/tpch/sql");
    if !benchmarks.is_dir() {
        eprintln!(
            "skipping TPC-H q02/q11 subplan candidates: {} not present (packaged crate)",
            benchmarks.display()
        );
        return;
    }
    for name in ["q02", "q11"] {
        let sql = std::fs::read_to_string(benchmarks.join(format!("{name}.sql"))).unwrap();
        let root = fixture.plan(&sql);
        let options = Options::default();
        let budget = Budget::new(options.memory_bytes);
        if candidate(&root, &budget, &options).is_none() {
            fn describe(node: &Node, root: &Node, details: &mut Vec<String>) {
                if let Some(c) = canonical(node, &mut 0)
                    && c.joins > 0
                {
                    details.push(format!(
                        "rows={} joins={} filtered={} source_rows={} uses={}",
                        node.estimated_rows,
                        c.joins,
                        c.filtered,
                        c.source_rows,
                        matches(root, node, &c)
                    ));
                }
                children(node, &mut |child| describe(child, root, details));
            }
            let mut details = Vec::new();
            describe(&root, &root, &mut details);
            panic!(
                "{name}: no candidate; clone bytes={}; {details:?}",
                build_groups::node_bytes(&root) * 16 + 65536
            );
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
                    .unwrap()
                    .batch;
                let budget = Budget::new(options.memory_bytes);
                let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
                assert_eq!(actual.batch, expected, "{name}/{workers}/{batch_size}");
                assert_eq!(
                    actual.metrics.subplan_materializations, 1,
                    "{name}/{workers}/{batch_size}: fallbacks={}",
                    actual.metrics.subplan_fallbacks
                );
                assert_eq!(actual.metrics.subplan_reuses, 1, "{name}");
                assert_eq!(actual.metrics.subplan_fallbacks, 0, "{name}");
                if name == "q02" {
                    // Retain the three European suppliers once, before their
                    // text payload expands across partsupp. Part membership
                    // must remain free to reach each fact scan independently.
                    assert_eq!(actual.metrics.subplan_rows, 3);
                    assert_eq!(actual.metrics.scan_materializations, 0);
                }
                let column = if name == "q02" { 3 } else { 0 };
                let keys = actual
                    .batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(
                    keys.values().as_ref(),
                    if name == "q02" {
                        &[1, 2, 2][..]
                    } else {
                        &[1, 2][..]
                    }
                );
                let money_column = if name == "q02" { 0 } else { 1 };
                let values = actual
                    .batch
                    .column(money_column)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                assert_eq!(values.scale(), 2);
                assert_eq!(
                    values.values().as_ref(),
                    if name == "q02" {
                        &[20000, 20000, 10000][..]
                    } else {
                        &[5000, 4000][..]
                    }
                );
                drop(actual);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.active_workers(), 0);
            }
        }
    }
}

#[test]
fn source_replacement_and_live_cancellation_are_terminal_and_release_credit() {
    use crate::parquet_scan::test_support::Observer;
    use std::{sync::mpsc, time::Duration};
    let mut fixture = Fixture::new(false);
    let root = fixture.plan(&format!("{JOIN} UNION ALL {JOIN}"));
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let (started, wait_started) = mpsc::sync_channel(1);
            let (resume, wait_resume) = mpsc::sync_channel(1);
            let mut observer = Observer::new(fixture.tables["f"].clone());
            observer.pause = Some((started, Mutex::new(wait_resume)));
            let options = Options {
                workers,
                batch_size,
                test_scan_observer: Some(Arc::new(observer)),
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            assert!(candidate(&root, &budget, &options).is_some());
            std::thread::scope(|scope| {
                let task =
                    scope.spawn(|| execute_once(&root, &options, budget.clone(), None, false));
                wait_started.recv_timeout(Duration::from_secs(10)).unwrap();
                assert!(budget.used() > 0);
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
    let options = Options::default();
    let budget = Budget::new(options.memory_bytes);
    let retained = candidate(&root, &budget, &options).unwrap();
    fixture.write(
        "f",
        vec![
            ("k", Arc::new(Int64Array::from(vec![1]))),
            ("v", Arc::new(Int64Array::from(vec![2]))),
        ],
    );
    assert!(retained.verify(&options).is_err());
    drop(retained);
    assert_eq!(budget.used(), 0);
}

#[test]
fn admitted_candidate_budget_failure_retries_original_plan_with_fresh_state() {
    let fixture = Fixture::new(false);
    let root = fixture.plan(&format!("{JOIN} UNION ALL {JOIN}"));
    let bytes = build_groups::node_bytes(&root) * 16 + 65536;
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                memory_bytes: 128 << 20,
                ..Options::default()
            };
            let control = baseline(&root, &options, Budget::new(options.memory_bytes)).unwrap();
            // Under pressure the original plan can run on the caller. Fund its
            // measured serial working set, not optional helpers' stack leases.
            let serial = Options {
                workers: 1,
                ..options.clone()
            };
            let required = baseline(&root, &serial, Budget::new(options.memory_bytes))
                .unwrap()
                .metrics
                .peak_reserved_bytes;
            let expected = control.batch;
            let budget = Budget::new(options.memory_bytes);
            // The plan and its source guard fit, but leave too little room for
            // a decoder. Removing the candidate credit funds the original plan.
            let pressure = budget
                .reserve(options.memory_bytes - bytes.max(required) - 16384)
                .unwrap();
            assert!(candidate(&root, &budget, &options).is_some());
            let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
            assert_eq!(actual.batch, expected);
            assert_eq!(actual.metrics.subplan_fallbacks, 1);
            assert_eq!(actual.metrics.subplan_materializations, 0);
            drop(actual);
            assert_eq!(budget.used(), pressure.bytes());
            assert_eq!(budget.active_workers(), 0);
            drop(pressure);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn complete_empty_filtered_result_is_reused() {
    let fixture = Fixture::new(false);
    let empty = JOIN.replace("d.flag=1", "d.flag=2");
    let root = fixture.plan(&format!("{empty} UNION ALL {empty}"));
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let budget = Budget::new(options.memory_bytes);
            let expected = baseline(&root, &options, Budget::new(options.memory_bytes))
                .unwrap()
                .batch;
            let actual = execute_once(&root, &options, budget.clone(), None, false).unwrap();
            assert_eq!(actual.batch, expected);
            assert_eq!(actual.batch.num_rows(), 0);
            assert_eq!(actual.metrics.subplan_materializations, 1);
            assert_eq!(actual.metrics.subplan_reuses, 1);
            assert_eq!(actual.metrics.subplan_rows, 0);
            drop(actual);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.active_workers(), 0);
        }
    }
}
