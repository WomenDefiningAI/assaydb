use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute},
    rel,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn late_payload_fixture(empty_probe: bool, empty_build: bool) -> Fixture {
    let mut fixture = Fixture::new(false, false);
    // Duplicate build/probe keys, NULL join keys and nullable wide payloads.
    // Identically typed join keys exercise the dictionary's hash-build path.
    fixture.write(
        "b",
        &["k", "g", "h"],
        vec![
            Arc::new(Int64Array::from_iter(
                (0..132).map(|i| (i < 130).then_some((i / 2) as i64)),
            )),
            Arc::new(StringArray::from_iter((0..132).map(|i| {
                (i / 2 % 13 != 0).then(|| format!("{:03}-{}", i / 2, "é\0雪".repeat(256)))
            }))),
            Arc::new(StringArray::from_iter((0..132).map(|i| {
                (i / 2 % 17 != 0).then(|| format!("{:03}-{}", 66 - i / 2, "payload".repeat(128)))
            }))),
        ],
        empty_build,
    );
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from_iter(
                (0..268).map(|i| (i < 264).then_some(((i / 4 * 37) % 67) as i64)),
            )),
            Arc::new(Int64Array::from_iter((0..268).map(|i| {
                let key = (i / 4 * 37) % 67;
                (key % 13 != 0).then_some(key as i64)
            }))),
        ],
        empty_probe,
    );
    fixture
}

#[test]
fn late_payload_sort_preserves_wide_values_order_nulls_and_boundary_ties() {
    let fixture = late_payload_fixture(false, false);
    for (suffix, heap) in [
        // Aggregate-only ordering keeps every group slot as an ID until LIMIT.
        ("ORDER BY total DESC NULLS LAST LIMIT 7", true),
        ("ORDER BY total DESC NULLS LAST,k LIMIT 7", true),
        ("ORDER BY total DESC NULLS LAST,k LIMIT 0", false),
        ("ORDER BY total DESC NULLS LAST,k LIMIT 65", false),
        ("ORDER BY total DESC NULLS LAST,k LIMIT 100", false),
        // Every count ties, so the original full sort determines the prefix.
        ("ORDER BY n LIMIT 7", false),
        // One group key is restored eagerly, the other stays an ID payload.
        ("ORDER BY h NULLS FIRST,k LIMIT 7", true),
    ] {
        let sql = format!(
            "SELECT b.g,b.k AS k,SUM(p.v) total,b.h AS h,COUNT(*) n,b.g again FROM p JOIN b ON p.k=b.k GROUP BY b.k,b.g,b.h {suffix}"
        );
        let serial = Fixture::options(1, 1, false);
        let expected = execute(
            &rel::prepare(&sql, &fixture.tables, &serial).unwrap(),
            &serial,
        )
        .unwrap()
        .batch;
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Fixture::options(workers, batch_size, true);
                let actual = execute(
                    &rel::prepare(&sql, &fixture.tables, &options).unwrap(),
                    &options,
                )
                .unwrap();
                assert_eq!(actual.batch, expected, "{sql}/{workers}/{batch_size}");
                // The existing ORDER BY schema is shared even for wide output.
                assert_eq!(actual.metrics.late_payload_sorts, 1, "{sql}");
                assert_eq!(actual.metrics.late_payload_rows, expected.num_rows());
                assert_eq!(actual.metrics.top_k_sorts > 0, heap, "{sql}");
                assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert!(actual.metrics.worker_threads_peak <= workers);
            }
        }
    }
}

#[test]
fn late_payload_empty_sides_and_computed_outputs_keep_the_existing_path() {
    for (probe, build) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = late_payload_fixture(probe, build);
        for (sql, computed) in [
            (
                "SELECT b.g,COUNT(*) n FROM p JOIN b ON p.k=b.k GROUP BY b.g ORDER BY n LIMIT 7",
                false,
            ),
            (
                "SELECT b.g,COUNT(*)+1 n FROM p JOIN b ON p.k=b.k GROUP BY b.g ORDER BY n LIMIT 7",
                true,
            ),
            (
                "SELECT b.g,COUNT(*) n FROM p JOIN b ON p.k=b.k GROUP BY b.g HAVING COUNT(*)>0 ORDER BY n LIMIT 7",
                true,
            ),
            (
                "SELECT b.g,COUNT(*) n FROM p JOIN b ON p.k+0.0=b.k GROUP BY b.g ORDER BY n LIMIT 7",
                true,
            ),
        ] {
            let serial = Fixture::options(1, 1, false);
            let expected = execute(
                &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
                &serial,
            )
            .unwrap()
            .batch;
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Fixture::options(workers, batch_size, true);
                    let actual = execute(
                        &rel::prepare(sql, &fixture.tables, &options).unwrap(),
                        &options,
                    )
                    .unwrap();
                    assert_eq!(
                        actual.batch, expected,
                        "{sql}/{probe}/{build}/{workers}/{batch_size}"
                    );
                    if computed {
                        assert_eq!(actual.metrics.late_payload_sorts, 0);
                    }
                }
            }
        }
    }
}

#[test]
fn late_payload_finalizes_unselected_groups_and_preserves_offset_refusal() {
    let mut fixture = Fixture::new(false, false);
    fixture.write(
        "b",
        &["k", "g"],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["first", "unselected"])),
        ],
        false,
    );
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1, 1, 1, 1, 2, 2])),
            Arc::new(
                Decimal128Array::from(vec![
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    6 * 10_i128.pow(37),
                    6 * 10_i128.pow(37),
                ])
                .with_precision_and_scale(38, 0)
                .unwrap(),
            ),
        ],
        false,
    );
    let query =
        "SELECT b.g,b.k AS k,SUM(p.v) total FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.k ORDER BY k";
    for limit in [0, 1, 7] {
        let sql = format!("{query} LIMIT {limit}");
        for optimized in [false, true] {
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Fixture::options(workers, batch_size, optimized);
                    let prepared = rel::prepare(&sql, &fixture.tables, &options).unwrap();
                    assert_eq!(
                        execute(&prepared, &options).err().unwrap(),
                        "decimal precision 38 overflow"
                    );
                }
            }
        }
    }
    for offset in [0, 1, 70] {
        for optimized in [false, true] {
            let options = Fixture::options(1, 7, optimized);
            let prepared = rel::prepare(
                &format!("{query} LIMIT 7 OFFSET {offset}"),
                &fixture.tables,
                &options,
            )
            .unwrap();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "decimal precision 38 overflow"
            );
        }
    }
}

#[test]
fn late_payload_memory_denial_replays_and_cancellation_does_not_emit() {
    let fixture = Fixture::new(false, false);
    let sql = "SELECT b.g,COUNT(*) n FROM p JOIN b ON p.k=b.k GROUP BY b.g ORDER BY n LIMIT 1";
    let serial = Fixture::options(1, 1, false);
    let expected = execute(
        &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
        &serial,
    )
    .unwrap()
    .batch;
    let mut options = Fixture::options(1, 1, true);
    options.memory_bytes = 64 * 1024;
    let actual = execute(
        &rel::prepare(sql, &fixture.tables, &options).unwrap(),
        &options,
    )
    .unwrap();
    assert_eq!(actual.batch, expected);
    assert_eq!(actual.metrics.late_payload_sorts, 0);
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Fixture::options(workers, batch_size, true);
            let prepared = rel::prepare(sql, &fixture.tables, &options).unwrap();
            options.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "cancelled: query execution"
            );
        }
    }
}

#[test]
fn prepared_sparse_matching_builds_reach_owned_inner_semi_and_anti_scan_fusion() {
    let mut fixture = Fixture::new(false, false);
    let rows = 262_147;
    fixture.write(
        "b",
        &["k"],
        vec![Arc::new(Int64Array::from_iter(
            (0..rows).map(|row| (row % 17 != 0).then_some(row as i64 * 1024)),
        ))],
        false,
    );
    let pattern = [Some(2048), Some(1024), None, Some(-1), Some(2048), Some(0)];
    let probes = (0..rows + 4)
        .map(|row| pattern[row % pattern.len()])
        .collect::<Vec<_>>();
    fixture.write(
        "p",
        &["k"],
        vec![Arc::new(Int64Array::from(probes.clone()))],
        false,
    );
    for kind in ["INNER", "SEMI", "ANTI"] {
        let sql = format!("SELECT p.k FROM p {kind} JOIN b ON p.k=b.k");
        let mut serial = Fixture::options(1, 8192, false);
        serial.memory_bytes = 512 << 20;
        // Freeze orientation and plan, then exercise only execution dispatch.
        let plan = rel::prepare(&sql, &fixture.tables, &serial).unwrap();
        let expected = execute(&plan, &serial).unwrap().batch;
        let handwritten = probes
            .iter()
            .copied()
            .filter(|key| matches!(key, Some(1024 | 2048)) != (kind == "ANTI"))
            .collect::<Vec<_>>();
        assert_eq!(
            expected
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            handwritten
        );
        for workers in [1, 2, 4, 10] {
            for batch_size in [4093, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations: true,
                    cancellation: Default::default(),
                    ..serial.clone()
                };
                let result = execute(&plan, &options).unwrap();
                assert_eq!(result.batch, expected, "{kind}/{workers}/{batch_size}");
                assert_eq!(
                    result.metrics.build_prepare_index_tasks > 0,
                    workers > 1,
                    "population: {kind}/{workers}"
                );
                assert_eq!(
                    result.metrics.lane_fused_joins > 0,
                    workers > 1,
                    "owned fusion: {kind}/{workers}"
                );
                assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert!(result.metrics.worker_threads_peak <= workers);
                options.cancellation.cancel();
                assert_eq!(
                    execute(&plan, &options).err().unwrap(),
                    "cancelled: query execution"
                );
            }
        }
    }
}

#[test]
fn completed_dense_and_unique_builds_keep_sql_order_nulls_coercions_and_errors() {
    let mut fixture = Fixture::new(false, false);
    fixture.write(
        "p",
        &["k", "f"],
        vec![
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(1),
                None,
                Some(-1),
                Some(2),
                Some(0),
            ])),
            Arc::new(Float64Array::from(vec![1e308; 6])),
        ],
        false,
    );
    // Build keys: NULL every 17th row; ordered unique, dispersed duplicates
    // (8191 is odd, so duplicate rows are spread over every chunk/partition),
    // or a strided unique hash build. Production admits parallel population
    // from 262,144 build rows (the exact one-row boundary is unit-tested in
    // src/join_index/prepare.rs); here 262,147 rows is admitted and 262,143
    // rows stays serial even when otherwise admissible.
    let build = |fixture: &mut Fixture, rows: usize, stride: i64, duplicate: bool| {
        fixture.write(
            "b",
            &["k", "ordinal"],
            vec![
                Arc::new(Int64Array::from_iter((0..rows).map(|row| {
                    (row % 17 != 0).then_some(
                        (if duplicate {
                            row * 8191 % (rows / 3)
                        } else {
                            row
                        }) as i64
                            * stride,
                    )
                }))),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row as i64),
                )),
            ],
            false,
        );
    };
    let mut serial = Fixture::options(1, 8192, true);
    serial.memory_bytes = 512 << 20;
    for (rows, stride, duplicate) in [
        (262_147, 1, false),
        (262_147, 1, true),
        (262_147, 1024, false),
        (262_143, 1, true),
    ] {
        build(&mut fixture, rows, stride, duplicate);
        // The first predicate stays in the exact numeric layout; the second
        // checks the existing mixed INTEGER/REAL coercion layout and fallback.
        for (predicate, numeric) in [("p.k=b.k", true), ("CAST(p.k AS REAL)=b.k", false)] {
            if rows < 262_144 && !numeric {
                continue;
            }
            let sql = format!("SELECT p.k,b.ordinal FROM p LEFT JOIN b ON {predicate}");
            let prepared = rel::prepare(&sql, &fixture.tables, &serial).unwrap();
            let expected = execute(&prepared, &serial).unwrap().batch;
            // Batch size 1 runs on the tiny fixtures below; here 7 splits the
            // build into odd-sized batches and 8192 is the default. The
            // coercion layout is always serial and costs several times more
            // per batch, so its batch 1/7 cases run on the tiny fixtures only.
            let batch_sizes: &[usize] = if numeric { &[7, 8192] } else { &[8192] };
            for workers in [1, 2, 4, 10] {
                for &batch_size in batch_sizes {
                    let options = Options {
                        workers,
                        batch_size,
                        ..serial.clone()
                    };
                    let result = execute(&prepared, &options).unwrap();
                    assert_eq!(
                        result.batch, expected,
                        "{rows}/{stride}/{duplicate}/{predicate}/{workers}/{batch_size}"
                    );
                    assert_eq!(
                        result.metrics.build_prepare_index_tasks > 0,
                        rows >= 262_144 && numeric && workers > 1 && (stride != 1 || duplicate)
                    );
                    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                    assert!(result.metrics.worker_threads_peak <= workers);
                }
            }
        }
    }
    build(&mut fixture, 262_147, 1024, false);
    let sql = "SELECT p.bad,b.bad FROM (SELECT k,f*f AS bad FROM p) p LEFT JOIN (SELECT k,ordinal*10000000000000000000000000000000000000 AS bad FROM b) b ON p.k=b.k";
    let prepared = rel::prepare(sql, &fixture.tables, &serial).unwrap();
    for workers in [1, 2, 4, 10] {
        for batch_size in [7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..serial.clone()
            };
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "decimal precision 38 overflow"
            );
            let options = Options {
                cancellation: Default::default(),
                ..options
            };
            options.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "cancelled: query execution"
            );
        }
    }
    // A few hundred build rows: the same shapes at batch size 1 as well,
    // below the population threshold. Duplicates, NULL keys, misses, both
    // predicates, build-error precedence over the failing probe, cancellation.
    let tiny = 300;
    for (stride, duplicate) in [(1, false), (1, true), (1024, false)] {
        build(&mut fixture, tiny, stride, duplicate);
        for predicate in ["p.k=b.k", "CAST(p.k AS REAL)=b.k"] {
            let sql = format!("SELECT p.k,b.ordinal FROM p LEFT JOIN b ON {predicate}");
            let mut reference = Fixture::options(1, 1, true);
            reference.memory_bytes = serial.memory_bytes;
            let prepared = rel::prepare(&sql, &fixture.tables, &reference).unwrap();
            let expected = execute(&prepared, &reference).unwrap().batch;
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..reference.clone()
                    };
                    let result = execute(&prepared, &options).unwrap();
                    assert_eq!(
                        result.batch, expected,
                        "tiny/{stride}/{duplicate}/{predicate}"
                    );
                    assert_eq!(result.metrics.build_prepare_index_tasks, 0);
                    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                    assert!(result.metrics.worker_threads_peak <= workers);
                }
            }
        }
        let prepared = rel::prepare(sql, &fixture.tables, &serial).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..serial.clone()
                };
                assert_eq!(
                    execute(&prepared, &options).err().unwrap(),
                    "decimal precision 38 overflow"
                );
                let options = Options {
                    cancellation: Default::default(),
                    ..options
                };
                options.cancellation.cancel();
                assert_eq!(
                    execute(&prepared, &options).err().unwrap(),
                    "cancelled: query execution"
                );
            }
        }
    }
    // Empty and all-NULL builds retain the serial path and LEFT null extension.
    let mut serial = Fixture::options(1, 1, true);
    serial.memory_bytes = 512 << 20;
    for (empty_probe, empty_build, all_null) in [
        (false, true, false),
        (true, false, false),
        (false, false, true),
    ] {
        fixture.write(
            "b",
            &["k"],
            vec![Arc::new(Int64Array::from(vec![
                if all_null { None } else { Some(1) },
                None,
            ]))],
            empty_build,
        );
        fixture.write(
            "p",
            &["k"],
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
            empty_probe,
        );
        let prepared = rel::prepare(
            "SELECT p.k,b.k FROM p LEFT JOIN b ON p.k=b.k",
            &fixture.tables,
            &serial,
        )
        .unwrap();
        let expected = execute(&prepared, &serial).unwrap().batch;
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..serial.clone()
                };
                let result = execute(&prepared, &options).unwrap();
                assert_eq!(result.batch, expected);
                assert_eq!(result.metrics.build_prepare_index_tasks, 0);
            }
        }
    }
}

#[test]
fn completed_hash_build_keeps_duplicate_probe_order_across_workers_and_batches() {
    let mut fixture = Fixture::new(false, false);
    let rows = 262_147;
    let unique = rows / 3;
    fixture.write(
        "b",
        &["k", "ordinal"],
        vec![
            Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                (i % 17 != 0).then_some((i / 3 * 8191 % unique * 1024) as i64)
            }))),
            Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64))),
        ],
        false,
    );
    fixture.write(
        "p",
        &["k", "f"],
        vec![
            Arc::new(Int64Array::from(vec![
                Some(2048),
                Some(1024),
                None,
                Some(-1),
                Some(2048),
                Some(0),
            ])),
            Arc::new(Float64Array::from(vec![1e308; 6])),
        ],
        false,
    );
    // LEFT fixes build/probe orientation. Raw duplicate order must be the same
    // without a sort, including NULL keys, misses, and repeated probes.
    let sql = "SELECT p.k,b.ordinal FROM p LEFT JOIN b ON p.k=b.k";
    let mut serial = Fixture::options(1, 1, true);
    serial.memory_bytes = 512 << 20;
    let prepared = rel::prepare(sql, &fixture.tables, &serial).unwrap();
    let expected = execute(&prepared, &serial).unwrap().batch;
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..serial.clone()
            };
            let result = execute(&prepared, &options).unwrap();
            assert_eq!(result.batch, expected, "{workers}/{batch_size}");
            assert_eq!(result.metrics.build_prepare_index_tasks > 0, workers > 1);
            assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
            assert!(result.metrics.worker_threads_peak <= workers);
        }
    }
    // A failed build still wins over a failing probe; preparation must not
    // start consuming the probe, including when parallel preparation is on.
    let sql = "SELECT p.bad,b.bad FROM (SELECT k,f*f AS bad FROM p) p LEFT JOIN (SELECT k,ordinal*10000000000000000000000000000000000000 AS bad FROM b) b ON p.k=b.k";
    let prepared = rel::prepare(sql, &fixture.tables, &serial).unwrap();
    let error = execute(&prepared, &serial).err().unwrap();
    assert_eq!(error, "decimal precision 38 overflow");
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..serial.clone()
            };
            assert_eq!(execute(&prepared, &options).err().unwrap(), error);
        }
    }
}

#[test]
fn completed_build_dictionary_parallelism_keeps_raw_sql_order_across_batches() {
    let mut fixture = Fixture::new(false, false);
    let rows = 65_543;
    fixture.write(
        "b",
        &["k", "g", "h"],
        vec![
            Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64))),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                if i % 13 == 0 {
                    None
                } else if i % 2 == 0 {
                    Some("é\0雪")
                } else {
                    Some("")
                }
            }))),
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 17 != 0).then_some((i % 997) as i64)),
            )),
        ],
        false,
    );
    // Keep the physical build on the right. Only eight probes match; the rest
    // exercise the same dictionary without making a large aggregate output.
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from_iter_values((0..70_000).map(|i| {
                if i < 8 {
                    [3, 1, 3, 13, 17, 2, 65_542, 0][i]
                } else {
                    100_000 + i as i64
                }
            }))),
            Arc::new(Int64Array::from_iter(
                (0..70_000).map(|i| (i % 3 != 0).then_some(i as i64)),
            )),
        ],
        false,
    );
    let sql = "SELECT b.g,b.h,COUNT(*),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h";
    let serial = Fixture::options(1, 1, true);
    let expected = execute(
        &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
        &serial,
    )
    .unwrap()
    .batch;
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut options = Fixture::options(workers, batch_size, true);
            options.memory_bytes = 256 << 20;
            let result = execute(
                &rel::prepare(sql, &fixture.tables, &options).unwrap(),
                &options,
            )
            .unwrap();
            assert_eq!(result.batch, expected, "{workers}/{batch_size}");
            assert_eq!(result.metrics.build_group_id_aggregates, 1);
            assert_eq!(
                result.metrics.build_group_parallel_dictionaries,
                usize::from(workers > 1)
            );
            assert!(result.metrics.worker_threads_peak <= workers);
            assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
        }
    }
}

struct Fixture {
    directory: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new(empty_probe: bool, empty_build: bool) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-build-group-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let mut result = Self {
            directory,
            tables: HashMap::new(),
        };
        let probe: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(3),
                Some(1),
                Some(3),
                None,
                Some(2),
                Some(4),
                Some(8),
                Some(5),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(9),
                Some(7),
                Some(9),
                Some(7),
                None,
                Some(8),
                Some(3),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                "z", "a", "x", "a", "b", "c", "d", "e",
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(100),
                    Some(250),
                    Some(-100),
                    Some(1),
                    None,
                    Some(300),
                    Some(1),
                    None,
                ])
                .with_precision_and_scale(15, 2)
                .unwrap(),
            ),
        ];
        let build: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(3),
                Some(3),
                Some(2),
                Some(4),
                Some(5),
                None,
                Some(8),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("é\0b"),
                Some("é\0b"),
                None,
                Some("a"),
                None,
                Some("unmatched"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(20),
                Some(30),
                Some(10),
                None,
                Some(99),
                Some(30),
            ])),
            Arc::new(Float64Array::from(vec![1., 3., 3., 2., 4., 5., 0., 8.])),
        ];
        result.write("p", &["k", "v", "s", "d"], probe, empty_probe);
        result.write("b", &["k", "g", "h", "f"], build, empty_build);
        result.write(
            "excluded",
            &["k"],
            vec![Arc::new(Int64Array::from(vec![42]))],
            false,
        );
        result
    }
    fn write(&mut self, name: &str, names: &[&str], arrays: Vec<ArrayRef>, empty: bool) {
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .zip(&arrays)
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = self.directory.join(format!("{name}.parquet"));
        let properties = WriterProperties::builder()
            .set_max_row_group_size(if batch.num_rows() > 10000 { 4096 } else { 3 })
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&file).unwrap(), schema, Some(properties)).unwrap();
        writer
            .write(&if empty { batch.slice(0, 0) } else { batch })
            .unwrap();
        writer.close().unwrap();
        self.tables.insert(name.into(), file);
    }
    fn options(workers: usize, batch_size: usize, optimized: bool) -> Options {
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            memory_bytes: 64 << 20,
            ..Options::default()
        }
    }
    fn matrix(&self, sql: &str, admitted: bool) -> RecordBatch {
        let options = Self::options(1, 1, false);
        let prepared = rel::prepare(sql, &self.tables, &options).unwrap();
        let expected = execute(&prepared, &options).unwrap().batch;
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Self::options(workers, batch_size, true);
                let prepared = rel::prepare(sql, &self.tables, &options).unwrap();
                let actual = execute(&prepared, &options).unwrap();
                assert_eq!(actual.batch, expected, "{workers}/{batch_size}: {sql}");
                assert_eq!(
                    actual.metrics.build_group_id_aggregates,
                    usize::from(admitted),
                    "{sql}"
                );
                assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert!(actual.metrics.worker_threads_peak <= workers);
            }
        }
        expected
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn sorted_matrix(fixture: &Fixture, sql: &str, admitted: bool, fallback: bool) -> RecordBatch {
    let serial = Fixture::options(1, 1, false);
    let expected = execute(
        &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
        &serial,
    )
    .unwrap()
    .batch;
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Fixture::options(workers, batch_size, true);
            let result = execute(
                &rel::prepare(sql, &fixture.tables, &options).unwrap(),
                &options,
            )
            .unwrap();
            assert_eq!(result.batch, expected, "{workers}/{batch_size}: {sql}");
            assert_eq!(
                result.metrics.sorted_distinct_aggregates,
                usize::from(admitted),
                "{sql}"
            );
            assert_eq!(
                result.metrics.sorted_distinct_fallbacks,
                usize::from(fallback),
                "{sql}"
            );
            assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
            assert!(result.metrics.worker_threads_peak <= workers);
        }
    }
    expected
}

#[test]
fn sorted_build_counts_keep_extrema_skew_null_groups_and_supplier_mark_position() {
    let mut fixture = Fixture::new(false, false);
    let keys = [
        Some(3),
        Some(1),
        Some(3),
        None,
        Some(2),
        Some(4),
        Some(8),
        Some(5),
        Some(99),
        Some(99),
    ];
    let values = [
        Some(i64::MIN),
        Some(0),
        Some(i64::MAX),
        Some(7),
        None,
        Some(i64::MAX),
        Some(3),
        None,
        Some(55),
        Some(55),
    ];
    // Duplicate join keys expand some probes; unmatched probes leave the total
    // below the physical cap. Repeat across both row-group and batch boundaries.
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from_iter(keys.into_iter().cycle().take(10_250))),
            Arc::new(Int64Array::from_iter(
                values.into_iter().cycle().take(10_250),
            )),
        ],
        false,
    );
    for sql in [
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k WHERE p.k NOT IN (SELECT k FROM excluded) GROUP BY b.g,b.h",
    ] {
        let batch = sorted_matrix(&fixture, sql, true, false);
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            groups.iter().collect::<Vec<_>>(),
            [Some("é\0b"), Some("a"), None, None]
        );
        assert_eq!(counts.values().as_ref(), &[2, 2, 1, 0]);
    }
    fixture.write(
        "excluded",
        &["k"],
        vec![Arc::new(Int64Array::from(vec![None, Some(42)]))],
        false,
    );
    assert_eq!(sorted_matrix(&fixture,
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k WHERE p.k NOT IN (SELECT k FROM excluded) GROUP BY b.g,b.h",
        true, false).num_rows(), 0);
}

#[test]
fn sorted_counts_decline_mixed_types_and_fanout_replays_without_partial_output() {
    let fixture = Fixture::new(false, false);
    // Nine join rows exceed the eight-row physical cap. The fresh hash replay
    // retains build-group interning and its original first-seen group order.
    let batch = sorted_matrix(
        &fixture,
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        false,
        true,
    );
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values().as_ref(), &[1, 2, 1, 0]);
    // A direct INTEGER column can carry precision-38 intermediates from an
    // upstream projection. It must replay, never truncate into the packed pair.
    sorted_matrix(
        &fixture,
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM (SELECT k,v+9223372036854775807 AS v FROM p) p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        false,
        true,
    );
    for sql in [
        "SELECT b.g,COUNT(DISTINCT p.v),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
        "SELECT b.g,COUNT(DISTINCT p.d) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
        "SELECT b.g,COUNT(DISTINCT CAST(p.v AS REAL)) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
        "SELECT b.g,COUNT(DISTINCT (p.v+1)) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
        "SELECT b.g,COUNT(DISTINCT p.v) FROM p LEFT JOIN b ON p.k=b.k GROUP BY b.g",
    ] {
        sorted_matrix(&fixture, sql, false, false);
    }
}

#[test]
fn sorted_counts_empty_sides_and_all_null_values() {
    for (probe, build) in [(true, false), (false, true), (true, true)] {
        let fixture = Fixture::new(probe, build);
        assert_eq!(
            sorted_matrix(
                &fixture,
                "SELECT b.g,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
                !(probe && build),
                false
            )
            .num_rows(),
            0
        );
    }
    let mut fixture = Fixture::new(false, false);
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(5), None])),
            Arc::new(Int64Array::from(vec![None; 4])),
        ],
        false,
    );
    let batch = sorted_matrix(
        &fixture,
        "SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        true,
        false,
    );
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values().as_ref(), &[0, 0, 0]);
}

#[test]
fn sorted_counts_preserve_mixed_aggregate_errors_and_cancel_without_output() {
    let fixture = Fixture::new(false, false);
    let sql = "SELECT b.g,COUNT(DISTINCT p.v),SUM(p.v*99999999999999999999999999999999999999),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g";
    let serial = Fixture::options(1, 1, false);
    let error = execute(
        &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
        &serial,
    )
    .err()
    .unwrap();
    for workers in [1, 2, 4, 10] {
        let options = Fixture::options(workers, 7, true);
        assert_eq!(
            execute(
                &rel::prepare(sql, &fixture.tables, &options).unwrap(),
                &options
            )
            .err()
            .unwrap(),
            error
        );
        let prepared = rel::prepare(
            "SELECT b.g,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
            &fixture.tables,
            &options,
        )
        .unwrap();
        options.cancellation.cancel();
        assert!(
            execute(&prepared, &options)
                .err()
                .unwrap()
                .starts_with("cancelled:")
        );
    }
}

#[test]
fn build_ids_preserve_null_tuples_duplicates_multiplicity_and_encounters() {
    let fixture = Fixture::new(false, false);
    let batch = fixture.matrix("SELECT b.g,b.h,COUNT(*),COUNT(DISTINCT p.v),SUM(p.v),SUM(p.d) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",true);
    let strings = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![Some("é\0b"), Some("a"), None, None]
    );
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values().as_ref(), &[4, 2, 2, 1]);
    let distinct = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(distinct.values().as_ref(), &[1, 2, 1, 0]);
    let sums = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        sums.iter().collect::<Vec<_>>(),
        vec![Some(36), Some(15), Some(3), None]
    );
}
#[test]
fn build_ids_pass_through_null_aware_membership_and_bounded_decimal_inputs() {
    let fixture = Fixture::new(false, false);
    fixture.matrix("SELECT b.g,b.h,COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k WHERE p.v NOT IN (SELECT k FROM excluded) GROUP BY b.g,b.h",true);
    fixture.matrix(
        "SELECT b.g,b.h,SUM(p.d*(1-p.d)) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        true,
    );
}

#[test]
fn build_ids_numeric_distinct_counts_preserve_nulls_and_first_encounters() {
    let fixture = Fixture::new(false, false);
    let batch = fixture.matrix(
        "SELECT b.g,b.h,COUNT(DISTINCT p.v),COUNT(DISTINCT p.d) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
        true,
    );
    let groups = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        groups.iter().collect::<Vec<_>>(),
        vec![Some("é\0b"), Some("a"), None, None]
    );
    let integers = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let decimals = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(integers.values().as_ref(), &[1, 2, 1, 0]);
    assert_eq!(decimals.values().as_ref(), &[2, 2, 1, 0]);
}

#[test]
fn build_ids_empty_sides() {
    for (probe, build) in [(true, false), (false, true), (true, true)] {
        let fixture = Fixture::new(probe, build);
        assert_eq!(
            fixture
                .matrix(
                    "SELECT b.g,b.h,COUNT(*),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.h",
                    true
                )
                .num_rows(),
            0
        );
    }
}
#[test]
fn build_ids_decline_cross_side_left_coerced_and_float_keys() {
    let fixture = Fixture::new(false, false);
    for sql in [
        "SELECT b.g,p.s,COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g,p.s",
        "SELECT b.g,COUNT(*) FROM p LEFT JOIN b ON p.k=b.k GROUP BY b.g",
        "SELECT b.g,CAST(b.h AS TEXT),COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g,CAST(b.h AS TEXT)",
        "SELECT b.g,b.f,COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g,b.f",
        "SELECT b.g,SUM(p.v*p.v*p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
    ] {
        fixture.matrix(sql, false);
    }
}
#[test]
fn build_ids_keep_scan_only_and_numeric_only_aggregates_on_existing_paths() {
    let fixture = Fixture::new(false, false);
    // Scan-only text grouping is the shape of the unchanged q01 path; even
    // text keys cannot qualify without an INNER join's build-side mapping.
    fixture.matrix("SELECT s,COUNT(*),SUM(v),AVG(d) FROM p GROUP BY s", false);
    // A build-side numeric tuple already uses numeric grouping, so interning
    // cannot replace any string-key work and must still decline.
    fixture.matrix(
        "SELECT b.h,COUNT(*),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.h",
        false,
    );
    // A residual join also retains its original plan.
    fixture.matrix(
        "SELECT b.g,COUNT(*) FROM p JOIN b ON p.k=b.k AND p.v<b.h GROUP BY b.g",
        false,
    );
}
#[test]
fn build_ids_resolve_internally_retained_unselected_group_columns() {
    let fixture = Fixture::new(false, false);
    // The binder retains the group in the inner aggregate output and projects
    // it away afterward, so the dictionary can resolve it at that boundary.
    let batch = fixture.matrix(
        "SELECT COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
        true,
    );
    let counts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values().as_ref(), &[4, 2, 3]);
}
#[test]
fn build_ids_keep_error_precedence_and_cancellation() {
    let fixture = Fixture::new(false, false);
    let sql = "SELECT b.g,SUM(p.v*99999999999999999999999999999999999999),SUM(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g";
    for sql in [
        sql,
        "SELECT b.g,SUM(99999999999999999999999999999999999999),MIN(p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
    ] {
        let serial = Fixture::options(1, 1, false);
        let error = execute(
            &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
            &serial,
        )
        .err()
        .unwrap();
        for workers in [1, 2, 4, 10] {
            for batch in [1, 7, 8192] {
                let options = Fixture::options(workers, batch, true);
                assert_eq!(
                    execute(
                        &rel::prepare(sql, &fixture.tables, &options).unwrap(),
                        &options
                    )
                    .err()
                    .unwrap(),
                    error
                );
            }
        }
    }
    for workers in [1, 2, 4, 10] {
        let options = Fixture::options(workers, 7, true);
        let prepared = rel::prepare(
            "SELECT b.g,COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g",
            &fixture.tables,
            &options,
        )
        .unwrap();
        options.cancellation.cancel();
        assert!(
            execute(&prepared, &options)
                .err()
                .unwrap()
                .contains("cancel")
        );
    }
}
#[test]
fn build_ids_optional_plan_reservation_declines_cleanly() {
    let fixture = Fixture::new(false, false);
    let sql = "SELECT b.g,COUNT(*) FROM p JOIN b ON p.k=b.k GROUP BY b.g";
    let mut options = Fixture::options(1, 1, true);
    options.memory_bytes = 64 * 1024;
    let result = execute(
        &rel::prepare(sql, &fixture.tables, &options).unwrap(),
        &options,
    )
    .unwrap();
    assert_eq!(result.metrics.build_group_id_aggregates, 0);
    let expected = fixture.matrix(sql, true);
    assert_eq!(result.batch, expected);
}

#[test]
fn build_ids_parallel_probe_delivery_matches_serial_first_encounters() {
    let mut fixture = Fixture::new(false, false);
    fixture.write(
        "p",
        &["k", "v"],
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..70_000).map(|i| (i * 17 % 1500) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..70_000).map(|i| (i % 37) as i64),
            )),
        ],
        false,
    );
    fixture.write(
        "b",
        &["k", "g"],
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..3000).map(|i| (i % 1500) as i64),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..3000).map(|i| format!("group-{}", i % 1500 % 73)),
            )),
        ],
        false,
    );
    let sql = "SELECT b.g,SUM(p.v),COUNT(DISTINCT p.v) FROM p JOIN b ON p.k=b.k GROUP BY b.g";
    let serial = Fixture::options(1, 8192, false);
    let expected = execute(
        &rel::prepare(sql, &fixture.tables, &serial).unwrap(),
        &serial,
    )
    .unwrap()
    .batch;
    for workers in [1, 2, 4, 10] {
        let options = Fixture::options(workers, 8192, true);
        let actual = execute(
            &rel::prepare(sql, &fixture.tables, &options).unwrap(),
            &options,
        )
        .unwrap();
        assert_eq!(actual.batch, expected, "{workers}");
        assert_eq!(actual.metrics.build_group_id_aggregates, 1);
        assert_eq!(actual.metrics.build_group_id_rows, 3000);
        assert_eq!(actual.metrics.build_group_id_groups, 73);
        if workers == 10 {
            assert!(actual.metrics.parallel_join_tasks > 0);
        }
    }
}

#[test]
fn build_ids_multiple_join_build_with_decimal_and_text_tuple() {
    let mut fixture = Fixture::new(false, false);
    // An explicitly retained plan has a joined build side. Both text and
    // decimal key columns originate there; the scaled input bound is < 10^38.
    fixture.write(
        "customers",
        &["k", "g", "amount", "nk"],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 8])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(123),
                    None,
                    Some(-456),
                    Some(0),
                    Some(98),
                    Some(123),
                ])
                .with_precision_and_scale(15, 2)
                .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![0, 1, 0, 1, 0, 1])),
        ],
        false,
    );
    fixture.write(
        "nations",
        &["k", "name"],
        vec![
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![Some("east"), None])),
        ],
        false,
    );
    fixture.matrix("SELECT customers.g,customers.amount,nations.name,SUM(p.d*(1-p.d)) FROM p JOIN (customers JOIN nations ON customers.nk=nations.k) ON p.k=customers.k GROUP BY customers.g,customers.amount,nations.name",true);
}
