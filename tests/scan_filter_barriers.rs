use arrow::{
    array::{ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
const OVERFLOW: &str = "decimal precision 38 overflow";
const FILTER: &str = "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE a*10000000000000000000000000000000000000 > 0) p JOIN b ON p.k=b.k";
const PROJECT: &str = "SELECT p.k,p.payload,p.v FROM (SELECT k,payload,a*10000000000000000000000000000000000000 AS v FROM r) p JOIN b ON p.k=b.k";
const RESIDUAL: &str = "SELECT p.k,p.payload FROM (SELECT r.k,r.payload FROM r JOIN c ON r.k=c.k AND r.a*10000000000000000000000000000000000000 > c.a) p JOIN b ON p.k=b.k";

struct Tables(HashMap<String, PathBuf>);
impl Tables {
    fn new() -> Self {
        Self::with_tail(1)
    }

    fn with_tail(tail: i64) -> Self {
        let mut tables = Self(HashMap::new());
        let keys = || Int64Array::from_iter_values(std::iter::once(-1).chain(1..512));
        tables.write(
            "r",
            vec![
                ("k", Arc::new(keys())),
                (
                    "a",
                    Arc::new(Int64Array::from_iter_values(
                        std::iter::once(10).chain(std::iter::repeat_n(tail, 511)),
                    )),
                ),
                ("payload", Arc::new(StringArray::from(vec!["p"; 512]))),
            ],
        );
        tables.write("b", vec![("k", Arc::new(Int64Array::from(vec![1])))]);
        tables.write(
            "c",
            vec![
                ("k", Arc::new(keys())),
                ("a", Arc::new(Int64Array::from(vec![0; 512]))),
                ("f", Arc::new(Float64Array::from(vec![1e308; 512]))),
            ],
        );
        tables
    }

    fn write(&mut self, name: &str, columns: Vec<(&str, ArrayRef)>) {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, column)| {
                    Field::new(*name, column.data_type().clone(), column.null_count() > 0)
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(|(_, column)| column).collect(),
        )
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "assaydb-scan-filter-barrier-{}-{}-{name}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        self.0.insert(name.into(), path);
    }
}
impl Drop for Tables {
    fn drop(&mut self) {
        for path in self.0.values() {
            std::fs::remove_file(path).unwrap();
        }
    }
}

fn assert_error_matrix(sql: &str, expected: &str) {
    let tables = Tables::new();
    let mut failures = Vec::new();
    for relational_optimizations in [false, true] {
        let base = Options {
            relational_optimizations,
            ..Options::default()
        };
        // Retain each plan across the worker/batch matrix; no synthetic estimates.
        let prepared = prepare(sql, &tables.0, &base).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..base.clone()
                };
                let actual = execute(&prepared, &options).err();
                if actual.as_deref() != Some(expected) {
                    failures.push(format!(
                        "optimized={relational_optimizations}, workers={workers}, batch={batch_size}: {actual:?}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "expected {expected}: {failures:#?}");
}

#[test]
fn literal_date_casts_keep_scan_filters_and_join_paths() {
    let mut tables = Tables::new();
    tables.write(
        "dates",
        vec![
            ("k", Arc::new(Int64Array::from_iter_values(0..512))),
            (
                "d",
                // A narrow range within decades of dates satisfies the real
                // reader-filter cost gate; retain NULLs in the source too.
                Arc::new(Date32Array::from_iter(
                    (0..512).map(|i| (i % 5 != 4).then_some(i * 31)),
                )),
            ),
            ("payload", Arc::new(StringArray::from(vec!["p"; 512]))),
        ],
    );
    let query = |lo: &str, hi: &str| {
        format!(
            "SELECT p.k,p.payload FROM (SELECT k,payload FROM dates WHERE d>={lo} AND d<{hi}) p JOIN b ON p.k=b.k"
        )
    };
    let base = Options {
        parquet_cache_bytes: Some(16 * 1024 * 1024),
        ..Options::default()
    };
    let literal = prepare(
        &query("DATE '1970-02-01'", "DATE '1970-04-01'"),
        &tables.0,
        &base,
    )
    .unwrap();
    for (lo, hi) in [
        ("CAST('1970-02-01' AS DATE)", "CAST('1970-04-01' AS DATE)"),
        ("'1970-02-01'::DATE", "'1970-04-01'::DATE"),
        ("CAST(' 1970-02-01 ' AS DATE)", "' 1970-04-01 '::DATE"),
    ] {
        let sql = query(lo, hi);
        let prepared = prepare(&sql, &tables.0, &base).unwrap();
        // DATE constants must reach the optimizer in the same form as typed
        // literals, including all relational filters and join decisions.
        assert_eq!(
            serde_json::to_value(&prepared.relational).unwrap(),
            serde_json::to_value(&literal.relational).unwrap(),
            "{sql}"
        );
        assert_eq!(
            serde_json::to_value(&prepared.plan).unwrap(),
            serde_json::to_value(&literal.plan).unwrap(),
            "{sql}"
        );
        for workers in [1, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..base.clone()
                };
                let expected = execute(&literal, &options).unwrap();
                let actual = execute(&prepared, &options).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}");
                assert_eq!(actual.batch.num_rows(), 1);
                assert_eq!(
                    actual.batch.column(0).as_ref(),
                    &Int64Array::from(vec![1]) as &dyn arrow::array::Array
                );
                assert_eq!(
                    actual.batch.column(1).as_ref(),
                    &StringArray::from(vec!["p"]) as &dyn arrow::array::Array
                );
                assert_eq!(
                    actual.metrics.static_scan_filters,
                    expected.metrics.static_scan_filters
                );
                assert!(
                    actual.metrics.static_scan_filters > 0,
                    "{sql}; workers={workers}, batch={batch_size}"
                );
                if workers > 1 {
                    assert!(
                        actual.metrics.lane_fused_joins > 0,
                        "{sql}; workers={workers}, batch={batch_size}"
                    );
                }
                assert_eq!(
                    actual.metrics.dynamic_scan_filters,
                    expected.metrics.dynamic_scan_filters
                );
                assert_eq!(
                    actual.metrics.lane_fused_joins,
                    expected.metrics.lane_fused_joins
                );
            }
        }
    }
}

#[test]
fn membership_does_not_hide_fallible_filter() {
    assert_error_matrix(FILTER, OVERFLOW);
}

#[test]
fn membership_does_not_hide_fallible_projection() {
    assert_error_matrix(PROJECT, OVERFLOW);
}

#[test]
fn membership_does_not_hide_fallible_residual() {
    assert_error_matrix(RESIDUAL, OVERFLOW);
}

#[test]
fn build_error_still_precedes_fallible_probe_predicate() {
    assert_error_matrix(
        "SELECT p.k,p.payload,b.v FROM (SELECT k,a,payload FROM r WHERE a*10000000000000000000000000000000000000 > 0) p JOIN (SELECT k,f*f AS v FROM c WHERE k=1) b ON p.k=b.k",
        "floating arithmetic overflow",
    );
}

#[test]
fn empty_membership_does_not_hide_probe_error() {
    assert_error_matrix(
        "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE a*10000000000000000000000000000000000000 > 0) p JOIN (SELECT k FROM b WHERE k<0) b ON p.k=b.k",
        OVERFLOW,
    );
}

#[test]
fn total_predicates_keep_exact_pair_and_bitmap_filters() {
    let tables = Tables::new();
    for sql in [
        "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE a>0) p JOIN b ON p.k=b.k",
        "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE a>0) p JOIN b ON p.k=b.k AND p.a=b.k",
        "SELECT p.k,p.payload FROM (SELECT r.k,r.payload FROM r JOIN c ON r.k=c.k AND r.a>c.a) p JOIN b ON p.k=b.k",
    ] {
        let off = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let serial = execute(&prepare(sql, &tables.0, &off).unwrap(), &off).unwrap();
        assert_eq!(serial.batch.num_rows(), 1);
        assert_eq!(
            serial.batch.column(0).as_ref(),
            &Int64Array::from(vec![1]) as &dyn arrow::array::Array
        );
        assert_eq!(
            serial.batch.column(1).as_ref(),
            &StringArray::from(vec!["p"]) as &dyn arrow::array::Array
        );
        for parquet_cache_bytes in [None, Some(0), Some(16 * 1024 * 1024)] {
            let base = Options {
                parquet_cache_bytes,
                ..Options::default()
            };
            let prepared = prepare(sql, &tables.0, &base).unwrap();
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        cancellation: Default::default(),
                        ..base.clone()
                    };
                    let actual = execute(&prepared, &options).unwrap();
                    assert_eq!(actual.batch, serial.batch, "{sql}");
                    assert!(actual.metrics.dynamic_scan_filters > 0, "{sql}");
                    assert_eq!(actual.batch.schema().field(0).data_type(), &DataType::Int64);
                    options.cancellation.cancel();
                    assert_eq!(
                        execute(&prepared, &options).err(),
                        Some("cancelled: query execution".into())
                    );
                }
            }
        }
    }
}

#[test]
fn decimal_comparisons_with_safe_constant_bounds_keep_membership() {
    let mut tables = Tables::new();
    let keys = std::iter::once(Some(-1))
        .chain((1..512).map(Some))
        .chain([None, Some(1)])
        .collect::<Vec<_>>();
    let rows = keys.len();
    let quantities = (0..rows)
        .map(|i| if i == 7 { None } else { Some(100) })
        .collect::<Vec<_>>();
    tables.write(
        "d",
        vec![
            ("k", Arc::new(Int64Array::from(keys))),
            (
                "q",
                Arc::new(
                    Decimal128Array::from(quantities)
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
            ),
            ("payload", Arc::new(StringArray::from(vec!["p"; rows]))),
        ],
    );
    tables.write(
        "keys",
        vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
        )],
    );
    let sql = "SELECT p.k,p.payload FROM (SELECT k,q,payload FROM d WHERE
        (q >= 1 AND q <= 1 + 10 AND payload IN ('p','x')) OR
        (q >= 10 AND q <= 10 + 10 AND payload = 'y') OR
        (q >= 20 AND q <= 20 + 10 AND payload = 'z')) p JOIN keys b ON p.k=b.k";
    let off = Options {
        relational_optimizations: false,
        ..Options::default()
    };
    let serial = execute(&prepare(sql, &tables.0, &off).unwrap(), &off).unwrap();
    assert_eq!(serial.batch.num_rows(), 4);
    assert_eq!(
        serial.batch.column(0).as_ref(),
        &Int64Array::from(vec![1; 4]) as &dyn arrow::array::Array
    );
    for parquet_cache_bytes in [None, Some(0), Some(16 * 1024 * 1024)] {
        let base = Options {
            parquet_cache_bytes,
            ..Options::default()
        };
        let prepared = prepare(sql, &tables.0, &base).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    cancellation: Default::default(),
                    ..base.clone()
                };
                let actual = execute(&prepared, &options).unwrap();
                assert_eq!(actual.batch, serial.batch);
                assert!(actual.metrics.dynamic_scan_filters > 0);
                options.cancellation.cancel();
                assert_eq!(
                    execute(&prepared, &options).err(),
                    Some("cancelled: query execution".into())
                );
            }
        }
    }
}

#[test]
fn overflowing_constant_intermediate_remains_a_membership_barrier() {
    assert_error_matrix(
        "SELECT p.k,p.payload FROM (SELECT k,payload FROM r WHERE a <
        (99999999999999999999999999999999999999 + 1) - 1) p JOIN b ON p.k=b.k",
        OVERFLOW,
    );
}

#[test]
fn decimal_rescaling_overflow_remains_a_membership_barrier() {
    let mut tables = Tables::new();
    tables.write(
        "d",
        vec![
            (
                "k",
                Arc::new(Int64Array::from_iter_values(
                    std::iter::once(-1).chain(1..512),
                )),
            ),
            (
                "q",
                Arc::new(
                    Decimal128Array::from_iter_values(
                        std::iter::once(10_i128.pow(37)).chain(std::iter::repeat_n(100, 511)),
                    )
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
            ),
            ("payload", Arc::new(StringArray::from(vec!["p"; 512]))),
        ],
    );
    for sql in [
        "SELECT p.k,p.payload FROM (SELECT k,payload FROM d WHERE q < 1.001) p JOIN b ON p.k=b.k",
        "SELECT p.k,p.payload FROM (SELECT k,payload FROM d WHERE q < 1000000000000000000000000000000000000) p JOIN b ON p.k=b.k",
    ] {
        for relational_optimizations in [false, true] {
            let base = Options {
                relational_optimizations,
                ..Options::default()
            };
            let prepared = prepare(sql, &tables.0, &base).unwrap();
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..base.clone()
                    };
                    assert_eq!(
                        execute(&prepared, &options).err().as_deref(),
                        Some(OVERFLOW)
                    );
                }
            }
        }
    }
}

#[test]
fn chained_runtime_filters_feed_fused_scan_lanes() {
    let tables = Tables::new();
    // Keep the post-join quarter-row estimate above four so the one-key outer
    // filter qualifies, while the 25-key inner filter stays below 1/16 of r.
    let sql = "SELECT p.k,p.payload FROM (SELECT r.k,r.payload FROM r JOIN (SELECT k FROM c WHERE k<25) c ON r.k=c.k WHERE r.a>0) p JOIN b ON p.k=b.k";
    let base = Options::default();
    let prepared = prepare(sql, &tables.0, &base).unwrap();
    let serial = execute(
        &prepared,
        &Options {
            workers: 1,
            relational_optimizations: false,
            ..base.clone()
        },
    )
    .unwrap();
    assert_eq!(serial.batch.num_rows(), 1);
    for workers in [2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                parquet_cache_bytes: Some(16 << 20),
                ..base.clone()
            };
            let actual = execute(&prepared, &options).unwrap();
            assert_eq!(actual.batch, serial.batch);
            // The outer exact membership and inner bitmap both reach the scan;
            // the two unique joins must consume those filtered batches in lanes.
            assert!(actual.metrics.dynamic_scan_filters >= 2);
            assert!(actual.metrics.bitmap_scan_filters > 0);
            assert!(actual.metrics.lane_fused_joins >= 2);
        }
    }
}

#[derive(Clone, Copy)]
enum FilterMetric {
    None,
    Dynamic,
    Demand,
    Reverse,
}

// Handwritten outcomes are the oracle; serial/off equality additionally checks
// schema, row order and execution independence throughout the complete matrix.
fn check_membership_matrix(
    tables: &Tables,
    sql: &str,
    expected: Result<Vec<ArrayRef>, &str>,
    metric: FilterMetric,
) {
    let mut serial = None;
    for relational_optimizations in [false, true] {
        let base = Options {
            relational_optimizations,
            ..Options::default()
        };
        let prepared = prepare(sql, &tables.0, &base).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    cancellation: Default::default(),
                    ..base.clone()
                };
                let result = execute(&prepared, &options);
                let context = format!(
                    "{sql}: optimized={relational_optimizations}, workers={workers}, batch={batch_size}"
                );
                match &expected {
                    Err(error) => assert_eq!(result.err().as_deref(), Some(*error), "{context}"),
                    Ok(columns) => {
                        let result = result.unwrap_or_else(|e| panic!("{context}: {e}"));
                        assert_eq!(result.batch.columns(), columns, "{context}");
                        if let Some(serial) = &serial {
                            assert_eq!(&result.batch, serial, "{context}");
                        } else {
                            serial = Some(result.batch.clone());
                        }
                        if relational_optimizations {
                            match metric {
                                FilterMetric::None => {}
                                FilterMetric::Dynamic => {
                                    assert!(result.metrics.dynamic_scan_filters > 0, "{context}");
                                }
                                FilterMetric::Demand => {
                                    assert!(
                                        result.metrics.demand_filtered_aggregates > 0,
                                        "{context}"
                                    );
                                }
                                FilterMetric::Reverse => {
                                    assert!(
                                        result.metrics.reverse_existence_joins > 0,
                                        "{context}"
                                    );
                                }
                            }
                        }
                    }
                }
                options.cancellation.cancel();
                assert_eq!(
                    execute(&prepared, &options).err().as_deref(),
                    Some("cancelled: query execution"),
                    "{context}"
                );
            }
        }
    }
}

fn key_payload(keys: Vec<i64>) -> Vec<ArrayRef> {
    let rows = keys.len();
    vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(StringArray::from(vec!["p"; rows])),
    ]
}

#[test]
fn positive_in_rewrite_observes_filter_residual_and_join_key_barriers() {
    let tables = Tables::new();
    // The first two queries are the review's exact positive-IN reproducers.
    // Guarded AND (Helen, October 17, 2026; SEMANTICS-LOG "Guarded AND/OR
    // planner rules"): a conjunct of the same WHERE that is FALSE decides the
    // row, so the overflow on k=-1 (not in b) is no longer raised; DuckDB
    // 1.5.6 and PostgreSQL 17 return these rows. Old expectation: OVERFLOW.
    check_membership_matrix(
        &tables,
        "SELECT r.k,r.payload FROM r WHERE r.a*10000000000000000000000000000000000000 > 0 AND r.k IN (SELECT k FROM b)",
        Ok(key_payload(vec![1])),
        FilterMetric::None,
    );
    check_membership_matrix(
        &tables,
        "SELECT r.k,r.payload FROM r WHERE r.a*10000000000000000000000000000000000000 > 0 AND r.k IN (SELECT k FROM b WHERE k<0)",
        Ok(key_payload(vec![])),
        FilterMetric::None,
    );
    // An inner join's ON conjuncts and WHERE form one conjunction, so the IN
    // conjunct guards the ON residual too (old expectation: OVERFLOW; DuckDB
    // and PostgreSQL return k=1).
    check_membership_matrix(
        &tables,
        "SELECT r.k,r.payload FROM r JOIN c ON r.k=c.k AND r.a*10000000000000000000000000000000000000 > c.a WHERE r.k IN (SELECT k FROM b)",
        Ok(key_payload(vec![1])),
        FilterMetric::None,
    );
    // A join key that may fail is computed for every row passing the total
    // conditions on its own table; an IN subquery is not one of them, so
    // these errors stay required (DuckDB and PostgreSQL return no rows).
    for sql in [
        "SELECT r.k,r.payload FROM r JOIN c ON r.a*10000000000000000000000000000000000000=c.k WHERE r.k IN (SELECT k FROM b)",
        "SELECT r.k,r.payload FROM c JOIN r ON c.k=r.a*10000000000000000000000000000000000000 WHERE r.k IN (SELECT k FROM b)",
        // Unguarded: k=-1 is a member of c, so its overflow is needed.
        "SELECT r.k,r.payload FROM r WHERE r.a*10000000000000000000000000000000000000 > 0 AND r.k IN (SELECT k FROM c)",
        "SELECT r.k,r.payload FROM r WHERE r.k IN (SELECT k FROM b) OR r.a*10000000000000000000000000000000000000 > 0",
    ] {
        check_membership_matrix(&tables, sql, Err(OVERFLOW), FilterMetric::None);
    }
    for sql in [
        "SELECT r.k,r.payload FROM r WHERE r.a>0 AND r.k IN (SELECT k FROM b)",
        "SELECT r.k,r.payload FROM r JOIN c ON r.k=c.k AND r.a>c.a WHERE r.k IN (SELECT k FROM b)",
        "SELECT r.k,r.payload FROM r JOIN c ON r.k+0=c.k WHERE r.k IN (SELECT k FROM b)",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(key_payload(vec![1])),
            FilterMetric::Dynamic,
        );
    }
}

#[test]
fn aggregate_demand_observes_having_and_output_barriers() {
    let tables = Tables::with_tail(0);
    for sql in [
        "SELECT b.k,g.a FROM b LEFT JOIN (SELECT k,MIN(a) AS a FROM r GROUP BY k HAVING MIN(a)*10000000000000000000000000000000000000 >= 0) g ON b.k=g.k",
        "SELECT b.k,g.v FROM b LEFT JOIN (SELECT k,MIN(a)*10000000000000000000000000000000000000 AS v FROM r GROUP BY k) g ON b.k=g.k",
        // Empty demand must not erase an error in an undemanded group either.
        "SELECT b.k,g.a FROM (SELECT k FROM b WHERE k<0) b LEFT JOIN (SELECT k,MIN(a) AS a FROM r GROUP BY k HAVING MIN(a)*10000000000000000000000000000000000000 >= 0) g ON b.k=g.k",
    ] {
        check_membership_matrix(&tables, sql, Err(OVERFLOW), FilterMetric::None);
    }
    for sql in [
        "SELECT b.k,g.a FROM b LEFT JOIN (SELECT k,MIN(a) AS a FROM r GROUP BY k HAVING MIN(a)>=0) g ON b.k=g.k",
        "SELECT b.k,g.v FROM b LEFT JOIN (SELECT k,MIN(a)*2 AS v FROM r GROUP BY k) g ON b.k=g.k",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![0])),
            ]),
            FilterMetric::Demand,
        );
    }
}

#[test]
fn moved_computed_keys_do_not_create_phantom_errors() {
    let mut tables = Tables::with_tail(0);
    tables.write("z", vec![("k", Arc::new(Int64Array::from(vec![0])))]);
    for sql in [
        // The exact installer's review reproducer: row -1 must never evaluate a*10^37.
        "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE k>0) p JOIN z ON p.a*10000000000000000000000000000000000000=z.k",
        "SELECT k,payload FROM (SELECT k,a,payload FROM r WHERE k>0) p WHERE a*10000000000000000000000000000000000000 IN (SELECT k FROM z)",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(key_payload((1..512).collect())),
            FilterMetric::None,
        );
    }
    for sql in [
        "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE k>0) p JOIN b ON p.k+0=b.k",
        "SELECT k,payload FROM r WHERE k>0 AND k+0 IN (SELECT k FROM b)",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(key_payload(vec![1])),
            FilterMetric::Dynamic,
        );
    }
}

#[test]
fn shared_membership_barrier_preserves_nulls_duplicates_and_empty_inputs() {
    let mut tables = Tables::with_tail(0);
    tables.write(
        "nullable",
        vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
        )],
    );
    tables.write(
        "empty",
        vec![("k", Arc::new(Int64Array::from(Vec::<i64>::new())))],
    );
    for (sql, expected, metric) in [
        (
            "SELECT k,payload FROM r WHERE a>=0 AND k IN (SELECT k FROM nullable)",
            key_payload(vec![1]),
            FilterMetric::Dynamic,
        ),
        (
            "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE k>0) p JOIN nullable b ON p.k+0=b.k",
            key_payload(vec![1, 1]),
            FilterMetric::Dynamic,
        ),
        (
            "SELECT k,payload FROM r WHERE a>=0 AND k IN (SELECT k FROM empty)",
            key_payload(vec![]),
            FilterMetric::None,
        ),
        (
            "SELECT p.k,p.payload FROM (SELECT k,a,payload FROM r WHERE k< -1) p JOIN b ON p.k+0=b.k",
            key_payload(vec![]),
            FilterMetric::None,
        ),
    ] {
        check_membership_matrix(&tables, sql, Ok(expected), metric);
    }
    check_membership_matrix(
        &tables,
        "SELECT b.k,g.a FROM nullable b LEFT JOIN (SELECT k,SUM(a) AS a FROM r GROUP BY k) g ON b.k=g.k ORDER BY 1 NULLS LAST",
        Ok(vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(0), Some(0), None])),
        ]),
        FilterMetric::Demand,
    );
    check_membership_matrix(
        &tables,
        "SELECT b.k,g.a FROM b LEFT JOIN (SELECT k,MIN(a) AS a FROM r WHERE k< -1 GROUP BY k) g ON b.k=g.k",
        Ok(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
        ]),
        FilterMetric::None,
    );
}

#[test]
fn totality_barrier_preserves_competing_build_error_precedence() {
    let tables = Tables::new();
    check_membership_matrix(
        &tables,
        "SELECT r.k,r.payload FROM r JOIN (SELECT k FROM c WHERE k*10000000000000000000000000000000000000>0) c ON r.k=c.k WHERE r.k IN (SELECT k FROM c WHERE f*f>0)",
        Err("floating arithmetic overflow"),
        FilterMetric::None,
    );
    check_membership_matrix(
        &tables,
        "SELECT b.k,g.a FROM (SELECT k,a*10000000000000000000000000000000000000 AS v FROM r) b LEFT JOIN (SELECT k,MIN(f*f) AS a FROM c GROUP BY k) g ON b.k=g.k WHERE b.v>0",
        Err("floating arithmetic overflow"),
        FilterMetric::None,
    );
}

#[test]
fn aggregate_demand_keeps_k1_bounded_avg_min_and_sum_admission() {
    let mut tables = Tables::new();
    let keys = (0..512)
        .map(Some)
        .chain([Some(1), Some(2), None])
        .collect::<Vec<_>>();
    let values = keys
        .iter()
        .map(|k| if *k == Some(2) { None } else { Some(100) })
        .collect::<Vec<_>>();
    tables.write(
        "d",
        vec![
            ("k", Arc::new(Int64Array::from(keys))),
            (
                "q",
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
            ),
        ],
    );
    for (sql, value) in [
        (
            "SELECT b.k,g.v FROM b LEFT JOIN (SELECT k,0.2*AVG(q) AS v FROM d GROUP BY k) g ON b.k=g.k",
            Arc::new(Float64Array::from(vec![0.2])) as ArrayRef,
        ),
        (
            "SELECT b.k,(SELECT 0.2*AVG(q) FROM d WHERE d.k=b.k) AS v FROM b",
            Arc::new(Float64Array::from(vec![0.2])),
        ),
        (
            "SELECT b.k,g.v FROM b LEFT JOIN (SELECT d.k,MIN(d.q) AS v FROM d JOIN c ON d.k=c.k WHERE c.a=0 GROUP BY d.k) g ON b.k=g.k",
            Arc::new(
                Decimal128Array::from(vec![100])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ),
        (
            "SELECT b.k,g.v FROM b LEFT JOIN (SELECT k,0.5*SUM(q) AS v FROM d GROUP BY k) g ON b.k=g.k",
            Arc::new(
                Decimal128Array::from(vec![1000])
                    .with_precision_and_scale(38, 3)
                    .unwrap(),
            ),
        ),
        (
            "SELECT b.k,(SELECT MIN(d.q) FROM d JOIN c ON d.k=c.k WHERE d.k=b.k AND c.a=0) AS v FROM b",
            Arc::new(
                Decimal128Array::from(vec![100])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ),
        (
            "SELECT b.k,(SELECT 0.5*SUM(q) FROM d WHERE d.k=b.k) AS v FROM b",
            Arc::new(
                Decimal128Array::from(vec![1000])
                    .with_precision_and_scale(38, 3)
                    .unwrap(),
            ),
        ),
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![Arc::new(Int64Array::from(vec![1])), value]),
            FilterMetric::Demand,
        );
    }
    tables.write(
        "demand",
        vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(1)])),
        )],
    );
    check_membership_matrix(
        &tables,
        "SELECT b.k,g.v FROM demand b LEFT JOIN (SELECT k,SUM(q) AS v FROM d GROUP BY k) g ON b.k=g.k ORDER BY 1 NULLS LAST",
        Ok(vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(200), Some(200), None, None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ]),
        FilterMetric::Demand,
    );
    // Decimal/integer membership uses exact comparison without changing keys.
    check_membership_matrix(
        &tables,
        "SELECT k,payload FROM r WHERE a>0 AND k IN (SELECT q FROM d)",
        Ok(key_payload(vec![1])),
        FilterMetric::None,
    );
    for relational_optimizations in [false, true] {
        let options = Options {
            relational_optimizations,
            ..Options::default()
        };
        // Old: refused as a mixed Int/Float IN subquery. New (Helen, Oct 15):
        // DOUBLE membership. No key in -1..511 equals 1e308 (DuckDB: no rows).
        let prepared = prepare(
            "SELECT k FROM r WHERE k IN (SELECT f FROM c)",
            &tables.0,
            &options,
        )
        .unwrap();
        assert_eq!(execute(&prepared, &options).unwrap().batch.num_rows(), 0);
    }
}

#[test]
fn aggregate_demand_cannot_hide_unbounded_decimal_sum() {
    let mut tables = Tables::new();
    tables.write(
        "wide",
        vec![
            (
                "k",
                Arc::new(Int64Array::from_iter_values(
                    [-1, -1].into_iter().chain(1..512),
                )),
            ),
            (
                "q",
                Arc::new(
                    Decimal128Array::from_iter_values(
                        [6 * 10_i128.pow(37); 2]
                            .into_iter()
                            .chain(std::iter::repeat_n(0, 511)),
                    )
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
            ),
        ],
    );
    check_membership_matrix(
        &tables,
        "SELECT b.k,g.v FROM b LEFT JOIN (SELECT k,SUM(q) AS v FROM wide GROUP BY k) g ON b.k=g.k",
        Err(OVERFLOW),
        FilterMetric::None,
    );
}

#[test]
fn planner_on_and_comma_guards_decide_fallible_keys_and_residuals() {
    let mut tables = Tables::with_tail(0);
    tables.write("z", vec![("k", Arc::new(Int64Array::from(vec![0])))]);
    // The first SQL is the review's exact suppressed-overflow reproducer.
    // Guarded AND (Helen, October 17, 2026; SEMANTICS-LOG "Guarded AND/OR
    // planner rules"): the total conjunct r.k IN (1) of the same ON/WHERE
    // rejects the -1 row, so its key/residual overflow is not raised; DuckDB
    // 1.5.6 and PostgreSQL 17 return these answers. Old expectation: OVERFLOW
    // for all six (test formerly named
    // planner_on_and_comma_pushdown_preserve_required_key_and_residual_errors).
    let one = || Ok(vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef]);
    let none = || {
        Ok(vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
        ])
    };
    for (sql, expected) in [
        (
            "SELECT r.k FROM r JOIN z ON r.a*10000000000000000000000000000000000000=z.k AND r.k IN (1)",
            one(),
        ),
        (
            "SELECT r.k FROM z JOIN r ON r.a*10000000000000000000000000000000000000=z.k AND r.k IN (1)",
            one(),
        ),
        (
            "SELECT r.k FROM r,z WHERE r.a*10000000000000000000000000000000000000=z.k AND r.k IN (1)",
            one(),
        ),
        (
            "SELECT r.k FROM r JOIN z ON r.a*10000000000000000000000000000000000000>z.k AND r.k IN (1)",
            none(),
        ),
        (
            "SELECT r.k FROM r,z WHERE r.a*10000000000000000000000000000000000000>z.k AND r.k IN (1)",
            none(),
        ),
        (
            "SELECT r.k FROM r WHERE EXISTS(SELECT 1 FROM z WHERE r.a*10000000000000000000000000000000000000=z.k AND r.k IN (1))",
            one(),
        ),
    ] {
        check_membership_matrix(&tables, sql, expected, FilterMetric::None);
    }
    // The same shapes with no conjunct rejecting k=-1 still raise at every
    // setting, as before.
    for sql in [
        "SELECT r.k FROM r JOIN z ON r.a*10000000000000000000000000000000000000=z.k",
        "SELECT r.k FROM z JOIN r ON r.a*10000000000000000000000000000000000000=z.k AND r.k IN (-1, 1)",
        "SELECT r.k FROM r,z WHERE r.a*10000000000000000000000000000000000000=z.k AND r.k < 2",
        "SELECT r.k FROM r JOIN z ON r.a*10000000000000000000000000000000000000>z.k AND r.k IN (-1, 1)",
        "SELECT r.k FROM r,z WHERE r.a*10000000000000000000000000000000000000>z.k",
        "SELECT r.k FROM r WHERE EXISTS(SELECT 1 FROM z WHERE r.a*10000000000000000000000000000000000000=z.k)",
    ] {
        check_membership_matrix(&tables, sql, Err(OVERFLOW), FilterMetric::None);
    }
}

#[test]
fn planner_on_and_comma_pushdown_do_not_evaluate_unmatched_residuals() {
    let mut tables = Tables::with_tail(0);
    // The first SQL is the review's exact phantom-overflow reproducer.
    // Only k=1 matches, and its a=0 makes the residual true without overflow.
    for sql in [
        "SELECT r.k FROM r JOIN b ON r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
        "SELECT r.k FROM b JOIN r ON r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
        "SELECT r.k FROM r,b WHERE r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
        "SELECT r.k FROM r,b WHERE r.k=b.k AND ((r.a*10000000000000000000000000000000000000 IN (0,1) AND r.k IN (SELECT k FROM b)) OR r.k=2)",
        "SELECT r.k FROM b LEFT JOIN r ON r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
        "SELECT r.k FROM r WHERE EXISTS(SELECT 1 FROM b WHERE r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1))",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![Arc::new(Int64Array::from(vec![1]))]),
            FilterMetric::None,
        );
    }
    tables.write(
        "empty",
        vec![("k", Arc::new(Int64Array::from(Vec::<i64>::new())))],
    );
    for sql in [
        "SELECT r.k FROM r JOIN empty b ON r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
        "SELECT r.k FROM empty b JOIN r ON r.k=b.k AND r.a*10000000000000000000000000000000000000 IN (0,1)",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]),
            FilterMetric::None,
        );
    }
}

#[test]
fn reverse_semijoin_keeps_original_build_error_precedence() {
    let tables = Tables::new();
    // First: the review's exact reverse-IN reproducer. The original RHS build
    // errors before the left predicate; reversing it would report OVERFLOW.
    for sql in [
        "SELECT b.k FROM b WHERE b.k*10000000000000000000000000000000000000*10>0 AND b.k IN (SELECT k FROM c WHERE f*f>0)",
        "SELECT b.k FROM b WHERE b.k*10000000000000000000000000000000000000*10>0 AND EXISTS(SELECT 1 FROM c WHERE c.k=b.k AND f*f>0)",
        "SELECT b.k FROM b WHERE b.k*10000000000000000000000000000000000000*10>0 AND NOT EXISTS(SELECT 1 FROM c WHERE c.k=b.k AND f*f>0)",
        "SELECT b.k FROM b WHERE b.k*10000000000000000000000000000000000000*10 IN (SELECT k FROM c WHERE f*f>0)",
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Err("floating arithmetic overflow"),
            FilterMetric::None,
        );
    }
}

#[test]
fn reverse_semijoin_keeps_fallible_residual_candidate_order() {
    let mut tables = Tables::new();
    tables.write("p", vec![("k", Arc::new(Int64Array::from(vec![1, 2])))]);
    tables.write(
        "e",
        vec![
            (
                "k",
                Arc::new(Int64Array::from_iter_values(
                    [2, 1].into_iter().chain(3..513),
                )),
            ),
            ("a", Arc::new(Int64Array::from(vec![10; 512]))),
            ("f", Arc::new(Float64Array::from(vec![1e308; 512]))),
        ],
    );
    // Forward probing visits p.k=1 (precision error) before p.k=2 (float
    // error). Reversal would visit the right input's k=2 occurrence first.
    for sql in [
        "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM e WHERE p.k=e.k AND CASE WHEN p.k=1 THEN e.a*10000000000000000000000000000000000000>0 ELSE e.f*e.f>0 END)",
        "SELECT p.k FROM p WHERE NOT EXISTS(SELECT 1 FROM e WHERE p.k=e.k AND CASE WHEN p.k=1 THEN e.a*10000000000000000000000000000000000000>0 ELSE e.f*e.f>0 END)",
    ] {
        check_membership_matrix(&tables, sql, Err(OVERFLOW), FilterMetric::None);
    }
}

#[test]
fn total_planner_predicates_keep_nulls_duplicates_coercion_and_reverse_admission() {
    let mut tables = Tables::new();
    tables.write(
        "p",
        vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1), Some(2)])),
        )],
    );
    tables.write(
        "d",
        vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
            ),
            (
                "q",
                Arc::new(
                    Decimal128Array::from(vec![Some(100), Some(200), None])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
            ),
        ],
    );
    for sql in [
        "SELECT p.k FROM p JOIN d ON p.k=d.k AND p.k IN (1,2) AND d.q IN (1,2) ORDER BY 1",
        "SELECT p.k FROM p,d WHERE p.k=d.k AND p.k IN (1,2) AND d.q IN (1,2) ORDER BY 1",
        "SELECT p.k FROM p JOIN d ON p.k=d.q AND p.k IN (1,2) AND d.k IN (1,2) ORDER BY 1",
    ] {
        let expected = if sql.contains("p.k=d.q") {
            vec![1, 1, 2]
        } else {
            vec![1, 1, 1, 1]
        };
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![Arc::new(Int64Array::from(expected))]),
            FilterMetric::None,
        );
    }
    for (sql, expected) in [
        (
            "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM c WHERE p.k=c.k AND c.a IN (0,1)) ORDER BY 1 NULLS LAST",
            vec![Some(1), Some(1), Some(2)],
        ),
        (
            "SELECT p.k FROM p WHERE NOT EXISTS(SELECT 1 FROM c WHERE p.k=c.k AND c.a IN (0,1)) ORDER BY 1 NULLS LAST",
            vec![None],
        ),
        (
            "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM c WHERE p.k=c.k AND c.a IN (0,1) AND p.k IN (1,2)) ORDER BY 1 NULLS LAST",
            vec![Some(1), Some(1), Some(2)],
        ),
        (
            "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM c WHERE p.k=c.k AND c.k<>p.k) ORDER BY 1 NULLS LAST",
            vec![],
        ),
        (
            "SELECT p.k FROM p WHERE EXISTS(SELECT 1 FROM r WHERE p.k=r.k AND r.payload NOT LIKE '%drop%' AND substring(r.payload FROM 1 FOR 2) IN ('p','é')) ORDER BY 1 NULLS LAST",
            vec![Some(1), Some(1), Some(2)],
        ),
    ] {
        check_membership_matrix(
            &tables,
            sql,
            Ok(vec![Arc::new(Int64Array::from(expected))]),
            FilterMetric::Reverse,
        );
    }
}
