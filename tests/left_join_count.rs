use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Execution, Options, execute},
    rel,
    spill::{SpillMode, SpillOptions},
};
use std::{fs, sync::Arc};

#[path = "left_join_count/fixture.rs"]
mod fixture;
use fixture::{Fixture, custom_fixture_with_nullability};

impl Fixture {
    fn options(&self, workers: usize, batch_size: usize, optimized: bool) -> Options {
        Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            memory_bytes: 64 << 20,
            spill: Some(SpillOptions {
                mode: SpillMode::Auto,
                directory: self.root.join("spill"),
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn run(&self, sql: &str, options: &Options) -> Result<Execution, assaydb::Error> {
        execute(&rel::prepare(sql, &self.tables, options)?, options)
    }
    fn compare(&self, sql: &str, batch: usize, fused: Option<usize>) -> RecordBatch {
        let expected = self.run(sql, &self.options(1, batch, false)).unwrap();
        for workers in [1, 2, 4, 10] {
            let options = self.options(workers, batch, true);
            let actual = self.run(sql, &options).unwrap();
            assert_eq!(actual.batch, expected.batch, "{workers}/{batch}: {sql}");
            // Arrow equality alone does not distinguish signed zero. Check the
            // surviving float representatives without normalizing their bits.
            for (actual, expected) in actual.batch.columns().iter().zip(expected.batch.columns()) {
                if let Some(actual) = actual.as_any().downcast_ref::<Float64Array>() {
                    let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                    for row in 0..actual.len() {
                        if actual.is_valid(row) {
                            assert_eq!(
                                actual.value(row).to_bits(),
                                expected.value(row).to_bits(),
                                "{workers}/{batch}/{row}: {sql}"
                            );
                        }
                    }
                }
            }
            if let Some(fused) = fused {
                assert_eq!(actual.metrics.fused_left_count_joins, fused, "{sql}");
            }
            assert!(actual.metrics.worker_threads_peak <= workers);
            assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
        expected.batch
    }
    fn compare_error(&self, sql: &str, batch: usize) -> assaydb::Error {
        let expected = self.run(sql, &self.options(1, batch, false)).err().unwrap();
        for workers in [1, 2, 4, 10] {
            let generic = self
                .run(sql, &self.options(workers, batch, false))
                .err()
                .unwrap();
            let actual = self
                .run(sql, &self.options(workers, batch, true))
                .err()
                .unwrap();
            assert_eq!(generic, expected, "generic {workers}: {sql}");
            assert_eq!(actual, expected, "optimized {workers}: {sql}");
        }
        expected
    }
    // Rule O's reference is fixed, not recomputed at each batch boundary.
    fn compare_probe_order(&self, sql: &str, fused: Option<usize>) -> RecordBatch {
        let expected = self
            .run(
                sql,
                &Options {
                    memory_bytes: 256 << 20,
                    ..self.options(1, 1, false)
                },
            )
            .unwrap()
            .batch;
        for workers in [1, 2, 4, 10] {
            for batch in [1, 7, 8192] {
                for optimized in [false, true] {
                    let options = Options {
                        memory_bytes: 256 << 20,
                        ..self.options(workers, batch, optimized)
                    };
                    let actual = self.run(sql, &options).unwrap();
                    assert_eq!(
                        actual.batch, expected,
                        "{workers}/{batch}/{optimized}: {sql}"
                    );
                    if optimized && let Some(fused) = fused {
                        assert_eq!(actual.metrics.fused_left_count_joins, fused, "{sql}");
                    }
                    assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                    assert!(actual.metrics.worker_threads_peak <= workers);
                    assert_eq!(actual.metrics.auto_spill_retries, 0);
                }
            }
        }
        expected
    }
}
fn custom_fixture(
    left: Vec<(&str, ArrayRef)>,
    right: Vec<(&str, ArrayRef)>,
    row_group_size: usize,
) -> Fixture {
    custom_fixture_with_nullability(left, right, row_group_size, true)
}

#[test]
fn required_count_windows_keep_order_null_extension_and_nullable_fallback() {
    for (left, right) in [(41, 83), (0, 83), (41, 0), (0, 0)] {
        let f = Fixture::new(left, right, false);
        for sql in [
            "SELECT l.g,COUNT(r.g),COUNT(*),COUNT(r.tag) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            "SELECT l.g,COUNT(r.g),COUNT(*),COUNT(r.tag) FROM l LEFT JOIN r ON l.k=r.k AND r.tag NOT LIKE '%drop%' GROUP BY l.g",
            // One nullable counted column keeps the entire old two-count path.
            "SELECT l.g,COUNT(r.g),COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        ] {
            f.compare_probe_order(sql, Some(1));
        }
    }
    // Identical non-NULL values with different field declarations: only the
    // required schema can establish the compact-window proof.
    for nullable in [false, true] {
        let f = custom_fixture_with_nullability(
            vec![
                ("k", Arc::new(Int64Array::from(vec![3, 1, 3, 99, 2]))),
                ("g", Arc::new(Int64Array::from(vec![20, 10, 20, 40, 30]))),
            ],
            vec![
                ("k", Arc::new(Int64Array::from(vec![3, 3, 1, 2, 3]))),
                ("v", Arc::new(Int64Array::from(vec![7, 8, 9, 10, 11]))),
            ],
            2,
            nullable,
        );
        let sql = "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
        let result = f.compare_probe_order(sql, Some(1));
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![6, 1, 0, 1])
        );
        for batch in [1, 7, 8192] {
            let error = f.compare_error("SELECT l.g+99999999999999999999999999999999999999,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g", batch);
            assert!(error.contains("overflow"), "{error}");
            for workers in [1, 2, 4, 10] {
                let options = f.options(workers, batch, true);
                options.cancellation.cancel();
                assert_eq!(
                    f.run(sql, &options).err().unwrap(),
                    "cancelled: query execution"
                );
                let options = Options {
                    memory_bytes: 1,
                    spill: None,
                    ..f.options(workers, batch, true)
                };
                assert!(
                    f.run(sql, &options)
                        .err()
                        .unwrap()
                        .starts_with("resource-limit:")
                );
            }
        }
    }
}

#[test]
fn left_count_keeps_duplicates_nulls_zero_groups_and_first_encounters() {
    let f = Fixture::new(8, 8, false);
    let sql = "SELECT l.g,COUNT(r.v) AS n,COUNT(*) AS all_rows,COUNT(r.z) AS z,COUNT(r.k) AS keys,COUNT(r.v) AS again FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
    for batch in [2, 5, 128] {
        let result = f.compare(sql, batch, Some(1));
        for (column, expected) in [
            (0, vec![20, 10, 30, 40]),
            (1, vec![4, 0, 0, 0]),
            (2, vec![6, 6, 2, 1]),
            (3, vec![0, 0, 0, 0]),
            (4, vec![6, 6, 0, 0]),
            (5, vec![4, 0, 0, 0]),
        ] {
            assert_eq!(
                result
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(expected)
            );
        }
        for sql in [
            "SELECT l.k,l.s,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND l.s=r.s GROUP BY l.k,l.s",
            "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.s=r.s GROUP BY l.g",
            "SELECT l.k,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND r.tag NOT LIKE '%drop%' WHERE l.g<>40 GROUP BY l.k",
            "SELECT n,COUNT(*) FROM (SELECT l.k,COUNT(r.v) AS n FROM l LEFT JOIN r ON l.k=r.k AND r.tag NOT LIKE '%drop%' GROUP BY l.k) q GROUP BY n ORDER BY n",
            "SELECT l.g AS g,COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g HAVING COUNT(*)>1 ORDER BY g DESC LIMIT 2",
        ] {
            f.compare(sql, batch, Some(1));
        }
    }
}

#[test]
fn left_count_handles_empty_inputs_and_all_filtered_matches() {
    for (left, right) in [(0, 8), (8, 0), (0, 0)] {
        let f = Fixture::new(left, right, false);
        let result = f.compare(
            "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            3,
            None,
        );
        assert_eq!(result.num_rows(), if left == 0 { 0 } else { 4 });
        if left != 0 {
            assert_eq!(
                result
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(vec![0; 4])
            );
        }
    }
    let f = Fixture::new(8, 8, false);
    f.compare("SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND r.tag='absent' GROUP BY l.g", 3, Some(1));
}

#[test]
fn left_count_grows_storage_and_preserves_cross_batch_multiplicity() {
    let f = Fixture::new(1600, 2400, true);
    for sql in [
        "SELECT l.k,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.k",
        "SELECT l.g,l.s,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND l.s=r.s GROUP BY l.g,l.s",
    ] {
        f.compare(sql, 53, Some(1));
    }
    let options = f.options(10, 53, true);
    let result = f
        .run(
            "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            &options,
        )
        .unwrap();
    let expected = f
        .run(
            "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            &f.options(1, 53, false),
        )
        .unwrap();
    assert_eq!(result.batch, expected.batch);
    assert_eq!(result.metrics.fused_left_count_rows, 1600);
    assert_eq!(
        result.metrics.join_output_rows, 0,
        "no joined payload rows are constructed"
    );
}

#[test]
fn counted_scan_and_probe_waves_keep_serial_order_with_mixed_integer_widths() {
    use arrow::array::Int32Array;
    let left = 521usize;
    let right = 907usize;
    let f = custom_fixture(
        vec![
            (
                "k",
                Arc::new(Int32Array::from_iter(
                    (0..left).map(|i| (i % 11 != 0).then_some((i % 73) as i32 - 5)),
                )),
            ),
            (
                "g",
                Arc::new(Int64Array::from_iter_values((0..left).map(|i| i as i64))),
            ),
        ],
        vec![
            (
                "k",
                Arc::new(Int64Array::from_iter(
                    (0..right).map(|i| (i % 13 != 0).then_some((i % 59) as i64 - 3)),
                )),
            ),
            (
                "v",
                Arc::new(Int64Array::from_iter(
                    (0..right).map(|i| (i % 3 != 0).then_some(i as i64)),
                )),
            ),
            (
                "s",
                Arc::new(StringArray::from_iter_values(
                    (0..right).map(|i| if i % 7 == 0 { "skip this" } else { "keep" }),
                )),
            ),
        ],
        37,
    );
    for batch in [31, 64, 8192] {
        f.compare("SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND r.s NOT LIKE '%skip%' GROUP BY l.g", batch, Some(1));
    }
    for workers in [1, 2, 4, 10] {
        let options = f.options(workers, 31, true);
        let result = f
            .run(
                "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
                &options,
            )
            .unwrap();
        assert!(
            result.metrics.post_decode_filter_batches > 0,
            "count build ran inside scan callbacks"
        );
        assert!(result.metrics.parallel_join_tasks > 0);
        assert_eq!(result.metrics.parallel_joins > 0, workers > 1);
        options.cancellation.cancel();
        assert!(
            f.run(
                "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
                &options
            )
            .err()
            .unwrap()
            .starts_with("cancelled:")
        );
    }
    // Fallible build expressions and non-count aggregates retain the old path
    // and the right-before-left error phase at every worker count.
    f.compare_error("SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k AND (r.k+9223372036854775807)+99999999999999999999999999999999999999>0 WHERE l.g+99999999999999999999999999999999999999>0 GROUP BY l.g", 31);
}

#[test]
fn left_count_declines_other_aggregates_residuals_and_error_expressions() {
    let f = Fixture::new(8, 8, false);
    let sql = "SELECT l.g,COUNT(r.v),SUM(r.z),AVG(r.z),SUM(r.f) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
    // Float SUM succeeds through the ordinary LEFT path; COUNT-only fusion
    // still declines the mixed aggregate list.
    f.compare(sql, 3, Some(0));
    assert_eq!(f.compare_error("SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=CAST(100.0/(r.k-r.k) AS BIGINT) GROUP BY l.g", 3), "division by zero");
    // Qualified GROUP BY keys now bind in ORDER BY as well. Keep the fixture
    // and assert its exact descending groups after HAVING and LIMIT.
    let ordered = f.compare("SELECT l.g,COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g HAVING COUNT(*)>1 ORDER BY l.g DESC LIMIT 2", 3, None);
    assert_eq!(
        ordered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[30, 20]
    );
    assert_eq!(
        ordered
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[2, 6]
    );
    for sql in [
        "SELECT l.g,COUNT(r.v),SUM(r.z),AVG(r.z),MIN(r.f),MAX(r.f) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,COUNT(DISTINCT r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,COUNT(l.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,COUNT(1) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k AND l.v<r.v GROUP BY l.g",
        "SELECT r.s,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY r.s",
        "SELECT l.g+1,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g+1",
        "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k+0=r.k GROUP BY l.g",
    ] {
        f.compare(sql, 3, Some(0));
    }
    for sql in [
        "SELECT l.g,COUNT(100.0/(r.v-r.v)) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k AND 100.0/(r.v-r.v)>0 GROUP BY l.g",
        "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k WHERE 100.0/(l.k-l.k)>0 GROUP BY l.g",
        "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=CAST(100.0/(r.k-r.k) AS INTEGER) GROUP BY l.g",
    ] {
        let expected = f.compare_error(sql, 3);
        assert!(expected.contains("zero"), "{expected}");
    }
}

#[test]
fn left_count_interleaves_matches_and_misses_in_probe_order() {
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from_iter_values(0..8))),
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    None,
                    Some(9),
                    Some(1),
                    Some(2),
                    Some(1),
                    None,
                    Some(2),
                    Some(3),
                ])),
            ),
        ],
        vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    None,
                    Some(2),
                ])),
            ),
            (
                "v",
                Arc::new(Int64Array::from(vec![Some(7), None, None, Some(8), None])),
            ),
        ],
        2,
    );
    let sql = "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
    f.compare_probe_order(sql, Some(1));
    f.compare_probe_order(&format!("{sql} LIMIT 2"), Some(1));
    f.compare_probe_order("SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k", Some(0));
    for batch in [1, 3, 5, 32] {
        let result = f.compare(sql, batch, Some(1));
        if batch == 3 {
            for (column, expected) in [
                (0, vec![0, 1, 2, 3, 4, 5, 6, 7]),
                (1, vec![0, 0, 1, 0, 1, 0, 0, 0]),
                (2, vec![1, 1, 2, 2, 2, 1, 2, 1]),
            ] {
                assert_eq!(
                    result
                        .column(column)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(expected)
                );
            }
        }
        // A downstream LIMIT observes encounter order, even without ORDER BY.
        f.compare(&format!("{sql} LIMIT 2"), batch, Some(1));
        for predicate in [
            "l.g<>3",
            "l.k IS NULL OR l.g<4",
            "l.g<0",
            "l.k IS NOT NULL AND l.g>=1",
        ] {
            f.compare(&format!("SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k WHERE {predicate} GROUP BY l.g"), batch, Some(1));
        }
        // Right-side WHERE conditions can distinguish a match from a miss and
        // therefore must not be evaluated on the left-only weighted stream.
        f.compare("SELECT l.g AS g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k WHERE r.v IS NULL GROUP BY l.g ORDER BY g", batch, Some(0));
    }
}

#[test]
fn left_count_preserves_typed_keys_and_payload_null_validity() {
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int32Array, NullArray,
    };
    let key_types: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![
            None,
            Some(i32::MIN),
            Some(7),
            Some(i32::MIN),
            Some(i32::MAX),
        ])),
        Arc::new(Int64Array::from(vec![
            None,
            Some(i64::MIN),
            Some(7),
            Some(i64::MIN),
            Some(i64::MAX),
        ])),
        Arc::new(
            Decimal128Array::from(vec![
                None,
                Some(-10_i128.pow(37)),
                Some(7),
                Some(-10_i128.pow(37)),
                Some(10_i128.pow(37)),
            ])
            .with_precision_and_scale(38, 2)
            .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![
            None,
            Some(-10),
            Some(7),
            Some(-10),
            Some(20_000),
        ])),
        Arc::new(BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            Some(false),
            Some(true),
        ])),
        Arc::new(StringArray::from(vec![
            None,
            Some("é\0雪"),
            Some(""),
            Some("é\0雪"),
            Some("z"),
        ])),
    ];
    for keys in key_types {
        let secondary: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(1),
            Some(2),
            Some(1),
            None,
        ]));
        let f = custom_fixture(
            vec![("k", keys.clone()), ("secondary", secondary.clone())],
            vec![
                ("k", keys),
                ("secondary", secondary),
                (
                    "f",
                    Arc::new(Float64Array::from(vec![
                        Some(99.0),
                        Some(f64::NAN),
                        None,
                        Some(-0.0),
                        Some(0.0),
                    ])),
                ),
                (
                    "b",
                    Arc::new(BinaryArray::from(vec![
                        Some(&b"ignored"[..]),
                        Some(&b"\0x"[..]),
                        None,
                        Some(&b""[..]),
                        Some(&b"last"[..]),
                    ])),
                ),
                ("n", Arc::new(NullArray::new(5))),
            ],
            2,
        );
        for batch in [1, 3, 8] {
            // A physical Null column binds as a NULL literal; the literal COUNT
            // path deliberately declines. Typed nullable payloads still fuse.
            let result = f.compare("SELECT l.k AS k,COUNT(r.f),COUNT(r.b),COUNT(r.n),COUNT(*),COUNT(r.k),COUNT(r.f) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.k ORDER BY k", batch, Some(0));
            f.compare("SELECT l.k,COUNT(r.f),COUNT(r.b),COUNT(*),COUNT(r.k),COUNT(r.f) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.k", batch, Some(1));
            assert_eq!(
                result
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(vec![0; result.num_rows()])
            );
            // Test compound key NULL rejection as well as the single-key index.
            f.compare("SELECT l.k,COUNT(r.f),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k AND l.secondary=r.secondary GROUP BY l.k", batch, Some(1));
        }
    }
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        None,
        Some(-0.0),
        Some(0.0),
        Some(1.5),
    ]));
    let f = custom_fixture(vec![("k", floats.clone())], vec![("k", floats)], 2);
    f.compare(
        "SELECT l.k,COUNT(r.k),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.k",
        3,
        Some(0),
    );
}

#[test]
fn left_count_preserves_float_group_bits_with_matches_null_keys_and_empty_builds() {
    for right_keys in [vec![Some(1), Some(1), None], vec![None; 3], vec![]] {
        let right_rows = right_keys.len();
        let f = custom_fixture(
            vec![
                (
                    "k",
                    Arc::new(Int64Array::from(vec![None, Some(1), Some(1), Some(2)])),
                ),
                (
                    "g",
                    Arc::new(Float64Array::from(vec![
                        Some(-0.0),
                        Some(0.0),
                        Some(-0.0),
                        None,
                    ])),
                ),
            ],
            vec![
                ("k", Arc::new(Int64Array::from(right_keys))),
                (
                    "v",
                    Arc::new(Int64Array::from_iter(
                        (0..right_rows).map(|i| (i == 0).then_some(7)),
                    )),
                ),
            ],
            2,
        );
        for batch in [1, 3, 8] {
            f.compare("SELECT l.g,COUNT(r.v),COUNT(*),COUNT(r.k) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g", batch, (right_rows != 0).then_some(1));
        }
    }
}

#[test]
fn left_probe_order_keeps_duplicate_build_order_and_residual_misses() {
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5]))),
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    None,
                    Some(9),
                    Some(1),
                    Some(2),
                    Some(1),
                    Some(3),
                ])),
            ),
        ],
        vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    None,
                    Some(1),
                ])),
            ),
            (
                "v",
                Arc::new(Int64Array::from(vec![
                    Some(30),
                    None,
                    Some(10),
                    Some(99),
                    Some(20),
                ])),
            ),
        ],
        2,
    );
    let sql = "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k";
    let result = f.compare_probe_order(sql, Some(0));
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![0, 1, 2, 2, 2, 3, 4, 4, 4, 5])
    );
    assert_eq!(
        result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![
            None,
            None,
            Some(30),
            Some(10),
            Some(20),
            None,
            Some(30),
            Some(10),
            Some(20),
            None
        ])
    );
    f.compare_probe_order(&format!("{sql} LIMIT 3"), Some(0));
    // Both predicates depend on both sides, so neither can be pushed into the
    // build scan. All duplicate candidates are FALSE/UNKNOWN in the first case.
    for predicate in ["r.v<l.g", "r.v>l.g", "r.v-l.g>15"] {
        let sql = format!("SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k AND {predicate}");
        let result = f.compare_probe_order(&sql, Some(0));
        if predicate == "r.v<l.g" {
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(vec![0, 1, 2, 3, 4, 5])
            );
            assert_eq!(result.column(1).null_count(), 6);
        }
    }
    f.compare_probe_order(
        "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        Some(1),
    );
    f.compare_probe_order("SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k WHERE l.g<>2 GROUP BY l.g LIMIT 3", Some(1));
    let sums = f.compare_probe_order(
        "SELECT l.g,SUM(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
        Some(0),
    );
    assert_eq!(
        sums.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![None, None, Some(60), None, Some(60), None])
    );
}

#[test]
fn left_probe_order_empty_inputs_and_coerced_keys() {
    for (left, right) in [(0, 8), (8, 0), (0, 0)] {
        let f = Fixture::new(left, right, false);
        f.compare_probe_order("SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k", Some(0));
        f.compare_probe_order(
            "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            None,
        );
    }
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from(vec![0, 1, 2, 3]))),
            (
                "k",
                Arc::new(Int64Array::from(vec![None, Some(9), Some(1), Some(2)])),
            ),
        ],
        vec![
            (
                "k",
                Arc::new(arrow::array::Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                ])),
            ),
            ("v", Arc::new(Int64Array::from(vec![30, 10, 20, 99]))),
        ],
        2,
    );
    for sql in [
        "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k",
        "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=CAST(r.k AS REAL)",
        "SELECT l.g,r.v FROM l LEFT JOIN r ON CAST(l.k AS REAL)=CAST(r.k AS REAL)",
    ] {
        f.compare_probe_order(sql, Some(0));
    }
}

#[test]
fn left_probe_order_survives_upstream_join_batch_changes() {
    let mut f = custom_fixture(
        vec![
            ("id", Arc::new(Int64Array::from_iter_values(1..=512))),
            (
                "x",
                Arc::new(Int64Array::from_iter_values(
                    (1..=512).map(|i| if i <= 3 { 1 } else { i }),
                )),
            ),
            (
                "y",
                Arc::new(Int64Array::from_iter_values(
                    (1..=512).map(|i| if i == 3 { 1 } else { i }),
                )),
            ),
            ("payload", Arc::new(StringArray::from(vec!["p"; 512]))),
        ],
        vec![(
            "y",
            Arc::new(Int64Array::from_iter_values(
                (0..128).map(|i| if i == 0 { 1 } else { 999 }),
            )),
        )],
        8192,
    );
    let tail = custom_fixture(
        vec![("x", Arc::new(Int64Array::from(vec![1])))],
        vec![("id", Arc::new(Int64Array::from(vec![3])))],
        8192,
    );
    f.tables.insert("p".into(), f.tables["l"].clone());
    f.tables.insert("b".into(), f.tables["r"].clone());
    f.tables.insert("c".into(), tail.tables["l"].clone());
    f.tables.insert("d".into(), tail.tables["r"].clone());
    let sql = "SELECT p.id,p.payload,d.id FROM p JOIN b ON p.y=b.y JOIN c ON p.x=c.x LEFT JOIN d ON p.id=d.id";
    let result = f.compare_probe_order(sql, Some(0));
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![1, 3])
    );
    assert_eq!(
        result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![None, Some(3)])
    );
    // The preserved F1 reproducer used batches of two, in addition to the card matrix.
    f.compare(sql, 2, Some(0));
}

#[test]
fn left_probe_order_parallel_ranges_and_count_groups() {
    // Enough estimated probe rows to enter the generic parallel path even at
    // batch_size=8192. Raw pair order also pins duplicate build insertion order.
    let rows = 65_537;
    let f = custom_fixture(
        vec![
            (
                "g",
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i % 11))),
            ),
            (
                "k",
                Arc::new(Int64Array::from_iter((0..rows).map(|i| match i % 4 {
                    0 => None,
                    1 => Some(9),
                    _ => Some(1),
                }))),
            ),
        ],
        vec![
            ("k", Arc::new(Int64Array::from(vec![1, 1]))),
            ("v", Arc::new(Int64Array::from(vec![30, 10]))),
        ],
        4093,
    );
    let sql = "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k";
    f.compare_probe_order(sql, Some(0));
    for batch in [1, 7, 8192] {
        let actual = f
            .run(
                sql,
                &Options {
                    memory_bytes: 256 << 20,
                    ..f.options(10, batch, true)
                },
            )
            .unwrap();
        assert!(
            actual.metrics.parallel_joins > 0,
            "parallel LEFT not exercised at {batch}"
        );
        assert!(actual.metrics.parallel_join_pending_batches_peak <= 5);
    }
    f.compare_probe_order(
        "SELECT l.g,COUNT(r.v),COUNT(*) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g LIMIT 3",
        Some(1),
    );
}

#[test]
fn left_probe_order_errors_and_cancellation() {
    let f = Fixture::new(8, 8, false);
    for sql in [
        "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k AND 100.0/(r.v-l.v)>0",
        "SELECT l.g,100.0/(l.g-l.g) FROM l LEFT JOIN r ON l.k=r.k",
    ] {
        let expected = f.run(sql, &f.options(1, 1, false)).err().unwrap();
        assert!(expected.contains("zero"), "{expected}");
        for batch in [1, 7, 8192] {
            assert_eq!(f.compare_error(sql, batch), expected);
        }
    }
    for workers in [1, 2, 4, 10] {
        for batch in [1, 7, 8192] {
            for sql in [
                "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k",
                "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g",
            ] {
                let options = f.options(workers, batch, true);
                let prepared = rel::prepare(sql, &f.tables, &options).unwrap();
                options.cancellation.cancel();
                assert!(
                    execute(&prepared, &options)
                        .err()
                        .unwrap()
                        .starts_with("cancelled:")
                );
                f.run(sql, &f.options(workers, batch, true)).unwrap();
            }
        }
    }
}

#[test]
fn left_spill_pins_partition_order_and_matches_before_misses_for_o2() {
    // Partition IDs depend on ahash's target implementation. Pin the ordering
    // rule using the existing fixed seeds and native i128 key bytes, not a
    // platform-specific literal partition sequence.
    let hash = ahash::RandomState::with_seeds(173, 907, 3109, 7177);
    let partition = |k: i64| (hash.hash_one((k as i128).to_ne_bytes().as_slice()) & 3) as usize;
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from_iter_values(0..64))),
            (
                "k",
                Arc::new(Int64Array::from_iter_values((0..64).map(|i| i % 32))),
            ),
        ],
        vec![
            ("k", Arc::new(Int64Array::from_iter_values(0..32))),
            ("v", Arc::new(Int64Array::from_iter_values(32..64))),
        ],
        8192,
    );
    let sql = "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k AND l.g-r.v=0";
    let mut expected_left = Vec::new();
    let mut expected_right = Vec::new();
    for part in 0..4 {
        // Each partition's later 32 rows match; its earlier 32 miss.
        for matched in [true, false] {
            for g in 0..64 {
                if partition(g % 32) == part && (g >= 32) == matched {
                    expected_left.push(g);
                    expected_right.push(matched.then_some(g));
                }
            }
        }
    }
    assert_ne!(expected_left, (0..64).collect::<Vec<_>>());
    fs::create_dir(f.root.join("spill")).unwrap();
    for workers in [1, 2, 4, 10] {
        let mut options = f.options(workers, 8192, true);
        options.spill.as_mut().unwrap().mode = SpillMode::Force;
        let result = f.run(sql, &options).unwrap();
        assert!(result.metrics.external_join_partitions > 0);
        assert_eq!(
            result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(expected_left.clone())
        );
        assert_eq!(
            result
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(expected_right.clone())
        );
    }
}

#[test]
fn left_earlier_miss_error_precedes_later_residual_error() {
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from(vec![0, 10]))),
            ("k", Arc::new(Int64Array::from(vec![None, Some(1)]))),
        ],
        vec![
            ("k", Arc::new(Int64Array::from(vec![1]))),
            ("v", Arc::new(Int64Array::from(vec![10]))),
        ],
        2,
    );
    let sql = "SELECT 100.0/l.g FROM l LEFT JOIN r ON l.k=r.k AND l.g*r.v*10000000000000000000000000000000000000>0";
    let expected = f.compare_error(sql, 1);
    assert!(expected.contains("zero"), "{expected}");
    for batch in [7, 8192] {
        assert_eq!(f.compare_error(sql, batch), expected);
    }
    // Phase 1 accepts the exact column cast. Keep this SQL and require both
    // the missed and matched left rows under every execution configuration.
    for batch in [1, 7, 8192] {
        let result = f.compare(
            "SELECT l.g FROM l LEFT JOIN r ON l.k=CAST(r.k AS DECIMAL(20,0))",
            batch,
            None,
        );
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![0, 10]),
        );
    }
}

#[test]
fn left_finishes_one_rows_residuals_before_downstream_errors() {
    let f = custom_fixture(
        vec![
            ("k", Arc::new(Int64Array::from(vec![1]))),
            ("g", Arc::new(Int64Array::from(vec![1]))),
        ],
        vec![
            ("k", Arc::new(Int64Array::from(vec![1, 1]))),
            ("v", Arc::new(Int64Array::from(vec![1, 10]))),
        ],
        8192,
    );
    let sql = "SELECT 100.0/(l.g-1) FROM l LEFT JOIN r ON l.k=r.k AND l.g*r.v*10000000000000000000000000000000000000>0";
    // Handwritten rule-4 oracle: candidate one reaches 10^37; candidate two
    // reaches 10^38. Neither match may reach the dividing projection first.
    // Before the repair batch 1 reported division by zero instead.
    for workers in [1, 2, 4, 10] {
        for batch in [1, 7, 8192] {
            for optimized in [false, true] {
                let options = Options {
                    memory_bytes: 256 << 20,
                    spill: None,
                    parquet_cache_bytes: None,
                    ..f.options(workers, batch, optimized)
                };
                assert_eq!(
                    f.run(sql, &options).err().unwrap(),
                    "decimal precision 38 overflow",
                    "{workers}/{batch}/{optimized}"
                );
            }
        }
    }
}

#[test]
fn left_residual_matches_keep_build_order_across_output_flushes() {
    // More successful matches than even the largest output batch. UNKNOWN
    // residuals and a NULL build key must not create extra matches or misses.
    let rows = 8205_i64;
    let f = custom_fixture(
        vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(1), Some(9), None])),
            ),
            ("g", Arc::new(Int64Array::from(vec![1, 2, 3]))),
        ],
        vec![
            (
                "k",
                Arc::new(arrow::array::Int32Array::from_iter(
                    (0..rows).map(|i| (i != rows - 1).then_some(1)),
                )),
            ),
            (
                "v",
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i != 7).then_some(i % 9 + 1)),
                )),
            ),
            (
                "id",
                Arc::new(Int64Array::from_iter_values((0..rows).rev())),
            ),
        ],
        8192,
    );
    // Every demanded product is at most 9*10^37, so the residual is total.
    let sql = "SELECT l.g,r.id FROM l LEFT JOIN r ON l.k=r.k AND l.g*r.v*10000000000000000000000000000000000000>0";
    let expected_groups =
        Int64Array::from_iter_values(std::iter::repeat_n(1, (rows - 2) as usize).chain([2, 3]));
    let expected_ids = Int64Array::from_iter(
        (0..rows - 1)
            .filter(|&i| i != 7)
            .map(|i| Some(rows - 1 - i))
            .chain([None, None]),
    );
    let mut serial = None;
    for workers in [1, 2, 4, 10] {
        for batch in [1, 7, 8192] {
            for optimized in [false, true] {
                let options = Options {
                    memory_bytes: 256 << 20,
                    spill: None,
                    parquet_cache_bytes: None,
                    ..f.options(workers, batch, optimized)
                };
                let result = f.run(sql, &options).unwrap();
                assert_eq!(
                    result
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &expected_groups,
                    "{workers}/{batch}/{optimized}"
                );
                assert_eq!(
                    result
                        .batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &expected_ids,
                    "{workers}/{batch}/{optimized}"
                );
                if let Some(serial) = &serial {
                    assert_eq!(&result.batch, serial, "{workers}/{batch}/{optimized}");
                } else {
                    serial = Some(result.batch);
                }
                assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert_eq!(result.metrics.auto_spill_retries, 0);
            }
        }
    }
}

#[test]
fn left_sparse_wide_payloads_across_rows_keep_probe_and_build_order() {
    use arrow::array::{BinaryArray, Int32Array};
    let f = custom_fixture(
        vec![
            ("g", Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5]))),
            (
                "k",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(99),
                    None,
                    Some(1),
                    Some(2),
                    Some(3),
                ])),
            ),
            ("v", Arc::new(Int64Array::from(vec![1; 6]))),
            (
                "s",
                Arc::new(StringArray::from_iter_values(
                    (0..6).map(|i| format!("probe-{i}").repeat(1024)),
                )),
            ),
        ],
        vec![
            // The same integer key crosses physical widths. The final NULL
            // build key must not match the NULL probe key.
            (
                "k",
                Arc::new(Int32Array::from_iter(
                    (0..50).map(|i| (i != 49).then_some(1)),
                )),
            ),
            ("id", Arc::new(Int64Array::from_iter_values(0..50))),
            (
                "v",
                Arc::new(Int64Array::from_iter((0..50).map(|i| match i % 7 {
                    0 => Some(0),
                    1 => None,
                    _ => Some(2),
                }))),
            ),
            (
                "s",
                Arc::new(StringArray::from_iter((0..50).map(|i| {
                    (i != 42).then(|| {
                        if i % 7 == 0 {
                            format!("{i:02}").repeat(32768)
                        } else {
                            String::new()
                        }
                    })
                }))),
            ),
            (
                "b",
                Arc::new(BinaryArray::from_iter(
                    (0..50).map(|i| (i != 7).then(|| vec![i as u8; 8192])),
                )),
            ),
        ],
        7,
    );
    let sql = "SELECT l.g,l.s,r.id,r.s,r.b FROM l LEFT JOIN r ON l.k=r.k AND l.v>r.v";
    // Written in probe order and then build insertion order, including misses.
    let groups = [0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 3, 3, 3, 3, 3, 3, 4, 5];
    let ids = [
        Some(0),
        Some(7),
        Some(14),
        Some(21),
        Some(28),
        Some(35),
        Some(42),
        None,
        None,
        Some(0),
        Some(7),
        Some(14),
        Some(21),
        Some(28),
        Some(35),
        Some(42),
        None,
        None,
    ];
    let expected_groups = Int64Array::from(groups.to_vec());
    let expected_ids = Int64Array::from(ids.to_vec());
    let expected_left =
        StringArray::from_iter_values(groups.iter().map(|g| format!("probe-{g}").repeat(1024)));
    let expected_text = StringArray::from_iter(ids.iter().map(|id| {
        id.filter(|&i| i != 42)
            .map(|i| format!("{i:02}").repeat(32768))
    }));
    let expected_binary = BinaryArray::from_iter(
        ids.iter()
            .map(|id| id.filter(|&i| i != 7).map(|i| vec![i as u8; 8192])),
    );
    let mut serial = None;
    for workers in [1, 2, 4, 10] {
        for batch in [1, 7, 8192] {
            for optimized in [false, true] {
                let options = Options {
                    memory_bytes: 64 << 20,
                    spill: None,
                    parquet_cache_bytes: None,
                    ..f.options(workers, batch, optimized)
                };
                let result = f.run(sql, &options).unwrap();
                assert_eq!(
                    result
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &expected_groups
                );
                assert_eq!(
                    result
                        .batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                    &expected_left
                );
                assert_eq!(
                    result
                        .batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &expected_ids
                );
                assert_eq!(
                    result
                        .batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                    &expected_text
                );
                assert_eq!(
                    result
                        .batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap(),
                    &expected_binary
                );
                if let Some(expected) = &serial {
                    assert_eq!(&result.batch, expected, "{workers}/{batch}/{optimized}");
                } else {
                    serial = Some(result.batch);
                }
                assert_eq!(result.metrics.auto_spill_retries, 0);
                assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
            }
        }
    }
}

#[test]
fn left_late_residual_error_precedes_every_payload_chunk() {
    let f = custom_fixture(
        vec![
            ("k", Arc::new(Int64Array::from(vec![1]))),
            ("g", Arc::new(Int64Array::from(vec![1]))),
        ],
        vec![
            ("k", Arc::new(Int64Array::from(vec![1; 23]))),
            (
                "v",
                Arc::new(Int64Array::from_iter_values(
                    (0..23).map(|i| if i == 22 { 10 } else { 1 }),
                )),
            ),
            (
                "s",
                Arc::new(StringArray::from_iter_values(
                    (0..23).map(|_| "x".repeat(65536)),
                )),
            ),
        ],
        7,
    );
    let sql = "SELECT 100.0/(l.g-1),r.s FROM l LEFT JOIN r ON l.k=r.k AND l.g*r.v*10000000000000000000000000000000000000>0";
    // Candidate 23 reaches 10^38 after more than three seven-row batches.
    // No earlier payload or dividing projection is eligible for emission.
    for workers in [1, 2, 4, 10] {
        for batch in [1, 7, 8192] {
            for optimized in [false, true] {
                let options = Options {
                    memory_bytes: 64 << 20,
                    spill: None,
                    parquet_cache_bytes: None,
                    ..f.options(workers, batch, optimized)
                };
                assert_eq!(
                    f.run(sql, &options).err().unwrap(),
                    "decimal precision 38 overflow",
                    "{workers}/{batch}/{optimized}"
                );
            }
        }
    }
}

#[test]
fn left_residual_empty_sides_do_not_evaluate_invented_candidates() {
    for (left_rows, right_rows) in [(0, 2), (2, 0), (0, 0)] {
        let f = custom_fixture(
            vec![
                ("k", Arc::new(Int64Array::from(vec![1; left_rows]))),
                (
                    "g",
                    Arc::new(Int64Array::from_iter_values(1..=left_rows as i64)),
                ),
            ],
            vec![
                ("k", Arc::new(Int64Array::from(vec![1; right_rows]))),
                ("v", Arc::new(Int64Array::from(vec![1; right_rows]))),
            ],
            8192,
        );
        // A real pair with g=1 would fail, but these fixtures have no pairs.
        let sql = "SELECT l.g,r.v FROM l LEFT JOIN r ON l.k=r.k AND 100.0/(l.g-1)>r.v";
        let expected = Int64Array::from_iter_values(1..=left_rows as i64);
        let result = f.compare_probe_order(sql, Some(0));
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &expected
        );
        assert_eq!(result.column(1).null_count(), left_rows);
    }
}

#[test]
fn left_count_u1_sixteen_mib_control_at_all_worker_counts() {
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::{
        collections::HashMap,
        fs::File,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);

    let root = std::env::temp_dir().join(format!(
        "assaydb-left-count-u1-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let mut f = Fixture {
        root,
        tables: HashMap::new(),
    };
    for (name, payload, batch_rows, batches) in [("l", "g", 131072, 10), ("r", "v", 1, 1)] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(payload, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1; batch_rows])),
                Arc::new(Int64Array::from(vec![1; batch_rows])),
            ],
        )
        .unwrap();
        let path = f.root.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(131072)
                    .build(),
            ),
        )
        .unwrap();
        for _ in 0..batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
        f.tables.insert(name.into(), path);
    }
    let sql = "SELECT l.g,COUNT(r.v) FROM l LEFT JOIN r ON l.k=r.k GROUP BY l.g";
    let mut serial = None;
    for workers in [1, 2, 4, 10] {
        let options = Options {
            memory_bytes: 16 << 20,
            spill: None,
            parquet_cache_bytes: None,
            ..f.options(workers, 131072, true)
        };
        let result = f.run(sql, &options).unwrap();
        assert_eq!(result.batch.num_rows(), 1, "{workers}");
        for (column, expected) in [(0, 1), (1, 1310720)] {
            assert_eq!(
                result
                    .batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(vec![expected]),
                "{workers}/{column}"
            );
        }
        if let Some(serial) = &serial {
            assert_eq!(&result.batch, serial, "{workers}");
        } else {
            serial = Some(result.batch);
        }
        assert_eq!(result.metrics.fused_left_count_joins, 1);
        assert_eq!(result.metrics.auto_spill_retries, 0);
        assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
    }
}
