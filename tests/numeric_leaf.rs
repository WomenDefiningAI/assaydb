//! Production relational leaf admission and serial chronology at every width.
use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Execution, Options, execute},
    rel,
    spill::{SpillMode, SpillOptions},
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::{
        metadata::ParquetMetaDataWriter,
        properties::{EnabledStatistics, WriterProperties},
        statistics::Statistics,
        writer::TrackedWrite,
    },
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Fixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new(batch: RecordBatch, statistics: EnabledStatistics) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "assaydb-numeric-leaf-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let path = root.join("input.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(256)
                    .set_statistics_enabled(statistics)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self {
            root,
            tables: HashMap::from([("t".into(), path)]),
        }
    }
    fn options(&self, workers: usize) -> Options {
        Options {
            workers,
            batch_size: 37,
            memory_bytes: 64 << 20,
            spill: Some(SpillOptions {
                mode: SpillMode::Auto,
                directory: self.root.join("spill"),
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn run(&self, sql: &str, workers: usize) -> Result<Execution, String> {
        let options = self.options(workers);
        // Keep auto mode's relational route, without its independent direct
        // pipeline shortcut masking whether this leaf was actually exercised.
        execute(&rel::prepare(sql, &self.tables, &options)?, &options)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn batch(
    keys: Vec<Option<i64>>,
    values: Vec<Option<i64>>,
    decimals: Vec<Option<i128>>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 0), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn assert_bits(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual, expected);
    for (a, b) in actual.columns().iter().zip(expected.columns()) {
        if let (Some(a), Some(b)) = (
            a.as_any().downcast_ref::<Float64Array>(),
            b.as_any().downcast_ref::<Float64Array>(),
        ) {
            assert_eq!(
                a.iter().map(|v| v.map(f64::to_bits)).collect::<Vec<_>>(),
                b.iter().map(|v| v.map(f64::to_bits)).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn ordered_numeric_runs_cross_batches_pages_and_owners_in_both_directions() {
    for descending in [false, true] {
        for sparse in [false, true] {
            let keys = (0..8192)
                .map(|i| {
                    // 513 exceeds a physical row group and is not aligned to
                    // either 256-row pages/groups or 37-row evaluation batches.
                    let key = (i / 513 - 8) * if sparse { 1_i64 << 50 } else { 1 };
                    Some(if descending { -key } else { key })
                })
                .collect();
            let values = (0..8192)
                .map(|i| (i / 513 != 3 && i % 11 != 0).then_some(i - 4096))
                .collect::<Vec<_>>();
            let decimals = values
                .iter()
                .map(|v| v.map(|n| i128::from(n) * 100))
                .collect();
            let fixture = Fixture::new(batch(keys, values, decimals), EnabledStatistics::None);
            for sql in [
                "SELECT k, SUM(v), AVG(v), COUNT(*), COUNT(v), SUM(d), AVG(d), SUM(v) FROM t GROUP BY k",
                "SELECT k, AVG(d), SUM(d), AVG(d) FROM t GROUP BY k",
                "SELECT k, SUM(d) FROM t GROUP BY k",
                "SELECT k, COUNT(*) FROM t GROUP BY k",
                "SELECT k FROM t GROUP BY k",
            ] {
                let expected = fixture.run(sql, 1).unwrap();
                for workers in [1, 2, 4, 10] {
                    let actual = fixture.run(sql, workers).unwrap();
                    assert_bits(&actual.batch, &expected.batch);
                    assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
                    if workers > 1 {
                        // A monotone leaf has neither a dense index nor hash
                        // routing, even for sparse keys; only run states live.
                        assert_eq!(actual.metrics.dense_group_index_peak_bytes, 0);
                        assert_eq!(actual.metrics.pipeline_source_routes, 0);
                        assert_eq!(actual.metrics.source_boundary_merges, workers - 1);
                    }
                    assert_eq!(actual.metrics.auto_spill_retries, 0);
                }
            }
        }
    }
}

#[test]
fn one_inversion_or_reappearing_key_retains_indexed_grouping() {
    for (first, second) in [(35, 42), (255, 262)] {
        let mut keys = (0..8192).map(|i| Some(i / 7)).collect::<Vec<_>>();
        keys.swap(first, second);
        let fixture = Fixture::new(
            batch(keys, vec![Some(1); 8192], vec![None; 8192]),
            EnabledStatistics::None,
        );
        let sql = "SELECT k, SUM(v), AVG(d), COUNT(*) FROM t GROUP BY k";
        let expected = fixture.run(sql, 1).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = fixture.run(sql, workers).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
            if workers > 1 {
                assert!(actual.metrics.dense_group_index_peak_bytes > 0);
            }
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
    }
}

#[test]
fn run_declines_coercions_distinct_and_empty_join_inputs_to_serial_semantics() {
    let fixture = Fixture::new(
        batch(
            (0..4096).map(|i| Some(i / 7)).collect(),
            vec![Some(1); 4096],
            vec![Some(100); 4096],
        ),
        EnabledStatistics::None,
    );
    // Decimal arithmetic continues to exercise these scale coercions,
    // independently of explicit DECIMAL cast support.
    for sql in [
        "SELECT k, SUM(v + (d + 0.00)) FROM t GROUP BY k",
        "SELECT k + 0.00, SUM(v) FROM t GROUP BY k + 0.00",
        "SELECT k, SUM(DISTINCT v) FROM t GROUP BY k",
        "SELECT a.k, SUM(b.v) FROM t a JOIN (SELECT k,v FROM t WHERE v < 0) b ON a.k=b.k GROUP BY a.k",
        "SELECT a.k, SUM(b.v) FROM (SELECT k,v FROM t WHERE v < 0) a JOIN t b ON a.k=b.k GROUP BY a.k",
    ] {
        let expected = fixture.run(sql, 1).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = fixture.run(sql, workers).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.pipeline_producers, 0);
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
    }
}

#[test]
fn numeric_leaf_owners_preserve_local_encounters_boundary_duplicates_and_null_totals() {
    for descending in [false, true] {
        for sparse in [false, true] {
            // Each row group is internally unsorted. Adjacent groups share
            // only their endpoint, including across every ownership boundary.
            let keys = (0..8192)
                .map(|i| {
                    let k = (i / 256) * 4 + [2, 0, 1, 4, 3, 2, 4, 0][i % 8];
                    let k = k as i64 * if sparse { 10_000_000 } else { 1 };
                    Some(if descending { -k } else { k })
                })
                .collect::<Vec<_>>();
            let values = (0..8192)
                .map(|i| (i % 8 != 2 && i % 11 != 0).then_some(i as i64 - 4096))
                .collect::<Vec<_>>();
            let decimals = values
                .iter()
                .map(|v| v.map(|n| i128::from(n) * 100))
                .collect();
            let fixture = Fixture::new(batch(keys, values, decimals), EnabledStatistics::Chunk);
            let sql = "SELECT k, COUNT(*), COUNT(v), SUM(v), AVG(v), SUM(d), AVG(d), SUM(v) FROM t GROUP BY k";
            let expected = fixture.run(sql, 1).unwrap();
            // The key seen third in each physical group has only NULL inputs.
            assert!(expected.batch.column(3).null_count() > 0);
            for workers in [1, 2, 4, 10] {
                let actual = fixture.run(sql, workers).unwrap();
                assert_bits(&actual.batch, &expected.batch);
                assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
                if workers > 1 {
                    assert_eq!(
                        actual.metrics.source_boundary_merges,
                        actual.metrics.pipeline_producers - 1
                    );
                    assert_eq!(actual.metrics.pipeline_source_routes, 0);
                    assert_eq!(actual.metrics.dense_group_rebuilds, 0);
                    assert_eq!(actual.metrics.rows_read, 8192);
                }
                assert!(actual.metrics.dense_group_index_peak_bytes <= 32 << 20);
                assert!(
                    actual.metrics.peak_reserved_bytes <= fixture.options(workers).memory_bytes
                );
                assert!(actual.metrics.worker_threads_peak <= workers);
                assert_eq!(actual.metrics.auto_spill_retries, 0);
            }
        }
    }
}

#[test]
fn numeric_leaf_observes_keys_without_statistics_and_declines_overlap_nulls_and_empty() {
    for shape in 0..4 {
        let rows = if shape == 3 { 0 } else { 4096 };
        let keys = (0..rows)
            .map(|i| match shape {
                0 => Some(i as i64 / 7),
                1 => Some((i % 13) as i64),
                _ => (i % 131 != 0).then_some(i as i64 / 7),
            })
            .collect();
        let fixture = Fixture::new(
            batch(keys, vec![None; rows], vec![None; rows]),
            EnabledStatistics::None,
        );
        let sql = "SELECT k, SUM(v), AVG(d), COUNT(v), COUNT(*) FROM t GROUP BY k";
        let expected = fixture.run(sql, 1).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = fixture.run(sql, workers).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(
                actual.metrics.pipeline_producers > 0,
                shape == 0 && workers > 1
            );
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
    }
}

#[test]
fn numeric_leaf_keeps_wide_decimal_cancellation_and_serial_error_precedence() {
    let high = 10_i128.pow(38) - 1;
    for cancellation in [false, true] {
        let decimals = (0..4096)
            .map(|i| {
                Some(if cancellation && i % 4 >= 2 {
                    -high
                } else {
                    high
                })
            })
            .collect();
        let fixture = Fixture::new(
            batch(vec![Some(0); 4096], vec![Some(i64::MAX); 4096], decimals),
            EnabledStatistics::Chunk,
        );
        for sql in [
            "SELECT k, SUM(d), AVG(d), COUNT(*) FROM t GROUP BY k",
            "SELECT k, SUM(v), SUM(d) FROM t GROUP BY k",
            "SELECT k, SUM(d), SUM(v) FROM t GROUP BY k",
            "SELECT k, SUM(CAST(d AS BIGINT)) FROM t GROUP BY k",
            "SELECT k, SUM(d / CAST(0 AS DECIMAL(1,0))) FROM t GROUP BY k",
        ] {
            let expected = fixture.run(sql, 1);
            if sql == "SELECT k, SUM(CAST(d AS BIGINT)) FROM t GROUP BY k" {
                // Even a completed zero cannot erase an earlier INTEGER
                // precision error; this expression must stay on today's path.
                assert!(expected.is_err());
            }
            for workers in [1, 2, 4, 10] {
                let actual = fixture.run(sql, workers);
                match (&expected, &actual) {
                    (Ok(expected), Ok(actual)) => {
                        assert_bits(&actual.batch, &expected.batch);
                        assert_eq!(actual.metrics.auto_spill_retries, 0);
                        if sql == "SELECT k, SUM(d), AVG(d), COUNT(*) FROM t GROUP BY k" {
                            assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
                        }
                    }
                    (Err(expected), Err(actual)) => {
                        assert_eq!(actual, expected, "workers={workers}: {sql}")
                    }
                    _ => panic!("serial/parallel status differs, workers={workers}: {sql}"),
                }
            }
        }
    }
}

#[test]
fn numeric_leaf_never_reassociates_float_sum() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![0; 4096])),
        Arc::new(Float64Array::from_iter(
            (0..4096).map(|i| Some([1e16, 1., -1e16, -0.][i % 4])),
        )),
    ];
    let fixture = Fixture::new(
        RecordBatch::try_new(schema, columns).unwrap(),
        EnabledStatistics::Chunk,
    );
    let sql = "SELECT k, SUM(v), AVG(v) FROM t GROUP BY k";
    let expected = fixture.run(sql, 1);
    for workers in [1, 2, 4, 10] {
        let actual = fixture.run(sql, workers);
        match (&expected, &actual) {
            (Ok(expected), Ok(actual)) => {
                assert_bits(&actual.batch, &expected.batch);
                assert_eq!(actual.metrics.pipeline_producers, 0);
            }
            (Err(expected), Err(actual)) => assert_eq!(actual, expected),
            _ => panic!("float SUM status differs at {workers} workers"),
        }
    }
}

// Rewrite only the footer: decoded pages remain unchanged. The forged bounds
// promise disjoint, non-NULL owners, so only the decoded proof can reject them.
fn forge_disjoint_statistics(fixture: &Fixture) {
    let path = &fixture.tables["t"];
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let metadata = reader.metadata();
    let groups = metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(rank, group)| {
            let mut columns = group.columns().to_vec();
            columns[0] = columns[0]
                .clone()
                .into_builder()
                .set_statistics(Statistics::int64(
                    Some(rank as i64 * 100),
                    Some(rank as i64 * 100 + 99),
                    None,
                    Some(0),
                    false,
                ))
                .build()
                .unwrap();
            group
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();
    let metadata = metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_row_groups(groups)
        .build();
    drop(reader);
    let bytes = fs::read(path).unwrap();
    let footer_size =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    let mut output = TrackedWrite::new(File::create(path).unwrap());
    output
        .write_all(&bytes[..bytes.len() - 8 - footer_size])
        .unwrap();
    ParquetMetaDataWriter::new_with_tracked(output, &metadata)
        .finish()
        .unwrap();
}

#[test]
fn numeric_leaf_never_authorizes_ownership_from_misleading_statistics() {
    for null_keys in [false, true] {
        let keys = (0..8192)
            .map(|i| {
                if null_keys {
                    (i % 131 != 0).then_some(i as i64 / 7)
                } else {
                    Some((i % 13) as i64)
                }
            })
            .collect();
        let fixture = Fixture::new(
            batch(keys, vec![Some(1); 8192], vec![None; 8192]),
            EnabledStatistics::None,
        );
        let sql = "SELECT k, SUM(v), COUNT(*), AVG(d) FROM t GROUP BY k";
        let expected = fixture.run(sql, 1).unwrap();
        forge_disjoint_statistics(&fixture);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&fixture.tables["t"]).unwrap())
                .unwrap();
        for (rank, group) in reader.metadata().row_groups().iter().enumerate() {
            let Some(Statistics::Int64(stats)) = group.column(0).statistics() else {
                panic!("forged statistics missing");
            };
            assert_eq!(stats.min_opt(), Some(&(rank as i64 * 100)));
            assert_eq!(stats.max_opt(), Some(&(rank as i64 * 100 + 99)));
        }
        drop(reader);
        for workers in [1, 2, 4, 10] {
            let actual = fixture.run(sql, workers).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.pipeline_producers, 0);
            assert_eq!(actual.metrics.auto_spill_retries, 0);
        }
    }
}
