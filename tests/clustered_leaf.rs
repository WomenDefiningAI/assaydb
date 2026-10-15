//! Production relational leaf admission and serial chronology at every width.
use arrow::{
    array::{Decimal128Array, Float64Array, Int64Array},
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

fn clustered_run(
    fixture: &Fixture,
    sql: &str,
    workers: usize,
    baseline: bool,
) -> Result<Execution, String> {
    let options = Options {
        workers,
        batch_size: 7,
        spill: None,
        relational_optimizations: !baseline,
        ..fixture.options(workers)
    };
    execute(&rel::prepare(sql, &fixture.tables, &options)?, &options)
}

#[test]
fn clustered_metadata_preserves_first_seen_order_null_totals_and_having_at_every_width() {
    // Row-group-local order differs from numeric sort order, and groups cross batches.
    let rows = 2048;
    let keys = (0..rows)
        .map(|i| Some((i / 256 * 3 + [2, 0, 1, 2][i % 4]) as i64))
        .collect();
    let values: Vec<_> = (0..rows)
        .map(|i| (i % 4 != 2).then_some(i as i64 - 512))
        .collect();
    let fixture = Fixture::new(
        batch(
            keys,
            values.clone(),
            values.iter().map(|v| v.map(i128::from)).collect(),
        ),
        EnabledStatistics::Chunk,
    );
    for sql in [
        "SELECT k, SUM(v), COUNT(v), COUNT(*), SUM(d), SUM(v) FROM t GROUP BY k",
        "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k HAVING SUM(v) > 0",
        "SELECT k, SUM(d), COUNT(*) FROM t GROUP BY k HAVING SUM(d) > 0",
        "SELECT k, SUM(v) FROM t GROUP BY k HAVING FALSE",
        "SELECT k, SUM(v) FROM t GROUP BY k HAVING 0 < SUM(v)",
        "SELECT k FROM t GROUP BY k HAVING SUM(v) > 0 LIMIT 3",
    ] {
        let expected = clustered_run(&fixture, sql, 1, true).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = clustered_run(&fixture, sql, workers, false).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(
                actual.metrics.clustered_row_groups, 8,
                "workers={workers}: {sql}"
            );
            assert_eq!(actual.metrics.rows_read, rows);
        }
    }
}

#[test]
fn clustered_invalid_metadata_replays_from_scratch_without_duplicate_contributions() {
    for null_keys in [false, true] {
        // Early groups partly fit forged ranges; a later mismatch must discard all prior work.
        let keys = (0..2048)
            .map(|i| {
                if i < 512 {
                    Some((i / 256 * 100 + i % 31) as i64)
                } else if null_keys && i % 17 == 0 {
                    None
                } else {
                    Some((i % 13) as i64)
                }
            })
            .collect();
        let fixture = Fixture::new(
            batch(keys, vec![Some(1); 2048], vec![None; 2048]),
            EnabledStatistics::Chunk,
        );
        let sql = "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k HAVING SUM(v) > 2";
        let expected = clustered_run(&fixture, sql, 1, true).unwrap();
        forge_disjoint_statistics(&fixture);
        for workers in [1, 2, 4, 10] {
            let actual = clustered_run(&fixture, sql, workers, false).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.clustered_row_groups, 0);
        }
    }
}

#[test]
fn clustered_having_never_hides_overflow_or_changes_error_precedence() {
    let rows = 2048;
    let keys = (0..rows)
        .map(|i| Some((i / 256 * 3 + i % 3) as i64))
        .collect();
    let decimals = (0..rows)
        .map(|i| Some(if i < 256 { 1 } else { 10_i128.pow(37) }))
        .collect();
    let fixture = Fixture::new(
        batch(keys, vec![Some(i64::MAX); rows], decimals),
        EnabledStatistics::Chunk,
    );
    for sql in [
        "SELECT k, SUM(d) FROM t GROUP BY k HAVING FALSE",
        "SELECT k, SUM(d) FROM t GROUP BY k HAVING SUM(d) < 0",
        "SELECT k, SUM(v), SUM(d) FROM t GROUP BY k HAVING SUM(v) < 0",
        "SELECT k, SUM(d), SUM(v) FROM t GROUP BY k HAVING SUM(v) < 0",
        "SELECT k, SUM(d) FROM t GROUP BY k LIMIT 1",
        "SELECT k, SUM(v), SUM(d) FROM t GROUP BY k",
    ] {
        let expected = clustered_run(&fixture, sql, 1, true).err().unwrap();
        for workers in [1, 2, 4, 10] {
            assert_eq!(
                clustered_run(&fixture, sql, workers, false).err().unwrap(),
                expected,
                "{sql}"
            );
        }
    }
}

#[test]
fn clustered_declines_nonlocal_or_fallible_shapes_and_handles_empty_joins() {
    let keys = (0..1024)
        .map(|i| Some((i / 256 * 3 + i % 3) as i64))
        .collect();
    let fixture = Fixture::new(
        batch(keys, vec![Some(1); 1024], vec![Some(1); 1024]),
        EnabledStatistics::Chunk,
    );
    for sql in [
        "SELECT k, v, SUM(d) FROM t GROUP BY k, v",
        "SELECT CAST(k AS TEXT), SUM(v) FROM t GROUP BY CAST(k AS TEXT)",
        "SELECT k, SUM(CAST(v AS REAL)) FROM t GROUP BY k",
        "SELECT k, SUM(v + 1) FROM t GROUP BY k",
        "SELECT k, SUM(v) FROM t WHERE v > 0 GROUP BY k",
        "SELECT k, SUM(v) FROM t GROUP BY k HAVING SUM(v) * SUM(v) > 0",
        "SELECT a.k FROM (SELECT k, SUM(v) AS n FROM t GROUP BY k HAVING SUM(v) > 0) a JOIN (SELECT k FROM t WHERE FALSE) b ON a.k=b.k",
        "SELECT a.k FROM (SELECT k FROM t WHERE FALSE) a JOIN (SELECT k, SUM(v) AS n FROM t GROUP BY k HAVING SUM(v) > 0) b ON a.k=b.k",
    ] {
        let expected = clustered_run(&fixture, sql, 1, true);
        for workers in [1, 2, 4, 10] {
            match (&expected, clustered_run(&fixture, sql, workers, false)) {
                (Ok(expected), Ok(actual)) => assert_bits(&actual.batch, &expected.batch),
                (Err(expected), Err(actual)) => assert_eq!(&actual, expected),
                (expected, actual) => panic!(
                    "status mismatch for {sql}: {:?} / {:?}",
                    expected.as_ref().err(),
                    actual.err()
                ),
            }
        }
    }
}

#[test]
fn shared_boundaries_and_singleton_chains_match_serial_with_having_and_limit() {
    let rows = 8 * 256;
    // A shared high endpoint is encountered before other keys in its first
    // owner, and again after new keys in the next non-singleton owner.
    let domains: [&[i64]; 8] = [
        &[2, 0, 1, 2],
        &[2],
        &[4, 2, 3, 4],
        &[4],
        &[6, 4, 5, 6],
        &[6],
        &[8, 6, 7, 8],
        &[9, 10],
    ];
    let keys = (0..rows)
        .map(|i| Some(domains[i / 256][i % domains[i / 256].len()]))
        .collect();
    let values: Vec<_> = (0..rows)
        .map(|i| {
            let key = domains[i / 256][i % domains[i / 256].len()];
            (key != 1 && i % 11 != 0).then_some(i as i64 - 1024)
        })
        .collect();
    let fixture = Fixture::new(
        batch(
            keys,
            values.clone(),
            values.iter().map(|v| v.map(i128::from)).collect(),
        ),
        EnabledStatistics::Chunk,
    );
    for sql in [
        "SELECT k, SUM(v), COUNT(v), COUNT(*), SUM(d), SUM(v) FROM t GROUP BY k",
        "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k HAVING SUM(v) > 0",
        "SELECT k, SUM(d), COUNT(*) FROM t GROUP BY k HAVING SUM(d) < 0",
        "SELECT k FROM t GROUP BY k HAVING SUM(v) > 0 LIMIT 3",
        "SELECT k, SUM(v) FROM t GROUP BY k HAVING FALSE",
    ] {
        let expected = clustered_run(&fixture, sql, 1, true).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = clustered_run(&fixture, sql, workers, false).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.clustered_row_groups, 8, "{sql}");
            assert_eq!(actual.metrics.rows_read, rows);
        }
    }
}

#[test]
fn boundary_decimal_partials_remain_wide_until_the_key_completes() {
    let rows = 4 * 256;
    for cancels in [false, true] {
        let decimals = (0..rows)
            .map(|i| {
                Some(if cancels && i >= rows / 2 {
                    -10_i128.pow(37)
                } else {
                    10_i128.pow(37)
                })
            })
            .collect();
        let fixture = Fixture::new(
            batch(vec![Some(7); rows], vec![Some(i64::MAX); rows], decimals),
            EnabledStatistics::Chunk,
        );
        for sql in [
            "SELECT k, SUM(d), COUNT(*) FROM t GROUP BY k",
            "SELECT k, SUM(d) FROM t GROUP BY k HAVING FALSE",
            "SELECT k, SUM(d) FROM t GROUP BY k HAVING SUM(d) < 0",
        ] {
            let expected = clustered_run(&fixture, sql, 1, true);
            for workers in [1, 2, 4, 10] {
                let actual = clustered_run(&fixture, sql, workers, false);
                if cancels {
                    let actual = actual.unwrap();
                    assert_bits(&actual.batch, &expected.as_ref().unwrap().batch);
                    assert_eq!(actual.metrics.clustered_row_groups, 4);
                } else {
                    assert_eq!(actual.err().unwrap(), "decimal precision 38 overflow");
                    assert_eq!(
                        expected.as_ref().err().unwrap(),
                        "decimal precision 38 overflow"
                    );
                }
            }
        }
    }
}

#[test]
fn clustered_barrier_error_survives_fallback_at_every_worker_and_batch_width() {
    // Review reproducer: the earlier group survives HAVING and would divide by
    // zero downstream; the later, rejected group's completion must fail first.
    let rows = 1024;
    let fixture = Fixture::new(
        batch(
            (0..rows).map(|i| Some(i64::from(i >= 512))).collect(),
            (0..rows).map(|i| Some(i64::from(i < 512))).collect(),
            (0..rows)
                .map(|i| Some(if i < 512 { 0 } else { 10_i128.pow(38) - 1 }))
                .collect(),
        ),
        EnabledStatistics::Chunk,
    );
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                batch_size,
                spill: None,
                ..fixture.options(workers)
            };
            // Explicit exact DECIMAL casts now bind. Both original spellings
            // must preserve the grouped completion error before downstream division.
            for sql in [
                "SELECT SUM(v) / CAST(0 AS DECIMAL(1,0)), SUM(d) FROM t GROUP BY k HAVING SUM(v) > 0",
                "SELECT SUM(v) / CAST('0' AS DECIMAL(1,0)), SUM(d) FROM t GROUP BY k HAVING SUM(v) > 0",
                "SELECT SUM(d) FROM t GROUP BY k HAVING FALSE",
            ] {
                let prepared = rel::prepare(sql, &fixture.tables, &options).unwrap();
                assert_eq!(
                    execute(&prepared, &options).err().unwrap(),
                    "decimal precision 38 overflow",
                    "workers={workers}, batch={batch_size}: {sql}",
                );
            }
        }
    }
}

#[test]
fn clustered_later_invalid_bounds_discard_speculative_completion_error() {
    // Forged disjoint bounds hide a repeated key in the last row group. Its
    // negative values cancel the first group's apparent overflow. Returning
    // that speculative overflow before validating the last group is unsound.
    let rows = 1024;
    let fixture = Fixture::new(
        batch(
            (0..rows)
                .map(|i| Some(if i >= 768 { 0 } else { (i / 256 * 100) as i64 }))
                .collect(),
            vec![Some(1); rows],
            (0..rows)
                .map(|i| {
                    Some(match i / 256 {
                        0 => 10_i128.pow(38) - 1,
                        3 => -(10_i128.pow(38) - 1),
                        _ => 0,
                    })
                })
                .collect(),
        ),
        EnabledStatistics::Chunk,
    );
    forge_disjoint_statistics(&fixture);
    let sql = "SELECT k, SUM(d) FROM t GROUP BY k";
    let expected = clustered_run(&fixture, sql, 1, true).unwrap();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                batch_size,
                spill: None,
                ..fixture.options(workers)
            };
            let prepared = rel::prepare(sql, &fixture.tables, &options).unwrap();
            let actual = execute(&prepared, &options).unwrap();
            assert_bits(&actual.batch, &expected.batch);
            assert_eq!(actual.metrics.clustered_row_groups, 0);
        }
    }
}

#[test]
fn clustered_invalid_bounds_replay_checks_rejected_groups_before_downstream_errors() {
    // The last row group invalidates clustered ownership without cancelling
    // key 100's overflow. Fresh canonical HAVING replay must check it before
    // projecting the earlier surviving group's division by zero.
    let rows = 1024;
    let fixture = Fixture::new(
        batch(
            (0..rows)
                .map(|i| {
                    Some(if i >= 768 {
                        299
                    } else {
                        (i / 256 * 100) as i64
                    })
                })
                .collect(),
            (0..rows).map(|i| Some(i64::from(i < 256))).collect(),
            (0..rows)
                .map(|i| {
                    Some(if (256..512).contains(&i) {
                        10_i128.pow(38) - 1
                    } else {
                        0
                    })
                })
                .collect(),
        ),
        EnabledStatistics::Chunk,
    );
    forge_disjoint_statistics(&fixture);
    let sql =
        "SELECT SUM(v) / CAST('0' AS DECIMAL(1,0)), SUM(d) FROM t GROUP BY k HAVING SUM(v) > 0";
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                batch_size,
                spill: None,
                ..fixture.options(workers)
            };
            let prepared = rel::prepare(sql, &fixture.tables, &options).unwrap();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                "decimal precision 38 overflow",
                "workers={workers}, batch={batch_size}",
            );
        }
    }
}
