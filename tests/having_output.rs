//! HAVING state selection must equal the unfused serial output/filter pipeline.
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Execution, Options, execute},
    rel,
};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    collections::HashMap,
    fs::{self, File},
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
    fn new(keys: Vec<Option<i64>>, values: Vec<Option<i64>>, decimals: Vec<Option<i128>>) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "assaydb-having-output-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new("d", DataType::Decimal128(38, 2), true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(
                Decimal128Array::from(decimals)
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let path = root.join("t.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(256)
                    .set_statistics_enabled(EnabledStatistics::None)
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
    fn run(
        &self,
        sql: &str,
        workers: usize,
        batch_size: usize,
        optimized: bool,
    ) -> Result<Execution, String> {
        let options = Options {
            workers,
            batch_size,
            relational_optimizations: optimized,
            memory_bytes: 64 << 20,
            ..Options::default()
        };
        execute(&rel::prepare(sql, &self.tables, &options)?, &options)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn having_selection_preserves_nulls_duplicates_chunks_and_leaf_boundaries() {
    for nullable in [false, true] {
        let keys = (0..8192)
            .map(|i| (!nullable || i % 131 != 0).then_some(i / 3))
            .collect();
        let values: Vec<_> = (0..8192)
            .map(|i| (i / 3 % 11 != 0).then_some(if i / 3 % 97 == 0 { 1000 } else { -3 }))
            .collect();
        let decimals = values
            .iter()
            .map(|v| v.map(|n| i128::from(n) * 100))
            .collect();
        let fixture = Fixture::new(keys, values, decimals);
        for sql in [
            "SELECT k,SUM(v),COUNT(v),COUNT(*),SUM(v) FROM t GROUP BY k HAVING SUM(v)>300",
            "SELECT k,SUM(d) FROM t GROUP BY k HAVING 3<SUM(d)",
            "SELECT k,COUNT(v),SUM(v) FROM t GROUP BY k HAVING COUNT(v)=0",
        ] {
            for batch_size in [37, 8192] {
                let expected = fixture.run(sql, 1, batch_size, false).unwrap();
                for workers in [1, 2, 4, 10] {
                    let actual = fixture.run(sql, workers, batch_size, true).unwrap();
                    assert_eq!(
                        actual.batch, expected.batch,
                        "workers={workers}, chunk={batch_size}: {sql}"
                    );
                    assert!(
                        actual.metrics.having_input_groups > actual.metrics.having_output_groups
                    );
                    assert_eq!(actual.metrics.having_output_groups, actual.batch.num_rows());
                    assert_eq!(
                        actual.metrics.pipeline_producers > 0,
                        !nullable && workers > 1
                    );
                    assert!(actual.metrics.peak_reserved_bytes <= 64 << 20);
                }
            }
        }
    }
}

#[test]
fn having_empty_scalar_and_grouped_inputs_and_all_null_sum_match_serial() {
    for empty in [false, true] {
        let count = if empty { 0 } else { 5 };
        let fixture = Fixture::new(vec![None; count], vec![None; count], vec![None; count]);
        for sql in [
            "SELECT k,SUM(v),COUNT(v) FROM t GROUP BY k HAVING COUNT(v)=0",
            "SELECT k,SUM(v) FROM t GROUP BY k HAVING SUM(v)=0",
            "SELECT SUM(v),COUNT(*) FROM t HAVING COUNT(*)=0",
            "SELECT SUM(v) FROM t HAVING SUM(v)>0",
            "SELECT k,SUM(v) FROM t WHERE FALSE GROUP BY k HAVING SUM(v)>0",
        ] {
            let expected = fixture.run(sql, 1, 2, false).unwrap();
            for workers in [1, 2, 4, 10] {
                let actual = fixture.run(sql, workers, 2, true).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "empty={empty}, workers={workers}: {sql}"
                );
            }
        }
    }
}

#[test]
fn rejected_sum_overflow_and_downstream_chunk_error_keep_serial_precedence() {
    let high = 10_i128.pow(38) - 1;
    let fixture = Fixture::new(
        vec![Some(0), Some(0), Some(1), Some(1)],
        vec![Some(i64::MAX); 4],
        vec![Some(high); 4],
    );
    for sql in [
        "SELECT k FROM t GROUP BY k HAVING SUM(d)<0",
        "SELECT SUM(v),SUM(d) FROM t GROUP BY k HAVING COUNT(*)=0",
        "SELECT SUM(CAST(d AS BIGINT) * 100) FROM t GROUP BY k HAVING COUNT(*)=0",
    ] {
        for batch_size in [1, 3, 8192] {
            let expected = fixture.run(sql, 1, batch_size, false).err().unwrap();
            for workers in [1, 2, 4, 10] {
                assert_eq!(
                    fixture.run(sql, workers, batch_size, true).err().unwrap(),
                    expected,
                    "{workers}/{batch_size}: {sql}"
                );
            }
        }
    }
    // Keep the original chunk boundaries when projection and aggregate output
    // can both fail; compare the exact serial error at each chunk size.
    let fixture = Fixture::new(
        vec![Some(0), Some(0), Some(1), Some(1)],
        vec![Some(i64::MAX); 4],
        vec![Some(0), Some(0), Some(high), Some(high)],
    );
    let sql = "SELECT SUM(v) * 10000000000000000000 FROM t GROUP BY k HAVING COUNT(*)>0";
    // Chunk 0's product has machine overflow; chunk 1's SUM has precision
    // overflow. A larger original chunk must report the SUM failure first.
    let sql_with_later =
        "SELECT SUM(v) * 10000000000000000000,SUM(d) FROM t GROUP BY k HAVING COUNT(*)>0";
    for sql in [sql, sql_with_later] {
        for batch_size in [1, 3] {
            let expected = fixture.run(sql, 1, batch_size, false).err().unwrap();
            for workers in [1, 2, 4, 10] {
                assert_eq!(
                    fixture.run(sql, workers, batch_size, true).err().unwrap(),
                    expected
                );
            }
        }
    }
}

#[test]
fn ordinary_multikey_distinct_aggregates_keep_the_unfused_serial_path() {
    for rows in [0, 513] {
        let fixture = Fixture::new(
            (0..rows).map(|i| (i % 7 != 0).then_some(i % 5)).collect(),
            (0..rows).map(|i| (i % 11 != 0).then_some(i % 3)).collect(),
            (0..rows)
                .map(|i| (i % 13 != 0).then_some(i128::from(i % 17) * 100))
                .collect(),
        );
        for sql in [
            "SELECT CAST(k AS TEXT),v,COUNT(DISTINCT d) FROM t GROUP BY CAST(k AS TEXT),v ORDER BY 3 DESC,1,2",
            "SELECT CAST(a.k AS TEXT),a.v,COUNT(DISTINCT b.d) FROM t a JOIN t b ON a.k=b.k WHERE a.v<>1 GROUP BY CAST(a.k AS TEXT),a.v ORDER BY 3 DESC,1,2",
        ] {
            for batch_size in [3, 256] {
                let expected = fixture.run(sql, 1, batch_size, false).unwrap();
                for workers in [1, 2, 4, 10] {
                    let actual = fixture.run(sql, workers, batch_size, true).unwrap();
                    assert_eq!(
                        actual.batch, expected.batch,
                        "rows={rows}, workers={workers}, chunk={batch_size}: {sql}"
                    );
                    assert_eq!(actual.metrics.having_input_groups, 0);
                    assert_eq!(actual.metrics.having_output_groups, 0);
                }
            }
        }
    }
}

#[test]
fn unsupported_outputs_predicates_and_join_inputs_retain_the_existing_path() {
    let fixture = Fixture::new(
        vec![Some(0), Some(0), None, Some(1)],
        vec![Some(2), Some(3), None, Some(0)],
        vec![Some(200), Some(300), None, Some(0)],
    );
    for sql in [
        "SELECT k,AVG(v) FROM t GROUP BY k HAVING SUM(v)>0",
        "SELECT k,MIN(v) FROM t GROUP BY k HAVING SUM(v)>0",
        "SELECT k,SUM(DISTINCT v) FROM t GROUP BY k HAVING COUNT(*)>0",
        "SELECT k,SUM(v) FROM t GROUP BY k HAVING SUM(v)>0.01",
        "SELECT k,SUM(v) FROM t GROUP BY k HAVING SUM(v)/COUNT(*)>0",
        "SELECT k,SUM(v) FROM t GROUP BY k HAVING SUM(v)>CAST(0 AS REAL)",
        "SELECT CAST(k AS TEXT),SUM(v) FROM t GROUP BY CAST(k AS TEXT) HAVING SUM(v)>0",
        "SELECT a.k,SUM(b.v) FROM t a JOIN t b ON a.k=b.k GROUP BY a.k HAVING SUM(b.v)>0 ORDER BY 1",
        "SELECT a.k,SUM(b.v) FROM t a JOIN (SELECT * FROM t WHERE FALSE) b ON a.k=b.k GROUP BY a.k HAVING SUM(b.v)>0",
        "SELECT a.k,SUM(b.v) FROM (SELECT * FROM t WHERE FALSE) a JOIN t b ON a.k=b.k GROUP BY a.k HAVING SUM(b.v)>0",
    ] {
        let expected = fixture.run(sql, 1, 2, false).unwrap();
        for workers in [1, 2, 4, 10] {
            let actual = fixture.run(sql, workers, 2, true).unwrap();
            assert_eq!(actual.batch, expected.batch, "workers={workers}: {sql}");
            assert_eq!(actual.metrics.having_input_groups, 0, "declined: {sql}");
        }
    }
}
