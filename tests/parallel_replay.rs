use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn shared_scan_aliases_subset_first_and_optimizer_controls_match() {
    let root = std::env::temp_dir().join(format!("assaydb-replay-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(
            (0..1024).map(|i| (i % 11 != 0).then_some(i % 97)),
        )),
        Arc::new(Int64Array::from_iter(
            (0..1024).map(|i| (i % 13 != 0).then_some(i % 17)),
        )),
        Arc::new(StringArray::from_iter((0..1024).map(|i| {
            if i % 7 == 0 {
                None
            } else if i % 2 == 0 {
                Some("é\0")
            } else {
                Some("🦆")
            }
        }))),
    ];
    let path = root.join("t.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for sql in [
        "SELECT a.x,COUNT(*) AS n FROM (SELECT x,y FROM t WHERE y<4) a JOIN (SELECT x,y FROM t WHERE y>9) b ON a.x=b.x GROUP BY a.x ORDER BY 1",
        "SELECT a.x,COUNT(*) AS joined_n FROM t a JOIN t b ON a.x=b.x WHERE a.y<4 AND b.y>9 GROUP BY a.x ORDER BY 1",
        "SELECT a.x,SUM(b.y) FROM (SELECT x FROM t WHERE y<4) a JOIN t b ON a.x=b.x WHERE b.y>9 GROUP BY a.x ORDER BY 1",
        "SELECT a.x,COUNT(b.s) FROM t a LEFT JOIN t b ON a.x=b.x AND b.y>9 WHERE a.y<4 GROUP BY a.x ORDER BY 1",
        "WITH c AS (SELECT x,y,s FROM t) SELECT a.x,COUNT(*) FROM c a JOIN c b ON a.x=b.x WHERE a.y<4 AND b.y>9 GROUP BY a.x ORDER BY 1",
        "SELECT a.x FROM t a WHERE a.y<4 AND EXISTS (SELECT 1 FROM t b WHERE b.x=a.x AND b.y>9) ORDER BY 1",
    ] {
        let base = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
        for batch_size in [32, 256] {
            for workers in [1, 2, 4, 10] {
                for projection_pushdown in [false, true] {
                    let options = Options {
                        batch_size,
                        workers,
                        projection_pushdown,
                        ..Options::default()
                    };
                    let result =
                        execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                    assert_eq!(
                        result.batch, expected.batch,
                        "{sql} workers{workers} batch{batch_size} pushdown{projection_pushdown}"
                    );
                    assert!(result.metrics.worker_threads_peak < workers);
                    if workers == 1 {
                        assert_eq!(result.metrics.parallel_replays, 0);
                    }
                    if sql.contains("AS n")
                        && batch_size == 32
                        && workers >= 4
                        && projection_pushdown
                    {
                        assert!(
                            result.metrics.parallel_replays > 0,
                            "parallel replay not exercised: workers={workers}, metrics={:?}, plan={:?}",
                            result.metrics,
                            prepare(sql, &tables, &options).unwrap().relational
                        );
                        assert!(result.metrics.replay_exact_filter_batches > 0);
                        assert!(result.metrics.scan_reuses > 0);
                    }
                }
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
