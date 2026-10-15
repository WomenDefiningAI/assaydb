use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};

struct Fixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("assaydb-lane-fusion-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let mut fixture = Self {
            root,
            tables: HashMap::new(),
        };
        for (name, keys) in [
            (
                "p",
                (0..73).map(|i| (i % 11 != 0).then_some(i % 9)).collect(),
            ),
            ("b", (0..7).map(Some).chain([None, None]).collect()),
            ("c", vec![Some(0), Some(2), Some(3), Some(6), None]),
            ("d", vec![Some(0), Some(3), Some(6)]),
            ("dup", (0..43).map(|i| Some(i % 3)).collect()),
            ("empty", vec![]),
        ] {
            fixture.write(name, keys);
        }
        fixture
    }
    fn write(&mut self, name: &str, keys: Vec<Option<i64>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(Int64Array::from_iter_values(0..keys.len() as i64)),
            Arc::new(StringArray::from_iter(
                keys.iter().map(|k| k.map(|k| format!("é\0{k}"))),
            )),
            Arc::new(Float64Array::from_iter(
                keys.iter().map(|k| k.map(|k| k as f64)),
            )),
        ];
        let path = self.root.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(11)
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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn lane_chains_equal_unfused_raw_order_across_workers_and_batches() {
    let fixture = Fixture::new();
    let queries = [
        ("SELECT p.id,b.id FROM p JOIN b ON p.k=b.k", true),
        ("SELECT p.id,b.id FROM p JOIN b ON p.s=b.s", true),
        ("SELECT p.id,b.id FROM p JOIN b ON p.f=b.f", true),
        (
            "SELECT p.id,b.id FROM p JOIN b ON p.k=b.k AND p.s=b.s",
            true,
        ),
        (
            "SELECT p.id,b.id FROM p JOIN b ON p.k=b.k AND p.id=b.id",
            true,
        ),
        ("SELECT p.id FROM p SEMI JOIN b ON p.k=b.k", true),
        ("SELECT p.id FROM p ANTI JOIN b ON p.k=b.k", true),
        ("SELECT p.id FROM p SEMI JOIN b ON p.s=b.s", true),
        ("SELECT p.id FROM p ANTI JOIN b ON p.f=b.f", true),
        ("SELECT p.id,b.id FROM p JOIN b ON p.k=b.f", false),
        (
            "SELECT p.id,b.id,c.id,d.id FROM p JOIN b ON p.k=b.k JOIN c ON p.k=c.k JOIN d ON p.k=d.k",
            true,
        ),
        (
            "SELECT p.id,b.id,c.id FROM p JOIN b ON p.k=b.k JOIN c ON p.k=c.k AND b.id=c.id",
            true,
        ),
        (
            "SELECT x.id,c.id FROM (SELECT p.id,p.k FROM p JOIN b ON p.k=b.k WHERE p.id>5) x JOIN c ON x.k=c.k",
            true,
        ),
        ("SELECT p.id,b.id FROM p JOIN empty b ON p.k=b.k", true),
        ("SELECT p.id FROM p SEMI JOIN empty b ON p.k=b.k", true),
        ("SELECT p.id FROM p ANTI JOIN empty b ON p.k=b.k", true),
        ("SELECT p.id,b.id FROM empty p JOIN b ON p.k=b.k", true),
        ("SELECT p.id,b.id FROM p JOIN dup b ON p.k=b.k", false),
        (
            "SELECT p.id,x.id,b.id FROM p JOIN dup x ON p.k=x.k JOIN b ON p.k=b.k",
            false,
        ),
        (
            "SELECT p.id,b.id FROM p LEFT JOIN b ON p.k=b.k ORDER BY 1,2",
            false,
        ),
        (
            "SELECT p.id,b.id FROM p JOIN b ON p.k=b.k AND p.id>b.id",
            false,
        ),
        ("SELECT p.id,b.id FROM p JOIN b ON p.k+1=b.k", false),
    ];
    for (sql, fused) in queries {
        let base = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        // Retain this exact physical plan while changing only execution options.
        let prepared = prepare(sql, &fixture.tables, &base).unwrap();
        let expected = execute(&prepared, &base).unwrap();
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations: true,
                    ..base.clone()
                };
                let result = execute(&prepared, &options).unwrap();
                assert_eq!(
                    result.batch, expected.batch,
                    "{sql}; workers={workers}, batch={batch_size}"
                );
                if fused && workers > 1 {
                    assert!(result.metrics.lane_fused_joins > 0, "not fused: {sql}");
                } else {
                    assert_eq!(
                        result.metrics.lane_fused_joins, 0,
                        "unexpected fusion: {sql}"
                    );
                }
                if sql.contains("JOIN d ON") && workers > 1 {
                    assert_eq!(result.metrics.lane_fused_joins, 3);
                }
                assert!(result.metrics.worker_threads_peak <= workers);
            }
        }
    }
    for sql in [
        "SELECT p.id/0,b.id FROM p JOIN b ON p.k=b.k",
        "SELECT x.id,b.id FROM (SELECT id,k FROM p WHERE id*9223372036854775807*9223372036854775807>0) x JOIN b ON x.k=b.k",
        "SELECT p.id,b.id FROM p JOIN (SELECT k,id*9223372036854775807*9223372036854775807 AS id FROM b) b ON p.k=b.k",
    ] {
        let base = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        let prepared = prepare(sql, &fixture.tables, &base).unwrap();
        let expected = execute(&prepared, &base);
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    relational_optimizations: true,
                    ..base.clone()
                };
                let actual = execute(&prepared, &options);
                match (&expected, actual) {
                    (Ok(expected), Ok(actual)) => assert_eq!(actual.batch, expected.batch),
                    (Err(expected), Err(actual)) => assert_eq!(&actual, expected, "{sql}"),
                    _ => panic!("changed success/error: {sql}"),
                }
            }
        }
    }
    // Exercise the existing cached-reader row filter through an admitted lane
    // chain. Retaining pushdown is a dispatch assertion, separate from equality.
    let sql = "SELECT p.id,b.id FROM (SELECT id,k FROM p WHERE id=7) p JOIN b ON p.k=b.k";
    let base = Options {
        workers: 1,
        relational_optimizations: false,
        ..Options::default()
    };
    let prepared = prepare(
        sql,
        &fixture.tables,
        &Options {
            relational_optimizations: true,
            ..base.clone()
        },
    )
    .unwrap();
    let expected = execute(&prepared, &base).unwrap();
    for workers in [2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let result = execute(
                &prepared,
                &Options {
                    workers,
                    batch_size,
                    relational_optimizations: true,
                    parquet_cache_bytes: Some(1 << 20),
                    ..base.clone()
                },
            )
            .unwrap();
            assert_eq!(result.batch, expected.batch);
            assert!(result.metrics.lane_fused_joins > 0);
            assert!(result.metrics.static_scan_filters > 0);
        }
    }
}
