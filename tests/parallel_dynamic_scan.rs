use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

struct Tables(HashMap<String, PathBuf>);
impl Tables {
    fn new() -> Self {
        let mut tables = Self(HashMap::new());
        for (name, rows) in [("many", 73), ("single", 5), ("empty", 0)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("k", DataType::Int64, true),
                Field::new("f", DataType::Float64, false),
                Field::new("d", DataType::Decimal128(38, 0), false),
                Field::new("s", DataType::Utf8, true),
            ]));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 11 != 0).then_some(i % 5)),
                )),
                Arc::new(Float64Array::from_iter_values((0..rows).map(|i| {
                    match i % 3 {
                        0 => 1e20,
                        1 => 1.0,
                        _ => -1e20,
                    }
                }))),
                Arc::new(
                    Decimal128Array::from_iter_values((0..rows).map(|i| match i % 3 {
                        0 => 10_i128.pow(38) - 1,
                        1 => -(10_i128.pow(38) - 1),
                        _ => 0,
                    }))
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
                ),
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then(|| format!("{}", i % 5))),
                )),
            ];
            let path = std::env::temp_dir().join(format!(
                "assaydb-dynamic-matrix-{}-{name}.parquet",
                std::process::id()
            ));
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(8)
                        .build(),
                ),
            )
            .unwrap();
            writer
                .write(&RecordBatch::try_new(schema, columns).unwrap())
                .unwrap();
            writer.close().unwrap();
            tables.0.insert(name.into(), path);
        }
        tables
    }
}
impl Drop for Tables {
    fn drop(&mut self) {
        for path in self.0.values() {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[test]
fn dynamic_scan_sql_matrix_preserves_nulls_duplicates_coercions_order_and_errors() {
    let tables = Tables::new();
    for relational_optimizations in [false, true] {
        let base = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations,
            ..Options::default()
        };
        for sql in [
            "SELECT id,k,s,f FROM many",
            "SELECT id,k,s FROM single",
            "SELECT id,k,s FROM empty",
            "SELECT id,k,CAST(s AS INTEGER),CAST(k AS REAL) FROM many WHERE k IS NULL OR k < 3",
            "SELECT a.id AS l,b.id AS r FROM many a JOIN single b ON a.k=b.k ORDER BY l,r",
            "SELECT a.id AS l,b.id AS r FROM many a JOIN single b ON a.k=b.k",
            "SELECT a.id AS l,b.id AS r FROM many a LEFT JOIN empty b ON a.k=b.k ORDER BY l,r",
            "SELECT a.id,b.id FROM empty a JOIN many b ON a.k=b.k",
            "SELECT a.k,COUNT(b.id),COUNT(*) FROM many a LEFT JOIN single b ON a.k=b.k GROUP BY a.k",
            "SELECT a.k,COUNT(b.id) FROM many a LEFT JOIN empty b ON a.k=b.k GROUP BY a.k",
            "SELECT a.k,COUNT(b.id) FROM empty a LEFT JOIN many b ON a.k=b.k GROUP BY a.k",
            "SELECT a.k,COUNT(b.id) FROM many a LEFT JOIN single b ON CAST(a.k AS REAL)=b.k GROUP BY a.k",
            "SELECT k,COUNT(*),SUM(id),AVG(id) FROM many GROUP BY k ORDER BY k",
            "SELECT k,COUNT(*),SUM(id) FROM many GROUP BY k",
            "SELECT SUM(k) FROM many WHERE k IS NULL",
            "SELECT SUM(d) FROM many WHERE id < 72",
            "SELECT id FROM many WHERE id < 0",
            "SELECT CASE WHEN id=8 THEN CAST('first' AS INTEGER) WHEN id=16 THEN CAST('second' AS INTEGER) ELSE id END FROM many",
            "SELECT CASE WHEN id=8 THEN d*10 WHEN id=16 THEN -d*10 ELSE d END FROM many",
            "SELECT SUM(CASE WHEN id=1 THEN 1 ELSE d END) FROM many",
        ] {
            let prepared =
                prepare(sql, &tables.0, &base).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let expected = execute(&prepared, &base).map(|output| output.batch);
            for workers in [1, 2, 4, 10] {
                for batch_size in [1, 7, 8192] {
                    let options = Options {
                        workers,
                        batch_size,
                        ..base.clone()
                    };
                    let actual = execute(&prepared, &options).map(|output| {
                        assert!((1..=workers).contains(&output.metrics.worker_threads_peak));
                        output.batch
                    });
                    assert_eq!(
                        actual, expected,
                        "optimized={relational_optimizations}, workers={workers}, batch={batch_size}: {sql}"
                    );
                }
            }
        }
        // FLOAT aggregate inputs are currently refused by the binder. Scheduling
        // must neither widen that semantic domain nor turn the refusal into output.
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = Options {
                    workers,
                    batch_size,
                    ..base.clone()
                };
                assert_eq!(
                    prepare(
                        "SELECT k,SUM(f),AVG(f) FROM many GROUP BY k",
                        &tables.0,
                        &options
                    )
                    .err()
                    .as_deref(),
                    Some("expected numeric type, got Float")
                );
            }
        }
        let prepared = prepare("SELECT id FROM many WHERE k < 3", &tables.0, &base).unwrap();
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                cancellation: Default::default(),
                ..base.clone()
            };
            options.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &options).err().as_deref(),
                Some("cancelled: query execution")
            );
        }
    }
}
