use arrow::{
    array::*,
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
fn relational_column_aliases_match_conversion_and_shared_scan_controls() {
    let root =
        std::env::temp_dir().join(format!("assaydb-column-projection-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("n", DataType::Int32, true),
        Field::new("d", DataType::Decimal128(16, 2), true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(0),
            Some(-1),
        ])),
        Arc::new(StringArray::from(vec![
            Some("é\0"),
            None,
            Some("long"),
            Some(""),
            Some("tail"),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
            Some(0),
            Some(-1),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(-123), None, Some(987), Some(0), Some(-1)])
                .with_precision_and_scale(16, 2)
                .unwrap(),
        ),
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
        "SELECT x.alias,x.duplicate,x.txt FROM (SELECT i AS alias,i AS duplicate,s AS txt FROM t) x ORDER BY 1 NULLS FIRST",
        "SELECT x.alias,x.amount FROM (SELECT n AS alias,d AS amount FROM t) x ORDER BY 1 NULLS FIRST",
        "WITH c AS (SELECT i AS alias,s AS txt FROM t) SELECT a.alias,a.txt FROM c a JOIN c b ON a.alias=b.alias ORDER BY 1,2",
        "SELECT x.alias FROM (SELECT i AS alias FROM t WHERE i IS NULL) x",
        "SELECT x.alias FROM (SELECT i AS alias FROM t WHERE FALSE) x",
    ] {
        let base = Options {
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
        for batch_size in [2, 17] {
            for workers in [1, 4, 10] {
                for prepared_pushdown in [false, true] {
                    let options = Options {
                        batch_size,
                        workers,
                        projection_pushdown: prepared_pushdown,
                        ..Options::default()
                    };
                    let prepared = prepare(sql, &tables, &options).unwrap();
                    // A public caller may use different options at execution time.
                    for execution_pushdown in [false, true] {
                        let options = Options {
                            projection_pushdown: execution_pushdown,
                            ..options.clone()
                        };
                        let actual = execute(&prepared, &options).unwrap();
                        assert_eq!(actual.batch, expected.batch, "{sql}");
                        if !execution_pushdown {
                            assert_eq!(actual.metrics.column_projection_batches, 0);
                        }
                        if sql.contains("duplicate") && prepared_pushdown && execution_pushdown {
                            assert!(actual.metrics.column_projection_batches > 0);
                        }
                    }
                }
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
