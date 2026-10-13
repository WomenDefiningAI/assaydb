//! Compatibility for physical build payloads that are carried but never evaluated.
use arrow::{
    array::{ArrayRef, BinaryArray, Int64Array, ListArray},
    datatypes::{DataType, Field, Int64Type, Schema},
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

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn write(path: &std::path::Path, batch: &RecordBatch) {
    let mut writer = ArrowWriter::try_new(
        File::create_new(path).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(2)
                .build(),
        ),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}
#[test]
fn unused_binary_and_nested_build_columns_preserve_semi_anti_with_or_without_pushdown() {
    let root =
        Fixture(std::env::temp_dir().join(format!("assaydb-funded-join-{}", std::process::id())));
    fs::create_dir(&root.0).unwrap();
    let left_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let left = RecordBatch::try_new(
        left_schema,
        vec![Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
        ]))],
    )
    .unwrap();
    let left_path = root.0.join("left.parquet");
    write(&left_path, &left);
    let payloads: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(vec![
            Some(b"opaque\0a".as_slice()),
            None,
            Some(b"b".as_slice()),
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![Some(3)]),
        ])),
    ];
    for (i, payload) in payloads.into_iter().enumerate() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("unused", payload.data_type().clone(), true),
        ]));
        let right = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(2), Some(2), None])),
                payload,
            ],
        )
        .unwrap();
        let right_path = root.0.join(format!("right-{i}.parquet"));
        write(&right_path, &right);
        let tables = HashMap::from([("l".into(), left_path.clone()), ("r".into(), right_path)]);
        for (mode, expected) in [
            ("SEMI", vec![Some(2)]),
            ("ANTI", vec![Some(1), Some(3), None]),
        ] {
            let sql = format!(
                "SELECT l.k AS result_key FROM l {mode} JOIN r ON l.k=r.k ORDER BY result_key NULLS LAST"
            );
            for projection_pushdown in [false, true] {
                for relational_optimizations in [false, true] {
                    let options = Options {
                        projection_pushdown,
                        relational_optimizations,
                        batch_size: 2,
                        memory_bytes: 16 << 20,
                        ..Options::default()
                    };
                    let prepared = prepare(&sql, &tables, &options).unwrap();
                    let result = execute(&prepared, &options).unwrap();
                    let actual = result
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "{i}/{mode}/pushdown={projection_pushdown}/optimized={relational_optimizations}"
                    );
                }
            }
        }
    }
}
