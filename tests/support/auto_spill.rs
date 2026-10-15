use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub const QUERIES: &str = include_str!("../fixtures/auto-spill-pipeline.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Fixture {
    pub root: PathBuf,
    pub tables: HashMap<String, PathBuf>,
}
impl Fixture {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "assaydb-auto-pipeline-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("spill")).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
            Field::new("d", DataType::Decimal128(38, 2), true),
            Field::new("n", DataType::Int64, true),
            Field::new("denom", DataType::Int64, false),
            Field::new("wide", DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
        ]));
        let rows = 4096;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| [Some(3), None, Some(1), Some(2)][i % 4]),
            )),
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                [Some("é"), Some(""), None, Some("a"), Some("longer")][i % 5]
            }))),
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some((i % 17) as i64 - 8)),
            )),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..rows).map(|i| (i % 11 != 0).then_some((i % 29) as i128 - 14)),
                )
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![None; rows])),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| i64::from(i != 1)),
            )),
            Arc::new(Int64Array::from(vec![i64::MAX; rows])),
            Arc::new(Float64Array::from_iter(
                (0..rows).map(|i| [Some(-0.0), Some(0.0), None, Some(1.5)][i % 4]),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let path = root.join("t.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(257)
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
    pub fn assert_clean(&self) {
        assert_eq!(fs::read_dir(self.root.join("spill")).unwrap().count(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

pub fn assert_bits(actual: &RecordBatch, expected: &RecordBatch, sql: &str) {
    // Arrow equality checks schema (including names/metadata), order, validity,
    // and exact nonfloating values. Compare float bits as well so signed zero
    // cannot be hidden by numeric equality. Null payload bytes are undefined.
    assert_eq!(actual, expected, "{sql}");
    for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
        if let Some(actual) = actual.as_any().downcast_ref::<Float64Array>() {
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..actual.len() {
                if actual.is_valid(row) {
                    assert_eq!(
                        actual.value(row).to_bits(),
                        expected.value(row).to_bits(),
                        "{sql}: float row {row}"
                    );
                }
            }
        }
    }
}
