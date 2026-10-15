use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
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

static NEXT: AtomicU64 = AtomicU64::new(0);
pub(super) struct Fixture {
    pub(super) root: PathBuf,
    pub(super) tables: HashMap<String, PathBuf>,
}
impl Fixture {
    pub(super) fn new(left_rows: usize, right_rows: usize, distinct: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "assaydb-left-count-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let mut tables = HashMap::new();
        for (name, rows, right) in [("l", left_rows, false), ("r", right_rows, true)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("g", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
                Field::new("z", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
                Field::new("tag", DataType::Utf8, false),
                Field::new("f", DataType::Float64, false),
            ]));
            let keys = if right {
                [
                    Some(3),
                    Some(3),
                    Some(3),
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(2),
                ]
            } else {
                [
                    Some(3),
                    Some(1),
                    Some(3),
                    None,
                    Some(2),
                    Some(99),
                    None,
                    Some(1),
                ]
            };
            let text = if right {
                [
                    Some("é\0x"),
                    Some("é\0x"),
                    Some("other"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    None,
                    Some("b"),
                ]
            } else {
                [
                    Some("é\0x"),
                    Some("a"),
                    Some("é\0x"),
                    None,
                    Some("b"),
                    Some("missing"),
                    None,
                    Some("a"),
                ]
            };
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                    if distinct {
                        (i % 17 != 0).then_some((i % 521) as i64)
                    } else {
                        keys[i % 8]
                    }
                }))),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| [20, 10, 20, 30, 10, 40, 30, 10][i % 8]),
                )),
                Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                    [Some(7), None, Some(9), None, None, None, Some(8), None][i % 8]
                }))),
                Arc::new(Int64Array::from(vec![None::<i64>; rows])),
                Arc::new(StringArray::from_iter((0..rows).map(|i| text[i % 8]))),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| if i % 8 == 2 { "drop" } else { "keep" }),
                )),
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| if i % 2 == 0 { 0.1 } else { -0.3 }),
                )),
            ];
            let path = root.join(format!("{name}.parquet"));
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(37)
                        .build(),
                ),
            )
            .unwrap();
            writer
                .write(&RecordBatch::try_new(schema, arrays).unwrap())
                .unwrap();
            writer.close().unwrap();
            tables.insert(name.into(), path);
        }
        Self { root, tables }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

pub(super) fn custom_fixture_with_nullability(
    left: Vec<(&str, ArrayRef)>,
    right: Vec<(&str, ArrayRef)>,
    row_group_size: usize,
    nullable: bool,
) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "assaydb-left-count-custom-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let mut tables = HashMap::new();
    for (name, columns) in [("l", left), ("r", right)] {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), nullable))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        let path = root.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group_size)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tables.insert(name.into(), path);
    }
    Fixture { root, tables }
}
