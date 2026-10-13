//! Public upstream Parquet page-decoding coverage (no local codec override).
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array, StringArray},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use std::{fs::File, sync::Arc};

#[test]
fn zstd_parquet_pages_roundtrip_nulls_decimals_and_dictionary_modes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("price", DataType::Decimal128(15, 2), true),
    ]));
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..1024).map(|i| (i % 7 != 0).then_some(i)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter((0..1024).map(|i| {
                (i % 11 != 0).then_some(["", "é🦀", "alpha", "beta"][i % 4])
            }))),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..1024).map(|i| (i % 5 != 0).then_some(i * 17 - 5000)),
                )
                .with_precision_and_scale(15, 2)
                .unwrap(),
            ),
        ],
    )
    .unwrap();
    for (version_index, version) in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0]
        .into_iter()
        .enumerate()
    {
        for dictionary in [false, true] {
            let path = std::env::temp_dir().join(format!(
                "assaydb-zstd-{}-{version_index}-{dictionary}.parquet",
                std::process::id()
            ));
            let properties = WriterProperties::builder()
                .set_writer_version(version)
                .set_compression(Compression::ZSTD(Default::default()))
                .set_dictionary_enabled(dictionary)
                .set_max_row_group_size(137)
                .set_write_batch_size(23)
                .set_data_page_row_count_limit(29)
                .build();
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema.clone(),
                Some(properties),
            )
            .unwrap();
            writer.write(&expected).unwrap();
            writer.close().unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                .unwrap()
                .with_batch_size(37)
                .build()
                .unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(concat_batches(&schema, &batches).unwrap(), expected);
            std::fs::remove_file(path).unwrap();
        }
    }
}
