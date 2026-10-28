use super::*;
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::ArrowWriter,
    basic::ZstdLevel,
    file::properties::{WriterProperties, WriterVersion},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn write(batch: &RecordBatch, properties: WriterProperties) -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-decoder-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
}

#[test]
fn lease_covers_measured_decoder_allocations_and_transfers_batch_credit() {
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for dictionary in [false, true] {
            for nullable in [false, true] {
                for codec in [
                    Compression::UNCOMPRESSED,
                    Compression::SNAPPY,
                    Compression::ZSTD(Default::default()),
                ] {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("n", DataType::Int64, nullable),
                        Field::new("s", DataType::Utf8, nullable),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(Int64Array::from_iter(
                                (0..1000).map(|i| (!nullable || i % 7 != 0).then_some(i)),
                            )) as ArrayRef,
                            Arc::new(StringArray::from_iter((0..1000).map(|i| {
                                (!nullable || i % 11 != 0).then(|| format!("value-{}-é", i % 29))
                            }))),
                        ],
                    )
                    .unwrap();
                    let fixture = Fixture::write(
                        &batch,
                        WriterProperties::builder()
                            .set_compression(codec)
                            .set_dictionary_enabled(dictionary)
                            .set_writer_version(version)
                            .set_write_batch_size(31)
                            .set_data_page_row_count_limit(73)
                            .build(),
                    );
                    for batch_size in [1, 7, 8192] {
                        let file = File::open(&fixture.0).unwrap();
                        let metadata = external_metadata(&file).unwrap();
                        let estimate = Estimate::reader(
                            &metadata,
                            &ProjectionMask::all(),
                            None,
                            None,
                            batch_size,
                        )
                        .unwrap();
                        let bound =
                            metadata.metadata().memory_size() + estimate.workspace + estimate.batch;
                        let budget = Budget::new(64 << 20);
                        let (rows, high) = super::allocator::measure(|| {
                            let mut reader = Reader::new(
                                file,
                                metadata,
                                ProjectionMask::all(),
                                None,
                                &Options {
                                    batch_size,
                                    parquet_cache_bytes: None,
                                    ..Options::default()
                                },
                                &budget,
                            )
                            .unwrap();
                            let mut rows = 0;
                            while let Some(batch) = reader.next() {
                                let batch = batch.unwrap();
                                let before = budget.used();
                                let lease = reader.take_batch_lease();
                                assert_eq!(budget.used(), before, "handoff must not reserve twice");
                                assert_eq!(lease.bytes(), batch.get_array_memory_size());
                                rows += batch.num_rows();
                                drop(batch);
                                drop(lease);
                            }
                            rows
                        });
                        assert_eq!(rows, 1000);
                        assert_eq!(budget.used(), 0);
                        assert!(
                            high <= bound,
                            "{version:?} dict={dictionary} nullable={nullable} {codec:?} batch={batch_size}: allocator={high} lease={bound}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn oversized_pages_dictionaries_and_zstd_frames_refuse_before_decode() {
    for (dictionary, codec) in [
        (true, Compression::UNCOMPRESSED),
        (false, Compression::UNCOMPRESSED),
        (false, Compression::ZSTD(ZstdLevel::try_new(22).unwrap())),
    ] {
        let payload = "x".repeat(64 << 20);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec![payload.as_str()]))],
        )
        .unwrap();
        let fixture = Fixture::write(
            &batch,
            WriterProperties::builder()
                .set_compression(codec)
                .set_dictionary_enabled(dictionary)
                .set_dictionary_page_size_limit(128 << 20)
                .set_data_page_size_limit(128 << 20)
                .build(),
        );
        drop(batch);
        drop(payload);
        let file = File::open(&fixture.0).unwrap();
        let metadata = external_metadata(&file).unwrap();
        assert!(
            metadata
                .metadata()
                .row_group(0)
                .column(0)
                .uncompressed_size()
                >= 64 << 20
        );
        if dictionary {
            assert!(
                metadata
                    .metadata()
                    .row_group(0)
                    .column(0)
                    .dictionary_page_offset()
                    .is_some()
            );
        }
        let budget = Budget::new(32 << 20);
        let (_, high) = super::allocator::measure(|| {
            let mut reader = Reader::new(
                file,
                metadata,
                ProjectionMask::all(),
                None,
                &Options::default(),
                &budget,
            )
            .unwrap();
            assert!(
                reader
                    .next()
                    .unwrap()
                    .unwrap_err()
                    .starts_with("resource-limit:")
            );
            assert!(reader.next().is_none());
        });
        assert!(
            high < 1 << 20,
            "refusal allocated {high} bytes before decoding"
        );
        assert!(high <= budget.peak() + (1 << 20));
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn metadata_sharing_and_reader_drop_release_exactly_once() {
    let fixture = Fixture::write(
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
        WriterProperties::builder().build(),
    );
    let budget = Budget::new(1 << 20);
    let (metadata, lease) = accounted_metadata(&File::open(&fixture.0).unwrap(), &budget).unwrap();
    let bytes = metadata.metadata().memory_size();
    assert_eq!(budget.used(), bytes);
    let copy = metadata_lease(&metadata.clone(), &budget).unwrap();
    assert_eq!(budget.used(), bytes);
    drop(lease);
    assert_eq!(budget.used(), bytes);
    drop(metadata);
    drop(copy);
    assert_eq!(budget.used(), 0);
}

#[test]
fn exact_fit_credit_succeeds_and_one_less_refuses_before_construction() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let fixture = Fixture::write(
        &batch,
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build(),
    );
    for fits in [false, true] {
        let file = File::open(&fixture.0).unwrap();
        let metadata = external_metadata(&file).unwrap();
        let estimate = Estimate::reader(&metadata, &ProjectionMask::all(), None, None, 3).unwrap();
        let required = metadata.metadata().memory_size() + estimate.workspace + estimate.batch;
        let budget = Budget::new(required - usize::from(!fits));
        let mut reader = Reader::new(
            file,
            metadata,
            ProjectionMask::all(),
            None,
            &Options {
                batch_size: 3,
                parquet_cache_bytes: None,
                ..Options::default()
            },
            &budget,
        )
        .unwrap();
        if fits {
            assert_eq!(budget.used(), required);
            let output = reader.next().unwrap().unwrap();
            assert_eq!(output, batch);
            let used = budget.used();
            let credit = reader.take_batch_lease();
            assert_eq!(budget.used(), used);
            let remaining = budget.reserve(budget.limit() - budget.used()).unwrap();
            assert!(
                reader.next().is_none(),
                "EOF must not reserve another batch"
            );
            drop(remaining);
            drop(output);
            drop(credit);
            assert!(reader.next().is_none());
        } else {
            assert!(
                reader
                    .take_decoder_refusal()
                    .unwrap()
                    .starts_with("resource-limit:")
            );
            assert!(reader.next().is_none());
        }
        drop(reader);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn byte_array_credit_covers_skew_and_dictionary_expansion_without_size_statistics() {
    for dictionary in [false, true] {
        let wide = "w".repeat(16384);
        let values = (0..1000).map(|i| if i < 3 { wide.as_str() } else { "" });
        let fixture = Fixture::write(
            &RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
                vec![Arc::new(StringArray::from_iter_values(values))],
            )
            .unwrap(),
            WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .build(),
        );
        let file = File::open(&fixture.0).unwrap();
        let metadata = external_metadata(&file).unwrap();
        let group = metadata.metadata().row_group(0);
        let estimate = Estimate::reader(&metadata, &ProjectionMask::all(), None, None, 3).unwrap();
        assert!(
            estimate.batch >= 2 * 3 * wide.len(),
            "first batch is wider than the chunk average"
        );
        let columns = group
            .columns()
            .iter()
            .cloned()
            .map(|column| {
                column
                    .into_builder()
                    .set_unencoded_byte_array_data_bytes(None)
                    .build()
                    .unwrap()
            })
            .collect();
        let legacy = group
            .clone()
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap();
        let estimate = Estimate::group(&legacy, &ProjectionMask::all(), 3).unwrap();
        assert!(estimate.batch >= 2 * 3 * wide.len());
        let budget = Budget::new(16 << 20);
        let mut reader = Reader::new(
            file,
            metadata,
            ProjectionMask::all(),
            None,
            &Options {
                batch_size: 3,
                parquet_cache_bytes: None,
                ..Options::default()
            },
            &budget,
        )
        .unwrap();
        let (batch, high) = super::allocator::measure(|| reader.next().unwrap().unwrap());
        assert!(high <= estimate.workspace + estimate.batch);
        assert_eq!(batch.num_rows(), 3);
        drop(batch);
        drop(reader);
        assert_eq!(budget.used(), 0);
    }
}

/// Integration of exact Parquet widening with the decoder lease: widened
/// layouts are funded by the same metadata-derived lease before decoding, and
/// the widened batch's credit is handed off without a second reservation.
#[test]
fn widened_layouts_decode_through_the_lease() {
    use arrow::array::{
        Array, Date32Array, Date64Array, DictionaryArray, Float32Array, Float64Array, Int8Array,
        LargeStringArray, UInt32Array,
    };
    use arrow::datatypes::Int8Type;
    let rows = 600;
    let dictionary: DictionaryArray<Int8Type> = (0..rows)
        .map(|i| ["north", "south", "east"][i % 3])
        .collect();
    let source = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("i8", DataType::Int8, true),
            Field::new("u32", DataType::UInt32, false),
            Field::new("f32", DataType::Float32, true),
            Field::new("large", DataType::LargeUtf8, false),
            Field::new("dict", dictionary.data_type().clone(), false),
            Field::new("d64", DataType::Date64, false),
        ])),
        vec![
            Arc::new(Int8Array::from_iter(
                (0..rows).map(|i| (i % 5 != 0).then_some((i % 120) as i8 - 60)),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                (0..rows).map(|i| u32::MAX - i as u32),
            )),
            Arc::new(Float32Array::from_iter((0..rows).map(|i| {
                (i % 7 != 0).then_some(if i % 11 == 0 {
                    f32::NAN
                } else {
                    i as f32 / 4.0
                })
            }))),
            Arc::new(LargeStringArray::from_iter_values(
                (0..rows).map(|i| format!("item-{}-é", i % 17)),
            )),
            Arc::new(dictionary),
            Arc::new(Date64Array::from_iter_values(
                (0..rows).map(|i| (i as i64 - 300) * 86_400_000),
            )),
        ],
    )
    .unwrap();
    let fixture = Fixture::write(
        &source,
        WriterProperties::builder()
            .set_write_batch_size(31)
            .set_data_page_row_count_limit(73)
            .build(),
    );
    let options = Options {
        batch_size: 64,
        parquet_cache_bytes: None,
        ..Options::default()
    };
    let file = File::open(&fixture.0).unwrap();
    let metadata = external_metadata(&file).unwrap();
    let estimate = Estimate::reader(&metadata, &ProjectionMask::all(), None, None, 64).unwrap();
    let required = metadata.metadata().memory_size() + estimate.workspace + estimate.batch;
    // One byte short of the decoder lease: refused before any decoder exists.
    let short = Budget::new(required - 1);
    let mut reader = Reader::new(
        file,
        metadata,
        ProjectionMask::all(),
        None,
        &options,
        &short,
    )
    .unwrap();
    assert!(
        reader
            .take_decoder_refusal()
            .unwrap()
            .starts_with("resource-limit:")
    );
    drop(reader);
    assert_eq!(short.used(), 0);
    // Funded: every batch is widened and its credit transfers exactly once.
    let file = File::open(&fixture.0).unwrap();
    let metadata = external_metadata(&file).unwrap();
    let budget = Budget::new(64 << 20);
    let mut reader = Reader::new(
        file,
        metadata,
        ProjectionMask::all(),
        None,
        &options,
        &budget,
    )
    .unwrap();
    assert!(reader.decoder_funded());
    let mut seen = 0;
    while let Some(batch) = reader.next() {
        let batch = batch.unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Date32,
            ]
        );
        let floats = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let i = seen + row;
            if i % 7 == 0 {
                assert!(floats.is_null(row));
            } else if i % 11 == 0 {
                assert!(floats.value(row).is_nan());
            } else {
                assert_eq!(floats.value(row), i as f64 / 4.0);
            }
        }
        let dates = batch
            .column(5)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(dates.value(0), seen as i32 - 300);
        let before = budget.used();
        let lease = reader.take_batch_lease();
        assert_eq!(budget.used(), before, "handoff must not reserve twice");
        assert_eq!(lease.bytes(), batch.get_array_memory_size());
        seen += batch.num_rows();
        drop(batch);
        drop(lease);
    }
    assert_eq!(seen, rows);
    drop(reader);
    assert_eq!(budget.used(), 0);
}
