//! FIXED_LEN_BYTE_ARRAY DECIMAL -> Arrow Decimal128 decode coverage.
//!
//! Files are written with the low-level `SerializedFileWriter` (ArrowWriter never emits FLBA for
//! precision <= 18) and read back through `ParquetRecordBatchReaderBuilder`. Every decoded value is
//! compared with an oracle defined in this file: big-endian sign extension of the raw stored bytes.
//! The test must pass against both pristine parquet 57.0.0 and any patched reader.
//!
//! Writer limits in parquet 57.0.0 (see `has_dictionary_support` in `column/writer/mod.rs`):
//! dictionary encoding is never produced for FIXED_LEN_BYTE_ARRAY with `WriterVersion::PARQUET_1_0`,
//! so dictionary cases run on V2 data pages only. PLAIN, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT
//! run on both V1 and V2 pages.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::column::page::Page;
use parquet::data_type::{FixedLenByteArray, FixedLenByteArrayType, Int64Type};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;

/// Independent oracle: sign-extend big-endian two's-complement bytes into i128.
fn oracle(raw: &[u8]) -> i128 {
    assert!((1..=16).contains(&raw.len()), "oracle width {}", raw.len());
    let mut value: i128 = if raw[0] & 0x80 != 0 { -1 } else { 0 };
    for &byte in raw {
        value = (value << 8) | i128::from(byte);
    }
    value
}

/// Largest DECIMAL precision a FLBA of `width` bytes can hold (parquet spec table).
const MAX_PRECISION: [u8; 17] = [
    0, 2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38,
];

fn precision_scale(width: usize) -> (u8, i8) {
    if width == 7 {
        // Production SF10 layout.
        return (15, 2);
    }
    let precision = MAX_PRECISION[width];
    (precision, (width as i8 % 4).min(precision as i8))
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn byte(&mut self) -> u8 {
        (self.next() >> 56) as u8
    }

    fn below(&mut self, bound: usize) -> usize {
        ((self.next() >> 33) % bound as u64) as usize
    }
}

fn hex(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

fn to_width(value: i128, width: usize) -> Vec<u8> {
    let raw = value.to_be_bytes()[16 - width..].to_vec();
    assert_eq!(oracle(&raw), value, "to_width({value}, {width})");
    raw
}

/// Raw FLBA values for one width: edge values, sign transitions, a pattern at every byte
/// position, deterministic pseudo-random values, and repeats (so dictionaries matter).
fn values_for_width(width: usize, seed: u64) -> Vec<Vec<u8>> {
    let bits = 8 * width as u32;
    let min = i128::MIN >> (128 - bits);
    let max = i128::MAX >> (128 - bits);
    let mut ints = vec![
        0,
        -1,
        1,
        min,
        max,
        min + 1,
        max - 1,
        2,
        -2,
        127,
        -128,
        128,
        -129,
        255,
        256,
        -256,
        -257,
        99,
        -99,
        12_345,
        -12_345,
    ];
    // Byte-boundary sign transitions (k = 16 is already covered by min/max).
    for k in 1..=(width as u32).min(15) {
        let half = 1i128 << (8 * k - 1);
        let full = 1i128 << (8 * k);
        ints.extend([half - 1, half, -half, -half - 1, -half + 1]);
        ints.extend([full - 1, full, -full, -full + 1]);
    }
    let mut values: Vec<Vec<u8>> = ints
        .into_iter()
        .filter(|v| (min..=max).contains(v))
        .map(|v| to_width(v, width))
        .collect();
    for pos in 0..width {
        for byte in [0x01, 0x7f, 0x80, 0xfe, 0xff] {
            let mut zeros = vec![0u8; width];
            zeros[pos] = byte;
            values.push(zeros);
            let mut ones = vec![0xffu8; width];
            ones[pos] = byte ^ 0xff;
            values.push(ones);
        }
    }
    let mut lcg = Lcg(seed ^ (width as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    for _ in 0..120 {
        values.push((0..width).map(|_| lcg.byte()).collect());
    }
    for _ in 0..60 {
        let pick = lcg.below(values.len());
        values.push(values[pick].clone());
    }
    values
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Nulls {
    Required,
    Dense,
    AllNull,
    Alternating,
    /// Few NULLs.
    SparseNull,
    /// Mostly NULL, sparse values.
    SparseValid,
}

const ALL_NULLS: [Nulls; 6] = [
    Nulls::Required,
    Nulls::Dense,
    Nulls::AllNull,
    Nulls::Alternating,
    Nulls::SparseNull,
    Nulls::SparseValid,
];

impl Nulls {
    fn valid(self, row: usize) -> bool {
        match self {
            Nulls::Required | Nulls::Dense => true,
            Nulls::AllNull => false,
            Nulls::Alternating => row % 2 == 1,
            Nulls::SparseNull => row % 11 != 4,
            Nulls::SparseValid => row.is_multiple_of(9),
        }
    }
}

fn build_rows(values: &[Vec<u8>], nulls: Nulls) -> Vec<Option<Vec<u8>>> {
    if nulls == Nulls::AllNull {
        return vec![None; values.len()];
    }
    let mut rows = Vec::new();
    let mut iter = values.iter();
    for row in 0.. {
        if nulls.valid(row) {
            match iter.next() {
                Some(value) => rows.push(Some(value.clone())),
                None => break,
            }
        } else {
            rows.push(None);
        }
    }
    rows
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Enc {
    Plain,
    Dict,
    /// Dictionary with a tiny dictionary-page limit, so the writer falls back mid-chunk.
    DictFallback,
    Delta,
    ByteStreamSplit,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    width: usize,
    enc: Enc,
    zstd: bool,
    v2: bool,
    nulls: Nulls,
    /// Small page/batch limits (many pages) or writer defaults.
    small_pages: bool,
}

impl Config {
    fn label(&self) -> String {
        format!(
            "w{} {:?} {} {} {:?} small_pages={}",
            self.width,
            self.enc,
            if self.zstd { "ZSTD" } else { "UNCOMPRESSED" },
            if self.v2 { "V2" } else { "V1" },
            self.nulls,
            self.small_pages
        )
    }

    fn props(&self) -> WriterProperties {
        self.props_for(None)
    }

    /// With `columns`, the value encoding is set only on those columns (others keep defaults).
    fn props_for(&self, columns: Option<&[&str]>) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_writer_version(if self.v2 {
                WriterVersion::PARQUET_2_0
            } else {
                WriterVersion::PARQUET_1_0
            })
            .set_compression(if self.zstd {
                Compression::ZSTD(ZstdLevel::default())
            } else {
                Compression::UNCOMPRESSED
            })
            .set_dictionary_enabled(matches!(self.enc, Enc::Dict | Enc::DictFallback));
        if self.small_pages {
            builder = builder
                .set_data_page_size_limit(96)
                .set_write_batch_size(13)
                .set_data_page_row_count_limit(29);
        }
        let encoding = match self.enc {
            Enc::Plain => Some(Encoding::PLAIN),
            Enc::Delta => Some(Encoding::DELTA_BYTE_ARRAY),
            Enc::ByteStreamSplit => Some(Encoding::BYTE_STREAM_SPLIT),
            Enc::Dict => None,
            Enc::DictFallback => {
                builder = builder.set_dictionary_page_size_limit(24);
                None
            }
        };
        if let Some(encoding) = encoding {
            builder = match columns {
                None => builder.set_encoding(encoding),
                Some(columns) => columns.iter().fold(builder, |b, c| {
                    b.set_column_encoding(ColumnPath::from(*c), encoding)
                }),
            };
        }
        builder.build()
    }
}

/// Uneven row-group boundaries so batches cross row groups mid-page.
fn row_group_ranges(rows: usize) -> Vec<std::ops::Range<usize>> {
    if rows < 6 {
        return std::iter::once(0..rows).collect();
    }
    let a = rows / 3 + 1;
    let b = (2 * rows) / 3 + 3;
    vec![0..a, a..b, b..rows]
}

fn def_levels(rows: &[Option<Vec<u8>>]) -> Vec<i16> {
    rows.iter().map(|r| i16::from(r.is_some())).collect()
}

fn present(rows: &[Option<Vec<u8>>]) -> Vec<FixedLenByteArray> {
    rows.iter()
        .flatten()
        .map(|r| FixedLenByteArray::from(r.clone()))
        .collect()
}

fn write_single(cfg: &Config, rows: &[Option<Vec<u8>>]) -> Bytes {
    let (precision, scale) = precision_scale(cfg.width);
    let repetition = if cfg.nulls == Nulls::Required {
        "required"
    } else {
        "optional"
    };
    let schema = parse_message_type(&format!(
        "message s {{ {repetition} fixed_len_byte_array({}) d (DECIMAL({precision},{scale})); }}",
        cfg.width
    ))
    .unwrap();
    let mut out = Vec::new();
    let mut writer =
        SerializedFileWriter::new(&mut out, Arc::new(schema), Arc::new(cfg.props())).unwrap();
    for range in row_group_ranges(rows.len()) {
        let slice = &rows[range];
        let mut row_group = writer.next_row_group().unwrap();
        let mut column = row_group.next_column().unwrap().unwrap();
        let defs = def_levels(slice);
        let defs = (cfg.nulls != Nulls::Required).then_some(&defs[..]);
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(&present(slice), defs, None)
            .unwrap();
        column.close().unwrap();
        row_group.close().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(out)
}

type Coverage = BTreeMap<String, usize>;
type Rows = [Option<Vec<u8>>];

/// Walk the physical pages of `column` and record what was really written. Returns the data
/// page encodings seen and the number of data pages.
fn record_pages(data: &Bytes, column: usize, coverage: &mut Coverage) -> (Vec<Encoding>, usize) {
    let reader = SerializedFileReader::new(data.clone()).unwrap();
    let mut encodings = Vec::new();
    let mut data_pages = 0;
    for rg in 0..reader.num_row_groups() {
        let codec = reader.metadata().row_group(rg).column(column).compression();
        let codec = match codec {
            Compression::ZSTD(_) => "ZSTD".to_string(),
            other => format!("{other:?}"),
        };
        let row_group = reader.get_row_group(rg).unwrap();
        let mut pages = row_group.get_column_page_reader(column).unwrap();
        while let Some(page) = pages.get_next_page().unwrap() {
            let key = match &page {
                Page::DataPage { encoding, .. } => {
                    data_pages += 1;
                    encodings.push(*encoding);
                    format!("{codec} DATA_PAGE {encoding:?}")
                }
                Page::DataPageV2 {
                    encoding,
                    is_compressed,
                    ..
                } => {
                    data_pages += 1;
                    encodings.push(*encoding);
                    format!("{codec} DATA_PAGE_V2 {encoding:?} is_compressed={is_compressed}")
                }
                Page::DictionaryPage { .. } => format!("{codec} DICTIONARY_PAGE"),
            };
            *coverage.entry(key).or_default() += 1;
        }
    }
    (encodings, data_pages)
}

fn expected_values(rows: &[Option<Vec<u8>>]) -> Vec<Option<i128>> {
    rows.iter().map(|r| r.as_deref().map(oracle)).collect()
}

/// Append one Decimal128 column's logical values, checking type and validity bookkeeping.
fn collect_decimal(
    batch: &RecordBatch,
    column: usize,
    precision: u8,
    scale: i8,
    out: &mut Vec<Option<i128>>,
    label: &str,
) {
    let array = batch.column(column);
    assert_eq!(
        array.data_type(),
        &DataType::Decimal128(precision, scale),
        "{label}: data type"
    );
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(array.len(), batch.num_rows(), "{label}: array length");
    let mut nulls = 0;
    for i in 0..array.len() {
        assert_eq!(array.is_valid(i), !array.is_null(i), "{label}: row {i}");
        if array.is_valid(i) {
            out.push(Some(array.value(i)));
        } else {
            nulls += 1;
            out.push(None);
        }
    }
    assert_eq!(array.null_count(), nulls, "{label}: null_count");
    if let Some(buffer) = array.nulls() {
        assert_eq!(buffer.len(), array.len(), "{label}: validity length");
        assert_eq!(buffer.null_count(), nulls, "{label}: validity null count");
    }
}

fn compare(label: &str, got: &[Option<i128>], expected: &[Option<i128>], raw: &[Option<Vec<u8>>]) {
    assert_eq!(got.len(), expected.len(), "{label}: row count");
    for (row, (g, e)) in got.iter().zip(expected).enumerate() {
        if g != e {
            panic!(
                "{label}: row {row} mismatch: got {g:?}, oracle {e:?}, raw bytes {}",
                raw[row]
                    .as_deref()
                    .map(hex)
                    .unwrap_or_else(|| "NULL".into())
            );
        }
    }
}

fn check_full_read(cfg: &Config, data: &Bytes, rows: &[Option<Vec<u8>>], batch_size: usize) {
    let label = format!("{} batch={batch_size}", cfg.label());
    let (precision, scale) = precision_scale(cfg.width);
    let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .unwrap()
        .with_batch_size(batch_size)
        .build()
        .unwrap();
    let mut got = Vec::with_capacity(rows.len());
    let mut sizes = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|e| panic!("{label}: read error {e:?}"));
        sizes.push(batch.num_rows());
        collect_decimal(&batch, 0, precision, scale, &mut got, &label);
    }
    if let Some((last, full)) = sizes.split_last() {
        assert!(
            full.iter().all(|&n| n == batch_size) && *last <= batch_size && *last > 0,
            "{label}: batch sizes {sizes:?}"
        );
    }
    compare(&label, &got, &expected_values(rows), rows);
}

/// Encodings that must appear in the data pages for a config.
fn check_encodings(cfg: &Config, encodings: &[Encoding], data_pages: usize, row_groups: usize) {
    let label = cfg.label();
    let has = |e: Encoding| encodings.contains(&e);
    let all_null = cfg.nulls == Nulls::AllNull;
    match cfg.enc {
        Enc::Plain => assert!(encodings.iter().all(|&e| e == Encoding::PLAIN), "{label}"),
        Enc::Delta => assert!(
            encodings.iter().all(|&e| e == Encoding::DELTA_BYTE_ARRAY),
            "{label}: {encodings:?}"
        ),
        Enc::ByteStreamSplit => assert!(
            encodings.iter().all(|&e| e == Encoding::BYTE_STREAM_SPLIT),
            "{label}: {encodings:?}"
        ),
        Enc::Dict => assert!(
            all_null || has(Encoding::RLE_DICTIONARY),
            "{label}: {encodings:?}"
        ),
        Enc::DictFallback => assert!(
            all_null || (has(Encoding::RLE_DICTIONARY) && has(Encoding::DELTA_BYTE_ARRAY)),
            "{label}: {encodings:?}"
        ),
    }
    if cfg.small_pages {
        assert!(
            data_pages > row_groups,
            "{label}: expected multiple pages, got {data_pages} over {row_groups} row groups"
        );
    }
}

fn encodings_for(v2: bool) -> &'static [Enc] {
    if v2 {
        &[
            Enc::Plain,
            Enc::Dict,
            Enc::DictFallback,
            Enc::Delta,
            Enc::ByteStreamSplit,
        ]
    } else {
        // parquet 57 never writes FLBA dictionaries on V1 pages.
        &[Enc::Plain, Enc::Delta, Enc::ByteStreamSplit]
    }
}

fn print_coverage(test: &str, coverage: &Coverage, files: usize) {
    println!("COVERAGE {test}: {files} files");
    for (key, count) in coverage {
        println!("COVERAGE {test}: {key} pages={count}");
    }
}

/// Full width x encoding x codec x page version x nullability matrix for one encoding family.
fn run_matrix(test: &str, wanted: &[Enc]) {
    let mut coverage = Coverage::new();
    let mut files = 0;
    for width in 1..=16 {
        let values = values_for_width(width, 0x5eed);
        for v2 in [false, true] {
            for &enc in encodings_for(v2).iter().filter(|e| wanted.contains(e)) {
                for zstd in [false, true] {
                    for nulls in ALL_NULLS {
                        let cfg = Config {
                            width,
                            enc,
                            zstd,
                            v2,
                            nulls,
                            small_pages: true,
                        };
                        let rows = build_rows(&values, nulls);
                        let data = write_single(&cfg, &rows);
                        files += 1;
                        let (encodings, pages) = record_pages(&data, 0, &mut coverage);
                        check_encodings(
                            &cfg,
                            &encodings,
                            pages,
                            row_group_ranges(rows.len()).len(),
                        );
                        for batch_size in [1, 7, 1024, 8192] {
                            check_full_read(&cfg, &data, &rows, batch_size);
                        }
                    }
                }
            }
        }
    }
    print_coverage(test, &coverage, files);
}

#[test]
fn oracle_matches_known_values() {
    assert_eq!(oracle(&[0x80]), -128);
    assert_eq!(oracle(&[0x7f]), 127);
    assert_eq!(oracle(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), -1);
    assert_eq!(oracle(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00]), 256);
    assert_eq!(oracle(&[0x80, 0, 0, 0, 0, 0, 0]), -(1i128 << 55));
    let mut min16 = [0u8; 16];
    min16[0] = 0x80;
    assert_eq!(oracle(&min16), i128::MIN);
    assert_eq!(oracle(&[0xff; 16]), -1);
    let mut max16 = [0xffu8; 16];
    max16[0] = 0x7f;
    assert_eq!(oracle(&max16), i128::MAX);
    assert_eq!(oracle(&[0x00, 0xff]), 255);
    assert_eq!(oracle(&[0xff, 0x00]), -256);
}

#[test]
fn flba_decimal_matrix_plain() {
    run_matrix("plain", &[Enc::Plain]);
}

#[test]
fn flba_decimal_matrix_dictionary() {
    run_matrix("dictionary", &[Enc::Dict, Enc::DictFallback]);
}

#[test]
fn flba_decimal_matrix_delta_byte_array() {
    run_matrix("delta_byte_array", &[Enc::Delta]);
}

#[test]
fn flba_decimal_matrix_byte_stream_split() {
    run_matrix("byte_stream_split", &[Enc::ByteStreamSplit]);
}

/// Production-shaped column (FLBA(7) DECIMAL(15,2), ZSTD, PLAIN, V1) at a larger row count, with
/// both writer-default and small pages, so 8192-row batches span pages and row groups.
#[test]
fn flba_decimal_production_shape_large() {
    let mut lcg = Lcg(0x00c0_ffee);
    let mut values = Vec::new();
    for i in 0..20_000 {
        let value = if i % 2 == 0 {
            // In DECIMAL(15,2) range.
            (lcg.next() % 2_000_000_000_000_000) as i128 - 999_999_999_999_999
        } else {
            oracle(&(0..7).map(|_| lcg.byte()).collect::<Vec<_>>())
        };
        values.push(to_width(value, 7));
    }
    let mut coverage = Coverage::new();
    let mut files = 0;
    for (enc, v2) in [
        (Enc::Plain, false),
        (Enc::Plain, true),
        (Enc::ByteStreamSplit, false),
        (Enc::Delta, true),
        (Enc::Dict, true),
    ] {
        for zstd in [true, false] {
            for nulls in [Nulls::Required, Nulls::SparseNull] {
                for small_pages in [false, true] {
                    let cfg = Config {
                        width: 7,
                        enc,
                        zstd,
                        v2,
                        nulls,
                        small_pages,
                    };
                    let rows = build_rows(&values, nulls);
                    let data = write_single(&cfg, &rows);
                    files += 1;
                    let (encodings, pages) = record_pages(&data, 0, &mut coverage);
                    check_encodings(&cfg, &encodings, pages, 3);
                    for batch_size in [1, 7, 1024, 8192] {
                        if batch_size == 1 && small_pages {
                            continue;
                        }
                        check_full_read(&cfg, &data, &rows, batch_size);
                    }
                }
            }
        }
    }
    print_coverage("production_shape_large", &coverage, files);
}

/// Deterministic alternating skip/select runs covering exactly `rows` rows.
fn make_selection(rows: usize, seed: u64) -> (RowSelection, Vec<bool>) {
    let mut lcg = Lcg(seed);
    let mut selectors = Vec::new();
    let mut mask = Vec::with_capacity(rows);
    let mut select = lcg.below(2) == 0;
    while mask.len() < rows {
        let run = (1 + lcg.below(60)).min(rows - mask.len());
        selectors.push(if select {
            RowSelector::select(run)
        } else {
            RowSelector::skip(run)
        });
        mask.extend(std::iter::repeat_n(select, run));
        select = !select;
    }
    (RowSelection::from(selectors), mask)
}

#[test]
fn flba_decimal_row_selection() {
    let mut coverage = Coverage::new();
    let mut files = 0;
    for width in [1, 3, 7, 8, 9, 15, 16] {
        let values = values_for_width(width, 0xab_cdef);
        for v2 in [false, true] {
            for &enc in encodings_for(v2) {
                for zstd in [false, true] {
                    for nulls in [Nulls::Required, Nulls::Alternating, Nulls::SparseNull] {
                        let cfg = Config {
                            width,
                            enc,
                            zstd,
                            v2,
                            nulls,
                            small_pages: true,
                        };
                        let rows = build_rows(&values, nulls);
                        let data = write_single(&cfg, &rows);
                        files += 1;
                        record_pages(&data, 0, &mut coverage);
                        let (precision, scale) = precision_scale(width);
                        for (seed, page_index) in [(1, false), (2, true), (3, true)] {
                            let (selection, mask) = make_selection(rows.len(), seed + width as u64);
                            let picked: Vec<Option<Vec<u8>>> = rows
                                .iter()
                                .zip(&mask)
                                .filter(|(_, keep)| **keep)
                                .map(|(r, _)| r.clone())
                                .collect();
                            for batch_size in [7, 1024] {
                                let label = format!(
                                    "{} selection seed={seed} page_index={page_index} batch={batch_size}",
                                    cfg.label()
                                );
                                let options = ArrowReaderOptions::new().with_page_index(page_index);
                                let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                                    data.clone(),
                                    options,
                                )
                                .unwrap()
                                .with_batch_size(batch_size)
                                .with_row_selection(selection.clone())
                                .build()
                                .unwrap();
                                let mut got = Vec::new();
                                for batch in reader {
                                    let batch = batch.unwrap_or_else(|e| panic!("{label}: {e:?}"));
                                    assert!(batch.num_rows() <= batch_size, "{label}");
                                    collect_decimal(&batch, 0, precision, scale, &mut got, &label);
                                }
                                compare(&label, &got, &expected_values(&picked), &picked);
                            }
                        }
                    }
                }
            }
        }
    }
    print_coverage("row_selection", &coverage, files);
}

/// Multi-column file: id INT64, then three FLBA decimals of different widths and nullability.
/// Reads several leaf projections, alone and combined with a row selection.
#[test]
fn flba_decimal_projection() {
    let schema = parse_message_type(
        "message s {
            required int64 id;
            optional fixed_len_byte_array(7) d7 (DECIMAL(15,2));
            required fixed_len_byte_array(16) d16 (DECIMAL(38,4));
            optional fixed_len_byte_array(3) d3 (DECIMAL(6,0));
        }",
    )
    .unwrap();
    let schema = Arc::new(schema);
    let d7_rows = build_rows(&values_for_width(7, 7), Nulls::SparseNull);
    let n = d7_rows.len();
    let cycle = |values: Vec<Vec<u8>>, nulls: Nulls| -> Vec<Option<Vec<u8>>> {
        (0..n)
            .map(|i| nulls.valid(i).then(|| values[i % values.len()].clone()))
            .collect()
    };
    let d16_rows = cycle(values_for_width(16, 16), Nulls::Required);
    let d3_rows = cycle(values_for_width(3, 3), Nulls::Alternating);
    let ids: Vec<i64> = (0..n as i64).map(|i| i * 3 - 7).collect();
    let mut coverage = Coverage::new();
    let mut files = 0;
    for v2 in [false, true] {
        for &enc in encodings_for(v2) {
            for zstd in [false, true] {
                let cfg = Config {
                    width: 7,
                    enc,
                    zstd,
                    v2,
                    nulls: Nulls::SparseNull,
                    small_pages: true,
                };
                let mut out = Vec::new();
                let mut writer = SerializedFileWriter::new(
                    &mut out,
                    schema.clone(),
                    Arc::new(cfg.props_for(Some(&["d7", "d16", "d3"]))),
                )
                .unwrap();
                for range in row_group_ranges(n) {
                    let mut row_group = writer.next_row_group().unwrap();
                    let mut column = row_group.next_column().unwrap().unwrap();
                    column
                        .typed::<Int64Type>()
                        .write_batch(&ids[range.clone()], None, None)
                        .unwrap();
                    column.close().unwrap();
                    for (rows, optional) in [(&d7_rows, true), (&d16_rows, false), (&d3_rows, true)]
                    {
                        let slice = &rows[range.clone()];
                        let defs = def_levels(slice);
                        let mut column = row_group.next_column().unwrap().unwrap();
                        column
                            .typed::<FixedLenByteArrayType>()
                            .write_batch(&present(slice), optional.then_some(&defs[..]), None)
                            .unwrap();
                        column.close().unwrap();
                    }
                    row_group.close().unwrap();
                }
                writer.close().unwrap();
                let data = Bytes::from(out);
                files += 1;
                for column in 1..=3 {
                    record_pages(&data, column, &mut coverage);
                }
                let decimals: [(&Rows, u8, i8); 3] =
                    [(&d7_rows, 15, 2), (&d16_rows, 38, 4), (&d3_rows, 6, 0)];
                let projections: [&[usize]; 5] = [&[1], &[2], &[3], &[1, 3], &[0, 2]];
                for leaves in projections {
                    for with_selection in [false, true] {
                        let (selection, mask) = make_selection(n, 99);
                        let keep = |i: usize| !with_selection || mask[i];
                        let label = format!(
                            "{} projection={leaves:?} selection={with_selection}",
                            cfg.label()
                        );
                        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
                            .unwrap()
                            .with_batch_size(64);
                        let mask_leaves = ProjectionMask::leaves(
                            builder.parquet_schema(),
                            leaves.iter().copied(),
                        );
                        let mut builder = builder.with_projection(mask_leaves);
                        if with_selection {
                            builder = builder.with_row_selection(selection);
                        }
                        let mut got: Vec<Vec<Option<i128>>> = vec![Vec::new(); leaves.len()];
                        let mut got_ids = Vec::new();
                        for batch in builder.build().unwrap() {
                            let batch = batch.unwrap_or_else(|e| panic!("{label}: {e:?}"));
                            assert_eq!(batch.num_columns(), leaves.len(), "{label}");
                            for (pos, &leaf) in leaves.iter().enumerate() {
                                if leaf == 0 {
                                    let ids = batch
                                        .column(pos)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    got_ids.extend(ids.values().iter().copied());
                                } else {
                                    let (_, p, s) = decimals[leaf - 1];
                                    collect_decimal(&batch, pos, p, s, &mut got[pos], &label);
                                }
                            }
                        }
                        for (pos, &leaf) in leaves.iter().enumerate() {
                            if leaf == 0 {
                                let want: Vec<i64> =
                                    (0..n).filter(|&i| keep(i)).map(|i| ids[i]).collect();
                                assert_eq!(got_ids, want, "{label}: ids");
                            } else {
                                let rows = decimals[leaf - 1].0;
                                let picked: Vec<Option<Vec<u8>>> = (0..n)
                                    .filter(|&i| keep(i))
                                    .map(|i| rows[i].clone())
                                    .collect();
                                compare(
                                    &format!("{label} leaf={leaf}"),
                                    &got[pos],
                                    &expected_values(&picked),
                                    &picked,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    print_coverage("projection", &coverage, files);
}
