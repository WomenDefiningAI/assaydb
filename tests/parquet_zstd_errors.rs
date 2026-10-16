//! ZSTD page error-compatibility recorder.
//!
//! Builds small valid ZSTD FLBA(7) DECIMAL(15,2) files in memory, corrupts the first data page in
//! controlled ways (keeping page header and footer sizes consistent where noted), reads each
//! variant through the Arrow reader and the raw page reader, and prints one deterministic line per
//! case: `ERRCASE <name> <result>`. It never asserts on the outcome; diff the output between
//! reader builds instead.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::column::page::Page;
use parquet::data_type::{FixedLenByteArray, FixedLenByteArrayType};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

// ---------------------------------------------------------------------------------------------
// Minimal Thrift compact-protocol rewriter: copies a struct byte for byte, passing every integer
// field (i16/i32/i64) through a callback keyed by its field-id path so it can be changed.
// ---------------------------------------------------------------------------------------------

struct Rewriter<'a> {
    input: &'a [u8],
    pos: usize,
    out: Vec<u8>,
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

impl<'a> Rewriter<'a> {
    fn byte(&mut self) -> u8 {
        let b = self.input[self.pos];
        self.pos += 1;
        b
    }

    fn varint(&mut self) -> u64 {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let b = self.byte();
            value |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }

    fn copy(&mut self, n: usize) {
        self.out
            .extend_from_slice(&self.input[self.pos..self.pos + n]);
        self.pos += n;
    }

    fn copy_varint(&mut self) -> u64 {
        let v = self.varint();
        put_varint(&mut self.out, v);
        v
    }

    fn structure(&mut self, path: &mut Vec<i16>, f: &mut dyn FnMut(&[i16], i64) -> i64) {
        let mut last = 0i16;
        loop {
            let header = self.byte();
            self.out.push(header);
            if header == 0 {
                return;
            }
            let ty = header & 0x0f;
            let delta = header >> 4;
            let id = if delta == 0 {
                zigzag_decode(self.copy_varint()) as i16
            } else {
                last + i16::from(delta)
            };
            last = id;
            path.push(id);
            self.value(ty, path, f);
            path.pop();
        }
    }

    fn value(&mut self, ty: u8, path: &mut Vec<i16>, f: &mut dyn FnMut(&[i16], i64) -> i64) {
        match ty {
            1 | 2 => {}
            3 => self.copy(1),
            4..=6 => {
                let v = zigzag_decode(self.varint());
                let new = f(path, v);
                put_varint(&mut self.out, zigzag_encode(new));
            }
            7 => self.copy(8),
            8 => {
                let len = self.copy_varint() as usize;
                self.copy(len);
            }
            9 | 10 => {
                let header = self.byte();
                self.out.push(header);
                let size = if header >> 4 == 15 {
                    self.copy_varint() as usize
                } else {
                    usize::from(header >> 4)
                };
                for _ in 0..size {
                    self.element(header & 0x0f, path, f);
                }
            }
            11 => {
                let size = self.copy_varint() as usize;
                if size > 0 {
                    let kinds = self.byte();
                    self.out.push(kinds);
                    for _ in 0..size {
                        self.element(kinds >> 4, path, f);
                        self.element(kinds & 0x0f, path, f);
                    }
                }
            }
            12 => self.structure(path, f),
            other => panic!("unsupported thrift compact type {other}"),
        }
    }

    fn element(&mut self, ty: u8, path: &mut Vec<i16>, f: &mut dyn FnMut(&[i16], i64) -> i64) {
        if ty == 1 || ty == 2 {
            self.copy(1);
        } else {
            self.value(ty, path, f);
        }
    }
}

/// Rewrite one Thrift struct starting at `input[0]`. Returns (new bytes, bytes consumed).
fn rewrite_struct(input: &[u8], f: &mut dyn FnMut(&[i16], i64) -> i64) -> (Vec<u8>, usize) {
    let mut rw = Rewriter {
        input,
        pos: 0,
        out: Vec::new(),
    };
    rw.structure(&mut Vec::new(), f);
    (rw.out, rw.pos)
}

// ---------------------------------------------------------------------------------------------
// File construction and page surgery.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Base {
    name: &'static str,
    v2: bool,
    optional: bool,
}

fn build_file(base: Base) -> Bytes {
    let repetition = if base.optional {
        "optional"
    } else {
        "required"
    };
    let schema = parse_message_type(&format!(
        "message s {{ {repetition} fixed_len_byte_array(7) d (DECIMAL(15,2)); }}"
    ))
    .unwrap();
    let props = WriterProperties::builder()
        .set_writer_version(if base.v2 {
            WriterVersion::PARQUET_2_0
        } else {
            WriterVersion::PARQUET_1_0
        })
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .build();
    let rows = 300usize;
    let defs: Vec<i16> = (0..rows)
        .map(|i| i16::from(!base.optional || i % 5 != 2))
        .collect();
    // Compressible values (small magnitudes), so V2 pages keep is_compressed = true.
    let values: Vec<FixedLenByteArray> = (0..rows)
        .filter(|&i| defs[i] == 1)
        .map(|i| {
            let v = (i as i128 * 37 % 5000) - 2500;
            FixedLenByteArray::from(v.to_be_bytes()[9..].to_vec())
        })
        .collect();
    let mut out = Vec::new();
    let mut writer =
        SerializedFileWriter::new(&mut out, Arc::new(schema), Arc::new(props)).unwrap();
    let mut row_group = writer.next_row_group().unwrap();
    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<FixedLenByteArrayType>()
        .write_batch(&values, base.optional.then_some(&defs[..]), None)
        .unwrap();
    column.close().unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    Bytes::from(out)
}

/// Layout of the first (only) data page and the footer.
struct Layout {
    header_start: usize,
    header_len: usize,
    payload_len: usize,
    uncompressed: i64,
    /// V2: uncompressed def+rep level bytes at the front of the payload.
    levels_len: usize,
    footer_start: usize,
    footer_len: usize,
}

impl Layout {
    fn page_end(&self) -> usize {
        self.header_start + self.header_len + self.payload_len
    }
}

fn layout(file: &[u8]) -> Layout {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(file)).unwrap();
    let column = reader.metadata().row_group(0).column(0);
    assert!(column.dictionary_page_offset().is_none());
    let header_start = column.data_page_offset() as usize;
    let (mut compressed, mut uncompressed, mut def_len, mut rep_len) = (0, 0, 0, 0);
    let (_, header_len) = rewrite_struct(&file[header_start..], &mut |path, v| {
        match path {
            [2] => uncompressed = v,
            [3] => compressed = v,
            [8, 5] => def_len = v,
            [8, 6] => rep_len = v,
            _ => {}
        }
        v
    });
    let footer_len = u32::from_le_bytes(file[file.len() - 8..file.len() - 4].try_into().unwrap());
    let footer_len = footer_len as usize;
    let layout = Layout {
        header_start,
        header_len,
        payload_len: compressed as usize,
        uncompressed,
        levels_len: (def_len + rep_len) as usize,
        footer_start: file.len() - 8 - footer_len,
        footer_len,
    };
    // Single page: it must fill the column chunk exactly.
    assert_eq!(
        layout.header_len + layout.payload_len,
        column.compressed_size() as usize,
        "single data page expected"
    );
    layout
}

/// A page modification: new payload bytes, plus optional header overrides.
struct Variant {
    name: String,
    payload: Vec<u8>,
    /// Header compressed_page_size. `None` = keep consistent with the new payload length.
    declared_compressed: Option<i64>,
    /// Header uncompressed_page_size. `None` = unchanged.
    declared_uncompressed: Option<i64>,
}

/// Rebuild the file with a replaced first data page; the footer's chunk sizes and any
/// offsets after the page are shifted by the size change so the layout stays consistent.
fn splice(file: &[u8], layout: &Layout, variant: &Variant) -> Bytes {
    let payload_len = variant.payload.len() as i64;
    let compressed = variant.declared_compressed.unwrap_or(payload_len);
    let (header, consumed) =
        rewrite_struct(&file[layout.header_start..], &mut |path, v| match path {
            [2] => variant.declared_uncompressed.unwrap_or(v),
            [3] => compressed,
            _ => v,
        });
    assert_eq!(consumed, layout.header_len);
    let old_page_end = layout.page_end() as i64;
    let delta = (header.len() + variant.payload.len()) as i64
        - (layout.header_len + layout.payload_len) as i64;
    let footer = &file[layout.footer_start..layout.footer_start + layout.footer_len];
    let (new_footer, consumed) = rewrite_struct(footer, &mut |path, v| match path {
        // RowGroup.total_compressed_size, ColumnMetaData.total_compressed_size
        [4, 6] | [4, 1, 3, 7] => v + delta,
        // Offsets that point past the page (file_offset, index and bloom filter offsets).
        [4, 1, 2] | [4, 1, 4] | [4, 1, 6] | [4, 1, 3, 14] if v >= old_page_end => v + delta,
        _ => v,
    });
    assert_eq!(consumed, layout.footer_len);
    let mut out = Vec::with_capacity(file.len() + 64);
    out.extend_from_slice(&file[..layout.header_start]);
    out.extend_from_slice(&header);
    out.extend_from_slice(&variant.payload);
    out.extend_from_slice(&file[layout.page_end()..layout.footer_start]);
    out.extend_from_slice(&new_footer);
    out.extend_from_slice(&(new_footer.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    Bytes::from(out)
}

fn variants(file: &[u8], layout: &Layout) -> Vec<Variant> {
    let start = layout.header_start + layout.header_len;
    let payload = &file[start..start + layout.payload_len];
    // V2 keeps levels uncompressed in front of the ZSTD frame; corrupt only the frame.
    let (levels, frame) = payload.split_at(layout.levels_len);
    let with_frame = |frame: Vec<u8>| -> Vec<u8> { [levels, &frame[..]].concat() };
    let plain = |name: String, payload: Vec<u8>| Variant {
        name,
        payload,
        declared_compressed: None,
        declared_uncompressed: None,
    };
    let n = frame.len();
    let mut out = vec![plain("baseline".into(), payload.to_vec())];

    for keep in [0, 1, 4, 5, 8, n / 2, n - 4, n - 1] {
        out.push(plain(
            format!("truncate_frame_keep_{keep}_of_{n}"),
            with_frame(frame[..keep].to_vec()),
        ));
    }
    // Header still claims the original size; the chunk itself is shorter.
    out.push(Variant {
        name: "truncate_frame_header_unchanged".into(),
        payload: with_frame(frame[..n / 2].to_vec()),
        declared_compressed: Some(layout.payload_len as i64),
        declared_uncompressed: None,
    });

    let mut offsets = vec![
        0,
        1,
        3,
        4,
        5,
        6,
        7,
        8,
        n / 4,
        n / 2,
        (3 * n) / 4,
        n - 4,
        n - 2,
        n - 1,
    ];
    offsets.dedup();
    for offset in offsets {
        let mut flipped = frame.to_vec();
        flipped[offset] ^= 0xff;
        out.push(plain(
            format!("flip_frame_byte_{offset}_of_{n}"),
            with_frame(flipped),
        ));
    }
    let mut flipped = frame.to_vec();
    flipped[n / 2] ^= 0x01;
    out.push(plain(
        format!("flip_frame_bit0_at_{}", n / 2),
        with_frame(flipped),
    ));

    for (label, tail) in [
        ("trailing_garbage_1_byte", vec![0xab]),
        ("trailing_garbage_8_bytes", vec![0xab; 8]),
        ("trailing_garbage_64_bytes", (0..64u8).collect()),
        ("trailing_zeros_16_bytes", vec![0; 16]),
        // A zstd skippable frame (magic 0x184D2A50, 4 byte length, 4 bytes data).
        (
            "trailing_skippable_frame",
            vec![0x50, 0x2a, 0x4d, 0x18, 4, 0, 0, 0, 1, 2, 3, 4],
        ),
        ("trailing_second_copy_of_frame", frame.to_vec()),
    ] {
        out.push(plain(label.into(), with_frame([frame, &tail[..]].concat())));
    }

    // Window descriptor edits. Frame header: magic(4), descriptor byte, then (when the
    // Single_Segment flag, bit 5, is clear) the Window_Descriptor byte:
    // exponent << 3 | mantissa, window = 2^(10 + exponent) * (1 + mantissa / 8).
    let descriptor = frame[4];
    if descriptor & 0x20 == 0 {
        for (label, window_byte) in [
            ("window_2p27_exact", 0x88u8),        // exponent 17, mantissa 0: 2^27
            ("window_2p27_plus_mantissa1", 0x89), // 2^27 + 2^24
            ("window_2p28", 0x90),                // exponent 18: 2^28
            ("window_2p31", 0xa8),                // exponent 21: 2^31
            ("window_max", 0xff),                 // exponent 31, mantissa 7
        ] {
            let mut edited = frame.to_vec();
            edited[5] = window_byte;
            out.push(plain(
                format!(
                    "{label}_descriptor_{descriptor:02x}_window_{:02x}_to_{window_byte:02x}",
                    frame[5]
                ),
                with_frame(edited),
            ));
        }
    } else {
        out.push(plain(
            format!("window_edit_skipped_single_segment_descriptor_{descriptor:02x}"),
            payload.to_vec(),
        ));
    }

    out.push(plain("zero_frame".into(), with_frame(vec![0; n])));
    out.push(plain("ff_frame".into(), with_frame(vec![0xff; n])));
    out.push(plain("empty_frame".into(), with_frame(Vec::new())));

    let u = layout.uncompressed;
    let levels = layout.levels_len as i64;
    for (label, declared) in [
        ("uncompressed_size_minus_1", u - 1),
        ("uncompressed_size_plus_1", u + 1),
        ("uncompressed_size_half", levels + (u - levels) / 2),
        ("uncompressed_size_double", u * 2),
        ("uncompressed_size_levels_only", levels),
        ("uncompressed_size_zero", 0),
        ("uncompressed_size_i32_max", i64::from(i32::MAX)),
        ("uncompressed_size_negative", -1),
    ] {
        out.push(Variant {
            name: label.into(),
            payload: payload.to_vec(),
            declared_compressed: None,
            declared_uncompressed: Some(declared),
        });
    }
    // Zero-size page with an (empty-output) valid frame and with garbage.
    for (label, frame_bytes) in [
        // Single-segment zstd frame with an empty raw last block: decodes to 0 bytes.
        (
            "zero_size_page_empty_frame",
            vec![0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00],
        ),
        ("zero_size_page_real_frame", frame.to_vec()),
        ("zero_size_page_garbage", vec![0xab; 9]),
    ] {
        out.push(Variant {
            name: label.into(),
            payload: with_frame(frame_bytes),
            declared_compressed: None,
            declared_uncompressed: Some(levels),
        });
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Readers.
// ---------------------------------------------------------------------------------------------

fn clean(s: String) -> String {
    s.replace('\n', "\\n")
}

fn guarded(f: impl FnOnce() -> String) -> String {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(s) => clean(s),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<non-string panic>".into());
            clean(format!("PANIC {msg}"))
        }
    }
}

fn read_arrow(data: &Bytes) -> String {
    let builder = match ParquetRecordBatchReaderBuilder::try_new(data.clone()) {
        Ok(b) => b,
        Err(e) => return format!("ERR(open) {e:?}"),
    };
    let reader = match builder.with_batch_size(128).build() {
        Ok(r) => r,
        Err(e) => return format!("ERR(build) {e:?}"),
    };
    let (mut rows, mut nulls, mut sum) = (0usize, 0usize, 0i128);
    for batch in reader {
        match batch {
            Ok(batch) => {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128");
                rows += array.len();
                nulls += array.null_count();
                for i in 0..array.len() {
                    if array.is_valid(i) {
                        sum = sum.wrapping_add(array.value(i).wrapping_mul(i as i128 + 1));
                    }
                }
            }
            Err(e) => return format!("ERR(batch@{rows}) {e:?}"),
        }
    }
    format!("OK rows={rows} nulls={nulls} checksum={sum}")
}

fn read_pages(data: &Bytes) -> String {
    let reader = match SerializedFileReader::new(data.clone()) {
        Ok(r) => r,
        Err(e) => return format!("ERR(open) {e:?}"),
    };
    let row_group = match reader.get_row_group(0) {
        Ok(r) => r,
        Err(e) => return format!("ERR(row_group) {e:?}"),
    };
    let mut pages = match row_group.get_column_page_reader(0) {
        Ok(p) => p,
        Err(e) => return format!("ERR(page_reader) {e:?}"),
    };
    let mut summary = Vec::new();
    loop {
        match pages.get_next_page() {
            Ok(None) => break,
            Ok(Some(page)) => {
                let kind = match &page {
                    Page::DataPage { .. } => "v1",
                    Page::DataPageV2 { .. } => "v2",
                    Page::DictionaryPage { .. } => "dict",
                };
                let buf = page.buffer();
                let fold = buf
                    .iter()
                    .fold(0u32, |h, &b| h.wrapping_mul(31).wrapping_add(u32::from(b)));
                summary.push(format!("{kind}:len={}:hash={fold:08x}", buf.len()));
            }
            Err(e) => return format!("ERR(after {}) {e:?}", summary.join(",")),
        }
    }
    format!("OK pages=[{}]", summary.join(","))
}

#[test]
fn record_zstd_page_error_cases() {
    // Quiet the default hook while probing for panics; restore afterwards.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let bases = [
        Base {
            name: "v1_required",
            v2: false,
            optional: false,
        },
        Base {
            name: "v1_optional",
            v2: false,
            optional: true,
        },
        Base {
            name: "v2_optional",
            v2: true,
            optional: true,
        },
    ];
    let mut lines = Vec::new();
    for base in bases {
        let file = build_file(base);
        let layout = layout(&file);
        // Sanity: an identity rewrite reproduces the file exactly.
        let identity = Variant {
            name: String::new(),
            payload: file[layout.header_start + layout.header_len..layout.page_end()].to_vec(),
            declared_compressed: None,
            declared_uncompressed: None,
        };
        assert_eq!(splice(&file, &layout, &identity), file, "identity splice");
        lines.push(format!(
            "ERRCASE {}/layout header_len={} payload_len={} uncompressed={} levels_len={}",
            base.name,
            layout.header_len,
            layout.payload_len,
            layout.uncompressed,
            layout.levels_len
        ));
        for variant in variants(&file, &layout) {
            let data = splice(&file, &layout, &variant);
            let arrow = guarded(|| read_arrow(&data));
            let pages = guarded(|| read_pages(&data));
            lines.push(format!(
                "ERRCASE {}/{}/arrow {arrow}",
                base.name, variant.name
            ));
            lines.push(format!(
                "ERRCASE {}/{}/pages {pages}",
                base.name, variant.name
            ));
        }
    }
    std::panic::set_hook(hook);
    for line in lines {
        println!("{line}");
    }
}
