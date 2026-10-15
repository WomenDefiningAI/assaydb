//! One test in a separate binary: count all Rust allocations, including workers,
//! without allocations from concurrently running tests contaminating the peak.
use arrow::{
    array::{Int64Array, StructArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute},
    rel,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::SeqCst) + bytes;
    PEAK.fetch_max(live, Ordering::SeqCst);
}

// SAFETY: every allocation retains System's layout/alignment contract; counters
// use no heap storage and deallocation forwards the original pointer/layout.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() {
            allocated(layout.size());
        }
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        if !result.is_null() {
            allocated(layout.size());
        }
        result
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // Make replacement peaks observable: keep the old allocation alive
        // until its fully counted replacement has been allocated and copied.
        let replacement = Layout::from_size_align(size, layout.align()).unwrap();
        let result = unsafe { self.alloc(replacement) };
        if !result.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(ptr, result, layout.size().min(size));
                self.dealloc(ptr, layout);
            }
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        Self::with_statistics(EnabledStatistics::None)
    }

    fn with_statistics(statistics: EnabledStatistics) -> Self {
        let root = std::env::temp_dir().join(format!("assaydb-leaf-heap-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let group_rows = 32768;
        let rows = 8 * group_rows;
        let fields = vec![
            Arc::new(Field::new("a", DataType::Int64, false)),
            Arc::new(Field::new("b", DataType::Int64, true)),
        ];
        let unused = StructArray::new(
            fields.clone().into(),
            vec![
                Arc::new(Int64Array::from(vec![0; rows])),
                Arc::new(Int64Array::from(vec![None::<i64>; rows])),
            ],
            None,
        );
        let keys = (0..rows).map(|i| ((i / group_rows) * 1024 + (i * 17) % 1024) as i64);
        let values = (0..rows).map(|i| (i % 1024 != 0 && i % 11 != 0).then_some(i as i64 - 100000));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unused", DataType::Struct(fields.into()), false),
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(unused),
                Arc::new(Int64Array::from_iter_values(keys)),
                Arc::new(Int64Array::from_iter(values)),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(root.join("input.parquet")).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(group_rows)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    // This measures the Rust heap; native compression allocations
                    // are the existing, separately documented decoder limitation.
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_statistics_enabled(statistics)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(root)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn nested_numeric_leaf_matches_serial_and_funds_measured_heap_including_replacements() {
    let fixture = Fixture::new();
    let path = fixture.0.join("input.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let groups = reader.metadata().row_groups();
    assert!(
        groups.iter().all(
            |group| group.column(2).compressed_size() > 100 * group.column(1).compressed_size()
        )
    );
    let selected_bytes = groups
        .iter()
        .map(|group| 2 * group.column(2).compressed_size() + group.column(3).compressed_size())
        .sum::<i64>();
    drop(reader);
    let tables = HashMap::from([("t".into(), path)]);
    let sql = "SELECT k, SUM(v), COUNT(v), COUNT(*) FROM t GROUP BY k";
    let options = Options {
        batch_size: 37,
        parquet_cache_bytes: Some(0),
        ..Options::default()
    };
    let prepared = rel::prepare(sql, &tables, &options).unwrap();
    let expected = execute(&prepared, &options).unwrap();
    assert_eq!(expected.batch.num_rows(), 8192);
    assert_eq!(expected.batch.column(1).null_count(), 8);
    for workers in [1, 2, 4, 10] {
        let options = Options {
            workers,
            ..options.clone()
        };
        let prepared = rel::prepare(sql, &tables, &options).unwrap();
        // Preparation/fixture data stay alive throughout this interval, so
        // subtracting the entry live heap isolates execution's additional heap.
        let baseline = LIVE.load(Ordering::SeqCst);
        PEAK.store(baseline, Ordering::SeqCst);
        let actual = execute(&prepared, &options).unwrap();
        let heap_peak = PEAK.load(Ordering::SeqCst).saturating_sub(baseline);
        assert_eq!(actual.batch, expected.batch);
        assert_eq!(actual.metrics.pipeline_producers > 0, workers > 1);
        assert_eq!(actual.metrics.auto_spill_retries, 0);
        if workers > 1 {
            assert_eq!(
                actual.metrics.selected_column_compressed_bytes,
                selected_bytes
            );
            assert!(heap_peak > 0);
            assert!(
                heap_peak <= actual.metrics.peak_reserved_bytes,
                "workers={workers}: Rust heap peak {heap_peak}, reserved peak {}",
                actual.metrics.peak_reserved_bytes
            );
        }
        assert!(actual.metrics.worker_threads_peak <= workers);
    }

    // Repeat the allocation audit for the single-pass metadata owner, keeping
    // this binary single-test so unrelated threads cannot contaminate its peak.
    drop(fixture);
    let fixture = Fixture::with_statistics(EnabledStatistics::Chunk);
    let tables = HashMap::from([("t".into(), fixture.0.join("input.parquet"))]);
    for sql in [
        sql,
        "SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k HAVING SUM(v) > 0",
    ] {
        let baseline_options = Options {
            relational_optimizations: false,
            ..options.clone()
        };
        let baseline = execute(
            &rel::prepare(sql, &tables, &baseline_options).unwrap(),
            &baseline_options,
        )
        .unwrap();
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                ..options.clone()
            };
            let prepared = rel::prepare(sql, &tables, &options).unwrap();
            let entry = LIVE.load(Ordering::SeqCst);
            PEAK.store(entry, Ordering::SeqCst);
            let actual = execute(&prepared, &options).unwrap();
            let heap_peak = PEAK.load(Ordering::SeqCst).saturating_sub(entry);
            assert_eq!(actual.batch, baseline.batch);
            assert_eq!(actual.metrics.clustered_row_groups, 8);
            assert!(heap_peak > 0);
            assert!(
                heap_peak <= actual.metrics.peak_reserved_bytes,
                "clustered workers={workers}: heap {heap_peak}, reservation {}",
                actual.metrics.peak_reserved_bytes
            );
            assert!(actual.metrics.worker_threads_peak <= workers);
        }
    }
}
