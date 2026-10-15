//! Parquet row-group readers and bounded, ordered parallel scan dispatch.
//! Predicate caches and prefetched batches belong to the current query only.
pub(crate) mod cursor;
#[cfg(test)]
pub(crate) mod test_support;
use crate::{
    Result,
    execute::Options,
    memory::{Budget, Reservation},
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::{executor::block_on, future::BoxFuture};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
            ParquetRecordBatchReaderBuilder, RowFilter, metrics::ArrowReaderMetrics,
        },
        async_reader::{
            AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
        },
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};
/// Normalize the external schema once, before any decoder, predicate or worker
/// sees it. Internal logical INTEGER markers cannot be supplied by a file.
/// Ordinary files keep the original metadata/reader path without rebuilding it.
pub(crate) fn external_metadata(file: &File) -> Result<ArrowReaderMetadata> {
    let metadata =
        ArrowReaderMetadata::load(file, Default::default()).map_err(|e| e.to_string())?;
    if !crate::types::Type::has_internal_metadata(metadata.schema()) {
        return Ok(metadata);
    }
    let schema = Arc::new(crate::types::Type::external_schema(metadata.schema()));
    ArrowReaderMetadata::try_new(
        metadata.metadata().clone(),
        ArrowReaderOptions::new().with_schema(schema),
    )
    .map_err(|e| e.to_string())
}

pub(crate) type FilterError = Arc<Mutex<Option<String>>>;
pub(crate) type PredicateFactory =
    Arc<dyn Fn(FilterError) -> Box<dyn parquet::arrow::arrow_reader::ArrowPredicate> + Send + Sync>;
pub(crate) type FilterFactory = Arc<dyn Fn(FilterError) -> Option<RowFilter> + Send + Sync>;
/// Exact post-decode processing. The returned lease bridges the handoff to
/// either the ordered worker queue or the synchronous consumer.
pub(crate) type BatchTransform = Arc<dyn Fn(RecordBatch) -> Result<TransformedBatch> + Send + Sync>;
pub(crate) struct TransformedBatch {
    pub batch: RecordBatch,
    pub lease: Reservation,
    // Direct execution still evaluates downstream expressions on empty input.
    pub preserve_empty: bool,
}
#[derive(Default)]
pub(crate) struct FilterStats {
    pub rows: usize,
    pub bytes: usize,
    pub seconds: f64,
    pub batches: usize,
    pub output_rows: usize,
}
/// Shared exact filtering after full Arrow decoding, including serial fallback.
pub(crate) fn exact_filter(
    expression: crate::expr::Expr,
    schema: &arrow::datatypes::Schema,
    budget: &Budget,
    cancellation: &crate::spill::Cancellation,
    preserve_empty: bool,
    stats: Arc<Mutex<FilterStats>>,
) -> Result<BatchTransform> {
    let mut needed = std::collections::BTreeSet::new();
    expression.columns(&mut needed);
    let columns = needed
        .into_iter()
        .map(|i| crate::types::Type::from_field(schema.field(i)).map(|ty| (i, ty)))
        .collect::<Result<Vec<_>>>()?;
    let budget = budget.clone();
    let cancellation = cancellation.clone();
    Ok(Arc::new(move |batch: RecordBatch| {
        cancellation.check()?;
        let start = std::time::Instant::now();
        let bytes = batch.get_array_memory_size();
        let _decoded = budget.reserve(bytes)?;
        let mut input = crate::expr::Input {
            columns: vec![None; batch.num_columns()],
            len: batch.num_rows(),
        };
        for (i, ty) in &columns {
            input.columns[*i] = Some(crate::expr::normalize_input(batch.column(*i), ty, &budget)?);
        }
        let mask = expression.eval(&input, &budget)?;
        drop(input);
        let _workspace = budget.reserve(
            bytes
                .saturating_mul(2)
                .saturating_add(batch.num_rows().saturating_mul(32))
                .saturating_add(128),
        )?;
        let output = arrow::compute::filter_record_batch(&batch, crate::expr::boolean(&mask.array))
            .map_err(|e| e.to_string())?;
        let lease = budget.reserve(output.get_array_memory_size())?;
        cancellation.check()?;
        let mut stats = stats.lock().unwrap();
        stats.rows += batch.num_rows();
        stats.bytes += bytes;
        stats.output_rows += output.num_rows();
        stats.batches += 1;
        stats.seconds += start.elapsed().as_secs_f64();
        Ok(TransformedBatch {
            batch: output,
            lease,
            preserve_empty,
        })
    }))
}
pub(crate) struct FilteredReader {
    inner: Reader,
    error: FilterError,
    transform: Option<BatchTransform>,
    delivered: Option<Reservation>,
    done: bool,
}

struct OwnedRead {
    bytes: Vec<u8>,
    _lease: Reservation,
}
impl AsRef<[u8]> for OwnedRead {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Default)]
struct ReadStats {
    bytes: usize,
    error: Option<String>,
}
struct LocalFile {
    file: File,
    metadata: Arc<ParquetMetaData>,
    budget: Budget,
    stats: Arc<Mutex<ReadStats>>,
}
impl AsyncFileReader for LocalFile {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let result = (|| -> Result<Bytes> {
                let len = range
                    .end
                    .checked_sub(range.start)
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or("invalid Parquet byte range")?;
                let lease = self
                    .budget
                    .reserve(len.checked_add(128).ok_or("Parquet read size overflow")?)?;
                let mut bytes = vec![0; len];
                self.file
                    .seek(SeekFrom::Start(range.start))
                    .map_err(|e| e.to_string())?;
                self.file
                    .read_exact(&mut bytes)
                    .map_err(|e| e.to_string())?;
                self.stats.lock().unwrap().bytes += len;
                Ok(Bytes::from_owner(OwnedRead {
                    bytes,
                    _lease: lease,
                }))
            })();
            result.map_err(|error| {
                self.stats.lock().unwrap().error = Some(error.clone());
                ParquetError::General(error)
            })
        })
    }
    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // This adapter is private and always constructed with metadata loaded
        // in this same request using the engine's ordinary reader options.
        Box::pin(async move { Ok(self.metadata.clone()) })
    }
}

pub(crate) struct CachedReader {
    stream: ParquetRecordBatchStream<LocalFile>,
    current: Option<ParquetRecordBatchReader>,
    metrics: ArrowReaderMetrics,
    stats: Arc<Mutex<ReadStats>>,
    _cache_lease: Reservation,
    done: bool,
}
pub(crate) enum Reader {
    Sync(Box<ParquetRecordBatchReader>),
    Cached(Box<CachedReader>),
    Filtered(Box<FilteredReader>),
    Parallel(Box<crate::parallel_scan::ParallelReader>),
}
impl Reader {
    #[allow(clippy::too_many_arguments)]
    pub fn with_transform(
        path: &Path,
        file: File,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        factory: FilterFactory,
        transform: Option<BatchTransform>,
        options: &Options,
        budget: &Budget,
    ) -> Result<Self> {
        #[cfg(test)]
        let transform = match &options.test_scan_observer {
            Some(observer) if observer.path == path => {
                Some(observer.wrap(transform, budget.clone()))
            }
            _ => transform,
        };
        if options.workers > 1 && metadata.metadata().num_row_groups() > 1 {
            let workers =
                budget.scan_readers(metadata.metadata().num_row_groups(), options.workers - 1);
            if workers.count > 0 {
                return Ok(Self::Parallel(Box::new(
                    crate::parallel_scan::ParallelReader::new(
                        path, metadata, projection, factory, transform, options, budget, workers,
                    )?,
                )));
            }
        }
        let error = Arc::default();
        let filter = factory(Arc::clone(&error));
        let inner = Self::new(file, metadata, projection, filter, options, budget)
            .map_err(|e| error.lock().unwrap().clone().unwrap_or(e))?;
        Ok(Self::Filtered(Box::new(FilteredReader {
            inner,
            error,
            transform,
            delivered: None,
            done: false,
        })))
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn filtered_group(
        file: File,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        factory: &FilterFactory,
        transform: Option<BatchTransform>,
        options: &Options,
        budget: &Budget,
        group: usize,
    ) -> Result<Self> {
        let error = Arc::default();
        let filter = factory(Arc::clone(&error));
        let inner = Self::new_groups(
            file,
            metadata,
            projection,
            filter,
            options,
            budget,
            Some(vec![group]),
        )
        .map_err(|e| error.lock().unwrap().clone().unwrap_or(e))?;
        Ok(Self::Filtered(Box::new(FilteredReader {
            inner,
            error,
            transform,
            delivered: None,
            done: false,
        })))
    }
    pub fn new(
        file: File,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        filter: Option<RowFilter>,
        options: &Options,
        budget: &Budget,
    ) -> Result<Self> {
        Self::new_groups(file, metadata, projection, filter, options, budget, None)
    }
    fn new_groups(
        file: File,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        filter: Option<RowFilter>,
        options: &Options,
        budget: &Budget,
        groups: Option<Vec<usize>>,
    ) -> Result<Self> {
        let cache = filter
            .as_ref()
            .and(options.parquet_cache_bytes)
            .and_then(|requested| {
                let cap = requested.min(options.memory_bytes / 64);
                let mut read_projection = projection.clone();
                if let Some(filter) = &filter {
                    for predicate in filter.predicates() {
                        read_projection.union(predicate.projection());
                    }
                }
                // Cache arrays are limited by upstream; reserve extra room for its
                // hash table/Arc bookkeeping. Compressed ranges carry their own
                // leases, which are released with the last Bytes owner.
                let cache_bytes = cap.checked_mul(3)?.checked_add(4096)?;
                let max_compressed = metadata
                    .metadata()
                    .row_groups()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| groups.as_ref().is_none_or(|groups| groups.contains(i)))
                    .map(|(_, group)| {
                        group
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| read_projection.leaf_included(*i))
                            .try_fold(0_usize, |size, (_, column)| {
                                size.checked_add(
                                    usize::try_from(column.compressed_size())
                                        .ok()?
                                        .checked_add(128)?,
                                )
                            })
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_iter()
                    .max()
                    .unwrap_or(0);
                // Avoid choosing a buffered scan when current headroom cannot
                // accommodate even one row group. Do not retry after emitting rows.
                drop(
                    budget
                        .reserve(cache_bytes.checked_add(max_compressed.checked_mul(2)?)?)
                        .ok()?,
                );
                Some((cap, budget.reserve(cache_bytes).ok()?))
            });
        if let Some((cap, lease)) = cache {
            let stats = Arc::new(Mutex::new(ReadStats::default()));
            let input = LocalFile {
                file,
                metadata: metadata.metadata().clone(),
                budget: budget.clone(),
                stats: stats.clone(),
            };
            let metrics = ArrowReaderMetrics::enabled();
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(input, metadata)
                .with_projection(projection)
                .with_batch_size(options.batch_size)
                .with_max_predicate_cache_size(cap)
                .with_metrics(metrics.clone());
            if let Some(groups) = groups {
                builder = builder.with_row_groups(groups);
            }
            if let Some(filter) = filter {
                builder = builder.with_row_filter(filter);
            }
            let stream = builder.build().map_err(|e| e.to_string())?;
            Ok(Self::Cached(Box::new(CachedReader {
                stream,
                current: None,
                metrics,
                stats,
                _cache_lease: lease,
                done: false,
            })))
        } else {
            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
                .with_projection(projection)
                .with_batch_size(options.batch_size);
            if let Some(groups) = groups {
                builder = builder.with_row_groups(groups);
            }
            if let Some(filter) = filter {
                builder = builder.with_row_filter(filter);
            }
            Ok(Self::Sync(Box::new(
                builder.build().map_err(|e| e.to_string())?,
            )))
        }
    }
    pub fn statistics(&self) -> (usize, usize, usize, usize) {
        match self {
            Self::Filtered(reader) => reader.inner.statistics(),
            Self::Parallel(reader) => reader.statistics().reader,
            Self::Sync(_) => (0, 0, 0, 0),
            Self::Cached(reader) => (
                1,
                reader.metrics.records_read_from_cache().unwrap_or(0),
                reader.metrics.records_read_from_inner().unwrap_or(0),
                reader.stats.lock().unwrap().bytes,
            ),
        }
    }
    /// The worker has reserved its queue owner before dropping this bridge.
    pub(super) fn transfer_batch_ownership(&mut self) {
        if let Self::Filtered(reader) = self {
            reader.delivered = None;
        }
    }
    pub fn parallel_statistics(&self) -> Option<crate::parallel_scan::Statistics> {
        match self {
            Self::Parallel(reader) => Some(reader.statistics()),
            Self::Filtered(reader) => reader.inner.parallel_statistics(),
            _ => None,
        }
    }
}
impl Iterator for Reader {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Parallel(reader) => reader.next(),
            Self::Filtered(reader) => {
                reader.delivered = None;
                if reader.done {
                    return None;
                }
                loop {
                    let Some(result) = reader.inner.next() else {
                        reader.done = true;
                        return None;
                    };
                    let batch = match result {
                        Ok(batch) => batch,
                        Err(error) => {
                            reader.done = true;
                            return Some(Err(reader
                                .error
                                .lock()
                                .unwrap()
                                .clone()
                                .unwrap_or(error)));
                        }
                    };
                    let Some(transform) = &reader.transform else {
                        return Some(Ok(batch));
                    };
                    match transform(batch) {
                        Ok(TransformedBatch {
                            batch,
                            lease,
                            preserve_empty,
                        }) => {
                            // Relational filters suppress empty output; direct
                            // execution preserves downstream empty-batch work.
                            if batch.num_rows() == 0 && !preserve_empty {
                                continue;
                            }
                            reader.delivered = Some(lease);
                            return Some(Ok(batch));
                        }
                        Err(error) => {
                            reader.done = true;
                            return Some(Err(error));
                        }
                    }
                }
            }
            Self::Sync(reader) => reader.next().map(|batch| batch.map_err(|e| e.to_string())),
            Self::Cached(reader) => {
                if reader.done {
                    return None;
                }
                let result = loop {
                    if let Some(current) = &mut reader.current {
                        if let Some(batch) = current.next() {
                            break Some(batch);
                        }
                        reader.current = None;
                    }
                    match block_on(reader.stream.next_row_group()) {
                        Ok(Some(group)) => reader.current = Some(group),
                        Ok(None) => {
                            reader.done = true;
                            break None;
                        }
                        Err(error) => {
                            reader.done = true;
                            break Some(Err(error.into()));
                        }
                    }
                };
                result.map(|batch| {
                    batch.map_err(|error| {
                        reader.done = true;
                        reader
                            .stats
                            .lock()
                            .unwrap()
                            .error
                            .clone()
                            .unwrap_or_else(|| error.to_string())
                    })
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, BooleanArray, Int64Array, StringArray},
        compute::{concat_batches, filter_record_batch},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ArrowPredicateFn},
        basic::{Compression, ZstdLevel},
        file::properties::{WriterProperties, WriterVersion},
    };
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture {
        path: PathBuf,
        batch: RecordBatch,
    }
    impl Fixture {
        fn new(dictionary: bool, version: WriterVersion, rows: usize) -> Self {
            let path = std::env::temp_dir().join(format!(
                "assaydb-cached-parquet-{}-{}.parquet",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, false),
                Field::new("s", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        (0..69)
                            .map(|i| if i % 7 == 0 { None } else { Some(i % 11 - 5) })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from((0..69).map(|i| i * 9).collect::<Vec<_>>())),
                    Arc::new(StringArray::from_iter((0..69).map(|i| {
                        if i % 10 == 0 {
                            None
                        } else {
                            Some(format!("payload{}é", i % 5))
                        }
                    }))),
                ],
            )
            .unwrap();
            let batch = batch.slice(0, rows);
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .set_writer_version(version)
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_max_row_group_size(17)
                .set_write_batch_size(5)
                .set_data_page_row_count_limit(5)
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, Some(properties))
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            Self { path, batch }
        }
        fn open(&self) -> (File, ArrowReaderMetadata) {
            let file = File::open(&self.path).unwrap();
            let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
            (file, metadata)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).unwrap();
        }
    }
    fn accepted(k: Option<i64>, mode: usize) -> bool {
        match mode {
            0 => true,
            1 => false,
            _ => k.is_some_and(|k| k % 3 == 0),
        }
    }
    fn predicate(metadata: &ArrowReaderMetadata, mode: usize) -> RowFilter {
        let key = ArrowPredicateFn::new(
            ProjectionMask::roots(metadata.parquet_schema(), [0]),
            move |batch: RecordBatch| {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                Ok(BooleanArray::from_iter(
                    keys.iter().map(|k| Some(accepted(k, mode))),
                ))
            },
        );
        let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
            vec![Box::new(key)];
        if mode == 3 {
            predicates.push(Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(metadata.parquet_schema(), [1]),
                |batch: RecordBatch| {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    Ok(BooleanArray::from_iter(
                        values.iter().map(|v| Some(v.is_some_and(|v| v > 180))),
                    ))
                },
            )));
        }
        RowFilter::new(predicates)
    }
    #[test]
    fn cached_reader_matches_uncached_across_pages_selections_and_projections() {
        let mut observed_reuse = 0;
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for dictionary in [false, true] {
                let f = Fixture::new(dictionary, version, 69);
                for batch_size in [1, 7, 64] {
                    for mode in 0..4 {
                        let keys = f
                            .batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let mask =
                            BooleanArray::from_iter(keys.iter().enumerate().map(|(i, k)| {
                                Some(accepted(k, mode) && (mode != 3 || i * 9 > 180))
                            }));
                        for columns in [vec![0, 1, 2], vec![0, 2], vec![2]] {
                            let expected = filter_record_batch(&f.batch, &mask)
                                .unwrap()
                                .project(&columns)
                                .unwrap();
                            let mut tiny_cache_decodes = 0;
                            for cache in [None, Some(0), Some(1), Some(1 << 20)] {
                                let budget = Budget::new(64 << 20);
                                let options = Options {
                                    batch_size,
                                    memory_bytes: 64 << 20,
                                    parquet_cache_bytes: cache,
                                    ..Options::default()
                                };
                                let (file, metadata) = f.open();
                                let projection = ProjectionMask::roots(
                                    metadata.parquet_schema(),
                                    columns.clone(),
                                );
                                let filter = predicate(&metadata, mode);
                                let mut reader = Reader::new(
                                    file,
                                    metadata,
                                    projection,
                                    Some(filter),
                                    &options,
                                    &budget,
                                )
                                .unwrap();
                                let batches = reader.by_ref().collect::<Result<Vec<_>>>().unwrap();
                                let stats = reader.statistics();
                                let actual = concat_batches(&expected.schema(), &batches).unwrap();
                                assert_eq!(
                                    actual, expected,
                                    "{version:?}/{dictionary}/{batch_size}/{mode}/{columns:?}/{cache:?}"
                                );
                                assert_eq!(stats.0, usize::from(cache.is_some()));
                                if cache.is_none_or(|n| n == 0) {
                                    assert_eq!(stats.1, 0);
                                }
                                if cache == Some(1) {
                                    tiny_cache_decodes = stats.2;
                                }
                                if cache == Some(1 << 20) && mode == 0 && columns.contains(&0) {
                                    assert!(
                                        stats.2 < tiny_cache_decodes,
                                        "shared cache must avoid decoding the overlapping output key again"
                                    );
                                }
                                observed_reuse += stats.1;
                                drop(reader);
                                drop(batches);
                                drop(actual);
                                assert_eq!(budget.used(), 0);
                            }
                        }
                    }
                }
            }
        }
        assert!(observed_reuse > 0);
    }
    #[test]
    fn exact_batch_transform_runs_once_in_parallel_and_every_serial_fallback() {
        for preserve_empty in [false, true] {
            for rows in [7, 69] {
                let f = Fixture::new(true, WriterVersion::PARQUET_2_0, rows);
                for cache in [None, Some(0), Some(1 << 20)] {
                    for (workers, occupied) in [(1, false), (4, false), (4, true)] {
                        let budget = Budget::new(64 << 20);
                        let permits = occupied.then(|| budget.workers(3, 3));
                        let seen = Arc::new(AtomicUsize::new(0));
                        let transform: BatchTransform = {
                            let seen = seen.clone();
                            let budget = budget.clone();
                            Arc::new(move |batch| {
                                let _decoded = budget.reserve(batch.get_array_memory_size())?;
                                seen.fetch_add(batch.num_rows(), Ordering::Relaxed);
                                let values = batch
                                    .column(1)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap();
                                let mask = BooleanArray::from(
                                    values
                                        .values()
                                        .iter()
                                        .map(|v| v % 18 == 0)
                                        .collect::<Vec<_>>(),
                                );
                                let output = filter_record_batch(&batch, &mask)
                                    .map_err(|e| e.to_string())?;
                                let lease = budget.reserve(output.get_array_memory_size())?;
                                Ok(TransformedBatch {
                                    batch: output,
                                    lease,
                                    preserve_empty,
                                })
                            })
                        };
                        let factory: FilterFactory = Arc::new(|_| {
                            Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                                ProjectionMask::all(),
                                |batch: RecordBatch| {
                                    Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                                },
                            ))]))
                        });
                        let (file, metadata) = f.open();
                        let options = Options {
                            workers,
                            batch_size: 1,
                            parquet_cache_bytes: cache,
                            ..Options::default()
                        };
                        let mut reader = Reader::with_transform(
                            &f.path,
                            file,
                            metadata,
                            ProjectionMask::all(),
                            factory,
                            Some(transform),
                            &options,
                            &budget,
                        )
                        .unwrap();
                        assert_eq!(
                            reader.parallel_statistics().is_some(),
                            workers > 1 && rows > 17 && !occupied
                        );
                        let mut batches = Vec::new();
                        for result in reader.by_ref() {
                            let batch = result.unwrap();
                            assert!(preserve_empty || batch.num_rows() > 0);
                            batches.push(batch);
                        }
                        assert_eq!(seen.load(Ordering::Relaxed), rows);
                        if preserve_empty {
                            assert_eq!(batches.len(), rows);
                        }
                        let actual = concat_batches(&f.batch.schema(), &batches).unwrap();
                        let mask =
                            BooleanArray::from((0..rows).map(|i| i % 2 == 0).collect::<Vec<_>>());
                        assert_eq!(actual, filter_record_batch(&f.batch, &mask).unwrap());
                        drop(reader);
                        drop(permits);
                        assert_eq!(budget.used(), 0);
                    }
                }
            }
        }
    }
    #[test]
    fn cached_read_errors_fallback_and_buffer_ownership_release_reservations() {
        let f = Fixture::new(true, WriterVersion::PARQUET_2_0, 69);
        let budget = Budget::new(64 << 20);
        let (file, metadata) = f.open();
        let stats = Arc::new(Mutex::new(ReadStats::default()));
        let mut local = LocalFile {
            file,
            metadata: metadata.metadata().clone(),
            budget: budget.clone(),
            stats,
        };
        let bytes = block_on(local.get_bytes(0..4)).unwrap();
        assert_eq!(&bytes[..], b"PAR1");
        let shared = bytes.clone();
        drop(bytes);
        assert!(budget.used() > 0);
        drop(shared);
        assert_eq!(budget.used(), 0);
        assert!(block_on(local.get_bytes(u64::MAX - 4..u64::MAX)).is_err());
        assert_eq!(budget.used(), 0);
        let beyond_end = local.file.metadata().unwrap().len() + 1;
        assert!(block_on(local.get_bytes(0..beyond_end)).is_err());
        assert_eq!(budget.used(), 0);
        local.budget = Budget::new(1);
        assert!(block_on(local.get_bytes(0..4)).is_err());
        assert!(
            local
                .stats
                .lock()
                .unwrap()
                .error
                .as_ref()
                .unwrap()
                .starts_with("resource-limit:")
        );
        assert_eq!(local.budget.used(), 0);
        for cache in [None, Some(0), Some(1 << 20)] {
            let (file, metadata) = f.open();
            let projection = ProjectionMask::all();
            let error_filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                ProjectionMask::roots(metadata.parquet_schema(), [0]),
                |_| {
                    Err(arrow::error::ArrowError::ComputeError(
                        "intentional predicate failure".into(),
                    ))
                },
            ))]);
            let options = Options {
                parquet_cache_bytes: cache,
                ..Options::default()
            };
            match Reader::new(
                file,
                metadata,
                projection,
                Some(error_filter),
                &options,
                &budget,
            ) {
                Ok(mut reader) => {
                    assert!(
                        reader
                            .next()
                            .unwrap()
                            .unwrap_err()
                            .contains("intentional predicate failure")
                    );
                    assert!(reader.next().is_none());
                }
                Err(error) => assert!(error.contains("intentional predicate failure")),
            }
            assert_eq!(budget.used(), 0);
        }
        let (file, metadata) = f.open();
        let filter = predicate(&metadata, 2);
        let tiny = Budget::new(1);
        let mut reader = Reader::new(
            file,
            metadata,
            ProjectionMask::all(),
            Some(filter),
            &Options::default(),
            &tiny,
        )
        .unwrap();
        assert_eq!(reader.statistics().0, 0);
        assert!(reader.next().unwrap().is_ok());
        drop(reader);
        assert_eq!(tiny.used(), 0);
        // A predicate-only column still requires compressed input even when
        // the output projects no columns (as in a count-only consumer).
        let (file, metadata) = f.open();
        let projection = ProjectionMask::none(metadata.parquet_schema().num_columns());
        let filter = predicate(&metadata, 2);
        let small = Budget::new(4096);
        let options = Options {
            parquet_cache_bytes: Some(0),
            ..Options::default()
        };
        let mut reader =
            Reader::new(file, metadata, projection, Some(filter), &options, &small).unwrap();
        assert_eq!(reader.statistics().0, 0);
        let rows = reader
            .by_ref()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>();
        let expected = f
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .filter(|k| accepted(*k, 2))
            .count();
        assert_eq!(rows, expected);
        drop(reader);
        assert_eq!(small.used(), 0);
        let empty = Fixture::new(false, WriterVersion::PARQUET_1_0, 0);
        let (file, metadata) = empty.open();
        let filter = predicate(&metadata, 2);
        let mut reader = Reader::new(
            file,
            metadata,
            ProjectionMask::all(),
            Some(filter),
            &Options::default(),
            &budget,
        )
        .unwrap();
        assert!(reader.next().is_none());
        assert_eq!(reader.statistics().1, 0);
        drop(reader);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn pipeline_cursor_moves_between_workers_preserving_tokens_and_output_credits() {
        fn is_send<T: Send>() {}
        is_send::<cursor::ScanCursor>();
        is_send::<cursor::ScanBatch>();
        let fixture = Fixture::new(true, WriterVersion::PARQUET_2_0, 69);
        for cache in [None, Some(0), Some(1 << 20)] {
            let budget = Budget::new(64 << 20);
            let options = Options {
                workers: 10,
                batch_size: 3,
                parquet_cache_bytes: cache,
                ..Options::default()
            };
            let factory: FilterFactory = Arc::new(|_| {
                Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                    ProjectionMask::all(),
                    |batch: RecordBatch| Ok(BooleanArray::from(vec![true; batch.num_rows()])),
                ))]))
            });
            let transform: BatchTransform = {
                let budget = budget.clone();
                Arc::new(move |batch| {
                    let _input = budget.reserve(batch.get_array_memory_size())?;
                    let values = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let mask = BooleanArray::from(
                        values
                            .values()
                            .iter()
                            .map(|v| v % 18 == 0)
                            .collect::<Vec<_>>(),
                    );
                    let batch = filter_record_batch(&batch, &mask).map_err(|e| e.to_string())?;
                    let lease = budget.reserve(batch.get_array_memory_size())?;
                    Ok(TransformedBatch {
                        batch,
                        lease,
                        preserve_empty: true,
                    })
                })
            };
            let mut outputs = Vec::new();
            // Deliberately create groups in reverse order. Encounter order is
            // encoded by source identity, independent of job arrival/completion.
            for group in (0..5).rev() {
                let (file, metadata) = fixture.open();
                let cursor = cursor::ScanCursor::new(
                    file,
                    metadata,
                    ProjectionMask::all(),
                    &factory,
                    Some(transform.clone()),
                    &options,
                    &budget,
                    group,
                )
                .unwrap();
                let (cursor, first) = std::thread::spawn(move || {
                    let mut cursor = cursor;
                    let first = cursor.next().unwrap().unwrap();
                    (cursor, first)
                })
                .join()
                .unwrap();
                assert_eq!(first.row_group, group);
                assert_eq!(first.batch_in_group, 0);
                outputs.push(first);
                let remaining = std::thread::spawn(move || {
                    let mut cursor = cursor;
                    let rest = cursor.by_ref().collect::<Result<Vec<_>>>().unwrap();
                    assert!(cursor.next().is_none());
                    let _stats = cursor.statistics();
                    rest
                })
                .join()
                .unwrap();
                for (i, batch) in remaining.iter().enumerate() {
                    assert_eq!(
                        (batch.row_group, batch.batch_in_group),
                        (group, i as u64 + 1)
                    );
                }
                outputs.extend(remaining);
            }
            assert_eq!(
                budget.peak_workers(),
                0,
                "cursor must not launch a nested pool"
            );
            assert!(
                budget.used() > 0,
                "output retains credits after source EOF/drop"
            );
            outputs.sort_by_key(|b| (b.row_group, b.batch_in_group));
            let batches = outputs.iter().map(|b| b.batch.clone()).collect::<Vec<_>>();
            let actual = concat_batches(&fixture.batch.schema(), &batches).unwrap();
            let expected = filter_record_batch(
                &fixture.batch,
                &BooleanArray::from((0..69).map(|i| i % 2 == 0).collect::<Vec<_>>()),
            )
            .unwrap();
            assert_eq!(actual, expected);
            drop(batches);
            drop(actual);
            // Explicitly consume both owned parts, as a pipeline sink will.
            for batch in outputs {
                drop(batch.batch);
                drop(batch.lease);
            }
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn pipeline_cursor_cancel_and_budget_errors_are_terminal_and_release_credits() {
        let fixture = Fixture::new(false, WriterVersion::PARQUET_1_0, 69);
        let factory: FilterFactory = Arc::new(|_| None);
        for cancel in [false, true] {
            let budget = Budget::new(if cancel { 1 << 20 } else { 1 });
            let options = Options {
                batch_size: 3,
                ..Options::default()
            };
            let (file, metadata) = fixture.open();
            let mut cursor = cursor::ScanCursor::new(
                file,
                metadata,
                ProjectionMask::all(),
                &factory,
                None,
                &options,
                &budget,
                0,
            )
            .unwrap();
            if cancel {
                options.cancellation.cancel();
            }
            let error = cursor.next().unwrap().err().unwrap();
            assert!(
                error.contains(if cancel { "cancel" } else { "resource-limit" }),
                "{error}"
            );
            assert!(cursor.next().is_none());
            drop(cursor);
            assert_eq!(budget.used(), 0);
        }
        let (file, metadata) = fixture.open();
        assert!(
            cursor::ScanCursor::new(
                file,
                metadata,
                ProjectionMask::all(),
                &factory,
                None,
                &Options::default(),
                &Budget::new(1 << 20),
                99
            )
            .is_err()
        );
    }

    #[test]
    fn pipeline_cursor_later_transform_error_preserves_held_output_owner() {
        let fixture = Fixture::new(false, WriterVersion::PARQUET_1_0, 17);
        let budget = Budget::new(1 << 20);
        let seen = Arc::new(AtomicUsize::new(0));
        let transform: BatchTransform = {
            let seen = seen.clone();
            let budget = budget.clone();
            Arc::new(move |batch| {
                let lease = budget.reserve(batch.get_array_memory_size())?;
                if seen.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Err("injected later transform failure".into());
                }
                Ok(TransformedBatch {
                    batch,
                    lease,
                    preserve_empty: true,
                })
            })
        };
        let (file, metadata) = fixture.open();
        let factory: FilterFactory = Arc::new(|_| None);
        let mut cursor = cursor::ScanCursor::new(
            file,
            metadata,
            ProjectionMask::all(),
            &factory,
            Some(transform),
            &Options {
                batch_size: 3,
                ..Options::default()
            },
            &budget,
            0,
        )
        .unwrap();
        let held = cursor.next().unwrap().unwrap();
        let charged = budget.used();
        assert!(charged >= held.batch.get_array_memory_size());
        assert_eq!(
            cursor.next().unwrap().err().unwrap(),
            "injected later transform failure"
        );
        assert!(cursor.next().is_none());
        drop(cursor);
        assert_eq!(budget.used(), charged);
        assert_eq!(held.batch, fixture.batch.slice(0, 3));
        drop(held);
        assert_eq!(budget.used(), 0);
    }
}
