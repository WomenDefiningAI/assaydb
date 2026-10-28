//! Metadata-derived decoder workspace, acquired before constructing a reader.
//! Chunk sizes bound pages, not allocator retention or total process memory.
use super::*;
use parquet::{
    basic::{Compression, Type as PhysicalType},
    file::metadata::RowGroupMetaData,
};
#[cfg(test)]
mod allocator;
#[cfg(test)]
mod tests;

pub(crate) fn metadata_lease(
    metadata: &ArrowReaderMetadata,
    budget: &Budget,
) -> Result<Arc<Reservation>> {
    budget
        .metadata(metadata.metadata())
        .inspect_err(|_| budget.record_decoder_refusal())
}

fn reserve(budget: &Budget, bytes: usize) -> Result<Reservation> {
    budget
        .reserve(bytes)
        .inspect_err(|_| budget.record_decoder_refusal())
}

#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct Estimate {
    pub workspace: usize,
    pub batch: usize,
}

impl Estimate {
    fn group(group: &RowGroupMetaData, projection: &ProjectionMask, batch: usize) -> Result<Self> {
        let overflow = || "resource-limit: Parquet decoder size overflow".to_string();
        let size = |n: i64| usize::try_from(n).map_err(|_| overflow());
        let rows = size(group.num_rows())?;
        let batch_rows = batch.min(rows);
        let mut result = Self::default();
        for (leaf, column) in group.columns().iter().enumerate() {
            if !projection.leaf_included(leaf) {
                continue;
            }
            let u = size(column.uncompressed_size())?;
            let c = size(column.compressed_size())?;
            let values = size(column.num_values())?;
            let dictionary = if column.dictionary_page_offset().is_some() {
                // A compressed dictionary's offset range cannot bound its
                // decompressed size. Use the entire uncompressed chunk, plus
                // the decoded copy and byte-array dictionary offsets.
                u.checked_mul(2)
                    .and_then(|n| n.checked_add(values.checked_mul(4)?))
                    .ok_or_else(overflow)?
            } else {
                0
            };
            let codec = match column.compression() {
                Compression::UNCOMPRESSED => 0,
                Compression::ZSTD(_) => u.checked_add(384 * 1024).ok_or_else(overflow)?,
                Compression::SNAPPY | Compression::LZ4 | Compression::LZ4_RAW => 64 * 1024,
                Compression::GZIP(_) => 256 * 1024,
                Compression::BROTLI(_) => 16 * 1024 * 1024,
                Compression::LZO => 64 * 1024,
            };
            result.workspace = result
                .workspace
                .checked_add(u)
                .and_then(|n| n.checked_add(c))
                .and_then(|n| n.checked_add(dictionary))
                .and_then(|n| n.checked_add(codec))
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(overflow)?;
            // Averages do not bound a skewed batch. For byte arrays use the
            // decoded chunk total when supplied by the writer. Without it,
            // plain data is bounded by U; dictionary expansion can repeat one
            // U-byte value for every output value. Repeated leaves can contain
            // every chunk value in a single row.
            let batch_values = if column.column_descr().max_rep_level() > 0 {
                values
            } else {
                batch_rows
            };
            let payload = match column.column_type() {
                PhysicalType::BYTE_ARRAY => match column.unencoded_byte_array_data_bytes() {
                    Some(bytes) => size(bytes)?,
                    None if column.dictionary_page_offset().is_some() => {
                        u.checked_mul(batch_values).ok_or_else(overflow)?
                    }
                    None => u,
                },
                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    usize::try_from(column.column_descr().type_length())
                        .map_err(|_| overflow())?
                        .max(16)
                        .checked_mul(batch_values)
                        .ok_or_else(overflow)?
                }
                _ => 16usize.checked_mul(batch_values).ok_or_else(overflow)?,
            };
            let bytes = batch_values
                .checked_mul(12)
                .and_then(|n| n.checked_add(payload))
                .and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(overflow)?;
            result.batch = result.batch.checked_add(bytes).ok_or_else(overflow)?;
        }
        Ok(result)
    }

    pub fn reader(
        metadata: &ArrowReaderMetadata,
        projection: &ProjectionMask,
        filter: Option<&RowFilter>,
        groups: Option<&[usize]>,
        batch: usize,
    ) -> Result<Self> {
        let mut projection = projection.clone();
        if let Some(filter) = filter {
            for predicate in filter.predicates() {
                projection.union(predicate.projection());
            }
        }
        let mut result = Self::default();
        let mut include = |group: &RowGroupMetaData| -> Result<()> {
            let next = Self::group(group, &projection, batch)?;
            result.workspace = result.workspace.max(next.workspace);
            result.batch = result.batch.max(next.batch);
            Ok(())
        };
        if let Some(groups) = groups {
            for &index in groups {
                include(
                    metadata
                        .metadata()
                        .row_groups()
                        .get(index)
                        .ok_or("invalid Parquet row group")?,
                )?;
            }
        } else {
            for group in metadata.metadata().row_groups() {
                include(group)?;
            }
        }
        Ok(result)
    }
}

pub(super) struct DecoderLease {
    workspace: Reservation,
    batch: usize,
    credit: Option<Reservation>,
    _metadata: Arc<Reservation>,
}

impl DecoderLease {
    pub fn new(
        metadata: &ArrowReaderMetadata,
        estimate: Estimate,
        budget: &Budget,
    ) -> Result<Self> {
        let metadata = metadata_lease(metadata, budget)?;
        let bytes = estimate
            .workspace
            .checked_add(estimate.batch)
            .ok_or("resource-limit: Parquet decoder size overflow")?;
        let mut workspace = reserve(budget, bytes)?;
        let credit = workspace.split_off(estimate.batch)?;
        Ok(Self {
            workspace,
            batch: estimate.batch,
            credit: Some(credit),
            _metadata: metadata,
        })
    }

    pub fn prepare(&mut self) -> Result<()> {
        if self.credit.is_none() {
            self.credit = Some(reserve(&self.workspace.budget(), self.batch)?);
        }
        Ok(())
    }

    pub fn output(&mut self, bytes: usize) -> Result<Reservation> {
        let mut credit = self.credit.take().expect("prepared before decoding");
        if bytes > credit.bytes() && !credit.try_grow(bytes - credit.bytes()) {
            // Use Budget's existing resource-limit diagnostic without changing
            // the live reservation on refusal.
            credit.absorb(reserve(&self.workspace.budget(), bytes - credit.bytes())?);
        }
        credit.release_excess(bytes);
        Ok(credit)
    }
}

pub(crate) struct LeasedReader {
    // Drop third-party buffers before returning their credit.
    pub inner: Reader,
    lease: DecoderLease,
    pub delivered: Option<Reservation>,
    done: bool,
    remaining_rows: Option<usize>,
}
impl LeasedReader {
    pub(super) fn new(inner: Reader, lease: DecoderLease, remaining_rows: Option<usize>) -> Self {
        Self {
            inner,
            lease,
            delivered: None,
            done: false,
            remaining_rows,
        }
    }
    pub fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.delivered = None;
        if self.done || self.remaining_rows == Some(0) {
            self.done = true;
            return None;
        }
        if let Err(error) = self.lease.prepare() {
            self.done = true;
            return Some(Err(error));
        }
        match self.inner.next() {
            Some(Ok(batch)) => match self.lease.output(batch.get_array_memory_size()) {
                Ok(credit) => {
                    if let Some(remaining) = &mut self.remaining_rows {
                        *remaining = remaining.saturating_sub(batch.num_rows());
                    }
                    self.delivered = Some(credit);
                    Some(Ok(batch))
                }
                Err(error) => {
                    self.done = true;
                    Some(Err(error))
                }
            },
            result => {
                self.done = true;
                result
            }
        }
    }
}
