//! Movable single-row-group source for owned pipeline jobs. No nested workers.
//! Output buffers carry their query reservation across worker handoff.
use super::*;
use crate::spill::Cancellation;

pub(crate) struct ScanBatch {
    pub row_group: usize,
    pub batch_in_group: u64,
    pub batch: RecordBatch,
    pub lease: Reservation,
}

pub(crate) struct ScanCursor {
    reader: Reader,
    budget: Budget,
    cancellation: Cancellation,
    row_group: usize,
    next_batch: u64,
    done: bool,
}

impl ScanCursor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file: File,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        factory: &FilterFactory,
        transform: Option<BatchTransform>,
        options: &Options,
        budget: &Budget,
        row_group: usize,
    ) -> Result<Self> {
        options.cancellation.check()?;
        if row_group >= metadata.metadata().num_row_groups() {
            return Err("invalid pipeline row-group index".into());
        }
        let mut serial = options.clone();
        serial.workers = 1;
        let reader = Reader::filtered_group(
            file, metadata, projection, factory, transform, &serial, budget, row_group,
        )?;
        Ok(Self {
            reader,
            budget: budget.clone(),
            cancellation: options.cancellation.clone(),
            row_group,
            next_batch: 0,
            done: false,
        })
    }

    pub(crate) fn statistics(&self) -> (usize, usize, usize, usize) {
        self.reader.statistics()
    }

    fn advance(&mut self) -> Result<Option<ScanBatch>> {
        self.cancellation.check()?;
        let Some(batch) = self.reader.next().transpose()? else {
            return Ok(None);
        };
        self.cancellation.check()?;
        // Transfer the exact-transform bridge instead of charging the same
        // retained output twice. Untransformed decoding needs a new output lease.
        let lease = match &mut self.reader {
            Reader::Filtered(reader) => reader.delivered.take(),
            _ => unreachable!("a cursor owns a filtered single-group reader"),
        };
        let lease = match lease {
            Some(lease) => lease,
            None => self.budget.reserve(batch.get_array_memory_size())?,
        };
        let batch_in_group = self.next_batch;
        self.next_batch = self
            .next_batch
            .checked_add(1)
            .ok_or("pipeline source batch counter overflow")?;
        Ok(Some(ScanBatch {
            row_group: self.row_group,
            batch_in_group,
            batch,
            lease,
        }))
    }
}

impl Iterator for ScanCursor {
    type Item = Result<ScanBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.advance() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}
