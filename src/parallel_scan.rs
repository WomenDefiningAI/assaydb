//! Bounded row-group workers with ordered delivery and query-owned reservations.
use crate::{
    Result,
    execute::Options,
    memory::{Budget, Reservation, Workers},
    parquet_scan::{BatchTransform, FilterFactory, Reader},
    spill::Cancellation,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ProjectionMask, arrow_reader::ArrowReaderMetadata};
use std::{
    collections::VecDeque,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Default, Clone)]
pub(crate) struct Statistics {
    pub tasks: usize,
    pub batches: usize,
    pub pending_peak: usize,
    pub pending_bytes_peak: usize,
    pub queue_slots: usize,
    pub worker_seconds: f64,
    pub caller_seconds: f64,
    pub caller_tasks: usize,
    pub backpressure_seconds: f64,
    pub reader: (usize, usize, usize, usize),
    pending: usize,
    pending_bytes: usize,
}
#[derive(Default)]
struct Lane {
    pending: Mutex<(usize, usize)>,
    ready: Condvar,
}
impl Lane {
    fn wait(&self, target: usize, stop: &AtomicBool, cancel: &Cancellation) -> Result<()> {
        let mut pending = self.pending.lock().unwrap();
        // Each lane keeps the old two-batch progress allowance, even if one
        // decoded batch exceeds its target. Later groups cannot consume the
        // current group's credit. The overshoot is at most one decoded batch
        // beyond the target, or two batches when the target is smaller.
        while pending.0 >= 2 && pending.1 >= target && !stop.load(Ordering::Acquire) {
            cancel.check()?;
            pending = self
                .ready
                .wait_timeout(pending, Duration::from_millis(20))
                .unwrap()
                .0;
        }
        cancel.check()
    }
}
struct Pending {
    stats: Arc<Mutex<Statistics>>,
    lane: Arc<Lane>,
    bytes: usize,
}
impl Pending {
    fn new(stats: &Arc<Mutex<Statistics>>, lane: &Arc<Lane>, bytes: usize) -> Self {
        let mut pending = lane.pending.lock().unwrap();
        pending.0 += 1;
        pending.1 += bytes;
        drop(pending);
        let mut s = stats.lock().unwrap();
        s.pending += 1;
        s.pending_peak = s.pending_peak.max(s.pending);
        s.pending_bytes += bytes;
        s.pending_bytes_peak = s.pending_bytes_peak.max(s.pending_bytes);
        Self {
            stats: stats.clone(),
            lane: lane.clone(),
            bytes,
        }
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        let mut s = self.stats.lock().unwrap();
        s.pending -= 1;
        s.pending_bytes -= self.bytes;
        drop(s);
        let mut pending = self.lane.pending.lock().unwrap();
        pending.0 -= 1;
        pending.1 -= self.bytes;
        drop(pending);
        self.lane.ready.notify_one();
    }
}
struct Batch {
    batch: RecordBatch,
    lease: Reservation,
    pending: Pending,
}
enum Message {
    Batch(usize, Batch),
    End(usize),
    Error(String),
}

// An unstarted group has exactly one owner. Background lanes keep their
// strided assignments; the already-counted caller may steal only before open.
const UNCLAIMED: u8 = 0;
const BACKGROUND: u8 = 1;
const CALLER: u8 = 2;
struct CallerConfig {
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    projection: ProjectionMask,
    factory: FilterFactory,
    transform: Option<BatchTransform>,
    options: Options,
    budget: Budget,
    queue_slots: usize,
    lane_bytes: usize,
}
struct CallerGroup {
    group: usize,
    reader: Option<Reader>,
    queue: VecDeque<Message>,
    complete: bool,
}

pub(crate) struct ParallelReader {
    receivers: Vec<Receiver<Message>>,
    handles: Vec<JoinHandle<()>>,
    lanes: Vec<Arc<Lane>>,
    stop: Arc<AtomicBool>,
    cancel: Cancellation,
    stats: Arc<Mutex<Statistics>>,
    group: usize,
    groups: usize,
    delivered: Option<Reservation>,
    ownership: Arc<Vec<AtomicU8>>,
    caller_config: CallerConfig,
    caller: Option<Box<CallerGroup>>,
    caller_lane: Arc<Lane>,
    claim_cursor: usize,
    done: bool,
    _metadata: Reservation,
    _workers: Workers,
}
impl ParallelReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: &Path,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        factory: FilterFactory,
        transform: Option<BatchTransform>,
        options: &Options,
        budget: &Budget,
        workers: Workers,
    ) -> Result<Self> {
        let slots = workers.count;
        // Ordered delivery needs enough lookahead to overlap whole row groups.
        // Bound message count independently of bytes; metadata sizes cannot
        // bound Arrow allocations (dictionary/RLE expansion can be enormous).
        let queue_slots = metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|group| (group.num_rows() as usize).div_ceil(options.batch_size))
            .max()
            .unwrap_or(1)
            .clamp(1, 32);
        let participants = slots + 1;
        let lane_bytes = (budget.limit().saturating_sub(budget.used()) / 32).min(64 * 1024 * 1024)
            / participants;
        let groups = metadata.metadata().num_row_groups();
        let metadata_lease = budget.reserve(
            participants
                .checked_mul(4096 + (queue_slots + 1) * 128)
                .and_then(|n| {
                    groups
                        .checked_mul(std::mem::size_of::<AtomicU8>())
                        .and_then(|g| n.checked_add(g))
                })
                .ok_or("worker metadata overflow")?,
        )?;
        let ownership = Arc::new(
            (0..groups)
                .map(|_| AtomicU8::new(UNCLAIMED))
                .collect::<Vec<_>>(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let transform = transform.map(|transform| {
            let stop = stop.clone();
            Arc::new(move |batch| {
                if stop.load(Ordering::Acquire) {
                    return Err("cancelled: parallel scan stopped".into());
                }
                transform(batch)
            }) as BatchTransform
        });
        let mut local_options = options.clone();
        local_options.workers = 1;
        local_options.parquet_cache_bytes =
            local_options.parquet_cache_bytes.map(|n| n / participants);
        let mut result = Self {
            receivers: Vec::with_capacity(slots),
            handles: Vec::with_capacity(slots),
            lanes: Vec::with_capacity(slots),
            stop,
            cancel: options.cancellation.clone(),
            stats: Arc::new(Mutex::new(Statistics {
                queue_slots,
                ..Default::default()
            })),
            group: 0,
            groups: metadata.metadata().num_row_groups(),
            delivered: None,
            ownership: ownership.clone(),
            caller_config: CallerConfig {
                path: path.to_owned(),
                metadata: metadata.clone(),
                projection: projection.clone(),
                factory: factory.clone(),
                transform: transform.clone(),
                options: local_options.clone(),
                budget: budget.clone(),
                queue_slots,
                lane_bytes,
            },
            caller: None,
            caller_lane: Arc::new(Lane::default()),
            claim_cursor: 0,
            done: false,
            _metadata: metadata_lease,
            _workers: workers,
        };
        for slot in 0..slots {
            // Independent opens are essential: File::try_clone shares the seek
            // offset on Unix, and Parquet's synchronous reader performs seeks.
            let file = File::open(path).map_err(|e| e.to_string())?;
            let (send, receive) = mpsc::sync_channel(queue_slots);
            result.receivers.push(receive);
            let lane = Arc::new(Lane::default());
            result.lanes.push(lane.clone());
            let metadata = metadata.clone();
            let projection = projection.clone();
            let factory = factory.clone();
            let budget = budget.clone();
            let stop = result.stop.clone();
            let transform = transform.clone();
            let stats = result.stats.clone();
            let options = local_options.clone();
            let ownership = ownership.clone();
            let handle = thread::Builder::new()
                .name(format!("assaydb-scan-{slot}"))
                .spawn(move || {
                    let work = || -> Result<()> {
                        for group in (slot..metadata.metadata().num_row_groups()).step_by(slots) {
                            if stop.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            options.cancellation.check()?;
                            if ownership[group]
                                .compare_exchange(
                                    UNCLAIMED,
                                    BACKGROUND,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_err()
                            {
                                continue;
                            }
                            let start = Instant::now();
                            let mut reader = Reader::filtered_group(
                                file.try_clone().map_err(|e| e.to_string())?,
                                metadata.clone(),
                                projection.clone(),
                                &factory,
                                transform.clone(),
                                &options,
                                &budget,
                                group,
                            )?;
                            let mut active = start.elapsed().as_secs_f64();
                            loop {
                                if stop.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                                options.cancellation.check()?;
                                let wait = Instant::now();
                                lane.wait(lane_bytes, &stop, &options.cancellation)?;
                                stats.lock().unwrap().backpressure_seconds +=
                                    wait.elapsed().as_secs_f64();
                                if stop.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                                let start = Instant::now();
                                let next = reader.next();
                                active += start.elapsed().as_secs_f64();
                                let Some(batch) = next else {
                                    break;
                                };
                                let batch = batch?;
                                options.cancellation.check()?;
                                let bytes = batch
                                    .get_array_memory_size()
                                    .checked_add(256)
                                    .ok_or("queued batch overflow")?;
                                let lease = budget.reserve(bytes)?;
                                reader.transfer_batch_ownership();
                                let pending = Pending::new(&stats, &lane, bytes);
                                let message = Message::Batch(
                                    group,
                                    Batch {
                                        batch,
                                        lease,
                                        pending,
                                    },
                                );
                                let wait = Instant::now();
                                if send.send(message).is_err() {
                                    return Ok(());
                                }
                                let mut s = stats.lock().unwrap();
                                s.batches += 1;
                                s.backpressure_seconds += wait.elapsed().as_secs_f64();
                            }
                            let metrics = reader.statistics();
                            drop(reader);
                            {
                                let mut stats = stats.lock().unwrap();
                                stats.tasks += 1;
                                stats.worker_seconds += active;
                                stats.reader.0 += metrics.0;
                                stats.reader.1 += metrics.1;
                                stats.reader.2 += metrics.2;
                                stats.reader.3 += metrics.3;
                            }
                            let wait = Instant::now();
                            if send.send(Message::End(group)).is_err() {
                                return Ok(());
                            }
                            stats.lock().unwrap().backpressure_seconds +=
                                wait.elapsed().as_secs_f64();
                        }
                        Ok(())
                    };
                    if let Err(error) = work() {
                        let _ = send.send(Message::Error(error));
                    }
                })
                .map_err(|e| format!("parallel scan spawn: {e}"))?;
            result.handles.push(handle);
        }
        Ok(result)
    }
    pub fn statistics(&self) -> Statistics {
        self.stats.lock().unwrap().clone()
    }
    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Disconnect first: a producer can be waiting to send into a full queue.
        self.receivers.clear();
        self.caller = None;
        for lane in &self.lanes {
            lane.ready.notify_all();
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
        self.delivered = None;
        self.done = true;
    }
    // Decode at most one Reader step, only while the expected background lane
    // is empty. A step may internally skip filtered-empty batches. Its errors
    // belong to this later group and must wait for ordered delivery.
    fn help_once(&mut self) -> bool {
        if self.stop.load(Ordering::Acquire) {
            return false;
        }
        if self.caller.is_none() {
            self.claim_cursor = self.claim_cursor.max(self.group);
            while self.claim_cursor < self.groups {
                let group = self.claim_cursor;
                self.claim_cursor += 1;
                if self.ownership[group]
                    .compare_exchange(UNCLAIMED, CALLER, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.caller = Some(Box::new(CallerGroup {
                        group,
                        reader: None,
                        queue: VecDeque::with_capacity(self.caller_config.queue_slots + 1),
                        complete: false,
                    }));
                    break;
                }
            }
        }
        let Some(caller) = &mut self.caller else {
            return false;
        };
        if caller.complete {
            return false;
        }
        let cfg = &self.caller_config;
        let (count, bytes) = *self.caller_lane.pending.lock().unwrap();
        // Never wait on caller-owned credit: only ordered consumption can free
        // it. Leave a one-decoded-batch overshoot and two-batch progress allowance.
        if caller.queue.len() >= cfg.queue_slots || (count >= 2 && bytes >= cfg.lane_bytes) {
            return false;
        }
        let start = Instant::now();
        let step = (|| -> Result<Option<Batch>> {
            cfg.options.cancellation.check()?;
            if caller.reader.is_none() {
                let file = File::open(&cfg.path).map_err(|e| e.to_string())?;
                caller.reader = Some(Reader::filtered_group(
                    file,
                    cfg.metadata.clone(),
                    cfg.projection.clone(),
                    &cfg.factory,
                    cfg.transform.clone(),
                    &cfg.options,
                    &cfg.budget,
                    caller.group,
                )?);
            }
            let reader = caller.reader.as_mut().unwrap();
            let Some(batch) = reader.next() else {
                return Ok(None);
            };
            let batch = batch?;
            let bytes = batch
                .get_array_memory_size()
                .checked_add(256)
                .ok_or("queued batch overflow")?;
            let lease = cfg.budget.reserve(bytes)?;
            reader.transfer_batch_ownership();
            let pending = Pending::new(&self.stats, &self.caller_lane, bytes);
            Ok(Some(Batch {
                batch,
                lease,
                pending,
            }))
        })();
        self.stats.lock().unwrap().caller_seconds += start.elapsed().as_secs_f64();
        match step {
            Ok(Some(batch)) => {
                self.stats.lock().unwrap().batches += 1;
                caller.queue.push_back(Message::Batch(caller.group, batch));
            }
            result => {
                if let Some(reader) = caller.reader.take() {
                    let metrics = reader.statistics();
                    let mut stats = self.stats.lock().unwrap();
                    stats.reader.0 += metrics.0;
                    stats.reader.1 += metrics.1;
                    stats.reader.2 += metrics.2;
                    stats.reader.3 += metrics.3;
                }
                caller.complete = true;
                match result {
                    Ok(None) => {
                        let mut stats = self.stats.lock().unwrap();
                        stats.tasks += 1;
                        stats.caller_tasks += 1;
                        caller.queue.push_back(Message::End(caller.group));
                    }
                    Err(error) => caller.queue.push_back(Message::Error(error)),
                    Ok(Some(_)) => unreachable!(),
                }
            }
        }
        true
    }
    fn read(&mut self) -> Result<Option<RecordBatch>> {
        self.delivered = None;
        while self.group < self.groups {
            self.cancel.check()?;
            if self.stop.load(Ordering::Acquire) {
                return Err("cancelled: parallel scan stopped".into());
            }
            let message = if self.ownership[self.group].load(Ordering::Acquire) == CALLER {
                if self.caller.as_ref().unwrap().queue.is_empty() {
                    assert!(self.help_once(), "current caller group can always progress");
                }
                self.caller.as_mut().unwrap().queue.pop_front().unwrap()
            } else {
                let slot = self.group % self.receivers.len();
                match self.receivers[slot].try_recv() {
                    Ok(message) => message,
                    Err(TryRecvError::Empty) => {
                        if self.help_once() {
                            continue;
                        }
                        match self.receivers[slot].recv_timeout(Duration::from_millis(20)) {
                            Ok(message) => message,
                            Err(RecvTimeoutError::Timeout) => continue,
                            Err(RecvTimeoutError::Disconnected) => {
                                return Err(
                                    "parallel scan worker terminated before row-group end".into()
                                );
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        return Err("parallel scan worker terminated before row-group end".into());
                    }
                }
            };
            match message {
                Message::Batch(
                    group,
                    Batch {
                        batch,
                        lease,
                        pending,
                    },
                ) => {
                    if group != self.group {
                        return Err("parallel scan row-group order violated".into());
                    }
                    drop(pending);
                    self.delivered = Some(lease);
                    return Ok(Some(batch));
                }
                Message::End(group) => {
                    if group != self.group {
                        return Err("parallel scan end order violated".into());
                    }
                    if self.ownership[group].load(Ordering::Acquire) == CALLER {
                        self.caller = None;
                    }
                    self.group += 1;
                }
                Message::Error(error) => return Err(error),
            }
        }
        self.shutdown();
        Ok(None)
    }
}
impl Iterator for ParallelReader {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.read() {
            Ok(batch) => batch.map(Ok),
            Err(error) => {
                self.shutdown();
                Some(Err(error))
            }
        }
    }
}
impl Drop for ParallelReader {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, BooleanArray, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::{
        arrow::{
            ArrowWriter,
            arrow_reader::{ArrowPredicateFn, RowFilter},
        },
        file::properties::WriterProperties,
    };
    use std::{fs, path::PathBuf, sync::atomic::AtomicUsize};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new(width: usize) -> Self {
            let path = std::env::temp_dir().join(format!(
                "assaydb-queue-{}-{}.parquet",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let schema = Arc::new(Schema::new(vec![
                Field::new("i", DataType::Int64, false),
                Field::new("s", DataType::Utf8, false),
            ]));
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(0..256)),
                Arc::new(StringArray::from_iter_values(
                    (0..256).map(|i| format!("{i}:{}", "w".repeat(width))),
                )),
            ];
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(8)
                        .build(),
                ),
            )
            .unwrap();
            writer
                .write(&RecordBatch::try_new(schema, arrays).unwrap())
                .unwrap();
            writer.close().unwrap();
            Self(path)
        }
        fn reader(
            &self,
            options: &Options,
            budget: &Budget,
            factory: FilterFactory,
        ) -> Result<Reader> {
            let file = File::open(&self.0).unwrap();
            let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
            Reader::with_transform(
                &self.0,
                file,
                metadata,
                ProjectionMask::all(),
                factory,
                None,
                options,
                budget,
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_file(&self.0).unwrap();
        }
    }
    fn full_queues(reader: &Reader, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while reader.parallel_statistics().unwrap().pending_peak < expected
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(reader.parallel_statistics().unwrap().pending_peak, expected);
    }
    #[test]
    fn caller_steals_only_unstarted_groups_and_orders_bounded_output_errors_and_cleanup() {
        let fixture = Fixture::new(64);
        for mode in ["complete", "later-error", "early-drop", "cancel"] {
            let budget = Budget::new(4 << 20);
            let options = Options {
                workers: 2,
                batch_size: 1,
                parquet_cache_bytes: Some(1 << 20),
                ..Options::default()
            };
            let (started, receive) = mpsc::channel();
            let (release, wait) = mpsc::channel();
            let wait = Mutex::new(wait);
            let transform: BatchTransform = {
                let budget = budget.clone();
                Arc::new(move |batch| {
                    let first = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0);
                    if first == 0 {
                        started.send(()).unwrap();
                        wait.lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(5))
                            .map_err(|e| e.to_string())?;
                    }
                    if (8..16).contains(&first) && matches!(mode, "later-error" | "early-drop") {
                        return Err("injected later caller error".into());
                    }
                    let lease = budget.reserve(batch.get_array_memory_size())?;
                    Ok(crate::parquet_scan::TransformedBatch {
                        batch,
                        lease,
                        preserve_empty: true,
                    })
                })
            };
            let file = File::open(&fixture.0).unwrap();
            let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
            let mut reader = Reader::with_transform(
                &fixture.0,
                file,
                metadata,
                ProjectionMask::all(),
                Arc::new(|_| None),
                Some(transform),
                &options,
                &budget,
            )
            .unwrap();
            receive.recv_timeout(Duration::from_secs(5)).unwrap();
            let Reader::Parallel(parallel) = &mut reader else {
                panic!("parallel reader required");
            };
            assert_eq!(parallel.ownership[0].load(Ordering::Acquire), BACKGROUND);
            assert!(parallel.help_once());
            assert_eq!(parallel.caller.as_ref().unwrap().group, 1);
            assert_eq!(parallel.ownership[1].load(Ordering::Acquire), CALLER);
            while parallel.help_once() {}
            assert!(
                parallel.caller.as_ref().unwrap().queue.len() <= parallel.caller_config.queue_slots
            );
            assert_eq!(
                parallel.caller_config.options.parquet_cache_bytes,
                Some((1 << 20) / 2)
            );
            let caller_pending = parallel.caller_lane.pending.lock().unwrap().0;
            assert!(caller_pending <= parallel.caller_config.queue_slots);
            release.send(()).unwrap();
            if mode == "cancel" {
                options.cancellation.cancel();
                assert!(
                    reader
                        .next()
                        .unwrap()
                        .unwrap_err()
                        .starts_with("cancelled:")
                );
            } else {
                let count = if mode == "complete" {
                    256
                } else if mode == "early-drop" {
                    1
                } else {
                    8
                };
                for row in 0..count {
                    let batch = reader.next().unwrap().unwrap();
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        row
                    );
                }
                if mode == "complete" {
                    assert!(reader.next().is_none());
                    let stats = reader.parallel_statistics().unwrap();
                    assert_eq!(stats.tasks, 32);
                    assert_eq!(stats.batches, 256);
                    assert!(stats.caller_tasks > 0 && stats.caller_seconds > 0.0);
                } else if mode == "later-error" {
                    assert_eq!(
                        reader.next().unwrap().unwrap_err(),
                        "injected later caller error"
                    );
                }
            }
            drop(reader);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.peak_workers(), 1);
            assert_eq!(budget.workers(1, 1).count, 1);
        }
    }

    #[test]
    fn blocked_producers_drop_and_cancel_without_leaking_buffers_or_worker_permits() {
        let fixture = Fixture::new(64);
        for cancel in [false, true] {
            let budget = Budget::new(4 << 20);
            let options = Options {
                workers: 4,
                batch_size: 1,
                parquet_cache_bytes: None,
                ..Options::default()
            };
            let mut reader = fixture
                .reader(&options, &budget, Arc::new(|_| None))
                .unwrap();
            full_queues(&reader, 24);
            assert_eq!(budget.peak_workers(), 3);
            // A second scan shares the same query limit instead of spawning
            // another pool while the first reader still owns its permits.
            let mut nested = fixture
                .reader(&options, &budget, Arc::new(|_| None))
                .unwrap();
            assert!(nested.parallel_statistics().is_none());
            assert!(nested.next().unwrap().is_ok());
            drop(nested);
            if cancel {
                options.cancellation.cancel();
                assert!(
                    reader
                        .next()
                        .unwrap()
                        .unwrap_err()
                        .starts_with("cancelled:")
                );
            } else {
                let first = reader.next().unwrap().unwrap();
                assert_eq!(
                    first
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
                assert!(budget.used() > 0);
                drop(first);
            }
            drop(reader);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(3, 3).count, 3);
        }
    }
    #[test]
    fn byte_backpressure_is_per_lane_and_cancellable_with_ordered_delivery() {
        let fixture = Fixture::new(4096);
        for cancel in [false, true] {
            let budget = Budget::new(128 << 10);
            let options = Options {
                workers: 4,
                batch_size: 1,
                parquet_cache_bytes: None,
                ..Options::default()
            };
            let mut reader = fixture
                .reader(&options, &budget, Arc::new(|_| None))
                .unwrap();
            assert_eq!(reader.parallel_statistics().unwrap().queue_slots, 8);
            // The byte gate, rather than the eight-message channel, stalls all
            // three lanes after their existing two-batch progress allowance.
            full_queues(&reader, 6);
            if cancel {
                options.cancellation.cancel();
                assert!(
                    reader
                        .next()
                        .unwrap()
                        .unwrap_err()
                        .starts_with("cancelled:")
                );
            } else {
                for row in 0..256 {
                    let batch = reader.next().unwrap().unwrap();
                    assert_eq!(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        row
                    );
                }
                assert!(reader.next().is_none());
            }
            drop(reader);
            assert_eq!(budget.used(), 0);
            assert!(budget.peak() <= budget.limit());
            assert_eq!(budget.workers(3, 3).count, 3);
        }
    }
    #[test]
    fn worker_failure_is_delivered_in_row_order_and_releases_queued_owners() {
        let fixture = Fixture::new(64);
        let budget = Budget::new(4 << 20);
        let options = Options {
            workers: 4,
            batch_size: 1,
            parquet_cache_bytes: None,
            ..Options::default()
        };
        let factory: FilterFactory = Arc::new(|error| {
            Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                ProjectionMask::all(),
                move |batch: RecordBatch| {
                    if batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0)
                        >= 8
                    {
                        let message = "injected predicate failure".to_string();
                        *error.lock().unwrap() = Some(message.clone());
                        return Err(arrow::error::ArrowError::ComputeError(message));
                    }
                    Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                },
            ))]))
        });
        let mut reader = fixture.reader(&options, &budget, factory).unwrap();
        for row in 0..8 {
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                row
            );
        }
        assert_eq!(
            reader.next().unwrap().unwrap_err(),
            "injected predicate failure"
        );
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn dropping_scan_stops_inside_a_run_of_empty_transformed_batches() {
        let fixture = Fixture::new(64);
        let budget = Budget::new(4 << 20);
        let options = Options {
            workers: 2,
            batch_size: 1,
            parquet_cache_bytes: None,
            ..Options::default()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(std::sync::Barrier::new(2));
        let (started, receive) = mpsc::channel();
        let transform: BatchTransform = {
            let calls = calls.clone();
            let release = release.clone();
            let budget = budget.clone();
            Arc::new(move |batch| {
                if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    started.send(()).unwrap();
                    release.wait();
                }
                let output = batch.slice(0, 0);
                let lease = budget.reserve(output.get_array_memory_size())?;
                Ok(crate::parquet_scan::TransformedBatch {
                    batch: output,
                    lease,
                    preserve_empty: false,
                })
            })
        };
        let file = File::open(&fixture.0).unwrap();
        let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
        let mut reader = Reader::with_transform(
            &fixture.0,
            file,
            metadata,
            ProjectionMask::all(),
            Arc::new(|_| None),
            Some(transform),
            &options,
            &budget,
        )
        .unwrap();
        receive.recv_timeout(Duration::from_secs(5)).unwrap();
        let Reader::Parallel(parallel) = &reader else {
            panic!("parallel reader required")
        };
        parallel.stop.store(true, Ordering::Release);
        release.wait();
        assert_eq!(
            reader.next().unwrap().unwrap_err(),
            "cancelled: parallel scan stopped"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(options.cancellation.check().is_ok());
        drop(reader);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.workers(1, 1).count, 1);
    }
    #[test]
    fn queue_memory_rejection_joins_workers_and_releases_reservations() {
        let fixture = Fixture::new(4096);
        let budget = Budget::new(32768);
        let options = Options {
            workers: 4,
            batch_size: 8,
            parquet_cache_bytes: None,
            ..Options::default()
        };
        let mut reader = fixture
            .reader(&options, &budget, Arc::new(|_| None))
            .unwrap();
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .starts_with("resource-limit:")
        );
        drop(reader);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak() <= 32768);
    }
}
