//! Query-owned temporary Arrow batches used by external operators.
//! Disk quota covers live file bytes, not an OS cap.
use crate::{
    Result,
    memory::{Budget, Reservation},
};
use arrow::{
    array::UInt64Array,
    compute::take,
    error::ArrowError,
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use serde::Serialize;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn io_error(operation: &str, error: io::Error) -> String {
    if error.to_string().starts_with("cancelled:") {
        return error.to_string();
    }
    let prefix = if error.kind() == io::ErrorKind::StorageFull {
        "resource-limit: "
    } else {
        ""
    };
    format!("{prefix}spill {operation}: {error}")
}
fn ipc_error(operation: &str, error: ArrowError) -> String {
    match error {
        ArrowError::IoError(_, error) => io_error(operation, error),
        error => format!("spill {operation}: {error}"),
    }
}

/// External-operator settings. Existing configurations force spilling; automatic
/// mode retries a read-only execution after accounted memory is exhausted.
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpillMode {
    #[default]
    Force,
    Auto,
}
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpillOptions {
    pub mode: SpillMode,
    pub directory: PathBuf,
    pub disk_quota_bytes: u64,
    pub sort_run_bytes: usize,
    pub merge_fan_in: usize,
}
impl Default for SpillOptions {
    fn default() -> Self {
        Self {
            mode: SpillMode::Force,
            directory: std::env::temp_dir(),
            disk_quota_bytes: 8 * 1024 * 1024 * 1024,
            sort_run_bytes: 1024 * 1024,
            merge_fan_in: 4,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Cancellation(Arc<AtomicBool>, Option<&'static AtomicBool>);
impl Cancellation {
    /// Observe an application-owned signal flag as well as explicit cancellation.
    /// The signal handler must only set the flag; query code performs cleanup.
    pub fn with_signal_flag(flag: &'static AtomicBool) -> Self {
        Self(Arc::default(), Some(flag))
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn check(&self) -> Result<()> {
        if self.0.load(Ordering::Acquire) || self.1.is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            Err("cancelled: query execution".into())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct SpillMetrics {
    pub live_bytes: u64,
    pub peak_bytes: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub files_created: u64,
    pub live_files: u64,
    pub cleanup_errors: u64,
}
struct State {
    metrics: SpillMetrics,
    quota: u64,
    pending_removals: Vec<PendingRemoval>,
    #[cfg(test)]
    fail_after: Option<u64>,
    #[cfg(test)]
    fault: Option<TestFault>,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum TestFault {
    WriteFailure,
    ReadFailure,
    StorageFull,
    CancelWrite,
    CancelRead,
    CleanupFile,
}
struct PendingRemoval {
    path: PathBuf,
    bytes: u64,
    _metadata: Option<Reservation>,
}
#[cfg(test)]
thread_local! {
    // Consumed by the next store on this test thread; never present in builds
    // used for benchmarks, and cannot affect concurrently running tests.
    static SQL_FAULT: std::cell::RefCell<Option<TestFault>> = const { std::cell::RefCell::new(None) };
}
struct Session {
    parent: PathBuf,
    directory: Mutex<Option<PathBuf>>,
    next_file: AtomicU64,
    state: Mutex<State>,
    memory: Budget,
    cancel: Cancellation,
    _metadata: Reservation,
}
impl Session {
    fn directory(&self) -> Result<PathBuf> {
        self.cancel.check()?;
        let mut stored = self.directory.lock().unwrap();
        if let Some(path) = stored.as_ref() {
            return Ok(path.clone());
        }
        for _ in 0..128 {
            let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = self
                .parent
                .join(format!("assaydb-spill-{}-{id}", std::process::id()));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => {
                    *stored = Some(path.clone());
                    return Ok(path);
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io_error("directory", e)),
            }
        }
        Err("spill directory: could not allocate unique private directory".into())
    }
    fn cleanup(&self) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            // Retry only paths this session created. Failed removals retain
            // their metadata reservation and remain charged to the disk quota.
            for pending in std::mem::take(&mut state.pending_removals) {
                match fs::remove_file(&pending.path) {
                    Ok(()) => {
                        state.metrics.live_bytes -= pending.bytes;
                        state.metrics.live_files -= 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        state.metrics.live_bytes -= pending.bytes;
                        state.metrics.live_files -= 1;
                    }
                    Err(_) => state.pending_removals.push(pending),
                }
            }
            if let Some(pending) = state.pending_removals.first() {
                return Err(format!(
                    "spill file cleanup: could not remove {}",
                    pending.path.display()
                ));
            }
        }
        let mut path = self.directory.lock().unwrap();
        if let Some(p) = path.as_ref() {
            // Never recursively remove a directory: only our owned files may
            // be deleted, and a nonempty directory must surface an error.
            fs::remove_dir(p).map_err(|e| {
                self.state.lock().unwrap().metrics.cleanup_errors += 1;
                format!("spill directory cleanup: {e}")
            })?;
            *path = None;
        }
        Ok(())
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[derive(Clone)]
pub struct SpillStore(Arc<Session>);
impl SpillStore {
    pub fn new(
        parent: &Path,
        disk_quota: u64,
        memory: Budget,
        cancel: Cancellation,
    ) -> Result<Self> {
        cancel.check()?;
        let metadata = memory.reserve(
            4096_usize
                .checked_add(parent.as_os_str().len().saturating_mul(2))
                .ok_or("spill metadata overflow")?,
        )?;
        Ok(Self(Arc::new(Session {
            parent: parent.to_owned(),
            directory: Mutex::new(None),
            next_file: AtomicU64::new(0),
            state: Mutex::new(State {
                metrics: SpillMetrics::default(),
                quota: disk_quota,
                pending_removals: Vec::new(),
                #[cfg(test)]
                fail_after: None,
                #[cfg(test)]
                fault: SQL_FAULT.with(|f| f.borrow_mut().take()),
            }),
            memory,
            cancel,
            _metadata: metadata,
        })))
    }
    pub fn metrics(&self) -> SpillMetrics {
        self.0.state.lock().unwrap().metrics.clone()
    }
    /// One batch per file keeps replay bounded. Callers must hold their input
    /// reservation throughout the call; this reserves additional codec work.
    pub fn write_batch(&self, batch: &RecordBatch) -> Result<SpillFile> {
        self.0.cancel.check()?;
        let memory_bytes = batch.get_array_memory_size();
        let _workspace = self.0.memory.reserve(workspace(memory_bytes, 0)?)?;
        let metadata = self
            .0
            .memory
            .reserve(4096 + self.0.parent.as_os_str().len().saturating_mul(2))?;
        let directory = self.0.directory()?;
        let id = self.0.next_file.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("batch-{id}.arrow"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_error("file create", e))?;
        {
            let mut state = self.0.state.lock().unwrap();
            state.metrics.files_created += 1;
            state.metrics.live_files += 1;
        }
        let owner = Arc::new(OwnedFile {
            session: self.0.clone(),
            path,
            bytes: AtomicU64::new(0),
            memory_bytes,
            rows: batch.num_rows(),
            _metadata: Some(metadata),
        });
        let writer = QuotaWriter {
            file,
            owner: owner.clone(),
        };
        let mut ipc = FileWriter::try_new(writer, batch.schema().as_ref())
            .map_err(|e| ipc_error("IPC header", e))?;
        ipc.write(batch).map_err(|e| ipc_error("IPC write", e))?;
        ipc.finish().map_err(|e| ipc_error("IPC finish", e))?;
        self.0.cancel.check()?;
        drop(ipc); // Close the file before publishing its immutable owner.
        Ok(SpillFile(owner))
    }
    /// End a query explicitly so cleanup failures are observable. Drop remains
    /// a best-effort fallback on every earlier return/panic path.
    pub fn finish(self) -> Result<SpillMetrics> {
        if Arc::strong_count(&self.0) != 1 {
            return Err("spill cleanup: live store/file handles remain".into());
        }
        self.0.cleanup()?;
        let metrics = self.metrics();
        if metrics.cleanup_errors != 0 || metrics.live_files != 0 || metrics.live_bytes != 0 {
            return Err("spill cleanup: unreleased files or earlier cleanup error".into());
        }
        Ok(metrics)
    }
}
fn workspace(memory: usize, file: usize) -> Result<usize> {
    memory
        .checked_mul(3)
        .and_then(|n| n.checked_add(file))
        .and_then(|n| n.checked_add(65536))
        .ok_or_else(|| "spill workspace overflow".into())
}
struct OwnedFile {
    session: Arc<Session>,
    path: PathBuf,
    bytes: AtomicU64,
    memory_bytes: usize,
    rows: usize,
    _metadata: Option<Reservation>,
}
impl Drop for OwnedFile {
    fn drop(&mut self) {
        #[cfg(test)]
        let injected = {
            let mut state = self.session.state.lock().unwrap();
            if matches!(state.fault, Some(TestFault::CleanupFile)) {
                state.fault = None;
                true
            } else {
                false
            }
        };
        #[cfg(not(test))]
        let injected = false;
        let result = if injected {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected removal failure",
            ))
        } else {
            fs::remove_file(&self.path)
        };
        let removed = match result {
            Ok(()) => true,
            Err(e) => e.kind() == io::ErrorKind::NotFound,
        };
        let mut state = self.session.state.lock().unwrap();
        if removed {
            state.metrics.live_bytes -= self.bytes.load(Ordering::Relaxed);
            state.metrics.live_files -= 1;
        } else {
            state.metrics.cleanup_errors += 1;
            state.pending_removals.push(PendingRemoval {
                path: std::mem::take(&mut self.path),
                bytes: self.bytes.load(Ordering::Relaxed),
                _metadata: self._metadata.take(),
            });
        }
    }
}
struct QuotaWriter {
    file: File,
    owner: Arc<OwnedFile>,
}
impl Write for QuotaWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.owner
            .session
            .cancel
            .check()
            .map_err(io::Error::other)?;
        let count = bytes.len().min(65536);
        if count == 0 {
            return Ok(0);
        }
        let count = {
            let mut state = self.owner.session.state.lock().unwrap();
            #[cfg(test)]
            let count = match state.fault {
                Some(
                    fault @ (TestFault::WriteFailure
                    | TestFault::StorageFull
                    | TestFault::CancelWrite),
                ) => {
                    if state.metrics.bytes_written >= 41 {
                        return Err(match fault {
                            TestFault::CancelWrite => {
                                self.owner.session.cancel.cancel();
                                io::Error::other("cancelled: query execution")
                            }
                            TestFault::StorageFull => {
                                io::Error::new(io::ErrorKind::StorageFull, "injected disk full")
                            }
                            _ => io::Error::other("injected SQL spill write failure"),
                        });
                    }
                    count.min((41 - state.metrics.bytes_written) as usize)
                }
                _ => count,
            };
            #[cfg(test)]
            let count = if let Some(limit) = state.fail_after {
                if state.metrics.bytes_written >= limit {
                    return Err(io::Error::other("injected spill write failure"));
                }
                count.min((limit - state.metrics.bytes_written) as usize)
            } else {
                count
            };
            // Charge before issuing the write, including concurrent writers.
            let proposed = state
                .metrics
                .live_bytes
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::other("spill byte counter overflow"))?;
            if proposed > state.quota {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "spill disk quota exceeded",
                ));
            }
            state.metrics.live_bytes = proposed;
            state.metrics.peak_bytes = state.metrics.peak_bytes.max(proposed);
            count
        };
        let result = self.file.write(&bytes[..count]);
        let written = result.as_ref().copied().unwrap_or(0);
        self.owner
            .bytes
            .fetch_add(written as u64, Ordering::Relaxed);
        let mut state = self.owner.session.state.lock().unwrap();
        state.metrics.live_bytes -= (count - written) as u64;
        state.metrics.bytes_written += written as u64;
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.owner
            .session
            .cancel
            .check()
            .map_err(io::Error::other)?;
        self.file.flush()
    }
}

struct CheckedReader {
    file: File,
    session: Arc<Session>,
}
impl Read for CheckedReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.session.cancel.check().map_err(io::Error::other)?;
        let count = bytes.len().min(65536);
        #[cfg(test)]
        let count = {
            let state = self.session.state.lock().unwrap();
            match state.fault {
                Some(fault @ (TestFault::ReadFailure | TestFault::CancelRead)) => {
                    if state.metrics.bytes_read >= 17 {
                        return Err(if matches!(fault, TestFault::CancelRead) {
                            self.session.cancel.cancel();
                            io::Error::other("cancelled: query execution")
                        } else {
                            io::Error::other("injected SQL spill read failure")
                        });
                    }
                    count.min((17 - state.metrics.bytes_read) as usize)
                }
                _ => count,
            }
        };
        let read = self.file.read(&mut bytes[..count])?;
        self.session.state.lock().unwrap().metrics.bytes_read += read as u64;
        Ok(read)
    }
}
impl Seek for CheckedReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.session.cancel.check().map_err(io::Error::other)?;
        self.file.seek(position)
    }
}

#[derive(Clone)]
pub struct SpillFile(Arc<OwnedFile>);
impl SpillFile {
    pub fn read_batch(&self) -> Result<SpillBatch> {
        self.0.session.cancel.check()?;
        let size = usize::try_from(self.0.bytes.load(Ordering::Relaxed))
            .map_err(|_| "spill file size overflow")?;
        // Private uncompressed IPC only. Reserve the file buffer, decoded
        // arrays and metadata before asking Arrow to allocate any of them.
        let reserved = workspace(self.0.memory_bytes, size)?
            .checked_add(
                self.0
                    .rows
                    .checked_mul(16)
                    .ok_or("spill replay indices overflow")?,
            )
            .ok_or("spill replay workspace overflow")?;
        let lease = self.0.session.memory.reserve(reserved)?;
        let file = File::open(&self.0.path).map_err(|e| format!("spill read: {e}"))?;
        if file.metadata().map_err(|e| e.to_string())?.len() != size as u64 {
            return Err("spill IPC: file length changed".into());
        }
        let checked = CheckedReader {
            file,
            session: self.0.session.clone(),
        };
        let mut reader =
            FileReader::try_new(checked, None).map_err(|e| ipc_error("IPC read", e))?;
        if reader.num_batches() != 1 {
            return Err("spill IPC: expected exactly one batch".into());
        }
        let batch = reader
            .next()
            .ok_or("spill IPC: missing batch")?
            .map_err(|e| ipc_error("IPC decode", e))?;
        self.0.session.cancel.check()?;
        if batch.num_rows() != self.0.rows {
            return Err("spill IPC: row count changed".into());
        }
        // IPC arrays can share a complete message body. Per-array capacity
        // accounting then counts that body repeatedly, and a small retained
        // slice can pin it. Compact each column into its own bounded buffers
        // while the reserved decode/copy workspace still covers both owners.
        let indices = UInt64Array::from_iter_values(0..self.0.rows as u64);
        let arrays = batch
            .columns()
            .iter()
            .map(|a| {
                self.0.session.cancel.check()?;
                take(a, &indices, None).map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            batch.schema(),
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(self.0.rows)),
        )
        .map_err(|e| e.to_string())?;
        if batch.get_array_memory_size() > reserved {
            return Err("spill IPC: compacted batch exceeds reserved workspace".into());
        }
        Ok(SpillBatch {
            batch,
            _lease: Arc::new(lease),
        })
    }
    pub fn bytes(&self) -> u64 {
        self.0.bytes.load(Ordering::Relaxed)
    }
}
/// Retain this wrapper (or its clone) while retaining any replayed arrays.
/// Operator consumers that copy/share arrays beyond it must reserve them.
#[derive(Clone)]
pub struct SpillBatch {
    pub batch: RecordBatch,
    _lease: Arc<Reservation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    fn parent() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "assaydb-spill-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }
    fn batch() -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(17),
                Some(i64::MAX),
            ])),
            Arc::new(StringArray::from(vec![
                Some("é🦀\0"),
                None,
                Some(""),
                Some("last"),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(-101), Some(0), None, Some(999)])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(f64::INFINITY),
                None,
                Some(1.25),
            ])),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(20, 2), true),
            Field::new("f", DataType::Float64, true),
        ]));
        RecordBatch::try_new(schema, arrays).unwrap()
    }
    fn empty(path: &Path) {
        assert_eq!(fs::read_dir(path).unwrap().count(), 0);
        fs::remove_dir(path).unwrap();
    }
    #[test]
    fn sql_io_faults_and_mid_io_cancellation_cleanup_and_allow_fresh_execution() {
        use crate::execute::{Options, execute, prepare};
        let root = parent();
        let input = root.join("input.parquet");
        let spill = root.join("spill");
        fs::create_dir(&spill).unwrap();
        let data = batch();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create_new(&input).unwrap(),
            data.schema(),
            Some(
                parquet::file::properties::WriterProperties::builder()
                    .set_max_row_group_size(1)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&data).unwrap();
        writer.close().unwrap();
        let tables = std::collections::HashMap::from([("t".into(), input.clone())]);
        for sql in [
            "SELECT i,s FROM t ORDER BY s,i",
            "SELECT s,COUNT(*) AS n FROM t GROUP BY s ORDER BY s",
            "SELECT COUNT(DISTINCT s) AS n FROM t",
            "SELECT a.i AS i,b.s AS s FROM t a LEFT JOIN t b ON a.i=b.i ORDER BY i",
            "SELECT i,s IN(SELECT s FROM t) AS yes FROM t ORDER BY i",
            "WITH x AS (SELECT i,s FROM t) SELECT a.i AS i,b.s AS s FROM x a JOIN x b ON a.i=b.i ORDER BY i",
        ] {
            for workers in [1, 4, 10] {
                for fault in [
                    TestFault::WriteFailure,
                    TestFault::ReadFailure,
                    TestFault::StorageFull,
                    TestFault::CancelWrite,
                    TestFault::CancelRead,
                    TestFault::CleanupFile,
                ] {
                    let mut options = Options {
                        workers,
                        batch_size: 2,
                        memory_bytes: 8 << 20,
                        spill: Some(SpillOptions {
                            mode: Default::default(),
                            directory: spill.clone(),
                            sort_run_bytes: 4096,
                            disk_quota_bytes: 16 << 20,
                            merge_fan_in: 2,
                        }),
                        ..Options::default()
                    };
                    let plan = prepare(sql, &tables, &options).unwrap();
                    let control = execute(&plan, &options).unwrap();
                    SQL_FAULT.with(|f| *f.borrow_mut() = Some(fault));
                    let error = execute(&plan, &options)
                        .err()
                        .expect("fault must abort SQL execution");
                    let expected = match fault {
                        TestFault::CancelWrite | TestFault::CancelRead => "cancelled:",
                        TestFault::StorageFull => "resource-limit:",
                        TestFault::WriteFailure => "spill IPC",
                        TestFault::ReadFailure => "spill IPC",
                        TestFault::CleanupFile => "spill cleanup:",
                    };
                    assert!(error.starts_with(expected), "{sql}: {error}");
                    assert_eq!(fs::read_dir(&spill).unwrap().count(), 0, "{sql}: {error}");
                    assert!(SQL_FAULT.with(|f| f.borrow().is_none()));
                    options.cancellation = Cancellation::default();
                    let recovered = execute(&plan, &options).unwrap();
                    assert_eq!(recovered.batch, control.batch, "{sql}");
                    assert_eq!(recovered.metrics.spill.live_files, 0);
                    assert_eq!(recovered.metrics.spill.live_bytes, 0);
                    assert_eq!(fs::read_dir(&spill).unwrap().count(), 0);
                }
            }
        }
        fs::remove_file(input).unwrap();
        fs::remove_dir(spill).unwrap();
        empty(&root);
    }
    #[test]
    fn final_alias_removal_failure_keeps_accounting_until_owned_cleanup_retry() {
        let parent = parent();
        let budget = Budget::new(2 << 20);
        let store =
            SpillStore::new(&parent, 1 << 20, budget.clone(), Cancellation::default()).unwrap();
        let file = store.write_batch(&batch()).unwrap();
        let alias = file.clone();
        store.0.state.lock().unwrap().fault = Some(TestFault::CleanupFile);
        let before = budget.used();
        let disk = store.metrics().live_bytes;
        drop(file);
        assert_eq!(store.metrics().cleanup_errors, 0);
        drop(alias);
        assert_eq!(store.metrics().cleanup_errors, 1);
        assert_eq!(store.metrics().live_files, 1);
        assert_eq!(store.metrics().live_bytes, disk);
        assert_eq!(budget.used(), before);
        assert!(
            store
                .finish()
                .unwrap_err()
                .contains("earlier cleanup error")
        );
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }
    #[test]
    fn replay_slices_empty_batches_and_alias_lifetimes() {
        let parent = parent();
        let budget = Budget::new(2 << 20);
        let store =
            SpillStore::new(&parent, 1 << 20, budget.clone(), Cancellation::default()).unwrap();
        let initial = budget.used();
        for expected in [batch(), batch().slice(1, 2), batch().slice(2, 0)] {
            let file = store.write_batch(&expected).unwrap();
            assert!(file.bytes() > 0);
            let alias = file.clone();
            drop(file);
            let replay = alias.read_batch().unwrap();
            assert_eq!(replay.batch, expected);
            let held = replay.clone();
            drop(replay);
            drop(alias);
            assert_eq!(store.metrics().live_bytes, 0);
            assert!(budget.used() > initial);
            assert_eq!(held.batch, expected);
            drop(held);
            assert_eq!(budget.used(), initial);
        }
        let metrics = store.finish().unwrap();
        assert_eq!(metrics.files_created, 3);
        assert!(metrics.bytes_read > 0);
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }
    #[test]
    fn ipc_shared_body_is_compacted_before_retention() {
        let parent = parent();
        let budget = Budget::new(2 << 20);
        let store =
            SpillStore::new(&parent, 1 << 20, budget.clone(), Cancellation::default()).unwrap();
        let schema = Arc::new(Schema::new(
            (0..12)
                .map(|i| Field::new(format!("s{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let columns = (0..12)
            .map(|column| {
                Arc::new(StringArray::from_iter((0..73).map(|row| {
                    (row % 7 != 0).then(|| format!("{column}:{row}:{}", "é".repeat(30)))
                }))) as ArrayRef
            })
            .collect();
        let expected = RecordBatch::try_new(schema, columns).unwrap();
        let file = store.write_batch(&expected).unwrap();
        let result = file.read_batch().unwrap();
        assert_eq!(result.batch, expected);
        assert!(result.batch.get_array_memory_size() < expected.get_array_memory_size() * 2);
        drop(file);
        drop(result);
        store.finish().unwrap();
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }

    #[test]
    fn disk_quota_and_partial_write_errors_cleanup_and_recover() {
        for fault in [false, true] {
            let parent = parent();
            let budget = Budget::new(2 << 20);
            let store = SpillStore::new(
                &parent,
                if fault { 1 << 20 } else { 128 },
                budget.clone(),
                Cancellation::default(),
            )
            .unwrap();
            if fault {
                store.0.state.lock().unwrap().fail_after = Some(41);
            }
            let error = store.write_batch(&batch()).err().unwrap();
            assert!(
                error.contains(if fault {
                    "injected spill write failure"
                } else {
                    "spill disk quota"
                }),
                "{error}"
            );
            assert!(store.metrics().bytes_written > 0);
            assert_eq!(store.metrics().live_bytes, 0);
            assert_eq!(store.metrics().live_files, 0);
            {
                let mut state = store.0.state.lock().unwrap();
                state.fail_after = None;
                state.quota = 1 << 20;
            }
            let file = store.write_batch(&batch()).unwrap();
            assert_eq!(file.read_batch().unwrap().batch, batch());
            drop(file);
            store.finish().unwrap();
            assert_eq!(budget.used(), 0);
            empty(&parent);
        }
    }
    #[test]
    fn cancellation_and_memory_failure_leave_no_files_or_leases() {
        let parent = parent();
        let budget = Budget::new(2 << 20);
        let cancel = Cancellation::default();
        let store = SpillStore::new(&parent, 1 << 20, budget.clone(), cancel.clone()).unwrap();
        let file = store.write_batch(&batch()).unwrap();
        let used = budget.used();
        let pressure = budget.reserve((2 << 20) - used - 1024).unwrap();
        assert!(file.read_batch().err().unwrap().contains("resource-limit"));
        assert!(
            store
                .write_batch(&batch())
                .err()
                .unwrap()
                .contains("resource-limit")
        );
        drop(pressure);
        assert_eq!(budget.used(), used);
        cancel.cancel();
        assert!(file.read_batch().err().unwrap().contains("cancelled"));
        assert!(
            store
                .write_batch(&batch())
                .err()
                .unwrap()
                .contains("cancelled")
        );
        drop(file);
        store.finish().unwrap();
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }
    #[test]
    fn failed_decode_and_unexpected_directory_contents_are_visible() {
        let parent = parent();
        let budget = Budget::new(2 << 20);
        let store =
            SpillStore::new(&parent, 1 << 20, budget.clone(), Cancellation::default()).unwrap();
        let file = store.write_batch(&batch()).unwrap();
        let used = budget.used();
        File::options()
            .write(true)
            .open(&file.0.path)
            .unwrap()
            .set_len(7)
            .unwrap();
        assert!(file.read_batch().is_err());
        assert_eq!(budget.used(), used);
        drop(file);
        let directory = store.0.directory.lock().unwrap().clone().unwrap();
        let unrelated = directory.join("not-owned");
        fs::write(&unrelated, b"preserve").unwrap();
        assert!(store.finish().unwrap_err().contains("cleanup"));
        assert_eq!(fs::read(&unrelated).unwrap(), b"preserve");
        fs::remove_file(unrelated).unwrap();
        fs::remove_dir(directory).unwrap();
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }
    #[test]
    fn cancellation_is_observed_between_io_chunks() {
        for reading in [false, true] {
            let parent = parent();
            let budget = Budget::new(2 << 20);
            let cancel = Cancellation::default();
            let store = SpillStore::new(&parent, 1 << 20, budget.clone(), cancel.clone()).unwrap();
            let file = store.write_batch(&batch()).unwrap();
            if reading {
                let mut reader = CheckedReader {
                    file: File::open(&file.0.path).unwrap(),
                    session: store.0.clone(),
                };
                assert_eq!(reader.read(&mut [0; 16]).unwrap(), 16);
                cancel.cancel();
                assert!(
                    reader
                        .read(&mut [0; 16])
                        .unwrap_err()
                        .to_string()
                        .contains("cancelled")
                );
                assert!(reader.seek(SeekFrom::Start(0)).is_err());
            } else {
                let mut writer = QuotaWriter {
                    file: OpenOptions::new().append(true).open(&file.0.path).unwrap(),
                    owner: file.0.clone(),
                };
                assert_eq!(writer.write(&vec![0; 70000]).unwrap(), 65536);
                let before = store.metrics().live_bytes;
                cancel.cancel();
                assert!(
                    writer
                        .write(&[1; 16])
                        .unwrap_err()
                        .to_string()
                        .contains("cancelled")
                );
                assert_eq!(store.metrics().live_bytes, before);
            }
            drop(file);
            store.finish().unwrap();
            assert_eq!(budget.used(), 0);
            empty(&parent);
        }
    }

    #[test]
    fn parallel_writers_share_disk_quota_and_drop_cleans_private_directory() {
        let parent = parent();
        let budget = Budget::new(4 << 20);
        let store =
            SpillStore::new(&parent, 32768, budget.clone(), Cancellation::default()).unwrap();
        let probe = store.write_batch(&batch()).unwrap();
        let quota = probe.bytes() * 2;
        drop(probe);
        store.0.state.lock().unwrap().quota = quota;
        std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let store = store.clone();
                    scope.spawn(move || store.write_batch(&batch()))
                })
                .collect::<Vec<_>>();
            let mut files = Vec::new();
            let mut rejected = 0;
            for handle in handles {
                match handle.join().unwrap() {
                    Ok(file) => files.push(file),
                    Err(error) => {
                        assert!(error.contains("spill disk quota"), "{error}");
                        rejected += 1;
                    }
                }
            }
            assert!(rejected >= 6);
            assert!(store.metrics().peak_bytes <= quota);
            assert_eq!(
                store.metrics().live_bytes,
                files.iter().map(SpillFile::bytes).sum::<u64>()
            );
            for file in &files {
                assert_eq!(file.read_batch().unwrap().batch, batch());
            }
        });
        assert_eq!(store.metrics().live_files, 0);
        drop(store);
        assert_eq!(budget.used(), 0);
        empty(&parent);
    }
}
