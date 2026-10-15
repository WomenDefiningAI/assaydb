//! Query-local observation of real reader work; absent from production builds.
use super::{BatchTransform, TransformedBatch};
use crate::memory::Budget;
use arrow::array::Int64Array;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender},
    },
    time::Duration,
};

pub(crate) struct Observer {
    pub path: PathBuf,
    pub matching_batches: AtomicUsize,
    pub lane_batches: AtomicUsize,
    /// Rendezvous after a matching batch; reports whether a scan worker paused.
    pub pause: Option<(SyncSender<bool>, Mutex<Receiver<()>>)>,
}

impl Observer {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            matching_batches: AtomicUsize::new(0),
            lane_batches: AtomicUsize::new(0),
            pause: None,
        }
    }

    pub fn wrap(
        self: &Arc<Self>,
        transform: Option<BatchTransform>,
        budget: Budget,
    ) -> BatchTransform {
        let observer = self.clone();
        Arc::new(move |batch| {
            let matching = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .any(|value| value == Some(3));
            let output = if let Some(transform) = &transform {
                transform(batch)?
            } else {
                TransformedBatch {
                    lease: budget.reserve(batch.get_array_memory_size())?,
                    batch,
                    preserve_empty: true,
                }
            };
            if output.batch.num_columns() == 0 {
                observer.lane_batches.fetch_add(1, Ordering::SeqCst);
            }
            if matching
                && observer.matching_batches.fetch_add(1, Ordering::SeqCst) == 0
                && let Some((started, resume)) = &observer.pause
            {
                let on_worker = std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("assaydb-scan-"));
                started.send(on_worker).map_err(|e| e.to_string())?;
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| format!("test reader rendezvous: {e}"))?;
            }
            Ok(output)
        })
    }
}
