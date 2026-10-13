//! Python boundary only. SQL is executed by the actual assaydb Session.
use arrow::ipc::writer::StreamWriter;
use assaydb::{
    execute::Options,
    session::{Error as SessionError, Limits, Output, Session},
};
use pyo3::{create_exception, exceptions::PyException, prelude::*, types::PyBytes};
use std::{
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::Mutex,
};

create_exception!(_native, AssayError, PyException);
create_exception!(_native, SQLError, AssayError);
create_exception!(_native, UnsupportedError, AssayError);
create_exception!(_native, ResourceError, AssayError);
create_exception!(_native, InternalError, AssayError);
create_exception!(_native, ConnectionClosedError, AssayError);

struct Failure {
    kind: &'static str,
    message: String,
    hint: Option<&'static str>,
}
impl From<SessionError> for Failure {
    fn from(error: SessionError) -> Self {
        let hint = error.guidance();
        Self {
            kind: error.kind,
            message: error.message,
            hint,
        }
    }
}
impl Failure {
    fn python(self) -> PyErr {
        let message = match self.hint {
            Some(hint) => format!("{}\nHint: {hint}", self.message),
            None => self.message.clone(),
        };
        let error = match self.kind {
            // The native session labels unprefixed parse/bind/evaluation
            // rejections "engine"; they are SQL errors, not wrapper failures.
            "semantic" | "engine" => SQLError::new_err(message),
            "unsupported" => UnsupportedError::new_err(message),
            "resource" => ResourceError::new_err(message),
            "closed" => ConnectionClosedError::new_err(message),
            _ => InternalError::new_err(message),
        };
        Python::attach(|py| {
            let _ = error.value(py).setattr("native_message", self.message);
            if let Some(hint) = self.hint {
                let _ = error.value(py).setattr("hint", hint);
            }
        });
        error
    }
}
fn failed(kind: &'static str, message: impl Into<String>) -> Failure {
    Failure {
        kind,
        message: message.into(),
        hint: None,
    }
}

/// Retained IPC bytes are capped independently from engine query accounting.
struct ExportBuffer {
    bytes: Vec<u8>,
    limit: usize,
    resource_error: bool,
}
impl Write for ExportBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.resource_error = true;
            return Err(io::Error::other("Arrow result exceeds max_result_bytes"));
        }
        self.bytes.try_reserve_exact(bytes.len()).map_err(|error| {
            self.resource_error = true;
            io::Error::other(format!("Arrow result allocation failed: {error}"))
        })?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Reply {
    ipc: Option<Vec<u8>>,
    rowcount: usize,
}
fn encode(output: Output, limit: usize) -> Result<Reply, Failure> {
    match output {
        Output::Statement { affected_rows } => Ok(Reply {
            ipc: None,
            rowcount: affected_rows,
        }),
        Output::Query { execution, .. } => {
            let mut buffer = ExportBuffer {
                bytes: Vec::new(),
                limit,
                resource_error: false,
            };
            let encoded = (|| {
                let mut writer = StreamWriter::try_new(&mut buffer, &execution.batch.schema())?;
                writer.write(&execution.batch)?;
                writer.finish()
            })();
            if let Err(error) = encoded {
                return Err(failed(
                    if buffer.resource_error {
                        "resource"
                    } else {
                        "internal"
                    },
                    error.to_string(),
                ));
            }
            Ok(Reply {
                ipc: Some(buffer.bytes),
                rowcount: execution.batch.num_rows(),
            })
        }
    }
}

#[pyclass(frozen, module = "assaydb._native")]
struct NativeConnection {
    session: Mutex<Option<Session>>,
    max_result_bytes: usize,
}

#[pymethods]
impl NativeConnection {
    #[new]
    fn new(
        py: Python<'_>,
        directory: PathBuf,
        workers: usize,
        memory_bytes: usize,
        catalog_bytes: usize,
        max_result_bytes: usize,
        allow_local_files: bool,
    ) -> PyResult<Self> {
        if max_result_bytes == 0 {
            return Err(ResourceError::new_err("max_result_bytes must be positive"));
        }
        let session = py
            .detach(move || {
                let options = Options {
                    workers,
                    memory_bytes,
                    ..Options::default()
                };
                let limits = Limits {
                    max_catalog_bytes: catalog_bytes,
                    ..Limits::default()
                };
                let mut session =
                    Session::new(&directory, options, limits).map_err(Failure::from)?;
                session.set_local_files_enabled(allow_local_files);
                Ok::<_, Failure>(session)
            })
            .map_err(Failure::python)?;
        Ok(Self {
            session: Mutex::new(Some(session)),
            max_result_bytes,
        })
    }

    fn execute(&self, py: Python<'_>, sql: &str) -> PyResult<(Option<Py<PyBytes>>, usize)> {
        let reply = py
            .detach(|| {
                let mut guard = self
                    .session
                    .lock()
                    .map_err(|_| failed("internal", "connection lock poisoned"))?;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| failed("closed", "connection is closed"))?;
                // A panic invalidates this session; never continue with partially
                // mutated catalog state. Ordinary SQL errors preserve normal reuse.
                match catch_unwind(AssertUnwindSafe(|| {
                    encode(
                        session.execute_sql(sql).map_err(Failure::from)?,
                        self.max_result_bytes,
                    )
                })) {
                    Ok(result) => result,
                    Err(_) => {
                        *guard = None;
                        Err(failed("internal", "engine panicked; connection closed"))
                    }
                }
            })
            .map_err(Failure::python)?;
        let ipc = reply
            .ipc
            .map(|bytes| {
                PyBytes::new_with(py, bytes.len(), |destination| {
                    destination.copy_from_slice(&bytes);
                    Ok(())
                })
                .map(Bound::unbind)
            })
            .transpose()?;
        Ok((ipc, reply.rowcount))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self
                .session
                .lock()
                .map_err(|_| failed("internal", "connection lock poisoned"))?;
            *guard = None;
            Ok::<_, Failure>(())
        })
        .map_err(Failure::python)
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            self.session
                .lock()
                .map(|guard| guard.is_none())
                .map_err(|_| failed("internal", "connection lock poisoned"))
        })
        .map_err(Failure::python)
    }
}

#[pymodule]
fn _native(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeConnection>()?;
    module.add("AssayError", py.get_type::<AssayError>())?;
    module.add("SQLError", py.get_type::<SQLError>())?;
    module.add("UnsupportedError", py.get_type::<UnsupportedError>())?;
    module.add("ResourceError", py.get_type::<ResourceError>())?;
    module.add("InternalError", py.get_type::<InternalError>())?;
    module.add(
        "ConnectionClosedError",
        py.get_type::<ConnectionClosedError>(),
    )?;
    Ok(())
}
