//! Single-replay controller. Source checks detect mutation under the
//! existing immutable-input contract; they do not provide snapshot isolation.
use super::*;
use std::{
    hash::Hasher,
    io::{Read, Seek, SeekFrom},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

#[derive(Debug)]
pub(crate) enum ResourceReason {
    AccountedMemory(String),
    SliceTooSmall,
    OversizedBatch,
    ClosureCredit,
}
impl ResourceReason {
    fn message(&self) -> &str {
        match self {
            Self::AccountedMemory(message) => message,
            Self::SliceTooSmall => "resource-limit: pipeline slice too small for empty state",
            Self::OversizedBatch => {
                "resource-limit: evaluated batch cannot fit empty pipeline slice"
            }
            Self::ClosureCredit => "pipeline closure memory limit",
        }
    }
}
#[derive(Debug)]
pub(crate) enum Failure {
    Resource(ResourceReason),
    Sql(String),
    Terminal(String),
}
impl Failure {
    pub(crate) fn sql(message: String) -> Self {
        // Current SQL evaluation uses string errors. Unknown errors deliberately
        // remain terminal rather than making invariant/decode failures replayable.
        if crate::execute::error_replay::semantic_error(&message) {
            Self::Sql(message)
        } else {
            Self::from(message)
        }
    }
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Resource(reason) => reason.message(),
            Self::Sql(message) | Self::Terminal(message) => message,
        }
    }
}
impl From<String> for Failure {
    fn from(message: String) -> Self {
        if message.starts_with("resource-limit: accounted memory ") {
            Self::Resource(ResourceReason::AccountedMemory(message))
        } else {
            Self::Terminal(message)
        }
    }
}
impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        Self::from(message.to_owned())
    }
}
pub(crate) type AttemptResult<T> = std::result::Result<T, Failure>;
#[derive(Default)]
struct Errors {
    resource: Option<String>,
    sql: Option<(usize, String)>,
    terminal: Option<String>,
}
#[derive(Default)]
pub(crate) struct Attempt {
    pub(crate) admitted: AtomicBool,
    errors: Mutex<Errors>,
}
impl Attempt {
    pub(crate) fn diagnostics(&self) -> (Option<String>, Option<String>) {
        let errors = self.errors.lock().unwrap();
        (
            errors.resource.clone().or_else(|| errors.terminal.clone()),
            errors.sql.as_ref().map(|(_, error)| error.clone()),
        )
    }
    pub(crate) fn record(&self, rank: usize, failure: Failure) -> String {
        let message = failure.message().to_owned();
        let mut errors = self.errors.lock().unwrap();
        match failure {
            Failure::Resource(_) => {
                errors.resource.get_or_insert_with(|| message.clone());
            }
            Failure::Sql(_) => {
                if errors.sql.as_ref().is_none_or(|(old, _)| rank < *old) {
                    errors.sql = Some((rank, message.clone()));
                }
            }
            Failure::Terminal(_) => {
                errors.terminal.get_or_insert_with(|| message.clone());
            }
        }
        message
    }
    pub(crate) fn rejection(&self, reason: &'static str) -> String {
        let failure = if reason == "pipeline closure memory limit" {
            Failure::Resource(ResourceReason::ClosureCredit)
        } else {
            Failure::Terminal(reason.into())
        };
        self.record(usize::MAX, failure)
    }
    /// A driver may return the same already-recorded producer/receipt error.
    /// Only genuinely new driver errors require classification at this boundary.
    pub(crate) fn returned(&self, message: String) {
        let errors = self.errors.lock().unwrap();
        let recorded = errors.resource.as_ref() == Some(&message)
            || errors.sql.as_ref().is_some_and(|(_, e)| *e == message)
            || errors.terminal.as_ref() == Some(&message);
        drop(errors);
        if !recorded {
            self.record(usize::MAX, Failure::from(message));
        }
    }
    pub(crate) fn decision(
        &self,
        original: String,
    ) -> std::result::Result<(String, Option<String>), String> {
        let errors = self.errors.lock().unwrap();
        if let Some(terminal) = &errors.terminal {
            return Err(terminal.clone());
        }
        match &errors.resource {
            Some(resource) => Ok((
                resource.clone(),
                errors.sql.as_ref().map(|(_, e)| e.clone()),
            )),
            None => Err(errors.sql.as_ref().map_or(original, |(_, e)| e.clone())),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Source {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    identity: (u64, u64),
    footer: u64,
}
impl Source {
    pub(crate) fn capture(path: &std::path::Path) -> Result<Self> {
        Self::capture_checked(path, &|| Ok(()))
    }
    pub(crate) fn capture_checked(
        path: &std::path::Path,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Self> {
        check()?;
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let before = file.metadata().map_err(|e| e.to_string())?;
        let len = before.len();
        if len < 12 {
            return Err("pipeline source has no Parquet footer".into());
        }
        file.seek(SeekFrom::End(-8)).map_err(|e| e.to_string())?;
        let mut trailer = [0_u8; 8];
        file.read_exact(&mut trailer).map_err(|e| e.to_string())?;
        let footer_len = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as u64;
        if &trailer[4..] != b"PAR1" || footer_len > len - 12 {
            return Err("pipeline source has invalid Parquet footer".into());
        }
        file.seek(SeekFrom::Start(len - 8 - footer_len))
            .map_err(|e| e.to_string())?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut remaining = footer_len + 8;
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            check()?;
            let n = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..n])
                .map_err(|e| e.to_string())?;
            hasher.write(&buffer[..n]);
            remaining -= n as u64;
        }
        let result = Self::from_metadata(&before, hasher.finish())?;
        if result
            != Self::from_metadata(&file.metadata().map_err(|e| e.to_string())?, result.footer)?
        {
            return Err("pipeline source changed while capturing identity".into());
        }
        Ok(result)
    }
    fn from_metadata(metadata: &std::fs::Metadata, footer: u64) -> Result<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().map_err(|e| e.to_string())?,
            #[cfg(unix)]
            identity: (metadata.dev(), metadata.ino()),
            footer,
        })
    }
    pub(crate) fn verify(&self, path: &std::path::Path) -> Result<()> {
        self.verify_checked(path, &|| Ok(()))
    }
    pub(crate) fn verify_checked(
        &self,
        path: &std::path::Path,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        if *self != Self::capture_checked(path, check)? {
            return Err("pipeline source changed during query; replay forbidden".into());
        }
        Ok(())
    }
}

pub(crate) fn execute(prepared: &Prepared, options: &Options, budget: Budget) -> Result<Execution> {
    let started = Instant::now();
    let mut report = PipelineAttemptReport::default();
    let result = execute_reported(prepared, options, budget.clone(), &mut report);
    report.total_seconds = started.elapsed().as_secs_f64();
    report.peak_reserved_bytes = budget.peak();
    report.peak_workers = budget.peak_workers();
    #[cfg(test)]
    if let Some(target) = &options.test_pipeline_interruption {
        let mut interruption = target.lock().unwrap();
        if interruption.budget.is_some() && interruption.attempt_report.is_none() {
            // Preserve the interrupted direct attempt before the outer forced
            // relational retry replaces the ordinary last-attempt report.
            interruption.attempt_report = Some(report.clone());
        }
    }
    #[cfg(test)]
    if let Some(target) = &options.test_pipeline_report {
        *target.lock().unwrap() = report;
    }
    result
}
fn execute_reported(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    report: &mut PipelineAttemptReport,
) -> Result<Execution> {
    let started = Instant::now();
    let entry_used = budget.used();
    let entry_workers = budget.active_workers();
    let path = prepared.plan.path.as_ref().expect("admitted pipeline path");
    let source = Source::capture(path)?;
    let attempt = Arc::new(Attempt::default());
    let outcome = super::execute_attempt(prepared, options, budget.clone(), false, attempt.clone());
    let initial_seconds = started.elapsed().as_secs_f64();
    let initial_peak = budget.peak();
    report.candidate_attempts = usize::from(attempt.admitted.load(Ordering::Acquire));
    report.candidate_seconds = initial_seconds;
    report.candidate_peak_reserved_bytes = initial_peak;
    {
        let errors = attempt.errors.lock().unwrap();
        report.candidate_error = errors.resource.clone().or_else(|| errors.terminal.clone());
        report.candidate_sql_error = errors.sql.as_ref().map(|(_, e)| e.clone());
    }
    options.cancellation.check()?;
    source.verify(path)?;
    if !attempt.admitted.load(Ordering::Acquire) {
        report.candidate_seconds = 0.0;
        return outcome.map(|mut result| {
            result.metrics.execution_seconds = started.elapsed().as_secs_f64();
            result
        }); // Before-consumption baseline selection is never retried.
    }
    match outcome {
        Ok(mut result) => {
            if attempt.admitted.load(Ordering::Acquire) {
                result.metrics.pipeline_candidate_attempts = 1;
                result.metrics.pipeline_candidate_seconds = initial_seconds;
                result.metrics.pipeline_candidate_peak_reserved_bytes = initial_peak;
            }
            result.metrics.execution_seconds = started.elapsed().as_secs_f64();
            Ok(result)
        }
        Err(original) => {
            attempt.returned(original.clone());
            {
                let errors = attempt.errors.lock().unwrap();
                report.candidate_error =
                    errors.resource.clone().or_else(|| errors.terminal.clone());
                report.candidate_sql_error = errors.sql.as_ref().map(|(_, e)| e.clone());
            }
            let (resource, sql) = attempt.decision(original)?;
            if budget.used() != entry_used || budget.active_workers() != entry_workers {
                return Err("pipeline candidate cleanup did not restore entry ownership".into());
            }
            options.cancellation.check()?;
            source.verify(path)?;
            report.fallback_attempts = 1;
            let fallback_started = Instant::now();
            let result = super::super::execute_baseline(prepared, options, budget.clone());
            report.fallback_seconds = fallback_started.elapsed().as_secs_f64();
            options.cancellation.check()?;
            source.verify(path)?;
            let mut result = result?;
            result.metrics.pipeline_candidate_attempts = 1;
            result.metrics.pipeline_fallbacks = 1;
            result.metrics.pipeline_candidate_seconds = initial_seconds;
            result.metrics.pipeline_fallback_seconds = fallback_started.elapsed().as_secs_f64();
            result.metrics.pipeline_candidate_peak_reserved_bytes = initial_peak;
            result.metrics.pipeline_candidate_error = Some(resource);
            result.metrics.pipeline_candidate_sql_error = sql;
            result.metrics.execution_seconds = started.elapsed().as_secs_f64();
            result.metrics.peak_reserved_bytes = budget.peak();
            result.metrics.worker_threads_peak = budget.peak_threads();
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classification_is_narrow_and_terminal_errors_dominate_mixed_failures() {
        let attempt = Attempt::default();
        attempt.record(0, Failure::sql("division by zero".into()));
        assert_eq!(
            attempt.decision("division by zero".into()).unwrap_err(),
            "division by zero"
        );
        attempt.record(1, Failure::Resource(ResourceReason::OversizedBatch));
        assert_eq!(
            attempt
                .decision("division by zero".into())
                .unwrap()
                .1
                .as_deref(),
            Some("division by zero")
        );
        attempt.record(2, Failure::from("resource-limit: spill disk quota"));
        assert_eq!(
            attempt.decision("division by zero".into()).unwrap_err(),
            "resource-limit: spill disk quota"
        );
        assert!(matches!(
            Failure::from("resource-limit: accounted memory 5 exceeds budget 4"),
            Failure::Resource(_)
        ));
        for error in [
            "memory accounting overflow",
            "resource-limit: other",
            "pipeline completion slots full",
            "pipeline row count mismatch",
            "panic",
        ] {
            assert!(
                matches!(Failure::from(error), Failure::Terminal(_)),
                "{error}"
            );
        }
    }

    #[test]
    fn source_identity_rejects_replacement_and_footer_mutation() {
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "assaydb-source-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let replacement = path.with_extension("replacement");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        for target in [&path, &replacement] {
            let mut writer = ArrowWriter::try_new(
                File::create_new(target).unwrap(),
                schema.clone(),
                Some(WriterProperties::default()),
            )
            .unwrap();
            writer
                .write(
                    &RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![1]))],
                    )
                    .unwrap(),
                )
                .unwrap();
            writer.close().unwrap();
        }
        let original = Source::capture(&path).unwrap();
        original.verify(&path).unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert!(original.verify(&path).is_err());
        let replaced = Source::capture(&path).unwrap();
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(b"0").unwrap();
        drop(file);
        assert!(replaced.verify(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
