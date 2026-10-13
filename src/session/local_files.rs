//! Bounded all-TEXT CSV import. Snapshots belong to one query, never the catalog.
use super::*;
use std::{cell::RefCell, io::Read};

pub(super) struct Imports<'a> {
    session: &'a Session,
    files: RefCell<Vec<(PendingFile, usize)>>,
    metadata: RefCell<Option<Reservation>>,
}
impl<'a> Imports<'a> {
    pub(super) fn new(session: &'a Session) -> Result<Self> {
        Ok(Self {
            session,
            files: RefCell::new(Vec::new()),
            metadata: RefCell::new(None),
        })
    }
    fn reserve_metadata(&self) -> Result<()> {
        let mut metadata = self.metadata.borrow_mut();
        if metadata.is_some() {
            return Ok(());
        }
        let bytes = self
            .session
            .directory
            .as_os_str()
            .len()
            .checked_add(512)
            .and_then(|n| n.checked_mul(32))
            .ok_or_else(|| resource("reader metadata overflow"))?;
        self.session.check_catalog(bytes)?;
        *metadata = Some(self.session.budget.reserve(bytes).map_err(resource)?);
        Ok(())
    }
    pub(super) fn memory_bytes(&self) -> usize {
        self.metadata
            .borrow()
            .as_ref()
            .map_or(0, Reservation::bytes)
    }
    pub(super) fn read(&self, function: &str, path: &Path) -> Result<PathBuf> {
        self.session
            .options
            .cancellation
            .check()
            .map_err(resource)?;
        if !fs::metadata(path).map_err(internal)?.is_file() {
            return Err(unsupported("local reader requires a regular file"));
        }
        match function {
            "read_parquet" => Ok(path.to_owned()),
            "read_csv" => self.read_csv(path),
            _ => Err(unsupported("unknown local reader")),
        }
    }
    pub(super) fn read_csv(&self, path: &Path) -> Result<PathBuf> {
        let s = self.session;
        s.options.cancellation.check().map_err(resource)?;
        self.reserve_metadata()?;
        let mut files = self.files.borrow_mut();
        // Bound simultaneous files and retained path/container overhead independently.
        if files.len() >= 32 {
            return Err(resource("at most 32 CSV readers per query"));
        }
        let file = File::open(path).map_err(internal)?;
        let metadata = file.metadata().map_err(internal)?;
        if !metadata.is_file() {
            return Err(unsupported("CSV input must be a regular file"));
        }
        let size = usize::try_from(metadata.len()).map_err(|_| resource("CSV size overflow"))?;
        // Raw UTF-8, one decoded row, Arrow value/offset growth, schema and builders.
        // Cell count is bounded by source bytes + 1, including empty fields.
        let upper = size
            .checked_add(1)
            .and_then(|n| n.checked_mul(32))
            .and_then(|n| n.checked_add(s.limits.max_columns.checked_mul(2048)?))
            .and_then(|n| n.checked_add(65536))
            .ok_or_else(|| resource("CSV allocation size overflow"))?;
        s.check_catalog(
            upper
                .checked_add(self.memory_bytes())
                .ok_or_else(|| resource("CSV budget overflow"))?,
        )?;
        let _credit = s.budget.reserve(upper).map_err(resource)?;
        let mut bytes = Vec::with_capacity(size + 1);
        file.take((size as u64) + 1)
            .read_to_end(&mut bytes)
            .map_err(internal)?;
        if bytes.len() != size {
            return Err(resource("CSV input changed size while reading"));
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| semantic("CSV must be UTF-8"))?;
        let mut rows = Rows {
            text,
            position: 0,
            max_columns: s.limits.max_columns,
        };
        let header = rows
            .next()?
            .ok_or_else(|| semantic("CSV requires a header record"))?;
        let mut names = HashSet::new();
        for name in &header {
            if name.is_empty() || name.contains('.') || !names.insert(name.to_lowercase()) {
                return Err(semantic(
                    "CSV headers must be nonempty, unique ignoring case, and contain no dot",
                ));
            }
        }
        let schema = Arc::new(Schema::new(
            header
                .iter()
                .map(|name| Field::new(name, arrow::datatypes::DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let mut builders = header
            .iter()
            .map(|_| StringBuilder::new())
            .collect::<Vec<_>>();
        let mut count = 0usize;
        while let Some(row) = rows.next()? {
            s.options.cancellation.check().map_err(resource)?;
            if count >= s.limits.max_rows {
                return Err(resource("CSV row limit exceeded"));
            }
            if row.len() != header.len() {
                return Err(semantic("CSV row/header column count mismatch"));
            }
            for (builder, value) in builders.iter_mut().zip(row) {
                builder.append_value(value);
            }
            count += 1;
        }
        let arrays = builders
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(internal)?;
        if batch.get_array_memory_size() > upper {
            return Err(resource("CSV Arrow output exceeded prepaid allowance"));
        }
        let path = s.directory.join(format!(
            "reader-{}.parquet",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let used = files
            .iter()
            .try_fold(0usize, |sum, (_, bytes)| sum.checked_add(*bytes))
            .ok_or_else(|| resource("CSV snapshot disk size overflow"))?;
        let (pending, disk) = s.write_snapshot_at(path.clone(), schema, &[batch], None, used)?;
        files.push((pending, disk));
        Ok(path)
    }
}

/// Strict RFC-style comma records: LF/CRLF, doubled quotes and quoted newlines.
/// No guessing of NULL tokens, types, whitespace or malformed quotes.
struct Rows<'a> {
    text: &'a str,
    position: usize,
    max_columns: usize,
}
impl Rows<'_> {
    fn next(&mut self) -> Result<Option<Vec<String>>> {
        let bytes = self.text.as_bytes();
        if self.position == bytes.len() {
            return Ok(None);
        }
        let mut row = Vec::new();
        loop {
            if row.len() >= self.max_columns {
                return Err(resource("CSV column limit exceeded"));
            }
            let mut end = self.position;
            let value = if bytes.get(end) == Some(&b'"') {
                let start = end + 1;
                end = start;
                loop {
                    match bytes.get(end) {
                        None => return Err(semantic("CSV unterminated quoted field")),
                        Some(b'"') if bytes.get(end + 1) == Some(&b'"') => end += 2,
                        Some(b'"') => break,
                        _ => end += 1,
                    }
                }
                let value = self.text[start..end].replace("\"\"", "\"");
                end += 1;
                value
            } else {
                let start = end;
                while let Some(&b) = bytes.get(end) {
                    if matches!(b, b',' | b'\n' | b'\r') {
                        break;
                    }
                    if b == b'"' {
                        return Err(semantic("CSV quote in unquoted field"));
                    }
                    end += 1;
                }
                self.text[start..end].to_owned()
            };
            row.push(value);
            match bytes.get(end) {
                None => {
                    self.position = end;
                    return Ok(Some(row));
                }
                Some(b',') => {
                    self.position = end + 1;
                }
                Some(b'\n') => {
                    self.position = end + 1;
                    return Ok(Some(row));
                }
                Some(b'\r') if bytes.get(end + 1) == Some(&b'\n') => {
                    self.position = end + 2;
                    return Ok(Some(row));
                }
                _ => {
                    return Err(semantic(
                        "CSV expected comma or record terminator after field",
                    ));
                }
            }
        }
    }
}
