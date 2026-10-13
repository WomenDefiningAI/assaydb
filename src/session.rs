//! Native, ephemeral, strictly typed SQL sessions. No reference engine is used.
//! Catalog publication happens only after a complete Parquet snapshot succeeds.
mod delete;
mod index;
mod insert_source;
mod local_files;
mod reindex;
mod replace;
mod update;
mod view;
use crate::{
    execute::{self, Execution, Options},
    memory::{Budget, Reservation},
    types::{self, Type},
};
use arrow::{
    array::*,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use sqlparser::{ast as s, dialect::GenericDialect, parser::Parser};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

mod constraints;

pub const MAX_SQL_BYTES: usize = 1024 * 1024;
static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Error {
    pub kind: &'static str,
    pub message: String,
}
type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}
impl std::error::Error for Error {}
impl Error {
    /// Optional human-facing advice; stable native error text/classification is unchanged.
    /// Casts choose a new explicit type policy, not a universal equivalent rewrite.
    pub fn guidance(&self) -> Option<&'static str> {
        if self.message.contains(
            "row-dependent mixed Int/Float conditional requires tagged numeric representation",
        ) {
            Some(
                "Choose a common type explicitly by casting each reachable CASE/COALESCE value arm before the conditional. REAL may round integers beyond 2^53; INTEGER truncates fractions and has range limits.",
            )
        } else if self
            .message
            .contains("mixed Int/Float IN subquery comparison")
        {
            Some(
                "Choose the same explicit type for the left expression and the subquery output. REAL may round integers beyond 2^53; INTEGER truncates fractions and has range limits. These casts choose semantics rather than preserve every original result.",
            )
        } else if self
            .message
            .to_ascii_lowercase()
            .contains("recursion limit")
        {
            Some(
                "Simplify deeply nested expressions or materialize intermediate results in separate statements. A CTE may still be expanded; it does not guarantee a lower nesting depth.",
            )
        } else {
            None
        }
    }
}

fn err(kind: &'static str, message: impl Into<String>) -> Error {
    Error {
        kind,
        message: message.into(),
    }
}
fn internal(e: impl std::fmt::Display) -> Error {
    err("internal", e.to_string())
}
fn unsupported(message: impl Into<String>) -> Error {
    err("unsupported", message)
}
fn semantic(message: impl Into<String>) -> Error {
    err("semantic", message)
}
fn resource(message: impl Into<String>) -> Error {
    err("resource", message)
}

#[derive(Clone, Debug)]
pub struct Limits {
    /// Accounted Arrow data plus conservative catalog/batch metadata estimates.
    pub max_catalog_bytes: usize,
    pub max_tables: usize,
    pub max_columns: usize,
    pub max_rows: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_catalog_bytes: 64 * 1024 * 1024,
            max_tables: 128,
            max_columns: 256,
            max_rows: 1_000_000,
        }
    }
}

pub enum Output {
    Statement {
        affected_rows: usize,
    },
    Query {
        execution: Box<Execution>,
        types: Vec<Type>,
    },
}
#[derive(Clone)]
struct Column {
    name: String,
    ty: Type,
    nullable: bool,
    precision: u8,
}
struct Table {
    unique: Vec<Vec<usize>>,
    metadata_bytes: usize,
    _metadata: Reservation,
    columns: Vec<Column>,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    path: PathBuf,
    bytes: usize,
    disk_bytes: usize,
    rows: usize,
    _reservations: Vec<Reservation>,
}
pub struct Session {
    directory: PathBuf,
    tables: HashMap<String, Table>,
    indexes: HashMap<String, index::Index>,
    views: HashMap<String, view::View>,
    options: Options,
    limits: Limits,
    bytes: usize,
    disk_bytes: usize,
    serial: u64,
    local_files_enabled: bool,
    budget: Budget,
    #[cfg(test)]
    cancel_at: Option<MutationPoint>,
}

#[derive(Clone, Copy, PartialEq)]
enum MutationPoint {
    WriterBatch,
    Publish,
}

/// Removes only a file we successfully created, never a caller-supplied path.
struct PendingFile {
    path: PathBuf,
    published: bool,
}
impl Drop for PendingFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}
struct LimitedWriter {
    file: File,
    remaining: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("session storage limit exceeded"));
        }
        let n = self.file.write(bytes)?;
        self.remaining -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn parse(sql: &str) -> Result<s::Statement> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(resource("SQL exceeds 1 MiB"));
    }
    let mut statements = Parser::parse_sql(&crate::dialect::QueryDialect, sql)
        .map_err(|e| unsupported(format!("SQL parser: {e}")))?;
    if statements.len() != 1 {
        return Err(unsupported("exactly one SQL statement required"));
    }
    Ok(statements.remove(0))
}
fn ident(i: &s::Ident) -> Result<String> {
    // Match the existing SELECT catalog's unquoted ASCII identifier scope.
    if i.quote_style.is_some()
        || i.value.is_empty()
        || !i
            .value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err(unsupported("quoted/non-ASCII identifiers"));
    }
    Ok(i.value.to_ascii_lowercase())
}
fn name(n: &s::ObjectName) -> Result<String> {
    if n.0.len() != 1 {
        return Err(unsupported("qualified table names"));
    }
    match &n.0[0] {
        s::ObjectNamePart::Identifier(i) => ident(i),
        _ => Err(unsupported("table name expression")),
    }
}

impl Session {
    pub fn new(parent: &Path, options: Options, limits: Limits) -> Result<Self> {
        options.cancellation.check().map_err(resource)?;
        if !(1..=64).contains(&options.workers)
            || options.batch_size == 0
            || options.batch_size > 65536
        {
            return Err(resource("invalid worker/batch limits"));
        }
        if limits.max_catalog_bytes == 0
            || limits.max_catalog_bytes > options.memory_bytes / 4
            || limits.max_tables == 0
            || limits.max_columns == 0
            || limits.max_rows == 0
        {
            return Err(resource(
                "catalog limit must be positive and <= one quarter of query memory limit",
            ));
        }
        let directory = loop {
            let p = parent.join(format!(
                "assaydb-session-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&p) {
                Ok(()) => break p,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(internal(e)),
            }
        };
        let budget = Budget::new(options.memory_bytes);
        Ok(Self {
            directory,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            views: HashMap::new(),
            options,
            limits,
            bytes: 0,
            disk_bytes: 0,
            serial: 0,
            local_files_enabled: false,
            budget,
            #[cfg(test)]
            cancel_at: None,
        })
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// Opt in to reading arbitrary local regular files named by SQL table functions.
    /// Disabled by default, including the native session protocol. Disabling also
    /// prevents subsequent queries of stored views containing these functions.
    pub fn set_local_files_enabled(&mut self, enabled: bool) {
        self.local_files_enabled = enabled;
    }

    pub fn catalog_bytes(&self) -> usize {
        self.bytes
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Output> {
        self.execute_sql_mode(sql, false)
    }

    // INSERT sources are query-internal values. The target assignment performs
    // the appropriate INTEGER/DECIMAL/REAL storage check before publication.
    fn execute_sql_mode(&mut self, sql: &str, internal_output: bool) -> Result<Output> {
        if internal_output && !matches!(parse(sql)?, s::Statement::Query(_)) {
            return Err(internal("read-only INSERT source was not a query"));
        }
        self.options.cancellation.check().map_err(resource)?;
        if let Some(result) = self.try_reindex(sql) {
            return result;
        }
        match parse(sql)? {
            s::Statement::CreateTable(create) => self.create(create),
            s::Statement::Insert(insert) => self.insert(insert),
            statement @ s::Statement::Delete(_) => self.delete(statement),
            s::Statement::CreateIndex(index) => self.create_index(index),
            statement @ s::Statement::CreateView { .. } => self.create_view(statement),
            s::Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                table: None,
            } if names.len() == 1
                && matches!(object_type, s::ObjectType::Index | s::ObjectType::View) =>
            {
                if object_type == s::ObjectType::Index {
                    self.drop_index(&names[0], if_exists)
                } else {
                    self.drop_view(&names[0], if_exists)
                }
            }
            statement @ s::Statement::Update { .. } => self.update(statement),
            s::Statement::Drop {
                object_type: s::ObjectType::Table,
                if_exists,
                names,
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                table: None,
            } if names.len() == 1 => {
                let name = name(&names[0])?;
                if self.views.contains_key(&name) {
                    return Err(semantic(format!("use DROP VIEW for view: {name}")));
                }
                if let Some(table) = self.tables.get(&name) {
                    self.mutation_checkpoint(MutationPoint::Publish)?;
                    fs::remove_file(&table.path).map_err(internal)?;
                    let table = self.tables.remove(&name).expect("existing table");
                    self.bytes -= table.bytes;
                    self.disk_bytes -= table.disk_bytes;
                    self.remove_table_indexes(&name);
                } else if !if_exists {
                    return Err(semantic(format!("unknown table: {name}")));
                }
                Ok(Output::Statement { affected_rows: 0 })
            }
            s::Statement::Query(_) => {
                let paths = self
                    .tables
                    .iter()
                    .map(|(n, t)| (n.clone(), t.path.clone()))
                    .collect();
                let mut options = self.options.clone();
                options.memory_bytes -= self.bytes;
                let convert = |e: String| {
                    let kind = if e.starts_with("unsupported") {
                        "unsupported"
                    } else if e.starts_with("semantic:") {
                        "semantic"
                    } else if e.starts_with("resource:")
                        || e.contains("budget")
                        || e.contains("memory")
                        || e.contains("cancel")
                    {
                        "resource"
                    } else {
                        "engine"
                    };
                    err(kind, e)
                };
                let imports = if self.local_files_enabled {
                    Some(local_files::Imports::new(self)?)
                } else {
                    None
                };
                let views = self
                    .views
                    .iter()
                    .map(|(name, view)| (name.clone(), &view.definition))
                    .collect();
                let mut plan = if let Some(imports) = &imports {
                    let reader = |function: &str, path: &Path| {
                        imports.read(function, path).map_err(|e| e.to_string())
                    };
                    execute::prepare_with_local_files(sql, &paths, &options, &views, &reader)
                } else if self.views.is_empty() {
                    execute::prepare(sql, &paths, &options)
                } else {
                    execute::prepare_with_views(sql, &paths, &options, &views)
                }
                .map_err(convert)?;
                if let Some(imports) = &imports {
                    options.memory_bytes =
                        options.memory_bytes.saturating_sub(imports.memory_bytes());
                }
                plan.internal_output = internal_output;
                let result = execute::execute(&plan, &options).map_err(convert)?;
                Ok(Output::Query {
                    execution: Box::new(result),
                    types: plan.plan.types.clone(),
                })
            }
            _ => Err(unsupported("unsupported session statement")),
        }
    }

    fn mutation_checkpoint(&self, _point: MutationPoint) -> Result<()> {
        #[cfg(test)]
        if self.cancel_at == Some(_point) {
            self.options.cancellation.cancel();
        }
        self.options.cancellation.check().map_err(resource)
    }

    fn create(&mut self, create: s::CreateTable) -> Result<Output> {
        // A complete AST comparison rejects every unimplemented modifier,
        // including future parser fields, before IF NOT EXISTS can short circuit.
        let s::Statement::CreateTable(mut allowed) = parse("CREATE TABLE t(x INT)")? else {
            unreachable!()
        };
        allowed.name = create.name.clone();
        allowed.columns = create.columns.clone();
        allowed.constraints = create.constraints.clone();
        allowed.if_not_exists = create.if_not_exists;
        if allowed != create {
            return Err(unsupported("CREATE TABLE modifiers/constraints"));
        }
        let table_name = name(&create.name)?;
        if create.columns.is_empty() || create.columns.len() > self.limits.max_columns {
            return Err(resource("column count outside session limit"));
        }
        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        for def in &create.columns {
            self.options.cancellation.check().map_err(resource)?;
            let column = column(def)?;
            if !seen.insert(column.name.clone()) {
                return Err(semantic(format!("duplicate column: {}", column.name)));
            }
            columns.push(column);
        }
        let unique = constraints::declarations(&create, &mut columns)?;
        if self.indexes.contains_key(&table_name) {
            return Err(semantic(format!("index already exists: {table_name}")));
        }
        if self.tables.contains_key(&table_name) || self.views.contains_key(&table_name) {
            if create.if_not_exists {
                return Ok(Output::Statement { affected_rows: 0 });
            }
            return Err(semantic(format!("table already exists: {table_name}")));
        }
        if self.tables.len() + self.views.len() >= self.limits.max_tables {
            return Err(resource("table count limit exceeded"));
        }
        let bytes = 512
            + columns.iter().map(|c| 256 + c.name.len()).sum::<usize>()
            + table_name.len()
            + unique.iter().map(|key| 32 + key.len() * 8).sum::<usize>();
        self.check_catalog(bytes)?;
        let reservation = self.budget.reserve(bytes).map_err(resource)?;
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(&c.name, c.ty.arrow(), c.nullable))
                .collect::<Vec<_>>(),
        ));
        let (mut pending, disk_bytes) = self.write_snapshot(schema.clone(), &[], None)?;
        self.mutation_checkpoint(MutationPoint::Publish)?;
        self.tables.insert(
            table_name,
            Table {
                unique,
                metadata_bytes: bytes,
                _metadata: reservation,
                columns,
                schema,
                batches: vec![],
                path: pending.path.clone(),
                bytes,
                disk_bytes,
                rows: 0,
                _reservations: vec![],
            },
        );
        self.bytes += bytes;
        self.disk_bytes += disk_bytes;
        pending.published = true;
        Ok(Output::Statement { affected_rows: 0 })
    }
    fn check_catalog(&self, more: usize) -> Result<()> {
        if more > self.limits.max_catalog_bytes.saturating_sub(self.bytes) {
            Err(resource("catalog memory limit exceeded"))
        } else {
            Ok(())
        }
    }

    fn insert(&mut self, insert: s::Insert) -> Result<Output> {
        let s::Statement::Insert(mut allowed) = parse("INSERT INTO t VALUES (0)")? else {
            unreachable!()
        };
        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| unsupported("INSERT requires a source query"))?;
        allowed.table = insert.table.clone();
        allowed.columns = insert.columns.clone();
        allowed.source = insert.source.clone();
        allowed.into = insert.into;
        let replacing = insert.replace_into || insert.or == Some(s::SqliteOnConflict::Replace);
        allowed.replace_into = insert.replace_into;
        if insert.or == Some(s::SqliteOnConflict::Replace) {
            allowed.or = insert.or;
        }
        if allowed != insert {
            return Err(unsupported("INSERT modifiers/constraints"));
        }
        insert_source::validate(source)?;
        let s::TableObject::TableName(object) = &insert.table else {
            return Err(unsupported("INSERT target"));
        };
        let table_name = name(object)?;
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| semantic(format!("unknown table: {table_name}")))?;
        let indices = if insert.columns.is_empty() {
            (0..table.columns.len()).collect::<Vec<_>>()
        } else {
            let mut seen = HashSet::new();
            let mut indices = Vec::new();
            for c in &insert.columns {
                let n = ident(c)?;
                if !seen.insert(n.clone()) {
                    return Err(semantic(format!("duplicate INSERT column: {n}")));
                }
                indices.push(
                    table
                        .columns
                        .iter()
                        .position(|c| c.name == n)
                        .ok_or_else(|| semantic(format!("unknown column: {n}")))?,
                );
            }
            indices
        };
        if !matches!(source.body.as_ref(), s::SetExpr::Values(_)) {
            return self.insert_query(&table_name, &indices, source, replacing);
        }
        let s::SetExpr::Values(values) = source.body.as_ref() else {
            unreachable!()
        };
        let s::Statement::Insert(template) = parse("INSERT INTO t VALUES (0)")? else {
            unreachable!()
        };
        let mut allowed_source = template.source.expect("template source");
        allowed_source.body = source.body.clone();
        if allowed_source != *source || values.explicit_row {
            return Err(unsupported("INSERT VALUES query modifiers"));
        }
        if !replacing && values.rows.len() > self.limits.max_rows.saturating_sub(table.rows) {
            return Err(resource("table row limit exceeded"));
        }
        let cells = values
            .rows
            .len()
            .checked_mul(table.columns.len())
            .ok_or_else(|| resource("INSERT cell count overflow"))?;
        if cells > 100_000 {
            return Err(resource("INSERT exceeds 100000 cells"));
        }
        // Charge retained output and the construction peak before allocating
        // Cells, copied strings, or Arrow arrays. Parser AST overhead is bounded
        // by MAX_SQL_BYTES but is explicitly outside these buffer reservations.
        let literal_bytes = values
            .rows
            .iter()
            .flatten()
            .map(literal_bytes)
            .sum::<usize>();
        let retained_upper = cells * 32 + literal_bytes + table.columns.len() * 512 + 512;
        if !replacing {
            self.check_catalog(retained_upper)?;
        }
        let mut retained = self.budget.reserve(retained_upper).map_err(resource)?;
        let scratch_bytes = cells * (std::mem::size_of::<Cell>() + 32)
            + values.rows.len() * std::mem::size_of::<Vec<Cell>>()
            + literal_bytes * 3
            + table.columns.len() * 256;
        let _scratch = self.budget.reserve(scratch_bytes).map_err(resource)?;
        let mut rows = Vec::with_capacity(values.rows.len());
        for values in &values.rows {
            self.options.cancellation.check().map_err(resource)?;
            if values.len() != indices.len() {
                return Err(semantic("INSERT row/column count mismatch"));
            }
            let mut row = vec![Cell::Null; table.columns.len()];
            for (value, &index) in values.iter().zip(&indices) {
                row[index] = literal(value, &table.columns[index])?;
            }
            for (value, column) in row.iter().zip(&table.columns) {
                if matches!(value, Cell::Null) && !column.nullable {
                    return Err(semantic(format!("NOT NULL violation: {}", column.name)));
                }
            }
            rows.push(row);
        }
        let batch = make_batch(table.schema.clone(), &table.columns, &rows)?;
        let added = batch.get_array_memory_size() + 256 + table.columns.len() * 128;
        if added > retained_upper {
            return Err(resource("Arrow output exceeds prepaid INSERT allowance"));
        }
        retained.release_excess(added);
        drop(rows);
        if replacing {
            self.replace_insert(&table_name, batch, retained)
        } else {
            self.append_batch(&table_name, batch, retained, added)
        }
    }

    fn append_batch(
        &mut self,
        table_name: &str,
        batch: RecordBatch,
        retained: Reservation,
        added: usize,
    ) -> Result<Output> {
        let table = self.tables.get(table_name).expect("validated table");
        let rows_added = batch.num_rows();
        let schema = table.schema.clone();
        // Both VALUES and query INSERT stage descriptors for every old batch.
        // Reserve here before cloning so no caller can omit this live peak.
        let staging_bytes = (table.batches.len() + 1)
            .checked_mul(std::mem::size_of::<RecordBatch>() + table.columns.len() * 16)
            .ok_or_else(|| resource("INSERT descriptor size overflow"))?;
        let _staging = self.budget.reserve(staging_bytes).map_err(resource)?;
        let mut batches = Vec::with_capacity(table.batches.len() + 1);
        batches.extend(table.batches.iter().cloned());
        batches.push(batch.clone());
        self.validate_constraints(table_name, table, &batches)?;
        let old_path = table.path.clone();
        let (mut pending, disk_bytes) = self.write_snapshot(schema, &batches, None)?;
        self.mutation_checkpoint(MutationPoint::Publish)?;
        fs::remove_file(&old_path).map_err(internal)?;
        let table = self.tables.get_mut(table_name).expect("validated table");
        self.disk_bytes = self.disk_bytes - table.disk_bytes + disk_bytes;
        table.disk_bytes = disk_bytes;
        table.path = pending.path.clone();
        table.batches.push(batch);
        table._reservations.push(retained);
        table.bytes += added;
        table.rows += rows_added;
        self.bytes += added;
        pending.published = true;
        Ok(Output::Statement {
            affected_rows: rows_added,
        })
    }

    fn replace_batches(
        &mut self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        retained: Reservation,
        retained_bytes: usize,
        affected_rows: usize,
    ) -> Result<Output> {
        let table = self.tables.get(table_name).expect("validated table");
        self.validate_constraints(table_name, table, &batches)?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        if rows > self.limits.max_rows {
            return Err(resource("table row limit exceeded"));
        }
        let bytes = table
            .metadata_bytes
            .checked_add(retained_bytes)
            .ok_or_else(|| resource("catalog byte count overflow"))?;
        if bytes
            > self
                .limits
                .max_catalog_bytes
                .saturating_sub(self.bytes - table.bytes)
        {
            return Err(resource("catalog memory limit exceeded"));
        }
        let old_path = table.path.clone();
        let schema = table.schema.clone();
        let (mut pending, disk_bytes) = self.write_snapshot(schema, &batches, None)?;
        self.mutation_checkpoint(MutationPoint::Publish)?;
        fs::remove_file(&old_path).map_err(internal)?;
        let table = self.tables.get_mut(table_name).expect("validated table");
        self.disk_bytes = self.disk_bytes - table.disk_bytes + disk_bytes;
        self.bytes = self.bytes - table.bytes + bytes;
        table.bytes = bytes;
        table.disk_bytes = disk_bytes;
        table.path = pending.path.clone();
        table.rows = rows;
        table.batches = batches;
        table._reservations = vec![retained];
        pending.published = true;
        Ok(Output::Statement { affected_rows })
    }

    fn insert_query(
        &mut self,
        table_name: &str,
        indices: &[usize],
        source: &s::Query,
        replacing: bool,
    ) -> Result<Output> {
        // Execute against the old catalog. A self-insert observes exactly one snapshot.
        let Output::Query { execution, types } =
            self.execute_sql_mode(&source.to_string(), true)?
        else {
            return Err(internal("read-only INSERT source returned a statement"));
        };
        let input = &execution.batch;
        let table = self.tables.get(table_name).expect("validated table");
        if input.num_columns() != indices.len() {
            return Err(semantic("INSERT query/column count mismatch"));
        }
        if !replacing && input.num_rows() > self.limits.max_rows.saturating_sub(table.rows) {
            return Err(resource("table row limit exceeded"));
        }
        for (src, &dst) in indices.iter().enumerate() {
            validate_assignment_type(&types[src], &table.columns[dst].ty)?;
        }
        let cells = input
            .num_rows()
            .checked_mul(table.columns.len())
            .ok_or_else(|| resource("INSERT cell count overflow"))?;
        if cells > 100_000 {
            return Err(resource("INSERT exceeds 100000 cells"));
        }
        // Execution has a separate query budget; charge its still-live Arrow output
        // to the session budget before allocating conversion or writer buffers.
        let _query_credit = self
            .budget
            .reserve(input.get_array_memory_size())
            .map_err(resource)?;
        let retained_upper =
            cells * 32 + input.get_array_memory_size() + table.columns.len() * 512 + 512;
        if !replacing {
            self.check_catalog(retained_upper)?;
        }
        let mut retained = self.budget.reserve(retained_upper).map_err(resource)?;
        let _scratch = self
            .budget
            .reserve(
                cells * (std::mem::size_of::<Cell>() + 32)
                    + input.get_array_memory_size() * 3
                    + input.num_rows() * 24,
            )
            .map_err(resource)?;
        let mut rows = Vec::with_capacity(input.num_rows());
        for r in 0..input.num_rows() {
            self.options.cancellation.check().map_err(resource)?;
            let mut row = vec![Cell::Null; table.columns.len()];
            for (src, &dst) in indices.iter().enumerate() {
                row[dst] = array_cell(
                    input.column(src).as_ref(),
                    &types[src],
                    r,
                    &table.columns[dst],
                )?;
            }
            rows.push(row);
        }
        let batch = make_batch(table.schema.clone(), &table.columns, &rows)?;
        let added = batch.get_array_memory_size() + 256 + table.columns.len() * 128;
        if added > retained_upper {
            return Err(resource("Arrow output exceeds prepaid INSERT allowance"));
        }
        retained.release_excess(added);
        drop(rows);
        drop(execution);
        if replacing {
            self.replace_insert(table_name, batch, retained)
        } else {
            self.append_batch(table_name, batch, retained, added)
        }
    }

    fn write_snapshot(
        &mut self,
        schema: Arc<Schema>,
        batches: &[RecordBatch],
        last: Option<&RecordBatch>,
    ) -> Result<(PendingFile, usize)> {
        let path = self
            .directory
            .join(format!("table-{}.parquet", self.serial));
        self.serial += 1;
        self.write_snapshot_at(path, schema, batches, last, 0)
    }

    fn write_snapshot_at(
        &self,
        path: PathBuf,
        schema: Arc<Schema>,
        batches: &[RecordBatch],
        last: Option<&RecordBatch>,
        extra_disk_bytes: usize,
    ) -> Result<(PendingFile, usize)> {
        self.options.cancellation.check().map_err(resource)?;
        let writer_limit = self.options.memory_bytes / 4;
        let _writer_credit = self.budget.reserve(writer_limit).map_err(resource)?;
        let disk_limit = self
            .limits
            .max_catalog_bytes
            .saturating_mul(4)
            .saturating_add(1024 * 1024);
        let file = File::create_new(&path).map_err(internal)?;
        let pending = PendingFile {
            path: path.clone(),
            published: false,
        };
        let sink = LimitedWriter {
            file,
            remaining: disk_limit
                .saturating_sub(self.disk_bytes)
                .saturating_sub(extra_disk_bytes),
        };
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_max_row_group_size(8192)
            .build();
        let convert = |e: parquet::errors::ParquetError| {
            if e.to_string().contains("session storage limit") {
                resource(e.to_string())
            } else {
                internal(e)
            }
        };
        let schema_bytes = schema.fields().len() * 512;
        let mut writer = ArrowWriter::try_new(sink, schema, Some(properties)).map_err(convert)?;
        for batch in batches.iter().chain(last) {
            for start in (0..batch.num_rows()).step_by(8192) {
                self.mutation_checkpoint(MutationPoint::WriterBatch)?;
                let slice = batch.slice(start, (batch.num_rows() - start).min(8192));
                // Slices share allocations, making this conservative even for
                // large backing arrays. Reserve before encoder growth; codec
                // allocator overhead still isn't an OS memory cap.
                let allowance = slice
                    .get_array_memory_size()
                    .saturating_mul(2)
                    .saturating_add(schema_bytes);
                if allowance > writer_limit {
                    return Err(resource("Parquet input exceeds writer allowance"));
                }
                if writer.memory_size() > writer_limit - allowance {
                    writer.flush().map_err(convert)?;
                }
                writer.write(&slice).map_err(convert)?;
                if writer.memory_size() > writer_limit {
                    return Err(resource("Parquet writer memory limit exceeded"));
                }
            }
        }
        self.options.cancellation.check().map_err(resource)?;
        writer.close().map_err(convert)?;
        let size = fs::metadata(&path).map_err(internal)?.len() as usize;
        Ok((pending, size))
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        for table in self.tables.values() {
            let _ = fs::remove_file(&table.path);
        }
        // Leave unrelated files alone, including anything added by a caller.
        let _ = fs::remove_dir(&self.directory);
    }
}

fn column(def: &s::ColumnDef) -> Result<Column> {
    use s::DataType as D;
    let mut precision = 38;
    let ty = match &def.data_type {
        D::Int(None) | D::Integer(None) | D::BigInt(None) => Type::Int,
        D::Real | D::Float(None) | D::Double(s::ExactNumberInfo::None) | D::DoublePrecision => {
            Type::Float
        }
        D::Boolean | D::Bool => Type::Bool,
        D::Text => Type::Text,
        D::Varchar(n)
        | D::CharacterVarying(n)
        | D::CharVarying(n)
        | D::Char(n)
        | D::Character(n) => {
            // Approved SQLite-compatible text policy: numeric declared widths
            // do not limit, truncate or pad text. Units/MAX syntax stays unsupported.
            if n.as_ref()
                .is_some_and(|n| !matches!(n, s::CharacterLength::IntegerLength { unit: None, .. }))
            {
                return Err(unsupported("character length unit/syntax"));
            }
            Type::Text
        }
        D::Decimal(n) | D::Numeric(n) | D::Dec(n) => {
            let (p, scale) = match n {
                s::ExactNumberInfo::None => (38, 0),
                s::ExactNumberInfo::Precision(p) => (*p, 0),
                s::ExactNumberInfo::PrecisionAndScale(p, scale) => (*p, *scale),
            };
            if p == 0 || p > 38 || scale > p {
                return Err(unsupported("DECIMAL precision/scale"));
            }
            precision = p as u8;
            Type::Decimal(scale as i8)
        }
        D::Date => Type::Date,
        _ => return Err(unsupported(format!("column type: {}", def.data_type))),
    };
    let mut nullable = true;
    for option in &def.options {
        if option.name.is_some() {
            return Err(unsupported("named column constraints"));
        }
        match option.option {
            s::ColumnOption::Null => (),
            s::ColumnOption::NotNull => nullable = false,
            s::ColumnOption::Unique {
                characteristics: None,
                is_primary,
            } => {
                if is_primary {
                    nullable = false;
                }
            }
            _ => return Err(unsupported("column constraint/default")),
        }
    }
    Ok(Column {
        name: ident(&def.name)?,
        ty,
        nullable,
        precision,
    })
}

#[derive(Clone)]
enum Cell {
    Null,
    Int(i64),
    Decimal(i128),
    Float(f64),
    Bool(bool),
    Text(String),
    Date(i32),
}
fn validate_assignment_type(source: &Type, target: &Type) -> Result<()> {
    if source == target
        || *source == Type::Null
        || matches!(
            (target, source),
            (Type::Decimal(_), Type::Int | Type::Decimal(_))
                | (Type::Float, Type::Int | Type::Decimal(_))
        )
    {
        if let (Type::Decimal(new), Type::Decimal(old)) = (target, source)
            && new < old
        {
            return Err(unsupported("implicit decimal scale loss is unsupported"));
        }
        Ok(())
    } else {
        Err(semantic(format!(
            "assignment type mismatch: {source:?} to {target:?}"
        )))
    }
}

// Assignment conversion uses existing typed numeric rules, never SQLite affinity.
fn array_cell(array: &dyn Array, source: &Type, row: usize, column: &Column) -> Result<Cell> {
    if crate::expr::is_sql_null(array, row) {
        return Ok(Cell::Null);
    }
    let exact = || -> Result<i128> {
        if let Some(a) = array.as_any().downcast_ref::<Decimal128Array>() {
            Ok(a.value(row))
        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
            Ok(i128::from(a.value(row)))
        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
            Ok(i128::from(a.value(row)))
        } else {
            Err(internal("invalid exact numeric array"))
        }
    };
    match (&column.ty, source) {
        (Type::Int, Type::Int) => Ok(Cell::Int(
            i64::try_from(exact()?).map_err(|_| semantic("integer assignment overflow"))?,
        )),
        (Type::Decimal(scale), Type::Int | Type::Decimal(_)) => {
            let old = source.scale().map_err(internal)?;
            let value = types::rescale(exact()?, old, *scale).map_err(|e| {
                if e.contains("unsupported") {
                    unsupported(e)
                } else {
                    semantic(e)
                }
            })?;
            let limit = 10_i128.pow(u32::from(column.precision));
            if value <= -limit || value >= limit {
                return Err(semantic("decimal precision overflow"));
            }
            Ok(Cell::Decimal(value))
        }
        (Type::Float, Type::Int | Type::Decimal(_) | Type::Float) => {
            let value = if *source == Type::Float {
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| internal("invalid float array"))?
                    .value(row)
            } else {
                crate::numeric_float::scaled(exact()?, source.scale().map_err(internal)?)
                    .map_err(unsupported)?
            };
            if !value.is_finite() {
                return Err(semantic("non-finite float assignment"));
            }
            Ok(Cell::Float(value))
        }
        (Type::Text, Type::Text) => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal("invalid text array"))?
                .value(row);
            Ok(Cell::Text(value.to_owned()))
        }
        (Type::Bool, Type::Bool) => Ok(Cell::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| internal("invalid bool array"))?
                .value(row),
        )),
        (Type::Date, Type::Date) => Ok(Cell::Date(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| internal("invalid date array"))?
                .value(row),
        )),
        _ => Err(semantic(format!(
            "assignment type mismatch: {source:?} to {:?}",
            column.ty
        ))),
    }
}

fn literal_bytes(expr: &s::Expr) -> usize {
    match expr {
        s::Expr::Nested(e) | s::Expr::UnaryOp { expr: e, .. } => literal_bytes(e),
        s::Expr::Value(v) => match &v.value {
            s::Value::SingleQuotedString(s) | s::Value::Number(s, _) => s.len(),
            _ => 0,
        },
        _ => 0,
    }
}
fn literal(expr: &s::Expr, column: &Column) -> Result<Cell> {
    if let s::Expr::Nested(e) = expr {
        return literal(e, column);
    }
    if matches!(expr,s::Expr::Value(v) if matches!(v.value,s::Value::Null)) {
        return Ok(Cell::Null);
    }
    if let s::Expr::Value(v) = expr
        && !matches!(
            v.value,
            s::Value::Number(_, false) | s::Value::SingleQuotedString(_) | s::Value::Boolean(_)
        )
    {
        return Err(unsupported("INSERT literal syntax"));
    }
    let numeric = match expr {
        s::Expr::Value(v) => match &v.value {
            s::Value::Number(n, false) => Some(n.clone()),
            _ => None,
        },
        s::Expr::UnaryOp {
            op: s::UnaryOperator::Plus,
            expr,
        }
        | s::Expr::UnaryOp {
            op: s::UnaryOperator::Minus,
            expr,
        } => {
            let s::Expr::Value(v) = expr.as_ref() else {
                return Err(unsupported("INSERT numeric expression"));
            };
            let s::Value::Number(n, false) = &v.value else {
                return Err(unsupported("INSERT numeric expression"));
            };
            Some(n.clone())
        }
        _ => None,
    };
    let numeric = if let (
        Some(n),
        s::Expr::UnaryOp {
            op: s::UnaryOperator::Minus,
            ..
        },
    ) = (&numeric, expr)
    {
        Some(format!("-{n}"))
    } else {
        numeric
    };
    if let Some(n) = numeric {
        if n.contains(['e', 'E']) && matches!(column.ty, Type::Int | Type::Decimal(_)) {
            return Err(unsupported("exponent literal for exact numeric column"));
        }
        return match column.ty {
            Type::Int => n
                .parse()
                .map(Cell::Int)
                .map_err(|_| semantic("integer literal/type overflow")),
            Type::Float => {
                let v: f64 = n.parse().map_err(|_| semantic("invalid float literal"))?;
                if v.is_finite() {
                    Ok(Cell::Float(v))
                } else {
                    Err(semantic("non-finite float literal"))
                }
            }
            Type::Decimal(scale) => {
                let (n0, old) = types::decimal_literal(n.trim_start_matches('-')).map_err(|e| {
                    if e.starts_with("unsupported") {
                        unsupported(e)
                    } else {
                        semantic(e)
                    }
                })?;
                let v = types::rescale(n0, old, scale).map_err(|e| {
                    if e.contains("unsupported") {
                        unsupported(e)
                    } else {
                        semantic(e)
                    }
                })?;
                if v >= 10_i128.pow(u32::from(column.precision)) {
                    return Err(semantic("decimal precision overflow"));
                }
                Ok(Cell::Decimal(if n.starts_with('-') { -v } else { v }))
            }
            _ => Err(semantic("numeric literal does not match column type")),
        };
    }
    match (expr, &column.ty) {
        (s::Expr::Value(v), Type::Text) => {
            if let s::Value::SingleQuotedString(text) = &v.value {
                Ok(Cell::Text(text.clone()))
            } else {
                Err(semantic("literal does not match TEXT column"))
            }
        }
        (s::Expr::Value(v), Type::Bool) => {
            if let s::Value::Boolean(b) = v.value {
                Ok(Cell::Bool(b))
            } else {
                Err(semantic("literal does not match BOOLEAN column"))
            }
        }
        (
            s::Expr::TypedString {
                data_type: s::DataType::Date,
                value,
            },
            Type::Date,
        ) => {
            let s::Value::SingleQuotedString(value) = &value.value else {
                return Err(unsupported("DATE literal syntax"));
            };
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|_| semantic("invalid DATE literal"))?;
            let days = date
                .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days();
            Ok(Cell::Date(
                i32::try_from(days).map_err(|_| semantic("DATE outside range"))?,
            ))
        }
        (s::Expr::Value(_), _) => Err(semantic("literal does not match column type")),
        _ => Err(unsupported(
            "INSERT supports typed literals, NULL and signed numeric literals",
        )),
    }
}

fn make_batch(schema: Arc<Schema>, columns: &[Column], rows: &[Vec<Cell>]) -> Result<RecordBatch> {
    for row in rows {
        for (value, column) in row.iter().zip(columns) {
            if matches!(value, Cell::Null) && !column.nullable {
                return Err(semantic(format!("NOT NULL violation: {}", column.name)));
            }
        }
    }
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        macro_rules! values {
            ($variant:ident) => {
                rows.iter()
                    .map(|r| match &r[i] {
                        Cell::$variant(v) => Some(*v),
                        Cell::Null => None,
                        _ => unreachable!("validated literal"),
                    })
                    .collect::<Vec<_>>()
            };
        }
        let array: ArrayRef = match column.ty {
            Type::Int => Arc::new(Int64Array::from(values!(Int))),
            Type::Float => Arc::new(Float64Array::from(values!(Float))),
            Type::Bool => Arc::new(BooleanArray::from(values!(Bool))),
            Type::Date => Arc::new(Date32Array::from(values!(Date))),
            Type::Decimal(s) => Arc::new(
                Decimal128Array::from(values!(Decimal))
                    .with_precision_and_scale(38, s)
                    .map_err(internal)?,
            ),
            Type::Text => {
                let bytes = rows
                    .iter()
                    .map(|r| match &r[i] {
                        Cell::Text(v) => v.len(),
                        _ => 0,
                    })
                    .sum();
                let mut builder = StringBuilder::with_capacity(rows.len(), bytes);
                for row in rows {
                    match &row[i] {
                        Cell::Text(v) => builder.append_value(v),
                        Cell::Null => builder.append_null(),
                        _ => unreachable!("validated literal"),
                    }
                }
                Arc::new(builder.finish())
            }
            Type::Null => unreachable!("no NULL column declaration"),
            Type::Binary => return Err(unsupported("binary catalog columns")),
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays).map_err(internal)
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    fn session() -> Session {
        Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap()
    }
    fn reset_test_cancellation(s: &mut Session) {
        // Cancellation tokens are intentionally sticky. Reset only the test's
        // injected token to inspect/reuse the unchanged native catalog.
        s.cancel_at = None;
        s.options.cancellation = crate::spill::Cancellation::default();
    }
    fn count(s: &mut Session) -> i64 {
        let Output::Query { execution, .. } = s.execute_sql("SELECT COUNT(*) FROM t").unwrap()
        else {
            panic!("query")
        };
        execution
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[test]
    fn cancelled_statement_entry_preserves_catalog_and_releases_nothing_owned() {
        let mut s = session();
        s.execute_sql("CREATE TABLE t(x INT)").unwrap();
        s.execute_sql("INSERT INTO t VALUES (1)").unwrap();
        let before = s.bytes;
        let disk = fs::read(&s.tables["t"].path).unwrap();
        s.options.cancellation.cancel();
        for sql in [
            "CREATE TABLE u(x INT)",
            "INSERT INTO t VALUES (2)",
            "DROP TABLE t",
            "SELECT x FROM t",
        ] {
            assert_eq!(s.execute_sql(sql).err().unwrap().kind, "resource");
        }
        assert_eq!(s.tables.len(), 1);
        assert_eq!(s.bytes, before);
        assert_eq!(s.budget.used(), before);
        assert_eq!(fs::read(&s.tables["t"].path).unwrap(), disk);
        reset_test_cancellation(&mut s);
        assert_eq!(count(&mut s), 1);
    }

    #[test]
    fn cancellation_during_writer_or_before_publication_rolls_back_pending_file() {
        for point in [MutationPoint::WriterBatch, MutationPoint::Publish] {
            let mut s = session();
            s.execute_sql("CREATE TABLE t(x INT)").unwrap();
            s.execute_sql("INSERT INTO t VALUES (1)").unwrap();
            let before = s.bytes;
            let path = s.tables["t"].path.clone();
            let disk = fs::read(&path).unwrap();
            s.cancel_at = Some(point);
            assert_eq!(
                s.execute_sql("INSERT INTO t VALUES (2),(3)")
                    .err()
                    .unwrap()
                    .kind,
                "resource"
            );
            assert_eq!(s.tables["t"].path, path);
            assert_eq!(fs::read(path).unwrap(), disk);
            assert_eq!(s.bytes, before);
            assert_eq!(s.budget.used(), before);
            assert_eq!(fs::read_dir(s.directory()).unwrap().count(), 1);
            reset_test_cancellation(&mut s);
            assert_eq!(count(&mut s), 1);
            s.execute_sql("INSERT INTO t VALUES (4)").unwrap();
            assert_eq!(count(&mut s), 2);
            s.cancel_at = Some(MutationPoint::Publish);
            assert_eq!(
                s.execute_sql("DROP TABLE t").err().unwrap().kind,
                "resource"
            );
            reset_test_cancellation(&mut s);
            assert_eq!(count(&mut s), 2);
            s.execute_sql("DROP TABLE t").unwrap();
            assert_eq!(s.bytes, 0);
            assert_eq!(s.budget.used(), 0);
        }
    }

    #[test]
    fn cancelled_create_publication_removes_snapshot_and_reservations() {
        let mut s = session();
        s.cancel_at = Some(MutationPoint::Publish);
        assert_eq!(
            s.execute_sql("CREATE TABLE t(x INT)").err().unwrap().kind,
            "resource"
        );
        assert!(s.tables.is_empty());
        assert_eq!(s.bytes, 0);
        assert_eq!(s.budget.used(), 0);
        assert_eq!(fs::read_dir(s.directory()).unwrap().count(), 0);
        reset_test_cancellation(&mut s);
        s.execute_sql("CREATE TABLE t(x INT)").unwrap();
        assert_eq!(count(&mut s), 0);
    }
}
