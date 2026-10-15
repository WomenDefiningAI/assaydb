//! Borrowed catalog access for the optional verified-mode crate.
use super::{Column, Session, Table, index::Index};
use crate::types::Type;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use std::{path::PathBuf, sync::Arc};

pub struct CatalogView<'a> {
    session: &'a Session,
    pub bytes: usize,
    pub disk_bytes: usize,
    pub directory: &'a PathBuf,
}

pub struct TableView<'a> {
    columns: &'a [Column],
    pub rows: usize,
    pub schema: &'a Arc<Schema>,
    pub batches: &'a [RecordBatch],
    pub path: &'a PathBuf,
    pub unique: &'a [Vec<usize>],
}

pub struct ColumnView<'a> {
    pub name: &'a String,
    pub ty: &'a Type,
    pub nullable: bool,
    pub precision: u8,
}

impl Session {
    pub fn catalog_view(&self) -> CatalogView<'_> {
        CatalogView {
            session: self,
            bytes: self.bytes,
            disk_bytes: self.disk_bytes,
            directory: &self.directory,
        }
    }
}

impl<'a> CatalogView<'a> {
    pub fn tables(&self) -> impl ExactSizeIterator<Item = (&'a String, TableView<'a>)> {
        self.session
            .tables
            .iter()
            .map(|(name, table)| (name, TableView::new(table)))
    }

    pub fn table(&self, name: &str) -> Option<TableView<'a>> {
        self.session.tables.get(name).map(TableView::new)
    }

    pub fn table_count(&self) -> usize {
        self.session.tables.len()
    }

    pub fn view_count(&self) -> usize {
        self.session.views.len()
    }

    pub fn index_count(&self) -> usize {
        self.session.indexes.len()
    }

    pub fn indexes(&self) -> impl Iterator<Item = (&'a String, serde_json::Value)> {
        self.session
            .indexes
            .iter()
            .map(|(name, index)| (name, index.verified_state()))
    }

    pub fn views(&self) -> impl Iterator<Item = (&'a String, serde_json::Value)> {
        self.session.views.iter().map(|(name, view)| {
            (
                name,
                serde_json::json!({"query_ast":format!("{:?}",view.definition.query),"columns":view.definition.columns}),
            )
        })
    }
}

impl<'a> TableView<'a> {
    fn new(table: &'a Table) -> Self {
        Self {
            columns: &table.columns,
            rows: table.rows,
            schema: &table.schema,
            batches: &table.batches,
            path: &table.path,
            unique: &table.unique,
        }
    }

    pub fn columns(&self) -> impl ExactSizeIterator<Item = ColumnView<'a>> {
        self.columns.iter().map(ColumnView::new)
    }

    pub fn column(&self, index: usize) -> ColumnView<'a> {
        ColumnView::new(&self.columns[index])
    }
}

impl<'a> ColumnView<'a> {
    fn new(column: &'a Column) -> Self {
        Self {
            name: &column.name,
            ty: &column.ty,
            nullable: column.nullable,
            precision: column.precision,
        }
    }
}

impl Index {
    fn verified_state(&self) -> serde_json::Value {
        serde_json::json!({"table":self.table,"columns":self._columns,"unique":self.unique,"key":self.key})
    }
}

/// Supports the verified crate's schema-corruption regression fixture.
#[doc(hidden)]
pub fn replace_table_schema(session: &mut Session, name: &str, schema: Arc<Schema>) {
    session.tables.get_mut(name).unwrap().schema = schema;
}
