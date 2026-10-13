//! Read-only catalog views: bind definitions against current snapshots at query time.
use super::*;

pub(super) struct View {
    pub definition: crate::rel::ViewDefinition,
    bytes: usize,
    _reservation: Reservation,
}

impl Session {
    pub(super) fn create_view(&mut self, statement: s::Statement) -> Result<Output> {
        let s::Statement::CreateView {
            name: view_name,
            columns,
            query,
            if_not_exists,
            ..
        } = &statement
        else {
            unreachable!()
        };
        let mut allowed = parse("CREATE VIEW v AS SELECT 1")?;
        let s::Statement::CreateView {
            name,
            columns: allowed_columns,
            query: allowed_query,
            if_not_exists: allowed_if,
            ..
        } = &mut allowed
        else {
            unreachable!()
        };
        *name = view_name.clone();
        *allowed_columns = columns.clone();
        *allowed_query = query.clone();
        *allowed_if = *if_not_exists;
        if statement != allowed {
            return Err(unsupported("CREATE VIEW modifiers"));
        }
        let view_name = super::name(view_name)?;
        if columns.len() > self.limits.max_columns {
            return Err(resource("view column count exceeds session limit"));
        }
        let mut names = Vec::with_capacity(columns.len());
        let mut seen = HashSet::new();
        for column in columns {
            if column.data_type.is_some() || column.options.is_some() {
                return Err(unsupported("view column type/options"));
            }
            let name = ident(&column.name)?;
            if !seen.insert(name.clone()) {
                return Err(semantic("duplicate view column"));
            }
            names.push(name);
        }
        if self.indexes.contains_key(&view_name) {
            return Err(semantic(format!("index already exists: {view_name}")));
        }
        if self.tables.contains_key(&view_name) || self.views.contains_key(&view_name) {
            if *if_not_exists {
                return Ok(Output::Statement { affected_rows: 0 });
            }
            return Err(semantic(format!("table/view already exists: {view_name}")));
        }
        if self.tables.len() + self.views.len() >= self.limits.max_tables {
            return Err(resource("table/view count limit exceeded"));
        }
        // Bound retained AST/string/container overhead conservatively by SQL size.
        let bytes = statement
            .to_string()
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| resource("view metadata overflow"))?;
        self.check_catalog(bytes)?;
        let reservation = self.budget.reserve(bytes).map_err(resource)?;
        let definition = crate::rel::ViewDefinition {
            query: *query.clone(),
            columns: names,
        };
        self.mutation_checkpoint(MutationPoint::Publish)?;
        self.views.insert(
            view_name,
            View {
                definition,
                bytes,
                _reservation: reservation,
            },
        );
        self.bytes += bytes;
        Ok(Output::Statement { affected_rows: 0 })
    }

    pub(super) fn drop_view(
        &mut self,
        view_name: &s::ObjectName,
        if_exists: bool,
    ) -> Result<Output> {
        let name = name(view_name)?;
        if self.tables.contains_key(&name) {
            return Err(semantic(format!("use DROP TABLE for table: {name}")));
        }
        if !self.views.contains_key(&name) && !if_exists {
            return Err(semantic(format!("unknown view: {name}")));
        }
        self.mutation_checkpoint(MutationPoint::Publish)?;
        if let Some(view) = self.views.remove(&name) {
            self.bytes -= view.bytes;
        }
        Ok(Output::Statement { affected_rows: 0 })
    }
}
