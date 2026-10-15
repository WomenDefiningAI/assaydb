//! Logical indexes retain no physical access path. UNIQUE definitions enforce
//! SQLite-compatible constraints through the ordinary atomic mutation paths.
use super::*;

pub(super) struct Index {
    pub(super) table: String,
    bytes: usize,
    // Preserve source column order/direction even though execution ignores
    // this optional access path. Repeated columns are legal nonunique keys.
    pub(super) _columns: Vec<(String, Option<bool>)>,
    pub(super) unique: bool,
    pub(super) key: Vec<usize>,
    _metadata: Reservation,
}

impl Session {
    // Validate index constraints in catalog row encounter order. Checking only
    // the final table would incorrectly allow a swap through an occupied key.
    // The shadow map is never published: any violation aborts the whole UPDATE.
    pub(super) fn validate_index_update(
        &self,
        table_name: &str,
        table: &Table,
        updated: &[RecordBatch],
    ) -> Result<()> {
        if updated.iter().map(RecordBatch::num_rows).sum::<usize>() != table.rows {
            return Err(internal("UPDATE index row inventory differs"));
        }
        for index in self
            .indexes
            .values()
            .filter(|i| i.unique && i.table == table_name)
        {
            let key = &index.key;
            let strings = table
                .batches
                .iter()
                .chain(updated)
                .flat_map(|batch| key.iter().map(move |&column| batch.column(column)))
                .try_fold(0usize, |n, array| {
                    n.checked_add(
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map_or(0, |a| a.value_data().len()),
                    )
                    .ok_or_else(|| resource("UPDATE index key size overflow"))
                })?;
            // Hash buckets, retained keys and two simultaneous per-row key
            // encodings (old/new) are funded before any key allocation.
            let allowance = key
                .len()
                .checked_mul(96)
                .and_then(|n| n.checked_add(256))
                .and_then(|n| table.rows.checked_mul(n))
                .and_then(|n| strings.checked_mul(2).and_then(|s| n.checked_add(s)))
                .ok_or_else(|| resource("UPDATE index memory overflow"))?;
            let _credit = self.budget.reserve(allowance).map_err(resource)?;
            let mut seen = HashMap::<Vec<u8>, usize>::with_capacity(table.rows);
            let old_rows = || {
                table
                    .batches
                    .iter()
                    .flat_map(|b| (0..b.num_rows()).map(move |r| (b, r)))
            };
            for (id, (batch, row)) in old_rows().enumerate() {
                self.options.cancellation.check().map_err(resource)?;
                if let Some(bytes) = constraints::key_bytes(table, key, batch, row)?
                    && seen.insert(bytes, id).is_some()
                {
                    return Err(internal("existing UNIQUE index contains duplicate keys"));
                }
            }
            let new_rows = updated
                .iter()
                .flat_map(|b| (0..b.num_rows()).map(move |r| (b, r)));
            for (id, ((old, old_row), (new, new_row))) in old_rows().zip(new_rows).enumerate() {
                self.options.cancellation.check().map_err(resource)?;
                let before = constraints::key_bytes(table, key, old, old_row)?;
                let after = constraints::key_bytes(table, key, new, new_row)?;
                if before == after {
                    continue;
                }
                if let Some(before) = before
                    && seen.remove(&before) != Some(id)
                {
                    return Err(internal("UPDATE index source identity differs"));
                }
                if let Some(after) = after
                    && seen.insert(after, id).is_some()
                {
                    return Err(semantic("UNIQUE/PRIMARY KEY violation"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn unique_keys<'a>(
        &'a self,
        table_name: &'a str,
        table: &'a Table,
    ) -> impl Iterator<Item = &'a Vec<usize>> + 'a {
        table.unique.iter().chain(
            self.indexes
                .values()
                .filter(move |index| index.unique && index.table == table_name)
                .map(|index| &index.key),
        )
    }

    pub(super) fn create_index(&mut self, create: s::CreateIndex) -> Result<Output> {
        let s::Statement::CreateIndex(allowed) = parse("CREATE INDEX i ON t(x)")? else {
            unreachable!()
        };
        let object = create
            .name
            .as_ref()
            .ok_or_else(|| unsupported("CREATE INDEX requires a name"))?;
        let index_name = name(object)?;
        let table_name = name(&create.table_name)?;
        if create.columns.is_empty() || create.columns.len() > self.limits.max_columns {
            return Err(resource("index column count outside session limit"));
        }
        // Build a normalized AST containing only supported fields. A direction
        // annotation has no correctness effect for a nonphysical access path.
        let template = allowed.columns[0].clone();
        let name_bytes = index_name
            .len()
            .checked_add(table_name.len())
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| resource("index metadata size overflow"))?;
        let bytes = create
            .columns
            .iter()
            .try_fold(name_bytes, |bytes, column| {
                let s::Expr::Identifier(identifier) = &column.column.expr else {
                    return Err(unsupported("index expressions/qualified columns"));
                };
                identifier
                    .value
                    .len()
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(256))
                    .and_then(|n| bytes.checked_add(n))
                    .ok_or_else(|| resource("index metadata size overflow"))
            })?;
        // Retained definition and normalization/header allocations are prepaid.
        let reservation = self.budget.reserve(bytes).map_err(resource)?;
        // These owned clones must drop before their construction credit on
        // every validation-error path, as well as after successful publication.
        let (index_name, table_name) = (index_name, table_name);
        let mut allowed = allowed;
        allowed.name = create.name.clone();
        allowed.table_name = create.table_name.clone();
        allowed.if_not_exists = create.if_not_exists;
        allowed.unique = create.unique;
        allowed.columns = create
            .columns
            .iter()
            .map(|column| {
                let mut normalized = template.clone();
                normalized.column.expr = column.column.expr.clone();
                normalized.column.options.asc = column.column.options.asc;
                normalized
            })
            .collect();
        if allowed != create {
            return Err(unsupported("CREATE INDEX modifiers"));
        }
        let columns = create
            .columns
            .iter()
            .map(|column| {
                let s::Expr::Identifier(identifier) = &column.column.expr else {
                    unreachable!()
                };
                Ok((ident(identifier)?, column.column.options.asc))
            })
            .collect::<Result<Vec<_>>>()?;

        if self.tables.contains_key(&index_name) || self.views.contains_key(&index_name) {
            return Err(semantic(format!(
                "catalog name already exists: {index_name}"
            )));
        }
        // SQLite IF NOT EXISTS is a no-op for an existing index, even if the
        // unused replacement definition names an absent table or column.
        if self.indexes.contains_key(&index_name) {
            if create.if_not_exists {
                return Ok(Output::Statement { affected_rows: 0 });
            }
            return Err(semantic(format!("index already exists: {index_name}")));
        }
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| semantic(format!("unknown table: {table_name}")))?;
        let key = columns
            .iter()
            .map(|(column, _)| {
                table
                    .columns
                    .iter()
                    .position(|known| &known.name == column)
                    .ok_or_else(|| semantic(format!("unknown column: {column}")))
            })
            .collect::<Result<Vec<_>>>()?;
        if self.indexes.len() >= self.limits.max_tables {
            return Err(resource("index count limit exceeded"));
        }
        self.check_catalog(bytes)?;
        if create.unique {
            self.validate_unique_key(table, &key, &table.batches)?;
        }
        self.mutation_checkpoint(MutationPoint::Publish)?;
        self.indexes.insert(
            index_name,
            Index {
                table: table_name,
                bytes,
                _columns: columns,
                unique: create.unique,
                key,
                _metadata: reservation,
            },
        );
        self.bytes += bytes;
        Ok(Output::Statement { affected_rows: 0 })
    }

    pub(super) fn drop_index(&mut self, object: &s::ObjectName, if_exists: bool) -> Result<Output> {
        let index_name = name(object)?;
        if !self.indexes.contains_key(&index_name) {
            if if_exists {
                return Ok(Output::Statement { affected_rows: 0 });
            }
            return Err(semantic(format!("unknown index: {index_name}")));
        }
        self.mutation_checkpoint(MutationPoint::Publish)?;
        let index = self.indexes.remove(&index_name).expect("validated index");
        self.bytes -= index.bytes;
        Ok(Output::Statement { affected_rows: 0 })
    }

    // Called only after DROP TABLE's existing publication checkpoint. No new
    // fallible operation may follow the table's irreversible file removal.
    pub(super) fn remove_table_indexes(&mut self, table_name: &str) {
        let mut removed = 0usize;
        self.indexes.retain(|_, index| {
            if index.table == table_name {
                removed += index.bytes;
                false
            } else {
                true
            }
        });
        self.bytes -= removed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn index_publication_cancellation_and_resource_denial_are_atomic() {
        let mut session =
            Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
        session.execute_sql("CREATE TABLE t(x INT)").unwrap();
        let bytes = session.catalog_bytes();
        let used = session.budget.used();
        let occupied = session
            .budget
            .reserve(session.budget.limit() - used)
            .unwrap();
        assert_eq!(
            session
                .execute_sql("CREATE INDEX i ON t(x)")
                .err()
                .unwrap()
                .kind,
            "resource"
        );
        drop(occupied);
        assert!(session.indexes.is_empty());
        assert_eq!(session.budget.used(), used);
        session.cancel_at = Some(MutationPoint::Publish);
        assert_eq!(
            session
                .execute_sql("CREATE INDEX i ON t(x)")
                .err()
                .unwrap()
                .kind,
            "resource"
        );
        assert!(session.indexes.is_empty());
        assert_eq!(session.catalog_bytes(), bytes);
        assert_eq!(session.budget.used(), used);
        session.cancel_at = None;
        session.options.cancellation = crate::spill::Cancellation::default();
        session
            .execute_sql("CREATE INDEX i ON t(x DESC,x ASC)")
            .unwrap();
        assert_eq!(
            session.indexes["i"]._columns,
            vec![("x".into(), Some(false)), ("x".into(), Some(true))]
        );
        let indexed_bytes = session.catalog_bytes();
        let indexed_used = session.budget.used();
        session.cancel_at = Some(MutationPoint::Publish);
        assert_eq!(
            session.execute_sql("DROP INDEX i").err().unwrap().kind,
            "resource"
        );
        assert!(session.indexes.contains_key("i"));
        assert_eq!(session.catalog_bytes(), indexed_bytes);
        assert_eq!(session.budget.used(), indexed_used);
        session.cancel_at = None;
        session.options.cancellation = crate::spill::Cancellation::default();
        session.execute_sql("DROP INDEX i").unwrap();
        assert_eq!(session.catalog_bytes(), bytes);
        assert_eq!(session.budget.used(), used);
    }
}
