//! Typed single-table DELETE with one atomic snapshot publication.
use super::update::{bound_predicate, expression_error, mutation_input};
use super::*;
use crate::{bind::Binder, expr::boolean};

impl Session {
    pub(super) fn delete(&mut self, statement: s::Statement) -> Result<Output> {
        let s::Statement::Delete(delete) = &statement else {
            unreachable!()
        };
        let s::FromTable::WithFromKeyword(from) = &delete.from else {
            return Err(unsupported("DELETE requires FROM table"));
        };
        let [target] = from.as_slice() else {
            return Err(unsupported("DELETE requires one table"));
        };
        let s::TableFactor::Table { name: object, .. } = &target.relation else {
            return Err(unsupported("DELETE target must be a table"));
        };
        // Only name and WHERE are supported. AST comparison also rejects new
        // parser modifiers unless we explicitly implement their semantics.
        let mut allowed = parse("DELETE FROM t")?;
        let s::Statement::Delete(allowed_delete) = &mut allowed else {
            unreachable!()
        };
        let s::FromTable::WithFromKeyword(allowed_from) = &mut allowed_delete.from else {
            unreachable!()
        };
        let s::TableFactor::Table {
            name: allowed_name, ..
        } = &mut allowed_from[0].relation
        else {
            unreachable!()
        };
        *allowed_name = object.clone();
        allowed_delete.selection = delete.selection.clone();
        if allowed != statement {
            return Err(unsupported("DELETE modifiers/target"));
        }

        let table_name = name(object)?;
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| semantic(format!("unknown table: {table_name}")))?;
        let binder = Binder {
            schema: &table.schema,
            table: Some(&table_name),
            facts: None,
        };
        let predicate = bound_predicate(&binder, delete.selection.as_ref(), "DELETE")?;
        crate::mixed_numeric::validate_expressions(&table.schema, [&predicate])
            .map_err(expression_error)?;
        self.options.cancellation.check().map_err(resource)?;
        // Retain exact row choices across bounded predicate batches. Filtering
        // each original catalog batch only once avoids repeated full-buffer
        // leases when several slices share the same Arrow allocation.
        let mask_bytes = table
            .rows
            .checked_mul(2)
            .and_then(|n| {
                table
                    .batches
                    .len()
                    .checked_mul(
                        1024 + std::mem::size_of::<Vec<bool>>()
                            + std::mem::size_of::<RecordBatch>(),
                    )
                    .and_then(|headers| n.checked_add(headers))
            })
            .ok_or_else(|| resource("DELETE selection size overflow"))?;
        let _mask_credit = self.budget.reserve(mask_bytes).map_err(resource)?;
        let mut keep = Vec::with_capacity(table.batches.len());
        let mut affected = 0usize;
        for original in &table.batches {
            let mut selected = Vec::with_capacity(original.num_rows());
            for start in (0..original.num_rows()).step_by(8192) {
                self.options.cancellation.check().map_err(resource)?;
                let batch = original.slice(start, (original.num_rows() - start).min(8192));
                let input = mutation_input(table, &batch, &self.budget)?;
                let mask = predicate
                    .eval(&input.input, &self.budget)
                    .map_err(expression_error)?;
                let mask = boolean(&mask.array);
                for row in 0..batch.num_rows() {
                    self.options.cancellation.check().map_err(resource)?;
                    let remove = mask.is_valid(row) && mask.value(row);
                    affected = affected
                        .checked_add(usize::from(remove))
                        .ok_or_else(|| resource("DELETE affected row count overflow"))?;
                    selected.push(!remove);
                }
            }
            keep.push(selected);
        }
        // Binding still validates empty targets; evaluation covers existing rows.
        // A successful no-op needs no new file or replacement catalog leases.
        if affected == 0 {
            self.options.cancellation.check().map_err(resource)?;
            return Ok(Output::Statement { affected_rows: 0 });
        }

        let allowance = self
            .limits
            .max_catalog_bytes
            .saturating_sub(self.bytes - (table.bytes - table.metadata_bytes));
        let mut retained = self.budget.reserve(allowance).map_err(resource)?;
        let mut batches = Vec::with_capacity(table.batches.len());
        let mut retained_bytes = 0usize;
        for (original, selected) in table.batches.iter().zip(&keep) {
            self.options.cancellation.check().map_err(resource)?;
            let rows = selected.iter().filter(|&&yes| yes).count();
            if rows == 0 {
                continue;
            }
            // Arrow filtering preserves native cell types/values; DELETE never
            // converts surviving cells through literals or a reference engine.
            let scratch = original
                .get_array_memory_size()
                .checked_mul(3)
                .and_then(|n| {
                    table
                        .columns
                        .len()
                        .checked_mul(512)
                        .and_then(|headers| n.checked_add(headers))
                })
                .ok_or_else(|| resource("DELETE filtering size overflow"))?;
            let _scratch = self.budget.reserve(scratch).map_err(resource)?;
            let output = if rows == original.num_rows() {
                original.clone()
            } else {
                let mask = BooleanArray::from(selected.clone());
                arrow::compute::filter_record_batch(original, &mask).map_err(internal)?
            };
            let added = output
                .get_array_memory_size()
                .checked_add(256)
                .and_then(|n| {
                    table
                        .columns
                        .len()
                        .checked_mul(128)
                        .and_then(|headers| n.checked_add(headers))
                })
                .ok_or_else(|| resource("DELETE output size overflow"))?;
            retained_bytes = retained_bytes
                .checked_add(added)
                .ok_or_else(|| resource("DELETE output size overflow"))?;
            if retained_bytes > allowance {
                return Err(resource("DELETE exceeds catalog memory limit"));
            }
            batches.push(output);
        }
        batches.shrink_to_fit();
        retained.release_excess(retained_bytes);
        drop(keep);
        drop(_mask_credit);
        self.replace_batches(&table_name, batches, retained, retained_bytes, affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delete_cancellation_at_write_and_publication_preserves_snapshot_and_credit() {
        for point in [MutationPoint::WriterBatch, MutationPoint::Publish] {
            let mut session =
                Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
            session.execute_sql("CREATE TABLE t(x INT)").unwrap();
            session
                .execute_sql("INSERT INTO t VALUES(1),(2),(3)")
                .unwrap();
            let path = session.tables["t"].path.clone();
            let snapshot = fs::read(&path).unwrap();
            let catalog_bytes = session.catalog_bytes();
            let used = session.budget.used();
            session.cancel_at = Some(point);
            assert_eq!(
                session
                    .execute_sql("DELETE FROM t WHERE x=2")
                    .err()
                    .unwrap()
                    .kind,
                "resource"
            );
            assert_eq!(session.tables["t"].path, path);
            assert_eq!(fs::read(&path).unwrap(), snapshot);
            assert_eq!(session.catalog_bytes(), catalog_bytes);
            assert_eq!(session.budget.used(), used);
            assert_eq!(fs::read_dir(session.directory()).unwrap().count(), 1);
            session.cancel_at = None;
            session.options.cancellation = crate::spill::Cancellation::default();
            assert!(session.execute_sql("DELETE FROM t WHERE x=2").is_ok());
        }
    }
    #[test]
    fn delete_resource_denial_and_pre_cancel_leave_original_snapshot() {
        let mut session =
            Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
        session.execute_sql("CREATE TABLE t(x INT)").unwrap();
        session.execute_sql("INSERT INTO t VALUES(1),(2)").unwrap();
        let path = session.tables["t"].path.clone();
        let snapshot = fs::read(&path).unwrap();
        let initial = session.budget.used();
        let occupied = session
            .budget
            .reserve(session.budget.limit() - initial)
            .unwrap();
        assert_eq!(
            session.execute_sql("DELETE FROM t").err().unwrap().kind,
            "resource"
        );
        drop(occupied);
        assert_eq!(session.budget.used(), initial);
        session.options.cancellation.cancel();
        assert_eq!(
            session.execute_sql("DELETE FROM t").err().unwrap().kind,
            "resource"
        );
        assert_eq!(session.tables["t"].path, path);
        assert_eq!(fs::read(&path).unwrap(), snapshot);
        assert_eq!(session.budget.used(), initial);
        assert_eq!(fs::read_dir(session.directory()).unwrap().count(), 1);
    }
}
