//! Atomic single-table UPDATE, evaluated using the ordinary typed expressions.
use super::*;
use crate::{
    bind::Binder,
    expr::{Expr, Input, Kind, Literal, boolean, normalize_input},
};

pub(super) fn expression_error(message: crate::Error) -> Error {
    let cause = message.cause();
    let mut error = if message.starts_with("unsupported") || message.contains("unsupported") {
        unsupported(message)
    } else if message.contains("budget")
        || message.contains("resource-limit")
        || message.contains("cancel")
    {
        resource(message)
    } else {
        semantic(message)
    };
    error.cause = cause;
    error
}

/// UPDATE and DELETE share the same typed boolean/NULL WHERE contract.
pub(super) fn bound_predicate(
    binder: &Binder<'_>,
    selection: Option<&s::Expr>,
    command: &str,
) -> Result<Expr> {
    let mut predicate = selection
        .map(|e| binder.expr(e))
        .transpose()
        .map_err(expression_error)?
        .unwrap_or(Expr {
            ty: Type::Bool,
            kind: Kind::Literal(Literal::Bool(true)),
        });
    if predicate.ty == Type::Null {
        predicate.ty = Type::Bool;
    }
    if predicate.ty != Type::Bool {
        return Err(semantic(format!("{command} WHERE requires BOOLEAN")));
    }
    Ok(predicate)
}
/// Descriptor credit outlives the normalized input and all its value owners.
pub(super) struct MutationInput {
    pub(super) input: Input,
    _envelope: Reservation,
}
pub(super) fn mutation_input(
    table: &Table,
    batch: &RecordBatch,
    budget: &Budget,
) -> Result<MutationInput> {
    let bytes = table
        .columns
        .len()
        .checked_mul(std::mem::size_of::<Option<crate::expr::Value>>() + 64)
        .and_then(|n| n.checked_add(256))
        .ok_or_else(|| resource("mutation input descriptors overflow"))?;
    let envelope = budget.reserve(bytes).map_err(resource)?;
    let input = Input {
        len: batch.num_rows(),
        columns: batch
            .columns()
            .iter()
            .zip(&table.columns)
            .map(|(array, column)| normalize_input(array, &column.ty, budget).map(Some))
            .collect::<crate::Result<Vec<_>>>()
            .map_err(expression_error)?,
    };
    Ok(MutationInput {
        input,
        _envelope: envelope,
    })
}

impl Session {
    pub(super) fn update(&mut self, statement: s::Statement) -> Result<Output> {
        let s::Statement::Update {
            table,
            assignments,
            selection,
            ..
        } = &statement
        else {
            unreachable!()
        };
        let mut allowed = parse("UPDATE t SET x=0")?;
        let s::Statement::Update {
            table: allowed_table,
            assignments: allowed_assignments,
            selection: allowed_selection,
            ..
        } = &mut allowed
        else {
            unreachable!()
        };
        let s::TableFactor::Table { name: object, .. } = &table.relation else {
            return Err(unsupported("UPDATE target must be a table"));
        };
        // Copy only the supported AST fields. Aliases, joins, FROM, conflict
        // modes, RETURNING, hints, and future modifiers remain explicit errors.
        let s::TableFactor::Table {
            name: allowed_name, ..
        } = &mut allowed_table.relation
        else {
            unreachable!()
        };
        *allowed_name = object.clone();
        *allowed_assignments = assignments.clone();
        *allowed_selection = selection.clone();
        if allowed != statement {
            return Err(unsupported("UPDATE modifiers/target"));
        }
        let table_name = name(object)?;
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| semantic(format!("unknown table: {table_name}")))?;
        let _statement = crate::zone::enter(self.options.time_zone, self.options.fixed_clock);
        let binder = Binder {
            schema: &table.schema,
            table: Some(&table_name),
            facts: None,
        };
        let predicate = bound_predicate(&binder, selection.as_ref(), "UPDATE")?;
        let mut bound = Vec::new();
        let mut seen = HashSet::new();
        // SQLite-compatible: earlier duplicate assignments are ignored entirely.
        // Bind only the rightmost RHS, then restore surviving source order.
        for assignment in assignments.iter().rev() {
            let s::AssignmentTarget::ColumnName(object) = &assignment.target else {
                return Err(unsupported("UPDATE tuple assignment"));
            };
            let column_name = name(object)?;
            let index = table
                .columns
                .iter()
                .position(|c| c.name == column_name)
                .ok_or_else(|| semantic(format!("unknown column: {column_name}")))?;
            if !seen.insert(index) {
                continue;
            }
            let rhs = binder.expr(&assignment.value).map_err(expression_error)?;
            let target = &table.columns[index].ty;
            validate_assignment_type(&rhs.ty, target)?;
            // CASE evaluates RHS only where WHERE is true, preserving errors
            // and NULL behavior of SELECT expressions on the selected rows.
            let ty = rhs.ty.clone();
            let masked = Expr {
                ty: ty.clone(),
                kind: Kind::Case(
                    vec![(predicate.clone(), rhs)],
                    Box::new(Expr {
                        ty,
                        kind: Kind::Literal(Literal::Null),
                    }),
                ),
            };
            bound.push((index, masked));
        }
        bound.reverse();
        if bound.is_empty() {
            return Err(unsupported("UPDATE requires assignments"));
        }
        crate::mixed_numeric::validate_expressions(
            &table.schema,
            std::iter::once(&predicate).chain(bound.iter().map(|(_, value)| value)),
        )
        .map_err(expression_error)?;
        let allowance = self
            .limits
            .max_catalog_bytes
            .saturating_sub(self.bytes - (table.bytes - table.metadata_bytes));
        let mut retained = self.budget.reserve(allowance).map_err(resource)?;
        let mut batches = Vec::with_capacity(table.batches.len());
        let mut retained_bytes = 0usize;
        let mut affected = 0usize;
        for original in &table.batches {
            for start in (0..original.num_rows()).step_by(8192) {
                self.options.cancellation.check().map_err(resource)?;
                let batch = original.slice(start, (original.num_rows() - start).min(8192));
                let input = mutation_input(table, &batch, &self.budget)?;
                let mask = predicate
                    .eval(&input.input, &self.budget)
                    .map_err(expression_error)?;
                let mask = boolean(&mask.array);
                let values = bound
                    .iter()
                    .map(|(_, e)| e.eval(&input.input, &self.budget))
                    .collect::<crate::Result<Vec<_>>>()
                    .map_err(expression_error)?;
                let source_bytes = batch.get_array_memory_size().saturating_add(
                    values
                        .iter()
                        .map(|v| v.array.get_array_memory_size())
                        .sum::<usize>(),
                );
                let cell_bytes = batch
                    .num_rows()
                    .saturating_mul(table.columns.len())
                    .saturating_mul(std::mem::size_of::<Cell>() + 64);
                let scratch_bytes = source_bytes
                    .saturating_mul(3)
                    .saturating_add(cell_bytes)
                    .saturating_add(batch.num_rows() * std::mem::size_of::<Vec<Cell>>())
                    .saturating_add(table.columns.len() * 512);
                let _scratch = self.budget.reserve(scratch_bytes).map_err(resource)?;
                let mut rows = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    self.options.cancellation.check().map_err(resource)?;
                    let selected = mask.is_valid(row) && mask.value(row);
                    let mut cells = batch
                        .columns()
                        .iter()
                        .zip(&table.columns)
                        .map(|(a, c)| array_cell(a.as_ref(), &c.ty, row, c))
                        .collect::<Result<Vec<_>>>()?;
                    if selected {
                        affected += 1;
                        for ((index, expression), value) in bound.iter().zip(&values) {
                            cells[*index] = array_cell(
                                value.array.as_ref(),
                                &expression.ty,
                                row,
                                &table.columns[*index],
                            )?;
                        }
                    }
                    rows.push(cells);
                }
                let output = make_batch(table.schema.clone(), &table.columns, &rows)?;
                let added = output.get_array_memory_size() + 256 + table.columns.len() * 128;
                retained_bytes = retained_bytes
                    .checked_add(added)
                    .ok_or_else(|| resource("UPDATE output size overflow"))?;
                if retained_bytes > allowance {
                    return Err(resource("UPDATE exceeds catalog memory limit"));
                }
                batches.push(output);
            }
        }
        retained.release_excess(retained_bytes);
        self.validate_index_update(&table_name, table, &batches)?;
        self.replace_batches(&table_name, batches, retained, retained_bytes, affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_cancellation_at_write_and_publication_preserves_snapshot_and_leases() {
        for point in [MutationPoint::WriterBatch, MutationPoint::Publish] {
            let mut session =
                Session::new(&std::env::temp_dir(), Options::default(), Limits::default()).unwrap();
            session.execute_sql("CREATE TABLE t(x INT)").unwrap();
            session.execute_sql("INSERT INTO t VALUES(1),(2)").unwrap();
            let old_path = session.tables["t"].path.clone();
            let old_bytes = fs::read(&old_path).unwrap();
            let catalog_bytes = session.catalog_bytes();
            let budget_used = session.budget.used();
            session.cancel_at = Some(point);
            assert_eq!(
                session
                    .execute_sql("UPDATE t SET x=x+1")
                    .err()
                    .unwrap()
                    .kind,
                "resource"
            );
            assert_eq!(session.tables["t"].path, old_path);
            assert_eq!(fs::read(&old_path).unwrap(), old_bytes);
            assert_eq!(session.catalog_bytes(), catalog_bytes);
            assert_eq!(session.budget.used(), budget_used);
            assert_eq!(fs::read_dir(session.directory()).unwrap().count(), 1);
            session.cancel_at = None;
            session.options.cancellation = crate::spill::Cancellation::default();
            assert!(session.execute_sql("UPDATE t SET x=x+1").is_ok());
        }
    }
}
