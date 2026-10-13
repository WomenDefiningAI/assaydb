use super::*;

pub(super) fn declarations(
    create: &s::CreateTable,
    columns: &mut [Column],
) -> Result<Vec<Vec<usize>>> {
    let mut unique = Vec::new();
    let mut primary = false;
    for (i, def) in create.columns.iter().enumerate() {
        let mut seen_null = false;
        let mut seen_not_null = false;
        for option in &def.options {
            match &option.option {
                s::ColumnOption::Null => seen_null = true,
                s::ColumnOption::NotNull => seen_not_null = true,
                s::ColumnOption::Unique {
                    is_primary,
                    characteristics: None,
                } => {
                    if *is_primary {
                        if primary {
                            return Err(semantic("multiple primary keys"));
                        }
                        primary = true;
                        columns[i].nullable = false;
                    }
                    unique.push(vec![i]);
                }
                _ => (), // column() already rejected unsupported options.
            }
        }
        if seen_null && (seen_not_null || !columns[i].nullable) {
            return Err(semantic("conflicting NULL constraints"));
        }
    }
    for constraint in &create.constraints {
        let (key_columns, is_primary) = match constraint {
            s::TableConstraint::Unique { columns, .. } => (columns, false),
            s::TableConstraint::PrimaryKey { columns, .. } => (columns, true),
            _ => return Err(unsupported("table constraint")),
        };
        let template = if is_primary {
            "CREATE TABLE t(x INT, PRIMARY KEY(x))"
        } else {
            "CREATE TABLE t(x INT, UNIQUE(x))"
        };
        let s::Statement::CreateTable(mut allowed) = parse(template)? else {
            unreachable!()
        };
        let expected = &mut allowed.constraints[0];
        match expected {
            s::TableConstraint::Unique {
                columns, name: n, ..
            } => {
                *columns = key_columns.clone();
                if let s::TableConstraint::Unique { name, .. } = constraint {
                    *n = name.clone();
                }
            }
            s::TableConstraint::PrimaryKey {
                columns, name: n, ..
            } => {
                *columns = key_columns.clone();
                if let s::TableConstraint::PrimaryKey { name, .. } = constraint {
                    *n = name.clone();
                }
            }
            _ => unreachable!(),
        }
        if expected != constraint {
            return Err(unsupported("key constraint modifiers"));
        }
        if is_primary {
            if primary {
                return Err(semantic("multiple primary keys"));
            }
            primary = true;
        }
        let mut key = Vec::new();
        for entry in key_columns {
            let s::Expr::Identifier(id) = &entry.column.expr else {
                return Err(unsupported("key expression"));
            };
            if (entry.column.options.asc.is_some() || entry.column.options.nulls_first.is_some())
                || entry.column.with_fill.is_some()
                || entry.operator_class.is_some()
            {
                return Err(unsupported("key ordering/operator class"));
            }
            let n = ident(id)?;
            let index = columns
                .iter()
                .position(|c| c.name == n)
                .ok_or_else(|| semantic(format!("unknown key column: {n}")))?;
            if key.contains(&index) {
                return Err(semantic("duplicate key column"));
            }
            key.push(index);
            if is_primary {
                columns[index].nullable = false;
            }
        }
        if key.is_empty() {
            return Err(semantic("empty key constraint"));
        }
        unique.push(key);
    }
    Ok(unique)
}

impl Session {
    pub(super) fn validate_constraints(
        &self,
        table_name: &str,
        table: &Table,
        batches: &[RecordBatch],
    ) -> Result<()> {
        for batch in batches {
            self.options.cancellation.check().map_err(resource)?;
            for (column, array) in table.columns.iter().zip(batch.columns()) {
                if !column.nullable && array.null_count() != 0 {
                    return Err(semantic(format!("NOT NULL violation: {}", column.name)));
                }
            }
        }
        for key in self.unique_keys(table_name, table) {
            self.validate_unique_key(table, key, batches)?;
        }
        Ok(())
    }

    pub(super) fn validate_unique_key(
        &self,
        table: &Table,
        key: &[usize],
        batches: &[RecordBatch],
    ) -> Result<()> {
        // Reserve owned hash keys and bucket overhead before copying values.
        // All scalar widths are bounded by 16; text adds its actual backing bytes.
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let strings: usize = batches
            .iter()
            .flat_map(|b| key.iter().map(move |&i| b.column(i)))
            .map(|a| {
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .map_or(0, |a| a.value_data().len())
            })
            .sum();
        let allowance = rows
            .checked_mul(128 + key.len() * 24)
            .and_then(|x| x.checked_add(strings.saturating_mul(2)))
            .ok_or_else(|| resource("constraint memory overflow"))?;
        let _credit = self.budget.reserve(allowance).map_err(resource)?;
        let mut seen = HashSet::<Vec<u8>>::with_capacity(rows);
        for batch in batches {
            for row in 0..batch.num_rows() {
                self.options.cancellation.check().map_err(resource)?;
                let Some(bytes) = key_bytes(table, key, batch, row)? else {
                    continue;
                };
                if !seen.insert(bytes) {
                    return Err(semantic("UNIQUE/PRIMARY KEY violation"));
                }
            }
        }
        Ok(())
    }
}

/// Shared exact key encoding for validation and REPLACE conflict lookup.
/// A tuple containing SQL NULL never conflicts under UNIQUE.
pub(super) fn key_bytes(
    table: &Table,
    key: &[usize],
    batch: &RecordBatch,
    row: usize,
) -> Result<Option<Vec<u8>>> {
    if key.iter().any(|&i| batch.column(i).is_null(row)) {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(key.len() * 24);
    for &i in key {
        let a = batch.column(i);
        macro_rules! scalar {
            ($ty:ty) => {
                bytes.extend_from_slice(
                    &a.as_any()
                        .downcast_ref::<$ty>()
                        .ok_or_else(|| internal("invalid catalog array"))?
                        .value(row)
                        .to_le_bytes(),
                )
            };
        }
        match table.columns[i].ty {
            Type::Int => scalar!(Int64Array),
            Type::Decimal(_) => scalar!(Decimal128Array),
            Type::Date => scalar!(Date32Array),
            Type::Bool => bytes.push(u8::from(
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            )),
            Type::Float => {
                let x = a
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row);
                bytes.extend_from_slice(&(if x == 0.0 { 0.0 } else { x }).to_le_bytes());
            }
            Type::Text => {
                let x = a.as_any().downcast_ref::<StringArray>().unwrap().value(row);
                bytes.extend_from_slice(&(x.len() as u64).to_le_bytes());
                bytes.extend_from_slice(x.as_bytes());
            }
            Type::Null => return Err(internal("NULL catalog column")),
            Type::Binary => return Err(unsupported("binary catalog columns")),
        }
    }
    Ok(Some(bytes))
}
