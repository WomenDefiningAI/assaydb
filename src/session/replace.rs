//! Statement-atomic SQLite-compatible replacement for PRIMARY/UNIQUE conflicts.
use super::*;

impl Session {
    pub(super) fn replace_insert(
        &mut self,
        table_name: &str,
        incoming: RecordBatch,
        incoming_credit: Reservation,
    ) -> Result<Output> {
        let table = self.tables.get(table_name).expect("validated table");
        let affected = incoming.num_rows();
        // NOT NULL remains ABORT: this catalog does not support defaults.
        for (column, array) in table.columns.iter().zip(incoming.columns()) {
            if !column.nullable && array.null_count() != 0 {
                return Err(semantic(format!("NOT NULL violation: {}", column.name)));
            }
        }
        let rows = table
            .rows
            .checked_add(affected)
            .ok_or_else(|| resource("REPLACE row count overflow"))?;
        let batch_count = table
            .batches
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("REPLACE batch count overflow"))?;
        // Each index can retain one owned key per source row, including stale
        // entries for deleted rows. Charge every index's full upper bound before
        // allocating maps/keys. Stale IDs are always checked against live[].
        let mut scratch = rows
            .checked_mul(32)
            .and_then(|n| n.checked_add(batch_count.checked_mul(256 + table.columns.len() * 32)?))
            .and_then(|n| {
                n.checked_add(
                    self.unique_keys(table_name, table)
                        .count()
                        .checked_mul(256)?,
                )
            })
            .ok_or_else(|| resource("REPLACE metadata size overflow"))?;
        for key in self.unique_keys(table_name, table) {
            let strings = table
                .batches
                .iter()
                .chain(std::iter::once(&incoming))
                .flat_map(|batch| key.iter().map(move |&i| batch.column(i)))
                .try_fold(0usize, |n, array| {
                    n.checked_add(
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map_or(0, |a| a.value_data().len()),
                    )
                    .ok_or_else(|| resource("REPLACE key size overflow"))
                })?;
            scratch = scratch
                .checked_add(
                    rows.checked_mul(128 + key.len() * 24)
                        .and_then(|n| n.checked_add(strings.checked_mul(2)?))
                        .ok_or_else(|| resource("REPLACE key size overflow"))?,
                )
                .ok_or_else(|| resource("REPLACE scratch size overflow"))?;
        }
        let _scratch = self.budget.reserve(scratch).map_err(resource)?;
        let mut live = vec![true; rows];
        let mut indexes = self
            .unique_keys(table_name, table)
            .map(|_| HashMap::<Vec<u8>, usize>::with_capacity(rows))
            .collect::<Vec<_>>();
        let mut id = 0;
        for batch in table.batches.iter().chain(std::iter::once(&incoming)) {
            for row in 0..batch.num_rows() {
                self.options.cancellation.check().map_err(resource)?;
                for (key, index) in self.unique_keys(table_name, table).zip(&mut indexes) {
                    if let Some(bytes) = constraints::key_bytes(table, key, batch, row)?
                        && let Some(old) = index.insert(bytes, id)
                        && id >= table.rows
                        && live[old]
                    {
                        // Existing catalog rows are already valid. For new
                        // rows, delete every live conflict across all keys,
                        // including earlier rows in this same INSERT.
                        live[old] = false;
                    }
                }
                id += 1;
            }
        }
        let final_rows = live.iter().filter(|&&yes| yes).count();
        if final_rows > self.limits.max_rows {
            return Err(resource("table row limit exceeded"));
        }
        // Full replacement is staged while old catalog and incoming buffers
        // remain charged. Reserve the allowed retained output before gathering.
        let allowance = self
            .limits
            .max_catalog_bytes
            .saturating_sub(self.bytes - (table.bytes - table.metadata_bytes));
        let mut retained = self.budget.reserve(allowance).map_err(resource)?;
        let mut batches = Vec::with_capacity(batch_count);
        let mut offset = 0;
        let mut retained_bytes = 0usize;
        for batch in table.batches.iter().chain(std::iter::once(&incoming)) {
            self.options.cancellation.check().map_err(resource)?;
            let end = offset + batch.num_rows();
            let selected = &live[offset..end];
            offset = end;
            if !selected.iter().any(|&yes| yes) {
                continue;
            }
            let count = selected.iter().filter(|&&yes| yes).count();
            let all = count == batch.num_rows();
            let upper = if all {
                batch.get_array_memory_size()
            } else {
                batch.columns().iter().try_fold(0usize, |bytes, array| {
                    let mut added = count
                        .checked_mul(17)
                        .and_then(|n| n.checked_add(256))
                        .ok_or_else(|| resource("REPLACE gather size overflow"))?;
                    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
                        for (row, &yes) in selected.iter().enumerate() {
                            if yes && strings.is_valid(row) {
                                added = added
                                    .checked_add(strings.value(row).len())
                                    .ok_or_else(|| resource("REPLACE gather size overflow"))?;
                            }
                        }
                    }
                    bytes
                        .checked_add(added)
                        .ok_or_else(|| resource("REPLACE gather size overflow"))
                })?
            }
            .checked_add(256 + table.columns.len() * 128)
            .ok_or_else(|| resource("REPLACE output size overflow"))?;
            if upper > allowance.saturating_sub(retained_bytes) {
                return Err(resource("REPLACE exceeds catalog memory limit"));
            }
            let output = if all {
                batch.clone()
            } else {
                let mask = BooleanArray::from(selected.to_vec());
                arrow::compute::filter_record_batch(batch, &mask).map_err(internal)?
            };
            retained_bytes = retained_bytes
                .checked_add(output.get_array_memory_size() + 256 + table.columns.len() * 128)
                .ok_or_else(|| resource("REPLACE output size overflow"))?;
            if retained_bytes > allowance {
                return Err(resource("REPLACE exceeds catalog memory limit"));
            }
            batches.push(output);
        }
        // The construction descriptor allowance covers the original capacity;
        // retain only exact live-batch descriptors in the published catalog.
        batches.shrink_to_fit();
        retained.release_excess(retained_bytes);
        drop(indexes);
        drop(live);
        drop(_scratch);
        drop(incoming);
        drop(incoming_credit);
        self.replace_batches(table_name, batches, retained, retained_bytes, affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replace_cancellation_and_resource_failure_preserve_snapshot_and_leases() {
        for point in [MutationPoint::WriterBatch, MutationPoint::Publish] {
            for sql in [
                "REPLACE INTO t VALUES(1,'b'),(3,'c')",
                "INSERT OR REPLACE INTO t SELECT id+1 AS next_id,label FROM t ORDER BY next_id",
            ] {
                for explicit_indexes in [false, true] {
                    let mut s =
                        Session::new(&std::env::temp_dir(), Options::default(), Limits::default())
                            .unwrap();
                    if explicit_indexes {
                        s.execute_sql("CREATE TABLE t(id INT,label TEXT)").unwrap();
                        s.execute_sql("CREATE UNIQUE INDEX uid ON t(id)").unwrap();
                        s.execute_sql("CREATE UNIQUE INDEX ulabel ON t(label)")
                            .unwrap();
                    } else {
                        s.execute_sql("CREATE TABLE t(id INT PRIMARY KEY,label TEXT UNIQUE)")
                            .unwrap();
                    }
                    s.execute_sql("INSERT INTO t VALUES(1,'a'),(2,'b')")
                        .unwrap();
                    let path = s.tables["t"].path.clone();
                    let disk = fs::read(&path).unwrap();
                    let used = s.budget.used();
                    let bytes = s.bytes;
                    let index_count = s.indexes.len();
                    s.cancel_at = Some(point);
                    let error = s.execute_sql(sql).err().unwrap();
                    assert_eq!(error.kind, "resource", "{sql}: {error}");
                    assert_eq!(s.tables["t"].path, path);
                    assert_eq!(fs::read(&path).unwrap(), disk);
                    assert_eq!(s.bytes, bytes);
                    assert_eq!(s.indexes.len(), index_count);
                    assert_eq!(s.budget.used(), used);
                    assert_eq!(fs::read_dir(s.directory()).unwrap().count(), 1);
                    s.cancel_at = None;
                    s.options.cancellation = crate::spill::Cancellation::default();
                    let blocker = s
                        .budget
                        .reserve(s.budget.limit() - s.budget.used())
                        .unwrap();
                    let error = s.execute_sql(sql).err().unwrap();
                    assert_eq!(error.kind, "resource", "{sql}: {error}");
                    drop(blocker);
                    assert_eq!(s.budget.used(), used);
                    assert_eq!(fs::read(&path).unwrap(), disk);
                    assert_eq!(s.indexes.len(), index_count);
                    assert!(s.execute_sql(sql).is_ok());
                    assert_eq!(s.indexes.len(), index_count);
                }
            }
        }
    }
}
