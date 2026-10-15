//! Private producer headroom. This bounds known Budget reservations,
//! not arbitrary allocator/decoder memory, and never mutates aggregate state.
use super::*;

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| "private aggregate preflight size overflow".into())
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| "private aggregate preflight size overflow".into())
}

impl Aggregation {
    /// Additional private-budget headroom sufficient for this batch's update
    /// followed immediately by raw export. All rows are conservatively new.
    ///
    /// The caller exclusively owns this private Budget, keeps evaluated arrays
    /// and prepared encoded keys query-funded, and must not allocate anything
    /// else from the private domain between this check and update/export. Keys
    /// must be the matching encoding of this batch with exactly `batch.rows`
    /// entries. Funding, row lengths, input presence and normalized Arrow types
    /// are checked. Plan/program/state identity and encoded-key contents must
    /// match; those provenance contracts still belong to the constructing driver.
    /// No post-mutation retry is
    /// supported. A non-fitting empty state/one oversized batch is terminal.
    pub(in crate::execute) fn batch_export_headroom(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        batch: &EvaluatedBatch,
        query: &Budget,
    ) -> Result<usize> {
        if batch.rows == 0
            || p.groups.is_empty()
            || self.allow_dense
            || self.numeric_map.dense_bytes() != 0
            || self.aggregate_workers.is_some()
            || !self.encounters_valid
            || self.encounters.len() != self.group_count
            || batch.keys.len() != p.groups.len()
            || batch.values.len() != p.aggregates.len()
            || p.aggregates
                .iter()
                .zip(&batch.values)
                .any(|(a, v)| a.input.is_some() != v.is_some())
            || self.states.len() != p.aggregates.len()
            || expressions.state_sources.len() != self.states.len()
            || self
                .states
                .iter()
                .any(|s| s.extremes.is_some() || s.distinct.is_some())
            || batch.keys.iter().any(|v| v.array.len() != batch.rows)
            || batch
                .values
                .iter()
                .flatten()
                .any(|v| v.array.len() != batch.rows)
            || p.aggregates.iter().any(|a| {
                a.distinct
                    || !matches!(
                        a.function,
                        Aggregate::Count | Aggregate::Sum | Aggregate::Avg
                    )
                    || (!matches!(a.function, Aggregate::Count)
                        && !a.input.as_ref().is_some_and(|e| {
                            matches!(e.ty, Type::Int | Type::Decimal(_) | Type::Null)
                        }))
            })
        {
            return Err("private aggregate preflight requires a supported nonempty tokenized batch without dense/helper state".into());
        }
        if self.budget.same_domain(query) {
            return Err("private aggregate preflight requires a separate query budget".into());
        }
        let normalized = |value: &Value, ty: &Type| {
            let expected = if *ty == Type::Int {
                arrow::datatypes::DataType::Decimal128(38, 0)
            } else {
                ty.arrow()
            };
            value.array.data_type() == &expected && value.funded_by(query)
        };
        if p.groups
            .iter()
            .zip(&batch.keys)
            .any(|(e, v)| !normalized(v, &e.ty))
            || p.aggregates.iter().zip(&batch.values).any(|(a, v)| {
                a.input
                    .as_ref()
                    .zip(v.as_ref())
                    .is_some_and(|(e, v)| !normalized(v, &e.ty))
            })
            || batch
                .prepared_keys
                .as_ref()
                .is_some_and(|keys| keys.len() != batch.rows || !keys.funded_by(query))
        {
            return Err("private aggregate preflight requires matching normalized query-funded input and keys".into());
        }
        let numeric = expressions.enabled
            && batch.keys.len() == 1
            && batch.keys[0].array.as_any().is::<Decimal128Array>();
        if !numeric && batch.prepared_keys.is_none() {
            return Err("private aggregate preflight requires query-funded prepared keys".into());
        }
        let state_bytes = self
            .states
            .iter()
            .enumerate()
            .try_fold(0, |bytes, (i, s)| {
                if expressions.state_sources[i] == i {
                    add(bytes, s.bytes_per_group())
                } else {
                    Ok(bytes)
                }
            })?;
        // Prepay replacement capacities for an all-new numeric batch, including
        // old/new overlap at a growth boundary. Encoded accounting is unchanged.
        let growth = if numeric {
            self.numeric_merge_headroom(expressions, batch.rows)?
        } else {
            let base = add(
                512,
                mul(
                    4,
                    add(
                        mul(p.groups.len(), std::mem::size_of::<Key>())?,
                        state_bytes,
                    )?,
                )?,
            )?;
            let encoded = batch.prepared_keys.as_ref().unwrap();
            (0..batch.rows).try_fold(0, |bytes, row| {
                add(bytes, add(base, mul(encoded.row(row).len(), 6)?)?)
            })?
        };
        let groups = add(self.group_count, batch.rows)?;
        // update_with_encounters reserves this while all old token storage is
        // still funded. Its retained capacity is no larger than this amount.
        let encounters = add(
            mul(groups, mul(2, std::mem::size_of::<EncounterToken>())?)?,
            256,
        )?;
        // Reserve a group-ID vector even if the fused single-SUM loop skips it.
        let group_ids = mul(batch.rows, std::mem::size_of::<usize>())?;
        // into_raw_inner moves encoded key boxes; it allocates descriptor slots,
        // not another copy of their bytes. Count numeric groups too, matching
        // its conservative descriptor reservation. 512 covers fixed headers.
        let export = add(
            mul(
                groups,
                add(
                    std::mem::size_of::<EncounterToken>(),
                    std::mem::size_of::<Option<Box<[u8]>>>(),
                )?,
            )?,
            512,
        )?;
        // Refill reservations can leave at most one 64 KiB chunk's slack after
        // all charged groups. Sum transient update AND export (not merely max).
        add(
            add(add(add(growth, 64 * 1024)?, encounters)?, group_ids)?,
            export,
        )
    }

    pub(in crate::execute) fn batch_export_fits(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        batch: &EvaluatedBatch,
        query: &Budget,
    ) -> Result<bool> {
        let needed = self.batch_export_headroom(p, expressions, batch, query)?;
        Ok(needed <= self.budget.limit().saturating_sub(self.budget.used()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Kind, expr_program::ExpressionProgram};
    use arrow::datatypes::{DataType, Field, Schema};

    fn fixture(sql: &str) -> (Plan, ExpressionProgram) {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Decimal128(38, 0), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Decimal128(38, 0), true),
        ]);
        let p = bind::bind(&bind::parse(sql).unwrap(), &schema, None, true).unwrap();
        let e = ExpressionProgram::new(&p, 3, true);
        (p, e)
    }
    fn batch(p: &Plan, count: usize, text: &str, query: &Budget) -> EvaluatedBatch {
        let number = || {
            Arc::new(
                Decimal128Array::from_iter((0..count).map(|i| (i % 5 != 0).then_some(i as i128)))
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ) as ArrayRef
        };
        let columns = [
            Value::adopt(number(), query).unwrap(),
            Value::adopt(
                Arc::new(StringArray::from_iter(
                    (0..count).map(|i| (i % 3 != 0).then_some(text)),
                )),
                query,
            )
            .unwrap(),
            Value::adopt(number(), query).unwrap(),
        ];
        let value = |e: &crate::expr::Expr| {
            let Kind::Column(i) = e.kind else {
                panic!("column fixture")
            };
            columns[i].clone()
        };
        let keys = p.groups.iter().map(value).collect::<Vec<_>>();
        let prepared_keys =
            Some(crate::keys::Keys::with_text_encoding(&keys, count, query, true).unwrap());
        EvaluatedBatch {
            rows: count,
            keys,
            values: p
                .aggregates
                .iter()
                .map(|a| a.input.as_ref().map(value))
                .collect(),
            prepared_keys,
        }
    }
    fn tokens(n: usize) -> Vec<EncounterToken> {
        (0..n).map(|i| EncounterToken::new(0, 0, i)).collect()
    }
    #[test]
    fn exact_preflight_headroom_funds_update_and_export_for_numeric_and_encoded_groups() {
        for sql in [
            "SELECT k,SUM(v) FROM t GROUP BY k",
            "SELECT k,SUM(v),AVG(v),AVG(v),COUNT(*) FROM t GROUP BY k",
            "SELECT s,COUNT(*) FROM t GROUP BY s",
            "SELECT s,k,SUM(v),AVG(v),COUNT(v) FROM t GROUP BY s,k",
        ] {
            let (p, e) = fixture(sql);
            for warm in [0, 7] {
                for rows in [1, 2, 3, 64, 130] {
                    let query = Budget::new(32 << 20);
                    let private = Budget::new(8 << 20);
                    let backing = query.reserve(private.limit()).unwrap();
                    let mut state = Aggregation::new(&p, &e, private.clone(), true).unwrap();
                    state.disable_dense();
                    if warm > 0 {
                        state
                            .update_with_encounters(
                                &p,
                                &e,
                                &mut Metrics::default(),
                                batch(&p, warm, "é\0old", &query),
                                &tokens(warm),
                            )
                            .unwrap();
                    }
                    let batch = batch(&p, rows, "é\0new", &query);
                    let before = private.used();
                    let groups = state.group_count;
                    let required = state.batch_export_headroom(&p, &e, &batch, &query).unwrap();
                    assert_eq!(private.used(), before);
                    assert_eq!(state.group_count, groups);
                    let mut block = private
                        .reserve(private.limit() - before - required + 1)
                        .unwrap();
                    assert!(!state.batch_export_fits(&p, &e, &batch, &query).unwrap());
                    block.release_excess(block.bytes() - 1);
                    assert!(state.batch_export_fits(&p, &e, &batch, &query).unwrap());
                    state
                        .update_with_encounters(
                            &p,
                            &e,
                            &mut Metrics::default(),
                            batch,
                            &tokens(rows),
                        )
                        .unwrap();
                    let raw = state
                        .into_raw_with_encounters(RawFunding::PrepaidPrivate(backing))
                        .unwrap();
                    assert!(private.peak() <= private.limit());
                    drop(raw);
                    drop(block);
                    assert_eq!(private.used(), 0);
                    assert_eq!(query.used(), 0);
                }
            }
        }
    }
    #[test]
    fn oversized_batch_and_missing_prepared_keys_fail_before_mutation() {
        let (p, e) = fixture("SELECT s,COUNT(*) FROM t GROUP BY s");
        let query = Budget::new(8 << 20);
        let private = Budget::new(128 << 10);
        let mut state = Aggregation::new(&p, &e, private.clone(), true).unwrap();
        state.disable_dense();
        let mut input = batch(&p, 2, &"x".repeat(256 << 10), &query);
        let before = private.used();
        assert!(!state.batch_export_fits(&p, &e, &input, &query).unwrap());
        assert_eq!(state.group_count, 0);
        assert_eq!(private.used(), before);
        input.prepared_keys = None;
        assert!(state.batch_export_headroom(&p, &e, &input, &query).is_err());
        assert!(
            state
                .batch_export_headroom(&p, &e, &batch(&p, 0, "", &query), &query)
                .is_err()
        );
        assert_eq!(state.group_count, 0);
        assert_eq!(private.used(), before);
    }
    #[test]
    fn unsupported_state_and_checked_arithmetic_are_rejected() {
        let (p, e) = fixture("SELECT k,COUNT(*) FROM t GROUP BY k");
        let query = Budget::new(8 << 20);
        let private = Budget::new(128 << 10);
        let mut state = Aggregation::new(&p, &e, private, true).unwrap();
        let input = batch(&p, 1, "", &query);
        assert!(state.batch_export_headroom(&p, &e, &input, &query).is_err());
        state.disable_dense();
        state.encounters_valid = false;
        assert!(state.batch_export_headroom(&p, &e, &input, &query).is_err());
        assert!(add(usize::MAX, 1).is_err());
        assert!(mul(usize::MAX, 2).is_err());
        let (p, e) = fixture("SELECT k,COUNT(DISTINCT v) FROM t GROUP BY k");
        let mut state = Aggregation::new(&p, &e, Budget::new(128 << 10), true).unwrap();
        state.disable_dense();
        assert!(
            state
                .batch_export_headroom(&p, &e, &batch(&p, 1, "", &query), &query)
                .is_err()
        );
    }

    #[test]
    fn malformed_types_shapes_and_foreign_funding_are_rejected_without_mutation() {
        let (p, e) = fixture("SELECT s,k,SUM(v),COUNT(*) FROM t GROUP BY s,k");
        let query = Budget::new(8 << 20);
        let private = Budget::new(1 << 20);
        let foreign = Budget::new(1 << 20);
        let mut state = Aggregation::new(&p, &e, private.clone(), true).unwrap();
        state.disable_dense();
        for case in 0..8 {
            let mut input = batch(&p, 2, "key", &query);
            match case {
                0 => input.values[0] = None,
                1 => {
                    input.values[0] =
                        Some(Value::adopt(Arc::new(Int64Array::from(vec![1, 2])), &query).unwrap())
                }
                2 => {
                    input.keys[0] =
                        Value::adopt(Arc::new(Int64Array::from(vec![1, 2])), &query).unwrap()
                }
                3 => input.prepared_keys = batch(&p, 1, "key", &query).prepared_keys,
                4 => {
                    input.prepared_keys =
                        Some(crate::keys::Keys::new(&input.keys, 2, &foreign).unwrap())
                }
                5 => input.values[0] = batch(&p, 2, "key", &foreign).values.remove(0),
                6 => input.keys[0] = batch(&p, 2, "key", &private).keys.remove(0),
                7 => input.values[1] = input.values[0].clone(),
                _ => unreachable!(),
            }
            let before = private.used();
            assert!(
                state.batch_export_headroom(&p, &e, &input, &query).is_err(),
                "case {case}"
            );
            assert_eq!(state.group_count, 0);
            assert_eq!(private.used(), before);
        }
        let input = batch(&p, 2, "key", &query);
        assert!(
            state
                .batch_export_headroom(&p, &e, &input, &private)
                .is_err()
        );
        drop(input);
        drop(state);
        assert_eq!(private.used(), 0);
        assert_eq!(query.used(), 0);
        assert_eq!(foreign.used(), 0);
    }
}
