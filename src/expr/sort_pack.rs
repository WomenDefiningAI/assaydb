//! STRING_AGG input packing. Each non-NULL value becomes one entry:
//! `[row-encoded ORDER BY keys][TEXT value bytes][key length, u32 LE]`.
//! The key prefix is arrow's memcmp-comparable row format with the requested
//! direction and NULL placement, so byte order is SQL ORDER BY order. Float
//! keys are canonicalized first: -0.0 sorts as 0.0 (a peer) and every NaN is
//! one value above +infinity, as in DuckDB and PostgreSQL. One packed column
//! travels through every aggregation path (including spill) unchanged.
use super::*;
use arrow::compute::SortOptions;
use arrow::row::{RowConverter, SortField};

/// The engine's one canonical Float key (signed zeros equal, one positive NaN
/// that Arrow's row format orders above +infinity), shared with group keys,
/// DISTINCT sets and ORDER BY.
fn canonical(array: &ArrayRef) -> ArrayRef {
    numeric_compare::canonical_array(array)
}

pub(super) fn evaluate(
    keys: &[(Expr, bool, bool)],
    value: &Expr,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let n = input.len;
    let value = value.eval(input, budget)?;
    if value.array.data_type() == &DataType::Null {
        let lease = budget.reserve(256)?;
        return Ok(Value::allocated(
            new_null_array(&DataType::Binary, n),
            lease,
        ));
    }
    let text = value
        .array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("internal: STRING_AGG value is not TEXT")?;
    let mut evaluated = Vec::with_capacity(keys.len());
    let mut columns = Vec::with_capacity(keys.len());
    let mut fields = Vec::with_capacity(keys.len());
    for (key, descending, nulls_first) in keys {
        let key = key.eval(input, budget)?;
        // A NULL-typed key is constant and cannot change the order.
        if key.array.data_type() == &DataType::Null {
            continue;
        }
        let array = canonical(&key.array);
        fields.push(SortField::new_with_options(
            array.data_type().clone(),
            SortOptions {
                descending: *descending,
                nulls_first: *nulls_first,
            },
        ));
        columns.push(array);
        evaluated.push(key);
    }
    let workspace = columns
        .iter()
        .map(|a| a.get_array_memory_size())
        .try_fold(0_usize, |total, bytes| {
            total.checked_add(bytes.checked_mul(3)?)
        })
        .and_then(|bytes| bytes.checked_add(n.checked_mul(64)?))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or("STRING_AGG key workspace overflow")?;
    let _workspace = budget.reserve(workspace)?;
    let rows = if fields.is_empty() {
        None
    } else {
        let converter = RowConverter::new(fields).map_err(|e| e.to_string())?;
        Some(
            converter
                .convert_columns(&columns)
                .map_err(|e| e.to_string())?,
        )
    };
    let mut total = 0_usize;
    for row in 0..n {
        if text.is_null(row) {
            continue;
        }
        let key = rows.as_ref().map_or(0, |rows| rows.row(row).as_ref().len());
        total = total
            .checked_add(key + text.value(row).len() + 4)
            .ok_or("STRING_AGG input size overflow")?;
    }
    if total > i32::MAX as usize {
        return Err("resource-limit: STRING_AGG input batch exceeds 2 GiB".into());
    }
    let lease = budget.reserve(
        total
            .checked_add(n.checked_mul(8).ok_or("STRING_AGG input size overflow")?)
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or("STRING_AGG input size overflow")?,
    )?;
    let mut builder = BinaryBuilder::with_capacity(n, total);
    let mut entry = Vec::new();
    for row in 0..n {
        if text.is_null(row) {
            builder.append_null();
            continue;
        }
        entry.clear();
        let key = rows.as_ref().map(|rows| rows.row(row));
        let key = key.as_ref().map_or(&[][..], |key| key.as_ref());
        entry.extend_from_slice(key);
        entry.extend_from_slice(text.value(row).as_bytes());
        let length = u32::try_from(key.len()).map_err(|_| "STRING_AGG key too large")?;
        entry.extend_from_slice(&length.to_le_bytes());
        builder.append_value(&entry);
    }
    drop(evaluated);
    Ok(Value::allocated(Arc::new(builder.finish()), lease))
}

/// Split one packed entry into its key bytes and TEXT value bytes.
pub(crate) fn split(entry: &[u8]) -> Result<(&[u8], &[u8])> {
    let (body, length) = entry
        .split_last_chunk::<4>()
        .ok_or("internal: malformed STRING_AGG entry")?;
    let key = u32::from_le_bytes(*length) as usize;
    if key > body.len() {
        return Err("internal: malformed STRING_AGG entry".into());
    }
    Ok(body.split_at(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(array: ArrayRef, budget: &Budget) -> Option<Value> {
        Some(Value::adopt(array, budget).unwrap())
    }

    #[test]
    fn packed_order_matches_sql_order_and_canonical_floats() {
        let budget = Budget::new(1 << 20);
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(0xfff8_0000_0000_0000)),
            Some(f64::INFINITY),
            None,
            Some(-2.5),
        ]));
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            Some("one"),
            Some("negzero"),
            Some("zero"),
            Some("nan"),
            Some("inf"),
            Some("null"),
            None,
        ]));
        let input = Input {
            columns: vec![column(floats, &budget), column(text, &budget)],
            len: 7,
        };
        let key = Expr {
            ty: Type::Float,
            kind: Kind::Column(0),
        };
        let value = Expr {
            ty: Type::Text,
            kind: Kind::Column(1),
        };
        for (descending, nulls_first) in [(false, false), (true, true)] {
            let packed = evaluate(
                &[(key.clone(), descending, nulls_first)],
                &value,
                &input,
                &budget,
            )
            .unwrap();
            let packed = packed.array.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert!(packed.is_null(6), "NULL values contribute no entry");
            let mut entries = (0..6)
                .map(|i| split(packed.value(i)).unwrap())
                .collect::<Vec<_>>();
            entries.sort_unstable();
            let order = entries
                .iter()
                .map(|(_, value)| std::str::from_utf8(value).unwrap())
                .collect::<Vec<_>>();
            let mut wanted = vec!["negzero", "zero", "one", "inf", "nan", "null"];
            if descending {
                wanted = vec!["null", "nan", "inf", "one", "negzero", "zero"];
            }
            assert_eq!(order, wanted);
            // -0.0 and 0.0 have identical key bytes: they are peers.
            assert_eq!(
                split(packed.value(1)).unwrap().0,
                split(packed.value(2)).unwrap().0
            );
        }
    }
}
