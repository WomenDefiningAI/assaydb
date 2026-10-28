//! IN uses ordinary equality compatibility, with a typed path when hash-key
//! identity would change NULL/numeric/NaN semantics. No text/numeric coercion.
//! Mixed Int/Float membership promotes Int to binary64; no SQLite affinity.
use super::*;
use crate::expr::{Kind, Op, Value};

pub(super) fn validate(value: &Expr, right: &Schema) -> Result<Type> {
    if right.fields().len() != 1 {
        return Err("semantic: IN subquery must return exactly one column".into());
    }
    let ty = Type::from_field(right.field(0))?;
    crate::bind::binary(
        Op::Eq,
        value.clone(),
        Expr {
            kind: Kind::Column(0),
            ty: ty.clone(),
        },
    )
    .map_err(|e| format!("unsupported: IN subquery {e}"))?;
    Ok(ty)
}
pub(super) fn hash_equality(left: &Type, right: &Type) -> bool {
    left == right && !matches!(left, Type::Null | Type::Float)
}
/// Positive WHERE consumes UNKNOWN just like FALSE, but must use the same typed
/// membership as projections and NOT IN. Drop its private marker immediately.
pub(super) fn positive(left: Node, right: Node, value: Expr, name: &str) -> Node {
    let original = left.schema.clone();
    let width = original.fields().len();
    let rows = left.estimated_rows;
    let fields = original
        .fields()
        .iter()
        .cloned()
        .chain(std::iter::once(Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::Boolean,
            true,
        ))))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let marker = Node {
        schema: schema.clone(),
        estimated_rows: rows,
        op: RelOp::TypedMark {
            left: Box::new(left),
            right: Box::new(right),
            value,
            negated: false,
        },
    };
    let filtered = Node {
        schema,
        estimated_rows: rows,
        op: RelOp::Filter {
            input: Box::new(marker),
            predicate: Expr {
                kind: Kind::Column(width),
                ty: Type::Bool,
            },
        },
    };
    Node {
        schema: original,
        estimated_rows: rows,
        op: RelOp::Project {
            input: Box::new(filtered),
            columns: (0..width).collect(),
        },
    }
}
struct Set {
    exact: Vec<i128>,
    floats: HashSet<u64>,
    has_rows: bool,
    has_null: bool,
    left: Type,
    right: Type,
    // All containers drop before their metadata/state credit.
    leases: Vec<Reservation>,
    _header: Reservation,
}
fn numeric_float(n: i128, ty: &Type) -> Result<f64> {
    crate::numeric_float::scaled(n, ty.scale()?)
}
fn float_key(value: &Value, ty: &Type, row: usize) -> Result<Option<u64>> {
    let number = if *ty == Type::Float {
        value
            .array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("invalid membership float array")?
            .value(row)
    } else {
        numeric_float(crate::expr::numeric(&value.array).value(row), ty)?
    };
    Ok(Some(
        crate::expr::numeric_compare::canonical(number).to_bits(),
    ))
}
impl Set {
    fn new(left: Type, right: Type, budget: &Budget) -> Result<Self> {
        Ok(Self {
            exact: Vec::new(),
            floats: HashSet::new(),
            has_rows: false,
            has_null: false,
            left,
            right,
            leases: Vec::new(),
            _header: budget.reserve(512)?,
        })
    }
    fn floating(&self) -> bool {
        self.left == Type::Float || self.right == Type::Float
    }
    fn push(
        &mut self,
        value: &Value,
        budget: &Budget,
        cancel: &crate::spill::Cancellation,
    ) -> Result<()> {
        for row in 0..value.array.len() {
            if row % 1024 == 0 {
                cancel.check()?;
            }
            self.has_rows = true;
            if is_sql_null(value.array.as_ref(), row) {
                self.has_null = true;
                continue;
            }
            // NULL LHS can never match; RHS still executes fully for its errors.
            if self.left == Type::Null {
                continue;
            }
            if self.floating() {
                if let Some(bits) = float_key(value, &self.right, row)?
                    && !self.floats.contains(&bits)
                {
                    let lease = budget.reserve(256)?;
                    self.leases.push(lease);
                    self.floats.insert(bits);
                }
            } else {
                let n = crate::expr::numeric(&value.array).value(row);
                let lease = budget.reserve(256)?;
                self.leases.push(lease);
                // Original RHS encounter order, including duplicates. No eager
                // rescaling may create errors when no non-NULL pair is compared.
                self.exact.push(n);
            }
        }
        Ok(())
    }
    fn contains(
        &self,
        value: &Value,
        row: usize,
        cancel: &crate::spill::Cancellation,
    ) -> Result<Option<bool>> {
        if !self.has_rows {
            return Ok(Some(false));
        }
        if is_sql_null(value.array.as_ref(), row) {
            return Ok(None);
        }
        let matched = if self.floating() {
            float_key(value, &self.left, row)?.is_some_and(|bits| self.floats.contains(&bits))
        } else if self.right == Type::Null {
            false
        } else {
            let lhs = crate::expr::numeric(&value.array).value(row);
            let a = self.left.scale()?;
            let b = self.right.scale()?;
            let scale = a.max(b);
            let mut matched = false;
            for (i, rhs) in self.exact.iter().enumerate() {
                if i % 1024 == 0 {
                    cancel.check()?;
                }
                // Match ordinary numeric equality's checked pairwise rescaling.
                // Visit all pairs in source order, even after an earlier match,
                // so finding a match does not suppress a later equality error.
                matched |=
                    crate::types::rescale(lhs, a, scale)? == crate::types::rescale(*rhs, b, scale)?;
            }
            matched
        };
        Ok(if matched {
            Some(true)
        } else if self.has_null {
            None
        } else {
            Some(false)
        })
    }
}
// Keep this distinct typed-membership implementation out of the shared
// relational dispatcher; ordinary same-type operators do not need its state.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
pub(super) fn run(
    node: &Node,
    left: &Node,
    right: &Node,
    value: &Expr,
    negated: bool,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let budget = ctx.budget.clone();
    let cancel = ctx.options.cancellation.clone();
    let right_type = Type::from_field(right.schema.field(0))?;
    let mut set = Set::new(value.ty.clone(), right_type.clone(), &budget)?;
    right.run(ctx, &mut |batch| {
        cancel.check()?;
        let v = normalize_input(batch.column(0), &right_type, &budget)?;
        set.push(&v, &budget, &cancel)
    })?;
    left.run(ctx, &mut |batch| {
        cancel.check()?;
        let bytes = batch
            .num_rows()
            .checked_mul(16)
            .and_then(|n| n.checked_add(batch.num_columns().checked_mul(256)?))
            .and_then(|n| n.checked_add(512))
            .ok_or("membership batch metadata overflow")?;
        let _scratch = budget.reserve(bytes)?;
        let v = value.eval(&input(&batch, &budget, [value])?, &budget)?;
        let mut results = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                cancel.check()?;
            }
            results.push(set.contains(&v, row, &cancel)?.map(|v| v != negated));
        }
        let mut arrays = batch.columns().to_vec();
        arrays.push(Arc::new(BooleanArray::from(results)));
        emit(RecordBatch::try_new(node.schema.clone(), arrays).map_err(|e| e.to_string())?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::Cancellation;
    use arrow::array::Decimal128Array;

    fn numbers(values: Vec<Option<i128>>, budget: &Budget) -> Value {
        Value::adopt(Arc::new(Decimal128Array::from(values)), budget).unwrap()
    }
    #[test]
    fn mixed_state_budget_is_bounded_and_error_cleanup_releases_credit() {
        let inputs = Budget::new(8192);
        let values = numbers(vec![Some(1), Some(2), Some(3)], &inputs);
        let budget = Budget::new(512 + 2 * 256);
        let mut set = Set::new(Type::Int, Type::Decimal(1), &budget).unwrap();
        let error = set
            .push(&values, &budget, &Cancellation::default())
            .unwrap_err();
        assert!(error.starts_with("resource-limit: accounted memory"));
        assert_eq!(set.exact, vec![1, 2]);
        assert_eq!(budget.used(), budget.limit());
        drop(set);
        assert_eq!(budget.used(), 0);
        drop(values);
        assert_eq!(inputs.used(), 0);
    }
    #[test]
    fn cancellation_checks_rhs_build_and_pairwise_probe() {
        let budget = Budget::new(8192);
        let values = numbers(vec![Some(1)], &budget);
        let mut set = Set::new(Type::Int, Type::Decimal(1), &budget).unwrap();
        let cancel = Cancellation::default();
        cancel.cancel();
        assert!(
            set.push(&values, &budget, &cancel)
                .unwrap_err()
                .starts_with("cancelled:")
        );
        assert!(!set.has_rows);
        set.push(&values, &budget, &Cancellation::default())
            .unwrap();
        assert!(
            set.contains(&values, 0, &cancel)
                .unwrap_err()
                .starts_with("cancelled:")
        );
        drop(set);
        drop(values);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn integer_real_keys_promote_deduplicate_and_keep_funding() {
        let input_budget = Budget::new(16384);
        let values = numbers(
            vec![
                Some(9007199254740993),
                Some(9007199254740992),
                Some(9007199254740992),
            ],
            &input_budget,
        );
        let reals = Value::adopt(
            Arc::new(Float64Array::from(vec![
                Some(9007199254740992.0),
                Some(0.5),
                None,
            ])),
            &input_budget,
        )
        .unwrap();
        let budget = Budget::new(512 + 2 * 256);
        let cancel = Cancellation::default();
        let mut state = Set::new(Type::Float, Type::Int, &budget).unwrap();
        state.push(&values, &budget, &cancel).unwrap();
        assert_eq!(
            state.floats,
            HashSet::from([9007199254740992.0_f64.to_bits()])
        );
        assert_eq!(budget.used(), 512 + 256);
        // Rounding merges all three original keys. Distinct additional keys
        // still encounter the same accountant limit before insertion.
        let extra = numbers(vec![Some(0), Some(1)], &input_budget);
        assert!(
            state
                .push(&extra, &budget, &cancel)
                .unwrap_err()
                .starts_with("resource-limit: accounted memory")
        );
        assert_eq!(state.floats.len(), 2);
        assert_eq!(budget.used(), budget.limit());
        drop(extra);
        assert_eq!(state.contains(&reals, 0, &cancel).unwrap(), Some(true));
        assert_eq!(state.contains(&reals, 1, &cancel).unwrap(), Some(false));
        assert_eq!(state.contains(&reals, 2, &cancel).unwrap(), None);
        // The removed exact mixed-key probe was a linear scan with periodic
        // cancellation checks; DOUBLE keys use the O(1) Float hash probe, which
        // (like Float/Float membership) relies on the per-batch checks.
        drop(state);
        assert_eq!(budget.used(), 0);
        let mut state = Set::new(Type::Int, Type::Float, &budget).unwrap();
        state.push(&reals, &budget, &cancel).unwrap();
        assert_eq!(state.floats.len(), 2);
        assert_eq!(state.contains(&values, 0, &cancel).unwrap(), Some(true));
        assert_eq!(state.contains(&values, 1, &cancel).unwrap(), Some(true));
        drop(state);
        drop(reals);
        drop(values);
        assert_eq!(budget.used(), 0);
        assert_eq!(input_budget.used(), 0);
    }

    #[test]
    fn exact_pairs_preserve_rhs_order_and_do_not_hide_later_overflow() {
        let budget = Budget::new(16384);
        let rhs = numbers(vec![Some(1), Some(10_i128.pow(38) - 1)], &budget);
        let lhs = numbers(vec![Some(10)], &budget);
        let cancel = Cancellation::default();
        let mut set = Set::new(Type::Decimal(1), Type::Decimal(0), &budget).unwrap();
        set.push(&rhs, &budget, &cancel).unwrap();
        assert!(
            set.contains(&lhs, 0, &cancel)
                .unwrap_err()
                .contains("overflow")
        );
        drop(set);
        drop(lhs);
        drop(rhs);
        assert_eq!(budget.used(), 0);
    }
}
