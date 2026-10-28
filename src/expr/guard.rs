//! Guarded AND/OR evaluation.
//!
//! Vector evaluation computes both operands of AND/OR for every row of a
//! batch. A row-dependent data error (division by zero, an invalid or
//! out-of-range cast, Float overflow, ...) in one operand must not abort the
//! query on a row whose result the other operand already decides:
//!
//! * `a AND b` is FALSE on every row where `a` or `b` is FALSE;
//! * `a OR b` is TRUE on every row where `a` or `b` is TRUE;
//!
//! even if the other operand fails on that row. In a row filter (WHERE,
//! JOIN ON, HAVING) a NULL operand of the top-level AND chain also decides
//! the row, which is rejected either way (`eval_filter`). Otherwise the
//! failure is raised, exactly as before. The rule does not depend on operand
//! order, so conjunct reordering, flattening or factoring cannot bypass a
//! guard, and it is decided row by row, so workers, batch size and spill
//! cannot change which rows succeed.
//!
//! The common path is unchanged: both operands are evaluated once over the
//! whole batch and combined. Only when an operand raises a row error does the
//! batch fall back to selection semantics: the failing operand is evaluated
//! again on just the rows the other operand leaves undecided. When both
//! operands fail on the batch, it is split in halves until the failures
//! separate; a single row on which both fail raises the left operand's error.
//! Resource, cancellation and internal failures are never suppressed.
use super::{Expr, Input, Kind, Op, Value, boolean, is_sql_null, take_input};
use crate::{
    Result,
    memory::{Budget, Reservation},
};
use arrow::array::{ArrayRef, BooleanArray};
use std::sync::Arc;

/// Data errors that depend on one row's values: exactly the errors that
/// canonical error replay selects by first failing row (division by zero,
/// overflow, CAST data errors, value-dependent refusals). Only these may be
/// suppressed on a row decided by the other operand of AND/OR. Resource
/// limits, cancellation and internal failures say nothing about a row and
/// always surface.
pub(crate) fn row_error(error: &crate::Error) -> bool {
    crate::execute::error_replay::semantic_error(error.as_str())
}

fn truth(array: &ArrayRef, row: usize) -> Option<bool> {
    (!is_sql_null(array.as_ref(), row)).then(|| boolean(array).value(row))
}

fn kleene(op: Op, x: Option<bool>, y: Option<bool>) -> Option<bool> {
    match (op, x, y) {
        (Op::And, Some(false), _) | (Op::And, _, Some(false)) => Some(false),
        (Op::And, Some(true), Some(true)) => Some(true),
        (Op::Or, Some(true), _) | (Op::Or, _, Some(true)) => Some(true),
        (Op::Or, Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// Where an AND/OR is evaluated. A row filter (WHERE, JOIN ON, HAVING)
/// keeps only TRUE rows, so along its top-level AND chain a NULL operand
/// rejects the row exactly like FALSE: PostgreSQL stops its qual list at the
/// first non-TRUE qual and DuckDB drops non-TRUE rows from the selection.
/// Everywhere else (values, OR, NOT, CASE conditions, FILTER) the exact Kleene
/// value is needed and only FALSE decides AND.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Value,
    Filter,
}

impl Expr {
    /// Evaluate a row filter: callers keep only TRUE rows and must not
    /// distinguish FALSE from NULL. The result equals `eval` wherever `eval`
    /// succeeds; a top-level conjunct that is NULL also decides the row.
    pub fn eval_filter(&self, input: &Input, budget: &Budget) -> Result<Value> {
        let Kind::Binary(Op::And, a, b) = &self.kind else {
            return self.eval(input, budget);
        };
        let lease = budget.reserve(super::workspace(input.len, 0)?)?;
        self.eval_logical(Op::And, a, b, input, budget, lease, Context::Filter)
    }

    fn operand(&self, context: Context, input: &Input, budget: &Budget) -> Result<Value> {
        match context {
            Context::Value => self.eval(input, budget),
            Context::Filter => self.eval_filter(input, budget),
        }
    }

    /// `Kind::Binary(And | Or)` in a value context.
    pub(super) fn eval_boolean(
        &self,
        op: Op,
        a: &Expr,
        b: &Expr,
        input: &Input,
        budget: &Budget,
        lease: Reservation,
    ) -> Result<Value> {
        self.eval_logical(op, a, b, input, budget, lease, Context::Value)
    }

    /// On success this is exactly the previous evaluation: left operand,
    /// right operand, Kleene combination.
    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    fn eval_logical(
        &self,
        op: Op,
        a: &Expr,
        b: &Expr,
        input: &Input,
        budget: &Budget,
        lease: Reservation,
        context: Context,
    ) -> Result<Value> {
        let av = match a.operand(context, input, budget) {
            Ok(value) => value,
            Err(error) if row_error(&error) => {
                return self.guarded(op, a, b, None, error, input, budget, lease, context);
            }
            Err(error) => return Err(error),
        };
        match b.operand(context, input, budget) {
            Ok(bv) => self.combine_binary(op, a, b, av, bv, input.len, lease),
            Err(error) if row_error(&error) => {
                self.guarded(op, a, b, Some(av), error, input, budget, lease, context)
            }
            Err(error) => Err(error),
        }
    }

    /// One operand failed with a row error. `left` is the left operand's
    /// value when it succeeded (so the right operand failed).
    #[cold]
    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    fn guarded(
        &self,
        op: Op,
        a: &Expr,
        b: &Expr,
        left: Option<Value>,
        failure: crate::Error,
        input: &Input,
        budget: &Budget,
        lease: Reservation,
        context: Context,
    ) -> Result<Value> {
        let n = input.len;
        let (decider, failing) = match left {
            Some(av) => (av, b),
            None => match b.operand(context, input, budget) {
                Ok(bv) => (bv, a),
                Err(error) if row_error(&error) => {
                    return self.bisect(op, a, b, failure, input, budget, lease, context);
                }
                Err(error) => return Err(error),
            },
        };
        // Whether the decider alone determines the row's result.
        let decides = |value: Option<bool>| match (op, context) {
            (Op::Or, _) => value == Some(true),
            (_, Context::Value) => value == Some(false),
            (_, Context::Filter) => value != Some(true),
        };
        let rows = (0..n as u32)
            .filter(|row| !decides(truth(&decider.array, *row as usize)))
            .collect::<Vec<_>>();
        // Every row needs the failing operand: its error is genuine.
        if rows.len() == n {
            return Err(failure);
        }
        let mut values = (0..n)
            .map(|row| truth(&decider.array, row))
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            let subset = take_input(input, &rows, budget)?;
            let other = failing.operand(context, &subset, budget)?;
            for (index, row) in rows.iter().enumerate() {
                let row = *row as usize;
                values[row] = kleene(op, truth(&other.array, index), values[row]);
            }
        }
        Ok(Value::allocated(
            Arc::new(BooleanArray::from(values)),
            lease,
        ))
    }

    /// Both operands failed somewhere in this batch. Split it until each
    /// part either succeeds or is a single row on which both operands fail;
    /// that row needs a value, and the left operand's error is raised (the
    /// error the eager evaluation reported for it). The earliest failing part
    /// wins, so the raised row is the first one in batch order.
    #[allow(clippy::too_many_arguments)]
    fn bisect(
        &self,
        op: Op,
        a: &Expr,
        b: &Expr,
        failure: crate::Error,
        input: &Input,
        budget: &Budget,
        lease: Reservation,
        context: Context,
    ) -> Result<Value> {
        let n = input.len;
        if n <= 1 {
            return Err(failure);
        }
        let middle = n / 2;
        let mut parts = Vec::with_capacity(2);
        for half in [0..middle, middle..n] {
            let rows = (half.start as u32..half.end as u32).collect::<Vec<_>>();
            let subset = take_input(input, &rows, budget)?;
            let lease = budget.reserve(super::workspace(rows.len(), 0)?)?;
            parts.push(self.eval_logical(op, a, b, &subset, budget, lease, context)?);
        }
        let refs = parts.iter().map(|v| v.array.as_ref()).collect::<Vec<_>>();
        let merged =
            arrow::compute::concat(&refs).map_err(|e| crate::Error::from(e.to_string()))?;
        Ok(Value::allocated(merged, lease))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Literal, types::Type};
    use arrow::array::{Array, Decimal128Array, StringArray};

    fn column(index: usize, ty: Type) -> Expr {
        Expr {
            kind: Kind::Column(index),
            ty,
        }
    }
    fn literal(value: i128, ty: Type) -> Expr {
        Expr {
            kind: Kind::Literal(Literal::Num(value)),
            ty,
        }
    }
    fn binary(op: Op, a: Expr, b: Expr, ty: Type) -> Expr {
        Expr {
            kind: Kind::Binary(op, Box::new(a), Box::new(b)),
            ty,
        }
    }
    fn not(a: Expr) -> Expr {
        Expr {
            kind: Kind::Not(Box::new(a)),
            ty: Type::Bool,
        }
    }
    /// x <> 0 (x DECIMAL(2) column 0)
    fn nonzero() -> Expr {
        binary(
            Op::Ne,
            column(0, Type::Decimal(2)),
            literal(0, Type::Decimal(2)),
            Type::Bool,
        )
    }
    /// 10 / x > 1 (DECIMAL division by zero raises)
    fn ratio() -> Expr {
        binary(
            Op::Gt,
            binary(
                Op::Div,
                literal(1000, Type::Decimal(2)),
                column(0, Type::Decimal(2)),
                Type::Float,
            ),
            Expr {
                kind: Kind::Cast(Box::new(literal(1, Type::Int))),
                ty: Type::Float,
            },
            Type::Bool,
        )
    }
    /// CAST(s AS BIGINT) > 5 (s TEXT column 1)
    fn parsed() -> Expr {
        // This control needs a fallible parser. Public INTEGER now uses the
        // permissive prefix contract; BIGINT retains the checked dialect cast.
        let cast = crate::expr::cast_dialect::bind(
            column(1, Type::Text),
            &sqlparser::ast::DataType::BigInt(None),
        )
        .unwrap();
        binary(Op::Gt, cast, literal(5, Type::Int), Type::Bool)
    }
    fn input(x: &[Option<i128>], s: &[Option<&str>], budget: &Budget) -> Input {
        let x: ArrayRef = Arc::new(
            Decimal128Array::from(x.to_vec())
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        let s: ArrayRef = Arc::new(StringArray::from(s.to_vec()));
        Input {
            len: x.len(),
            columns: vec![
                Some(Value::adopt(x, budget).unwrap()),
                Some(Value::adopt(s, budget).unwrap()),
            ],
        }
    }
    fn truths(value: &Value) -> Vec<Option<bool>> {
        (0..value.array.len())
            .map(|i| truth(&value.array, i))
            .collect()
    }

    #[test]
    fn decided_rows_suppress_errors_in_either_operand_order() {
        let budget = Budget::new(1 << 24);
        let x = [Some(1000), Some(0), None, Some(-350), Some(50), Some(0)];
        let s = [Some("1"); 6];
        let data = input(&x, &s, &budget);
        let expected = vec![
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(false),
        ];
        for e in [
            binary(Op::And, nonzero(), ratio(), Type::Bool),
            binary(Op::And, ratio(), nonzero(), Type::Bool),
        ] {
            assert_eq!(truths(&e.eval(&data, &budget).unwrap()), expected);
        }
        // x = 0 OR 10/x > 1, both orders.
        let zero = not(nonzero());
        let expected_or = vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
        ];
        for e in [
            binary(Op::Or, zero.clone(), ratio(), Type::Bool),
            binary(Op::Or, ratio(), zero.clone(), Type::Bool),
        ] {
            assert_eq!(truths(&e.eval(&data, &budget).unwrap()), expected_or);
        }
        assert_eq!(budget.used(), data_bytes(&data));
    }

    fn data_bytes(input: &Input) -> usize {
        input
            .columns
            .iter()
            .flatten()
            .map(|v| v.array.get_array_memory_size())
            .sum()
    }

    #[test]
    fn needed_rows_still_raise_the_operand_error() {
        let budget = Budget::new(1 << 24);
        // NULL guard does not decide AND: the failing value is needed.
        let data = input(&[Some(0)], &[Some("1")], &budget);
        let null_guard = Expr {
            kind: Kind::Literal(Literal::Null),
            ty: Type::Bool,
        };
        for e in [
            binary(Op::And, null_guard.clone(), ratio(), Type::Bool),
            binary(Op::And, ratio(), null_guard.clone(), Type::Bool),
            binary(Op::Or, null_guard.clone(), ratio(), Type::Bool),
            binary(Op::And, nonzero(), ratio(), Type::Bool).clone(),
            not(binary(Op::And, ratio(), ratio(), Type::Bool)),
        ] {
            let error = e.eval(&data, &budget).err();
            let expected = if matches!(&e.kind, Kind::Binary(Op::And, a, _) if **a == nonzero()) {
                None
            } else {
                Some("division by zero")
            };
            assert_eq!(error.as_deref(), expected, "{e:?}");
        }
    }

    #[test]
    fn filters_treat_a_null_top_level_conjunct_as_deciding() {
        let budget = Budget::new(1 << 24);
        let data = input(&[Some(0), Some(50)], &[Some("1"), Some("1")], &budget);
        let null_guard = Expr {
            kind: Kind::Literal(Literal::Null),
            ty: Type::Bool,
        };
        let falsehood = Expr {
            kind: Kind::Literal(Literal::Bool(false)),
            ty: Type::Bool,
        };
        // NULL AND 10/x > 1 rejects both rows as a filter; the value needs 10/0.
        for e in [
            binary(Op::And, null_guard.clone(), ratio(), Type::Bool),
            binary(Op::And, ratio(), null_guard.clone(), Type::Bool),
            binary(
                Op::And,
                binary(Op::And, ratio(), null_guard.clone(), Type::Bool),
                ratio(),
                Type::Bool,
            ),
        ] {
            let mask = e.eval_filter(&data, &budget).unwrap();
            assert!(truths(&mask).iter().all(|v| *v != Some(true)), "{e:?}");
            assert_eq!(e.eval(&data, &budget).err().unwrap(), "division by zero");
        }
        // Below OR or NOT the exact value is needed again, as in PostgreSQL.
        for e in [
            binary(
                Op::Or,
                binary(Op::And, null_guard.clone(), ratio(), Type::Bool),
                falsehood.clone(),
                Type::Bool,
            ),
            not(binary(Op::And, null_guard.clone(), ratio(), Type::Bool)),
        ] {
            assert_eq!(
                e.eval_filter(&data, &budget).err().unwrap(),
                "division by zero"
            );
        }
        // Successful filters equal the value evaluation exactly.
        let e = binary(Op::And, nonzero(), ratio(), Type::Bool);
        assert_eq!(
            truths(&e.eval_filter(&data, &budget).unwrap()),
            truths(&e.eval(&data, &budget).unwrap())
        );
    }

    #[test]
    fn both_operands_failing_bisect_to_the_first_needed_row_at_every_width() {
        // Row-by-row reference: (x <> 0 AND 10/x > 1) AND (s numeric AND CAST(s) > 5)
        // flattened as (10/x > 1 AND CAST(s) > 5) guarded by both sides.
        let rows: Vec<(Option<i128>, Option<&str>)> = vec![
            (Some(100), Some("9")),
            (Some(0), Some("bad")), // both fail, but the OR arm below decides
            (Some(0), Some("1")),   // left fails, CAST(1) > 5 is FALSE: decided
            (Some(100), Some("x")), // right fails, 10/1 > 1 is TRUE: needed
            (Some(1000), Some("7")),
            (Some(0), Some("8")), // left fails, right TRUE: needed
        ];
        let budget = Budget::new(1 << 26);
        let and = binary(Op::And, ratio(), parsed(), Type::Bool);
        // Without row 3 and row 5 nothing is needed except decided rows... but row 1
        // fails on both sides, so it raises the left (division) error.
        let raise = |picked: &[usize]| {
            let x = picked.iter().map(|i| rows[*i].0).collect::<Vec<_>>();
            let s = picked.iter().map(|i| rows[*i].1).collect::<Vec<_>>();
            let data = input(&x, &s, &budget);
            and.eval(&data, &budget).map(|v| truths(&v))
        };
        assert_eq!(
            raise(&[0, 2, 4]).unwrap(),
            vec![Some(true), Some(false), Some(false)]
        );
        // Row 1: both fail -> left error. Row 3: CAST fails, needed. Row 5: division needed.
        assert_eq!(raise(&[0, 1, 2]).err().unwrap(), "division by zero");
        assert_eq!(
            raise(&[0, 2, 3, 5]).err().unwrap(),
            "invalid input syntax for type bigint: \"x\""
        );
        assert_eq!(raise(&[0, 2, 5, 3]).err().unwrap(), "division by zero");
        // Many interleaved failures of both operands, every batch width.
        let mut x = Vec::new();
        let mut s = Vec::new();
        for i in 0..200 {
            x.push(Some(if i % 3 == 0 { 0 } else { 100 }));
            s.push(Some(if i % 5 == 0 {
                "bad"
            } else if i % 2 == 0 {
                "1"
            } else {
                "9"
            }));
        }
        // OR guard deciding every failing row: (x = 0 OR 10/x > 1) AND (s = 'bad' OR CAST(s) > 5)
        let text_bad = binary(
            Op::Eq,
            column(1, Type::Text),
            Expr {
                kind: Kind::Literal(Literal::Text("bad".into())),
                ty: Type::Text,
            },
            Type::Bool,
        );
        let guarded = binary(
            Op::And,
            binary(Op::Or, ratio(), not(nonzero()), Type::Bool),
            binary(Op::Or, parsed(), text_bad, Type::Bool),
            Type::Bool,
        );
        let expected = (0..200)
            .map(|i| Some(i % 5 == 0 || i % 2 == 1))
            .collect::<Vec<_>>();
        for width in [1, 2, 3, 7, 64, 200] {
            for start in (0..200).step_by(width) {
                let end = (start + width).min(200);
                let data = input(&x[start..end], &s[start..end], &budget);
                assert_eq!(
                    truths(&guarded.eval(&data, &budget).unwrap()),
                    expected[start..end],
                    "width {width} start {start}"
                );
            }
        }
        let data = input(&x, &s, &budget);
        let unguarded = binary(Op::And, ratio(), parsed(), Type::Bool);
        // First needed failing row is row 0: x = 0 (left fails) and s = 'bad' (right fails).
        assert_eq!(
            unguarded.eval(&data, &budget).err().unwrap(),
            "division by zero"
        );
    }

    #[test]
    fn resource_and_internal_failures_are_never_suppressed() {
        assert!(row_error(&"division by zero".into()));
        assert!(row_error(
            &"invalid input syntax for type integer: \"a\"".into()
        ));
        assert!(row_error(&"integer out of range".into()));
        assert!(row_error(&crate::Error::decimal_precision_overflow()));
        for message in [
            "resource-limit: accounted memory exhausted",
            "cancelled: query execution",
            "internal: mixed numeric provenance column",
            "missing projected column",
            "batch size overflow",
        ] {
            assert!(!row_error(&message.into()), "{message}");
        }
        // Every memory limit either yields the exact answer or a resource
        // error, including limits that fail only inside the fallback; a
        // decided row never hides a resource failure.
        let budget = Budget::new(1 << 24);
        let data = input(&[Some(0), Some(100)], &[Some("1"), Some("1")], &budget);
        let e = binary(Op::And, nonzero(), ratio(), Type::Bool);
        let (mut failed, mut succeeded) = (0, 0);
        for limit in (0..8192).step_by(16) {
            let tight = Budget::new(limit);
            match e.eval(&data, &tight) {
                Ok(value) => {
                    assert_eq!(truths(&value), vec![Some(false), Some(true)]);
                    succeeded += 1;
                }
                Err(error) => {
                    assert!(error.starts_with("resource-limit"), "{limit}: {error}");
                    failed += 1;
                }
            }
            assert_eq!(tight.used(), 0);
        }
        assert!(failed > 0 && succeeded > 0);
    }
}
