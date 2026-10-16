use crate::{
    Result,
    memory::{Budget, Reservation},
    types::{Type, checked, factor, rescale},
};
use arrow::array::*;
use arrow::datatypes::DataType;
use serde::Serialize;
use std::sync::Arc;
pub(crate) mod casts;
mod integer;
use integer::{add_sub, multiply};
mod nullif;
pub(crate) mod numeric_compare;
mod remainder;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Literal {
    Num(i128),
    Bool(bool),
    Text(String),
    Binary(Vec<u8>),
    Date(i32),
    Null,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Kind {
    Column(usize),
    Function(String, Box<Expr>),
    Position(Box<Expr>, Box<Expr>),
    NullIf(Box<Expr>, Box<Expr>),
    Literal(Literal),
    Binary(Op, Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Neg(Box<Expr>),
    IsNull(Box<Expr>, bool),
    Like(Box<Expr>, Box<Expr>, bool),
    Year(Box<Expr>),
    Substring(Box<Expr>, usize, usize),
    Case(Vec<(Expr, Expr)>, Box<Expr>),
    Cast(Box<Expr>),
    CastDecimal(Box<Expr>, u8, i8),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Expr {
    pub kind: Kind,
    pub ty: Type,
}
#[derive(Clone)]
pub struct Value {
    pub array: ArrayRef,
    _lease: Arc<Reservation>,
}
impl Value {
    pub(crate) fn funded_by(&self, budget: &Budget) -> bool {
        self._lease.budget().same_domain(budget)
    }
    pub fn adopt(array: ArrayRef, budget: &Budget) -> Result<Self> {
        let lease = budget.reserve(array.get_array_memory_size())?;
        Ok(Self {
            array,
            _lease: Arc::new(lease),
        })
    }
    fn allocated(array: ArrayRef, lease: Reservation) -> Self {
        Self {
            array,
            _lease: Arc::new(lease),
        }
    }
}
pub struct Input {
    pub columns: Vec<Option<Value>>,
    pub len: usize,
}
/// SQL NULL includes Arrow's NullArray, which has no physical validity bitmap
/// and therefore returns false from Array::is_null despite every value being NULL.
/// Other accepted input types have no encoded/nested logical validity to merge.
pub fn is_sql_null(array: &dyn Array, index: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(index)
}
pub fn numeric(a: &ArrayRef) -> &Decimal128Array {
    a.as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("bound numeric array")
}
pub fn boolean(a: &ArrayRef) -> &BooleanArray {
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bound boolean array")
}
fn decimal(values: Vec<Option<i128>>, scale: i8) -> Result<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .map_err(|e| e.to_string())?,
    ))
}
fn decimal_non_null(values: Vec<i128>, scale: i8) -> Result<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .map_err(|e| e.to_string())?,
    ))
}
// Keep the new checked operation's row loop outside the common evaluator.
// Its output remains the ordinary normalized Int representation, funded by
// Expr::eval's existing workspace reservation.
#[inline(never)]
fn integer_division(a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(a.len());
    for (x, y) in numeric(a).iter().zip(numeric(b).iter()) {
        values.push(integer::divide(x, y)?);
    }
    decimal(values, 0)
}
// Prove every scaled operand and result, not only the final expression value.
fn narrow_factors(amax: u128, bmax: u128, op: Op, af: i128, bf: i128) -> Option<(i64, i64)> {
    let limit = i64::MAX as u128;
    if amax > limit || bmax > limit {
        return None;
    }
    let af = i64::try_from(af).ok()?;
    let bf = i64::try_from(bf).ok()?;
    if af < 0 || bf < 0 {
        return None;
    }
    let bound = match op {
        Op::Mul => amax.checked_mul(bmax)?,
        Op::Add | Op::Sub => amax
            .checked_mul(af as u128)?
            .checked_add(bmax.checked_mul(bf as u128)?)?,
        _ => return None,
    };
    if bound > limit {
        return None;
    }
    Some((af, bf))
}
fn narrow_batch_arithmetic(
    a: &[i128],
    b: &[i128],
    op: Op,
    af: i128,
    bf: i128,
) -> Option<Vec<i128>> {
    let amax = a.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0);
    let bmax = b.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0);
    let (af, bf) = narrow_factors(amax, bmax, op, af, bf)?;
    // Absolute bounds prove both scaled operands and every result fit i64.
    // Cancellation is not used to justify a narrower intermediate. Precision38
    // and i128 checks therefore cannot fail on this path, including empty input.
    let values = match op {
        Op::Mul => a
            .iter()
            .zip(b)
            .map(|(x, y)| i128::from((*x as i64) * (*y as i64)))
            .collect(),
        Op::Add => a
            .iter()
            .zip(b)
            .map(|(x, y)| i128::from((*x as i64) * af + (*y as i64) * bf))
            .collect(),
        Op::Sub => a
            .iter()
            .zip(b)
            .map(|(x, y)| i128::from((*x as i64) * af - (*y as i64) * bf))
            .collect(),
        _ => unreachable!(),
    };
    Some(values)
}
fn workspace(n: usize, extra: usize) -> Result<usize> {
    n.checked_mul(64 + extra)
        .and_then(|x| x.checked_add(256))
        .ok_or("batch size overflow".into())
}
impl Expr {
    /// Factor deterministic boolean expressions using three-valued distributivity.
    /// Keep all distinct operands: do not fold away branches that could error.
    pub fn simplify_boolean(self) -> Self {
        let Kind::Binary(op @ (Op::And | Op::Or), a, b) = self.kind else {
            return self;
        };
        fn terms(expr: Expr, op: Op, out: &mut Vec<Expr>) {
            if let Kind::Binary(inner, a, b) = &expr.kind
                && *inner == op
            {
                terms((**a).clone(), op, out);
                terms((**b).clone(), op, out);
                return;
            }
            if !out.contains(&expr) {
                out.push(expr);
            }
        }
        fn join(terms: Vec<Expr>, op: Op) -> Expr {
            terms
                .into_iter()
                .reduce(|a, b| Expr {
                    ty: Type::Bool,
                    kind: Kind::Binary(op, Box::new(a), Box::new(b)),
                })
                .unwrap_or(Expr {
                    ty: Type::Bool,
                    kind: Kind::Literal(Literal::Bool(op == Op::And)),
                })
        }
        let mut operands = Vec::new();
        terms(a.simplify_boolean(), op, &mut operands);
        terms(b.simplify_boolean(), op, &mut operands);
        if op == Op::Or && operands.len() > 1 {
            let mut branches = operands
                .iter()
                .map(|e| {
                    let mut out = Vec::new();
                    terms(e.clone(), Op::And, &mut out);
                    out
                })
                .collect::<Vec<_>>();
            let common = branches[0]
                .iter()
                .filter(|e| branches.iter().all(|b| b.contains(e)))
                .cloned()
                .collect::<Vec<_>>();
            if !common.is_empty() {
                for branch in &mut branches {
                    branch.retain(|e| !common.contains(e));
                }
                let residual = join(
                    branches.into_iter().map(|b| join(b, Op::And)).collect(),
                    Op::Or,
                );
                let mut all = common;
                all.push(residual);
                return join(all, Op::And);
            }
        }
        join(operands, op)
    }

    pub(crate) fn remap_columns(&mut self, mapping: &[Option<usize>]) -> Result<()> {
        match &mut self.kind {
            Kind::Column(i) => {
                *i = mapping
                    .get(*i)
                    .copied()
                    .flatten()
                    .ok_or("planner removed a required column")?
            }
            Kind::Binary(_, a, b)
            | Kind::Like(a, b, _)
            | Kind::Position(a, b)
            | Kind::NullIf(a, b) => {
                a.remap_columns(mapping)?;
                b.remap_columns(mapping)?;
            }
            Kind::Function(_, a)
            | Kind::Cast(a)
            | Kind::CastDecimal(a, _, _)
            | Kind::Not(a)
            | Kind::Neg(a)
            | Kind::IsNull(a, _)
            | Kind::Year(a)
            | Kind::Substring(a, _, _) => a.remap_columns(mapping)?,
            Kind::Case(branches, other) => {
                for (condition, value) in branches {
                    condition.remap_columns(mapping)?;
                    value.remap_columns(mapping)?;
                }
                other.remap_columns(mapping)?;
            }
            Kind::Literal(_) => {}
        }
        Ok(())
    }

    pub fn columns(&self, set: &mut std::collections::BTreeSet<usize>) {
        match &self.kind {
            Kind::Column(i) => {
                set.insert(*i);
            }
            Kind::Binary(_, a, b)
            | Kind::Like(a, b, _)
            | Kind::Position(a, b)
            | Kind::NullIf(a, b) => {
                a.columns(set);
                b.columns(set);
            }
            Kind::Function(_, a)
            | Kind::Cast(a)
            | Kind::CastDecimal(a, _, _)
            | Kind::Not(a)
            | Kind::Neg(a)
            | Kind::IsNull(a, _)
            | Kind::Year(a)
            | Kind::Substring(a, _, _) => a.columns(set),
            Kind::Case(branches, other) => {
                for (a, b) in branches {
                    a.columns(set);
                    b.columns(set);
                }
                other.columns(set);
            }
            _ => {}
        }
    }
    pub fn eval(&self, input: &Input, budget: &Budget) -> Result<Value> {
        let n = input.len;
        if let Kind::Column(i) = &self.kind {
            return input.columns[*i]
                .clone()
                .ok_or_else(|| "missing projected column".into());
        }
        let extra = match &self.kind {
            Kind::Literal(Literal::Text(s)) => s.len(),
            Kind::Literal(Literal::Binary(bytes)) => {
                n.checked_mul(bytes.len())
                    .filter(|n| *n <= i32::MAX as usize)
                    .ok_or("resource-limit: binary array exceeds 32-bit offsets")?;
                bytes.len()
            }
            _ => 0,
        };
        let lease = budget.reserve(workspace(n, extra)?)?;
        let array: ArrayRef = match &self.kind {
            Kind::NullIf(a, b) => {
                drop(lease);
                return nullif::evaluate(a, b, &self.ty, input, budget);
            }
            Kind::CastDecimal(a, precision, scale) => {
                drop(lease);
                return casts::evaluate_decimal(a, *precision, *scale, input, budget);
            }
            Kind::Cast(a) => {
                drop(lease);
                return casts::evaluate(a, &self.ty, input, budget);
            }
            Kind::Literal(v) => match v {
                Literal::Num(x) => decimal_non_null(vec![*x; n], self.ty.scale()?)?,
                Literal::Bool(x) => Arc::new(BooleanArray::from(vec![*x; n])),
                Literal::Date(x) => Arc::new(Date32Array::from(vec![*x; n])),
                Literal::Text(x) => Arc::new(StringArray::from(vec![x.as_str(); n])),
                Literal::Binary(x) => {
                    // The payload product was checked before reserving above.
                    // Exact preallocation avoids geometric MutableBuffer growth
                    // retaining more bytes than the literal workspace funds.
                    let mut builder = BinaryBuilder::with_capacity(n, n * x.len());
                    for _ in 0..n {
                        builder.append_value(x);
                    }
                    Arc::new(builder.finish())
                }
                Literal::Null => match &self.ty {
                    Type::Int | Type::Decimal(_) => decimal(vec![None; n], self.ty.scale()?)?,
                    _ => new_null_array(&self.ty.arrow(), n),
                },
            },
            Kind::Not(a) => {
                let v = a.eval(input, budget)?;
                Arc::new(BooleanArray::from_iter(
                    boolean(&v.array).iter().map(|v| v.map(|x| !x)),
                ))
            }
            Kind::IsNull(a, neg) => {
                let v = a.eval(input, budget)?;
                Arc::new(BooleanArray::from_iter(
                    (0..n).map(|i| Some(is_sql_null(v.array.as_ref(), i) != *neg)),
                ))
            }
            Kind::Neg(a) => {
                let v = a.eval(input, budget)?;
                if a.ty == Type::Null {
                    return Ok(Value::allocated(
                        new_null_array(&internal_type(&self.ty), n),
                        lease,
                    ));
                }
                if a.ty == Type::Float {
                    return Ok(Value::allocated(
                        Arc::new(Float64Array::from_iter(
                            to_float(&v.array, &a.ty)?.iter().map(|v| v.map(|v| -v)),
                        )),
                        lease,
                    ));
                }
                let values = numeric(&v.array)
                    .iter()
                    .map(|x| x.map(|x| checked(x.checked_neg())).transpose())
                    .collect::<Result<Vec<_>>>()?;
                decimal(values, self.ty.scale()?)?
            }
            Kind::Binary(op, a, b) => {
                if *op == Op::Or
                    && let Some(array) = string_membership(self, input, budget)?
                {
                    return Ok(Value::allocated(array, lease));
                }
                if let Some(array) = scalar_comparison(*op, a, b, input, budget)? {
                    return Ok(Value::allocated(array, lease));
                }
                if let Some(array) = scalar_arithmetic(*op, a, b, &self.ty, input, budget)? {
                    return Ok(Value::allocated(array, lease));
                }
                let av = a.eval(input, budget)?;
                let bv = b.eval(input, budget)?;
                if a.ty == Type::Null && b.ty == Type::Null {
                    return Ok(Value::allocated(
                        new_null_array(&internal_type(&self.ty), n),
                        lease,
                    ));
                }
                if *op == Op::Mod {
                    return Ok(Value::allocated(
                        remainder::evaluate(&av.array, &a.ty, &bv.array, &b.ty, &self.ty)?,
                        lease,
                    ));
                }
                if self.ty == Type::Float && matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div) {
                    let aa = to_float(&av.array, &a.ty)?;
                    let bb = to_float(&bv.array, &b.ty)?;
                    let out = aa
                        .iter()
                        .zip(bb.iter())
                        .map(|(x, y)| match (x, y) {
                            (Some(x), Some(y)) => {
                                if *op == Op::Div && y == 0.0 {
                                    // The approved SQLite change covers Int/Float.
                                    // Preserve the separate exact DECIMAL contract.
                                    if matches!(a.ty, Type::Decimal(_))
                                        || matches!(b.ty, Type::Decimal(_))
                                    {
                                        return Err("division by zero".into());
                                    }
                                    return Ok(None);
                                }
                                let v = match op {
                                    Op::Add => x + y,
                                    Op::Sub => x - y,
                                    Op::Mul => x * y,
                                    _ => x / y,
                                };
                                if !v.is_finite() {
                                    return Err("floating arithmetic overflow".into());
                                }
                                Ok(Some(v))
                            }
                            _ => Ok(None),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(Value::allocated(Arc::new(Float64Array::from(out)), lease));
                }
                if matches!(op, Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge)
                    && (a.ty == Type::Float || b.ty == Type::Float)
                {
                    if matches!(
                        (&a.ty, &b.ty),
                        (Type::Int, Type::Float) | (Type::Float, Type::Int)
                    ) {
                        return Ok(Value::allocated(
                            numeric_compare::compare(*op, &av, &bv, a.ty == Type::Int),
                            lease,
                        ));
                    }
                    let aa = to_float(&av.array, &a.ty)?;
                    let bb = to_float(&bv.array, &b.ty)?;
                    let out = aa.iter().zip(bb.iter()).map(|(x, y)| match (x, y) {
                        (Some(x), Some(y)) => Some(match op {
                            Op::Eq => x == y,
                            Op::Ne => x != y,
                            Op::Lt => x < y,
                            Op::Le => x <= y,
                            Op::Gt => x > y,
                            _ => x >= y,
                        }),
                        _ => None,
                    });
                    return Ok(Value::allocated(
                        Arc::new(BooleanArray::from_iter(out)),
                        lease,
                    ));
                }
                match op {
                    // Float/Decimal division has already returned above. The
                    // binder admits this remaining division only for Int/Int.
                    Op::Div => integer_division(&av.array, &bv.array)?,
                    Op::Mod => unreachable!("remainder returned above"),
                    Op::And | Op::Or => {
                        let aa = boolean(&av.array);
                        let bb = boolean(&bv.array);
                        Arc::new(
                            if *op == Op::And {
                                arrow::compute::and_kleene(aa, bb)
                            } else {
                                arrow::compute::or_kleene(aa, bb)
                            }
                            .map_err(|e| e.to_string())?,
                        )
                    }
                    Op::Add | Op::Sub | Op::Mul => {
                        let sa = a.ty.scale()?;
                        let sb = b.ty.scale()?;
                        let scale = self.ty.scale()?;
                        let (af, bf) = if *op == Op::Mul {
                            (1, 1)
                        } else {
                            (factor(scale - sa)?, factor(scale - sb)?)
                        };
                        let aa = numeric(&av.array);
                        let bb = numeric(&bv.array);
                        let calculate = |x: i128, y: i128| -> Result<i128> {
                            if *op == Op::Mul {
                                multiply(x, y)
                            } else {
                                add_sub(x, y, af, bf, *op == Op::Sub)
                            }
                        };
                        if aa.null_count() == 0 && bb.null_count() == 0 {
                            if n >= 64
                                && let Some(values) =
                                    narrow_batch_arithmetic(aa.values(), bb.values(), *op, af, bf)
                            {
                                return Ok(Value::allocated(
                                    decimal_non_null(values, scale)?,
                                    lease,
                                ));
                            }
                            // Avoid Option<i128> temporaries and validity-building
                            // passes when the input batches have no NULL values.
                            let mut values = Vec::with_capacity(n);
                            for (x, y) in aa.values().iter().zip(bb.values()) {
                                values.push(calculate(*x, *y)?);
                            }
                            decimal_non_null(values, scale)?
                        } else {
                            let mut values = Vec::with_capacity(n);
                            for (x, y) in aa.iter().zip(bb.iter()) {
                                values.push(match (x, y) {
                                    (Some(x), Some(y)) => Some(calculate(x, y)?),
                                    _ => None,
                                });
                            }
                            decimal(values, scale)?
                        }
                    }
                    _ => {
                        // Compare whole typed arrays when scales already agree.
                        // This avoids checked decimal rescaling on every row of
                        // the common scan predicates; mixed scales retain the
                        // exact checked path below.
                        if a.ty == b.ty && a.ty != Type::Null {
                            use arrow::compute::kernels::cmp;
                            let left = av.array.as_ref();
                            let right = bv.array.as_ref();
                            let values = match op {
                                Op::Eq => cmp::eq(&left, &right),
                                Op::Ne => cmp::neq(&left, &right),
                                Op::Lt => cmp::lt(&left, &right),
                                Op::Le => cmp::lt_eq(&left, &right),
                                Op::Gt => cmp::gt(&left, &right),
                                Op::Ge => cmp::gt_eq(&left, &right),
                                _ => unreachable!(),
                            }
                            .map_err(|e| e.to_string())?;
                            return Ok(Value::allocated(Arc::new(values), lease));
                        }
                        if let (Ok(sa), Ok(sb)) = (a.ty.scale(), b.ty.scale()) {
                            let scale = sa.max(sb);
                            let af = factor(scale - sa)?;
                            let bf = factor(scale - sb)?;
                            let aa = numeric(&av.array);
                            let bb = numeric(&bv.array);
                            let values = aa
                                .iter()
                                .zip(bb.iter())
                                .map(|(x, y)| match (x, y) {
                                    (Some(x), Some(y)) => {
                                        let x = checked(x.checked_mul(af))?;
                                        let y = checked(y.checked_mul(bf))?;
                                        Ok(Some(match op {
                                            Op::Eq => x == y,
                                            Op::Ne => x != y,
                                            Op::Lt => x < y,
                                            Op::Le => x <= y,
                                            Op::Gt => x > y,
                                            Op::Ge => x >= y,
                                            _ => unreachable!(),
                                        }))
                                    }
                                    _ => Ok(None),
                                })
                                .collect::<Result<Vec<_>>>()?;
                            return Ok(Value::allocated(
                                Arc::new(BooleanArray::from(values)),
                                lease,
                            ));
                        }
                        let values = (0..n)
                            .map(|i| {
                                if is_sql_null(av.array.as_ref(), i)
                                    || is_sql_null(bv.array.as_ref(), i)
                                {
                                    return Ok(None);
                                }
                                let cmp = match &a.ty {
                                    Type::Int | Type::Decimal(_) => {
                                        let scale = a.ty.scale()?.max(b.ty.scale()?);
                                        rescale(numeric(&av.array).value(i), a.ty.scale()?, scale)?
                                            .cmp(&rescale(
                                                numeric(&bv.array).value(i),
                                                b.ty.scale()?,
                                                scale,
                                            )?)
                                    }
                                    Type::Date => av
                                        .array
                                        .as_any()
                                        .downcast_ref::<Date32Array>()
                                        .unwrap()
                                        .value(i)
                                        .cmp(
                                            &bv.array
                                                .as_any()
                                                .downcast_ref::<Date32Array>()
                                                .unwrap()
                                                .value(i),
                                        ),
                                    Type::Text => av
                                        .array
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap()
                                        .value(i)
                                        .cmp(
                                            bv.array
                                                .as_any()
                                                .downcast_ref::<StringArray>()
                                                .unwrap()
                                                .value(i),
                                        ),
                                    Type::Bool => boolean(&av.array)
                                        .value(i)
                                        .cmp(&boolean(&bv.array).value(i)),
                                    _ => return Err("unsupported comparison type".into()),
                                };
                                Ok(Some(match op {
                                    Op::Eq => cmp.is_eq(),
                                    Op::Ne => !cmp.is_eq(),
                                    Op::Lt => cmp.is_lt(),
                                    Op::Le => !cmp.is_gt(),
                                    Op::Gt => cmp.is_gt(),
                                    Op::Ge => !cmp.is_lt(),
                                    _ => unreachable!(),
                                }))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Arc::new(BooleanArray::from(values))
                    }
                }
            }
            Kind::Function(name, a) => {
                let v = a.eval(input, budget)?;
                match name.as_str() {
                    "length" => decimal(
                        v.array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .map(|v| v.map(|s| s.chars().count() as i128))
                            .collect(),
                        0,
                    )?,
                    "abs" if a.ty == Type::Float => Arc::new(Float64Array::from_iter(
                        to_float(&v.array, &a.ty)?
                            .iter()
                            .map(|v| v.map(|x| if x < 0.0 { -x } else { x })),
                    )),
                    "abs" => decimal(
                        numeric(&v.array)
                            .iter()
                            .map(|v| v.map(|v| checked(v.checked_abs())).transpose())
                            .collect::<Result<Vec<_>>>()?,
                        self.ty.scale()?,
                    )?,
                    "factorial" => decimal(
                        numeric(&v.array)
                            .iter()
                            .map(|v| {
                                v.map(|v| {
                                    if !(0..=20).contains(&v) {
                                        return Err("FACTORIAL argument outside 0..20".into());
                                    }
                                    Ok((1..=v).product())
                                })
                                .transpose()
                            })
                            .collect::<Result<Vec<_>>>()?,
                        0,
                    )?,
                    _ => unreachable!(),
                }
            }
            Kind::Position(a, b) => {
                let a = a.eval(input, budget)?;
                let b = b.eval(input, budget)?;
                let a = a.array.as_any().downcast_ref::<StringArray>().unwrap();
                let b = b.array.as_any().downcast_ref::<StringArray>().unwrap();
                decimal(
                    a.iter()
                        .zip(b.iter())
                        .map(|(needle, text)| {
                            needle.zip(text).map(|(needle, text)| {
                                text.find(needle)
                                    .map(|i| text[..i].chars().count() as i128 + 1)
                                    .unwrap_or(0)
                            })
                        })
                        .collect(),
                    0,
                )?
            }
            Kind::Like(a, b, negated) => {
                let a = a.eval(input, budget)?;
                let aa = a.array.as_any().downcast_ref::<StringArray>().unwrap();
                if let Kind::Literal(Literal::Text(pattern)) = &b.kind {
                    let pattern = LikePattern::new(pattern);
                    Arc::new(BooleanArray::from_iter(
                        aa.iter().map(|s| s.map(|s| pattern.matches(s) != *negated)),
                    ))
                } else {
                    let b = b.eval(input, budget)?;
                    let bb = b.array.as_any().downcast_ref::<StringArray>().unwrap();
                    Arc::new(BooleanArray::from_iter(
                        aa.iter()
                            .zip(bb.iter())
                            .map(|(s, p)| s.zip(p).map(|(s, p)| like(s, p) != *negated)),
                    ))
                }
            }
            Kind::Year(a) => {
                use chrono::Datelike;
                let a = a.eval(input, budget)?;
                let aa = a.array.as_any().downcast_ref::<Date32Array>().unwrap();
                let origin = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                decimal(
                    aa.iter()
                        .map(|v| {
                            v.map(|d| {
                                origin
                                    .checked_add_signed(chrono::Duration::days(i64::from(d)))
                                    .map(|d| i128::from(d.year()))
                                    .ok_or_else(|| "date out of range".to_string())
                            })
                            .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?,
                    0,
                )?
            }
            Kind::Substring(a, start, length) => {
                let a = a.eval(input, budget)?;
                let aa = a.array.as_any().downcast_ref::<StringArray>().unwrap();
                Arc::new(StringArray::from_iter(
                    aa.iter().map(|s| s.map(|s| substring(s, *start, *length))),
                ))
            }
            Kind::Case(branches, other) => {
                // Evaluate only selected rows in each arm (including ELSE).
                // Unreachable errors such as division by zero must stay unevaluated.
                if self.ty == Type::Binary {
                    return binary_case(branches, other, input, budget);
                }
                let mut remaining = (0..n as u32).collect::<Vec<_>>();
                let mut pieces = Vec::new();
                let mut positions = vec![0_u32; n];
                let mut offset = 0;
                for (condition, value) in branches {
                    if remaining.is_empty() {
                        break;
                    }
                    let subset = take_input(input, &remaining, budget)?;
                    let mask = condition.eval(&subset, budget)?;
                    let mut selected = Vec::new();
                    let mut next = Vec::new();
                    for (i, row) in remaining.iter().enumerate() {
                        if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i) {
                            selected.push(*row);
                        } else {
                            next.push(*row);
                        }
                    }
                    append_case(
                        value,
                        &selected,
                        input,
                        budget,
                        &self.ty,
                        &mut pieces,
                        &mut positions,
                        &mut offset,
                    )?;
                    remaining = next;
                }
                append_case(
                    other,
                    &remaining,
                    input,
                    budget,
                    &self.ty,
                    &mut pieces,
                    &mut positions,
                    &mut offset,
                )?;
                if pieces.is_empty() {
                    new_null_array(&internal_type(&self.ty), n)
                } else {
                    let refs = pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                    let merged = arrow::compute::concat(&refs).map_err(|e| e.to_string())?;
                    arrow::compute::take(&merged, &UInt32Array::from(positions), None)
                        .map_err(|e| e.to_string())?
                }
            }
            Kind::Column(_) => unreachable!(),
        };
        Ok(Value::allocated(array, lease))
    }
}
fn widen_integer<T: arrow::datatypes::ArrowPrimitiveType>(
    a: &PrimitiveArray<T>,
    budget: &Budget,
) -> Result<Value>
where
    T::Native: Into<i128>,
{
    let bytes = a
        .len()
        .checked_mul(16)
        .and_then(|n| n.checked_add(a.nulls().map_or(0, |n| n.buffer().capacity())))
        .and_then(|n| n.checked_add(256))
        .ok_or("integer conversion size overflow")?;
    let lease = budget.reserve(bytes)?;
    // Every i32/i64 fits precision 38. Widen the physical values exactly and
    // share sliced validity; no rescaling or per-value precision test is needed.
    let values = a
        .values()
        .iter()
        .copied()
        .map(Into::into)
        .collect::<Vec<i128>>();
    let array = Decimal128Array::new(values.into(), a.nulls().cloned())
        .with_precision_and_scale(38, 0)
        .map_err(|e| e.to_string())?;
    Ok(Value::allocated(Arc::new(array), lease))
}

pub fn normalize_input(a: &ArrayRef, ty: &Type, budget: &Budget) -> Result<Value> {
    if *ty == Type::Int {
        if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
            return widen_integer(a, budget);
        }
        if let Some(a) = a.as_any().downcast_ref::<Int32Array>() {
            return widen_integer(a, budget);
        }
    }
    let a = match ty {
        Type::Decimal(scale) => Arc::new(
            numeric(a)
                .clone()
                .with_precision_and_scale(38, *scale)
                .map_err(|e| e.to_string())?,
        ) as ArrayRef,
        Type::Int => {
            arrow::compute::cast(a, &DataType::Decimal128(38, 0)).map_err(|e| e.to_string())?
        }
        _ => a.clone(),
    };
    Value::adopt(a, budget)
}

fn internal_type(t: &Type) -> DataType {
    if *t == Type::Int {
        DataType::Decimal128(38, 0)
    } else {
        t.arrow()
    }
}
pub(crate) fn to_float(a: &ArrayRef, t: &Type) -> Result<Float64Array> {
    if *t == Type::Float {
        return Ok(a.as_any().downcast_ref::<Float64Array>().unwrap().clone());
    }
    if *t == Type::Null {
        return Ok(Float64Array::from(vec![None; a.len()]));
    }
    let divisor = factor(t.scale()?)?;
    let mut builder = Float64Builder::with_capacity(a.len());
    for value in numeric(a) {
        builder.append_option(
            value
                .map(|n| crate::numeric_float::scaled_by(n, divisor))
                .transpose()?,
        );
    }
    Ok(builder.finish())
}
fn take_input(input: &Input, rows: &[u32], budget: &Budget) -> Result<Input> {
    let indices = UInt32Array::from(rows.to_vec());
    Ok(Input {
        len: rows.len(),
        columns: input
            .columns
            .iter()
            .map(|c| {
                c.as_ref()
                    .map(|v| {
                        if v.array.data_type() == &DataType::Binary {
                            // CASE subsets contain each original row at most once.
                            let mut lease = budget.reserve(
                                v.array
                                    .get_array_memory_size()
                                    .checked_mul(2)
                                    .and_then(|bytes| {
                                        bytes.checked_add(rows.len().saturating_mul(16))
                                    })
                                    .ok_or("binary selection size overflow")?,
                            )?;
                            let array = arrow::compute::take(&v.array, &indices, None)
                                .map_err(|e| e.to_string())?;
                            lease.release_excess(array.get_array_memory_size());
                            return Ok(Value::allocated(array, lease));
                        }
                        Value::adopt(
                            arrow::compute::take(&v.array, &indices, None)
                                .map_err(|e| e.to_string())?,
                            budget,
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?,
    })
}
/// Retain each selected arm's Value owner through concat/take. Binary payloads
/// have no fixed per-row upper bound, so reserve their actual total separately.
fn binary_case(
    branches: &[(Expr, Expr)],
    other: &Expr,
    input: &Input,
    budget: &Budget,
) -> Result<Value> {
    let n = input.len;
    let mut remaining = (0..n as u32).collect::<Vec<_>>();
    let mut pieces = Vec::<Value>::new();
    let mut positions = vec![0_u32; n];
    let mut offset = 0;
    let mut append = |expr: &Expr, rows: &[u32]| -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let subset = take_input(input, rows, budget)?;
        let value = expr.eval(&subset, budget)?;
        let value = if expr.ty == Type::Null {
            let lease = budget.reserve(rows.len().saturating_mul(8).saturating_add(256))?;
            Value::allocated(new_null_array(&DataType::Binary, rows.len()), lease)
        } else {
            value
        };
        for row in rows {
            positions[*row as usize] = offset;
            offset += 1;
        }
        pieces.push(value);
        Ok(())
    };
    for (condition, value) in branches {
        if remaining.is_empty() {
            break;
        }
        let subset = take_input(input, &remaining, budget)?;
        let mask = condition.eval(&subset, budget)?;
        let mut selected = Vec::new();
        let mut next = Vec::new();
        for (i, row) in remaining.iter().enumerate() {
            if !is_sql_null(mask.array.as_ref(), i) && boolean(&mask.array).value(i) {
                selected.push(*row);
            } else {
                next.push(*row);
            }
        }
        append(value, &selected)?;
        remaining = next;
    }
    append(other, &remaining)?;
    let payload = pieces.iter().try_fold(0usize, |bytes, value| {
        let array = value
            .array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or("invalid binary CASE arm")?;
        array.iter().flatten().try_fold(bytes, |bytes, value| {
            bytes
                .checked_add(value.len())
                .filter(|n| *n <= i32::MAX as usize)
                .ok_or("resource-limit: binary array exceeds 32-bit offsets")
        })
    })?;
    let mut lease = budget.reserve(
        payload
            .checked_mul(3)
            .and_then(|bytes| {
                n.checked_mul(32)
                    .and_then(|metadata| bytes.checked_add(metadata))
            })
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or("binary CASE output size overflow")?,
    )?;
    let array = if pieces.is_empty() {
        new_null_array(&DataType::Binary, n)
    } else {
        let refs = pieces
            .iter()
            .map(|value| value.array.as_ref())
            .collect::<Vec<_>>();
        let merged = arrow::compute::concat(&refs).map_err(|e| e.to_string())?;
        arrow::compute::take(&merged, &UInt32Array::from(positions), None)
            .map_err(|e| e.to_string())?
    };
    lease.release_excess(array.get_array_memory_size());
    Ok(Value::allocated(array, lease))
}
#[allow(clippy::too_many_arguments)]
fn append_case(
    e: &Expr,
    rows: &[u32],
    input: &Input,
    budget: &Budget,
    ty: &Type,
    pieces: &mut Vec<ArrayRef>,
    positions: &mut [u32],
    offset: &mut u32,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let input = take_input(input, rows, budget)?;
    let v = e.eval(&input, budget)?;
    let a = if e.ty == Type::Null {
        new_null_array(&internal_type(ty), rows.len())
    } else if *ty == Type::Float {
        Arc::new(to_float(&v.array, &e.ty)?) as ArrayRef
    } else if ty.scale().is_ok() {
        let scale = ty.scale()?;
        decimal(
            numeric(&v.array)
                .iter()
                .map(|n| {
                    n.map(|n| rescale(n, e.ty.scale().unwrap(), scale))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?,
            scale,
        )?
    } else {
        v.array.clone()
    };
    for row in rows {
        positions[*row as usize] = *offset;
        *offset += 1;
    }
    pieces.push(a);
    Ok(())
}
// Borrow a character-aligned slice and let the Arrow builder copy it once.
// This avoids a separate String allocation for every row, including NULLs.
fn substring(text: &str, start: usize, length: usize) -> &str {
    if length == 0 {
        return "";
    }
    let mut chars = text.char_indices();
    let Some((begin, _)) = chars.nth(start) else {
        return "";
    };
    let end = chars.nth(length - 1).map_or(text.len(), |(i, _)| i);
    &text[begin..end]
}

/// SQL LIKE over Unicode scalar values, with % and _ wildcards.
fn like(text: &str, pattern: &str) -> bool {
    let s = text.chars().collect::<Vec<_>>();
    let p = pattern.chars().collect::<Vec<_>>();
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while i < s.len() {
        if j < p.len() && (p[j] == '_' || p[j] == s[i]) {
            i += 1;
            j += 1;
        } else if j < p.len() && p[j] == '%' {
            star = Some(j);
            j += 1;
            retry = i;
        } else if let Some(k) = star {
            retry += 1;
            i = retry;
            j = k + 1;
        } else {
            return false;
        }
    }
    while j < p.len() && p[j] == '%' {
        j += 1;
    }
    j == p.len()
}

// Broadcast comparison constants without constructing a batch-sized literal
// array. Failed literal rescaling falls back to the nullable checked path:
// an all-NULL column must not raise an otherwise unobserved overflow.
fn scalar_comparison(
    op: Op,
    a: &Expr,
    b: &Expr,
    input: &Input,
    budget: &Budget,
) -> Result<Option<ArrayRef>> {
    if !matches!(op, Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge) {
        return Ok(None);
    }
    let (literal, column, reversed) = match (&a.kind, &b.kind) {
        (Kind::Literal(_), Kind::Literal(_)) => return Ok(None),
        (Kind::Literal(_), _) => (a, b, true),
        (_, Kind::Literal(_)) => (b, a, false),
        _ => return Ok(None),
    };
    if matches!(column.ty, Type::Float | Type::Null) {
        return Ok(None);
    }
    let mut literal = literal.clone();
    if literal.ty != column.ty {
        match (&literal.kind, literal.ty.scale(), column.ty.scale()) {
            (Kind::Literal(Literal::Num(n)), Ok(old), Ok(new)) if new >= old => {
                let Ok(n) = rescale(*n, old, new) else {
                    return Ok(None);
                };
                literal.kind = Kind::Literal(Literal::Num(n));
                literal.ty = column.ty.clone();
            }
            _ => return Ok(None),
        }
    }
    let constant = literal.eval(
        &Input {
            columns: vec![],
            len: 1,
        },
        budget,
    )?;
    let values = column.eval(input, budget)?;
    let scalar = Scalar::new(constant.array);
    let values = values.array.as_ref();
    let (left, right): (&dyn Datum, &dyn Datum) = if reversed {
        (&scalar, &values)
    } else {
        (&values, &scalar)
    };
    use arrow::compute::kernels::cmp;
    let result = match op {
        Op::Eq => cmp::eq(left, right),
        Op::Ne => cmp::neq(left, right),
        Op::Lt => cmp::lt(left, right),
        Op::Le => cmp::lt_eq(left, right),
        Op::Gt => cmp::gt(left, right),
        Op::Ge => cmp::gt_eq(left, right),
        _ => unreachable!(),
    }
    .map_err(|e| e.to_string())?;
    Ok(Some(Arc::new(result)))
}

// Broadcast direct numeric literals without a batch-sized constant buffer.
// Both operands are still evaluated in their original order. NULL literals,
// floating arithmetic and division retain the ordinary path.
fn scalar_arithmetic(
    op: Op,
    a: &Expr,
    b: &Expr,
    output: &Type,
    input: &Input,
    budget: &Budget,
) -> Result<Option<ArrayRef>> {
    if !matches!(op, Op::Add | Op::Sub | Op::Mul)
        || !matches!(a.ty, Type::Int | Type::Decimal(_))
        || !matches!(b.ty, Type::Int | Type::Decimal(_))
        || !matches!(output, Type::Int | Type::Decimal(_))
    {
        return Ok(None);
    }
    let (literal, expression, reversed) = match (&a.kind, &b.kind) {
        (Kind::Literal(_), Kind::Literal(_)) => return Ok(None),
        (Kind::Literal(Literal::Num(_)), _) => (a, b, true),
        (_, Kind::Literal(Literal::Num(_))) => (b, a, false),
        _ => return Ok(None),
    };
    let scalar_input = Input {
        columns: vec![],
        len: 1,
    };
    let (constant, values) = if reversed {
        (
            literal.eval(&scalar_input, budget)?,
            expression.eval(input, budget)?,
        )
    } else {
        let values = expression.eval(input, budget)?;
        (literal.eval(&scalar_input, budget)?, values)
    };
    let constant = numeric(&constant.array).value(0);
    let values = numeric(&values.array);
    let scale = output.scale()?;
    let (vf, cf) = if op == Op::Mul {
        (1, 1)
    } else {
        (
            factor(scale - expression.ty.scale()?)?,
            factor(scale - literal.ty.scale()?)?,
        )
    };
    let calculate = |v| {
        if op == Op::Mul {
            multiply(v, constant)
        } else if reversed {
            add_sub(constant, v, cf, vf, op == Op::Sub)
        } else {
            add_sub(v, constant, vf, cf, op == Op::Sub)
        }
    };
    if values.null_count() == 0 {
        if input.len >= 64 {
            let vmax = values
                .values()
                .iter()
                .map(|v| v.unsigned_abs())
                .max()
                .unwrap_or(0);
            if let Some((vf, cf)) = narrow_factors(vmax, constant.unsigned_abs(), op, vf, cf) {
                let c = constant as i64;
                let scaled = if op == Op::Mul { c } else { c * cf };
                let result = match op {
                    Op::Mul => values
                        .values()
                        .iter()
                        .map(|v| i128::from((*v as i64) * c))
                        .collect(),
                    Op::Add => values
                        .values()
                        .iter()
                        .map(|v| i128::from((*v as i64) * vf + scaled))
                        .collect(),
                    Op::Sub if reversed => values
                        .values()
                        .iter()
                        .map(|v| i128::from(scaled - (*v as i64) * vf))
                        .collect(),
                    Op::Sub => values
                        .values()
                        .iter()
                        .map(|v| i128::from((*v as i64) * vf - scaled))
                        .collect(),
                    _ => unreachable!(),
                };
                return decimal_non_null(result, scale).map(Some);
            }
        }
        let mut result = Vec::with_capacity(input.len);
        for v in values.values() {
            result.push(calculate(*v)?);
        }
        decimal_non_null(result, scale).map(Some)
    } else {
        let mut result = Vec::with_capacity(input.len);
        for v in values.iter() {
            result.push(v.map(calculate).transpose()?);
        }
        decimal(result, scale).map(Some)
    }
}

// Recognize a complete OR of equalities to non-NULL string constants. The
// expression may be computed, but must be identical in every branch. Current
// scalar expressions are deterministic and pure; evaluate it once without
// skipping its errors. Mixed predicates and NULL constants keep Kleene OR.
fn string_terms<'a>(
    expr: &'a Expr,
    common: &mut Option<&'a Expr>,
    visit: &mut impl FnMut(&'a str),
) -> bool {
    match &expr.kind {
        Kind::Binary(Op::Or, a, b) => {
            string_terms(a, common, visit) && string_terms(b, common, visit)
        }
        Kind::Literal(Literal::Bool(false)) => true,
        Kind::Binary(Op::Eq, a, b) if a.ty == Type::Text && b.ty == Type::Text => {
            let (value, literal) = match (&a.kind, &b.kind) {
                (Kind::Literal(_), Kind::Literal(_)) => return false,
                (Kind::Literal(Literal::Text(s)), _) => (b.as_ref(), s),
                (_, Kind::Literal(Literal::Text(s))) => (a.as_ref(), s),
                _ => return false,
            };
            if common.is_some_and(|old| old != value) {
                return false;
            }
            *common = Some(value);
            visit(literal);
            true
        }
        _ => false,
    }
}
fn string_membership_mask(values: &StringArray, mut member: impl FnMut(&str) -> bool) -> ArrayRef {
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    let (bits, nulls) = if values.null_count() == 0 {
        (
            BooleanBuffer::collect_bool(values.len(), |i| member(values.value(i))),
            None,
        )
    } else {
        let bits = BooleanBuffer::collect_bool(values.len(), |i| {
            values.is_valid(i) && member(values.value(i))
        });
        // Copy the sliced validity into bounded output storage. Cloning the
        // original bitmap could retain a much larger parent allocation after
        // its input reservation has ended. The caller reserves output first.
        let valid = BooleanBuffer::collect_bool(values.len(), |i| values.is_valid(i));
        (bits, Some(NullBuffer::new(valid)))
    };
    Arc::new(BooleanArray::new(bits, nulls))
}
fn string_membership(expr: &Expr, input: &Input, budget: &Budget) -> Result<Option<ArrayRef>> {
    let mut common = None;
    let mut count = 0_usize;
    if !string_terms(expr, &mut common, &mut |_| count += 1) || count < 2 {
        return Ok(None);
    }
    let values = common.unwrap().eval(input, budget)?;
    let values = values
        .array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("bound string expression");
    // Four borrowed string slices fit in 64 bytes on this 64-bit target.
    // Small lists avoid hash setup/probing while still evaluating the shared
    // expression once. Duplicates are harmless in either representation.
    if count <= 4 {
        let mut members = [""; 4];
        let mut len = 0;
        let valid = string_terms(expr, &mut None, &mut |s| {
            members[len] = s;
            len += 1;
        });
        debug_assert!(valid && len == count);
        return Ok(Some(string_membership_mask(values, |s| {
            members[..len].contains(&s)
        })));
    }
    let _lease = budget.reserve(
        count
            .checked_mul(64)
            .and_then(|n| n.checked_add(256))
            .ok_or("string membership size overflow")?,
    )?;
    // Borrow literals from the immutable expression; all state is batch-local.
    let mut members =
        std::collections::HashSet::with_capacity_and_hasher(count, ahash::RandomState::new());
    let valid = string_terms(expr, &mut None, &mut |s| {
        members.insert(s);
    });
    debug_assert!(valid);
    Ok(Some(string_membership_mask(values, |s| {
        members.contains(s)
    })))
}

// Patterns containing only percent wildcards can search UTF-8 byte slices;
// literals always end on character boundaries. Underscore retains the Unicode
// character matcher. Compile once per batch, not once per input row.
struct LikePattern<'a> {
    pattern: &'a str,
    parts: Vec<&'a str>,
    searches: Vec<memchr::memmem::Finder<'a>>,
    has_underscore: bool,
}
impl<'a> LikePattern<'a> {
    fn new(pattern: &'a str) -> Self {
        let parts: Vec<_> = pattern.split('%').collect();
        let has_underscore = pattern.contains('_');
        let searches = if !has_underscore && parts.len() > 2 {
            parts[1..parts.len() - 1]
                .iter()
                .filter(|p| !p.is_empty())
                .map(|p| memchr::memmem::Finder::new(p.as_bytes()))
                .collect()
        } else {
            Vec::new()
        };
        Self {
            pattern,
            parts,
            searches,
            has_underscore,
        }
    }
    fn matches(&self, text: &str) -> bool {
        if self.has_underscore {
            return like(text, self.pattern);
        }
        if self.parts.len() == 1 {
            return text == self.pattern;
        }
        let Some(rest) = text.strip_prefix(self.parts[0]) else {
            return false;
        };
        let Some(mut rest) = rest.strip_suffix(self.parts[self.parts.len() - 1]) else {
            return false;
        };
        for search in &self.searches {
            let Some(index) = search.find(rest.as_bytes()) else {
                return false;
            };
            // Both operands are valid UTF-8, so a literal byte match starts and
            // ends on character boundaries, even when the haystack is Unicode.
            rest = &rest[index + search.needle().len()..];
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_arithmetic_bounds_preserve_checked_values_and_fallbacks() {
        let limit = 10_i128.pow(38);
        let values = [
            i128::MIN,
            -limit,
            -limit + 1,
            i64::MIN as i128,
            -9_000_000_000_000_000_000,
            -3,
            -1,
            0,
            1,
            3,
            9_000_000_000_000_000_000,
            i64::MAX as i128,
            limit - 1,
            limit,
            i128::MAX,
        ];
        for op in [Op::Add, Op::Sub, Op::Mul] {
            for x in values {
                for y in values {
                    for (af, bf) in [
                        (1, 1),
                        (100, 1),
                        (1, 100),
                        (1_000_000_000_000_000_000, 1),
                        (10_i128.pow(19), 1),
                    ] {
                        let result = narrow_batch_arithmetic(&[0, x], &[0, y], op, af, bf);
                        let expected = if op == Op::Mul {
                            checked(x.checked_mul(y))
                        } else {
                            checked(x.checked_mul(af)).and_then(|a| {
                                checked(y.checked_mul(bf)).and_then(|b| {
                                    checked(if op == Op::Add {
                                        a.checked_add(b)
                                    } else {
                                        a.checked_sub(b)
                                    })
                                })
                            })
                        };
                        if let Some(result) = result {
                            assert_eq!(result, vec![0, expected.unwrap()]);
                        }
                    }
                }
            }
            let a: Vec<_> = (0..8192).map(|i| (i % 197) - 98).collect();
            let b: Vec<_> = (0..8192).map(|i| (i % 89) - 44).collect();
            let actual = narrow_batch_arithmetic(&a, &b, op, 100, 1).unwrap();
            let expected: Vec<_> = a
                .iter()
                .zip(&b)
                .map(|(x, y)| match op {
                    Op::Add => x * 100 + y,
                    Op::Sub => x * 100 - y,
                    _ => x * y,
                })
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(narrow_batch_arithmetic(&[], &[], op, 1, 1), Some(vec![]));
        }
        assert!(
            narrow_batch_arithmetic(
                &[9_000_000_000_000_000_000],
                &[-9_000_000_000_000_000_000],
                Op::Add,
                1,
                1
            )
            .is_none()
        );
        assert!(narrow_batch_arithmetic(&[1], &[1], Op::Add, -1, 1).is_none());
    }

    #[test]
    fn direct_integer_widening_matches_cast_for_extremes_nulls_and_slices() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(-1),
                Some(0),
                Some(i64::MAX),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(-1),
                Some(0),
                Some(i32::MAX),
                None,
            ])),
            Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
            Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
            Arc::new(Int64Array::from(vec![None; 31])),
            Arc::new(Int32Array::from(vec![None; 31])),
            Arc::new(
                Decimal128Array::from(vec![Some(123), None])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ];
        for base in arrays {
            for offset in 0..=base.len() {
                for len in [0, (base.len() - offset) / 2, base.len() - offset] {
                    let a = base.slice(offset, len);
                    let expected = arrow::compute::cast(&a, &DataType::Decimal128(38, 0)).unwrap();
                    let budget = Budget::new(1 << 20);
                    let result = normalize_input(&a, &Type::Int, &budget).unwrap();
                    assert_eq!(result.array.as_ref(), expected.as_ref());
                    assert!(budget.used() >= result.array.get_array_memory_size());
                    if let (Some(before), Some(after)) = (a.nulls(), result.array.nulls()) {
                        assert_eq!(
                            before.inner().values().as_ptr(),
                            after.inner().values().as_ptr()
                        );
                        assert_eq!(before.offset(), after.offset());
                    }
                    drop(result);
                    assert_eq!(budget.used(), 0);
                    let tiny = Budget::new(0);
                    assert!(normalize_input(&a, &Type::Int, &tiny).is_err());
                    assert_eq!(tiny.used(), 0);
                }
            }
        }
    }

    #[test]
    fn borrowed_substrings_match_character_iteration() {
        for text in ["", "abcdef", "é🦀a中", "a\0b", "e\u{301}🙂"] {
            for start in [0, 1, 2, 3, 4, 9, usize::MAX] {
                for length in [0, 1, 2, 3, 4, 9, usize::MAX] {
                    let expected: String = text.chars().skip(start).take(length).collect();
                    assert_eq!(substring(text, start, length), expected);
                }
            }
        }
    }

    #[test]
    fn scaled_narrow_add_sub_preserves_all_intermediate_overflow_checks() {
        let limit = 10_i128.pow(38);
        let values = [
            i128::MIN,
            -limit,
            -limit + 1,
            i128::from(i64::MIN) - 1,
            i128::from(i64::MIN),
            -1,
            0,
            1,
            i128::from(i64::MAX),
            i128::from(i64::MAX) + 1,
            limit - 1,
            limit,
            i128::MAX,
        ];
        let factors = [
            -1,
            0,
            1,
            100,
            10_i128.pow(18),
            10_i128.pow(18) + 1,
            10_i128.pow(19),
            10_i128.pow(37),
            10_i128.pow(38),
        ];
        for x in values {
            for y in values {
                for af in factors {
                    for bf in factors {
                        for subtract in [false, true] {
                            let expected = (|| {
                                let x = checked(x.checked_mul(af))?;
                                let y = checked(y.checked_mul(bf))?;
                                checked(if subtract {
                                    x.checked_sub(y)
                                } else {
                                    x.checked_add(y)
                                })
                            })();
                            assert_eq!(
                                add_sub(x, y, af, bf, subtract),
                                expected,
                                "{x}, {y}, {af}, {bf}, {subtract}"
                            );
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn narrow_multiply_matches_checked_precision38_at_signed_boundaries() {
        let limit = 10_i128.pow(38);
        let values = [
            i128::MIN,
            i128::MIN + 1,
            -limit,
            -limit + 1,
            i128::from(i64::MIN) - 1,
            i128::from(i64::MIN),
            i128::from(i64::MIN) + 1,
            -1,
            0,
            1,
            i128::from(i64::MAX) - 1,
            i128::from(i64::MAX),
            i128::from(i64::MAX) + 1,
            limit - 1,
            limit,
            i128::MAX,
        ];
        for x in values {
            for y in values {
                assert_eq!(multiply(x, y), checked(x.checked_mul(y)), "{x} * {y}");
            }
        }
    }
    #[test]
    fn non_null_arithmetic_matches_nullable_path_and_checks_overflow() {
        let budget = Budget::new(1 << 20);
        let make = |a: Vec<Option<i128>>, b: Vec<Option<i128>>| Input {
            len: a.len(),
            columns: vec![
                Some(Value::adopt(decimal(a, 2).unwrap(), &budget).unwrap()),
                Some(Value::adopt(decimal(b, 0).unwrap(), &budget).unwrap()),
            ],
        };
        for op in [Op::Add, Op::Sub, Op::Mul] {
            let expr = Expr {
                ty: Type::Decimal(2),
                kind: Kind::Binary(
                    op,
                    Box::new(Expr {
                        ty: Type::Decimal(2),
                        kind: Kind::Column(0),
                    }),
                    Box::new(Expr {
                        ty: Type::Int,
                        kind: Kind::Column(1),
                    }),
                ),
            };
            let plain = expr
                .eval(
                    &make(vec![Some(-101), Some(250)], vec![Some(2), Some(-3)]),
                    &budget,
                )
                .unwrap();
            let nullable = expr
                .eval(
                    &make(
                        vec![Some(-101), Some(250), None],
                        vec![Some(2), Some(-3), Some(0)],
                    ),
                    &budget,
                )
                .unwrap();
            assert_eq!(
                numeric(&plain.array).iter().collect::<Vec<_>>(),
                numeric(&nullable.array).iter().take(2).collect::<Vec<_>>()
            );
            assert!(is_sql_null(nullable.array.as_ref(), 2));
            assert_eq!(
                expr.eval(&make(vec![], vec![]), &budget)
                    .unwrap()
                    .array
                    .len(),
                0
            );
            let large = 10_i128.pow(38) - 1;
            let multiplier = if op == Op::Sub { -2 } else { 2 };
            assert!(
                expr.eval(&make(vec![Some(large)], vec![Some(multiplier)]), &budget)
                    .err()
                    .unwrap()
                    .contains("overflow")
            );
            assert!(is_sql_null(
                expr.eval(&make(vec![Some(large)], vec![None]), &budget)
                    .unwrap()
                    .array
                    .as_ref(),
                0
            ));
            // A late failure must discard partially built output and release
            // its reservation, in both nullable and NULL-free loops.
            for nullable in [false, true] {
                let mut a = vec![Some(1); 1025];
                let b = vec![Some(multiplier); 1025];
                if nullable {
                    a[0] = None;
                }
                a[1024] = Some(large);
                let input = make(a, b);
                let before = budget.used();
                assert!(expr.eval(&input, &budget).is_err());
                assert_eq!(budget.used(), before);
            }
        }
    }

    #[test]
    fn scalar_arithmetic_matches_arrays_with_slices_nulls_scales_and_errors() {
        let limit = 10_i128.pow(38);
        let constants = [
            i128::MIN,
            -limit + 1,
            -3,
            0,
            2,
            i64::MAX as i128,
            limit - 1,
            i128::MAX,
        ];
        for (vs, cs) in [(0, 0), (2, 0), (0, 2), (18, 1)] {
            for op in [Op::Add, Op::Sub, Op::Mul] {
                let scale = if op == Op::Mul { vs + cs } else { vs.max(cs) };
                for c in constants {
                    for reversed in [false, true] {
                        for nullable in [false, true] {
                            let mut numbers: Vec<_> = (0..70).map(|i| Some((i % 11) - 5)).collect();
                            if nullable {
                                numbers[3] = None;
                            }
                            // Separate slices exercise successful narrow batches,
                            // late overflow and empty batches without changing values.
                            numbers.extend([Some(limit - 1), Some(i128::MIN), None]);
                            for (offset, len) in [(0, 64), (1, 70), (69, 3), (72, 1), (73, 0)] {
                                let budget = Budget::new(1 << 20);
                                let array =
                                    decimal(numbers.clone(), vs).unwrap().slice(offset, len);
                                let input = Input {
                                    len,
                                    columns: vec![
                                        Some(Value::adopt(array, &budget).unwrap()),
                                        Some(
                                            Value::adopt(
                                                decimal_non_null(vec![c; len], cs).unwrap(),
                                                &budget,
                                            )
                                            .unwrap(),
                                        ),
                                    ],
                                };
                                let column = Expr {
                                    ty: Type::Decimal(vs),
                                    kind: Kind::Column(0),
                                };
                                let literal = Expr {
                                    ty: Type::Decimal(cs),
                                    kind: Kind::Literal(Literal::Num(c)),
                                };
                                let full = Expr {
                                    ty: Type::Decimal(cs),
                                    kind: Kind::Column(1),
                                };
                                let build = |constant: Expr| Expr {
                                    ty: Type::Decimal(scale),
                                    kind: if reversed {
                                        Kind::Binary(
                                            op,
                                            Box::new(constant),
                                            Box::new(column.clone()),
                                        )
                                    } else {
                                        Kind::Binary(
                                            op,
                                            Box::new(column.clone()),
                                            Box::new(constant),
                                        )
                                    },
                                };
                                let before = budget.used();
                                let actual = build(literal)
                                    .eval(&input, &budget)
                                    .map(|v| v.array.to_data());
                                let expected =
                                    build(full).eval(&input, &budget).map(|v| v.array.to_data());
                                assert_eq!(
                                    actual, expected,
                                    "{op:?} {vs}/{cs} c={c} reversed={reversed} slice={offset}/{len}"
                                );
                                assert_eq!(budget.used(), before);
                            }
                        }
                    }
                }
            }
        }
        // Scalar zero cannot suppress evaluation errors from its other operand.
        let zero = Expr {
            ty: Type::Int,
            kind: Kind::Literal(Literal::Num(0)),
        };
        let column = Expr {
            ty: Type::Int,
            kind: Kind::Column(0),
        };
        let fail = Expr {
            ty: Type::Int,
            kind: Kind::Neg(Box::new(column)),
        };
        let budget = Budget::new(1 << 20);
        let input = Input {
            len: 1,
            columns: vec![Some(
                Value::adopt(decimal_non_null(vec![i128::MIN], 0).unwrap(), &budget).unwrap(),
            )],
        };
        for reversed in [false, true] {
            let expr = Expr {
                ty: Type::Decimal(6),
                kind: if reversed {
                    Kind::Binary(Op::Mul, Box::new(zero.clone()), Box::new(fail.clone()))
                } else {
                    Kind::Binary(Op::Mul, Box::new(fail.clone()), Box::new(zero.clone()))
                },
            };
            assert_eq!(
                expr.eval(&input, &budget).err().as_deref(),
                Some("numeric overflow")
            );
            let tiny = Budget::new(0);
            assert!(
                expr.eval(&input, &tiny)
                    .err()
                    .unwrap()
                    .contains("resource-limit")
            );
            assert_eq!(tiny.used(), 0);
        }
    }

    #[test]
    fn string_membership_masks_bound_sliced_validity_storage() {
        let parent = StringArray::from_iter((0..32768).map(|i| {
            if i % 67 == 0 {
                None
            } else if i % 3 == 0 {
                Some("é")
            } else {
                Some("x")
            }
        }));
        for offset in [0, 1, 63, 64, 65, 127, 32700] {
            for len in [0, 1, 7, 8, 9, 64] {
                let values = parent.slice(offset, len);
                let mut calls = 0;
                let mask = string_membership_mask(&values, |s| {
                    calls += 1;
                    s == "é"
                });
                assert_eq!(calls, len - values.null_count());
                assert_eq!(
                    boolean(&mask).iter().collect::<Vec<_>>(),
                    values
                        .iter()
                        .map(|v| v.map(|s| s == "é"))
                        .collect::<Vec<_>>()
                );
                assert!(mask.get_array_memory_size() <= workspace(len, 0).unwrap());
                if let Some(nulls) = mask.nulls() {
                    assert!(nulls.buffer().capacity() <= 128);
                    assert_ne!(
                        nulls.buffer().as_ptr(),
                        parent.nulls().unwrap().buffer().as_ptr()
                    );
                }
            }
        }
    }

    #[test]
    fn string_membership_matches_unfused_or_with_nulls_unicode_and_slices() {
        let text = |s: &str| Expr {
            ty: Type::Text,
            kind: Kind::Literal(Literal::Text(s.into())),
        };
        let binary = |op, a, b| Expr {
            ty: Type::Bool,
            kind: Kind::Binary(op, Box::new(a), Box::new(b)),
        };
        let budget = Budget::new(1 << 22);
        let strings = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("é🦀tail"),
            Some("ab"),
            Some(""),
            Some("a\0b"),
            Some("🦀é"),
            Some("none"),
            Some("abcdefghij"),
        ]));
        let lists = [
            vec!["a", "ab"],
            vec!["", "é🦀", "a\0", "🦀é"],
            vec!["", "é🦀", "a\0", "🦀é", "ab", "ab", "none"],
            vec!["abcdefghij", "long absent value", "🦀", "é", "a"],
        ];
        for list in lists {
            for (offset, len) in [(0, 9), (1, 7), (1, 1), (9, 0)] {
                for computed in [false, true] {
                    let common = Expr {
                        ty: Type::Text,
                        kind: if computed {
                            Kind::Substring(
                                Box::new(Expr {
                                    ty: Type::Text,
                                    kind: Kind::Column(0),
                                }),
                                1,
                                2,
                            )
                        } else {
                            Kind::Column(0)
                        },
                    };
                    for reversed in [false, true] {
                        let mut columns = vec![Some(
                            Value::adopt(Arc::new(strings.slice(offset, len)), &budget).unwrap(),
                        )];
                        let mut fused = Expr {
                            ty: Type::Bool,
                            kind: Kind::Literal(Literal::Bool(false)),
                        };
                        let mut unfused = fused.clone();
                        for (i, s) in list.iter().enumerate() {
                            columns.push(Some(
                                Value::adopt(Arc::new(StringArray::from(vec![*s; len])), &budget)
                                    .unwrap(),
                            ));
                            let col = Expr {
                                ty: Type::Text,
                                kind: Kind::Column(i + 1),
                            };
                            let (a, b) = if reversed {
                                (text(s), common.clone())
                            } else {
                                (common.clone(), text(s))
                            };
                            fused = binary(Op::Or, fused, binary(Op::Eq, a, b));
                            let (a, b) = if reversed {
                                (col, common.clone())
                            } else {
                                (common.clone(), col)
                            };
                            unfused = binary(Op::Or, unfused, binary(Op::Eq, a, b));
                        }
                        let input = Input { columns, len };
                        for negate in [false, true] {
                            let wrap = |e: Expr| {
                                if negate {
                                    Expr {
                                        ty: Type::Bool,
                                        kind: Kind::Not(Box::new(e)),
                                    }
                                } else {
                                    e
                                }
                            };
                            let before = budget.used();
                            let actual = wrap(fused.clone())
                                .eval(&input, &budget)
                                .unwrap()
                                .array
                                .to_data();
                            let expected = wrap(unfused.clone())
                                .eval(&input, &budget)
                                .unwrap()
                                .array
                                .to_data();
                            assert_eq!(actual, expected);
                            assert_eq!(budget.used(), before);
                        }
                        assert!(
                            string_membership(&fused, &input, &budget)
                                .unwrap()
                                .is_some()
                        );
                        // A NULL constant changes unmatched rows to UNKNOWN.
                        let null = Expr {
                            ty: Type::Text,
                            kind: Kind::Literal(Literal::Null),
                        };
                        let mixed =
                            binary(Op::Or, fused.clone(), binary(Op::Eq, common.clone(), null));
                        assert!(
                            string_membership(&mixed, &input, &budget)
                                .unwrap()
                                .is_none()
                        );
                        let values = fused.eval(&input, &budget).unwrap();
                        let with_null = mixed.eval(&input, &budget).unwrap();
                        for (a, b) in boolean(&values.array)
                            .iter()
                            .zip(boolean(&with_null.array).iter())
                        {
                            assert_eq!(b, if a == Some(true) { Some(true) } else { None });
                        }
                        let different = binary(
                            Op::Or,
                            fused,
                            binary(
                                Op::Eq,
                                Expr {
                                    ty: Type::Text,
                                    kind: Kind::Column(1),
                                },
                                text("x"),
                            ),
                        );
                        assert!(
                            string_membership(&different, &input, &budget)
                                .unwrap()
                                .is_none()
                        );
                    }
                }
            }
        }
        let input = Input {
            len: 1,
            columns: vec![Some(
                Value::adopt(decimal_non_null(vec![i128::MIN], 0).unwrap(), &budget).unwrap(),
            )],
        };
        let fail = Expr {
            ty: Type::Int,
            kind: Kind::Neg(Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Column(0),
            })),
        };
        let condition = binary(
            Op::Gt,
            fail,
            Expr {
                ty: Type::Int,
                kind: Kind::Literal(Literal::Num(0)),
            },
        );
        let common = Expr {
            ty: Type::Text,
            kind: Kind::Case(vec![(condition, text("yes"))], Box::new(text("no"))),
        };
        let expr = binary(
            Op::Or,
            binary(Op::Eq, common.clone(), text("yes")),
            binary(Op::Eq, common, text("no")),
        );
        let before = budget.used();
        assert_eq!(
            expr.eval(&input, &budget).err().as_deref(),
            Some("numeric overflow")
        );
        assert_eq!(budget.used(), before);
        let tiny = Budget::new(0);
        assert!(
            expr.eval(&input, &tiny)
                .err()
                .unwrap()
                .contains("resource-limit")
        );
        assert_eq!(tiny.used(), 0);
    }

    #[test]
    fn boolean_factoring_preserves_three_valued_truth() {
        let budget = Budget::new(1 << 20);
        let mut columns = vec![vec![], vec![], vec![]];
        for a in [Some(true), Some(false), None] {
            for b in [Some(true), Some(false), None] {
                for c in [Some(true), Some(false), None] {
                    columns[0].push(a);
                    columns[1].push(b);
                    columns[2].push(c);
                }
            }
        }
        let input = Input {
            columns: columns
                .into_iter()
                .map(|v| Some(Value::adopt(Arc::new(BooleanArray::from(v)), &budget).unwrap()))
                .collect(),
            len: 27,
        };
        let col = |i| Expr {
            ty: Type::Bool,
            kind: Kind::Column(i),
        };
        let binary = |op, a, b| Expr {
            ty: Type::Bool,
            kind: Kind::Binary(op, Box::new(a), Box::new(b)),
        };
        let and = |a, b| binary(Op::And, a, b);
        let or = |a, b| binary(Op::Or, a, b);
        for expr in [
            or(and(col(0), col(1)), and(col(0), col(2))),
            or(col(0), and(col(0), col(1))),
            and(col(0), col(0)),
            or(and(col(0), col(1)), and(col(1), col(0))),
            or(
                or(and(col(0), col(1)), and(col(0), col(2))),
                and(col(0), and(col(1), col(2))),
            ),
        ] {
            let simplified = expr.clone().simplify_boolean();
            assert_eq!(
                expr.eval(&input, &budget).unwrap().array.to_data(),
                simplified.eval(&input, &budget).unwrap().array.to_data()
            );
        }
    }

    #[test]
    fn compiled_like_matches_character_reference() {
        fn strings(alphabet: &[&str], length: usize) -> Vec<String> {
            let mut out = vec![String::new()];
            let mut level = out.clone();
            for _ in 0..length {
                level = level
                    .iter()
                    .flat_map(|s| alphabet.iter().map(move |c| format!("{s}{c}")))
                    .collect();
                out.extend(level.iter().cloned());
            }
            out
        }
        let texts = strings(&["a", "b", "é", "🦀"], 4);
        for pattern in strings(&["a", "b", "é", "🦀", "%", "_"], 4) {
            let compiled = LikePattern::new(&pattern);
            for text in &texts {
                assert_eq!(
                    compiled.matches(text),
                    like(text, &pattern),
                    "{text:?} LIKE {pattern:?}"
                );
            }
        }
    }

    #[test]
    fn scalar_comparisons_match_broadcast_arrays() {
        let budget = Budget::new(1 << 24);
        let cases: Vec<(Type, ArrayRef, Literal, Type)> = vec![
            (
                Type::Decimal(2),
                decimal(vec![Some(-100), None, Some(200), Some(201)], 2).unwrap(),
                Literal::Num(2),
                Type::Int,
            ),
            (
                Type::Text,
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("é"),
                    Some("z"),
                ])),
                Literal::Text("z".into()),
                Type::Text,
            ),
            (
                Type::Bool,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ])),
                Literal::Bool(false),
                Type::Bool,
            ),
            (
                Type::Date,
                Arc::new(Date32Array::from(vec![Some(1), None, Some(2), Some(3)])),
                Literal::Date(2),
                Type::Date,
            ),
        ];
        for (ty, array, literal, literal_ty) in cases {
            for len in [0, 1, 4] {
                let scalar = Expr {
                    ty: literal_ty.clone(),
                    kind: Kind::Literal(literal.clone()),
                };
                let input = Input {
                    columns: vec![
                        Some(Value::adopt(array.slice(0, len), &budget).unwrap()),
                        Some(
                            scalar
                                .eval(
                                    &Input {
                                        columns: vec![],
                                        len,
                                    },
                                    &budget,
                                )
                                .unwrap(),
                        ),
                    ],
                    len,
                };
                let column = Expr {
                    ty: ty.clone(),
                    kind: Kind::Column(0),
                };
                let broadcast = Expr {
                    ty: literal_ty.clone(),
                    kind: Kind::Column(1),
                };
                for op in [Op::Eq, Op::Ne, Op::Lt, Op::Le, Op::Gt, Op::Ge] {
                    for reverse in [false, true] {
                        let expr = |rhs: Expr| Expr {
                            ty: Type::Bool,
                            kind: if reverse {
                                Kind::Binary(op, Box::new(rhs), Box::new(column.clone()))
                            } else {
                                Kind::Binary(op, Box::new(column.clone()), Box::new(rhs))
                            },
                        };
                        let actual = expr(scalar.clone()).eval(&input, &budget).unwrap();
                        let expected = expr(broadcast.clone()).eval(&input, &budget).unwrap();
                        assert_eq!(actual.array.to_data(), expected.array.to_data());
                    }
                }
            }
        }
    }
}
