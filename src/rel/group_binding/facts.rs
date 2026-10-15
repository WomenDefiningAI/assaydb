//! Bounded, error-aware facts used only after grouped name/type validation.
//! No representative row and no data statistics. Physical input fields bound
//! integer column ranges; internal INTEGER fields carry precision-38 values.
use crate::{
    expr::{Expr, Kind, Literal, Op},
    types::{Type, checked},
};

#[derive(Clone, Copy)]
enum Known {
    Unknown,
    Null,
    Bool(bool),
    Int(i128),
}
#[derive(Clone, Copy)]
struct Fact {
    total: bool,
    known: Known,
    range: Option<(i128, i128)>,
    // Every non-NULL value is finite and has magnitude <= 2^exponent.
    // None supplies no floating bound (including arbitrary Float columns).
    float_exponent: Option<u16>,
}
impl Fact {
    fn unknown() -> Self {
        Self {
            total: false,
            known: Known::Unknown,
            range: None,
            float_exponent: None,
        }
    }
    fn total() -> Self {
        Self {
            total: true,
            ..Self::unknown()
        }
    }
    fn null() -> Self {
        Self {
            known: Known::Null,
            ..Self::total()
        }
    }
    fn boolean(v: bool) -> Self {
        Self {
            known: Known::Bool(v),
            ..Self::total()
        }
    }
    fn int(v: i128) -> Self {
        Self {
            known: Known::Int(v),
            range: Some((v, v)),
            ..Self::total()
        }
    }
    fn is_null(self) -> bool {
        matches!(self.known, Known::Null)
    }
}
pub(super) fn simplify(
    expr: &mut Expr,
    schema: &arrow::datatypes::Schema,
    provenance_narrow: &[bool],
) {
    let narrow_columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            matches!(
                field.data_type(),
                arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
            ) || provenance_narrow.get(index).copied().unwrap_or(false)
        })
        .collect::<Vec<_>>();
    visit(expr, &mut 256, &narrow_columns);
}

fn bounded(range: Option<(i128, i128)>) -> Option<(i128, i128)> {
    let (lo, hi) = range?;
    checked(Some(lo)).ok()?;
    checked(Some(hi)).ok()?;
    Some((lo, hi))
}
fn arithmetic(op: Op, a: Fact, b: Fact) -> Fact {
    if !a.total || !b.total {
        return Fact::unknown();
    }
    if a.is_null() || b.is_null() {
        return Fact::null();
    }
    let range = (|| {
        let (al, ah) = a.range?;
        let (bl, bh) = b.range?;
        match op {
            Op::Add => Some((al.checked_add(bl)?, ah.checked_add(bh)?)),
            Op::Sub => Some((al.checked_sub(bh)?, ah.checked_sub(bl)?)),
            Op::Mul => {
                let vs = [
                    al.checked_mul(bl)?,
                    al.checked_mul(bh)?,
                    ah.checked_mul(bl)?,
                    ah.checked_mul(bh)?,
                ];
                Some((*vs.iter().min()?, *vs.iter().max()?))
            }
            _ => None,
        }
    })();
    let Some(range) = bounded(range) else {
        return Fact::unknown();
    };
    let known = if let (Known::Int(_), Known::Int(_)) = (a.known, b.known) {
        Known::Int(range.0)
    } else {
        Known::Unknown
    };
    Fact {
        total: true,
        known,
        range: Some(range),
        ..Fact::total()
    }
}
/// Integer division is total only across the entire Cartesian operand range.
/// On a denominator interval of one sign, extrema occur at the four corners;
/// truncation toward zero is monotone on each sign region. Check both i128
/// division and precision-38 result bounds.
fn integer_division(a: Fact, b: Fact) -> Fact {
    if !a.total || !b.total {
        return Fact::unknown();
    }
    let range = (|| {
        let (al, ah) = bounded(a.range)?;
        let (bl, bh) = bounded(b.range)?;
        if bl <= 0 && bh >= 0 {
            return None;
        }
        let values = [
            al.checked_div(bl)?,
            al.checked_div(bh)?,
            ah.checked_div(bl)?,
            ah.checked_div(bh)?,
        ];
        let lo = *values.iter().min()?;
        let hi = *values.iter().max()?;
        checked(Some(lo)).ok()?;
        checked(Some(hi)).ok()?;
        Some((lo, hi))
    })();
    let Some(range) = range else {
        return Fact::unknown();
    };
    Fact {
        total: true,
        known: if matches!((a.known, b.known), (Known::Int(_), Known::Int(_))) {
            Known::Int(range.0)
        } else {
            Known::Unknown
        },
        range: Some(range),
        ..Fact::total()
    }
}

/// Facts for an Int CASE with no statically selected arm. False/NULL tests
/// never evaluate their values; after a true test, neither later tests nor ELSE
/// execute. Nullable output bounds contain only possible non-NULL values.
fn conditional(facts: &[(Fact, Fact)], other: Fact) -> Fact {
    let mut all_null = true;
    let mut range: Option<(i128, i128)> = None;
    let mut unknown_range = false;
    let mut merge = |value: Fact| -> bool {
        if !value.total {
            return false;
        }
        if value.is_null() {
            return true;
        }
        all_null = false;
        if let Some((lo, hi)) = value.range {
            range = Some(range.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
        } else {
            unknown_range = true;
        }
        true
    };
    let mut reaches_else = true;
    for (test, value) in facts {
        if !test.total {
            return Fact::unknown();
        }
        if matches!(test.known, Known::Null | Known::Bool(false)) {
            continue;
        }
        if !merge(*value) {
            return Fact::unknown();
        }
        if matches!(test.known, Known::Bool(true)) {
            reaches_else = false;
            break;
        }
    }
    if reaches_else && !merge(other) {
        return Fact::unknown();
    }
    Fact {
        total: true,
        known: if all_null {
            Known::Null
        } else {
            Known::Unknown
        },
        range: if unknown_range { None } else { range },
        ..Fact::total()
    }
}

/// Bound the evaluator's numeric-to-f64 conversion without performing it.
/// Precision-38 Int values have magnitude < 2^127. Rounding to f64 therefore
/// stays below the deliberately loose 2^128 envelope, including wide operands.
fn float_bound(fact: Fact, ty: &Type) -> Option<u16> {
    if !fact.total {
        return None;
    }
    match ty {
        Type::Int if bounded(fact.range).is_some() => Some(128),
        Type::Float => fact.float_exponent,
        _ => None,
    }
}

/// Inputs bounded by 2^a and 2^b have an exact sum/difference bounded by
/// 2^(max(a,b)+1). That power of two is exactly representable, so rounding
/// cannot escape the closed bound. The 1022 ceiling deliberately leaves
/// substantial headroom below f64 overflow. No values or signed zero change.
fn float_add_sub(a: Fact, b: Fact, at: &Type, bt: &Type) -> Fact {
    let exponent = (|| {
        let exponent = float_bound(a, at)?
            .max(float_bound(b, bt)?)
            .checked_add(1)?;
        (exponent <= 1022).then_some(exponent)
    })();
    match exponent {
        Some(exponent) => Fact {
            float_exponent: Some(exponent),
            ..Fact::total()
        },
        None => Fact::unknown(),
    }
}

fn binary(op: Op, a: Fact, b: Fact, at: &Type, bt: &Type, result: &Type) -> Fact {
    // These operators evaluate both children; NULL never hides a child error.
    if !a.total || !b.total {
        return Fact::unknown();
    }
    if matches!(op, Op::And | Op::Or) {
        use Known::*;
        return match (op, a.known, b.known) {
            (Op::And, Bool(false), _) | (Op::And, _, Bool(false)) => Fact::boolean(false),
            (Op::Or, Bool(true), _) | (Op::Or, _, Bool(true)) => Fact::boolean(true),
            (Op::And, Bool(true), Bool(true)) => Fact::boolean(true),
            (Op::Or, Bool(false), Bool(false)) => Fact::boolean(false),
            (_, Null, Null)
            | (Op::And, Null, Bool(true))
            | (Op::And, Bool(true), Null)
            | (Op::Or, Null, Bool(false))
            | (Op::Or, Bool(false), Null) => Fact::null(),
            _ => Fact::total(),
        };
    }
    if a.is_null() || b.is_null() {
        return Fact::null();
    }
    if matches!(op, Op::Add | Op::Sub | Op::Mul)
        && *at == Type::Int
        && *bt == Type::Int
        && *result == Type::Int
    {
        return arithmetic(op, a, b);
    }
    if matches!(op, Op::Add | Op::Sub) && *result == Type::Float {
        return float_add_sub(a, b, at, bt);
    }
    if matches!(op, Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge) {
        // Equal exact scales need no potentially overflowing rescale. Float
        // and mixed-scale cases stay unknown, apart from the NULL rule above.
        if at != bt
            || !matches!(
                at,
                Type::Int | Type::Bool | Type::Text | Type::Binary | Type::Date
            )
        {
            return Fact::unknown();
        }
        if let (Known::Int(a), Known::Int(b)) = (a.known, b.known) {
            return Fact::boolean(match op {
                Op::Eq => a == b,
                Op::Ne => a != b,
                Op::Lt => a < b,
                Op::Le => a <= b,
                Op::Gt => a > b,
                Op::Ge => a >= b,
                _ => unreachable!(),
            });
        }
        return Fact::total();
    }
    if op == Op::Div && *at == Type::Int && *bt == Type::Int && *result == Type::Int {
        return integer_division(a, b);
    }
    Fact::unknown()
}
fn visit(e: &mut Expr, left: &mut usize, narrow_columns: &[bool]) -> Fact {
    if *left == 0 {
        return Fact::unknown();
    }
    *left -= 1;
    // Do not newly admit the deferred dynamic conditional storage-class case.
    // Even removing a false condition could expose an Int arm under a static
    // Float parent and change downstream division versus SQLite. Leave that
    // entire node, including its dependencies, untouched.
    if let Kind::Case(branches, other) = &e.kind {
        if branches.len() > *left {
            return Fact::unknown();
        }
        if branches
            .iter()
            .map(|(_, value)| value)
            .chain(std::iter::once(other.as_ref()))
            .any(|value| value.ty != e.ty && !matches!(value.kind, Kind::Literal(Literal::Null)))
        {
            return Fact::unknown();
        }
    }
    let mut replacement: Option<Option<usize>> = None;
    let fact = match &mut e.kind {
        Kind::Literal(Literal::Null) => Fact::null(),
        Kind::Literal(Literal::Bool(v)) => Fact::boolean(*v),
        Kind::Literal(Literal::Num(v)) if e.ty == Type::Int => Fact::int(*v),
        Kind::Literal(_) => Fact::total(),
        Kind::Column(index) => Fact {
            range: (e.ty == Type::Int).then(|| {
                if narrow_columns.get(*index).copied().unwrap_or(false) {
                    (i64::MIN as i128, i64::MAX as i128)
                } else {
                    (-(10_i128.pow(38) - 1), 10_i128.pow(38) - 1)
                }
            }),
            ..Fact::total()
        },
        Kind::Binary(op, a, b) => {
            let af = visit(a, left, narrow_columns);
            let bf = visit(b, left, narrow_columns);
            binary(*op, af, bf, &a.ty, &b.ty, &e.ty)
        }
        Kind::Neg(a) => {
            let a = visit(a, left, narrow_columns);
            if !a.total {
                Fact::unknown()
            } else if a.is_null() {
                Fact::null()
            } else if e.ty == Type::Float && a.float_exponent.is_some() {
                // Negating a proved finite float preserves its magnitude.
                a
            } else if e.ty == Type::Int {
                let range = bounded(
                    a.range
                        .and_then(|(lo, hi)| Some((hi.checked_neg()?, lo.checked_neg()?))),
                );
                if let Some(range) = range {
                    Fact {
                        total: true,
                        known: match a.known {
                            Known::Int(v) => Known::Int(-v),
                            _ => Known::Unknown,
                        },
                        range: Some(range),
                        ..Fact::total()
                    }
                } else {
                    Fact::unknown()
                }
            } else {
                Fact::unknown()
            }
        }
        Kind::Not(a) => {
            let a = visit(a, left, narrow_columns);
            if !a.total {
                Fact::unknown()
            } else {
                match a.known {
                    Known::Null => Fact::null(),
                    Known::Bool(v) => Fact::boolean(!v),
                    _ => Fact::total(),
                }
            }
        }
        Kind::IsNull(a, negated) => {
            let a = visit(a, left, narrow_columns);
            if !a.total {
                Fact::unknown()
            } else {
                match a.known {
                    Known::Null => Fact::boolean(!*negated),
                    Known::Bool(_) | Known::Int(_) => Fact::boolean(*negated),
                    _ => Fact::total(),
                }
            }
        }
        Kind::Cast(a) | Kind::CastDecimal(a, _, _) => {
            let fact = visit(a, left, narrow_columns);
            if !fact.total {
                Fact::unknown()
            } else if fact.is_null() {
                Fact::null()
            } else if a.ty == Type::Int && e.ty == Type::Int {
                fact
            } else if a.ty == Type::Int && e.ty == Type::Float {
                Fact {
                    float_exponent: float_bound(fact, &a.ty),
                    ..Fact::total()
                }
            } else {
                Fact::unknown()
            }
        }
        Kind::NullIf(a, b) => {
            let af = visit(a, left, narrow_columns);
            let bf = visit(b, left, narrow_columns);
            if !af.total || !bf.total {
                Fact::unknown()
            } else if af.is_null() {
                Fact::null()
            } else if a.ty == Type::Int && bf.is_null() {
                af
            } else if a.ty == Type::Int
                && b.ty == Type::Int
                && bounded(af.range).is_some()
                && bounded(bf.range).is_some()
            {
                match (af.known, bf.known) {
                    (Known::Int(a), Known::Int(b)) if a == b => Fact::null(),
                    (Known::Int(_), Known::Int(_)) => af,
                    _ => Fact {
                        total: true,
                        known: Known::Unknown,
                        range: af.range,
                        ..Fact::total()
                    },
                }
            } else {
                Fact::unknown()
            }
        }
        Kind::Case(branches, other) => {
            // Binding already checked every original arm. Only false/NULL
            // conditions are removed, and only when their evaluation is total.
            if branches.len() > *left {
                return Fact::unknown();
            }
            let mut facts = Vec::with_capacity(branches.len());
            for (condition, value) in branches.iter_mut() {
                facts.push((
                    visit(condition, left, narrow_columns),
                    visit(value, left, narrow_columns),
                ));
            }
            let other_fact = visit(other, left, narrow_columns);
            let first_live = facts.iter().position(|(f, _)| {
                !(f.total && matches!(f.known, Known::Null | Known::Bool(false)))
            });
            let chosen = match first_live {
                None => Some((None, other.as_ref(), other_fact)),
                Some(i) if facts[i].0.total && matches!(facts[i].0.known, Known::Bool(true)) => {
                    Some((Some(i), &branches[i].1, facts[i].1))
                }
                _ => None,
            };
            if let Some((index, value, fact)) = chosen {
                // Never silently alter existing static conditional promotion.
                if value.ty == e.ty || fact.is_null() {
                    replacement = Some(index);
                    fact
                } else {
                    Fact::unknown()
                }
            } else if e.ty == Type::Int {
                conditional(&facts, other_fact)
            } else {
                Fact::unknown()
            }
        }
        // Unsupported proof nodes stay live; no speculative runtime evaluation.
        _ => Fact::unknown(),
    };
    if let Some(index) = replacement {
        let Kind::Case(branches, other) =
            std::mem::replace(&mut e.kind, Kind::Literal(Literal::Null))
        else {
            unreachable!()
        };
        e.kind = match index {
            Some(i) => branches.into_iter().nth(i).unwrap().1.kind,
            None => other.kind,
        };
    }
    if fact.total {
        match fact.known {
            Known::Null => e.kind = Kind::Literal(Literal::Null),
            Known::Bool(v) => e.kind = Kind::Literal(Literal::Bool(v)),
            Known::Int(v) => e.kind = Kind::Literal(Literal::Num(v)),
            Known::Unknown => {}
        }
    }
    fact
}

#[cfg(test)]
mod interval_tests {
    use super::*;
    #[test]
    fn finite_bounds_are_outward_and_never_inferred_from_float_storage() {
        let wide = 10_i128.pow(38) - 1;
        for range in [(i64::MIN as i128, i64::MAX as i128), (-wide, wide), (0, 0)] {
            let fact = Fact {
                range: Some(range),
                ..Fact::total()
            };
            assert_eq!(float_bound(fact, &Type::Int), Some(128));
            for value in [range.0, range.1] {
                assert!((value as f64).abs() <= 2_f64.powi(128));
            }
        }
        let near = Fact {
            float_exponent: Some(1021),
            ..Fact::total()
        };
        let result = float_add_sub(near, near, &Type::Float, &Type::Float);
        assert!(result.total);
        assert_eq!(result.float_exponent, Some(1022));
        assert!((2_f64.powi(1021) + 2_f64.powi(1021)).is_finite());
        assert!(!float_add_sub(result, result, &Type::Float, &Type::Float).total);
        assert!(!float_add_sub(Fact::total(), near, &Type::Float, &Type::Float).total);
        assert!(!float_add_sub(Fact::unknown(), near, &Type::Int, &Type::Float).total);
        // The Float column may contain finite values, infinities or NaNs. No
        // input value is inspected and its type alone establishes no bound.
        let mut column = Expr {
            kind: Kind::Column(0),
            ty: Type::Float,
        };
        assert!(visit(&mut column, &mut 256, &[]).float_exponent.is_none());
        let mut cast = Expr {
            ty: Type::Float,
            kind: Kind::Cast(Box::new(Expr {
                kind: Kind::Column(0),
                ty: Type::Int,
            })),
        };
        assert!(!visit(&mut cast.clone(), &mut 1, &[]).total);
        assert_eq!(visit(&mut cast, &mut 256, &[]).float_exponent, Some(128));
    }

    #[test]
    fn division_range_contains_every_small_interior_quotient() {
        for al in -4..=4 {
            for ah in al..=4 {
                for bl in -4..=4 {
                    for bh in bl..=4 {
                        let a = Fact {
                            range: Some((al, ah)),
                            ..Fact::total()
                        };
                        let b = Fact {
                            range: Some((bl, bh)),
                            ..Fact::total()
                        };
                        let result = integer_division(a, b);
                        if bl <= 0 && bh >= 0 {
                            assert!(!result.total);
                            continue;
                        }
                        assert!(result.total);
                        let (lo, hi) = result.range.unwrap();
                        for x in al..=ah {
                            for y in bl..=bh {
                                let q = x / y;
                                assert!(lo <= q && q <= hi);
                            }
                        }
                    }
                }
            }
        }
        for (a, b, accepted) in [
            ((i64::MIN as i128, i64::MAX as i128), (-1, -1), true),
            ((i64::MIN as i128, i64::MAX as i128), (-2, -2), true),
            ((i64::MAX as i128 + 1, i64::MAX as i128 + 1), (1, 1), true),
            ((i64::MAX as i128 + 1, i64::MAX as i128 + 1), (2, 2), true),
        ] {
            assert_eq!(
                integer_division(
                    Fact {
                        range: Some(a),
                        ..Fact::total()
                    },
                    Fact {
                        range: Some(b),
                        ..Fact::total()
                    }
                )
                .total,
                accepted
            );
        }
    }
}
