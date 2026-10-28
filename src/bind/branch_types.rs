//! Reachability facts for typed conditionals, never dynamic storage classes.
use super::*;
use crate::{
    expr::{Input, is_sql_null},
    memory::Budget,
};
use arrow::array::{BooleanArray, Decimal128Array, Float64Array};

pub(super) fn integer_literal_syntax(expr: &sql::Expr) -> bool {
    match expr {
        sql::Expr::Value(value) => matches!(&value.value, sql::Value::Number(text, _)
            if !text.contains(['.', 'e', 'E'])),
        sql::Expr::Nested(expr) => integer_literal_syntax(expr),
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Plus | sql::UnaryOperator::Minus,
            expr,
        } => integer_literal_syntax(expr),
        _ => false,
    }
}

/// Only syntactic integer literals (with unary signs), never folded arithmetic,
/// columns or casts. Binary64 represents every integer in this closed interval:
/// below 2^53 at most 53 significant bits are needed; 2^53 is a power of two.
pub(crate) fn promote_float_literal(expr: &mut Expr) {
    fn integer(expr: &Expr) -> Option<i128> {
        if expr.ty != Type::Int {
            return None;
        }
        match &expr.kind {
            Kind::Literal(Literal::Num(n)) => Some(*n),
            Kind::Neg(a) => integer(a)?.checked_neg(),
            _ => None,
        }
    }
    if let Some(n) = integer(expr)
        && n.unsigned_abs() <= (1_u128 << 53)
    {
        // The expression IR represents floating constants as casts of exact
        // numeric literals. Choose the Float type here, before binding any
        // consumer, and collapse unary signs in the integer domain (also -0).
        *expr = Expr {
            ty: Type::Float,
            kind: Kind::Cast(Box::new(literal(Type::Int, Literal::Num(n)))),
        };
    }
}

/// Bounded column-free proof through the existing checked evaluator. Failure
/// means unknown; lazy runtime errors are never raised during preparation.
pub(crate) fn constant(expr: &Expr) -> Option<Expr> {
    fn bounded(e: &Expr, left: &mut usize) -> bool {
        if *left == 0 {
            return false;
        }
        *left -= 1;
        match &e.kind {
            Kind::Literal(Literal::Num(_) | Literal::Bool(_) | Literal::Null) => true,
            Kind::Neg(a)
            | Kind::Not(a)
            | Kind::IsNull(a, _)
            | Kind::Cast(a)
            | Kind::DialectCast(a, _)
            | Kind::CastVarchar(a, _)
            | Kind::CastDecimal(a, _, _) => bounded(a, left),
            Kind::Binary(_, a, b) | Kind::NullIf(a, b) => bounded(a, left) && bounded(b, left),
            Kind::Function(name, a) if matches!(name.as_str(), "abs" | "factorial") => {
                bounded(a, left)
            }
            _ => false,
        }
    }
    if !bounded(expr, &mut 64) {
        return None;
    }
    let value = expr
        .eval(
            &Input {
                columns: Vec::new(),
                len: 1,
            },
            &Budget::new(65536),
        )
        .ok()?;
    let literal = if is_sql_null(value.array.as_ref(), 0) {
        Literal::Null
    } else {
        match expr.ty {
            Type::Int | Type::Decimal(_) => Literal::Num(
                value
                    .array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()?
                    .value(0),
            ),
            Type::Bool => Literal::Bool(
                value
                    .array
                    .as_any()
                    .downcast_ref::<BooleanArray>()?
                    .value(0),
            ),
            // Literal has no floating representation. Retain the successfully
            // evaluated expression for its non-NULL fact, never round it.
            Type::Float
                if value
                    .array
                    .as_any()
                    .downcast_ref::<Float64Array>()?
                    .value(0)
                    .is_finite() =>
            {
                return Some(expr.clone());
            }
            _ => return None,
        }
    };
    Some(Expr {
        ty: expr.ty.clone(),
        kind: Kind::Literal(literal),
    })
}

pub(super) fn coalesce(values: Vec<(Expr, bool)>, binder: &Binder<'_>) -> Result<Expr> {
    let validated_type = values
        .iter()
        .try_fold(Type::Null, |ty, (value, _)| common_type(&ty, &value.ty))?;
    let mut reachable = Vec::new();
    for (value, eligible) in values {
        if let Some(proven) = constant(&value) {
            if matches!(proven.kind, Kind::Literal(Literal::Null)) {
                continue;
            }
            reachable.push((value, eligible));
            break;
        }
        reachable.push((value, eligible));
    }
    if reachable.is_empty() {
        return Ok(literal(validated_type, Literal::Null));
    }
    let (other, other_eligible) = reachable.pop().unwrap();
    if reachable.is_empty() {
        return Ok(other);
    }
    let ty = reachable
        .iter()
        .try_fold(other.ty.clone(), |ty, (value, _)| {
            common_type(&ty, &value.ty)
        })?;
    let eligible = reachable
        .iter()
        .map(|(_, eligible)| *eligible)
        .chain([other_eligible])
        .collect::<Vec<_>>();
    let branches = reachable
        .into_iter()
        .map(|(value, _)| {
            (
                bool_expr(Kind::IsNull(Box::new(value.clone()), true)),
                value,
            )
        })
        .collect();
    crate::mixed_numeric::normalize_literal_arms(
        Expr {
            ty,
            kind: Kind::Case(branches, Box::new(other)),
        },
        binder.schema,
        binder.facts,
        &eligible,
    )
}

pub(super) fn case(
    branches: Vec<(Expr, Expr, bool)>,
    mut other: Expr,
    mut other_eligible: bool,
    binder: &Binder<'_>,
) -> Result<Expr> {
    let validated_type = branches
        .iter()
        .try_fold(other.ty.clone(), |ty, (_, value, _)| {
            common_type(&ty, &value.ty)
        })?;
    let mut reachable = Vec::new();
    let mut eligible = Vec::new();
    for (condition, value, literal) in branches {
        match constant(&condition).map(|e| e.kind) {
            Some(Kind::Literal(Literal::Bool(false) | Literal::Null)) => {}
            Some(Kind::Literal(Literal::Bool(true))) => {
                other = value;
                other_eligible = literal;
                break;
            }
            _ => {
                reachable.push((condition, value));
                eligible.push(literal);
            }
        }
    }
    if reachable.is_empty() {
        // A selected NULL has no storage class; retain its validated contextual
        // type so surrounding unary numeric operators bind as before.
        if other.ty == Type::Null && matches!(other.kind, Kind::Literal(Literal::Null)) {
            other.ty = validated_type;
        }
        return Ok(other);
    }
    let ty = reachable
        .iter()
        .try_fold(other.ty.clone(), |ty, (_, value)| {
            common_type(&ty, &value.ty)
        })?;
    eligible.push(other_eligible);
    crate::mixed_numeric::normalize_literal_arms(
        Expr {
            ty,
            kind: Kind::Case(reachable, Box::new(other)),
        },
        binder.schema,
        binder.facts,
        &eligible,
    )
}

#[cfg(test)]
mod literal_promotion_tests {
    use super::*;

    #[test]
    fn binary64_exact_integer_interval() {
        // Cover every binary exponent in the admitted domain. For |v| < 2^53,
        // its integer bit length is <= f64::MANTISSA_DIGITS, so no bits are
        // discarded; the endpoint 2^53 has one significant bit. The sign bit
        // does not consume significand precision. This argument covers every
        // admitted integer, not just the boundary/interior witnesses below.
        assert_eq!(f64::MANTISSA_DIGITS, 53);
        for exponent in 0..=53 {
            let power = 1_i128 << exponent;
            for offset in 0..=4096 {
                for magnitude in [power - offset, power + offset] {
                    if !(0..=(1_i128 << 53)).contains(&magnitude) {
                        continue;
                    }
                    let significant_bits = 128 - magnitude.leading_zeros();
                    assert!(significant_bits <= 53 || magnitude == 1_i128 << 53);
                    for n in [magnitude, -magnitude] {
                        assert_eq!((n as f64) as i128, n);
                    }
                }
            }
        }
        for n in [
            0,
            1,
            -1,
            (1_i128 << 53) - 1,
            -(1_i128 << 53) + 1,
            1_i128 << 53,
            -(1_i128 << 53),
        ] {
            let mut expr = literal(Type::Int, Literal::Num(n));
            promote_float_literal(&mut expr);
            assert_eq!(expr.ty, Type::Float);
            let result = expr
                .eval(
                    &Input {
                        columns: vec![],
                        len: 1,
                    },
                    &Budget::new(65536),
                )
                .unwrap();
            let value = result
                .array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert_eq!(value as i128, n);
            assert_eq!(value.to_bits(), (n as f64).to_bits());
        }
        for n in [
            (1_i128 << 53) + 1,
            -(1_i128 << 53) - 1,
            i128::MIN,
            i128::MAX,
        ] {
            let mut expr = literal(Type::Int, Literal::Num(n));
            let original = expr.clone();
            promote_float_literal(&mut expr);
            assert_eq!(expr, original);
        }
    }
}
