//! Reachability facts for typed conditionals, never dynamic storage classes.
use super::*;
use crate::{
    expr::{Input, is_sql_null},
    memory::Budget,
};
use arrow::array::{BooleanArray, Decimal128Array, Float64Array};

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
            Kind::Neg(a) | Kind::Not(a) | Kind::IsNull(a, _) | Kind::Cast(a) => bounded(a, left),
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

pub(super) fn coalesce(values: Vec<Expr>) -> Result<Expr> {
    let validated_type = values
        .iter()
        .try_fold(Type::Null, |ty, value| common_type(&ty, &value.ty))?;
    let mut reachable = Vec::new();
    for value in values {
        if let Some(proven) = constant(&value) {
            if matches!(proven.kind, Kind::Literal(Literal::Null)) {
                continue;
            }
            reachable.push(value);
            break;
        }
        reachable.push(value);
    }
    if reachable.is_empty() {
        return Ok(literal(validated_type, Literal::Null));
    }
    let other = reachable.pop().unwrap();
    if reachable.is_empty() {
        return Ok(other);
    }
    let ty = reachable
        .iter()
        .try_fold(other.ty.clone(), |ty, value| common_type(&ty, &value.ty))?;
    let branches = reachable
        .into_iter()
        .map(|value| {
            (
                bool_expr(Kind::IsNull(Box::new(value.clone()), true)),
                value,
            )
        })
        .collect();
    Ok(Expr {
        ty,
        kind: Kind::Case(branches, Box::new(other)),
    })
}

pub(super) fn case(branches: Vec<(Expr, Expr)>, mut other: Expr) -> Result<Expr> {
    let validated_type = branches
        .iter()
        .try_fold(other.ty.clone(), |ty, (_, value)| {
            common_type(&ty, &value.ty)
        })?;
    let mut reachable = Vec::new();
    for (condition, value) in branches {
        match constant(&condition).map(|e| e.kind) {
            Some(Kind::Literal(Literal::Bool(false) | Literal::Null)) => {}
            Some(Kind::Literal(Literal::Bool(true))) => {
                other = value;
                break;
            }
            _ => reachable.push((condition, value)),
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
    Ok(Expr {
        ty,
        kind: Kind::Case(reachable, Box::new(other)),
    })
}
