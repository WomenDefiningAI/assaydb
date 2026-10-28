//! Bind all original alternatives, then exclude only proved unreachable result
//! arms from type promotion. Keep conditions: their eager errors still belong
//! to the query even when their Boolean result is known.
use super::*;

fn bounded(e: &Expr, left: &mut usize) -> bool {
    if *left == 0 {
        return false;
    }
    *left -= 1;
    match &e.kind {
        Kind::Literal(_) | Kind::Column(_) => true,
        Kind::Neg(a)
        | Kind::Not(a)
        | Kind::IsNull(a, _)
        | Kind::Cast(a)
        | Kind::DialectCast(a, _)
        | Kind::CastVarchar(a, _)
        | Kind::CastDecimal(a, _, _)
        | Kind::Function(_, a)
        | Kind::Year(a)
        | Kind::Substring(a, _, _) => bounded(a, left),
        Kind::Binary(_, a, b)
        | Kind::NullIf(a, b)
        | Kind::Position(a, b)
        | Kind::Like(a, b, _)
        | Kind::Pack(a, b) => bounded(a, left) && bounded(b, left),
        Kind::Case(branches, other) => {
            branches
                .iter()
                .all(|(c, v)| bounded(c, left) && bounded(v, left))
                && bounded(other, left)
        }
        Kind::Call(_, args) | Kind::Math(_, args) => args.iter().all(|a| bounded(a, left)),
        Kind::SortPack(..) => false,
    }
}
fn null() -> Expr {
    Expr {
        ty: Type::Null,
        kind: Kind::Literal(Literal::Null),
    }
}

pub(crate) fn normalize_expression(
    e: Expr,
    schema: &Schema,
    facts: Option<&[Fact]>,
) -> Result<Expr> {
    normalize_literal_arms(e, schema, facts, &[])
}

pub(crate) fn normalize_literal_arms(
    mut e: Expr,
    schema: &Schema,
    facts: Option<&[Fact]>,
    _literal_arms: &[bool],
) -> Result<Expr> {
    // Recursive Binder calls normalize children first, so ancestors are bound
    // from the corrected types. No second evaluator or post-planning retyping.
    if !matches!(e.kind, Kind::Case(..)) || !bounded(&e, &mut 256) {
        return Ok(e);
    }
    let fallback;
    let input = if let Some(facts) = facts {
        facts
    } else {
        fallback = fields(schema)?;
        &fallback
    };
    let Kind::Case(branches, other) = &mut e.kind else {
        unreachable!()
    };
    let mut ty = Type::Null;
    let mut end = branches.len();
    let mut reaches_else = true;
    let mut dead = Vec::new();
    let mut float_value = false;
    for (i, (condition, value)) in branches.iter().enumerate() {
        // Nonrejecting facts: final whole-query guard runs after ordinary name
        // and type binding, including later projections and unreachable arms.
        let fact = expression_mode(condition, input, false)?;
        if fact.nulls == NULL || fact.boolean == Some(false) {
            dead.push(i);
            continue;
        }
        ty = crate::bind::common_type(&ty, &value.ty)?;
        if e.ty == Type::Float && !float_value {
            float_value = expression_mode(value, input, false)?.classes & FLOAT != 0;
        }
        if fact.boolean == Some(true) && fact.nulls == VALUE {
            end = i + 1;
            reaches_else = false;
            break;
        }
    }
    if reaches_else {
        ty = crate::bind::common_type(&ty, &other.ty)?;
        if e.ty == Type::Float && !float_value {
            float_value = expression_mode(other, input, false)?.classes & FLOAT != 0;
        }
    }
    // Keep the established dead-arm normalization. Every reachable integer
    // arm of a mixed Float conditional converts to binary64 (Helen, October
    // 15: promote like DuckDB 1.5.6, accepting rounding above 2^53). Exact
    // integer literals (|n| <= 2^53) keep the established exact Float literal;
    // every other Int arm is marked so consumers can see promoted provenance.
    if ty == Type::Float && float_value {
        let promote = |value: &mut Expr| {
            if value.ty != Type::Int {
                return;
            }
            crate::bind::branch_types::promote_float_literal(value);
            if value.ty == Type::Int {
                *value = Expr {
                    ty: Type::Float,
                    kind: Kind::Function(super::PROMOTE.into(), Box::new(value.clone())),
                };
            }
        };
        for (i, (_, value)) in branches.iter_mut().take(end).enumerate() {
            if !dead.contains(&i) {
                promote(value);
            }
        }
        if reaches_else {
            promote(other);
        }
    }
    // This is a type repair, not general dependency pruning. When the reachable
    // type already matches, retain the original CASE and every dependency;
    // grouped binding and its eager error/name precedence remain unchanged.
    // A wholly untyped NULL keeps its existing contextual type as before.
    if ty == Type::Null || ty == e.ty {
        return Ok(e);
    }
    for i in dead {
        branches[i].1 = null();
    }
    branches.truncate(end);
    if !reaches_else {
        **other = null();
    }
    e.ty = ty;
    Ok(e)
}
