//! Resolve final SELECT ordering before aggregate/window extraction. Hidden
//! sort expressions use the same typed projection and completed sort as outputs.
use super::*;

fn unnest(mut e: &s::Expr) -> &s::Expr {
    while let s::Expr::Nested(inner) = e {
        e = inner;
    }
    e
}

fn name(i: &s::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    }
}

fn expression(item: &s::SelectItem) -> &s::Expr {
    match item {
        s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } => e,
        _ => unreachable!("wildcards already expanded"),
    }
}

fn equivalent(a: &s::Expr, b: &s::Expr, input: &Schema) -> bool {
    if unnest(a) == unnest(b) {
        return true;
    }
    fn normalized(e: &s::Expr, input: &Schema) -> Result<s::Expr> {
        let mut e = e.clone();
        syntax::rewrite(&mut e, &mut |part| {
            *part = unnest(part).clone();
            if matches!(
                part,
                s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
            ) {
                let bound = binder(input).expr(part)?;
                if let crate::expr::Kind::Column(column) = bound.kind {
                    *part = s::Expr::Identifier(s::Ident::with_quote(
                        '"',
                        format!("source column {column}"),
                    ));
                }
            }
            if let s::Expr::Function(function) = part {
                for part in &mut function.name.0 {
                    if let s::ObjectNamePart::Identifier(id) = part {
                        *id = s::Ident::with_quote('"', name(id));
                    }
                }
            }
            Ok(false)
        })?;
        Ok(e)
    }
    // Match syntax modulo resolved column identity, identifier case and
    // parentheses. Do not equate arbitrary expressions merely because the
    // evaluator folds them (DISTINCT must still reject unprojected expressions).
    matches!((normalized(a, input), normalized(b, input)), (Ok(a), Ok(b)) if a == b)
}

/// FETCH ... ONLY uses the same completed-relation limit as LIMIT, including
/// compound queries. Reject unsupported peer/percentage forms explicitly;
/// treating WITH TIES as a plain limit would silently discard result rows.
pub(super) fn lower_fetch(query: &mut s::Query, options: &Options) -> Result<()> {
    let Some(fetch) = query.fetch.take() else {
        return Ok(());
    };
    if fetch.with_ties || fetch.percent {
        return Err("unsupported: FETCH WITH TIES/PERCENT".into());
    }
    let offset = match query.limit_clause.take() {
        None => None,
        Some(s::LimitClause::LimitOffset {
            limit: None,
            offset,
            limit_by,
        }) if limit_by.is_empty() => offset,
        _ => return Err("FETCH cannot be combined with LIMIT".into()),
    };
    options.cancellation.check()?;
    let quantity = fetch
        .quantity
        .unwrap_or_else(|| s::Expr::Value(s::Value::Number("1".into(), false).into()));
    let empty = Schema::empty();
    let mut bound = binder(&empty).expr(&quantity)?;
    if bound.ty == Type::Text {
        bound = crate::expr::cast_dialect::bind(
            bound,
            &s::DataType::Decimal(s::ExactNumberInfo::PrecisionAndScale(38, 0)),
        )?;
    }
    if bound.ty == Type::Bool {
        bound = crate::expr::casts::bind(bound, Type::Int)?;
    }
    let bound = crate::expr::cast_dialect::bind(bound, &s::DataType::BigInt(None))?;
    let budget = Budget::new(options.memory_bytes);
    let value = bound.eval(
        &Input {
            columns: Vec::new(),
            len: 1,
        },
        &budget,
    )?;
    let quantity = if is_sql_null(value.array.as_ref(), 0) {
        None
    } else {
        let quantity = crate::expr::numeric(&value.array).value(0);
        if quantity < 0 {
            return Err("FETCH count must not be negative".into());
        }
        Some(s::Expr::Value(
            s::Value::Number(quantity.to_string(), false).into(),
        ))
    };
    query.limit_clause = Some(s::LimitClause::LimitOffset {
        limit: quantity,
        offset,
        limit_by: Vec::new(),
    });
    Ok(())
}

pub(super) fn pagination(clause: Option<s::LimitClause>) -> Result<Option<(usize, usize)>> {
    fn integer(e: s::Expr, label: &str) -> Result<usize> {
        if let s::Expr::Value(v) = e
            && let s::Value::Number(n, _) = v.value
            && n.bytes().all(|b| b.is_ascii_digit())
        {
            return Ok(n
                .parse::<usize>()
                .ok()
                .filter(|n| *n <= i64::MAX as usize)
                .ok_or_else(|| format!("{label} out of range"))?);
        }
        Err(format!("unsupported: {label} requires a non-negative integer literal").into())
    }
    match clause {
        None => Ok(None),
        Some(s::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => {
            let count = limit
                .map(|e| integer(e, "LIMIT"))
                .transpose()?
                .unwrap_or(usize::MAX);
            let offset = offset
                .map(|e| integer(e.value, "OFFSET"))
                .transpose()?
                .unwrap_or(0);
            Ok(Some((count, offset)))
        }
        _ => Err("unsupported: LIMIT comma syntax or LIMIT BY".into()),
    }
}

pub(super) fn order_projection(
    select: &mut s::Select,
    order: &mut Option<s::OrderBy>,
    input: &Schema,
) -> Result<()> {
    if matches!(select.distinct, Some(s::Distinct::On(_))) {
        return Err("unsupported: DISTINCT ON".into());
    }
    let Some(s::OrderBy {
        kind: s::OrderByKind::Expressions(keys),
        ..
    }) = order
    else {
        return Ok(());
    };
    let visible = select.projection.len();
    for key in keys {
        let e = unnest(&key.expr);
        if let s::Expr::UnaryOp {
            op: s::UnaryOperator::Minus,
            expr,
        } = e
            && let s::Expr::Value(v) = unnest(expr)
        {
            if matches!(&v.value, s::Value::Number(n, _) if n.bytes().all(|b| b.is_ascii_digit())) {
                return Err("ORDER BY ordinal out of range".into());
            }
            return Err("unsupported: ORDER BY negative non-integer literal".into());
        }
        let index = if let s::Expr::Value(v) = e {
            match &v.value {
                s::Value::Number(n, _) => Some(
                    n.parse::<usize>()
                        .ok()
                        .and_then(|n| n.checked_sub(1))
                        .filter(|n| *n < visible)
                        .ok_or("ORDER BY ordinal out of range")?,
                ),
                _ => return Err("unsupported: ORDER BY literal".into()),
            }
        } else if let s::Expr::Identifier(id) = e {
            let wanted = name(id);
            let hits = select.projection[..visible]
                .iter()
                .enumerate()
                .filter(|(_, item)| match item {
                    s::SelectItem::ExprWithAlias { alias, .. } => name(alias) == wanted,
                    s::SelectItem::UnnamedExpr(e) => e.to_string() == wanted,
                    _ => false,
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if hits.len() > 1 {
                return Err("ORDER BY name must identify one output column".into());
            }
            hits.first().copied()
        } else {
            None
        };
        let index = index.or_else(|| {
            select
                .projection
                .iter()
                .position(|item| equivalent(e, expression(item), input))
        });
        let index = if let Some(index) = index {
            index
        } else {
            if select.distinct.is_some() {
                return Err("DISTINCT ORDER BY expression must appear in SELECT".into());
            }
            let index = select.projection.len();
            select
                .projection
                .push(s::SelectItem::UnnamedExpr(key.expr.clone()));
            index
        };
        // Positional references cannot collide with user aliases, and hidden
        // columns never extend the range of user-written ordinals.
        key.expr = s::Expr::Value(s::Value::Number((index + 1).to_string(), false).into());
    }
    Ok(())
}
