//! Infer GROUP BY ALL from visible SELECT expressions, before hidden ordering
//! and QUALIFY columns are appended. Existing explicit groups are untouched.
use super::*;
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

const EXPANSION_ERROR: &str = "resource: GROUP BY ALL alias expansion budget exceeded";

#[derive(Clone, Copy)]
struct Cost {
    nodes: usize,
    bytes: usize,
}

fn cost(expr: &s::Expr, limit: usize) -> Result<Cost> {
    struct Count {
        nodes: usize,
        limit: usize,
    }
    impl Visitor for Count {
        type Break = ();
        fn pre_visit_expr(&mut self, _: &s::Expr) -> ControlFlow<()> {
            if self.nodes == self.limit {
                return ControlFlow::Break(());
            }
            self.nodes += 1;
            ControlFlow::Continue(())
        }
    }
    let mut count = Count { nodes: 0, limit };
    if expr.visit(&mut count).is_break() {
        return Err(EXPANSION_ERROR.into());
    }
    Ok(Cost {
        nodes: count.nodes,
        bytes: expr.to_string().len(),
    })
}

fn charge(remaining: &mut Cost, used: Cost) -> Result<()> {
    if used.nodes > remaining.nodes || used.bytes > remaining.bytes {
        return Err(EXPANSION_ERROR.into());
    }
    remaining.nodes -= used.nodes;
    remaining.bytes -= used.bytes;
    Ok(())
}

fn alias_key(id: &s::Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_lowercase()
    }
}

/// Only a bare, one-column SELECT of a literal or outer identifier is safely
/// inlined here. No FROM, predicate, modifiers, functions, or evaluation-count
/// dependent expression may cross this scope boundary.
fn trivial_scalar(expr: &s::Expr, template: &s::Query) -> Option<s::Expr> {
    let s::Expr::Subquery(query) = expr else {
        return None;
    };
    let s::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [item] = select.projection.as_slice() else {
        return None;
    };
    let value = match item {
        s::SelectItem::UnnamedExpr(value) | s::SelectItem::ExprWithAlias { expr: value, .. } => {
            value
        }
        _ => return None,
    };
    if !matches!(
        value,
        s::Expr::Value(_) | s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
    ) {
        return None;
    }
    // Compare every other AST field with a bare SELECT, including future
    // modifiers represented in Query/Select. Token position is nonsemantic.
    let mut shape = query.as_ref().clone();
    let s::SetExpr::Select(shape_select) = shape.body.as_mut() else {
        unreachable!()
    };
    let s::SetExpr::Select(template_select) = template.body.as_ref() else {
        unreachable!()
    };
    shape_select.select_token = template_select.select_token.clone();
    shape_select.projection = template_select.projection.clone();
    (shape == *template).then(|| value.clone())
}

fn expression(item: &s::SelectItem) -> &s::Expr {
    match item {
        s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => unreachable!("wildcards already expanded"),
    }
}

fn same_output(left: &s::Expr, right: &s::Expr, input: &Schema) -> bool {
    fn unnest(mut expr: &s::Expr) -> &s::Expr {
        while let s::Expr::Nested(inner) = expr {
            expr = inner;
        }
        expr
    }
    let (left, right) = (unnest(left), unnest(right));
    if left == right {
        return true;
    }
    // Qualified and unqualified spellings of the very same source column
    // are identical outputs. Do not equate arbitrary expressions or different
    // columns merely because their display aliases collide.
    if matches!(
        left,
        s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
    ) && matches!(
        right,
        s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_)
    ) && let (Ok(left), Ok(right)) = (binder(input).expr(left), binder(input).expr(right))
        && let (crate::expr::Kind::Column(left), crate::expr::Kind::Column(right)) =
            (left.kind, right.kind)
    {
        return left == right;
    }
    false
}

pub(super) fn lower(
    select: &mut s::Select,
    order: &mut Option<s::OrderBy>,
    input: &Schema,
) -> Result<()> {
    let s::GroupByExpr::All(modifiers) = &select.group_by else {
        return Ok(());
    };
    if !modifiers.is_empty() {
        return Err("unsupported: GROUP BY ALL modifiers".into());
    }
    let template = bind::parse("SELECT 1")?;
    let mut groups = Vec::new();
    let mut aliases = HashMap::<String, (s::Expr, Cost)>::new();
    // An alias chain can multiply a small parsed AST exponentially. Charge
    // both syntax nodes and text payload before cloning any stored alias.
    let mut remaining = Cost {
        nodes: crate::nesting::HEIGHT_LIMIT,
        bytes: crate::session::MAX_SQL_BYTES,
    };
    for item in &mut select.projection {
        let (source, output_alias) = match item {
            s::SelectItem::UnnamedExpr(expr) => (expr, None),
            s::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.clone())),
            _ => return Err("unsupported: GROUP BY ALL requires expanded wildcards".into()),
        };
        let source_cost = cost(source, remaining.nodes)?;
        charge(&mut remaining, source_cost)?;
        let mut expr = source.clone();
        let original = expr.clone();
        syntax::rewrite(&mut expr, &mut |part| {
            if let Some(value) = trivial_scalar(part, &template) {
                *part = value;
            }
            if let s::Expr::Identifier(id) = part
                && binder(input)
                    .expr(&s::Expr::Identifier(id.clone()))
                    .is_err()
                && let Some((value, value_cost)) = aliases.get(&alias_key(id))
            {
                charge(&mut remaining, *value_cost)?;
                *part = value.clone();
                return Ok(true);
            }
            Ok(false)
        })?;
        if expr != original
            && crate::nesting::measure(&format!("SELECT {expr}"))
                .map_err(crate::Error::from)?
                .is_none()
        {
            return Err("resource: GROUP BY ALL expanded expression exceeds nesting limits".into());
        }
        let mut column = false;
        let mut aggregate = false;
        let mut window = false;
        syntax::rewrite(&mut expr.clone(), &mut |part| {
            if syntax::is_aggregate(part) {
                aggregate = true;
                return Ok(true);
            }
            match part {
                s::Expr::Identifier(_) | s::Expr::CompoundIdentifier(_) => column = true,
                s::Expr::Function(function) if function.over.is_some() => window = true,
                s::Expr::Subquery(_) | s::Expr::Exists { .. } | s::Expr::InSubquery { .. } => {
                    return Err("unsupported: GROUP BY ALL subquery composition".into());
                }
                _ => {}
            }
            Ok(false)
        })?;
        if column && aggregate {
            return Err("semantic: GROUP BY ALL cannot mix aggregates with non-aggregated columns in one expression".into());
        }
        if column && window {
            return Err("semantic: GROUP BY ALL cannot group on a window clause".into());
        }
        if column && !groups.contains(&expr) {
            groups.push(expr.clone());
        }
        if let Some(alias) = output_alias {
            let expanded_cost = cost(&expr, crate::nesting::HEIGHT_LIMIT)?;
            aliases.insert(alias_key(&alias), (expr.clone(), expanded_cost));
            *item = s::SelectItem::ExprWithAlias { expr, alias };
        } else if expr != original {
            // Expanding an earlier alias or trivial subquery must not rename
            // this visible output to an implementation-generated expression.
            *item = s::SelectItem::ExprWithAlias {
                expr,
                alias: s::Ident::with_quote('"', original.to_string()),
            };
        }
    }
    // GROUP BY ALL accepts repeated copies of a grouping output. Resolve an
    // ORDER BY name shared only by identical outputs to the first ordinal;
    // leave genuinely ambiguous aliases to the unchanged ordering validator.
    if let Some(s::OrderBy {
        kind: s::OrderByKind::Expressions(keys),
        ..
    }) = order
    {
        for key in keys {
            let mut expr = &key.expr;
            while let s::Expr::Nested(inner) = expr {
                expr = inner;
            }
            let s::Expr::Identifier(id) = expr else {
                continue;
            };
            let wanted = alias_key(id);
            let hits = select
                .projection
                .iter()
                .enumerate()
                .filter(|(_, item)| match item {
                    s::SelectItem::ExprWithAlias { alias, .. } => alias_key(alias) == wanted,
                    s::SelectItem::UnnamedExpr(expr) => expr.to_string() == wanted,
                    _ => false,
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if let [first, rest @ ..] = hits.as_slice()
                && !rest.is_empty()
                && rest.iter().all(|index| {
                    same_output(
                        expression(&select.projection[*first]),
                        expression(&select.projection[*index]),
                        input,
                    )
                })
            {
                key.expr = s::Expr::Value(s::Value::Number((first + 1).to_string(), false).into());
            }
        }
    }
    select.group_by = s::GroupByExpr::Expressions(groups, Vec::new());
    Ok(())
}
