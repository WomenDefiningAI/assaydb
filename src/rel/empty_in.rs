//! Approved empty-list semantics, scoped to normally bound but unused operands.
//! This is not general dead-code elimination: live expressions and aggregates
//! retain their original evaluation/error behavior.
use super::*;

fn empty_operand(expr: &s::Expr) -> Option<(&s::Expr, bool)> {
    match expr {
        s::Expr::InList {
            expr,
            list,
            negated,
        } if list.is_empty() => Some((expr, *negated)),
        _ => None,
    }
}
impl Planner<'_> {
    /// Validate lifted subqueries against a disposable copy of the input graph.
    /// No temporary scalar/marker/join node reaches the executable graph.
    pub(super) fn bind_empty_in(
        &mut self,
        expr: &mut s::Expr,
        input: &Node,
        ctes: &HashMap<String, Node>,
    ) -> Result<bool> {
        let Some((operand, negated)) = empty_operand(expr) else {
            return Ok(false);
        };
        let mut operand = operand.clone();
        let validation = self.subqueries(&mut operand, input.clone(), ctes)?;
        binder(&validation.schema).expr(&operand)?;
        *expr = s::Expr::Value(s::Value::Boolean(negated).into());
        Ok(true)
    }
}

/// Keep the visible default name when subquery rewriting replaces the whole IN.
pub(super) fn preserve_projection_name(item: &mut s::SelectItem) -> Result<()> {
    let s::SelectItem::UnnamedExpr(expr) = item else {
        return Ok(());
    };
    let mut found = false;
    syntax::rewrite(&mut expr.clone(), &mut |expr| {
        found |= empty_operand(expr).is_some();
        Ok(false)
    })?;
    if found {
        *item = s::SelectItem::ExprWithAlias {
            alias: s::Ident::with_quote('"', expr.to_string()),
            expr: expr.clone(),
        };
    }
    Ok(())
}

/// Preserve the ordinary unique aggregate order while recording live uses.
/// Repeated occurrences outside empty operands make the shared aggregate live.
/// Window calls are not ordinary aggregates. Traversal reaches their OVER keys,
/// however, so SUM(x) inside a ranking key belongs to the grouped input stage.
/// Empty operands retain the same validation-only aggregate suppression there;
/// the window planner likewise validates and discards their unused windows.
pub(super) fn aggregate_usage(select: &s::Select) -> Result<(Vec<s::Expr>, Vec<bool>)> {
    fn visit(
        expr: &s::Expr,
        dead: bool,
        aggregates: &mut Vec<s::Expr>,
        live: &mut Vec<bool>,
    ) -> Result<()> {
        syntax::rewrite(&mut expr.clone(), &mut |expr| {
            if let Some((operand, _)) = empty_operand(expr) {
                visit(operand, true, aggregates, live)?;
                return Ok(true);
            }
            if syntax::is_aggregate(expr) {
                if let Some(index) = aggregates.iter().position(|old| old == expr) {
                    live[index] |= !dead;
                } else {
                    aggregates.push(expr.clone());
                    live.push(!dead);
                }
                return Ok(true);
            }
            Ok(false)
        })
    }
    let mut aggregates = Vec::new();
    let mut live = Vec::new();
    for item in &select.projection {
        let expr = match item {
            s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Err("unsupported: wildcard projection".into()),
        };
        visit(expr, false, &mut aggregates, &mut live)?;
    }
    if let Some(expr) = &select.having {
        visit(expr, false, &mut aggregates, &mut live)?;
    }
    Ok((aggregates, live))
}

/// Called only AFTER binding the unchanged inner aggregate query. Keep its
/// groups, output schema, scalar empty-input row and original type diagnostics.
/// Typed NULL inputs remove only runtime operand work that empty IN discards.
pub(super) fn suppress_dead_inputs(node: &mut Node, live: &[bool]) -> Result<()> {
    let RelOp::Unary { plan, .. } = &mut node.op else {
        return Err("internal: empty IN expected bound aggregate plan".into());
    };
    if plan.aggregates.len() != live.len() {
        return Err("internal: empty IN aggregate usage mismatch".into());
    }
    if live.iter().all(|live| *live) {
        return Ok(());
    }
    for (aggregate, &live) in plan.aggregates.iter_mut().zip(live) {
        if !live && let Some(input) = &mut aggregate.input {
            input.kind = crate::expr::Kind::Literal(crate::expr::Literal::Null);
        }
    }
    // Column liveness is recomputed from the now-bound executable expressions;
    // grouping/source cardinality remains present even if no value is needed.
    let mut columns = std::collections::BTreeSet::new();
    if let Some(expr) = &plan.filter {
        expr.columns(&mut columns);
    }
    for expr in &plan.groups {
        expr.columns(&mut columns);
    }
    for aggregate in &plan.aggregates {
        if let Some(expr) = &aggregate.input {
            expr.columns(&mut columns);
        }
    }
    for output in &plan.output {
        if let bind::Output::Expr(expr) = output {
            expr.columns(&mut columns);
        }
    }
    plan.columns = columns.into_iter().collect();
    Ok(())
}
