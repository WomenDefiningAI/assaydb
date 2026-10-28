//! DISTINCT ON selects from the completed, qualified projection. Reuse the
//! funded/spilling window engine rather than an unaccounted in-memory key set.
use super::*;
use crate::expr::{Kind, Literal, Op};
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

const EXPANSION_ERROR: &str = "resource: DISTINCT ON alias expansion budget exceeded";

// Account for appended key trees and copied alias payload before cloning. These
// are planning limits, independent of the execution allocator's memory budget.
struct ExpansionBudget {
    nodes: usize,
    bytes: usize,
}
impl ExpansionBudget {
    fn charge(&mut self, expression: &s::Expr) -> Result<()> {
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
        let mut count = Count {
            nodes: 0,
            limit: self.nodes,
        };
        if expression.visit(&mut count).is_break() {
            return Err(EXPANSION_ERROR.into());
        }
        let bytes = expression.to_string().len();
        if bytes > self.bytes {
            return Err(EXPANSION_ERROR.into());
        }
        self.nodes -= count.nodes;
        self.bytes -= bytes;
        Ok(())
    }
}

fn unnest(mut expr: &s::Expr) -> &s::Expr {
    while let s::Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}
fn name(id: &s::Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_lowercase()
    }
}
fn expression(item: &s::SelectItem) -> &s::Expr {
    match item {
        s::SelectItem::UnnamedExpr(expr) | s::SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => unreachable!("wildcards already expanded"),
    }
}

/// Resolve user positions against only the original output. ORDER BY adds its
/// hidden columns first; ON keys are then projected through the normal aggregate,
/// subquery and window lowering alongside the visible expressions.
pub(super) fn prepare(
    select: &mut s::Select,
    order: &mut Option<s::OrderBy>,
    input: &Schema,
) -> Result<Option<Vec<usize>>> {
    let keys = if matches!(select.distinct, Some(s::Distinct::On(_))) {
        let Some(s::Distinct::On(keys)) = select.distinct.take() else {
            unreachable!()
        };
        Some(keys)
    } else {
        None
    };
    let visible = select.projection.len();
    select_modifiers::order_projection(select, order, input)?;
    let Some(keys) = keys else {
        return Ok(None);
    };
    let mut columns = Vec::with_capacity(keys.len());
    let mut expansion = ExpansionBudget {
        nodes: crate::nesting::HEIGHT_LIMIT,
        bytes: crate::session::MAX_SQL_BYTES,
    };
    for key in keys {
        let expr = unnest(&key);
        let index = if let s::Expr::Value(value) = expr {
            match &value.value {
                s::Value::Number(number, _) if number.bytes().all(|b| b.is_ascii_digit()) => Some(
                    number
                        .parse::<usize>()
                        .ok()
                        .and_then(|n| n.checked_sub(1))
                        .filter(|n| *n < visible)
                        .ok_or("DISTINCT ON ordinal out of range")?,
                ),
                _ => return Err("unsupported: DISTINCT ON non-integer literal".into()),
            }
        } else if let s::Expr::UnaryOp {
            op: s::UnaryOperator::Minus,
            expr,
        } = expr
            && matches!(unnest(expr), s::Expr::Value(_))
        {
            return Err("DISTINCT ON ordinal out of range".into());
        } else if let s::Expr::Identifier(id) = expr {
            let wanted = name(id);
            let hits = select.projection[..visible]
                .iter()
                .enumerate()
                .filter(|(_, item)| match item {
                    s::SelectItem::ExprWithAlias { alias, .. } => name(alias) == wanted,
                    s::SelectItem::UnnamedExpr(expr) => expr.to_string() == wanted,
                    _ => false,
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            // DuckDB resolves a bare duplicate alias to the last output.
            hits.last().copied()
        } else {
            None
        };
        let index = index.or_else(|| {
            select
                .projection
                .iter()
                .position(|item| unnest(expression(item)) == expr)
        });
        columns.push(if let Some(index) = index { index } else {
            let index = select.projection.len();
            let mut key = key;
            expansion.charge(&key)?;
            let mut changed = false;
            // Inside an expression input columns win; absent input names may
            // resolve to output aliases. A whole bare key above is alias-first.
            syntax::rewrite(&mut key, &mut |part| {
                if let s::Expr::Identifier(id) = part {
                    let wanted = name(id);
                    if binder(input).expr(part).is_err()
                        && let Some(item) = select.projection[..visible].iter().rev().find(|item| {
                            matches!(item, s::SelectItem::ExprWithAlias { alias, .. } if name(alias) == wanted)
                        }) {
                            expansion.charge(expression(item))?;
                            *part = expression(item).clone();
                            changed = true;
                            return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if changed && crate::nesting::measure(&format!("SELECT {key}"))
                .map_err(crate::Error::from)?.is_none()
            {
                return Err("resource: DISTINCT ON expanded expression exceeds nesting limits".into());
            }
            select.projection.push(s::SelectItem::UnnamedExpr(key));
            index
        });
    }
    Ok(Some(columns))
}

fn column(input: &Node, index: usize) -> Result<Expr> {
    let ty = Type::from_field(input.schema.field(index))?;
    Ok(Expr {
        kind: if ty == Type::Null {
            Kind::Literal(Literal::Null)
        } else {
            Kind::Column(index)
        },
        ty,
    })
}

/// Called after user QUALIFY and before dropping hidden projected keys or LIMIT.
/// The row-number engine retains encounter ordinals for ties and restores its
/// input order; apply the final sort to the survivors without changing schemas.
pub(super) fn apply(mut input: Node, keys: &[usize]) -> Result<Node> {
    let RelOp::Unary { plan, .. } = &mut input.op else {
        return Err("internal: DISTINCT ON requires completed projection".into());
    };
    let order = std::mem::take(&mut plan.order);
    let input = window::canonical_input(input)?;
    let specification = window::Specification {
        function: window::Ranking::RowNumber,
        aggregate: None,
        offset: None,
        value: None,
        frame: window::Frame::Partition,
        partition: keys
            .iter()
            .map(|&index| column(&input, index))
            .collect::<Result<Vec<_>>>()?,
        order: order
            .iter()
            .map(|sort| {
                Ok((
                    column(&input, sort.column)?,
                    sort.descending,
                    sort.nulls_first,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let width = input.schema.fields().len();
    let original_schema = input.schema.clone();
    let mut fields = input.schema.fields().to_vec();
    fields.push(Arc::new(
        specification
            .result_type()
            .field("__distinct_on_ordinal", false),
    ));
    let mut node = Node {
        schema: Arc::new(Schema::new(fields)),
        estimated_rows: input.estimated_rows,
        op: RelOp::Window {
            input: Box::new(input),
            specification: Box::new(specification),
        },
    };
    let predicate = Expr {
        ty: Type::Bool,
        kind: Kind::Binary(
            Op::Eq,
            Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Column(width),
            }),
            Box::new(Expr {
                ty: Type::Int,
                kind: Kind::Literal(Literal::Num(1)),
            }),
        ),
    };
    node = Node {
        schema: node.schema.clone(),
        estimated_rows: node.estimated_rows,
        op: RelOp::Filter {
            input: Box::new(node),
            predicate,
        },
    };
    if !order.is_empty() {
        node = Node {
            schema: node.schema.clone(),
            estimated_rows: node.estimated_rows,
            op: RelOp::WindowOrder {
                input: Box::new(node),
                order,
            },
        };
    }
    Ok(Node {
        schema: original_schema,
        estimated_rows: node.estimated_rows,
        op: RelOp::Project {
            input: Box::new(node),
            columns: (0..width).collect(),
        },
    })
}
