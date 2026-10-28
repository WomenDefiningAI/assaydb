//! DISTINCT set operations reuse the existing exact grouped-state engine.
//!
//! Side tags encode membership, not row counts: MIN/MAX distinguish left-only,
//! right-only and shared groups, including NULLs. This avoids a separate hash
//! table implementation and inherits grouping's reservations and spill paths.
use super::{Node, RelOp, schema};
use crate::{
    Result,
    bind::{Agg, Aggregate, Output, Plan},
    expr::{Expr, Kind, Literal, Op},
    types::Type,
};
use sqlparser::ast::SetOperator;

fn column(index: usize, ty: &Type) -> Expr {
    Expr {
        ty: ty.clone(),
        kind: Kind::Column(index),
    }
}

fn number(value: i128) -> Expr {
    Expr {
        ty: Type::Int,
        kind: Kind::Literal(Literal::Num(value)),
    }
}

fn projection(input: &Node, names: Vec<String>) -> Result<Plan> {
    let types = input
        .schema
        .fields()
        .iter()
        .map(|field| Type::from_field(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(Plan {
        table: None,
        path: None,
        columns: (0..types.len()).collect(),
        filter: None,
        groups: Vec::new(),
        aggregates: Vec::new(),
        output: types
            .iter()
            .enumerate()
            .map(|(i, ty)| Output::Expr(column(i, ty)))
            .collect(),
        names,
        types,
        order: Vec::new(),
        aggregation: false,
        projection_pushdown: true,
    })
}

fn unary(input: Node, plan: Plan) -> Node {
    Node {
        schema: schema(&plan.names, &plan.types),
        estimated_rows: input.estimated_rows,
        op: RelOp::Unary {
            plan,
            input: Box::new(input),
        },
    }
}

/// Align UNION BY NAME inputs before ordinary set type reconciliation. Missing
/// columns are NULL expressions, so they acquire the other side's type through
/// the same union normalization as positional sets.
pub(super) fn align_by_name(left: Node, right: Node) -> Result<(Node, Node)> {
    fn indices(input: &Node) -> Result<std::collections::HashMap<String, usize>> {
        let mut indices = std::collections::HashMap::new();
        for (index, field) in input.schema.fields().iter().enumerate() {
            if indices.insert(field.name().to_lowercase(), index).is_some() {
                return Err(
                    format!("UNION BY NAME duplicate column name: {}", field.name()).into(),
                );
            }
        }
        Ok(indices)
    }
    fn align(
        input: Node,
        names: &[String],
        indices: &std::collections::HashMap<String, usize>,
    ) -> Result<Node> {
        let mut plan = projection(&input, names.to_vec())?;
        plan.output.clear();
        plan.types.clear();
        for name in names {
            let expr = if let Some(&index) = indices.get(&name.to_lowercase()) {
                column(index, &Type::from_field(input.schema.field(index))?)
            } else {
                Expr {
                    ty: Type::Null,
                    kind: Kind::Literal(Literal::Null),
                }
            };
            plan.types.push(expr.ty.clone());
            plan.output.push(Output::Expr(expr));
        }
        Ok(unary(input, plan))
    }
    let left_indices = indices(&left)?;
    let right_indices = indices(&right)?;
    let mut names: Vec<String> = left
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    names.extend(
        right
            .schema
            .fields()
            .iter()
            .filter(|f| !left_indices.contains_key(&f.name().to_lowercase()))
            .map(|f| f.name().clone()),
    );
    Ok((
        align(left, &names, &left_indices)?,
        align(right, &names, &right_indices)?,
    ))
}

fn tagged(input: Node, names: &[String], side: i128) -> Result<Node> {
    let mut plan = projection(&input, names.to_vec())?;
    plan.output.push(Output::Expr(number(side)));
    plan.types.push(Type::Int);
    plan.names.push("__set_side".into());
    Ok(unary(input, plan))
}

/// DISTINCT belongs after the complete projected rows (including aggregates
/// and HAVING), but before result ordering and LIMIT. Grouping all output
/// positions reuses the existing funded equality/state/output and spill paths.
/// Never group the original input columns: different rows can project equally.
pub(super) fn deduplicate_projection(mut input: Node) -> Result<Node> {
    let RelOp::Unary {
        plan: projected, ..
    } = &mut input.op
    else {
        return Err("internal: DISTINCT expected a bound projection".into());
    };
    let order = std::mem::take(&mut projected.order);
    let names = input
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut plan = projection(&input, names)?;
    plan.aggregation = true;
    plan.groups = plan
        .types
        .iter()
        .enumerate()
        .map(|(index, ty)| column(index, ty))
        .collect();
    plan.output = (0..plan.types.len()).map(Output::Group).collect();
    plan.order = order;
    Ok(unary(input, plan))
}

pub(super) fn distinct(
    left: Node,
    right: Node,
    operation: SetOperator,
    names: Vec<String>,
    types: Vec<Type>,
) -> Result<Node> {
    let width = types.len();
    distinct_prefix(left, right, operation, names, types, width)
}

/// DISTINCT set operation over all `types` columns, returning only the first
/// `visible` of them (private trailing key columns are dropped in the same
/// final projection).
fn distinct_prefix(
    left: Node,
    right: Node,
    operation: SetOperator,
    names: Vec<String>,
    types: Vec<Type>,
    visible: usize,
) -> Result<Node> {
    let width = types.len();
    let keys = (0..width)
        .map(|i| format!("__set_key_{i}"))
        .collect::<Vec<_>>();
    let membership = operation != SetOperator::Union;
    let (left, right) = if membership {
        (tagged(left, &keys, 0)?, tagged(right, &keys, 1)?)
    } else {
        (left, right)
    };
    let mut union_names = keys.clone();
    let mut union_types = types.clone();
    if membership {
        union_names.push("__set_side".into());
        union_types.push(Type::Int);
    }
    let union = Node {
        schema: schema(&union_names, &union_types),
        estimated_rows: left.estimated_rows.saturating_add(right.estimated_rows),
        op: RelOp::UnionAll {
            left: Box::new(left),
            right: Box::new(right),
        },
    };
    let mut plan = projection(&union, keys.clone())?;
    plan.aggregation = true;
    plan.groups = types
        .iter()
        .enumerate()
        .map(|(i, ty)| column(i, ty))
        .collect();
    plan.output = (0..width).map(Output::Group).collect();
    plan.types = types.clone();
    if membership {
        for (index, (function, name)) in
            [(Aggregate::Min, "__set_min"), (Aggregate::Max, "__set_max")]
                .into_iter()
                .enumerate()
        {
            plan.aggregates.push(Agg {
                function,
                input: Some(column(width, &Type::Int)),
                ty: Type::Int,
                distinct: false,
            });
            plan.output.push(Output::Aggregate(index));
            plan.types.push(Type::Int);
            plan.names.push(name.into());
        }
    }
    let mut node = unary(union, plan);
    if membership {
        let eq = |index, value| Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                Op::Eq,
                Box::new(column(index, &Type::Int)),
                Box::new(number(value)),
            ),
        };
        let predicate = Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                Op::And,
                Box::new(eq(width, 0)),
                Box::new(eq(
                    width + 1,
                    i128::from(operation == SetOperator::Intersect),
                )),
            ),
        };
        node = Node {
            schema: node.schema.clone(),
            estimated_rows: node.estimated_rows,
            op: RelOp::Filter {
                predicate,
                input: Box::new(node),
            },
        };
    }
    let mut output = projection(&node, names)?;
    output.columns.truncate(visible);
    output.output.truncate(visible);
    output.types.truncate(visible);
    output.names.truncate(visible);
    Ok(unary(node, output))
}

/// ROW_NUMBER() OVER (PARTITION BY every column). Rows of one partition are
/// equal in every column, so which copy receives which ordinal is immaterial;
/// the window's canonical input order still makes it deterministic.
fn numbered(input: Node) -> Result<Node> {
    let input = super::window::canonical_input(input)?;
    let partition = input
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let ty = Type::from_field(field)?;
            Ok(if ty == Type::Null {
                Expr {
                    ty,
                    kind: Kind::Literal(Literal::Null),
                }
            } else {
                column(index, &ty)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let specification = super::window::Specification {
        function: super::window::Ranking::RowNumber,
        aggregate: None,
        offset: None,
        value: None,
        frame: super::window::Frame::Partition,
        partition,
        order: Vec::new(),
    };
    let mut fields = input.schema.fields().to_vec();
    fields.push(std::sync::Arc::new(
        specification.result_type().field("__set_ordinal", false),
    ));
    Ok(Node {
        schema: std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
        estimated_rows: input.estimated_rows,
        op: RelOp::Window {
            input: Box::new(input),
            specification: Box::new(specification),
        },
    })
}

/// INTERSECT ALL / EXCEPT ALL with multiset semantics: a row occurring m times
/// on the left and n times on the right occurs min(m, n) times (INTERSECT ALL)
/// or max(m - n, 0) times (EXCEPT ALL). Numbering the copies of each distinct
/// row 1..m and 1..n turns both into the existing DISTINCT operation over
/// (row, ordinal); NULLs compare equal exactly as in DISTINCT set operations.
pub(super) fn multiset(
    left: Node,
    right: Node,
    operation: SetOperator,
    names: Vec<String>,
    types: Vec<Type>,
) -> Result<Node> {
    // Number copies after conversion to the result types, exactly as the
    // rows are compared: distinct inputs can become equal when widened.
    let converted = |input: Node| -> Result<Node> {
        let same = input
            .schema
            .fields()
            .iter()
            .zip(&types)
            .map(|(field, ty)| Ok(Type::from_field(field)? == *ty))
            .collect::<Result<Vec<_>>>()?;
        if same.iter().all(|same| *same) {
            return Ok(input);
        }
        let target = schema(&names, &types);
        Ok(Node {
            schema: target.clone(),
            estimated_rows: input.estimated_rows,
            op: RelOp::UnionAll {
                left: Box::new(input),
                right: Box::new(Node {
                    schema: target,
                    estimated_rows: 0,
                    op: RelOp::Empty,
                }),
            },
        })
    };
    let (left, right) = (converted(left)?, converted(right)?);
    let width = types.len();
    let mut ordinal_names = names.clone();
    ordinal_names.push("__set_ordinal".into());
    let mut ordinal_types = types;
    ordinal_types.push(Type::Int);
    distinct_prefix(
        numbered(left)?,
        numbered(right)?,
        operation,
        ordinal_names,
        ordinal_types,
        width,
    )
}

fn output_index(names: &[String], key: &sqlparser::ast::Expr) -> Result<usize> {
    use sqlparser::ast::{Expr as E, Value};
    let mut key = key;
    while let E::Nested(inner) = key {
        key = inner;
    }
    match key {
        E::Identifier(id) => {
            let matching = |name: &str| {
                if id.quote_style.is_some() {
                    name == id.value
                } else {
                    name.eq_ignore_ascii_case(&id.value)
                }
            };
            let mut hits = names
                .iter()
                .enumerate()
                .filter(|(_, name)| matching(name))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if hits.is_empty() {
                // Assay displays an unaliased qualified column as `t.k`; both
                // references name that output column `k`.
                hits = names
                    .iter()
                    .enumerate()
                    .filter(|(_, name)| name.rsplit_once('.').is_some_and(|(_, last)| matching(last)))
                    .map(|(index, _)| index)
                    .collect();
            }
            match hits.as_slice() {
                [index] => Ok(*index),
                [] => Err(format!(
                    "ORDER BY name {} is not an output column of the set operation",
                    id.value
                )
                .into()),
                _ => Err("ORDER BY name must identify one output column".into()),
            }
        }
        E::Value(value) => match &value.value {
            Value::Number(n, _) if n.bytes().all(|b| b.is_ascii_digit()) => n
                .parse::<usize>()
                .ok()
                .and_then(|n| n.checked_sub(1))
                .filter(|n| *n < names.len())
                .ok_or_else(|| "ORDER BY ordinal out of range".into()),
            _ => Err("unsupported: ORDER BY literal".into()),
        },
        E::UnaryOp { .. } => Err("ORDER BY ordinal out of range".into()),
        E::CompoundIdentifier(_) => Err(
            "unsupported: qualified ORDER BY name over a set operation (PostgreSQL rejects it; name the output column or use its position)"
                .into(),
        ),
        _ => Err(
            "unsupported: ORDER BY expression over a set operation (PostgreSQL accepts only output names and positions)"
                .into(),
        ),
    }
}

/// ORDER BY / LIMIT / OFFSET of a compound (or parenthesized) query. Keys
/// name output columns or positions, with the same ASC/DESC and NULLS rules
/// as an ordinary SELECT ORDER BY. The sort and pagination reuse the ordinary
/// final projection and LIMIT operators.
pub(super) fn compound_modifiers(
    node: Node,
    order: Option<sqlparser::ast::OrderBy>,
    limit: Option<sqlparser::ast::LimitClause>,
) -> Result<Node> {
    use sqlparser::ast::OrderByKind;
    let limit = super::select_modifiers::pagination(limit)?;
    let mut node = node;
    if let Some(order) = order {
        if order.interpolate.is_some() {
            return Err("unsupported: ORDER BY interpolate".into());
        }
        let OrderByKind::Expressions(keys) = &order.kind else {
            return Err("unsupported: ORDER BY ALL".into());
        };
        let names = node
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let mut plan = projection(&node, names.clone())?;
        for key in keys {
            if key.with_fill.is_some() {
                return Err("unsupported: ORDER BY fill".into());
            }
            plan.order.push(crate::bind::Sort {
                column: output_index(&names, &key.expr)?,
                descending: key.options.asc == Some(false),
                nulls_first: key.options.nulls_first.unwrap_or(false),
            });
        }
        node = Node {
            schema: super::column_projection::output_schema(&plan, &node.schema),
            estimated_rows: node.estimated_rows,
            op: RelOp::Unary {
                plan,
                input: Box::new(node),
            },
        };
    }
    if let Some((count, offset)) = limit {
        super::series::cap_identity_source(
            &mut node,
            if count == 0 {
                0
            } else {
                count as u128 + offset as u128
            },
        );
        node = Node {
            schema: node.schema.clone(),
            estimated_rows: node.estimated_rows.saturating_sub(offset).min(count),
            op: RelOp::Limit {
                count,
                offset,
                input: Box::new(node),
            },
        };
    }
    Ok(node)
}
