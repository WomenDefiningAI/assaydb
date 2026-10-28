//! Backward column liveness over the completed physical plan. Expressions and
//! cardinality-changing operators remain in place; only unused input columns
//! and carried-through fields are removed. Special scalar/mark semantics are
//! conservative barriers, with pruning still applied inside their children.
use crate::{
    Result,
    bind::Output,
    rel::{JoinKind, Node, RelOp},
};
use arrow::datatypes::{Schema, SchemaRef};
use std::{collections::BTreeSet, sync::Arc};

type Needed = BTreeSet<usize>;
fn all(node: &Node) -> Needed {
    (0..node.schema.fields().len()).collect()
}
fn schema(schema: &Schema, columns: &[usize]) -> Result<SchemaRef> {
    Ok(Arc::new(
        schema.project(columns).map_err(|e| e.to_string())?,
    ))
}
fn mapping(width: usize, kept: &[usize]) -> Vec<Option<usize>> {
    let mut map = vec![None; width];
    for (new, old) in kept.iter().enumerate() {
        map[*old] = Some(new);
    }
    map
}
fn index(map: &[Option<usize>], old: usize) -> Result<usize> {
    map.get(old)
        .copied()
        .flatten()
        .ok_or_else(|| "planner removed a required column".into())
}
fn project(input: Node, columns: Vec<usize>) -> Result<Node> {
    if columns.iter().copied().eq(0..input.schema.fields().len()) {
        return Ok(input);
    }
    Ok(Node {
        schema: schema(&input.schema, &columns)?,
        estimated_rows: input.estimated_rows,
        op: RelOp::Project {
            input: Box::new(input),
            columns,
        },
    })
}
pub(crate) fn prune(node: Node) -> Result<Node> {
    let needed = all(&node);
    Ok(walk::<false>(node, needed)?.0)
}
/// Scoped liveness for the private build-group plan; ordinary plans keep their barriers.
pub(crate) fn prune_build_groups(node: Node) -> Result<Node> {
    let needed = all(&node);
    Ok(walk::<true>(node, needed)?.0)
}
fn barrier<const MARKS: bool>(child: Box<Node>) -> Result<Box<Node>> {
    let needed = all(&child);
    Ok(Box::new(walk::<MARKS>(*child, needed)?.0))
}
fn walk<const MARKS: bool>(node: Node, mut needed: Needed) -> Result<(Node, Vec<usize>)> {
    // Keep a physical column for row counts if a parent only needs cardinality.
    // This avoids introducing zero-column batches into existing sort/join paths.
    if needed.is_empty() && !node.schema.fields().is_empty() {
        needed.insert(0);
    }
    let kept = needed.iter().copied().collect::<Vec<_>>();
    let old_width = node.schema.fields().len();
    let rows = node.estimated_rows;
    let old_schema = node.schema;
    let narrowed = schema(&old_schema, &kept)?;
    let result = match node.op {
        RelOp::Scan {
            path,
            columns,
            distinct,
            statistics,
        } => Node {
            schema: narrowed,
            estimated_rows: rows,
            op: RelOp::Scan {
                path,
                columns: kept.iter().map(|i| columns[*i]).collect(),
                distinct: kept.iter().map(|i| distinct[*i]).collect(),
                statistics: kept.iter().map(|i| statistics[*i].clone()).collect(),
            },
        },
        RelOp::Rename { input } => {
            let (input, _) = walk::<MARKS>(*input, needed)?;
            Node {
                schema: narrowed,
                estimated_rows: rows,
                op: RelOp::Rename {
                    input: Box::new(input),
                },
            }
        }
        RelOp::Limit {
            input,
            count,
            offset,
        } => {
            let (input, _) = walk::<MARKS>(*input, needed)?;
            Node {
                schema: narrowed,
                estimated_rows: rows,
                op: RelOp::Limit {
                    input: Box::new(input),
                    count,
                    offset,
                },
            }
        }
        RelOp::Project { input, columns } => {
            let width = input.schema.fields().len();
            let (input, child_kept) =
                walk::<MARKS>(*input, kept.iter().map(|i| columns[*i]).collect())?;
            let map = mapping(width, &child_kept);
            project(
                input,
                kept.iter()
                    .map(|i| index(&map, columns[*i]))
                    .collect::<Result<_>>()?,
            )?
        }
        RelOp::Filter {
            input,
            mut predicate,
        } => {
            let mut child_needed = needed;
            predicate.columns(&mut child_needed);
            let (input, child_kept) = walk::<MARKS>(*input, child_needed)?;
            let map = mapping(old_width, &child_kept);
            predicate.remap_columns(&map)?;
            let filtered = Node {
                schema: input.schema.clone(),
                estimated_rows: rows,
                op: RelOp::Filter {
                    input: Box::new(input),
                    predicate,
                },
            };
            project(
                filtered,
                kept.iter()
                    .map(|i| index(&map, *i))
                    .collect::<Result<_>>()?,
            )?
        }
        RelOp::Unary { input, mut plan } => {
            let width = input.schema.fields().len();
            // Retain all computed outputs, including sort keys and expressions
            // that can raise errors. Their source columns define child liveness.
            let (input, child_kept) =
                walk::<MARKS>(*input, plan.columns.iter().copied().collect())?;
            let map = mapping(width, &child_kept);
            if let Some(expr) = &mut plan.filter {
                expr.remap_columns(&map)?;
            }
            for expr in &mut plan.groups {
                expr.remap_columns(&map)?;
            }
            for agg in &mut plan.aggregates {
                if let Some(expr) = &mut agg.input {
                    expr.remap_columns(&map)?;
                }
            }
            for output in &mut plan.output {
                if let Output::Expr(expr) = output {
                    expr.remap_columns(&map)?;
                }
            }
            plan.columns = plan
                .columns
                .iter()
                .map(|i| index(&map, *i))
                .collect::<Result<_>>()?;
            let unary = Node {
                schema: old_schema,
                estimated_rows: rows,
                op: RelOp::Unary {
                    input: Box::new(input),
                    plan,
                },
            };
            project(unary, kept.clone())?
        }
        RelOp::Join {
            left,
            right,
            kind,
            mut keys,
            mut residual,
        } => {
            let left_width = left.schema.fields().len();
            let right_width = right.schema.fields().len();
            let mut left_needed = Needed::new();
            let mut right_needed = Needed::new();
            let mut combined_needed = needed;
            if let Some(expr) = &residual {
                expr.columns(&mut combined_needed);
            }
            for i in combined_needed {
                if i < left_width {
                    left_needed.insert(i);
                } else {
                    right_needed.insert(i - left_width);
                }
            }
            for (a, b) in &keys {
                a.columns(&mut left_needed);
                b.columns(&mut right_needed);
            }
            let (left, left_kept) = walk::<MARKS>(*left, left_needed)?;
            let (right, right_kept) = walk::<MARKS>(*right, right_needed)?;
            let left_map = mapping(left_width, &left_kept);
            let right_map = mapping(right_width, &right_kept);
            for (a, b) in &mut keys {
                a.remap_columns(&left_map)?;
                b.remap_columns(&right_map)?;
            }
            let mut map = left_map;
            map.extend(right_map.iter().map(|i| i.map(|i| i + left_kept.len())));
            if let Some(expr) = &mut residual {
                expr.remap_columns(&map)?;
            }
            let join_schema = if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                left.schema.clone()
            } else {
                Arc::new(Schema::new(
                    left.schema
                        .fields()
                        .iter()
                        .chain(right.schema.fields())
                        .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
                        .collect::<Vec<_>>(),
                ))
            };
            let join = Node {
                schema: join_schema,
                estimated_rows: rows,
                op: RelOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                    keys,
                    residual,
                },
            };
            project(
                join,
                kept.iter()
                    .map(|i| index(&map, *i))
                    .collect::<Result<_>>()?,
            )?
        }
        RelOp::Mark {
            left,
            right,
            mut value,
            negated,
        } if MARKS => {
            // Membership still evaluates both children and its value, even if
            // the marker is unselected. Only unused carried payload is removed.
            let width = left.schema.fields().len();
            let mut left_needed = needed.iter().copied().filter(|i| *i < width).collect();
            value.columns(&mut left_needed);
            let (left, left_kept) = walk::<MARKS>(*left, left_needed)?;
            let mut map = mapping(width, &left_kept);
            value.remap_columns(&map)?;
            map.push(Some(left_kept.len()));
            let fields = left
                .schema
                .fields()
                .iter()
                .cloned()
                .chain([old_schema.fields()[width].clone()])
                .collect::<Vec<_>>();
            let marked = Node {
                schema: Arc::new(Schema::new(fields)),
                estimated_rows: rows,
                op: RelOp::Mark {
                    left: Box::new(left),
                    right: barrier::<MARKS>(right)?,
                    value,
                    negated,
                },
            };
            project(
                marked,
                kept.iter()
                    .map(|i| index(&map, *i))
                    .collect::<Result<_>>()?,
            )?
        }
        // These nodes add synthetic values, coerce types or enforce cardinality.
        // Retain their full immediate schemas while optimizing their descendants.
        op => {
            let op = match op {
                // Complete tuple order depends on every physical column.
                RelOp::WindowOrder { input, order } => RelOp::WindowOrder {
                    input: barrier::<MARKS>(input)?,
                    order,
                },
                RelOp::Window {
                    input,
                    specification,
                } => RelOp::Window {
                    input: barrier::<MARKS>(input)?,
                    specification,
                },
                // Every reference to a CTE id must retain the same full layout.
                RelOp::Materialize {
                    id,
                    input,
                    shared_scan,
                    forced,
                } => RelOp::Materialize {
                    id,
                    input: barrier::<MARKS>(input)?,
                    shared_scan,
                    forced,
                },
                RelOp::Scalar { input } => RelOp::Scalar {
                    input: barrier::<MARKS>(input)?,
                },
                RelOp::ScalarDefault {
                    input,
                    fallback,
                    value,
                    marker,
                } => RelOp::ScalarDefault {
                    input: barrier::<MARKS>(input)?,
                    fallback: barrier::<MARKS>(fallback)?,
                    value,
                    marker,
                },
                RelOp::Mark {
                    left,
                    right,
                    value,
                    negated,
                } => RelOp::Mark {
                    left: barrier::<MARKS>(left)?,
                    right: barrier::<MARKS>(right)?,
                    value,
                    negated,
                },
                RelOp::TypedMark {
                    left,
                    right,
                    value,
                    negated,
                } => RelOp::TypedMark {
                    left: barrier::<MARKS>(left)?,
                    right: barrier::<MARKS>(right)?,
                    value,
                    negated,
                },
                RelOp::UnionAll { left, right } => RelOp::UnionAll {
                    left: barrier::<MARKS>(left)?,
                    right: barrier::<MARKS>(right)?,
                },
                RelOp::CorrelatedMark {
                    left,
                    right,
                    predicate,
                    marker,
                } => RelOp::CorrelatedMark {
                    left: barrier::<MARKS>(left)?,
                    right: barrier::<MARKS>(right)?,
                    predicate,
                    marker,
                },
                RelOp::Generate { .. } => op, // scalar arguments have no input columns
                RelOp::One
                | RelOp::Empty
                | RelOp::RecursiveCte { .. }
                | RelOp::RecursiveScan { .. } => op,
                _ => unreachable!(),
            };
            project(
                Node {
                    schema: old_schema,
                    estimated_rows: rows,
                    op,
                },
                kept.clone(),
            )?
        }
    };
    Ok((result, kept))
}
