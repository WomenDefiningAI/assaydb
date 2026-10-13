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
    output.columns.truncate(width);
    output.output.truncate(width);
    output.types.truncate(width);
    Ok(unary(node, output))
}
