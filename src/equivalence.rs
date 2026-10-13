//! Column equalities proved by relational operators, never inferred from data.
use crate::{
    bind::Output,
    expr::{Expr, Kind},
    rel::{JoinKind, Node, RelOp},
    types::Type,
};

fn merge(classes: &mut [usize], a: usize, b: usize) {
    let a = classes[a];
    let b = classes[b];
    for class in classes {
        if *class == b {
            *class = a;
        }
    }
}

fn project(input: &Node, columns: &[Option<usize>]) -> Vec<usize> {
    let source = columns_equal(input);
    let mut classes: Vec<_> = (0..columns.len()).collect();
    for (i, a) in columns.iter().enumerate() {
        for (j, b) in columns[..i].iter().enumerate() {
            if let (Some(a), Some(b)) = (a, b)
                && source[*a] == source[*b]
            {
                merge(&mut classes, i, j);
            }
        }
    }
    classes
}

fn columns_equal(node: &Node) -> Vec<usize> {
    match &node.op {
        RelOp::Rename { input }
        | RelOp::Filter { input, .. }
        | RelOp::Materialize { input, .. } => columns_equal(input),
        RelOp::Project { input, columns } => project(
            input,
            &columns.iter().copied().map(Some).collect::<Vec<_>>(),
        ),
        RelOp::Unary { input, plan } if !plan.aggregation => project(
            input,
            &plan
                .output
                .iter()
                .map(|output| match output {
                    Output::Expr(Expr {
                        kind: Kind::Column(i),
                        ..
                    }) => Some(*i),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ),
        RelOp::Join {
            left,
            right,
            keys,
            kind: JoinKind::Inner,
            ..
        } => {
            let offset = left.schema.fields().len();
            let mut classes = columns_equal(left);
            classes.extend(columns_equal(right).into_iter().map(|i| i + offset));
            for (a, b) in keys {
                if let (Kind::Column(a_col), Kind::Column(b_col)) = (&a.kind, &b.kind)
                    && a.ty == b.ty
                    && !matches!(a.ty, Type::Float | Type::Null)
                {
                    merge(&mut classes, *a_col, offset + *b_col);
                }
            }
            classes
        }
        // In particular, an outer join does not prove its ON keys equal in
        // its output: one side can be NULL-extended. Keep other boundaries
        // conservative until their equality rules are separately supported.
        _ => (0..node.schema.fields().len()).collect(),
    }
}

pub(crate) fn reduce_join_keys(left: &Node, right: &Node, keys: &mut Vec<(Expr, Expr)>) {
    let left = columns_equal(left);
    let right = columns_equal(right);
    let mut seen = Vec::new();
    keys.retain(|(a, b)| {
        let (Kind::Column(a_col), Kind::Column(b_col)) = (&a.kind, &b.kind) else {
            return true;
        };
        let signature = (left[*a_col], right[*b_col], a.ty.clone(), b.ty.clone());
        if seen.contains(&signature) {
            false
        } else {
            seen.push(signature);
            true
        }
    });
}
