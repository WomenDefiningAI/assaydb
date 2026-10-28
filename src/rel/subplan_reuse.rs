//! Optional query-local reuse above total filters and ordered INNER joins.
//! A projection is a view of the same ordered rows, not a different producer.
//! Aggregates, computed projections, outer/existence joins and limits are barriers.
use super::*;
use arrow::datatypes::DataType;

#[cfg(test)]
mod tests;

pub(super) const ID: usize = usize::MAX / 2;
const MAX_NODES: usize = 256;
const COLUMN_STRIDE: usize = 65536;

fn children<'a>(node: &'a Node, visit: &mut impl FnMut(&'a Node)) {
    match &node.op {
        RelOp::Materialize {
            shared_scan: false, ..
        } => (),
        RelOp::WindowOrder { input, .. }
        | RelOp::Window { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => visit(input),
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. }
        | RelOp::UnionAll { left, right } => {
            visit(left);
            visit(right);
        }
        RelOp::ScalarDefault {
            input, fallback, ..
        } => {
            visit(input);
            visit(fallback);
        }
        RelOp::Generate { .. } => (),
        RelOp::Scan { .. }
        | RelOp::One
        | RelOp::Empty
        | RelOp::RecursiveCte { .. }
        | RelOp::RecursiveScan { .. } => (),
    }
}

#[derive(PartialEq, Eq)]
enum Shape {
    Scan(PathBuf, usize),
    Filter(Box<Shape>, Expr),
    Join(Box<Shape>, Box<Shape>, Vec<(Expr, Expr)>, Option<Expr>),
}
struct Canonical {
    shape: Shape,
    columns: Vec<usize>,
    joins: usize,
    filtered: bool,
    source_rows: usize,
    source_bytes: usize,
}

// Cost estimates only: retained buffers still require actual Budget credit.
// Counting rows alone rewards a join that repeats wide dimension payload on
// every fact row, even when a later consumer filter discards almost all of it.
fn row_bytes(node: &Node) -> usize {
    node.schema.fields().iter().fold(0_usize, |bytes, field| {
        bytes.saturating_add(
            1 + match field.data_type() {
                DataType::Utf8 => 32,
                DataType::Decimal128(_, _) => 16,
                DataType::Int32 | DataType::Date32 => 4,
                DataType::Boolean => 1,
                _ => 8,
            },
        )
    })
}

fn expression(expr: &Expr, columns: &[usize]) -> Option<Expr> {
    let mut expr = expr.clone();
    expr.remap_columns(&columns.iter().copied().map(Some).collect::<Vec<_>>())
        .ok()?;
    Some(expr)
}

// Column identities encode physical scan occurrence and physical column ordinal.
// The occurrence distinguishes two aliases of the same source in a self join.
// No join commutation or predicate reassociation changes encounter/error order.
fn canonical(node: &Node, next: &mut usize) -> Option<Canonical> {
    match &node.op {
        RelOp::Scan { path, columns, .. } => {
            let offset = next.checked_mul(COLUMN_STRIDE)?;
            *next = next.checked_add(1)?;
            if columns.iter().any(|i| *i >= COLUMN_STRIDE) {
                return None;
            }
            Some(Canonical {
                shape: Shape::Scan(path.clone(), node.estimated_rows),
                columns: columns.iter().map(|i| offset + i).collect(),
                joins: 0,
                filtered: false,
                source_rows: node.estimated_rows,
                source_bytes: node.estimated_rows.saturating_mul(row_bytes(node)),
            })
        }
        RelOp::Rename { input }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => canonical(input, next),
        RelOp::Project { input, columns } => {
            let mut c = canonical(input, next)?;
            c.columns = columns
                .iter()
                .map(|i| c.columns.get(*i).copied())
                .collect::<Option<_>>()?;
            Some(c)
        }
        RelOp::Filter { input, predicate }
            if membership_safety::expression_is_total(predicate, input) =>
        {
            let mut c = canonical(input, next)?;
            c.shape = Shape::Filter(Box::new(c.shape), expression(predicate, &c.columns)?);
            c.filtered = true;
            Some(c)
        }
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            keys,
            residual,
        } if !keys.is_empty() && membership_safety::join_conditions_are_total(node) => {
            let mut a = canonical(left, next)?;
            let b = canonical(right, next)?;
            let keys = keys
                .iter()
                .map(|(x, y)| Some((expression(x, &a.columns)?, expression(y, &b.columns)?)))
                .collect::<Option<Vec<_>>>()?;
            a.columns.extend(b.columns);
            let residual = match residual {
                Some(e) => Some(expression(e, &a.columns)?),
                None => None,
            };
            Some(Canonical {
                shape: Shape::Join(Box::new(a.shape), Box::new(b.shape), keys, residual),
                columns: a.columns,
                joins: a.joins + b.joins + 1,
                filtered: a.filtered || b.filtered,
                source_rows: a.source_rows.max(b.source_rows),
                source_bytes: a.source_bytes.saturating_add(b.source_bytes),
            })
        }
        _ => None,
    }
}

fn projection(base: &Node, a: &Canonical, node: &Node) -> Option<Vec<usize>> {
    if node.schema.fields().is_empty() {
        return None;
    }
    let b = canonical(node, &mut 0)?;
    if a.shape != b.shape {
        return None;
    }
    b.columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            a.columns.iter().enumerate().find_map(|(j, origin)| {
                let x = base.schema.field(j);
                let y = node.schema.field(i);
                (origin == column
                    && x.data_type() == y.data_type()
                    && x.is_nullable() == y.is_nullable()
                    && x.metadata() == y.metadata())
                .then_some(j)
            })
        })
        .collect()
}

fn matches(node: &Node, base: &Node, a: &Canonical) -> usize {
    if projection(base, a, node).is_some() {
        // Count disjoint references only; a Project and its child are one use.
        return 1;
    }
    let mut n = 0;
    children(node, &mut |child| n += matches(child, base, a));
    n
}

fn strip_raw(node: &mut Node) {
    if matches!(
        node.op,
        RelOp::Materialize {
            shared_scan: false,
            ..
        }
    ) {
        return;
    }
    if matches!(
        node.op,
        RelOp::Materialize {
            shared_scan: true,
            ..
        }
    ) {
        let RelOp::Materialize { input, .. } = std::mem::replace(&mut node.op, RelOp::Empty) else {
            unreachable!()
        };
        *node = *input;
    }
    for child in crate::cte::children(node) {
        strip_raw(child);
    }
}

fn rewrite(node: &mut Node, base: &Node, a: &Canonical, producer: &Node) {
    if let Some(columns) = projection(base, a, node) {
        let stored = Node {
            schema: producer.schema.clone(),
            estimated_rows: producer.estimated_rows,
            op: RelOp::Materialize {
                id: ID,
                input: Box::new(producer.clone()),
                shared_scan: true,
                forced: false,
            },
        };
        let projected = Node {
            schema: Arc::new(
                stored
                    .schema
                    .project(&columns)
                    .expect("checked subplan projection"),
            ),
            estimated_rows: node.estimated_rows,
            op: RelOp::Project {
                input: Box::new(stored),
                columns,
            },
        };
        // Aliases and each consumer's schema remain local to the reference.
        node.op = RelOp::Rename {
            input: Box::new(projected),
        };
        return;
    }
    if matches!(
        node.op,
        RelOp::Materialize {
            shared_scan: false,
            ..
        }
    ) {
        return;
    }
    for child in crate::cte::children(node) {
        rewrite(child, base, a, producer);
    }
}

pub(super) struct Candidate<'a> {
    pub root: Node,
    sources: shared_retry::Sources<'a>,
    _credit: Reservation,
}
impl Candidate<'_> {
    pub(super) fn verify(&self, options: &Options) -> Result<()> {
        self.sources.verify(options)
    }
}

pub(super) fn candidate<'a>(
    root: &'a Node,
    budget: &Budget,
    options: &Options,
) -> Option<Candidate<'a>> {
    // Fixed stack descriptors and a cheap repeated-source screen avoid cloning
    // any expressions or plans for queries with no repeated physical input.
    fn collect<'a>(node: &'a Node, nodes: &mut [Option<&'a Node>; MAX_NODES], n: &mut usize) {
        if *n < MAX_NODES {
            nodes[*n] = Some(node);
        }
        *n += 1;
        children(node, &mut |child| collect(child, nodes, n));
    }
    let mut nodes = [None; MAX_NODES];
    let mut n = 0;
    collect(root, &mut nodes, &mut n);
    if n > MAX_NODES
        || nodes[..n]
            .iter()
            .flatten()
            .any(|node| matches!(node.op, RelOp::Materialize { id, .. } if id >= ID))
    {
        return None;
    }
    let repeated = nodes[..n].iter().flatten().enumerate().any(|(i, a)| {
        matches!(&a.op, RelOp::Scan { path, .. } if nodes[..i].iter().flatten().any(|b| matches!(&b.op, RelOp::Scan { path: p, .. } if path == p)))
    });
    if !repeated {
        return None;
    }
    // Two transient canonical trees, a retained plan and one producer per
    // disjoint reference fit within this conservative clone/descriptor credit.
    let bytes = build_groups::node_bytes(root)
        .checked_mul(16)?
        .checked_add(65536)?;
    if bytes > budget.limit() / 8 {
        return None;
    }
    let credit = budget.reserve(bytes).ok()?;
    let mut best = None;
    let mut best_score = (0, 0, std::cmp::Reverse(usize::MAX));
    for node in nodes[..n].iter().flatten() {
        let Some(a) = canonical(node, &mut 0) else {
            continue;
        };
        if a.columns.is_empty()
            || a.joins == 0
            || !a.filtered
            || node.estimated_rows > a.source_rows / 2
            || node.estimated_rows.saturating_mul(row_bytes(node)) > a.source_bytes
        {
            continue;
        }
        let uses = matches(root, node, &a);
        if !(2..=8).contains(&uses) {
            continue;
        }
        let score = (
            a.source_rows.saturating_mul(uses - 1),
            a.joins,
            std::cmp::Reverse(a.columns.len()),
        );
        if score > best_score {
            best = Some((*node, a));
            best_score = score;
        }
    }
    let (base, a) = best?;
    let sources = shared_retry::Sources::capture(root, budget, options).ok()?;
    let mut producer = base.clone();
    strip_raw(&mut producer);
    let mut rewritten = root.clone();
    // Keep matching against the existing raw-scan views until every reference
    // has been replaced. Then remove redundant raw caches outside the producer.
    rewrite(&mut rewritten, base, &a, &producer);
    fn remove_old_raw(node: &mut Node) {
        if matches!(
            node.op,
            RelOp::Materialize { id: ID, .. }
                | RelOp::Materialize {
                    shared_scan: false,
                    ..
                }
        ) {
            return;
        }
        if matches!(
            node.op,
            RelOp::Materialize {
                shared_scan: true,
                ..
            }
        ) {
            let RelOp::Materialize { input, .. } = std::mem::replace(&mut node.op, RelOp::Empty)
            else {
                unreachable!()
            };
            *node = *input;
        }
        for child in crate::cte::children(node) {
            remove_old_raw(child);
        }
    }
    remove_old_raw(&mut rewritten);
    // Leave remaining scans visible to membership pushdown and the existing
    // bounded filtered-scan cache. Rewrapping them in raw caches would retain
    // the unfiltered fact table again when a narrower joined boundary wins.
    // Any failed optional execution still retries the original raw-sharing plan.
    Some(Candidate {
        root: rewritten,
        sources,
        _credit: credit,
    })
}
