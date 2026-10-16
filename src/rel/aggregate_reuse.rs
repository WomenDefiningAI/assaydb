//! Reuse a qualifying grouped exact SUM as the probe of an outer SUM.
//!
//! For a fact key k, every fact row sees the same dimension matches: all
//! joins on its path use k, never the measure. Each dimension match therefore
//! contributes SUM(fact[k]) once, including duplicate dimension matches. The
//! absolute physical-cardinality bound below proves every old/new prefix fits
//! precision 38, so this distributivity does not hide an arithmetic error.
//! Only the leftmost probe is replaced: first encounters of keys and dimension
//! matches keep their order. All-NULL groups retain a NULL contribution.
use super::*;
use crate::{bind::Output, expr::Kind};

#[cfg(test)]
mod tests;

// Private to this optional attempt; ordinary CTE IDs are checked below.
const BUILD_ID: usize = usize::MAX - 2;

fn max_source_rows(node: &Node) -> u128 {
    let mut rows = if matches!(node.op, RelOp::Scan { .. }) {
        node.estimated_rows as u128
    } else {
        0
    };
    children(node, &mut |child| rows = rows.max(max_source_rows(child)));
    rows
}

/// A fully retained build must prove the original SUM's u64 count cannot
/// overflow. This is an actual output count, never an estimated join size.
pub(super) fn build_row_limit(id: usize, input: &Node) -> Option<u128> {
    (id == BUILD_ID).then(|| u128::from(u64::MAX) / max_source_rows(input).max(1))
}

fn scan_only(node: &Node) -> bool {
    match &node.op {
        RelOp::Scan { .. } => true,
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => scan_only(input),
        _ => false,
    }
}

fn single_build(node: &Node) -> Option<&Node> {
    match &node.op {
        RelOp::Rename { input } | RelOp::Project { input, .. } => single_build(input),
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } if scan_only(left) => Some(right),
        _ => None,
    }
}

pub(super) struct Candidate<'a> {
    pub root: Node,
    sources: shared_retry::Sources<'a>,
    _credit: Reservation,
}

impl Candidate<'_> {
    // Reuse and optional retry both require the same immutable physical input.
    pub(super) fn verify(&self, options: &Options) -> Result<()> {
        self.sources.verify(options)
    }
}

fn column(expr: &Expr) -> Option<usize> {
    match expr.kind {
        Kind::Column(i) => Some(i),
        _ => None,
    }
}

// Only lossless, direct references may stand between a column and its scan.
fn scan_column(node: &Node, i: usize) -> Option<(&Node, usize)> {
    match &node.op {
        RelOp::Scan { columns, .. } => Some((node, *columns.get(i)?)),
        RelOp::Rename { input }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => scan_column(input, i),
        RelOp::Project { input, columns } => scan_column(input, *columns.get(i)?),
        _ => None,
    }
}

// Resolve an outer SUM only down the physical probe path, without crossing a
// filter, aggregate, outer/existence join or expression evaluation.
fn probe_column(node: &Node, i: usize) -> Option<(&Node, usize)> {
    match &node.op {
        RelOp::Join {
            left,
            kind: JoinKind::Inner,
            ..
        } if i < left.schema.fields().len() => probe_column(left, i),
        RelOp::Project { input, columns } => probe_column(input, *columns.get(i)?),
        RelOp::Rename { input } => probe_column(input, i),
        _ => scan_column(node, i),
    }
}

fn same_source(a: &Node, b: &Node) -> bool {
    matches!((&a.op, &b.op), (RelOp::Scan { path: a, .. }, RelOp::Scan { path: b, .. }) if a == b)
        && a.estimated_rows == b.estimated_rows
}

struct Producer<'a> {
    node: &'a Node,
    key_output: usize,
    sum_output: usize,
    key_physical: usize,
}

fn producer<'a>(node: &'a Node, fact: &Node, measure: usize) -> Option<Producer<'a>> {
    let RelOp::Filter {
        input: aggregate,
        predicate,
    } = &node.op
    else {
        return None;
    };
    let RelOp::Unary { input, plan } = &aggregate.op else {
        return None;
    };
    let [group] = plan.groups.as_slice() else {
        return None;
    };
    let [sum] = plan.aggregates.as_slice() else {
        return None;
    };
    if !plan.aggregation
        || plan.filter.is_some()
        || !plan.order.is_empty()
        || sum.distinct
        || !matches!(sum.function, bind::Aggregate::Sum)
        || !membership_safety::aggregate_is_total(plan, input)
        || !membership_safety::expression_is_total(predicate, aggregate)
        || plan.output.len() != 2
    {
        return None;
    }
    let (key_scan, key_physical) = scan_column(input, column(group)?)?;
    let (sum_scan, sum_physical) = scan_column(input, column(sum.input.as_ref()?)?)?;
    if !same_source(key_scan, fact)
        || !std::ptr::eq(key_scan, sum_scan)
        || sum_physical != measure
        || key_physical == measure
        || !matches!(group.ty, Type::Int | Type::Decimal(_))
    {
        return None;
    }
    Some(Producer {
        node,
        key_output: plan
            .output
            .iter()
            .position(|o| matches!(o, Output::Group(0)))?,
        sum_output: plan
            .output
            .iter()
            .position(|o| matches!(o, Output::Aggregate(0)))?,
        key_physical,
    })
}

// This restricted traversal is also a structural barrier: no scalar subquery,
// anti/outer join, LIMIT or unrelated aggregation is silently crossed.
fn children(node: &Node, visit: &mut impl FnMut(&Node)) {
    match &node.op {
        RelOp::WindowOrder { input, .. } | RelOp::Window { input, .. } => visit(input),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Materialize { input, .. }
        | RelOp::Limit { input, .. } => visit(input),
        RelOp::Join { left, right, .. } => {
            visit(left);
            visit(right);
        }
        _ => (),
    }
}

fn find_producer<'a>(node: &'a Node, fact: &Node, measure: usize) -> Option<Producer<'a>> {
    if let Some(found) = producer(node, fact, measure) {
        return Some(found);
    }
    match &node.op {
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => find_producer(input, fact, measure),
        RelOp::Join { left, right, .. } => {
            match (
                find_producer(left, fact, measure),
                find_producer(right, fact, measure),
            ) {
                (Some(found), None) | (None, Some(found)) => Some(found),
                _ => None,
            }
        }
        _ => None,
    }
}

fn output_column(plan: &Plan, i: usize) -> Option<usize> {
    if plan.aggregation || plan.filter.is_some() || !plan.order.is_empty() {
        return None;
    }
    match plan.output.get(i)? {
        Output::Expr(e) => column(e),
        _ => None,
    }
}

// Does a column carry the key from this precise HAVING producer?
fn producer_key(node: &Node, i: usize, p: &Producer<'_>) -> bool {
    if std::ptr::eq(node, p.node) {
        return i == p.key_output;
    }
    match &node.op {
        RelOp::Rename { input } => producer_key(input, i, p),
        RelOp::Project { input, columns } => {
            columns.get(i).is_some_and(|i| producer_key(input, *i, p))
        }
        RelOp::Unary { input, plan } => {
            output_column(plan, i).is_some_and(|i| producer_key(input, i, p))
        }
        _ => false,
    }
}

// Equality propagates membership, including through duplicate dimension rows.
// A NULL cannot satisfy any of these ordinary equality joins.
fn qualified(node: &Node, i: usize, p: &Producer<'_>) -> bool {
    if producer_key(node, i, p) {
        return true;
    }
    match &node.op {
        RelOp::Rename { input } | RelOp::Filter { input, .. } => qualified(input, i, p),
        RelOp::Project { input, columns } => {
            columns.get(i).is_some_and(|i| qualified(input, *i, p))
        }
        RelOp::Unary { input, plan } => {
            output_column(plan, i).is_some_and(|i| qualified(input, i, p))
        }
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner | JoinKind::Semi,
            keys,
            residual: None,
        } => {
            let width = left.schema.fields().len();
            if i < width {
                qualified(left, i, p)
                    || keys.iter().any(|(a, b)| {
                        column(a) == Some(i) && column(b).is_some_and(|j| qualified(right, j, p))
                    })
            } else {
                qualified(right, i - width, p)
                    || keys.iter().any(|(a, b)| {
                        column(b) == Some(i - width)
                            && column(a).is_some_and(|j| qualified(left, j, p))
                    })
            }
        }
        _ => false,
    }
}

// Exact scan counts are upper bounds; estimates on joins/filters are never used.
fn rows(node: &Node) -> Option<u128> {
    match &node.op {
        RelOp::Scan { .. } => Some(node.estimated_rows as u128),
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => rows(input),
        RelOp::Unary { input, plan } if !plan.aggregation || !plan.groups.is_empty() => rows(input),
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => rows(left)?.checked_mul(rows(right)?),
        RelOp::Join {
            left,
            kind: JoinKind::Semi,
            ..
        } => rows(left),
        _ => None,
    }
}

struct Probe {
    key: Option<usize>,
    value: usize,
    qualified: bool,
}

fn probe(node: &Node, fact: &Node, p: &Producer<'_>, value_physical: usize) -> Option<Probe> {
    if std::ptr::eq(node, fact) {
        let RelOp::Scan { columns, .. } = &node.op else {
            return None;
        };
        // All fact-dependent expressions must be a key or this exact measure.
        if columns.len() != 2 {
            return None;
        }
        return Some(Probe {
            key: Some(columns.iter().position(|i| *i == p.key_physical)?),
            value: columns.iter().position(|i| *i == value_physical)?,
            qualified: false,
        });
    }
    match &node.op {
        RelOp::Rename { input }
        | RelOp::Materialize {
            input,
            shared_scan: true,
            ..
        } => probe(input, fact, p, value_physical),
        RelOp::Project { input, columns } => {
            let mut path = probe(input, fact, p, value_physical)?;
            if columns.iter().filter(|i| **i == path.value).count() != 1 {
                return None;
            }
            path.value = columns.iter().position(|i| *i == path.value)?;
            path.key = path
                .key
                .and_then(|key| columns.iter().position(|i| *i == key));
            Some(path)
        }
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            keys,
            residual: None,
        } => {
            let mut path = probe(left, fact, p, value_physical)?;
            if !membership_safety::input_is_total(right)
                || keys.is_empty()
                || keys.iter().any(|(a, b)| {
                    a.ty != b.ty
                        || column(a).is_none()
                        || column(b).is_none()
                        || column(a) == Some(path.value)
                })
            {
                return None;
            }
            path.qualified |= keys.iter().any(|(a, b)| {
                path.key.is_some()
                    && column(a) == path.key
                    && column(b).is_some_and(|i| qualified(right, i, p))
            });
            Some(path)
        }
        _ => None,
    }
}

fn outer(node: &Node) -> Option<(&Plan, &Node)> {
    match &node.op {
        RelOp::Unary { plan, input } if plan.aggregation => Some((plan, input)),
        RelOp::Unary { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Project { input, .. }
        | RelOp::Rename { input }
        | RelOp::Filter { input, .. } => outer(input),
        _ => None,
    }
}

fn source_count(node: &Node, fact: &Node) -> usize {
    let mut count = usize::from(same_source(node, fact));
    children(node, &mut |child| count += source_count(child, fact));
    count
}

fn max_id(node: &Node) -> usize {
    let mut id = match node.op {
        RelOp::Materialize { id, .. } => id,
        _ => 0,
    };
    children(node, &mut |child| id = id.max(max_id(child)));
    id
}

fn contains(node: &Node, target: &Node) -> bool {
    let mut found = std::ptr::eq(node, target);
    children(node, &mut |child| found |= contains(child, target));
    found
}

// Rebuild just the admitted path; output names remain stable when the retained
// key/measure arrays widen from a storage precision to internal precision 38.
fn rewrite(
    node: &Node,
    fact: &Node,
    p: &Producer<'_>,
    stored: &Node,
    guarded: Option<&Node>,
) -> Node {
    if guarded.is_some_and(|build| std::ptr::eq(node, build)) {
        let input = rewrite(node, fact, p, stored, None);
        return Node {
            schema: input.schema.clone(),
            estimated_rows: input.estimated_rows,
            op: RelOp::Materialize {
                id: BUILD_ID,
                input: Box::new(input),
                shared_scan: false,
            },
        };
    }
    if std::ptr::eq(node, p.node) {
        return stored.clone();
    }
    if std::ptr::eq(node, fact) {
        let RelOp::Scan { columns, .. } = &fact.op else {
            unreachable!()
        };
        let columns = columns
            .iter()
            .map(|i| {
                if *i == p.key_physical {
                    p.key_output
                } else {
                    p.sum_output
                }
            })
            .collect::<Vec<_>>();
        let projected = Node {
            schema: Arc::new(stored.schema.project(&columns).unwrap()),
            estimated_rows: stored.estimated_rows,
            op: RelOp::Project {
                input: Box::new(stored.clone()),
                columns,
            },
        };
        return Node {
            schema: Arc::new(Schema::new(
                projected
                    .schema
                    .fields()
                    .iter()
                    .zip(fact.schema.fields())
                    .map(|(new, old)| new.as_ref().clone().with_name(old.name()))
                    .collect::<Vec<_>>(),
            )),
            estimated_rows: projected.estimated_rows,
            op: RelOp::Rename {
                input: Box::new(projected),
            },
        };
    }
    let op = match &node.op {
        RelOp::Rename { input } => RelOp::Rename {
            input: Box::new(rewrite(input, fact, p, stored, guarded)),
        },
        RelOp::Project { input, columns } => RelOp::Project {
            input: Box::new(rewrite(input, fact, p, stored, guarded)),
            columns: columns.clone(),
        },
        RelOp::Filter { input, predicate } => RelOp::Filter {
            input: Box::new(rewrite(input, fact, p, stored, guarded)),
            predicate: predicate.clone(),
        },
        RelOp::Unary { input, plan } => RelOp::Unary {
            input: Box::new(rewrite(input, fact, p, stored, guarded)),
            plan: plan.clone(),
        },
        RelOp::Limit { input, count } => RelOp::Limit {
            input: Box::new(rewrite(input, fact, p, stored, guarded)),
            count: *count,
        },
        RelOp::Materialize {
            input,
            id,
            shared_scan,
        } => {
            // A raw shared-scan ID must never cache the substituted aggregate.
            if *shared_scan && contains(input, fact) {
                return rewrite(input, fact, p, stored, guarded);
            }
            RelOp::Materialize {
                input: Box::new(rewrite(input, fact, p, stored, guarded)),
                id: *id,
                shared_scan: *shared_scan,
            }
        }
        RelOp::Join {
            left,
            right,
            kind,
            keys,
            residual,
        } => RelOp::Join {
            left: Box::new(rewrite(left, fact, p, stored, guarded)),
            right: Box::new(rewrite(right, fact, p, stored, guarded)),
            kind: *kind,
            keys: keys.clone(),
            residual: residual.clone(),
        },
        _ => node.op.clone(),
    };
    let mut result = Node {
        schema: node.schema.clone(),
        estimated_rows: node.estimated_rows,
        op,
    };
    result.schema = match &result.op {
        RelOp::Rename { input } => Arc::new(Schema::new(
            input
                .schema
                .fields()
                .iter()
                .zip(node.schema.fields())
                .map(|(new, old)| new.as_ref().clone().with_name(old.name()))
                .collect::<Vec<_>>(),
        )),
        RelOp::Project { input, columns } => Arc::new(input.schema.project(columns).unwrap()),
        RelOp::Filter { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Materialize { input, .. } => input.schema.clone(),
        RelOp::Join {
            left,
            right,
            kind: JoinKind::Inner,
            ..
        } => combined(&left.schema, &right.schema),
        RelOp::Join {
            left,
            kind: JoinKind::Semi,
            ..
        } => left.schema.clone(),
        RelOp::Unary { plan, input } => column_projection::output_schema(plan, &input.schema),
        _ => result.schema,
    };
    result
}

pub(super) fn candidate<'a>(
    root: &'a Node,
    budget: &Budget,
    options: &Options,
) -> Option<Candidate<'a>> {
    let (plan, input) = outer(root)?;
    let [sum] = plan.aggregates.as_slice() else {
        return None;
    };
    if plan.groups.is_empty()
        || plan.filter.is_some()
        || !plan.order.is_empty()
        || sum.distinct
        || !matches!(sum.function, bind::Aggregate::Sum)
        || !matches!(sum.ty, Type::Int | Type::Decimal(_))
        || plan.output.iter().any(|o| matches!(o, Output::Expr(_)))
    {
        return None;
    }
    let measure = sum.input.as_ref()?;
    let (fact, physical) = probe_column(input, column(measure)?)?;
    let p = find_producer(input, fact, physical)?;
    let path = probe(input, fact, &p, physical)?;
    if !path.qualified
        || column(measure) != Some(path.value)
        || plan
            .groups
            .iter()
            .any(|e| column(e).is_none() || column(e) == Some(path.value))
        || source_count(input, fact) != 2
    {
        return None;
    }
    let RelOp::Scan { columns, .. } = &fact.op else {
        return None;
    };
    let field = fact
        .schema
        .field(columns.iter().position(|i| *i == physical)?);
    let magnitude = match field.data_type() {
        arrow::datatypes::DataType::Int32 => 1_u128 << 31,
        arrow::datatypes::DataType::Int64 => 1_u128 << 63,
        arrow::datatypes::DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision) && (0..=38).contains(scale) =>
        {
            10_u128.pow(u32::from(*precision)) - 1
        }
        _ => return None,
    };
    let row_bound = rows(input)?;
    if magnitude.checked_mul(row_bound)? >= 10_u128.pow(38) {
        return None;
    }
    // SUM tracks non-NULL counts even when COUNT was not requested. When the
    // cross-product bound exceeds u64, cap the actual complete dimension build
    // before probing. Its subtree contains the producer's full physical fact
    // count, so max_source_rows is at least the original probe row count.
    let guarded = if row_bound > u128::from(u64::MAX) {
        Some(single_build(input)?)
    } else {
        None
    };
    let id = max_id(root).checked_add(1)?;
    if id >= BUILD_ID {
        return None;
    }
    // Covers simultaneous original clone, rewritten tree, the second producer
    // body, widened schemas, and all temporary descriptors before allocation.
    let bytes = build_groups::node_bytes(root)
        .checked_mul(16)?
        .checked_add(65536)?;
    let credit = budget.reserve(bytes).ok()?;
    let sources = shared_retry::Sources::capture(root, budget, options).ok()?;
    let stored = Node {
        schema: p.node.schema.clone(),
        estimated_rows: p.node.estimated_rows,
        op: RelOp::Materialize {
            id,
            input: Box::new(p.node.clone()),
            shared_scan: false,
        },
    };
    Some(Candidate {
        root: rewrite(root, fact, &p, &stored, guarded),
        sources,
        _credit: credit,
    })
}
