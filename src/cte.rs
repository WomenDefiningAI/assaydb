//! Keep materialization boundaries only for CTEs referenced more than once.
use crate::rel::{Node, RelOp};
use std::collections::HashMap;

fn children(node: &mut Node) -> Vec<&mut Node> {
    match &mut node.op {
        RelOp::WindowOrder { input, .. }
        | RelOp::Window { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input }
        | RelOp::Materialize { input, .. } => vec![input],
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. }
        | RelOp::UnionAll { left, right } => vec![left, right],
        RelOp::ScalarDefault {
            input, fallback, ..
        } => vec![input, fallback],
        RelOp::Scan { .. } | RelOp::One | RelOp::Empty => vec![],
    }
}
fn count(node: &mut Node, uses: &mut HashMap<usize, usize>) {
    if let RelOp::Materialize { id, .. } = node.op {
        *uses.entry(id).or_default() += 1;
    }
    for child in children(node) {
        count(child, uses);
    }
}
fn inline(node: &mut Node, uses: &HashMap<usize, usize>) {
    for child in children(node) {
        inline(child, uses);
    }
    if let RelOp::Materialize { id, .. } = &node.op
        && uses[id] == 1
    {
        let RelOp::Materialize { input, .. } = std::mem::replace(&mut node.op, RelOp::Empty) else {
            unreachable!()
        };
        *node = *input;
    }
}
pub(crate) fn repeated(mut node: Node) -> Node {
    let mut uses = HashMap::new();
    count(&mut node, &mut uses);
    inline(&mut node, &uses);
    node
}

// Share already-pruned physical scans, including references to a subset of
// an existing projection. Raw data is query-local; each reference keeps its
// filters after cache lookup. Never synthesize a wider union of projections.
type ScanKey = (std::path::PathBuf, Vec<usize>);
fn scan_key(node: &Node) -> Option<ScanKey> {
    match &node.op {
        RelOp::Scan { path, columns, .. } if !columns.is_empty() => {
            Some((path.clone(), columns.clone()))
        }
        _ => None,
    }
}
fn materialization_ids(node: &mut Node, max_id: &mut usize) {
    if let RelOp::Materialize { id, .. } = &node.op {
        *max_id = (*max_id).max(*id);
    }
    for child in children(node) {
        materialization_ids(child, max_id);
    }
}
struct ScanInfo {
    uses: usize,
    bytes: usize,
    node: Node,
}
struct ScanReuse {
    id: usize,
    scan: Node,
    columns: Vec<usize>,
}
fn subset(
    base_key: &ScanKey,
    base: &ScanInfo,
    key: &ScanKey,
    info: &ScanInfo,
) -> Option<Vec<usize>> {
    if base_key.0 != key.0 || base.node.estimated_rows != info.node.estimated_rows {
        return None;
    }
    key.1
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let position = base_key.1.iter().position(|c| c == column)?;
            (base.node.schema.field(position) == info.node.schema.field(i)).then_some(position)
        })
        .collect()
}
fn scan_counts(node: &mut Node, counts: &mut std::collections::BTreeMap<ScanKey, ScanInfo>) {
    // Repeated CTE bodies already execute once. Do not count their cloned plan
    // bodies as additional reads or retain a redundant cache inside them.
    if matches!(node.op, RelOp::Materialize { .. }) {
        return;
    }
    if let Some(key) = scan_key(node) {
        let width: usize = node.schema.fields().iter().map(|f| match f.data_type() {
            arrow::datatypes::DataType::Utf8 => 32,
            arrow::datatypes::DataType::Decimal128(_, _) => 16,
            arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Int32 => 4,
            arrow::datatypes::DataType::Boolean => 1,
            _ => 8,
        } + 1).sum();
        let bytes = node
            .estimated_rows
            .saturating_mul(width)
            .saturating_add(4096);
        let entry = counts.entry(key).or_insert_with(|| ScanInfo {
            uses: 0,
            bytes,
            node: node.clone(),
        });
        entry.uses += 1;
    }
    for child in children(node) {
        scan_counts(child, counts);
    }
}
fn wrap_scans(node: &mut Node, ids: &HashMap<ScanKey, ScanReuse>) {
    if matches!(node.op, RelOp::Materialize { .. }) {
        return;
    }
    if let Some(key) = scan_key(node)
        && let Some(reuse) = ids.get(&key)
    {
        let materialized = Node {
            schema: reuse.scan.schema.clone(),
            estimated_rows: reuse.scan.estimated_rows,
            op: RelOp::Materialize {
                id: reuse.id,
                input: Box::new(reuse.scan.clone()),
                shared_scan: true,
            },
        };
        // Project after lookup: the cache schema remains canonical, even when
        // a narrower reference happens to run first. scan_target remaps any
        // per-reference runtime filter through this projection.
        if reuse
            .columns
            .iter()
            .copied()
            .eq(0..reuse.scan.schema.fields().len())
        {
            node.op = materialized.op;
        } else {
            node.op = RelOp::Project {
                input: Box::new(materialized),
                columns: reuse.columns.clone(),
            };
        }
        return;
    }
    for child in children(node) {
        wrap_scans(child, ids);
    }
}
pub(crate) fn share_scans(mut root: Node, memory_bytes: usize) -> crate::Result<Node> {
    let mut max_id = 0;
    materialization_ids(&mut root, &mut max_id);
    let mut counts = std::collections::BTreeMap::new();
    scan_counts(&mut root, &mut counts);
    let mut candidates = counts
        .iter()
        .filter_map(|(key, base)| {
            if base.bytes > memory_bytes / 8 {
                return None;
            }
            let mut uses = 0_usize;
            let mut scanned = 0_usize;
            for (other_key, info) in &counts {
                if subset(key, base, other_key, info).is_some() {
                    uses = uses.saturating_add(info.uses);
                    scanned = scanned.saturating_add(info.bytes.saturating_mul(info.uses));
                }
            }
            (uses > 1).then_some((key.clone(), scanned.saturating_sub(base.bytes)))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(_, saved)| std::cmp::Reverse(*saved));
    let mut total = 0_usize;
    let mut ids = HashMap::new();
    for (key, _) in candidates {
        let base = &counts[&key];
        if ids.contains_key(&key) || total.saturating_add(base.bytes) > memory_bytes / 4 {
            continue;
        }
        let references = counts
            .iter()
            .filter(|(other, _)| !ids.contains_key(*other))
            .filter_map(|(other, info)| {
                subset(&key, base, other, info).map(|columns| (other, info.uses, columns))
            })
            .collect::<Vec<_>>();
        if references.iter().map(|(_, uses, _)| uses).sum::<usize>() < 2 {
            continue;
        }
        total += base.bytes;
        max_id = max_id.checked_add(1).ok_or("materialization id overflow")?;
        for (other, _, columns) in references {
            ids.insert(
                other.clone(),
                ScanReuse {
                    id: max_id,
                    scan: base.node.clone(),
                    columns,
                },
            );
        }
    }
    wrap_scans(&mut root, &ids);
    Ok(root)
}
