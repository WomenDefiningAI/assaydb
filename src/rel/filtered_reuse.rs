//! Query-local, bounded retention of complete membership-filtered scans.
//! No SQL expression, join, aggregate, or consumer predicate is cached.
use super::*;
use crate::execute::pipeline::fallback::Source;

#[cfg(test)]
mod tests;

pub(super) struct Cache {
    candidates: Vec<usize>,
    entries: Vec<Arc<Entry>>,
    recording: bool,
    _credit: Reservation,
}

struct Entry {
    path: PathBuf,
    columns: Vec<usize>,
    schema: SchemaRef,
    source: Source,
    filter: Arc<ScanKeyFilter>,
    batches: Vec<RecordBatch>,
    rows: usize,
    row_limit: usize,
    byte_limit: usize,
    credit: Reservation,
}

// Only direct, unchained numeric filters reach this cache. Their membership
// lease and optional bitmap remain pinned even after the runtime owner exits.
// Count the existing reservations, rather than reserving the same storage twice;
// the entry's descriptor credit also covers the retained Arc/column metadata.
fn filter_bytes(filter: &ScanKeyFilter) -> usize {
    filter._lease.bytes().saturating_add(
        filter
            .numeric
            .as_ref()
            .map_or(0, |bitmap| bitmap.reserved_bytes()),
    )
}

impl Entry {
    fn retained_bytes(&self) -> usize {
        self.credit
            .bytes()
            .saturating_add(filter_bytes(&self.filter))
    }
}

// Inspect the retained plan without allocating or changing any scan projection.
fn scans<'a>(node: &'a Node, visit: &mut impl FnMut(&'a Node)) {
    match &node.op {
        RelOp::Generate { .. } => (),
        RelOp::Scan { .. } => visit(node),
        RelOp::Materialize { .. }
        | RelOp::One
        | RelOp::Empty
        | RelOp::RecursiveCte { .. }
        | RelOp::RecursiveScan { .. } => (),
        RelOp::WindowOrder { input, .. }
        | RelOp::Window { input, .. }
        | RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input } => scans(input, visit),
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. }
        | RelOp::CorrelatedMark { left, right, .. }
        | RelOp::UnionAll { left, right } => {
            scans(left, visit);
            scans(right, visit);
        }
        RelOp::ScalarDefault {
            input, fallback, ..
        } => {
            scans(input, visit);
            scans(fallback, visit);
        }
    }
}

impl Cache {
    fn retained_bytes(&self) -> usize {
        self.entries
            .iter()
            .fold(self._credit.bytes(), |bytes, entry| {
                bytes.saturating_add(entry.retained_bytes())
            })
    }

    pub(super) fn new(root: &Node, budget: &Budget) -> Option<Arc<Mutex<Self>>> {
        // A fixed descriptor ceiling bounds planning work and cache metadata.
        let mut nodes = [None; 64];
        let mut count = 0usize;
        scans(root, &mut |node| {
            if count < nodes.len() {
                nodes[count] = Some(node);
            }
            count += 1;
        });
        if count < 2 || count > nodes.len() {
            return None;
        }
        let repeated = |node: &Node| {
            let RelOp::Scan { path, .. } = &node.op else {
                return false;
            };
            nodes[..count].iter().flatten().any(|other| {
                !std::ptr::eq(node, *other)
                    && matches!(&other.op, RelOp::Scan { path: p, .. } if p == path)
            })
        };
        if !nodes[..count].iter().flatten().any(|node| repeated(node)) {
            return None;
        }
        let bytes = count.checked_mul(512)?.checked_add(1024)?;
        if bytes > (budget.limit() / 32).min(32 << 20) {
            return None;
        }
        let credit = budget.reserve(bytes).ok()?;
        let candidates = nodes[..count]
            .iter()
            .flatten()
            .filter(|node| repeated(node))
            .map(|node| *node as *const Node as usize)
            .collect();
        Some(Arc::new(Mutex::new(Self {
            candidates,
            entries: Vec::with_capacity(count),
            recording: false,
            _credit: credit,
        })))
    }
}

fn direct(filter: &ScanKeyFilter) -> Option<(usize, &Type)> {
    if filter.previous.is_some() || filter.numeric_pair.is_some() {
        return None;
    }
    let [
        Expr {
            kind: crate::expr::Kind::Column(column),
            ty,
        },
    ] = filter.expressions.as_slice()
    else {
        return None;
    };
    matches!(ty, Type::Int | Type::Decimal(_)).then_some((*column, ty))
}

fn eligible(node: &Node, ctx: &Context<'_>) -> bool {
    let id = node as *const Node as usize;
    ctx.filtered_reuse
        .as_ref()
        .is_some_and(|cache| cache.lock().unwrap().candidates.contains(&id))
        && !ctx.static_filters.contains_key(&id)
        && !ctx.scan_transforms.contains_key(&id)
        && ctx
            .scan_filters
            .get(&id)
            .is_some_and(|filter| direct(filter).is_some())
}

/// Capture must see ordered scan batches before a fused probe changes their schema.
pub(super) fn needs_ordered_scan(node: &Node, ctx: &Context<'_>) -> bool {
    eligible(node, ctx)
        && ctx
            .filtered_reuse
            .as_ref()
            .is_some_and(|cache| cache.lock().unwrap().recording)
}

/// Only retain a speculative demand pass: the ordinary last consumer must not
/// lose lane fusion or allocate an unused cache. Nested preparation restores scope.
pub(super) struct Capture {
    cache: Option<Arc<Mutex<Cache>>>,
    previous: bool,
}
impl Capture {
    pub(super) fn new(ctx: &Context<'_>) -> Self {
        let cache = ctx.filtered_reuse.clone();
        let previous = cache
            .as_ref()
            .is_some_and(|cache| std::mem::replace(&mut cache.lock().unwrap().recording, true));
        Self { cache, previous }
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let Some(cache) = &self.cache {
            cache.lock().unwrap().recording = self.previous;
        }
    }
}

fn implies(current: &ScanKeyFilter, previous: &ScanKeyFilter) -> bool {
    match (&current.numeric, &previous.numeric) {
        (Some(a), Some(b)) => a.is_subset_of(b),
        (None, Some(b)) => current.members.iter().all(|key| {
            key.as_ref()
                .try_into()
                .ok()
                .is_some_and(|bytes| b.contains(i128::from_ne_bytes(bytes)))
        }),
        (None, None) => current.members.is_subset(&previous.members),
        _ => false,
    }
}

/// Replay only a physical-column subset with a membership implying the producer's.
/// The current membership is reapplied, so a superset never leaks extra rows.
pub(super) fn replay(
    node: &Node,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    if !eligible(node, ctx)
        || ctx
            .lane_transforms
            .contains_key(&(node as *const Node as usize))
    {
        return Ok(false);
    }
    let RelOp::Scan { path, columns, .. } = &node.op else {
        return Ok(false);
    };
    let filter = &ctx.scan_filters[&(node as *const Node as usize)];
    let (key, ty) = direct(filter).unwrap();
    let cache = ctx.filtered_reuse.as_ref().unwrap().lock().unwrap();
    let entry = cache
        .entries
        .iter()
        .find(|entry| {
            let (old_key, old_ty) = direct(&entry.filter).unwrap();
            entry.path == *path
                && ty == old_ty
                && columns[key] == entry.columns[old_key]
                && columns.iter().enumerate().all(|(i, column)| {
                    entry
                        .columns
                        .iter()
                        .position(|c| c == column)
                        .is_some_and(|j| node.schema.field(i) == entry.schema.field(j))
                })
                && implies(filter, &entry.filter)
        })
        .cloned();
    drop(cache);
    let Some(entry) = entry else { return Ok(false) };
    ctx.options.cancellation.check()?;
    if Source::capture_checked(path, &|| ctx.options.cancellation.check())
        .ok()
        .as_ref()
        != Some(&entry.source)
    {
        return Ok(false);
    }
    let _metadata = ctx
        .budget
        .reserve(columns.len().saturating_mul(128).saturating_add(512))?;
    let projection = columns
        .iter()
        .map(|column| entry.columns.iter().position(|c| c == column).unwrap())
        .collect::<Vec<_>>();
    // Match shared materialized replay: count each applied membership once,
    // including an empty retained scan, rather than once per retained batch.
    for filter in filter.iter() {
        ctx.metrics.dynamic_scan_filters += 1;
        ctx.metrics.cached_scan_filters += 1;
        ctx.metrics.bitmap_scan_filters += usize::from(filter.numeric.is_some());
        ctx.metrics.numeric_pair_scan_filters += usize::from(filter.numeric_pair.is_some());
    }
    for batch in &entry.batches {
        ctx.options.cancellation.check()?;
        let _scratch = ctx.budget.reserve(
            batch
                .get_array_memory_size()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        // Borrow the retained plan schema; Schema::project would deep-clone
        // arbitrary user metadata in addition to the funded column vector.
        let projected = RecordBatch::try_new(
            node.schema.clone(),
            projection
                .iter()
                .map(|i| batch.column(*i).clone())
                .collect(),
        )
        .map_err(|e| e.to_string())?;
        let start = Instant::now();
        let mask = scan_key_mask(&projected, filter, &ctx.budget)?;
        let filtered = filter_record_batch(&projected, &mask).map_err(|e| e.to_string())?;
        ctx.metrics.filter_seconds += start.elapsed().as_secs_f64();
        // The Parquet membership reader does not deliver fully rejected batches.
        if filtered.num_rows() != 0 {
            emit(filtered)?;
        }
    }
    ctx.metrics.filtered_scan_reuses += 1;
    Ok(true)
}

pub(super) struct Pending(Entry);
impl Pending {
    pub(super) fn new(node: &Node, ctx: &Context<'_>) -> Option<Self> {
        if !needs_ordered_scan(node, ctx)
            || ctx
                .lane_transforms
                .contains_key(&(node as *const Node as usize))
        {
            return None;
        }
        let RelOp::Scan { path, columns, .. } = &node.op else {
            return None;
        };
        let cache = ctx.filtered_reuse.as_ref()?.lock().unwrap();
        // One bounded store per file; avoid retaining incompatible alternatives.
        if cache.entries.iter().any(|entry| entry.path == *path) {
            return None;
        }
        let limit = (ctx.budget.limit() / 32).min(32 << 20);
        let byte_limit = limit.checked_sub(cache.retained_bytes())?;
        let filter = &ctx.scan_filters[&(node as *const Node as usize)];
        let bytes = path
            .as_os_str()
            .len()
            .checked_add(columns.len().checked_mul(16)?)?
            .checked_add(2048)?;
        if bytes.saturating_add(filter_bytes(filter)) > byte_limit {
            return None;
        }
        let credit = ctx.budget.reserve(bytes).ok()?;
        let source = Source::capture_checked(path, &|| ctx.options.cancellation.check()).ok()?;
        Some(Self(Entry {
            path: path.clone(),
            columns: columns.clone(),
            schema: node.schema.clone(),
            source,
            filter: filter.clone(),
            batches: Vec::new(),
            rows: 0,
            row_limit: (node.estimated_rows / 8).clamp(1, 262_144),
            byte_limit,
            credit,
        }))
    }

    /// Fund retained Arrow buffers and both vector capacities before cloning.
    /// A refused reservation or runtime cap drops the entire unpublished store.
    pub(super) fn push(&mut self, batch: &RecordBatch) -> bool {
        let entry = &mut self.0;
        let rows = entry.rows.saturating_add(batch.num_rows());
        let bytes = batch
            .get_array_memory_size()
            .saturating_add(batch.num_columns().saturating_mul(128))
            .saturating_add(4 * std::mem::size_of::<RecordBatch>() + 256);
        if batch.schema().fields() != entry.schema.fields()
            || rows > entry.row_limit
            || entry.retained_bytes().saturating_add(bytes) > entry.byte_limit
            || !entry.credit.try_grow(bytes)
            || entry.batches.try_reserve(1).is_err()
        {
            return false;
        }
        entry.batches.push(batch.clone());
        entry.rows = rows;
        true
    }

    pub(super) fn finish(self, ctx: &mut Context<'_>) {
        if Source::capture_checked(&self.0.path, &|| ctx.options.cancellation.check())
            .ok()
            .as_ref()
            != Some(&self.0.source)
        {
            return;
        }
        let mut cache = ctx.filtered_reuse.as_ref().unwrap().lock().unwrap();
        // Nested demand passes can publish between admission and completion.
        // Recheck cumulative ownership before making this store visible.
        let limit = (ctx.budget.limit() / 32).min(32 << 20);
        if cache.entries.len() < cache.entries.capacity()
            && !cache.entries.iter().any(|entry| entry.path == self.0.path)
            && cache
                .retained_bytes()
                .saturating_add(self.0.retained_bytes())
                <= limit
        {
            cache.entries.push(Arc::new(self.0));
            ctx.metrics.filtered_scan_materializations += 1;
        }
    }
}
