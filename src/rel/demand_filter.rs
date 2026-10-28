//! Optional aggregate/wide-scan demand membership, bounded by the query budget.
use crate::memory::{Budget, Reservation};
use std::collections::HashSet;

pub(super) struct ProbeCache {
    pub(super) batches: Vec<arrow::record_batch::RecordBatch>,
    pub(super) lease: Reservation,
    limit: usize,
}

impl ProbeCache {
    fn new(budget: &Budget) -> crate::Result<Self> {
        Ok(Self {
            batches: Vec::new(),
            lease: budget.reserve(256)?,
            limit: (budget.limit() / 16).min(64 << 20),
        })
    }

    /// Cap exhaustion drops caching, not membership. Allocation refusal drops
    /// the entire preparation. Charge payload and both vector growth buffers
    /// before transferring ownership of a batch out of its live callback.
    fn push(&mut self, batch: arrow::record_batch::RecordBatch) -> crate::Result<bool> {
        let bytes = batch
            .num_columns()
            .checked_mul(64)
            .and_then(|n| n.checked_add(batch.get_array_memory_size()))
            .and_then(|n| {
                n.checked_add(4 * std::mem::size_of::<arrow::record_batch::RecordBatch>() + 256)
            })
            .ok_or("demand cache size overflow")?;
        if self.lease.bytes().saturating_add(bytes) > self.limit {
            return Ok(false);
        }
        if !self.lease.try_grow(bytes) {
            return Err("optional demand cache reservation refused".into());
        }
        self.batches.try_reserve(1).map_err(|e| e.to_string())?;
        self.batches.push(batch);
        Ok(true)
    }
}

pub(super) struct PreparedDemand {
    pub(super) id: usize,
    pub(super) filter: super::ScanKeyFilter,
    pub(super) probe: Option<ProbeCache>,
    pub(super) wide_scan: bool,
}

/// Preparation never emits query output. A failure
/// (including a SQL error encountered speculatively) defers to the ordinary
/// build-then-probe traversal, which chooses the observable error.
pub(super) fn prepare(
    ctx: &mut super::Context<'_>,
    left: &super::Node,
    right: &super::Node,
    keys: &[(crate::expr::Expr, crate::expr::Expr)],
) -> Option<PreparedDemand> {
    use super::*;
    if !ctx.options.relational_optimizations
        || keys.is_empty()
        || keys
            .iter()
            .any(|(e, _)| !membership_safety::expression_is_total(e, left))
        || left.estimated_rows > right.estimated_rows
    {
        return None;
    }
    // Fund target traversal/remapping and the installed filter's descriptors
    // before cloning. Keep this reservation through the filter's entire life.
    let metadata = ctx
        .budget
        .reserve(runtime_filter::traversal_bytes(right))
        .ok()?;
    let aggregate =
        aggregate_scan_target(right, keys.iter().map(|(_, e)| e.clone()).collect()).ok()?;
    let wide_scan = aggregate.is_none();
    let target = if let Some(target) = aggregate {
        target
    } else {
        // The new path never moves a fallible probe, computed/coerced key or
        // partial cached execution before the build. scan_target independently
        // proves every crossed build operator total, including sibling joins.
        if keys.len() != 1
            || !matches!(keys[0].0.kind, crate::expr::Kind::Column(_))
            || !matches!(keys[0].1.kind, crate::expr::Kind::Column(_))
            || keys[0].0.ty != Type::Int
            || keys[0].1.ty != Type::Int
            || !membership_safety::input_is_total(left)
            || !membership_safety::input_is_total(right)
            // The cost credit is saved payload decoding. With the legacy
            // reader, retain the existing build and resource/spill behavior.
            || ctx.options.parquet_cache_bytes.is_none_or(|bytes| bytes == 0)
            || ctx.demand_cache.is_none()
            || ctx.options.error_replay.canonical()
        {
            return None;
        }
        let target = scan_target(right, vec![keys[0].1.clone()]).ok()??;
        if target.payload < 32 || target.has_filter {
            return None;
        }
        target
    };
    let domain = if wide_scan {
        distinct(right, &keys[0].1).min(target.key_domain)
    } else {
        distinct(right, &keys[0].1)
    };
    let worthwhile = |count: usize| {
        if wide_scan {
            runtime_filter::wide_worthwhile(&target, domain, count)
        } else {
            count.saturating_mul(16) < domain
        }
    };
    if wide_scan && !worthwhile(left.estimated_rows) {
        return None;
    }
    let id = target.id;
    if ctx.scan_filters.contains_key(&id) {
        return None;
    }
    // Even one demanded key cannot pay for the extra pass in this domain.
    // Estimates affect admission only, never the complete membership itself.
    if domain <= 16 {
        return None;
    }
    let budget = ctx.budget.clone();
    let mut demand = Members::new(&budget)?;
    // Reuse only a complete, bounded probe with the same input semantics as
    // the real traversal. Ancestor filters/transforms are omitted below, so
    // their presence permits membership (a superset) but forbids batch reuse.
    let mut probe = if ctx.demand_cache.is_some()
        && ctx.scan_filters.is_empty()
        && ctx.static_filters.is_empty()
        && ctx.scan_transforms.is_empty()
        && ctx.lane_transforms.is_empty()
        && ctx.replay_transforms.is_empty()
    {
        Some(ProbeCache::new(&budget).ok()?)
    } else {
        None
    };
    if wide_scan && probe.is_none() {
        return None;
    }
    // Use fresh execution state. Omitting ancestor pruning can only enlarge
    // the demand set; it cannot remove a key needed by the real probe. No
    // partial materialization, filter or shared-pipeline attempt survives.
    let mut preparation = Context {
        filtered_reuse: ctx.filtered_reuse.clone(),
        build_groups: None,
        demand_cache: ctx.demand_cache,
        shared_attempt: None,
        options: ctx.options,
        scan_filters: Default::default(),
        static_filters: Default::default(),
        scan_transforms: Default::default(),
        lane_transforms: Default::default(),
        replay_transforms: Default::default(),
        materialized: Default::default(),
        recursive_bindings: None,
        sideways_registry_lease: None,
        spill: None,
        budget: budget.clone(),
        metrics: Metrics::default(),
    };
    let capture = filtered_reuse::Capture::new(&preparation);
    let result = left.run(&mut preparation, &mut |batch| {
        #[cfg(test)]
        tests::checkpoint("probe-caching", &budget, ctx.options)?;
        // Fund just this live sample, including expression/vector metadata.
        // This workspace lease dies each callback, independently of caching.
        let bytes = batch
            .num_columns()
            .checked_add(keys.len())
            .and_then(|n| n.checked_mul(256))
            .and_then(|n| n.checked_add(512))
            .and_then(|n| n.checked_add(batch.get_array_memory_size()))
            .ok_or("demand sample size overflow")?;
        let _sample = budget.reserve(bytes)?;
        #[cfg(test)]
        tests::checkpoint("normalization", &budget, ctx.options)?;
        let normalized = input(&batch, &budget, keys.iter().map(|(e, _)| e))?;
        #[cfg(test)]
        tests::checkpoint("evaluation", &budget, ctx.options)?;
        let values = keys
            .iter()
            .map(|(e, _)| e.eval(&normalized, &budget))
            .collect::<Result<Vec<_>>>()?;
        #[cfg(test)]
        tests::checkpoint("encoding", &budget, ctx.options)?;
        let encoded = crate::keys::Keys::new(&values, batch.num_rows(), &budget)?;
        #[cfg(test)]
        tests::checkpoint("membership", &budget, ctx.options)?;
        for row in 0..batch.num_rows() {
            if encoded.valid(row)
                && (!demand.insert(encoded.row(row))
                    || demand
                        .members
                        .len()
                        .saturating_mul(if wide_scan { 2 } else { 16 })
                        >= domain)
            {
                // Stop immediately on refusal or unselectivity. Do not finish
                // scanning (or cache) a probe for a filter we will not install.
                return Err("optional demand preparation declined".into());
            }
        }
        // The byte-cost estimate is monotone in membership size. One check
        // per already bounded sample avoids wide integer division per row.
        if wide_scan && !worthwhile(demand.members.len()) {
            return Err("optional wide demand cost declined".into());
        }
        // Check selectivity on this bounded sample BEFORE retaining it. A
        // large selective probe keeps only membership and streams on replay.
        if let Some(cache) = &mut probe
            && !cache.push(batch)?
        {
            if wide_scan {
                return Err("optional wide demand cache cap exceeded".into());
            }
            probe = None;
        }
        Ok(())
    });
    drop(capture);
    merge(&mut ctx.metrics, &preparation.metrics);
    // Unlike transform metrics, these scans belong to a separate execution.
    ctx.metrics.rows_read += preparation.metrics.rows_read;
    result.ok()?;
    ctx.scan_filters.try_reserve(1).ok()?;
    if probe.is_some() {
        // These are complete immutable stores, just as if the ordinary probe
        // had already run. Preserve shared-source reuse instead of decoding it
        // again for the build. On failure the whole preparation is discarded;
        // later failures use the cache-free whole-query retry.
        // Fund transferred map metadata for the storage's lifetime, including
        // reservation-vector growth, rather than the shorter probe-cache lease.
        for stored in preparation.materialized.values_mut() {
            let stored = Arc::get_mut(stored)?;
            let metadata = budget.reserve(512).ok()?;
            stored._leases.try_reserve(1).ok()?;
            stored._leases.push(metadata);
        }
        // SIP registry credit belongs to the receiving context, never to a
        // transferred store or the temporary preparation's shorter lifetime.
        if !preparation.materialized.is_empty()
            && (ctx.sideways_registry_lease.is_some()
                || preparation.sideways_registry_lease.is_some())
        {
            sideways::fund_registry(ctx, preparation.materialized.len()).ok()?;
        }
        ctx.materialized
            .try_reserve(preparation.materialized.len())
            .ok()?;
        for (id, stored) in preparation.materialized.drain() {
            ctx.materialized.entry(id).or_insert(stored);
        }
    }
    drop(preparation);
    let Members {
        members, mut lease, ..
    } = demand;
    lease.absorb(metadata);
    let expressions = target.expressions;
    if probe.is_some()
        && let Some(attempt) = &ctx.demand_cache
    {
        attempt.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    Some(PreparedDemand {
        id,
        filter: ScanKeyFilter {
            previous: None,
            optional_total: wide_scan,
            numeric: numeric_membership(&expressions, &members, &budget),
            numeric_pair: numeric_pair_membership(&expressions, &members, &budget),
            expressions,
            members: Arc::new(members),
            _lease: Arc::new(lease),
        },
        probe,
        wide_scan,
    })
}

pub(super) struct Members {
    pub(super) members: HashSet<Box<[u8]>, ahash::RandomState>,
    pub(super) lease: Reservation,
    limit: usize,
}

impl Members {
    pub(super) fn new(budget: &Budget) -> Option<Self> {
        let limit = budget.limit() / 16;
        if limit < 256 {
            return None;
        }
        Some(Self {
            members: HashSet::default(),
            lease: budget.reserve(256).ok()?,
            limit,
        })
    }

    /// False means discard the entire optional filter, never use a partial set.
    pub(super) fn insert(&mut self, key: &[u8]) -> bool {
        if self.members.contains(key) {
            return true;
        }
        if self.members.len() == 1 << 20 {
            return false;
        }
        // Prepay payload and table growth. With the pinned standard HashSet's
        // 7/8 load factor, simultaneous old + doubled tables use fewer than
        // 4 buckets per member, each a two-word Box plus a control byte.
        // 128 bytes/member covers these buckets and allocation padding; the
        // initial 256 covers small tables/control tails. Twice the key length
        // additionally covers the boxed bytes conservatively. No lease gap at
        // installation, and no uncharged Vec of per-member reservations.
        let Some(bytes) = key.len().checked_mul(2).and_then(|n| n.checked_add(128)) else {
            return false;
        };
        if self
            .lease
            .bytes()
            .checked_add(bytes)
            .is_none_or(|total| total > self.limit)
            || !self.lease.try_grow(bytes)
            || self.members.try_reserve(1).is_err()
        {
            return false;
        }
        self.members.insert(key.into());
        true
    }
}

#[cfg(test)]
pub(super) mod tests;
