//! Scoped recursive iteration cache isolation.
//! Parent imports Context and related private types with `use super::super::*`.
//! A physical cache scope is an epoch: nothing from a previous invocation is
//! addressable while the recursive arm runs. No epoch counter can wrap.
//! Recursive operators MUST be optimizer barriers before enabling this scope:
//! no required predicate may be removed from a plan in anticipation of an outer
//! runtime scan transform. Plans must remain executable with these maps empty.
use super::super::*;
use std::ops::{Deref, DerefMut};

struct Saved<'a> {
    filtered_reuse: Option<Arc<Mutex<filtered_reuse::Cache>>>,
    build_groups: Option<build_groups::Request>,
    demand_cache: Option<&'a std::sync::atomic::AtomicBool>,
    shared_attempt: Option<Arc<crate::execute::pipeline::fallback::Attempt>>,
    scan_filters: HashMap<usize, Arc<ScanKeyFilter>>,
    static_filters: HashMap<usize, Expr>,
    scan_transforms: HashMap<usize, Expr>,
    lane_transforms: HashMap<usize, crate::parquet_scan::BatchTransform>,
    replay_transforms: HashMap<usize, Expr>,
    materialized: HashMap<usize, Arc<Materialized>>,
    sideways_registry_lease: Option<Reservation>,
}

pub(super) struct IterationScope<'c, 'a> {
    ctx: &'c mut Context<'a>,
    saved: Option<Saved<'a>>,
}
impl<'c, 'a> IterationScope<'c, 'a> {
    pub(super) fn enter(ctx: &'c mut Context<'a>) -> Result<Self> {
        ctx.options.cancellation.check()?;
        let saved = Saved {
            filtered_reuse: std::mem::take(&mut ctx.filtered_reuse),
            build_groups: std::mem::take(&mut ctx.build_groups),
            demand_cache: std::mem::take(&mut ctx.demand_cache),
            shared_attempt: std::mem::take(&mut ctx.shared_attempt),
            scan_filters: std::mem::take(&mut ctx.scan_filters),
            static_filters: std::mem::take(&mut ctx.static_filters),
            scan_transforms: std::mem::take(&mut ctx.scan_transforms),
            lane_transforms: std::mem::take(&mut ctx.lane_transforms),
            replay_transforms: std::mem::take(&mut ctx.replay_transforms),
            materialized: std::mem::take(&mut ctx.materialized),
            sideways_registry_lease: std::mem::take(&mut ctx.sideways_registry_lease),
        };
        Ok(Self {
            ctx,
            saved: Some(saved),
        })
    }
}
impl<'c, 'a> Deref for IterationScope<'c, 'a> {
    type Target = Context<'a>;
    fn deref(&self) -> &Self::Target {
        self.ctx
    }
}
impl<'c, 'a> DerefMut for IterationScope<'c, 'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx
    }
}
impl Drop for IterationScope<'_, '_> {
    fn drop(&mut self) {
        let saved = self.saved.take().expect("iteration scope restored once");
        // Restore maps first, dropping their current contents before returning
        // the saved registry reservation. Saved outer allocations stayed funded.
        self.ctx.filtered_reuse = saved.filtered_reuse;
        self.ctx.build_groups = saved.build_groups;
        self.ctx.demand_cache = saved.demand_cache;
        self.ctx.shared_attempt = saved.shared_attempt;
        self.ctx.scan_filters = saved.scan_filters;
        self.ctx.static_filters = saved.static_filters;
        self.ctx.scan_transforms = saved.scan_transforms;
        self.ctx.lane_transforms = saved.lane_transforms;
        self.ctx.replay_transforms = saved.replay_transforms;
        self.ctx.materialized = saved.materialized;
        self.ctx.sideways_registry_lease = saved.sideways_registry_lease;
    }
}

// Query resources remain in Context: options, budget, spill store and metrics.
// Disable shared_attempt within the arm because it contains whole-query fusion
// candidates, not merely source identity. Root shared_retry Sources must traverse
// anchor AND step and verify both externally, independent of this optional path.
// Disable demand_cache/filtered_reuse/build_groups initially. Re-enable only with
// explicit proof of iteration independence; cloning their Arcs is not isolation.
// Metrics remain query-wide; recursive iteration profiles must be aggregated.
// Future recursive-binding stack is separate from these caches: retain outer
// bindings so nested recursion can read enclosing frontiers; add a binding guard
// restoring only its own ID. Reserve its descriptor before pushing, and drop its
// Arc snapshots before releasing descriptor credit. No HashMap insert may occur
// without budgeted capacity growth.

#[cfg(test)]
mod tests {
    use super::*;
    fn context(options: &Options, budget: Budget) -> Context<'_> {
        Context {
            filtered_reuse: None,
            build_groups: None,
            demand_cache: None,
            shared_attempt: None,
            options,
            scan_filters: HashMap::new(),
            static_filters: HashMap::new(),
            scan_transforms: HashMap::new(),
            lane_transforms: HashMap::new(),
            replay_transforms: HashMap::new(),
            materialized: HashMap::new(),
            recursive_bindings: None,
            sideways_registry_lease: None,
            spill: None,
            budget,
            metrics: Metrics::default(),
        }
    }
    fn stored(budget: &Budget) -> Arc<Materialized> {
        Arc::new(Materialized::from_test_batches(Vec::new(), budget).unwrap())
    }
    #[test]
    fn nested_epochs_restore_outer_identity_after_error() {
        let options = Options::default();
        let budget = Budget::new(1 << 20);
        let mut ctx = context(&options, budget.clone());
        let outer = stored(&budget);
        ctx.materialized.insert(7, outer.clone());
        let outcome = (|| -> Result<()> {
            let mut first = IterationScope::enter(&mut ctx)?;
            assert!(first.materialized.is_empty());
            let inner = stored(&budget);
            first.materialized.insert(7, inner.clone());
            {
                let mut second = IterationScope::enter(&mut first)?;
                assert!(second.materialized.is_empty());
                second.materialized.insert(7, stored(&budget));
            }
            assert!(Arc::ptr_eq(first.materialized.get(&7).unwrap(), &inner));
            Err("injected recursive arm failure".into())
        })();
        assert!(outcome.is_err());
        assert!(Arc::ptr_eq(ctx.materialized.get(&7).unwrap(), &outer));
    }
    #[test]
    fn cancellation_restores_saved_cache_and_registry_credit() {
        let options = Options::default();
        let budget = Budget::new(1 << 20);
        let mut ctx = context(&options, budget.clone());
        ctx.sideways_registry_lease = Some(budget.reserve(512).unwrap());
        let outcome = (|| -> Result<()> {
            let mut scope = IterationScope::enter(&mut ctx)?;
            scope.sideways_registry_lease = Some(budget.reserve(256).unwrap());
            options.cancellation.cancel();
            scope.options.cancellation.check()
        })();
        assert!(outcome.is_err());
        assert_eq!(budget.used(), 512);
        assert!(ctx.sideways_registry_lease.is_some());
        assert!(IterationScope::enter(&mut ctx).is_err());
        drop(ctx);
        assert_eq!(budget.used(), 0);
    }
}
