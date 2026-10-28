//! Scoped recursive bindings and execution.
use super::super::*;
use super::{driver, history, normalization, scope::IterationScope, worktable::Snapshot};
use std::ops::{Deref, DerefMut};

pub(in crate::rel) struct Binding {
    id: usize,
    snapshot: Arc<Snapshot>,
    accumulated: Option<history::View>,
    parent: Option<Box<Binding>>,
    _credit: Reservation,
}
struct Bound<'c, 'a> {
    ctx: &'c mut Context<'a>,
}
impl<'c, 'a> Bound<'c, 'a> {
    fn enter(
        ctx: &'c mut Context<'a>,
        id: usize,
        snapshot: Arc<Snapshot>,
        accumulated: Option<history::View>,
    ) -> Result<Self> {
        ctx.options.cancellation.check()?;
        let credit = ctx.budget.reserve(std::mem::size_of::<Binding>() + 256)?;
        let parent = ctx.recursive_bindings.take();
        ctx.recursive_bindings = Some(Box::new(Binding {
            id,
            snapshot,
            accumulated,
            parent,
            _credit: credit,
        }));
        Ok(Self { ctx })
    }
}
impl<'a> Deref for Bound<'_, 'a> {
    type Target = Context<'a>;
    fn deref(&self) -> &Self::Target {
        self.ctx
    }
}
impl DerefMut for Bound<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx
    }
}
impl Drop for Bound<'_, '_> {
    fn drop(&mut self) {
        let mut binding = self
            .ctx
            .recursive_bindings
            .take()
            .expect("active recursive binding");
        self.ctx.recursive_bindings = binding.parent.take();
    }
}
pub(in crate::rel) fn scan(
    id: usize,
    accumulated: bool,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let mut current = ctx.recursive_bindings.as_deref();
    let binding = loop {
        let binding = current.ok_or("internal: recursive scan outside its invocation")?;
        if binding.id == id {
            break binding;
        }
        current = binding.parent.as_deref();
    };
    if accumulated {
        let view = binding
            .accumulated
            .as_ref()
            .ok_or("internal: recurring scan missing history")?
            .clone();
        view.replay(&ctx.options.cancellation, emit)
    } else {
        let snapshot = binding.snapshot.clone();
        snapshot.replay(&ctx.options.cancellation, emit)
    }
}
// Mirror the recursive plan fields alongside the execution context and sink.
#[allow(clippy::too_many_arguments)]
pub(in crate::rel) fn run(
    node: &Node,
    id: usize,
    anchor: &Node,
    step: &Node,
    distinct: bool,
    accumulate: bool,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let schema = node.schema.clone();
    let budget = ctx.budget.clone();
    let cancel = ctx.options.cancellation.clone();
    let spill = ctx.spill.clone();
    let result = driver::run(
        ctx,
        schema.clone(),
        distinct,
        accumulate,
        budget.clone(),
        cancel.clone(),
        spill,
        |ctx, send| {
            let mut scope = IterationScope::enter(ctx)?;
            anchor.run(&mut scope, &mut |batch| {
                normalization::emit(batch, &schema, &budget, &cancel, send)
            })
        },
        |ctx, delta, accumulated, send| {
            let mut binding = Bound::enter(ctx, id, delta.clone(), accumulated)?;
            let mut scope = IterationScope::enter(&mut binding)?;
            step.run(&mut scope, &mut |batch| {
                normalization::emit(batch, &schema, &budget, &cancel, send)
            })
        },
    )?;
    result.replay(&cancel, emit)
}

#[cfg(test)]
mod tests {
    use super::super::worktable::Builder;
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
    fn snapshot(budget: &Budget, options: &Options, value: i64) -> Arc<Snapshot> {
        let schema = Arc::new(Schema::new(vec![Type::Int.field("n", false)]));
        let mut builder = Builder::new(
            schema.clone(),
            budget.clone(),
            options.cancellation.clone(),
            None,
        )
        .unwrap();
        builder
            .push(
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
                    .unwrap(),
            )
            .unwrap();
        Arc::new(builder.finish().unwrap())
    }
    #[test]
    fn nested_binding_restores_outer_identity_on_failure_and_cancellation() {
        for cancel in [false, true] {
            let options = Options::default();
            let budget = Budget::new(1 << 20);
            let outer_snapshot = snapshot(&budget, &options, 11);
            let inner_snapshot = snapshot(&budget, &options, 22);
            let mut ctx = context(&options, budget.clone());
            {
                let mut outer = Bound::enter(&mut ctx, 7, outer_snapshot.clone(), None).unwrap();
                let baseline = budget.used();
                let result = (|| -> Result<()> {
                    let mut inner = Bound::enter(&mut outer, 7, inner_snapshot.clone(), None)?;
                    assert!(Arc::ptr_eq(
                        &inner.recursive_bindings.as_ref().unwrap().snapshot,
                        &inner_snapshot
                    ));
                    let mut rows = Vec::new();
                    scan(7, false, &mut inner, &mut |b| {
                        rows.push(
                            b.column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(0),
                        );
                        Ok(())
                    })?;
                    assert_eq!(rows, vec![22]);
                    if cancel {
                        options.cancellation.cancel();
                        inner.options.cancellation.check()?;
                    }
                    Err("injected recursive invocation failure".into())
                })();
                assert!(result.is_err());
                assert!(Arc::ptr_eq(
                    &outer.recursive_bindings.as_ref().unwrap().snapshot,
                    &outer_snapshot
                ));
                assert!(outer.recursive_bindings.as_ref().unwrap().parent.is_none());
                assert_eq!(budget.used(), baseline);
            }
            assert!(ctx.recursive_bindings.is_none());
            drop(ctx);
            drop(inner_snapshot);
            drop(outer_snapshot);
            assert_eq!(budget.used(), 0);
        }
    }
}
