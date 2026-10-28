//! Iterative fixpoint driver with immutable frontier snapshots.
use super::{
    admission::Admission,
    history,
    worktable::{Builder, Snapshot},
};
use crate::{
    Result,
    memory::{Budget, Reservation},
    spill::{Cancellation, SpillStore},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::sync::Arc;

pub(super) struct Generation {
    pub(super) snapshot: Arc<Snapshot>,
    _credit: Reservation,
}
pub(super) struct ResultTable {
    generations: Vec<Generation>,
}
impl ResultTable {
    pub(super) fn replay(
        &self,
        cancel: &Cancellation,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        for generation in &self.generations {
            generation.snapshot.replay(cancel, emit)?;
        }
        cancel.check()
    }
}

// Keep each execution resource and both callbacks explicit at this boundary.
#[allow(clippy::too_many_arguments)]
pub(super) fn run<S>(
    state: &mut S,
    schema: SchemaRef,
    distinct: bool,
    accumulate: bool,
    budget: Budget,
    cancel: Cancellation,
    spill: Option<SpillStore>,
    mut anchor: impl FnMut(&mut S, &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
    mut step: impl FnMut(
        &mut S,
        &Arc<Snapshot>,
        Option<history::View>,
        &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()>,
) -> Result<ResultTable> {
    let mut admission = Admission::new(schema.clone(), distinct, budget.clone(), cancel.clone())?;
    let mut builder = Builder::new(
        schema.clone(),
        budget.clone(),
        cancel.clone(),
        spill.clone(),
    )?;
    anchor(state, &mut |batch| {
        admission.admit(batch, &mut |batch| builder.push(batch))
    })?;
    let mut next = builder.finish()?;
    let mut generations = Vec::new();
    let history = if accumulate {
        Some(history::History::new(budget.clone())?)
    } else {
        None
    };
    while next.rows() != 0 {
        cancel.check()?;
        let credit = budget.reserve(
            std::mem::size_of::<Generation>()
                .checked_mul(4)
                .and_then(|n| n.checked_add(256))
                .ok_or("recursive generation accounting overflow")?,
        )?;
        generations
            .try_reserve(1)
            .map_err(|_| "resource-limit: recursive generation allocation")?;
        generations.push(Generation {
            snapshot: Arc::new(next),
            _credit: credit,
        });
        let delta = &generations.last().expect("nonempty generations").snapshot;
        let view = history
            .as_ref()
            .map(|history| history.append(delta.clone(), &cancel))
            .transpose()?;
        let mut builder = Builder::new(
            schema.clone(),
            budget.clone(),
            cancel.clone(),
            spill.clone(),
        )?;
        // Both current delta and accumulated generations are read-only until the
        // arm completes. Caller installs bindings + IterationScope, runs the
        // already-bound step plan, then restores both guards on every exit path.
        step(state, delta, view, &mut |batch| {
            admission.admit(batch, &mut |batch| builder.push(batch))
        })?;
        next = builder.finish()?;
    }
    cancel.check()?;
    Ok(ResultTable { generations })
}
// No rows escape run before successful completion. State drops on error/cancel.
// This implements ordinary UNION/ALL fixpoints with immutable accumulated scans;
// it does not implement USING KEY or early consumer demand/outer LIMIT. Infinite
// ALL recursion requires cancellation or explicit resource failure in this draft.
// Planner must normalize anchor and step to a single exact schema first.
// Shared mutable execution state is passed explicitly to both callbacks, so a
// Context adapter never needs two simultaneously captured mutable borrows.
