//! Producer-side routing. Only this module constructs fragments; admission never
//! scans their local-ID vectors or rehashes group bytes on the coordinator.
use super::*;

#[derive(Clone)]
pub(in crate::execute) struct RoutingPlan(Arc<Contract>);
struct Contract {
    identity: Arc<()>,
    hash: ahash::RandomState,
    partitions: usize,
    groups: Vec<Type>,
    states: Vec<StateLayout>,
    enabled: bool,
    state_bytes: usize,
    budget: Budget,
    // Fixed shared descriptors and identity must outlive the last producer plan.
    _metadata: Reservation,
    _base: Arc<Reservation>,
}
struct StateLayout {
    canonical: usize,
    function: u8,
    ty: Type,
    input: Option<Type>,
    sums: bool,
}
/// Not Clone: one fragment is admitted once. Fields are visible only to the
/// ownership implementation, never to a constructing producer or scheduler.
pub(in crate::execute) struct RoutedPartial {
    pub(super) raw: Arc<RawPartial>,
    pub(super) groups: Vec<Vec<usize>>,
    pub(super) growth: Vec<usize>,
    pub(super) plan: RoutingPlan,
    // Last: route vectors and raw/plan handles are destroyed before this credit.
    pub(super) credit: Reservation,
}
fn function_id(function: &Aggregate) -> u8 {
    match function {
        Aggregate::Count => 0,
        Aggregate::Sum => 1,
        Aggregate::Avg => 2,
        Aggregate::Min => 3,
        Aggregate::Max => 4,
    }
}
impl RoutingPlan {
    pub(super) fn new(
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        inner: &Partitioned,
        identity: Arc<()>,
        partitions: usize,
    ) -> Result<Self> {
        if expressions.state_sources.len() != p.aggregates.len()
            || expressions
                .state_sources
                .iter()
                .enumerate()
                .any(|(i, &canonical)| {
                    canonical > i || expressions.state_sources[canonical] != canonical
                })
        {
            return Err("invalid partition routing state layout".into());
        }
        let bytes = p
            .groups
            .len()
            .checked_mul(std::mem::size_of::<Type>())
            .and_then(|n| {
                n.checked_add(
                    p.aggregates
                        .len()
                        .checked_mul(std::mem::size_of::<StateLayout>())?,
                )
            })
            .and_then(|n| n.checked_add(1024))
            .ok_or("routing plan metadata overflow")?;
        let metadata = inner.budget.reserve(bytes)?;
        let groups = p.groups.iter().map(|group| group.ty.clone()).collect();
        let states = p
            .aggregates
            .iter()
            .zip(&expressions.state_sources)
            .map(|(aggregate, &canonical)| StateLayout {
                canonical,
                function: function_id(&aggregate.function),
                ty: aggregate.ty.clone(),
                input: aggregate.input.as_ref().map(|input| input.ty.clone()),
                sums: matches!(aggregate.function, Aggregate::Sum | Aggregate::Avg),
            })
            .collect::<Vec<_>>();
        let state_bytes = states
            .iter()
            .enumerate()
            .filter(|(i, s)| *i == s.canonical)
            .try_fold(0usize, |n, (_, state)| {
                n.checked_add(
                    std::mem::size_of::<u64>()
                        + if state.sums {
                            std::mem::size_of::<i256>()
                        } else {
                            0
                        },
                )
            })
            .ok_or("partition state size overflow")?;
        Ok(Self(Arc::new(Contract {
            identity,
            hash: inner.hash.clone(),
            partitions,
            groups,
            states,
            enabled: expressions.enabled,
            state_bytes,
            budget: inner.budget.clone(),
            _metadata: metadata,
            _base: inner._metadata.clone(),
        })))
    }
    pub(super) fn state_sources(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.states.iter().map(|state| state.canonical)
    }
    pub(super) fn state_bytes(&self) -> usize {
        self.0.state_bytes
    }
    pub(super) fn check_layout(
        &self,
        p: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
    ) -> Result<()> {
        let contract = &self.0;
        if p.groups.len() != contract.groups.len()
            || p.groups
                .iter()
                .zip(&contract.groups)
                .any(|(group, ty)| group.ty != *ty)
            || p.aggregates.len() != contract.states.len()
            || expressions.state_sources.len() != contract.states.len()
            || expressions.enabled != contract.enabled
            || p.aggregates
                .iter()
                .zip(&contract.states)
                .zip(&expressions.state_sources)
                .any(|((aggregate, state), &canonical)| {
                    aggregate.distinct
                        || function_id(&aggregate.function) != state.function
                        || aggregate.ty != state.ty
                        || aggregate.input.as_ref().map(|input| &input.ty) != state.input.as_ref()
                        || canonical != state.canonical
                })
        {
            return Err("partition routing plan layout mismatch".into());
        }
        Ok(())
    }
    fn validate_raw(&self, raw: &RawPartial) -> Result<()> {
        let contract = &self.0;
        if !raw.query_budget().same_domain(&contract.budget) {
            return Err("raw partial query funding domain mismatch".into());
        }
        if raw.encounters.len() != raw.group_count {
            return Err("raw partial encounter token count mismatch".into());
        }
        let numeric = contract.enabled
            && contract.groups.len() == 1
            && matches!(contract.groups[0], Type::Int | Type::Decimal(_));
        if raw.group_keys.len() != contract.groups.len()
            || raw.states.len() != contract.states.len()
            || raw.null_group.is_some_and(|group| group >= raw.group_count)
            || (raw.group_count != 0 && raw.numeric_group_keys.is_some() != numeric)
            || raw
                .numeric_group_keys
                .as_ref()
                .is_some_and(|keys| keys.len() != raw.group_count)
            || (raw.numeric_group_keys.is_some() && !raw.encoded.is_empty())
            || (raw.numeric_group_keys.is_none() && raw.encoded.len() != raw.group_count)
            || raw.group_keys.iter().any(|keys| {
                keys.len()
                    != if raw.numeric_group_keys.is_some() {
                        0
                    } else {
                        raw.group_count
                    }
            })
            || raw.states.iter().zip(&contract.states).enumerate().any(
                |(index, (state, layout))| {
                    let rows = if index == layout.canonical {
                        raw.group_count
                    } else {
                        0
                    };
                    state.counts.len() != rows
                        || state.sums.is_some() != layout.sums
                        || state
                            .sums
                            .as_ref()
                            .is_some_and(|values| values.len() != rows)
                        || state.extremes.is_some()
                        || state.distinct.is_some()
                },
            )
        {
            return Err("raw partial routing layout mismatch".into());
        }
        Ok(())
    }
    /// The caller exported this raw state from the same immutable bound
    /// plan/program and key encoder. Shape/funding are checked here; encoded-byte
    /// meaning and globally unique source tokens remain producer contracts.
    pub(in crate::execute) fn route(
        &self,
        raw: Arc<RawPartial>,
        cancellation: &crate::spill::Cancellation,
    ) -> Result<RoutedPartial> {
        cancellation.check()?;
        self.validate_raw(&raw)?;
        let contract = &self.0;
        // Covers vector growth/transients, destination IDs, all eventual
        // jobs/completions, Arc headers and bounded completion error metadata.
        let metadata = raw
            .group_count
            .checked_mul(64)
            .and_then(|n| n.checked_add(contract.partitions.checked_mul(2048)?))
            .and_then(|n| n.checked_add(8192))
            .ok_or("fragment metadata overflow")?;
        let credit = contract.budget.reserve(metadata)?;
        let mut groups = vec![Vec::new(); contract.partitions];
        let mut growth = vec![0usize; contract.partitions];
        let fixed = if raw.numeric_group_keys.is_some() {
            contract
                .state_bytes
                .checked_add(16)
                .and_then(|n| n.checked_mul(4))
                .and_then(|n| n.checked_add(128))
        } else {
            contract
                .groups
                .len()
                .checked_mul(std::mem::size_of::<Key>())
                .and_then(|n| n.checked_add(contract.state_bytes))
                .and_then(|n| n.checked_mul(4))
                .and_then(|n| n.checked_add(512))
        }
        .and_then(|n| n.checked_add(ORDER_BYTES_PER_GROUP))
        .ok_or("partition growth overflow")?;
        for local in 0..raw.group_count {
            cancellation.check()?;
            let mut hash = contract.hash.build_hasher();
            let key_bytes = if let Some(keys) = &raw.numeric_group_keys {
                (raw.null_group != Some(local)).hash(&mut hash);
                if raw.null_group != Some(local) {
                    keys[local].hash(&mut hash);
                }
                0
            } else {
                let key = raw.encoded[local]
                    .as_ref()
                    .ok_or("raw partial missing encoded group")?;
                key.hash(&mut hash);
                key.len()
                    .checked_mul(6)
                    .ok_or("partition growth overflow")?
            };
            let slot = (hash.finish() % contract.partitions as u64) as usize;
            let bytes = fixed
                .checked_add(key_bytes)
                .ok_or("partition growth overflow")?;
            growth[slot] = growth[slot]
                .checked_add(bytes)
                .ok_or("partition growth overflow")?;
            groups[slot].push(local);
        }
        Ok(RoutedPartial {
            raw,
            groups,
            growth,
            plan: self.clone(),
            credit,
        })
    }
    pub(super) fn validate_fragment(&self, fragment: &RoutedPartial) -> Result<()> {
        // Identity also binds seed, partition count and layout: constructors are
        // private and no producer can replace any fragment field.
        if !Arc::ptr_eq(&self.0, &fragment.plan.0)
            || !Arc::ptr_eq(&self.0.identity, &fragment.plan.0.identity)
            || !fragment.credit.belongs_to(&self.0.budget)
            || !fragment.raw.query_budget().same_domain(&self.0.budget)
            || fragment.groups.len() != self.0.partitions
            || fragment.growth.len() != self.0.partitions
            || fragment
                .groups
                .iter()
                .try_fold(0usize, |n, groups| n.checked_add(groups.len()))
                != Some(fragment.raw.group_count)
        {
            return Err("wrong-owner or invalid routed partial".into());
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn set_hash(&mut self, hash: ahash::RandomState) {
        Arc::get_mut(&mut self.0)
            .expect("seed immutable after sharing routing plan")
            .hash = hash;
    }
    #[cfg(test)]
    pub(super) fn hash_for_test(&self) -> &ahash::RandomState {
        &self.0.hash
    }
}

#[cfg(test)]
mod tests;
