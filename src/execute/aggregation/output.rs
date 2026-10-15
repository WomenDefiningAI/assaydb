//! Statically dispatched finalization shared by ordinary and partition owners.
//! Input updates intentionally do not belong to this interface.
use super::*;
use std::ops::Range;

pub(in crate::execute) trait AggregateOutput {
    fn finish_updates(&mut self) -> Result<()>;
    fn group_count(&self) -> usize;
    fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
    ) -> Result<RecordBatch>;
    /// The default retains the existing owner finalization and total filtering.
    fn having_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
        having: &having::Having,
    ) -> Result<RecordBatch> {
        having.filter(self.output_batch(plan, expressions, schema, range)?)
    }
    fn output_bytes(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        range: Range<usize>,
    ) -> Result<usize>;
}
impl AggregateOutput for Aggregation {
    fn finish_updates(&mut self) -> Result<()> {
        Aggregation::finish_updates(self)
    }
    fn group_count(&self) -> usize {
        self.group_count
    }
    fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
    ) -> Result<RecordBatch> {
        Aggregation::output_batch(self, plan, expressions, schema, range)
    }
    /// Ordinary groups are visited in their unchanged encounter order.
    fn having_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
        having: &having::Having,
    ) -> Result<RecordBatch> {
        having::batch(
            || range.clone().map(move |group| (self, group)),
            range.len(),
            plan,
            expressions,
            schema,
            having,
        )
    }
    fn output_bytes(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        range: Range<usize>,
    ) -> Result<usize> {
        Aggregation::output_bytes(self, plan, expressions, range)
    }
}

/// Constructed only after the producer/partition barrier. Keep the partition
/// owner before cancellation, matching the former Backend variant field order.
pub(in crate::execute) struct PartitionOutput {
    state: OwnedPartitions,
    cancellation: crate::spill::Cancellation,
}
impl PartitionOutput {
    pub(in crate::execute) fn new(
        state: OwnedPartitions,
        cancellation: crate::spill::Cancellation,
    ) -> Self {
        Self {
            state,
            cancellation,
        }
    }
}
impl AggregateOutput for PartitionOutput {
    fn finish_updates(&mut self) -> Result<()> {
        self.state.finish(&self.cancellation)
    }
    fn group_count(&self) -> usize {
        self.state.group_count()
    }
    fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
    ) -> Result<RecordBatch> {
        self.state.output_batch(plan, expressions, schema, range)
    }
    fn output_bytes(
        &self,
        plan: &Plan,
        _expressions: &crate::expr_program::ExpressionProgram,
        range: Range<usize>,
    ) -> Result<usize> {
        self.state.output_bytes(plan, range)
    }
}

// Existing forced-partition SQL tests retain their enum-based serial oracle.
// The production Executor default resolves directly to Aggregation instead.
#[cfg(test)]
impl AggregateOutput for Backend {
    fn finish_updates(&mut self) -> Result<()> {
        Backend::finish_updates(self)
    }
    fn group_count(&self) -> usize {
        Backend::group_count(self)
    }
    fn output_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
    ) -> Result<RecordBatch> {
        Backend::output_batch(self, plan, expressions, schema, range)
    }
    /// Test backends use the same ordinary kernel or retain partition finalization.
    fn having_batch(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        schema: &SchemaRef,
        range: Range<usize>,
        having: &having::Having,
    ) -> Result<RecordBatch> {
        match self {
            Self::Single(state) => state.having_batch(plan, expressions, schema, range, having),
            Self::Partitioned { .. } => {
                having.filter(self.output_batch(plan, expressions, schema, range)?)
            }
        }
    }
    fn output_bytes(
        &self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        range: Range<usize>,
    ) -> Result<usize> {
        Backend::output_bytes(self, plan, expressions, range)
    }
}
