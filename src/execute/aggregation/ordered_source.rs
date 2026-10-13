//! Source-order coordinates for whole stable-filtered batches. The row coordinate
//! is a filtered ordinal, not physical-row provenance. Never split a batch and
//! reset that ordinal; this API deliberately requires strictly increasing prefixes.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::execute) struct SourceBatch {
    row_group: u64,
    batch: u64,
}
impl SourceBatch {
    pub(in crate::execute) fn new(row_group: u64, batch: u64) -> Self {
        Self { row_group, batch }
    }
    pub(super) fn token(self, filtered_row: usize) -> EncounterToken {
        EncounterToken::new(self.row_group, self.batch, filtered_row)
    }
}
impl Aggregation {
    pub(in crate::execute) fn update_ordered_source(
        &mut self,
        plan: &Plan,
        expressions: &crate::expr_program::ExpressionProgram,
        metrics: &mut Metrics,
        batch: EvaluatedBatch,
        source: SourceBatch,
    ) -> Result<()> {
        // All admission checks precede reservation and mutation. Once any
        // arbitrary-token state exists, it cannot use this ordered shortcut.
        if plan.groups.is_empty()
            || batch.rows == 0
            || !self.encounters_valid
            || self.encounters.len() != self.group_count
            || match self.last_ordered_batch {
                Some(previous) => source <= previous,
                None => self.group_count != 0,
            }
        {
            return Err(
                "ordered aggregation requires a strictly increasing complete source batch".into(),
            );
        }
        self.update_tracked::<true>(plan, expressions, metrics, batch, None, Some(source))?;
        self.last_ordered_batch = Some(source);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
