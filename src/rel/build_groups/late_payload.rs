//! Keep dictionary IDs in non-sort group slots until a completed sort's LIMIT.
//! Only direct, identically typed columns cross this boundary; all expressions
//! and all aggregate completion checks remain at their original positions.
use super::*;
#[cfg(test)]
mod tests;

pub(super) struct Selection<'a> {
    order: &'a bind::Plan,
    output_schema: &'a SchemaRef,
    count: usize,
}

/// Only selected arrays and their own credit may cross the consumer boundary.
/// In particular, a slice of the sorted batch would retain unselected buffers.
pub(super) struct Selected {
    batch: RecordBatch,
    _credit: Reservation,
}

impl Selected {
    fn copy(batch: RecordBatch, budget: &Budget) -> Result<Self> {
        let rows = u32::try_from(batch.num_rows())
            .map_err(|_| "resource-limit: accounted memory: selected row index width")?;
        let _indices_credit =
            budget.reserve(batch.num_rows().saturating_mul(4).saturating_add(1024))?;
        let indices = UInt32Array::from_iter_values(0..rows);
        // Fund array descriptors and gather construction. The output schema is
        // the existing ORDER BY Arc, owned by the query throughout delivery.
        let descriptors = 1024_usize.saturating_add(batch.num_columns().saturating_mul(1024));
        let bytes = batch.columns().iter().try_fold(descriptors, |n, array| {
            crate::join_filter::gather_bytes(array, &indices).map(|bytes| n.saturating_add(bytes))
        })?;
        let mut credit = budget.reserve(bytes)?;
        let arrays = batch
            .columns()
            .iter()
            .map(|array| {
                arrow::compute::take(array, &indices, None)
                    .map_err(|e| crate::Error::from(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(batch.schema(), arrays)
            .map_err(|e| crate::Error::from(e.to_string()))?;
        // As in the parent sorter, release construction slack before delivery.
        // No schema or Fields were allocated, so retain the parent's exact
        // array-bytes-plus-1024 credit, including for wide/long-named schemas.
        credit.release_excess(batch.get_array_memory_size().saturating_add(1024));
        Ok(Self {
            batch,
            _credit: credit,
        })
    }

    pub(super) fn emit(self, emit: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        // Retain only this result's lease throughout the callback, even if the
        // callback mutates state and then fails. Consumer errors never replay.
        emit(self.batch)
    }
}

/// Admit just ORDER BY / direct projection / dictionary aggregation. In
/// particular HAVING, computed projections, spill and canonical replay retain
/// their ordinary path. No cardinality or query-specific assumptions are used.
pub(in crate::rel) fn try_ordered_limit(
    order: &bind::Plan,
    output_schema: &SchemaRef,
    child: &Node,
    count: usize,
    ctx: &mut Context<'_>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    // The caller supplies the existing ORDER BY output schema, including any
    // aliases/reordering. Selection shares it verbatim and never builds Fields.
    // A future path needing a new schema must fund its full size and decline
    // before consumption if it exceeds the parent's array-bytes-plus-1024 cap.
    let RelOp::Unary { plan, input } = &child.op else {
        return Ok(false);
    };
    if !plan.aggregation
        || order.aggregation
        || order.filter.is_some()
        || !order.groups.is_empty()
        || !order.aggregates.is_empty()
        || order.order.is_empty()
        || order.output.iter().enumerate().any(|(i, output)| {
            let bind::Output::Expr(Expr {
                kind: Kind::Column(column),
                ty,
            }) = output
            else {
                return true;
            };
            plan.types.get(*column) != Some(ty) || order.types.get(i) != Some(ty)
        })
    {
        return Ok(false);
    }
    let selection = Selection {
        order,
        output_schema,
        count,
    };
    if selection.id_column(plan).is_none() {
        return Ok(false);
    }
    // Pay for all cloned plans, remapping vectors, expressions and schemas
    // before constructing any of them, independently of the dictionary credit.
    let Ok(credit) = ctx.budget.reserve(
        plan_bytes(plan)
            .saturating_add(plan_bytes(order))
            .saturating_mul(16)
            .saturating_add(64 * 1024),
    ) else {
        return Ok(false);
    };
    let prepared = Prepared {
        auto_simple: None,
        internal_output: true,
        plan: plan.clone(),
        schema: input.schema.clone(),
        relational: None,
    };
    let result = try_execute_inner(&prepared, input, ctx, emit, Some(selection));
    drop(prepared);
    drop(credit);
    if let Some((metrics, selected)) = result? {
        merge(&mut ctx.metrics, &metrics);
        ctx.options.cancellation.check()?;
        if let Some(selected) = selected {
            selected.emit(emit)?;
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

impl Selection<'_> {
    /// Admission checked every outer expression is a direct column reference.
    fn source_column(&self, column: usize) -> usize {
        let bind::Output::Expr(Expr {
            kind: Kind::Column(source),
            ..
        }) = self.order.output[column]
        else {
            unreachable!("direct projection admission")
        };
        source
    }

    /// Sort keys must have their real values for every group before sorting.
    fn sort_key(&self, column: usize) -> bool {
        self.order
            .order
            .iter()
            .any(|s| self.source_column(s.column) == column)
    }

    /// Retain an ID in a deferred text slot; its value cannot be replaced by
    /// eager sort-key restoration. With no such payload there is no useful win.
    pub(super) fn id_column(&self, plan: &bind::Plan) -> Option<usize> {
        plan.output.iter().enumerate().position(|(i, output)| {
            matches!(output, bind::Output::Group(_))
                && plan.types[i] == Type::Text
                && !self.sort_key(i)
        })
    }

    /// The ordinary sorter funds retained ID/key arrays and replacement peaks
    /// before allocation, and owns TK1's unchanged ambiguous-tie fallback. Its
    /// producer runs every aggregate finalization before selecting any row.
    pub(super) fn execute(
        &self,
        request: &Request,
        (prepared, id_column): (&Prepared, usize),
        ctx: &mut Context<'_>,
        produce: &mut impl FnMut(
            &mut Context<'_>,
            &mut dyn FnMut(RecordBatch) -> Result<()>,
        ) -> Result<Metrics>,
    ) -> Result<(Metrics, Option<Selected>)> {
        let budget = ctx.budget.clone();
        let options = ctx.options;
        let mut partial = Prepared {
            auto_simple: None,
            internal_output: true,
            plan: prepared.plan.clone(),
            schema: prepared.schema.clone(),
            relational: None,
        };
        for (i, output) in partial.plan.output.iter_mut().enumerate() {
            if matches!(output, bind::Output::Group(_)) && !self.sort_key(i) {
                // This descriptor is used only by Request::output: non-Group
                // slots pass through the already finalized numeric batch.
                *output = bind::Output::Expr(Expr {
                    kind: Kind::Column(i),
                    ty: Type::Int,
                });
                partial.plan.types[i] = Type::Int;
            }
        }
        let restore_sort_groups = partial
            .plan
            .output
            .iter()
            .any(|output| matches!(output, bind::Output::Group(_)));
        let types = &partial.plan.types;
        let sort = Prepared {
            auto_simple: None,
            internal_output: true,
            schema: schema(&partial.plan.names, types),
            relational: None,
            plan: bind::Plan {
                table: None,
                path: None,
                columns: (0..types.len()).collect(),
                filter: None,
                groups: Vec::new(),
                aggregates: Vec::new(),
                output: types
                    .iter()
                    .enumerate()
                    .map(|(i, ty)| {
                        bind::Output::Expr(Expr {
                            kind: Kind::Column(i),
                            ty: ty.clone(),
                        })
                    })
                    .collect(),
                names: partial.plan.names.clone(),
                types: types.clone(),
                order: self
                    .order
                    .order
                    .iter()
                    .map(|s| bind::Sort {
                        column: self.source_column(s.column),
                        descending: s.descending,
                        nulls_first: s.nulls_first,
                    })
                    .collect(),
                aggregation: false,
                projection_pushdown: true,
            },
        };
        let mut metrics = Metrics::default();
        let result = execute::execute_stream_top_k(
            &sort,
            options,
            budget.clone(),
            Some(self.count),
            |send| {
                metrics = if restore_sort_groups {
                    produce(ctx, &mut |batch| {
                        request.output(batch, &partial, id_column, &budget, send)
                    })
                } else {
                    // With aggregate-only sort keys, every group slot stays an
                    // ID and the finalized batch already has the sort schema.
                    // Avoid constructing unused representative indices and a
                    // replacement batch for every group chunk. The sorter still
                    // consumes every chunk before selection and restoration.
                    produce(ctx, send)
                }?;
                Ok(())
            },
        )?;
        merge(&mut metrics, &result.metrics);
        let rows = self.count.min(result.batch.num_rows());
        options.cancellation.check()?;
        let mut selected = None;
        if rows > 0 {
            // Fund an independent copy while the dictionary, restoration leases
            // and full sorted result still cover the replacement peak. Nothing
            // reaches the consumer until their entire execution scope returns.
            request.output(
                result.batch.slice(0, rows),
                prepared,
                id_column,
                &budget,
                &mut |batch| {
                    let columns = (0..self.order.output.len())
                        .map(|i| batch.column(self.source_column(i)).clone())
                        .collect();
                    selected = Some(Selected::copy(
                        RecordBatch::try_new(self.output_schema.clone(), columns)
                            .map_err(|e| crate::Error::from(e.to_string()))?,
                        &budget,
                    )?);
                    Ok(())
                },
            )?;
        }
        metrics.late_payload_sorts += 1;
        metrics.late_payload_rows += rows;
        Ok((metrics, selected))
    }
}
