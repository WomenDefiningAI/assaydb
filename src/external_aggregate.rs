//! Partition evaluated aggregate inputs, preserving each group's input order.
//! Retrying a partition never publishes partial aggregate output.
use crate::{
    Result,
    execute::{self, Metrics, Options, Prepared},
    expr::{Input, Kind, Value, boolean, is_sql_null, normalize_input},
    expr_program::ExpressionProgram,
    keys::Keys,
    memory::{Budget, Reservation},
    spill::SpillStore,
    spill_partition::{Partition, PartitionWriter},
    types::Type,
};
use arrow::{
    array::{BooleanArray, Int64Array, new_null_array},
    compute::filter,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::{sync::Arc, time::Instant};

struct Projection {
    prepared: Prepared,
    expressions: ExpressionProgram,
    // Per evaluated aggregate input: a DISTINCT Float input whose spilled
    // values use canonical DISTINCT identity (-0.0 as 0.0, one NaN encoding),
    // so the sort-based deduplication below agrees with the in-memory sets.
    canonical_floats: Vec<bool>,
}
impl Projection {
    fn new(original: &Prepared, options: &Options) -> Self {
        let mut plan = original.plan.clone();
        let canonical_floats = plan
            .aggregates
            .iter()
            .filter(|a| a.input.is_some())
            .map(|a| a.distinct && a.input.as_ref().is_some_and(|e| e.ty == Type::Float))
            .collect();
        let expressions = ExpressionProgram::new(
            &plan,
            original.schema.fields().len(),
            options.relational_optimizations,
        );
        let mut types = plan.groups.iter().map(|e| e.ty.clone()).collect::<Vec<_>>();
        for (index, group) in plan.groups.iter_mut().enumerate() {
            group.kind = Kind::Column(index);
        }
        for aggregate in &mut plan.aggregates {
            if let Some(expr) = &mut aggregate.input {
                let index = types.len();
                types.push(expr.ty.clone());
                expr.kind = Kind::Column(index);
            }
        }
        let fields = types
            .iter()
            .enumerate()
            .map(|(index, ty)| ty.field(&format!("aggregate_input_{index}"), true))
            .collect::<Vec<_>>();
        plan.path = None;
        plan.table = None;
        plan.filter = None;
        plan.order.clear();
        plan.columns = (0..fields.len()).collect();
        Self {
            prepared: Prepared {
                auto_simple: None,
                internal_output: original.internal_output,
                plan,
                schema: Arc::new(Schema::new(fields)),
                relational: None,
            },
            expressions,
            canonical_floats,
        }
    }
    fn batch(
        &self,
        original: &Prepared,
        batch: &RecordBatch,
        budget: &Budget,
        options: &Options,
    ) -> Result<(RecordBatch, Reservation)> {
        let _workspace = budget.reserve(
            batch
                .get_array_memory_size()
                .checked_mul(2)
                .and_then(|n| n.checked_add(4096))
                .ok_or("aggregate projection workspace overflow")?,
        )?;
        let mut input = Input {
            columns: vec![None; batch.num_columns()],
            len: batch.num_rows(),
        };
        for i in 0..batch.num_columns() {
            if options.projection_pushdown && !original.plan.columns.contains(&i) {
                continue;
            }
            input.columns[i] = Some(normalize_input(
                batch.column(i),
                &Type::from_field(batch.schema().field(i))?,
                budget,
            )?);
        }
        if let Some(predicate) = &original.plan.filter {
            let mask = predicate.eval_filter(&input, budget)?;
            let mask = boolean(&mask.array);
            let len = mask.iter().filter(|v| *v == Some(true)).count();
            let columns = input
                .columns
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|value| {
                            let _workspace = budget
                                .reserve(value.array.get_array_memory_size().saturating_mul(2))?;
                            Value::adopt(
                                filter(&value.array, mask).map_err(|e| e.to_string())?,
                                budget,
                            )
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            input = Input { columns, len };
        }
        let (groups, values) = self.expressions.evaluate(&mut input, budget)?;
        let canonical_values = values
            .iter()
            .flatten()
            .zip(&self.canonical_floats)
            .filter(|(_, canonical)| **canonical)
            .map(|(value, _)| value);
        let key_bytes =
            groups
                .iter()
                .chain(canonical_values)
                .try_fold(0usize, |bytes, value| {
                    bytes
                        .checked_add(crate::expr::numeric_compare::canonical_bytes(&value.array)?)
                        .ok_or_else(|| "float group key size overflow".to_string())
                })?;
        let _key_credit = (key_bytes != 0)
            .then(|| budget.reserve(key_bytes))
            .transpose()?;
        let arrays = groups
            .iter()
            .chain(values.iter().flatten())
            .enumerate()
            .map(|(index, v)| {
                // One canonical Float identity (signed zeros equal, one NaN):
                // group keys and spilled DISTINCT Float aggregate inputs, so
                // sort-based deduplication agrees with in-memory DISTINCT sets.
                // The key credit funds every copy, including repeated expressions.
                if index < groups.len() || self.canonical_floats[index - groups.len()] {
                    crate::expr::numeric_compare::canonical_array(&v.array)
                } else {
                    v.array.clone()
                }
            })
            .collect();
        // Keep internal wide numeric values. Narrowing an intermediate BIGINT
        // expression here would introduce errors absent from ordinary SUM.
        let batch = RecordBatch::try_new_with_options(
            self.prepared.schema.clone(),
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(input.len)),
        )
        .map_err(|e| e.to_string())?;
        let lease = budget.reserve(batch.get_array_memory_size())?;
        Ok((batch, lease))
    }
}

struct Processor<'a> {
    prepared: &'a Prepared,
    options: &'a Options,
    budget: &'a Budget,
    store: &'a SpillStore,
    emit: &'a mut dyn FnMut(RecordBatch) -> Result<()>,
    metrics: Metrics,
}
impl Processor<'_> {
    fn partition(&mut self, mut partition: Partition, depth: usize) -> Result<()> {
        self.options.cancellation.check()?;
        self.metrics.external_aggregate_partitions += 1;
        let result =
            execute::execute_stream(self.prepared, self.options, self.budget.clone(), |send| {
                for file in &partition.files {
                    self.options.cancellation.check()?;
                    let replay = file.read_batch()?;
                    send(replay.batch.clone())?;
                }
                Ok(())
            });
        match result {
            Ok(result) => {
                crate::rel::merge(&mut self.metrics, &result.metrics);
                // Free input file reservations before downstream retention.
                drop(partition);
                (self.emit)(result.batch.clone())
            }
            Err(error) if error.starts_with("resource-limit: accounted memory") => {
                if self.prepared.plan.aggregates.iter().any(|a| a.distinct)
                    && (!partition.mixed_hashes || depth >= 31)
                {
                    let (partition, prepared) = self.deduplicate(partition)?;
                    let mut child = Processor {
                        prepared: &prepared,
                        options: self.options,
                        budget: self.budget,
                        store: self.store,
                        emit: self.emit,
                        metrics: Metrics::default(),
                    };
                    child.partition(partition, depth)?;
                    crate::rel::merge(&mut self.metrics, &child.metrics);
                    self.metrics.external_aggregate_distinct_passes += 1;
                    return Ok(());
                }
                if depth >= 31 || self.prepared.plan.groups.is_empty() {
                    return Err(format!(
                        "resource-limit: aggregate partition cannot be split further: {error}"
                    )
                    .into());
                }
                let mut writer = PartitionWriter::new(
                    self.store,
                    self.budget,
                    self.options,
                    self.prepared.plan.groups.len(),
                    depth + 1,
                )?;
                while let Some(file) = partition.files.pop_front() {
                    let replay = file.read_batch()?;
                    writer.push(&replay.batch)?;
                }
                let children = writer.finish()?;
                self.metrics.external_aggregate_repartitions += 1;
                for child in children {
                    if child.rows > 0 {
                        self.partition(child, depth + 1)?;
                    }
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    // A single hot group's DISTINCT set cannot be made smaller by hashing the
    // group again. Sort by group/value/sequence, mask later duplicates for each
    // aggregate independently, then restore original sequence. This preserves
    // every row for COUNT(*) and deterministic encounter order. SUM range
    // checks occur at finalization under aggregate semantic v2.
    fn deduplicate(&self, mut partition: Partition) -> Result<(Partition, Prepared)> {
        let mut prepared = Prepared {
            auto_simple: None,
            internal_output: self.prepared.internal_output,
            plan: self.prepared.plan.clone(),
            schema: self.prepared.schema.clone(),
            relational: None,
        };
        let mut fields = prepared
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        let ordinal = fields.len();
        fields.push(Field::new("spill_input_sequence", DataType::Int64, false));
        let schema = Arc::new(Schema::new(fields));
        let settings = self.options.spill.as_ref().unwrap();
        let make_sort = |columns: Vec<usize>| {
            crate::external_sort::ExternalSort::new(
                schema.clone(),
                columns
                    .into_iter()
                    .map(|column| crate::bind::Sort {
                        column,
                        descending: false,
                        nulls_first: true,
                    })
                    .collect(),
                self.store.clone(),
                self.budget.clone(),
                self.options.cancellation.clone(),
                settings
                    .sort_run_bytes
                    .min((self.options.memory_bytes / 32).max(1)),
                settings.merge_fan_in,
                self.options.batch_size,
            )
        };
        let mut with_sequence = false;
        for aggregate in &mut prepared.plan.aggregates {
            if !aggregate.distinct {
                continue;
            }
            let Some(input) = &aggregate.input else {
                return Err("unsupported: DISTINCT wildcard".into());
            };
            let Kind::Column(column) = input.kind else {
                return Err("invalid spilled aggregate binding".into());
            };
            let mut order = (0..prepared.plan.groups.len()).collect::<Vec<_>>();
            order.push(column);
            order.push(ordinal);
            let mut sorter = make_sort(order)?;
            let mut sequence = 0_i64;
            while let Some(file) = partition.files.pop_front() {
                let replay = file.read_batch()?;
                if with_sequence {
                    sorter.push(replay.batch.clone())?;
                } else {
                    let count = i64::try_from(replay.batch.num_rows())
                        .map_err(|_| "aggregate sequence overflow")?;
                    let end = sequence
                        .checked_add(count)
                        .ok_or("aggregate sequence overflow")?;
                    let _lease = self.budget.reserve(
                        replay
                            .batch
                            .num_rows()
                            .checked_mul(8)
                            .ok_or("aggregate sequence allocation overflow")?,
                    )?;
                    let mut arrays = replay.batch.columns().to_vec();
                    arrays.push(Arc::new(Int64Array::from_iter_values(sequence..end)));
                    sorter.push(
                        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?,
                    )?;
                    sequence = end;
                }
            }
            let mut previous: Option<(Vec<u8>, Reservation)> = None;
            sorter.finish(&mut |batch| {
                self.options.cancellation.check()?;
                let _workspace = self.budget.reserve(
                    batch
                        .get_array_memory_size()
                        .checked_mul(3)
                        .and_then(|n| n.checked_add(batch.num_rows().saturating_mul(16) + 4096))
                        .ok_or("distinct mask workspace overflow")?,
                )?;
                let mut keys = batch.columns()[..prepared.plan.groups.len()]
                    .iter()
                    .map(|a| Value::adopt(a.clone(), self.budget))
                    .collect::<Result<Vec<_>>>()?;
                keys.push(Value::adopt(batch.column(column).clone(), self.budget)?);
                let keys = Keys::new(&keys, batch.num_rows(), self.budget)?;
                let mut duplicates = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let key = keys.row(row);
                    let same = previous.as_ref().is_some_and(|(old, _)| old == key);
                    duplicates.push(same && !is_sql_null(batch.column(column).as_ref(), row));
                    if !same {
                        let lease = self
                            .budget
                            .reserve(key.len().checked_add(128).ok_or("distinct key overflow")?)?;
                        previous = Some((key.to_vec(), lease));
                    }
                }
                let mut arrays = batch.columns().to_vec();
                if duplicates.iter().any(|v| *v) {
                    let nulls = new_null_array(arrays[column].data_type(), batch.num_rows());
                    arrays[column] = arrow::compute::kernels::zip::zip(
                        &BooleanArray::from(duplicates),
                        &nulls,
                        &arrays[column],
                    )
                    .map_err(|e| e.to_string())?;
                }
                let masked =
                    RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?;
                partition.files.push_back(self.store.write_batch(&masked)?);
                Ok(())
            })?;
            aggregate.distinct = false;
            with_sequence = true;
        }
        let mut restore = make_sort(vec![ordinal])?;
        while let Some(file) = partition.files.pop_front() {
            let replay = file.read_batch()?;
            restore.push(replay.batch.clone())?;
        }
        restore.finish(&mut |batch| {
            let arrays = batch.columns()[..ordinal].to_vec();
            let batch =
                RecordBatch::try_new(prepared.schema.clone(), arrays).map_err(|e| e.to_string())?;
            partition.files.push_back(self.store.write_batch(&batch)?);
            Ok(())
        })?;
        Ok((partition, prepared))
    }
}

pub(crate) fn run(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    store: SpillStore,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    let start = Instant::now();
    let projection = Projection::new(prepared, options);
    let mut writer = PartitionWriter::new(&store, &budget, options, prepared.plan.groups.len(), 0)?;
    let mut input_rows = 0;
    produce(&mut |batch| {
        options.cancellation.check()?;
        let (projected, _lease) = projection.batch(prepared, &batch, &budget, options)?;
        input_rows += projected.num_rows();
        writer.push(&projected)
    })?;
    let partitions = writer.finish()?;
    let mut processor = Processor {
        prepared: &projection.prepared,
        options,
        budget: &budget,
        store: &store,
        emit,
        metrics: Metrics::default(),
    };
    if input_rows == 0 {
        processor.partition(Partition::default(), 0)?;
    } else {
        for partition in partitions {
            if partition.rows > 0 {
                processor.partition(partition, 0)?;
            }
        }
    }
    processor.metrics.external_aggregate_rows = input_rows;
    processor.metrics.external_aggregate_seconds = start.elapsed().as_secs_f64();
    Ok(processor.metrics)
}
