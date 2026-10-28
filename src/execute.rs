//! Batched execution with bounded scan workers and query-wide spill selection.
mod aggregate_workers;
mod aggregation;
pub(crate) use aggregation::SORTED_DISTINCT_DECLINED;
pub(crate) use aggregation::having::Having;
pub(crate) use aggregation::window::WindowAccumulator;
use aggregation::{AggregateOutput, States};
#[cfg(not(test))]
type OrdinaryAggregation = aggregation::Aggregation;
#[cfg(test)]
type OrdinaryAggregation = aggregation::Backend;
pub(crate) mod error_replay;
pub(crate) mod pipeline;
mod scan_projection;
use crate::{
    Result,
    bind::{self, Aggregate, Output, Plan},
    expr::{Input, Value, boolean, is_sql_null, normalize_input, numeric},
    memory::{Budget, Reservation},
    types::Type,
};
use arrow::{
    array::*,
    compute::{FilterBuilder, SortColumn, SortOptions, concat_batches, take},
    datatypes::{Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
pub use error_replay::{ErrorReplay, ErrorReplayMetrics};
use parquet::arrow::ProjectionMask;
use serde::Serialize;
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc, time::Instant};

#[derive(Clone)]
pub struct Options {
    #[cfg(test)]
    pub(crate) test_scan_observer: Option<Arc<crate::parquet_scan::test_support::Observer>>,
    /// Cumulative error-only replay diagnostics, including unsuccessful replays.
    pub error_replay: ErrorReplay,
    #[cfg(test)]
    pub(crate) test_source_owned_pipeline: bool,
    #[cfg(test)]
    pub(crate) test_source_owned_report:
        Option<Arc<std::sync::Mutex<pipeline::source_owned::Report>>>,
    #[cfg(test)]
    pub(crate) test_shared_batch_pipeline: bool,
    #[cfg(test)]
    pub(crate) test_shared_after_emit_failure: bool,
    /// Force serial partition state for SQL-level correctness tests only.
    #[cfg(test)]
    pub(crate) test_aggregate_partitions: Option<usize>,
    /// Exercise automatic production admission inside unit tests.
    #[cfg(test)]
    pub(crate) test_pipeline_default: bool,
    /// Force the direct owned pipeline correctness path; absent in production.
    #[cfg(test)]
    pub(crate) test_pipeline_partitions: Option<usize>,
    /// Fixed prepaid private aggregation slice; correctness experiments only.
    #[cfg(test)]
    pub(crate) test_pipeline_slice_bytes: Option<usize>,
    /// Exercise one clean resource fallback to the unchanged baseline, tests only.
    #[cfg(test)]
    pub(crate) test_pipeline_fallback: bool,
    #[cfg(test)]
    pub(crate) test_pipeline_report: Option<Arc<std::sync::Mutex<PipelineAttemptReport>>>,
    /// Refuse decoder reservations after a live producer yields, tests only.
    #[cfg(test)]
    pub(crate) test_pipeline_interruption: Option<Arc<std::sync::Mutex<PipelineInterruption>>>,
    /// Synchronize real autonomous dispatches without changing their quantum.
    #[cfg(test)]
    pub(crate) test_pipeline_continuation: Option<Arc<pipeline::continuation_tests::Hooks>>,
    /// Total execution thread limit, including the calling/coordinator thread.
    pub workers: usize,
    pub batch_size: usize,
    pub memory_bytes: usize,
    pub projection_pushdown: bool,
    pub relational_optimizations: bool,
    /// None selects the ordinary reader; Some(0) isolates buffering without
    /// decoded predicate reuse. Nonzero capacities remain per-scan and bounded.
    pub parquet_cache_bytes: Option<usize>,
    pub spill: Option<crate::spill::SpillOptions>,
    pub cancellation: crate::spill::Cancellation,
    /// Session time zone for TIMESTAMPTZ calendar operations (default UTC).
    pub time_zone: crate::zone::SessionZone,
    /// Fixed statement clock (microseconds since 1970-01-01 UTC) for
    /// reproducible NOW()/CURRENT_*; None reads the system clock.
    pub fixed_clock: Option<i64>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            test_scan_observer: None,
            error_replay: ErrorReplay::default(),
            #[cfg(test)]
            test_source_owned_pipeline: false,
            #[cfg(test)]
            test_source_owned_report: None,
            #[cfg(test)]
            test_shared_batch_pipeline: false,
            #[cfg(test)]
            test_shared_after_emit_failure: false,
            #[cfg(test)]
            test_aggregate_partitions: None,
            #[cfg(test)]
            test_pipeline_partitions: None,
            #[cfg(test)]
            test_pipeline_default: false,
            #[cfg(test)]
            test_pipeline_slice_bytes: None,
            #[cfg(test)]
            test_pipeline_fallback: false,
            #[cfg(test)]
            test_pipeline_report: None,
            #[cfg(test)]
            test_pipeline_interruption: None,
            #[cfg(test)]
            test_pipeline_continuation: None,
            workers: 1,
            batch_size: 8192,
            memory_bytes: 256 * 1024 * 1024,
            projection_pushdown: true,
            relational_optimizations: true,
            parquet_cache_bytes: Some(16 * 1024 * 1024),
            spill: None,
            cancellation: crate::spill::Cancellation::default(),
            time_zone: crate::zone::SessionZone::default(),
            fixed_clock: None,
        }
    }
}
#[cfg(test)]
#[derive(Default)]
pub(crate) struct PipelineInterruption {
    pub budget: Option<Budget>,
    pub queued_sources: usize,
    pub aggregated_rows: usize,
    pub attempt_report: Option<PipelineAttemptReport>,
}
/// Attempt diagnostics also survive an unsuccessful baseline replay.
#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct PipelineAttemptReport {
    pub candidate_attempts: usize,
    pub fallback_attempts: usize,
    pub candidate_seconds: f64,
    pub fallback_seconds: f64,
    pub total_seconds: f64,
    pub candidate_peak_reserved_bytes: usize,
    pub peak_reserved_bytes: usize,
    pub peak_workers: usize,
    pub candidate_error: Option<crate::Error>,
    pub candidate_sql_error: Option<crate::Error>,
}
#[derive(Debug, Default, Serialize)]
pub struct Metrics {
    /// Completed sorts whose exact prefix was selected by the bounded heap.
    pub top_k_sorts: usize,
    /// Dictionary group payload restored only after a completed ORDER BY/LIMIT.
    pub late_payload_sorts: usize,
    pub late_payload_rows: usize,
    /// Wide scan builds restricted by a complete retained probe membership.
    pub demand_filtered_scans: usize,
    /// Row groups aggregated with validated non-decreasing Parquet intervals.
    pub clustered_row_groups: usize,
    pub pipeline_producers: usize,
    pub pipeline_background_producers: usize,
    pub pipeline_source_batches: usize,
    pub pipeline_step_jobs: usize,
    pub pipeline_pressure_flushes: usize,
    pub pipeline_exchange_peak: usize,
    pub pipeline_worker_handoffs: usize,
    /// Coordinator proof/boundary wall intervals for admitted source-owned
    /// operators; sums across operators, not a whole-query serial fraction.
    pub source_domain_proof_seconds: f64,
    pub source_boundary_merge_seconds: f64,
    pub source_boundary_merges: usize,
    pub pipeline_source_routes: usize,
    pub pipeline_source_routed_groups: usize,
    pub pipeline_background_routes: usize,
    pub pipeline_runtime_submitted: [usize; 3],
    pub pipeline_runtime_caller: [usize; 3],
    pub pipeline_runtime_background: [usize; 3],
    pub pipeline_runtime_completed: [usize; 3],
    pub pipeline_runtime_peak_outstanding: usize,
    pub pipeline_runtime_peak_queued: usize,
    pub pipeline_candidate_attempts: usize,
    pub pipeline_fallbacks: usize,
    pub pipeline_candidate_seconds: f64,
    pub pipeline_fallback_seconds: f64,
    pub pipeline_candidate_peak_reserved_bytes: usize,
    pub pipeline_candidate_error: Option<crate::Error>,
    pub pipeline_candidate_sql_error: Option<crate::Error>,
    pub auto_spill_retries: usize,
    pub auto_spill_initial_seconds: f64,
    pub auto_spill_initial_peak_reserved_bytes: usize,
    pub auto_spill_initial_error: Option<crate::Error>,
    /// Live execution threads, including the coordinator and blocked workers.
    pub worker_threads_peak: usize,
    pub parallel_scan_tasks: usize,
    pub parallel_scan_batches: usize,
    pub parallel_scan_pending_batches_peak: usize,
    pub parallel_scan_pending_bytes_peak: usize,
    pub parallel_scan_queue_slots_max: usize,
    /// Summed active reader time, including pre/post-decode filters. This can
    /// overlap filter_seconds and is not additive with coordinator wall time.
    pub parallel_scan_worker_seconds: f64,
    pub parallel_scan_caller_seconds: f64,
    pub parallel_scan_caller_tasks: usize,
    pub parallel_scan_backpressure_seconds: f64,
    pub parallel_replays: usize,
    pub replay_tasks: usize,
    pub replay_batches: usize,
    pub replay_input_rows: usize,
    pub replay_exact_filter_batches: usize,
    pub replay_pending_batches_peak: usize,
    pub replay_pending_bytes_peak: usize,
    /// Unique-build joins composed into ordered scan-lane transforms.
    pub lane_fused_joins: usize,
    /// A qualifying grouped SUM replaced a second scan of the same input.
    pub aggregate_scan_reuses: usize,
    pub parallel_joins: usize,
    pub parallel_join_tasks: usize,
    pub parallel_join_batches: usize,
    pub parallel_join_pending_batches_peak: usize,
    pub parallel_join_pending_bytes_peak: usize,
    /// Inclusive worker wall time, including credit waits and blocked output sends.
    pub parallel_join_worker_wall_seconds: f64,
    pub parallel_aggregate_batches: usize,
    pub scan_expression_batches: usize,
    pub scan_group_key_batches: usize,
    /// Summed preparation work, overlapping active scan-lane time.
    pub scan_expression_seconds: f64,
    pub scan_normalize_seconds: f64,
    /// Summed helper work; overlaps aggregate_seconds wall time.
    pub parallel_aggregate_worker_seconds: f64,
    pub spilled_materializations: usize,
    pub materialization_replay_batches: usize,
    pub external_join_partitions: usize,
    pub external_join_repartitions: usize,
    pub external_join_block_fallbacks: usize,
    pub external_join_input_rows: usize,
    pub external_join_seconds: f64,
    pub external_aggregate_partitions: usize,
    pub external_aggregate_repartitions: usize,
    pub external_aggregate_distinct_passes: usize,
    pub external_aggregate_rows: usize,
    pub external_aggregate_seconds: f64,
    pub spill: crate::spill::SpillMetrics,
    pub external_sort_runs: usize,
    pub external_sort_merge_passes: usize,
    pub external_sort_rows: usize,
    /// Caller wall time in reader delivery/normalization. This can include
    /// expression preparation on caller assistance or synchronous fallback;
    /// scan_expression_seconds is summed work, not an additive wall stage.
    pub scan_decode_seconds: f64,
    pub filter_seconds: f64,
    pub expression_seconds: f64,
    pub column_projection_batches: usize,
    pub column_projection_rows: usize,
    pub aggregate_seconds: f64,
    pub shared_expression_evaluations: usize,
    pub shared_aggregate_states: usize,
    pub bounded_sum_batches: usize,
    pub dense_group_rebuilds: usize,
    pub dense_group_batches: usize,
    pub dense_group_index_peak_bytes: usize,
    pub short_group_cache_batches: usize,
    pub short_group_cache_hits: usize,
    pub short_group_cache_misses: usize,
    pub short_group_cache_bypasses: usize,
    pub short_group_cache_peak_bytes: usize,
    pub cte_materializations: usize,
    pub cte_reuses: usize,
    pub scan_reuses: usize,
    pub filtered_scan_materializations: usize,
    pub filtered_scan_reuses: usize,
    pub paired_existence_scans: usize,
    pub scan_materializations: usize,
    /// Complete selective join results retained and replayed within this query.
    pub subplan_materializations: usize,
    pub subplan_reuses: usize,
    pub subplan_rows: usize,
    pub subplan_fallbacks: usize,
    pub cached_scan_filters: usize,
    pub buffered_parquet_scans: usize,
    pub parquet_cached_column_rows: usize,
    pub parquet_uncached_column_rows: usize,
    pub buffered_compressed_bytes_read: usize,
    pub output_sort_seconds: f64,
    pub having_input_groups: usize,
    pub having_output_groups: usize,
    pub execution_seconds: f64,
    pub rows_read: usize,
    pub rows_after_filter: usize,
    pub batches: usize,
    pub decoded_arrow_bytes_total: usize,
    pub peak_reserved_bytes: usize,
    pub selected_column_compressed_bytes: i64,
    /// A parallel reservation refusal replayed with fresh state on the caller.
    pub decoder_serial_retries: usize,
    /// A parallel scan transform exhausted its budget before any result was published.
    pub parallel_transform_serial_retries: usize,
    pub workers: usize,
    pub operator_profiles: Vec<serde_json::Value>,
    pub join_seconds: f64,
    pub residual_candidate_rows: usize,
    pub residual_gathered_bytes: usize,
    pub join_build_rows: usize,
    /// Completed-build phases exclude upstream collection and downstream probes.
    pub build_prepare_concat_seconds: f64,
    pub build_prepare_key_seconds: f64,
    pub build_prepare_index_seconds: f64,
    pub build_prepare_column_tasks: usize,
    pub build_prepare_index_tasks: usize,
    pub build_group_dictionary_seconds: f64,
    pub build_group_parallel_dictionaries: usize,
    pub build_group_id_aggregates: usize,
    pub build_group_id_rows: usize,
    pub build_group_id_groups: usize,
    pub build_group_id_fallbacks: usize,
    pub sorted_distinct_aggregates: usize,
    pub sorted_distinct_pairs: usize,
    pub sorted_distinct_fallbacks: usize,
    pub fused_left_count_joins: usize,
    pub fused_left_count_rows: usize,
    pub fused_left_count_serial_retries: usize,
    pub reverse_existence_joins: usize,
    pub reverse_existence_lane_joins: usize,
    pub reverse_existence_numeric_lane_joins: usize,
    pub reverse_existence_lane_batches: usize,
    /// Summed membership work in decoder lanes; overlaps scan worker time.
    pub reverse_existence_probe_seconds: f64,
    pub reverse_existence_merge_seconds: f64,
    pub reverse_existence_emit_seconds: f64,
    pub dynamic_scan_filters: usize,
    pub sideways_prepared_builds: usize,
    pub sideways_scan_filters: usize,
    pub static_scan_filters: usize,
    pub post_decode_scan_filters: usize,
    pub post_decode_filter_batches: usize,
    pub post_decode_filter_input_rows: usize,
    pub post_decode_filter_output_rows: usize,
    pub demand_filtered_aggregates: usize,
    pub dense_joins: usize,
    pub join_rejection_bitmaps: usize,
    pub join_rejection_bitmap_bytes: usize,
    pub join_rejection_probes: usize,
    pub join_rejection_misses: usize,
    pub bitmap_scan_filters: usize,
    pub numeric_pair_scan_filters: usize,
    pub join_output_rows: usize,
}
pub struct Prepared {
    /// Optional direct plan for auto spill's in-memory attempt. The relational
    /// plan remains authoritative for nonadmission and the forced-spill retry.
    pub(crate) auto_simple: Option<Box<Prepared>>,
    /// Internal relational results retain precision-38 INTEGER values.
    pub(crate) internal_output: bool,
    pub plan: Plan,
    pub schema: SchemaRef,
    pub relational: Option<Box<crate::rel::Node>>,
}
pub struct Execution {
    pub batch: RecordBatch,
    pub metrics: Metrics,
    pub(crate) _reservations: Vec<Reservation>,
}
pub fn prepare(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
) -> Result<Prepared> {
    validate_prepare_options(options)?;
    let _statement = crate::zone::enter(options.time_zone, options.fixed_clock);
    if let Some(spill) = &options.spill {
        let mut prepared = crate::rel::prepare(sql, tables, options)?;
        if spill.mode == crate::spill::SpillMode::Auto
            && let Ok(mut simple) = prepare_simple(sql, tables, options)
            && simple.plan.types == prepared.plan.types
        {
            // The binders can use different display names for unaliased
            // expressions. Auto mode has always exposed the relational names.
            // Copy metadata only; all execution buffers still use one Budget.
            simple.plan.names.clone_from(&prepared.plan.names);
            prepared.auto_simple = Some(Box::new(simple));
        }
        return Ok(prepared);
    }
    prepare_simple(sql, tables, options).or_else(|_| crate::rel::prepare(sql, tables, options))
}
fn validate_prepare_options(options: &Options) -> Result<()> {
    options.cancellation.check()?;
    if !(1..=64).contains(&options.workers) {
        return Err("workers must be 1..64".into());
    }
    if let Some(spill) = &options.spill
        && (spill.sort_run_bytes == 0 || !(2..=32).contains(&spill.merge_fan_in))
    {
        return Err("invalid external sort settings".into());
    }
    Ok(())
}

pub(crate) fn prepare_with_views(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
    views: &HashMap<String, &crate::rel::ViewDefinition>,
) -> Result<Prepared> {
    validate_prepare_options(options)?;
    let _statement = crate::zone::enter(options.time_zone, options.fixed_clock);
    crate::rel::prepare_with_views(sql, tables, options, views)
}

/// Session-owned CSV snapshots stay alive through execution in the caller.
pub(crate) fn prepare_with_local_files(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
    views: &HashMap<String, &crate::rel::ViewDefinition>,
    reader: &crate::rel::LocalReader<'_>,
) -> Result<Prepared> {
    validate_prepare_options(options)?;
    let _statement = crate::zone::enter(options.time_zone, options.fixed_clock);
    if views.is_empty()
        && options.spill.is_none()
        && let Ok(prepared) = prepare_simple(sql, tables, options)
    {
        return Ok(prepared);
    }
    crate::rel::prepare_with_local_files(sql, tables, options, views, Some(reader))
}

fn prepare_simple(
    sql: &str,
    tables: &HashMap<String, PathBuf>,
    options: &Options,
) -> Result<Prepared> {
    let query = bind::parse(sql)?;
    let name = bind::table_name(&query)?;
    let path = name
        .as_ref()
        .map(|name| {
            tables
                .get(name)
                .cloned()
                .ok_or_else(|| format!("unknown table: {name}"))
        })
        .transpose()?;
    let schema = if let Some(path) = &path {
        if !path.is_file() {
            return Err("M1 table must be a single Parquet file".into());
        }
        let planning_budget = Budget::new(options.memory_bytes);
        let (metadata, _metadata_lease) = crate::parquet_scan::accounted_metadata(
            &File::open(path).map_err(|e| crate::Error::from(e.to_string()))?,
            &planning_budget,
        )?;
        metadata.schema().clone()
    } else {
        Arc::new(Schema::empty())
    };
    let schema = Arc::new(Type::input_schema(&schema));
    let plan = bind::bind(&query, &schema, path, options.projection_pushdown)?;
    crate::mixed_numeric::validate_plan(&plan, &schema)?;
    Ok(Prepared {
        auto_simple: None,
        internal_output: false,
        plan,
        schema,
        relational: None,
    })
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Key {
    Null,
    Num(i128),
    Text(String),
    Binary(Vec<u8>),
    Bool(bool),
    Date(i32),
    Float(u64),
    Timestamp(i64),
}
impl Key {
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            Self::Text(value) => value.len(),
            Self::Binary(value) => value.len(),
            _ => 0,
        }
    }
}
/// One temporary owned binary key at a time; persistent keys reserve separately.
pub(crate) fn binary_key_scratch(value: &Value, budget: &Budget) -> Result<Option<Reservation>> {
    value
        .array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(|array| {
            let bytes = array.iter().flatten().map(<[u8]>::len).max().unwrap_or(0);
            budget.reserve(bytes.saturating_add(128))
        })
        .transpose()
}
/// The canonical aggregate/grouping key of a REAL value: the engine's one
/// canonical Float form (signed zeros equal, every NaN one value).
pub(crate) fn float_key(n: f64) -> Key {
    Key::Float(crate::expr::numeric_compare::canonical(n).to_bits())
}
/// MIN/MAX (and ARG_MIN/ARG_MAX) order of two non-NULL keys of one type:
/// Float keys use the SQL NaN total order (NaN above +infinity).
pub(crate) fn compare_extreme_keys(a: &Key, b: &Key) -> std::cmp::Ordering {
    if let (Key::Float(a), Key::Float(b)) = (a, b) {
        crate::expr::numeric_compare::real_order(f64::from_bits(*a), f64::from_bits(*b))
    } else {
        a.cmp(b)
    }
}
pub(crate) fn key(v: &Value, ty: &Type, i: usize) -> Key {
    if is_sql_null(v.array.as_ref(), i) {
        return Key::Null;
    }
    match ty {
        Type::Int | Type::Decimal(_) => Key::Num(numeric(&v.array).value(i)),
        Type::Text => Key::Text(
            v.array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .into(),
        ),
        Type::Binary => Key::Binary(
            v.array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(i)
                .to_vec(),
        ),
        Type::Bool => Key::Bool(boolean(&v.array).value(i)),
        Type::Date => Key::Date(
            v.array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(i),
        ),
        Type::Timestamp | Type::TimestampTz => {
            Key::Timestamp(crate::expr::timestamp::value(v.array.as_ref(), i))
        }
        Type::Float => {
            let n = v
                .array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i);
            float_key(n)
        }
        Type::Null => Key::Null,
    }
}
// Allocate only the columns required by this aggregate. A SUM/AVG group
// carries 40 bytes (wide sum and count); COUNT carries eight. Shared aliases stay
// empty, and DISTINCT sets are allocated only for groups that encounter values.
struct Executor<'a, A = OrdinaryAggregation> {
    prepared: &'a Prepared,
    expressions: Arc<crate::expr_program::ExpressionProgram>,
    budget: Budget,
    metrics: Metrics,
    aggregation: A,
    projection_leases: Vec<Reservation>,
    projected: Vec<RecordBatch>,
    output_schema: SchemaRef,
}
fn filter_input(
    input: &Input,
    mask: &BooleanArray,
    budget: &Budget,
    optimize: bool,
) -> Result<Input> {
    // Arrow combines nullable mask bits once; NULL remains false. The retained
    // input Values keep source buffers accounted while columns are filtered.
    let bitmap = (mask.null_count() > 0)
        .then(|| budget.reserve(mask.len().div_ceil(8).saturating_add(128)))
        .transpose()?;
    // Index vectors use one usize per selected row; run vectors can grow and
    // reallocate. This upper allowance covers both pinned Arrow strategies,
    // including small Vec capacities and old/new run storage during growth.
    let scratch = (optimize && input.columns.iter().flatten().count() > 1)
        .then(|| {
            budget
                .reserve(mask.len().saturating_mul(32).saturating_add(128))
                .ok()
        })
        .flatten();
    let builder = FilterBuilder::new(mask);
    let predicate = if scratch.is_some() {
        builder.optimize()
    } else {
        builder
    }
    .build();
    let filtered: Result<Input> = (|| {
        let mut columns = Vec::with_capacity(input.columns.len());
        for v in &input.columns {
            columns.push(if let Some(v) = v {
                let _allocation =
                    budget.reserve(v.array.get_array_memory_size().saturating_mul(2))?;
                Some(Value::adopt(
                    predicate
                        .filter(&v.array)
                        .map_err(|e| crate::Error::from(e.to_string()))?,
                    budget,
                )?)
            } else {
                None
            });
        }
        Ok(Input {
            columns,
            len: predicate.count(),
        })
    })();
    if scratch.is_some()
        && matches!(&filtered, Err(error) if error.starts_with("resource-limit: accounted memory"))
    {
        // Optional selection acceleration must not turn an otherwise fitting
        // batch into an error. Failed partial columns have already dropped.
        drop(predicate);
        drop(scratch);
        drop(bitmap);
        return filter_input(input, mask, budget, false);
    }
    filtered
}
impl Executor<'_> {
    fn batch(&mut self, input: Input) -> Result<()> {
        self.batch_with_filter(input, false)
    }
    fn batch_with_filter(&mut self, mut input: Input, filter_applied: bool) -> Result<()> {
        let p = &self.prepared.plan;
        if !filter_applied {
            self.metrics.rows_read += input.len;
        }
        self.metrics.batches += 1;
        let start = Instant::now();
        if !filter_applied && let Some(predicate) = &p.filter {
            let mask = predicate.eval_filter(&input, &self.budget)?;
            input = filter_input(&input, boolean(&mask.array), &self.budget, true)?;
        }
        self.metrics.filter_seconds += start.elapsed().as_secs_f64();
        self.metrics.rows_after_filter += input.len;
        let start = Instant::now();
        if !p.aggregation {
            let values = p
                .output
                .iter()
                .map(|o| match o {
                    Output::Expr(e) => e.eval(&input, &self.budget),
                    _ => unreachable!(),
                })
                .collect::<Result<Vec<_>>>()?;
            let arrays = values
                .iter()
                .zip(&p.types)
                .map(|(v, t)| {
                    materialize(
                        &v.array,
                        &physical_output_type(t, self.prepared.internal_output),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(self.output_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?;
            self.projection_leases
                .push(self.budget.reserve(batch.get_array_memory_size() + 256)?);
            self.projected.push(batch);
            self.metrics.expression_seconds += start.elapsed().as_secs_f64();
            return Ok(());
        }
        let (keys, values) = self.expressions.evaluate(&mut input, &self.budget)?;
        self.metrics.shared_expression_evaluations += self.expressions.bindings.len();
        self.metrics.expression_seconds += start.elapsed().as_secs_f64();
        self.aggregate_values(input.len, keys, values, None)
    }
    fn aggregate_values(
        &mut self,
        rows: usize,
        keys: Vec<Value>,
        values: Vec<Option<Value>>,
        prepared_keys: Option<crate::keys::Keys>,
    ) -> Result<()> {
        self.aggregation.update(
            &self.prepared.plan,
            &self.expressions,
            &mut self.metrics,
            aggregation::EvaluatedBatch {
                rows,
                keys,
                values,
                prepared_keys,
            },
        )
    }
}
impl<'a, A> Executor<'a, A> {
    /// Change only the post-input output owner; ordinary updates remain concrete.
    fn with_aggregation<B>(self, aggregation: B) -> Executor<'a, B> {
        let Self {
            prepared,
            expressions,
            budget,
            metrics,
            aggregation: previous,
            projection_leases,
            projected,
            output_schema,
        } = self;
        // Match the old assignment: retire ordinary state before finalization.
        drop(previous);
        Executor {
            prepared,
            expressions,
            budget,
            metrics,
            aggregation,
            projection_leases,
            projected,
            output_schema,
        }
    }
}
impl<A: AggregateOutput> Executor<'_, A> {
    /// A breaker completes before downstream consumers. Check its completed
    /// rows in encounter order, then columns, without retaining a second result
    /// or applying any aggregate contribution twice. Only replay calls this.
    fn canonical_finalize(&mut self, options: &Options) -> Result<()> {
        if !self.prepared.plan.aggregation {
            return Ok(());
        }
        self.aggregation.finish_updates()?;
        for row in 0..self.aggregation.group_count() {
            options.cancellation.check()?;
            let range = row..row + 1;
            let _lease = self
                .budget
                .reserve(self.aggregate_output_bytes(range.clone())?)?;
            let _batch = self.aggregate_batch(range)?;
        }
        Ok(())
    }
    fn aggregate_batch(&self, range: std::ops::Range<usize>) -> Result<RecordBatch> {
        self.aggregation.output_batch(
            &self.prepared.plan,
            &self.expressions,
            &self.output_schema,
            range,
        )
    }
    fn aggregate_output_bytes(&self, range: std::ops::Range<usize>) -> Result<usize> {
        self.aggregation
            .output_bytes(&self.prepared.plan, &self.expressions, range)
    }
    fn finish_batches(
        mut self,
        start: Instant,
        options: &Options,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<Metrics> {
        if options.error_replay.canonical() {
            self.canonical_finalize(options)?;
        }
        self.aggregation.finish_updates()?;
        debug_assert!(self.prepared.plan.aggregation && self.prepared.plan.order.is_empty());
        // Retain all group state until input has completed. Only the output is
        // chunked, preserving encounter order; SUM range checks occur per output.
        // A downstream filter/join may consume and release each chunk immediately.
        for offset in (0..self.aggregation.group_count().max(1)).step_by(options.batch_size) {
            options.cancellation.check()?;
            let range = offset..(offset + options.batch_size).min(self.aggregation.group_count());
            let output_start = Instant::now();
            let mut lease = self
                .budget
                .reserve(self.aggregate_output_bytes(range.clone())?)?;
            let batch = self.aggregate_batch(range)?;
            lease.release_excess(batch.get_array_memory_size().saturating_add(1024));
            self.metrics.output_sort_seconds += output_start.elapsed().as_secs_f64();
            emit(batch)?;
        }
        options.cancellation.check()?;
        self.metrics.execution_seconds = start.elapsed().as_secs_f64();
        self.metrics.peak_reserved_bytes = self.budget.peak();
        self.metrics.workers = options.workers;
        self.metrics.worker_threads_peak = self.budget.peak_threads();
        Ok(self.metrics)
    }
    /// Selection changes only materialization; chunk and output check order stay fixed.
    fn finish_having_batches(
        mut self,
        start: Instant,
        options: &Options,
        having: Option<&Having>,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<Metrics> {
        let Some(having) = having else {
            return self.finish_batches(start, options, emit);
        };
        if options.error_replay.canonical() {
            self.canonical_finalize(options)?;
        }
        self.aggregation.finish_updates()?;
        debug_assert!(self.prepared.plan.aggregation && self.prepared.plan.order.is_empty());
        // Retain all group state until input has completed. Only the output is
        // chunked, preserving encounter order; SUM range checks occur per output.
        // A downstream filter/join may consume and release each chunk immediately.
        for offset in (0..self.aggregation.group_count().max(1)).step_by(options.batch_size) {
            options.cancellation.check()?;
            let range = offset..(offset + options.batch_size).min(self.aggregation.group_count());
            let output_start = Instant::now();
            let mut lease = self
                .budget
                .reserve(self.aggregate_output_bytes(range.clone())?)?;
            let batch = self.aggregation.having_batch(
                &self.prepared.plan,
                &self.expressions,
                &self.output_schema,
                range.clone(),
                having,
            )?;
            self.metrics.having_input_groups += range.len();
            self.metrics.having_output_groups += batch.num_rows();
            lease.release_excess(batch.get_array_memory_size().saturating_add(1024));
            self.metrics.output_sort_seconds += output_start.elapsed().as_secs_f64();
            // The replaced Filter does not emit empty chunks to its consumer.
            if batch.num_rows() > 0 {
                emit(batch)?;
            }
        }
        options.cancellation.check()?;
        self.metrics.execution_seconds = start.elapsed().as_secs_f64();
        self.metrics.peak_reserved_bytes = self.budget.peak();
        self.metrics.workers = options.workers;
        self.metrics.worker_threads_peak = self.budget.peak_threads();
        Ok(self.metrics)
    }
    fn finish(
        mut self,
        start: Instant,
        top_k: Option<usize>,
        options: &Options,
    ) -> Result<Execution> {
        self.aggregation.finish_updates()?;
        let output_start = Instant::now();
        let p = &self.prepared.plan;
        let size = if p.aggregation {
            self.aggregation.group_count()
        } else {
            self.projected.iter().map(RecordBatch::num_rows).sum()
        };
        if size > u32::MAX as usize {
            return Err("resource-limit: output exceeds 32-bit sort indices".into());
        }
        // Include old/new result arrays and sort indices during materialization.
        let projected_bytes: usize = self
            .projected
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum();
        let output_bytes = if p.aggregation {
            self.aggregate_output_bytes(0..size)?
        } else {
            size.checked_mul((p.output.len() + 1) * 128)
                .and_then(|n| n.checked_add(1024))
                .ok_or("output size overflow")?
                .max(projected_bytes.saturating_mul(2))
        };
        let mut output_lease = self.budget.reserve(output_bytes)?;
        let mut batch = if p.aggregation {
            self.aggregate_batch(0..size)?
        } else {
            concat_batches(&self.output_schema, &self.projected)
                .map_err(|e| crate::Error::from(e.to_string()))?
        };
        if !p.order.is_empty() && batch.num_rows() > 0 {
            let columns = p
                .order
                .iter()
                .map(|s| SortColumn {
                    values: batch.column(s.column).clone(),
                    options: Some(SortOptions {
                        descending: s.descending,
                        nulls_first: s.nulls_first,
                    }),
                })
                .collect::<Vec<_>>();
            let selected = top_k
                .map(|k| {
                    crate::external_sort::try_top_k_indices(
                        &columns,
                        batch.num_rows(),
                        k,
                        &self.budget,
                        &options.cancellation,
                    )
                })
                .transpose()?
                .flatten();
            self.metrics.top_k_sorts += usize::from(selected.is_some());
            let (indices, _top_k_credit) = if let Some((indices, credit)) = selected {
                (indices, Some(credit))
            } else {
                (
                    crate::external_sort::sort_indices(columns, batch.num_rows(), &self.budget)?,
                    None,
                )
            };
            let arrays = batch
                .columns()
                .iter()
                .map(|a| take(a, &indices, None).map_err(|e| crate::Error::from(e.to_string())))
                .collect::<Result<Vec<_>>>()?;
            batch = RecordBatch::try_new(self.output_schema.clone(), arrays)
                .map_err(|e| crate::Error::from(e.to_string()))?;
        }
        // Builders, temporary option vectors and sort indices have been freed.
        // Keep only the returned arrays charged during downstream consumption.
        output_lease.release_excess(batch.get_array_memory_size().saturating_add(1024));
        self.metrics.output_sort_seconds = output_start.elapsed().as_secs_f64();
        self.metrics.execution_seconds = start.elapsed().as_secs_f64();
        self.metrics.peak_reserved_bytes = self.budget.peak();
        Ok(Execution {
            batch,
            metrics: self.metrics,
            _reservations: vec![output_lease],
        })
    }
}
pub(crate) fn count_array(counts: impl Iterator<Item = u64>, ty: &Type) -> Result<ArrayRef> {
    if *ty == Type::Int {
        Ok(Arc::new(Int64Array::from(
            counts
                .map(|n| i64::try_from(n).map_err(|_| crate::Error::from("BIGINT overflow")))
                .collect::<Result<Vec<_>>>()?,
        )))
    } else {
        Ok(Arc::new(
            Decimal128Array::from(counts.map(i128::from).collect::<Vec<_>>())
                .with_precision_and_scale(38, 0)
                .map_err(|e| crate::Error::from(e.to_string()))?,
        ))
    }
}

pub(crate) fn physical_output_type(ty: &Type, internal: bool) -> Type {
    if internal && *ty == Type::Int {
        Type::Decimal(0)
    } else {
        ty.clone()
    }
}

pub(crate) fn output_array(array: ArrayRef, field: &Field) -> Result<ArrayRef> {
    if Type::is_wide_integer(field) && array.data_type() != field.data_type() {
        Ok(arrow::compute::cast(&array, field.data_type())
            .map_err(|e| crate::Error::from(e.to_string()))?)
    } else {
        Ok(array)
    }
}

pub(crate) fn materialize(array: &ArrayRef, ty: &Type) -> Result<ArrayRef> {
    // Arrow's permissive casts can turn overflow into NULL. SQL overflow must
    // fail explicitly, so narrowing a wide integer aggregate is checked here.
    match ty {
        Type::Int => {
            let numbers = numeric(array);
            let narrow = |v| i64::try_from(v).map_err(|_| crate::Error::from("BIGINT overflow"));
            // Fallible iterator collection grows geometrically and Arrow keeps
            // that spare Vec capacity. The output length is known, so reserve
            // exactly that length before the checked loop instead of retaining
            // up to nearly twice the required values buffer.
            let mut values = Vec::with_capacity(numbers.len());
            if numbers.null_count() == 0 {
                for value in numbers.values().iter().copied() {
                    values.push(narrow(value)?);
                }
            } else {
                for (row, value) in numbers.values().iter().enumerate() {
                    // Physical values beneath NULL are unconstrained and must
                    // neither overflow nor become visible in the output.
                    values.push(if numbers.is_valid(row) {
                        narrow(*value)?
                    } else {
                        0
                    });
                }
            }
            Ok(Arc::new(Int64Array::new(
                values.into(),
                numbers
                    .nulls()
                    .filter(|nulls| nulls.null_count() > 0)
                    .cloned(),
            )))
        }
        Type::Decimal(scale) => Ok(Arc::new(
            numeric(array)
                .clone()
                .with_precision_and_scale(38, *scale)
                .map_err(|e| crate::Error::from(e.to_string()))?,
        )),
        _ => Ok(array.clone()),
    }
}
pub(crate) fn keys_array(keys: &[Key], ty: &Type) -> Result<ArrayRef> {
    Ok(match ty {
        Type::Binary => {
            keys.iter().try_fold(0usize, |bytes, key| {
                bytes
                    .checked_add(key.payload_bytes())
                    .filter(|n| *n <= i32::MAX as usize)
                    .ok_or("resource-limit: binary array exceeds 32-bit offsets")
            })?;
            Arc::new(BinaryArray::from(
                keys.iter()
                    .map(|key| match key {
                        Key::Binary(bytes) => Some(bytes.as_slice()),
                        Key::Null => None,
                        _ => unreachable!("bound binary key"),
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        Type::Text => Arc::new(StringArray::from(
            keys.iter()
                .map(|g| match g {
                    Key::Text(s) => Some(s.as_str()),
                    Key::Null => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        Type::Bool => Arc::new(BooleanArray::from(
            keys.iter()
                .map(|g| match g {
                    Key::Bool(v) => Some(*v),
                    Key::Null => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        Type::Date => Arc::new(Date32Array::from(
            keys.iter()
                .map(|g| match g {
                    Key::Date(v) => Some(*v),
                    Key::Null => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        Type::Timestamp | Type::TimestampTz => crate::expr::timestamp::labelled(
            TimestampMicrosecondArray::from(
                keys.iter()
                    .map(|g| match g {
                        Key::Timestamp(v) => Some(*v),
                        Key::Null => None,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
            ),
            ty,
        ),
        Type::Int | Type::Decimal(_) => {
            let values = keys
                .iter()
                .map(|g| match g {
                    Key::Num(v) => Some(*v),
                    Key::Null => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            let a: ArrayRef = Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, ty.scale()?)
                    .map_err(|e| crate::Error::from(e.to_string()))?,
            );
            materialize(&a, ty)?
        }
        Type::Float => Arc::new(Float64Array::from(
            keys.iter()
                .map(|k| match k {
                    Key::Float(v) => Some(f64::from_bits(*v)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        Type::Null => new_null_array(&arrow::datatypes::DataType::Null, keys.len()),
    })
}
pub fn execute(prepared: &Prepared, options: &Options) -> Result<Execution> {
    let _statement = crate::zone::enter(options.time_zone, options.fixed_clock);
    if options.spill.is_some() && prepared.relational.is_none() {
        return Err("spill execution requires a plan prepared with spill options".into());
    }
    let original = execution_policy(options, |options, budget| {
        execute_in_budget(prepared, options, budget)
    });
    error_replay::select(original, options, |canonical, budget| {
        execute_in_budget(prepared, canonical, budget)
    })
}
pub(crate) fn execution_policy(
    options: &Options,
    run: impl Fn(&Options, Budget) -> Result<Execution>,
) -> Result<Execution> {
    options.cancellation.check()?;
    if !(1..=64).contains(&options.workers) {
        return Err("workers must be 1..64".into());
    }
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    if !options
        .spill
        .as_ref()
        .is_some_and(|s| s.mode == crate::spill::SpillMode::Auto)
    {
        return run(options, Budget::new(options.memory_bytes));
    }
    let started = Instant::now();
    let memory = Budget::new(options.memory_bytes);
    let mut in_memory = options.clone();
    in_memory.spill = None;
    match run(&in_memory, memory.clone()) {
        Ok(result) => Ok(result),
        Err(error) if error.starts_with("resource-limit: accounted memory") => {
            let initial_seconds = started.elapsed().as_secs_f64();
            options.cancellation.check()?;
            if memory.used() != 0 {
                return Err(format!(
                    "{error}; automatic spill: failed attempt retained reservations"
                )
                .into());
            }
            let mut forced = options.clone();
            forced.spill.as_mut().unwrap().mode = crate::spill::SpillMode::Force;
            let mut result = run(&forced, Budget::new(options.memory_bytes))
                .map_err(|retry| retry.context(format!("; automatic spill retry after {error}")))?;
            result.metrics.auto_spill_retries = 1;
            result.metrics.auto_spill_initial_seconds = initial_seconds;
            result.metrics.auto_spill_initial_peak_reserved_bytes = memory.peak();
            result.metrics.auto_spill_initial_error = Some(error);
            result.metrics.peak_reserved_bytes =
                result.metrics.peak_reserved_bytes.max(memory.peak());
            result.metrics.worker_threads_peak = result
                .metrics
                .worker_threads_peak
                .max(memory.peak_threads());
            result.metrics.execution_seconds = started.elapsed().as_secs_f64();
            Ok(result)
        }
        Err(error) => Err(error),
    }
}
fn execute_in_budget(prepared: &Prepared, options: &Options, budget: Budget) -> Result<Execution> {
    if options.error_replay.canonical() {
        if let Some(root) = &prepared.relational {
            return crate::rel::execute_in_budget(root, options, budget, prepared.internal_output);
        }
        error_replay::admit_aggregate(&prepared.plan)?;
        // The same immutable Prepared owns the direct plan on both attempts.
        let source = prepared
            .plan
            .path
            .as_ref()
            .map(|path| {
                pipeline::fallback::Source::capture_checked(path, &|| options.cancellation.check())
            })
            .transpose()?;
        let result = execute_baseline(prepared, options, budget);
        if let Some(source) = source {
            source.verify_checked(prepared.plan.path.as_ref().unwrap(), &|| {
                options.cancellation.check()
            })?;
        }
        return result;
    }
    // execution_policy removes auto spill only for the in-memory attempt.
    // Force retains its spill options, so it must reach the relational plan.
    if options.spill.is_none()
        && let Some(simple) = &prepared.auto_simple
        && pipeline::eligible(simple, options)
        && budget.active_workers() == 0
    {
        let started = Instant::now();
        let entry_used = budget.used();
        match pipeline::execute(simple, options, budget.clone()) {
            Err(error)
                if matches!(
                    pipeline::fallback::Failure::sql(error.clone()),
                    pipeline::fallback::Failure::Sql(_)
                ) =>
            {
                // The direct plan narrows each partition's public columns
                // before later aggregates finish. The relational plan keeps
                // INTEGERs wide until the public query boundary. Replay SQL
                // failures there to preserve its error precedence (and LIMIT
                // behavior). Successful direct results need no extra work.
                // Resource and terminal failures retain their existing policy.
                if budget.used() != entry_used || budget.active_workers() != 0 {
                    return Err("auto direct SQL replay: attempt retained ownership".into());
                }
                let root = prepared
                    .relational
                    .as_ref()
                    .ok_or("auto direct plan lacks relational plan")?;
                let mut result =
                    crate::rel::execute_in_budget(root, options, budget, prepared.internal_output)?;
                result.metrics.execution_seconds = started.elapsed().as_secs_f64();
                return Ok(result);
            }
            result => return result,
        }
    }
    if let Some(root) = &prepared.relational {
        return crate::rel::execute_in_budget(root, options, budget, prepared.internal_output);
    }
    if pipeline::eligible(prepared, options) && budget.active_workers() == 0 {
        return pipeline::execute(prepared, options, budget);
    }
    execute_baseline(prepared, options, budget)
}
/// Explicit entry for nonadmission/replay; never redispatches to the pipeline.
fn execute_baseline(prepared: &Prepared, options: &Options, budget: Budget) -> Result<Execution> {
    let start = Instant::now();
    let p = &prepared.plan;
    let mut executor = executor(
        prepared,
        budget.clone(),
        options.relational_optimizations && !options.error_replay.canonical(),
    )?;
    #[cfg(test)]
    executor.aggregation.force_partitioned(
        &prepared.plan,
        &executor.expressions,
        budget.clone(),
        options,
    )?;
    if let Some(path) = &p.path {
        let scan_start = Instant::now();
        let file = File::open(path).map_err(|e| crate::Error::from(e.to_string()))?;
        let (metadata, _metadata_lease) = crate::parquet_scan::accounted_metadata(&file, &budget)?;
        // First support independent states in ordinary direct scans. Admit
        // using runtime row count, keeping small inputs and spill execution on
        // their existing serial path. No table/query identity is consulted.
        if p.aggregation
            && options.relational_optimizations
            && options.spill.is_none()
            && options.workers >= 4
            && metadata.metadata().file_metadata().num_rows() as usize
                >= options.batch_size.saturating_mul(2).max(1024)
        {
            executor
                .aggregation
                .start_workers(p, &executor.expressions, options)?;
        }
        for i in &p.columns {
            Type::from_field(prepared.schema.field(*i))?;
        }
        executor.metrics.selected_column_compressed_bytes = metadata
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|g| p.columns.iter().map(|i| g.column(*i).compressed_size()))
            .sum();
        let projection = ProjectionMask::roots(metadata.parquet_schema(), p.columns.clone());
        let transform_stats = Arc::new(std::sync::Mutex::new(
            crate::parquet_scan::FilterStats::default(),
        ));
        let transform = if options.relational_optimizations {
            p.filter
                .clone()
                .map(|mut expression| {
                    let mut mapping = vec![None; prepared.schema.fields().len()];
                    for (local, original) in p.columns.iter().enumerate() {
                        mapping[*original] = Some(local);
                    }
                    expression.remap_columns(&mapping)?;
                    let schema = metadata
                        .schema()
                        .project(&p.columns)
                        .map_err(|e| crate::Error::from(e.to_string()))?;
                    crate::parquet_scan::exact_filter(
                        expression,
                        &schema,
                        &budget,
                        &options.cancellation,
                        true,
                        transform_stats.clone(),
                    )
                })
                .transpose()?
        } else {
            None
        };
        let transformed = transform.is_some();
        executor.metrics.post_decode_scan_filters += usize::from(transformed);
        let projection_stats =
            Arc::new(std::sync::Mutex::new(scan_projection::Statistics::default()));
        let (transform, aggregate_projection) =
            if p.aggregation && options.relational_optimizations && options.workers > 1 {
                let (transform, layout) = scan_projection::transform(
                    prepared,
                    executor.expressions.clone(),
                    transform,
                    options,
                    &budget,
                    projection_stats.clone(),
                )?;
                (Some(transform), Some(layout))
            } else {
                (transform, None)
            };
        let mut reader = crate::parquet_scan::Reader::with_transform(
            path,
            file,
            metadata,
            projection,
            Arc::new(|_| None),
            transform,
            options,
            &budget,
        )?;
        executor.metrics.scan_decode_seconds += scan_start.elapsed().as_secs_f64();
        loop {
            options.cancellation.check()?;
            let scan_start = Instant::now();
            let next = reader.next();
            options.cancellation.check()?;
            let Some(batch) = next else {
                executor.metrics.scan_decode_seconds += scan_start.elapsed().as_secs_f64();
                break;
            };
            let batch = batch.map_err(|e| crate::Error::from(e.to_string()))?;
            if let Some(projection) = &aggregate_projection {
                let rows = batch.num_rows();
                let (keys, values, encoded) = projection.unpack(&batch, &budget)?;
                drop(batch);
                executor.metrics.scan_decode_seconds += scan_start.elapsed().as_secs_f64();
                executor.metrics.batches += 1;
                executor.metrics.rows_after_filter += rows;
                executor.metrics.shared_expression_evaluations +=
                    executor.expressions.bindings.len();
                executor.aggregate_values(rows, keys, values, encoded)?;
                continue;
            }
            let bytes = batch.get_array_memory_size();
            if !transformed {
                executor.metrics.decoded_arrow_bytes_total += bytes;
            }
            let decoded = reader.take_batch_lease();
            let mut input = Input {
                columns: vec![None; prepared.schema.fields().len()],
                len: batch.num_rows(),
            };
            for (local, original) in p.columns.iter().enumerate() {
                input.columns[*original] = Some(normalize_input(
                    batch.column(local),
                    &Type::from_field(prepared.schema.field(*original))?,
                    &budget,
                )?);
            }
            drop(batch);
            drop(decoded);
            executor.metrics.scan_decode_seconds += scan_start.elapsed().as_secs_f64();
            executor.batch_with_filter(input, transformed)?;
        }
        if aggregate_projection.is_some() {
            let stats = projection_stats.lock().unwrap();
            executor.metrics.scan_expression_batches += stats.batches;
            executor.metrics.scan_group_key_batches += stats.group_key_batches;
            executor.metrics.scan_expression_seconds += stats.expression_seconds;
            executor.metrics.scan_normalize_seconds += stats.normalize_seconds;
            executor.metrics.expression_seconds += stats.expression_seconds;
            if !transformed {
                executor.metrics.rows_read += stats.rows;
                executor.metrics.decoded_arrow_bytes_total += stats.raw_bytes;
            }
        }
        if transformed {
            let stats = transform_stats.lock().unwrap();
            executor.metrics.rows_read += stats.rows;
            executor.metrics.decoded_arrow_bytes_total += stats.bytes;
            executor.metrics.filter_seconds += stats.seconds;
            executor.metrics.post_decode_filter_batches += stats.batches;
            executor.metrics.post_decode_filter_input_rows += stats.rows;
            executor.metrics.post_decode_filter_output_rows += stats.output_rows;
            if reader.parallel_statistics().is_none() {
                executor.metrics.scan_decode_seconds -= stats.seconds;
            }
        }
        if let Some(stats) = reader.parallel_statistics() {
            executor.metrics.parallel_scan_tasks += stats.tasks;
            executor.metrics.parallel_scan_batches += stats.batches;
            executor.metrics.parallel_scan_pending_batches_peak = stats.pending_peak;
            executor.metrics.parallel_scan_pending_bytes_peak = stats.pending_bytes_peak;
            executor.metrics.parallel_scan_queue_slots_max = stats.queue_slots;
            executor.metrics.parallel_scan_worker_seconds += stats.worker_seconds;
            executor.metrics.parallel_scan_caller_seconds += stats.caller_seconds;
            executor.metrics.parallel_scan_caller_tasks += stats.caller_tasks;
            executor.metrics.parallel_scan_backpressure_seconds += stats.backpressure_seconds;
        }
    } else {
        executor.batch(Input {
            columns: Vec::new(),
            len: 1,
        })?;
    }
    options.cancellation.check()?;
    if options.error_replay.canonical() {
        executor.canonical_finalize(options)?;
    }
    let mut result = executor.finish(start, None, options)?;
    result.metrics.workers = options.workers;
    result.metrics.worker_threads_peak = budget.peak_threads();
    options.cancellation.check()?;
    Ok(result)
}

fn executor(prepared: &Prepared, budget: Budget, optimizations: bool) -> Result<Executor<'_>> {
    let p = &prepared.plan;
    let output_schema = Arc::new(Schema::new(
        p.names
            .iter()
            .zip(&p.types)
            .map(|(n, t)| t.field(n, prepared.internal_output))
            .collect::<Vec<_>>(),
    ));
    let expressions = Arc::new(crate::expr_program::ExpressionProgram::new(
        p,
        prepared.schema.fields().len(),
        optimizations,
    ));
    let shared_aggregate_states = expressions
        .state_sources
        .iter()
        .enumerate()
        .filter(|(i, source)| *i != **source)
        .count();
    let aggregation = OrdinaryAggregation::new(p, &expressions, budget.clone(), optimizations)?;
    let executor = Executor {
        prepared,
        expressions,
        budget: budget.clone(),
        metrics: Metrics {
            workers: 1,
            shared_aggregate_states,
            ..Metrics::default()
        },
        aggregation,
        projection_leases: Vec::new(),
        projected: Vec::new(),
        output_schema,
    };
    Ok(executor)
}
pub(crate) fn execute_stream(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
) -> Result<Execution> {
    execute_stream_top_k(prepared, options, budget, None, produce)
}

/// LIMIT changes only the completed sort's materialization, never production,
/// expression evaluation, accumulation or finalization.
pub(crate) fn execute_stream_top_k(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    top_k: Option<usize>,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
) -> Result<Execution> {
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    let start = Instant::now();
    let mut executor = executor(
        prepared,
        budget.clone(),
        options.relational_optimizations && !options.error_replay.canonical(),
    )?;
    #[cfg(test)]
    executor.aggregation.force_partitioned(
        &prepared.plan,
        &executor.expressions,
        budget.clone(),
        options,
    )?;
    fill_from_stream(&mut executor, prepared, options, &budget, produce)?;
    options.cancellation.check()?;
    if options.error_replay.canonical() {
        executor.canonical_finalize(options)?;
    }
    let mut result = executor.finish(start, top_k, options)?;
    result.metrics.workers = options.workers;
    result.metrics.worker_threads_peak = budget.peak_threads();
    options.cancellation.check()?;
    Ok(result)
}

fn fill_from_stream(
    executor: &mut Executor<'_>,
    prepared: &Prepared,
    options: &Options,
    budget: &Budget,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
) -> Result<()> {
    produce(&mut |batch| {
        options.cancellation.check()?;
        let mut input = Input {
            len: batch.num_rows(),
            columns: vec![None; batch.num_columns()],
        };
        if options.projection_pushdown {
            for i in &prepared.plan.columns {
                input.columns[*i] = Some(normalize_input(
                    batch.column(*i),
                    &Type::from_field(prepared.schema.field(*i))?,
                    budget,
                )?);
            }
        } else {
            for (i, a) in batch.columns().iter().enumerate() {
                input.columns[i] = Some(normalize_input(
                    a,
                    &Type::from_field(prepared.schema.field(i))?,
                    budget,
                )?);
            }
        }
        executor.batch(input)
    })?;
    Ok(())
}
pub(crate) fn execute_aggregate_stream(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    let start = Instant::now();
    let mut executor = executor(
        prepared,
        budget.clone(),
        options.relational_optimizations && !options.error_replay.canonical(),
    )?;
    #[cfg(test)]
    executor.aggregation.force_partitioned(
        &prepared.plan,
        &executor.expressions,
        budget.clone(),
        options,
    )?;
    fill_from_stream(&mut executor, prepared, options, &budget, produce)?;
    executor.finish_batches(start, options, emit)
}

/// Only the build-ID owner calls this boundary: it can discard the entire
/// attempt and replay the original child before any output on a cap refusal.
pub(crate) fn execute_bounded_distinct_stream(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    row_limit: usize,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    let start = Instant::now();
    let mut executor = executor(prepared, budget.clone(), options.relational_optimizations)?;
    #[cfg(test)]
    executor.aggregation.force_partitioned(
        &prepared.plan,
        &executor.expressions,
        budget.clone(),
        options,
    )?;
    executor.metrics.sorted_distinct_aggregates = usize::from(
        executor
            .aggregation
            .enable_sorted_distinct(&prepared.plan, row_limit, options),
    );
    fill_from_stream(&mut executor, prepared, options, &budget, produce)?;
    executor.finish_batches(start, options, emit)
}

/// Keep ordinary streams on their original path; admitted HAVING only changes output.
pub(crate) fn execute_having_stream(
    prepared: &Prepared,
    options: &Options,
    budget: Budget,
    having: Option<&Having>,
    produce: impl FnOnce(&mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    if having.is_none() {
        return execute_aggregate_stream(prepared, options, budget, produce, emit);
    }
    if options.batch_size == 0 || options.batch_size > 131072 {
        return Err("batch size must be 1..131072".into());
    }
    let start = Instant::now();
    let mut executor = executor(prepared, budget.clone(), options.relational_optimizations)?;
    #[cfg(test)]
    executor.aggregation.force_partitioned(
        &prepared.plan,
        &executor.expressions,
        budget.clone(),
        options,
    )?;
    fill_from_stream(&mut executor, prepared, options, &budget, produce)?;
    executor.finish_having_batches(start, options, having, emit)
}

/// Feed total left-side group expressions and exact COUNT contributions through
/// the ordinary encounter-ordered state/output owner. This entry never starts
/// aggregate helpers, so upstream scans retain the entire worker allowance.
pub(crate) fn execute_counted_stream(
    prepared: &Prepared,
    groups: &[crate::expr::Expr],
    options: &Options,
    budget: Budget,
    produce: impl FnOnce(&mut dyn FnMut(Input, Vec<Value>) -> Result<()>) -> Result<()>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Metrics> {
    let start = Instant::now();
    let executor = executor(prepared, budget.clone(), true)?;
    #[cfg(not(test))]
    let mut executor = executor;
    // Even test-only partitioned backends must retain the single ordinary owner:
    // weighted inputs are not ordinary COUNT partials until they are consumed.
    #[cfg(test)]
    let mut executor = {
        let aggregation = aggregation::Aggregation::new(
            &prepared.plan,
            &executor.expressions,
            budget.clone(),
            true,
        )?;
        executor.with_aggregation(aggregation)
    };
    produce(&mut |input, weights| {
        options.cancellation.check()?;
        let expression_start = Instant::now();
        let keys = groups
            .iter()
            .map(|e| e.eval(&input, &budget))
            .collect::<Result<Vec<_>>>()?;
        executor.metrics.expression_seconds += expression_start.elapsed().as_secs_f64();
        executor.metrics.rows_after_filter += input.len;
        executor.metrics.batches += 1;
        executor.aggregation.update_counted(
            &prepared.plan,
            &executor.expressions,
            &mut executor.metrics,
            aggregation::EvaluatedBatch {
                rows: input.len,
                keys,
                values: weights.into_iter().map(Some).collect(),
                prepared_keys: None,
            },
        )
    })?;
    executor.finish_batches(start, options, emit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;
    #[test]
    fn counted_groups_keep_zero_and_existing_count_overflow_errors() {
        let group = crate::expr::Expr {
            ty: Type::Int,
            kind: crate::expr::Kind::Column(0),
        };
        let prepared = Prepared {
            auto_simple: None,
            internal_output: false,
            relational: None,
            schema: Arc::new(Schema::new(vec![Type::Int.field("g", false)])),
            plan: Plan {
                table: None,
                path: None,
                columns: vec![0],
                filter: None,
                groups: vec![group.clone()],
                aggregates: vec![bind::Agg {
                    function: Aggregate::Count,
                    input: None,
                    ty: Type::Int,
                    distinct: false,
                }],
                output: vec![Output::Group(0), Output::Aggregate(0)],
                names: vec!["g".into(), "n".into()],
                types: vec![Type::Int, Type::Int],
                order: vec![],
                aggregation: true,
                projection_pushdown: true,
            },
        };
        for (weights, error) in [
            (vec![0, 0], None),
            (vec![u64::MAX], Some("BIGINT overflow")),
            (vec![u64::MAX, 1], Some("aggregate count overflow")),
        ] {
            for workers in [1, 2, 4, 10] {
                let budget = Budget::new(1 << 20);
                let options = Options {
                    workers,
                    ..Options::default()
                };
                let mut output = Vec::new();
                let result = execute_counted_stream(
                    &prepared,
                    std::slice::from_ref(&group),
                    &options,
                    budget.clone(),
                    |send| {
                        for weight in &weights {
                            let input = Input {
                                len: 1,
                                columns: vec![Some(Value::adopt(
                                    Arc::new(
                                        Decimal128Array::from(vec![7])
                                            .with_precision_and_scale(38, 0)
                                            .unwrap(),
                                    ),
                                    &budget,
                                )?)],
                            };
                            send(
                                input,
                                vec![Value::adopt(
                                    Arc::new(UInt64Array::from(vec![*weight])),
                                    &budget,
                                )?],
                            )?;
                        }
                        Ok(())
                    },
                    &mut |batch| {
                        output.push(batch);
                        Ok(())
                    },
                );
                if let Some(error) = error {
                    assert_eq!(result.err().unwrap(), error);
                } else {
                    result.unwrap();
                    assert_eq!(
                        output[0]
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                }
                drop(output);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.peak_workers(), 0);
            }
        }
    }
    #[test]
    fn integer_output_retains_only_its_known_value_length() {
        for nullable in [false, true] {
            let len = 100_003;
            let numbers: ArrayRef = Arc::new(Decimal128Array::from_iter(
                (0..len).map(|i| (!nullable || i % 7 != 0).then_some(i as i128)),
            ));
            let output = materialize(&numbers, &Type::Int).unwrap();
            let values = output
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values();
            assert_eq!(values.len(), len);
            assert_eq!(values.inner().capacity(), len * std::mem::size_of::<i64>());
        }
    }

    #[test]
    fn integer_materialization_checks_valid_values_and_preserves_sliced_nulls() {
        use arrow::buffer::NullBuffer;
        for values in [
            vec![],
            vec![0, -1, i64::MIN as i128, i64::MAX as i128],
            vec![i128::MIN, 7, i128::MAX, i64::MAX as i128 + 1],
            (0..19)
                .map(|i| if i % 4 == 0 { i128::MAX } else { i })
                .collect(),
        ] {
            for validity in [
                None,
                Some(vec![true; values.len()]),
                Some((0..values.len()).map(|i| i % 2 == 1).collect()),
                Some(vec![false; values.len()]),
            ] {
                let array =
                    Decimal128Array::new(values.clone().into(), validity.map(NullBuffer::from));
                for offset in 0..=array.len() {
                    for len in [0, array.len() - offset] {
                        let array: ArrayRef = Arc::new(array.slice(offset, len));
                        let expected = numeric(&array)
                            .iter()
                            .map(|v| {
                                v.map(|v| {
                                    i64::try_from(v)
                                        .map_err(|_| crate::Error::from("BIGINT overflow"))
                                })
                                .transpose()
                            })
                            .collect::<Result<Vec<_>>>()
                            .map(Int64Array::from);
                        let actual = materialize(&array, &Type::Int);
                        match (expected, actual) {
                            (Ok(expected), Ok(actual)) => assert_eq!(
                                &expected,
                                actual.as_any().downcast_ref::<Int64Array>().unwrap()
                            ),
                            (Err(expected), Err(actual)) => assert_eq!(expected, actual),
                            _ => panic!("narrowing result differs"),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn shared_filter_preserves_sliced_nullable_masks_and_missing_columns() {
        for stride in [1, 2, 13, 1000] {
            let budget = Budget::new(1 << 20);
            let numbers: ArrayRef = Arc::new(Decimal128Array::from_iter(
                (0..80).map(|i| (i % 7 != 0).then_some(i as i128)),
            ));
            let texts: ArrayRef = Arc::new(StringArray::from_iter(
                (0..80).map(|i| (i % 5 != 0).then(|| format!("é\0{i}"))),
            ));
            let input = Input {
                columns: vec![
                    Some(Value::adopt(numbers.slice(3, 65), &budget).unwrap()),
                    None,
                    Some(Value::adopt(texts.slice(3, 65), &budget).unwrap()),
                    Some(Value::adopt(Arc::new(NullArray::new(65)), &budget).unwrap()),
                ],
                len: 65,
            };
            let mask =
                BooleanArray::from_iter((0..80).map(|i| (i % 11 != 0).then_some(i % stride == 0)))
                    .slice(3, 65);
            let result = filter_input(&input, &mask, &budget, true).unwrap();
            let selected: Vec<usize> = (0..65)
                .filter(|i| mask.is_valid(*i) && mask.value(*i))
                .collect();
            assert_eq!(result.len, selected.len());
            assert!(result.columns[1].is_none());
            let numbers = numeric(&result.columns[0].as_ref().unwrap().array);
            assert_eq!(
                numbers.iter().collect::<Vec<_>>(),
                selected
                    .iter()
                    .map(|i| ((i + 3) % 7 != 0).then_some((i + 3) as i128))
                    .collect::<Vec<_>>()
            );
            let texts = result.columns[2]
                .as_ref()
                .unwrap()
                .array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                texts
                    .iter()
                    .map(|s| s.map(str::to_owned))
                    .collect::<Vec<_>>(),
                selected
                    .iter()
                    .map(|i| ((i + 3) % 5 != 0).then(|| format!("é\0{}", i + 3)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                result.columns[3].as_ref().unwrap().array.len(),
                selected.len()
            );
            drop(result);
            drop(input);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn shared_filter_optional_index_falls_back_when_output_needs_the_headroom() {
        fn run(limit: usize, optimize: bool) -> (usize, Vec<Option<i128>>) {
            let budget = Budget::new(limit);
            let numbers: ArrayRef = Arc::new(Decimal128Array::from_iter_values(0..128));
            let input = Input {
                columns: (0..3)
                    .map(|_| Some(Value::adopt(numbers.clone(), &budget).unwrap()))
                    .collect(),
                len: 128,
            };
            let mask = BooleanArray::from((0..128).map(|i| i % 2 == 0).collect::<Vec<_>>());
            let result = filter_input(&input, &mask, &budget, optimize).unwrap();
            let values = numeric(&result.columns[0].as_ref().unwrap().array)
                .iter()
                .collect();
            drop(result);
            drop(input);
            assert_eq!(budget.used(), 0);
            (budget.peak(), values)
        }
        let (needed, expected) = run(1 << 20, false);
        let (peak, actual) = run(needed, true);
        assert_eq!(actual, expected);
        assert!(peak <= needed);
    }
    #[test]
    fn automatic_policy_retries_only_accounted_memory_and_at_most_once() {
        use std::cell::Cell;
        let options = Options {
            spill: Some(crate::spill::SpillOptions {
                mode: crate::spill::SpillMode::Auto,
                ..Default::default()
            }),
            ..Default::default()
        };
        for message in [
            "decimal overflow",
            "cancelled: query execution",
            "resource-limit: spill disk quota",
            "spill IPC decode: malformed",
        ] {
            let calls = Cell::new(0);
            let error = execution_policy(&options, |o, budget| {
                calls.set(calls.get() + 1);
                assert!(o.spill.is_none());
                let _lease = budget.reserve(128)?;
                Err(message.to_string().into())
            })
            .err()
            .unwrap();
            assert_eq!(error, message);
            assert_eq!(calls.get(), 1);
        }
        let calls = Cell::new(0);
        let error = execution_policy(&options, |o, _| {
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                assert!(o.spill.is_none());
            } else {
                assert_eq!(
                    o.spill.as_ref().unwrap().mode,
                    crate::spill::SpillMode::Force
                );
            }
            Err("resource-limit: accounted memory remains insufficient".into())
        })
        .err()
        .unwrap();
        assert_eq!(calls.get(), 2);
        assert!(error.contains("automatic spill retry"));
        let calls = Cell::new(0);
        let error = execution_policy(&options, |_, _| {
            calls.set(calls.get() + 1);
            options.cancellation.cancel();
            Err("resource-limit: accounted memory exhausted".into())
        })
        .err()
        .unwrap();
        assert_eq!(calls.get(), 1);
        assert!(error.starts_with("cancelled:"));
    }
    fn state(groups: usize) -> States {
        let mut state = States::new(&bind::Agg {
            function: Aggregate::Sum,
            input: None,
            ty: Type::Decimal(2),
            distinct: false,
        });
        for _ in 0..groups {
            state.push();
        }
        state
    }
    #[test]
    fn bounded_batch_sums_match_checked_prefixes_and_preserve_fallback_states() {
        for group_count in [1, 4, 64] {
            let values: Vec<_> = (0..257)
                .map(|i| match i % 5 {
                    0 => i128::from(i64::MIN),
                    1 => i128::from(i64::MAX),
                    2 => -100,
                    3 => 0,
                    _ => 101,
                })
                .collect();
            let groups: Vec<_> = (0..values.len()).map(|i| (i * 7) % group_count).collect();
            let mut fast = state(group_count);
            let mut expected = state(group_count);
            for (number, group) in values.iter().zip(&groups) {
                let sum = &mut expected.sums.as_mut().unwrap()[*group];
                *sum = crate::aggregate_sum::add(*sum, *number).unwrap();
                expected.counts[*group] = expected.counts[*group].checked_add(1).unwrap();
            }
            assert!(fast.add_narrow_batch(&Decimal128Array::from(values), &groups));
            assert_eq!(fast.sums, expected.sums);
            assert_eq!(fast.counts, expected.counts);
        }
        let limit = 10_i128.pow(38) - 1;
        for (sum, count, values, width) in [
            (limit - 1, 0, vec![Some(2)], 1),
            (-limit + 1, 0, vec![Some(-2)], 1),
            (0, u64::MAX, vec![Some(1)], 1),
            (0, 0, vec![Some(i128::from(i64::MAX) + 1)], 1),
            (0, 0, vec![Some(i128::from(i64::MIN) - 1)], 1),
            (0, 0, vec![Some(1), None], 1),
            (0, 0, vec![Some(1)], 65),
        ] {
            let mut current = state(width);
            current.sums.as_mut().unwrap()[0] = i256::from_i128(sum);
            current.counts[0] = count;
            let sums = current.sums.clone();
            let counts = current.counts.clone();
            let groups = vec![0; values.len()];
            assert!(!current.add_narrow_batch(&Decimal128Array::from(values), &groups));
            assert_eq!(current.sums, sums);
            assert_eq!(current.counts, counts);
        }
    }

    #[test]
    fn many_scalar_wide_states_charge_their_actual_vector_capacities() {
        let sql = format!(
            "SELECT {}",
            (1..=16)
                .map(|i| format!("SUM({i})"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let prepared = prepare(&sql, &HashMap::new(), &Options::default()).unwrap();
        let budget = Budget::new(1 << 20);
        let state = executor(&prepared, budget.clone(), true).unwrap();
        let aggregation::Backend::Single(single) = &state.aggregation else {
            panic!("default backend")
        };
        let buffers: usize = single
            .states
            .iter()
            .map(|s| {
                s.counts.capacity() * std::mem::size_of::<u64>()
                    + s.sums
                        .as_ref()
                        .map_or(0, |s| s.capacity() * std::mem::size_of::<i256>())
            })
            .sum();
        assert!(budget.used() >= buffers);
        let required = budget.used();
        drop(state);
        assert_eq!(budget.used(), 0);
        let tight = Budget::new(required - 1);
        assert!(executor(&prepared, tight.clone(), true).is_err());
        assert_eq!(tight.used(), 0);
        let fitting = Budget::new(required);
        drop(executor(&prepared, fitting.clone(), true).unwrap());
        assert_eq!(fitting.used(), 0);
    }
}

#[cfg(test)]
mod partitioned_sql_tests;

#[cfg(test)]
mod auto_spill_tests;

#[cfg(test)]
mod left_count_budget_tests;
