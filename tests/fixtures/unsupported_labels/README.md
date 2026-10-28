> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Missing-feature outcome labels

This is a reporting-only candidate. No previously refused query is implemented.
The 44 inline cases include 24 explicitly unsupported queries, four unchanged
SQL type errors, and 16 successful controls. NULLs, empty input, duplicate values,
DISTINCT, both Date/Text operand orders, Float/Bool aggregates, decimal sales,
and ordered duplicate rows are retained. Rust exercises every source case with
workers 1/4/10 and batches 1/7/8192 through both public execution and native sessions.

`tools/testing/unsupported_labels_reference.py` runs directly using the project's
Python before the independent-reference test. It requires
DuckDB 1.5.6, retains reference rows or the exact reference error for every case,
and writes `expected.json`. This in-process reference needs no shared activity
lock. The card's debug checks run through `card_dev_check.py`; the later full SLT,
55K hunt, release, analyst and performance gates remain the driver's responsibility.

Reference generation completed with DuckDB 1.5.6: 44 cases retained, 35 successful
reference outcomes and nine reference errors. All 16 Assay success controls have
successful reference results; all four genuine-error controls have reference
errors. The remaining reference errors are the four Bool AVG cases and
`text_date_column` (`TEXT < DATE`). The committed JSON retains their exact errors;
it does not turn those refusals into successful comparisons. PostgreSQL was not
executed by this card.

The first debug run in this retry passed formatting, Clippy and wire-label
controls, then exposed two test-serializer panics on Arrow's Null array type.
The serializer now handles that type explicitly as JSON null; no fixture answer
or production behavior changed. Failed test directories remain preserved.

The rerun passed `cargo fmt`, offline/locked all-target Clippy with `-D warnings`,
and offline/locked all-target Rust tests through `card_dev_check.py`. The wrapper
waited 421 seconds for a lane. All five label integration tests passed, including
the independent reference comparison and workers 1/4/10 × batches 1/7/8192 matrix;
the session executable's Parquet reply test also passed in the full run.
No release, full SLT, 55K, analyst or performance acceptance run is claimed here.

Successful results compare exactly, preserving duplicate multiplicity and ordered
rows. DECIMAL values use exact decimal strings; floating results use exact JSON
numbers (these fixtures use exactly representable binary values). Column names
also compare. Refused queries compare the handwritten Assay status and message,
while the independent reference outcome stays visible. A refusal is not counted
as an answer match. Reference errors for the four error controls must remain
errors. These cases are additive; frozen SLT/hunt inputs and receipts are unchanged.

## Deliberate differences and decisions needed

* Float SUM/AVG remain unsupported; an accumulation/rounding policy is future work.
* Bool SUM/AVG retain the directly requested unsupported class. This does not
  assert that DuckDB and PostgreSQL accept both forms, or choose Boolean-to-number
  coercion semantics. Their signatures must be reconciled before implementation.
* Date arithmetic remains unsupported. Date multiplication, Date aggregates and
  incompatible Int/Text comparisons stay errors; they are not newly admitted.
* Date/Text comparisons remain unsupported for literals, columns and expressions.
  Literal coercion and typed-column coercion need separate decisions before any
  feature implementation. No coercion is introduced by this label change.
* Arrow admission now prefixes every rejected input type. The type set admitted
  by `Type::from_arrow` is unchanged. Four Parquet round-trip types exercise CLI
  failure events and session reply construction. Other rejected Arrow types are
  checked directly because Parquet may normalize some to supported types.
* The session executable does not enable local-file SQL. Its Parquet wire test
  invokes the real reply handler with the existing library reader opt-in; the
  production protocol is unchanged. All SQL label cases also run through actual
  CLI/session child processes.

## Old -> new assertions

* `auto_pipeline_keeps_sql_errors_and_noneligible_dispatch`
  (`tests/auto_spill_pipeline.rs`), the Float SUM/AVG assertion in
  `tests/parallel_dynamic_scan.rs`,
  `left_count_declines_other_aggregates_residuals_and_error_expressions`
  (`tests/left_join_count.rs`), and the aggregate binding assertions in
  `src/rel/aggregate_reuse/tests.rs` and
  `src/execute/pipeline/continuation_tests.rs`:
  `expected numeric type, got Float` ->
  `unsupported: expected numeric type, got Float`.
* `count_wave_late_exits_hold_slots_through_delivery_errors_and_unwind` and
  `ordered_waves_release_funding_on_success_error_and_cancellation`
  (`src/rel/left_count/parallel/tests.rs`), both Time32 failure assertions:
  `unsupported input type: {type}` ->
  `unsupported: unsupported input type: {type}`. Fixtures, emitted rows and
  resource assertions are unchanged.
* `direct_filter_keeps_projected_type_validation_even_when_all_rows_reject`
  (`tests/parallel.rs`): substring `unsupported input type` -> exact leading
  prefix `unsupported: unsupported input type:`.

No additional label leak was found in the worklist's label-leak section. Session
DDL unsupported types already carry `error_kind=unsupported`; already-prefixed
window/cast/function refusals need no change. General `Type::scale` errors,
Int/Text or Text/Binary comparisons, invalid Date operations, and decimal cast
overflow remain errors. They are not missing-feature classifications in this card.

Estimated reporting impact: approximately 1,946 SLT Float SUM/AVG records move
from execution_error to unsupported, based on the supplied worklist. Date/Arrow
record counts are unmeasured. Success counts, wrong-answer counts, applicability
and denominators must not change. This is an estimate, not a rerun or a rewrite
of archived receipts.

October 15 integrated-base correction: `date_text_literal` is now supported by
the DATE feature. Its previous unsupported expectation becomes DuckDB-generated
Boolean true. The other 43 cases and every remaining exact refusal are retained;
the generator has refreshed expected.json. The three labels tests use the same
case status, so the newly supported query is no longer sent to failure-only CLI
checks and is instead checked against its reference answer.

## Combined-branch reconciliation (Oct 15)

All 44 original queries remain: 23 successes, 17 unsupported forms and four
genuine type errors. The six newly supported cases compare exact independent
DuckDB 1.5.6 rows and output names; the existing worker/batch/session matrix is
unchanged. These Float inputs and answers are exactly binary-representable.

| Cases | Old expectation | New expectation |
| --- | --- | --- |
| `float_sum_duplicates` | `unsupported: expected numeric type, got Float` | Exact Float `2.0` |
| `float_avg_duplicates` | Same Float refusal | Exact Float `1.0` |
| `float_sum_empty`, `float_sum_nulls`, `float_avg_empty`, `float_avg_nulls` | Same Float refusal | NULL, independently evaluated for each original query |
| `float_sum_distinct`, `float_avg_distinct` | Same Float refusal, then (Oct 15) `unsupported: DISTINCT SUM/AVG over Float` | Oct 16: exact DuckDB Float `1.0` (approved Float DISTINCT rule) |

The CLI/session exact-class test still executes every remaining failure. Since
October 16 it pins the still-unsupported Bool SUM case to the unsupported class
and exact message (the two DISTINCT Float cases are now DuckDB-answer checks); ordinary Float SUM/AVG success cases remain in the value/reference
and worker/batch/session comparisons. Bool, Date arithmetic, Date/Text columns,
Arrow, and genuine SQL-type-error expectations are unchanged. The earlier
`date_text_literal` success was already present at b9212315 and is unchanged.

Final combined-branch validation passed through `card_dev_check.py` in the
`card-dev` lane: `cargo fmt`, `cargo clippy --offline --locked --all-targets --
-D warnings`, and `cargo test --offline --locked --all-targets` (exit 0).
The command used pipefail and retained the complete test log at
`[retained private validation log]`. The initial focused failures remain
in `[retained private validation log]` and
`[retained private validation log]`; no failure evidence was deleted.
All 237 original SQL queries and their independent DuckDB outcomes are unchanged;
69 cases are additive. All 17 modified files are within the card's scope; no
production source, frozen/dependency content or Git state changed. The driver's
release, accuracy, strict analyst and speed checks are pending.
