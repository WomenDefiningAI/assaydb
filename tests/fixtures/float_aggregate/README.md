> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Float SUM/AVG reference corpus

Helen's October 15, 2026 decision in the `cov/floatsum` task card authorizes
plain IEEE binary64 addition in fixed plan order, without compensation, and
comparison to DuckDB 1.5.6 at relative 1e-12 / absolute 1e-12 near zero.

Regenerate with the project Python:

```sh
python3 tools/testing/float_aggregate_reference.py
```

`cases.sql` has 52 independent inline queries. `expected.json` retains every
DuckDB answer or error, including the eleven deliberately refused cases. The
generator also records 11 nonfinite/overflow/subnormal/zero boundary probes.
Rust supplies those exact input values through Parquet, avoiding the separate
nonfinite text-to-REAL cast refusal. No reference results come from Assay.
The generator maps the explicit `AS REAL` spelling to DuckDB `AS DOUBLE`:
Assay REAL is binary64 whereas DuckDB REAL is binary32. Both SQL texts are saved.
Integer result widths and untyped NULL representation can differ; decimal
values, NULL positions, counts, dates, text, row order and duplicate multiplicity
compare exactly. Only floating results use the approved tolerance. NaN and
infinities have explicit JSON string encodings, never JSON null or nonstandard
JSON numeric tokens.

SUM starts at positive zero and adds every non-NULL value in canonical input
order. AVG divides that completed sum by its non-NULL count. All-NULL and empty
inputs yield NULL. Floating partial aggregation is disabled; per-column helper
owners consume all batches in order. Forced external aggregation partitions raw
rows and replays each group's rows in order; it never merges floating sums.
`tests/float_aggregate.rs` checks exact bits with workers 1/2/4/10, batch sizes
1/7/8192, ordinary and forced-spill execution, and both optimizer settings.
Handwritten cancellation controls distinguish this from compensated/exact
summation. Separate tests grow numeric-key state capacity and check memory
refusal and spill cleanup.

DuckDB probes observed +infinity, -infinity and NaN propagation, including NaN
for opposite infinities. Finite overflow returns infinity in DuckDB. The installed
PostgreSQL 17 `server/utils/float.h` `float8_pl` explicitly calls
`float_overflow_error` for infinity produced from finite operands; the installed
catalog identifies it as the double SUM transition. No PostgreSQL server run
is claimed. Following the card's reference-disagreement rule, finite-prefix
overflow stays refused with
`unsupported: Float SUM/AVG finite-prefix overflow (DuckDB/PostgreSQL disagreement)`.
These three deliberate differences remain in the fixture; a maintainer decision
is needed before choosing overflow-to-infinity versus an SQL overflow error.

**October 16 update.** Float DISTINCT SUM/AVG is now approved and supported
for every input (see SEMANTICS-LOG, "Aggregate modifiers"). Four former
DISTINCT refusals (`distinct_sum`, `distinct_avg`, `distinct_nonnull_constant`,
`distinct_nullable_column`) compare against their unchanged DuckDB answers.
`distinct_fallible_null` now reaches its DECIMAL `1.0/0.0`, which raises
`division by zero` in Assay and PostgreSQL 17 (DuckDB returns NULL); it is an
explicit recorded difference. The paragraph below is historical.

Float SUM/AVG DISTINCT of a proven total constant NULL are supported: the
distinct value set is empty, so the ordinary accumulator returns typed NULL.
This proof uses the existing bounded checked constant evaluator and leaves the
original expression in the plan. Nullable columns, non-NULL constants and
failed proofs (including fallible expressions) remain refused with
`unsupported: DISTINCT SUM/AVG over Float`; their finite DuckDB values remain
recorded. Bool, Text and Date SUM/AVG are explicit unsupported inputs. Float
window aggregates remain outside this card, with the existing aggregate-window
refusal. COUNT and exact-numeric aggregate contracts are unchanged.

Predicted SLT transitions: finite non-DISTINCT Float SUM/AVG records whose other
operators already work should change from `expected numeric type, got Float`
to success. The worklist identifies 1,946 Float-error records as the candidate
population, not a measured pass increase. Other DISTINCT, windows, overflow and other
remaining syntax/type refusals are excluded from that prediction. Full SLT and
55K exact-record validation belong to the driver; no corpus result or complete
list of beyond-tolerance SLT disagreements is claimed here.

Attempt 1's 303,828-record quick check measured 1,021 fail → pass, zero wrong
answers and zero pass → fail. Its four rejected `result_changes` were all
error → unsupported transitions for DISTINCT of constant Float NULL. Attempt 2
adds the following unchanged SQL as regression copies; their existing frozen
NULL answers are now required. These four additional error → pass transitions
are predictions until the driver reruns its gate.

| Frozen record in `test/random/expr/` | Expected row |
| --- | --- |
| `slt_good_104.test:17527` | `(67, NULL)` |
| `slt_good_56.test:90834` | `(NULL)` |
| `slt_good_80.test:32416` | `(NULL, -2560)` |
| `slt_good_81.test:17090` | `(NULL)` |

The ten additive cases include these four regressions, empty input, duplicate
groups, a proven NULLIF constant, and three refusal boundaries. A Parquet test
checks typed NULLs, group counts and actual forced spill with workers 1/2/4/10,
batches 1/7/8192 and both optimizer settings. Existing general Float DISTINCT
refusal assertions remain unchanged. No gate policy or frozen record changed.

Existing assertion transition: the Float SUM case in
`src/rel/aggregate_reuse/tests.rs` previously accepted the binder error
`expected numeric type, got Float`; it now requires binding success, refusal of
the exact-SUM reuse optimization, and ordinary result parity across workers and
batches. Other declined optimization shapes retain their assertions.


Other existing assertion transitions for supported forms:

- `src/execute/pipeline/continuation_tests.rs`,
  `full_8192_batches_engage_parallel_admission_and_preserve_serial_sql`:
  Float SUM/AVG binder error → binding/execution success and serial parity;
  continuation admission remains false, with zero producers.
- `tests/parallel_dynamic_scan.rs`,
  `dynamic_scan_sql_matrix_preserves_nulls_duplicates_coercions_order_and_errors`:
  grouped Float SUM/AVG binder error → identical adversarial results across
  workers/batches, with zero partial-pipeline producers.
- `tests/auto_spill_pipeline.rs`,
  `auto_pipeline_keeps_sql_errors_and_noneligible_dispatch`: Float SUM binder
  error → bit-identical ordinary/auto dispatch, with zero partial producers.
- `tests/left_join_count.rs`,
  `left_count_declines_other_aggregates_residuals_and_error_expressions`:
  mixed COUNT/exact SUM/AVG/Float SUM binder error → successful ordinary LEFT
  results with COUNT-only fusion still declined. Other refusal assertions stay.

- `tests/demand_filter_errors.rs`,
  `demand_declines_fallible_expressions_and_preserves_first_error`: Float SUM/AVG
  binder errors → successful bit-identical LEFT-join results; demand pruning
  remains declined because arbitrary Float inputs lack a totality proof.
  All non-Float error expectations remain intact.


The original `left_join` reference case is retained with its existing
qualified-ORDER-BY refusal. The additive `left_join_supported_order` case changes
only ORDER BY to the supported output ordinal and verifies the same values.
This is a fixture syntax boundary, not a change to ORDER BY binding. Forced
external aggregation is asserted for grouped plans; scalar aggregation has
constant state and uses its ordered in-memory accumulator even in force mode.


Attempt 1 validation: DuckDB 1.5.6 generation completed (42 inline cases,
11 boundary probes); the required wrapper's `cargo fmt`, offline/locked
all-target Clippy with `-D warnings`, and offline/locked all-target debug tests
passed. The boundary matrix includes both scalar and genuinely spilled grouped
SUM/AVG, including NaN, infinities, subnormals and overflow refusals. No supported
mini-corpus value disagreed beyond the approved tolerance. Earlier failed debug
attempts were retained; their old-refusal and fixture-syntax corrections are
listed above. Full SLT/55K, release checks, frozen 22 and speed gates remain for
the driver; this is not a full-corpus or performance result.

Attempt 2 validation: regenerated all 52 inline cases and 11 boundary probes
with DuckDB 1.5.6. The required card wrapper passed `cargo fmt`, offline/locked
all-target Clippy with `-D warnings`, and offline/locked all-target debug tests,
including the four exact SQL regressions and the constant-NULL spill matrix.
The existing non-NULL DISTINCT refusal cases still pass unchanged. The driver's
quick-accuracy rerun is pending; no acceptance result is inferred from debug
tests.

## Combined-branch reconciliation (Oct 15)

All 52 queries and 11 boundary probes are retained: 42 inline successes, eight
unsupported forms and two genuine type errors. Regeneration changes metadata
only for existing cases; their DuckDB outcomes are unchanged.

| Case | Old expectation | New expectation |
| --- | --- | --- |
| `left_join` | `unsupported: ORDER BY must name an output or ordinal` | Ordered DuckDB rows `[(1, 3.0, 1.5, 2), (3, NULL, NULL, 0)]` |
| `bool_sum`, `bool_avg` | `unsupported: SUM/AVG input type Bool` | `unsupported: expected numeric type, got Bool` |
| `text_sum` | `unsupported: SUM/AVG input type Text` | Type error `expected numeric type, got Text` |
| `date_avg` | `unsupported: SUM/AVG input type Date` | Type error `expected numeric type, got Date` |
| `window` | `unsupported: aggregate window functions` | `unsupported: Float/Double sum OVER` |

The original qualified-ORDER-BY query already succeeds at b9212315. The initial
focused debug reproduction showed `left: None` versus the fixture's
`Some("unsupported: ORDER BY must name an output or ordinal")`: the assertion
expected a refusal, rather than observing a new engine refusal. The merged
SELECT-modifier implementation already resolves source-column identity and
rewrites ordering to an output ordinal before grouped projection extraction.
No engine behavior was changed. Both original LEFT queries now require their
independent reference answers, using the existing Float tolerance and exact
NULL/count/order comparison. Additional exact ORDER BY regressions and rejection
boundaries are in the SELECT modifiers fixture.

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

## Finite-prefix overflow decision (Oct 17)

The maintainer decided on 2026-10-17 that a Float SUM/AVG whose running total
overflows returns +/-infinity like DuckDB 1.5.6 (PostgreSQL raises). The three
boundary probes `finite_positive_overflow`, `finite_negative_overflow` and
`finite_overflow_then_cancel` therefore change from the refusal
`unsupported: Float SUM/AVG finite-prefix overflow (DuckDB/PostgreSQL disagreement)`
to their recorded DuckDB answers (+inf/+inf, -inf/-inf, +inf/+inf), which were
already in this fixture and are unchanged by regeneration. Their
`refusal`/`decision_needed` fields are replaced by a `decision` field; no other
case changed. The fixture now has ten inline cases with an expected refusal or
error (eight unsupported forms, two type errors) and no
refused boundary probe. The overflow corpus with PostgreSQL server results is
in [nonfinite_arith](../nonfinite_arith/README.md).
