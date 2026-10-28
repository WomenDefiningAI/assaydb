> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# NaN total ordering — correctness regression

The 77 permanent queries contain 76 successful comparisons/relational/window
cases and one explicit existing boundary (integration: aggregate FILTER and
GREATEST/LEAST became supported, so `filter_boundary`, `greatest_boundary` and
`least_boundary` now compare with their recorded DuckDB answers). `readings.parquet` is a tiny
PyArrow-written fixture with 40 sensor readings in ten row groups. It includes
positive/negative quiet/signaling NaN payloads, signed zeros, infinities, NULLs
and finite values. Row group zero contains a NaN even though its maximum
statistic is -0.25. The Rust test checks that this row survives `reading>100`.
`float32.parquet` checks that Float32 input (widened exactly to binary64 since
the Parquet widening change) keeps the NaN rules; its answers are DuckDB 1.5.6's.

Regenerate only with the project's DuckDB 1.5.6/PyArrow environment:

```sh
.venv/bin/python tools/testing/nan_order_reference.py
```

The generator never executes Assay. It loads the Parquet file unfiltered into
DuckDB's temporary table and independently cross-checks every answer against an
unoptimized scan of the original Parquet file. In this environment, DuckDB
1.5.6's direct Parquet scan with predicate pushdown returned no rows for the NaN
readings at ids 33 and 35 (a row group whose statistics report a maximum of
-0.25), while the materialized table and the unoptimized scan both returned
them. `reference-pushdown-diagnostic.json` records the three outcomes and the
queries that reproduce them. The first-attempt generator, cases and goldens
(which used the pushdown answers) are kept in private card evidence, not in
this fixture. The supported-domain revision
also replaces unsupported exponent syntax with finite thresholds ±100 (all
finite readings lie within ±10), keeping every case and query family. The four
set-operation cases use an outer SELECT for ordering because this branch refuses
top-level set ORDER BY; set-syntax-diagnostic.json preserves their original SQL
and results, which are unchanged by the syntax wrapper. `expected.json` records the source/input
hashes, versions, logical types and reference rows. `random.json` retains all
1,000 unique SQL queries, seed 20261016, and their DuckDB results. Random queries
cover comparisons with literals/columns, Boolean negation, filtered aggregates,
GROUP BY, DISTINCT, ordering/LIMIT, windows, joins and membership subqueries.
Every successful result compares complete ordered rows, duplicates and logical
types exactly. NaNs and signed zeros use SQL equivalence; every other binary64
value compares by bits, without tolerance. Original input payload bits are
checked separately after sorting, so key normalization cannot change projections.

`tests/nan_order.rs` runs all fixed cases at workers 1/4/10, batches 1/7/8192 and
spill off/on. All random queries run both without spill and with forced spill;
worker/batch settings rotate, and alternate queries disable relational
optimizations. Forced-spill counters must show actual external sort, join and
aggregation work. Every query checks temporary spill cleanup; successful cases
check the accounted memory limit. A native unit control directly exercises hash
runtime-filter membership across different NaN payloads, signed zeros and NULL,
and requires all reservations to be released. The realistic sensor query filters high
readings and ranks them within sensor, including NaN gaps as the highest values.

PostgreSQL 17 agreement is source/documentation based, **not a server run**:
connection to its local socket was denied by the sandbox. The installed 17.11
`src/include/utils/float.h` implements `float8_eq/ne/lt/le/gt/ge/min/max`; its hash
is recorded in the receipt. The installed PostgreSQL 17 numeric-type manual
also specifies that all floating NaNs compare equal and above all non-NaNs and
that quoted NaN input is supported. See the corresponding upstream
[source](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/include/utils/float.h)
and [manual](https://www.postgresql.org/docs/17/datatype-numeric.html#DATATYPE-FLOAT).
Thus MIN returns a non-NaN when one exists and NaN for an all-NaN non-NULL group;
MAX returns NaN whenever one exists. NULLs remain ignored by MIN/MAX. ASC puts
NaN after +infinity; DESC puts it first among non-NULL values. Tests specify
NULL placement explicitly; Assay's existing defaults are unchanged.

Existing boundaries remain explicit: text-to-DOUBLE now parses (cast-dialect change), but `CAST('NaN' AS DOUBLE)`
still refuses as a nonfinite result although both references accept it (the
case pins the new refusal message); legacy text-to-REAL retains its
SQLite parser; nonfinite session assignment remains refused. Float32 input is
accepted through exact Parquet widening and follows the same NaN rules.
Float SUM/AVG window admission and mixed Int/Float IN-subquery refusals are
unchanged. These are not counted as successful differential cases. Float
MIN/MAX windows now use the same corrected extreme kernel as ordinary MIN/MAX.

Reference-backed assertion transitions:

- `numeric_compare::exact_relation_covers_fraction_sign_and_wide_bounds`:
  integer versus NaN unordered → integer less than NaN.
- `in_subquery_types::float_nan_signed_zero_preserve_equality_and_mixed_membership_is_refused`:
  NaN IN a NaN-only subquery false → true; WHERE membership returns zero rows
  → the single NaN row (id 0). All other membership expectations stay.
- `window_ranking::typed_float_zero_nan_and_exact_decimal_keys_use_existing_grouping_equality`:
  id 5 `[id,rank,dense_rank,partition_row,decimal_rank]` changes
  `[5,7,4,1,4]` → `[5,5,3,3,4]`; id 6 changes `[6,8,5,1,8]` → `[6,8,4,1,8]`.
  Every source bit, decimal result and resource assertion is retained.
- The aggregate-window NaN-key control changes explicit refusal → `[3,3]`
  for both whole partitions and RANGE peers. Float SUM/AVG refusal checks remain.
- The top-k sort control's comparator uses canonical float keys; signed zeros
  tie before the secondary key. Its cancellation, budget and release assertions
  are unchanged. The finite comparison and all-NaN scalar truth tables have
  independent handwritten expectations in the new numeric comparison unit test.

The old window-ranking receipts and generator remain historical evidence of the
payload-dependent policy; their NaN disagreement is superseded by this fixture,
not rewritten. The source audit found no float statistics pruning: `prune.rs`
prunes columns, Parquet predicate/selection fast paths exclude Float, and
cardinality/clustered-domain statistics use Int32/Int64 only. Hash-based runtime
filters share the corrected `Keys` encoding; numeric bitmap filters exclude
Float. General expression programs reuse the corrected scalar evaluator.

Validation attempts are kept in private card evidence.
Release-mode accuracy, strict analyst, unchanged-corpus and speed gates remain
the integration driver's responsibility. No full-corpus or timing claim follows
from this fixture.
