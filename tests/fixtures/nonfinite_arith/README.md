> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Nonfinite Float arithmetic and overflowing Float SUM/AVG

Regenerate with the project environment (DuckDB 1.5.6, PyArrow) and a local
PostgreSQL 17 server reachable through `psql` (database `postgres`):

```sh
.venv/bin/python tools/testing/nonfinite_arith_reference.py
```

The generator never executes Assay. It writes three small PyArrow Parquet
inputs and `expected.json`:

- `pairs.parquet` (`v`): 144 rows, every ordered pair (x, y) of the twelve
  values NaN, sign-bit NaN, +inf, -inf, 0, -0, 1.5, -2, 1e308, -1e308, 1e-308
  (subnormal) and NULL; row id = 12 * index(x) + index(y); 13-row groups.
- `values.parquet` (`u`): the twelve values; 5-row groups.
- `aggregate.parquet` (`a`): eleven SUM/AVG groups interleaved round-robin in
  file order (4-row groups), including finite-prefix overflow, overflow then
  cancellation, overflow then -inf, overflow then NaN, a near-maximum rounding
  case, an order that does not overflow, NULL-only and finite controls.

DuckDB reads materialized copies of the Parquet files (the NaN order fixture
records a DuckDB Parquet-pushdown NaN omission) with `threads=1`, so SUM order
is file order. PostgreSQL evaluates every cell as a separate statement over the
same binary64 values written as float8 literals, through a temporary PL/pgSQL
function that captures each error. Float results are projected as text, so
signed zeros and nonfinite values survive. PostgreSQL AVG(float8) keeps a sum
of squares that can overflow where SUM does not, so each aggregate is also run
with AVG removed (`postgresql_without_avg`).

Each cell records DuckDB's outcome, PostgreSQL's outcome, the required Assay
outcome and the rule deriving it. The rules are:

| Cells | Rule |
|---|---|
| 680 | references agree: Assay returns that answer (NaN / zero = NaN and `ABS(-0.0)` = 0 by the 2026-10-17 decisions) |
| 45 | division by a Float zero: DuckDB +/-inf or NaN, PostgreSQL error; Assay keeps its approved SQLite NULL |
| 19 | finite operands overflow: DuckDB infinity, PostgreSQL error; Assay keeps `floating arithmetic overflow` |
| 12 | `ROUND(x, 2)`: PostgreSQL has no `round(float8, int)`; the ROUND decision follows DuckDB |
| 11 | division by a DECIMAL zero (`1.5 / 0`, `x / 0.0`): DuckDB +/-inf or NaN, PostgreSQL error; Assay keeps `division by zero` |
| 4 | finite-prefix SUM overflow: DuckDB +/-inf, PostgreSQL error; 2026-10-17 maintainer decision follows DuckDB |
| 3 | underflow to zero: DuckDB zero, PostgreSQL error; Assay unchanged (zero) |
| 1 | PostgreSQL AVG sum-of-squares overflow while SUM agrees; Assay follows DuckDB, unchanged |
| 1 | `SUM(x * 10)`: the per-row finite overflow error precedes the SUM |

Finite floats are stored as shortest round-trip strings (for example
`"0.9999999999999999"` for `1e308 * 1e-308`), because JSON number parsers are
not required to round correctly; NaN, `+inf`, `-inf`, `0` and `-0` are labels.
Integers stay JSON numbers.

`tests/nonfinite_arith.rs` runs one bulk query per expression over all rows
expected to succeed, so NaN and infinite rows cross batch and worker boundaries,
and one single-row query per expected error. Settings are workers 1/2/4/10 ×
batches 1/7/8192 × relational optimizations off/on, plus forced spill for every
aggregate case (grouped cases must report external aggregation partitions).
Results must be bit-identical across settings (NaN payloads collapsed). A CLI
test checks the JSON output spellings (`NaN`, `inf`, `-inf`) over Parquet and
the retained overflow error.

The `CASE WHEN x / 0 IS NULL ...` and `COALESCE(x / 0, 7)` cells guard the
planner fact that a constant zero divisor yields NULL. A Float dividend may be
NaN and NaN / 0 is NaN, so that fact no longer folds Float dividends:
`COALESCE(x / 0, 7)` is DOUBLE (7.0, or NaN for a NaN row), as in DuckDB.
`zero_divisor_fold_typing` also checks that Int dividends keep the fold and
their INTEGER result.
