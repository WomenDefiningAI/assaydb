> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Ordered-set and distribution aggregates — reference fixture

`cases.sql` holds 46 queries over six small Parquet tables written by
`tools/testing/ordered_set_reference.py` from literal rows (exact binary64 bits
including -0.0, NaN, a negative-payload NaN and infinities; DECIMAL columns
declared with precision 12, 20 and 38, where DuckDB's DECIMAL interpolation
rules differ):

* `vals` — 22 rows in groups A–E and NULL, with ties, NULLs, an all-NULL group
  and non-finite REAL values;
* `empty` — the same schema without rows;
* `regions` — a join dimension;
* `edge` — REAL neighbours where DuckDB's and PostgreSQL's interpolation
  formulas overflow differently;
* `seq` — 1..25, where PERCENTILE_DISC(0.28) selects row 7 in DuckDB (exact
  ceil(n·f)) and row 8 in PostgreSQL (binary64);
* `big` — 6,000 deterministic pseudo-random rows in 7 groups for many-batch,
  multi-worker and spill runs.

Regenerate only with the project's DuckDB 1.5.6/PyArrow environment and a local
PostgreSQL 17 (`psql -h /tmp`):

```sh
.venv/bin/python tools/testing/ordered_set_reference.py --pg --tpch data/tpch-1
```

The generator never runs Assay. `expected.json` records, per case, DuckDB's
rows and logical types (or its error), PostgreSQL's outcome (`pg`: agrees,
differs with `pg_rows`, error, or no equivalent; `pg_sql` gives PostgreSQL's
spelling where it lacks MEDIAN/QUANTILE_*), and:

* `assay_error` + `difference` — Assay refuses (typed `unsupported:`) or raises
  (`semantic:`), with the reason when DuckDB answers;
* `permitted` (with `tie_column`) — every value DuckDB's plain MODE(x) may
  return for a group (equally frequent values, from an independent query);
  Assay must return the first (smallest) one;
* `assay_rows`/`assay_types` (with `assay_sql`) — maintainer-decided
  differences (October 17): APPROX_COUNT_DISTINCT returns the exact
  COUNT(DISTINCT) instead of DuckDB's HyperLogLog estimate, and interpolated
  DECIMAL quantiles return DOUBLE like PostgreSQL. `exact_cont(list(CAST(x AS
  VARCHAR)), 'q', desc)` in `assay_sql` is expanded by the generator: DuckDB
  collects each group's decimal values and Python computes the exact
  interpolation with `fractions.Fraction`, rounded once to binary64.
  `pg_vs_assay` records that PostgreSQL agrees with these rows within 1e-12.

`tests/ordered_set_aggregates.rs` runs every case at workers 1/2/4/10, batch
sizes 1/7/8192, with and without forced spill and with relational
optimizations off, requiring identical REAL bits across settings and REAL
values within 1e-12 of DuckDB (DuckDB's build fuses one multiply-add, so its
last bit can differ). `analysis.sql` and `analysis_expected.json` hold the
TPC-H SF1 examples (run with `ASSAY_TPCH_SF1_DIR` set).
