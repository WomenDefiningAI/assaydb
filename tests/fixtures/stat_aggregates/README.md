> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Statistical and boolean aggregate reference corpus

Covers VARIANCE/VAR_SAMP/VAR_POP, STDDEV/STDDEV_SAMP/STDDEV_POP,
COVAR_SAMP/COVAR_POP, CORR, the REGR_* family, BOOL_AND/BOOL_OR, COUNT_IF and
ARG_MAX/ARG_MIN/MAX_BY/MIN_BY (maintainer scope, October 17, 2026). The
semantics, refusals and numerics are recorded in
[SEMANTICS-LOG.md](../../../SEMANTICS-LOG.md) ("Statistical and boolean
aggregates").

Regenerate with the project Python (DuckDB 1.5.6; PostgreSQL via `psql` on the
local socket; the TPC-H directory is the SF1 Parquet set):

```sh
.venv/bin/python tools/testing/stat_aggregates_reference.py --pg --tpch <tpch-sf1-dir>
```

No expected value comes from Assay. The generator writes the six Parquet tables
from literal rows (`stats` 18 rows with NULLs, duplicates, -0.0/0.0, ties and a
one-row group; `empty`; `regions`; `nonfinite` with NaN/infinities and a finite
overflow group; `pairs`, REAL (y, x) pairs with NaN/infinities; `wide` with BIGINT/DECIMAL(38) extremes and values beyond
double precision; `big`, 6,000 seeded pseudo-random rows in 12 row groups),
runs the 72 cases of `cases.sql` with `SET threads=1`, and records:

- `rows`/`types` or the DuckDB error for every case, and `pg`: `agrees`,
  `differs` (with `pg_rows`), or the PostgreSQL error (COUNT_IF and the ARG_MAX
  family do not exist there);
- `assay_error` for every refusal or error, with `difference` explaining any
  case where DuckDB returns rows;
- `expected_rows` for cases marked `"expected_from": "postgresql"`: the
  PostgreSQL answer, which Assay must return by the maintainer's October 17
  decision (degenerate CORR/REGR_SLOPE groups are NULL; DuckDB's NaN stays in
  `rows` as the documented difference). Requires `--pg`;
- `permitted` for the two tie cases: every value DuckDB may return per group,
  in input order, from an independent query. Assay must return the first;
- `exact_rows` for three cases computed with Python fractions from the literal
  rows, and DuckDB's own error against them. DuckDB's regression intercept on
  `big` is off by 6.7e-11 relative (cancellation), and on `wide` group 2 it
  rounds inputs above 2^53 before VAR_POP (4.0 instead of 8/3). Assay is
  compared with the exact values (1e-14 relative) for these cases (`wide`:
  "DuckDB rounds inputs to DOUBLE; exact by decision 2026-10-17"), with
  `expected_rows` where present, and with DuckDB (1e-12 relative, 1e-12
  absolute near zero) for all others.

`tests/stat_aggregates.rs` runs every case at workers 1/2/4/10, batch sizes
1/7/8192 (the 6,000-row table skips batch size 1), ordinary and forced spill,
plus planner optimizations off; checks result types against DuckDB's, that REAL
result bits are identical in every setting, that forced spill really
partitions and that aggregate helpers engage. Separate tests cover
handwritten arithmetic controls, budget charging with spill parity for 20,000
groups, the CLI over Parquet, and (with `ASSAY_TPCH_SF1_DIR` set) the six
`analysis.sql` TPC-H SF1 examples whose answers are in
`analysis_expected.json`, including exact values for the column moments.

Measured on this corpus: maximum relative difference from DuckDB 7.6e-15 where
DuckDB is accurate, and 4.5e-16 from the exact values. On TPC-H SF1 Assay is
within 2.5e-16 of every exact value; DuckDB's price-on-quantity intercept is off
by 1.8e-10 single-threaded (7.6e-12 to 3.6e-11 in multi-threaded runs).
