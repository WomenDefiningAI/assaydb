> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Window extras reference corpus (October 18, 2026)

Covers PERCENT_RANK/CUME_DIST, IGNORE/RESPECT NULLS for FIRST_VALUE,
LAST_VALUE, NTH_VALUE, LAG and LEAD, NTH_VALUE FROM LAST (refused), named
WINDOW clauses, GROUPS frames, EXCLUDE, frames starting FOLLOWING or ending
PRECEDING, RANGE over DECIMAL and REAL keys, the decided DESC NULLS LAST
default, and statistical, boolean, COUNT_IF, ARG_MAX family, MEDIAN/QUANTILE,
FILTER, DISTINCT and MODE window aggregates (maintainer scope "everything listed",
2026-10-17). Semantics and refusals: [SEMANTICS-LOG.md](../../../SEMANTICS-LOG.md)
("Window extras").

Regenerate with the project Python (DuckDB 1.5.6; PostgreSQL 17 through `psql`
on the local socket; the TPC-H directory is the SF1 Parquet set):

```sh
.venv/bin/python tools/testing/window_extras_reference.py --pg --tpch <tpch-sf1-dir>
```

No expected value comes from Assay. The generator writes `events.parquet` (24
literal rows in 5 row groups: partitions with NULL, an all-NULL partition,
INTEGER/DECIMAL/REAL order keys with ties and NULLs, REAL NaN, +/-infinity,
-0.0 and 0.0, dates across a leap day, text and booleans) and an empty table,
runs the 94 cases of `cases.sql` with `SET threads=1`, and records:

- DuckDB's `rows`/`types`, or its error, for every case;
- each case's PostgreSQL outcome and the required relation `pg`: `agree`
  (same rows, floats within 1e-12 relative), `rejects` (PostgreSQL has no such
  syntax or function: IGNORE NULLS, DISTINCT/MEDIAN/COUNT_IF/ARG_MAX windows,
  QUALIFY, window STRING_AGG ORDER BY, so DuckDB alone defines the answer) or
  `differs: ...` (a recorded decision or a reference disagreement that Assay
  refuses). The generator fails on any other PostgreSQL behaviour;
- `expected_rows` for the two `"expected_from": "postgresql"` cases, whose
  degenerate CORR/REGR_SLOPE frames are NULL by the 2026-10-17 decision
  (DuckDB's NaN stays in `rows`);
- `expected_rows` for the two `"expected_from": "postgresql_equivalent"` MODE
  cases: window MODE ties go to the smallest value like grouped MODE (decision
  2026-10-18). PostgreSQL has no window MODE, so `expected_sql_postgresql`, an
  equivalent query applying PostgreSQL's grouped `mode() WITHIN GROUP` to each
  frame's rows, produces the answer; DuckDB's position-based choice stays in
  `rows` (`reference_matches_expected` is false). Their DuckDB answers depend on
  frame position by design, so these two skip the shuffled-order check
  (`peer_order`); the tie-free `mode_without_ties` case keeps it;
- `assay_error` for every expected refusal or error, with `difference` when
  DuckDB answers.

Peer order: every case is re-run on three shuffled insertion orders and must
give identical DuckDB answers, so each admitted case has exactly one permitted
answer (value functions over peers use arguments that are constant within a
peer group or full ORDER BY tie-breakers).

`analysis.sql` holds six realistic TPC-H SF1 analyses (percentile rank of
customers by revenue within nation, 7-day moving standard deviation of daily
revenue, last urgent price carried forward with IGNORE NULLS, a named window
shared by several functions, EXCLUDE CURRENT ROW comparisons, a rolling median
with DISTINCT and FILTER); `analysis_expected.json` holds DuckDB's answers and
the input file sizes and hashes. `tests/window_extras.rs` runs the corpus at
workers 1/4/10 x batch 1/7/8192 x spill off/on (512-byte sort runs, so buffered
partitions spill) plus two unoptimized settings, pins identical bits across
settings, runs CLI checks over Parquet, a decimal MEDIAN decision check and a
20,000-row single-partition check against naive models written in the test.
