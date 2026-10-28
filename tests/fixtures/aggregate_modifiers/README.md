> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Aggregate modifier reference corpus

Helen's October 16, 2026 `cov/aggmods` task card: aggregate `FILTER (WHERE ...)`,
Float `DISTINCT` SUM/AVG (approved: DuckDB value identity, then the fixed
canonical-order sum), `STRING_AGG(text, sep ORDER BY ...)` and
`GROUP BY ROLLUP / CUBE / GROUPING SETS` with `GROUPING(...)`.

Regenerate with the project Python (PostgreSQL agreement and TPC-H optional):

```sh
python3 tools/testing/aggregate_modifiers_reference.py \
  --pg --tpch "$ASSAY_TPCH_SF1_DIR"
```

The script writes the three Parquet tables with pyarrow from literal rows in the
script (`sales` has 18 rows in five row groups: NULL keys, `-0.0`, both NaN signs,
`+inf`, duplicate and empty strings, non-ASCII text; `empty_sales` has the same
schema and no rows; `big` holds overflow probes). DuckDB 1.5.6 reads those exact
files; Assay reads the same committed files. No reference value comes from Assay.
The answers were generated and saved before Assay first ran the corpus. One
refusal case (`filter_fallible_argument_refused`) was later re-spelled because
Assay refuses its original text-to-BIGINT column cast for an unrelated reason;
its DuckDB/PostgreSQL outcomes were regenerated, all other answers are unchanged.

`cases.sql` has 89 cases (70 reference answers, 19 deliberate refusals or errors).
Each case has a `pg` note. With `--pg`, every case also runs through `psql` on the
local PostgreSQL 17 against session-local TEMP tables holding the same rows (text
columns `COLLATE "C"`); `pg_agrees` is a loose numeric comparison used only for
these notes. All 70 answered cases agree with PostgreSQL except one:
PostgreSQL's AVG(float8) transition also accumulates squares and overflows on
1e308 values (SUM and COUNT agree). Refusals whose reference outcomes differ are
listed with the reason in `difference`.

`tests/aggregate_modifiers.rs` compares names, type classes (DuckDB HUGEINT and
narrow DECIMAL precisions map to Assay BIGINT and precision-38 DECIMAL with the
same scale) and values exactly: floats compare as binary64 values parsed from
DuckDB's shortest representation (NaN equals NaN; -0.0 equals 0.0 because SUM
folds from +0). Every case runs at workers 1/4/10 x batch 1/7/8192 x forced spill
off/on, plus optimizations off at workers 4 / batch 7 with and without spill.
Additional tests pin the first-occurrence DISTINCT summation order bit for bit,
DISTINCT identity through the sort-based spill deduplication, and funded
STRING_AGG buffer growth with a clean resource-limit refusal.

`analysis.sql` holds seven TPC-H SF1 analyses (FILTER counts per month and per
ship mode, average distinct discount as DECIMAL and DOUBLE, nations per region,
richest customers per nation, revenue ROLLUP region -> nation, status/priority
CUBE). `analysis_expected.json` records DuckDB answers and the Parquet sizes and
hashes; the test runs only when `ASSAY_TPCH_SF1_DIR` names that directory, at
workers 1/4/10 x batch 1000/8192. Only the Float DISTINCT discount columns use the
approved relative 1e-12 Float SUM/AVG tolerance: DuckDB sums distinct values in
hash order (FOB gives 0.5500000000000002), Assay in first-occurrence order.
