> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Mixed Int/Float promotion fixture

Helen's October 15, 2026 decision: a reachable Int arm of a Float-typed CASE,
COALESCE or NULLIF-argument conditional, and the Int side of a mixed Int/Float
IN subquery, are promoted to DOUBLE exactly as DuckDB 1.5.6 does (PostgreSQL
agrees where it has the form), accepting binary64 rounding above 2^53.

- `setup.sql`, `cases.sql`: 57 cases over values at and around 2^53, 2^53+1,
  2^63 (i64::MAX rounds to 2^63), -2^63, NULL and -0.0, with division, cast,
  remainder, aggregate, grouping, ordering, join and IN-subquery consumers.
- `analysis_setup.sql`, `analysis.sql`: order lines LEFT JOIN discounts with
  `COALESCE(discount_cents, rate * qty * price, 0)`. The `expected_rows` were
  computed by hand (worked in the file) and fixed before Assay ran; the
  generator only checks that DuckDB agrees.
- `expected.json`: DuckDB answers from `tools/testing/mixed_intfloat_reference.py`
  (`SET threads=1`, `SET integer_division=true`; INTEGER→BIGINT, REAL→DOUBLE).
  DuckDB stores the INSERT literal `-0.0` as +0, so the reference re-inserts
  that one row with an IEEE -0.0, the value Assay and SQLite store.
- `migrated.json`: DuckDB answers for old test assertions that expected the two
  removed refusals (`tools/testing/mixed_intfloat_migrated_reference.py`).

`tests/mixed_intfloat.rs` runs every case at workers 1/4/10 × batch 1/7/8192 ×
optimizations off/on and compares Float bits exactly.

## Scoped consumer rules (promoted values only)

Provenance: promoted arms are wrapped in the internal `$promote_float` marker
(evaluated exactly as `CAST(int AS REAL)`); `mixed_numeric::Fact::promoted`
propagates through CASE, casts to REAL, Float arithmetic, unary minus, ABS,
ROUND, SUM/AVG/MIN/MAX, projections, CTEs, subqueries and joins. Explicit CASTs,
literals and ordinary REAL columns never carry it.

- **A** comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`, BETWEEN, IN list, simple CASE,
  NULLIF) of a promoted value with an Int compares in DOUBLE, as DuckDB does.
  Ordinary Int/Float comparisons stay exact (`control_*`, `nullif_int_real`).
- **B** (removed October 17, 2026) REAL `%` formerly kept SQLite truncation and
  refused a promoted operand outside [-2^63, 2^63). REAL `%` is now `fmod` on
  the binary64 operands (maintainer decision), exactly DuckDB's computation on
  the promoted value, so the `b_remainder_*` cases return DuckDB's rows.
- **E** `CAST(promoted AS INTEGER)` outside [-2^63, 2^63) (or NaN) is an
  `INTEGER out of range` error, like DuckDB's conversion error and PostgreSQL's
  `bigint out of range`.

## Retained differences (not part of this change)

- Assay has no nonfinite REAL values: overflow errors (`nonfinite_overflow`),
  where DuckDB returns +inf.
- `ABS(-0.0)` returns -0.0 (SQLite); DuckDB returns +0.0 (superseded
  2026-10-17: Assay now returns +0.0, like DuckDB and PostgreSQL). ORDER BY places
  -0.0 before 0.0; DuckDB treats them as ties. Those rows are excluded.
- CAST of non-integral REAL to INTEGER truncates (SQLite);
  DuckDB rounds half to even. Cases avoid these existing policies.
