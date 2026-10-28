> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Decimal casts from Float and bare targets

The 62 cases in `cases.sql` are additive, inline SQL. Regenerate `expected.json`
with the project environment:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 tools/testing/decimal_float_cast_reference.py
```

The generator requires DuckDB **1.5.6**, never invokes Assay, and retains each
DuckDB result/error as well as the expected Assay result/error. Assay REAL is
binary64, so reference SQL substitutes DOUBLE for REAL explicitly. Decimal
results use fixed-scale strings (including zeros), with no value tolerance.
Rust checks all cases, output numeric types/scales, ordered sequences or bags
with duplicate multiplicities, both optimizer settings, workers 1/4/10 and
batch sizes 1/7/8192. Additional scanned-column tests cover NaN, infinities,
overflow, masked rows and signed zeros. DDL defaults and unchanged Float
AVG(decimal)/division result types have separate controls.

Helen's October 15 rule rounds the exact binary value half away from zero.
Python `Fraction` supplies an independent oracle, using DuckDB's source DOUBLE
values and retained IEEE bits. It also generates 2,130 boundary cases from
explicit bit patterns, halfway neighbors, precision boundaries, extreme
exponents, signed zeros, subnormals, nonfinite values and a fixed random seed.
The Rust test applies those bits directly to the expression evaluator and
checks coefficients/errors and reservation release on success and failure.

Every difference from DuckDB 1.5.6 in this fixture is listed here and under
`differences` in the JSON:

| Cases | DuckDB | Assay | Reason |
|---|---|---|---|
| `float_2.675`, `numeric_float` | 2.68 | 2.67 | Binary64 2.675 is below the exact halfway value |
| `float_-2.675` | -2.68 | -2.67 | Symmetric exact binary rounding |
| `scale18` | 0.100000000000000000 | 0.100000000000000006 | Exact binary64 0.1 at scale 18 |
| `text_column_stays_refused` | 1.250, 2.500 | `unsupported:` | Existing text-column restriction retained |
| `bool_stays_refused` | 1.000 | `unsupported:` | Existing Boolean-source restriction retained |
| `precision_only_stays_refused` | 1 | `unsupported:` | Precision-only targets remain outside scope |

No case is removed for disagreeing. Expected errors retain DuckDB's error text
and independently specified Assay errors. NaN/inf are injected through typed
Arrow/Parquet because permissive SQL text-to-REAL parsing has a separate
existing contract. The corpus includes NULL, empty/all-NULL aggregate inputs,
duplicate/tied results, numeric aliases, Int/Decimal/Float, literal text,
Date/text-column/Bool refusals, overflow, CASE/WHERE masks and analyst
division/AVG/inventory shapes.

Bare DECIMAL/NUMERIC/DEC CAST and `::` defaults are (18,3); bare CREATE TABLE
columns stay (38,0). This deliberate context difference must not be normalized
away. Precision-only casts and text-column conversions need separate future
scope; no additional maintainer decision is needed to implement this card.
PostgreSQL was not run here. Helen's exact-binary and bare-default decisions
explicitly settle those semantics even where other databases differ.

Two prior refusal controls in `tests/decimal_cast.rs` now assert values:
`CAST(1 AS DECIMAL)` -> 1.000 and
`CAST(CAST(1 AS REAL) AS DECIMAL(4,2))` -> 1.00. All other existing assertions
remain. Native validation and the frozen accuracy/performance gates are left
to the driver; this fixture is not a full-coverage result.
