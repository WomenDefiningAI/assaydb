> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Scalar math functions and GREATEST/LEAST

Regenerate with the project environment (DuckDB 1.5.6, PyArrow) and a local
PostgreSQL 17 server reachable through `psql` (database `postgres`). Generation
is append-only: remove `expected.json` deliberately before regenerating.

```sh
.venv/bin/python tools/testing/scalar_math_reference.py
```

The generator never executes Assay. It writes six PyArrow Parquet inputs and
`expected.json`:

- `values.parquet` (`u`): 30 binary64 values (NaN, +/-inf, +/-0, +/-1, 0.5,
  2.5, pi, pi/2, 1e22, 1e23, subnormal 1e-308 and 5e-324, +/-1e308, EXP
  overflow/underflow boundaries 709.7, 710, -745.2, -740, -2.567, NULL, ...);
  7-row groups.
- `pairs.parquet` (`v`): every ordered pair of 18 values including NaN,
  infinities, signed zeros, 1/3, -8, 1e308, 1e-308 and NULL; row id =
  18 * index(x) + index(y); 13-row groups.
- `nums.parquet` (`n`, 12 rows, 5-row groups) and `empty.parquet` (`empty`):
  BIGINT `i`/`j` (including 2^53 + 1 and the BIGINT extremes), DECIMAL(15,2)
  `d`, DECIMAL(15,3) `e`, DOUBLE `f` (including -0), VARCHAR `t`, DATE `dt`,
  group `g` and NULLs.

DuckDB reads materialized copies (`threads=1`, so Float aggregates use file
order). PostgreSQL evaluates every grid cell as a separate statement over the
same binary64 values written as float8 literals, through a temporary PL/pgSQL
function that captures each error; LOG(b, x) uses PostgreSQL's only
two-argument overload, NUMERIC.

- `remainder_float.parquet` (`rf`, 15 x 15 REAL pairs: NaN, +/-inf, +/-0,
  +/-7.5, +/-2, 0.5, 3, -0.25, 1024, 123456.75, NULL) and
  `remainder_decimal.parquet` (`rd`, 10 x 10 DECIMAL(15,2) pairs: +/-7.55,
  +/-2.00, 0.00, 0.50, 1000.00, 0.01, 555285.16, NULL); 11-row groups.

## Grid cells (2,634 function cells + 850 remainder cells)

Each cell records DuckDB's outcome, PostgreSQL's outcome, the required Assay
outcome and the rule deriving it:

| Cells | Rule |
|---|---|
| 2,166 | references agree (bit-identical binary64): Assay returns that answer |
| 265 | references both raise: Assay raises its documented domain error |
| 80 | PostgreSQL has no float8 overload (`log2`, `trunc(x, n)`): DuckDB answer |
| 27 | LOG(b, x) agrees with PostgreSQL NUMERIC within 1e-12 relative (NUMERIC has no signed zero; DuckDB divides two rounded logarithms) |
| 24 | finite overflow (EXP, POWER): DuckDB infinity, PostgreSQL error; Assay keeps `floating arithmetic overflow` |
| 20 | POWER of a negative number to a non-integer exponent: DuckDB NaN, PostgreSQL error; refused (`unsupported:`) |
| 15 | LOG with NaN beside an out-of-domain argument: DuckDB raises, PostgreSQL NaN; refused |
| 13 | underflow to zero (EXP, POWER, RADIANS): DuckDB zero, PostgreSQL error; Assay returns zero like its arithmetic |
| 10 | PostgreSQL has no float8 overload, DuckDB domain error: Assay raises the same domain error |
| 10 | POWER of zero to a negative exponent: DuckDB infinity, PostgreSQL error; refused |
| 4 | LOG of +infinity to a base in (0, 1]: DuckDB -infinity or error, PostgreSQL +infinity; refused |

Remainder cells (maintainer decision 2026-10-17: `%` and MOD follow DuckDB and
PostgreSQL; REAL is fmod, DECIMAL exact) compare DuckDB `%`/`mod` with
PostgreSQL NUMERIC `mod` (PostgreSQL has no float8 remainder; every REAL value
is exact as a 15-digit decimal, PostgreSQL's float8 -> NUMERIC precision):

| Cells | Rule |
|---|---|
| 676 | references agree (354 REAL, 282 exact DECIMAL, 40 where NUMERIC's unsigned zero meets DuckDB's -0) |
| 91 | DECIMAL % DOUBLE: PostgreSQL has no such operator; DuckDB fmod answer |
| 52 | REAL zero divisor with a non-NaN dividend: DuckDB NaN, PostgreSQL error; Assay keeps its approved NULL (as division) |
| 4 | NaN % 0 and MOD(NaN, 0) (zero and negative zero): both references NaN; Assay NaN (integration candidate 2 alignment with the 2026-10-17 NaN / 0 decision; formerly NULL) |
| 27 | zero divisor with a DECIMAL operand: DuckDB NULL, PostgreSQL error; Assay keeps `division by zero` (as division) |

`degrees` records DEGREES, which Assay refuses: DuckDB multiplies by 180/pi and
PostgreSQL divides by pi/180, so finite results differ in the last bit (3 of the
30 grid values here).

## Cases (62)

`cases.sql` holds 42 answer cases and 20 error/refusal cases, each with its
DuckDB 1.5.6 result, column names and types. Assay must match DuckDB's values;
types compare after mapping DuckDB's narrower integers (TINYINT for SIGN,
INTEGER for literals, HUGEINT) to BIGINT and DECIMAL(p,s) to DECIMAL(38,s),
Assay's single INTEGER and precision-38 DECIMAL. Two cases carry an explicit
`assay_types` with a `type_note` (untyped NULL results). Cases with
`"difference"` record why Assay refuses or answers differently where DuckDB
answers; `r004` lists `assay_rows` (NULL for REAL zero divisors, the approved
rule, where DuckDB returns NaN).

## Test

`tests/scalar_math.rs` runs every grid expression in bulk over all
reference-successful rows (workers 1/4/10 x batches 1/7/8192 x optimizer
off/on) and each expected error singly; every case in all 18 settings, plus
forced spill for grouped cases (external aggregation partitions must be used);
first-row error selection under parallel execution; refusals; and the CLI over
Parquet. Floats are compared bit for bit on macOS (where the references were
generated and both engines call the same libm) and within 4 ulp elsewhere.
