> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Exact Parquet widening

`tools/testing/parquet_types_reference.py` writes these small inputs with
PyArrow 25.0.1 and freezes `expected.json` using DuckDB 1.5.6, before Assay is
run. No Assay output is used to construct expectations. The manifest records
each file's SHA-256, size, schema and row-group count. Total Parquet size is
36,204 bytes (23 files), below the 2 MB limit.

`sales.parquet` models a pandas-style sales export: dictionary region/category,
Float32 price, Int16 quantity, UInt32 store ID, LargeUtf8 notes, NULLs in every
column and six row groups. It is written directly with PyArrow because pandas
is absent from the authorized project environment. `analysis.sql` contains
revenue by region, top categories, average price per store, and store joins.
All their answers are in the same frozen reference file.

The 68 reference cases compare integers, text, dates and NULLs exactly.
Floating values compare binary64 bits, including signed zero and special
values; there is no epsilon. Aggregations explicitly cast Float32 input to
DOUBLE in both engines. Unordered queries compare duplicate-preserving
multisets; ordered queries compare the entire ordered result.

One frozen answer intentionally exceeds Assay's existing public BIGINT result
range: UInt64 SUM returns 9,223,372,036,854,775,812 in DuckDB's HUGEINT. Its
reference answer is retained verbatim, and the regression asserts Assay's
existing `BIGINT overflow` error. This is an output boundary, not an input
refusal; the same input's individual values, filtering and grouping succeed.

One frozen answer differs only in the representative of an equal-value group:
`float32_groups` has a single -0.0 input (no +0.0). DuckDB returns that group's
first-seen key, -0.0; Assay's existing GROUP BY contract treats signed zeros as
equal and emits the canonical +0.0 key, exactly as it already does for a native
Float64 column. The reference is kept verbatim; the regression maps only that
key cell and requires every other cell to match. Bit-exact widening of -0.0
itself is pinned by `float32_values` and `float_special_bits`.

Parquet DATE stores days, so ordinary writers discard Date64 sub-day values.
The three `date64_*_ms`/invalid Date64 fixtures use physical INT64 milliseconds
with an explicit Arrow Date64 schema hint, installed through the writer's
key/value metadata API. DuckDB reads that physical INT64; the exact-midnight
control uses the recorded `reference_sql` to convert milliseconds to DATE.
The non-midnight and out-of-Date32-range inputs must refuse, never truncate.
The UInt64 overflow fixture includes `i64::MAX + 1` in a later row group.

`uint32_high` holds sorted, NULL-free UInt32 keys at or above 2^31 in every
row group, and `uint32_lookup` is a small UInt32 dimension. Parquet keeps such
values in signed INT32 with unsigned-ordered statistics, so a signed reading of
min/max is a plausible but wrong range (2^31 reads as -2^31). Integer bounds
derived from Parquet statistics (cost estimates, clustered grouping and
left-join count windows) therefore ignore unsigned columns; the grouping, join,
left-join count and filter cases over these files pin the exact answers.

`tests/parquet_types.rs` runs all reference cases with workers 1/4/10,
batches 1/7/8192, and forced spill on/off. It separately exercises cached
predicates, optimizations on/off, session local-file reads, and that FLOAT4
DDL stays refused. Scan unit tests check reservation failure, exact sufficient budgets,
release on failure/drop, and value-domain guards.

Old → new: Float32, small/unsigned integers, LargeUtf8, Utf8View and supported
string dictionaries refuse → exact scan widening. UInt64 is admitted only
through `i64::MAX`; Date64 only at exact midnight within Date32 range. Session
DDL is unchanged: REAL is already binary64, and FLOAT4 (binary32 in DuckDB and
PostgreSQL, and refused as a CAST target) stays refused.
The old label tests for these newly supported types move to positive controls;
Timestamp and all remaining unsupported type assertions are preserved.
Ordinary Binary already succeeds on this branch and remains supported;
LargeBinary/BinaryView/FixedSizeBinary remain refused. Timestamp, Time,
Duration, Interval, Decimal256 and nested types remain `unsupported:`.

The driver's release, quick accuracy and strict analyst checks remain the
authoritative acceptance gates. This fixture establishes no full-corpus or
performance claim.
