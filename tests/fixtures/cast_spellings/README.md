# CAST reference fixtures

The active receipt is `expected-strings-distinct-float.json`. It contains 137
cases. The Rust tests retain workers 1/4/10, batches 1/7/8192 and optimization
off/on, exact decimals, binary64 bits, NULLs, ordering and bag multiplicities.
Explicit refusals and documented differences remain distinct from exact matches.

The 24 `lab_*` cases are original laboratory-reading examples. They cover
DOUBLE / DOUBLE PRECISION / FLOAT8, CAST / ::, AVG / SUM, and decimal scales
4 / 6 through a three-table join. Independently chosen readings include signed
near-half boundaries, duplicate measurements, multiple instruments per group,
interleaved groups, mixed values and all-NULL groups.

Generate with DuckDB 1.5.6 using `python scripts/cast_spellings_reference.py`.
No Assay engine output is used to generate answers. Separate Decimal arithmetic
checked the 24 replacement cases. Historical receipt variants retain their
other cases and use the same replacement laboratory cases where applicable.
