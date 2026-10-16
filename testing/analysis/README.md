# Frozen retail analysis SQL

`original/` contains all 20 original SQL cases, with exact original bytes.
`exact-product/` contains the 11 separately frozen variants. Neither replaces the
other. `sql-manifest.json` records every SQL SHA-256. No customer data, raw logs or
expected rows are included. SQL is original Assay test work under repository terms.

Data source: Chen, D. (2015), [Online Retail, UCI](https://doi.org/10.24432/C5BW33),
CC BY 4.0. Preserve attribution and the transformation manifest. Original workbook
values were converted to typed Parquet for the frozen workload; downloading or
converting a fresh copy does not prove byte identity with those inputs.

Run original 20 on both real and synthetic fixtures (40 records). Run variants 11
on both fixtures, plus two exact-decimal edge probes (24 records). Keep original
SQL, reference answers, comparator and decimal scaling unchanged. The previous
scoped run had 11 unsupported original cases per fixture and three unsupported
variant cases per fixture. That is historical classification, not an instruction
to skip them or an assertion about a new candidate.

In the independently reviewed PR6 comparison on October 16, 2026, tested source
`8bea917e` passed all 64 cases. Tested main `d86aa0dd` passed 36 and refused 28;
all 28 refusals became passes. Current source supports the exact decimal column
casts and selected ranking forms needed here. This does not extend support to
all casts or window functions. Reference answers were unchanged; no fresh retail
reference-engine execution is claimed. See the
[qualification report](../../docs/ENGINE-SNAPSHOT-QUALIFICATION.md) and
[current SQL boundaries](../../docs/SQL-COVERAGE.md).

Portable execution and redistribution review of synthetic fixtures, independent
expected answers, comparator and edge probes are pending. SQL files alone do not
constitute the complete real-world gate. Broader analysis must be separately
frozen and reviewed before adding it; never rename these 64 cases as full coverage.
