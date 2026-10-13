# External benchmark inputs

This engine export does not redistribute benchmark corpora, TPC-derived query
files, generator sources, generated rows/Parquet, or encoded golden datasets.
Public download access alone does not establish redistribution rights.

## TPC-derived workloads

Obtain specifications and tools directly from
[TPC's official downloads](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
Review the applicable copyright notices, EULA, derived-work permissions, and
result-reporting rules before use. Generate inputs locally and keep them out
of Git. Official downloads are not claimed to reproduce the frozen development
workload byte-for-byte; no rewritten query or changed answer is substituted.

Excluded development inputs include 22 queries from Polars benchmark commit
2d368574ae96ea27439906c4ae751e94fcd55532 and generator/golden inputs from
datafusion-contrib/tpcgen-rs commit 99bedaee4cfb1d6b67a59100a222d32643079832.
Generator Apache-2.0 licensing does not by itself resolve legacy distribution
text, copied benchmark material, or generated-data redistribution.

## SQL logic tests

SQLite sqllogictest is pinned to Fossil check-in
ab599cbdd0976a63c5cfc7755588f716a7621f27. Obtain it from the
[official project](https://www.sqlite.org/sqllogictest/) and retain its notices.
The [original copyright notice](licenses/SQLLOGICTEST-COPYRIGHT.md) offers CC0
among its alternatives. This is separate from SQLite core's public-domain status.

DuckDB v1.5.6 and DataFusion 54.0.0 corpus inventories were separate development
inputs. Review their original MIT or Apache-2.0/NOTICE terms and each copied
file or external fixture before redistribution. Those corpora are not bundled.
Original Assay unit/integration fixtures needed by this export are retained.
