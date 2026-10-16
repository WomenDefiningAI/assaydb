# What Assay isn't

Assay is an experimental local analytical engine, not a complete SQLite or
DuckDB replacement. This page describes current source at the PR6 merge
`b07d7e8` (October 16, 2026); published Alpha 1/2 packages remain unchanged.
Some SQL is explicitly unsupported. See the source-checked
[SQL coverage table](SQL-COVERAGE.md). Passing a fixed SQL
corpus or a package test suite cannot prove every query correct.

- Connections own temporary catalogs. There are no durable database files,
  transactions, Python DB-API parameter binding or streaming cursors in this API.
- Results are materialized, then copied through Arrow IPC into Python-owned
  memory. Arrow conversion is not a claim of zero-copy execution.
- Accounted query/catalog/result limits do not bound all process memory.
  Arrow buffers, encoder scratch and downstream DataFrames can add allocations.
  The root native build’s patched decoder also leaves some native allocations, retained Zstandard
  state and conversion/output buffers outside engine accounting. Its 128 MiB
  decoder-window admission is not a cap on the whole process. This open limitation
  was accepted for experimental PR6 only; it is not a general safety guarantee.
  The separate Python workspace uses registry Parquet 57.0.0 without this patch;
  neither the patched decoder's admission check nor its validation can be assumed
  for Python source builds or wheels. Python limits are not hard process caps either.
- Local readers accept regular files, without URLs or globs. CSV is a bounded
  import with a required header and TEXT columns. Keep input files unchanged
  during execution. Disabling local readers is not a directory sandbox.
- Only read Parquet files you trust: a malformed file can exhaust memory.
  Query and catalog limits do not contain every allocation made while parsing
  Parquet metadata. Bounded malformed-input tests record errors and panics; they
  do not establish safety for arbitrary files. Earlier review recorded a parser
  panic for one malformed-metadata case; this documentation audit does not
  establish that every such panic is fixed. The current decoder changes do
  not make the reader a sandbox.
- The engine runs on one machine. It has no distributed execution or remote
  service API. Additional worker threads do not imply linear scaling.
- Explicit refusals protect cases for which supported semantics are incomplete.
  Cast guidance is advice, not an automatic rewrite: converting to REAL may
  round integers above 2^53, and converting to INTEGER truncates fractional values.

Wheels remove the need for users to install Rust; they do not remove Python
runtime dependencies. PyArrow is required, while pandas and Polars are optional
conversion packages. A platform or stable-ABI tag is not proof of every machine,
operating-system version or interpreter combination. Published alpha packages remain experimental. Historical checks do not validate
newly rebuilt artifacts. See [release provenance](RELEASES.md).

Current source adds exact decimal expression casts and selected ranking functions,
not universal decimal or window support. Aggregate windows, frames and many SQL
combinations still fail or are refused. The accepted PR6 SLT run retains 640
unsupported cases and 2,001 errors. Zero observed wrong answers in that collection
is not proof of correctness for all inputs.

The ten-worker SF10 result is 0.957067× Polars total time and 1.033839× geometric
mean. Both would need to reach 0.95× to meet the stated goal; it remains unmet.
The run used particular data, tools and a sampled host-quietness check. Sequential
phases and a connection interruption limit timing interpretation. Assay is not
production-ready, performance-qualified Alpha 3, or a formally proved engine.
See the [source-bound qualification and limits](ENGINE-SNAPSHOT-QUALIFICATION.md).
