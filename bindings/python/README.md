# Assay for Python

Native analytical SQL with Arrow results. Version `0.1.0a2` is an experimental
alpha. The packaged fast engine is tested, not proved, and does not include
linked verified execution. Separate proof research describes its own narrower
scope. Only read Parquet files you trust.

This installation uses the published Alpha 2 package, not the newer source now
on public main. PR6 (`b07d7e8`, October 16, 2026) added exact decimal expression
casts and selected ranking functions to current source. To try those additions,
follow the [source-build instructions](BUILDING.md) and retain the Git revision;
unchanged package version metadata does not identify the newer build. See the
[current SQL coverage](../../docs/SQL-COVERAGE.md) and
[source qualification](../../docs/ENGINE-SNAPSHOT-QUALIFICATION.md).

Install the published version from PyPI:

```sh
python -m pip install --only-binary=:all: 'assaydb==0.1.0a2'
```

A supplied wheel can also be installed with
`python -m pip install --only-binary=:all: /path/to/assaydb.whl`, using its actual
filename. No Rust compiler is needed to install a wheel. Python 3.10 or later
and PyArrow are required.

```python
import assaydb

with assaydb.connect() as db:
    db.execute("CREATE TABLE events (id INTEGER, label TEXT)")
    db.execute("INSERT INTO events VALUES (9007199254740993, 'hello'), (2, 'world')")
    result = db.sql("SELECT id, label FROM events ORDER BY id")

print(result.to_arrow())
# result.to_pandas()  # Install pandas separately if needed.
# result.to_polars()  # Install polars separately if needed.
```

Read local files with `read_parquet('events.parquet')` and
`read_csv('events.csv')` in SQL. CSV has a required header and all columns start
as TEXT; use `CAST(id AS INTEGER)` for integers. Empty fields are empty strings,
so use `NULLIF(column, '')` explicitly when wanted. Quoted commas and newlines are
supported. Paths must name local regular files, without URLs or globs; keep files
unchanged while queries run. Connections enable reading by default;
`connect(allow_local_files=False)` refuses these functions and is not a directory
sandbox. CSV is a bounded import intended for small files; Parquet uses scans.

`execute()` and `sql()` execute one statement. Query results provide
`to_arrow()`, `to_pandas()`, `to_polars()`, `rowcount` and `is_query`.
DML results provide affected `rowcount`; asking for a DML table raises `SQLError`.
Native operations release the GIL, and one connection serializes its operations.
Connections own independent temporary catalogs; completed results survive close.

These are not persistent database files or Python DB-API connections. Parameters,
transactions and streaming cursors are not implemented by this wrapper.
Unsupported SQL raises `UnsupportedError`, invalid SQL raises `SQLError`, limits
raise `ResourceError`, and closed connections raise `ConnectionClosedError`.
All derive from `AssayError`; `InternalError` reports unexpected engine/storage
failures. A caught panic closes the connection. Refusal advice appears in the
exception text and `.hint`, with the original diagnostic in `.native_message`.
Advice never rewrites or retries SQL. REAL casts may round large integers;
INTEGER casts truncate fractional values.

Defaults: one worker, 256 MiB accounted query memory, 64 MiB catalog allowance
and 64 MiB serialized result limit. Set `workers`, `memory_bytes`, `catalog_bytes`
and `max_result_bytes` explicitly to override them. Catalog allowance must be at
most one quarter of query memory. `directory=` chooses the parent of a private
temporary catalog directory. Closing removes only that session's own files.
These limits do not cap total process memory.

Arrow IPC preserves engine types, binary values and NULLs, while copying results
into Python-owned memory. It is not zero-copy execution. pandas conversion uses
Arrow dtypes by default to preserve nullable integers above 2^53; supplying your
own `types_mapper` changes that conversion policy. Polars converts the resulting
Arrow table; neither optional dependency executes SQL for Assay.

Historical package checks do not validate this sanitized export.
See [release provenance](../../docs/RELEASES.md) for source references and limits.
Package tests and wheel tags do not establish full SQL compatibility.

See the [notebook](notebooks/quickstart.ipynb) and
[build instructions](BUILDING.md).
