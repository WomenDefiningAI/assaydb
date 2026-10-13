# Assay

Assay is an experimental analytical SQL engine written in Rust, with Python
sessions and Arrow results. This snapshot contains Alpha 2:
Python `0.1.0a2` and Rust version metadata `0.1.0-alpha.2`.

The fast engine is tested, not formally proved. This export contains no linked
verified mode, proof workspaces, website, or benchmark datasets. Catalogs are
temporary and SQL support is incomplete. Only read Parquet files you trust.
Accounted query limits do not cap total process memory.

## Try Python

Published wheels target CPython 3.10+ on macOS arm64 and Linux x86_64.
Use a virtual environment:
```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --only-binary=:all: assaydb==0.1.0a2
```

```python
import assaydb

with assaydb.connect() as con:
    con.execute("CREATE TABLE events (id INTEGER, label TEXT)")
    con.execute("INSERT INTO events VALUES (1, 'hello'), (2, 'world')")
    result = con.sql("SELECT id, label FROM events ORDER BY id")

print(result.to_arrow().to_pylist())
```

Results remain usable after the connection closes. PyArrow is required;
pandas/Polars conversions are optional. See the [Python API](bindings/python/README.md),
[notebook](bindings/python/notebooks/quickstart.ipynb), and
[build instructions](bindings/python/BUILDING.md).

## Rust and contributions

The native command-line tools use JSON protocols. Start with the
[crate guide](docs/CRATE-README.md). Read [SQL coverage](docs/SQL-COVERAGE.md),
[semantics](SEMANTICS-LOG.md), and [limitations](docs/WHAT-ASSAY-ISNT.md).

[CONTRIBUTING](CONTRIBUTING.md) explains how to fork, check a bounded change, and
open a draft PR. Report wrong answers with minimal redistributable data and
independently derived expected results. Use [SECURITY](SECURITY.md) for sensitive
reports and follow the [code of conduct](CODE_OF_CONDUCT.md).

[Release provenance](docs/RELEASES.md) distinguishes original release sources
from this sanitized export. It records registry-specific source evidence without
equating PyPI and crates.io provenance.
[Benchmark input instructions](docs/BENCHMARK-INPUTS.md) explain excluded inputs.

## Licensing and attribution

Original Assay code is [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE).
Dependencies and adapted community policies keep their own licenses and notices;
see [THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES) and THIRD-PARTY-LICENSES.
We acknowledge Apache Arrow/Parquet, sqlparser-rs, PyO3, maturin, and SQLite's
D. Richard Hipp and contributors. Required upstream notices are retained.
