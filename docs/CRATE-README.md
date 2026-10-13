# Assay engine alpha

Assay is an experimental analytical SQL engine with Arrow results. Version
`0.1.0-alpha.1` packages the fast engine and two JSON protocol executables,
`assaydb` and `assaydb-session`. It is not an interactive SQL shell. The engine is
tested, not proved, and this package does not include linked verified execution.
Separate verification evidence is research with a narrower scope.

## First query from the packaged crate

Use Rust 1.90.0 and a native C/C++ linker on macOS arm64 or Linux x86_64.
From an unpacked source archive or this checkout:

```sh
cargo install --path . --locked --root ./local-install --bin assaydb
mkdir first-query
cd first-query
printf '%s\n' '{"sql":"SELECT 1 AS answer", "output":"answer.json"}' | ../local-install/bin/assaydb
cat answer.json
```

The `rows` field is `[[1]]`. Standard output contains JSON progress events;
query rows go to the requested file. The output path must not already exist.
The command exits nonzero on failure and emits a `failure` event. Each invocation
runs one request. `tables` can map SQL table names to trusted local Parquet paths.
The separate `assaydb-session` binary uses a persistent JSON-lines session protocol
for temporary catalogs; Python is the documented higher-level session API.

To install the published version directly from crates.io:

```sh
cargo install assaydb --version 0.1.0-alpha.1 --locked --bin assaydb
```

See [release provenance](RELEASES.md) for the independent source-verification status.

## Limits and provenance

SQL support is incomplete. Unsupported SQL is refused, and resource accounting
is not a hard process-memory cap. Only read trusted Parquet files. There are
significant performance gaps on larger workloads; no current speed guarantee is
made. Catalogs are temporary; this is not a production database replacement.
No Windows, Intel Mac or Linux arm64 support is claimed by this alpha matrix.

See [SQL coverage](SQL-COVERAGE.md) and [release provenance](RELEASES.md).
Proof workspaces and linked verified execution are not included.

Original code is MIT OR Apache-2.0. Both license texts, third-party notices and
third-party licenses are included. The export retains engine source, Rust tests and required fixtures,
the pinned lockfile/toolchain and this guide. It excludes
benchmark datasets, campaign output and verification workspaces.
