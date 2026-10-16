# Build and verify the Python package

Use Rust 1.90.0, a native C/C++ linker, and Python 3.10 or later.
These commands build the checked-out source. At PR6 merge `b07d7e8`
(October 16, 2026), that source is newer than published Alpha 2, although version
metadata remains `0.1.0a2`. Record `git rev-parse HEAD` with your artifact.
A successful build does not turn it into the reviewed PyPI wheel or a new release.
The binding has its own Cargo workspace/lockfile and uses the engine by path.
Its lockfile resolves registry Parquet 57.0.0. It does not inherit the repository
root's native CLI patch at `vendor/parquet-57.0.0-decoder-v1`: Cargo applies patches
from the active workspace root, not from a path dependency. The staged Python
source archive uses this separate resolution too. Shared engine source therefore
does not mean identical decoder dependencies. PR6's native CLI timing and patched
decoder evidence do not qualify a Python source build or wheel; those need their
own validation with the actual dependency resolution and build settings.
PyO3 is pinned to 0.29.0; maturin is pinned to 1.9.6.

From the repository root:
```sh
python3 -m venv .venv-build
.venv-build/bin/python -m pip install maturin==1.9.6
python3 scripts/with_engine_activity.py python-wheel \
  python3 scripts/with_release_paths.py .venv-build/bin/maturin build --release --locked \
  --manifest-path bindings/python/Cargo.toml --out dist
```

This produces a wheel for the build host. Distributed manylinux wheels require
a compatible build environment and audit; renaming a wheel does not provide
compatibility. See [maturin's distribution guide](https://www.maturin.rs/distribution.html).
Release builds remap private build paths and strip debug information.
Inspect actual native bytes before distributing any new artifact. The checked-in
release profiles use `strip = "debuginfo"`; PR6’s timing record reports
`strip=none`. These packaging commands do not claim to reproduce the measured
binaries. Keep the actual command, flags and binary hash with any new result.

Use the actual wheel filename:
```sh
python3 scripts/with_engine_activity.py python-install \
  python3 bindings/python/scripts/verify_wheel.py --wheel dist/ACTUAL_WHEEL.whl \
  --output artifacts/wheel-check
```

The verifier installs into a fresh temporary environment, runs package tests and
the notebook, and checks sample files with independent SQLite/DuckDB queries.
Optional conversions require pandas/Polars; DuckDB is only a test reference.
The verifier requires no skipped package tests and removes Rust from its test PATH.
It does not publish packages.

For a self-contained source archive, start from a committed checkout:
```sh
python3 scripts/with_engine_activity.py python-sdist \
  .venv-build/bin/python bindings/python/scripts/stage_sdist.py --output /tmp/assay-source
cd /tmp/assay-source
python3 -m venv .venv-build
.venv-build/bin/python -m pip install maturin==1.9.6
.venv-build/bin/python -m maturin sdist --out dist
```

The destination must not already exist. Staging validates committed inputs,
copies only allowlisted build inputs and the explicitly allowlisted compile-time fixtures,
and records hashes and package-only path/include rewrites. The pinned engine
release profile is retained at the staged workspace root. A new sdist must also
be unpacked, rebuilt, and checked in a fresh installed environment.

On shared hosts, select the same ASSAY_ACTIVITY_LOCK_PATH for all checkouts.
The lock prevents builds/tests from competing with native measurements.
[Release provenance](../../docs/RELEASES.md) distinguishes historical package
checks from validation of this sanitized export. No new build or test result is
claimed by these instructions.
