# Build and verify the Python package

Use Rust 1.90.0, a native C/C++ linker, and Python 3.10 or later.
The binding has its own Cargo workspace/lockfile and uses the engine by path.
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
Inspect actual native bytes before distributing any new artifact.

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
copies only allowlisted build inputs and the two compile-time engine fixtures,
and records hashes and package-only path/include rewrites. The pinned engine
release profile is retained at the staged workspace root. A new sdist must also
be unpacked, rebuilt, and checked in a fresh installed environment.

On shared hosts, select the same ASSAY_ACTIVITY_LOCK_PATH for all checkouts.
The lock prevents builds/tests from competing with native measurements.
[Release provenance](../../docs/RELEASES.md) distinguishes historical package
checks from validation of this sanitized export. No new build or test result is
claimed by these instructions.
