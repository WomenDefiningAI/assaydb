# Independent ranking reference reproduction

Use Rust 1.90.0 and the locked dependencies. Serialize native tests and reference
checks with the shared activity lock described in CONTRIBUTING.md. Use a fresh
build target when switching source trees and retain source, tool and binary hashes.

The reference generator requires DuckDB 1.5.6, PyArrow and SQLite 3.51.x. The frozen
fixture records an Apple SQLite 3.51.0 build; an arbitrary upstream 3.51 release
must not be silently presented as that same executable. Record and review the
chosen executable's provenance and hash before regenerating. Never overwrite
`expected.json` during verification.

```sh
python3 scripts/with_engine_activity.py ranking-reference python3 scripts/window_ranking_reference.py --sqlite /path/to/pinned/sqlite3 --output /tmp/window-ranking-reference.json
python3 scripts/with_engine_activity.py rust-tests cargo test --locked --all-targets
python3 scripts/with_engine_activity.py clippy cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

Compare all 80 case records, finite-zero boundaries and disagreements with the
frozen expected file. Shared-domain disagreement must be zero. The NaN payload
policy difference is intentional and separately reported, never counted as
cross-engine agreement. Native tests cover the worker/batch/spill matrices,
activation counters, cancellation, reservations, ordering and explicit refusals.
These focused checks do not replace the repository's complete merge gate.
