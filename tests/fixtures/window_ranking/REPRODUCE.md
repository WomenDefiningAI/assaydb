# Independent W1 reproduction

Local focused results are provisional. Test Mini must use the exact candidate
commit supplied in the handoff and record its own source, toolchain and binary
hashes. Do not infer acceptance from the Buildlab receipt.

Use the pinned Rust toolchain and lockfile, the host's canonical Cargo target,
and one shared activity lock. After changing engine worktrees, clean only the
`assaydb` package in the relevant profile and require an actual compilation.
The project Python must contain DuckDB 1.5.6 and PyArrow; the reference generator
also requires SQLite 3.51. Do not install dependencies during timed work.

Run these commands serially under the activity wrapper (shown individually;
an orchestrator may instead inherit the single outer lock descriptor):

```sh
.venv/bin/python research/tools/with_engine_activity.py w1-check cargo check --locked --offline --all-targets
.venv/bin/python research/tools/with_engine_activity.py w1-reference .venv/bin/python tools/testing/window_ranking_reference.py --output /tmp/w1-reference-independent.json
.venv/bin/python research/tools/with_engine_activity.py w1-tests cargo test --locked --offline --all-targets
.venv/bin/python research/tools/with_engine_activity.py w1-clippy cargo clippy --locked --offline --all-targets -- -D warnings
```

Retain the independently generated reference file and compare its 80 case
records, boundary outputs and disagreement list with `expected.json`. Shared
domain disagreement must be zero. The explicitly recorded NaN payload policy
difference is required evidence, not a successful cross-engine comparison.
The fixture's REAL column must stay Float64, asserted by the native tests.

The full test run includes `tests/window_ranking.rs` and
`rel::window::tests::window_cancellation_and_errors_release_every_reservation`.
Check every worker/batch/spill matrix, exact output order, selected and hidden
ORDER BY ranks, wildcard widths, grouped/HAVING and join inputs, DISTINCT,
typed Decimal/Float boundaries, cancellation, quota and memory failures.
Require the positive parallel-reader counters in the four-rowgroup tests and
multiple join/aggregate partitions in the upstream LIMIT regressions. Verify
partial ordering over scans/prior windows with and without LIMIT, preservation
of hidden/descending primary keys, and the explicit materialized-LIMIT refusal.
The input-sort unit test separately checks unchanged Arrow schemas, cancellation,
reservation release and spill cleanup.

Before acceptance, run the unchanged full SQLite sqllogictest corpus and 55K
differential hunt and compare record by record against the baseline. Any new
wrong result or previously passing query that refuses/errors rejects the card.
Retain all failed attempts. The standard frozen 22, no-regression speed guards,
main-relative paired one-core total/geomean bounds and remaining approved
performance gates also apply; the mini-corpus does not replace them. Root's
handoff supplies the receiving host's exact standard gate commands and scope.

`tools/testing/window_ranking_result.json` records admitted/refused forms,
semantics, local validation status and per-function soundness notes. Update
acceptance claims only from independently reproduced exact-commit evidence.
