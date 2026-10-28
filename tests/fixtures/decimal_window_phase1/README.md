# Additive DECIMAL × W1 composition controls

These 12 new queries use six independently specified rows and hand-derived
expectations. They do not change either feature's frozen reference suite,
SLT/hunt inputs, or any benchmark denominator. Status: source-only, unrun.

`cases.json` pins each SQL query, expected ordered rows or reached precision
error, and its derivation. Rounded DECIMAL keys create real ties and partitions;
other controls cover aggregate inputs, aliases, nested casts, NULL placement,
casted window results, CASE masks, WHERE filtering and LIMIT error visibility.
Nine queries expect exact values and three expect cast overflow.

`tests/decimal_window_composition.rs` writes the six rows as six Parquet row
groups and checks every query at 1/4 configured workers, batch sizes 1/7 and
spill configured off/on. The designated peer-ranking query must report positive
`parallel_scan_tasks` at four workers and positive `external_sort_runs` when
spill is configured. No actual parallel/spill activation is claimed until those
assertions run and pass; they do not claim a parallel ranking implementation.
The test also checks reservation bounds. It requires both features merged.

`research/tools/decimal_window_phase1.py` compares a saved combined session
binary and pinned DuckDB1.5.6 independently against the same hand expectations.
It writes fresh provenance and raw response files without modifying this fixture.
The Rust lane checks typed Arrow values, while the session/reference lane
compares exact numeric values and NULLs. All queries order their output.

Run only after the root's combined branch contains final W1 and DECIMAL heads,
and after the profiling slot releases the shared canonical activity lock.
Use the canonical target and force a package rebuild when switching worktrees.
Run the focused Rust test before the Python reference comparison; preserve all
failed attempts and do not rewrite or rescore an executed fixture. These controls
are provisional local composition checks, not Test Mini final acceptance.
