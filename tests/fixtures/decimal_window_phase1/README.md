# DECIMAL and ranking composition controls

These 12 queries use six independently specified rows and hand-derived answers.
Nine expect exact values and three expect reached cast-overflow errors. The
frozen SQL and answers are unchanged by this export. They cover rounded ties,
partitions, aggregate inputs, aliases, nested casts, NULL placement, CASE masks,
WHERE filtering, casted ranks and LIMIT error visibility.

`tests/decimal_window_composition.rs` checks the controls at workers 1/4, batch
sizes 1/7 and spill off/on. Activation assertions require positive parallel scan
and external sort counters in designated cases; configured workers alone do not
prove parallel execution. Reservation checks concern engine-accounted memory,
not a hard process-memory cap. See the qualification report for run status.
