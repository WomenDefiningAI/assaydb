# Ranking window references

`cases.sql` contains 80 permanent cases. Each comment records whether output
order is specified and whether the query belongs to the SQLite cross-check
domain. `setup.sql` defines tiny typed tables. Integer/text cases are checked
against both DuckDB 1.5.6 and SQLite 3.51; typed decimal/date/Boolean/REAL
cases use DuckDB with exact INTEGER rank outputs. No case is discarded on a
disagreement. The generator exits nonzero and retains both outputs.

Every ROW_NUMBER in the shared reference cases has a unique ordering key.
Reference peer order is not a contract: separate Rust tests pin Assay's input
ordinal tie break and unchanged input output order at workers 1/2/4/10, batch
sizes 1/7/8192, and memory/spill execution.
The typed native fixture has four Parquet row groups and requires a positive
parallel scan task counter at workers greater than one. Upstream LIMIT
regressions use 128 integer keys and require more than one external join or
aggregate partition under forced spill. These activation checks supplement the
original small-fixture matrix; worker settings alone do not prove parallelism.

Nested derived-table LIMIT over joins/groups is stabilized before row selection
by completing the visible tuple ordering while retaining explicit primary keys.
A LIMIT needing canonicalization inside a CTE materialization is refused explicitly;
This includes single-use CTEs, which are inlined only after window planning.
CTEs with ranking before their LIMIT remain supported. See [semantics](../../../SEMANTICS-LOG.md) for this conservative boundary.
A separate reverse-order 128-row/four-rowgroup Parquet fixture checks partial
ORDER BY over scans and completed windows, with and without LIMIT. It requires
positive parallel reader and external sort counters. WindowOrder sorts physical
batches unchanged so downstream bindings retain the original Arrow schema.

Float zero keys are canonicalized before sorting, so -0.0 and +0.0 share a
partition/peer group. Private key copies leave output payload bits untouched.
The additional `finite_zero_boundary` receipt checks that choice in DuckDB and
SQLite. Decimal keys retain all decimal digits, including values beyond exact
binary64 integer precision; floats and decimals are never implicitly mixed.

NaN keys retain the established Assay GROUP BY equality: distinct NaN payloads
are distinct groups. `nan_boundary` records the exact input bits, DuckDB's
observed outputs, and the chosen Assay policy separately. DuckDB treats NaNs as
equal, so a payload-dependent difference is intentional, recorded, and excluded
from any claim that the NaN case agrees across engines. This case remains a
permanent native Parquet regression, not a dropped reference failure. SQLite's
SQL fixture interface cannot retain NaN payloads.

Regenerate under the shared activity lock, from the repository root:

```sh
python3 scripts/with_engine_activity.py window-reference \
  python3 scripts/window_ranking_reference.py --sqlite /path/to/pinned/sqlite3 --output /tmp/window-ranking-reference.json
```

No corpus-wide or performance claim follows from these mini-corpus checks.
Final acceptance requires independent Test Mini reproduction of the exact
candidate commits, full unchanged corpus/differential checks, and benchmark
no-regression gates.

PR6's October 16, 2026 qualification includes an independent 80-case regeneration
and 60 SQLite cross-checks. The unavailable Apple SQLite executable was replaced
by an explicitly reviewed official upstream 3.51.0 build; original expected
answers were not changed. This approval applies to that run, not to arbitrary
future executable substitutions. See [reproduction](REPRODUCE.md) and the
[qualification report](../../../docs/ENGINE-SNAPSHOT-QUALIFICATION.md).
