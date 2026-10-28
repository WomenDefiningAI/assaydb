> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# TIMESTAMPTZ and session time zone candidate

Maintainer decisions, October 17, 2026: session time zone default UTC, set per
session; NOW()/CURRENT_* fixed at statement start with a fixed clock for tests;
IANA tzdb 2025b compiled in through `chrono-tz` 0.10.4; EXTRACT(EPOCH) DOUBLE.
The engine evaluates its own kernels (`src/zone.rs`); the references only
freeze answers.

`tools/testing/timestamptz_reference.py --pg` writes the Parquet files with
PyArrow (time-zone-aware columns as pandas writes them: UTC instants labelled
with the writer's zone), runs `cases.sql` on DuckDB 1.5.6 and on a live
PostgreSQL 17 server (TEMP tables with the same instants), **both with
`SET TimeZone` set to the case's `zone`**, and freezes `expected.json`.
TIMESTAMPTZ columns are compared as each engine prints them (DuckDB VARCHAR,
psql text). It refuses to overwrite a receipt.

| File | Contents |
| --- | --- |
| `events.parquet` | 302 events (user_id, event_at µs labelled America/New_York, amount) around both 2024 US DST changes, NULLs |
| `events_ns`, `events_ms` | The same instants in ns (label UTC) and ms (label Asia/Kolkata, truncated) |
| `edge.parquet` | New York gap/overlap instants, São Paulo skipped midnight, Lord Howe 30-minute transitions, epoch, leap day, NULL |
| `lmt.parquet` | 1880-01-01 (local mean time offsets with seconds) |
| `walls`, `texts` | Wall clocks and TIMESTAMPTZ text for casts |
| `empty_tz`, `ns_fraction` | Empty input; refused sub-microsecond ns |

118 cases in seven zones (UTC 42, America/New_York 33, Asia/Kolkata 21,
America/Los_Angeles 10, Australia/Lord_Howe 6, America/Sao_Paulo 5,
America/St_Johns 1): 91 answered (all agree in both references), 23
refusals, 4 data errors. `tests/timestamptz.rs` runs every case at workers
1/4/10 × batch 1/7/8192 × spill off/on (plus optimizer off) with the case's
session zone, prints every TIMESTAMPTZ value as the CLI does, and requires
spilled grouping; it also checks fixed-clock determinism, session `SET`,
TIMESTAMPTZ tables and the CLI `timezone`/`fixed_clock` fields.

## Agreement summary (both engines in the same zone)

| Operation | DuckDB 1.5.6 | PostgreSQL 17 | Assay |
| --- | --- | --- | --- |
| Display | `2024-03-10 01:30:00-05`, `+05:30` | same | same |
| Display with a seconds offset (LMT) | `+05:21` | `+05:21:10` | refuse |
| Skipped / repeated wall clock (cast, literal, AT TIME ZONE, DAY step into a gap) | later instant | later instant | same |
| DAY/MONTH step landing on a repeated time | earlier offset | later offset | refuse |
| DATE_TRUNC hour+ onto a skipped or repeated wall clock | 02:30+11 / 01:00-05 | 01:30+10:30 / 01:00-04 | refuse |
| `PST` in text | Los Angeles rules | fixed -08 | refuse |
| `'+05'` as a zone | error | POSIX UTC-5 | refuse |
| TO_TIMESTAMP(1.0000005) | …00.000001 | …00 (epoch-2000 subtraction first) | refuse |
| TO_TIMESTAMP(infinity) | error | infinity | refuse |
| EXTRACT(TIMEZONE_HOUR/MINUTE) | -3 / -30 (St John's) | same | same |
| Grouping by DATE_TRUNC('hour') over a fall-back night | merges the repeated hour | splits it | refuse |
