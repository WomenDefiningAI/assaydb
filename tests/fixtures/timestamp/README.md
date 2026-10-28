> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# TIMESTAMP (without time zone) candidate

Maintainer decision, October 17, 2026: TIMESTAMP first; TIMESTAMPTZ and the
session clock wait for an approved time-zone policy. The engine evaluates its
own kernels (`src/expr/timestamp.rs`); the references only freeze answers.

`tools/testing/timestamp_reference.py --pg` writes every Parquet file here with
PyArrow, runs `cases.sql` on DuckDB 1.5.6 over those files and on a live
PostgreSQL 17 server (psql over the local socket, TEMP tables holding the same
rows, `TimeZone=UTC`), and freezes `expected.json`. It refuses to overwrite a
receipt. A case Assay answers must be answered with the same values by both
references; a refusal records the `difference`; an `error` case must be an
error in both references.

| File | Contents |
| --- | --- |
| `events.parquet` | 367-row event log (user_id, event_ts µs, amount DECIMAL(10,2)): fractions, duplicates, NULLs, year/month ends, leap day, a negative-epoch value |
| `events_s/_ms/_ns.parquet` | The same instants in second (Parquet stores milliseconds), millisecond and nanosecond units; s/ms truncate the fraction |
| `edge.parquet` | 0001-01-01, 9999-12-31 23:59:59.999999, 1969-12-31 23:59:59.5, the epoch, leap days, month ends, NULL |
| `texts.parquet`, `texts_tab.parquet` | TIMESTAMP text for run-time casts |
| `empty_ts.parquet`, `int96.parquet` | Empty input; legacy INT96 |
| `ns_fraction`, `ms_overflow`, `far` | Refused inputs: sub-microsecond nanoseconds, millisecond overflow, a year-11476 value (stored and compared, calendar operations refuse) |
| `tz_utc`, `tz_ny` | isAdjustedToUTC=true (UTC and a pandas named zone): refused before the TIMESTAMPTZ card, now TIMESTAMPTZ |

139 cases: 92 answered (90 checked on PostgreSQL; two read time-zone-aware
Parquet, which PostgreSQL cannot), 39 refusals, 8 data errors. Five cases that
refused pending the time-zone policy are answered since the TIMESTAMPTZ card
(see `../timestamptz/README.md`). Three answered cases subtract two DOUBLE epochs; they equal DuckDB
exactly and PostgreSQL's exact NUMERIC within one microsecond
(`pg_abs_tolerance`). `tests/timestamp.rs` runs every case at workers 1/4/10 ×
batch 1/7/8192 × spill off/on (plus optimizer off), checks names, types and
values, and requires grouped TIMESTAMP keys to reach external aggregation.
It also checks unit equivalence, session tables, and the CLI over Parquet.

Regenerate (remove `expected.json` first; the Parquet bytes are deterministic):

```sh
python tools/testing/timestamp_reference.py --pg
```

## Agreement summary

| Operation | DuckDB 1.5.6 | PostgreSQL 17 | Assay |
| --- | --- | --- | --- |
| Literal / CAST text, shared grammar | value | same value | value |
| 7th fractional digit | truncates | rounds | refuse |
| Offset / `Z` / zone name in TIMESTAMP text | accepted | accepted | refuse (use TIMESTAMPTZ) |
| `BC`, compact digits, `:60`, trailing space without seconds, tab | some reject | accepts | refuse |
| Impossible field (Feb 30, hour 25) | error | error | error |
| TIMESTAMP → TEXT | `2024-03-01 10:30:00.5` | same | same |
| EXTRACT year…minute, microsecond | BIGINT | NUMERIC, same value | BIGINT |
| EXTRACT second/millisecond with a smaller fraction | truncated BIGINT | fractional NUMERIC | refuse |
| EXTRACT epoch | DOUBLE | NUMERIC (exact) | DOUBLE |
| DATE_TRUNC µs…year | TIMESTAMP | same | TIMESTAMP |
| DATE_TRUNC century/millennium, 'quarters' | 2000 / accepted | 2001 / rejected | refuse |
| ± INTERVAL 'n' HOUR/MINUTE/SECOND/DAY/MONTH/YEAR | value, month clamp | same | same |
| ± INTERVAL 'n' WEEK/MILLISECOND | adds the unit | unit word is a column alias (one second) | refuse |
| TIMESTAMP − TIMESTAMP | INTERVAL | INTERVAL | refuse (no INTERVAL value) |
| Parquet ns with sub-µs digits | TIMESTAMP_NS | — | refuse |
| Parquet isAdjustedToUTC=true | TIMESTAMPTZ | — | TIMESTAMPTZ (since the TIMESTAMPTZ card) |
