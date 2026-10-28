> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# DATE toolkit candidate

Helen's October 15 decision selects DATE results for DATE_TRUNC and whole-date
interval arithmetic, keeps the session clock refused, and defers TIMESTAMP.
The engine evaluates its own calendar kernels; DuckDB is used only to freeze
independent test answers. This is tested engine code, not a verified-core proof.

`cases.sql` contains 77 cases: leap centuries 1900/2000, month ends, destination
month clamping, negative intervals, pre-1970 and far-future dates, ISO Mondays,
NULLs, empty inputs, grouping, computed dates, invalid units and overflow.
The Rust comparison checks ordered values, duplicates, NULLs, names and types at
workers 1/4/10 × batches 1/7/8192 × optimizer off/on. Handwritten tests independently
check leap-day arithmetic, NULL overload types, clock refusals and extreme Date32
inputs with reservation cleanup.

`analysis.sql` adds monthly order revenue, weekly order counts, days since each
customer's last order (first 20 customers), and a 90-day order window on TPC-H
SF1. It also preserves the exact strict analyst q01 SQL. `analysis_expected.json`
records DuckDB answers and SHA256/size identities for all five input files.
All answers were frozen before executing this worktree. SF1 checks are separate
from the portable test suite; the test runs all five queries at workers 1/4/10.
These are correctness examples, not timing measurements or full analyst gates.

Generate with the pinned project Python and DuckDB 1.5.6:

```sh
python tools/testing/date_toolkit_reference.py --sf1 /path/to/tpch-1
```

The generator refuses to overwrite receipts; use `--output` and
`--analysis-output` for a fresh attempt. It retains reference errors and deliberate
Assay refusals. For accepted timestamp-valued reference expressions it requires
exact midnight before representing Helen's DATE result. The nested DATE example
records explicit DATE casts in `reference_sql` at those boundaries. DATE integer
results normalize DuckDB INTEGER/HUGEINT labels to Assay BIGINT; values stay exact.
Decimal scale/precision, column names and ordered rows compare exactly.

Run the optional SF1 test through the authorized card lane with
`ASSAY_DATE_TOOLKIT_SF1=/path/to/tpch-1 cargo test --offline --locked --test date_toolkit -- --ignored`.
The driver owns release, full accuracy, strict-suite and performance acceptance.

## PostgreSQL agreement (documentation, no server)

The card forbids network access and provides no PostgreSQL server. These are
documentation-based common-domain decisions, not measured PostgreSQL results.
The references are PostgreSQL's [date/time functions and operators](https://www.postgresql.org/docs/current/functions-datetime.html)
and [date/time input, including intervals](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT).

| Operation | PostgreSQL documented behavior and decision |
| --- | --- |
| DATE_TRUNC year/quarter/month/week/day | Truncates lower fields; ISO weeks begin Monday. PostgreSQL returns timestamp. Assay returns the same calendar date as DATE per Helen. Literal unit only, case insensitive. |
| DATE +/- day interval | Calendar days. Correction (Oct 17, live PostgreSQL 17.11): `INTERVAL '1' WEEK` is not a WEEK interval in PostgreSQL; it parses as `INTERVAL '1'` (one second) with column alias `week` (`DATE '2024-01-03' + INTERVAL '1' WEEK` returns `week` = 2024-01-03 00:00:01; with `AS x` it is a syntax error). DuckDB adds 7 days, so WEEK intervals now refuse (`interval_10`, `interval_11`). |
| DATE +/- month/year interval | Preserve the day if valid, otherwise use the destination month's final day; twelve months per year. Both references return timestamps at midnight; Assay returns DATE. |
| DATE +/- integer; integer + DATE | Whole-day offsets with signed 32-bit INTEGER values, same calendar value. Assay's wider INTEGER representation is range-checked before use. |
| DATE - DATE | Signed number of days; Assay returns BIGINT, PostgreSQL integer and DuckDB BIGINT. |
| DATE_PART | Existing literal calendar fields YEAR/QUARTER/MONTH/DAY/DOW/ISODOW/DOY/WEEK/ISOYEAR remain. Values agree; PostgreSQL returns double precision, Assay keeps the earlier approved BIGINT result. No EPOCH/time expansion. |
| YEAR(), MONTH(), DAY(), DATE_DIFF(), DATEDIFF() | PostgreSQL does not supply these built-ins. They remain unsupported. |
| INTERVAL n DAY/MONTH/etc. | DuckDB accepts unquoted counts; PostgreSQL interval input requires a quoted literal. Refused. |
| CURRENT_DATE, NOW(), CURRENT_TIMESTAMP | Old → new (Oct 17 clock policy): refused → the statement's start instant in the session zone (default UTC). The fixture keeps deterministic `IS NOT NULL` cases; fixed-clock values are checked in `tests/timestamptz.rs`. |

Only quoted whole signed counts with DAY/WEEK/MONTH/YEAR qualifiers are accepted
as the right operand of DATE +/- INTERVAL. Compound, fractional, dynamic and
time units remain refused, as do standalone/stored INTERVAL and TIMESTAMP types.
Bare NULL in DATE_TRUNC is refused because the reference overload is ambiguous;
typed DATE NULL propagates. DATE +/- bare NULL selects a nullable day offset;
typed DATE NULL minus DATE selects the day difference. The bare-NULL overload
choice follows DuckDB; PostgreSQL can resolve DATE - unknown as DATE - DATE.
Both return NULL, but their inferred result types need not agree. Use typed
NULLs when the result type matters.

New toolkit operations use the finite AD portion of chrono's representable
calendar (including dates beyond year 9999 when already represented as Date32).
BC/infinite/out-of-range dates, offsets outside signed 32-bit INTEGER, and interval
components outside signed 32-bit days/months refuse with `unsupported:`. Extreme
reference overflows are retained as errors, not treated as matching error text:
engine wording differs. This conservative range is explicit, not a claim to
support every PostgreSQL or DuckDB DATE. Existing date comparisons/EXTRACT are
unchanged.

## Receipt preservation and transitions

The original date-fields and missing-feature-label corpora/receipts are unchanged.
Their Rust tests explicitly promote only d059 (DATE_TRUNC month) and the four
date arithmetic label records from refusal to their existing reference values.
Every other assertion remains. The old `unsupported_forms_remain_explicit` list
loses only DATE_TRUNC month, now checked as a success across its complete matrix.

Generator setup failures occurred before Assay execution: bare NULL DATE_TRUNC
was ambiguous; nested timestamp + integer required the recorded reference DATE
casts; serializing a refused standalone interval required timedelta handling.
The partial JSON from that serialization attempt is not a test input and is not
carried into the integrated tree; it remains in the date-toolkit PR history.
No reference case was discarded and no Assay result supplied an expected value.

The first native attempt stopped in rustfmt at an unclosed delimiter in the new
kernel; it was corrected before compilation. Its diagnostic is retained in
`[retained private validation log]`. Subsequent attempts use numbered logs in
that directory; no failed run is rescored or removed.

The resumed attempt restored all 17 files from the driver's saved `failed.patch`.
The failing handwritten check expected a query-style `unsupported:` prefix in
the stored-INTERVAL DDL error's message. DDL already returned kind `unsupported`
and message `column type: INTERVAL`. Old → new assertion: message-prefix check
→ exact kind and message checks. No engine behavior or refusal was changed.
The reproduced failure is retained in
`[retained private validation log]`.

Resumed validation reproduced both DuckDB receipts exactly and checked all five
SF1 input hashes/sizes. The SF1 Rust test passed through `card_dev_check.py`:
all five queries at workers 1/4/10 (15 exact comparisons), including strict
analyst q01, in 22.40 seconds after compilation. Evidence is retained in
`[retained private validation log]`; the recreated reference receipts are
`[retained private validation log]` and
`[retained private validation log]`. The original receipts remain
byte-identical, including the earlier failed generator artifact.

`cargo fmt`, all-target offline/locked Clippy with `-D warnings`, and the full
offline/locked debug all-target test suite passed through `card_dev_check.py`.
This includes the corrected handwritten check and all 77 cases across the 18
worker/batch/optimizer configurations (1,386 corpus executions). Retained logs:
`[retained private validation log]` and
`[retained private validation log]`. Existing vendored Parquet warnings
remain; no vendor source was changed. The final scope/proof audit confirms exactly
the 17 allowed files, unchanged frozen bytes, and no added forbidden constructs.
Driver release/accuracy/full strict-suite acceptance remains pending.
