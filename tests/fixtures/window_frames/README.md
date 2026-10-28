> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Window frames and common analysis functions

`cases.sql` contains 80 fixed queries. `expected.json` was generated with installed
DuckDB 1.5.6, one reference thread, before any Assay check. No Assay output feeds
the generator. The generator creates outputs exclusively and records SQL hashes.
The Rust test also checks every query against the corresponding reference SQL.
The October 16 tie decision adds `ties_reference.json` without changing any of
those SQL statements or DuckDB answers. The generator exhaustively enumerates
the Cartesian product of peer permutations and evaluates each frame in an
independent Python model. Fifteen of the nineteen modeled value-function cases
are labeled **order-dependent per SQL (ties)**; the other four retain exact
DuckDB matching. Each whole answer uses one consistent ordering of each peer
group. The test requires both membership in the complete permitted-answer set
and exact equality to Assay's independently computed canonical answer: UNION ALL
branch order (ascending id here), then stable window partition/key ordering.
DuckDB's frozen and freshly observed answers are both recorded and checked for
membership. No Assay result is used to construct either expected answer.

`handwritten_ties.json` (`window_frames_reference.py --handwritten`, DuckDB 1.5.6)
covers four handwritten Rust controls whose answers depend on peer order:
LAST_VALUE/NTH_VALUE over the k=1 peers, the AVG(f) OVER() Float fold (0.25
canonically; 0 or 0.5 in other peer orders), and NTILE(2)/FIRST_VALUE over all
twelve `wr` rows (compared as multisets, since those queries have no final ORDER
BY). The independent model enumerates every peer permutation (or every distinct
choice of leading rows), records the complete permitted set and the canonical
answer, and checks DuckDB's recorded answer for membership. The Rust tests
require membership and exact canonical equality.

`siblings.sql` adds three deterministic cases, wf081–wf083, with the full `k,id`
tiebreaker for peer-frame FIRST_VALUE, default-frame LAST_VALUE and moving-ROWS
NTH_VALUE respectively. Their separate `siblings_expected.json` uses DuckDB
1.5.6 and the same strict comparison. The original 80 cases remain unchanged.
Reproduce the additive references with `window_frames_reference.py --ties` and
`--siblings`, each with `--output` pointing to a fresh evidence directory.

Integers, exact decimals, dates, strings, booleans, NULL positions, multiplicities,
and final ordering compare exactly. Reference integer widths are normalized.
Float comparison uses the already approved absolute/relative 1e-12 policy;
separate handwritten controls require exact Assay bits across every configuration.

The matrix is workers 1/4/10 × batches 1/7/8192 × spill disabled/enabled (512-byte
sort runs), including peers, NULL keys/values/partitions, empty input, frames
larger than partitions, zero offsets, descending keys with explicit NULL placement,
Date ranges across January/February and leap-day/March, and grouped QUALIFY.
Case wf080 retains DuckDB's widened integer result, and requires Assay's existing
BIGINT public-output overflow. It does not reinterpret overflow as a successful
reference match. Handwritten precision-38 controls require frame completion errors
even when QUALIFY/LIMIT would hide a row. Cancellation and reservation cleanup
also exercise the new frame path in the existing internal window tests.
The Float overflow control retains the exact scientific-literal binder refusal
and adds typed DOUBLE table input that actually reaches summation. It asserts the
complete existing `unsupported: Float SUM/AVG finite-prefix overflow
(DuckDB/PostgreSQL disagreement)` diagnostic, including the error class.
The unchanged wf070 and wf074 queries also regress debug stack failures in
QUALIFY alias expansion and grouped QUALIFY over UNION ALL. SELECT windows are
reused by QUALIFY, and projection/aggregation execution has a separate handler
so its workspace is not placed on every recursive operator's stack frame.

`analysis.sql` has four realistic queries on the unchanged TPC-H SF1 data:

- Daily discounted shipment revenue and its seven-calendar-day moving average
  (`RANGE INTERVAL '6' DAY PRECEDING`, including the current day).
- Daily running share of total order revenue within each customer segment.
- The three highest-revenue customers in each segment using QUALIFY.
- First and last order per customer, with an explicit full frame for LAST_VALUE.

All four DuckDB answers were frozen before Assay checks. The compact
`sf1_reference_manifest.json` records input identities, SQL, types, counts,
complete-answer hashes, and boundary rows. Complete answers remain in the card's
private scratch evidence. SF1 Assay execution belongs to the driver; the ignored
`sf1_analysis_against_frozen_reference` test consumes those answers and never
generates them. To regenerate references into a fresh directory, use the approved
project Python and `tools/testing/window_frames_reference.py --sf1 /path/to/tpch-1
--output /path/to/fresh/evidence`. The driver supplies `WINDOW_FRAMES_SF1` and
`WINDOW_FRAMES_REFERENCE` under its shared lock. No benchmark SQL is changed.

PostgreSQL 17.11's installed `sql-expressions.html` and `functions-window.html`
establish the common frame, peer, positive-n, and RESPECT NULLS behavior. No
PostgreSQL server execution is claimed. Date offsets use literal whole-day
intervals, since PostgreSQL requires datetime offsets to be intervals. Numeric
DATE offsets, month/time intervals, and RANGE boundary arithmetic outside the
common Int64/timestamp domain are explicitly refused. QUALIFY deliberately
follows DuckDB; PostgreSQL has no such clause.

Old → new assertions: Float SUM/AVG OVER, bounded/following aggregate frames,
FIRST_VALUE/LAST_VALUE/NTH_VALUE/NTILE and QUALIFY move from explicit refusal to
independently checked successful results. The prior ranking, aggregate and offset
test suites retain their other refusals. The float_aggregate suite uses its
already-frozen DuckDB answer for its old window-refusal case; its historical
fixture remains untouched. Float MIN/MAX, EXCLUDE, GROUPS, IGNORE
NULLS, nonliteral offsets, offset RANGE over other key types, and named windows
remain refused. No frozen corpus or proof files change.

October 16 resume validation: formatting, all-target Clippy with warnings denied,
and all-target debug tests passed through the card's assigned build lane. The
83-case matrix passed all 18 configurations, as did the handwritten overflow
and canonical Float-bit controls and the existing window-ranking suite. Earlier
failed attempts remain retained. The SF1 test stays ignored in debug runs;
SF1 execution, release, exact-accuracy and strict gates belong to the driver.
