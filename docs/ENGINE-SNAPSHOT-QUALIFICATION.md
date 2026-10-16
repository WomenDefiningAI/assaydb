# Engine snapshot qualification

## Measured speed of this exported build

Fresh SF10 measurements on Test Mini (`Mac.localdomain`), October 16, 2026,
00:03:45–01:53:28 UTC (October 15 Pacific), compare exported candidate
`8bea917ed73be0aadcf36ef6538377e58171e2a1` with measured public-main baseline
`d86aa0dd769041c23d64bfc760f7b31cc07f3af7`. Lower ratios mean less time.

| Workers | Candidate / measured main, total | Candidate / Polars, total | Candidate / Polars, geometric mean |
| --- | --- | --- | --- |
| 10 | 0.909973 | 0.957067 | 1.033839 |
| 1 | 0.904884 | 0.869895 | 0.890102 |

On this workload, the ten-worker total took about **9.00% less time than measured
main** and **4.29% less time than Polars**, while its geometric mean was
**3.38% above Polars**. This does not establish the 0.95 target or an Alpha3 release.
All 22 queries, three rotated orders, one warmup and five timed repetitions were
retained. Host quietness was sampled; a connection interruption caused no restart,
but zero timing impact cannot be established. Sequential-role bootstrap intervals
do not eliminate temporal drift. Engine-accounted resources are not hard process
caps. Detailed intervals, slower cases and audit limits remain below.

Current public main is now `c03d4bf901ed2c3f4b38a2b3d61fd767ab9d04e7` after
site PR #7. Read-only comparison of complete Git inventories found 18 changed
website, documentation, website-test/rendering and testing-display files. Engine
source, dependencies, toolchain and native build inputs are unchanged. The measured
baseline remains `d86aa0dd769041c23d64bfc760f7b31cc07f3af7`; no run of the new
main is claimed, and this comparison is not merge acceptance.

**Acceptance remains unresolved.** Zero wrong answers and 99.9555% applicable
passes meet the historical internal SLT launch threshold. That threshold does not
establish an approved public engine-PR policy. The public zero-error policy and
infrastructure-only no-regression policy are not broadened here. Maintainer/owner
decision on engine acceptance is still required; completed measurements do not
clear this PR for merge.

This is a source promotion for review, not an Alpha3 release or package upload.
The immutable source is `76e816727c249c285223635f7b7f75cf7154c901`, source tree
`22abe151f6b6639976fcf437882c5ca447ca016f`. The public branch starts at
`d86aa0dd769041c23d64bfc760f7b31cc07f3af7` and retains the sanitized Alpha 1/2
history, website and PR #5 accepted evidence. No private Git history is imported.

## Scope and export fidelity

This snapshot adds exact DECIMAL expression casts, ROW_NUMBER/RANK/DENSE_RANK,
compact group/build kernels and two local Parquet decoder changes. It includes
regression tests and tiny original ranking/composition fixtures. Engine Rust and
regression SQL/answers are copied unchanged. The two modified vendored Rust files
receive attribution comments; Cargo comments and documentation are sanitized.
The reference-generator adaptation changes only repository-root and portable
activity-lock admission. No frozen query, expected answer or denominator changes.

`ENGINE-EXPORT-MANIFEST.json` inventories the proposed public delta and source
hashes. The earlier `EXPORT-MANIFEST.json` remains historical provenance.
Optional unreferenced compressed exact-integer corpus and private robustness,
release, verification and orchestration machinery are excluded. Raw datasets,
TPC-derived SQL, private reports and session links are not published. Upstream
Parquet LICENSE and NOTICE are retained unchanged. Vendored crate archive SHA-256:
`7a0f31027ef1af7549f7cec603a9a21dce706d3f8d7c2060a68f43c1773be95a`.

## Retained frozen-source campaign

These are measurements of the source identified above, not fresh runs of the
public export. The historical comparison baseline is
`bc576ddf610d665056c6a340f617eb0990979218`, not the current public main executable.
The trusted controller and its receipt are pinned in the retained private evidence.
All work ran on the same test host with shared activity locking. No PGO was used.

| Check | Frozen-source result |
| --- | --- |
| Standard checks | 871 Rust tests, 113 controller tests and 5 Python staging tests passed; formatting and Clippy passed |
| Acceptance-small | 43 tests per role; both reports verified |
| Full SLT | 7,421,339 records; 5,939,714 applicable: 5,937,073 passed, 640 unsupported, 2,001 engine errors; 1,481,625 controls; zero wrong/not-run or status regressions |
| Frozen 55K | 55,000 queries / 60,840 raw records: 46,980 passed, 6,975 unsupported, 1,020 native errors, 25 reference errors; zero wrong/not-run or semantic regressions |
| Retail | 64/64 passed: original, exact-product variants and edge controls against frozen independent answers |
| Decoder controls | 303 error records, eight FLBA controls and 82 coverage records per role matched |
| DECIMAL controls | 701 casts: 614 values and 87 expected errors; 12 composition cases: nine values and three expected errors |
| Ranking reference regeneration | Fresh 80-case / 60 SQLite cross-check regeneration matched frozen answers, finite-zero and NaN-policy records; approved upstream 3.51.0 variation recorded separately |

The 60 SLT and 354 differential output differences were investigated using full
typed results and resolved as unordered row-order differences, not semantic
changes. Existing unsupported/error outcomes remain failures for strict
compatibility. PR #5's infrastructure acceptance does not automatically accept
this engine change or authorize a different acceptance policy.

The retained timing campaign used three rotated engine orders, 22 queries,
one warmup plus five timed repetitions, workers 1 and 10, and both roles:
4,752 samples total. Assay/Polars total ratios were 0.86721250 (one worker) and
0.95978520 (ten workers); geometric ratios were 0.88834742 and 1.03648370.
These are diagnostic frozen-source results. They are not a fresh public-main
baseline, a measurement of this exported build or proof of the 0.95 release goal.
Interrupted attempts and intact recovery phases remain in private evidence.
Quiet-host admission was sampled; connection interruptions cannot be claimed to
have had zero timing impact. These observations limit the timing interpretation.

## Exported-tree checks and remaining acceptance

Fresh exported-tree checks ran against `7f5b2d2ae67a0f77873afa95e289d58d96c0a714`
(tree `99cc174c875a316e12c4a791fe07c45e713ee614`). [Sanitized evidence](evidence/engine-snapshot-checks.json)
records 871 Rust tests, 113 controller tests, five staging tests, 85 public
mechanics tests and 13 contributor tests passing, plus formatting and Clippy.
Acceptance-small passed 43 tests for each of the exact public base and candidate,
with live and preserved-copy report verification. All 64 frozen retail checks
passed against the fresh exported binary. No Rust tests were ignored. Subsequent
changes are documentation/evidence only. Passing the decoder recorder tests is not
a fresh upstream parity comparison; decoder parity remains attributed to the
retained frozen-source campaign.

The local checks above are complete. This draft remains unmerged for maintainer
review. Completed follow-ups and remaining review decisions are distinguished below;
the separate Alpha3 performance target is not claimed by this source promotion.

### Fresh comparison with current public main

Separately built public base `d86aa0dd769041c23d64bfc760f7b31cc07f3af7` and
candidate `8bea917ed73be0aadcf36ef6538377e58171e2a1` ran with the same pinned
inputs and tools on the test host under the exclusive activity lock. These are
fresh exported-build results, separate from the historical source campaign above.

| Check | Fresh paired result |
| --- | --- |
| Full SLT | 7,421,339 records across 622 files compared; zero differences in status or retained evidence, excluding elapsed validation time |
| Full 55K | 55,000 queries and 60,840 raw records per role; zero differences in outcomes, native/reference payloads, setup replies or metamorphic results |
| Retail | Candidate 64/64 passed; main 36 passed and 28 unsupported. All 28 unsupported cases became passes; no regressions |
| SF10 capacity/correctness | 66/66 capacity cases per role; four prerequisite correctness phases passed 44/44 each; raw output and resource evidence audited |
| SF10 timing | All 22 queries, both roles, workers 1 and 10: 3,960 timed samples plus 792 warmups; one-core 2% regression screen passed |

Each SLT role has 5,937,073 passes out of 7,421,339 fixed records (80.0000%;
80.04% non-SQLite maximum), then 5,937,073/5,939,714 applicable (99.9555%).
The 640 unsupported and 2,001 engine errors remain failures. Zero wrong, timeout
or not-run outcomes were recorded. This meets the historical internal SLT launch threshold; applicability as a
public engine-PR acceptance policy remains unresolved.

Each 55K role retains 46,980 passes, 6,975 unsupported cases, 1,020 native errors
and 25 reference errors. Zero wrong, timeout or not-run outcomes were recorded.
The paired comparisons do not turn these failures into passes. Retail used
unchanged frozen independent answers; no fresh reference-engine retail run is
claimed. Sanitized source/binary identities and comparison receipt hashes are
in the linked evidence JSON. Final-head documentation changes require review.

### Fresh SF10 measurements and limits

The complete timing run used three rotated engine orders, one warmup and five
measured repetitions per query/engine/round, with 10,000 bootstrap draws. All 22
queries are included. Ratios below are candidate/reference; lower is faster.

| Workers | Reference | Total ratio | 95% interval | Geometric ratio |
| --- | --- | --- | --- | --- |
| 1 | Current public main | 0.904884 | 0.904016–0.906413 | 0.918253 |
| 10 | Current public main | 0.909973 | 0.906268–0.913181 | 0.923451 |
| 1 | Polars | 0.869895 | 0.864793–0.870605 | 0.890102 |
| 10 | Polars | 0.957067 | 0.927656–0.965075 | 1.033839 |
| 1 | DuckDB | 0.655962 | 0.655180–0.656706 | 0.654032 |
| 10 | DuckDB | 0.718009 | 0.696466–0.722211 | 0.760285 |

The existing one-core 2% regression screen against public main passed. At ten
workers, q16 has a slower point estimate of 1.008164 (95% interval
0.955933–1.038697); this case remains in the complete denominator. The ten-worker
Polars total is above 0.95 and its geometric ratio is above 1. The 0.95 goal is
not established. SF10 does not establish the older formal SF1 parity claim.

Fresh correctness prepasses supplied 176 JSON comparisons and 88 native Arrow
comparisons across the timing phases. The audit checked 4,892 artifact hashes;
all phases finished with successful provenance checks, process cleanup and lock
release. Timed samples retain row counts and resource evidence, not every timed
output payload. Raw dataset generation was not reverified; staged file hashes
were verified. Host quietness was sampled. A connection interruption did not
restart the run, but zero timing impact cannot be established. Bootstrap intervals
pair matching rotations from sequential role phases and do not eliminate temporal
drift. These results retain the engine-accounting limits below.

### Other completed checks and remaining review

- The fresh source-bound corrected capacity run passed 66/66 per role and
  independent raw JSON/Arrow/settings/provenance/cleanup review. It measured the
  frozen source build, not the exported binary. The original candidate 0/66
  result remains preserved. The corrected thread contract includes the
  coordinator, bounded by configured workers; memory/spill checks are unchanged.
  These passes do not close decoder native-memory accounting.
- SQLite reference provenance is resolved for this run by explicit approval of
  the official upstream 3.51.0 variation. Source/compiler/binary hashes are retained.
  All 80 cases, 60 SQLite cross-checks and complete boundary records matched; only
  the disclosed SQLite version metadata differed. Frozen answers were not replaced.
- Independent review verified the engine/build export boundary: native engine
  files and build configuration match the frozen source, apart from the documented
  attribution comments. Retained full-corpus evidence is historical source-equivalent
  evidence, not a fresh run of this binary or a full comparison against current public
  main. Fresh current-public-main SLT, 55K and retail comparisons are now complete
  as reported above, including fresh SF10 capacity, correctness and paired timing.
  Existing evidence may be reused only after exact code, tooling, input and
  same-day/host equivalence is verified.
- The historical internal SLT numeric launch threshold is met. Public engine
  acceptance remains unresolved: no approved extension of the infrastructure-only
  no-regression policy is established. Owner/maintainer decision is required;
  strict zero-error compatibility is not claimed and no policy is changed.

Decoder native allocations, retained Zstandard context and conversion/output
buffers are not fully covered by engine reservation accounting. The decoder's
128 MiB window admission is not a hard process-memory limit. D04 remains open;
no universal decoder resource parity, hard RSS cap or performance-qualified
Alpha3 claim follows from this promotion.

The website automatically deploys committed content after merge. Its existing
accepted PR #5 measurements remain unchanged by this proposed source promotion.
New measurements require their own review and acceptance before replacing them.
