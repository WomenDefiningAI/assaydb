# Engine snapshot qualification

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
| Ranking reference regeneration | Not run: exact SQLite prerequisite unresolved; native tests used frozen answers |

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

Exact exported-tree checks are pending at this implementation checkpoint.
A subsequent evidence-only commit will record their source identity and outcomes.
Complete qualification is not claimed and this PR must remain unmerged until
maintainer review resolves the following boundaries:

- Run and review the source-bound corrected capacity checks; the original
  candidate 0/66 result is preserved. The corrected thread contract includes
  the coordinator, bounded by configured workers. It does not relax memory or
  spill checks or convert old failed receipts into new passes.
- Authenticate the SQLite 3.51 executable used for fresh ranking reference checks.
  Frozen answers record an Apple 3.51.0 build without an executable acquisition
  hash. An upstream build requires an explicit, documented reference-provenance
  decision; 3.54 is not an acceptable silent substitution.
- Review exact exported build/source equivalence before reusing any campaign
  evidence. Changes to executable behavior invalidate affected evidence. A fresh
  public-main paired benchmark has not been run.
- Decide whether the retained stable engine/reference errors satisfy the chosen
  engine-PR acceptance policy. Strict zero-error compatibility is not achieved.

Decoder native allocations, retained Zstandard context and conversion/output
buffers are not fully covered by engine reservation accounting. The decoder's
128 MiB window admission is not a hard process-memory limit. D04 remains open;
no universal decoder resource parity, hard RSS cap or performance-qualified
Alpha3 claim follows from this promotion.

The website automatically deploys committed content after merge. Its existing
accepted PR #5 measurements remain unchanged by this proposed source promotion.
New measurements require their own review and acceptance before replacing them.
