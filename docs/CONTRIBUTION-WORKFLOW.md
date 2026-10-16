# Contribution checks and maintainer workflow

## Fork, check, and open a draft PR

1. Fork `WomenDefiningAI/assaydb`, clone your fork, and branch from upstream main:
   ```sh
   git clone https://github.com/YOUR_GITHUB_LOGIN/assaydb.git
   cd assaydb
   git remote add upstream https://github.com/WomenDefiningAI/assaydb.git
   git fetch upstream main
   git switch -c contribution/short-description upstream/main
   ```
   Replace YOUR_GITHUB_LOGIN with your login.
2. Make a focused change and open a draft PR early to agree on scope. For engine
   changes, use Rust 1.90.0 and a native linker:
   ```sh
   python3 scripts/with_engine_activity.py rust-tests cargo test --locked --all-targets
   python3 scripts/with_engine_activity.py clippy cargo clippy --locked --all-targets -- -D warnings
   cargo fmt --all -- --check
   ```
   Add a regression for the behavior, including relevant NULLs, empty inputs,
   duplicates, boundaries, and refusals. Documentation changes also need link, example, and privacy review; these
   local checks do not exempt a PR from the full independent merge gate. Python changes need fresh installed-package
   checks following [BUILDING](../bindings/python/BUILDING.md).
3. Review every proposed commit for private data, credentials, session links,
   personal contact details, and upstream licensing. Use a GitHub noreply email
   if you prefer to keep your personal email private.
4. Push updates to your fork and keep the draft PR targeting main. Describe the change,
   exact checks and outcomes, anything not run, limitations, and attribution.

Do not change expected answers, benchmark inputs, queries, normalizers, or
denominators to make a check pass. Unsupported cases remain failures. Include
all observed failures when reporting measurements. No passing check authorizes
a package upload or a new correctness or proof claim.

On a shared host, unrelated checkouts must use the same
`ASSAY_ACTIVITY_LOCK_PATH`. Serialize builds, tests, and native measurements;
never benchmark a busy host or take independent nested locks.

## Maintainer verification and current availability

Every PR considered for merge requires independent Test Mini full SLT, frozen
55K differential, SF10 correctness and timing, real-world original and variant
analysis, standard checks and acceptance-small evidence. Missing inputs or runs
block merge. There is no automatic documentation-only exemption. See
[the full-suite setup and current blockers](../testing/README.md).

Open a focused draft PR, collect evidence against the exact candidate and base,
and attach or link all results for maintainer review. Keep passed, unsupported,
wrong, error, excluded and not-run outcomes distinct. Use fresh same-host baseline
and candidate measurements with identical pinned tools and inputs. Changes that
affect execution invalidate prior results. Evidence-only changes require explicit
final-head delta review; never relabel an old result as a new run.

Public automation does not yet enforce this complete gate. The result-contract
prototype performs structural validation; trusted collector integration and raw
measurement review remain required. GitHub checks and merged-PR attribution alone
are not full-suite qualification. PR #4 was merged without full SLT, 55K or SF10
runs and remains unqualified under this requirement.

For performance work, start with a small slow-query reproduction using data you
can redistribute. Agree on the workload, baseline and timing method before an
expensive comparison. Development benchmark datasets and proof workspaces are
not included in this export; future formal-proof or harness work needs separately
agreed scope and available sources.

Merged-PR credit is not correctness certification or benchmark acceptance.
Charts may change only from reviewed, accepted measurements; existing historical
values remain until such evidence is available. Publishing a Python or Rust
package is a separate release action, not an automatic consequence of merging.


## Include reviewed metrics in the original PR

Do not plan a separate metrics PR after merging an engine contribution. PR8 is a
one-off repair for PR6. Before merging a qualifying engine, binding, test or build
change, include the reviewed record in `site/evidence/contribution-metrics.json`
in that same PR. The contributor feed renders it only after GitHub reports the
PR merged. Raw ratios retain full precision; displayed speed uses two decimals.

1. Freeze and independently test the candidate against the current public base.
   Obtain maintainer review of accuracy, paired SF10 timing and all limitations.
   The supported speed method is currently 22 SF10 queries, ten workers, three
   rotations, one warmup and five timed repetitions. Agree a schema extension
   before merging a contribution that needs another method; do not invent values.
2. Commit sanitized evidence under `docs/evidence/` or `site/evidence/` in the PR.
   Its JSON contains `tested_source`, `baseline_source`, `accuracy` and `speed`.
   Accuracy and measured speed fields match the display schema exactly, except
   the evidence's speed omits `final_engine_equivalent_head`: that identity does
   not exist until this evidence commit has been made.
3. Have a maintainer audit the delta from the tested candidate to this evidence
   commit. Record this existing commit as `review.audited_head`. Only reviewed
   inert documentation or static website files may differ; engine, build, inputs,
   harness and execution changes require a fresh run. The validator rejects
   changes outside its narrow presentation-file allowance. Review is still needed
   to determine whether an allowed file actually affects test inputs or execution.
4. Append the display record in the same PR with `status: accepted`,
   `evidence_commit: merged-pr`, the relative `evidence_path`, original
   `measured_source`, and accepted accuracy/speed. Set speed's
   `final_engine_equivalent_head` to the existing audited head, not a future final
   commit. Include `review` with `decision: accepted`, public `reviewer` handle,
   `audited_head`, SHA-256 of exact evidence bytes as `evidence_sha256`, and
   `audited_delta`: a Git-sorted list of `{path, entry}` for every changed file
   between tested source and audited head. Each `entry` is the trimmed output of
   `git ls-tree AUDITED_HEAD -- PATH`, including mode and blob ID (empty on deletion).
5. After audit, only this PR's display record may change. Other changes require
   repeating the audit; execution-affecting changes require new testing. A base
   change requires a fresh matching baseline and review. Run:
   ```sh
   node scripts/validate-contribution-metrics.cjs BASE_SHA HEAD_SHA PR_NUMBER
   ```
   The `Contribution metrics / reviewed-metrics` pull-request check performs the
   same structural validation using code from the trusted base, never executing
   candidate scripts. It needs no private inputs, secrets or engine execution.

`merged-pr` is resolved to GitHub's merge commit only when the merged row renders.
The record therefore does not need its own final commit SHA or a follow-up PR.
The evidence digest and audited source identities bind the reviewed contents;
adding the display record does not claim a new execution of the final head.

The check fails missing/invalid metrics and stale source bindings for qualifying
changes. It does not establish that a claimed reviewer actually accepted the
result, replace independent raw-evidence review, or enforce the full testing gate.
Maintainers must verify acceptance and require the check in repository rules.
This change does not modify branch protection. Website-only PRs remain subject
to the existing human merge policy but do not invent engine metrics. PR8 has a
narrow bootstrap exception because the trusted base does not yet contain this
validator; subsequent PRs run the validator from their base.

The premerge classifier is broader than contributor attribution: engine/build
files, all non-Markdown testing inputs and tests, controller/harness/collector
code, general execution scripts/tools, binding inputs and engine CI workflows
require a metrics record. New collector filenames are covered automatically;
removals and renames are checked too. Explicit presentation-only exceptions cover
`site/`, `scripts/website-tests/`, the speed-chart renderer, the site-evidence
exporter and its tests, and the metrics validator/workflow itself. Ordinary
Markdown docs are exempt; `docs/BENCHMARK-INPUTS.md` remains an execution contract.
These exceptions affect only this structural metrics check, not human merge policy.
