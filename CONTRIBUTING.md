# Contributing to Assay

Start with [SQL coverage](docs/SQL-COVERAGE.md), [semantics](SEMANTICS-LOG.md),
[limitations](docs/WHAT-ASSAY-ISNT.md), and the [code of conduct](CODE_OF_CONDUCT.md).
Choose a small change and explain the behavior it fixes. Discuss new SQL semantics
or expensive benchmark work with maintainers before implementation.

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
   checks following [BUILDING](bindings/python/BUILDING.md).
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
[the full-suite setup and current blockers](testing/README.md).

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

## Wrong answers and security

Use the [wrong-answer template](.github/ISSUE_TEMPLATE/wrong-answer.md). Include
the package version or commit, platform, minimal redistributable data and SQL,
actual result, expected result and its derivation, and resource/worker options.
Do not attach private datasets, credentials, or raw session transcripts.
Use [SECURITY](SECURITY.md) for sensitive reports.

## Licensing and credit

Original contributions use MIT OR Apache-2.0. Preserve upstream copyright,
license, and attribution notices; do not copy incompatible material. In your PR,
request the GitHub handle or display name you want credited and describe the
contribution. Maintainers review credit alongside the change.

This repository preserves the Alpha 1 and Alpha 2 exports and subsequent reviewed
fast-engine source snapshots. Proof workspaces
and development benchmark datasets are outside this export. The static website
is maintained separately under `site/`; see [website maintenance](docs/WEBSITE.md).
[Release provenance](docs/RELEASES.md) explains the source references and limits.
