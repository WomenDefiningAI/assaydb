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
2. Make a focused change. For engine changes, use Rust 1.90.0 and a native linker:
   ```sh
   python3 scripts/with_engine_activity.py rust-tests cargo test --locked --all-targets
   python3 scripts/with_engine_activity.py clippy cargo clippy --locked --all-targets -- -D warnings
   cargo fmt --all -- --check
   ```
   Add a regression for the behavior, including relevant NULLs, empty inputs,
   duplicates, boundaries, and refusals. Documentation-only changes need link,
   example, and privacy review. Python changes need fresh installed-package
   checks following [BUILDING](bindings/python/BUILDING.md).
3. Review every proposed commit for private data, credentials, session links,
   personal contact details, and upstream licensing. Use a GitHub noreply email
   if you prefer to keep your personal email private.
4. Push to your fork and open a draft PR targeting main. Describe the change,
   exact checks and outcomes, anything not run, limitations, and attribution.

Do not change expected answers, benchmark inputs, queries, normalizers, or
denominators to make a check pass. Unsupported cases remain failures. Include
all observed failures when reporting measurements. No passing check authorizes
a package upload or a new correctness or proof claim.

On a shared host, unrelated checkouts must use the same
`ASSAY_ACTIVITY_LOCK_PATH`. Serialize builds, tests, and native measurements;
never benchmark a busy host or take independent nested locks.

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

This repository exports the Alpha 1 and Alpha 2 fast engine. Proof workspaces,
the website, and development benchmark datasets are outside this export.
[Release provenance](docs/RELEASES.md) explains the source references and limits.
