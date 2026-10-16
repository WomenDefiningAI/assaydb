# Contributing to Assay

We need help with **speed** and **SQL coverage**. Verification coverage comes
after Alpha 3. Start small: explain one slow query or a SQL behavior that needs
better support, using data you can share.

[Discuss scope and evidence with maintainers in a GitHub issue first](https://github.com/WomenDefiningAI/assaydb/issues/new).
Then open a focused draft PR from public `main`. The repository has a
[wrong-answer template](https://github.com/WomenDefiningAI/assaydb/issues/new?template=wrong-answer.md)
for incorrect results; use a plain issue to propose other work.

Read [SQL coverage](docs/SQL-COVERAGE.md), [semantics](SEMANTICS-LOG.md),
[limitations](docs/WHAT-ASSAY-ISNT.md), and the [code of conduct](CODE_OF_CONDUCT.md).
Follow the [technical contribution workflow](docs/CONTRIBUTION-WORKFLOW.md)
for fork/branch commands, regression checks, shared-host coordination, privacy,
and exact-source evidence review. Keep failures and unsupported cases visible;
never change expected answers, inputs or denominators to improve a result.

## Contribution validation for each PR: our process

Independent validation on Test Mini includes:

- **SLT (SQLLogicTest):** known SQL problems and expected answers from
  [SQLite’s suite](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).
- **hunt55K:** 55,000 differential queries compare results with independent
  references. See our [public generators](testing/hunt55k) and
  [frozen-input requirements](testing/README.md#sources).
- **SF10 correctness:** check the 22-query analytical workload on roughly 10 GB,
  including worker and resource cases. See [TPC-H background](https://www.tpc.org/tpch/)
  and our [benchmark input limits](docs/BENCHMARK-INPUTS.md).
- **SF10 timing:** repeated same-host comparisons with pinned inputs, tools and
  settings; retain every outcome and distinguish totals from geometric means.
  These are research measurements, not audited TPC-H results.
- **Retail real-world tests:** original analysis, product variants and decimal
  edge cases on real and synthetic fixtures. See the
  [UCI Online Retail source](https://doi.org/10.24432/C5BW33) and our
  [public SQL and documentation](testing/analysis/README.md).

Standard checks and acceptance-small complement those workloads. Every PR
considered for merge needs independent full-suite evidence; missing evidence
blocks merge, with no automatic website-only exemption. Maintainers review and
accept the exact-source results. GitHub checks alone do not establish this gate.
The [full testing workflow](testing/README.md) retains the technical requirements,
input availability, no-regression rules and release limits.

PR5 has [reviewed infrastructure evidence](site/evidence/full-suite/latest.json),
not an Alpha 3 release qualification. PR6 has scoped owner acceptance and is merged; its measured-source
[qualification and limits](docs/ENGINE-SNAPSHOT-QUALIFICATION.md) remain explicit. The Alpha 3 goal is 0.95× Polars for both total time and geometric
mean on the agreed 10 GB workload; it has not been achieved by the published
reviewed evidence. Merge, chart acceptance and package release are separate decisions.

## Credit and contribution metrics

The website shows merged engine and meaningful testing PRs in a compact table
with author, linked title/number, merge date, SQL pass percentage and paired speed
change. Open Details for counts, failures, source identities, evidence and limits.
Website-only PRs are excluded. Credit is not certification.

Accuracy is the percentage of applicable SQL tests passed, alongside passed/total
counts and an expandable wrong/unsupported/error/not-run breakdown. The suite and
measured source stay explicit; exclusions are outside the applicable denominator.
Speed is paired suite wall-time percentage change against tested public main on
the same host and configuration: a negative change means less time (faster), a
positive change means more time (slower). No reviewed evidence means **Not measured**.

PR5 has reviewed SLT accuracy evidence. Its timing compares a historical source,
not the tested public main baseline required by this display metric, so its speed
field remains Not measured. A ratio to Polars is not a PR improvement against main.
Future qualifying contributions must include reviewed metrics and source binding
**in the original PR before merge**, not in a follow-up PR. The
[premerge metrics workflow](docs/CONTRIBUTION-WORKFLOW.md#include-reviewed-metrics-in-the-original-pr)
explains the required record, audited nonengine delta and structural CI check.
The feed displays the record after merge; a passing check does not replace human acceptance.

The [result contract](docs/CONTRIBUTION-RESULTS.md) preserves evidence requirements;
these display definitions do not change acceptance policy.

## Documentation and website review for every merge

Review current SQL support, limitations, source versus published-package
distinctions, and verified results for every contribution. Include necessary
documentation and website updates in the original PR before merge, alongside
its reviewed evidence. If either needs no update, explain why explicitly in
the PR. Preserve historical results and never attribute measurements to an
untested build. After merge, maintainers check the live site against the merged
changes and record any publication delay or mismatch for follow-up.

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
