# Contribution result contract and implementation status

The structural validator is implemented in `testing/result_contract.py`.
Record adapters, raw-sample timing calculation and static site evidence generation
are implemented and tested with synthetic fixtures. Integration with incoming
full-suite collectors and maintainer acceptance remains pending; this is not an
operating full-suite runner or hosted CI gate. TestMini execution and accepted-result publication are being
prepared. No new run, baseline, proof or accepted measurement is claimed here.
At the 2026-10-13 workflow review, the public ruleset had no branch target;
these requirements must not be presented as enforced merge checks. This document changes no repository or
hosting settings.

## Review and acceptance

The intended sequence is draft PR → local checks → maintainer review and agreed
scope → appropriate TestMini correctness/benchmark runs → evidence attached or
linked to the PR → maintainer acceptance → approval and merge. Every considered PR requires full independent Test Mini SLT, 55K, SF10 and
real-world evidence plus standard checks. Missing required results block merge;
there is no automatic documentation-only exemption. Package publication remains a
separate release action. A merged PR earns attribution, not a correctness
certificate or an automatic benchmark endorsement.

## Required result identity and method

Each future result must identify:

- Public repository and PR; tested source SHA; current PR head SHA at collection;
  baseline/base SHA; and a digest of the full run inputs, including the selected
  source, commands, configuration, suites, queries and datasets.
- Pinned machine identity/specification, OS, toolchain and dependency versions;
  suite and method versions; dataset/query hashes and scale; worker count and
  resource settings; timing boundary (including what setup or I/O is excluded),
  warmup policy, repetitions and aggregation method.
- Every outcome: failures, wrong answers, unsupported cases, refusals, timeouts,
  crashes and slower cases. Preserve total denominators, exclusions with reasons,
  per-query results and run counts. Missing or unrun checks remain explicit.
- Immutable result artifacts and a manifest of their cryptographic hashes, plus
  the human-readable summary linked from the PR. Never overwrite an old result
  with a rerun; publish a new version linked to its predecessor.

The baseline and candidate must use compatible inputs, machine conditions and
methods. A change to the base invalidates comparisons whose assumptions no longer
hold; review compatibility and rerun affected comparisons before acceptance.
Do not adjust expected answers or exclusions just to improve a result.

## Isolation and trusted collection

PR code is untrusted. Future execution must be isolated from the protected
collector, publishing credentials and privileged repository/host controls. The
protected collector must validate the revision and input identity, collect
bounded results, hash immutable artifacts, and record which execution produced
them. Neither PR code nor its prose may declare its own trusted acceptance.
Collector configuration and the decision to accept results require maintainer
review. Public artifacts must omit secrets, personal contact details and private
data; use redistributable inputs. Hashes preserve identity, not correctness.

## Tested source and final PR head

Any change that can affect execution requires a new run. This includes source,
dependencies, toolchain, build or test configuration, harnesses, expected answers,
inputs, queries, normalizers and measurement methods. Old results remain tied to
the old tested source SHA and must not be relabeled as testing a new revision.

An evidence commit necessarily changes the PR head. Preserve the tested source
SHA, record the new final head, and validate the complete delta between them.
Only explicitly allowlisted inert evidence and presentation files may differ;
review the exact files and contents, not just their directory names. A script,
configuration, executable input or other execution-affecting change cannot use
this exception. Record the delta and final-head validation in the acceptance
record; otherwise rerun at the new head. Recheck this relationship after further
pushes or base changes, and merge only the reviewed head.

## Accepted data and charts

A protected, versioned acceptance record must link the accepted result hashes,
tested source, final head, base, scope, limitations and maintainer decision.
The accepted chart-data update belongs to the reviewed merge; the intended site deployment then publishes that version from main. Automatic Vercel deployment from public main was verified for PR #4;
that deployment did not qualify its source under the full-suite gate. Future charts
and their no-JavaScript fallbacks must derive from the same versioned accepted
data, never from PR prose or the contribution-credit list. Until that path exists
and new measurements are accepted, retain all current historical chart JSON,
values, machine/release context and caveats unchanged.

## Website display metrics

These definitions do not replace the acceptance contract above. The contributor
list is populated from merged public PR metadata. Explicit reviewed display data
lives in `site/evidence/contribution-metrics.json`, versioned as
`assay-contribution-display/v1`; no metrics are mined from PR prose.

- **Accuracy:** applicable SQLLogicTest passed / applicable total × 100, with
  integer counts and wrong/unsupported/error/not-run outcomes. Excluded control
  records remain separately visible. The renderer validates that outcomes sum to
  the denominator and links the exact measured source and immutable public evidence.
- **Speed:** paired suite wall-time percentage change versus tested public main
  on the same host/configuration: 100 × (candidate / main − 1). Negative means
  faster; positive means slower. A future measured speed entry requires a reviewed
  schema extension with paired source/main identities, scope and timing fields.

The current record for PR5 contains accepted SQLLogicTest counts and null speed.
Its historical timing baseline is not public main. Other missing records show
Not measured. The renderer never substitutes a Polars ratio or historical-source
comparison for a main-baseline improvement. PR5's retained measured source is
not relabeled as a fresh execution of its merged head. Values are attributed to
reviewed evidence, not inferred from contributor credit.
