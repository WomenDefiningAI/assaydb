# Historical development source snapshot

This document records the earlier promotion. See [current engine qualification](ENGINE-SNAPSHOT-QUALIFICATION.md) for the newer snapshot; the historical results below are not relabeled.

This promotion is a source update, not an Alpha 3 release or a performance
qualification. Published Alpha 1/2 history, package versions, notices and historical
website measurements are preserved. No new timing ratio is claimed.

## Provenance and scope

The initial frozen source revision is `4ff468df907766e8e43b78d5182bfc178afe7bdf`.
Its engine, build, bindings and integration-test files are identical to source
revision `727b1cb556448e07fb293650f7f975ac8957bec6`; the intervening change only
corrects controller tests. The public branch descends only from public main
`e4c616fd1168115308e021615c915fe2bb26a43d`. No development Git history is imported.

`SNAPSHOT-MANIFEST.json` records the selected files and byte digests. Engine code
is copied without behavioral edits. Changes since Alpha 2 cover aggregate state
and memory accounting, clustered and heap aggregation, join and membership lanes,
filtering, parallel scans and their regression tests. Dependency lockfiles are
unchanged. This is one frozen snapshot, not an integration of unmerged proposals.

The allowlist contains engine source, pinned build inputs, original integration
tests and required fixtures, bindings and mandatory licenses. Existing sanitized
public documentation and notices remain authoritative. Python source staging
adds the engine's newly referenced test-support files and a recursive fixture
check. The existing portable shared-lock wrapper remains in use.

Excluded: development notes and coordination, private history, benchmark datasets,
raw results, full corpus inventories, proof workspaces, and unresolved TPC-derived
material. Mandatory upstream attribution is retained.

## Validation status

The tested public candidate is `dee9b536dc3b0fdf6a43b05e30c2ed7c5dd500e9`.
[Compact validation evidence](../site/evidence/source-snapshot-validation.json)
records hashes, scope and limitations. Complete raw evidence is retained privately.

| Check | Result |
| --- | --- |
| Rust all-targets | 812 passed, zero failed; one explicitly ignored 16M-row reproducer |
| Clippy / formatting | Passed; warnings denied |
| Python source staging / contributor feed | 5 / 12 passed |
| Controller self-tests | 113 passed after correcting invocation working directory |
| Fresh release acceptance-small | 43 passed; live and preserved-copy verification passed |
| Original retail, real and synthetic separately | Each 9 passed / 11 unsupported |
| Exact-product retail, real and synthetic separately | Each 8 passed / 3 unsupported |
| Exact-decimal edge probes | 2 passed |

Retail classifications match the preserved baseline. Zero wrong answers and zero
engine errors were observed in these 64 replays. SQL, reference answers and exact
comparison rules were unchanged. Unsupported remains failure, not a pass.
Subsequent documentation/site-evidence-only commits require explicit final-head
validation; any executable-source delta requires fresh checks.

Full SQL logic corpus, frozen 55K plans and SF10 benchmark inputs are unavailable
on the validation host and are not run. PR #4 merged with these campaigns not run. The owner subsequently clarified
that every considered PR requires the full independent Test Mini gate. This
snapshot remains unqualified under that gate; its smaller checks are not a
substitute. To run them, provide the exact pinned corpus/plans
and licensed benchmark inputs with expected hashes and references.

## Website and contribution evidence

The existing site loads merged public pull requests for contributor attribution.
That feed is not correctness certification. No automatic accepted-test-result
importer exists; charts retain their historical values. Vercel deployment and live
content must be checked after an authorized merge, not inferred from repository
configuration. No package publishing is part of this promotion.
