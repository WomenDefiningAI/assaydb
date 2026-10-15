# Development source snapshot

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

Validation is in progress against the exact exported candidate. No previous
candidate's results establish this snapshot's correctness. Unsupported, wrong
answers, errors and not-run checks remain distinct; unsupported is not a pass.

Full SQL logic corpus, frozen 55K plans and SF10 benchmark inputs are unavailable
on the validation host. These gates remain pending. The minimal next step is to
provide the exact pinned corpus/plans and licensed benchmark inputs with their
expected hashes and references, or obtain a maintainer decision defining the
required gates for this source-only promotion. No release-gate waiver is implied.

## Website and contribution evidence

The existing site loads merged public pull requests for contributor attribution.
That feed is not correctness certification. No automatic accepted-test-result
importer exists; charts retain their historical values. Vercel deployment and live
content must be checked after an authorized merge, not inferred from repository
configuration. No package publishing is part of this promotion.
