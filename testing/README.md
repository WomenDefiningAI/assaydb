# Independent full-suite PR validation

Every PR considered for merge requires independent Test Mini full SLT, frozen
55K differential, SF10 correctness and timing, real-world original and variant
analysis, standard checks and acceptance-small evidence. Missing inputs or runs
block qualification and merge, including for this setup PR. There is no automatic
documentation-only exemption. GitHub branch settings are unchanged; this is not
claimed to be an enforced hosted check.

## Current implementation and blockers

`result_contract.py` validates source/base/tree and reviewer-supplied identities,
all suite entries, hash-linked artifacts, complete ordered record streams,
counts, baseline compatibility and no previously passing record becoming a
failure. It rejects wrong/error/not-run records. Unsupported cases remain visible
failures; a stable unsupported case is not a wrong-answer regression. Frozen SLT
control exclusions are counted separately and cannot silently shrink scope.

This is structural validation, not a trusted runner or automated acceptance.
Hash-pinned SLT, hunt query-stream and retail adapters are implemented in
`collector_adapter.py`. They retain original outcomes and compare every identity
against an independently frozen scope. Unknown records and unauthorized exclusions
fail. The original collector must still verify lifecycle, auxiliary/metamorphic
records and build provenance before supplying streams. Wiring that boundary to
the incoming full-suite runners remains pending. Protected collection, final-head
delta review and maintainer acceptance remain required. A candidate-controlled receipt or `complete` flag is not proof.
Do not use this validator alone to authorize a merge or a performance claim.

Run mechanics tests (synthetic fixtures only):

```sh
python3 -m unittest discover -s testing/tests -v
```

Validate collector-produced evidence with a separately reviewed policy:

```sh
python3 testing/result_contract.py PRIVATE_RESULTS/bundle.json \
  --policy PRIVATE_RESULTS/reviewer-policy.json --output NEW_SUMMARY.json
```

The `--policy` file is an authority input supplied by the protected reviewer,
never accepted from the candidate or inferred from a bundle. For every suite,
`scope_sha256` pins the actual independently generated ordered JSONL scope file.
The bundle supplies `scope_records: {path, sha256}`; both digests must match the
reviewer's pin. Each scope row contains `index`, `id`, `sql_sha256`, and an
optional `exclusion_reason` derived only from frozen SLT directives. Both result
streams must match every scope row, including exclusions; mutual agreement and
matching totals alone are insufficient. Scope bytes are rehashed after validation.
Generate and review these scope files from frozen inputs before running the
candidate. Changing scope requires a new reviewer policy and input review.

Keep raw evidence and policy outside the public checkout. Each record carries an
index, stable identity, SQL hash and status. Candidate and baseline streams must
have equal identities in equal order; suite counts must equal those streams.
Record exact tool/input manifests, reference versions, scope hashes, host, base
and candidate commits, and the candidate tree. Collect baseline and candidate on
the same host and Pacific day with identical tools. Timing is recomputed by `timing.py` from both complete 22-query sample sets:
one warmup and five timed samples per query, medians excluding warmup, maximum
timed memory and zero spill retries. Host/source/tool/input/request/resource/day
identities must match reviewed policy. Per-query and total limits are explicit
policy inputs, never inherited Dev Mini measurements. This versioned paired
protocol is not a claim of compatibility with every historical benchmark method;
the incoming runner adapter and the selected method still need review.

Execution-affecting changes invalidate previous evidence. Evidence-only final-head
changes require an explicit reviewed file-by-file delta; never relabel results.
Keep complete logs, failed attempts and earlier evidence. All native execution
uses the shared host activity lock; measurements require a quiet host.

## Sources

- `slt/full-scope.json`: frozen official SQLite sqllogictest inventory at
  [check-in ab599cbdd0976a63c5cfc7755588f716a7621f27](https://www.sqlite.org/sqllogictest/info/ab599cbdd0976a63c5cfc7755588f716a7621f27).
  622 files; 7,421,339 fixed records; 5,939,714 applicable. Retain the
  [upstream copyright notice](../docs/licenses/SQLLOGICTEST-COPYRIGHT.md), CC0 option.
  Corpus retrieval and exact runner integration remain pending.
- `hunt55k/generate_v2.py` and `generate_v3.py`: original Assay generators, unchanged
  bytes under the repository MIT OR Apache-2.0 terms. The additive 5K preserves the
  original 50K prefix. The frozen plans/recipes and reference library are missing;
  **these generators alone do not constitute an executable 55K suite**. Do not
  manufacture replacements. Required plan hashes: v2
  `16f92c49617461eb8837a78a021b3c48e37f82098add02bcf24ab0ab265ffd80`, v3
  `a373c28404d62b0a2c37d24f2220a992c0bf8bd3a8246630e17e9a306f1dedcf`.
- `analysis/`: original SQL and separately identified exact-product variants;
  frozen SQL hashes, no raw customer rows or expected-result datasets. See its README.
- SF10: follow [external benchmark input guidance](../docs/BENCHMARK-INPUTS.md).
  The exact dataset, query/reference bundle, protocol and fresh same-host baseline
  are prerequisites. Public download availability is not redistribution permission.
  Unresolved TPC-derived material stays outside this export.

## Website integration

Vercel deploys reviewed `site/` content from public main automatically. Contributor
credit is fetched separately from GitHub. Neither mechanism executes tests.
`site_evidence.py` validates the private bundle, requires a separate maintainer
acceptance record pinned to bundle/policy/source/final-head identities, and emits
both `site/evidence/full-suite/latest.json` and the matching static HTML outcome
table and timing bars. Both views come from one computed object; JavaScript is
not required. The shipped state is pending and contains no measurements.
Historical charts are untouched. No synthetic accepted fixture is shipped.

```sh
python3 testing/site_evidence.py PRIVATE_RESULTS/bundle.json \
  --policy PRIVATE_RESULTS/reviewer-policy.json \
  --acceptance PRIVATE_RESULTS/acceptance.json --expected-head REVIEWED_HEAD \
  --site site
```

Acceptance records must be produced in the maintainer-controlled review process.
The tool verifies their consistency, not the author's authority or honesty; a
candidate-supplied acceptance file cannot authorize publication. Stage the output
in the same PR, review the evidence-only delta to the final head, and rerun for
any executable change. The command performs no network, merge or deployment.

PR #4's scoped validation is historical, incomplete full-suite evidence. Its
merge does not qualify its source under this gate. It must receive independent
full-suite evidence before any such qualification is claimed.

## Separate candidate/controller admission adapter

`candidate_boundary.py` implements read-only identity verification for a public
candidate whose repository intentionally differs from the private controller.
**Unvalidated on full suites.** It does not build, import candidate code, launch
collectors, alter the installed controller or relax the original private runner.

```sh
python3 testing/candidate_boundary.py --policy PRIVATE_POLICY.json \
  --policy-sha256 INDEPENDENT_REVIEWED_POLICY_SHA256 \
  --candidate CLEAN_CANDIDATE_CHECKOUT --controller PINNED_CONTROLLER_CHECKOUT \
  --inputs PRIVATE_FROZEN_INPUTS --build IMMUTABLE_BUILD_ARTIFACTS \
  --output NEW_ADMISSION_RECEIPT.json
```

The reviewer policy has separate `candidate`, `controller`, `inputs` and `build`
objects. `candidate` pins commit, tree and the SHA256 of every tracked file;
a fresh source checkout must have no modified, untracked or ignored files.
`controller` pins its own commit/tree, explicit tool directories and their exact
complete file inventory; missing or extra files (including caches) refuse.
These inventories are never compared to each other or merged together.

`inputs` maps role names to confined relative paths and hashes, including
`slt-scope`, `hunt-plan`, `sqlite-oracle`, `query-manifest` and `dataset-manifest`.
`build` pins a separate protected-collector receipt, both native binary hashes,
source/controller identities, exact command and environment digest. The versioned
`assay-separated-build/v1` receipt must contain identical before/after source
inventory digests, controller tool digest, a successful exit, pinned Cargo JSON
events and binary snapshots for `assaydb` and `assaydb-session`. Both native
artifacts must have Cargo `fresh: false`; cache-only, missing, duplicate or changed
artifacts refuse. Paths are confined; symlinks refuse. Existing receipt schemas
must get an explicitly reviewed conversion, not a silent reinterpretation.

Policy construction and build execution belong to the protected collector, not
the candidate. A hash proves identity, not who created the artifact. The separate
reviewer pin must come from outside candidate-controlled inputs. Before executing
any suite, retain admission receipts for both baseline and candidate and use the
same controller/tool/input identities. The runtime integration must preserve
source/build provenance, lock ownership, lifecycle, metamorphic, exclusions,
comparison and same-host/day checks. This adapter does not make the old private
runner accept the public checkout; that runtime integration remains unvalidated.
