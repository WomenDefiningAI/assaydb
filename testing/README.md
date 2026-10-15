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
failure. The original v1 zero-error policy remains unchanged. The explicitly
reviewed v2 `infrastructure-no-regression/v1` policy permits unchanged classified
engine/reference errors for infrastructure PR acceptance; it still rejects wrong
answers, missing execution, infrastructure failures, changed error signatures,
changed passing answers, and previously passing records becoming failures.
Unsupported cases remain visible
failures; a stable unsupported case is not a wrong-answer regression. Frozen SLT
control exclusions are counted separately and cannot silently shrink scope.

This is structural validation, not a trusted runner or automated acceptance.
Hash-pinned SLT, hunt query-stream and retail adapters are implemented in
`collector_adapter.py`. They retain original outcomes and compare every identity
against an independently frozen scope. Unknown records and unauthorized exclusions
fail. The original collector must still verify lifecycle, auxiliary/metamorphic
records and build provenance before supplying streams. Protected collection, final-head
delta review and maintainer acceptance remain required. A candidate-controlled receipt or `complete` flag is not proof.
Do not use this validator alone to authorize a merge or a performance claim.

The v2 policy requires full per-record answer/failure fingerprints and original
classifications. Aggregate equality cannot establish no regression. Protected
adapters authenticate the complete raw streams and frozen input scope before
normalization. Unordered answers may be canonicalized only under the unchanged
original comparator. An SLT raw-hash difference requires a separately pinned
full-payload equivalence proof; matching rendered prefixes is insufficient.
Future accuracy improvements are measured against this frozen baseline, retaining
all errors and unsupported cases until they actually pass. Infrastructure
acceptance never marks an error-free or performance-qualified release.

Standard checks use an explicit `candidate-checks` comparison mode in v2: their
command outcomes must all pass the separately pinned expectations. This does
not invent a baseline execution of the compiler, formatter or test runner.
SQL suites retain paired native evidence and frozen reference checks.

A multi-suite v2 bundle may use aggregate tool/input inventories for packaging.
The timing policy's `collection_manifests` then pins both original manifest
artifacts separately. Timing samples keep their original collection identities;
the validator checks those against the protected pins, not replacement hashes
from the later bundle inventory.

Run mechanics tests (synthetic fixtures only):

```sh
ASSAY_ACTIVITY_LOCK_PATH=/path/to/shared/engine-activity.lock \
  python3 scripts/with_engine_activity.py mechanics \
  python3 -B -m unittest discover -s testing/tests -v
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

`rotated_timing.py` separately supports `assay-sf10-rotated-three-five/v1`:
baseline and candidate, workers 1 and 10, three rotated engine orders, all 22
queries, one warmup and five timed samples for Assay, Polars and DuckDB. It retains
all 4,752 samples and recomputes total and geometric-mean Assay/reference ratios
and 10,000-draw paired round-block bootstrap intervals. It does not reinterpret
these as paired-five results or impose an Alpha 3 target on infrastructure work.
The retained collector has phase-completion timestamps, not individual sample
wall-clock timestamps; the adapter preserves that distinction. Original process
events and CPU/memory metrics stay in private evidence.

`thread_metric.py` distinguishes helper permits excluding the coordinator from
tracked live execution threads including it. A protected policy pins the entire
versioned binding, source/tree/inventory, build receipt and binary. Unknown or
swapped contracts, invalid numeric types and effective-worker mismatches refuse.
This predicate does not replace memory/spill checks or measure whole-process OS
thread counts. Rechecking retained outputs produces a separate receipt and never
overwrites an original failure or claims fresh execution.

Execution-affecting changes invalidate previous evidence. Evidence-only final-head
changes require an explicit reviewed file-by-file delta; never relabel results.
Keep complete logs, failed attempts and earlier evidence. All native execution
uses the shared host activity lock; measurements require a quiet host.

## Sources

- `slt/full-scope.json`: frozen official SQLite sqllogictest inventory at
  [check-in ab599cbdd0976a63c5cfc7755588f716a7621f27](https://www.sqlite.org/sqllogictest/info/ab599cbdd0976a63c5cfc7755588f716a7621f27).
  622 files; 7,421,339 fixed records; 5,939,714 applicable. Retain the
  [upstream copyright notice](../docs/licenses/SQLLOGICTEST-COPYRIGHT.md), CC0 option.
  The corpus has been privately acquired and hash-verified on the test host.
  Full execution and independent runtime review have completed for the tested
  PR candidate. Final-head acceptance and evidence integration remain separate.
- `hunt55k/generate_v2.py` and `generate_v3.py`: original Assay generators, unchanged
  bytes under the repository MIT OR Apache-2.0 terms. The additive 5K preserves the
  original 50K prefix. The frozen plans/recipes and reference library have been privately acquired and
  verified on the test host; they are not bundled here;
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

Vercel deploys reviewed `site/` content from public main automatically. No separate
manual website upload is needed after merge. Contributor credit is fetched
separately from GitHub, including the reviewed testing-code allowlist; ordinary
website/setup-only changes remain excluded. Neither mechanism executes tests.
`site_evidence.py` validates the private bundle, requires a separate maintainer
acceptance record pinned to bundle/policy/source/reviewed-code-head identities, and emits
both `site/evidence/full-suite/latest.json` and the matching static HTML outcome
table and timing comparisons. Both views come from one computed object; JavaScript is
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
The acceptance field `reviewed_code_head` identifies the reviewed implementation
commit; `generated_output_reviewed` attests review of the rendered output.
The later output-only commit is checked separately at the PR head. An evidence
file cannot embed the hash of the commit that contains that file.

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

Admission Git reads use the fixed Test Mini `/usr/bin/git`, a minimal explicit
environment, disabled global/system configuration, no pager/hooks/fsmonitor,
disabled replacement objects/lazy fetch and denied transport protocols. Only
`rev-parse HEAD`, `rev-parse HEAD^{tree}` and `ls-tree -rz --full-tree HEAD` are
allowed. No `status`, `diff` or `ls-files` worktree comparisons run: those can
invoke repository-configured fsmonitor or attribute clean filters. Worktree bytes
and extra files are checked directly without consulting Git attributes. A trusted
Git installation remains a host prerequisite; the adapter does not bootstrap it.

## Separated runtime integration (draft)

`runtime_context.py` rechecks the admitted candidate build, unchanged controller,
private expanded input files, transfer receipts, interpreter and reviewed collector
overlay before and after execution. `runtime_launch.py` dispatches only an invocation
from an independently pinned private policy. Its tools must run from a protected
copy outside candidate source, with a sanitized Python environment. This is not
an operating-system sandbox; do not expose publishing credentials to execution.

The collector overlay adapts provenance checks separately from frozen SQL, scoring,
transport and lifecycle code. Its exact private diff and complete file inventory
require independent review. No private runner notes or corpus are published here.
Bounded synthetic integration checks and the pinned SQLite loader check passed;
full SLT/55K collection and fresh baseline/candidate builds remain pending.

Each private `assay-separated-runtime/v1` policy contains the existing boundary
policy, separate root mappings, exact overlay and admission-tool inventories, every
expanded input hash, transfer-receipt hashes, interpreter identity, engine environment,
host lock and invocation arguments. A fixture invocation cannot be labeled a full
collection. Build receipts retain Cargo events and require both native artifacts
to have `fresh: false`; changed or substituted source/binaries fail admission.
The installed controller pin is unchanged. Full-suite qualification and merge remain
blocked until all required evidence and independent review are complete.

Production launch uses a protected directory containing exactly the six pinned
admission files (`runtime_launch.sh`, `runtime_launch.py`, `runtime_context.py`,
`candidate_boundary.py`, `result_contract.py`, `timing.py`). Invoke the shell entry
point by absolute path with the absolute pinned interpreter as its first argument.
The shell clears ambient environment and supplies `-I -S -B` before Python starts.
The bootstrap authenticates policy and admission files before helper imports;
a fresh isolated worker then dispatches the pinned collector. Actual loaded module
origins must belong to pinned admission/overlay files or the host standard library.
The SQLite oracle subprocess uses the same isolation/no-bytecode flags. This
prevents ambient startup hooks, module paths and cached parent modules from being
accepted as the reviewed execution. It is not an operating-system sandbox.

Freeze and run use separate independently pinned invocation policies. Their stable
runtime identity excludes the phase policy hash to avoid a receipt-hash cycle,
but retains complete build/source/controller/input/tool/interpreter/environment
pins, pair identity, host/Pacific day and canonical lock path/device/inode. Each
policy file is still checked against its own externally supplied SHA on every
admission/postflight. Phase arguments cannot change substantive provenance.
Host/date mismatch refuses; both paired policies must name the same reviewed pair.
