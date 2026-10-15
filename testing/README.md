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
Adapters from the existing collectors, timing sample/protocol verification,
protected collection, final-head delta review and maintainer acceptance are still
required. A candidate-controlled receipt or `complete` flag is not proof.
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

Keep raw evidence and policy outside the public checkout. Each record carries an
index, stable identity, SQL hash and status. Candidate and baseline streams must
have equal identities in equal order; suite counts must equal those streams.
Record exact tool/input manifests, reference versions, scope hashes, host, base
and candidate commits, and the candidate tree. Collect baseline and candidate on
the same host and Pacific day with identical tools. Timing also needs the reviewed
protocol, complete raw samples and independent review of the calculation.

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
The validated, privacy-reviewed summary and maintainer acceptance should be part
of the same reviewed PR as the change. The current validator deliberately emits
no accepted status or chart ratios. Existing historical charts remain unchanged
until the full evidence and calculation adapter are reviewed. Automatic chart
consumption of this new contract is not implemented yet.

PR #4's scoped validation is historical, incomplete full-suite evidence. Its
merge does not qualify its source under this gate. It must receive independent
full-suite evidence before any such qualification is claimed.
