# Release source references

| Snapshot | Python | Rust metadata | Original source commit |
| --- | --- | --- | --- |
| Alpha 1 | 0.1.0a1 | 0.1.0-alpha.1 | 44b5ab5ede4504649217ee9909f9c265b44b31b7 |
| Alpha 2 | 0.1.0a2 | 0.1.0-alpha.2 | 17b575b7c556c47ccc461209f02d28e6a5cf6045 |

This tree is the sanitized Alpha 1 snapshot, not current development main.
EXPORT-MANIFEST.json records each exported path, upstream Git blob identity when
applicable, sanitization notes, and SHA-256 of the exported bytes.
Export commits will have different identities from their original sources.

## Publication evidence and limits

Historical PyPI publication runs 38086132580 (Alpha 1) and 38096755596 (Alpha 2)
explicitly selected the source commits above. Their verification jobs reported
that the three published distribution hashes matched their reviewed receipts.

Alpha 2 crates.io publication run 38102932588 ran from a later workflow revision
but explicitly selected 17b575b7c556c47ccc461209f02d28e6a5cf6045 as SOURCE_SHA.
The publish job compared package bytes and the verification job reported a
matching registry checksum. Workflow revision and package source are distinct.

Alpha 1 crates.io provenance was checked independently using the official
crates.io index checksum and published crate archive. The archive SHA-256 is
`da4da6c02e19373c12d321208879169d6d5c9f0e3c7b9fe6030086aed3edad4e`.
Its .cargo_vcs_info.json identifies
`44b5ab5ede4504649217ee9909f9c265b44b31b7` with an empty path_in_vcs.
All 172 original package inputs match that source, including the original
manifest, engine/tests, licenses, and optional compressed regression corpus;
Cargo.toml and .cargo_vcs_info.json are Cargo-generated package files.
This establishes the Rust source independently of the PyPI publication record.

Historical package checks are not new tests of this sanitized export.
No repository scripts, builds, SQL, package tests, or proof campaigns were run
during export review. This export has not been published.

## Release differences

Alpha 2 retains the physical Int64 representation for eligible direct references
to Int64 source columns, reducing intermediate projection/join state.
Computed integer expressions and other wide sources retain precision-38 handling.
The engine change is in src/rel.rs and src/rel/column_projection.rs; release
metadata and locks also change. This makes no new timing or memory claim.

## Sanitization

Engine and binding implementation files, lockfiles, and retained regression
inputs are copied from the identified snapshots without behavioral edits.
Documentation is rewritten to remove private coordination, stale preparation
claims, excluded-source links, and unsupported validation claims.
The activity-lock wrapper is made independent of the omitted benchmark harness.
All differences are identified by the export manifest.

No private Git history, session links, operational notes, raw run logs,
TPC-derived SQL, generators, or benchmark datasets are imported.
The website and verification/toolkit workspaces are outside this export.
