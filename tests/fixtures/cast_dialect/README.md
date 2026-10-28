> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# PostgreSQL 17 / DuckDB 1.5.6 cast matrix

Helen's October 15 decision: return the common result; refuse disagreements.
Claude's resume instruction permits documentation-based PostgreSQL evidence and
requires unestablished cells to remain refused. Helen’s October 16 follow-up
retains REAL as binary64 and permits SQLite differences only when the saved
answer matches DuckDB 1.5.6 under the frozen comparator.

`expected-v2.json` contains 234 explicit source/target/edge cases, DuckDB's raw
observations, a separately stated PostgreSQL documentation model, its citations,
and agreement/error/unknown classifications. PostgreSQL was **not executed**.
No offline PostgreSQL C sources were found. Installed documentation 17.11 is
hashed in the receipt. Do not confuse Boolean's documented `t`/`f` output
function with a specified explicit Boolean-to-TEXT cast: that cell is unknown.

Reference sections (installed at `/opt/homebrew/opt/postgresql@17/share/doc/postgresql@17/html/`):

- [§8.1.1–8.1.3 Numeric Types](https://www.postgresql.org/docs/17/datatype-numeric.html): signed integer ranges, decimal precision/scale, half-away versus ties-even, REAL/DOUBLE widths. The local tie table is at lines 177–197. Applying the rounding rule to casts is an inference, not a PostgreSQL execution receipt.
- [§4.1.2.6–4.1.2.7 Constants and casts](https://www.postgresql.org/docs/17/sql-syntax-lexical.html): whole input, signed numeric spellings and cast syntax. Hex/underscore cast input remains unknown; SQL literal grammar alone is insufficient evidence for every type input function.
- [§8.6 Boolean Type](https://www.postgresql.org/docs/17/datatype-boolean.html): input spellings, prefixes, whitespace and case (local lines 15–24). Bool/Int casts remain unknown.
- [§8.5.1.1 Date input](https://www.postgresql.org/docs/17/datatype-datetime.html): ISO calendar dates; ambiguous/session-dependent formats remain unknown.
- [§8.3 Character Types](https://www.postgresql.org/docs/17/datatype-character.html): explicit VARCHAR truncation versus DuckDB's ignored length.

Generate with the project Python and `tools/testing/cast_dialect_reference.py`.
It refuses to overwrite an existing receipt. Goldens were generated before the
Assay matrix test. The first two receipts are preserved: the first model wrongly
retained a signed Decimal zero; the second used an untyped `-0.00` source that
Assay's existing binder classifies as Float. The final Decimal-to-text cases
explicitly declare DECIMAL, including zero, so their source type is unambiguous.
These are new supplementary fixtures, never edits to a frozen corpus.

`tests/cast_dialect.rs` checks CAST and `::` across workers 1/4/10 and batches
1/7/8192, plus table-based revenue and imported quantity analysis. Scalar
unknown/refusal cases are retained alongside successes. Boolean flag rendering
is currently a conservative unknown refusal, not the previous `1`/`0` answer.

`tools/testing/cast_dialect_sweep.py` scans all 622 SLT receipts and all 55,000
hunt records, selecting baseline passes with CAST/`::`. It checks SQL hashes,
uses the existing comparators, retains candidate replies and setup failures,
and qualifies SQLite differences through a fresh DuckDB 1.5.6 setup replay.
`SET integer_division=true` preserves the integer division convention. REAL maps
to DOUBLE; CREATE TABLE integer storage maps to BIGINT; explicit INTEGER casts
retain their checked 32-bit target (superseded October 17: Assay's explicit
INTEGER casts are 64-bit, so the sweep now maps INTEGER/INT/SIGNED to BIGINT in
every statement, like accuracy harness v2; see SEMANTICS-LOG). Errors, unmatched answers and setup failures
block. Divergences are counted separately by pattern. The baseline identity, binary hash,
counts, breakdowns and ten examples belong to its append-only scratch report.

Preserved first attempt: **blocked**. Its sweep found 1 SLT + 56 hunt wrong transitions,
1,020 hunt refusals, and 9 SLT binding errors. The matrix test passed; full Rust
testing failed in 18 integration targets. See SEMANTICS-LOG.md and the retained
`<private Dev Mini scratch>/castdialect-CARD-REPORT.md` for counts,
ten examples, required harness labelling and remaining work.


October 16 v2: REAL/FLOAT8/DOUBLE mean binary64, FLOAT4 stays refused. The
reference generator records `oracle_sql` with REAL → DOUBLE and marks this
explicit policy exception; PostgreSQL's native REAL width is not misreported.
The v2 matrix has 130 agreements, 50 shared errors, 9 disagreements, 45 unknowns.
`expected.json` and the two earlier attempts preserve all prior observations.
`legacy-final.json` is an additive DuckDB receipt for changed test expectations:
48 numeric boundaries, 43 text-to-REAL boundaries, 62 decimal/float casts,
137 spelling cases, and 39 handwritten controls. Original SQL, metadata and
SQLite goldens are retained. Internal legacy conversion tests remain separate
from SQL's explicit cast contract.

The first legacy overlay is preserved as `legacy-v2.json`: its DuckDB SQL did
not type an untyped negative-zero literal like Assay. v3 explicitly casts that
literal from text to DOUBLE, preserving the existing binary64 sign assertion.

v3 additionally exposed an oracle SQL quoting bug; v4 changes only unquoted
negative-zero tokens. Both earlier overlay receipts remain intact.

Final continuation completed: formatting, all-target Clippy and debug tests pass;
all nine SLT binding regressions pass again. The final sweep has 476,400 SLT
passes plus one exact DuckDB-qualified rounding divergence; 8,180 hunt passes
and 1,020 refusals; zero unmatched wrong answers or other errors. See the final
SEMANTICS-LOG receipt and `castdialect-CARD-REPORT-resume3.md` in private scratch.
The final legacy overlay adds the missing DECIMAL(18,6) nonintegral Float
refusal policies while preserving the independent DuckDB observations.

October 16 correction: owner decisions supersede the generic refusal rule. Casts
that returned a result on f4377d5d keep it unless a decision changed it (base
literal dates stay; INTEGER rounding and Boolean `true`/`false` text follow
DuckDB); Float → DECIMAL is exact binary rounding and bare
DECIMAL is (18,3) (October 15); cells where both references error raise exact
data errors. `tests/cast_dialect.rs` lists every such cell; see SEMANTICS-LOG.md.
The `decimal_float` refusal policies in `legacy-final.json` are no longer used.
