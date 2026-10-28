> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# String function reference corpus (October 16, 2026)

Generate with the project Python (DuckDB 1.5.6, in process; no Assay output is
an input):

```
python3 tools/testing/string_functions_reference.py \
  --tpch "$ASSAY_TPCH_SF1_DIR"
```

The SQL cases and the analysis queries were written, and their DuckDB answers
fixed, before the Assay implementation ran. Generation is deterministic; the
script also writes `src/expr/text_case/table.rs`, the case-mapping table.

## Files

- `setup.sql`: one 16-row table: NULL, empty and long (301-character) strings,
  ß, Turkish İ/ı, a decomposed é (e + U+0301), a ZWJ family emoji, Greek with
  final sigma, ligatures/titlecase (ﬁ, ǅ, ŉ), separators, `%`/`_` data.
- `cases.sql`: 90 cases. Each carries a `pg` column: `agree`, or what
  PostgreSQL 17 does instead (from its documentation and source behavior; no
  PostgreSQL server was run). Cases with `assay_error` are refusals or errors,
  each with a recorded `difference`; the DuckDB answer or error is still kept.
- `expected.json`: DuckDB rows (names, types, values) or errors for every case,
  plus exhaustive controls:
  - `case_mapping`: DuckDB UPPER/LOWER for all 1,112,064 scalar values
    (1,451 / 1,433 changed); the Rust test checks every one.
  - `ilike_folding`: DuckDB ILIKE equals LIKE over its LOWER mapping for all
    case partners (the model Assay implements).
  - `trim_spaces`: the 17 characters DuckDB's one-argument TRIM removes
    (U+0020 and the Unicode space separators); Assay refuses the 16 that
    PostgreSQL keeps when they sit at a trimmed edge.
  - `reverse_safe`: code point ranges for which DuckDB's grapheme REVERSE equals
    PostgreSQL's code point REVERSE, checked against DuckDB on 3.3M pairs and
    5,000 random strings; the Rust test checks every code point's admission.
  - `double_to_varchar`: 3,704 binary64 values (powers of ten, boundaries,
    random bit patterns) with DuckDB's text.
  - `controls`: SQL with raw CR/LF/control characters, kept out of `cases.sql`.
- `analysis.sql` / `analysis_expected.json`: TPC-H SF1 analysis examples, with
  the SF1 file sizes and SHA-256 digests the answers were computed from.

## Comparison

`tests/string_functions.rs` compares every case exactly (names, types, values;
NULLs; order where `ordered`) at workers 1/4/10 x batch 1/7/8192 x optimizer
on/off, with refusals matched by message prefix. DuckDB's COUNT/SUM HUGEINT is
compared as Assay's BIGINT. `concat_operator_chain_null` returns only NULLs;
DuckDB's Python API reports their untyped type as INTEGER, Assay types TEXT,
so only that case skips the type comparison (`null_result_type`).

The TPC-H test runs when `ASSAY_TPCH_SF1_DIR` names the SF1 Parquet directory
(the file sizes must match), over workers 1/4/10 x batch 1/7/8192:

```
ASSAY_TPCH_SF1_DIR="$ASSAY_TPCH_SF1_DIR" \
  cargo test --offline --locked --test string_functions tpch
```

## Analysis examples (TPC-H SF1)

- `customer_name_cleanup`: per segment, strip `Customer#` and zero padding
  (SPLIT_PART + LTRIM), relabel with LOWER/REPLACE, and count addresses whose
  TRIM differs (886 to 992 per segment have edge spaces).
- `phone_country_code`: LEFT(c_phone, 2) and SPLIT_PART(c_phone, '-', 1) per
  nation; all 25 nations map to one code (10..34) with exact balances.
- `phone_masking`: `'S' || LPAD(...)` supplier codes and masked phones with
  RIGHT and STRPOS, filtered by STARTS_WITH.
- `part_name_search`: ILIKE '%FOREST%GREEN%' / NOT ILIKE '%brass' search with
  CONTAINS, grouped by brand.
- `part_first_word`: SPLIT_PART first word with STARTS_WITH on p_type.
- `segment_labels`: `n_name || ' / ' || LOWER(segment)` and CONCAT labels.

All six match DuckDB exactly at every setting.

## Refusals (DuckDB and PostgreSQL disagree)

| Case | DuckDB 1.5.6 | PostgreSQL 17 | Assay |
|---|---|---|---|
| TRIM with an edge U+00A0/U+2000..U+200A/U+3000 etc. | trims | keeps | `unsupported: TRIM of non-ASCII space characters` |
| LEFT/RIGHT count beyond int32 | answers | error (int4) | `unsupported: LEFT/RIGHT count outside the 32-bit range` |
| SPLIT_PART field 0 | `''` | error | `unsupported: SPLIT_PART field 0` |
| SPLIT_PART empty delimiter (nonempty text) | splits characters | one field | `unsupported: SPLIT_PART with an empty delimiter` |
| LPAD/RPAD empty fill when padding is needed | error | unpadded input | `unsupported: LPAD/RPAD with an empty fill` |
| LPAD/RPAD with two arguments | no such function | pads with spaces | `unsupported: LPAD/RPAD require a fill argument` |
| REVERSE of combining marks, ZWJ/emoji sequences, CR etc. | grapheme clusters | code points | `unsupported: REVERSE of combining or joining characters` |
| ILIKE pattern containing `\` without ESCAPE | literal | default escape | `unsupported: ILIKE pattern with a backslash requires ESCAPE` |
| ESCAPE with a non-ASCII character | error | accepted | `unsupported: LIKE ESCAPE requires an ASCII character` |
| ILIKE ESCAPE with an ASCII letter | not established | not established | `unsupported: ILIKE ESCAPE with a cased letter` (conservative: escape-versus-folding order unverified) |
| `||`/CONCAT with BOOLEAN | `true` | `true` (`||`), `t` (CONCAT) | `unsupported: BOOLEAN in string concatenation` (Assay BOOLEAN text is 1) |
| `||` with no TEXT operand (`1 || 2`) | `12` | no operator | `unsupported: || requires a TEXT operand` |
| CONTAINS with an untyped NULL | overload error | no CONTAINS | `unsupported: CONTAINS with an untyped NULL argument` |
| DOUBLE negative zero to text | `-0.0` | `-0` | `unsupported: negative zero DOUBLE to TEXT` |
| DOUBLE inf/nan to text | `inf`/`nan` | `Infinity`/`NaN` | `unsupported: non-finite DOUBLE to TEXT` |

Errors in both references stay errors: CONCAT(), LIKE patterns ending in the
escape character, multi-character ESCAPE strings, and LPAD lengths beyond int32.

## Followed DuckDB where PostgreSQL differs (Oct 16 decision)

- UPPER/LOWER (and ILIKE folding) use DuckDB's one-to-one Unicode mapping:
  UPPER('ß') = 'ẞ', LOWER('İ') = 'i', no final sigma, ligatures unchanged.
  PostgreSQL depends on the collation provider: libc and the PG 17 builtin
  provider keep ß and mostly agree; ICU uses full mapping (SS, i̇, final ς, FI).
- DOUBLE to TEXT (CAST, `::VARCHAR`, `||`, CONCAT) uses DuckDB's shortest
  round-trip text: `2.0`, `1000000000000000.0`, `1e+16`, `1e-05`.
  PostgreSQL uses the same digits but prints `2` and switches to an exponent at
  1e15. SQLite's REAL text (`%!.15g`) also differs.
- CONTAINS exists only in DuckDB; PostgreSQL's equivalent `strpos(s, x) > 0`
  agrees.
