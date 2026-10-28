> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Regular-expression reference corpus (October 18, 2026)

Generate with the project Python (DuckDB 1.5.6 in process, PostgreSQL 17 through
`psql -h /tmp -d postgres`; no Assay output is an input):

```
.venv/bin/python tools/testing/regexp_reference.py --tpch <TPC-H SF1 Parquet dir> --pg-scratch <scratch dir>
```

The cases, the analysis queries, the grid and the admission rules were written,
and their reference answers fixed, before the Assay implementation ran.

## Files

- `texts.parquet`: the 36 grid texts: NULL, empty, ASCII words, `a\nb`, a
  vertical tab, tab, é/É/ß, the Kelvin sign, a decomposed é, an emoji, an
  Arabic digit, NBSP, punctuation, `Clerk#000000951`, a TPC-H comment. No text
  contains `<` or `>`, so a `<>` replacement marks every match boundary.
- `grid.json`: 3,889 pattern x function entries (346 patterns: 256 systematic
  constructs and 90 seeded random ones, x 12 function families) plus 252
  group-reference entries (126 capture patterns, first and global), each over
  all 36 texts: 149,076 cells. Each entry keeps DuckDB's answer per text, and
  PostgreSQL's where it defines the function (`"same"` when identical), and the
  outcome Assay must produce, derived by the generator's `Model` (an
  independent Python implementation of the admission rules):
  `answer` (must equal DuckDB, and PostgreSQL where it applies),
  `refuse:static` (pattern outside the subset), `refuse:<rule>` (row rule),
  `error` (both references reject the pattern). The generator fails if an
  admitted cell's references differ or a required error is answered by a
  reference. Counts: 76,026 answer, 14,292 error, 50,400 static refusals,
  4,688 case_fold, 2,336 class_non_ascii, 943 extent, 282 newline,
  109 vertical_tab.
- `cases.sql` / `expected.json`: 83 curated statements over `r.parquet` and
  `empty.parquet` (NULLs, empty input, a single row, groups, column patterns
  and replacements, options, every refusal and both-reject error). Each holds
  DuckDB's answer (or `duckdb_sql`, the DuckDB spelling of a PostgreSQL-only
  function) and PostgreSQL's (or `pg_sql`), run on TEMP tables with the same rows.
  Every refusal records a `difference`.
- `analysis.sql` / `analysis_expected.json`: TPC-H SF1 examples with DuckDB's
  answers and PostgreSQL's for the functions it defines (SF1 columns loaded into
  TEMP tables), plus the SF1 file sizes and digests.

## Comparison

`tests/regexp_functions.rs` runs the cases at workers 1/4/10 x batch 1/7/8192 x
optimizer on/off, the grid at two settings (every cell's outcome class and
message, every answer's value), first-failing-row refusal replay at all 18
settings, grouping with spill, and the CLI over Parquet. The SF1 test runs when
`ASSAY_TPCH_SF1_DIR` names the Parquet directory.
