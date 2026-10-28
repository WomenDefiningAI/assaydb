> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Aggregate window reference cases

`cases.sql` contains 66 independent queries over inline UNION ALL inputs. No
catalog, benchmark data, engine output, or expected-result cache is used by the
engine. `expected.json` is generated only by DuckDB 1.5.6 using
`tools/testing/aggregate_windows_reference.py` with the project Python. The JSON
records the source SHA-256, SQL, order contract, DuckDB types and exact rows.
The Rust test verifies source/JSON identity and every case across workers 1/4/10,
batch sizes 1/7/8192 and both in-memory and forced external sorting.

Ordered cases compare rows in sequence; unordered cases compare multisets with
multiplicity. Decimal trailing zeroes and integer physical widths are normalized
without changing numeric values. Dates use ISO strings; AVG and the existing Float64
exact-numeric division used for share ratios compare Float64 bits. Untyped NULL SUM/AVG retains Assay's existing
Null result type, rather than DuckDB's inferred numeric type; NULL cells compare
exactly. Ordinary GROUP BY/window type identity is separately asserted in Rust.

These fixtures cover the shared exact-input partition/default RANGE/ROWS rules
in the card's reviewed DuckDB/PostgreSQL contract. PostgreSQL was not executed
here. The explicitly requested Boolean MIN/MAX follows Assay GROUP BY (false
before true) and DuckDB; PostgreSQL does not provide a built-in Boolean MIN/MAX
aggregate. PostgreSQL's equivalent operations are bool_and/bool_or. The card
explicitly requests Boolean support, so that existing type extension is retained.

DESC keys specify NULLS FIRST/LAST because PostgreSQL and DuckDB defaults differ.
The implementation refuses an aggregate window with unspecified DESC NULL
placement pending a maintainer decision. ROWS reference queries use unique order
keys; independent handwritten tests pin the inherited ROW_NUMBER tie rule on
nonunique keys. Integer/decimal overflow, decimal cancellation beyond precision
38 inside a completed peer group, forbidden frames/modifiers/functions, budget
refusal, cancellation and spill cleanup are additional Rust controls. No cases
were dropped and no tolerances or engine-derived goldens are used.

Full frozen accuracy, strict analyst and SF10/performance gates remain the
integration driver's responsibility. q07 also needs the separate GROUP BY
ordinal and date-extraction cards; this fixture tests its window component.

Aggregate windows currently refuse NaN partition/order keys with an explicit
`unsupported:` message. Finite Float keys, including signed-zero peers, remain
supported; COUNT of a Float expression remains supported. This avoids silently
applying ranking's legacy NaN-payload grouping extension to the new frames.

During fixture development, aw056 initially used DOUBLE PRECISION casts, which
this engine already refuses. The retained case now uses the card's direct
SUM(d)/SUM(SUM(d)) OVER () analyst shape; its DuckDB answer was regenerated.
The original failure/query/hash are preserved in the check record. EXCLUDE and
some named-WINDOW spellings are refused by the parser: their public Error
kind/display carries `unsupported:`, while the raw parser message begins
`SQL parser:`. Tests require the unsupported kind and public prefix for every
refusal, and check the corresponding binder/parser raw-message prefix. No engine answer was
changed or used as a reference result in either fixture repair.
