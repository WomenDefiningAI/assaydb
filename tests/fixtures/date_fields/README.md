> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# DATE calendar fields and literal comparisons

This additive 67-case corpus uses small tables populated entirely by inline
literal VALUES in `setup.sql`. It includes all nine requested calendar fields,
both EXTRACT and DATE_PART, leap centuries/days, Dec 29–Jan 3 ISO boundaries,
duplicate dates, NULLs, empty inputs, revenue grouping/HAVING, equality joins,
all six comparisons in both directions, BETWEEN and IN/NOT IN.

`tools/testing/date_fields_reference.py` must generate `expected.json` using
DuckDB **1.5.6**, never Assay. The card authorizes this in-process reference run
directly from the checkout root with the project Python:

```sh
python3 tools/testing/date_fields_reference.py
```

The generator refuses to overwrite an earlier receipt; use `--output` with a
new path for another attempt. It records source
hashes, every query, DuckDB column names/types, exact values and full reference
errors. An unexplained outcome difference fails generation. PostgreSQL was not
executed here; the common calendar-field values follow the reviewed task card.

The Rust test checks every case at workers **1/4/10**, batch sizes **1/7/8192**,
with relational optimizations both enabled and disabled. Ordered rows compare in
order; unordered rows compare as duplicate-preserving multisets. Column names,
types, NULL locations, integer and decimal values are checked. DuckDB HUGEINT
aggregate outputs normalize only their type label to Assay's checked BIGINT;
values remain exact. Handwritten tests independently cover calendar values,
bare/typed NULLs and prepare-time errors, including empty inputs.

Deliberate boundaries are explicit in each affected case's `difference` field:

- The card requires an exact ASCII `YYYY-MM-DD` implicit-literal grammar with
  years 0001–9999. Reference date parsers can be more permissive (for example,
  non-padded dates). These cases remain SQL errors under the requested contract.
  Bind-time validation also applies on empty inputs even if a reference would
  defer conversion until a row is evaluated. Existing explicit DATE casts keep
  their prior parsing behavior.
- EPOCH, time fields, DATE_TRUNC, computed field names and typed TEXT-vs-DATE
  comparisons remain `unsupported:`. YEAR/MONTH/DAY function aliases are omitted
  because PostgreSQL does not provide those built-ins. DATE_PART's integral
  calendar values return Assay Int/BIGINT as requested; PostgreSQL's DATE_PART
  returns double precision and EXTRACT returns numeric.
- ISOYEAR results before AD 1 are explicitly refused pending a common BC
  year-numbering decision. Existing EXTRACT YEAR behavior is unchanged. Dates
  beyond chrono's representable calendar range also explicitly refuse.

Local validation passed: DuckDB 1.5.6 reference generation (67 cases, no
unexplained outcomes), `cargo fmt`, offline/locked all-target Clippy with
`-D warnings`, and offline/locked all-target debug tests through the authorized
`card_dev_check.py` wrapper. All five DATE tests passed, including the complete
18-configuration reference comparison. PostgreSQL execution was not performed.
No existing refusal-test assertions were changed. Frozen benchmark inputs and
the verified core are untouched. Driver validation remains pending; these local
checks do not establish full analyst, SLT, hunt, SF10 or performance acceptance.

Local recovery evidence: the restored test setup initially used a 64 MiB
catalog limit with a 64 MiB query budget; it now uses the required 16 MiB catalog
limit. The first debug run passed formatting/Clippy and two DATE tests, then
three DATE tests failed during setup with `INSERT supports typed literals, NULL
and signed numeric literals`. The setup's decimal CAST expressions were changed
to exact decimal literals with identical values. The original DuckDB receipt,
including the original setup SQL, is retained at
`attempts/insert-cast-setup/expected.json`. All 67 query texts, schemas, values
and reference errors in the regenerated receipt are identical as JSON values;
no case or expected answer was changed.
