> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Join and set-operation reference corpus (October 16, 2026)

Generate with the project Python (DuckDB 1.5.6, in process; no Assay output is
an input):

```
python3 tools/testing/joins_sets_reference.py \
  --tpch "$ASSAY_TPCH_SF1_DIR"
```

The SQL cases and the TPC-H analysis queries were written, and their DuckDB
answers fixed, before Assay ran them. Generation is deterministic.

## Files

- `setup.sql`: two order sources with duplicate and NULL customer keys, a
  customer table, an empty table, composite-key tables (`pa`/`pb`) with NULLs in
  either key column, DOUBLE/BOOLEAN/TEXT tables, and multiset tables
  (`ma`/`mb`/`md`) with repeated rows, NULLs and a DECIMAL column.
- `cases.sql`: 84 cases. Each carries a `pg` column: `agree`, or what
  PostgreSQL 17 does instead (from its documentation and planner source; no
  PostgreSQL server was run). Cases with `assay_error` are refusals or errors,
  each with a recorded `difference`; the DuckDB answer or error is kept.
- `expected.json`: DuckDB names, types and rows (DOUBLE as IEEE bits) or errors.
- `analysis.sql` / `analysis_expected.json`: TPC-H SF1 analysis examples, with
  the SF1 file sizes and SHA-256 digests the answers were computed from.

## Comparison

`tests/joins_sets.rs` compares every case exactly (names, types, values, NULLs;
order where `ordered`, otherwise as multisets) at workers 1/4/10 x batch
1/7/8192 x {optimizer off, optimizer on, optimizer on + forced spill, optimizer
off + forced spill}, refusals by message prefix. DuckDB's COUNT/SUM HUGEINT is
compared as Assay's BIGINT, INTEGER as BIGINT, and DECIMAL by scale (each
engine has its own result-precision rule).

The same file also checks FULL/RIGHT joins (equi, residual, text residual,
non-equi only, USING with grouped merged keys, empty sides) and INTERSECT/EXCEPT
ALL over generated Parquet (1,500 x 1,100 rows, duplicate and NULL keys)
against a brute-force nested-loop oracle written in the test, at workers
1/4/10 x batch 1/7/8192 x forced spill on/off, with an 8 MiB budget under spill
(peak reserved bytes must stay within it; no spill file may remain).

The TPC-H test runs when `ASSAY_TPCH_SF1_DIR` names the SF1 Parquet directory
(the file sizes must match), over workers 1/4/10 x batch 1/7/8192. It reads the
6M-row lineitem table, so prefer a release build:

```
ASSAY_TPCH_SF1_DIR="$ASSAY_TPCH_SF1_DIR" \
  cargo test --release --offline --locked --test joins_sets tpch
```

`ASSAY_TPCH_CASE=<id>` restricts it to one analysis case.

## PostgreSQL agreement by form

- RIGHT/FULL [OUTER] JOIN ... ON: same semantics (SQL standard). PostgreSQL's
  planner rejects a FULL JOIN whose condition has no merge- or hash-joinable
  equality ("FULL JOIN is only supported with merge-joinable or hash-joinable
  join conditions"); DuckDB answers and so does Assay (`full_non_equi_only`).
- USING: PostgreSQL docs, "Joined Tables": one output column per USING column;
  for FULL it is COALESCE(a.k, b.k), for LEFT the left column, for RIGHT the
  right column. DuckDB's values agree; qualified `a.k`/`b.k` keep the input
  columns in both. `SELECT *` lists the merged columns first in PostgreSQL but
  at the left column's position in DuckDB; they agree only when the USING
  columns lead the left input, so Assay refuses other `SELECT *` forms.
- NATURAL: USING over all common column names in both. With no common column
  PostgreSQL cross-joins while DuckDB errors: refused.
- INTERSECT ALL / EXCEPT ALL: both use min(m, n) and max(m - n, 0) copies;
  NULLs are not distinct; INTERSECT binds tighter than UNION/EXCEPT.
- Compound ORDER BY / LIMIT / OFFSET: both accept output names and positions.
  PostgreSQL rejects qualified names and expressions; DuckDB accepts qualified
  names and rejects expressions. Both refused in Assay.

## Analysis examples (TPC-H SF1)

- `billing_shipping_reconciliation`: January 1995 billing (orders placed) vs
  shipping (line items shipped) per customer, FULL OUTER JOIN ... USING
  (custkey): 6,865 billed-only, 27,841 shipped-only and 10,660 matched
  customers, with each side's totals; COUNT(custkey) of the merged key counts
  every row.
- `reconciliation_largest_gaps`: the ten customers with the largest
  billed-minus-shipped gap, reported by the merged key from either source.
- `idle_suppliers_by_nation` / `idle_suppliers_sample`: suppliers with no
  shipments in 1996-03-01..07, via the week's distinct shipping suppliers RIGHT
  JOIN supplier ... WHERE the left key IS NULL.
- `repeat_customer_overlap` (+ `_by_segment`): INTERSECT ALL of 1995 and 1996
  order customer keys (146,890 paired orders) against INTERSECT (75,739
  customers).
- `largest_orders_current_and_archive` / `largest_orders_per_source`: top-N
  over a UNION ALL of a current and an archived period with ORDER BY/LIMIT on
  the compound query and on parenthesized branches.
- `supplier_customer_nations_except_all`: EXCEPT ALL of supplier and
  high-balance customer nation keys, surplus per nation.

## Refusals and errors

| Case | DuckDB 1.5.6 | PostgreSQL 17 | Assay |
|---|---|---|---|
| `SELECT *` over USING when the USING columns do not lead the left input | merged column at the left position | merged columns first | `unsupported: SELECT * over a USING/NATURAL join ...` |
| NATURAL JOIN without common columns | error | cross join | `unsupported: NATURAL JOIN without common columns` |
| USING (k, k) | accepted | error | `unsupported: column k appears more than once in USING` |
| USING columns of different types (INTEGER vs DECIMAL) | left type | common type | `unsupported: USING column a has different types ...` |
| Alias on a parenthesized join group | rejects `j.x` | answers | `unsupported: alias on parenthesized join` (kept) |
| LATERAL | answers | answers | `unsupported: table factor ...` (kept) |
| Qualified ORDER BY after a set operation | answers | error | `unsupported: qualified ORDER BY name over a set operation` |
| ORDER BY expression after a set operation | error | error | `unsupported: ORDER BY expression over a set operation` |
| FULL JOIN without ON/USING | syntax error | syntax error | `unsupported: join requires ON` |
| USING column missing on one side | error | error | `semantic: USING column ... does not exist ...` |
