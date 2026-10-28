> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Guarded AND/OR evaluation reference corpus

October 17, 2026 coverage task: an error on a row that another operand of the
same AND/OR already decides must not abort the query. See the semantics log
entry "Guarded AND/OR evaluation" for the rule.

Regenerate with the project Python (needs the local PostgreSQL 17 server):

```sh
.venv/bin/python tools/testing/guarded_eval_reference.py
```

The script writes `g.parquet` (16 rows, row groups of 3: zero, NULL and
nonzero DECIMAL divisors, ±1e308 Float values, numeric and non-numeric text,
valid/invalid date text, NULL flags), `h.parquet` (10 join rows, zero
divisors, one orphan) and the empty `g_empty.parquet` with pyarrow from
literal rows. DuckDB 1.5.6 (one thread) reads those exact files; PostgreSQL
17 runs the same SQL through `psql` against session-local TEMP tables with the
same rows (text `COLLATE "C"`). No expected value comes from Assay.

`cases.sql` has 113 cases (100 from the guarded-evaluation card and 13 `c3_*`
cases added by integration candidate 3 for LN/SQRT/LOG/POWER, DECIMAL `%` by
zero, TIMESTAMP casts and FILTER over a math argument). Each records the observed reference outcome
(`references`: both answer / both raise / PostgreSQL raises / DuckDB raises)
and the expected Assay outcome:

* `expect: rows` - DuckDB's rows for the case SQL, or, where DuckDB raises,
  DuckDB's rows for the `oracle` SQL (the same question with the guard first
  or as CASE). When PostgreSQL answers the case SQL its rows must agree.
* `expect: error` - the exact Assay error text; at least one reference raises.

The generator refuses to write a case that both references answer but that
expects an Assay error, and requires the `lenient` label on exactly the cases
both references raise on but Assay answers (approved by Helen, October 17,
2026: "more lenient than DuckDB/PostgreSQL; never returns a value computed
from a failed step"). Summary: 60 both answer -> rows; 12 both raise ->
error; 11 PostgreSQL raises -> error (DuckDB returns NULL for a zero divisor
or an infinity on overflow, Assay keeps its error); 1 DuckDB raises -> error
(FILTER over a cast argument stays refused); 5 PostgreSQL raises -> rows and
4 DuckDB raises -> rows (one reference's evaluation order raises on a decided
row); 7 both raise -> rows (`lenient`). Every non-"both answer" case carries
a `difference` note.

Fallible operations covered: DECIMAL and Float/DECIMAL division by zero, Float
overflow, text → INTEGER, Float → INTEGER range, DECIMAL(3,2) precision and
text → DATE casts. Contexts: WHERE (AND, OR, NOT, BETWEEN, IN, NOT IN, nested
and deep guards, both orders), NULL conjuncts in WHERE/JOIN ON/HAVING versus
projections, OR and FILTER predicates, SELECT-list AND/OR/NOT, CASE,
COALESCE, NULLIF, inner/left JOIN ON, cross-table WHERE guards, HAVING over
aggregates and keys, grouped keys/aggregates sharing a guarded operand,
aggregate FILTER predicates and arguments, windows, derived tables, IN
subqueries, empty and single-row input, batches in which both operands fail
on different rows, and the planner shapes behind the guard rules: IN/NOT IN
subquery guards (semi/anti-join) and guarded IN values, guarded join keys in
ON, comma, right-hand, LEFT JOIN and EXISTS/NOT EXISTS form, ON residuals
guarded by WHERE or another ON, a three-way comma join guarded by its last
table, each with an unguarded twin that still raises.

`tests/guarded_eval.rs` runs every case at workers 1/4/10 × batch 1/2/3/8192
× forced spill off/on, plus optimizations off at three settings with and
without spill (30 settings); checks names, type classes and values (floats as
binary64, sums within 1e-12 relative); pins unguarded first-row/left-operand
errors at every setting; and runs eight cases through the CLI over Parquet.
