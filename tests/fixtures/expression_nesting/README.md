> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Expression nesting fixture

`cases.json` holds 29 cases for the nesting limit of 250 (`src/nesting.rs`;
definition and rationale in `SEMANTICS-LOG.md`):

- 16 unchanged SQL records from the frozen SQLite SLT source (check-in
  `ab599cbdd0976a63c5cfc7755588f716a7621f27`) whose retained full-run outcome
  was sqlparser's `recursion limit exceeded`: the deepest and the shallowest
  record of each of the eight corpus directories among the 275 such records
  (depth 73-106 here). Each cites its file, record-header line, source-file
  SHA-256 and SQL SHA-256; its setup is the file's preceding unchanged
  `statement ok` SQL. Expected rows come from DuckDB and must reproduce the
  corpus's own SQLite answer (values or hash), never from Assay.
- Nine constructs at exactly depth 250: parentheses, AND chain, OR chain, NOT
  chain, nested CASE, nested calls, nested arithmetic, nested scalar subqueries
  and an IN list inside nesting. `n` is the repetition count and `pad` the
  parentheses around the innermost operand; `over_n`/`over_pad` give the same
  construct one level deeper, which must be refused. `depth` is the nesting
  depth, except for the AND and OR chains, where it is the height: since
  October 17 a left-deep chain is one nesting level however long it is
  (sqlparser parses it in a loop), so their recorded one-link-longer variants
  are answered (`TRUE`); chains are refused only beyond the height limit of
  4096 (see `../expression_chains/README.md`).
- An arithmetic ladder at depths 54, 104 and 204.
- A dashboard-builder filter: 60 nested OR-groups of AND/BETWEEN predicates
  over small lineitem/orders tables.

Answers come from DuckDB **1.5.6** with one thread. Generated constructs run
with DuckDB's optimizer disabled; DuckDB answered all of them under its default
`max_expression_depth` (1000), so no case records a DuckDB refusal. Each case
runs in a child process that is killed after 300 s. Regenerate with the project
Python and the retained corpus/outcome directories:

```sh
python tools/testing/expression_nesting_reference.py --corpus SOURCE --outcomes SLT_RUN
python tools/testing/expression_nesting_reference.py --verify
```

`tests/expression_nesting.rs` checks every construct's depth against the
engine's fence, runs all cases at workers 1/4/10 x batch 1/8192 x spill off/on
from a 512 KiB host thread, and checks depth 250 and 251 for each nesting
construct, and height 4096 and 4097 for the AND/OR chains, through a session
(with view expansion and teardown), the `assaydb-session` protocol and the
`assaydb` CLI in correctness and EXPLAIN modes. It also checks a chain of 64
plain views (the planner's own expansion guard) and nested query levels
through views at exactly 256 and 257 (`QUERY_LEVEL_LIMIT`).
`tools/testing/expression_nesting_python.py` repeats the boundary checks and
view cleanup on a 512 KiB Python thread with an installed wheel.

`engine_stack::tests::nesting_stack_headroom` (macOS) runs each construct at its
limit, plus nine more (derived tables, IN subqueries, UNION chains, WHERE
chains, mixed operators), 15 operator chains and lists at the height or 64 KiB
limit, and nested queries through views at 256 levels (alone, and with an OR
chain or a UNION ALL chain at the height limit in the innermost view), on a
fresh production-size engine stack, and reads resident stack pages with
`mincore`. It requires at most half the stack for every case, and at most
8 MiB in release for expression constructs. Measured maxima (release / debug):
expression constructs 5.5 / 66 MB, plan-level constructs 21.9 / 119 MB.

To measure other records, write a JSON array of `{"file", "line", "sql"}` and
run the ignored `measure_record_depths` test with `ASSAY_NESTING_RECORDS` set
to its path.
