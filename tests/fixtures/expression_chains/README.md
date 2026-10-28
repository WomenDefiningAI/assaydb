> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Operator-chain fixture

`cases.json` holds 18 cases for the height limit of 4096 (`src/nesting.rs`;
definition and rationale in `SEMANTICS-LOG.md`). Before the expression fence,
flat chains were limited only by the host stack, and the October 16 fence
counted every link of a chain as a nesting level, refusing chains the earlier
build answered. These cases pin the lengths that must stay answered:

- OR and AND chains at 300 operands and at the longest chain that fits in the
  64 KiB statement limit (3,330 and 3,027 operands).
- `+` chains at 300 operands and exactly at the height limit (4,093 operands);
  the longest `+` chain within 64 KiB (5,456 operands) is beyond it and must be
  refused as `unsupported` (the earlier build crashed beyond 3,297).
- Comparison chains (`(x > 2000) = TRUE = TRUE ...`) at 300 and exactly at the
  height limit.
- Chains mixed with nesting: an OR chain of 3,000 inside 240 parentheses, a
  `+` chain of 1,000 nested calls, an OR chain inside 100 nested CASEs, and an
  OR chain of 300 in a scalar subquery (planning a scalar subquery is cubic in
  the length of its chain, before and after the fence: about 13 s at 1,000).
- UNION ALL chains of 481 operands (the earlier build's maximum) and 509.
- IN, NOT IN and string IN lists at the 64 KiB statement limit (10,510-10,942
  items).

Each case is a generator `shape` and length `n`; the test rebuilds the SQL and
checks its byte length and FNV-1a hash. `setup` creates a 64-row `orders`
table in four inserts. Answers come from DuckDB **1.5.6** with one thread and
`max_expression_depth` raised; DuckDB rejects `a = b = c`, so comparison chains
are given to it with explicit left-associative parentheses (the same tree).
Regenerate or recheck with the project Python:

```sh
python tools/testing/expression_chains_reference.py
python tools/testing/expression_chains_reference.py --verify
```

`tests/expression_chains.rs` checks every length against the engine's fence,
runs all cases through sessions on a 512 KiB host thread at workers/batch
1/8192, 4/16 and 10/7, runs the OR, AND, `+` and IN cases through the
`assaydb` CLI over Parquet, and checks `||` chains. On the expression-nesting
branch `||` was refused by the binder; in integration candidate 1, with string
functions merged, a 300-link `||` chain answers DuckDB 1.5.6's rows (each
comment repeated 300 times) and a `||` chain exactly at the height limit binds
iteratively and answers on the 512 KiB host thread.
