# SQL candidate coverage and qualification

This source contribution carries the locally tested `b5b34e53618e9975949eaa0e72b08bca8a61db44` candidate onto current public main. It is experimental source, not a package release, full SQL parity, Alpha 3 qualification or a new performance result. Existing published packages remain separate.

## Source changes

The candidate includes the previously prepared SQL expansion (joins/set operations, numeric and text functions, dates/timestamps, aggregate modifiers, statistics, regex and window frames), the public INTEGER/REAL compatibility repair, and these later relation features. Tests establish their particular domains and refusal boundaries, not every combination:

- Derived and CTE column aliases: `tests/derived_column_aliases.rs`, `tests/cte_column_aliases.rs`.
- CTE materialization hints and bounded recursive execution: `tests/cte_materialization_hints.rs`, `tests/recursive_cte.rs`.
- DISTINCT ON, GROUP BY ALL and SELECT modifiers: `tests/distinct_on.rs`, `tests/group_by_all.rs`, `tests/select_modifiers.rs`.
- VALUES relations, UNION BY NAME, integer series and text additions: `tests/values_relation.rs`, `tests/union_by_name.rs`, `src/rel/integer_series_tests.rs`, `tests/concat_ws_translate.rs`.

Resource limits, incompatible type refusals and guarded error behavior remain part of the contract. The fast engine is not formally proved. Private benchmark inputs and proof workspaces are excluded.

## Evidence and missing merge gates

The [dated candidate receipt](../site/evidence/validation-update-20261028.json) reports 5,939,628 / 5,939,714 applicable SQLLogicTest passes, 31 unsupported cases, 55 errors and zero wrong answers. The differential suite separately retains 54,171 direct passes, 739 documented differences, 65 expected native rejections and 25 oracle errors. These categories are not interchangeable.

Retained October 23 standard, installed Python, packaging and retail results belong to the identified candidate. Fresh October 28 candidate SF10 correctness checked all 22 queries at one and ten workers; its 66 resource cases passed. None of this supplies missing paired timing against public main. Prior timing of an older source must not be attributed to this candidate.

Merge remains blocked pending the required paired performance campaign, complete source-bound evidence review and final-head acceptance. The contribution metrics record is deliberately not marked accepted. Public-main website changes are preserved; its existing candidate notice is still accurate while qualification is pending. No new chart or speed claim is proposed.

## Remaining SQL acceptance backlog

These are test-design tasks, not implemented fixes or newly executed tests. Agree semantics before broadening behavior; preserve the frozen corpus and all nonpasses.

| Area | Remaining observations | Minimal original test design | Acceptance requirements |
|---|---|---|---|
| HAVING name resolution | 49 of the 55 grouped-binding errors involve output aliases colliding with input names | `CREATE TABLE t(a INTEGER, b INTEGER); INSERT INTO t VALUES (1,10),(1,20); SELECT a AS b FROM t GROUP BY a HAVING b=1;` | Pin alias-versus-input precedence explicitly; compare independent dialect references. Cover collisions, unique aliases, qualified input names, grouped expressions, NULL, duplicates and workers 1/10. Do not silently choose an arbitrary ungrouped value. |
| Ungrouped HAVING | Six grouped-binding cases are also refused by the pinned DuckDB reference | On the same table: `SELECT a FROM t GROUP BY a HAVING b>0;` | Decide whether permissive SQLite grouping is in scope. If not, preserve a stable documented rejection; if supported, define deterministic behavior and test multiple input orders. These remain errors in the retained corpus receipt. |
| Empty heterogeneous membership | Four unsupported cases: TEXT/BINARY left values against an empty INTEGER subquery, each IN/NOT IN | Empty `t(n INTEGER)`; evaluate `'hello' IN (SELECT n FROM t)`, its NOT IN form, and the same pair with `x'303132'` | SQLite empty-set results are false/true regardless of left value. Decide the dialect contract before relaxing binding; test NULL/empty/nonempty RHS, binary/text/numeric types, scalar/filter contexts and both worker settings. Existing numeric promotion tests must stay passing. |
| Trigger/session support | 27 trigger-related nonpasses: 14 unsupported session statements and 13 parser refusals | Original fixture: create two integer tables and an AFTER INSERT trigger copying `NEW.n`; insert one row, inspect both tables, drop trigger, insert again | Define supported trigger grammar, timing, OLD/NEW, mutation atomicity, recursive-trigger limits and failure rollback before implementation. Test insert/update/delete and BEFORE/AFTER; retain unsupported status until execution exists. |

This corpus backlog is not an inventory of all SQL. Broader coverage requires a versioned dialect/feature matrix with independently specified semantics, positive and negative tests, composition tests, resource bounds and differential references. Transactions, durability and unrestricted server SQL are not promised by a high corpus pass rate.
