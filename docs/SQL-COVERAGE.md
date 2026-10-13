# SQL coverage

This table was checked against the source and regression tests in this export.
It describes supported forms, not every combination of SQL syntax. A parser
accepting a statement does not mean the engine supports it. SQLite corpus
coverage does not establish broad SQL coverage or SQLite drop-in compatibility.
The [semantics log](../SEMANTICS-LOG.md) takes precedence over shorthand below.
No new execution is claimed by this documentation review.

| Area | Supported forms | Refused or limited forms | Source and regression evidence |
|---|---|---|---|
| SELECT | Projection, aliases, WHERE, SELECT ALL/DISTINCT, derived tables, ORDER BY output names/ordinals, LIMIT/OFFSET | DISTINCT ON, window clauses, TOP, QUALIFY, FETCH, locking clauses and many vendor modifiers; arbitrary ORDER BY expressions | [rel.rs](../src/rel.rs), [bind.rs](../src/bind.rs), [select_distinct](../tests/select_distinct.rs), [engine](../tests/engine.rs) |
| CTEs | Nonrecursive WITH, including reused CTEs within a query | Recursive CTEs and explicit CTE column-alias lists | [rel.rs](../src/rel.rs), [cte.rs](../src/cte.rs), [engine](../tests/engine.rs) |
| Sets | UNION ALL; duplicate-eliminating UNION, INTERSECT, EXCEPT | INTERSECT ALL, EXCEPT ALL; compound-query ORDER BY/LIMIT; incompatible columns | [rel.rs](../src/rel.rs), [set_ops.rs](../src/rel/set_ops.rs), [engine](../tests/engine.rs) |
| Joins | INNER, LEFT OUTER, CROSS/comma, LEFT SEMI and LEFT ANTI; ON conditions; parenthesized joins | RIGHT/FULL joins, NATURAL/USING; general lateral forms; aliases on parenthesized join groups | [rel.rs](../src/rel.rs), [join_forms](../tests/join_forms.rs), [engine](../tests/engine.rs) |
| Subqueries | Scalar, IN/NOT IN, EXISTS/NOT EXISTS; supported equality-correlated aggregates and non-equality COUNT(*)/existence cases | General correlated scalar queries without aggregates; correlated scalar LIMIT/HAVING/GROUP BY; conditional correlated forms requiring masked evaluation | [rel.rs](../src/rel.rs), [correlated_count](../tests/correlated_count.rs), [existence](../tests/existence.rs) |
| Membership | Compatible typed IN-lists and single-column subqueries; empty IN ()/NOT IN () with SQLite results | Mixed Int/Float IN-subqueries in either direction; text/numeric subquery comparisons; multi-column membership | [in_subquery_types](../tests/in_subquery_types.rs), [mixed_in_subquery_guard](../tests/mixed_in_subquery_guard.rs), [empty_in](../tests/empty_in.rs) |
| Aggregation | COUNT, SUM, AVG, MIN/MAX, supported DISTINCT aggregates, GROUP BY/HAVING | Window functions, aggregate FILTER/OVER/ordered-set modifiers, ROLLUP/CUBE; Float SUM/AVG and Binary MIN/MAX; GROUP BY ordinals | [bind.rs](../src/bind.rs), [wide_aggregate](../tests/wide_aggregate.rs), [engine](../tests/engine.rs) |
| Grouped columns | Group keys and supported expressions derived from them; validated dependencies | Arbitrary ungrouped columns, ambiguous names and unsupported grouped forms | [grouped_binding](../tests/grouped_binding.rs), [grouped_null_dependencies](../tests/grouped_null_dependencies.rs) |
| Types | Signed integers, fixed decimals, Float64, Boolean, UTF-8 text, Date32, NULL; binary literals and supported Arrow Binary inputs | No timestamp/time-zone, array, map or struct type; declarations are typed, not general SQLite dynamic storage; declared CHAR/VARCHAR lengths are intentionally ignored | [types.rs](../src/types.rs), [session.rs](../src/session.rs), [binary](../tests/binary.rs) |
| Arithmetic | Checked +, -, *, unary signs; Int/Int division truncates toward zero; supported numeric comparisons are exact for Int/Float | Overflow returns an error; decimal/float contracts differ from integer contracts; division/remainder by zero follows the documented type-specific rules | [integer_division](../tests/integer_division.rs), [null_zero](../tests/null_zero.rs), [exact_numeric_comparison](../tests/exact_numeric_comparison.rs) |
| Conditionals | CASE, COALESCE and NULLIF with supported reachable types and short-circuit semantics | Row-dependent reachable Int/Float alternatives are explicitly refused for every consumer; proven dead arms do not promote | [branch_types.rs](../src/bind/branch_types.rs), [mixed_numeric_guard](../tests/mixed_numeric_guard.rs), [dead_arm_types](../tests/dead_arm_types.rs) |
| Casts | SQLite-compatible approved INTEGER/REAL/TEXT conversions, with documented source restrictions; strict literal DATE/DECIMAL casts | Float/Decimal-to-TEXT, invalid UTF-8 Binary-to-TEXT, nonfinite REAL and many general strict casts; BIGINT is not the permissive INTEGER cast | [casts.rs](../src/expr/casts.rs), [cast_types](../tests/cast_types.rs), [cast_real_oracle](../tests/cast_real_oracle.rs) |
| Text/date functions | LENGTH(text), POSITION, SUBSTRING with constant positive start/nonnegative length, LIKE without ESCAPE; EXTRACT(YEAR FROM date) | General string/date function library, LIKE ESCAPE, arbitrary substring bounds and other EXTRACT fields | [bind.rs](../src/bind.rs), [engine](../tests/engine.rs) |
| Other scalar functions | ABS, FACTORIAL, COALESCE, NULLIF in accepted signatures | Unlisted names, named arguments, unsupported arities/modifiers | [bind.rs](../src/bind.rs), [scalar_variants](../tests/scalar_variants.rs) |
| Tables and mutations | CREATE/DROP TABLE; INSERT VALUES and accepted SELECT sources; UPDATE, DELETE; REPLACE/INSERT OR REPLACE; NOT NULL and primary/unique constraints | Triggers, foreign keys, general ALTER, transactions, RETURNING and many mutation modifiers; nested DML as INSERT source is rejected before execution | [session](../tests/session.rs), [update](../tests/update.rs), [delete](../tests/delete.rs), [nested_insert](../tests/nested_insert.rs) |
| Indexes/views | CREATE/DROP INDEX; UNIQUE indexes enforce constraints; REINDEX accepted as a no-op; CREATE/DROP VIEW | Nonunique indexes have no physical acceleration; expression/partial indexes and unsupported view modifiers refused | [index](../tests/index.rs), [unique_indexes](../tests/unique_indexes.rs), [reindex](../tests/reindex.rs), [views](../tests/views.rs) |
| Local files/Python | read_parquet and read_csv for regular local files; Python API enables readers; Arrow results, optional pandas/Polars conversion | No URLs/globs or directory sandbox; CSV needs a header and reads TEXT; trusted Parquet only | [local_files](../tests/local_files.rs), [Python API](../bindings/python/README.md) |

Named tests above establish examples and refusal boundaries, not exhaustive
coverage of each row. Refusals may arise from a combination even when both parts
are individually supported. The Python API is not Python DB-API; no parameter
binding, durable database files, transactions or streaming cursors are promised.
See [What Assay isn't](WHAT-ASSAY-ISNT.md).

## Proof boundary

The fast engine is not formally proved. Proof workspaces and linked verified
execution are not included in this export. SQL tests establish bounded examples
and refusal boundaries; they do not prove all SQL behavior.
