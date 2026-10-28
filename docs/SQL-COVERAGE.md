# SQL coverage

The proposed current source adds the features described in [candidate SQL coverage](SQL-CANDIDATE-COVERAGE.md). Qualification remains incomplete. The table below is retained as the explicitly historical PR6 contract; it must not be read as the new candidate feature inventory.

## Historical PR6 coverage

This table describes current source at PR6 merge
`b07d7e876dfde65dda09c06808ba811b9fca49fd`, reviewed on October 16, 2026 against
engine code and regression tests. It does not describe every published package:
Alpha 1/2 packages have not gained the newer decimal-expression casts or ranking functions.
It describes supported forms, not every combination of SQL syntax. A parser
accepting a statement does not mean the engine supports it. SQLite corpus
coverage does not establish broad SQL coverage or SQLite drop-in compatibility.
The [semantics log](../SEMANTICS-LOG.md) takes precedence over shorthand below.
No new execution is claimed by this documentation review.

| Area | Supported forms | Refused or limited forms | Source and regression evidence |
|---|---|---|---|
| SELECT | Projection, aliases, WHERE, SELECT ALL/DISTINCT, derived tables, ORDER BY output names/ordinals, LIMIT/OFFSET | DISTINCT ON, named WINDOW clauses and frames, TOP, QUALIFY, FETCH, locking clauses and many vendor modifiers; arbitrary ORDER BY expressions | [rel.rs](../src/rel.rs), [bind.rs](../src/bind.rs), [select_distinct](../tests/select_distinct.rs), [engine](../tests/engine.rs) |
| CTEs | Nonrecursive WITH, including reused CTEs within a query | Recursive CTEs and explicit CTE column-alias lists | [rel.rs](../src/rel.rs), [cte.rs](../src/cte.rs), [engine](../tests/engine.rs) |
| Sets | UNION ALL; duplicate-eliminating UNION, INTERSECT, EXCEPT | INTERSECT ALL, EXCEPT ALL; compound-query ORDER BY/LIMIT; incompatible columns | [rel.rs](../src/rel.rs), [set_ops.rs](../src/rel/set_ops.rs), [engine](../tests/engine.rs) |
| Joins | INNER, LEFT OUTER, CROSS/comma, LEFT SEMI and LEFT ANTI; ON conditions; parenthesized joins | RIGHT/FULL joins, NATURAL/USING; general lateral forms; aliases on parenthesized join groups | [rel.rs](../src/rel.rs), [join_forms](../tests/join_forms.rs), [engine](../tests/engine.rs) |
| Subqueries | Scalar, IN/NOT IN, EXISTS/NOT EXISTS; supported equality-correlated aggregates and non-equality COUNT(*)/existence cases | General correlated scalar queries without aggregates; correlated scalar LIMIT/HAVING/GROUP BY; conditional correlated forms requiring masked evaluation | [rel.rs](../src/rel.rs), [correlated_count](../tests/correlated_count.rs), [existence](../tests/existence.rs) |
| Membership | Compatible typed IN-lists and single-column subqueries; empty IN ()/NOT IN () with SQLite results | Mixed Int/Float IN-subqueries in either direction; text/numeric subquery comparisons; multi-column membership | [in_subquery_types](../tests/in_subquery_types.rs), [mixed_in_subquery_guard](../tests/mixed_in_subquery_guard.rs), [empty_in](../tests/empty_in.rs) |
| Aggregation | COUNT, SUM, AVG, MIN/MAX, supported DISTINCT aggregates, GROUP BY/HAVING | Aggregate-window functions, aggregate FILTER/OVER/ordered-set modifiers, ROLLUP/CUBE; Float SUM/AVG and Binary MIN/MAX; GROUP BY ordinals | [bind.rs](../src/bind.rs), [wide_aggregate](../tests/wide_aggregate.rs), [engine](../tests/engine.rs) |
| Grouped columns | Group keys and supported expressions derived from them; validated dependencies | Arbitrary ungrouped columns, ambiguous names and unsupported grouped forms | [grouped_binding](../tests/grouped_binding.rs), [grouped_null_dependencies](../tests/grouped_null_dependencies.rs) |
| Types | Signed integers, fixed decimals, Float64, Boolean, UTF-8 text, Date32, NULL; binary literals and supported Arrow Binary inputs | No timestamp/time-zone, array, map or struct type; declarations are typed, not general SQLite dynamic storage; declared CHAR/VARCHAR lengths are intentionally ignored | [types.rs](../src/types.rs), [session.rs](../src/session.rs), [binary](../tests/binary.rs) |
| Arithmetic | Checked +, -, *, unary signs; Int/Int division truncates toward zero; supported numeric comparisons are exact for Int/Float | Overflow returns an error; decimal/float contracts differ from integer contracts; division/remainder by zero follows the documented type-specific rules | [integer_division](../tests/integer_division.rs), [null_zero](../tests/null_zero.rs), [exact_numeric_comparison](../tests/exact_numeric_comparison.rs) |
| Conditionals | CASE, COALESCE and NULLIF with supported reachable types and short-circuit semantics | Row-dependent reachable Int/Float alternatives are explicitly refused for every consumer; proven dead arms do not promote | [branch_types.rs](../src/bind/branch_types.rs), [mixed_numeric_guard](../tests/mixed_numeric_guard.rs), [dead_arm_types](../tests/dead_arm_types.rs) |
| Casts | Supported SQLite-compatible INTEGER/REAL/TEXT conversions; strict literal DATE casts; explicit INTEGER/DECIMAL expression casts to DECIMAL(p,s), including columns and aggregates | DECIMAL targets require precision 1–38 and scale 0–18, no greater than precision; Float and text-column sources are refused; narrowing rounds ties away from zero and overflow errors. Literal-text DECIMAL conversion remains lossless. Float/Decimal-to-TEXT, invalid UTF-8 Binary-to-TEXT and nonfinite REAL remain refused; BIGINT is not the permissive INTEGER cast | [casts.rs](../src/expr/casts.rs), [decimal_cast](../tests/decimal_cast.rs), [cast_types](../tests/cast_types.rs), [cast_real_oracle](../tests/cast_real_oracle.rs) |
| Ranking windows | ROW_NUMBER, RANK and DENSE_RANK with inline OVER; optional PARTITION BY and ORDER BY; explicit ASC/DESC and NULLS FIRST/LAST | No frames, named WINDOW, QUALIFY, aggregate windows, LAG/LEAD or general analytic functions. Windows in WHERE/GROUP BY/HAVING or aggregate arguments are refused. Some CTE inputs needing canonical ordering are refused | [window ranking tests](../tests/window_ranking.rs), [decimal/ranking composition](../tests/decimal_window_composition.rs), [precise semantics](../SEMANTICS-LOG.md#exact-decimal-casts-and-ranking-windows) |
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

## What the retained tests say

PR6's tested source passed 5,937,073 / 5,939,714 applicable SQLLogicTest records
(99.9555%). There were still 640 unsupported cases and 2,001 engine errors, with
zero observed wrong answers or not-run outcomes. The fixed collection has
7,421,339 records; exclusions remain visible in the
[qualification report](ENGINE-SNAPSHOT-QUALIFICATION.md). These are measurements
of the identified source, not a guarantee for all SQL or every later build.
