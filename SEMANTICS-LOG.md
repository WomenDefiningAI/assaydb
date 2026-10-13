# Assay SQL semantics

These contracts describe the Alpha 1 and Alpha 2 engine sources.
The reference for SQLite-compatible choices is SQLite 3.54.0 bundled with
sqllogictest check-in `ab599cbdd0976a63c5cfc7755588f716a7621f27`;
amalgamation SHA-256 `b739c477092f587417d402fe227eb13c0a83f7534e87082f4a1b80d429a57931`.
Assay also has exact DECIMAL and checked integer extensions. It does not promise
all SQLite syntax, affinity rules or dynamic storage-class behavior.
A semantic contract is not a proof that every query follows it.

## Nested DML in INSERT sources: explicit unsupported

A parsed INSERT source containing statement-shaped INSERT, UPDATE or DELETE
query bodies is rejected before recursive session execution. The check also
covers parenthesized, set-operation and CTE query bodies. Such input previously
could run the inner mutation and then panic when a row result was assumed.
No nested-DML semantics are implemented; rejection must leave both source and
destination catalog snapshots and accounting unchanged. Ordinary VALUES,
SELECT, CTE and UNION inputs retain their existing rules. A non-query result at
the guarded internal boundary now returns an internal error instead of panicking.
This is a regression fix, not a change to benchmark inputs or expected answers.


## Catalog and statements

Duplicate UPDATE assignments use the last assignment. REPLACE and INSERT OR
REPLACE delete a conflicting row before inserting the replacement. Declared
VARCHAR/CHAR lengths do not truncate text. These are intentional SQLite-compatible
choices. UNIQUE indexes enforce uniqueness, including composite keys and SQLite's
multiple-NULL behavior. Ordinary indexes and REINDEX are accepted without a
physical index; they must never change answers. CREATE/DROP VIEW and DELETE use
native session state. Triggers remain unsupported. Failed mutations retain the
statement's rollback behavior.

## Projection, grouping and joins

SELECT ALL is the default. DISTINCT removes duplicate complete projected rows
before ORDER BY/LIMIT. NULLs compare equal for duplicate removal; no SQLite
storage-affinity conversion is introduced. DISTINCT ON and unsupported modifiers
remain refusals. The supported join forms and refusals are listed in [SQL coverage](docs/SQL-COVERAGE.md).
Unsupported combinations are explicit errors.
Grouped name resolution accepts references to actual grouping keys and uses
bounded NULL/type facts for provably irrelevant dependencies. It does not adopt
arbitrary representative-row selection for ungrouped bare columns.
HAVING resolves unambiguous input names before SELECT aliases. CTEs and subqueries
keep their own binding scopes. Ordinary unary + preserves its operand.

## Arithmetic, NULL and casts

Int/Int division truncates toward zero. Internal INTEGER intermediates remain
exact within precision 38, including MIN/-1 as an intermediate 2^63 value.
Returned/stored INTEGER values must fit BIGINT; no lossy REAL fallback occurs. Int and REAL division by zero return NULL. DECIMAL division retains its
zero-divisor error. A literal such as `1.0` is DECIMAL; `CAST(1 AS REAL)` is REAL.

Remainder accepts Int/REAL and ordinary NULL coercion. It follows the dividend's
sign; MIN%-1 is zero. REAL operands individually convert by saturating, truncating
conversion to signed 64-bit integers. An Int operand is never first rounded to
REAL. A converted zero divisor returns NULL; the result is REAL if either operand
is REAL. DECIMAL/text/blob/boolean/date remainder is unsupported. Nonfinite
arithmetic remains rejected.

Untyped NULL output preserves row cardinality as Arrow Null, including all-NULL
arithmetic and supported SUM/AVG inputs. Required operands still bind and execute;
NULL does not hide their errors. Sorting statically NULL keys preserves ties.
SUM/AVG use wide accumulator storage, but their precision policy depends on the
input type. In this export's current exact-integer runtime, INTEGER running totals
enforce precision 38 after each addition and when partial totals merge. This
applies to INTEGER SUM and the INTEGER accumulation used by AVG; a prefix that
exceeds 38 digits errors even if later cancellation would bring the completed
total back into range. BIGINT narrowing remains a returned/stored-value check.
DECIMAL retains its separately approved wide-accumulator/finalization policy.
Counts and unsupported floating aggregates retain their explicit checks; this
is not SQLite's entire numeric policy. The earlier blanket final-only SUM/AVG
description does not describe the current INTEGER implementation.

INTEGER/REAL/TEXT casts follow the approved SQLite conversion paths where
supported. TEXT/BLOB-to-REAL conversion intentionally follows the pinned SQLite
rounding, not a claim of strictly correctly rounded IEEE conversion. Boundary
oracle fixtures retain the exact results. Text/numeric coercion is not generally
implicit outside these supported casts.

## Conditionals and exact comparison

CASE/COALESCE select lazily after ordinary name/type binding. NULLIF returns its
first operand or NULL; its second operand contributes comparison, never result
type. Provably unreachable alternatives do not promote the selected result:
`COALESCE(nonnullable_bigint, CAST(5 AS REAL))` preserves Int, including
9007199254740993. Proofs use bounded planner/schema facts, not observed data.
Outer joins clear unsafe non-NULL facts. Unknown proofs never justify rounding.

A reachable, potentially row-dependent mixture of Int and REAL in a conditional
returns explicit unsupported for every consumer, including bare projection,
arithmetic, aggregates, CAST and comparison. This temporary guard prevents known
precision/type-dependent wrong answers until tagged per-row values are ready.
It does not reject a proved-dead alternative or a homogeneous result. These
refusals are failures in every reported compatibility denominator.

Ordinary Int/REAL equality and ordering compare the original integer against the
exact binary floating value in both operand orders, without first rounding Int to
REAL. NULLIF and IN-lists reuse exact comparison. DECIMAL extensions and operand
error order are unchanged.

Mixed Int/REAL IN (SELECT ...) and NOT IN (SELECT ...) are explicitly unsupported
in either operand order, both projected and filtered, including empty sets and
typed NULL. SQLite's IN-subquery affinity is intentionally not emulated. Same-type
membership and untyped NULL membership retain their existing rules. RHS arity and
binding errors are checked normally; text-vs-numeric stays a binding error.
Empty literal IN () returns false and NOT IN () true, with normal left binding.

## Local files and Python

Python connections enable local `read_parquet` and bounded UTF-8, headered
`read_csv`; CSV columns are TEXT, without inferred numeric types or NULL tokens.
The native session protocol leaves these readers disabled by default. Temporary
materialization belongs to the query/session and is cleaned up on completion.
Python exception guidance changes wording only; it does not widen accepted SQL.
See the quickstart and package tests for the supported API and file behavior.
