> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Exact Float alternatives to integer literals

`cases.sql` contains 52 independent queries over inline CTE tables: 42 supported
cases, eight deliberately retained mixed-numeric refusals and two Float SUM cases
awaiting CV6. Nothing is dropped during reference generation. `expected.json`
records all DuckDB answers, including cases Assay must refuse or defer.

Regenerate with the project Python and installed DuckDB 1.5.6:

```sh
python3 tools/testing/mixed_float_literals_reference.py
```

The only reference SQL adaptation spells Assay REAL as DOUBLE PRECISION:
Assay REAL is binary64, whereas DuckDB/PostgreSQL REAL is binary32. Both source
and reference SQL, reference types, version and source hash are retained.
DECIMAL precision is allowed to widen from 10 to Assay's 38; scale and exact
values must match. INTEGER widths normalize to Assay Int64, without value loss.
All Float outputs require Float64, including empty results.
Reference Float cells store exact IEEE-754 bits as hexadecimal strings. The
first debug attempt exposed decimal JSON reparsing loss: DuckDB's exact
9007199254740991.0 became 9007199254740990.0 in serde_json's default reader,
while Assay returned the correct value. Bit encoding removes that harness error
without changing any reference value or comparison tolerance. That attempt also
exposed the session error display's repeated `unsupported:` prefix; assertions
now require both the prefix and the specific mixed-conditional message.

Ordered queries have unique row ordering or unique group keys. Other queries
compare multisets with duplicate multiplicity. The Rust matrix runs every
non-CV6 case at workers 1/4/10, batch sizes 1/7/8192, optimization off/on.

Intentional boundaries: DuckDB accepts the eight `unsupported` cases using its
own coercion rules; Assay keeps the approved temporary refusal for integer
expressions/columns and literals beyond 2^53. Larger exactly representable
integers are still excluded. NULLIF keeps its first operand's result type;
IN-lists return Boolean. This card changes neither ordinary large Int/Float
equality nor mixed IN-subquery refusal. Existing proved-dead conditional rules
also remain, including their integer result type.

The CAST-to-INTEGER and remainder cases use integral inputs to isolate literal
promotion from existing dialect differences. Assay's SQLite-style fractional
remainder is unchanged; PostgreSQL has no floating `%` operator. The two SUM
queries have independent answers and an explicitly ignored CV6 Rust test.

An attempted isolated PostgreSQL 17.11 reference cluster could not initialize:
the sandbox refused its shared-memory allocation. `postgres.json` retains the
failure; no PostgreSQL execution or agreement is claimed. No alternate transport
or sandbox workaround was attempted.
