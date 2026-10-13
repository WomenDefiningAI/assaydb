---
name: Wrong answer
about: A returned value disagrees with the expected result
---

Found a wrong answer? That's our top-priority bug. Open a wrong-answer report with
the SQL, the data and the result you expected. Provide a minimal reproduction so maintainers can investigate and add a regression.

Do not include secrets or private data. Use SECURITY.md for sensitive reports.

## Version and environment

Package version or source commit; OS/architecture; Python/native API; worker and memory options.

## Minimal data and SQL

```sql
-- CREATE TABLE, INSERT, then the query; or attach a small redistributable file.
```

## Actual result

Include column types, NULLs, duplicate rows and ordering.

## Expected result and reason

Show the expected values and an independent derivation or reference-engine version.
Mention dialect differences or unsupported statements earlier in the session.

## Reproduction frequency

Always or intermittent; include a seed if generated. A refusal/error is distinct
from a successfully returned wrong value, but both can be reported.
