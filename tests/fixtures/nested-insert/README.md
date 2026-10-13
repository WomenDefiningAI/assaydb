# Nested INSERT crash regression

`original-fuzz-input.bin` is the unchanged 212-byte saved fuzz artifact
`crash-331ad7e19c6246d467893113f7c819584aca67cd`, containing the flag byte
and eight NUL-separated requests. SHA-256: `8eb5df50b297aef6367d758976e092e359a88b4c93ad40234323395a6ea9a6b5`. This fixture is
original project fuzz output, not a benchmark input or upstream SLT mutation.

`minimal.sql` isolates the create-table and nested-INSERT statements. The
integration tests require explicit unsupported, byte-identical catalog files,
unchanged rows/accounting, a usable session afterward, and original campaign
cleanup. Adjacent nested UPDATE/DELETE, replacement, CTE, parenthesized, set-op
and subquery routes are checked separately. No new execution is claimed by this export review.
