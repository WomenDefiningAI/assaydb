# External SF1 qualification inputs

Full contribution qualification requires separately provisioned SF1 Parquet data
and 19 frozen SQL/reference files. The latter are deliberately not distributed.
`tests/support/external_sf1_hashes.json` lists their relative names and SHA-256
checksums. Original query and reference bytes must not be edited or regenerated
from Assay output to satisfy a test.

Set `ASSAY_EXTERNAL_SF1_INPUTS` to the private reference directory,
`ASSAY_TPCH_SF1_DIR` to the Parquet directory, and optionally
`ASSAY_REFERENCE_PYTHON` to Python 3. Run `python scripts/external_sf1.py --check-all`
before qualification. Each test also verifies and consumes the same bytes.
Missing, changed or escaping reference paths fail explicitly.

The aggregate-modifier, join/set, ordered-set, regular-expression, statistical,
string, date-toolkit, window-frame and extra-window families retain their original cases and exact answers.
Their SF1 tests are mandatory in full qualification, including explicitly ignored
driver tests. A default test invocation alone is not full qualification.
Follow each driver test's documented data-directory environment variable as well.
No reduced query set, unsupported answer or skipped test counts as a pass.
