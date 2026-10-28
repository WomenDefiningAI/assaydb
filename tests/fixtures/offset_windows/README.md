# Window offset and rounding reference fixtures

The 80 SQL cases and their independent DuckDB 1.5.6 answers are retained.
The original facility-energy example `annual_energy_change` exercises CTEs,
a two-table join, year extraction, GROUP BY ordinal, exact Decimal product/SUM,
repeated LAG, DOUBLE division and ROUND. Annual totals are 120, 150, 90 and 180;
percentage changes are NULL, 25, -40 and 100. These were checked independently
by arithmetic, without using Assay output.

The Rust test retains workers 1/4/10, batches 1/7/8192 and in-memory/external
sorting, exact decimals, Float64 bits, NULLs, ordering and multiplicities.
The remaining 79 cases, 2,080 float controls and three integer-width controls
are unchanged. Unsupported cases remain explicit failures of support.
Generate with `python scripts/offset_windows_reference.py` using DuckDB 1.5.6.
