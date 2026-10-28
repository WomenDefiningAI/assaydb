> Reference-generation commands below describe the original development workflow.
> Generators not included in this export require separately provisioned tooling;
> no historical validation result certifies this candidate.

# Float MIN/MAX OVER frames with NaN (integration test)

Integration candidate 1 merges the NaN total order (MIN skips NaN when a number
is present, MAX returns NaN when present, signed zeros equal, NaN peers) with
the window-frame engine. Bounded frames compute MIN/MAX with the frame engine's
monotone queue, ordered by the same Float comparator as grouped MIN/MAX.

`expected.json` holds DuckDB 1.5.6 answers for 11 frame shapes (bounded and
unbounded ROWS, Int RANGE offsets, FOLLOWING ends, ordering by a Float column
with NaN peers, whole partitions and prefix frames), each computing MIN/MAX of
two Float columns over `../nan_order/readings.parquet` (NaN payloads, signed
zeros, infinities, NULLs). DuckDB reads the file into an in-memory table first
because its Parquet statistics pushdown ignores NaN. Floats are compared as
binary64 bits with signed zeros folded to +0.0, as in the NaN fixture: Assay's
MIN/MAX key is canonical. The Rust test runs each case at workers 1/4/10,
batch sizes 1/7/8192, with and without forced spill.

Regenerate with `tools/testing/window_float_extremes_reference.py` using the
project's DuckDB 1.5.6 environment.
