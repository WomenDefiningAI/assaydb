"""Run the unchanged package tests and quickstart in an installed environment."""
from __future__ import annotations

import argparse
import csv
import importlib.metadata
import io
import json
from pathlib import Path
import platform
import shutil
import sqlite3
import sys
import tempfile
import unittest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("Run in the fresh install virtual environment")
    if shutil.which("rustc") or shutil.which("cargo"):
        raise RuntimeError("Install verification must not have Rust on PATH")
    import assaydb
    import duckdb
    import pandas
    import polars
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not Path(assaydb.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()):
        raise RuntimeError("Assay did not import from the installed environment")
    suite = unittest.defaultTestLoader.discover(str(args.package / "tests"), pattern="test_*.py")
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    print(stream.getvalue(), end="")
    receipt = {
        "python": sys.version,
        "platform": {"system": platform.system(), "machine": platform.machine(), "libc": platform.libc_ver()},
        "versions": {name: importlib.metadata.version(name) for name in ("assaydb", "pyarrow", "pandas", "polars", "duckdb")},
        "sqlite": sqlite3.sqlite_version,
        "tests": {"run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped)},
        "rust_on_path": False,
    }
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    if result.testsRun != 12 or not result.wasSuccessful() or result.skipped:
        raise RuntimeError("Expected all twelve installed-package tests to pass without skips")

    with assaydb.connect() as con:
        con.execute("CREATE TABLE events (id INTEGER, label TEXT)")
        con.execute("INSERT INTO events VALUES (1, 'hello'), (2, 'world')")
        quickstart = con.sql("SELECT id,label FROM events ORDER BY id")
    assert quickstart.to_arrow().to_pylist() == [{"id": 1, "label": "hello"}, {"id": 2, "label": "world"}]

    rows = [(1, "one"), (2, "two"), (9007199254740993, "large")]
    with tempfile.TemporaryDirectory(prefix="assay-files-") as directory:
        directory = Path(directory)
        pq.write_table(pa.table({"id": [r[0] for r in rows], "label": [r[1] for r in rows]}), directory / "events.parquet")
        with (directory / "events.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["id", "label"])
            writer.writerows(rows)
        sq = sqlite3.connect(":memory:")
        sq.execute("CREATE TABLE events(id INTEGER,label TEXT)")
        sq.executemany("INSERT INTO events VALUES (?,?)", rows)
        expected = sq.execute("SELECT id,label FROM events WHERE id>1 ORDER BY id").fetchall()
        checks = []
        with assaydb.connect() as con, duckdb.connect() as reference:
            for kind in ("csv", "parquet"):
                path = str(directory / ("events." + kind)).replace("'", "''")
                # Assay INTEGER follows SQLite's signed 64-bit domain; DuckDB's
                # spelling for that same domain is BIGINT (INTEGER is 32-bit).
                sql = f"SELECT CAST(id AS INTEGER) AS id,label FROM read_{kind}('{path}') WHERE CAST(id AS INTEGER)>1 ORDER BY id"
                reference_sql = sql.replace("AS INTEGER", "AS BIGINT")
                actual = [tuple(row.values()) for row in con.sql(sql).to_arrow().to_pylist()]
                assert actual == reference.execute(reference_sql).fetchall() == expected, kind
                checks.append({"format": kind, "assay_sql": sql, "duckdb_sql": reference_sql, "rows": actual, "sqlite_agreement": True, "duckdb_agreement": True})
        sq.close()
    namespace = {"__name__": "__main__"}
    notebook = json.loads((args.package / "notebooks/quickstart.ipynb").read_text())
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            exec(compile("".join(cell["source"]), "quickstart.ipynb", "exec"), namespace)
    receipt.update({"quickstart": "passed", "notebook": "passed", "sample_references": checks})
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
