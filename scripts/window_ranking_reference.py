#!/usr/bin/env python3
"""Regenerate W1 goldens using pinned references, never the Assay implementation.

Run under the canonical activity wrapper with the pinned project Python. All
queries remain in cases.sql; a disagreement is recorded and makes generation
fail, never removed or silently normalized into a pass.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import struct
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default="/usr/bin/sqlite3")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from with_engine_activity import inherited_lock, lock_path
    if inherited_lock(lock_path()) is None:
        raise SystemExit("run under scripts/with_engine_activity.py")
    import duckdb

    if duckdb.__version__ != "1.5.6":
        raise SystemExit(f"requires DuckDB 1.5.6, found {duckdb.__version__}")
    sqlite_version = subprocess.check_output([args.sqlite, "--version"], text=True).strip()
    if not sqlite_version.startswith("3.51."):
        raise SystemExit(f"requires sqlite3 3.51, found {sqlite_version}")
    root = Path(__file__).resolve().parents[1] / "tests/fixtures/window_ranking"
    setup = (root / "setup.sql").read_text()
    source = (root / "cases.sql").read_text()
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    connection.execute(setup)
    sqlite_setup = setup.split("CREATE TABLE typed_wr", 1)[0]
    cases = []
    disagreements = []
    for section in source.split("-- case: ")[1:]:
        metadata, sql = section.split("\n", 1)
        case = json.loads(metadata)
        sql = sql.strip()
        rows = [list(row) for row in connection.execute(sql).fetchall()]
        case.update(sql=sql, rows=rows)
        if case["sqlite"]:
            raw = subprocess.check_output(
                [args.sqlite, "-json", ":memory:", sqlite_setup + sql], text=True
            )
            sqlite_rows = [list(row.values()) for row in json.loads(raw or "[]")]
            compare = lambda values: values if case["ordered"] else sorted(
                values, key=lambda row: json.dumps(row, ensure_ascii=False)
            )
            if compare(rows) != compare(sqlite_rows):
                case["sqlite_rows"] = sqlite_rows
                disagreements.append(case["id"])
        cases.append(case)
    result = {
        "duckdb_version": duckdb.__version__,
        "sqlite_version": sqlite_version,
        "setup_sha256": hashlib.sha256(setup.encode()).hexdigest(),
        "setup_sql": setup,
        "cases_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "row_number_policy": "Every reference ROW_NUMBER has a unique ORDER BY; canonical peer order is checked independently in Rust.",
        "float_policy": "Finite and signed-zero keys match SQL numeric equality. NaN payload distinctions follow existing Assay GROUP BY; see README.md and native Parquet boundary tests.",
        "disagreements": disagreements,
        "cases": cases,
    }
    import pyarrow as pa
    nan1 = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
    nan2 = struct.unpack(">d", bytes.fromhex("7ff8000000000002"))[0]
    connection.register("wr_float_boundary", pa.table({
        "id": list(range(8)), "f": [-0.0, 0.0, -0.0, nan1, nan1, nan2, None, 0.5],
    }))
    boundary_sql = "SELECT id, RANK() OVER (ORDER BY f NULLS LAST) AS r, DENSE_RANK() OVER (ORDER BY f NULLS LAST) AS d, ROW_NUMBER() OVER (PARTITION BY f ORDER BY id) AS n FROM wr_float_boundary ORDER BY id"
    duck_rows = [list(row) for row in connection.execute(boundary_sql).fetchall()]
    assay_policy = [[0,1,1,1],[1,1,1,2],[2,1,1,3],[3,5,3,1],[4,5,3,2],[5,7,4,1],[6,8,5,1],[7,4,2,1]]
    result["nan_boundary"] = {
        "sql": boundary_sql, "input_float_bits": ["8000000000000000","0000000000000000","8000000000000000","7ff8000000000001","7ff8000000000001","7ff8000000000002",None,"3fe0000000000000"],
        "duckdb_rows": duck_rows, "assay_grouping_policy_rows": assay_policy,
        "matches_duckdb": duck_rows == assay_policy,
        "reason": "Existing Assay GROUP BY preserves NaN payloads; window keys deliberately retain that equality contract. This explicitly reported difference is outside the 80 shared-domain goldens.",
        "sqlite": "NaN payloads are not representable by the SQLite SQL fixture interface.",
    }
    finite_sql = boundary_sql
    connection.execute("CREATE TEMP TABLE wr_finite_boundary AS SELECT * FROM wr_float_boundary WHERE id<3 OR id>5")
    finite_sql = finite_sql.replace("FROM wr_float_boundary", "FROM wr_finite_boundary")
    finite_rows = [list(row) for row in connection.execute(finite_sql).fetchall()]
    sqlite_finite_setup = "CREATE TABLE wr_finite_boundary(id INTEGER,f REAL); INSERT INTO wr_finite_boundary VALUES(0,-0.0),(1,0.0),(2,-0.0),(6,NULL),(7,0.5);"
    sqlite_finite_rows = [list(row.values()) for row in json.loads(subprocess.check_output([args.sqlite,"-json",":memory:",sqlite_finite_setup+finite_sql],text=True))]
    result["finite_zero_boundary"] = {"sql":finite_sql,"duckdb_rows":finite_rows,"sqlite_rows":sqlite_finite_rows}
    if finite_rows != sqlite_finite_rows:
        disagreements.append("finite_zero_boundary")
    output = args.output or root / "expected.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"wrote {len(cases)} cases; {sum(case['sqlite'] for case in cases)} SQLite cross-checks; disagreements={disagreements}")
    if disagreements:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
