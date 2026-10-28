#!/usr/bin/env python3
"""Generate CAST goldens from in-process DuckDB 1.5.6.

Every SQL case is retained, including declared refusals and the unchanged Bool
format difference. Floating results are compared as binary64 bits, without a
tolerance. Run with the canonical project .venv; no packages are installed.
"""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import struct


def encode(value):
    if isinstance(value, float):
        return {"f64": struct.pack(">d", value).hex()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    checkout = Path(__file__).resolve().parents[1]
    import duckdb

    if duckdb.__version__ != "1.5.6":
        raise SystemExit(f"requires DuckDB 1.5.6, found {duckdb.__version__}")
    root = checkout / "tests/fixtures/cast_spellings"
    source = (root / "cases.sql").read_text()
    cases = []
    setup = [
        "CREATE TABLE discounts (k INTEGER, l_discount DECIMAL(4,2))",
        "INSERT INTO discounts VALUES (1,0.05),(1,0.05),(2,-0.02),(2,NULL)",
        'CREATE TABLE lab_groups (group_id INTEGER, label VARCHAR)',
        "INSERT INTO lab_groups VALUES (11,'positive_low'),(22,'positive_high'),(33,'negative_low'),(44,'negative_high'),(55,'missing'),(66,'mixed')",
        'CREATE TABLE instruments (instrument_id INTEGER, group_id INTEGER)',
        'INSERT INTO instruments VALUES (101,11),(102,22),(103,33),(104,44),(105,55),(106,66),(201,11),(202,22),(203,33),(204,44)',
        'CREATE TABLE readings (instrument_id INTEGER, reading DECIMAL(12,8))',
        'INSERT INTO readings VALUES (101,0.07324999),(102,0.07325001),(103,-0.07324999),(104,-0.07325001),(105,NULL),(106,0.03),(201,0.07324999),(202,0.07325001),(203,-0.07324999),(204,-0.07325001),(105,NULL),(106,0.07),(101,0.07324999),(102,0.07325001),(103,-0.07324999),(104,-0.07325001),(106,0.11),(106,NULL)',
    ]
    with duckdb.connect() as connection:
        connection.execute("SET threads=1")
        for sql in setup:
            connection.execute(sql)
        for section in source.split("-- case: ")[1:]:
            metadata, sql = section.split("\n", 1)
            case = json.loads(metadata)
            case["sql"] = sql.strip()
            try:
                result = connection.execute(case["sql"])
                case["types"] = [str(column[1]) for column in result.description]
                case["rows"] = [[encode(v) for v in row] for row in result.fetchall()]
            except duckdb.ConversionException as error:
                if not case.get("reference_error"):
                    raise
                case["reference_error_text"] = str(error)
            else:
                if case.get("reference_error"):
                    raise RuntimeError(f"expected reference error: {case['id']}")
            cases.append(case)
    assert len(cases) == 137
    result = {
        "duckdb_version": duckdb.__version__,
        "cases_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "float_policy": "Exact binary64 bits, including signed zero.",
        "setup": setup,
        "cases": cases,
    }
    output = args.output or root / "expected-strings-distinct-float.json"
    # Preserve an earlier receipt if rerunning the reference ever changes it.
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if output.exists() and output.read_text() != rendered:
        raise SystemExit("existing reference differs; choose a new --output to retain both")
    output.write_text(rendered)
    print(f"wrote {len(cases)} cases to {output}")


if __name__ == "__main__":
    main()
