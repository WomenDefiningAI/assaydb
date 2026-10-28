#!/usr/bin/env python3
"""Generate the immutable exact-domain mini-corpus with installed DuckDB 1.5.6.

Run with the project Python. No engine output is an input to this generator.
Column labels/physical integer widths are intentionally not a cross-engine contract;
NULLs, order, multiplicities, exact decimals and Float64 AVG bits are compared.
"""
import datetime
import decimal
import hashlib
import json
from pathlib import Path
import struct


def cell(value):
    if isinstance(value, decimal.Decimal):
        return {"decimal": format(value, "f").rstrip("0").rstrip(".") if "." in format(value, "f") else str(value)}
    if isinstance(value, datetime.date):
        return {"date": value.isoformat()}
    if isinstance(value, float):
        return {"float_bits": struct.pack(">d", value).hex()}
    return value


def main():
    import duckdb
    if duckdb.__version__ != "1.5.6":
        raise SystemExit(f"requires DuckDB 1.5.6, found {duckdb.__version__}")
    root = Path(__file__).resolve().parents[1] / "tests/fixtures/offset_windows"
    source = (root / "cases.sql").read_text()
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    cases = []
    for part in source.split("-- case: ")[1:]:
        metadata, sql = part.split("\n", 1)
        case = json.loads(metadata)
        case["sql"] = sql.strip()
        try:
            result = connection.execute(case["sql"])
        except duckdb.Error as error:
            if not case.get("reference_error"):
                raise
            case["error"] = str(error)
            cases.append(case)
            continue
        if case.get("reference_error"):
            raise RuntimeError("reference error control unexpectedly succeeded")
        case["types"] = [str(column[1]) for column in result.description]
        case["rows"] = [[cell(value) for value in row] for row in result.fetchall()]
        cases.append(case)
    assert 30 <= len(cases) <= 80
    import math, random
    values = [0.0,-0.0,math.inf,-math.inf,math.nan,1.25,-1.25,2.675,-2.675,1.005,-1.005,1e308,-1e308,1.7976931348623157e308,5e-324,-5e-324]
    rng = random.Random(20261015)
    values += [math.ldexp(rng.uniform(-1,1),rng.randrange(-1074,1024)) for _ in range(64)]
    controls = []
    for value in values:
        for digits in [0,1,2,3,7,15,22,23,24,50,100,200,308,309,2147483647,-1,-2,-7,-15,-22,-23,-24,-100,-308,-309,-2147483648]:
            result = connection.execute("SELECT ROUND(?::DOUBLE,?::INTEGER)", [value,digits]).fetchone()[0]
            controls.append({"input": struct.pack(">d",value).hex(), "digits":digits, "output":struct.pack(">d",result).hex()})
    integer_boundaries = []
    for sql in ["SELECT ROUND(2147483647,-1)", "SELECT ROUND(CAST(2147483647 AS BIGINT),-1)", "SELECT ROUND(CAST(9223372036854775807 AS BIGINT),-1)"]:
        result = connection.execute(sql)
        integer_boundaries.append({"sql":sql,"types":[str(d[1]) for d in result.description],"rows":result.fetchall(),"assay_error":"unsupported: negative ROUND at integral-width boundary (reference precision policy differs)"})
    output = {"integer_width_controls": integer_boundaries, "float_controls": controls, "duckdb_version": duckdb.__version__, "cases_sha256": hashlib.sha256(source.encode()).hexdigest(), "cases": cases}
    (root / "expected.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    print(f"Generated {len(cases)} exact DuckDB {duckdb.__version__} reference cases")


if __name__ == "__main__":
    main()
