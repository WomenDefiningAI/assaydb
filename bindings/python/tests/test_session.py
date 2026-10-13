"""Run against the installed wheel, not a source-tree import or reference engine."""
import concurrent.futures
from decimal import Decimal
import importlib.util
from pathlib import Path
import tempfile
import unittest

import assaydb
import pyarrow as pa


class NativeSessionTests(unittest.TestCase):
    def test_dml_arrow_lifetime_and_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            sentinel = Path(directory) / "keep.txt"
            sentinel.write_text("caller-owned")
            with assaydb.connect(directory=directory) as connection:
                connection.execute("CREATE TABLE t (i INTEGER, s TEXT)")
                result = connection.execute("INSERT INTO t VALUES (9007199254740993, 'large'), (NULL, 'null')")
                self.assertFalse(result.is_query)
                self.assertEqual(result.rowcount, 2)
                with self.assertRaises(assaydb.SQLError):
                    result.to_arrow()
                connection.execute("UPDATE t SET s='updated' WHERE i IS NOT NULL")
                saved = connection.sql("SELECT i,s FROM t ORDER BY s")
                self.assertEqual(saved.rowcount, 2)
                with assaydb.connect(directory=directory) as other:
                    with self.assertRaises(assaydb.SQLError):
                        other.execute("SELECT * FROM t")
                self.assertEqual(connection.execute("DELETE FROM t WHERE i IS NULL").rowcount, 1)
            table = saved.to_arrow()
            self.assertEqual(table.schema.field("i").type, pa.int64())
            self.assertEqual(table.to_pydict(), {"i": [None, 9007199254740993], "s": ["null", "updated"]})
            self.assertIs(saved.to_arrow(), table)
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["keep.txt"])
            self.assertTrue(connection.closed)
            connection.close()
            with self.assertRaises(assaydb.ConnectionClosedError):
                connection.execute("SELECT 1")

    def test_typed_values_not_stringified(self):
        table = assaydb.query("SELECT 9007199254740993 AS i, CAST('12.34' AS DECIMAL(10,2)) AS d, X'00FF' AS b, NULL AS n, CAST(1 AS REAL) AS f").to_arrow()
        self.assertEqual(table.to_pylist(), [{"i": 9007199254740993, "d": Decimal("12.34"), "b": b"\x00\xff", "n": None, "f": 1.0}])
        self.assertTrue(pa.types.is_decimal(table.schema.field("d").type))
        self.assertTrue(pa.types.is_binary(table.schema.field("b").type))
        self.assertTrue(pa.types.is_null(table.schema.field("n").type))

    def test_failures_are_typed_and_connection_remains_usable(self):
        with assaydb.connect() as connection:
            with self.assertRaises(assaydb.UnsupportedError) as refused:
                connection.execute("SELECT 1 IN (SELECT CAST(1 AS REAL))")
            self.assertTrue(str(refused.exception).startswith(refused.exception.native_message))
            self.assertIn("Hint:", str(refused.exception))
            self.assertIn("2^53", refused.exception.hint)
            with self.assertRaises(assaydb.SQLError):
                connection.execute("SELECT nonexistent")
            self.assertEqual(connection.execute("SELECT 7 AS n").to_arrow()["n"].to_pylist(), [7])
        with assaydb.connect(max_result_bytes=32) as connection:
            with self.assertRaises(assaydb.ResourceError):
                connection.execute("SELECT 1")
            # Export failure affects the completed result, not the catalog.
            connection.execute("CREATE TABLE t (i INTEGER)")
            self.assertEqual(connection.execute("INSERT INTO t VALUES (1)").rowcount, 1)

    def test_nested_insert_refuses_without_mutation_or_connection_loss(self):
        with assaydb.connect() as connection:
            connection.execute("CREATE TABLE t1 (x INTEGER, y VARCHAR(8))")
            nested = "INSERT INTO t1 INSERT INTO t1 VALUES (0, 'false')"
            with self.assertRaises(assaydb.UnsupportedError) as refused:
                connection.execute(nested)
            self.assertIn("INSERT source must be read-only", refused.exception.native_message)
            self.assertEqual(connection.sql("SELECT x,y FROM t1").to_arrow().to_pylist(), [])
            self.assertFalse(connection.closed)
            self.assertEqual(connection.execute("INSERT INTO t1 VALUES (1, 'alive')").rowcount, 1)
            with self.assertRaises(assaydb.UnsupportedError):
                connection.execute(nested)
            self.assertEqual(
                connection.sql("SELECT x,y FROM t1 ORDER BY x").to_arrow().to_pylist(),
                [{"x": 1, "y": "alive"}],
            )

    def test_one_statement_no_translated_sql(self):
        with assaydb.connect() as connection:
            with self.assertRaises(assaydb.AssayError):
                connection.execute("CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (1)")
            with self.assertRaises(assaydb.SQLError):
                connection.execute("SELECT * FROM t")

    def test_same_connection_serializes_concurrent_calls(self):
        with assaydb.connect() as connection:
            connection.execute("CREATE TABLE t(i INTEGER)")
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(connection.execute, f"INSERT INTO t VALUES ({i})") for i in range(12)]
                self.assertEqual(sum(f.result(timeout=30).rowcount for f in futures), 12)
            self.assertEqual(connection.execute("SELECT COUNT(*) AS n FROM t").to_arrow()["n"].to_pylist(), [12])

    def test_limits_are_explicit(self):
        with assaydb.connect(catalog_bytes=1) as connection:
            self.assertEqual(connection.sql("SELECT 1 AS n").to_arrow()["n"].to_pylist(), [1])
        for name in ("workers", "memory_bytes", "catalog_bytes", "max_result_bytes"):
            for value in (False, 0, -1, 1.5):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    assaydb.connect(**{name: value})

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "optional pandas not installed")
    def test_pandas_nullable_large_integer_preserved(self):
        with assaydb.connect() as connection:
            connection.execute("CREATE TABLE t(i INTEGER)")
            connection.execute("INSERT INTO t VALUES (9007199254740993),(NULL)")
            frame = connection.execute("SELECT i FROM t ORDER BY i NULLS LAST").to_pandas()
        self.assertEqual(int(frame.iloc[0, 0]), 9007199254740993)
        self.assertIn("pyarrow", str(frame.dtypes.iloc[0]))
        self.assertTrue(frame.isna().iloc[1, 0])

    @unittest.skipUnless(importlib.util.find_spec("polars"), "optional polars not installed")
    def test_polars_arrow_conversion(self):
        self.assertEqual(assaydb.query("SELECT 9007199254740993 AS i").to_polars().to_dicts(), [{"i": 9007199254740993}])


if __name__ == "__main__":
    unittest.main()
