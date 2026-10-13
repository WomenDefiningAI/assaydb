import pathlib
import tempfile
import unittest

import assaydb
import pyarrow as pa
import pyarrow.parquet as pq


def literal(path):
    return "'" + str(path).replace("'", "''") + "'"


class LocalFileTests(unittest.TestCase):
    def test_default_parquet_and_csv_and_restriction(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            parquet = directory / "quoted'file.parquet"
            csv = directory / "events.csv"
            pq.write_table(pa.table({"id": pa.array([9007199254740993, None], type=pa.int64()), "label": ["large", "missing"]}), parquet)
            csv.write_text('id,label\n9007199254740993,"hello, world"\n2,"two\nlines"\n', encoding="utf-8")
            before = {p.name: p.read_bytes() for p in (parquet, csv)}
            with assaydb.connect(directory=directory) as connection:
                table = connection.sql(f"SELECT id,label FROM read_parquet({literal(parquet)}) ORDER BY id NULLS LAST").to_arrow()
                self.assertEqual(table["id"].to_pylist(), [9007199254740993, None])
                table = connection.sql(f"SELECT CAST(id AS INTEGER) AS id,label FROM read_csv({literal(csv)}) ORDER BY id").to_arrow()
                self.assertEqual(table.to_pydict(), {"id": [2, 9007199254740993], "label": ["two\nlines", "hello, world"]})
            with assaydb.connect(directory=directory, allow_local_files=False) as restricted:
                for name, path in (("read_parquet", parquet), ("read_csv", csv)):
                    with self.assertRaises(assaydb.UnsupportedError):
                        restricted.execute(f"SELECT * FROM {name}({literal(path)})")
            self.assertEqual({p.name: p.read_bytes() for p in directory.iterdir()}, before)

    def test_reader_error_cleanup_and_hint(self):
        with tempfile.TemporaryDirectory() as directory:
            csv = pathlib.Path(directory) / "bad.csv"
            csv.write_text('id,label\n1,"unterminated', encoding="utf-8")
            with assaydb.connect(directory=directory) as connection:
                with self.assertRaises(assaydb.SQLError):
                    connection.sql(f"SELECT * FROM read_csv({literal(csv)})")
                self.assertEqual(connection.sql("SELECT 2 AS n").to_arrow()["n"].to_pylist(), [2])
                with self.assertRaises(assaydb.UnsupportedError) as failure:
                    connection.sql("SELECT 1 IN (SELECT CAST(1 AS REAL))")
                self.assertIsInstance(failure.exception.hint, str)
            self.assertEqual([p.name for p in pathlib.Path(directory).iterdir()], ["bad.csv"])


if __name__ == "__main__":
    unittest.main()
