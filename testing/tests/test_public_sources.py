import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).parents[1]


class PublicSourceTests(unittest.TestCase):
    def test_frozen_sql_bytes_and_separate_scopes(self):
        manifest=json.loads((ROOT/'analysis/sql-manifest.json').read_text())
        self.assertEqual(len(manifest),31)
        self.assertEqual(sum(p.startswith('original/') for p in manifest),20)
        self.assertEqual(sum(p.startswith('exact-product/') for p in manifest),11)
        for path,sha in manifest.items():
            self.assertEqual(hashlib.sha256((ROOT/'analysis'/path).read_bytes()).hexdigest(),sha)

    def test_scope_is_complete(self):
        scope=json.loads((ROOT/'slt/full-scope.json').read_text())
        self.assertEqual(len(scope['files']),622)
        self.assertEqual(scope['fixed_total'],7421339)
        self.assertEqual(scope['applicable_total'],5939714)
        self.assertEqual(sum(x['records'] for x in scope['files']),7421339)

    def test_preserved_generator_pins(self):
        spec=importlib.util.spec_from_file_location('v3',ROOT/'hunt55k/generate_v3.py')
        v3=importlib.util.module_from_spec(spec);spec.loader.exec_module(v3)
        v2=v3.base_generator()
        self.assertEqual(v2.lit("a'b"),"'a''b'")
        self.assertEqual(v2.lit(-7),'(-7)')
        with self.assertRaises(ValueError):v2.query('SELECT 1; DROP TABLE t','test')
        with self.assertRaises(ValueError):v2.binary('a','--','b')
        # Missing frozen plan must refuse, never synthesize an alternative.
        if not (ROOT/'hunt55k/plan-v2.json').exists():
            with self.assertRaises(FileNotFoundError):v3.original_plan()


if __name__=='__main__':unittest.main()
