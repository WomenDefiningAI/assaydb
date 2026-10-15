import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('contract', Path(__file__).parents[1] / 'result_contract.py')
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        def put(name, data):
            path = self.root / name
            path.write_text(data)
            return {'path': name, 'sha256': c.digest(path)}
        self.put = put
        inputs = put('inputs.json', '{}')
        tools = put('tools.json', '{}')
        self.policy = dict(candidate_commit='a'*40, candidate_tree='b'*40, base_commit='c'*40,
                           host='synthetic-fixture-host', tool_manifest_sha256=tools['sha256'], input_manifest_sha256=inputs['sha256'], suites={})
        self.bundle = dict(self.policy, schema='assay-pr-evidence/v1', independent_test_host=True,
                           tool_manifest=tools, input_manifest=inputs, suites={})
        self.record = dict(index=0, id='fixture', sql_sha256=hashlib.sha256(b'SELECT 1').hexdigest(), status='passed')
        rows = put('records.jsonl', json.dumps(self.record)+'\n')
        from timing_fixture import fixture
        timing_policy, timing_rows, timing_path = fixture(self.root)
        for key in ('tool_manifest_sha256', 'input_manifest_sha256'):
            timing_policy[key] = self.policy[key]
            for row in timing_rows:
                row[key] = self.policy[key]
        timing_samples = put(timing_path.name, ''.join(json.dumps(row)+'\n' for row in timing_rows))
        for name in c.REQUIRED:
            self.policy['suites'][name] = dict(version='fixture/v1', scope_sha256='d'*64, reference_version='fixture', total=1, excluded=0, timing_policy=timing_policy)
            self.bundle['suites'][name] = dict(self.policy['suites'][name], host=self.policy['host'], baseline_host=self.policy['host'], pacific_day='2026-01-01', baseline_pacific_day='2026-01-01', baseline_commit='c'*40, baseline_tool_manifest_sha256=tools['sha256'], records=rows, baseline_records=rows, timing_samples=timing_samples, counts={s:int(s=='passed') for s in c.STATUSES})

    def check(self):
        # Tiny synthetic fixtures test validation mechanics, never a suite result.
        with patch.object(c, 'MINIMUM', {name:1 for name in c.REQUIRED}):
            return c.validate(self.bundle, self.policy, self.root)

    def test_valid_structure_is_not_approval(self):
        self.assertIn('maintainer review', self.check()['qualification'])

    def test_full_scope_cannot_be_reduced(self):
        with self.assertRaisesRegex(ValueError, 'narrowed scope'):
            c.validate(self.bundle, self.policy, self.root)

    def test_missing_suite(self):
        del self.bundle['suites']['slt']
        with self.assertRaisesRegex(ValueError, 'missing'):
            self.check()

    def test_stale_candidate(self):
        self.bundle['candidate_commit']='e'*40
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            self.check()

    def test_cross_host(self):
        self.bundle['suites']['slt']['baseline_host']='other-host'
        with self.assertRaisesRegex(ValueError, 'cross-host'):
            self.check()

    def test_hash_tampering(self):
        (self.root/'records.jsonl').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            self.check()

    def test_blocking_outcomes(self):
        for status in ('wrong','error','not-run'):
            with self.subTest(status=status):
                row=dict(self.record,status=status)
                self.bundle['suites']['slt']['records']=self.put(status+'.jsonl',json.dumps(row)+'\n')
                with self.assertRaisesRegex(ValueError,'blocking outcome'):
                    self.check()

    def test_regression(self):
        self.bundle['suites']['slt']['records']=self.put('unsupported.jsonl',json.dumps(dict(self.record,status='unsupported'))+'\n')
        with self.assertRaisesRegex(ValueError,'regression'):
            self.check()

    def test_truncated_stream(self):
        self.bundle['suites']['slt']['records']=self.put('empty.jsonl','')
        with self.assertRaisesRegex(ValueError,'truncated'):
            self.check()

    def test_path_escape(self):
        self.bundle['suites']['slt']['records']=dict(path='../escape',sha256='0'*64)
        with self.assertRaisesRegex(ValueError,'unsafe'):
            self.check()

    def test_changed_exclusion(self):
        self.bundle['suites']['slt']['records']=self.put('excluded.jsonl',json.dumps(dict(self.record,status='excluded',reason='unsupported-feature'))+'\n')
        with self.assertRaises(ValueError):
            self.check()

    def test_changed_method(self):
        self.bundle['suites']['slt']['reference_version']='host-default'
        with self.assertRaisesRegex(ValueError,'method mismatch'):
            self.check()


if __name__ == '__main__':
    unittest.main()
