import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]))
from timing import validate_samples
from timing_fixture import fixture


class TimingTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.policy,self.rows,self.path=fixture(Path(temp.name))

    def check(self):
        self.path.write_text(''.join(json.dumps(r)+'\n' for r in self.rows))
        return validate_samples(self.path,self.policy)

    def test_excludes_warmup(self):
        result=self.check()
        self.assertEqual(result['total_wall_ratio'],1)
        self.assertEqual(result['sum_query_medians_seconds']['candidate'],22)

    def test_missing_duplicate_and_extra_samples(self):
        original=self.rows[:]
        for rows in (original[:-1],original+[original[0]],original+[dict(original[0],sample=6)]):
            self.rows=rows
            with self.assertRaises(ValueError):self.check()

    def test_invalid_fields(self):
        for field,value in [('wall_seconds',float('nan')),('cpu_seconds',float('inf')),
                            ('spill_retries',1),('warmup',False),('host','other'),
                            ('commit','b'*40),('request_sha256','f'*64),
                            ('started','2026-01-02T12:00:00Z'),('status','error')]:
            with self.subTest(field=field):
                before=self.rows[0];self.rows[0]=dict(before,**{field:value})
                with self.assertRaises(ValueError):self.check()
                self.rows[0]=before

    def test_late_spill_and_memory(self):
        self.rows[-1]['spill_retries']=1
        with self.assertRaisesRegex(ValueError,'spill'):self.check()
        self.rows[-1]['spill_retries']=0
        self.rows[-1]['peak_memory_bytes']=1000
        self.policy['max_peak_memory_bytes']=500
        with self.assertRaisesRegex(ValueError,'memory'):self.check()

    def test_regression_recomputed_not_declared(self):
        for row in self.rows:
            if row['role']=='candidate' and not row['warmup']:row['wall_seconds']=2
        with self.assertRaisesRegex(ValueError,'regression'):self.check()


if __name__=='__main__':unittest.main()
