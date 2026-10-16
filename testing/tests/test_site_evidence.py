import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]))
from site_evidence import PENDING,START,END,public_record,render,stage
from timing import validate_samples
from timing_fixture import fixture


class SiteTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name)
        policy,_,path=fixture(self.root)
        self.summary=dict(candidate_commit='a'*40,candidate_tree='b'*40,base_commit='c'*40,
                          host='private-host',counts={},timing=validate_samples(path,policy))
        self.acceptance=dict(decision='accepted',bundle_sha256='d'*64,policy_sha256='e'*64,
                             reviewed_code_head='f'*40,candidate_commit='a'*40,candidate_tree='b'*40,
                             base_commit='c'*40,pr=1,reviewer='fixture-reviewer',
                             raw_evidence_reviewed=True,generated_output_reviewed=True)

    def record(self):return public_record(self.summary,self.acceptance,'d'*64,'e'*64,'f'*40)

    def test_missing_review_and_stale_head(self):
        for key,value in [('decision','pending'),('bundle_sha256','0'*64),('reviewed_code_head','0'*40),('raw_evidence_reviewed',False),('generated_output_reviewed',False),('reviewer','<script>')]:
            with self.subTest(key=key):
                old=self.acceptance[key];self.acceptance[key]=value
                with self.assertRaises(ValueError):self.record()
                self.acceptance[key]=old

    def test_private_fields_not_exported(self):
        self.assertNotIn('private-host',json.dumps(self.record()))

    def test_both_views_derive_from_record(self):
        page=self.root/'contribute.html';page.write_text('BEFORE'+START+END+'AFTER')
        record=self.record();stage(record,self.root)
        self.assertEqual(json.loads((self.root/'evidence/full-suite/latest.json').read_text()),record)
        self.assertEqual(page.read_text(),'BEFORE'+render(record)+'AFTER')
        self.assertNotIn('Reviewed PR evidence', page.read_text())
        stage(record,self.root)
        self.assertEqual(page.read_text(),'BEFOREAFTER')

    def test_pending_has_no_measurements(self):
        markup=render(PENDING)
        self.assertNotIn('<meter',markup)
        self.assertEqual(markup, '')

    def test_rotated_evidence_preserves_method_and_excludes_private_fields(self):
        comparison=dict(full_suite=True,denominator=22,full_suite_ratios=dict(
            total=1.05,geomean=1.12,confidence_95={'total':[1.04,1.06],'geomean':[1.11,1.13]},
            private_path='/private/fixture'))
        self.summary['timing']=dict(protocol='assay-sf10-rotated-three-five/v1',
            private_host='private-host',phases={f'{role}-timing-w{workers}':
                {'comparisons':{'assaydb_polars':comparison,'assaydb_duckdb':comparison}}
                for role in ('baseline','candidate') for workers in (1,10)})
        self.summary.update(acceptance_policy='infrastructure-no-regression/v1',
                            nonpass_categories={'hunt55k':{'native-error':1020,'oracle-error':25}})
        record=self.record();markup=render(record)
        self.assertNotIn('/private/',json.dumps(record));self.assertNotIn('private-host',json.dumps(record))
        self.assertEqual(markup, '')
        self.assertEqual(record['timing']['phases']['candidate-timing-w10']['polars']['total'], 1.05)
        self.assertFalse(record['release_qualified'])


if __name__=='__main__':unittest.main()
