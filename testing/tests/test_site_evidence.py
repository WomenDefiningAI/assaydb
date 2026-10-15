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
                             final_head='f'*40,candidate_commit='a'*40,candidate_tree='b'*40,
                             base_commit='c'*40,pr=1,reviewer='fixture-reviewer',
                             raw_evidence_reviewed=True,final_delta_reviewed=True)

    def record(self):return public_record(self.summary,self.acceptance,'d'*64,'e'*64,'f'*40)

    def test_missing_review_and_stale_head(self):
        for key,value in [('decision','pending'),('bundle_sha256','0'*64),('final_head','0'*40),('raw_evidence_reviewed',False),('reviewer','<script>')]:
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
        self.assertIn('22.000000',page.read_text())

    def test_pending_has_no_measurements(self):
        markup=render(PENDING)
        self.assertNotIn('<meter',markup)
        self.assertIn('No independently qualified',markup)


if __name__=='__main__':unittest.main()
