import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]))
import rotated_timing as rt
from rotated_statistics import analyze_pair


class RotatedTimingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'samples.jsonl'
        self.policy=dict(protocol=rt.PROTOCOL,workers=[1,10],host='fixture',pacific_day='2026-01-01',phases={})
        self.policy.update(candidate_commit='a'*40,baseline_commit='b'*40,
            tool_manifest_sha256='d'*64,input_manifest_sha256='e'*64,
            binary_sha256={'baseline':'c'*64,'candidate':'c'*64},
            resource_config_sha256={'1':'f'*64,'10':'f'*64})
        self.rows=[]
        for role in ('baseline','candidate'):
            for workers in (1,10):
                phase=dict(source_commit=('a' if role=='candidate' else 'b')*40,
                           binary_sha256='c'*64,tool_manifest_sha256='d'*64,
                           input_manifest_sha256='e'*64,resource_config_sha256='f'*64,requests={},
                           collection_completed='2026-01-01T12:00:00+00:00')
                self.policy['phases'][f'{role}-timing-w{workers}']=phase
                for r in (1,2,3):
                    for e in rt.ENGINES:
                        for q in rt.IDS:
                            phase['requests'][f'r{r}-{e}-{q}']='0'*64
                            for i in range(6):
                                self.rows.append(dict({k:v for k,v in phase.items() if k!='requests'},
                                    role=role,workers=workers,round=r,engine=e,query=q,sample=i,
                                    order=list(rt.ORDERS[r-1]),warmup=i==0,status='passed',
                                    host='fixture',request_sha256='0'*64,
                                    wall_seconds=1.0))

    def validate(self):
        self.path.write_text(''.join(json.dumps(r)+'\n' for r in self.rows))
        with patch.object(rt,'analyze',return_value={'fixture':True}) as compute:
            result=rt.validate_samples(self.path,self.policy)
        return result,compute

    def test_complete_population_preserves_all_dimensions(self):
        result,compute=self.validate()
        self.assertEqual(result['timed_samples'],3960)
        self.assertEqual(result['warmups'],792)
        self.assertEqual(compute.call_count,4)
        for call in compute.call_args_list:
            self.assertEqual(len(call.args[0]),198)
            self.assertTrue(all(len(r['seconds'])==5 for r in call.args[0]))
        self.assertFalse(result['performance_qualified'])

    def test_rejects_missing_duplicate_wrong_warmup_and_configuration(self):
        original=copy.deepcopy(self.rows)
        for mode in ('missing','duplicate','warmup','workers','round','order','wall_seconds','source_commit','status'):
            with self.subTest(mode=mode):
                self.rows=copy.deepcopy(original)
                if mode=='missing':self.rows.pop()
                elif mode=='duplicate':self.rows[-1]=self.rows[0]
                else:self.rows[0][mode]={'warmup':False,'workers':True,'round':True,'order':[],
                                        'wall_seconds':True,'source_commit':'0'*40,'status':'error'}[mode]
                with self.assertRaises(ValueError):self.validate()

    def test_estimator_uses_all_rounds_and_reports_both_ratios(self):
        vectors={(e,q):[[v]*5 for v in values] for e,values in
                 [('assaydb',[1.,2.,3.]),('polars',[2.,4.,6.])] for q in rt.IDS}
        stats,_=analyze_pair(vectors,rt.IDS,'polars',100)
        self.assertAlmostEqual(stats['total'],.5)
        self.assertAlmostEqual(stats['geomean'],.5)
        for bounds in stats['confidence_95'].values():
            for value in bounds:self.assertAlmostEqual(value,.5)

    def test_stale_matching_phase_and_rows_cannot_replace_bundle_identity(self):
        self.policy['phases']['candidate-timing-w1']['source_commit']='0'*40
        for row in self.rows:
            if row['role']=='candidate' and row['workers']==1:row['source_commit']='0'*40
        with self.assertRaisesRegex(ValueError,'source role mismatch'):self.validate()


if __name__=='__main__':unittest.main()
