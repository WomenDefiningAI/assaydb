import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]))
from collector_adapter import adapt,adapt_compared,convert,digest,semantic_digest


class AdapterTests(unittest.TestCase):
    def test_slt_signatures_preserve_error_detail_and_full_answer_hash(self):
        row=dict(file='test/a',line=1,kind='query',record_index=0,sql_sha256='a'*64,
                 status='engine-error',detail='first error',reply_metadata={'status':'error'})
        first=adapt_compared(row,'slt',0)
        second=adapt_compared(dict(row,detail='different error'),'slt',0)
        self.assertNotEqual(first['failure_signature_sha256'],second['failure_signature_sha256'])
        good=dict(row,status='engine-pass',actual_sha256='b'*64,reply_metadata={'status':'ok'})
        self.assertNotEqual(adapt_compared(good,'slt',0)['answer_sha256'],
                            adapt_compared(dict(good,actual_sha256='c'*64),'slt',0)['answer_sha256'])
        self.assertEqual(semantic_digest(good,'slt'),semantic_digest(dict(good,validated_at_elapsed_seconds=10),'slt'))
        del good['actual_sha256']
        with self.assertRaises(ValueError):adapt_compared(good,'slt',0)

    def test_hunt_unordered_canonicalization_preserves_multiplicity_and_ordered_results(self):
        row=dict(ordered=False,native={'status':'ok','kind':'query','result':{'rows':[[1],[2],[1]]}})
        reordered=dict(row,native={'status':'ok','kind':'query','result':{'rows':[[1],[1],[2]]}})
        missing=dict(row,native={'status':'ok','kind':'query','result':{'rows':[[1],[2]]}})
        self.assertEqual(semantic_digest(row,'hunt55k'),semantic_digest(reordered,'hunt55k'))
        self.assertNotEqual(semantic_digest(row,'hunt55k'),semantic_digest(missing,'hunt55k'))
        self.assertNotEqual(semantic_digest(dict(row,ordered=True),'hunt55k'),
                            semantic_digest(dict(reordered,ordered=True),'hunt55k'))

    def test_slt_outcomes_preserved(self):
        for original,status in [('engine-pass','passed'),('engine-wrong-answer','wrong'),('session-failure','error'),('engine-unsupported','unsupported'),('session-unavailable','not-run')]:
            row=dict(file='test/a',line=1,kind='query',record_index=0,sql_sha256='a'*64,status=original)
            result=adapt(row,'slt',0)
            self.assertEqual(result['status'],status);self.assertEqual(result['original_status'],original)

    def test_resource_internal_and_float_errors_remain_errors(self):
        for status in ('engine-resource','engine-internal','engine-error'):
            row=dict(file='test/a',line=1,kind='query',record_index=0,
                     sql_sha256='a'*64,status=status,detail='expected numeric type, got Float')
            result=adapt(row,'slt',0)
            self.assertEqual(result['status'],'error')
            self.assertEqual(result['original_status'],status)
            # No message-based downgrade of historical Float errors.
            self.assertIn('original_record_sha256',result)

    def test_unknown_and_auxiliary_refused(self):
        with self.assertRaises(ValueError):adapt(dict(kind='metamorphic'),'hunt55k',0)
        with self.assertRaises(KeyError):adapt(dict(status='surprise'),'slt',0)

    def test_hunt_sql_identity(self):
        row=dict(kind='query',dataset=0,index=0,family='integer',ordered=True,sql='SELECT 1',sql_sha256='0'*64,outcome={'status':'pass'})
        with self.assertRaisesRegex(ValueError,'hash'):adapt(row,'hunt55k',0)
        row['sql_sha256']=hashlib.sha256(row['sql'].encode()).hexdigest()
        self.assertEqual(adapt(row,'hunt55k',0)['status'],'passed')

    def test_scope_and_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);raw=root/'raw';scope=root/'scope'
            row=dict(file='test/a',line=1,kind='query',record_index=0,sql_sha256='a'*64,status='conditional-not-executed')
            raw.write_text(json.dumps(row)+'\n')
            target=dict(id=['test/a',1,'query',0],sql_sha256='a'*64)
            scope.write_text(json.dumps(target)+'\n')
            with self.assertRaisesRegex(ValueError,'exclusion'):convert(raw,scope,root/'failed','slt',digest(raw),digest(scope))
            target['exclusion_reason']='onlyif-other-engine';scope.write_text(json.dumps(target)+'\n')
            result=convert(raw,scope,root/'valid','slt',digest(raw),digest(scope))
            self.assertEqual(result['counts']['excluded'],1)
            with self.assertRaises(FileExistsError):convert(raw,scope,root/'valid','slt',digest(raw),digest(scope))


if __name__=='__main__':unittest.main()
