import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]))
from collector_adapter import adapt,convert,digest


class AdapterTests(unittest.TestCase):
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
