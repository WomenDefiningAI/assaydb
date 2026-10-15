import datetime
import socket
import shutil
import subprocess
from types import ModuleType
from unittest.mock import patch
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
from candidate_boundary import digest
from runtime_context import RuntimeContext, inherited_lock
from runtime_launch import launch
import test_candidate_boundary as fixtures


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.BoundaryTests('test_different_inventories_are_verified_separately')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        b = self.base
        self.overlay = b.build.parent/'overlay'; self.overlay.mkdir()
        (self.overlay/'fixture.py').write_text('assert 2 + 2 == 4\n')
        (b.inputs/'transfer.json').write_text('{"synthetic":true}\n')
        adapter = Path(__file__).parents[1]
        self.protected=b.build.parent/'protected';self.protected.mkdir()
        admission=('runtime_context.py','runtime_launch.py','runtime_launch.sh','candidate_boundary.py','result_contract.py','timing.py')
        for name in admission:shutil.copyfile(adapter/name,self.protected/name)
        self.policy = dict(schema='assay-separated-runtime/v1',
                           host_lock=os.environ['ASSAY_ACTIVITY_LOCK_PATH'],
                           boundary=b.policy, protected_launcher=str(self.protected/'runtime_launch.py'),
                           run_pair=dict(id='synthetic-pair',host=socket.gethostname(),pacific_day=datetime.datetime.now(ZoneInfo('America/Los_Angeles')).date().isoformat()),
                           interpreter_sha256=digest(Path(sys.executable).resolve()),
                           engine_environment={k:v for k,v in os.environ.items() if k.startswith(('ASSAYDB_','POLARS_','RAYON_','DATAFUSION_','OMP_','MKL_','DYLD_','LD_'))},
                           expanded_inputs={p.name:digest(p) for p in b.inputs.iterdir()},
                           overlay_files={'fixture.py':digest(self.overlay/'fixture.py')},
                           adapter_files={n:digest(adapter/n) for n in admission},
                           transfer_receipts={'transfer.json':digest(b.inputs/'transfer.json')},
                           invocations={'bounded-fixture':dict(classification='bounded-fixture',entrypoint='fixture.py',argv=[])})
        self.path=b.build.parent/'runtime-policy.json'
        self.roots=dict(candidate=b.candidate,controller=b.controller,inputs=b.inputs,build=b.build,overlay=self.overlay)

    def context(self):
        self.path.write_text(json.dumps(self.policy))
        return RuntimeContext(self.path,digest(self.path),self.roots)

    def test_launch_and_postflight(self):
        ctx=self.context();launch(ctx,'bounded-fixture')
        self.assertEqual(ctx.initial,ctx.postflight())

    def test_modified_expanded_bytes(self):
        ctx=self.context();(self.base.inputs/'transfer.json').write_text('changed')
        with self.assertRaisesRegex(ValueError,'runtime file changed'):ctx.check()

    def test_receipt_must_be_bound(self):
        self.policy['transfer_receipts']['omitted.json']='a'*64
        with self.assertRaisesRegex(ValueError,'receipt not bound'):self.context()

    def test_extra_overlay_module_refused(self):
        (self.overlay/'extra.py').write_text('raise RuntimeError()')
        with self.assertRaisesRegex(ValueError,'overlay inventory'):self.context()

    def test_wrong_candidate_refused(self):
        self.base.policy['candidate']['commit']='0'*40
        with self.assertRaisesRegex(ValueError,'candidate commit'):self.context()

    def test_policy_change_after_admission(self):
        ctx=self.context();self.path.write_text('{}')
        with self.assertRaisesRegex(ValueError,'policy changed'):ctx.check()

    def test_substituted_manifest(self):
        ctx=self.context();p=self.base.build/'session-manifest.json'
        p.write_text(json.dumps(dict(commit='0'*40,source={},sha256='a'*64)))
        with self.assertRaisesRegex(ValueError,'binary commit'):ctx.verify_manifest(self.base.build/'assaydb-session',p)

    def test_source_and_controller_roots_cannot_overlap(self):
        self.roots['overlay']=self.base.controller
        with self.assertRaisesRegex(ValueError,'separate'):self.context()

    def test_unheld_descriptor_refused(self):
        p=self.base.build/'unheld.lock';p.touch()
        with p.open() as stream:
            with self.assertRaisesRegex(ValueError,'not held'):inherited_lock(p)

    def test_full_run_cannot_masquerade_as_fixture(self):
        self.policy['invocations']['bounded-fixture']['classification']='full-collection'
        ctx=self.context()
        with self.assertRaisesRegex(ValueError,'fixture/full'):launch(ctx,'bounded-fixture')



    def test_ambient_import_paths_and_preloaded_replacement_ignored(self):
        (self.overlay/'helper.py').write_text("VALUE='pinned'\n")
        (self.overlay/'fixture.py').write_text("import helper; assert helper.VALUE == 'pinned'\n")
        self.policy['overlay_files']={p.name:digest(p) for p in self.overlay.iterdir()}
        ctx=self.context()
        marker=self.base.build/'injection-marker'
        attack=self.base.build/'ambient';attack.mkdir()
        for name in ('sitecustomize.py','usercustomize.py','runtime_context.py','candidate_boundary.py','json.py'):
            (attack/name).write_text('from pathlib import Path; Path('+repr(str(marker))+').touch(); raise RuntimeError("injected")\n')
        fake=ModuleType('helper');fake.__file__=str(attack/'helper.py');fake.VALUE='substituted'
        with patch.dict(os.environ,{'PYTHONPATH':str(attack),'PYTHONHOME':str(attack),'PATH':str(attack)}):
            with patch.dict(sys.modules,{'helper':fake}):launch(ctx,'bounded-fixture')
        self.assertFalse(marker.exists())

    def test_shell_bootstrap_ignores_python_startup_hooks(self):
        ctx=self.context();marker=self.base.build/'startup-marker'
        attack=self.base.build/'startup';attack.mkdir()
        (attack/'sitecustomize.py').write_text('from pathlib import Path; Path('+repr(str(marker))+').touch()\n')
        env=dict(os.environ,PYTHONPATH=str(attack),PYTHONHOME=str(attack),PATH=str(attack),
                 ASSAY_RUNTIME_POLICY=str(self.path),ASSAY_RUNTIME_POLICY_SHA256=ctx.sha,
                 ASSAY_RUNTIME_ROOTS=json.dumps({k:str(v) for k,v in self.roots.items()}))
        result=subprocess.run(['/bin/sh',str(self.protected/'runtime_launch.sh'),sys.executable,
                               '--suite','bounded-fixture'],env=env,pass_fds=ctx.lock_fds,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertFalse(marker.exists())

    def test_host_and_day_are_checked(self):
        self.policy['run_pair']['host']='not-this-host'
        with self.assertRaisesRegex(ValueError,'host mismatch'):self.context()
        self.policy['run_pair']['host']=socket.gethostname()
        self.policy['run_pair']['pacific_day']='2000-01-01'
        with self.assertRaisesRegex(ValueError,'Pacific day'):self.context()

    def test_invocation_change_does_not_change_stable_provenance(self):
        first=self.context().initial
        self.policy['invocations']['bounded-fixture']['argv']=['new-phase-only']
        self.assertEqual(first,self.context().initial)
        self.policy['boundary']['build']['environment_sha256']='b'*64
        with self.assertRaisesRegex(ValueError,'build provenance'):self.context()

    def test_two_process_freeze_then_hash_pinned_run(self):
        script = """import hashlib,json,sys
from pathlib import Path
from runtime_context import from_environment
context=from_environment()
receipt=Path(sys.argv[2])
if sys.argv[1]=='freeze':
    with receipt.open('x') as out:json.dump(context.initial,out,sort_keys=True)
else:
    assert hashlib.sha256(receipt.read_bytes()).hexdigest()==sys.argv[3]
    assert json.loads(receipt.read_text())==context.initial
context.postflight()
"""
        (self.overlay/'fixture.py').write_text(script)
        self.policy['overlay_files']['fixture.py']=digest(self.overlay/'fixture.py')
        receipt=self.base.build/'synthetic-freeze.json'
        spec=self.policy['invocations']['bounded-fixture']
        spec['argv']=['freeze',str(receipt)]
        freeze=self.context();launch(freeze,'bounded-fixture')
        old_sha=freeze.sha
        spec['argv']=['run',str(receipt),digest(receipt)]
        run=self.context()
        self.assertNotEqual(old_sha,run.sha)
        self.assertEqual(freeze.initial,run.initial)
        launch(run,'bounded-fixture')
        # A substantive identity change cannot be disguised as a phase change.
        self.policy['run_pair']['id']='different-pair'
        with self.assertRaisesRegex(ValueError,'isolated collector failed'):
            launch(self.context(),'bounded-fixture')

    def test_authenticated_native_environment_applied_by_bootstrap(self):
        self.policy['engine_environment']['RAYON_NUM_THREADS']='1'
        with patch.dict(os.environ,{'RAYON_NUM_THREADS':'1'}):
            ctx=self.context();launch(ctx,'bounded-fixture')

    def test_lock_identity_is_part_of_stable_provenance(self):
        ctx=self.context()
        lock=Path(self.policy['host_lock']).resolve()
        self.assertEqual(ctx.initial['host_lock'],dict(path=str(lock),device=lock.stat().st_dev,inode=lock.stat().st_ino))
