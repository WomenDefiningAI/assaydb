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
        self.policy = dict(schema='assay-separated-runtime/v1',
                           host_lock=os.environ['ASSAY_ACTIVITY_LOCK_PATH'],
                           boundary=b.policy, interpreter_sha256=digest(Path(sys.executable).resolve()),
                           engine_environment={k:v for k,v in os.environ.items() if k.startswith(('ASSAYDB_','POLARS_','RAYON_','DATAFUSION_','OMP_','MKL_','DYLD_','LD_'))},
                           expanded_inputs={p.name:digest(p) for p in b.inputs.iterdir()},
                           overlay_files={'fixture.py':digest(self.overlay/'fixture.py')},
                           adapter_files={n:digest(adapter/n) for n in ('runtime_context.py','runtime_launch.py','candidate_boundary.py','result_contract.py','timing.py')},
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


