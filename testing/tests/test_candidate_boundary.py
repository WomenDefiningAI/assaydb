import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]))
from candidate_boundary import digest,fingerprint,git,verify


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        root=Path(temp.name).resolve()
        self.candidate=root/'candidate';self.controller=root/'controller'
        self.inputs=root/'inputs';self.build=root/'build'
        for p in (self.candidate,self.controller,self.inputs,self.build):p.mkdir()
        for repo,relative in ((self.candidate,'src/main.rs'),(self.controller,'runner/check.py')):
            path=repo/relative;path.parent.mkdir();path.write_text('synthetic fixture; never executed\n')
            subprocess.run(['git','init','-q',str(repo)],check=True)
            subprocess.run(['git','-C',str(repo),'add','.'],check=True)
            subprocess.run(['git','-C',str(repo),'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','Synthetic fixture'],check=True)
        def identity(repo,relative):
            return dict(commit=git(repo,'rev-parse','HEAD'),tree=git(repo,'rev-parse','HEAD^{tree}'),files={relative:digest(repo/relative)})
        cand=identity(self.candidate,'src/main.rs');ctrl=identity(self.controller,'runner/check.py');ctrl['tool_directories']=['runner']
        inputs={}
        for role in ('slt-scope','hunt-plan','sqlite-oracle','query-manifest','dataset-manifest'):
            (self.inputs/role).write_text('synthetic '+role)
            inputs[role]=dict(path=role,sha256=digest(self.inputs/role))
        events=[];bins={}
        for name in ('assaydb','assaydb-session'):
            (self.build/name).write_text('not an executable; synthetic '+name)
            bins[name]=dict(path=name,sha256=digest(self.build/name),original_executable='fixture-build/'+name)
            events.append(dict(reason='compiler-artifact',target={'name':name},fresh=False,executable=bins[name]['original_executable']))
        (self.build/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
        self.receipt=dict(schema='assay-separated-build/v1',candidate_commit=cand['commit'],candidate_tree=cand['tree'],
                          controller_commit=ctrl['commit'],environment_sha256='a'*64,
                          command=['cargo','build','--release','--locked','--offline','--bins','--message-format=json'],
                          source_before_sha256=fingerprint(cand['files']),source_after_sha256=fingerprint(cand['files']),
                          controller_tools_sha256=fingerprint(ctrl['files']),returncode=0,
                          cargo_events=dict(path='events.jsonl',sha256=digest(self.build/'events.jsonl')),binaries=bins)
        self.policy=dict(candidate=cand,controller=ctrl,inputs=inputs,
                         build={k:self.receipt[k] for k in ('candidate_commit','candidate_tree','controller_commit','environment_sha256','command')})
        self.policy['build']['binaries']={k:v['sha256'] for k,v in bins.items()}
        self.repin_receipt()

    def repin_receipt(self):
        (self.build/'receipt.json').write_text(json.dumps(self.receipt))
        self.policy['build']['receipt']=dict(path='receipt.json',sha256=digest(self.build/'receipt.json'))

    def check(self):return verify(self.policy,self.candidate,self.controller,self.inputs,self.build)

    def test_different_inventories_are_verified_separately(self):
        result=self.check()
        self.assertIn('no suite execution',result['status'])
        self.assertNotEqual(result['source_inventory_sha256'],result['controller_tools_sha256'])

    def test_substituted_candidate_commit(self):
        self.policy['candidate']['commit']='0'*40
        with self.assertRaisesRegex(ValueError,'candidate commit'):self.check()

    def test_changed_candidate_bytes(self):
        (self.candidate/'src/main.rs').write_text('substitute')
        with self.assertRaises(ValueError):self.check()

    def test_missing_candidate_file(self):
        (self.candidate/'src/main.rs').unlink()
        with self.assertRaises(ValueError):self.check()

    def test_changed_or_extra_tooling(self):
        path=self.controller/'runner/check.py';old=path.read_bytes();path.write_text('substitute')
        with self.assertRaisesRegex(ValueError,'tool inventory'):self.check()
        path.write_bytes(old);(path.parent/'extra.py').write_text('extra')
        with self.assertRaisesRegex(ValueError,'tool inventory'):self.check()

    def test_scope_and_missing_oracle(self):
        path=self.inputs/'slt-scope';old=path.read_bytes();path.write_text('substitute')
        with self.assertRaisesRegex(ValueError,'frozen input mismatch'):self.check()
        path.write_bytes(old);(self.inputs/'sqlite-oracle').unlink()
        with self.assertRaisesRegex(ValueError,'frozen input mismatch'):self.check()

    def test_substituted_binary(self):
        (self.build/'assaydb').write_text('substitute')
        with self.assertRaisesRegex(ValueError,'binary identity'):self.check()

    def test_rehashed_receipt_cannot_change_candidate(self):
        self.receipt['candidate_commit']='0'*40;self.repin_receipt()
        with self.assertRaisesRegex(ValueError,'build provenance'):self.check()

    def test_cached_build_and_source_drift(self):
        p=self.build/'events.jsonl';old=p.read_text();p.write_text(old.replace('"fresh": false','"fresh": true'))
        self.receipt['cargo_events']['sha256']=digest(p);self.repin_receipt()
        with self.assertRaisesRegex(ValueError,'cached native'):self.check()
        p.write_text(old);self.receipt['cargo_events']['sha256']=digest(p)
        self.receipt['source_after_sha256']='0'*64;self.repin_receipt()
        with self.assertRaisesRegex(ValueError,'source identity drift'):self.check()

    def test_path_escape_and_symlink(self):
        self.policy['inputs']['slt-scope']['path']='../outside'
        with self.assertRaisesRegex(ValueError,'unsafe mapped'):self.check()
        self.policy['inputs']['slt-scope']['path']='slt-scope'
        p=self.inputs/'slt-scope';p.unlink();p.symlink_to(self.inputs/'hunt-plan')
        with self.assertRaisesRegex(ValueError,'symlink'):self.check()

    def marker_hook(self):
        marker=self.build/'hook-ran'
        hook=self.build/'marker-hook.sh'
        # Harmless fixture only: even a regression can only create a temp marker.
        hook.write_text('#!/bin/sh\n: > "'+str(marker)+'"\n')
        hook.chmod(0o700)
        return marker,hook

    def config(self,key,value):
        subprocess.run(['/usr/bin/git','-C',str(self.candidate),'config',key,str(value)],check=True)

    def test_repo_fsmonitor_never_executes(self):
        marker,hook=self.marker_hook()
        self.config('core.fsmonitor',hook)
        self.check()
        self.assertFalse(marker.exists())

    def test_clean_filter_never_executes(self):
        # Commit the attributes before configuring a filter, so fixture setup
        # itself never invokes the marker. Admission must hash bytes directly.
        attrs=self.candidate/'.gitattributes';attrs.write_text('src/* filter=marker\n')
        subprocess.run(['/usr/bin/git','-C',str(self.candidate),'add','.gitattributes'],check=True)
        subprocess.run(['/usr/bin/git','-C',str(self.candidate),'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','Synthetic attributes'],check=True)
        c=self.policy['candidate'];c['files']['.gitattributes']=digest(attrs)
        c.update(commit=git(self.candidate,'rev-parse','HEAD'),tree=git(self.candidate,'rev-parse','HEAD^{tree}'))
        for key,value in [('candidate_commit',c['commit']),('candidate_tree',c['tree'])]:
            self.policy['build'][key]=self.receipt[key]=value
        self.receipt['source_before_sha256']=self.receipt['source_after_sha256']=fingerprint(c['files'])
        self.repin_receipt()
        marker,hook=self.marker_hook()
        self.config('filter.marker.clean',hook);self.config('filter.marker.process',hook)
        self.config('filter.marker.required','true')
        self.check()
        self.assertFalse(marker.exists())

    def test_environment_git_and_path_injection_ignored(self):
        marker,hook=self.marker_hook()
        fake=self.build/'git';fake.write_bytes(hook.read_bytes());fake.chmod(0o700)
        global_config=self.build/'global-config';global_config.write_text('[core]\n fsmonitor = '+str(hook)+'\n')
        injected={'PATH':str(self.build),'GIT_DIR':str(self.controller/'.git'),
                  'GIT_WORK_TREE':str(self.controller),'GIT_CONFIG_GLOBAL':str(global_config),
                  'GIT_CONFIG_COUNT':'1','GIT_CONFIG_KEY_0':'core.fsmonitor','GIT_CONFIG_VALUE_0':str(hook)}
        with patch.dict(os.environ,injected):self.check()
        self.assertFalse(marker.exists())

    def test_non_plumbing_git_operation_refused(self):
        with self.assertRaisesRegex(ValueError,'unapproved Git operation'):
            git(self.candidate,'status','--porcelain')


if __name__=='__main__':unittest.main()
