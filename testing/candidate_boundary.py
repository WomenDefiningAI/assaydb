"""Verify separate candidate/build, controller/tool and frozen-input identities.

Read-only admission adapter, not a builder or suite runner. Use a reviewer-owned
policy and its independently pinned SHA. Never import candidate/archived Python.
Full-suite integration and real-run validation remain pending.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from result_contract import digest, require


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def confined(root, relative):
    rel=Path(relative)
    require(not rel.is_absolute() and rel.parts and '..' not in rel.parts, 'unsafe mapped path')
    path=root/rel
    require(not any(p.is_symlink() for p in [path,*path.parents]), 'mapped symlink refused')
    require(path.resolve().is_relative_to(root.resolve()), 'mapped path escapes root')
    return path


def git(root, *args):
    return subprocess.check_output(['git','-C',str(root),*args],text=True).strip()


def candidate_identity(root, policy):
    require(git(root,'rev-parse','HEAD')==policy['commit'], 'substituted candidate commit')
    require(git(root,'rev-parse','HEAD^{tree}')==policy['tree'], 'substituted candidate tree')
    names=subprocess.check_output(['git','-C',str(root),'ls-files','-z']).decode().rstrip('\0').split('\0')
    require(set(names)==set(policy['files']), 'candidate file inventory mismatch')
    require(not git(root,'status','--porcelain','--untracked-files=all'), 'candidate checkout dirty')
    # Includes ignored files: no hidden build/import controls in a fresh source checkout.
    require(not git(root,'ls-files','--others'), 'candidate has untracked or ignored files')
    for name, expected in policy['files'].items():
        path=confined(root,name)
        require(path.is_file() and digest(path)==expected, 'candidate source mismatch: '+name)
    return fingerprint(policy['files'])


def controller_identity(root, policy):
    require(git(root,'rev-parse','HEAD')==policy['commit'], 'substituted controller commit')
    require(git(root,'rev-parse','HEAD^{tree}')==policy['tree'], 'substituted controller tree')
    actual={}
    for directory in policy['tool_directories']:
        folder=confined(root,directory)
        require(folder.is_dir(), 'missing controller tool directory')
        for path in folder.rglob('*'):
            require(not path.is_symlink(), 'controller tool symlink')
            if path.is_file():actual[path.relative_to(root).as_posix()]=digest(path)
    require(actual==policy['files'] and bool(actual), 'controller tool inventory mismatch')
    return fingerprint(actual)


def frozen_identity(root, policy):
    require({'slt-scope','hunt-plan','sqlite-oracle','query-manifest','dataset-manifest'} <= set(policy), 'missing frozen input policy')
    actual={}
    for role, item in policy.items():
        path=confined(root,item['path'])
        require(path.is_file() and digest(path)==item['sha256'], 'frozen input mismatch: '+role)
        actual[role]=dict(path=item['path'],sha256=item['sha256'])
    return fingerprint(actual)


def verify_build(root, policy, candidate_sha, tools_sha):
    path=confined(root,policy['receipt']['path'])
    require(path.is_file() and digest(path)==policy['receipt']['sha256'], 'build receipt pin mismatch')
    receipt=json.loads(path.read_text())
    require(receipt['schema']=='assay-separated-build/v1', 'unsupported build receipt')
    for key in ('candidate_commit','candidate_tree','controller_commit','environment_sha256','command'):
        require(receipt[key]==policy[key], 'build provenance mismatch: '+key)
    require(receipt['source_before_sha256']==receipt['source_after_sha256']==candidate_sha, 'build source identity drift')
    require(receipt['controller_tools_sha256']==tools_sha, 'build controller tool mismatch')
    require(receipt['returncode']==0 and type(receipt['returncode']) is int, 'build failed')
    log=confined(root,receipt['cargo_events']['path'])
    require(log.is_file() and digest(log)==receipt['cargo_events']['sha256'], 'build event hash mismatch')
    events={}
    for line in log.read_text().splitlines():
        event=json.loads(line)
        if event.get('reason')=='compiler-artifact' and event.get('executable'):
            name=event['target']['name']
            if name in ('assaydb','assaydb-session'):
                require(name not in events, 'duplicate build artifact')
                events[name]=event
    require(set(events)==set(receipt['binaries'])=={'assaydb','assaydb-session'}, 'incomplete native build')
    binaries={}
    for name,item in receipt['binaries'].items():
        require(events[name]['fresh'] is False, 'cached native artifact')
        require(events[name]['executable']==item['original_executable'], 'build artifact path mismatch')
        binary=confined(root,item['path'])
        require(binary.is_file() and digest(binary)==item['sha256']==policy['binaries'][name], 'binary identity mismatch')
        binaries[name]=item['sha256']
    return binaries


def verify(policy, candidate, controller, inputs, build):
    candidate_sha=candidate_identity(candidate,policy['candidate'])
    tools_sha=controller_identity(controller,policy['controller'])
    inputs_sha=frozen_identity(inputs,policy['inputs'])
    bp=policy['build']
    require(bp['candidate_commit']==policy['candidate']['commit'] and bp['candidate_tree']==policy['candidate']['tree'], 'build/candidate mapping mismatch')
    require(bp['controller_commit']==policy['controller']['commit'], 'build/controller mapping mismatch')
    binaries=verify_build(build,bp,candidate_sha,tools_sha)
    # Recheck after reading receipts; this does not replace runtime isolation.
    require(candidate_identity(candidate,policy['candidate'])==candidate_sha, 'candidate changed during verification')
    require(controller_identity(controller,policy['controller'])==tools_sha, 'controller changed during verification')
    require(frozen_identity(inputs,policy['inputs'])==inputs_sha, 'inputs changed during verification')
    return dict(schema='assay-candidate-boundary/v1',candidate_commit=policy['candidate']['commit'],
                candidate_tree=policy['candidate']['tree'],controller_commit=policy['controller']['commit'],
                source_inventory_sha256=candidate_sha,controller_tools_sha256=tools_sha,
                frozen_mapping_sha256=inputs_sha,binaries=binaries,
                status='identity-verified-only; no suite execution or acceptance')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--policy',type=Path,required=True)
    p.add_argument('--policy-sha256',required=True,help='Independent reviewer pin, not supplied by candidate')
    for role in ('candidate','controller','inputs','build'):
        p.add_argument('--'+role,type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    require(digest(a.policy)==a.policy_sha256,'reviewer policy pin mismatch')
    result=verify(json.loads(a.policy.read_text()),a.candidate.resolve(),a.controller.resolve(),a.inputs.resolve(),a.build.resolve())
    require(digest(a.policy)==a.policy_sha256,'policy changed during verification')
    result['policy_sha256']=a.policy_sha256
    with a.output.open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')


if __name__=='__main__':main()
