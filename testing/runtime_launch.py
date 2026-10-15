"""Isolated collector dispatch. Start through runtime_launch.sh with pinned Python."""
import sys

# This guard precedes non-builtin imports. The shell entry point supplies these
# flags before Python startup, so site hooks and PYTHONPATH never get consulted.
if __name__ == '__main__' and not (sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode):
    raise SystemExit('use runtime_launch.sh: isolated Python without site/bytecode is required')

import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess


def bootstrap():
    root = Path(__file__).resolve().parent
    path = Path(os.environ['ASSAY_RUNTIME_POLICY'])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != os.environ['ASSAY_RUNTIME_POLICY_SHA256']:
        raise ValueError('runtime policy pin mismatch before imports')
    policy = json.loads(raw)
    expected = policy['adapter_files']
    required = {'runtime_launch.py', 'runtime_launch.sh', 'runtime_context.py',
                'candidate_boundary.py', 'result_contract.py', 'timing.py'}
    if set(expected) != required or {p.name for p in root.iterdir()} != required:
        raise ValueError('protected adapter directory must contain exactly pinned admission files')
    for name, sha in expected.items():
        p = root/name
        if p.is_symlink() or not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest() != sha:
            raise ValueError('admission file changed before imports: ' + name)
    # -I -S supplied only standard-library paths. Append, never prefer an
    # application directory over the standard library.
    sys.path.append(str(root))
    for key, value in policy['engine_environment'].items():
        if not isinstance(value, str) or not key.startswith(('ASSAYDB_', 'POLARS_', 'RAYON_', 'DATAFUSION_', 'OMP_', 'MKL_')):
            raise ValueError('unsafe engine environment key')
        os.environ[key] = value
    from runtime_context import from_environment
    return from_environment()


def clean_environment(context):
    environment = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C',
                   'ASSAY_RUNTIME_POLICY': str(context.path),
                   'ASSAY_RUNTIME_POLICY_SHA256': context.sha,
                   'ASSAY_RUNTIME_ROOTS': json.dumps({k:str(v) for k,v in context.roots.items()})}
    # Native settings must be explicit; never propagate Python, loader, shell,
    # credential or arbitrary ambient variables into the fresh process.
    for key, value in context.policy['engine_environment'].items():
        if not key.startswith(('ASSAYDB_', 'POLARS_', 'RAYON_', 'DATAFUSION_', 'OMP_', 'MKL_')):
            raise ValueError('unsafe engine environment key')
        environment[key] = value
    return environment


def invocation(context, suite):
    from candidate_boundary import confined
    from result_contract import require
    require(suite in ('slt', 'hunt-freeze', 'hunt-run', 'bounded-fixture'), 'unknown invocation')
    spec = context.policy['invocations'][suite]
    require(spec['classification'] in ('bounded-fixture', 'full-collection'), 'invocation classification')
    require((suite == 'bounded-fixture') == (spec['classification'] == 'bounded-fixture'), 'fixture/full invocation mismatch')
    context.check()
    entry = confined(context.roots['overlay'], spec['entrypoint'])
    require(spec['entrypoint'] in context.policy['overlay_files'], 'unbound entrypoint')
    require(all(isinstance(x, str) for x in spec['argv']), 'invalid invocation arguments')
    return spec, entry


def worker(context, suite):
    spec, entry = invocation(context, suite)
    root = context.roots['overlay']
    os.chdir(root)
    sys.path.append(str(root))
    sys.argv = [str(entry), *spec['argv']]
    try:
        try:
            runpy.run_path(str(entry), run_name='__main__')
        except SystemExit as error:
            if error.code not in (None, 0):
                raise ValueError('collector failed: ' + str(error.code)) from error
    finally:
        context.postflight()


def launch(context, suite):
    invocation(context, suite)
    # Never execute collectors in this process: even library callers may have
    # ambient cached modules. The child reauthenticates all pins before imports.
    script = Path(context.policy['protected_launcher']).resolve()
    from result_contract import digest, require
    require(script.name == 'runtime_launch.py' and
            digest(script) == context.policy['adapter_files']['runtime_launch.py'], 'launcher substitution')
    result = subprocess.run([sys.executable, '-I', '-S', '-B', str(script),
                             '--worker', '--suite', suite],
                            env=clean_environment(context), pass_fds=context.lock_fds,
                            cwd=script.parent)
    context.postflight()
    require(result.returncode == 0, 'isolated collector failed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', required=True,
                        choices=('slt', 'hunt-freeze', 'hunt-run', 'bounded-fixture'))
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    context = bootstrap()
    (worker if args.worker else launch)(context, args.suite)


if __name__ == '__main__':
    main()
