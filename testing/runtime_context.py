"""Protected runtime admission for separately pinned source, tools and inputs.

Collectors call this before importing their reviewed overlay and at postflight.
It does not classify SQL, rebuild an oracle, or grant full-run authorization.
All policies, paths and receipts are private collector artifacts, outside source.
"""
import datetime
import socket
from zoneinfo import ZoneInfo
import fcntl
import json
import os
import sys
from pathlib import Path

from candidate_boundary import confined, fingerprint, verify
from result_contract import digest, require


def inventory(root, expected):
    """Verify every enumerated byte; policies must enumerate transitive inputs."""
    require(bool(expected), 'empty runtime inventory')
    for name, sha in expected.items():
        path = confined(root, name)
        require(path.is_file() and digest(path) == sha, 'runtime file changed: ' + name)
    return fingerprint(expected)


def inherited_lock(path):
    stat = path.stat()
    with path.open('r') as probe:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise ValueError('host activity lock is not held')
    result = []
    for entry in Path('/dev/fd').iterdir():
        try:
            fd = int(entry.name)
            current = os.fstat(fd)
            if fd > 2 and (stat.st_dev, stat.st_ino) == (current.st_dev, current.st_ino):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result.append(fd)
        except (OSError, ValueError):
            continue
    require(bool(result), 'inherited host activity lock required')
    return tuple(result)


class RuntimeContext:
    def __init__(self, policy_path, policy_sha256, roots, *, enforce_imports=False):
        self.enforce_imports = enforce_imports
        self.path = Path(policy_path).resolve()
        self.sha = policy_sha256
        require(digest(self.path) == self.sha, 'runtime policy pin mismatch')
        self.policy = json.loads(self.path.read_text())
        require(self.policy['schema'] == 'assay-separated-runtime/v1', 'runtime schema')
        require(set(roots) == {'candidate', 'controller', 'inputs', 'build', 'overlay'}, 'runtime roots')
        self.roots = {key: Path(value).resolve() for key, value in roots.items()}
        require(len(set(self.roots.values())) == 5, 'runtime roots must be separate')
        for a, root in self.roots.items():
            require(all(a == b or not root.is_relative_to(other)
                        for b, other in self.roots.items()), 'nested runtime roots refused')
        self.lock_fds = inherited_lock(Path(self.policy['host_lock']))
        self.initial = self.check()

    def check(self):
        require(digest(self.path) == self.sha, 'runtime policy changed')
        inherited_lock(Path(self.policy['host_lock']))
        p, roots = self.policy, self.roots
        pair = p['run_pair']
        require(bool(pair['id']) and pair['host'] == socket.gethostname(), 'run host mismatch')
        require(pair['pacific_day'] == datetime.datetime.now(ZoneInfo('America/Los_Angeles')).date().isoformat(), 'run Pacific day mismatch')
        if self.enforce_imports:
            verify_import_origins(self)
        require(digest(Path(sys.executable).resolve()) == p['interpreter_sha256'], 'runtime interpreter changed')
        environment = {k:v for k,v in os.environ.items() if k.startswith(
            ('ASSAYDB_', 'POLARS_', 'RAYON_', 'DATAFUSION_', 'OMP_', 'MKL_', 'DYLD_', 'LD_'))}
        require(environment == p['engine_environment'], 'runtime engine environment changed')
        boundary = verify(p['boundary'], **{k: roots[k] for k in ('candidate', 'controller', 'inputs', 'build')})
        # The input receipt and every expanded input are independently pinned.
        # Hashing only a manifest is insufficient to admit its referenced bytes.
        inputs = inventory(roots['inputs'], p['expanded_inputs'])
        require(bool(p['transfer_receipts']), 'missing transfer receipts')
        for name, sha in p['transfer_receipts'].items():
            require(p['expanded_inputs'].get(name) == sha, 'transfer receipt not bound to verified inputs')
        overlay = inventory(roots['overlay'], p['overlay_files'])
        actual = set()
        for directory, dirs, files in os.walk(roots['overlay'], followlinks=False):
            base = Path(directory)
            require(not any((base/d).is_symlink() for d in dirs), 'overlay directory symlink')
            actual.update((base/f).relative_to(roots['overlay']).as_posix() for f in files)
        require(actual == set(p['overlay_files']), 'overlay inventory differs')
        adapter = Path(__file__).resolve().parent
        inventory(adapter, p['adapter_files'])
        require({'runtime_context.py', 'runtime_launch.py', 'runtime_launch.sh', 'candidate_boundary.py', 'result_contract.py', 'timing.py'} <= set(p['adapter_files']), 'incomplete admission adapter pins')
        return dict(boundary=boundary, expanded_inputs_sha256=inputs,
                    overlay_sha256=overlay, adapter_sha256=fingerprint(p['adapter_files']),
                    transfer_receipts=p['transfer_receipts'],
                    boundary_policy_sha256=fingerprint(p['boundary']), run_pair=pair,
                    host_lock=dict(path=str(Path(p['host_lock']).resolve()),
                                   device=Path(p['host_lock']).stat().st_dev,
                                   inode=Path(p['host_lock']).stat().st_ino),
                    interpreter_sha256=p['interpreter_sha256'],
                    engine_environment_sha256=fingerprint(environment))

    def verify_manifest(self, binary, manifest):
        self.check()
        p = self.policy['boundary']
        build = json.loads(Path(manifest).read_text())
        require(build['commit'] == p['candidate']['commit'], 'runtime binary commit mismatch')
        require(build['source'] == p['candidate']['files'], 'runtime binary source mismatch')
        expected = p['build']['binaries']['assaydb-session']
        require(digest(Path(binary)) == build['sha256'] == expected, 'runtime binary substitution')
        return build

    def source_identity(self):
        self.check()
        return dict(self.policy['boundary']['candidate']['files'])

    def postflight(self):
        require(self.check() == self.initial, 'runtime provenance changed')
        return self.initial


def verify_import_origins(context):
    require(sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode,
            'isolated interpreter without site/bytecode required')
    require(os.environ.get('PATH') == '/usr/bin:/bin' and
            not any(k.startswith(('PYTHON', 'DYLD_', 'LD_')) for k in os.environ),
            'unsanitized runtime environment')
    adapter = Path(__file__).resolve().parent
    overlay = context.roots['overlay']
    stdlib = Path(os.__file__).resolve().parent
    for name, module in tuple(sys.modules.items()):
        value = getattr(module, '__file__', None)
        if not value:
            spec = getattr(module, '__spec__', None)
            require(name in sys.builtin_module_names or getattr(spec, 'origin', None) in ('built-in', 'frozen'),
                    'unattributed loaded module: ' + name)
            continue
        path = Path(value).resolve()
        if path.is_relative_to(adapter):
            rel = path.relative_to(adapter).as_posix()
            require(context.policy['adapter_files'].get(rel) == digest(path), 'unbound admission module: ' + name)
        elif path.is_relative_to(overlay):
            rel = path.relative_to(overlay).as_posix()
            require(context.policy['overlay_files'].get(rel) == digest(path), 'unbound collector module: ' + name)
        else:
            require(path.is_relative_to(stdlib) and 'site-packages' not in path.parts,
                    'ambient module origin: ' + name)
    for name in ('candidate_boundary', 'result_contract', 'timing', 'runtime_context'):
        module = sys.modules.get(name)
        require(module is not None and Path(module.__file__).resolve() == adapter/(name+'.py'),
                'admission module origin mismatch: ' + name)


def from_environment():
    # Set only by the reviewed collector launcher, never by candidate execution.
    roots = json.loads(os.environ['ASSAY_RUNTIME_ROOTS'])
    return RuntimeContext(os.environ['ASSAY_RUNTIME_POLICY'],
                          os.environ['ASSAY_RUNTIME_POLICY_SHA256'], roots, enforce_imports=True)
