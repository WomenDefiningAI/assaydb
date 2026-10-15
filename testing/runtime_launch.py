"""Launch one reviewer-pinned collector invocation beneath the host lock.

Use a protected copy of these tools, outside the candidate checkout. The private
policy pins the invocation and overlay. No candidate Python is imported.
"""
import argparse
import os
from pathlib import Path
import runpy
import sys

from candidate_boundary import confined
from runtime_context import from_environment
from result_contract import require


def launch(context, suite):
    require(suite in ('slt', 'hunt-freeze', 'hunt-run', 'bounded-fixture'), 'unknown invocation')
    spec = context.policy['invocations'][suite]
    require(spec['classification'] in ('bounded-fixture', 'full-collection'), 'invocation classification')
    require((suite == 'bounded-fixture') == (spec['classification'] == 'bounded-fixture'), 'fixture/full invocation mismatch')
    context.check()
    root = context.roots['overlay']
    entry = confined(root, spec['entrypoint'])
    require(spec['entrypoint'] in context.policy['overlay_files'], 'unbound entrypoint')
    require(all(isinstance(x, str) for x in spec['argv']), 'invalid invocation arguments')
    # Arguments and absolute private input/output paths are part of the separately
    # reviewed policy. Never accept an arbitrary command from candidate metadata.
    old_cwd, old_argv, old_path = Path.cwd(), sys.argv[:], sys.path[:]
    try:
        os.chdir(root)
        sys.argv = [str(entry), *spec['argv']]
        sys.path.insert(0, str(root))
        try:
            runpy.run_path(str(entry), run_name='__main__')
        except SystemExit as error:
            require(error.code in (None, 0), 'collector failed: ' + str(error.code))
    finally:
        os.chdir(old_cwd)
        sys.argv, sys.path = old_argv, old_path
        context.postflight()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', required=True,
                        choices=('slt', 'hunt-freeze', 'hunt-run', 'bounded-fixture'))
    args = parser.parse_args()
    launch(from_environment(), args.suite)


if __name__ == '__main__':
    main()
