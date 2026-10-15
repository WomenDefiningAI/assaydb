"""Validate a complete PR evidence bundle. Validation is not maintainer approval.

Run from a protected collector environment, never with publishing credentials in
candidate execution. Policy and expected identities are supplied by the reviewer.
"""
import argparse
from collections import Counter
import hashlib
import itertools
import json
from pathlib import Path
import re

STATUSES = ('passed', 'unsupported', 'wrong', 'error', 'not-run', 'excluded')
REQUIRED = ('slt', 'hunt55k', 'sf10-correctness', 'sf10-timing',
            'retail-original', 'retail-exact-product', 'standard', 'acceptance-small')
MINIMUM = {'slt': 7421339, 'hunt55k': 55000, 'sf10-correctness': 22,
           'sf10-timing': 22, 'retail-original': 40, 'retail-exact-product': 24,
           'standard': 1, 'acceptance-small': 43}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def artifact(root, spec):
    rel = Path(spec['path'])
    require(not rel.is_absolute() and '..' not in rel.parts, 'unsafe artifact path')
    path = root / rel
    require(path.is_file() and not any(p.is_symlink() for p in [path, *path.parents]), 'missing or linked artifact')
    require(digest(path) == spec['sha256'], 'artifact hash mismatch')
    return path


def records(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def validate(bundle, policy, root):
    require(bundle['schema'] == 'assay-pr-evidence/v1', 'unknown schema')
    for key in ('candidate_commit', 'candidate_tree', 'base_commit', 'host', 'tool_manifest_sha256', 'input_manifest_sha256'):
        require(bundle[key] == policy[key], 'identity mismatch: ' + key)
    for key in ('candidate_commit', 'candidate_tree', 'base_commit'):
        require(re.fullmatch('[0-9a-f]{40}', bundle[key]) is not None, 'invalid git identity')
    require(bundle['independent_test_host'] is True, 'independent execution missing')
    for key in ('tool_manifest', 'input_manifest'):
        path = artifact(root, bundle[key])
        require(digest(path) == policy[key + '_sha256'], 'manifest pin mismatch')
    require(set(bundle['suites']) == set(REQUIRED), 'missing or unexpected suite')
    require(set(policy['suites']) == set(REQUIRED), 'incomplete reviewer policy')
    summaries = {}
    for name in REQUIRED:
        suite = bundle['suites'][name]
        expected = policy['suites'][name]
        for key in ('version', 'scope_sha256', 'reference_version'):
            require(suite[key] == expected[key] and bool(suite[key]), name + ': method mismatch')
        require(type(expected['total']) is int and expected['total'] >= MINIMUM[name], name + ': narrowed scope')
        require(suite['host'] == policy['host'] == suite['baseline_host'], name + ': cross-host comparison')
        require(suite['pacific_day'] == suite['baseline_pacific_day'], name + ': comparison day mismatch')
        require(suite['baseline_commit'] == policy['base_commit'], name + ': stale baseline')
        require(suite['baseline_tool_manifest_sha256'] == policy['tool_manifest_sha256'], name + ': baseline tool mismatch')
        counts = Counter({status: 0 for status in STATUSES})
        candidate = records(artifact(root, suite['records']))
        baseline = records(artifact(root, suite['baseline_records']))
        for index, pair in enumerate(itertools.zip_longest(candidate, baseline)):
            current, previous = pair
            require(current is not None and previous is not None, name + ': truncated comparison')
            require(current['index'] == previous['index'] == index, name + ': duplicate or reordered index')
            require(current['id'] == previous['id'] and current['sql_sha256'] == previous['sql_sha256'], name + ': changed record identity')
            require(current['status'] in STATUSES and previous['status'] in STATUSES, name + ': unknown outcome')
            require(current['status'] not in ('wrong', 'error', 'not-run'), name + ': blocking outcome')
            require(previous['status'] not in ('wrong', 'error', 'not-run'), name + ': invalid baseline')
            require(previous['status'] != 'passed' or current['status'] == 'passed', name + ': regression')
            if 'excluded' in (current['status'], previous['status']):
                require(current['status'] == previous['status'] == 'excluded', name + ': changed exclusion')
                require(current.get('reason') in ('onlyif-other-engine', 'after-halt', 'control-only') and current.get('reason') == previous.get('reason'), name + ': unapproved exclusion')
                require(name == 'slt', name + ': exclusion outside frozen SLT')
            counts[current['status']] += 1
        require(sum(counts.values()) == expected['total'], name + ': incomplete denominator')
        require(dict(counts) == suite['counts'], name + ': reported counts differ from records')
        require(counts['excluded'] == expected['excluded'], name + ': changed exclusion denominator')
        if name == 'sf10-timing':
            # The private timing adapter must retain every raw sample. No scalar
            # from another host or an older campaign is a substitute.
            timing = json.loads(artifact(root, suite['timing']).read_text())
            require(timing['protocol'] == expected['timing_protocol'], 'timing protocol mismatch')
            require(timing['host'] == policy['host'], 'timing host mismatch')
            require(timing['candidate_commit'] == policy['candidate_commit'] and timing['base_commit'] == policy['base_commit'], 'timing source mismatch')
            require(timing['complete'] is True and timing['regression_gate_passed'] is True, 'timing gate incomplete or failed')
            artifact(root, timing['raw_samples'])
        summaries[name] = dict(counts)
    return {'schema': bundle['schema'], 'candidate_commit': bundle['candidate_commit'],
            'candidate_tree': bundle['candidate_tree'], 'base_commit': bundle['base_commit'],
            'host': bundle['host'], 'counts': summaries,
            'qualification': 'structurally-valid; maintainer review of raw evidence required'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('bundle', type=Path)
    p.add_argument('--policy', required=True, type=Path, help='Reviewer-owned expected identities and scope')
    p.add_argument('--output', required=True, type=Path, help='New sanitized summary for review; never overwrites')
    a = p.parse_args()
    summary = validate(json.loads(a.bundle.read_text()), json.loads(a.policy.read_text()), a.bundle.parent.resolve())
    with a.output.open('x') as stream:
        json.dump(summary, stream, indent=2)
        stream.write('\n')


if __name__ == '__main__':
    main()
