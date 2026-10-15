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
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from timing import validate_samples

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
        # The authority is the separately supplied reviewer policy, not a hash
        # echoed by the candidate. Verify actual scope bytes before consuming any
        # result. Candidate and baseline agreement alone cannot establish coverage.
        scope_path = artifact(root, suite['scope_records'])
        require(digest(scope_path) == expected['scope_sha256'], name + ': frozen scope pin mismatch')
        frozen_scope = records(scope_path)
        counts = Counter({status: 0 for status in STATUSES})
        candidate = records(artifact(root, suite['records']))
        baseline = records(artifact(root, suite['baseline_records']))
        for index, triple in enumerate(itertools.zip_longest(candidate, baseline, frozen_scope)):
            current, previous, target = triple
            require(current is not None and previous is not None and target is not None, name + ': truncated comparison or frozen scope')
            require(type(target['index']) is int and target['index'] == index, name + ': reordered frozen scope')
            for row in (current, previous):
                require(row['id'] == target['id'] and row['sql_sha256'] == target['sql_sha256'], name + ': record differs from frozen scope')
                reason = target.get('exclusion_reason')
                if reason is not None:
                    require(name == 'slt' and reason in ('onlyif-other-engine', 'after-halt', 'control-only'), name + ': invalid frozen exclusion')
                    require(row['status'] == 'excluded' and row.get('reason') == reason, name + ': record exclusion differs from frozen scope')
                else:
                    require(row['status'] != 'excluded' and row.get('reason') is None, name + ': exclusion absent from frozen scope')
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
        require(digest(scope_path) == expected['scope_sha256'], name + ': frozen scope changed during validation')
        require(sum(counts.values()) == expected['total'], name + ': incomplete denominator')
        require(dict(counts) == suite['counts'], name + ': reported counts differ from records')
        require(counts['excluded'] == expected['excluded'], name + ': changed exclusion denominator')
        if name == 'sf10-timing':
            timing_policy = dict(expected['timing_policy'])
            for key, value in {
                'host': policy['host'], 'candidate_commit': policy['candidate_commit'],
                'baseline_commit': policy['base_commit'],
                'tool_manifest_sha256': policy['tool_manifest_sha256'],
                'input_manifest_sha256': policy['input_manifest_sha256'],
                'pacific_day': suite['pacific_day'],
            }.items():
                require(timing_policy[key] == value, 'timing policy identity mismatch: ' + key)
            timing_summary = validate_samples(artifact(root, suite['timing_samples']), timing_policy)
        summaries[name] = dict(counts)
    return {'schema': bundle['schema'], 'candidate_commit': bundle['candidate_commit'],
            'candidate_tree': bundle['candidate_tree'], 'base_commit': bundle['base_commit'],
            'host': bundle['host'], 'counts': summaries, 'timing': timing_summary,
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
