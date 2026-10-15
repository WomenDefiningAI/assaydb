"""Recompute paired SF10 metrics from all samples; never accept receipt booleans."""
import datetime as dt
import json
import math
import statistics
from zoneinfo import ZoneInfo

PROTOCOL = 'assay-sf10-paired-five/v1'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def number(value, positive=False):
    require(type(value) in (float, int) and math.isfinite(value), 'nonfinite or nonnumeric metric')
    require(value > 0 if positive else value >= 0, 'invalid metric range')
    return value


def validate_samples(path, policy):
    require(policy['protocol'] == PROTOCOL, 'unsupported timing protocol')
    queries = policy['queries']
    require(set(queries) == {f'q{i:02}' for i in range(1, 23)} and len(queries) == 22, 'timing requires all 22 distinct queries')
    require(set(policy['max_wall_ratio']) == set(queries), 'missing timing limits')
    number(policy['max_total_wall_ratio'], True)
    groups = {}
    days = set()
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            role, query, index = row['role'], row['query'], row['sample']
            require(role in ('baseline', 'candidate') and query in queries, 'unknown timing scope')
            require(type(index) is int and 0 <= index <= 5, 'unexpected timing sample')
            require(type(row['warmup']) is bool and row['warmup'] == (index == 0), 'warmup boundary mismatch')
            key = (role, query, index)
            require(key not in groups, 'duplicate timing sample')
            require(row['host'] == policy['host'], 'timing host mismatch')
            require(row['commit'] == policy[role + '_commit'], 'timing source mismatch')
            for name in ('tool_manifest_sha256', 'input_manifest_sha256', 'workers', 'resource_config_sha256'):
                require(row[name] == policy[name], 'timing configuration mismatch: ' + name)
            require(row['request_sha256'] == policy['request_sha256'][query], 'timing query mismatch')
            stamp = dt.datetime.fromisoformat(row['started'].replace('Z', '+00:00'))
            require(stamp.tzinfo is not None, 'timing timestamp lacks timezone')
            days.add(stamp.astimezone(ZoneInfo('America/Los_Angeles')).date().isoformat())
            require(row['status'] == 'passed', 'failed timing sample')
            for name in ('wall_seconds', 'cpu_seconds'):
                number(row[name], True)
            number(row['peak_memory_bytes'])
            require(type(row['spill_retries']) is int and row['spill_retries'] == 0, 'spill retry')
            groups[key] = row
    require(len(groups) == 2 * 22 * 6, 'incomplete timing sample population')
    require(days == {policy['pacific_day']}, 'timing crosses collection day')
    output = {}
    totals = {'baseline': 0.0, 'candidate': 0.0}
    for query in queries:
        metrics = {}
        for role in totals:
            timed = [groups[role, query, i] for i in range(1, 6)]
            metrics[role] = {name: statistics.median(r[name] for r in timed)
                             for name in ('wall_seconds', 'cpu_seconds')}
            metrics[role]['peak_memory_bytes'] = max(r['peak_memory_bytes'] for r in timed)
            if 'max_peak_memory_bytes' in policy:
                require(metrics[role]['peak_memory_bytes'] <= number(policy['max_peak_memory_bytes'], True), 'timing memory limit')
            totals[role] += metrics[role]['wall_seconds']
        ratio = metrics['candidate']['wall_seconds'] / metrics['baseline']['wall_seconds']
        require(ratio <= number(policy['max_wall_ratio'][query], True), 'per-query timing regression: ' + query)
        output[query] = dict(metrics, wall_ratio=ratio)
    ratio = totals['candidate'] / totals['baseline']
    require(ratio <= policy['max_total_wall_ratio'], 'total timing regression')
    return dict(protocol=PROTOCOL, query_count=22, timed_samples_per_query=5,
                warmup_samples_per_query=1, ratio_direction='candidate/baseline',
                sum_query_medians_seconds=totals, total_wall_ratio=ratio, queries=output)
