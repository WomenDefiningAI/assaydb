"""Validate the separately versioned, complete rotated SF10 sample population.

Protected adapters supply authenticated process/request/sample metadata; this
module recomputes statistics, never treating a supplied summary as evidence.
"""
import datetime as dt
import json
from zoneinfo import ZoneInfo
from timing import number, require
from rotated_statistics import analyze, ENGINES, IDS, ORDERS

PROTOCOL = 'assay-sf10-rotated-three-five/v1'


def validate_samples(path, policy):
    require(policy['protocol'] == PROTOCOL, 'unsupported rotated protocol')
    require(policy['workers'] == [1, 10], 'rotated worker scope mismatch')
    expected_phases = {f'{r}-timing-w{w}' for r in ('baseline','candidate') for w in (1,10)}
    require(set(policy['phases']) == expected_phases, 'rotated phase scope mismatch')
    for role in ('baseline','candidate'):
        for workers in (1,10):
            phase=policy['phases'][f'{role}-timing-w{workers}']
            require(phase['source_commit'] == policy[role+'_commit'], 'rotated source role mismatch')
            for field in ('tool_manifest_sha256','input_manifest_sha256'):
                require(phase[field] == policy[field], 'rotated manifest mismatch')
            require(phase['binary_sha256'] == policy['binary_sha256'][role], 'rotated binary role mismatch')
            require(phase['resource_config_sha256'] == policy['resource_config_sha256'][str(workers)],
                    'rotated worker resource mismatch')
            require(set(phase['requests']) == {f'r{r}-{e}-{q}' for r in (1,2,3) for e in ENGINES for q in IDS},
                    'rotated request scope mismatch')
    groups = {}
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            role, workers, rotation, engine, query, index = (row[k] for k in
                ('role', 'workers', 'round', 'engine', 'query', 'sample'))
            require(role in ('baseline', 'candidate') and type(workers) is int and
                    workers in (1, 10) and type(rotation) is int and rotation in (1, 2, 3) and
                    engine in ENGINES and query in IDS and type(index) is int and 0 <= index <= 5,
                    'unexpected rotated timing sample')
            require(row['order'] == list(ORDERS[rotation-1]), 'rotation order mismatch')
            key = role, workers, rotation, engine, query, index
            require(key not in groups, 'duplicate rotated sample')
            require(type(row['warmup']) is bool and row['warmup'] == (index == 0), 'warmup mismatch')
            require(row['status'] == 'passed', 'incomplete rotated measurement')
            phase = policy['phases'][f'{role}-timing-w{workers}']
            for field in ('source_commit', 'binary_sha256', 'tool_manifest_sha256',
                          'input_manifest_sha256', 'resource_config_sha256'):
                require(row[field] == phase[field], 'rotated identity mismatch: ' + field)
            require(row['host'] == policy['host'], 'rotated host mismatch')
            require(row['request_sha256'] == phase['requests'][f'r{rotation}-{engine}-{query}'],
                    'rotated request mismatch')
            # The original collector records phase completion, not individual
            # sample wall-clock timestamps. Do not manufacture sample times.
            require(row['collection_completed'] == phase['collection_completed'], 'completion receipt mismatch')
            stamp = dt.datetime.fromisoformat(row['collection_completed'].replace('Z', '+00:00'))
            require(stamp.tzinfo is not None and
                    stamp.astimezone(ZoneInfo('America/Los_Angeles')).date().isoformat() == policy['pacific_day'],
                    'rotated collection day mismatch')
            number(row['wall_seconds'], True)
            groups[key] = row
    require(len(groups) == 2 * 2 * 3 * 3 * 22 * 6, 'incomplete rotated population')
    phases = {}
    for role in ('baseline', 'candidate'):
        for workers in (1, 10):
            records = [dict(round=r, engine=e, query=q, status='completed',
                            seconds=[groups[role, workers, r, e, q, i]['wall_seconds'] for i in range(1, 6)])
                       for r in (1, 2, 3) for e in ENGINES for q in IDS]
            phases[f'{role}-timing-w{workers}'] = analyze(records)
    return dict(protocol=PROTOCOL, phases=phases, warmups=792, timed_samples=3960,
                ratio_direction='assaydb/reference', performance_qualified=False,
                limitation='Diagnostic measured ratios; no Alpha3 target or portability qualification.')
