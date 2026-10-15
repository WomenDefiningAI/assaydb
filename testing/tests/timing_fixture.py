"""Synthetic protocol fixtures; never publish these as measurements."""
import json
from timing import PROTOCOL


def fixture(root):
    queries = [f'q{i:02}' for i in range(1, 23)]
    policy = dict(protocol=PROTOCOL, queries=queries, host='synthetic-fixture-host',
                  candidate_commit='a'*40, baseline_commit='c'*40,
                  tool_manifest_sha256='d'*64, input_manifest_sha256='e'*64,
                  workers=1, resource_config_sha256='f'*64, pacific_day='2026-01-01',
                  request_sha256={q:'0'*64 for q in queries},
                  max_wall_ratio={q:1.15 for q in queries}, max_total_wall_ratio=1.1)
    rows=[]
    for role in ('baseline','candidate'):
        for query in queries:
            for index in range(6):
                rows.append(dict(role=role, query=query, sample=index, warmup=index==0,
                                 host=policy['host'], commit=policy[role+'_commit'],
                                 tool_manifest_sha256=policy['tool_manifest_sha256'],
                                 input_manifest_sha256=policy['input_manifest_sha256'],
                                 workers=1, resource_config_sha256='f'*64, request_sha256='0'*64,
                                 started='2026-01-01T12:00:00Z',status='passed',
                                 wall_seconds=999 if index==0 else 1,
                                 cpu_seconds=999 if index==0 else 1,
                                 peak_memory_bytes=123,spill_retries=0))
    path=root/'timing-samples.jsonl'
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
    return policy,rows,path
