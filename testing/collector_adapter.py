"""Lossless adapters for hash-pinned collector record streams, without execution.

The protected collector must first validate complete lifecycle/provenance receipts.
This adapter does not import or execute code supplied in an evidence archive.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import itertools
import json
from pathlib import Path

from result_contract import STATUSES, digest, require

SLT = {'engine-pass':'passed', 'engine-wrong-answer':'wrong',
       'engine-unsupported':'unsupported', 'runner-unsupported':'unsupported',
       'engine-error':'error', 'session-failure':'error', 'session-unavailable':'not-run',
       'conditional-not-executed':'excluded', 'control-only':'excluded', 'after-halt':'excluded'}
HUNT = {'pass':'passed', 'wrong-answer':'wrong', 'native-unsupported':'unsupported',
        'native-error':'error', 'oracle-error':'error', 'invalid-domain':'error',
        'pending':'not-run', 'native-setup-blocked':'error', 'oracle-setup-blocked':'error',
        'native-session-failure':'error', 'oracle-session-failure':'error',
        'native-unavailable':'not-run', 'oracle-unavailable':'not-run'}


def rows(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        for line in stream:
            yield json.loads(line)


def adapt(row, kind, index):
    if kind == 'slt':
        original = row['status']
        status = SLT[original]
        identity = [row[k] for k in ('file','line','kind','record_index')]
        sql_sha = row['sql_sha256']
    elif kind == 'hunt55k':
        require(row['kind'] == 'query', 'non-query hunt records require lifecycle/auxiliary validation first')
        original = row['outcome']['status']
        status = HUNT[original]
        sql_sha = hashlib.sha256(row['sql'].encode()).hexdigest()
        require(sql_sha == row['sql_sha256'], 'hunt SQL hash mismatch')
        identity = [row[k] for k in ('dataset','index','family','ordered')]
    elif kind == 'retail':
        original = row['status']
        require(original in STATUSES and original != 'excluded', 'unknown retail status')
        status = original
        identity = [row[k] for k in ('suite','scope','case')]
        sql_sha = row['sql_sha256']
    else:
        raise ValueError('unknown adapter')
    result = dict(index=index,id=identity,sql_sha256=sql_sha,status=status,
                  original_status=original,original_record_sha256=hashlib.sha256(
                      json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest())
    if status == 'excluded':
        result['reason'] = {'conditional-not-executed':'onlyif-other-engine',
                            'control-only':'control-only','after-halt':'after-halt'}[original]
    return result


def convert(raw, expected, output, kind, raw_sha256, expected_sha256):
    require(digest(raw) == raw_sha256 and digest(expected) == expected_sha256, 'collector/scope hash mismatch')
    counts=Counter({s:0 for s in STATUSES})
    # Scope records are generated from original frozen inputs, not candidate output.
    # A failure leaves a visibly incomplete new artifact; it is never reused.
    with output.open('x') as stream:
        for index, (row, target) in enumerate(itertools.zip_longest(rows(raw),rows(expected))):
            require(row is not None and target is not None, 'missing/extra collector record')
            normalized=adapt(row,kind,index)
            require(all(normalized.get(k)==target.get(k) for k in ('id','sql_sha256')), 'collector identity differs from frozen scope')
            if normalized['status']=='excluded':
                require(normalized['reason']==target.get('exclusion_reason'), 'exclusion not authorized by frozen scope')
            else:
                require(not target.get('exclusion_reason'), 'frozen exclusion unexpectedly executed')
            stream.write(json.dumps(normalized,sort_keys=True)+'\n')
            counts[normalized['status']]+=1
    require(digest(raw)==raw_sha256 and digest(expected)==expected_sha256, 'collector inputs changed during conversion')
    return dict(counts=counts,records_sha256=digest(output),raw_sha256=raw_sha256,scope_sha256=expected_sha256)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('kind',choices=('slt','hunt55k','retail'))
    p.add_argument('raw',type=Path);p.add_argument('scope',type=Path);p.add_argument('output',type=Path)
    p.add_argument('--raw-sha256',required=True);p.add_argument('--scope-sha256',required=True)
    a=p.parse_args()
    print(json.dumps(convert(a.raw,a.scope,a.output,a.kind,a.raw_sha256,a.scope_sha256),sort_keys=True))
