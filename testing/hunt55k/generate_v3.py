"""Append5000 NOT NULL/NULL-extension diagnostics; preserve original50000 exactly.

No SQL engine imports or query execution. Expanded output is exclusive-create.
"""
import argparse
from collections import Counter
import copy
import hashlib
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE_SHA256 = '16f92c49617461eb8837a78a021b3c48e37f82098add02bcf24ab0ab265ffd80'
V2_GENERATOR_SHA256 = '29d69c4f752d55b1c1481b8f25249be0c1fb2916b56e89f8caae8a4c49f4544b'
BOUNDARIES = (-(2**63), -(2**63)+1, -(2**53)-1, -(2**53), -2, -1,
              0, 1, 2, 2**53-1, 2**53, 2**53+1, 2**63-2, 2**63-1)
SHAPES = ('nonnull_coalesce', 'nonnull_case', 'nonnull_dead_decimal',
          'cte_renamed_nonnull', 'left_join_null_extension')
CONSUMERS = ('bare', 'divide', 'nested_divide', 'cast_integer_divide',
             'abs_divide', 'minimum', 'maximum', 'nested_coalesce_divide',
             'case_after_selection', 'derived_alias_divide')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def base_generator():
    path = HERE/'generate_v2.py'
    if digest(path) != V2_GENERATOR_SHA256:
        raise ValueError('original generator identity changed')
    spec = importlib.util.spec_from_file_location('unchanged_hunt_v2', path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def original_plan():
    path = HERE/'plan-v2.json'
    if digest(path) != BASE_SHA256:
        raise ValueError('original frozen50000 plan changed')
    result = json.loads(path.read_text())
    if result['query_count'] != 50000 or len(result['datasets']) != 100:
        raise ValueError('original denominator changed')
    return result


def rows(seed, size, right=False):
    # Rotation covers exact integer endpoints without RNG-version dependence.
    return [[i*2 if right else i, BOUNDARIES[(seed+i) % len(BOUNDARIES)],
             (seed+i) % 5 - 2] for i in range(size)]


def added_dataset(index, recipe, v2):
    seed = recipe['seed_base'] + index
    left = rows(seed, recipe['sizes'][index % len(recipe['sizes'])])
    # Offset size cadence supplies empty/nonempty right sides independently.
    right = rows(seed+31, recipe['right_sizes'][(index//5) % len(recipe['right_sizes'])], True)
    setup = []
    for name, values in [('t', left), ('u', right)]:
        setup.append(f'CREATE TABLE {name}(id INTEGER NOT NULL,n INTEGER NOT NULL,k INTEGER NOT NULL)')
        if values:
            setup.append('INSERT INTO '+name+' VALUES '+','.join(
                '('+','.join(v2.lit(x) for x in row)+')' for row in values))
    sources = [
        ('', 't', 'id', 'COALESCE(n,CAST(3 AS REAL))'),
        ('', 't', 'id', 'CASE WHEN n IS NULL THEN CAST(3 AS REAL) ELSE CASE WHEN n IS NOT NULL THEN n ELSE CAST(5 AS REAL) END END'),
        ('', 't', 'id', 'COALESCE(n,5.0)'),
        ('WITH q AS (SELECT id,n AS v FROM t) ', 'q', 'id', 'COALESCE(v,CAST(3 AS REAL))'),
        ('', 't LEFT JOIN u ON t.id=u.id', 't.id', 'COALESCE(u.n,CAST(3 AS REAL))'),
    ]
    queries = []
    for shape, (prefix, source, identity, expression) in zip(SHAPES, sources, strict=True):
        divided = v2.binary(expression, '/', '2')
        uses = [expression, divided, v2.binary(divided, '/', v2.lit(-2)),
                v2.binary('CAST('+expression+' AS INTEGER)', '/', '2'),
                'ABS('+divided+')', 'MIN('+expression+')', 'MAX('+expression+')',
                v2.binary('COALESCE('+expression+',CAST(11 AS REAL))','/','2'),
                'CASE WHEN ('+expression+') IS NULL THEN 0 ELSE '+divided+' END', None]
        for consumer, use in zip(CONSUMERS, uses, strict=True):
            if consumer in ('minimum', 'maximum'):
                sql = prefix+'SELECT '+use+' AS z FROM '+source
            elif consumer == 'derived_alias_divide':
                sql = prefix+'SELECT id,(x / 2) AS z FROM (SELECT '+identity+' AS id,'+expression+' AS x FROM '+source+') AS projected ORDER BY id'
            else:
                sql = prefix+'SELECT '+identity+' AS id,'+use+' AS z FROM '+source+' ORDER BY id'
            query = v2.query(sql, recipe['family'], True)
            query.update(conditional_shape=shape, consumer=consumer,
                         nullability_boundary=shape == 'left_join_null_extension',
                         unreachable_decimal_literal=shape == 'nonnull_dead_decimal')
            queries.append(query)
    # The first four source shapes select the same original Int payload. Each
    # consumer must agree across the direct, CASE, dead-Decimal and renamed CTE forms.
    # Outer-join controls deliberately do not share these identities.
    relations = [dict(kind='equal', indices=[consumer, shape*10+consumer])
                 for shape in (1,2,3) for consumer in range(10)]
    return dict(seed=seed, label=f'dataset-{100+index:03d}-nonnull-{len(left)}-right-{len(right)}',
                rows={'t':left,'u':right}, setup=setup, queries=queries, metamorphic=relations)


def generate(recipe):
    if (recipe['base_plan'] != 'plan-v2.json' or recipe['base_plan_sha256'] != BASE_SHA256
            or recipe['base_datasets'] != 100 or recipe['base_queries'] != 50000
            or recipe['added_datasets'] != 100 or recipe['added_queries_per_dataset'] != 50
            or recipe['added_queries'] != 5000 or recipe['query_count'] != 55000
            or recipe['family'] != 'proven_dead_numeric'):
        raise ValueError('unapproved additive scope')
    original = original_plan()
    v2 = base_generator()
    datasets = copy.deepcopy(original['datasets'])
    datasets.extend(added_dataset(i, recipe, v2) for i in range(100))
    counts = Counter(q['family'] for d in datasets for q in d['queries'])
    assert datasets[:100] == original['datasets']
    assert len(datasets) == 200 and sum(counts.values()) == 55000
    assert counts['proven_dead_numeric'] == 5000
    return dict(recipe=copy.deepcopy(recipe), datasets=datasets, query_count=55000,
                family_counts=dict(counts), inherited_plan_sha256=BASE_SHA256,
                inherited_preparation=copy.deepcopy(original['preparation']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    recipe_path = HERE/'recipe-v3.json'
    result = generate(json.loads(recipe_path.read_text()))
    result['preparation'] = dict(generator_sha256=digest(__file__),
        recipe_sha256=digest(recipe_path), execution_authorized=False, engine_identity=None)
    with args.output.open('x') as stream:
        json.dump(result, stream, sort_keys=True, ensure_ascii=False, separators=(',',':'))
        stream.write('\n')


if __name__ == '__main__':
    main()
