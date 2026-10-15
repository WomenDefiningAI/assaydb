"""Outcome-independent 50k recipe. Generates JSON only; no engine imports or SQL."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import random

HERE = Path(__file__).resolve().parent
FAMILIES = ('integer', 'conditional', 'boundary', 'grouped', 'join',
            'subquery', 'distinct_order', 'text_binary', 'lazy_null', 'metamorphic')
BOUNDARIES = (-(2**63), -(2**63)+1, -(2**53)-1, -(2**53), -1,
              0, 1, 2**53-1, 2**53, 2**53+1, 2**63-2, 2**63-1)


def lit(value):
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    # Parenthesized negative tokens make subtraction/negation unambiguous.
    return '(' + str(value) + ')' if value < 0 else str(value)


def binary(left, op, right):
    if op not in ('+', '-', '*', '/', '%', '=', '<>', '<', '<=', '>', '>='):
        raise ValueError('unreviewed operator')
    return '(' + left + ' ' + op + ' ' + right + ')'


def integer_expr(rng, depth):
    if depth == 0:
        return rng.choice(('a', 'b', 'k', 'NULL', lit(-7), '0', '3'))
    a = integer_expr(rng, depth-1)
    variant = rng.randrange(6)
    if variant < 2:
        return binary(a, ('+', '-')[variant], integer_expr(rng, depth-1))
    if variant < 4:
        return binary(a, ('/', '%')[variant-2], lit(rng.choice((-7,-2,-1,0,1,2,7))))
    if variant == 4:
        return 'COALESCE(' + a + ',' + lit(rng.choice((-7,0,7))) + ')'
    return 'CASE WHEN b IS NULL THEN ' + a + ' ELSE ' + integer_expr(rng, depth-1) + ' END'


def query(sql, family, ordered=False):
    if any(token in sql for token in ('--', '/*', '*/', ';')):
        raise ValueError('unsafe lexical token')
    return dict(sql=sql, sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
                family=family, ordered=ordered, comparison='exact-numeric-value-v1')


def rows_for(seed, size, null_pattern):
    rng = random.Random(seed)
    rows = []
    for i in range(size):
        a = rng.choice((None,-7,-3,-1,0,0,1,3,7))
        b = rng.choice((None,-7,-2,-1,0,1,2,7))
        k = rng.choice((None,-1,0,0,1,2))
        n = BOUNDARIES[(i + seed) % len(BOUNDARIES)]
        s = rng.choice((None,'','0','03','-7','abc',"a'b",'λ'))
        if null_pattern == 'all-null':
            a = b = k = n = s = None
        elif null_pattern == 'nonnull':
            a, b, k, s = a or 0, b or 0, k or 0, s or ''
        rows.append([i, a, b, k, n, s])
    return rows


def generate(recipe):
    datasets = []
    for d in range(recipe['datasets']):
        seed = recipe['seed_base'] + d
        rng = random.Random(seed)
        size = recipe['sizes'][d % len(recipe['sizes'])]
        pattern = recipe['null_patterns'][(d // len(recipe['sizes'])) % len(recipe['null_patterns'])]
        rows = rows_for(seed, size, pattern)
        # Independent right-side values, duplicate/nonmatching/NULL join keys.
        right = rows_for(seed + 1000000, recipe['right_sizes'][d % len(recipe['right_sizes'])], pattern)
        setup = []
        for name, values in [('t', rows), ('u', right)]:
            setup.append('CREATE TABLE ' + name + '(id INTEGER,a INTEGER,b INTEGER,k INTEGER,n INTEGER,s TEXT)')
            if values:
                setup.append('INSERT INTO ' + name + ' VALUES ' + ','.join(
                    '(' + ','.join(map(lit, row)) + ')' for row in values))
        queries, relations = [], []
        def add(family, sql, ordered=False):
            index = len(queries)
            queries.append(query(sql, family, ordered))
            return index
        for i in range(50):
            c = lit(i-25)
            expr = integer_expr(rng, 2+i%3)
            add('integer', f'SELECT id,{binary(expr, "+", c)} AS z FROM t ORDER BY id', True)
            mixed = [f'COALESCE(n,CAST({c} AS REAL))',
                     f'CASE WHEN b IS NULL THEN n ELSE CAST({c} AS REAL) END',
                     f'NULLIF(COALESCE(a,CAST({c} AS REAL)),b)',
                     f'COALESCE(NULLIF(a,b),CAST({c} AS REAL))',
                     f'CASE WHEN k<0 THEN COALESCE(a,CAST(b AS REAL)) ELSE {c} END'][i%5]
            use = i//5
            uses = [mixed, binary(mixed,'/','2'), binary(mixed,'%','2'),
                    binary('CAST('+mixed+' AS REAL)','/','2'),
                    binary('CAST('+mixed+' AS INTEGER)','/','2'),
                    'ABS('+mixed+')', binary(mixed,'+','1'),
                    binary(mixed,'=','n'), 'MIN('+mixed+')', 'MAX('+mixed+')']
            if use >= 8:
                add('conditional', 'SELECT '+uses[use]+' AS z FROM t')
            else:
                add('conditional', 'WITH q AS (SELECT id,'+mixed+' AS x FROM t) SELECT id,'+
                    (binary('x','/','2') if use==1 else binary('x','%','2') if use==2 else uses[use])+
                    ' AS z FROM '+('q' if use in (1,2) else 't')+' ORDER BY id', True)
            n = lit(BOUNDARIES[i % len(BOUNDARIES)])
            boundary = [binary('n','/',lit((i%7)-3)), binary('n','%',lit((i%7)-3)),
                        binary('n','=', 'CAST('+n+' AS REAL)'),
                        'CAST(CAST(n AS REAL) AS INTEGER)',
                        'NULLIF(n,CAST('+n+' AS REAL))'][i%5]
            add('boundary', 'SELECT id,'+boundary+' AS z FROM t WHERE id >= '+lit(i//5-5)+' ORDER BY id', True)
            agg = ['SUM(a)','AVG(a)','MIN(a)','MAX(a)','COUNT(a)',
                   'SUM(DISTINCT a)','COUNT(DISTINCT a)','SUM(CAST(a AS REAL))',
                   'AVG(CAST(a AS REAL))','COUNT(*)'][i%10]
            add('grouped', f'SELECT k,{agg} AS z FROM t GROUP BY k HAVING COUNT(*) >= {lit(i//10)}')
            join = ['JOIN u ON t.k=u.k','LEFT JOIN u ON t.k=u.k',
                    'JOIN u ON t.k=u.k AND t.a<u.b','CROSS JOIN u',
                    'JOIN u ON t.a=u.b'][i%5]
            add('join', f'SELECT t.id,u.id,t.a,u.b FROM t {join} WHERE t.id >= {lit(i//5-5)}')
            sub = [f'a IN(SELECT b FROM u WHERE id >= {c})',
                   f'a NOT IN(SELECT b FROM u WHERE id >= {c})',
                   f'EXISTS(SELECT 1 FROM u WHERE u.k=t.k AND u.id >= {c})',
                   f'a IN(SELECT CAST(b AS REAL) FROM u WHERE id >= {c})',
                   f'COALESCE((SELECT SUM(b) FROM u WHERE id >= {c}),a)'][i%5]
            add('subquery', f'SELECT id,{sub} AS z FROM t ORDER BY id', True)
            if i%2:
                add('distinct_order', f'SELECT DISTINCT k,(a % {lit(i%7)}) AS z FROM t WHERE id >= {lit(i//5-5)}')
            else:
                add('distinct_order', f'SELECT id,a FROM t WHERE id >= {lit(i//5-5)} ORDER BY id DESC LIMIT {i%11}', True)
            text = [f'CAST({c} AS TEXT)', 'CAST(s AS INTEGER)', 'CAST(s AS REAL)',
                    "NULLIF(s,'0')", "COALESCE(s,'')", 's = CAST('+c+' AS TEXT)',
                    "X'303132' = X'303132'", "X'00ff' <> X'00'",
                    "COALESCE(NULL,X'00ff')", "NULLIF(X'00ff',X'00ff')"][i%10]
            add('text_binary', f'SELECT id,{text} AS z FROM t WHERE id >= {lit(i//10-2)} ORDER BY id', True)
            lazy = [f'CASE WHEN a IS NULL THEN {c} ELSE (a / 0) END',
                    f'COALESCE(a,({c} / 0),b)', f'NULLIF(a,{c}) % 0',
                    f'CASE WHEN TRUE THEN {c} ELSE (9223372036854775807 + 1) END',
                    f'CASE WHEN FALSE THEN (9223372036854775807 * 9223372036854775807) ELSE {c} END',
                    f'COALESCE(NULLIF(a,NULL),{c})', f'((NULL + a) / {c})',
                    f'(a % CAST({c} AS REAL))', f'(CAST(a AS REAL) / {c})',
                    f'CASE WHEN b=0 THEN {c} ELSE (a / b) END'][i%10]
            add('lazy_null', f'SELECT id,{lazy} AS z FROM t ORDER BY id', True)
        # 8 TLP (4) + NoREC (2) sets, then two order-independent scalar identities.
        for i in range(8):
            pred = ['a > '+lit(i-4), 'b = '+lit(i-4), 'a IS NULL', 'a=b'][i%4]
            ids = [add('metamorphic', 'SELECT id,a,b,k FROM t'+suffix) for suffix in
                   ['', ' WHERE '+pred, ' WHERE NOT ('+pred+')', ' WHERE ('+pred+') IS NULL']]
            relations.append(dict(kind='tlp', indices=ids))
            ids = [add('metamorphic', sql, True) for sql in
                   ['SELECT COUNT(*) FROM t WHERE '+pred,
                    'SELECT COALESCE(SUM(CASE WHEN '+pred+' THEN 1 ELSE 0 END),0) FROM t']]
            relations.append(dict(kind='norec', indices=ids))
        add('metamorphic','SELECT COUNT(*) FROM t', True)
        add('metamorphic','SELECT COUNT(id) FROM t', True)
        relations.append(dict(kind='equal', indices=[len(queries)-2,len(queries)-1]))
        datasets.append(dict(seed=seed,label=f'dataset-{d:03d}-{pattern}-{size}',
                             rows={'t':rows,'u':right},setup=setup,queries=queries,metamorphic=relations))
    return dict(recipe=recipe, datasets=datasets,
                query_count=sum(len(d['queries']) for d in datasets),
                family_counts=dict(Counter(q['family'] for d in datasets for q in d['queries'])))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    recipe = json.loads((HERE/'recipe-v2.json').read_text())
    plan = generate(recipe)
    plan['preparation'] = dict(generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                              recipe_sha256=hashlib.sha256((HERE/'recipe-v2.json').read_bytes()).hexdigest(),
                              execution_authorized=False, engine_identity=None)
    with args.output.open('x') as stream:
        json.dump(plan, stream, sort_keys=True, ensure_ascii=False, separators=(',',':'))
        stream.write('\n')


if __name__ == '__main__':
    main()
