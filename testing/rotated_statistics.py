"""Rotated-round estimator with paired round and within-round bootstrap.

Original Assay implementation, under repository MIT OR Apache-2.0 terms.
"""
import math
import random
import statistics

IDS = [f'q{i:02d}' for i in range(1, 23)]
ENGINES = ('assaydb', 'polars', 'duckdb')
ORDERS = (ENGINES, ('polars', 'duckdb', 'assaydb'), ('duckdb', 'assaydb', 'polars'))
DRAWS = 10000
SEED = 57290


def quantile(values, probability):
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def metrics(pairs):
    return {'total': sum(a for a, _ in pairs) / sum(b for _, b in pairs),
            'geomean': math.exp(statistics.mean(math.log(a / b) for a, b in pairs))}


def analyze_pair(vectors, common, reference, draws):
    """Same estimator and RNG consumption as harness.m3_verify.analyze.

    For a complete pair, pool all66 query-round medians rather than collapsing
    each query to one median. A shared selected round preserves pairing across
    all queries and both engines. Each comparator starts the same seeded stream;
    its identical-size inventory therefore has identical round/sample indices.
    """
    rounds = [[(statistics.median(vectors['assaydb', q][r]),
                statistics.median(vectors[reference, q][r])) for q in common] for r in range(3)]
    pooled = metrics([pair for pairs in rounds for pair in pairs])
    rng = random.Random(SEED)
    distributions = {m: [] for m in pooled}
    per_query = {q: [] for q in common}
    for _ in range(draws):
        pairs = []
        query_pairs = {q: [] for q in common}
        for _ in range(3):
            r = rng.randrange(3)
            for q in common:
                pair = tuple(statistics.median(rng.choices(vectors[e, q][r], k=5))
                             for e in ('assaydb', reference))
                pairs.append(pair)
                query_pairs[q].append(pair)
        sample = metrics(pairs)
        for name in pooled:
            distributions[name].append(sample[name])
        for q in common:
            per_query[q].append(metrics(query_pairs[q])['total'])
    calculated = {**pooled,
                  'sum_query_round_medians_seconds': {
                      'assaydb': sum(a for pairs in rounds for a, _ in pairs),
                      reference: sum(b for pairs in rounds for _, b in pairs)},
                  'confidence_95': {m: [quantile(distributions[m], p) for p in (.025, .975)] for m in pooled},
                  'rounds': [metrics(pairs) for pairs in rounds]}
    gaps = {q: {'point': metrics([rounds[r][i] for r in range(3)])['total'],
                'confidence_95': [quantile(per_query[q], p) for p in (.025, .975)]}
            for i, q in enumerate(common)}
    return calculated, gaps


def analyze(records, *, draws=DRAWS):
    """Failures remain in denominator; incomplete common sets are never full-suite ratios."""
    if type(draws) is not int or draws <= 0:
        raise ValueError('positive bootstrap draws required')
    table = {}
    for row in records:
        key = row['round'], row['engine'], row['query']
        if key in table or key[0] not in (1, 2, 3) or key[1] not in ENGINES or key[2] not in IDS:
            raise ValueError('duplicate or unexpected timing record')
        values = row.get('seconds', [])
        if row['status'] == 'completed' and (len(values) != 5 or any(type(v) not in (float, int) or not math.isfinite(v) or v <= 0 for v in values)):
            raise ValueError('invalid complete sample vector')
        table[key] = row
    expected = {(r, e, q) for r in (1, 2, 3) for e in ENGINES for q in IDS}
    if table.keys() != expected:
        raise ValueError('retain all 198 timing outcomes, including failures')
    completed = {e: [q for q in IDS if all(table[r, e, q]['status'] == 'completed' for r in (1, 2, 3))] for e in ENGINES}
    vectors = {(e, q): [table[r, e, q]['seconds'] for r in (1, 2, 3)] for e in ENGINES for q in completed[e]}
    # Display means of round medians so summed displayed query costs match the
    # full pooled total (up to the common factor of three and printed rounding).
    points = {(e, q): statistics.mean(statistics.median(v) for v in vectors[e, q]) for e, q in vectors}
    comparisons = {}
    query_rows = [{'query': q, 'seconds': {e: points.get((e, q)) for e in ENGINES},
                   'round_medians_seconds': {e: [statistics.median(v) for v in vectors[e, q]] if (e, q) in vectors else None for e in ENGINES},
                   'outcomes': {e: [table[r, e, q]['status'] for r in (1, 2, 3)] for e in ENGINES},
                   'ratios': {}} for q in IDS]
    for reference in ('polars', 'duckdb'):
        common = [q for q in IDS if q in completed['assaydb'] and q in completed[reference]]
        name = 'assaydb_' + reference
        item = {'denominator': 22, 'complete_queries': common, 'missing_queries': [q for q in IDS if q not in common],
                'full_suite': len(common) == 22, 'full_suite_ratios': None, 'common_set_diagnostic': None}
        if common:
            calculated, gaps = analyze_pair(vectors, common, reference, draws)
            item['full_suite_ratios' if len(common) == 22 else 'common_set_diagnostic'] = calculated
            for row in query_rows:
                if row['query'] in gaps:
                    row['ratios'][name] = gaps[row['query']]
        comparisons[name] = item
    return {'comparisons': comparisons, 'queries': query_rows, 'bootstrap': {'draws': draws, 'seed': SEED,
            'rounds': 3, 'fixed_queries': 22, 'within_round_samples': 5,
            'point_estimator': 'M3 pooled total ratio of sums over66 query-round medians; geometric mean of66 paired ratios',
            'query_seconds_estimator': 'arithmetic mean of three round medians; gap ratio of sums over those three rounds',
            'interval': 'marginal percentile95; three round clusters and15 measured samples per engine/query, not15 independent observations; no hardware portability claim'}}
