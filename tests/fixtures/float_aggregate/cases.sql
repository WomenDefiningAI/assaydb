-- case: {"id": "sum", "ordered": false}
SELECT SUM(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "avg", "ordered": false}
SELECT AVG(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "sum_avg_count", "ordered": false}
SELECT SUM(x),AVG(x),COUNT(x),COUNT(*) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "sum_alias", "ordered": false}
SELECT SUM(x) AS revenue,SUM(x) AS again,AVG(x) AS mean FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "minmax", "ordered": false}
SELECT SUM(x),AVG(x),MIN(x),MAX(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "count_distinct", "ordered": false}
SELECT SUM(x),COUNT(DISTINCT x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "negative", "ordered": false}
SELECT SUM(-x),AVG(-x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "arithmetic", "ordered": false}
SELECT SUM(x*2),AVG(x+1) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "nullif", "ordered": false}
SELECT SUM(NULLIF(x,1.5/1)),AVG(NULLIF(x,1.5/1)) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "case", "ordered": false}
SELECT SUM(CASE WHEN k=1 THEN x ELSE NULL END),AVG(CASE WHEN k=2 THEN x ELSE NULL END) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "coalesce", "ordered": false}
SELECT SUM(COALESCE(x,0.0/1)),AVG(COALESCE(x,0.0/1)) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "filtered", "ordered": false}
SELECT SUM(x),AVG(x),COUNT(x),COUNT(*) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t WHERE k=2;

-- case: {"id": "empty", "ordered": false}
SELECT SUM(x),AVG(x),COUNT(x),COUNT(*) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t WHERE k=9;

-- case: {"id": "all_null", "ordered": false}
SELECT SUM(x),AVG(x),COUNT(x),COUNT(*) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t WHERE x IS NULL;

-- case: {"id": "group_numeric", "ordered": true}
SELECT k,SUM(x),AVG(x),COUNT(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t GROUP BY k ORDER BY k;

-- case: {"id": "group_bag", "ordered": false}
SELECT k,SUM(x),AVG(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t GROUP BY k;

-- case: {"id": "having", "ordered": true}
SELECT k,SUM(x) AS revenue,AVG(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t GROUP BY k HAVING SUM(x)>2 ORDER BY k;

-- case: {"id": "group_empty", "ordered": false}
SELECT k,SUM(x),AVG(x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t WHERE k=9 GROUP BY k;

-- case: {"id": "group_null_key", "ordered": false}
SELECT k,SUM(x),AVG(x) FROM (SELECT NULL AS k,1.5/1 AS x UNION ALL SELECT NULL,2.5/1) t GROUP BY k;

-- case: {"id": "group_all_null", "ordered": false}
SELECT k,SUM(x),AVG(x),COUNT(x) FROM (SELECT 1 AS k,CAST(NULL AS REAL) AS x UNION ALL SELECT 1,NULL) t GROUP BY k;

-- case: {"id": "group_text", "ordered": true}
SELECT k,SUM(x),AVG(x) FROM (SELECT 'east' AS k,12.5/1 AS x UNION ALL SELECT 'west',2.25/1 UNION ALL SELECT 'east',-1.5/1) t GROUP BY k ORDER BY k;

-- case: {"id": "group_date", "ordered": true}
SELECT k,SUM(x),AVG(x) FROM (SELECT DATE '2026-01-01' AS k,12.5/1 AS x UNION ALL SELECT DATE '2026-01-02',2.25/1 UNION ALL SELECT DATE '2026-01-01',-1.5/1) t GROUP BY k ORDER BY k;

-- case: {"id": "integer_unchanged", "ordered": false}
SELECT SUM(x),AVG(x),COUNT(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT NULL) t;

-- case: {"id": "decimal_unchanged", "ordered": false}
SELECT SUM(x),AVG(x) FROM (SELECT 1.25 AS x UNION ALL SELECT 2.50 UNION ALL SELECT NULL) t;

-- case: {"id": "null_untyped", "ordered": false}
SELECT SUM(NULL),AVG(NULL),COUNT(NULL);

-- case: {"id": "scalar_real", "ordered": false}
SELECT SUM(CAST(3 AS REAL)),AVG(CAST(3 AS REAL));

-- case: {"id": "singleton_negative", "ordered": false}
SELECT SUM(-2.5/1),AVG(-2.5/1);

-- case: {"id": "cancellation", "ordered": false}
SELECT SUM(x),AVG(x) FROM (SELECT 10000000000000000.0/1 AS x UNION ALL SELECT 1.0/1 UNION ALL SELECT -10000000000000000.0/1) t;

-- case: {"id": "fractional", "ordered": false}
SELECT SUM(x),AVG(x) FROM (SELECT 0.1/1 AS x UNION ALL SELECT 0.2/1 UNION ALL SELECT 0.3/1) t;

-- case: {"id": "nested", "ordered": false}
SELECT SUM(s),AVG(s) FROM (SELECT k,SUM(x) AS s FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t GROUP BY k) g;

-- case: {"id": "join", "ordered": false}
SELECT SUM(t.x),AVG(t.x),COUNT(*) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t JOIN (SELECT 1 AS k UNION ALL SELECT 1 UNION ALL SELECT 2) d ON t.k=d.k;

-- case: {"id": "left_join", "ordered": true}
SELECT d.k,SUM(t.x),AVG(t.x),COUNT(t.x) FROM (SELECT 1 AS k UNION ALL SELECT 3) d LEFT JOIN (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t ON d.k=t.k GROUP BY d.k ORDER BY d.k;

-- case: {"id": "cte", "ordered": false}
WITH revenue AS (SELECT k,SUM(x) AS s FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t GROUP BY k) SELECT SUM(s),AVG(s) FROM revenue;

-- case: {"id": "ratios", "ordered": false}
SELECT SUM(x)/COUNT(x),AVG(x)*2 FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "distinct_sum", "ordered": false}
SELECT SUM(DISTINCT x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "distinct_avg", "ordered": false}
SELECT AVG(DISTINCT x) FROM (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t;

-- case: {"id": "bool_sum", "ordered": false, "refusal": "unsupported: expected numeric type, got Bool"}
SELECT SUM(TRUE);

-- case: {"id": "bool_avg", "ordered": false, "refusal": "unsupported: expected numeric type, got Bool"}
SELECT AVG(TRUE);

-- case: {"id": "text_sum", "ordered": false, "refusal": "expected numeric type, got Text"}
SELECT SUM('abc');

-- case: {"id": "date_avg", "ordered": false, "refusal": "expected numeric type, got Date"}
SELECT AVG(DATE '2026-01-01');

-- case: {"id": "window", "ordered": false, "refusal": "unsupported: Float/Double sum OVER"}
SELECT SUM(1.5/1) OVER ();

-- case: {"id": "left_join_supported_order", "ordered": true}
SELECT d.k,SUM(t.x),AVG(t.x),COUNT(t.x) FROM (SELECT 1 AS k UNION ALL SELECT 3) d LEFT JOIN (SELECT 1 AS k, 1.5/1 AS x UNION ALL SELECT 2, 2.25/1 UNION ALL SELECT 1, NULL UNION ALL SELECT 2, -0.5/1 UNION ALL SELECT 1, 1.5/1) t ON d.k=t.k GROUP BY d.k ORDER BY 1;

-- case: {"id": "distinct_null_slt_104_17527", "ordered": false}
SELECT ALL 67 AS col1, + AVG ( DISTINCT + CAST ( NULL AS REAL ) ) col0;

-- case: {"id": "distinct_null_slt_56_90834", "ordered": false}
SELECT ALL SUM ( DISTINCT + CAST ( NULL AS REAL ) ) col0;

-- case: {"id": "distinct_null_slt_80_32416", "ordered": false}
SELECT DISTINCT - 53 + + + COUNT ( * ) * AVG ( DISTINCT + CAST ( NULL AS REAL ) ) AS col1, - 80 * + 32;

-- case: {"id": "distinct_null_slt_81_17090", "ordered": false}
SELECT ( 2 ) / + 87 / SUM ( DISTINCT - CAST ( + CAST ( NULL AS INTEGER ) AS REAL ) );

-- case: {"id": "distinct_null_empty", "ordered": false}
SELECT SUM(DISTINCT CAST(NULL AS REAL)),AVG(DISTINCT CAST(NULL AS REAL)),COUNT(*) FROM (SELECT 1 AS k) t WHERE k=2;

-- case: {"id": "distinct_null_grouped", "ordered": true}
SELECT k,SUM(DISTINCT CAST(NULL AS REAL)),AVG(DISTINCT -CAST(NULL AS REAL)),COUNT(*) FROM (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 1) t GROUP BY k ORDER BY k;

-- case: {"id": "distinct_nullif_constant", "ordered": false}
SELECT SUM(DISTINCT NULLIF(CAST(1 AS REAL),CAST(1 AS REAL))),AVG(DISTINCT NULLIF(CAST(1 AS REAL),CAST(1 AS REAL)));

-- case: {"id": "distinct_nonnull_constant", "ordered": false}
SELECT SUM(DISTINCT CAST(1 AS REAL));

-- case: {"id": "distinct_nullable_column", "ordered": false}
SELECT AVG(DISTINCT x) FROM (SELECT CAST(NULL AS REAL) AS x UNION ALL SELECT CAST(NULL AS REAL)) t;

-- case: {"id": "distinct_fallible_null", "ordered": false, "refusal": "division by zero", "difference": "DECIMAL 1.0/0.0 raises division by zero in Assay and PostgreSQL (DuckDB returns NULL); previously masked by the DISTINCT Float refusal"}
SELECT SUM(DISTINCT (CAST(NULL AS REAL)+(1.0/0.0)));
