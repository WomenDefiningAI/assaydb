-- REAL is binary64 in Assay; the reference uses DOUBLE explicitly.
-- Exact dyadic rounding overrides and every DuckDB difference are retained in expected.json.

-- case: {"id": "float_1.25", "ordered": false, "exact_source_sql": "SELECT CAST(1.25 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(1.25 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-1.25", "ordered": false, "exact_source_sql": "SELECT CAST(-1.25 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-1.25 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_1.125", "ordered": false, "exact_source_sql": "SELECT CAST(1.125 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(1.125 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-1.125", "ordered": false, "exact_source_sql": "SELECT CAST(-1.125 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-1.125 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_2.675", "ordered": false, "exact_source_sql": "SELECT CAST(2.675 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(2.675 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-2.675", "ordered": false, "exact_source_sql": "SELECT CAST(-2.675 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-2.675 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_1.005", "ordered": false, "exact_source_sql": "SELECT CAST(1.005 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(1.005 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-1.005", "ordered": false, "exact_source_sql": "SELECT CAST(-1.005 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-1.005 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_0.145", "ordered": false, "exact_source_sql": "SELECT CAST(0.145 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(0.145 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_9.995", "ordered": false, "exact_source_sql": "SELECT CAST(9.995 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(9.995 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-9.995", "ordered": false, "exact_source_sql": "SELECT CAST(-9.995 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-9.995 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_0.0", "ordered": false, "exact_source_sql": "SELECT CAST(0.0 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(0.0 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "float_-0.0", "ordered": false, "exact_source_sql": "SELECT CAST(-0.0 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-0.0 AS REAL) AS DECIMAL(10,2)) AS v;

-- case: {"id": "numeric_float", "ordered": false, "exact_source_sql": "SELECT CAST(2.675 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT (CAST(2.675 AS REAL))::NUMERIC(10,2) AS v;

-- case: {"id": "dec_float", "ordered": false, "exact_source_sql": "SELECT CAST(-1.125 AS REAL) AS v;", "precision": 10, "scale": 2}
SELECT CAST(CAST(-1.125 AS REAL) AS DEC(10,2)) AS v;

-- case: {"id": "scale18", "ordered": false, "exact_source_sql": "SELECT CAST(0.1 AS REAL) AS v;", "precision": 38, "scale": 18}
SELECT CAST(CAST(0.1 AS REAL) AS DECIMAL(38,18)) AS v;

-- case: {"id": "tie_integer", "ordered": false, "exact_source_sql": "SELECT CAST(999.5 AS REAL) AS v;", "precision": 4, "scale": 0}
SELECT CAST(CAST(999.5 AS REAL) AS DECIMAL(4,0)) AS v;

-- case: {"id": "negative_tie_integer", "ordered": false, "exact_source_sql": "SELECT CAST(-999.5 AS REAL) AS v;", "precision": 4, "scale": 0}
SELECT CAST(CAST(-999.5 AS REAL) AS DECIMAL(4,0)) AS v;

-- case: {"id": "rounding_carry_overflow", "ordered": false, "exact_source_sql": "SELECT CAST(9.5 AS REAL) AS v;", "precision": 1, "scale": 0}
SELECT CAST(CAST(9.5 AS REAL) AS DECIMAL(1,0)) AS v;

-- case: {"id": "negative_carry_overflow", "ordered": false, "exact_source_sql": "SELECT CAST(-9.5 AS REAL) AS v;", "precision": 1, "scale": 0}
SELECT CAST(CAST(-9.5 AS REAL) AS DECIMAL(1,0)) AS v;

-- case: {"id": "large_finite_overflow", "ordered": false, "exact_source_sql": "SELECT CAST('1e300' AS REAL) AS v;", "precision": 38, "scale": 18}
SELECT CAST(CAST('1e300' AS REAL) AS DECIMAL(38,18)) AS v;

-- case: {"id": "large_in_range", "ordered": false, "exact_source_sql": "SELECT CAST('1e37' AS REAL) AS v;", "precision": 38, "scale": 0}
SELECT CAST(CAST('1e37' AS REAL) AS DECIMAL(38,0)) AS v;

-- case: {"id": "subnormal", "ordered": false, "exact_source_sql": "SELECT CAST('5e-324' AS REAL) AS v;", "precision": 38, "scale": 18}
SELECT CAST(CAST('5e-324' AS REAL) AS DECIMAL(38,18)) AS v;

-- case: {"id": "negative_subnormal", "ordered": false, "exact_source_sql": "SELECT CAST('-5e-324' AS REAL) AS v;", "precision": 38, "scale": 18}
SELECT CAST(CAST('-5e-324' AS REAL) AS DECIMAL(38,18)) AS v;

-- case: {"id": "bare_DECIMAL", "ordered": false}
SELECT CAST(12 AS DECIMAL) AS v;

-- case: {"id": "colon_DECIMAL", "ordered": false}
SELECT (-12.3456)::DECIMAL AS v;

-- case: {"id": "null_DECIMAL", "ordered": false}
SELECT CAST(NULL AS DECIMAL) AS v;

-- case: {"id": "bare_float_DECIMAL", "ordered": false, "exact_source_sql": "SELECT CAST(1.25 AS REAL) AS v;", "precision": 18, "scale": 3}
SELECT CAST(CAST(1.25 AS REAL) AS DECIMAL) AS v;

-- case: {"id": "bare_NUMERIC", "ordered": false}
SELECT CAST(12 AS NUMERIC) AS v;

-- case: {"id": "colon_NUMERIC", "ordered": false}
SELECT (-12.3456)::NUMERIC AS v;

-- case: {"id": "null_NUMERIC", "ordered": false}
SELECT CAST(NULL AS NUMERIC) AS v;

-- case: {"id": "bare_float_NUMERIC", "ordered": false, "exact_source_sql": "SELECT CAST(1.25 AS REAL) AS v;", "precision": 18, "scale": 3}
SELECT CAST(CAST(1.25 AS REAL) AS NUMERIC) AS v;

-- case: {"id": "bare_DEC", "ordered": false}
SELECT CAST(12 AS DEC) AS v;

-- case: {"id": "colon_DEC", "ordered": false}
SELECT (-12.3456)::DEC AS v;

-- case: {"id": "null_DEC", "ordered": false}
SELECT CAST(NULL AS DEC) AS v;

-- case: {"id": "bare_float_DEC", "ordered": false, "exact_source_sql": "SELECT CAST(1.25 AS REAL) AS v;", "precision": 18, "scale": 3}
SELECT CAST(CAST(1.25 AS REAL) AS DEC) AS v;

-- case: {"id": "bare_text_literal", "ordered": false}
SELECT CAST('12.345' AS DECIMAL) AS v;

-- case: {"id": "decimal_tie", "ordered": false}
SELECT CAST(1.245 AS DECIMAL(5,2)) AS v;

-- case: {"id": "decimal_negative_tie", "ordered": false}
SELECT CAST(-1.245 AS DECIMAL(5,2)) AS v;

-- case: {"id": "bare_boundary", "ordered": false}
SELECT CAST(999999999999999.999 AS DECIMAL) AS v;

-- case: {"id": "bare_overflow", "ordered": false, "assay_error": "decimal cast precision overflow"}
SELECT CAST(1000000000000000 AS DECIMAL) AS v;

-- case: {"id": "bare_rounding_overflow", "ordered": false, "assay_error": "decimal cast precision overflow"}
SELECT CAST(999999999999999.9995 AS NUMERIC) AS v;

-- case: {"id": "explicit_null_float", "ordered": false}
SELECT CAST(CAST(NULL AS REAL) AS NUMERIC(10,2)) AS v;

-- case: {"id": "bare_decimal_source", "ordered": false}
SELECT CAST(CAST(1.2345 AS DECIMAL(9,4)) AS DEC) AS v;

-- case: {"id": "case_dead_overflow", "ordered": false}
SELECT CASE WHEN TRUE THEN CAST(1.25 AS DECIMAL) ELSE CAST(CAST(1000000000000000 AS REAL) AS DECIMAL) END AS v;

-- case: {"id": "case_reached_overflow", "ordered": false, "assay_error": "decimal cast precision overflow"}
SELECT CASE WHEN FALSE THEN CAST(1.25 AS DECIMAL) ELSE CAST(CAST(1000000000000000 AS REAL) AS DECIMAL) END AS v;

-- case: {"id": "unit_price_numeric", "ordered": true, "exact_source_sql": "WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT id, price / qty AS v FROM sales ORDER BY id;", "precision": 10, "scale": 2, "exact_columns": [1]}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT id, CAST(price / qty AS NUMERIC(10,2)) AS v FROM sales ORDER BY id;

-- case: {"id": "average_decimal_cast", "ordered": true, "exact_source_sql": "WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT grp, AVG(price) AS v FROM sales GROUP BY grp ORDER BY grp;", "precision": 10, "scale": 4, "exact_columns": [1]}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT grp, CAST(AVG(price) AS DECIMAL(10,4)) AS v FROM sales GROUP BY grp ORDER BY grp;

-- case: {"id": "inventory_bare", "ordered": true}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT id, price::decimal * qty AS v FROM sales ORDER BY id;

-- case: {"id": "sum_of_float_cast", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT SUM(CAST(price / qty AS DECIMAL(10,2))) AS v FROM sales;

-- case: {"id": "duplicates", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT CAST(price / qty AS DECIMAL(10,2)) AS v FROM sales;

-- case: {"id": "distinct_ties", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT DISTINCT CAST(price / qty AS DECIMAL(10,2)) AS v FROM sales;

-- case: {"id": "empty_projection", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT CAST(price / qty AS DECIMAL(10,2)) AS v FROM sales WHERE id < 0;

-- case: {"id": "empty_average", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT CAST(AVG(price) AS DECIMAL(10,2)) AS v FROM sales WHERE id < 0;

-- case: {"id": "all_null_average", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT CAST(AVG(price) AS DECIMAL) AS v FROM sales WHERE id = 4;

-- case: {"id": "grouped_cast_key", "ordered": false}
WITH sales AS (SELECT 1 AS id, 1 AS grp, 1.25 AS price, 2 AS qty UNION ALL SELECT 2, 1, 2.75, 2 UNION ALL SELECT 3, 2, 1.25, 2 UNION ALL SELECT 4, 2, NULL, 1) SELECT CAST(price / qty AS DECIMAL(10,2)) AS v, COUNT(*) AS n FROM sales GROUP BY CAST(price / qty AS DECIMAL(10,2));

-- case: {"id": "filtered_overflow", "ordered": false}
WITH x AS (SELECT 1 AS id, CAST(1.25 AS REAL) AS f UNION ALL SELECT 2, CAST(1000000000000000 AS REAL)) SELECT CAST(f AS DECIMAL) AS v FROM x WHERE id = 1;

-- case: {"id": "masked_overflow", "ordered": true}
WITH x AS (SELECT 1 AS id, CAST(1.25 AS REAL) AS f UNION ALL SELECT 2, CAST(1000000000000000 AS REAL)) SELECT id, CASE WHEN id = 1 THEN CAST(f AS DECIMAL) ELSE CAST(0 AS DECIMAL) END AS v FROM x ORDER BY id;

-- case: {"id": "date_stays_refused", "ordered": false, "assay_error": "unsupported: DECIMAL cast requires an exact numeric source or Float", "reason": "Date sources remain outside the approved numeric cast domain."}
SELECT CAST(DATE '2026-10-15' AS DECIMAL) AS v;

-- case: {"id": "text_column_stays_refused", "ordered": false, "assay_error": "unsupported: DECIMAL cast requires an exact numeric source or Float", "reason": "Text columns remain refused; only lossless text literals are admitted."}
SELECT CAST(t AS NUMERIC) AS v FROM (SELECT '1.25' AS t UNION ALL SELECT '2.50') x;

-- case: {"id": "bool_stays_refused", "ordered": false, "assay_error": "unsupported: DECIMAL cast requires an exact numeric source or Float", "reason": "Boolean sources remain outside the approved numeric cast domain."}
SELECT CAST(TRUE AS DEC) AS v;

-- case: {"id": "precision_only_stays_refused", "ordered": false, "assay_error": "unsupported: CAST AS NUMERIC(5)", "reason": "Precision-only syntax remains outside this card."}
SELECT CAST(1 AS NUMERIC(5)) AS v;

