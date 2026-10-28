-- case: {"id":"float_sum_duplicates","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"float_sum_empty","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t WHERE FALSE;

-- case: {"id":"float_sum_distinct","ordered":false,"assay_status":"ok"}
SELECT SUM(DISTINCT x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"float_sum_nulls","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t WHERE x IS NULL;

-- case: {"id":"float_avg_duplicates","ordered":false,"assay_status":"ok"}
SELECT AVG(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"float_avg_empty","ordered":false,"assay_status":"ok"}
SELECT AVG(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t WHERE FALSE;

-- case: {"id":"float_avg_distinct","ordered":false,"assay_status":"ok"}
SELECT AVG(DISTINCT x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"float_avg_nulls","ordered":false,"assay_status":"ok"}
SELECT AVG(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t WHERE x IS NULL;

-- case: {"id":"bool_sum_duplicates","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT SUM(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t;

-- case: {"id":"bool_sum_empty","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT SUM(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t WHERE FALSE;

-- case: {"id":"bool_sum_distinct","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT SUM(DISTINCT x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t;

-- case: {"id":"bool_sum_nulls","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT SUM(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t WHERE x IS NULL;

-- case: {"id":"bool_avg_duplicates","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT AVG(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t;

-- case: {"id":"bool_avg_empty","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT AVG(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t WHERE FALSE;

-- case: {"id":"bool_avg_distinct","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT AVG(DISTINCT x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t;

-- case: {"id":"bool_avg_nulls","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Bool","difference":"Aggregate capability remains refused; no accumulation or Boolean numeric semantics chosen."}
SELECT AVG(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t WHERE x IS NULL;

-- case: {"id":"date_plus_int","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Date","difference":"Date arithmetic remains unimplemented."}
SELECT DATE '2024-06-01' + 1 AS value;

-- case: {"id":"int_plus_date","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Date","difference":"Date arithmetic remains unimplemented."}
SELECT 1 + DATE '2024-06-01' AS value;

-- case: {"id":"date_minus_int","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Date","difference":"Date arithmetic remains unimplemented."}
SELECT DATE '2024-06-01' - 1 AS value;

-- case: {"id":"date_difference","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: expected numeric type, got Date","difference":"Date arithmetic remains unimplemented."}
SELECT DATE '2024-06-02' - DATE '2024-06-01' AS value;

-- case: {"id":"date_text_literal","ordered":false,"assay_status":"ok"}
SELECT d >= '2024-06-01' AS value FROM (SELECT DATE '2024-06-01' AS d, '2024-06-01' AS s) t;

-- case: {"id":"date_text_column","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: comparison requires compatible types","difference":"Date/Text coercion remains refused; literal and typed-column policies must be decided separately before implementation."}
SELECT d = s AS value FROM (SELECT DATE '2024-06-01' AS d, '2024-06-01' AS s) t;

-- case: {"id":"text_date_column","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: comparison requires compatible types","difference":"Date/Text coercion remains refused; literal and typed-column policies must be decided separately before implementation."}
SELECT s < d AS value FROM (SELECT DATE '2024-06-01' AS d, '2024-06-01' AS s) t;

-- case: {"id":"date_text_nonliteral","ordered":false,"assay_status":"unsupported","assay_message":"unsupported: comparison requires compatible types","difference":"Date/Text coercion remains refused; literal and typed-column policies must be decided separately before implementation."}
SELECT d = SUBSTRING(s FROM 1 FOR 10) AS value FROM (SELECT DATE '2024-06-01' AS d, '2024-06-01' AS s) t;

-- case: {"id":"int_text_type_error","ordered":false,"assay_status":"error","assay_message":"comparison requires compatible types"}
SELECT i < s AS value FROM (SELECT 1 AS i, 'bad' AS s) t;

-- case: {"id":"text_sum_type_error","ordered":false,"assay_status":"error","assay_message":"expected numeric type, got Text"}
SELECT SUM(s) AS value FROM (SELECT 'bad' AS s) t;

-- case: {"id":"date_sum_type_error","ordered":false,"assay_status":"error","assay_message":"expected numeric type, got Date"}
SELECT SUM(d) AS value FROM (SELECT DATE '2024-06-01' AS d) t;

-- case: {"id":"date_multiply_type_error","ordered":false,"assay_status":"error","assay_message":"expected numeric type, got Date"}
SELECT DATE '2024-06-01' * 2 AS value;

-- case: {"id":"int_sum_duplicates","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT 2 AS x UNION ALL SELECT 2 UNION ALL SELECT NULL) t;

-- case: {"id":"int_avg_duplicates","ordered":false,"assay_status":"ok"}
SELECT AVG(x) AS value FROM (SELECT 2 AS x UNION ALL SELECT 4 UNION ALL SELECT NULL) t;

-- case: {"id":"decimal_sum_duplicates","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT 1.25 AS x UNION ALL SELECT 1.25 UNION ALL SELECT NULL) t;

-- case: {"id":"decimal_avg","ordered":false,"assay_status":"ok"}
SELECT AVG(x) AS value FROM (SELECT 1.25 AS x UNION ALL SELECT 2.75) t;

-- case: {"id":"empty_sum","ordered":false,"assay_status":"ok"}
SELECT SUM(x) AS value FROM (SELECT 2 AS x) t WHERE FALSE;

-- case: {"id":"null_sum","ordered":false,"assay_status":"ok"}
SELECT SUM(NULL) AS value;

-- case: {"id":"null_avg","ordered":false,"assay_status":"ok"}
SELECT AVG(NULL) AS value;

-- case: {"id":"float_count","ordered":false,"assay_status":"ok"}
SELECT COUNT(x) AS value FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"float_extrema","ordered":false,"assay_status":"ok"}
SELECT MIN(x) AS lo, MAX(x) AS hi FROM (SELECT CAST(1 AS REAL) AS x UNION ALL SELECT CAST(1 AS REAL) UNION ALL SELECT NULL) t;

-- case: {"id":"bool_count","ordered":false,"assay_status":"ok"}
SELECT COUNT(x) AS value FROM (SELECT TRUE AS x UNION ALL SELECT TRUE UNION ALL SELECT FALSE UNION ALL SELECT NULL) t;

-- case: {"id":"date_typed_compare","ordered":false,"assay_status":"ok"}
SELECT DATE '2024-06-01' < DATE '2024-06-02' AS value;

-- case: {"id":"text_compare","ordered":false,"assay_status":"ok"}
SELECT 'a' < 'b' AS value;

-- case: {"id":"date_null_compare","ordered":false,"assay_status":"ok"}
SELECT DATE '2024-06-01' = NULL AS value;

-- case: {"id":"float_arithmetic","ordered":false,"assay_status":"ok"}
SELECT CAST(2 AS REAL) + 1 AS value;

-- case: {"id":"ordered_duplicate_rows","ordered":true,"assay_status":"ok"}
SELECT x FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t ORDER BY x;

-- case: {"id":"grouped_decimal_sales","ordered":true,"assay_status":"ok"}
SELECT region, SUM(amount) AS sales FROM (SELECT 'west' AS region, 1.25 AS amount UNION ALL SELECT 'west', 1.25 UNION ALL SELECT 'east', 2.50 UNION ALL SELECT 'west', NULL) t GROUP BY region ORDER BY region;
