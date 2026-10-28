-- case: {"id": "lag_default", "ordered": true}
SELECT id,LAG(v) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_0", "ordered": true}
SELECT id,LAG(v,0) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_1", "ordered": true}
SELECT id,LAG(v,1) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_2", "ordered": true}
SELECT id,LAG(v,2) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_99", "ordered": true}
SELECT id,LAG(v,99) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_9223372036854775807", "ordered": true}
SELECT id,LAG(v,9223372036854775807) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_fallback", "ordered": true}
SELECT id,LAG(v,2,-7) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_empty", "ordered": true}
SELECT LAG(v) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t WHERE FALSE;

-- case: {"id": "lag_desc", "ordered": true}
SELECT id,LAG(v) OVER (ORDER BY v DESC NULLS FIRST,id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_nulls_last", "ordered": true}
SELECT id,LAG(v) OVER (ORDER BY v ASC NULLS LAST,id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_null_expr", "ordered": true, "assay_error": "unsupported: untyped NULL offset input with non-NULL default (reference disagreement)", "reference_error": true, "difference": "DuckDB 1.5.6 rejects casting the integer default to its untyped NULL input; common semantics need a decision. Original case retained."}
SELECT id,LAG(NULL,1,5) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_all_null", "ordered": true}
SELECT id,LAG(NULL) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_decimal", "ordered": true}
SELECT id,LAG(CAST(v AS DECIMAL(38,2)),1,-1.25) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_date", "ordered": true}
SELECT id,LAG(DATE '2024-01-02',1,DATE '2023-12-31') OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_text", "ordered": true}
SELECT id,LAG(CASE WHEN v IS NULL THEN NULL ELSE 'héllo' END,1,'none') OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_bool", "ordered": true}
SELECT id,LAG(v>20,1,FALSE) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_float", "ordered": true}
SELECT id,LAG(CAST(v AS DOUBLE),1,0.5) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_frame_ignored", "ordered": true}
SELECT id,LAG(v,2,-9) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_range_ignored", "ordered": true}
SELECT id,LAG(v) OVER (ORDER BY id RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_expression", "ordered": true}
SELECT id,LAG(v+1) OVER (ORDER BY id) * 2 AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lag_duplicates", "ordered": false}
SELECT LAG(v) OVER (ORDER BY v) AS x FROM (SELECT 3 AS v UNION ALL SELECT 3 UNION ALL SELECT 5) t;

-- case: {"id": "lead_default", "ordered": true}
SELECT id,LEAD(v) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_0", "ordered": true}
SELECT id,LEAD(v,0) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_1", "ordered": true}
SELECT id,LEAD(v,1) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_2", "ordered": true}
SELECT id,LEAD(v,2) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_99", "ordered": true}
SELECT id,LEAD(v,99) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_9223372036854775807", "ordered": true}
SELECT id,LEAD(v,9223372036854775807) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_fallback", "ordered": true}
SELECT id,LEAD(v,2,-7) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_empty", "ordered": true}
SELECT LEAD(v) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t WHERE FALSE;

-- case: {"id": "lead_desc", "ordered": true}
SELECT id,LEAD(v) OVER (ORDER BY v DESC NULLS FIRST,id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_nulls_last", "ordered": true}
SELECT id,LEAD(v) OVER (ORDER BY v ASC NULLS LAST,id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_null_expr", "ordered": true, "assay_error": "unsupported: untyped NULL offset input with non-NULL default (reference disagreement)", "reference_error": true, "difference": "DuckDB 1.5.6 rejects casting the integer default to its untyped NULL input; common semantics need a decision. Original case retained."}
SELECT id,LEAD(NULL,1,5) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_all_null", "ordered": true}
SELECT id,LEAD(NULL) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_decimal", "ordered": true}
SELECT id,LEAD(CAST(v AS DECIMAL(38,2)),1,-1.25) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_date", "ordered": true}
SELECT id,LEAD(DATE '2024-01-02',1,DATE '2023-12-31') OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_text", "ordered": true}
SELECT id,LEAD(CASE WHEN v IS NULL THEN NULL ELSE 'héllo' END,1,'none') OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_bool", "ordered": true}
SELECT id,LEAD(v>20,1,FALSE) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_float", "ordered": true}
SELECT id,LEAD(CAST(v AS DOUBLE),1,0.5) OVER (PARTITION BY p ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_frame_ignored", "ordered": true}
SELECT id,LEAD(v,2,-9) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_range_ignored", "ordered": true}
SELECT id,LEAD(v) OVER (ORDER BY id RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_expression", "ordered": true}
SELECT id,LEAD(v+1) OVER (ORDER BY id) * 2 AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t ORDER BY id;

-- case: {"id": "lead_duplicates", "ordered": false}
SELECT LEAD(v) OVER (ORDER BY v) AS x FROM (SELECT 3 AS v UNION ALL SELECT 3 UNION ALL SELECT 5) t;

-- case: {"id": "round_double_None", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE)) AS pos, ROUND(CAST(-123.455 AS DOUBLE)) AS neg;

-- case: {"id": "round_double_0", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),0) AS pos, ROUND(CAST(-123.455 AS DOUBLE),0) AS neg;

-- case: {"id": "round_double_1", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),1) AS pos, ROUND(CAST(-123.455 AS DOUBLE),1) AS neg;

-- case: {"id": "round_double_2", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),2) AS pos, ROUND(CAST(-123.455 AS DOUBLE),2) AS neg;

-- case: {"id": "round_double_3", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),3) AS pos, ROUND(CAST(-123.455 AS DOUBLE),3) AS neg;

-- case: {"id": "round_double_9", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),9) AS pos, ROUND(CAST(-123.455 AS DOUBLE),9) AS neg;

-- case: {"id": "round_double_-1", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),-1) AS pos, ROUND(CAST(-123.455 AS DOUBLE),-1) AS neg;

-- case: {"id": "round_double_-3", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),-3) AS pos, ROUND(CAST(-123.455 AS DOUBLE),-3) AS neg;

-- case: {"id": "round_double_-39", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),-39) AS pos, ROUND(CAST(-123.455 AS DOUBLE),-39) AS neg;

-- case: {"id": "round_double_2147483647", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),2147483647) AS pos, ROUND(CAST(-123.455 AS DOUBLE),2147483647) AS neg;

-- case: {"id": "round_double_-2147483648", "ordered": false}
SELECT ROUND(CAST(123.455 AS DOUBLE),-2147483648) AS pos, ROUND(CAST(-123.455 AS DOUBLE),-2147483648) AS neg;

-- case: {"id": "round_decimal_None", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3))) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3))) AS neg;

-- case: {"id": "round_decimal_0", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),0) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),0) AS neg;

-- case: {"id": "round_decimal_1", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),1) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),1) AS neg;

-- case: {"id": "round_decimal_2", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),2) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),2) AS neg;

-- case: {"id": "round_decimal_3", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),3) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),3) AS neg;

-- case: {"id": "round_decimal_9", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),9) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),9) AS neg;

-- case: {"id": "round_decimal_-1", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),-1) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),-1) AS neg;

-- case: {"id": "round_decimal_-3", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),-3) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),-3) AS neg;

-- case: {"id": "round_decimal_-39", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),-39) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),-39) AS neg;

-- case: {"id": "round_decimal_2147483647", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),2147483647) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),2147483647) AS neg;

-- case: {"id": "round_decimal_-2147483648", "ordered": false}
SELECT ROUND(CAST(123.455 AS DECIMAL(38,3)),-2147483648) AS pos, ROUND(CAST(-123.455 AS DECIMAL(38,3)),-2147483648) AS neg;

-- case: {"id": "round_nulls", "ordered": false}
SELECT ROUND(CAST(NULL AS DOUBLE),2) AS f, ROUND(CAST(NULL AS DECIMAL(38,2)),1) AS d, ROUND(1.25,NULL) AS n;

-- case: {"id": "round_int", "ordered": false}
SELECT ROUND(125,-1) AS pos, ROUND(-125,-1) AS neg, ROUND(12,3) AS same;

-- case: {"id": "round_float_half", "ordered": false}
SELECT ROUND(CAST(2.675 AS DOUBLE),2) AS a, ROUND(CAST(-2.675 AS DOUBLE),2) AS b, ROUND(CAST(1.005 AS DOUBLE),2) AS c;

-- case: {"id": "round_38_boundary", "ordered": false}
SELECT ROUND(CAST(99999999999999999999999999999999999.995 AS DECIMAL(38,3)),2) AS x;

-- case: {"id": "annual_energy_change", "ordered": true}
WITH calendar AS (
 SELECT 10 AS period_id, DATE '2018-06-01' AS measured_on UNION ALL SELECT 20, DATE '2019-06-01' UNION ALL SELECT 30, DATE '2020-06-01' UNION ALL SELECT 40, DATE '2021-06-01'
), energy AS (
 SELECT 10 AS period_id, CAST(80 AS DECIMAL(15,2)) AS quantity, CAST(0.75 AS DECIMAL(15,2)) AS factor
 UNION ALL SELECT 10,40.00,1.50 UNION ALL SELECT 20,100.00,1.50 UNION ALL SELECT 30,120.00,0.75 UNION ALL SELECT 40,120.00,1.50
), annual AS (
 SELECT EXTRACT(YEAR FROM measured_on) AS measured_year, SUM(quantity * factor) AS energy_total
 FROM calendar JOIN energy ON calendar.period_id = energy.period_id GROUP BY 1
)
SELECT measured_year, energy_total, LAG(energy_total) OVER (ORDER BY measured_year) AS preceding_total,
 ROUND(100.0 * (energy_total - LAG(energy_total) OVER (ORDER BY measured_year)) / LAG(energy_total) OVER (ORDER BY measured_year), 2) AS change_percent
FROM annual ORDER BY measured_year;

-- case: {"id": "grouped_direct", "ordered": true}
SELECT p,SUM(v) AS total,LAG(SUM(v)) OVER (ORDER BY p NULLS LAST) AS prior FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t GROUP BY p ORDER BY p NULLS LAST;

-- case: {"id": "window_nesting_derived", "ordered": true}
SELECT id,LAG(x) OVER (ORDER BY id) AS y FROM (SELECT id,LEAD(v) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id, 1 AS p, 10 AS v UNION ALL SELECT 2,1,NULL UNION ALL SELECT 3,1,30 UNION ALL SELECT 4,2,40 UNION ALL SELECT 5,2,50 UNION ALL SELECT 6,NULL,60 UNION ALL SELECT 7,NULL,NULL) t) q ORDER BY id;

-- case: {"id":"typed_null_defaults","ordered":false}
SELECT LAG(CAST(NULL AS INTEGER),1,5) OVER (ORDER BY id) AS i, LEAD(CAST(NULL AS DECIMAL(38,2)),1,1.25) OVER (ORDER BY id) AS d FROM (SELECT 1 AS id UNION ALL SELECT 2) t;

-- case: {"id":"signed_parenthesized_literals","ordered":true}
SELECT id,LAG(id,(+1),0) OVER (ORDER BY id) AS prior,ROUND(CAST(12.55 AS DOUBLE),(-1)) AS r FROM (SELECT 1 AS id UNION ALL SELECT 2) t ORDER BY id;

-- case: {"id":"groups_frame_ignored","ordered":true}
SELECT id,LEAD(id) OVER (ORDER BY id GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS following FROM (SELECT 1 AS id UNION ALL SELECT 2) t ORDER BY id;

-- case: {"id":"round_empty_and_masked","ordered":false}
SELECT ROUND(CAST(1.25 AS DOUBLE),2) AS r FROM (SELECT 1 AS id) t WHERE FALSE;

-- case: {"id": "decimal_negative_width_boundary", "ordered": false, "assay_error": "unsupported: negative ROUND at integral-width boundary (reference precision policy differs)", "difference": "DuckDB returns zero at its declared integral-width boundary. Exact half-away rounding produces a carry; this new domain stays refused pending a precision policy."}
SELECT ROUND(CAST(999.9 AS DECIMAL(4,1)),-3) AS x;

-- case: {"id": "decimal_negative_precision38", "ordered": false, "assay_error": "unsupported: negative ROUND at integral-width boundary (reference precision policy differs)", "difference": "DuckDB returns zero at its declared integral-width boundary. Exact half-away rounding produces a carry; this new domain stays refused pending a precision policy."}
SELECT ROUND(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)),-38) AS x;

-- case: {"id": "int_negative_width_boundary", "ordered": false, "assay_error": "unsupported: negative ROUND at integral-width boundary (reference precision policy differs)", "difference": "DuckDB returns zero at its declared integral-width boundary. Exact half-away rounding produces a carry; this new domain stays refused pending a precision policy."}
SELECT ROUND(CAST(9223372036854775807 AS BIGINT),-19) AS x;

-- case: {"id": "decimal_default_int_positive", "ordered": true}
SELECT id,LAG(CAST(id AS BIGINT),1,1.5) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id UNION ALL SELECT 2) t ORDER BY id;

-- case: {"id": "decimal_default_int_negative", "ordered": true}
SELECT id,LAG(CAST(id AS BIGINT),1,-2.5) OVER (ORDER BY id) AS x FROM (SELECT 1 AS id UNION ALL SELECT 2) t ORDER BY id;
