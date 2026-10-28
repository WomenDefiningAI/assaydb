-- case: {"id": "coalesce_int_real"}
SELECT id, COALESCE(n, r) FROM p ORDER BY id;

-- case: {"id": "coalesce_real_int"}
SELECT id, COALESCE(r, n) FROM p ORDER BY id;

-- case: {"id": "coalesce_real_literal"}
SELECT id, COALESCE(n, CAST(10 AS REAL)) FROM p ORDER BY id;

-- case: {"id": "coalesce_negative_zero"}
SELECT id, COALESCE(n, -CAST(0 AS REAL)) FROM p ORDER BY id;

-- case: {"id": "coalesce_three_args"}
SELECT id, COALESCE(n, r, -1) FROM p ORDER BY id;

-- case: {"id": "case_searched"}
SELECT id, CASE WHEN b IS NULL THEN n ELSE r END FROM p ORDER BY id;

-- case: {"id": "case_computed_int_arm"}
SELECT id, CASE WHEN r IS NULL THEN n / 2 ELSE r END FROM p ORDER BY id;

-- case: {"id": "case_simple"}
SELECT id, CASE id WHEN 1 THEN n WHEN 4 THEN r ELSE n END FROM p ORDER BY id;

-- case: {"id": "case_nested"}
SELECT id, COALESCE(CASE WHEN b = 1 THEN n END, r, n) FROM p ORDER BY id;

-- case: {"id": "nullif_int_real", "assay_rows": [[1, 0], [2, 7], [3, -7], [4, null], [5, null], [6, 9007199254740992], [7, 9007199254740993], [8, -9007199254740993], [9, 9223372036854775807], [10, -9223372036854775808], [11, 9223372036854775806], [12, null]], "note": "Not promoted: ordinary Int/Float NULLIF operands keep the exact comparison (SQLite); DuckDB compares in DOUBLE (row 7 NULL)"}
SELECT id, NULLIF(n, r) FROM p ORDER BY id;

-- case: {"id": "nullif_promoted"}
SELECT id, NULLIF(COALESCE(n, r), n) FROM p ORDER BY id;

-- case: {"id": "a_eq_original_coalesce"}
SELECT id, COALESCE(n, CAST(10 AS REAL)) = n FROM p ORDER BY id;

-- case: {"id": "a_eq_original_case"}
SELECT id, CASE WHEN b IS NULL THEN n ELSE CAST(11 AS REAL) END = n FROM p ORDER BY id;

-- case: {"id": "a_ge_original"}
SELECT id, COALESCE(n, r) >= n FROM p ORDER BY id;

-- case: {"id": "a_ne_original"}
SELECT id, COALESCE(n, r) <> n FROM p ORDER BY id;

-- case: {"id": "a_original_on_left"}
SELECT id, n = COALESCE(n, r) FROM p ORDER BY id;

-- case: {"id": "a_between"}
SELECT id FROM p WHERE COALESCE(n, r) BETWEEN n AND n ORDER BY id;

-- case: {"id": "a_in_list"}
SELECT id, COALESCE(n, r) IN (n, 7) FROM p ORDER BY id;

-- case: {"id": "a_simple_case_operand"}
SELECT id, CASE COALESCE(n, r) WHEN n THEN 1 ELSE 0 END FROM p ORDER BY id;

-- case: {"id": "a_filter_large_literal"}
SELECT id FROM p WHERE COALESCE(n, r) = 9007199254740993 ORDER BY id;

-- case: {"id": "a_arithmetic_then_compare"}
SELECT id, COALESCE(n, r) * 1 = n FROM p ORDER BY id;

-- case: {"id": "a_derived_column"}
SELECT id, x = n FROM (SELECT id, n, COALESCE(n, r) AS x FROM p) q ORDER BY id;

-- case: {"id": "a_aggregate_compare"}
SELECT MAX(COALESCE(n, r)) = MAX(n), MIN(COALESCE(n, r)) = MIN(n) FROM p;

-- case: {"id": "divide_float"}
SELECT id, COALESCE(n, r) / 2 FROM p ORDER BY id;

-- case: {"id": "divide_int_only_control"}
SELECT id, COALESCE(n, 3) / 2 FROM p ORDER BY id;

-- case: {"id": "divide_by_zero"}
SELECT id, COALESCE(n, r) / 0 FROM p ORDER BY id;

-- case: {"id": "multiply_add"}
SELECT id, COALESCE(n, r) * 2, COALESCE(n, r) + 1 FROM p ORDER BY id;

-- case: {"id": "negate_abs"}
SELECT id, -COALESCE(n, r), ABS(COALESCE(n, r)) FROM p WHERE id <> 12 ORDER BY id;

-- case: {"id": "e_cast_integer_in_range"}
SELECT id, CAST(COALESCE(n, r) AS INTEGER) FROM p WHERE id NOT IN (9, 11) ORDER BY id;

-- case: {"id": "e_cast_integer_minus_2_63"}
SELECT id, CAST(COALESCE(n, r) AS INTEGER) FROM p WHERE id = 10;

-- case: {"id": "e_cast_integer_2_63", "assay_error": "INTEGER out of range: CAST of promoted Int/Float value"}
SELECT id, CAST(COALESCE(n, r) AS INTEGER) FROM p ORDER BY id;

-- case: {"id": "e_cast_integer_divide", "assay_error": "INTEGER out of range: CAST of promoted Int/Float value"}
SELECT id, CAST(COALESCE(n, CAST(-5 AS REAL)) AS INTEGER) / 2 FROM p WHERE id = 11;

-- case: {"id": "e_cast_integer_divide_in_range"}
SELECT id, CAST(COALESCE(n, CAST(-5 AS REAL)) AS INTEGER) / 2 FROM p WHERE id IN (2, 3, 5, 6, 7, 10) ORDER BY id;

-- case: {"id": "e_cast_case_left_join", "assay_error": "INTEGER out of range: CAST of promoted Int/Float value"}
SELECT a.id, CAST(COALESCE(c.n, CAST(4 AS REAL)) AS INTEGER) FROM p a LEFT JOIN p c ON a.id = c.id AND c.id = 9 WHERE a.id IN (1, 9) ORDER BY a.id;

-- case: {"id": "b_remainder_in_range"}
SELECT id, COALESCE(n, r) % 2 FROM p WHERE id IN (1, 2, 3, 6, 7) ORDER BY id;

-- case: {"id": "b_remainder_2_63", "note": "REAL remainder is fmod (decision 2026-10-17): the former out-of-INTEGER-range refusal is gone; answer = DuckDB"}
SELECT id, COALESCE(n, r) % 2 FROM p WHERE id = 9;

-- case: {"id": "b_remainder_minus_2_63_exact", "note": "fmod of -2^63 (formerly: SQLite truncation, exact at -2^63)"}
SELECT id, COALESCE(n, r) % 3 FROM p WHERE id = 10;

-- case: {"id": "b_remainder_below_minus_2_63", "note": "REAL remainder is fmod (decision 2026-10-17): the former out-of-INTEGER-range refusal is gone; answer = DuckDB"}
SELECT id, (COALESCE(n, r) * 2) % 3 FROM p WHERE id = 10;

-- case: {"id": "b_remainder_cte_2_63", "note": "REAL remainder is fmod (decision 2026-10-17): the former out-of-INTEGER-range refusal is gone; answer = DuckDB"}
WITH q AS (SELECT id, COALESCE(n, CAST(-15 AS REAL)) AS x FROM p) SELECT id, x % 2 FROM q WHERE id = 11;

-- case: {"id": "b_remainder_cte_in_range"}
WITH q AS (SELECT id, COALESCE(n, CAST(-15 AS REAL)) AS x FROM p) SELECT id, x % 4 FROM q WHERE id IN (2, 3, 5, 7) ORDER BY id;

-- case: {"id": "sum_avg_exact"}
SELECT SUM(COALESCE(n, r)), AVG(COALESCE(n, r)), COUNT(COALESCE(n, r)) FROM p WHERE id <= 5 OR id = 12;

-- case: {"id": "sum_case_arms"}
SELECT SUM(CASE WHEN b = 1 THEN n ELSE r END) FROM p WHERE id <= 4;

-- case: {"id": "min_max"}
SELECT MIN(COALESCE(n, r)), MAX(COALESCE(n, r)) FROM p;

-- case: {"id": "group_by_promoted_key"}
SELECT COALESCE(n, r) AS k, COUNT(*) FROM p WHERE id <> 5 GROUP BY COALESCE(n, r) ORDER BY k;

-- case: {"id": "count_distinct_promoted"}
SELECT COUNT(DISTINCT COALESCE(n, r)) FROM p;

-- case: {"id": "order_by_promoted"}
SELECT id FROM p WHERE id NOT IN (5, 12) ORDER BY COALESCE(n, r), id;

-- case: {"id": "in_subquery_int_in_real"}
SELECT id FROM p WHERE n IN (SELECT r FROM p WHERE r IS NOT NULL) ORDER BY id;

-- case: {"id": "in_subquery_real_in_int"}
SELECT id FROM p WHERE r IN (SELECT n FROM p WHERE n IS NOT NULL) ORDER BY id;

-- case: {"id": "not_in_subquery"}
SELECT id FROM p WHERE n NOT IN (SELECT r FROM p WHERE r IS NOT NULL) ORDER BY id;

-- case: {"id": "in_subquery_projection"}
SELECT id, n IN (SELECT r FROM p WHERE r IS NOT NULL) FROM p ORDER BY id;

-- case: {"id": "in_subquery_promoted"}
SELECT id FROM p WHERE n IN (SELECT COALESCE(n, r) FROM p WHERE id >= 6) ORDER BY id;

-- case: {"id": "join_on_promoted"}
SELECT a.id, c.id FROM p a JOIN p c ON COALESCE(a.n, a.r) = c.n ORDER BY 1, 2;

-- case: {"id": "left_join_coalesce"}
SELECT a.id, COALESCE(c.n, CAST(0 AS REAL)) FROM p a LEFT JOIN p c ON a.id = c.id AND c.b = 1 ORDER BY a.id;

-- case: {"id": "derived_alias_arithmetic"}
SELECT id, x * 2 FROM (SELECT id, COALESCE(n, r) AS x FROM p) q ORDER BY id;

-- case: {"id": "nonfinite_overflow", "assay_error": "floating arithmetic overflow", "note": "Assay has no nonfinite REAL values (existing policy); DuckDB returns +inf"}
SELECT id, COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) * COALESCE(n, r) FROM p WHERE id = 9;

-- case: {"id": "control_explicit_cast_exact", "assay_rows": [[1, true], [2, true], [3, true], [4, null], [5, null], [6, true], [7, false], [8, false], [9, false], [10, true], [11, false], [12, null]], "note": "Not promoted: explicit CAST and ordinary columns keep exact Int/Float comparison (SQLite); DuckDB compares in DOUBLE"}
SELECT id, CAST(n AS REAL) = n FROM p ORDER BY id;

-- case: {"id": "control_column_exact", "assay_rows": [[1, false], [2, null], [3, false], [4, null], [5, null], [6, null], [7, false], [8, null], [9, null], [10, null], [11, null], [12, null]], "note": "Not promoted: explicit CAST and ordinary columns keep exact Int/Float comparison (SQLite); DuckDB compares in DOUBLE"}
SELECT id, r = n FROM p ORDER BY id;
