-- case: {"id": "right_basic", "ordered": true, "pg": "agree"}
SELECT a.id AS a_id, b.cust AS b_cust, b.note AS note FROM orders_a a RIGHT JOIN orders_b b ON a.cust = b.cust ORDER BY note, a_id NULLS LAST;

-- case: {"id": "right_outer_spelling", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a RIGHT OUTER JOIN orders_b b ON a.cust = b.cust;

-- case: {"id": "right_duplicates_both_sides", "ordered": false, "pg": "agree"}
SELECT pa.x AS x, pb.y AS y FROM pa RIGHT JOIN pb ON pa.k = pb.k;

-- case: {"id": "right_null_keys_never_match", "ordered": false, "pg": "agree"}
SELECT pa.x AS x, pb.y AS y FROM pa RIGHT JOIN pb ON pa.k2 = pb.k2 AND pa.k = pb.k;

-- case: {"id": "right_non_equi", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a RIGHT JOIN orders_b b ON a.amount > b.amount AND a.region = b.region;

-- case: {"id": "right_on_filters_each_side", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a RIGHT JOIN orders_b b ON a.cust = b.cust AND a.amount > 50 AND b.note <> 'second row';

-- case: {"id": "right_empty_left", "ordered": false, "pg": "agree"}
SELECT n.name AS name, c.name AS cname FROM nothing n RIGHT JOIN custs c ON n.cust = c.cust;

-- case: {"id": "right_empty_right", "ordered": false, "pg": "agree"}
SELECT c.name AS cname, n.name AS name FROM custs c RIGHT JOIN nothing n ON n.cust = c.cust;

-- case: {"id": "right_unmatched_rows_only", "ordered": true, "pg": "agree"}
SELECT c.name AS name, c.tier AS tier FROM orders_a a RIGHT JOIN custs c ON a.cust = c.cust WHERE a.id IS NULL ORDER BY name;

-- case: {"id": "right_in_join_tree", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note, c.name AS name FROM orders_a a JOIN orders_b b ON a.cust = b.cust RIGHT JOIN custs c ON b.cust = c.cust;

-- case: {"id": "right_aggregate", "ordered": true, "pg": "agree"}
SELECT c.name AS name, COUNT(a.id) AS orders, SUM(a.amount) AS total FROM orders_a a RIGHT JOIN custs c ON a.cust = c.cust GROUP BY c.name ORDER BY name;

-- case: {"id": "right_text_and_boolean_keys", "ordered": false, "pg": "agree"}
SELECT fa.tag AS a_tag, fb.tag AS b_tag FROM fa RIGHT JOIN fb ON fa.flag = fb.flag AND fa.tag = fb.tag;

-- case: {"id": "full_basic", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust;

-- case: {"id": "full_outer_spelling_star", "ordered": false, "pg": "agree"}
SELECT * FROM custs c FULL OUTER JOIN nothing n ON c.cust = n.cust;

-- case: {"id": "full_duplicates_both_sides", "ordered": false, "pg": "agree"}
SELECT pa.x AS x, pb.y AS y FROM pa FULL JOIN pb ON pa.k = pb.k;

-- case: {"id": "full_composite_null_keys", "ordered": false, "pg": "agree"}
SELECT pa.x AS x, pb.y AS y FROM pa FULL JOIN pb ON pa.k = pb.k AND pa.k2 = pb.k2;

-- case: {"id": "full_non_equi_only", "ordered": false, "pg": "same semantics, but PostgreSQL's planner rejects a FULL JOIN with no hash- or merge-joinable equality (FULL JOIN is only supported with merge-joinable or hash-joinable join conditions)"}
SELECT pa.x AS x, pb.y AS y FROM pa FULL JOIN pb ON pa.x * 10 > pb.y AND pb.y > 40;

-- case: {"id": "full_equi_plus_residual", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust AND a.amount = b.amount;

-- case: {"id": "full_on_filters_each_side", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust AND a.region = 'north' AND b.amount > 10;

-- case: {"id": "full_on_false", "ordered": false, "pg": "same semantics; not established whether PostgreSQL's planner accepts a constant FULL JOIN condition"}
SELECT c.name AS name, pb.y AS y FROM custs c FULL JOIN pb ON FALSE;

-- case: {"id": "full_both_empty", "ordered": false, "pg": "agree"}
SELECT n.name AS a, m.name AS b FROM nothing n FULL JOIN nothing m ON n.cust = m.cust;

-- case: {"id": "full_empty_left", "ordered": false, "pg": "agree"}
SELECT n.name AS a, c.name AS b FROM nothing n FULL JOIN custs c ON n.cust = c.cust;

-- case: {"id": "full_float_bool_text_columns", "ordered": false, "pg": "agree"}
SELECT fa.f AS af, fa.flag AS aflag, fb.f AS bf, fb.flag AS bflag, fb.tag AS btag FROM fa FULL JOIN fb ON fa.f = fb.f;

-- case: {"id": "full_decimal_date_payload", "ordered": false, "pg": "agree"}
SELECT a.amount AS a_amount, a.day AS day, b.amount AS b_amount FROM orders_a a FULL JOIN orders_b b ON a.amount = b.amount;

-- case: {"id": "full_where_unmatched_either", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust WHERE a.id IS NULL OR b.cust IS NULL;

-- case: {"id": "full_coalesce_group", "ordered": true, "pg": "agree"}
SELECT COALESCE(a.cust, b.cust) AS cust, COUNT(a.id) AS a_rows, COUNT(b.note) AS b_rows, SUM(a.amount) AS a_total, SUM(b.amount) AS b_total FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust GROUP BY COALESCE(a.cust, b.cust) ORDER BY cust NULLS LAST;

-- case: {"id": "full_then_left", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note, c.name AS name FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust LEFT JOIN custs c ON c.cust = COALESCE(a.cust, b.cust);

-- case: {"id": "left_then_full", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, c.name AS name, b.note AS note FROM orders_a a LEFT JOIN custs c ON a.cust = c.cust FULL JOIN orders_b b ON b.cust = c.cust;

-- case: {"id": "full_parenthesized_right_input", "ordered": false, "pg": "agree"}
SELECT c.name AS name, a.id AS a_id, b.note AS note FROM custs c FULL JOIN (orders_a a JOIN orders_b b ON a.cust = b.cust) ON c.cust = a.cust;

-- case: {"id": "full_chain", "ordered": false, "pg": "agree"}
SELECT pa.x AS x, pb.y AS y, c.name AS name FROM pa FULL JOIN pb ON pa.k = pb.k AND pa.k2 = pb.k2 FULL JOIN custs c ON c.cust = pb.y;

-- case: {"id": "full_derived_inputs", "ordered": false, "pg": "agree"}
SELECT s.cust AS cust, s.n AS a_orders, t.n AS b_rows FROM (SELECT cust, COUNT(*) AS n FROM orders_a GROUP BY cust) s FULL JOIN (SELECT cust, COUNT(*) AS n FROM orders_b GROUP BY cust) t ON s.cust = t.cust;

-- case: {"id": "full_self_join", "ordered": false, "pg": "agree"}
SELECT x.id AS earlier, y.id AS later FROM orders_a x FULL JOIN orders_a y ON x.id + 1 = y.id;

-- case: {"id": "full_count_star", "ordered": true, "pg": "agree"}
SELECT COUNT(*) AS n, COUNT(a.id) AS a_n, COUNT(b.note) AS b_n FROM orders_a a FULL JOIN orders_b b ON a.cust = b.cust;

-- case: {"id": "using_inner", "ordered": true, "pg": "agree"}
SELECT cust, a.id AS a_id, b.note AS note FROM orders_a a JOIN orders_b b USING (cust) ORDER BY a_id, note;

-- case: {"id": "using_left_merged_is_left", "ordered": true, "pg": "agree"}
SELECT cust, a.cust AS a_cust, b.cust AS b_cust, a.id AS a_id FROM orders_a a LEFT JOIN orders_b b USING (cust) ORDER BY a_id, b.note;

-- case: {"id": "using_right_merged_is_right", "ordered": true, "pg": "agree"}
SELECT cust, a.cust AS a_cust, b.cust AS b_cust, b.note AS note FROM orders_a a RIGHT JOIN orders_b b USING (cust) ORDER BY note, a.id;

-- case: {"id": "using_full_merged_coalesce", "ordered": false, "pg": "agree (merged column is COALESCE(a.cust, b.cust))"}
SELECT cust, a.cust AS a_cust, b.cust AS b_cust, a.id AS a_id, b.note AS note FROM orders_a a FULL JOIN orders_b b USING (cust);

-- case: {"id": "using_full_two_columns", "ordered": false, "pg": "agree"}
SELECT k, k2, pa.x AS x, pb.y AS y FROM pa FULL JOIN pb USING (k, k2);

-- case: {"id": "using_star_leading_columns", "ordered": false, "pg": "agree (USING columns lead the left table, so both list k, k2, x, y)"}
SELECT * FROM pa JOIN pb USING (k, k2);

-- case: {"id": "using_star_full", "ordered": false, "pg": "agree"}
SELECT * FROM custs c FULL JOIN orders_b b USING (cust);

-- case: {"id": "using_qualified_star", "ordered": false, "pg": "agree (t.* keeps all of t's columns, including the USING column)"}
SELECT b.*, c.* FROM orders_b b FULL JOIN custs c USING (cust);

-- case: {"id": "using_where_on_merged", "ordered": true, "pg": "agree"}
SELECT cust, c.name AS name, b.note AS note FROM custs c FULL JOIN orders_b b USING (cust) WHERE cust >= 20 ORDER BY cust, note NULLS LAST;

-- case: {"id": "using_group_by_merged", "ordered": true, "pg": "agree"}
SELECT cust, COUNT(*) AS n, SUM(amount) AS total FROM custs FULL JOIN orders_b USING (cust) GROUP BY cust ORDER BY cust NULLS LAST;

-- case: {"id": "using_case_insensitive", "ordered": false, "pg": "agree"}
SELECT cust, b.note AS note FROM custs c JOIN orders_b b USING (CUST);

-- case: {"id": "using_chain_full", "ordered": false, "pg": "agree (each later USING column is COALESCE of the earlier merged column and the new side)"}
SELECT cust, c.cust AS c_cust, b.cust AS b_cust, a.cust AS a_cust FROM custs c FULL JOIN orders_b b USING (cust) FULL JOIN orders_a a USING (cust);

-- case: {"id": "using_chain_star", "ordered": false, "pg": "agree"}
SELECT * FROM custs c JOIN orders_b b USING (cust) JOIN orders_a a USING (cust);

-- case: {"id": "using_in_subquery", "ordered": true, "pg": "agree"}
SELECT name, (SELECT COUNT(*) FROM orders_a a JOIN orders_b b USING (cust) WHERE cust = c.cust) AS matches FROM custs c ORDER BY name;

-- case: {"id": "using_exists_correlated", "ordered": true, "pg": "agree"}
SELECT c.name AS name FROM custs c WHERE EXISTS (SELECT 1 FROM orders_a a JOIN orders_b b USING (cust) WHERE cust = c.cust) ORDER BY name;

-- case: {"id": "natural_inner", "ordered": false, "pg": "agree (common columns cust, region, amount)"}
SELECT cust, region, amount, a.id AS a_id, b.note AS note FROM orders_a a NATURAL JOIN orders_b b;

-- case: {"id": "natural_left_unmentioned_columns", "ordered": false, "pg": "agree"}
SELECT a.id AS a_id, b.note AS note FROM orders_a a NATURAL LEFT JOIN orders_b b;

-- case: {"id": "natural_full_star", "ordered": false, "pg": "agree (k, k2 lead pa)"}
SELECT * FROM pa NATURAL FULL JOIN pb;

-- case: {"id": "natural_right", "ordered": false, "pg": "agree"}
SELECT cust, name, note FROM custs NATURAL RIGHT JOIN orders_b;

-- case: {"id": "intersect_all_duplicates_nulls", "ordered": false, "pg": "agree (min(m, n) copies; NULLs equal)"}
SELECT a, b FROM ma INTERSECT ALL SELECT a, b FROM mb;

-- case: {"id": "except_all_left_minus_right", "ordered": false, "pg": "agree (max(m - n, 0) copies)"}
SELECT a, b FROM ma EXCEPT ALL SELECT a, b FROM mb;

-- case: {"id": "except_all_right_minus_left", "ordered": false, "pg": "agree"}
SELECT a, b FROM mb EXCEPT ALL SELECT a, b FROM ma;

-- case: {"id": "intersect_all_single_column", "ordered": true, "pg": "agree"}
SELECT a FROM ma INTERSECT ALL SELECT a FROM mb ORDER BY a NULLS FIRST;

-- case: {"id": "intersect_all_with_empty", "ordered": false, "pg": "agree"}
SELECT cust, name FROM custs INTERSECT ALL SELECT cust, name FROM nothing;

-- case: {"id": "except_all_with_empty", "ordered": false, "pg": "agree"}
SELECT cust, name FROM custs EXCEPT ALL SELECT cust, name FROM nothing;

-- case: {"id": "intersect_all_precedence", "ordered": false, "pg": "agree (INTERSECT binds tighter than UNION/EXCEPT)"}
SELECT a FROM ma UNION ALL SELECT a FROM mb INTERSECT ALL SELECT a FROM ma;

-- case: {"id": "except_all_left_associative", "ordered": false, "pg": "agree"}
SELECT a FROM ma EXCEPT ALL SELECT a FROM mb EXCEPT ALL SELECT a FROM ma WHERE a = 1;

-- case: {"id": "intersect_all_widened_types", "ordered": false, "pg": "agree (compared as DECIMAL)"}
SELECT a, b FROM ma INTERSECT ALL SELECT a, b FROM md;

-- case: {"id": "intersect_all_floats_booleans", "ordered": false, "pg": "agree"}
SELECT f, flag FROM fa INTERSECT ALL SELECT f, flag FROM fb;

-- case: {"id": "intersect_all_count", "ordered": true, "pg": "agree"}
SELECT COUNT(*) AS n FROM (SELECT cust FROM orders_a INTERSECT ALL SELECT cust FROM orders_b) t;

-- case: {"id": "intersect_distinct_unchanged", "ordered": false, "pg": "agree"}
SELECT a, b FROM ma INTERSECT SELECT a, b FROM mb;

-- case: {"id": "union_all_order_limit", "ordered": true, "pg": "agree"}
SELECT cust, amount FROM orders_a UNION ALL SELECT cust, amount FROM orders_b ORDER BY amount DESC NULLS LAST, cust NULLS LAST LIMIT 4;

-- case: {"id": "union_all_order_by_ordinal_offset", "ordered": true, "pg": "agree"}
SELECT id AS n FROM orders_a UNION ALL SELECT y FROM pb ORDER BY 1 LIMIT 3 OFFSET 2;

-- case: {"id": "union_order_by_name_case", "ordered": true, "pg": "agree"}
SELECT cust AS customer FROM orders_a UNION SELECT cust FROM orders_b ORDER BY CUSTOMER NULLS FIRST;

-- case: {"id": "union_order_by_unaliased_qualified_output", "ordered": true, "pg": "agree (the output column is named cust)", "assay_columns": ["a.cust"]}
SELECT a.cust FROM orders_a a UNION SELECT b.cust FROM orders_b b ORDER BY cust NULLS FIRST;

-- case: {"id": "except_order_desc_nulls_first", "ordered": true, "pg": "agree"}
SELECT cust FROM custs EXCEPT SELECT cust FROM orders_a ORDER BY cust DESC NULLS FIRST;

-- case: {"id": "intersect_all_order_limit", "ordered": true, "pg": "agree"}
SELECT a, b FROM ma INTERSECT ALL SELECT a, b FROM mb ORDER BY a NULLS LAST, b LIMIT 3;

-- case: {"id": "limit_without_order_count", "ordered": true, "pg": "agree"}
SELECT COUNT(*) AS n FROM (SELECT cust FROM orders_a UNION ALL SELECT cust FROM orders_b LIMIT 5) t;

-- case: {"id": "parenthesized_branches_with_limits", "ordered": false, "pg": "agree"}
(SELECT id FROM orders_a ORDER BY id DESC LIMIT 2) UNION ALL (SELECT x FROM pa ORDER BY x LIMIT 2);

-- case: {"id": "parenthesized_compound_outer_order", "ordered": true, "pg": "agree"}
(SELECT cust FROM orders_a UNION SELECT cust FROM orders_b) ORDER BY 1 DESC NULLS LAST LIMIT 3;

-- case: {"id": "compound_order_in_derived_table", "ordered": true, "pg": "agree"}
SELECT t.v AS v FROM (SELECT amount AS v FROM orders_a UNION ALL SELECT amount FROM orders_b ORDER BY 1 DESC LIMIT 2) t ORDER BY v;

-- case: {"id": "refuse_using_star_not_leading", "ordered": false, "pg": "differs: PostgreSQL lists the merged cust first (cust, id, region, ...); DuckDB keeps it at the left position (id, cust, region, ...)", "assay_error": "unsupported: SELECT * over a USING/NATURAL join", "difference": "column order differs between the references"}
SELECT * FROM orders_a a JOIN orders_b b USING (cust);

-- case: {"id": "refuse_natural_no_common", "ordered": false, "pg": "differs: PostgreSQL returns the cross join", "assay_error": "unsupported: NATURAL JOIN without common columns", "difference": "DuckDB rejects, PostgreSQL cross-joins"}
SELECT * FROM custs NATURAL JOIN (SELECT 1 AS q) t;

-- case: {"id": "refuse_duplicate_using", "ordered": false, "pg": "differs: PostgreSQL rejects a repeated USING column", "assay_error": "unsupported: column cust appears more than once in USING", "difference": "DuckDB accepts, PostgreSQL rejects"}
SELECT cust FROM custs JOIN orders_b USING (cust, cust);

-- case: {"id": "refuse_using_type_mismatch", "ordered": false, "pg": "differs: PostgreSQL's merged column has the common type (numeric); DuckDB keeps the left type", "assay_error": "unsupported: USING column a has different types", "difference": "merged column type differs between the references"}
SELECT a FROM ma JOIN md USING (a);

-- case: {"id": "refuse_parenthesized_join_alias", "ordered": false, "pg": "differs: PostgreSQL answers through the join alias; DuckDB rejects j.x", "assay_error": "unsupported: alias on parenthesized join", "difference": "references disagree; kept refused"}
SELECT j.x AS x FROM (pa JOIN pb USING (k, k2)) AS j;

-- case: {"id": "refuse_lateral", "ordered": false, "pg": "agree (both answer)", "assay_error": "unsupported: table factor", "difference": "LATERAL kept refused"}
SELECT c.name AS name, t.n AS n FROM custs c, LATERAL (SELECT COUNT(*) AS n FROM orders_b b WHERE b.cust = c.cust) t;

-- case: {"id": "refuse_compound_qualified_order", "ordered": true, "pg": "differs: PostgreSQL rejects a qualified ORDER BY over a set operation", "assay_error": "unsupported: qualified ORDER BY name over a set operation", "difference": "DuckDB accepts, PostgreSQL rejects"}
SELECT a.cust FROM orders_a a UNION ALL SELECT b.cust FROM orders_b b ORDER BY a.cust;

-- case: {"id": "refuse_compound_order_expression", "ordered": true, "pg": "agree (PostgreSQL accepts only output column names or positions after a set operation)", "assay_error": "unsupported: ORDER BY expression over a set operation", "difference": "both references reject"}
SELECT cust FROM orders_a UNION ALL SELECT cust FROM orders_b ORDER BY cust + 1;

-- case: {"id": "error_full_without_condition", "ordered": false, "pg": "agree (syntax error)", "assay_error": "unsupported: join requires ON", "difference": "both references reject"}
SELECT * FROM custs FULL JOIN orders_b;

-- case: {"id": "error_using_missing_column", "ordered": false, "pg": "agree (error)", "assay_error": "semantic: USING column note does not exist on the left side", "difference": "both references reject"}
SELECT * FROM custs JOIN orders_b USING (note);
