-- case: {"id": "where_decimal_zero_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x <> 0 AND 10 / x > 1;

-- case: {"id": "where_decimal_zero_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT count(*) AS n FROM g WHERE x <> 0 AND 10 / x > 1"}
SELECT count(*) AS n FROM g WHERE 10 / x > 1 AND x <> 0;

-- case: {"id": "where_decimal_zero_or_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x = 0 OR 10 / x > 1;

-- case: {"id": "where_decimal_zero_or_guard_reversed", "ordered": true, "references": "PostgreSQL raises", "expect": "rows", "oracle": "SELECT count(*) AS n FROM g WHERE x = 0 OR 10 / x > 1", "difference": "PostgreSQL evaluates the OR left to right and raises on 10/0; DuckDB returns NULL for 10/0. Assay: the TRUE operand decides the row in either order"}
SELECT count(*) AS n FROM g WHERE 10 / x > 1 OR x = 0;

-- case: {"id": "where_not_or_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE NOT (x = 0 OR 10 / x <= 1);

-- case: {"id": "where_between_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x BETWEEN 1 AND 100 AND 10 / x > 1;

-- case: {"id": "where_in_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x IN (1, 2.5, 8, 10) AND 10 / x > 1;

-- case: {"id": "where_not_in_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x NOT IN (0) AND 10 / x > 1;

-- case: {"id": "where_three_conjuncts_float_by_decimal", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE x IS NOT NULL AND x <> 0 AND f / x > 0 ORDER BY id;

-- case: {"id": "where_nested_or_of_guards", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE (x <> 0 AND 10 / x > 1) OR (x = 0 AND d > 5) ORDER BY id;

-- case: {"id": "where_deep_nesting", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE x <> 0 AND (d IS NULL OR (10 / x > 1 AND f BETWEEN -1000 AND 1000 AND f * 10 > 0)) ORDER BY id;

-- case: {"id": "where_integer_division_control", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE d <> 0 AND 60 / d > 9;

-- case: {"id": "where_unguarded_decimal_division", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; Assay keeps its DECIMAL division-by-zero error (no guard decides the row)"}
SELECT count(*) AS n FROM g WHERE 10 / x > 1;

-- case: {"id": "where_guard_misses_a_row", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; row id=11 passes the guard d <> 0 with x = 0, so Assay raises"}
SELECT count(*) AS n FROM g WHERE d <> 0 AND 10 / x > 1;

-- case: {"id": "where_null_conjunct_decides_filter", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE flag = 'num' AND CAST(s AS INTEGER) > 5;

-- case: {"id": "where_cast_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE s IN ('12', '7', '3', '2', '40', '6', '-8', '15', '5', '9') AND CAST(s AS INTEGER) > 5;

-- case: {"id": "where_cast_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT count(*) AS n FROM g WHERE s IN ('12', '7', '3', '2', '40', '6', '-8', '15', '5', '9') AND CAST(s AS INTEGER) > 5"}
SELECT count(*) AS n FROM g WHERE CAST(s AS INTEGER) > 5 AND s IN ('12', '7', '3', '2', '40', '6', '-8', '15', '5', '9');

-- case: {"id": "where_cast_guard_two_conjuncts", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE flag IS NOT NULL AND flag = 'num' AND CAST(s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "where_cast_or_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE id IN (2, 5, 8, 12) OR CAST(s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "where_cast_or_guard_reversed", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT id FROM g WHERE id IN (2, 5, 8, 12) OR CAST(s AS INTEGER) > 5 ORDER BY id", "difference": "Both references evaluate this OR left to right and raise on CAST('abc'); Assay's rule is order-independent: id IN (...) is TRUE on every failing row, so the row is decided. Expected rows come from the guard-first spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT id FROM g WHERE CAST(s AS INTEGER) > 5 OR id IN (2, 5, 8, 12) ORDER BY id;

-- case: {"id": "where_unguarded_cast", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise; Assay raises the first failing row's error"}
SELECT count(*) AS n FROM g WHERE CAST(s AS INTEGER) > 5;

-- case: {"id": "where_cast_guard_misses_rows", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"x9\"", "difference": "Both raise on 'x9' (id 5), which the guard id <> 2 does not exclude"}
SELECT count(*) AS n FROM g WHERE id <> 2 AND CAST(s AS INTEGER) > 5;

-- case: {"id": "where_float_overflow_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE f BETWEEN -1000 AND 1000 AND f * 10 > 1;

-- case: {"id": "where_float_overflow_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT count(*) AS n FROM g WHERE f BETWEEN -1000 AND 1000 AND f * 10 > 1"}
SELECT count(*) AS n FROM g WHERE f * 10 > 1 AND f BETWEEN -1000 AND 1000;

-- case: {"id": "where_unguarded_float_overflow", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "floating arithmetic overflow", "difference": "DuckDB returns an infinity, PostgreSQL raises; Assay keeps its Float overflow error"}
SELECT count(*) AS n FROM g WHERE f * 10 > 1;

-- case: {"id": "where_float_to_integer_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE f BETWEEN -1000 AND 1000 AND CAST(f AS INTEGER) > 1 ORDER BY id;

-- case: {"id": "where_float_to_integer_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT id FROM g WHERE f BETWEEN -1000 AND 1000 AND CAST(f AS INTEGER) > 1 ORDER BY id"}
SELECT id FROM g WHERE CAST(f AS INTEGER) > 1 AND f BETWEEN -1000 AND 1000 ORDER BY id;

-- case: {"id": "where_unguarded_float_to_integer", "ordered": true, "references": "both raise", "expect": "error", "error": "integer out of range", "difference": "Both raise; Assay raises its 64-bit INTEGER range error"}
SELECT count(*) AS n FROM g WHERE CAST(f AS INTEGER) > 1;

-- case: {"id": "where_decimal_precision_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE abs(x) < 9 AND CAST(x AS DECIMAL(3,2)) > 1 ORDER BY id;

-- case: {"id": "where_unguarded_decimal_precision", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "decimal cast precision overflow", "difference": "DuckDB 1.5.6 does not raise here (it evaluates the comparison without the cast on these rows), PostgreSQL raises numeric field overflow; Assay keeps its cast error"}
SELECT id FROM g WHERE CAST(x AS DECIMAL(3,2)) > 1 ORDER BY id;

-- case: {"id": "where_date_cast_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE ds LIKE '20__-0%' AND CAST(ds AS DATE) >= DATE '2024-01-01' ORDER BY id;

-- case: {"id": "where_unguarded_date_cast", "ordered": true, "references": "both raise", "expect": "error", "error": "unsupported: CAST Text AS Date (PostgreSQL/DuckDB agreement unestablished)", "difference": "Both raise on 'TBD'; Assay refuses text it cannot prove both references parse alike (unchanged)"}
SELECT id FROM g WHERE id <> 2 AND CAST(ds AS DATE) >= DATE '2024-01-01' ORDER BY id;

-- case: {"id": "where_both_operands_fail_each_row_decided", "ordered": true, "references": "PostgreSQL raises", "expect": "rows", "oracle": "SELECT id FROM g WHERE id NOT IN (2, 5, 11, 12) AND CASE WHEN x <> 0 THEN 10 / x > 1 END AND CASE WHEN s NOT IN ('zz') THEN CAST(s AS INTEGER) > 5 END ORDER BY id", "difference": "PostgreSQL raises on 10/0 for id 7 (its qual order reaches the division first); DuckDB answers. Every failing row is decided by another conjunct, so Assay answers"}
SELECT id FROM g WHERE id NOT IN (2, 5, 11, 12) AND 10 / x > 1 AND CAST(s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "where_both_operands_fail_left_error", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise (DuckDB names the cast, PostgreSQL the division by zero); on row id 2 both Assay operands fail and the left operand's error is raised"}
SELECT id FROM g WHERE CAST(s AS INTEGER) > 5 AND 10 / x > 1 ORDER BY id;

-- case: {"id": "where_both_operands_fail_left_error_reversed", "ordered": true, "references": "both raise", "expect": "error", "error": "division by zero", "difference": "Both raise (DuckDB names the cast, PostgreSQL the division by zero); on row id 2 both Assay operands fail and the left operand's error is raised"}
SELECT id FROM g WHERE 10 / x > 1 AND CAST(s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "select_and_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, x <> 0 AND 10 / x > 1 AS r FROM g ORDER BY id;

-- case: {"id": "select_and_guard_reversed", "ordered": true, "references": "PostgreSQL raises", "expect": "rows", "difference": "PostgreSQL evaluates the projected AND left to right and raises on 10/0; DuckDB returns NULL for 10/0. Assay: the FALSE operand decides in either order"}
SELECT id, 10 / x > 1 AND x <> 0 AS r FROM g ORDER BY id;

-- case: {"id": "select_or_cast_guard", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT id, CASE WHEN id IN (2, 5, 8, 12) THEN TRUE ELSE CAST(s AS INTEGER) > 5 END AS r FROM g ORDER BY id", "difference": "DuckDB evaluates both operands of a projected OR and raises; PostgreSQL short-circuits. Assay answers like PostgreSQL"}
SELECT id, id IN (2, 5, 8, 12) OR CAST(s AS INTEGER) > 5 AS r FROM g ORDER BY id;

-- case: {"id": "select_not_and_cast_guard", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT id, NOT CASE WHEN id IN (2, 5, 8, 12) THEN FALSE ELSE CAST(s AS INTEGER) > 5 END AS r FROM g ORDER BY id", "difference": "DuckDB evaluates both operands of a projected AND and raises; PostgreSQL short-circuits. Assay answers like PostgreSQL"}
SELECT id, NOT (id NOT IN (2, 5, 8, 12) AND CAST(s AS INTEGER) > 5) AS r FROM g ORDER BY id;

-- case: {"id": "select_null_guard_needs_value", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "A NULL operand does not decide a projected AND (the value is FALSE or NULL depending on the failing operand); PostgreSQL raises, DuckDB returns NULL for 10/0. Assay raises"}
SELECT id, (d > 100 OR NULL) AND 10 / x > 1 AS r FROM g ORDER BY id;

-- case: {"id": "select_case_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, CASE WHEN x <> 0 THEN 10 / x END AS r FROM g ORDER BY id;

-- case: {"id": "select_case_and_condition", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT id, CASE WHEN flag IS NOT NULL AND flag = 'num' THEN CASE WHEN CAST(s AS INTEGER) > 5 THEN 'big' ELSE 'other' END ELSE 'other' END AS r FROM g ORDER BY id"}
SELECT id, CASE WHEN flag IS NOT NULL AND flag = 'num' AND CAST(s AS INTEGER) > 5 THEN 'big' ELSE 'other' END AS r FROM g ORDER BY id;

-- case: {"id": "select_coalesce_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, COALESCE(CASE WHEN flag = 'num' THEN CAST(s AS INTEGER) END, -1) AS r FROM g ORDER BY id;

-- case: {"id": "select_coalesce_lazy_second_argument", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, COALESCE(d, CAST(s AS INTEGER)) AS r FROM g WHERE id <> 4 ORDER BY id;

-- case: {"id": "select_nullif_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, CASE WHEN NULLIF(x, 0) IS NOT NULL THEN 10 / x END AS r FROM g ORDER BY id;

-- case: {"id": "select_nullif_divisor", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, 10 / NULLIF(x, 0) AS r FROM g ORDER BY id;

-- case: {"id": "select_unguarded", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; Assay keeps its error"}
SELECT id, 10 / x AS r FROM g ORDER BY id;

-- case: {"id": "join_on_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND h.y <> 0 AND g.x / h.y > 1 ORDER BY g.id;

-- case: {"id": "join_on_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND h.y <> 0 AND g.x / h.y > 1 ORDER BY g.id"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND g.x / h.y > 1 AND h.y <> 0 ORDER BY g.id;

-- case: {"id": "left_join_on_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT g.id AS id, h.label AS label FROM g LEFT JOIN h ON g.id = h.id AND h.label = 'ok' AND CAST(g.s AS INTEGER) > 5 ORDER BY g.id;

-- case: {"id": "join_where_cross_table_guard", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT g.id AS id FROM g JOIN h ON g.id = h.id WHERE CASE WHEN h.label = 'ok' THEN CAST(g.s AS INTEGER) > 5 END ORDER BY g.id", "difference": "Both references push the single-table conjunct CAST(g.s ...) > 5 below the join and raise on rows the guard h.label = 'ok' (or the join itself) discards. Assay decides per joined row: every failing row has h.label <> 'ok' or no match. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id WHERE h.label = 'ok' AND CAST(g.s AS INTEGER) > 5 ORDER BY g.id;

-- case: {"id": "join_unguarded", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; Assay keeps its error"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id WHERE g.x / h.y > 1 ORDER BY g.id;

-- case: {"id": "having_aggregate_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, count(*) AS n FROM g GROUP BY k HAVING sum(x) <> 0 AND 10 / sum(x) > 0 ORDER BY k;

-- case: {"id": "having_aggregate_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows", "oracle": "SELECT k, count(*) AS n FROM g GROUP BY k HAVING sum(x) <> 0 AND 10 / sum(x) > 0 ORDER BY k"}
SELECT k, count(*) AS n FROM g GROUP BY k HAVING 10 / sum(x) > 0 AND sum(x) <> 0 ORDER BY k;

-- case: {"id": "having_key_cast_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT s, count(*) AS n FROM g GROUP BY s HAVING s IN ('12', '7', '40', '6', '15', '9') AND CAST(s AS INTEGER) > 5 ORDER BY s;

-- case: {"id": "having_unguarded", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; Assay keeps its error"}
SELECT k, count(*) AS n FROM g GROUP BY k HAVING 10 / sum(x) > 0 ORDER BY k;

-- case: {"id": "grouped_where_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, d, count(*) AS n FROM g WHERE x <> 0 AND 10 / x > 1 GROUP BY k, d ORDER BY k, d;

-- case: {"id": "grouped_filter_predicate_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, count(*) FILTER (WHERE x <> 0 AND 10 / x > 1) AS n FROM g GROUP BY k ORDER BY k;

-- case: {"id": "grouped_filter_predicate_cast_guard", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT k, count(CASE WHEN flag IS NOT NULL AND flag = 'num' THEN CASE WHEN CAST(s AS INTEGER) > 5 THEN 1 END END) AS n FROM g GROUP BY k ORDER BY k", "difference": "DuckDB evaluates the FILTER predicate's operands eagerly and raises; PostgreSQL short-circuits. Assay answers like PostgreSQL"}
SELECT k, count(*) FILTER (WHERE flag IS NOT NULL AND flag = 'num' AND CAST(s AS INTEGER) > 5) AS n FROM g GROUP BY k ORDER BY k;

-- case: {"id": "grouped_case_and_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, sum(CASE WHEN x <> 0 AND 10 / x > 1 THEN 1 ELSE 0 END) AS n FROM g GROUP BY k ORDER BY k;

-- case: {"id": "grouped_keys_share_guarded_operand", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT x <> 0 AND 10 / x > 1 AS a, x = 0 OR 10 / x < 5 AS b, count(*) AS n FROM g GROUP BY 1, 2 ORDER BY 1, 2;

-- case: {"id": "grouped_aggregates_share_guarded_operand", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, count(x <> 0 AND 10 / x > 1 OR NULL) AS a, count(x = 0 OR 10 / x < 5 OR NULL) AS b FROM g GROUP BY k ORDER BY k;

-- case: {"id": "filter_argument_decimal_division", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, sum(10 / x) FILTER (WHERE x <> 0) AS r FROM g GROUP BY k ORDER BY k;

-- case: {"id": "filter_argument_float_overflow", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT sum(f * 10) FILTER (WHERE f BETWEEN -1000 AND 1000) AS r, count(*) AS n FROM g;

-- case: {"id": "filter_argument_float_by_decimal", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT k, avg(f / x) FILTER (WHERE x <> 0 AND f BETWEEN -1000 AND 1000) AS r FROM g GROUP BY k ORDER BY k;

-- case: {"id": "filter_argument_cast_refused", "ordered": true, "references": "DuckDB raises", "expect": "error", "error": "unsupported: aggregate FILTER over an argument that may fail (DuckDB evaluates it on filtered-out rows; PostgreSQL does not)", "difference": "DuckDB evaluates FILTER arguments on filtered-out rows and raises a conversion error; PostgreSQL does not. Assay keeps its refusal for arguments that can raise an error DuckDB also raises"}
SELECT sum(CAST(s AS INTEGER)) FILTER (WHERE flag = 'num') AS r FROM g;

-- case: {"id": "filter_argument_kept_row_fails", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "The kept row x = 0 needs 10/0: PostgreSQL raises, DuckDB returns NULL. Assay keeps its error"}
SELECT sum(10 / x) FILTER (WHERE x >= 0) AS r FROM g;

-- case: {"id": "window_case_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, sum(CASE WHEN x <> 0 THEN 10 / x END) OVER (PARTITION BY k ORDER BY id) AS r FROM g WHERE k IS NOT NULL ORDER BY id;

-- case: {"id": "window_over_guarded_where", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, count(*) OVER (PARTITION BY k) AS n FROM g WHERE flag IS NOT NULL AND flag = 'num' AND CAST(s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "window_with_guarded_projection", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id, x <> 0 AND 10 / x > 1 AS r, row_number() OVER (ORDER BY id DESC) AS rn FROM g ORDER BY id;

-- case: {"id": "derived_table_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM (SELECT * FROM g WHERE x <> 0) q WHERE 10 / x > 1;

-- case: {"id": "in_subquery_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE x <> 0 AND id IN (SELECT id FROM h WHERE h.y <> 0 AND 10 / h.y > 1) ORDER BY id;

-- case: {"id": "empty_input_unguarded", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g_empty WHERE 10 / x > 1;

-- case: {"id": "single_row_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE id = 7 AND x <> 0 AND 10 / x > 1 ORDER BY id;

-- case: {"id": "single_row_unguarded", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "division by zero", "difference": "DuckDB returns NULL for a zero divisor, PostgreSQL raises division by zero; Assay keeps its error"}
SELECT id FROM g WHERE id = 7 AND 10 / x > 1 ORDER BY id;

-- case: {"id": "where_null_conjunct_decides_reversed", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE CAST(s AS INTEGER) > 5 AND flag = 'num';

-- case: {"id": "where_null_conjunct_under_or_needs_value", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "invalid input syntax for type integer: \"n/a\"", "difference": "DuckDB's filter selection also drops the NULL rows of an AND nested under OR; PostgreSQL needs that AND's exact value under OR and raises. Assay applies the row-filter rule only to the top-level AND chain, like PostgreSQL, and raises"}
SELECT count(*) AS n FROM g WHERE (flag = 'num' AND CAST(s AS INTEGER) > 5) OR id = 99;

-- case: {"id": "join_on_null_conjunct_decides", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g JOIN h ON g.id = h.id AND g.flag = 'num' AND CAST(g.s AS INTEGER) > 0;

-- case: {"id": "having_null_conjunct_decides", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT s, count(*) AS n FROM g GROUP BY s HAVING (CASE WHEN s IN ('abc', 'x9', 'zz', 'n/a') THEN NULL ELSE TRUE END) AND CAST(s AS INTEGER) > 5 ORDER BY s;

-- case: {"id": "select_null_conjunct_needs_value", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"n/a\"", "difference": "A projected AND needs the exact value: NULL AND (failing operand) is FALSE or NULL. Both references raise; so does Assay"}
SELECT id, flag = 'num' AND CAST(s AS INTEGER) > 5 AS r FROM g WHERE id IN (1, 12) ORDER BY id;

-- case: {"id": "filter_predicate_null_conjunct_needs_value", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"n/a\"", "difference": "A FILTER predicate is not a top-level row filter in either reference: both raise; so does Assay"}
SELECT count(*) FILTER (WHERE flag = 'num' AND CAST(s AS INTEGER) > 5) AS n FROM g;

-- case: {"id": "planner_in_subquery_guards_where_division", "ordered": true, "references": "PostgreSQL raises", "expect": "rows", "difference": "PostgreSQL evaluates 10 / x before its IN semi-join; DuckDB returns NULL for 10/0. In Assay the IN conjunct decides the row"}
SELECT id FROM g WHERE 10 / x > 0 AND id IN (SELECT id FROM h WHERE label = 'ok') ORDER BY id;

-- case: {"id": "planner_in_subquery_guards_where_cast", "ordered": true, "references": "PostgreSQL raises", "expect": "rows", "difference": "PostgreSQL filters the cast before its IN semi-join; DuckDB answers. In Assay the IN conjunct decides the row"}
SELECT id FROM g WHERE CAST(s AS INTEGER) > 5 AND id IN (SELECT id FROM h WHERE label = 'ok') ORDER BY id;

-- case: {"id": "planner_not_in_subquery_guards_where_cast", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT id FROM g WHERE id NOT IN (SELECT id FROM h WHERE label <> 'ok') AND id <> 5 AND CASE WHEN id NOT IN (SELECT id FROM h WHERE label <> 'ok') AND id <> 5 THEN CAST(s AS INTEGER) > 5 END ORDER BY id", "difference": "DuckDB evaluates the cast before its NOT IN anti-join and raises; PostgreSQL answers. Every failing row is rejected by NOT IN or id <> 5, so Assay answers like PostgreSQL. Expected rows from the CASE spelling (oracle)"}
SELECT id FROM g WHERE CAST(s AS INTEGER) > 5 AND id NOT IN (SELECT id FROM h WHERE label <> 'ok') AND id <> 5 ORDER BY id;

-- case: {"id": "planner_in_subquery_value_guarded", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE flag = 'num' AND CAST(s AS INTEGER) IN (SELECT id FROM h) ORDER BY id;

-- case: {"id": "planner_not_in_subquery_value_guarded", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE flag = 'num' AND CAST(s AS INTEGER) NOT IN (SELECT id FROM h WHERE id < 10) ORDER BY id;

-- case: {"id": "planner_in_subquery_value_unguarded", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise on 'abc'; so does Assay"}
SELECT id FROM g WHERE CAST(s AS INTEGER) IN (SELECT id FROM h) ORDER BY id;

-- case: {"id": "planner_join_key_guarded_on", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT g.id AS id, h.label AS label FROM g JOIN h ON CAST(g.s AS INTEGER) = h.id AND g.flag = 'num' ORDER BY id;

-- case: {"id": "planner_join_key_guarded_on_right", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT g.id AS id FROM h JOIN g ON h.id = CAST(g.s AS INTEGER) AND g.flag IS NOT NULL AND g.flag = 'num' ORDER BY id;

-- case: {"id": "planner_join_key_guarded_comma", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT g.id AS id FROM g, h WHERE CAST(g.s AS INTEGER) = h.id AND g.flag = 'num' ORDER BY id;

-- case: {"id": "planner_join_key_guarded_left_join", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT g.id AS id, h.label AS label FROM g LEFT JOIN h ON CASE WHEN g.flag = 'num' THEN CAST(g.s AS INTEGER) END = h.id AND g.flag = 'num' ORDER BY id", "difference": "Both references compute the LEFT JOIN key CAST(g.s ...) for every g row; the ON conjunct g.flag = 'num' rejects every failing row, so Assay computes the key only where it holds. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT g.id AS id, h.label AS label FROM g LEFT JOIN h ON CAST(g.s AS INTEGER) = h.id AND g.flag = 'num' ORDER BY id;

-- case: {"id": "planner_join_key_unguarded", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise on 'abc'; so does Assay"}
SELECT g.id AS id FROM g JOIN h ON CAST(g.s AS INTEGER) = h.id ORDER BY id;

-- case: {"id": "planner_exists_key_guarded", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE EXISTS (SELECT 1 FROM h WHERE h.id = CAST(g.s AS INTEGER) AND g.flag = 'num') ORDER BY id;

-- case: {"id": "planner_not_exists_key_guarded", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE flag = 'num' AND NOT EXISTS (SELECT 1 FROM h WHERE h.id = CAST(g.s AS INTEGER) AND g.flag = 'num') ORDER BY id;

-- case: {"id": "planner_exists_key_unguarded", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise on 'abc'; so does Assay"}
SELECT id FROM g WHERE EXISTS (SELECT 1 FROM h WHERE h.id = CAST(g.s AS INTEGER)) ORDER BY id;

-- case: {"id": "planner_on_residual_guarded_by_where", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND CASE WHEN h.label = 'ok' THEN CAST(g.s AS INTEGER) > 0 END WHERE h.label = 'ok' ORDER BY id", "difference": "Both references evaluate the failing conjunct before (or below) the conjunct that rejects the row (the cast is pushed into the g scan). Inner-join ON conjuncts and WHERE form one conjunction; h.label = 'ok' rejects every failing row. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND CAST(g.s AS INTEGER) > 0 WHERE h.label = 'ok' ORDER BY id;

-- case: {"id": "planner_on_residual_guarded_by_other_on", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT g.id AS id FROM g JOIN h ON g.id = h.id JOIN h AS h2 ON h2.id = h.id AND h2.label = 'ok' WHERE CASE WHEN h2.label = 'ok' THEN CAST(g.s AS INTEGER) > 0 END ORDER BY id", "difference": "Both references evaluate the failing conjunct before (or below) the conjunct that rejects the row (the cast is pushed into the g scan). The other ON's h2.label = 'ok' rejects every failing row. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT g.id AS id FROM g JOIN h ON g.id = h.id AND CAST(g.s AS INTEGER) > 0 JOIN h AS h2 ON h2.id = h.id AND h2.label = 'ok' ORDER BY id;

-- case: {"id": "planner_three_way_guard_on_last_table", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT g.id AS id FROM g, h, h AS h2 WHERE g.id = h.id AND h.id = h2.id AND h2.label = 'ok' AND CASE WHEN h2.label = 'ok' THEN CAST(g.s AS INTEGER) > 5 END ORDER BY id", "difference": "Both references evaluate the failing conjunct before (or below) the conjunct that rejects the row (the cast is pushed into the g scan). h2.label = 'ok' rejects every failing row. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT g.id AS id FROM g, h, h AS h2 WHERE g.id = h.id AND h.id = h2.id AND h2.label = 'ok' AND CAST(g.s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "planner_three_way_unguarded", "ordered": true, "references": "both raise", "expect": "error", "error": "invalid input syntax for type integer: \"abc\"", "difference": "Both raise on 'abc'; so does Assay"}
SELECT g.id AS id FROM g, h, h AS h2 WHERE g.id = h.id AND h.id = h2.id AND CAST(g.s AS INTEGER) > 5 ORDER BY id;

-- case: {"id": "planner_or_with_subquery_guard", "ordered": true, "references": "both raise", "expect": "rows", "oracle": "SELECT id FROM g WHERE CASE WHEN id IN (SELECT id FROM h WHERE label = 'ok') THEN CAST(s AS INTEGER) > 5 ELSE FALSE END OR id = 99 ORDER BY id", "difference": "Both references raise on the cast before the IN subquery inside the OR; in Assay the FALSE IN conjunct decides the AND. Expected rows from the CASE spelling (oracle)", "lenient": "more lenient than DuckDB/PostgreSQL; never returns a value computed from a failed step"}
SELECT id FROM g WHERE (CAST(s AS INTEGER) > 5 AND id IN (SELECT id FROM h WHERE label = 'ok')) OR id = 99 ORDER BY id;

-- case: {"id": "c3_ln_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE f > 0 AND LN(f) > 0;

-- case: {"id": "c3_ln_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE LN(f) > 0 AND f > 0;

-- case: {"id": "c3_sqrt_or_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE f < 0 OR SQRT(f) > 1 ORDER BY id;

-- case: {"id": "c3_log_guard_select", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT id, CASE WHEN f > 0 THEN LOG(f) > 1 WHEN f <= 0 THEN FALSE END AS big FROM g ORDER BY id", "difference": "DuckDB evaluates LOG on every row of a projected AND and raises on LOG(0); PostgreSQL answers. Assay: the FALSE operand decides the row (expected rows from the CASE spelling, oracle)"}
SELECT id, f > 0 AND LOG(f) > 1 AS big FROM g ORDER BY id;

-- case: {"id": "c3_power_overflow_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT id FROM g WHERE f > 0 AND f < 10000000000 AND POWER(f, 3) > 1 ORDER BY id;

-- case: {"id": "c3_power_overflow_unguarded", "ordered": true, "references": "PostgreSQL raises", "expect": "error", "error": "floating arithmetic overflow", "difference": "f < 10000000000 keeps f = -1e308, whose cube overflows: PostgreSQL raises (value out of range: overflow) and so does Assay; DuckDB returns -infinity"}
SELECT id FROM g WHERE f < 10000000000 AND POWER(f, 3) > 1 ORDER BY id;

-- case: {"id": "c3_decimal_mod_zero_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE x <> 0 AND 10 % x > 1;

-- case: {"id": "c3_decimal_mod_zero_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE 10 % x > 1 AND x <> 0;

-- case: {"id": "c3_ln_unguarded", "ordered": true, "references": "both raise", "expect": "error", "error": "cannot take logarithm of zero", "difference": "Both raise on LN(0) (row 2 is the first failing row); so does Assay"}
SELECT count(*) AS n FROM g WHERE LN(f) > 0;

-- case: {"id": "c3_timestamp_cast_guard", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE flag = 'num' AND CAST(ds AS TIMESTAMP) >= TIMESTAMP '2024-01-01 00:00:00';

-- case: {"id": "c3_timestamp_cast_guard_reversed", "ordered": true, "references": "both answer", "expect": "rows"}
SELECT count(*) AS n FROM g WHERE CAST(ds AS TIMESTAMP) >= TIMESTAMP '2024-01-01 00:00:00' AND flag = 'num';

-- case: {"id": "c3_filter_ln_argument_refused", "ordered": true, "references": "DuckDB raises", "expect": "error", "error": "unsupported: aggregate FILTER over an argument that may fail (DuckDB evaluates it on filtered-out rows; PostgreSQL does not)", "difference": "DuckDB evaluates the FILTER argument on every row and raises on LN(0); PostgreSQL answers. A math domain error is one DuckDB raises, so FILTER admission refuses the argument"}
SELECT SUM(LN(f)) FILTER (WHERE f > 0) AS s FROM g;

-- case: {"id": "c3_filter_predicate_guard", "ordered": true, "references": "DuckDB raises", "expect": "rows", "oracle": "SELECT COUNT(*) FILTER (WHERE CASE WHEN f > 0 THEN LN(f) > 0 END) AS n FROM g", "difference": "DuckDB evaluates LN on every row of the FILTER predicate and raises on LN(0); PostgreSQL answers. Assay: the FALSE operand decides the row (expected rows from the CASE spelling, oracle)"}
SELECT COUNT(*) FILTER (WHERE f > 0 AND LN(f) > 0) AS n FROM g;
