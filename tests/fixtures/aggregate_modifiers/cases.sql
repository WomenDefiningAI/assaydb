-- case: {"id": "filter_count_star_ungrouped", "ordered": true, "pg": "agree"}
SELECT COUNT(*) FILTER (WHERE urgent) AS urgent_rows, COUNT(*) FILTER (WHERE NOT urgent) AS other_rows, COUNT(*) AS all_rows FROM sales;

-- case: {"id": "filter_grouped_several_conditions", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) FILTER (WHERE urgent) AS urgent_rows, COUNT(*) FILTER (WHERE NOT urgent) AS other_rows, COUNT(*) FILTER (WHERE urgent IS NULL) AS unknown_rows, COUNT(*) AS all_rows FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_sum_avg_min_max", "ordered": true, "pg": "agree (PostgreSQL AVG of BIGINT returns NUMERIC; values agree)"}
SELECT region, SUM(qty) FILTER (WHERE urgent) AS urgent_qty, AVG(qty) FILTER (WHERE qty > 2) AS avg_big_qty, MIN(price) FILTER (WHERE urgent) AS min_urgent_price, MAX(price) FILTER (WHERE NOT urgent) AS max_other_price FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_all_rows_filtered_ungrouped", "ordered": true, "pg": "agree"}
SELECT COUNT(*) FILTER (WHERE qty > 100) AS n, COUNT(qty) FILTER (WHERE qty > 100) AS nq, SUM(qty) FILTER (WHERE qty > 100) AS s, AVG(price) FILTER (WHERE qty > 100) AS a, MIN(city) FILTER (WHERE qty > 100) AS lo, MAX(d) FILTER (WHERE qty > 100) AS hi FROM sales;

-- case: {"id": "filter_group_with_no_matching_rows", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) FILTER (WHERE urgent) AS urgent_rows, SUM(price) FILTER (WHERE urgent) AS urgent_price, COUNT(*) AS all_rows FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_null_condition_is_skipped", "ordered": true, "pg": "agree"}
SELECT COUNT(*) FILTER (WHERE urgent) AS t, COUNT(*) FILTER (WHERE NOT urgent) AS f, COUNT(*) FILTER (WHERE urgent OR urgent IS NULL) AS t_or_unknown, COUNT(*) FILTER (WHERE NULL) AS never FROM sales;

-- case: {"id": "filter_count_column", "ordered": true, "pg": "agree"}
SELECT region, COUNT(qty) FILTER (WHERE urgent) AS urgent_qty_rows, COUNT(city) FILTER (WHERE price > 5) AS priced_city_rows FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_in_having", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING COUNT(*) FILTER (WHERE urgent) >= 2 ORDER BY region;

-- case: {"id": "filter_order_by_alias", "ordered": true, "pg": "agree"}
SELECT region, SUM(qty) FILTER (WHERE urgent) AS uq FROM sales WHERE region IS NOT NULL GROUP BY region ORDER BY uq DESC NULLS LAST, region;

-- case: {"id": "filter_order_by_expression", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) AS n FROM sales WHERE region IS NOT NULL GROUP BY region ORDER BY COUNT(*) FILTER (WHERE urgent) DESC, region;

-- case: {"id": "filter_count_distinct", "ordered": true, "pg": "agree"}
SELECT region, COUNT(DISTINCT city) FILTER (WHERE urgent) AS urgent_cities, COUNT(DISTINCT city) AS cities FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_date_min_max", "ordered": true, "pg": "agree"}
SELECT MAX(d) FILTER (WHERE urgent) AS last_urgent, MIN(d) FILTER (WHERE NOT urgent) AS first_other FROM sales;

-- case: {"id": "filter_text_min_max", "ordered": true, "pg": "agree with COLLATE \"C\" text columns"}
SELECT region, MIN(city) FILTER (WHERE qty >= 3) AS first_city, MAX(note) FILTER (WHERE urgent) AS last_note FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_empty_table_ungrouped", "ordered": true, "pg": "agree"}
SELECT COUNT(*) FILTER (WHERE urgent) AS n, SUM(qty) FILTER (WHERE urgent) AS s, AVG(qty) FILTER (WHERE urgent) AS a FROM empty_sales;

-- case: {"id": "filter_empty_table_grouped", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) FILTER (WHERE urgent) AS n FROM empty_sales GROUP BY region ORDER BY region;

-- case: {"id": "filter_decimal_arithmetic_argument", "ordered": true, "pg": "agree"}
SELECT region, SUM(price * qty) FILTER (WHERE d >= DATE '2024-02-01') AS late_value, SUM(price * qty) AS all_value FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_float_sum", "ordered": true, "pg": "agree"}
SELECT region, SUM(disc) FILTER (WHERE urgent) AS urgent_disc, COUNT(disc) FILTER (WHERE urgent) AS urgent_disc_rows FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_float_distinct", "ordered": true, "pg": "agree"}
SELECT region, SUM(DISTINCT disc) FILTER (WHERE NOT urgent) AS s, AVG(DISTINCT disc) FILTER (WHERE NOT urgent) AS a FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_in_derived_table", "ordered": true, "pg": "agree"}
SELECT SUM(u) AS urgent_rows, MAX(u) AS busiest FROM (SELECT region, COUNT(*) FILTER (WHERE urgent) AS u FROM sales GROUP BY region) t;

-- case: {"id": "filter_in_scalar_subquery", "ordered": true, "pg": "agree"}
SELECT (SELECT COUNT(*) FILTER (WHERE urgent) FROM sales) AS urgent_rows, (SELECT SUM(qty) FILTER (WHERE region = 'EU') FROM sales) AS eu_qty;

-- case: {"id": "filter_like_condition", "ordered": true, "pg": "agree"}
SELECT COUNT(*) FILTER (WHERE city LIKE 'S%') AS s_cities, COUNT(*) FILTER (WHERE city LIKE '%ö%') AS umlaut_cities FROM sales;

-- case: {"id": "filter_same_aggregate_different_filters", "ordered": true, "pg": "agree"}
SELECT region, SUM(qty) AS all_qty, SUM(qty) FILTER (WHERE urgent) AS urgent_qty, SUM(qty) FILTER (WHERE NOT urgent) AS other_qty FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "filter_fallible_argument_refused", "ordered": true, "pg": "disagree: PostgreSQL evaluates the argument only for rows passing FILTER (returns 8.51 here); DuckDB evaluates it on every row and raises a conversion error", "assay_error": "unsupported: aggregate FILTER over an argument that may fail", "difference": "DuckDB evaluates the argument on filtered-out rows and errors, PostgreSQL does not; Assay refuses arguments it cannot prove total"}
SELECT SUM(CAST(price AS DECIMAL(3,2))) FILTER (WHERE price < 5) AS s FROM sales;

-- case: {"id": "filter_window_refused", "ordered": true, "pg": "agree (both engines support FILTER on window aggregates)"}
SELECT id, SUM(qty) FILTER (WHERE urgent) OVER () AS s FROM sales ORDER BY id;

-- case: {"id": "distinct_float_grouped", "ordered": true, "pg": "agree (PostgreSQL also equates -0 with 0 and all NaNs)"}
SELECT region, SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a, COUNT(DISTINCT disc) AS n, SUM(disc) AS plain FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "distinct_float_ungrouped_where", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a, COUNT(DISTINCT disc) AS n FROM sales WHERE region = 'EU';

-- case: {"id": "distinct_float_expression", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc * 2) AS s, AVG(DISTINCT -disc) AS a FROM sales WHERE region = 'EU';

-- case: {"id": "distinct_float_signed_zero", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a, COUNT(DISTINCT disc) AS n FROM sales WHERE id IN (4, 5);

-- case: {"id": "distinct_float_nan_payloads", "ordered": true, "pg": "agree (NaN only; PostgreSQL has a single NaN)"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a, COUNT(DISTINCT disc) AS n FROM sales WHERE id IN (6, 17);

-- case: {"id": "distinct_float_all_null", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a, COUNT(DISTINCT disc) AS n FROM sales WHERE id IN (11, 13);

-- case: {"id": "distinct_float_empty_table", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a FROM empty_sales;

-- case: {"id": "distinct_float_empty_table_grouped", "ordered": true, "pg": "agree"}
SELECT region, SUM(DISTINCT disc) AS s FROM empty_sales GROUP BY region;

-- case: {"id": "distinct_float_infinity", "ordered": true, "pg": "agree"}
SELECT SUM(DISTINCT disc) AS s, AVG(DISTINCT disc) AS a FROM sales WHERE region = 'APAC';

-- case: {"id": "distinct_float_avoids_duplicate_overflow", "ordered": true, "pg": "SUM and COUNT agree; PostgreSQL AVG(float8) raises overflow because its transition also accumulates squares (pre-existing approved Float AVG = SUM / COUNT)"}
SELECT SUM(DISTINCT x) AS s, AVG(DISTINCT x) AS a, COUNT(DISTINCT x) AS n FROM big WHERE g = 1;

-- case: {"id": "distinct_float_overflow_refused", "ordered": true, "pg": "disagree: PostgreSQL raises float overflow; DuckDB returns infinity", "difference": "Integration candidate 2: overflowing Float SUM returns +/-infinity like DuckDB (decision 2026-10-17); the id keeps its original name"}
SELECT SUM(DISTINCT x) AS s FROM big WHERE g = 2;

-- case: {"id": "distinct_float_cast_integers", "ordered": true, "pg": "agree"}
SELECT region, SUM(DISTINCT CAST(qty AS DOUBLE PRECISION)) AS s, AVG(DISTINCT CAST(qty AS DOUBLE PRECISION)) AS a FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "distinct_float_having", "ordered": true, "pg": "agree"}
SELECT region, SUM(DISTINCT disc) AS s FROM sales WHERE region IN ('EU', 'MEA') OR region IS NULL GROUP BY region HAVING SUM(DISTINCT disc) >= 0.5 ORDER BY region NULLS LAST;

-- case: {"id": "distinct_float_alongside_exact_distinct", "ordered": true, "pg": "agree (PostgreSQL AVG of NUMERIC returns NUMERIC; values agree)"}
SELECT region, SUM(DISTINCT disc) AS sd, SUM(DISTINCT qty) AS sq, AVG(DISTINCT price) AS ap, COUNT(*) AS n FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_cities_per_region", "ordered": true, "pg": "agree with COLLATE \"C\" (default en_US collation orders Ñ before S)"}
SELECT region, STRING_AGG(city, ', ' ORDER BY city) AS cities FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_order_by_id_desc_nulls_last", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(note, ',' ORDER BY id DESC NULLS LAST) AS notes FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_null_keys_sort_last", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(city, '|' ORDER BY qty) AS by_qty FROM sales WHERE region IN ('EU', 'APAC') GROUP BY region ORDER BY region;

-- case: {"id": "string_agg_desc_nulls_first", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(city, '|' ORDER BY qty DESC NULLS FIRST) AS by_qty FROM sales WHERE region IN ('EU', 'APAC') GROUP BY region ORDER BY region;

-- case: {"id": "string_agg_empty_separator", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(note, '' ORDER BY id) AS joined FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_non_ascii_separator", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(city, ' → ' ORDER BY id) AS route FROM sales WHERE region = 'APAC';

-- case: {"id": "string_agg_empty_table", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(city, ',' ORDER BY id) AS cities, COUNT(*) AS n FROM empty_sales;

-- case: {"id": "string_agg_empty_table_grouped", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(city, ',' ORDER BY id) AS cities FROM empty_sales GROUP BY region;

-- case: {"id": "string_agg_all_rows_filtered", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(city, ',' ORDER BY id) AS cities FROM sales WHERE qty > 100;

-- case: {"id": "string_agg_keeps_duplicates", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(city, ',' ORDER BY city) AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_distinct_ordered_by_argument", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(DISTINCT city, ',' ORDER BY city) AS cities FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_filter", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(city, ';' ORDER BY id) FILTER (WHERE urgent) AS urgent_cities FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_two_order_keys", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(CAST(id AS TEXT), ',' ORDER BY region NULLS FIRST, qty DESC NULLS LAST, id) AS ids FROM sales;

-- case: {"id": "string_agg_order_by_date", "ordered": true, "pg": "agree"}
SELECT region, STRING_AGG(city, ',' ORDER BY d DESC NULLS LAST) AS recent_first FROM sales WHERE city IS NOT NULL GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_order_by_float_nan_last", "ordered": true, "pg": "agree (NaN sorts above infinity in both)"}
SELECT STRING_AGG(CAST(id AS TEXT), ',' ORDER BY disc) AS ids FROM sales WHERE id IN (5, 6, 7, 9, 18);

-- case: {"id": "string_agg_in_having", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING STRING_AGG(city, ',' ORDER BY city) LIKE '%Köln%' ORDER BY region;

-- case: {"id": "string_agg_with_other_aggregates", "ordered": true, "pg": "agree"}
SELECT COUNT(*) AS n, STRING_AGG(region, '/' ORDER BY id) AS regions, SUM(qty) AS q, MAX(note) AS last_note FROM sales WHERE id <= 5;

-- case: {"id": "string_agg_empty_string_values", "ordered": true, "pg": "agree"}
SELECT STRING_AGG(note, ',' ORDER BY id) AS notes FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_unordered_refused", "ordered": true, "pg": "agree in SQL: order without ORDER BY is unspecified in both engines", "assay_error": "unsupported: STRING_AGG without ORDER BY", "difference": "Unordered concatenation order is unspecified; Assay requires an explicit ORDER BY"}
SELECT region, STRING_AGG(city, ',') AS cities FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "string_agg_one_argument_refused", "ordered": true, "pg": "disagree: PostgreSQL has no one-argument STRING_AGG; DuckDB defaults the separator to ','", "assay_error": "unsupported: STRING_AGG requires an explicit separator", "difference": "Reference engines disagree on the one-argument form"}
SELECT STRING_AGG(city ORDER BY city) AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_null_separator_refused", "ordered": true, "pg": "disagree: PostgreSQL treats a NULL delimiter as empty; DuckDB returns NULL", "assay_error": "unsupported: STRING_AGG separator must be a non-NULL string literal", "difference": "Reference engines disagree on a NULL separator"}
SELECT STRING_AGG(city, NULL ORDER BY city) AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_column_separator_refused", "ordered": true, "pg": "disagree: PostgreSQL accepts a per-row delimiter; DuckDB rejects a non-constant separator", "assay_error": "unsupported: STRING_AGG separator must be a non-NULL string literal", "difference": "Reference engines disagree on non-constant separators"}
SELECT STRING_AGG(city, note ORDER BY city) AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_integer_input_refused", "ordered": true, "pg": "disagree: PostgreSQL has no STRING_AGG(bigint, text); DuckDB casts to VARCHAR", "assay_error": "unsupported: STRING_AGG requires TEXT input", "difference": "Reference engines disagree on implicit casting of non-text input"}
SELECT STRING_AGG(qty, ',' ORDER BY id) AS q FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_distinct_other_order_refused", "ordered": true, "pg": "disagree: PostgreSQL requires DISTINCT ORDER BY expressions to appear in the argument list", "assay_error": "unsupported: STRING_AGG DISTINCT requires ORDER BY the aggregated argument", "difference": "PostgreSQL rejects this form; DuckDB accepts it"}
SELECT STRING_AGG(DISTINCT city, ',' ORDER BY id) AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_desc_default_nulls_refused", "ordered": true, "pg": "disagree: PostgreSQL DESC defaults to NULLS FIRST; DuckDB to NULLS LAST", "assay_error": "unsupported: STRING_AGG ORDER BY DESC requires explicit NULLS FIRST or NULLS LAST", "difference": "Reference engines place NULL keys differently"}
SELECT region, STRING_AGG(city, '|' ORDER BY qty DESC) AS by_qty FROM sales WHERE region = 'EU' GROUP BY region;

-- case: {"id": "string_agg_tied_keys_refused", "ordered": true, "pg": "agree in SQL: peers have no defined order in either engine", "assay_error": "unsupported: STRING_AGG ORDER BY ties between different values", "difference": "The order among tied keys is unspecified; Assay refuses instead of choosing one"}
SELECT STRING_AGG(city, ',' ORDER BY region) AS cities FROM sales;

-- case: {"id": "string_agg_signed_zero_tie_refused", "ordered": true, "pg": "agree in SQL: -0 and 0 are equal sort keys", "assay_error": "unsupported: STRING_AGG ORDER BY ties between different values", "difference": "-0.0 and 0.0 are peers; their order is unspecified"}
SELECT STRING_AGG(CAST(id AS TEXT), ',' ORDER BY disc) AS ids FROM sales WHERE id IN (4, 5);

-- case: {"id": "listagg_refused", "ordered": true, "pg": "disagree: PostgreSQL has no LISTAGG", "assay_error": "unsupported: LISTAGG", "difference": "Only DuckDB provides LISTAGG; use STRING_AGG(value, separator ORDER BY ...)"}
SELECT LISTAGG(city, ',') AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "group_concat_refused", "ordered": true, "pg": "disagree: PostgreSQL has no GROUP_CONCAT", "assay_error": "unsupported: GROUP_CONCAT", "difference": "Only DuckDB provides GROUP_CONCAT; use STRING_AGG(value, separator ORDER BY ...)"}
SELECT GROUP_CONCAT(city, ',') AS cities FROM sales WHERE region = 'EU';

-- case: {"id": "string_agg_window_refused", "ordered": true, "pg": "disagree: PostgreSQL rejects aggregate ORDER BY in window functions; DuckDB accepts it", "assay_error": "unsupported:", "difference": "Window STRING_AGG is outside the supported window subset"}
SELECT id, STRING_AGG(city, ',' ORDER BY id) OVER () AS s FROM sales ORDER BY id;

-- case: {"id": "rollup_region_city_grouping", "ordered": true, "pg": "agree"}
SELECT region, city, SUM(qty) AS q, COUNT(*) AS n, GROUPING(region) AS gr, GROUPING(city) AS gc, GROUPING(region, city) AS g FROM sales GROUP BY ROLLUP(region, city) ORDER BY gr, region NULLS LAST, gc, city NULLS LAST;

-- case: {"id": "rollup_order_by_grouping_expression", "ordered": true, "pg": "agree"}
SELECT region, SUM(price) AS revenue FROM sales GROUP BY ROLLUP(region) ORDER BY GROUPING(region), region NULLS FIRST;

-- case: {"id": "cube_region_urgent", "ordered": true, "pg": "agree"}
SELECT region, urgent, COUNT(*) AS n, GROUPING(region, urgent) AS g FROM sales GROUP BY CUBE(region, urgent) ORDER BY g, region NULLS LAST, urgent NULLS LAST;

-- case: {"id": "grouping_sets_explicit", "ordered": true, "pg": "agree"}
SELECT region, urgent, SUM(qty) AS q, GROUPING(region) AS gr, GROUPING(urgent) AS gu FROM sales GROUP BY GROUPING SETS ((region), (urgent), ()) ORDER BY gr, gu, region NULLS LAST, urgent NULLS LAST;

-- case: {"id": "rollup_having_grouping", "ordered": true, "pg": "agree"}
SELECT region, city, COUNT(*) AS n FROM sales GROUP BY ROLLUP(region, city) HAVING GROUPING(city) = 1 AND COUNT(*) > 2 ORDER BY GROUPING(region), region NULLS LAST;

-- case: {"id": "rollup_empty_table_grand_total", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) AS n, SUM(qty) AS q, GROUPING(region) AS g FROM empty_sales GROUP BY ROLLUP(region);

-- case: {"id": "rollup_all_rows_filtered", "ordered": true, "pg": "agree"}
SELECT region, city, COUNT(*) AS n, MAX(price) AS p FROM sales WHERE qty > 100 GROUP BY ROLLUP(region, city);

-- case: {"id": "rollup_plain_key_plus_rollup", "ordered": true, "pg": "agree"}
SELECT region, urgent, COUNT(*) AS n, GROUPING(urgent) AS gu FROM sales WHERE region IS NOT NULL GROUP BY region, ROLLUP(urgent) ORDER BY region, gu, urgent NULLS LAST;

-- case: {"id": "rollup_duplicate_grouping_sets", "ordered": true, "pg": "agree (both keep duplicate grouping sets)"}
SELECT region, COUNT(*) AS n, GROUPING(region) AS g FROM sales GROUP BY region, ROLLUP(region) ORDER BY region NULLS LAST, n;

-- case: {"id": "rollup_with_filter_aggregates", "ordered": true, "pg": "agree"}
SELECT region, COUNT(*) FILTER (WHERE urgent) AS urgent_rows, COUNT(*) AS n, GROUPING(region) AS g FROM sales GROUP BY ROLLUP(region) ORDER BY g, region NULLS LAST;

-- case: {"id": "rollup_with_string_agg", "ordered": true, "pg": "agree with COLLATE \"C\""}
SELECT region, STRING_AGG(DISTINCT city, ',' ORDER BY city) AS cities, GROUPING(region) AS g FROM sales GROUP BY ROLLUP(region) ORDER BY g, region NULLS LAST;

-- case: {"id": "rollup_with_distinct_float", "ordered": true, "pg": "agree"}
SELECT region, SUM(DISTINCT disc) AS s, COUNT(DISTINCT disc) AS n, GROUPING(region) AS g FROM sales WHERE region IN ('EU', 'MEA') GROUP BY ROLLUP(region) ORDER BY g, region;

-- case: {"id": "rollup_expression_key", "ordered": true, "pg": "agree"}
SELECT EXTRACT(MONTH FROM d) AS m, region, SUM(qty) AS q FROM sales GROUP BY ROLLUP(EXTRACT(MONTH FROM d), region) ORDER BY GROUPING(EXTRACT(MONTH FROM d)), m NULLS LAST, GROUPING(region), region NULLS LAST;

-- case: {"id": "grouping_plain_group_by", "ordered": true, "pg": "agree"}
SELECT region, GROUPING(region) AS g, COUNT(*) AS n FROM sales GROUP BY region ORDER BY region NULLS LAST;

-- case: {"id": "rollup_composite_element", "ordered": true, "pg": "agree"}
SELECT region, urgent, COUNT(*) AS n, GROUPING(region, urgent) AS g FROM sales GROUP BY ROLLUP((region, urgent)) ORDER BY g, region NULLS LAST, urgent NULLS LAST;

-- case: {"id": "grouping_sets_empty_set_only", "ordered": true, "pg": "agree"}
SELECT COUNT(*) AS n, SUM(qty) AS q FROM sales GROUP BY GROUPING SETS (());

-- case: {"id": "cube_three_keys_counts", "ordered": true, "pg": "agree"}
SELECT gg AS g, COUNT(*) AS rows_in_set, SUM(n) AS total FROM (SELECT region, urgent, note, COUNT(*) AS n, GROUPING(region, urgent, note) AS gg FROM sales GROUP BY CUBE(region, urgent, note)) t GROUP BY gg ORDER BY g;

-- case: {"id": "rollup_with_rollup_modifier_refused", "ordered": true, "pg": "agree: both reject MySQL GROUP BY ... WITH ROLLUP", "assay_error": "unsupported:", "difference": "MySQL WITH ROLLUP spelling is not supported by either reference engine"}
SELECT region, COUNT(*) AS n FROM sales GROUP BY region WITH ROLLUP;

-- case: {"id": "grouping_id_refused", "ordered": true, "pg": "disagree: PostgreSQL has no GROUPING_ID", "assay_error": "unsupported: GROUPING_ID", "difference": "Only DuckDB provides GROUPING_ID; use GROUPING(...)"}
SELECT region, GROUPING_ID(region) AS g FROM sales GROUP BY ROLLUP(region) ORDER BY g, region NULLS LAST;

-- case: {"id": "grouping_non_key_error", "ordered": true, "pg": "agree: both reject GROUPING of a non-grouping expression", "assay_error": "GROUPING arguments must be grouping expressions", "difference": "Both engines reject this query"}
SELECT region, GROUPING(city) AS g FROM sales GROUP BY ROLLUP(region);

-- case: {"id": "rollup_window_refused", "ordered": true, "pg": "agree (both support windows over grouping sets)", "assay_error": "unsupported: window functions with ROLLUP/CUBE/GROUPING SETS", "difference": "Windows over grouping-set results are not yet supported"}
SELECT region, SUM(qty) AS q, RANK() OVER (ORDER BY SUM(qty) DESC NULLS LAST) AS r FROM sales GROUP BY ROLLUP(region);
