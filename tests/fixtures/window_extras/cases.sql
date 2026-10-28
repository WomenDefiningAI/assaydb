-- case: {"id": "rank_dist_order", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (ORDER BY k) AS pr, CUME_DIST() OVER (ORDER BY k) AS cd FROM events ORDER BY id;

-- case: {"id": "rank_dist_partition_desc", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (PARTITION BY g ORDER BY d DESC NULLS LAST) AS pr, CUME_DIST() OVER (PARTITION BY g ORDER BY d DESC NULLS LAST) AS cd FROM events ORDER BY id;

-- case: {"id": "rank_dist_desc_default_nulls", "ordered": true, "pg": "differs: PostgreSQL places NULLs first for DESC; Assay follows DuckDB (NULLS LAST, decision 2026-10-16)"}
SELECT id, PERCENT_RANK() OVER (PARTITION BY g ORDER BY k DESC) AS pr, CUME_DIST() OVER (PARTITION BY g ORDER BY k DESC) AS cd FROM events ORDER BY id;

-- case: {"id": "rank_dist_no_order", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (PARTITION BY g) AS pr, CUME_DIST() OVER (PARTITION BY g) AS cd, CUME_DIST() OVER () AS whole FROM events ORDER BY id;

-- case: {"id": "rank_dist_float_peers", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (ORDER BY f) AS pr, CUME_DIST() OVER (ORDER BY f NULLS FIRST) AS cd FROM events ORDER BY id;

-- case: {"id": "rank_dist_frame_ignored", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (PARTITION BY g ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS pr, CUME_DIST() OVER (PARTITION BY g ORDER BY x GROUPS 1 PRECEDING EXCLUDE CURRENT ROW) AS cd FROM events ORDER BY id;

-- case: {"id": "rank_dist_single_and_multi_key", "ordered": true, "pg": "agree"}
SELECT id, PERCENT_RANK() OVER (ORDER BY g NULLS FIRST, k NULLS FIRST) AS pr, CUME_DIST() OVER (PARTITION BY id) AS single FROM events ORDER BY id;

-- case: {"id": "rank_dist_empty", "ordered": true, "pg": "agree"}
SELECT PERCENT_RANK() OVER (ORDER BY k) AS pr, CUME_DIST() OVER () AS cd FROM empty_events;

-- case: {"id": "rank_dist_qualify", "ordered": true, "pg": "rejects"}
SELECT id, y FROM events QUALIFY CUME_DIST() OVER (ORDER BY y) <= 0.25 ORDER BY id;

-- case: {"id": "rank_dist_grouped", "ordered": true, "pg": "agree"}
SELECT g, SUM(x) AS total, PERCENT_RANK() OVER (ORDER BY SUM(x) NULLS FIRST) AS pr, CUME_DIST() OVER (ORDER BY SUM(x) NULLS FIRST) AS cd FROM events GROUP BY g ORDER BY g NULLS LAST;

-- case: {"id": "ignore_nulls_whole_partition", "ordered": true, "pg": "rejects"}
SELECT id, FIRST_VALUE(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv, LAST_VALUE(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_carry_forward", "ordered": true, "pg": "rejects"}
SELECT id, LAST_VALUE(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY id) AS carried, LAST_VALUE(s IGNORE NULLS) OVER (ORDER BY id) AS text_carried, LAST_VALUE(f IGNORE NULLS) OVER (ORDER BY id) AS real_carried FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_sliding", "ordered": true, "pg": "rejects"}
SELECT id, FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS fv, LAST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS lv, FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS ahead FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_nth", "ordered": true, "pg": "rejects"}
SELECT id, NTH_VALUE(x, 2 IGNORE NULLS) OVER (PARTITION BY g ORDER BY id) AS second, NTH_VALUE(x, 3 IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS third, NTH_VALUE(d, 1 IGNORE NULLS) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_d FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_lag_lead", "ordered": true, "pg": "rejects"}
SELECT id, LAG(x IGNORE NULLS) OVER (ORDER BY id) AS lag1, LEAD(x IGNORE NULLS) OVER (ORDER BY id) AS lead1, LAG(x, 2, -1 IGNORE NULLS) OVER (ORDER BY id) AS lag2, LEAD(x, 3, -1 IGNORE NULLS) OVER (ORDER BY id) AS lead3, LAG(x, 0 IGNORE NULLS) OVER (ORDER BY id) AS lag0 FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_lag_partitioned", "ordered": true, "pg": "rejects"}
SELECT id, LAG(d IGNORE NULLS) OVER (PARTITION BY g ORDER BY id) AS prev_d, LEAD(s, 1, 'none' IGNORE NULLS) OVER (PARTITION BY g ORDER BY id) AS next_s, LAG(f IGNORE NULLS) OVER (PARTITION BY g ORDER BY k NULLS FIRST, id) AS prev_f FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_exclude", "ordered": true, "pg": "rejects"}
SELECT id, FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS neighbour, LAST_VALUE(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS other_key FROM events WHERE id IN (1, 2, 3, 4, 6, 7, 9, 10, 11, 12) ORDER BY id;

-- case: {"id": "respect_nulls_default", "ordered": true, "pg": "rejects"}
SELECT id, FIRST_VALUE(x RESPECT NULLS) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS fv, LAG(x, 1, 0 RESPECT NULLS) OVER (ORDER BY id) AS lag1 FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_standard_placement", "ordered": true, "pg": "agree", "assay_error": "unsupported:"}
SELECT id, LAG(x) IGNORE NULLS OVER (ORDER BY id) AS lag1 FROM events ORDER BY id;

-- case: {"id": "ignore_nulls_aggregate", "ordered": true, "pg": "agree", "assay_error": "unsupported:"}
SELECT id, SUM(x IGNORE NULLS) OVER (ORDER BY id) AS s FROM events ORDER BY id;

-- case: {"id": "nth_value_from_last", "ordered": true, "pg": "agree", "assay_error": "sql parser error"}
SELECT id, NTH_VALUE(x, 2) FROM LAST OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v FROM events ORDER BY id;

-- case: {"id": "named_shared", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER w AS s, COUNT(*) OVER w AS n, MAX(d) OVER w AS m, ROW_NUMBER() OVER w AS rn, LAG(id) OVER w AS prev FROM events WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id;

-- case: {"id": "named_refined_frame", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s3, AVG(y) OVER (w ROWS 2 PRECEDING) AS a3, FIRST_VALUE(id) OVER (w RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS fv FROM events WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id;

-- case: {"id": "named_refined_order_chain", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (p ORDER BY id) AS running, SUM(x) OVER q AS q_running, COUNT(x) OVER p AS per_group FROM events WINDOW p AS (PARTITION BY g), q AS (p ORDER BY id DESC) ORDER BY id;

-- case: {"id": "named_copy_with_frame", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER w AS s, MIN(x) OVER w AS m FROM events WINDOW w AS (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ORDER BY id;

-- case: {"id": "named_case_insensitive_unused", "ordered": true, "pg": "agree"}
SELECT id, RANK() OVER W AS r FROM events WINDOW w AS (ORDER BY k), unused AS (PARTITION BY g) ORDER BY id;

-- case: {"id": "named_order_by_and_grouped", "ordered": true, "pg": "agree"}
SELECT g, SUM(x) AS total, SUM(SUM(x)) OVER w AS running FROM events GROUP BY g WINDOW w AS (ORDER BY g NULLS LAST) ORDER BY SUM(SUM(x)) OVER w NULLS FIRST, g NULLS LAST;

-- case: {"id": "named_qualify", "ordered": true, "pg": "rejects"}
SELECT id, x FROM events WINDOW w AS (PARTITION BY g ORDER BY x DESC NULLS LAST, id) QUALIFY ROW_NUMBER() OVER w = 1 ORDER BY id;

-- case: {"id": "named_copy_frame_error", "ordered": true, "pg": "agree", "assay_error": "cannot copy window"}
SELECT id, SUM(x) OVER (w ROWS 1 PRECEDING) AS s FROM events WINDOW w AS (ORDER BY id ROWS 2 PRECEDING) ORDER BY id;

-- case: {"id": "named_override_order_error", "ordered": true, "pg": "agree", "assay_error": "cannot override ORDER BY"}
SELECT id, SUM(x) OVER (w ORDER BY k) AS s FROM events WINDOW w AS (ORDER BY id) ORDER BY id;

-- case: {"id": "named_duplicate_error", "ordered": true, "pg": "agree", "assay_error": "window \"w\" is already defined"}
SELECT id, SUM(x) OVER w AS s FROM events WINDOW w AS (ORDER BY id), w AS (ORDER BY k) ORDER BY id;

-- case: {"id": "named_missing_error", "ordered": true, "pg": "agree", "assay_error": "unsupported: named window"}
SELECT id, SUM(x) OVER v AS s FROM events WINDOW w AS (ORDER BY id) ORDER BY id;

-- case: {"id": "named_partition_refinement", "ordered": true, "pg": "differs: PostgreSQL rejects PARTITION BY in a window that references another window; DuckDB accepts it", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL: cannot override PARTITION BY clause)"}
SELECT id, SUM(x) OVER (w PARTITION BY g) AS s FROM events WINDOW w AS (ORDER BY id) ORDER BY id;

-- case: {"id": "named_forward_reference", "ordered": true, "pg": "differs: PostgreSQL rejects a window defined later in the WINDOW clause; DuckDB accepts it", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL: window does not exist)"}
SELECT id, SUM(x) OVER q AS s FROM events WINDOW q AS (p ORDER BY id), p AS (PARTITION BY g) ORDER BY id;

-- case: {"id": "named_unused_invalid", "ordered": true, "pg": "differs: PostgreSQL rejects an invalid unused window; DuckDB ignores it", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL binds unused window definitions)"}
SELECT id FROM events WINDOW w AS (PARTITION BY missing_column) ORDER BY id;

-- case: {"id": "groups_offsets", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS back1, COUNT(*) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS fwd1, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS wide FROM events ORDER BY id;

-- case: {"id": "groups_following_preceding", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS ahead, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS behind, COUNT(*) OVER (ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before_count, COUNT(x) OVER (ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS after_count FROM events ORDER BY id;

-- case: {"id": "groups_desc_nulls_first", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (PARTITION BY g ORDER BY d DESC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s, MIN(y) OVER (PARTITION BY g ORDER BY d DESC NULLS FIRST GROUPS 1 PRECEDING) AS m FROM events ORDER BY id;

-- case: {"id": "groups_float_and_values", "ordered": true, "pg": "agree"}
SELECT id, COUNT(*) OVER (ORDER BY f GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n, FIRST_VALUE(f) OVER (ORDER BY f GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS low_key, LAST_VALUE(k) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_key FROM events ORDER BY id;

-- case: {"id": "groups_without_order", "ordered": true, "pg": "differs: PostgreSQL requires ORDER BY for GROUPS; DuckDB treats the partition as one peer group", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL: GROUPS mode requires an ORDER BY clause)"}
SELECT id, SUM(x) OVER (PARTITION BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM events ORDER BY id;

-- case: {"id": "exclude_current_row", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) AS s, AVG(x) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS others_avg FROM events ORDER BY id;

-- case: {"id": "exclude_without_frame", "ordered": true, "pg": "agree", "assay_error": "sql parser error"}
SELECT id, COUNT(*) OVER (PARTITION BY g EXCLUDE CURRENT ROW) AS others FROM events ORDER BY id;

-- case: {"id": "exclude_group_ties_range", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS other_keys, SUM(x) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE TIES) AS near_ties, COUNT(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE GROUP) AS none FROM events ORDER BY id;

-- case: {"id": "exclude_groups_frames", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) AS ties, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) AS grp, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) AS all_rows FROM events ORDER BY id;

-- case: {"id": "exclude_min_max_float", "ordered": true, "pg": "agree"}
SELECT id, MIN(f) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) AS mn, MAX(f) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) AS mx, MAX(s) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS other_text FROM events ORDER BY id;

-- case: {"id": "exclude_float_sum_avg", "ordered": true, "pg": "agree"}
SELECT id, SUM(y) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING EXCLUDE CURRENT ROW) AS s, AVG(y) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) AS a FROM events ORDER BY id;

-- case: {"id": "exclude_value_functions", "ordered": true, "pg": "agree"}
SELECT id, FIRST_VALUE(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS fv, LAST_VALUE(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS lv, NTH_VALUE(k, 2) OVER (ORDER BY k NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS second_other, FIRST_VALUE(k) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) AS first_key FROM events ORDER BY id;

-- case: {"id": "exclude_decimal_int_sum", "ordered": true, "pg": "agree"}
SELECT id, SUM(d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS before_d, SUM(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING EXCLUDE CURRENT ROW) AS next_two FROM events ORDER BY id;

-- case: {"id": "following_start_preceding_end_rows", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev2, COUNT(x) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS next3, AVG(d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS rest_avg, SUM(x) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before FROM events ORDER BY id;

-- case: {"id": "inverted_frame_count", "ordered": true, "pg": "differs: PostgreSQL counts 0 rows in a frame whose start follows its end; DuckDB 1.5.6 returns negative counts", "assay_error": "unsupported: frame whose start follows its end", "difference": "References disagree (DuckDB COUNT(*) returns -1 for ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING)"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS c FROM events ORDER BY id;

-- case: {"id": "inverted_frame_other_aggregates", "ordered": true, "pg": "differs: for NULL keys PostgreSQL uses the NULL peer group where DuckDB's inverted RANGE frame is empty", "assay_error": "unsupported: frame whose start follows its end", "difference": "References disagree on RANGE frames of NULL keys (FIRST_VALUE: PostgreSQL 12, DuckDB NULL)"}
SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS s, MAX(x) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING AND 1 FOLLOWING) AS m, FIRST_VALUE(x) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 3 PRECEDING) AS fv, AVG(y) OVER (ORDER BY k GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) AS a FROM events ORDER BY id;

-- case: {"id": "following_start_preceding_end_empty", "ordered": true, "pg": "agree"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 30 FOLLOWING AND 31 FOLLOWING) AS c0, SUM(x) OVER (ORDER BY id ROWS BETWEEN 22 FOLLOWING AND 40 FOLLOWING) AS s0, MIN(s) OVER (ORDER BY id ROWS BETWEEN 30 PRECEDING AND 20 PRECEDING) AS m0, FIRST_VALUE(x) OVER (ORDER BY id ROWS BETWEEN 30 FOLLOWING AND 31 FOLLOWING) AS f0 FROM events ORDER BY id;

-- case: {"id": "following_start_value_functions", "ordered": true, "pg": "agree"}
SELECT id, FIRST_VALUE(x) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS next_x, LAST_VALUE(x) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_x, NTH_VALUE(y, 2) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING) AS second_next, MAX(day) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS next_day FROM events ORDER BY id;

-- case: {"id": "following_start_range_int", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY k RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS ahead, SUM(x) OVER (ORDER BY k RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) AS behind, COUNT(*) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 2 FOLLOWING AND 4 FOLLOWING) AS desc_ahead, SUM(id) OVER (ORDER BY k NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) AS null_peers FROM events ORDER BY id;

-- case: {"id": "following_start_range_date", "ordered": true, "pg": "agree"}
SELECT id, COUNT(*) OVER (ORDER BY day RANGE BETWEEN INTERVAL '1' DAY FOLLOWING AND INTERVAL '7' DAY FOLLOWING) AS next_week, SUM(x) OVER (ORDER BY day RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS prior_month FROM events ORDER BY id;

-- case: {"id": "following_start_float_sums", "ordered": true, "pg": "agree"}
SELECT id, SUM(y) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS s, AVG(y) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS a, SUM(y) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS running_before FROM events ORDER BY id;

-- case: {"id": "frame_shape_errors", "ordered": true, "pg": "agree", "assay_error": "frame starting from following row cannot have preceding rows"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) AS c FROM events ORDER BY id;

-- case: {"id": "frame_shape_current_preceding", "ordered": true, "pg": "agree", "assay_error": "frame starting from current row cannot have preceding rows"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) AS c FROM events ORDER BY id;

-- case: {"id": "frame_shape_following_current", "ordered": true, "pg": "agree", "assay_error": "frame starting from following row cannot end with current row"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS 1 FOLLOWING) AS c FROM events ORDER BY id;

-- case: {"id": "frame_shape_unbounded_following_start", "ordered": true, "pg": "agree", "assay_error": "frame start cannot be UNBOUNDED FOLLOWING"}
SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS c FROM events ORDER BY id;

-- case: {"id": "range_decimal_offsets", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS int_offset, SUM(x) OVER (ORDER BY d RANGE BETWEEN 0.75 PRECEDING AND 0.255 FOLLOWING) AS fine_offset, COUNT(*) OVER (PARTITION BY g ORDER BY d DESC NULLS FIRST RANGE BETWEEN 2.5 PRECEDING AND CURRENT ROW) AS desc_count FROM events ORDER BY id;

-- case: {"id": "range_decimal_following", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY d RANGE BETWEEN 0.01 FOLLOWING AND 2.25 FOLLOWING) AS ahead, MIN(id) OVER (ORDER BY d RANGE BETWEEN 3 PRECEDING AND 0.5 PRECEDING) AS behind_min FROM events ORDER BY id;

-- case: {"id": "range_float_offsets", "ordered": true, "pg": "agree"}
SELECT id, COUNT(*) OVER (ORDER BY f RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS near, SUM(x) OVER (ORDER BY f DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND 2.25 FOLLOWING) AS desc_sum, COUNT(*) OVER (ORDER BY f RANGE BETWEEN 0.5 FOLLOWING AND 3 FOLLOWING) AS ahead FROM events ORDER BY id;

-- case: {"id": "range_float_specials", "ordered": true, "pg": "agree"}
SELECT id, f, COUNT(*) OVER (ORDER BY f NULLS FIRST RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW) AS c, MIN(id) OVER (ORDER BY f RANGE BETWEEN CURRENT ROW AND 0.25 FOLLOWING) AS m FROM events ORDER BY id;

-- case: {"id": "range_float_value_functions", "ordered": true, "pg": "agree"}
SELECT id, FIRST_VALUE(f) OVER (ORDER BY f RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS low, LAST_VALUE(f) OVER (ORDER BY f RANGE BETWEEN CURRENT ROW AND 2.5 FOLLOWING) AS high FROM events ORDER BY id;

-- case: {"id": "range_multiple_keys", "ordered": true, "pg": "agree", "assay_error": "unsupported:"}
SELECT id, SUM(x) OVER (ORDER BY g, k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM events ORDER BY id;

-- case: {"id": "range_int_fractional_offset", "ordered": true, "pg": "differs: PostgreSQL rejects a numeric offset for an integer key; DuckDB accepts it", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL: RANGE with offset PRECEDING/FOLLOWING is not supported for column type bigint and offset type numeric)"}
SELECT id, SUM(x) OVER (ORDER BY k RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) AS s FROM events ORDER BY id;

-- case: {"id": "range_date_numeric_offset", "ordered": true, "pg": "differs: PostgreSQL requires an INTERVAL offset for DATE; DuckDB accepts an integer", "assay_error": "unsupported:", "difference": "References disagree (PostgreSQL: not supported for column type date and offset type integer)"}
SELECT id, COUNT(*) OVER (ORDER BY day RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS c FROM events ORDER BY id;

-- case: {"id": "desc_default_aggregates", "ordered": true, "pg": "differs: PostgreSQL places NULLs first for DESC; Assay follows DuckDB (NULLS LAST, decision 2026-10-16)"}
SELECT id, SUM(x) OVER (ORDER BY k DESC) AS running, COUNT(*) OVER (ORDER BY k DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n, FIRST_VALUE(id) OVER (ORDER BY k DESC, id) AS top FROM events ORDER BY id;

-- case: {"id": "desc_default_offsets", "ordered": true, "pg": "differs: PostgreSQL places NULLs first for DESC; Assay follows DuckDB (NULLS LAST, decision 2026-10-16)"}
SELECT id, LAG(id) OVER (ORDER BY d DESC, id) AS prev, LEAD(id, 2, 0) OVER (ORDER BY d DESC, id) AS next2 FROM events ORDER BY id;

-- case: {"id": "desc_default_nonnull_key_agrees", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) OVER (ORDER BY id DESC) AS from_end, LAST_VALUE(x) OVER (ORDER BY id DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_lower FROM events ORDER BY id;

-- case: {"id": "stats_running", "ordered": true, "pg": "agree"}
SELECT id, STDDEV_SAMP(x) OVER (ORDER BY id) AS sd, VAR_POP(d) OVER (ORDER BY id) AS vp, STDDEV_POP(y) OVER (ORDER BY id) AS sdp FROM events ORDER BY id;

-- case: {"id": "stats_sliding", "ordered": true, "pg": "agree"}
SELECT id, STDDEV_SAMP(x) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sd4, VARIANCE(y) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS v5, VAR_SAMP(d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS vd FROM events ORDER BY id;

-- case: {"id": "stats_two_argument", "ordered": true, "pg": "agree", "expected_from": "postgresql", "decision": "degenerate CORR/REGR_SLOPE frames return NULL like PostgreSQL (DuckDB NaN; decision 2026-10-17)"}
SELECT id, COVAR_POP(y, x) OVER (ORDER BY id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS cp, CORR(x, d) OVER (PARTITION BY g) AS c, REGR_SLOPE(y, x) OVER (ORDER BY id) AS slope, REGR_COUNT(y, x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS rc, REGR_INTERCEPT(y, x) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS icpt FROM events ORDER BY id;

-- case: {"id": "stats_regr_family", "ordered": true, "pg": "agree"}
SELECT id, REGR_R2(y, x) OVER w AS r2, REGR_SXX(y, x) OVER w AS sxx, REGR_SYY(y, x) OVER w AS syy, REGR_SXY(y, x) OVER w AS sxy, REGR_AVGX(y, x) OVER w AS ax, REGR_AVGY(y, x) OVER w AS ay, COVAR_SAMP(y, x) OVER w AS cs FROM events WINDOW w AS (ORDER BY id ROWS BETWEEN 4 PRECEDING AND 1 FOLLOWING) ORDER BY id;

-- case: {"id": "stats_groups_exclude", "ordered": true, "pg": "agree"}
SELECT id, STDDEV_POP(x) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS sd, VAR_POP(y) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS vp FROM events ORDER BY id;

-- case: {"id": "stats_empty_and_single", "ordered": true, "pg": "agree", "expected_from": "postgresql", "decision": "single-row CORR returns NULL like PostgreSQL (DuckDB NaN; decision 2026-10-17)"}
SELECT id, STDDEV_SAMP(x) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS one_row, VAR_POP(x) OVER (ORDER BY id ROWS BETWEEN 30 FOLLOWING AND 40 FOLLOWING) AS empty_frame, CORR(x, y) OVER (PARTITION BY id) AS single FROM events ORDER BY id;

-- case: {"id": "bool_windows", "ordered": true, "pg": "agree"}
SELECT id, BOOL_AND(flag) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS all3, BOOL_OR(flag) OVER (PARTITION BY g ORDER BY id) AS any_so_far, BOOL_AND(x > 3) OVER (PARTITION BY g) AS all_big FROM events ORDER BY id;

-- case: {"id": "count_if_windows", "ordered": true, "pg": "rejects"}
SELECT id, COUNT_IF(flag) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS recent_true, COUNT_IF(x > 5) OVER (PARTITION BY g) AS big FROM events ORDER BY id;

-- case: {"id": "arg_extreme_windows", "ordered": true, "pg": "rejects"}
SELECT id, ARG_MAX(id, y) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS best_id, ARG_MIN(s, id) OVER (PARTITION BY g) AS first_s, MAX_BY(d, id) OVER (PARTITION BY g ORDER BY id) AS latest_d, MIN_BY(id, k) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS low_k_id FROM events WHERE id NOT IN (15, 16) ORDER BY id;

-- case: {"id": "median_quantile_windows", "ordered": true, "pg": "rejects"}
SELECT id, MEDIAN(x) OVER (ORDER BY id) AS running_median, QUANTILE_CONT(y, 0.25) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS q1, QUANTILE_DISC(x, 0.5) OVER (PARTITION BY g) AS disc_median, MEDIAN(f) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS real_median FROM events ORDER BY id;

-- case: {"id": "quantile_exclude_desc", "ordered": true, "pg": "rejects"}
SELECT id, QUANTILE_CONT(x, -0.75) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING EXCLUDE CURRENT ROW) AS q, QUANTILE(s, 0.5) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS text_q FROM events ORDER BY id;

-- case: {"id": "filter_windows", "ordered": true, "pg": "agree"}
SELECT id, SUM(x) FILTER (WHERE flag) OVER (ORDER BY id) AS true_running, COUNT(*) FILTER (WHERE x > 5) OVER (PARTITION BY g) AS big, AVG(y) FILTER (WHERE g <> 'a') OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS a, STDDEV_SAMP(x) FILTER (WHERE id % 2 = 0) OVER (ORDER BY id) AS even_sd, MAX(d) FILTER (WHERE k IS NOT NULL) OVER w AS m FROM events WINDOW w AS (PARTITION BY g) ORDER BY id;

-- case: {"id": "filter_window_fallible", "ordered": true, "pg": "differs: PostgreSQL skips the argument on filtered-out rows and answers; DuckDB evaluates it and raises", "assay_error": "unsupported: aggregate FILTER over an argument that may fail", "difference": "DuckDB evaluates the argument on filtered-out rows, PostgreSQL does not"}
SELECT id, SUM(CAST(s AS INTEGER)) FILTER (WHERE FALSE) OVER () AS v FROM events ORDER BY id;

-- case: {"id": "distinct_windows", "ordered": true, "pg": "rejects"}
SELECT id, COUNT(DISTINCT x) OVER (ORDER BY id) AS seen, SUM(DISTINCT x) OVER (PARTITION BY g) AS distinct_total, AVG(DISTINCT d) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS distinct_avg, COUNT(DISTINCT s) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS texts FROM events ORDER BY id;

-- case: {"id": "distinct_float_and_stats", "ordered": true, "pg": "rejects"}
SELECT id, SUM(DISTINCT y) OVER (PARTITION BY g ORDER BY id) AS dy, STDDEV_SAMP(DISTINCT x) OVER (ORDER BY id) AS dsd, MIN(DISTINCT f) OVER (ORDER BY id ROWS 2 PRECEDING) AS dmin FROM events ORDER BY id;

-- case: {"id": "mode_ties_smallest_value", "ordered": true, "pg": "rejects", "expected_from": "postgresql_equivalent", "peer_order": "reference position-dependent", "decision": "window MODE ties go to the smallest value, as grouped MODE (decision 2026-10-18); DuckDB picks by frame position (rows keeps its answer)", "expected_sql_postgresql": "WITH r AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS n, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS pn, COUNT(*) OVER (PARTITION BY g) AS pc FROM events) SELECT a.id, (SELECT mode() WITHIN GROUP (ORDER BY b.x) FROM r b WHERE b.n BETWEEN a.n - 3 AND a.n) AS m, (SELECT mode() WITHIN GROUP (ORDER BY b.s) FROM r b WHERE b.g IS NOT DISTINCT FROM a.g AND b.pn BETWEEN a.pn - 2 AND a.pn + 2) AS ms, (SELECT mode() WITHIN GROUP (ORDER BY b.k) FROM r b WHERE b.g IS NOT DISTINCT FROM a.g) AS mk FROM r a ORDER BY a.id"}
SELECT id, MODE(x) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS m, MODE(s) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS ms, MODE(k) OVER (PARTITION BY g) AS mk FROM events ORDER BY id;

-- case: {"id": "mode_ties_running_exclude", "ordered": true, "pg": "rejects", "expected_from": "postgresql_equivalent", "peer_order": "reference position-dependent", "decision": "window MODE ties go to the smallest value, as grouped MODE (decision 2026-10-18); DuckDB picks by frame position (rows keeps its answer)", "expected_sql_postgresql": "WITH r AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS n FROM events) SELECT a.id, (SELECT mode() WITHIN GROUP (ORDER BY b.d) FROM r b WHERE b.n <= a.n) AS running_d, (SELECT mode() WITHIN GROUP (ORDER BY b.f) FROM r b WHERE b.n BETWEEN a.n - 4 AND a.n + 4 AND b.n <> a.n) AS near_f, (SELECT mode() WITHIN GROUP (ORDER BY b.flag) FROM r b WHERE b.n BETWEEN a.n - 2 AND a.n) AS bool_mode FROM r a ORDER BY a.id"}
SELECT id, MODE(d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_d, MODE(f) OVER (ORDER BY id ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING EXCLUDE CURRENT ROW) AS near_f, MODE(flag) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bool_mode FROM events ORDER BY id;

-- case: {"id": "mode_without_ties", "ordered": true, "pg": "rejects"}
SELECT id, MODE(x) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_mode, MODE(s) OVER (PARTITION BY g) AS text_mode FROM events WHERE id IN (1, 2, 3, 15, 16, 17) ORDER BY id;

-- case: {"id": "mode_within_group_over", "ordered": true, "pg": "agree", "assay_error": "unsupported:"}
SELECT id, MODE() WITHIN GROUP (ORDER BY x) OVER (ORDER BY id) AS m FROM events ORDER BY id;

-- case: {"id": "percentile_within_group_over", "ordered": true, "pg": "agree", "assay_error": "unsupported:"}
SELECT id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER (ORDER BY id) AS p FROM events ORDER BY id;

-- case: {"id": "string_agg_window_refused", "ordered": true, "pg": "rejects", "assay_error": "unsupported:", "difference": "Remaining limit: window STRING_AGG"}
SELECT id, STRING_AGG(s, ',' ORDER BY id) OVER (PARTITION BY g) AS v FROM events ORDER BY id;

-- case: {"id": "all_null_partition", "ordered": true, "pg": "agree"}
SELECT id, STDDEV_POP(x) OVER (PARTITION BY g) AS sd, PERCENT_RANK() OVER (PARTITION BY g ORDER BY k) AS pr, COUNT(x) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS c FROM events WHERE g = 'e' ORDER BY id;

-- case: {"id": "empty_input_new_forms", "ordered": true, "pg": "agree"}
SELECT STDDEV_SAMP(x) OVER (ORDER BY id ROWS 2 PRECEDING) AS sd, SUM(x) OVER (ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING EXCLUDE TIES) AS s, CUME_DIST() OVER (ORDER BY id) AS cd FROM empty_events;
