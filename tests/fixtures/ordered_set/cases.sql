-- case: {"id": "median_ungrouped_types", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i), percentile_cont(0.5) WITHIN GROUP (ORDER BY d), percentile_cont(0.5) WITHIN GROUP (ORDER BY v), percentile_cont(0.5) WITHIN GROUP (ORDER BY w), percentile_cont(0.5) WITHIN GROUP (ORDER BY f), percentile_disc(0.5) WITHIN GROUP (ORDER BY s), percentile_disc(0.5) WITHIN GROUP (ORDER BY b) FROM vals", "assay_sql": "SELECT MEDIAN(i), exact_cont(list(CAST(d AS VARCHAR)), '0.5', false), exact_cont(list(CAST(v AS VARCHAR)), '0.5', false), exact_cont(list(CAST(w AS VARCHAR)), '0.5', false), MEDIAN(f), MEDIAN(s), MEDIAN(b) FROM vals", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT MEDIAN(i), MEDIAN(d), MEDIAN(v), MEDIAN(w), MEDIAN(f), MEDIAN(s), MEDIAN(b) FROM vals;

-- case: {"id": "median_grouped", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY i), percentile_cont(0.5) WITHIN GROUP (ORDER BY d), percentile_cont(0.5) WITHIN GROUP (ORDER BY v), percentile_cont(0.5) WITHIN GROUP (ORDER BY f), percentile_disc(0.5) WITHIN GROUP (ORDER BY s) FROM vals GROUP BY g ORDER BY g", "assay_sql": "SELECT g, MEDIAN(i), exact_cont(list(CAST(d AS VARCHAR)), '0.5', false), exact_cont(list(CAST(v AS VARCHAR)), '0.5', false), MEDIAN(f), MEDIAN(s) FROM vals GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, MEDIAN(i), MEDIAN(d), MEDIAN(v), MEDIAN(f), MEDIAN(s) FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "percentile_cont_grouped_asc", "assay_sql": "SELECT g, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i), exact_cont(list(CAST(d AS VARCHAR)), '0.3', false), exact_cont(list(CAST(d AS VARCHAR)), '0.75', false), exact_cont(list(CAST(v AS VARCHAR)), '0.9', false) FROM vals GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i) AS i25, PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY d) AS d30, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY d) AS d75, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY v) AS v90 FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "percentile_cont_grouped_desc", "assay_sql": "SELECT g, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i DESC), exact_cont(list(CAST(d AS VARCHAR)), '0.5', true), exact_cont(list(CAST(d AS VARCHAR)), '0.75', true), PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY i DESC) FROM vals GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i DESC) AS i25, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d DESC) AS d50, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY d DESC NULLS FIRST) AS d75, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY i DESC NULLS LAST) AS i90 FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "percentile_cont_real_finite_groups"}
SELECT g, PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY f) AS f10, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f DESC) AS f50, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f) AS f90 FROM vals WHERE g IN ('A', 'B', 'C') OR g IS NULL GROUP BY g ORDER BY g;

-- case: {"id": "real_nonfinite_order", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY f), percentile_disc(0.5) WITHIN GROUP (ORDER BY f), percentile_disc(0.75) WITHIN GROUP (ORDER BY f), percentile_disc(0.25) WITHIN GROUP (ORDER BY f), percentile_cont(0.25) WITHIN GROUP (ORDER BY f) FROM vals WHERE g = 'E'"}
SELECT MEDIAN(f), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY f), PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY f), QUANTILE_DISC(f, 0.25), QUANTILE_CONT(f, 0.25) FROM vals WHERE g = 'E';

-- case: {"id": "percentile_disc_types"}
SELECT g, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY i) AS i50, PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY d DESC) AS d30, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s) AS s50, PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY dt DESC) AS dt25, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY b) AS b90, PERCENTILE_DISC(0.4) WITHIN GROUP (ORDER BY f) AS f40 FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "fraction_bounds", "pg_sql": "SELECT percentile_cont(0) WITHIN GROUP (ORDER BY i), percentile_cont(1) WITHIN GROUP (ORDER BY i), percentile_disc(0.0) WITHIN GROUP (ORDER BY s), percentile_disc(1.0) WITHIN GROUP (ORDER BY s), percentile_cont(1) WITHIN GROUP (ORDER BY d DESC), percentile_disc(1) WITHIN GROUP (ORDER BY dt DESC) FROM vals", "assay_sql": "SELECT PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY i), PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY i), PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY s), PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY s), exact_cont(list(CAST(d AS VARCHAR)), '1', true), PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY dt DESC) FROM vals", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY i), PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY i), PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY s), PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY s), PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY d DESC), PERCENTILE_DISC(1) WITHIN GROUP (ORDER BY dt DESC) FROM vals;

-- case: {"id": "fraction_as_double", "pg_sql": "SELECT percentile_cont(0.3::float8) WITHIN GROUP (ORDER BY i), percentile_disc(0.3::float8) WITHIN GROUP (ORDER BY i) FROM vals"}
SELECT PERCENTILE_CONT(CAST(0.3 AS DOUBLE)) WITHIN GROUP (ORDER BY i), PERCENTILE_DISC(CAST(0.3 AS DOUBLE)) WITHIN GROUP (ORDER BY i) FROM vals;

-- case: {"id": "duckdb_quantile_spellings", "pg_sql": "SELECT g, percentile_cont(0.25) WITHIN GROUP (ORDER BY i), percentile_disc(0.25) WITHIN GROUP (ORDER BY i), percentile_disc(0.75) WITHIN GROUP (ORDER BY s), percentile_cont(0.25) WITHIN GROUP (ORDER BY d DESC), percentile_disc(0.5) WITHIN GROUP (ORDER BY dt DESC), percentile_cont(1) WITHIN GROUP (ORDER BY i) FROM vals GROUP BY g ORDER BY g", "assay_sql": "SELECT g, QUANTILE_CONT(i, 0.25), QUANTILE_DISC(i, 0.25), QUANTILE(s, 0.75), exact_cont(list(CAST(d AS VARCHAR)), '0.25', true), QUANTILE_DISC(dt, -0.5), QUANTILE_CONT(i, 1) FROM vals GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, QUANTILE_CONT(i, 0.25), QUANTILE_DISC(i, 0.25), QUANTILE(s, 0.75), QUANTILE_CONT(d, -0.25), QUANTILE_DISC(dt, -0.5), QUANTILE_CONT(i, 1) FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "quantile_negative_zero_is_ascending", "pg_sql": "none"}
SELECT QUANTILE_CONT(i, -0.0), QUANTILE_DISC(s, -0.0) FROM vals;

-- case: {"id": "decimal_interpolation_is_double", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY d), percentile_cont(0.5) WITHIN GROUP (ORDER BY v), percentile_cont(0.3) WITHIN GROUP (ORDER BY v DESC), percentile_cont(0.5) WITHIN GROUP (ORDER BY w) FROM vals WHERE g IN ('A', 'C', 'E') GROUP BY g ORDER BY g", "assay_sql": "SELECT g, exact_cont(list(CAST(d AS VARCHAR)), '0.5', false), exact_cont(list(CAST(v AS VARCHAR)), '0.5', false), exact_cont(list(CAST(v AS VARCHAR)), '0.3', true), exact_cont(list(CAST(w AS VARCHAR)), '0.5', false) FROM vals WHERE g IN ('A', 'C', 'E') GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, MEDIAN(d), MEDIAN(v), QUANTILE_CONT(v, -0.3), MEDIAN(w) FROM vals WHERE g IN ('A', 'C', 'E') GROUP BY g ORDER BY g;

-- case: {"id": "empty_input_scalar", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i), percentile_cont(0.5) WITHIN GROUP (ORDER BY d), percentile_disc(0.5) WITHIN GROUP (ORDER BY s), mode() WITHIN GROUP (ORDER BY s), count(DISTINCT s) FROM empty", "assay_sql": "SELECT MEDIAN(i), exact_cont(list(CAST(d AS VARCHAR)), '0.5', false), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s), MODE(s), COUNT(DISTINCT s) FROM empty", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT MEDIAN(i), MEDIAN(d), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s), MODE(s), APPROX_COUNT_DISTINCT(s) FROM empty;

-- case: {"id": "empty_input_grouped"}
SELECT g, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f), MODE() WITHIN GROUP (ORDER BY i) FROM empty GROUP BY g;

-- case: {"id": "single_row", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i), percentile_cont(0.9) WITHIN GROUP (ORDER BY d DESC), percentile_disc(0.1) WITHIN GROUP (ORDER BY s), mode() WITHIN GROUP (ORDER BY dt), percentile_cont(0.37) WITHIN GROUP (ORDER BY f) FROM vals WHERE id = 1", "assay_sql": "SELECT MEDIAN(i), exact_cont(list(CAST(d AS VARCHAR)), '0.9', true), PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY s), MODE(dt), QUANTILE_CONT(f, 0.37) FROM vals WHERE id = 1", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT MEDIAN(i), PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY d DESC), PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY s), MODE(dt), QUANTILE_CONT(f, 0.37) FROM vals WHERE id = 1;

-- case: {"id": "all_null_group_and_null_literal", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY i), mode() WITHIN GROUP (ORDER BY s), percentile_disc(0.5) WITHIN GROUP (ORDER BY dt) FROM vals WHERE g = 'D' GROUP BY g"}
SELECT g, MEDIAN(i), MODE(s), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY dt) FROM vals WHERE g = 'D' GROUP BY g;

-- case: {"id": "mode_plain_ties", "pg_sql": "SELECT g, mode() WITHIN GROUP (ORDER BY m) FROM vals GROUP BY g ORDER BY g", "tie_column": 1, "ties": "SELECT g, list(m ORDER BY m) FROM (SELECT g, m, count(*) AS c, max(count(*)) OVER (PARTITION BY g) AS mx FROM vals WHERE m IS NOT NULL GROUP BY g, m) WHERE c = mx GROUP BY g ORDER BY g"}
SELECT g, MODE(m) FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "mode_within_group_ties"}
SELECT g, MODE() WITHIN GROUP (ORDER BY m) AS asc_mode, MODE() WITHIN GROUP (ORDER BY m DESC) AS desc_mode, MODE() WITHIN GROUP (ORDER BY s) AS s_mode, MODE() WITHIN GROUP (ORDER BY s DESC) AS s_desc FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "mode_types", "pg_sql": "SELECT mode() WITHIN GROUP (ORDER BY i), mode() WITHIN GROUP (ORDER BY d), mode() WITHIN GROUP (ORDER BY dt), mode() WITHIN GROUP (ORDER BY b), mode() WITHIN GROUP (ORDER BY f) FROM vals WHERE g = 'C'"}
SELECT MODE() WITHIN GROUP (ORDER BY i), MODE() WITHIN GROUP (ORDER BY d), MODE() WITHIN GROUP (ORDER BY dt), MODE() WITHIN GROUP (ORDER BY b), MODE() WITHIN GROUP (ORDER BY f) FROM vals WHERE g = 'C';

-- case: {"id": "refuse_mode_real_nan_payloads", "assay_error": "unsupported:", "difference": "DuckDB's MODE counts NaN payloads (NaN, -NaN) as different values, so all four values tie (-inf WITHIN GROUP, NaN by first input position); PostgreSQL has one NaN, which is then the mode", "pg_sql": "none"}
SELECT MODE() WITHIN GROUP (ORDER BY f), MODE(f) FROM vals WHERE g = 'E';

-- case: {"id": "mode_real_canonical_nan"}
SELECT g, MODE() WITHIN GROUP (ORDER BY f) FROM vals WHERE id <> 22 AND g IN ('C', 'E') GROUP BY g ORDER BY g;

-- case: {"id": "approx_count_distinct_is_exact", "pg_sql": "SELECT g, count(DISTINCT s), count(DISTINCT i), count(DISTINCT dt) FROM vals GROUP BY g ORDER BY g", "assay_sql": "SELECT g, count(DISTINCT s), count(DISTINCT i), count(DISTINCT dt) FROM vals GROUP BY g ORDER BY g", "difference": "DuckDB returns a HyperLogLog estimate; Assay returns the exact distinct count (decision for the maintainer)"}
SELECT g, APPROX_COUNT_DISTINCT(s), APPROX_COUNT_DISTINCT(i), APPROX_COUNT_DISTINCT(dt) FROM vals GROUP BY g ORDER BY g;

-- case: {"id": "approx_count_distinct_estimate_differs", "pg_sql": "SELECT count(DISTINCT s), count(DISTINCT x) FROM big", "assay_sql": "SELECT count(DISTINCT s), count(DISTINCT x) FROM big", "difference": "DuckDB returns a HyperLogLog estimate; Assay returns the exact distinct count (decision for the maintainer)"}
SELECT APPROX_COUNT_DISTINCT(s), APPROX_COUNT_DISTINCT(x) FROM big;

-- case: {"id": "expressions_having_and_outer_arithmetic", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY i * 2 + 1) AS m, percentile_cont(0.5) WITHIN GROUP (ORDER BY i) + 1 AS shifted, percentile_disc(0.5) WITHIN GROUP (ORDER BY CASE WHEN b THEN s END) AS true_s FROM vals GROUP BY g HAVING percentile_cont(0.5) WITHIN GROUP (ORDER BY i) > 2 ORDER BY g"}
SELECT g, MEDIAN(i * 2 + 1) AS m, MEDIAN(i) + 1 AS shifted, QUANTILE_DISC(CASE WHEN b THEN s END, 0.5) AS true_s FROM vals GROUP BY g HAVING MEDIAN(i) > 2 ORDER BY g;

-- case: {"id": "join_grouping", "pg_sql": "SELECT r.zone, percentile_cont(0.5) WITHIN GROUP (ORDER BY v.i), percentile_cont(0.5) WITHIN GROUP (ORDER BY v.d), mode() WITHIN GROUP (ORDER BY v.m) FROM vals v JOIN regions r ON v.g = r.g GROUP BY r.zone ORDER BY r.zone", "assay_sql": "SELECT r.zone, MEDIAN(v.i), exact_cont(list(CAST(v.d AS VARCHAR)), '0.5', false), MODE() WITHIN GROUP (ORDER BY v.m) FROM vals v JOIN regions r ON v.g = r.g GROUP BY r.zone ORDER BY r.zone", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT r.zone, MEDIAN(v.i), MEDIAN(v.d), MODE() WITHIN GROUP (ORDER BY v.m) FROM vals v JOIN regions r ON v.g = r.g GROUP BY r.zone ORDER BY r.zone;

-- case: {"id": "shared_input_state", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.9) WITHIN GROUP (ORDER BY x), mode() WITHIN GROUP (ORDER BY x), percentile_cont(0.1) WITHIN GROUP (ORDER BY x DESC), count(*) FROM big GROUP BY g ORDER BY g"}
SELECT g, MEDIAN(x), PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY x), MODE() WITHIN GROUP (ORDER BY x), QUANTILE_CONT(x, -0.1), COUNT(*) FROM big GROUP BY g ORDER BY g;

-- case: {"id": "many_batches_grouped", "pg_sql": "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY q), percentile_cont(0.9) WITHIN GROUP (ORDER BY q), percentile_disc(0.1) WITHIN GROUP (ORDER BY f), percentile_cont(0.75) WITHIN GROUP (ORDER BY f), mode() WITHIN GROUP (ORDER BY m), percentile_disc(0.5) WITHIN GROUP (ORDER BY s DESC) FROM big GROUP BY g ORDER BY g", "tie_column": 5, "ties": "SELECT g, list(m ORDER BY m) FROM (SELECT g, m, count(*) AS c, max(count(*)) OVER (PARTITION BY g) AS mx FROM big WHERE m IS NOT NULL GROUP BY g, m) WHERE c = mx GROUP BY g ORDER BY g", "assay_sql": "SELECT g, exact_cont(list(CAST(q AS VARCHAR)), '0.5', false), exact_cont(list(CAST(q AS VARCHAR)), '0.9', false), PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY f), QUANTILE_CONT(f, 0.75), MODE(m), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s DESC) FROM big GROUP BY g ORDER BY g", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT g, MEDIAN(q), PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY q), PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY f), QUANTILE_CONT(f, 0.75), MODE(m), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY s DESC) FROM big GROUP BY g ORDER BY g;

-- case: {"id": "many_batches_scalar", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_cont(0.99) WITHIN GROUP (ORDER BY q DESC), percentile_cont(0.01) WITHIN GROUP (ORDER BY f), mode() WITHIN GROUP (ORDER BY s), percentile_disc(0.333) WITHIN GROUP (ORDER BY x) FROM big", "assay_sql": "SELECT MEDIAN(x), exact_cont(list(CAST(q AS VARCHAR)), '0.99', true), QUANTILE_CONT(f, 0.01), MODE() WITHIN GROUP (ORDER BY s), PERCENTILE_DISC(0.333) WITHIN GROUP (ORDER BY x) FROM big", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT MEDIAN(x), PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY q DESC), QUANTILE_CONT(f, 0.01), MODE() WITHIN GROUP (ORDER BY s), PERCENTILE_DISC(0.333) WITHIN GROUP (ORDER BY x) FROM big;

-- case: {"id": "error_fraction_out_of_range", "assay_error": "semantic:"}
SELECT PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY i) FROM vals;

-- case: {"id": "error_negative_percentile", "assay_error": "semantic:"}
SELECT PERCENTILE_DISC(-0.25) WITHIN GROUP (ORDER BY i) FROM vals;

-- case: {"id": "error_quantile_out_of_range", "assay_error": "semantic:", "pg_sql": "none"}
SELECT QUANTILE_DISC(i, -1.5) FROM vals;

-- case: {"id": "error_null_fraction", "assay_error": "unsupported:", "difference": "PostgreSQL returns NULL; DuckDB raises a binder error"}
SELECT PERCENTILE_CONT(NULL) WITHIN GROUP (ORDER BY i) FROM vals;

-- case: {"id": "error_non_constant_fraction", "assay_error": "unsupported:"}
SELECT PERCENTILE_CONT(i / 10.0) WITHIN GROUP (ORDER BY i) FROM vals;

-- case: {"id": "error_percentile_cont_text", "assay_error": "semantic:"}
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s) FROM vals;

-- case: {"id": "error_quantile_cont_boolean", "assay_error": "semantic:", "pg_sql": "none"}
SELECT QUANTILE_CONT(b, 0.5) FROM vals;

-- case: {"id": "refuse_median_date_timestamp", "assay_error": "unsupported:", "difference": "DuckDB interpolates DATE to TIMESTAMP (not yet an Assay type); PostgreSQL has no MEDIAN and rejects PERCENTILE_CONT over DATE", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY dt) FROM vals"}
SELECT MEDIAN(dt) FROM vals;

-- case: {"id": "refuse_fraction_list", "assay_error": "unsupported:", "difference": "a list of fractions returns a LIST, which Assay does not have yet", "pg_sql": "SELECT percentile_cont(ARRAY[0.25, 0.75]) WITHIN GROUP (ORDER BY i) FROM vals"}
SELECT QUANTILE_CONT(i, [0.25, 0.75]) FROM vals;

-- case: {"id": "refuse_zero_fraction_desc", "assay_error": "unsupported:", "difference": "DuckDB 1.5.6 returns the minimum (as if ascending); PostgreSQL returns the first value in DESC order (the maximum)"}
SELECT PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY i DESC) FROM vals;

-- case: {"id": "refuse_distinct", "assay_error": "unsupported:", "difference": "DISTINCT ordered-set aggregates are not implemented (PostgreSQL rejects DISTINCT with WITHIN GROUP)", "pg_sql": "none"}
SELECT MEDIAN(DISTINCT i) FROM vals;

-- case: {"id": "refuse_percentile_disc_binary64_index", "assay_error": "unsupported:", "difference": "0.28 * 25 = 7: DuckDB computes ceil(n * q) exactly and returns row 7; PostgreSQL computes it in binary64 (7.000000000000001) and returns row 8"}
SELECT PERCENTILE_DISC(0.28) WITHIN GROUP (ORDER BY x) FROM seq;

-- case: {"id": "quantile_disc_exact_index", "pg_sql": "none"}
SELECT QUANTILE_DISC(x, 0.28), PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY x), PERCENTILE_DISC(0.32) WITHIN GROUP (ORDER BY x DESC) FROM seq;

-- case: {"id": "decimal_desc_through_subquery", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x DESC) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s", "assay_sql": "SELECT exact_cont(list(CAST(x AS VARCHAR)), '0.5', true) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s"}
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x DESC) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s;

-- case: {"id": "decimal_asc_through_subquery", "pg_sql": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s", "assay_sql": "SELECT exact_cont(list(CAST(x AS VARCHAR)), '0.5', false) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s", "difference": "interpolated DECIMAL returns DOUBLE like PostgreSQL (maintainer decision October 17); DuckDB returns DECIMAL with direction-dependent rounding"}
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT d AS x FROM vals WHERE id IN (1, 2)) s;

-- case: {"id": "refuse_real_infinite_neighbours", "assay_error": "unsupported:", "difference": "between +inf and +inf DuckDB returns +inf and PostgreSQL NaN"}
SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY x) FROM edge WHERE g = 1;

-- case: {"id": "refuse_real_overflow", "assay_error": "unsupported:", "difference": "between -1e308 and 1e308 DuckDB returns 0.0 and PostgreSQL +inf (overflow of hi - lo)"}
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) FROM edge WHERE g = 2;

-- case: {"id": "duckdb_only_spelling_overflow", "pg_sql": "none"}
SELECT g, MEDIAN(x), QUANTILE_CONT(x, 0.25) FROM edge GROUP BY g ORDER BY g;
