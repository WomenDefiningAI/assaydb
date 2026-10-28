-- case: {"id": "variance_family_int", "ordered": true}
SELECT STDDEV(x), STDDEV_SAMP(x), STDDEV_POP(x), VARIANCE(x), VAR_SAMP(x), VAR_POP(x) FROM stats;

-- case: {"id": "variance_family_decimal", "ordered": true}
SELECT STDDEV(q), STDDEV_SAMP(q), STDDEV_POP(q), VARIANCE(q), VAR_SAMP(q), VAR_POP(q) FROM stats;

-- case: {"id": "variance_family_real", "ordered": true}
SELECT STDDEV(f), STDDEV_SAMP(f), STDDEV_POP(f), VARIANCE(f), VAR_SAMP(f), VAR_POP(f) FROM stats;

-- case: {"id": "variance_of_expressions", "ordered": true}
SELECT STDDEV(x * 2 + 1), VAR_POP(q - 1), STDDEV_POP(f * y), VAR_SAMP(CASE WHEN b THEN x END) FROM stats;

-- case: {"id": "covariance_correlation_exact", "ordered": true}
SELECT COVAR_SAMP(z, x), COVAR_POP(z, x), CORR(z, x), CORR(x, z) FROM stats;

-- case: {"id": "covariance_correlation_mixed", "ordered": true}
SELECT COVAR_POP(f, q), COVAR_SAMP(q, x), CORR(q, x), CORR(f, y), CORR(y, x) FROM stats;

-- case: {"id": "regression_family_exact", "ordered": true}
SELECT REGR_SLOPE(z, x), REGR_INTERCEPT(z, x), REGR_R2(z, x), REGR_SXX(z, x), REGR_SYY(z, x), REGR_SXY(z, x), REGR_AVGX(z, x), REGR_AVGY(z, x), REGR_COUNT(z, x) FROM stats;

-- case: {"id": "regression_family_real", "ordered": true}
SELECT REGR_SLOPE(y, f), REGR_INTERCEPT(y, f), REGR_R2(y, f), REGR_SXX(y, f), REGR_SYY(y, f), REGR_SXY(y, f), REGR_AVGX(y, f), REGR_AVGY(y, f), REGR_COUNT(y, f) FROM stats;

-- case: {"id": "regression_family_decimal_int", "ordered": true}
SELECT REGR_SLOPE(q, x), REGR_INTERCEPT(q, x), REGR_R2(q, x), REGR_SXX(q, x), REGR_SXY(q, x), REGR_AVGY(q, x), REGR_COUNT(q, x) FROM stats;

-- case: {"id": "grouped_variance", "ordered": true}
SELECT region, COUNT(*) AS n, STDDEV(x) AS sd_x, VAR_POP(q) AS vp_q, STDDEV_SAMP(f) AS sd_f, VAR_SAMP(y) AS vs_y FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "grouped_correlation", "ordered": true}
SELECT region, CORR(z, x) AS c_zx, CORR(y, f) AS c_yf, COVAR_SAMP(q, x) AS cov_qx, REGR_SLOPE(y, f) AS slope, REGR_INTERCEPT(z, x) AS icpt, REGR_R2(y, f) AS r2 FROM stats WHERE region IN ('EU', 'AMER', 'APAC') GROUP BY region ORDER BY region;

-- case: {"id": "grouped_regression_degenerate_groups", "ordered": true}
SELECT region, REGR_INTERCEPT(z, x) AS icpt, REGR_R2(z, x) AS r2, REGR_SXX(z, x) AS sxx, REGR_SYY(z, x) AS syy, REGR_AVGX(z, x) AS ax, REGR_COUNT(z, x) AS n, COVAR_POP(z, x) AS cp, COVAR_SAMP(z, x) AS cs, STDDEV_SAMP(x) AS sd FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "constant_y_r2_is_one", "ordered": true}
SELECT REGR_R2(c, x), REGR_SLOPE(c, x), REGR_INTERCEPT(c, x), REGR_SYY(c, x), COVAR_POP(c, x) FROM stats;

-- case: {"id": "constant_x_intercept_and_r2_null", "ordered": true}
SELECT REGR_INTERCEPT(x, c), REGR_R2(x, c), REGR_SXX(x, c), REGR_AVGX(x, c), COVAR_SAMP(x, c), VAR_POP(c), STDDEV_SAMP(c) FROM stats;

-- case: {"id": "empty_input", "ordered": true}
SELECT STDDEV(x), VAR_POP(f), COVAR_POP(z, x), CORR(z, x), REGR_SLOPE(z, x), REGR_SXX(z, x), REGR_COUNT(z, x), ARG_MAX(name, x), BOOL_AND(b), BOOL_OR(b), COUNT_IF(b) FROM empty;

-- case: {"id": "empty_input_grouped", "ordered": true}
SELECT region, STDDEV(x), CORR(z, x), REGR_COUNT(z, x), ARG_MIN(name, x), COUNT_IF(b) FROM empty GROUP BY region;

-- case: {"id": "all_null_group", "ordered": true}
SELECT STDDEV_POP(x), VAR_SAMP(x), COVAR_POP(z, x), REGR_AVGX(z, x), REGR_COUNT(z, x), CORR(z, x), REGR_SLOPE(z, x), ARG_MAX(name, x) FROM stats WHERE x IS NULL;

-- case: {"id": "single_row", "ordered": true}
SELECT STDDEV_SAMP(x), STDDEV_POP(x), VAR_SAMP(q), VAR_POP(f), COVAR_POP(z, x), COVAR_SAMP(z, x), REGR_SXX(z, x), REGR_SXY(z, x), REGR_INTERCEPT(z, x), REGR_R2(z, x), REGR_AVGY(z, x), REGR_COUNT(z, x) FROM stats WHERE id = 1;

-- case: {"id": "null_arguments", "ordered": true}
SELECT STDDEV(NULL), VAR_POP(NULL), CORR(NULL, x), REGR_COUNT(NULL, x), REGR_AVGX(x, NULL), COUNT_IF(NULL) FROM stats;

-- case: {"id": "distinct_one_argument", "ordered": true}
SELECT STDDEV(DISTINCT x), VAR_POP(DISTINCT q), VAR_SAMP(DISTINCT f), STDDEV_POP(DISTINCT y) FROM stats;

-- case: {"id": "distinct_two_argument_exact", "ordered": true}
SELECT CORR(DISTINCT z, x), REGR_COUNT(DISTINCT z, x), COVAR_POP(DISTINCT q, x), REGR_SLOPE(DISTINCT z, x) FROM stats;

-- case: {"id": "distinct_grouped", "ordered": true}
SELECT region, STDDEV(DISTINCT x) AS sd, VAR_POP(DISTINCT f) AS vp, COUNT_IF(DISTINCT b) AS ci FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "boolean_family", "ordered": true}
SELECT BOOL_AND(b), BOOL_OR(b), COUNT_IF(b), COUNT_IF(x > 4), BOOL_AND(x > 0), BOOL_OR(x > 100) FROM stats;

-- case: {"id": "boolean_grouped", "ordered": true}
SELECT region, BOOL_AND(b) AS all_b, BOOL_OR(b) AS any_b, COUNT_IF(b) AS n_b, COUNT_IF(NOT b) AS n_not_b, COUNT_IF(b IS NULL) AS n_null FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "boolean_all_null_and_distinct", "ordered": true}
SELECT BOOL_AND(b), BOOL_OR(b), COUNT_IF(b), BOOL_AND(DISTINCT b), COUNT_IF(DISTINCT b) FROM stats WHERE b IS NULL;

-- case: {"id": "boolean_distinct", "ordered": true}
SELECT BOOL_AND(DISTINCT b), BOOL_OR(DISTINCT b), COUNT_IF(DISTINCT b), COUNT_IF(DISTINCT x > 4) FROM stats;

-- case: {"id": "arg_extremes_unique_values", "ordered": true}
SELECT ARG_MAX(name, id), ARG_MIN(name, id), MAX_BY(q, dt), MIN_BY(dt, y), ARG_MAX(x, name), ARG_MIN(f, name), ARG_MAX(b, z), ARG_MIN(id, q) FROM stats;

-- case: {"id": "arg_extremes_grouped", "ordered": true}
SELECT region, ARG_MAX(name, id) AS last_name, ARG_MIN(x, id) AS first_x, MAX_BY(q, dt) AS latest_q, MIN_BY(f, dt) AS earliest_f FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "arg_extremes_skip_null_arguments", "ordered": true}
SELECT ARG_MAX(q, z), ARG_MIN(x, f), ARG_MAX(name, y), MAX_BY(b, id), ARG_MAX(f, y) FROM stats WHERE region = 'EU';

-- case: {"id": "arg_extremes_real_and_decimal_values", "ordered": true}
SELECT ARG_MAX(name, f), ARG_MIN(name, y), ARG_MAX(name, q), ARG_MAX(dt, y) FROM stats;

-- case: {"id": "arg_max_ties_grouped", "ordered": true, "ties": "SELECT s.region, list(s.name ORDER BY s.id) FROM stats s JOIN (SELECT region, max(x) AS m FROM stats WHERE name IS NOT NULL GROUP BY region) t ON s.region IS NOT DISTINCT FROM t.region AND s.x = t.m WHERE s.name IS NOT NULL GROUP BY s.region ORDER BY s.region NULLS LAST"}
SELECT region, ARG_MAX(name, x) AS top FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "arg_min_ties_boolean_value", "ordered": true, "ties": "SELECT 0, list(id ORDER BY id) FROM stats WHERE b = false"}
SELECT 0 AS k, ARG_MIN(id, b) AS first_false FROM stats;

-- case: {"id": "arg_max_distinct_is_ignored", "ordered": true}
SELECT ARG_MAX(DISTINCT name, id), MIN_BY(DISTINCT x, id) FROM stats;

-- case: {"id": "composed_expressions", "ordered": true}
SELECT region, STDDEV(x) * 2 AS twice_sd, VAR_POP(q) + COVAR_POP(z, x) AS combo, CASE WHEN COUNT_IF(b) > 1 THEN 'many' ELSE 'few' END AS flag, REGR_COUNT(z, x) - COUNT(*) AS missing FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "having_on_statistic", "ordered": true}
SELECT region, COUNT(*) AS n FROM stats GROUP BY region HAVING STDDEV_POP(x) > 1 ORDER BY region;

-- case: {"id": "order_by_statistic", "ordered": true}
SELECT region, VAR_SAMP(x) AS v FROM stats GROUP BY region ORDER BY v DESC NULLS LAST, region;

-- case: {"id": "derived_table_filter", "ordered": true}
SELECT region, s FROM (SELECT region, STDDEV(x) AS s FROM stats GROUP BY region) t WHERE s > 1 ORDER BY region;

-- case: {"id": "join_then_group", "ordered": true}
SELECT r.zone, CORR(s.z, s.x) AS c, STDDEV_SAMP(s.q) AS sd, ARG_MAX(s.name, s.id) AS last_name, BOOL_OR(s.b) AS any_b FROM stats s JOIN regions r ON s.region = r.region GROUP BY r.zone ORDER BY r.zone;

-- case: {"id": "wide_integer_and_decimal_extremes", "ordered": true}
SELECT STDDEV_SAMP(w) AS sd_w, VAR_POP(w) AS vp_w, VAR_POP(d) AS vp_d, STDDEV_SAMP(e) AS sd_e, COVAR_POP(d, w) AS cov_dw, REGR_SXX(e, d) AS sxx, CORR(d, w) AS c FROM wide WHERE g = 1;

-- case: {"id": "exact_beyond_double_precision", "ordered": true, "exact": "wide_g2", "difference": "DuckDB rounds inputs to DOUBLE; exact by decision 2026-10-17. Inputs 2^53+1, 2^53+3, 2^53+5 become 2^53, 2^53+4, 2^53+4 in DuckDB; Assay equals PostgreSQL's exact NUMERIC VAR/STDDEV and the true value"}
SELECT VAR_POP(w), VAR_SAMP(d), STDDEV_POP(e), COVAR_POP(d, w), REGR_SXY(d, w) FROM wide WHERE g = 2;

-- case: {"id": "real_cancellation", "ordered": true}
SELECT g, VAR_SAMP(r) AS vs, STDDEV_POP(r) AS sp, REGR_SXX(r, r) AS sxx FROM wide GROUP BY g ORDER BY g;

-- case: {"id": "many_batches_grouped", "ordered": true, "exact": "big_grouped"}
SELECT g, COUNT(*) AS n, STDDEV_SAMP(x) AS sd_x, VAR_POP(q) AS vp_q, STDDEV_POP(f) AS sp_f, CORR(y, f) AS c, REGR_SLOPE(y, f) AS slope, REGR_INTERCEPT(y, f) AS icpt, COVAR_SAMP(q, x) AS cov, REGR_R2(y, f) AS r2 FROM big GROUP BY g ORDER BY g;

-- case: {"id": "many_batches_scalar", "ordered": true, "exact": "big_scalar"}
SELECT STDDEV_SAMP(x), VAR_POP(q), STDDEV_SAMP(f), CORR(y, f), REGR_SLOPE(y, x), REGR_INTERCEPT(q, x), REGR_COUNT(y, x), COUNT_IF(x > 0), BOOL_OR(f > 1003000) FROM big;

-- case: {"id": "many_batches_arg_extremes", "ordered": true}
SELECT g, ARG_MAX(id, f) AS id_max_f, ARG_MIN(id, q) AS id_min_q, MAX_BY(x, id) AS last_x FROM big GROUP BY g ORDER BY g;

-- case: {"id": "finite_control_group", "ordered": true}
SELECT VAR_POP(v), STDDEV_SAMP(v) FROM nonfinite WHERE g = 6;

-- case: {"id": "corr_constant_argument_is_null", "ordered": true, "expected_from": "postgresql", "difference": "DuckDB returns NaN; NULL by decision 2026-10-17 (PostgreSQL and the SQL standard)"}
SELECT CORR(x, c), CORR(c, x), REGR_SLOPE(x, c) FROM stats;

-- case: {"id": "degenerate_groups_are_null_beside_others", "ordered": true, "expected_from": "postgresql", "difference": "DuckDB returns NaN for the one-row MEA and NULL-region groups; NULL by decision 2026-10-17 (PostgreSQL and the SQL standard)"}
SELECT region, COUNT(*) AS n, CORR(z, x) AS c, REGR_SLOPE(z, x) AS slope, REGR_INTERCEPT(z, x) AS icpt FROM stats GROUP BY region ORDER BY region;

-- case: {"id": "degenerate_groups_having_is_null", "ordered": true, "expected_from": "postgresql", "difference": "DuckDB's NaN is not NULL, so DuckDB returns no rows; NULL by decision 2026-10-17 (PostgreSQL and the SQL standard)"}
SELECT region, COUNT(*) AS n, CORR(z, x) AS c FROM stats GROUP BY region HAVING CORR(z, x) IS NULL ORDER BY region;

-- case: {"id": "degenerate_groups_excluded_by_having_count", "ordered": true}
SELECT region, COUNT(*) AS n, REGR_COUNT(z, x) AS pairs, CORR(z, x) AS c, REGR_SLOPE(z, x) AS slope FROM stats GROUP BY region HAVING REGR_COUNT(z, x) > 1 ORDER BY region;

-- case: {"id": "regr_slope_single_row_is_null", "ordered": true, "expected_from": "postgresql", "difference": "DuckDB returns NaN; NULL by decision 2026-10-17 (PostgreSQL and the SQL standard)"}
SELECT REGR_SLOPE(z, x), CORR(z, x), REGR_INTERCEPT(z, x), REGR_R2(z, x) FROM stats WHERE id = 1;

-- case: {"id": "refuse_nonfinite_input", "ordered": true, "assay_error": "unsupported: STDDEV_SAMP over a NaN or infinite REAL input", "difference": "DuckDB raises an out-of-range error; PostgreSQL returns NaN"}
SELECT STDDEV_SAMP(v) FROM nonfinite WHERE g = 1;

-- case: {"id": "refuse_nonfinite_nan_input", "ordered": true, "assay_error": "unsupported: CORR over a NaN or infinite REAL input", "difference": "DuckDB raises an out-of-range error; PostgreSQL returns NaN"}
SELECT CORR(v, v) FROM nonfinite WHERE g = 2;

-- case: {"id": "refuse_single_infinity", "ordered": true, "assay_error": "unsupported: VAR_POP over a NaN or infinite REAL input", "difference": "DuckDB returns 0 for VAR_POP of one infinity; PostgreSQL returns NaN"}
SELECT VAR_POP(v) FROM nonfinite WHERE g = 3;

-- case: {"id": "nonfinite_single_row_sample_is_null", "ordered": true}
SELECT STDDEV_SAMP(v), VAR_SAMP(v) FROM nonfinite WHERE g = 3;

-- case: {"id": "nonfinite_pair_averages_and_counts", "ordered": true}
SELECT g, REGR_AVGX(y, x) AS ax, REGR_AVGY(y, x) AS ay, REGR_COUNT(y, x) AS n FROM pairs GROUP BY g ORDER BY g;

-- case: {"id": "nonfinite_single_pairs", "ordered": true, "expected_from": "postgresql", "difference": "CORR of one pair with exactly one nonfinite value and REGR_SLOPE of one pair with finite X: DuckDB NaN, PostgreSQL NULL; NULL by decision 2026-10-17. The other cells agree"}
SELECT g, CORR(y, x) AS c, REGR_SLOPE(y, x) AS slope, COVAR_POP(y, x) AS cp, REGR_SXY(y, x) AS sxy, COVAR_SAMP(y, x) AS cs FROM pairs WHERE g IN (1, 2, 3) GROUP BY g ORDER BY g;

-- case: {"id": "nonfinite_y_with_constant_finite_x", "ordered": true, "expected_from": "postgresql", "difference": "REGR_SLOPE: DuckDB NaN, PostgreSQL NULL; NULL by decision 2026-10-17. The other cells agree"}
SELECT g, REGR_SLOPE(y, x) AS slope, REGR_INTERCEPT(y, x) AS icpt, REGR_R2(y, x) AS r2, REGR_SXX(y, x) AS sxx FROM pairs WHERE g IN (1, 4) GROUP BY g ORDER BY g;

-- case: {"id": "nonfinite_x_with_finite_y_sums", "ordered": true}
SELECT g, REGR_SYY(y, x) AS syy FROM pairs WHERE g IN (2, 5, 6) GROUP BY g ORDER BY g;

-- case: {"id": "refuse_nonfinite_covariance_order_dependent", "ordered": true, "assay_error": "unsupported: COVAR_POP over a NaN or infinite REAL input", "difference": "DuckDB returns +inf for these rows (its value depends on row order); PostgreSQL returns NaN"}
SELECT COVAR_POP(y, x) FROM pairs WHERE g = 6;

-- case: {"id": "refuse_nonfinite_corr_constant_y", "ordered": true, "assay_error": "unsupported: CORR over a NaN or infinite REAL input", "difference": "DuckDB raises an out-of-range error; PostgreSQL returns NULL"}
SELECT CORR(y, x) FROM pairs WHERE g = 4;

-- case: {"id": "refuse_nonfinite_regr_sxx", "ordered": true, "assay_error": "unsupported: REGR_SXX over a NaN or infinite REAL input", "difference": "DuckDB returns 0; PostgreSQL returns NaN"}
SELECT REGR_SXX(y, x) FROM pairs WHERE g = 2;

-- case: {"id": "finite_overflow_error", "ordered": true, "assay_error": "statistical aggregate out of range: intermediate value exceeds DOUBLE"}
SELECT VAR_POP(v) FROM nonfinite WHERE g = 4;

-- case: {"id": "refuse_every", "ordered": true, "assay_error": "unsupported: EVERY (DuckDB 1.5.6 has no EVERY; BOOL_AND is equivalent)"}
SELECT EVERY(b) FROM stats;

-- case: {"id": "refuse_arg_max_count_argument", "ordered": true, "assay_error": "unsupported: ARG_MAX with a count argument", "difference": "DuckDB returns a LIST; Assay has no LIST type"}
SELECT ARG_MAX(name, x, 2) FROM stats;

-- case: {"id": "refuse_distinct_two_argument_real", "ordered": true, "assay_error": "unsupported: CORR(DISTINCT ...) over REAL", "difference": "Remaining limit: DISTINCT pairs with a REAL argument are not yet supported"}
SELECT CORR(DISTINCT y, f) FROM stats;

-- case: {"id": "refuse_filter_on_this_base", "ordered": true, "difference": "Integration candidate 2: FILTER arrived with the aggregate-modifier change (#100); the case now compares with DuckDB (PostgreSQL agrees); the id keeps its original name"}
SELECT STDDEV(x) FILTER (WHERE x > 2) FROM stats;

-- case: {"id": "refuse_window", "ordered": true}
SELECT id, STDDEV(x) OVER () FROM stats ORDER BY id;

-- case: {"id": "error_text_argument", "ordered": true, "assay_error": "STDDEV requires a numeric argument, got Text"}
SELECT STDDEV(name) FROM stats;

-- case: {"id": "error_boolean_on_integer", "ordered": true, "assay_error": "BOOL_AND requires a BOOLEAN argument, got Int"}
SELECT BOOL_AND(x) FROM stats;

-- case: {"id": "error_count_if_integer", "ordered": true, "assay_error": "COUNT_IF requires a BOOLEAN argument, got Int"}
SELECT COUNT_IF(x) FROM stats;

-- case: {"id": "error_corr_text", "ordered": true, "assay_error": "CORR requires numeric arguments, got Text"}
SELECT CORR(name, x) FROM stats;

-- case: {"id": "error_corr_arity", "ordered": true, "assay_error": "CORR requires two arguments"}
SELECT CORR(x) FROM stats;
