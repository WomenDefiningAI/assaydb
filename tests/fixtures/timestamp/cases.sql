-- case: {"id": "lit_basic", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-03-01 10:30:00' AS x

-- case: {"id": "lit_fraction", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-03-01 10:30:00.5' AS a, TIMESTAMP '2024-03-01 10:30:00.000001' AS b, TIMESTAMP '2024-03-01 10:30:00.123456' AS c, TIMESTAMP '2024-03-01 10:30:00.120' AS d

-- case: {"id": "lit_forms", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-03-01' AS a, TIMESTAMP '2024-03-01T10:30:00' AS b, TIMESTAMP '2024-03-01 10:30' AS c, TIMESTAMP '2024-3-1 1:2:3' AS d, TIMESTAMP ' 2024-03-01  10:30:00 ' AS e, TIMESTAMP ' 2024-03-01T10:30' AS g, TIMESTAMP '2024-03-01 10:30:00.' AS f

-- case: {"id": "lit_hour24", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-02-29 24:00:00' AS a, TIMESTAMP '2024-12-31 24:00:00' AS b

-- case: {"id": "lit_range_ends", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '0001-01-01 00:00:00' AS a, TIMESTAMP '9999-12-31 23:59:59.999999' AS b, TIMESTAMP '0099-03-01 10:30:00' AS c

-- case: {"id": "lit_without_tz", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP WITHOUT TIME ZONE '2024-03-01 10:30:00' AS x

-- case: {"id": "lit_seven_digits", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL rounds a seventh fractional digit (…457), DuckDB truncates (…456)."}
SELECT TIMESTAMP '2024-03-01 10:30:00.1234567' AS x

-- case: {"id": "lit_offset", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Offsets in TIMESTAMP text: both currently ignore it here, but the meaning belongs to the pending time-zone policy."}
SELECT TIMESTAMP '2024-03-01 10:30:00+02' AS x

-- case: {"id": "lit_zulu", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Zone designators belong to the pending time-zone policy."}
SELECT TIMESTAMP '2024-03-01 10:30:00Z' AS x

-- case: {"id": "lit_leap_second", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL rolls :60 over to the next minute; DuckDB rejects it.", "pg_skip": false}
SELECT TIMESTAMP '2024-03-01 23:59:60' AS x

-- case: {"id": "lit_bc", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL accepts BC; DuckDB rejects it."}
SELECT TIMESTAMP '2024-03-01 10:30:00 BC' AS x

-- case: {"id": "lit_compact", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL accepts compact digits; DuckDB rejects them."}
SELECT TIMESTAMP '20240301 103000' AS x

-- case: {"id": "lit_epoch_word", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Special words: both read 'epoch' as 1970-01-01, but 'infinity' differs (PostgreSQL infinity, DuckDB a sentinel); all words refused together."}
SELECT TIMESTAMP 'epoch' AS x

-- case: {"id": "lit_infinity", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL infinity vs DuckDB 9999-12-31 23:59:59.999999 sentinel display.", "pg_sql": "SELECT CAST('infinity' AS TIMESTAMP)::text AS x"}
SELECT CAST('infinity' AS TIMESTAMP) AS x

-- case: {"id": "lit_year_10000", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Outside AD 0001..9999 (Assay's common display range); both references accept it."}
SELECT TIMESTAMP '10000-03-01 10:30:00' AS x

-- case: {"id": "lit_tab", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Whitespace other than spaces: PostgreSQL trims tabs and newlines; DuckDB accepts a leading tab but rejects a trailing newline."}
SELECT CAST(s AS TIMESTAMP) AS x FROM texts_tab

-- case: {"id": "lit_trailing_space_minutes", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB rejects trailing spaces unless seconds are present; PostgreSQL accepts them."}
SELECT TIMESTAMP '2024-03-01 10:30 ' AS x

-- case: {"id": "lit_bad_day", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMP '2024-02-30 10:00:00' AS x

-- case: {"id": "lit_bad_hour", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMP '2024-03-01 25:00:00' AS x

-- case: {"id": "lit_bad_minute", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMP '2024-03-01 10:60:00' AS x

-- case: {"id": "lit_bad_month", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMP '2024-13-01' AS x

-- case: {"id": "lit_bad_hour24", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMP '2024-03-01 24:00:01' AS x

-- case: {"id": "lit_precision", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL rounds to .123; DuckDB TIMESTAMP_MS truncates (also .123 here, but .1235 differs)."}
SELECT CAST(TIMESTAMP '2024-03-01 10:30:00.123456' AS TIMESTAMP(3)) AS x

-- case: {"id": "lit_tz", "ordered": true, "assay_outcome": "rows", "note": "Old -> new (TIMESTAMPTZ card): was refused pending the time-zone policy; session zone UTC."}
SELECT CAST(CAST(TIMESTAMP '2024-03-01 10:30:00' AS TIMESTAMP WITH TIME ZONE) AS VARCHAR) AS x

-- case: {"id": "cast_text_literal", "ordered": true, "assay_outcome": "rows"}
SELECT CAST('2024-03-01 10:30:00.25' AS TIMESTAMP) AS a, '2024-03-01'::TIMESTAMP AS b

-- case: {"id": "cast_text_column", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(s AS TIMESTAMP) AS t FROM texts ORDER BY id

-- case: {"id": "cast_text_column_bad", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Text outside the shared grammar is refused (both references reject '...x', like DATE's text refusal)."}
SELECT CAST(s || 'x' AS TIMESTAMP) AS t FROM texts WHERE id = 0

-- case: {"id": "cast_text_column_invalid", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT CAST(REPLACE(s, '10:30', '25:30') AS TIMESTAMP) AS t FROM texts WHERE id = 0

-- case: {"id": "cast_date_to_ts", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(DATE '2024-02-29' AS TIMESTAMP) AS a, CAST(DATE '0001-01-01' AS TIMESTAMP) AS b

-- case: {"id": "cast_ts_to_date", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(t AS DATE) AS d FROM edge ORDER BY id

-- case: {"id": "cast_ts_to_text", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(t AS VARCHAR) AS v, CAST(t AS TEXT) AS w FROM edge ORDER BY id

-- case: {"id": "cast_ts_identity", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(t AS TIMESTAMP) AS t FROM edge WHERE id = 2

-- case: {"id": "concat", "ordered": true, "assay_outcome": "rows"}
SELECT 'at ' || t AS a, CONCAT('t=', t) AS b FROM edge WHERE id IN (2, 6) ORDER BY id

-- case: {"id": "extract_year", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_quarter", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(QUARTER FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_month", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MONTH FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_day", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DAY FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_dow", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DOW FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_isodow", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(ISODOW FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_doy", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DOY FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_week", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(WEEK FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_isoyear", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(ISOYEAR FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_hour", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(HOUR FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_minute", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MINUTE FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_microsecond", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MICROSECOND FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_microseconds", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MICROSECONDS FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "extract_second_whole", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(SECOND FROM t) AS x FROM edge WHERE id IN (0, 3, 4, 6) ORDER BY id

-- case: {"id": "extract_second_fraction", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB returns whole seconds (59, BIGINT); PostgreSQL returns 59.500000 (NUMERIC)."}
SELECT EXTRACT(SECOND FROM t) AS x FROM edge WHERE id = 2

-- case: {"id": "extract_millisecond_whole", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MILLISECOND FROM t) AS x, DATE_PART('milliseconds', t) AS y FROM edge WHERE id IN (0, 2, 3, 5) ORDER BY id

-- case: {"id": "extract_millisecond_fraction", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB truncates to 0 (BIGINT); PostgreSQL returns 0.001 (NUMERIC)."}
SELECT EXTRACT(MILLISECOND FROM t) AS x FROM edge WHERE id = 7

-- case: {"id": "extract_epoch", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(EPOCH FROM t) AS x FROM edge ORDER BY id

-- case: {"id": "date_part_epoch", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_PART('epoch', t) AS x, DATE_PART('hour', t) AS h FROM edge WHERE id IN (2, 5) ORDER BY id

-- case: {"id": "extract_decade", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Not in this card's field list (values agree for AD years)."}
SELECT EXTRACT(DECADE FROM t) AS x FROM edge WHERE id = 6

-- case: {"id": "extract_julian", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB DOUBLE vs PostgreSQL NUMERIC with different digits."}
SELECT EXTRACT(JULIAN FROM t) AS x FROM edge WHERE id = 6

-- case: {"id": "extract_null", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(HOUR FROM CAST(NULL AS TIMESTAMP)) AS x

-- case: {"id": "extract_empty", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(EPOCH FROM t) AS x FROM empty_ts

-- case: {"id": "trunc_microsecond", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('microsecond', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_millisecond", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('millisecond', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_second", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('second', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_minute", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('minute', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_hour", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('hour', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_day", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('day', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_week", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('week', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_month", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('month', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_quarter", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('quarter', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_year", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('year', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_plural_case", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('HOURS', t) AS a, DATE_TRUNC('Days', t) AS b, DATE_TRUNC('milliseconds', t) AS c, DATE_TRUNC('months', t) AS d, DATE_TRUNC('years', t) AS e, DATE_TRUNC('weeks', t) AS f FROM edge WHERE id = 5

-- case: {"id": "trunc_quarters", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB accepts 'quarters'; PostgreSQL rejects it."}
SELECT DATE_TRUNC('quarters', t) AS x FROM edge WHERE id = 5

-- case: {"id": "trunc_century", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB truncates to 2000-01-01, PostgreSQL to 2001-01-01."}
SELECT DATE_TRUNC('century', t) AS x FROM edge WHERE id = 6

-- case: {"id": "trunc_millennium", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB 2000-01-01, PostgreSQL 2001-01-01."}
SELECT DATE_TRUNC('millennium', t) AS x FROM edge WHERE id = 6

-- case: {"id": "trunc_date_hour", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DATE input keeps the DATE toolkit boundary (both references answer with TIMESTAMP).", "pg_sql": "SELECT DATE_TRUNC('hour', DATE '2024-01-01')::timestamp AS x"}
SELECT DATE_TRUNC('hour', DATE '2024-01-01') AS x

-- case: {"id": "add_time_units", "ordered": true, "assay_outcome": "rows"}
SELECT id, t + INTERVAL '1' HOUR AS h, t - INTERVAL '90' MINUTE AS m, t + INTERVAL '45' SECOND AS s FROM edge WHERE id BETWEEN 2 AND 8 ORDER BY id

-- case: {"id": "add_days", "ordered": true, "assay_outcome": "rows"}
SELECT id, t + INTERVAL '1' DAY AS a, t - INTERVAL '366' DAY AS b FROM edge WHERE id BETWEEN 2 AND 8 ORDER BY id

-- case: {"id": "add_months_clamp", "ordered": true, "assay_outcome": "rows"}
SELECT id, t + INTERVAL '1' MONTH AS a, t - INTERVAL '1' MONTH AS b, t + INTERVAL '1' YEAR AS c, t + INTERVAL '-13' MONTH AS d FROM edge WHERE id BETWEEN 2 AND 8 ORDER BY id

-- case: {"id": "add_signed_counts", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '+2' HOUR AS a, TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '-1' DAY AS b, TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '100000' HOUR AS c

-- case: {"id": "add_null", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(NULL AS TIMESTAMP) + INTERVAL '1' HOUR AS a, t + INTERVAL '1' MONTH AS b FROM edge WHERE id = 9

-- case: {"id": "add_overflow", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "10000-01-01 is outside AD 0001..9999; both references print it."}
SELECT t + INTERVAL '1' SECOND AS x FROM edge WHERE id = 1

-- case: {"id": "sub_underflow", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "1 BC: PostgreSQL prints '0001-12-31 23:59:59 BC', DuckDB '0001-12-31 (BC) 23:59:59'."}
SELECT t - INTERVAL '1' SECOND AS x FROM edge WHERE id = 0

-- case: {"id": "add_week", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL parses WEEK as a column alias of INTERVAL '1' (one second): 10:00:01; DuckDB adds 7 days."}
SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1' WEEK AS x

-- case: {"id": "add_millisecond_unit", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL parses MILLISECOND as a column alias (adds one second); DuckDB adds 1 ms."}
SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1' MILLISECOND AS x

-- case: {"id": "add_fractional_count", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Fractional interval counts are outside the DATE toolkit's whole-count interval grammar."}
SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1.5' SECOND AS x

-- case: {"id": "add_text_interval", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Unit-in-string intervals are outside the DATE toolkit's interval grammar."}
SELECT TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1 hour' AS x

-- case: {"id": "interval_on_left", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "INTERVAL is only a right operand in Assay; both references accept this order."}
SELECT INTERVAL '1' HOUR + TIMESTAMP '2024-01-31 10:00:00' AS x

-- case: {"id": "date_plus_hour", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Both return TIMESTAMP 2024-01-31 01:00:00; the DATE toolkit keeps its DATE-only boundary."}
SELECT DATE '2024-01-31' + INTERVAL '1' HOUR AS x

-- case: {"id": "ts_minus_ts", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Returns INTERVAL (1 day 01:00:00) in both; Assay has no INTERVAL value type.", "reference_sql": "SELECT CAST(TIMESTAMP '2024-01-31 10:00:00' - TIMESTAMP '2024-01-30 09:00:00' AS VARCHAR) AS x", "pg_sql": "SELECT (TIMESTAMP '2024-01-31 10:00:00' - TIMESTAMP '2024-01-30 09:00:00')::text AS x"}
SELECT TIMESTAMP '2024-01-31 10:00:00' - TIMESTAMP '2024-01-30 09:00:00' AS x

-- case: {"id": "ts_minus_date", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Returns INTERVAL in both references.", "reference_sql": "SELECT CAST(TIMESTAMP '2024-01-31 10:00:00' - DATE '2024-01-30' AS VARCHAR) AS x", "pg_sql": "SELECT (TIMESTAMP '2024-01-31 10:00:00' - DATE '2024-01-30')::text AS x"}
SELECT TIMESTAMP '2024-01-31 10:00:00' - DATE '2024-01-30' AS x

-- case: {"id": "ts_plus_int", "ordered": true, "assay_outcome": "error", "assay_error": "expected numeric type"}
SELECT TIMESTAMP '2024-01-31 10:00:00' + 1 AS x

-- case: {"id": "epoch_difference", "ordered": true, "assay_outcome": "rows", "pg_abs_tolerance": 1e-06, "note": "Difference of two DOUBLE epochs (DuckDB's type): equals DuckDB exactly; PostgreSQL's exact NUMERIC differs below one microsecond."}
SELECT EXTRACT(EPOCH FROM TIMESTAMP '2024-01-31 10:00:00') - EXTRACT(EPOCH FROM TIMESTAMP '2024-01-30 09:00:00.5') AS seconds

-- case: {"id": "to_timestamp", "ordered": true, "assay_outcome": "rows", "note": "Old -> new (TIMESTAMPTZ card): was refused pending the time-zone policy."}
SELECT CAST(TO_TIMESTAMP(1709289015) AS VARCHAR) AS x

-- case: {"id": "strftime", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB STRFTIME and PostgreSQL TO_CHAR use different format grammars."}
SELECT STRFTIME(TIMESTAMP '2024-03-01 10:30:00', '%Y') AS x

-- case: {"id": "now", "ordered": true, "assay_outcome": "rows", "note": "Old -> new (session clock policy): was refused; values are pinned with a fixed clock in tests/timestamptz.rs."}
SELECT NOW() IS NOT NULL AS x

-- case: {"id": "compare_literals", "ordered": true, "assay_outcome": "rows"}
SELECT id, t > TIMESTAMP '2000-01-01 00:00:00' AS a, t = '1970-01-01 00:00:00' AS b, t >= DATE '2024-01-31' AS c, t < '2016-01-03 06:07:08.09' AS d FROM edge ORDER BY id

-- case: {"id": "between_mixed", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM edge WHERE t BETWEEN '1969-12-31' AND DATE '2016-01-03' ORDER BY id

-- case: {"id": "in_list", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM edge WHERE t IN (TIMESTAMP '1970-01-01 00:00:00', '2024-01-31 10:30:00', DATE '2024-03-31') ORDER BY id

-- case: {"id": "not_in_list", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM edge WHERE t NOT IN ('1970-01-01 00:00:00') ORDER BY id

-- case: {"id": "date_equals_midnight", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMP '2024-01-31 00:00:00' = DATE '2024-01-31' AS a, DATE '2024-01-31' < TIMESTAMP '2024-01-31 00:00:00.000001' AS b

-- case: {"id": "compare_bad_literal", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT id FROM edge WHERE t > '2024-02-30'

-- case: {"id": "compare_text_column", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Only string literals convert implicitly (like DATE); PostgreSQL has no timestamp = text operator."}
SELECT edge.id FROM edge, texts WHERE t = s

-- case: {"id": "case_mixed_types", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Both references widen DATE to TIMESTAMP in CASE; Assay refuses mixed DATE/TIMESTAMP results."}
SELECT CASE WHEN id = 0 THEN t ELSE DATE '2024-01-01' END AS x FROM edge WHERE id = 0

-- case: {"id": "coalesce_nullif", "ordered": true, "assay_outcome": "rows"}
SELECT id, COALESCE(t, TIMESTAMP '1999-01-01 00:00:00') AS a, NULLIF(t, TIMESTAMP '1970-01-01 00:00:00') AS b FROM edge WHERE id IN (3, 6, 9) ORDER BY id

-- case: {"id": "is_null", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n FROM edge WHERE t IS NULL

-- case: {"id": "hourly_counts", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('hour', event_ts) AS hour, COUNT(*) AS events, SUM(amount) AS amount FROM events GROUP BY 1 ORDER BY 1 NULLS LAST

-- case: {"id": "sessions_per_day", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(DATE_TRUNC('day', event_ts) AS DATE) AS day, COUNT(DISTINCT user_id) AS users, COUNT(*) AS events FROM events WHERE event_ts IS NOT NULL GROUP BY 1 ORDER BY 1

-- case: {"id": "first_last_per_user", "ordered": true, "assay_outcome": "rows", "pg_abs_tolerance": 1e-06, "note": "Difference of two DOUBLE epochs (DuckDB's type): equals DuckDB exactly; PostgreSQL's exact NUMERIC differs below one microsecond."}
SELECT user_id, MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts, EXTRACT(EPOCH FROM MAX(event_ts)) - EXTRACT(EPOCH FROM MIN(event_ts)) AS span_seconds, COUNT(event_ts) AS n FROM events GROUP BY user_id ORDER BY user_id NULLS LAST

-- case: {"id": "group_by_ts", "ordered": true, "assay_outcome": "rows"}
SELECT event_ts, COUNT(*) AS n FROM events GROUP BY event_ts HAVING COUNT(*) > 1 ORDER BY event_ts NULLS LAST

-- case: {"id": "distinct_ts", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(DISTINCT event_ts) AS a, COUNT(event_ts) AS b, COUNT(*) AS c FROM events

-- case: {"id": "distinct_rows", "ordered": true, "assay_outcome": "rows"}
SELECT DISTINCT DATE_TRUNC('day', event_ts) AS d FROM events ORDER BY d NULLS LAST

-- case: {"id": "order_desc", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts FROM events ORDER BY event_ts DESC NULLS LAST, user_id LIMIT 12

-- case: {"id": "order_nulls_first", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts FROM events ORDER BY event_ts NULLS FIRST, user_id LIMIT 5

-- case: {"id": "min_max_all", "ordered": true, "assay_outcome": "rows"}
SELECT MIN(event_ts) AS a, MAX(event_ts) AS b FROM events

-- case: {"id": "min_max_empty", "ordered": true, "assay_outcome": "rows"}
SELECT MIN(t) AS a, MAX(t) AS b, COUNT(t) AS c FROM empty_ts

-- case: {"id": "where_range", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n FROM events WHERE event_ts >= TIMESTAMP '2024-02-29 00:00:00' AND event_ts < '2024-03-01'

-- case: {"id": "where_date", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n FROM events WHERE event_ts >= DATE '2024-03-01'

-- case: {"id": "join_on_ts", "ordered": true, "assay_outcome": "rows"}
SELECT a.user_id AS user_id, COUNT(*) AS n FROM events a JOIN events_ms b ON a.event_ts = b.event_ts GROUP BY a.user_id ORDER BY a.user_id NULLS LAST

-- case: {"id": "left_join_ts", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n, COUNT(b.event_ts) AS matched FROM events a LEFT JOIN events_s b ON a.event_ts = b.event_ts AND a.user_id = b.user_id

-- case: {"id": "in_subquery", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n FROM events WHERE event_ts IN (SELECT event_ts FROM events_s)

-- case: {"id": "scalar_subquery", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts FROM events WHERE event_ts = (SELECT MAX(event_ts) FROM events)

-- case: {"id": "union_distinct", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n FROM (SELECT event_ts FROM events UNION SELECT event_ts FROM events_ms) u

-- case: {"id": "intersect_except", "ordered": true, "assay_outcome": "rows"}
SELECT (SELECT COUNT(*) FROM (SELECT event_ts FROM events INTERSECT SELECT event_ts FROM events_s) i) AS i, (SELECT COUNT(*) FROM (SELECT event_ts FROM events EXCEPT SELECT event_ts FROM events_s) e) AS e

-- case: {"id": "cte_reuse", "ordered": true, "assay_outcome": "rows"}
WITH d AS (SELECT user_id, DATE_TRUNC('day', event_ts) AS day FROM events) SELECT a.day AS day, COUNT(*) AS n FROM d a JOIN d b ON a.user_id = b.user_id AND a.day = b.day GROUP BY a.day ORDER BY a.day NULLS LAST

-- case: {"id": "unit_s", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n, COUNT(DISTINCT event_ts) AS d, MIN(event_ts) AS a, MAX(event_ts) AS b FROM events_s

-- case: {"id": "unit_ms", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n, COUNT(DISTINCT event_ts) AS d, MIN(event_ts) AS a, MAX(event_ts) AS b, SUM(EXTRACT(MICROSECOND FROM event_ts)) AS us FROM events_ms

-- case: {"id": "unit_ns", "ordered": true, "assay_outcome": "rows", "note": "DuckDB types MAX(ns column) TIMESTAMP_NS; values equal Assay's microsecond TIMESTAMP."}
SELECT user_id, MAX(event_ts) AS last_ts, DATE_TRUNC('hour', MIN(event_ts)) AS first_hour FROM events_ns GROUP BY user_id ORDER BY user_id NULLS LAST

-- case: {"id": "unit_int96", "ordered": true, "assay_outcome": "rows"}
SELECT t, EXTRACT(MICROSECOND FROM t) AS us FROM int96 ORDER BY t NULLS LAST

-- case: {"id": "window_row_number", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_ts, amount) AS rn FROM events WHERE user_id IN (3, 10) ORDER BY user_id, rn

-- case: {"id": "window_lag_lead", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts, LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts, amount) AS prev, LEAD(event_ts, 2) OVER (PARTITION BY user_id ORDER BY event_ts, amount) AS next2 FROM events WHERE user_id = 4 ORDER BY event_ts, amount

-- case: {"id": "window_gap_seconds", "ordered": true, "assay_outcome": "rows", "pg_abs_tolerance": 1e-06, "note": "Difference of two DOUBLE epochs (DuckDB's type): equals DuckDB exactly; PostgreSQL's exact NUMERIC differs below one microsecond."}
SELECT user_id, event_ts, EXTRACT(EPOCH FROM event_ts) - EXTRACT(EPOCH FROM LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts, amount)) AS gap FROM events WHERE user_id = 5 ORDER BY event_ts, amount

-- case: {"id": "window_range_hour", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts, COUNT(*) OVER (PARTITION BY user_id ORDER BY event_ts RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS last_hour FROM events WHERE user_id IN (2, 7) ORDER BY user_id, event_ts, last_hour

-- case: {"id": "window_range_day_minutes", "ordered": true, "assay_outcome": "rows"}
SELECT event_ts, SUM(amount) OVER (ORDER BY event_ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '30' MINUTE FOLLOWING) AS s FROM events WHERE user_id = 1 ORDER BY event_ts

-- case: {"id": "window_range_seconds", "ordered": true, "assay_outcome": "rows"}
SELECT t, COUNT(*) OVER (ORDER BY t RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) AS n FROM edge WHERE id BETWEEN 2 AND 8 ORDER BY t

-- case: {"id": "window_range_month", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Month RANGE offsets are not admitted (variable length); both references support them."}
SELECT COUNT(*) OVER (ORDER BY event_ts RANGE BETWEEN INTERVAL '1' MONTH PRECEDING AND CURRENT ROW) AS n FROM events

-- case: {"id": "window_range_number", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Numeric offsets on a TIMESTAMP key are errors in both references."}
SELECT COUNT(*) OVER (ORDER BY event_ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS n FROM events

-- case: {"id": "window_min_max_first", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_ts, MIN(event_ts) OVER (PARTITION BY user_id) AS first_ts, FIRST_VALUE(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts DESC NULLS LAST, amount) AS latest FROM events WHERE user_id = 6 ORDER BY event_ts, amount

-- case: {"id": "input_tz_utc", "ordered": true, "assay_outcome": "rows", "pg_skip": true, "note": "Old -> new (TIMESTAMPTZ card): isAdjustedToUTC=true now reads as TIMESTAMPTZ (DuckDB); PostgreSQL has no Parquet reader."}
SELECT CAST(t AS VARCHAR) AS t FROM tz_utc

-- case: {"id": "input_tz_named", "ordered": true, "assay_outcome": "rows", "pg_skip": true, "note": "Old -> new (TIMESTAMPTZ card): pandas tz-aware column reads as TIMESTAMPTZ."}
SELECT COUNT(t) AS n FROM tz_ny

-- case: {"id": "input_ns_fraction", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: column 't': TIMESTAMP_NS value is not a whole microsecond", "difference": "DuckDB keeps TIMESTAMP_NS (…15.500000001); PostgreSQL has no nanosecond type. Whole-microsecond ns columns are read.", "pg_skip": true}
SELECT COUNT(t) AS n FROM ns_fraction

-- case: {"id": "input_ms_overflow", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: column 't': TIMESTAMP milliseconds exceed the microsecond range", "difference": "Beyond the 64-bit microsecond range of both references' TIMESTAMP.", "pg_skip": true, "reference_sql": "SELECT CAST(t AS VARCHAR) AS t FROM ms_overflow"}
SELECT COUNT(t) AS n FROM ms_overflow

-- case: {"id": "far_compare", "ordered": true, "assay_outcome": "rows", "note": "Raw microsecond comparison of a year-11476 value, which both references hold."}
SELECT id, t > TIMESTAMP '9999-12-31 23:59:59' AS later FROM far ORDER BY id

-- case: {"id": "far_extract", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Calendar operations cover AD 0001..9999 only.", "pg_skip": true}
SELECT EXTRACT(YEAR FROM t) AS y FROM far WHERE id = 2

