-- case: {"id": "lit_forms_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+05' AS a, TIMESTAMPTZ '2024-03-10 06:30:00+0530' AS b, TIMESTAMPTZ '2024-03-10 06:30:00-05:30' AS c, TIMESTAMPTZ '2024-03-10 06:30:00.25Z' AS d, TIMESTAMPTZ '2024-03-10 06:30:00 UTC' AS e, TIMESTAMPTZ '2024-03-10 01:30:00 America/New_York' AS f, TIMESTAMPTZ '2024-03-10 06:30:00 asia/kolkata' AS g

-- case: {"id": "lit_session_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-07-01 12:00:00' AS a, TIMESTAMPTZ '2024-07-01' AS b, TIMESTAMP WITH TIME ZONE '2024-01-15 08:00:00.5' AS c

-- case: {"id": "lit_forms_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+05' AS a, TIMESTAMPTZ '2024-03-10 06:30:00+0530' AS b, TIMESTAMPTZ '2024-03-10 06:30:00-05:30' AS c, TIMESTAMPTZ '2024-03-10 06:30:00.25Z' AS d, TIMESTAMPTZ '2024-03-10 06:30:00 UTC' AS e, TIMESTAMPTZ '2024-03-10 01:30:00 America/New_York' AS f, TIMESTAMPTZ '2024-03-10 06:30:00 asia/kolkata' AS g

-- case: {"id": "lit_session_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-07-01 12:00:00' AS a, TIMESTAMPTZ '2024-07-01' AS b, TIMESTAMP WITH TIME ZONE '2024-01-15 08:00:00.5' AS c

-- case: {"id": "lit_forms_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+05' AS a, TIMESTAMPTZ '2024-03-10 06:30:00+0530' AS b, TIMESTAMPTZ '2024-03-10 06:30:00-05:30' AS c, TIMESTAMPTZ '2024-03-10 06:30:00.25Z' AS d, TIMESTAMPTZ '2024-03-10 06:30:00 UTC' AS e, TIMESTAMPTZ '2024-03-10 01:30:00 America/New_York' AS f, TIMESTAMPTZ '2024-03-10 06:30:00 asia/kolkata' AS g

-- case: {"id": "lit_session_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-07-01 12:00:00' AS a, TIMESTAMPTZ '2024-07-01' AS b, TIMESTAMP WITH TIME ZONE '2024-01-15 08:00:00.5' AS c

-- case: {"id": "lit_forms_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+05' AS a, TIMESTAMPTZ '2024-03-10 06:30:00+0530' AS b, TIMESTAMPTZ '2024-03-10 06:30:00-05:30' AS c, TIMESTAMPTZ '2024-03-10 06:30:00.25Z' AS d, TIMESTAMPTZ '2024-03-10 06:30:00 UTC' AS e, TIMESTAMPTZ '2024-03-10 01:30:00 America/New_York' AS f, TIMESTAMPTZ '2024-03-10 06:30:00 asia/kolkata' AS g

-- case: {"id": "lit_session_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-07-01 12:00:00' AS a, TIMESTAMPTZ '2024-07-01' AS b, TIMESTAMP WITH TIME ZONE '2024-01-15 08:00:00.5' AS c

-- case: {"id": "lit_seconds_offset", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+05:30:15' AS a

-- case: {"id": "lit_gap_ny", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 02:30:00' AS gap, TIMESTAMPTZ '2024-11-03 01:30:00' AS overlap

-- case: {"id": "lit_gap_named", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 02:30:00 America/New_York' AS gap, TIMESTAMPTZ '2024-11-03 01:30:00 America/New_York' AS overlap

-- case: {"id": "lit_lord_howe", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-10-06 02:15:00' AS gap, TIMESTAMPTZ '2024-04-07 01:45:00' AS overlap, TIMESTAMPTZ '2024-07-01 12:00:00' AS winter

-- case: {"id": "lit_abbreviation", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Zone abbreviations: DuckDB reads PST as America/Los_Angeles (13:30Z), PostgreSQL as fixed -08 (14:30Z)."}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00 PST' AS a

-- case: {"id": "lit_spaced_offset", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB rejects a space before a numeric offset; PostgreSQL accepts it."}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00 +05:30' AS a

-- case: {"id": "lit_offset_16", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Offsets beyond 15 hours are outside the forms checked in both references."}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+16' AS a

-- case: {"id": "lit_seven_digits", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL rounds a seventh fractional digit, DuckDB truncates."}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00.1234567+00' AS a

-- case: {"id": "lit_unknown_zone", "zone": "UTC", "ordered": true, "assay_outcome": "error", "assay_error": "time zone"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00 Mars/Olympus' AS a

-- case: {"id": "lit_bad_day", "zone": "UTC", "ordered": true, "assay_outcome": "error", "assay_error": "date/time field value out of range"}
SELECT TIMESTAMPTZ '2024-02-30 06:30:00+00' AS a

-- case: {"id": "lmt_display", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Local mean time offset +05:21:10: DuckDB prints +05:21, PostgreSQL +05:21:10."}
SELECT t FROM lmt

-- case: {"id": "lmt_utc", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT t, EXTRACT(HOUR FROM t) AS h FROM lmt

-- case: {"id": "lmt_extract", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS m, CAST(t AS TIMESTAMP) AS w FROM lmt

-- case: {"id": "parquet_values_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_at FROM events WHERE user_id = 3 ORDER BY event_at, amount

-- case: {"id": "parquet_values_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_at FROM events WHERE user_id = 3 ORDER BY event_at, amount

-- case: {"id": "parquet_values_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_at FROM events WHERE user_id = 3 ORDER BY event_at, amount

-- case: {"id": "parquet_units_agree", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT (SELECT COUNT(*) FROM events a JOIN events_ns b ON a.event_at = b.event_at AND a.user_id = b.user_id) AS ns_matches, (SELECT COUNT(DISTINCT event_at) FROM events_ms) AS ms_distinct, (SELECT MAX(event_at) FROM events_ns) AS ns_max

-- case: {"id": "ns_fraction", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: column 't': TIMESTAMP_NS value is not a whole microsecond", "difference": "DuckDB keeps TIMESTAMP_NS; no common microsecond reading.", "pg_skip": true}
SELECT COUNT(t) AS n FROM ns_fraction

-- case: {"id": "cast_edge_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT id, t, CAST(t AS TIMESTAMP) AS wall, CAST(t AS DATE) AS day, CAST(t AS VARCHAR) AS text FROM edge ORDER BY id

-- case: {"id": "cast_edge_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, t, CAST(t AS TIMESTAMP) AS wall, CAST(t AS DATE) AS day, CAST(t AS VARCHAR) AS text FROM edge ORDER BY id

-- case: {"id": "cast_edge_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT id, t, CAST(t AS TIMESTAMP) AS wall, CAST(t AS DATE) AS day, CAST(t AS VARCHAR) AS text FROM edge ORDER BY id

-- case: {"id": "cast_edge_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, t, CAST(t AS TIMESTAMP) AS wall, CAST(t AS DATE) AS day, CAST(t AS VARCHAR) AS text FROM edge ORDER BY id

-- case: {"id": "cast_edge_America/Sao_Paulo", "zone": "America/Sao_Paulo", "ordered": true, "assay_outcome": "rows"}
SELECT id, t, CAST(t AS TIMESTAMP) AS wall, CAST(t AS DATE) AS day, CAST(t AS VARCHAR) AS text FROM edge ORDER BY id

-- case: {"id": "cast_walls_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(w AS TIMESTAMPTZ) AS t, w AT TIME ZONE 'America/New_York' AS ny FROM walls ORDER BY id

-- case: {"id": "cast_walls_Australia/Lord_Howe", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(w AS TIMESTAMPTZ) AS t, w AT TIME ZONE 'America/New_York' AS ny FROM walls ORDER BY id

-- case: {"id": "cast_walls_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(w AS TIMESTAMPTZ) AS t, w AT TIME ZONE 'America/New_York' AS ny FROM walls ORDER BY id

-- case: {"id": "cast_date_midnight_gap", "zone": "America/Sao_Paulo", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(DATE '2018-11-04' AS TIMESTAMPTZ) AS gap, CAST(DATE '2018-11-05' AS TIMESTAMPTZ) AS after

-- case: {"id": "cast_text_column", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, CAST(s AS TIMESTAMPTZ) AS t FROM texts ORDER BY id

-- case: {"id": "concat_text", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT 'at ' || t AS a, CONCAT('t=', t) AS b FROM edge WHERE id = 1

-- case: {"id": "compare_mixed_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT id, t = TIMESTAMP '2024-03-10 07:00:00' AS ts, t >= DATE '2024-03-10' AS d, t < '2024-03-10 03:00:00' AS lit, t = TIMESTAMPTZ '2024-03-10 03:00:00-04' AS tz FROM edge WHERE id IN (0, 1, 7) ORDER BY id

-- case: {"id": "compare_mixed_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, t = TIMESTAMP '2024-03-10 07:00:00' AS ts, t >= DATE '2024-03-10' AS d, t < '2024-03-10 03:00:00' AS lit, t = TIMESTAMPTZ '2024-03-10 03:00:00-04' AS tz FROM edge WHERE id IN (0, 1, 7) ORDER BY id

-- case: {"id": "between_in", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM edge WHERE t BETWEEN '2024-03-10' AND TIMESTAMP '2024-11-03 01:45:00' AND t NOT IN ('2024-11-03 01:30:00-04') ORDER BY id

-- case: {"id": "compare_text_column", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Only string literals convert implicitly; PostgreSQL has no timestamptz = text operator."}
SELECT edge.id FROM edge, texts WHERE t = s

-- case: {"id": "case_mixed", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Both references widen TIMESTAMP to TIMESTAMPTZ in CASE; Assay refuses mixed results."}
SELECT CASE WHEN id = 0 THEN t ELSE TIMESTAMP '2024-01-01 00:00:00' END AS x FROM edge WHERE id = 0

-- case: {"id": "extract_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS y, EXTRACT(MONTH FROM t) AS mo, EXTRACT(DAY FROM t) AS d, EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS mi, EXTRACT(DOW FROM t) AS dow, EXTRACT(DOY FROM t) AS doy, EXTRACT(WEEK FROM t) AS w, EXTRACT(MICROSECOND FROM t) AS us, EXTRACT(EPOCH FROM t) AS e, EXTRACT(TIMEZONE FROM t) AS tz, EXTRACT(TIMEZONE_HOUR FROM t) AS tzh, EXTRACT(TIMEZONE_MINUTE FROM t) AS tzm FROM edge ORDER BY id

-- case: {"id": "extract_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS y, EXTRACT(MONTH FROM t) AS mo, EXTRACT(DAY FROM t) AS d, EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS mi, EXTRACT(DOW FROM t) AS dow, EXTRACT(DOY FROM t) AS doy, EXTRACT(WEEK FROM t) AS w, EXTRACT(MICROSECOND FROM t) AS us, EXTRACT(EPOCH FROM t) AS e, EXTRACT(TIMEZONE FROM t) AS tz, EXTRACT(TIMEZONE_HOUR FROM t) AS tzh, EXTRACT(TIMEZONE_MINUTE FROM t) AS tzm FROM edge ORDER BY id

-- case: {"id": "extract_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS y, EXTRACT(MONTH FROM t) AS mo, EXTRACT(DAY FROM t) AS d, EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS mi, EXTRACT(DOW FROM t) AS dow, EXTRACT(DOY FROM t) AS doy, EXTRACT(WEEK FROM t) AS w, EXTRACT(MICROSECOND FROM t) AS us, EXTRACT(EPOCH FROM t) AS e, EXTRACT(TIMEZONE FROM t) AS tz, EXTRACT(TIMEZONE_HOUR FROM t) AS tzh, EXTRACT(TIMEZONE_MINUTE FROM t) AS tzm FROM edge ORDER BY id

-- case: {"id": "extract_Australia/Lord_Howe", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS y, EXTRACT(MONTH FROM t) AS mo, EXTRACT(DAY FROM t) AS d, EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS mi, EXTRACT(DOW FROM t) AS dow, EXTRACT(DOY FROM t) AS doy, EXTRACT(WEEK FROM t) AS w, EXTRACT(MICROSECOND FROM t) AS us, EXTRACT(EPOCH FROM t) AS e, EXTRACT(TIMEZONE FROM t) AS tz, EXTRACT(TIMEZONE_HOUR FROM t) AS tzh, EXTRACT(TIMEZONE_MINUTE FROM t) AS tzm FROM edge ORDER BY id

-- case: {"id": "extract_America/St_Johns", "zone": "America/St_Johns", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM t) AS y, EXTRACT(MONTH FROM t) AS mo, EXTRACT(DAY FROM t) AS d, EXTRACT(HOUR FROM t) AS h, EXTRACT(MINUTE FROM t) AS mi, EXTRACT(DOW FROM t) AS dow, EXTRACT(DOY FROM t) AS doy, EXTRACT(WEEK FROM t) AS w, EXTRACT(MICROSECOND FROM t) AS us, EXTRACT(EPOCH FROM t) AS e, EXTRACT(TIMEZONE FROM t) AS tz, EXTRACT(TIMEZONE_HOUR FROM t) AS tzh, EXTRACT(TIMEZONE_MINUTE FROM t) AS tzm FROM edge ORDER BY id

-- case: {"id": "extract_second_fraction", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB 59 (BIGINT), PostgreSQL 59.500000 (NUMERIC)."}
SELECT EXTRACT(SECOND FROM t) AS s FROM edge WHERE id = 0

-- case: {"id": "date_part_forms", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_PART('hour', t) AS h, DATE_PART('epoch', t) AS e, DATE_PART('isodow', t) AS i FROM edge WHERE id = 2

-- case: {"id": "trunc_microsecond_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('microsecond', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_microsecond_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('microsecond', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_millisecond_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('millisecond', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_millisecond_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('millisecond', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_second_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('second', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_second_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('second', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_minute_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('minute', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_minute_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('minute', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_day_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('day', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_day_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('day', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_week_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('week', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_week_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('week', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_month_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('month', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_month_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('month', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_quarter_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('quarter', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_quarter_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('quarter', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_year_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('year', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_year_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('year', t) AS x FROM edge WHERE id <> 6 AND id <> 2 AND id <> 3 ORDER BY id

-- case: {"id": "trunc_hour_ny", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('hour', t) AS x FROM edge WHERE id IN (0, 1, 4, 5, 7, 8, 9, 10) ORDER BY id

-- case: {"id": "trunc_hour_kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('hour', t) AS x FROM edge ORDER BY id

-- case: {"id": "trunc_minute_repeated_hour", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('minute', t) AS m, DATE_TRUNC('day', t) AS d FROM edge WHERE id IN (2, 3) ORDER BY id

-- case: {"id": "trunc_hour_repeated", "zone": "America/New_York", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Truncating 01:30 EDT (first) to the repeated 01:00: DuckDB gives 01:00-05, PostgreSQL 01:00-04."}
SELECT DATE_TRUNC('hour', t) AS x FROM edge WHERE id = 2

-- case: {"id": "trunc_day_midnight_gap", "zone": "America/Sao_Paulo", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Truncation onto a skipped wall clock: both give 01:00-02 here, but they differ at Lord Howe (next case), so every skipped result refuses."}
SELECT DATE_TRUNC('day', t) AS x FROM edge WHERE id = 4

-- case: {"id": "cast_date_sao_paulo", "zone": "America/Sao_Paulo", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(t AS DATE) AS d, CAST(t AS TIMESTAMP) AS w FROM edge WHERE id = 4

-- case: {"id": "trunc_lord_howe_day", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "rows"}
SELECT id, DATE_TRUNC('day', t) AS d FROM edge WHERE id = 5

-- case: {"id": "trunc_lord_howe_hour_gap", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Truncating 02:30+11 to the skipped 02:00: DuckDB gives 02:30+11, PostgreSQL 01:30+10:30."}
SELECT DATE_TRUNC('hour', t) AS h FROM edge WHERE id = 5

-- case: {"id": "add_land_in_gap_lord_howe", "zone": "Australia/Lord_Howe", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-10-05 02:15:00+10:30' + INTERVAL '1' DAY AS a, TIMESTAMPTZ '2024-09-06 02:15:00+10:30' + INTERVAL '1' MONTH AS b

-- case: {"id": "add_land_in_gap_sao_paulo", "zone": "America/Sao_Paulo", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2018-11-03 00:30:00-03' + INTERVAL '1' DAY AS a

-- case: {"id": "trunc_century", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB 2000, PostgreSQL 2001."}
SELECT DATE_TRUNC('century', t) AS x FROM edge WHERE id = 7

-- case: {"id": "add_exact_across_dst", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 01:30:00-05' + INTERVAL '1' HOUR AS a, TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '24' HOUR AS b, TIMESTAMPTZ '2024-11-03 01:30:00-04' + INTERVAL '60' MINUTE AS c, TIMESTAMPTZ '2024-11-03 01:30:00-05' - INTERVAL '3600' SECOND AS d

-- case: {"id": "add_calendar_day", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '1' DAY AS a, TIMESTAMPTZ '2024-11-02 12:00:00-04' + INTERVAL '1' DAY AS b, TIMESTAMPTZ '2024-03-11 12:00:00-04' - INTERVAL '2' DAY AS c

-- case: {"id": "add_calendar_month", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-01-31 12:00:00-05' + INTERVAL '1' MONTH AS a, TIMESTAMPTZ '2024-03-10 01:30:00-05' - INTERVAL '1' MONTH AS b, TIMESTAMPTZ '2024-02-29 08:00:00-05' + INTERVAL '1' YEAR AS c

-- case: {"id": "add_land_in_gap", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-09 02:30:00-05' + INTERVAL '1' DAY AS a, TIMESTAMPTZ '2024-02-10 02:30:00-05' + INTERVAL '1' MONTH AS b

-- case: {"id": "add_land_in_overlap", "zone": "America/New_York", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Landing on the repeated 01:30: DuckDB keeps -04, PostgreSQL picks -05."}
SELECT TIMESTAMPTZ '2024-11-02 01:30:00-04' + INTERVAL '1' DAY AS a

-- case: {"id": "add_utc_days", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT t + INTERVAL '1' DAY AS a, t - INTERVAL '1' MONTH AS b FROM edge WHERE id IN (8, 9) ORDER BY id

-- case: {"id": "add_week", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL parses WEEK as a column alias (one second); DuckDB adds 7 days.", "reference_sql": "SELECT TIMESTAMPTZ '2024-01-31 10:00:00+00' + INTERVAL '1' WEEK AS x"}
SELECT TIMESTAMPTZ '2024-01-31 10:00:00+00' + INTERVAL '1' WEEK AS x

-- case: {"id": "tstz_minus_tstz", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "INTERVAL result; Assay has no INTERVAL value type.", "reference_sql": "SELECT CAST(TIMESTAMPTZ '2024-03-10 06:30:00+00' - TIMESTAMPTZ '2024-03-09 06:30:00+00' AS VARCHAR) AS x", "pg_sql": "SELECT (TIMESTAMPTZ '2024-03-10 06:30:00+00' - TIMESTAMPTZ '2024-03-09 06:30:00+00')::text AS x"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+00' - TIMESTAMPTZ '2024-03-09 06:30:00+00' AS x

-- case: {"id": "tstz_plus_int", "zone": "UTC", "ordered": true, "assay_outcome": "error", "assay_error": "expected numeric type"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+00' + 1 AS x

-- case: {"id": "at_time_zone", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TIMESTAMPTZ '2024-03-10 06:30:00+00' AT TIME ZONE 'Asia/Kolkata' AS a, TIMESTAMP '2024-03-10 06:30:00' AT TIME ZONE 'Asia/Kolkata' AS b, TIMESTAMP '2024-03-10 02:30:00' AT TIME ZONE 'America/New_York' AS c, TIMESTAMP '2024-11-03 01:30:00' AT TIME ZONE 'America/New_York' AS d, TIMESTAMPTZ '2024-03-10 06:30:00+00' AT TIME ZONE 'utc' AS e

-- case: {"id": "at_time_zone_session", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT t AT TIME ZONE 'Australia/Lord_Howe' AS lh, CAST(t AT TIME ZONE 'UTC' AS TIMESTAMPTZ) AS back FROM edge WHERE id IN (5, 6) ORDER BY id

-- case: {"id": "at_time_zone_offset", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB rejects '+05'; PostgreSQL reads it as POSIX UTC-5 (inverted sign)."}
SELECT TIMESTAMP '2024-03-10 06:30:00' AT TIME ZONE '+05' AS x

-- case: {"id": "to_timestamp_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TO_TIMESTAMP(1709289015) AS a, TO_TIMESTAMP(1709289015.5) AS b, TO_TIMESTAMP(1709289015.1234567) AS c, TO_TIMESTAMP(-1.5) AS d, TO_TIMESTAMP(0) AS e, TO_TIMESTAMP(CAST(1709289015.25 AS DECIMAL(18,3))) AS f, TO_TIMESTAMP(NULL) AS g

-- case: {"id": "to_timestamp_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT TO_TIMESTAMP(1709289015) AS a, TO_TIMESTAMP(1709289015.5) AS b, TO_TIMESTAMP(1709289015.1234567) AS c, TO_TIMESTAMP(-1.5) AS d, TO_TIMESTAMP(0) AS e, TO_TIMESTAMP(CAST(1709289015.25 AS DECIMAL(18,3))) AS f, TO_TIMESTAMP(NULL) AS g

-- case: {"id": "to_timestamp_near_ties", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT TO_TIMESTAMP(0.0000025) AS b, TO_TIMESTAMP(-0.0000005) AS c, TO_TIMESTAMP(1709289015.1234565) AS d

-- case: {"id": "to_timestamp_rounding_differs", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "DuckDB rounds 1.0000005 s to 1.000001; PostgreSQL subtracts its 2000 epoch first in binary64 and gets 1.000000."}
SELECT TO_TIMESTAMP(1.0000005) AS a

-- case: {"id": "to_timestamp_nan", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Both references error; Assay's existing cast policy already refuses CAST('NaN' AS DOUBLE) (TO_TIMESTAMP of a NaN value errors: unit test).", "pg_sql": "SELECT TO_TIMESTAMP(CAST('NaN' AS DOUBLE PRECISION)) AS x"}
SELECT TO_TIMESTAMP(CAST('NaN' AS DOUBLE)) AS x

-- case: {"id": "to_timestamp_infinity", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "PostgreSQL returns infinity, DuckDB errors.", "pg_sql": "SELECT TO_TIMESTAMP(CAST('Infinity' AS DOUBLE PRECISION))::text AS x"}
SELECT TO_TIMESTAMP(CAST('Infinity' AS DOUBLE)) AS x

-- case: {"id": "to_timestamp_huge", "zone": "UTC", "ordered": true, "assay_outcome": "error", "assay_error": "timestamp out of range"}
SELECT TO_TIMESTAMP(100000000000000000000) AS x

-- case: {"id": "to_timestamp_text", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "The text/format form is PostgreSQL's; DuckDB has no two-argument TO_TIMESTAMP."}
SELECT TO_TIMESTAMP('2024-03-10', 'YYYY-MM-DD') AS x

-- case: {"id": "clock_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT NOW() = CURRENT_TIMESTAMP AS a, CAST(NOW() AS DATE) = CURRENT_DATE AS b, NOW() IS NOT NULL AS c

-- case: {"id": "clock_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT NOW() = CURRENT_TIMESTAMP AS a, CAST(NOW() AS DATE) = CURRENT_DATE AS b, NOW() IS NOT NULL AS c

-- case: {"id": "clock_Asia/Kolkata", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT NOW() = CURRENT_TIMESTAMP AS a, CAST(NOW() AS DATE) = CURRENT_DATE AS b, NOW() IS NOT NULL AS c

-- case: {"id": "daily_active_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(event_at AS DATE) AS day, COUNT(DISTINCT user_id) AS users, COUNT(*) AS events, SUM(amount) AS amount FROM events WHERE event_at IS NOT NULL GROUP BY 1 ORDER BY 1

-- case: {"id": "hourly_UTC", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('hour', event_at) AS hour, COUNT(*) AS events FROM events WHERE event_at BETWEEN TIMESTAMPTZ '2024-03-10 04:00:00+00' AND TIMESTAMPTZ '2024-03-10 12:00:00+00' GROUP BY 1 ORDER BY 1

-- case: {"id": "daily_active_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(event_at AS DATE) AS day, COUNT(DISTINCT user_id) AS users, COUNT(*) AS events, SUM(amount) AS amount FROM events WHERE event_at IS NOT NULL GROUP BY 1 ORDER BY 1

-- case: {"id": "hourly_America/Los_Angeles", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('hour', event_at) AS hour, COUNT(*) AS events FROM events WHERE event_at BETWEEN TIMESTAMPTZ '2024-03-10 04:00:00+00' AND TIMESTAMPTZ '2024-03-10 12:00:00+00' GROUP BY 1 ORDER BY 1

-- case: {"id": "daily_active_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(event_at AS DATE) AS day, COUNT(DISTINCT user_id) AS users, COUNT(*) AS events, SUM(amount) AS amount FROM events WHERE event_at IS NOT NULL GROUP BY 1 ORDER BY 1

-- case: {"id": "hourly_America/New_York", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('hour', event_at) AS hour, COUNT(*) AS events FROM events WHERE event_at BETWEEN TIMESTAMPTZ '2024-03-10 04:00:00+00' AND TIMESTAMPTZ '2024-03-10 12:00:00+00' GROUP BY 1 ORDER BY 1

-- case: {"id": "hourly_fallback_utc", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('hour', event_at) AS hour, COUNT(*) AS events FROM events WHERE event_at BETWEEN TIMESTAMPTZ '2024-11-03 03:00:00+00' AND TIMESTAMPTZ '2024-11-03 09:00:00+00' GROUP BY 1 ORDER BY 1

-- case: {"id": "hourly_fallback_ny", "zone": "America/New_York", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Grouping rows from the repeated 01:00 hour by DATE_TRUNC('hour'): DuckDB and PostgreSQL assign different offsets to the truncated hour."}
SELECT DATE_TRUNC('hour', event_at) AS hour, COUNT(*) AS events FROM events WHERE event_at BETWEEN TIMESTAMPTZ '2024-11-03 03:00:00+00' AND TIMESTAMPTZ '2024-11-03 09:00:00+00' GROUP BY 1 ORDER BY 1

-- case: {"id": "first_last_per_user", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows", "pg_abs_tolerance": 1e-06}
SELECT user_id, MIN(event_at) AS first_at, MAX(event_at) AS last_at, EXTRACT(EPOCH FROM MAX(event_at)) - EXTRACT(EPOCH FROM MIN(event_at)) AS span, COUNT(event_at) AS n FROM events GROUP BY user_id ORDER BY user_id NULLS LAST

-- case: {"id": "group_distinct_order", "zone": "Asia/Kolkata", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(DISTINCT event_at) AS a, COUNT(event_at) AS b, MIN(event_at) AS c, MAX(event_at) AS d FROM events

-- case: {"id": "order_desc", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_at FROM events ORDER BY event_at DESC NULLS LAST, user_id LIMIT 8

-- case: {"id": "join_units", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT a.user_id AS user_id, COUNT(*) AS n FROM events a JOIN events_ms b ON a.event_at = b.event_at GROUP BY a.user_id ORDER BY a.user_id NULLS LAST

-- case: {"id": "in_subquery_union", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT (SELECT COUNT(*) FROM events WHERE event_at IN (SELECT event_at FROM events_ns)) AS a, (SELECT COUNT(*) FROM (SELECT event_at FROM events UNION SELECT event_at FROM events_ms) u) AS b

-- case: {"id": "window_lag_range", "zone": "America/Los_Angeles", "ordered": true, "assay_outcome": "rows"}
SELECT user_id, event_at, LAG(event_at) OVER (PARTITION BY user_id ORDER BY event_at, amount) AS prev, COUNT(*) OVER (PARTITION BY user_id ORDER BY event_at RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) AS recent FROM events WHERE user_id IN (2, 5) ORDER BY user_id, event_at, amount

-- case: {"id": "window_range_day", "zone": "UTC", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "A DAY offset is a calendar day in the session zone (23/25 hours across DST); Assay admits exact HOUR/MINUTE/SECOND offsets only."}
SELECT COUNT(*) OVER (ORDER BY event_at RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS n FROM events

-- case: {"id": "empty_input", "zone": "America/New_York", "ordered": true, "assay_outcome": "rows"}
SELECT MIN(t) AS a, COUNT(t) AS b, MAX(DATE_TRUNC('day', t)) AS c FROM empty_tz

-- case: {"id": "is_null_coalesce", "zone": "UTC", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n, MAX(COALESCE(t, TIMESTAMPTZ '1999-01-01 00:00:00+00')) AS m FROM edge WHERE t IS NULL

