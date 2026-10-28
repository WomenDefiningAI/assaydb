-- case: {"id": "trunc_year_1900-03-01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('year',DATE '1900-03-01') AS x;

-- case: {"id": "trunc_year_2000-02-29", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('year',DATE '2000-02-29') AS x;

-- case: {"id": "trunc_year_2016-01-03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('year',DATE '2016-01-03') AS x;

-- case: {"id": "trunc_quarter_1900-03-01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('quarter',DATE '1900-03-01') AS x;

-- case: {"id": "trunc_quarter_2000-02-29", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('quarter',DATE '2000-02-29') AS x;

-- case: {"id": "trunc_quarter_2016-01-03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('quarter',DATE '2016-01-03') AS x;

-- case: {"id": "trunc_month_1900-03-01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',DATE '1900-03-01') AS x;

-- case: {"id": "trunc_month_2000-02-29", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',DATE '2000-02-29') AS x;

-- case: {"id": "trunc_month_2016-01-03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',DATE '2016-01-03') AS x;

-- case: {"id": "trunc_week_1900-03-01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('week',DATE '1900-03-01') AS x;

-- case: {"id": "trunc_week_2000-02-29", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('week',DATE '2000-02-29') AS x;

-- case: {"id": "trunc_week_2016-01-03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('week',DATE '2016-01-03') AS x;

-- case: {"id": "trunc_day_1900-03-01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('day',DATE '1900-03-01') AS x;

-- case: {"id": "trunc_day_2000-02-29", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('day',DATE '2000-02-29') AS x;

-- case: {"id": "trunc_day_2016-01-03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('day',DATE '2016-01-03') AS x;

-- case: {"id": "interval_00", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1900-01-31' + INTERVAL '1' MONTH AS x;

-- case: {"id": "interval_01", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-01-31' + INTERVAL '1' MONTH AS x;

-- case: {"id": "interval_02", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-03-31' - INTERVAL '1' MONTH AS x;

-- case: {"id": "interval_03", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-02-29' + INTERVAL '1' YEAR AS x;

-- case: {"id": "interval_04", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-02-29' + INTERVAL '4' YEAR AS x;

-- case: {"id": "interval_05", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-02-29' - INTERVAL '100' YEAR AS x;

-- case: {"id": "interval_06", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-02-29' + INTERVAL '-1' YEAR AS x;

-- case: {"id": "interval_07", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1900-03-01' - INTERVAL '1' DAY AS x;

-- case: {"id": "interval_08", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-03-01' - INTERVAL '1' DAY AS x;

-- case: {"id": "interval_09", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1969-12-31' + INTERVAL '1' DAY AS x;

-- case: {"id": "interval_10", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Correction (Oct 17): live PostgreSQL 17.11 parses WEEK here as a column alias of INTERVAL '1' (one second): SELECT DATE '2024-01-03' + INTERVAL '1' WEEK returns column \"week\" = 2024-01-03 00:00:01, and with AS x it is a syntax error; DuckDB adds 7 days. References disagree, so Assay refuses."}
SELECT DATE '2024-01-03' + INTERVAL '1' WEEK AS x;

-- case: {"id": "interval_11", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Correction (Oct 17): live PostgreSQL 17.11 parses WEEK here as a column alias of INTERVAL '1' (one second): SELECT DATE '2024-01-03' + INTERVAL '1' WEEK returns column \"week\" = 2024-01-03 00:00:01, and with AS x it is a syntax error; DuckDB adds 7 days. References disagree, so Assay refuses."}
SELECT DATE '2024-01-03' - INTERVAL '-1' WEEK AS x;

-- case: {"id": "interval_12", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-01-31' + INTERVAL '13' MONTH AS x;

-- case: {"id": "interval_13", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-03-31' + INTERVAL '-13' MONTH AS x;

-- case: {"id": "interval_14", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '9999-01-31' + INTERVAL '1' MONTH AS x;

-- case: {"id": "interval_15", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '0001-01-01' + INTERVAL '0' YEAR AS x;

-- case: {"id": "interval_16", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-02-29' - INTERVAL '-12' MONTH AS x;

-- case: {"id": "add", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1900-02-28'+1 AS x;

-- case: {"id": "reverse_add", "ordered": true, "assay_outcome": "rows"}
SELECT 1+DATE '2000-02-28' AS x;

-- case: {"id": "subtract", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-03-01'-1 AS x;

-- case: {"id": "negative", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1970-01-01'+ -1 AS x;

-- case: {"id": "double_negative", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '1969-12-31'- -1 AS x;

-- case: {"id": "difference", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2000-03-01'-DATE '1900-03-01' AS x;

-- case: {"id": "negative_difference", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '0001-01-01'-DATE '9999-12-31' AS x;

-- case: {"id": "null_add", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-01-01'+NULL AS x;

-- case: {"id": "null_subtract", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '2024-01-01'-NULL AS x;

-- case: {"id": "null_trunc", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',CAST(NULL AS DATE)) AS x;

-- case: {"id": "null_interval", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(NULL AS DATE)+INTERVAL '1' MONTH AS x;

-- case: {"id": "null_difference", "ordered": true, "assay_outcome": "rows"}
SELECT CAST(NULL AS DATE)-DATE '2024-01-01' AS x;

-- case: {"id": "nested", "ordered": true, "assay_outcome": "rows", "reference_sql": "SELECT CAST(DATE_TRUNC('month',CAST(DATE '2024-01-31'+INTERVAL '1' MONTH AS DATE)) AS DATE)+1 AS x;", "difference": "Explicit reference DATE casts implement Helen\u2019s DATE output decision at each nested boundary."}
SELECT DATE_TRUNC('month',DATE '2024-01-31'+INTERVAL '1' MONTH)+1 AS x;

-- case: {"id": "iso_year", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('week',DATE '2014-12-31') AS x;

-- case: {"id": "far_future", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('quarter',DATE '9999-12-31') AS x;

-- case: {"id": "date_part", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_PART('isoyear',DATE '2016-01-03') AS x;

-- case: {"id": "columns", "ordered": true, "assay_outcome": "rows"}
SELECT id,d+CAST(n AS INTEGER) AS plus,d-CAST(n AS INTEGER) AS minus,d-DATE '1970-01-01' AS days FROM dates ORDER BY id;

-- case: {"id": "trunc_columns", "ordered": true, "assay_outcome": "rows"}
SELECT id,DATE_TRUNC('year',d) AS y,DATE_TRUNC('quarter',d) AS q,DATE_TRUNC('month',d) AS m,DATE_TRUNC('week',d) AS w,DATE_TRUNC('day',d) AS d FROM dates ORDER BY id;

-- case: {"id": "monthly_revenue", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',d) AS month,SUM(amount) AS revenue FROM dates GROUP BY DATE_TRUNC('month',d) ORDER BY month NULLS LAST;

-- case: {"id": "weekly_counts", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('week',d) AS week,COUNT(*) AS n FROM dates GROUP BY DATE_TRUNC('week',d) ORDER BY week NULLS LAST;

-- case: {"id": "days_since", "ordered": true, "assay_outcome": "rows"}
SELECT DATE '9999-12-31'-MAX(d) AS days FROM dates;

-- case: {"id": "window", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM dates WHERE d>=DATE '2024-03-31'-INTERVAL '90' DAY AND d<=DATE '2024-03-31' ORDER BY id;

-- case: {"id": "empty", "ordered": true, "assay_outcome": "rows"}
SELECT DATE_TRUNC('month',d) AS x,d+CAST(n AS INTEGER) AS y FROM empty_dates;

-- case: {"id": "case", "ordered": true, "assay_outcome": "rows"}
SELECT id,CASE WHEN d IS NULL THEN DATE '2000-01-01' ELSE DATE_TRUNC('month',d) END AS x FROM dates ORDER BY id;

-- case: {"id": "month_column", "ordered": true, "assay_outcome": "rows"}
SELECT id,d+INTERVAL '1' MONTH AS next,d-INTERVAL '1' YEAR AS prev FROM dates ORDER BY id;

-- case: {"id": "hour", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE_TRUNC('hour',DATE '2024-01-01') AS x;

-- case: {"id": "minute", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE_TRUNC('minute',DATE '2024-01-01') AS x;

-- case: {"id": "interval_hour", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+INTERVAL '1' HOUR AS x;

-- case: {"id": "unquoted", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+INTERVAL 1 DAY AS x;

-- case: {"id": "fractional", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+INTERVAL '1.5' MONTH AS x;

-- case: {"id": "output_interval", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT INTERVAL '1' DAY AS x;

-- case: {"id": "current_date", "ordered": true, "assay_outcome": "rows"}
SELECT CURRENT_DATE IS NOT NULL AS x;

-- case: {"id": "now", "ordered": true, "assay_outcome": "rows"}
SELECT NOW() IS NOT NULL AS x;

-- case: {"id": "current_timestamp", "ordered": true, "assay_outcome": "rows"}
SELECT CURRENT_TIMESTAMP IS NOT NULL AS x;

-- case: {"id": "datediff", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATEDIFF('day',DATE '2024-01-01',DATE '2024-02-01') AS x;

-- case: {"id": "date_diff", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE_DIFF('day',DATE '2024-01-01',DATE '2024-02-01') AS x;

-- case: {"id": "year_alias", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT YEAR(DATE '2024-01-01') AS x;

-- case: {"id": "month_alias", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT MONTH(DATE '2024-01-01') AS x;

-- case: {"id": "day_alias", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DAY(DATE '2024-01-01') AS x;

-- case: {"id": "overflow_interval", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+INTERVAL '2147483647' YEAR AS x;

-- case: {"id": "overflow_date", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+2147483647 AS x;

-- case: {"id": "wide_offset", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '2024-01-01'+2147483648 AS x;

-- case: {"id": "bc", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE '0001-01-01'-1 AS x;

-- case: {"id": "computed_unit", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE_TRUNC(LOWER('month'),DATE '2024-01-01') AS x;

-- case: {"id": "trunc_text", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Explicit DATE-only common-domain boundary; see the PostgreSQL agreement and range notes in README.md."}
SELECT DATE_TRUNC('month',CAST('2024-01-01' AS VARCHAR)) AS x;
