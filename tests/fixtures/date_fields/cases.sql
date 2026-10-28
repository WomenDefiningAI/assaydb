-- case: {"id": "d001", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(YEAR FROM d) AS extracted, date_part('year',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d002", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(QUARTER FROM d) AS extracted, date_part('quarter',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d003", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(MONTH FROM d) AS extracted, date_part('month',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d004", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DAY FROM d) AS extracted, date_part('day',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d005", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DOW FROM d) AS extracted, date_part('dow',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d006", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(ISODOW FROM d) AS extracted, date_part('isodow',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d007", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(DOY FROM d) AS extracted, date_part('doy',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d008", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(WEEK FROM d) AS extracted, date_part('week',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d009", "ordered": true, "assay_outcome": "rows"}
SELECT id, EXTRACT(ISOYEAR FROM d) AS extracted, date_part('isoyear',d) AS function_value FROM dates ORDER BY id;

-- case: {"id": "d010", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(YEAR FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(YEAR FROM CAST(NULL AS DATE)) AS typed_null, date_part('year',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d011", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(QUARTER FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(QUARTER FROM CAST(NULL AS DATE)) AS typed_null, date_part('quarter',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d012", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(MONTH FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(MONTH FROM CAST(NULL AS DATE)) AS typed_null, date_part('month',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d013", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(DAY FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(DAY FROM CAST(NULL AS DATE)) AS typed_null, date_part('day',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d014", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(DOW FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(DOW FROM CAST(NULL AS DATE)) AS typed_null, date_part('dow',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d015", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(ISODOW FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(ISODOW FROM CAST(NULL AS DATE)) AS typed_null, date_part('isodow',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d016", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(DOY FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(DOY FROM CAST(NULL AS DATE)) AS typed_null, date_part('doy',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d017", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(WEEK FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(WEEK FROM CAST(NULL AS DATE)) AS typed_null, date_part('week',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d018", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(ISOYEAR FROM CAST(NULL AS DATE)) AS null_value, EXTRACT(ISOYEAR FROM CAST(NULL AS DATE)) AS typed_null, date_part('isoyear',CAST(NULL AS DATE)) AS function_null;

-- case: {"id": "d019", "ordered": true, "assay_outcome": "rows"}
SELECT id, d = '2016-01-01' AS forward, '2016-01-01' = d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d020", "ordered": true, "assay_outcome": "rows"}
SELECT id, d <> '2016-01-01' AS forward, '2016-01-01' <> d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d021", "ordered": true, "assay_outcome": "rows"}
SELECT id, d < '2016-01-01' AS forward, '2016-01-01' < d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d022", "ordered": true, "assay_outcome": "rows"}
SELECT id, d <= '2016-01-01' AS forward, '2016-01-01' <= d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d023", "ordered": true, "assay_outcome": "rows"}
SELECT id, d > '2016-01-01' AS forward, '2016-01-01' > d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d024", "ordered": true, "assay_outcome": "rows"}
SELECT id, d >= '2016-01-01' AS forward, '2016-01-01' >= d AS reversed FROM dates ORDER BY id;

-- case: {"id": "d025", "ordered": true, "assay_outcome": "rows"}
SELECT id, d BETWEEN '2015-12-29' AND '2016-01-03' AS inside, d NOT BETWEEN '2015-12-29' AND '2016-01-03' AS outside FROM dates ORDER BY id;

-- case: {"id": "d026", "ordered": true, "assay_outcome": "rows"}
SELECT id, d IN ('2024-02-29','2024-02-29','2016-01-01',NULL) AS inside, d NOT IN ('2024-02-29','2024-02-29','2016-01-01',NULL) AS outside FROM dates ORDER BY id;

-- case: {"id": "d027", "ordered": true, "assay_outcome": "rows"}
SELECT id, '2016-01-01' BETWEEN d AND '2025-01-01' AS inside FROM dates ORDER BY id;

-- case: {"id": "d028", "ordered": true, "assay_outcome": "rows"}
SELECT id, '2016-01-01' IN ('2015-01-01',d,NULL) AS inside FROM dates ORDER BY id;

-- case: {"id": "d029", "ordered": true, "assay_outcome": "rows"}
SELECT id, '2016-01-01' NOT IN ('2015-01-01',d,NULL) AS outside FROM dates ORDER BY id;

-- case: {"id": "d030", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(YEAR FROM d) AS y, EXTRACT(MONTH FROM d) AS m, region, SUM(amount) AS revenue FROM dates WHERE d >= '2015-01-01' AND d < '2025-01-01' GROUP BY EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), region ORDER BY y,m,region;

-- case: {"id": "d031", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(ISOYEAR FROM d) AS y, EXTRACT(WEEK FROM d) AS w, COUNT(*) AS orders FROM dates GROUP BY EXTRACT(ISOYEAR FROM d),EXTRACT(WEEK FROM d) ORDER BY y NULLS LAST,w NULLS LAST;

-- case: {"id": "d032", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(QUARTER FROM d) AS q, SUM(amount) AS revenue FROM dates GROUP BY EXTRACT(QUARTER FROM d) HAVING SUM(amount)>20 ORDER BY q NULLS LAST;

-- case: {"id": "d033", "ordered": false, "assay_outcome": "rows"}
SELECT EXTRACT(MONTH FROM d) AS m FROM dates;

-- case: {"id": "d034", "ordered": true, "assay_outcome": "rows"}
SELECT DISTINCT EXTRACT(MONTH FROM d) AS m FROM dates ORDER BY m NULLS LAST;

-- case: {"id": "d035", "ordered": true, "assay_outcome": "rows"}
SELECT a.id AS aid,b.id AS bid FROM dates a JOIN dates b ON a.d=b.d WHERE a.d='2024-02-29' ORDER BY aid,bid;

-- case: {"id": "d036", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(MONTH FROM d) AS m FROM empty_dates;

-- case: {"id": "d037", "ordered": true, "assay_outcome": "rows"}
SELECT COUNT(*) AS n, SUM(EXTRACT(DAY FROM d)) AS days FROM empty_dates;

-- case: {"id": "d038", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(MONTH FROM d) AS m, COUNT(*) AS n FROM empty_dates GROUP BY EXTRACT(MONTH FROM d);

-- case: {"id": "d039", "ordered": true, "assay_outcome": "rows"}
SELECT d='2024-02-29' AS same FROM empty_dates;

-- case: {"id": "d040", "ordered": true, "assay_outcome": "rows"}
SELECT CAST('2024-01-01' AS DATE) = '2024-01-01' AS same, '2024-01-01' = CAST('2024-01-01' AS DATE) AS reversed;

-- case: {"id": "d041", "ordered": false, "assay_outcome": "rows"}
SELECT EXTRACT(MONTH FROM CASE WHEN id<17 THEN d ELSE CAST(NULL AS DATE) END) AS m FROM dates;

-- case: {"id": "d042", "ordered": false, "assay_outcome": "rows"}
SELECT date_part('MoNtH',d) AS m, EXTRACT(DAY FROM COALESCE(d,DATE '2000-02-29')) AS day FROM dates;

-- case: {"id": "d043", "ordered": true, "assay_outcome": "rows"}
SELECT EXTRACT(YEAR FROM DATE '0001-01-01') AS y, EXTRACT(DOW FROM DATE '0001-01-01') AS dow, EXTRACT(ISOYEAR FROM DATE '9999-12-31') AS iso;

-- case: {"id": "d044", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal"}
SELECT DATE '2024-01-01'='2024-02-30' AS bad;

-- case: {"id": "d045", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal"}
SELECT DATE '2024-01-01'='2023-02-29' AS bad;

-- case: {"id": "d046", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal"}
SELECT DATE '2024-01-01'='1900-02-29' AS bad;

-- case: {"id": "d047", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal"}
SELECT DATE '2024-01-01'='2024-13-01' AS bad;

-- case: {"id": "d048", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal"}
SELECT DATE '2024-01-01'='abc' AS bad;

-- case: {"id": "d049", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "The card explicitly requires exact YYYY-MM-DD with a nonzero four-digit year; reference parsers may accept a broader date grammar."}
SELECT DATE '2024-01-01'='2024-1-5' AS bad;

-- case: {"id": "d050", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "The card explicitly requires exact YYYY-MM-DD with a nonzero four-digit year; reference parsers may accept a broader date grammar."}
SELECT DATE '2024-01-01'='2024-01-5' AS bad;

-- case: {"id": "d051", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "The card explicitly requires exact YYYY-MM-DD with a nonzero four-digit year; reference parsers may accept a broader date grammar."}
SELECT DATE '2024-01-01'='2024-01-01 ' AS bad;

-- case: {"id": "d052", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "The card explicitly requires exact YYYY-MM-DD with a nonzero four-digit year; reference parsers may accept a broader date grammar."}
SELECT DATE '2024-01-01'='0000-01-01' AS bad;

-- case: {"id": "d053", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: EXTRACT/DATE_PART requires DATE input"}
SELECT EXTRACT(MONTH FROM 1) AS m;

-- case: {"id": "d054", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: EXTRACT/DATE_PART requires DATE input"}
SELECT EXTRACT(MONTH FROM CAST('1.25' AS DECIMAL(10,2))) AS m;

-- case: {"id": "d055", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: EXTRACT/DATE_PART requires DATE input"}
SELECT EXTRACT(MONTH FROM CAST('2024-01-01' AS TEXT)) AS m;

-- case: {"id": "d056", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT EXTRACT(EPOCH FROM d) AS x FROM dates;

-- case: {"id": "d057", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT EXTRACT(HOUR FROM d) AS x FROM dates;

-- case: {"id": "d058", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT date_part('hour',d) AS x FROM dates;

-- case: {"id": "d059", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT DATE_TRUNC('month',d) AS x FROM dates;

-- case: {"id": "d060", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT year(d) AS x FROM dates;

-- case: {"id": "d061", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT month(d) AS x FROM dates;

-- case: {"id": "d062", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT day(d) AS x FROM dates;

-- case: {"id": "d063", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT d=t AS x FROM dates;

-- case: {"id": "d064", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported:", "difference": "Intentionally outside the card: non-calendar fields/DATE_TRUNC, PostgreSQL-missing aliases, typed TEXT comparison or nonliteral DATE_PART field."}
SELECT date_part(t,d) AS x FROM dates;

-- case: {"id": "d065", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "Assay must validate literals at bind time even on empty input; reference execution may defer the conversion until a row exists."}
SELECT d IN ('2024-01-01','2024-02-30') AS bad FROM empty_dates;

-- case: {"id": "d066", "ordered": true, "assay_outcome": "error", "assay_error": "invalid DATE literal", "difference": "Assay must validate literals at bind time even on empty input; reference execution may defer the conversion until a row exists."}
SELECT 'abc' BETWEEN d AND '2024-01-01' AS bad FROM empty_dates;

-- case: {"id": "d067", "ordered": true, "assay_outcome": "unsupported", "assay_error": "unsupported: ISOYEAR before AD 1", "difference": "BC ISO year numbering needs a DuckDB/PostgreSQL agreement decision; the existing EXTRACT YEAR path is unchanged."}
SELECT EXTRACT(ISOYEAR FROM DATE '0000-01-01') AS y;

