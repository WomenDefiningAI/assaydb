-- case: {"id": "double_cast", "ordered": false}
SELECT CAST(9007199254740993 AS DOUBLE) AS v;

-- case: {"id": "double_colon", "ordered": false}
SELECT (-9007199254740995)::DOUBLE AS v;

-- case: {"id": "double_null", "ordered": false}
SELECT CAST(NULL AS DOUBLE) AS v;

-- case: {"id": "double_decimal", "ordered": false}
SELECT CAST(12345678901234567890.123456789012345678 AS DOUBLE) AS v;

-- case: {"id": "double_precision_cast", "ordered": false}
SELECT CAST(9007199254740993 AS DOUBLE PRECISION) AS v;

-- case: {"id": "double_precision_colon", "ordered": false}
SELECT (-9007199254740995)::DOUBLE PRECISION AS v;

-- case: {"id": "double_precision_null", "ordered": false}
SELECT CAST(NULL AS DOUBLE PRECISION) AS v;

-- case: {"id": "double_precision_decimal", "ordered": false}
SELECT CAST(12345678901234567890.123456789012345678 AS DOUBLE PRECISION) AS v;

-- case: {"id": "float8_cast", "ordered": false}
SELECT CAST(9007199254740993 AS FLOAT8) AS v;

-- case: {"id": "float8_colon", "ordered": false}
SELECT (-9007199254740995)::FLOAT8 AS v;

-- case: {"id": "float8_null", "ordered": false}
SELECT CAST(NULL AS FLOAT8) AS v;

-- case: {"id": "float8_decimal", "ordered": false}
SELECT CAST(12345678901234567890.123456789012345678 AS FLOAT8) AS v;

-- case: {"id": "float_cast", "ordered": false, "assay_error": "unsupported: CAST AS FLOAT precision differs between references", "difference": "DuckDB 1.5.6 uses binary32 for bare FLOAT, unlike the card's binary64 premise and PostgreSQL. Assay refuses the ambiguous target pending a precision decision."}
SELECT CAST(9007199254740993 AS FLOAT) AS v;

-- case: {"id": "float_colon", "ordered": false, "assay_error": "unsupported: CAST AS FLOAT precision differs between references", "difference": "DuckDB 1.5.6 uses binary32 for bare FLOAT, unlike the card's binary64 premise and PostgreSQL. Assay refuses the ambiguous target pending a precision decision."}
SELECT (-9007199254740995)::FLOAT AS v;

-- case: {"id": "float_null", "ordered": false, "assay_error": "unsupported: CAST AS FLOAT precision differs between references", "difference": "DuckDB 1.5.6 uses binary32 for bare FLOAT, unlike the card's binary64 premise and PostgreSQL. Assay refuses the ambiguous target pending a precision decision."}
SELECT CAST(NULL AS FLOAT) AS v;

-- case: {"id": "float_decimal", "ordered": false, "assay_error": "unsupported: CAST AS FLOAT precision differs between references", "difference": "DuckDB 1.5.6 uses binary32 for bare FLOAT, unlike the card's binary64 premise and PostgreSQL. Assay refuses the ambiguous target pending a precision decision."}
SELECT CAST(12345678901234567890.123456789012345678 AS FLOAT) AS v;

-- case: {"id": "text_integer", "ordered": false}
SELECT CAST(-9223372036854775808 AS TEXT) AS v;

-- case: {"id": "text_decimal", "ordered": false}
SELECT CAST(1.50 AS TEXT) AS p, CAST(-0.05 AS TEXT) AS n, CAST(0.00 AS TEXT) AS z;

-- case: {"id": "text_date", "ordered": false}
SELECT CAST(DATE '2024-02-29' AS TEXT) AS v;

-- case: {"id": "text_null_empty", "ordered": false}
SELECT CAST(NULL AS TEXT) AS n, CAST('' AS TEXT) AS e;

-- case: {"id": "varchar_integer", "ordered": false}
SELECT CAST(-9223372036854775808 AS VARCHAR) AS v;

-- case: {"id": "varchar_decimal", "ordered": false}
SELECT CAST(1.50 AS VARCHAR) AS p, CAST(-0.05 AS VARCHAR) AS n, CAST(0.00 AS VARCHAR) AS z;

-- case: {"id": "varchar_date", "ordered": false}
SELECT CAST(DATE '2024-02-29' AS VARCHAR) AS v;

-- case: {"id": "varchar_null_empty", "ordered": false}
SELECT CAST(NULL AS VARCHAR) AS n, CAST('' AS VARCHAR) AS e;

-- case: {"id": "string_integer", "ordered": false}
SELECT CAST(-9223372036854775808 AS STRING) AS v;

-- case: {"id": "string_decimal", "ordered": false}
SELECT CAST(1.50 AS STRING) AS p, CAST(-0.05 AS STRING) AS n, CAST(0.00 AS STRING) AS z;

-- case: {"id": "string_date", "ordered": false}
SELECT CAST(DATE '2024-02-29' AS STRING) AS v;

-- case: {"id": "string_null_empty", "ordered": false}
SELECT CAST(NULL AS STRING) AS n, CAST('' AS STRING) AS e;

-- case: {"id": "character_varying_integer", "ordered": false}
SELECT CAST(-9223372036854775808 AS CHARACTER VARYING) AS v;

-- case: {"id": "character_varying_decimal", "ordered": false}
SELECT CAST(1.50 AS CHARACTER VARYING) AS p, CAST(-0.05 AS CHARACTER VARYING) AS n, CAST(0.00 AS CHARACTER VARYING) AS z;

-- case: {"id": "character_varying_date", "ordered": false}
SELECT CAST(DATE '2024-02-29' AS CHARACTER VARYING) AS v;

-- case: {"id": "character_varying_null_empty", "ordered": false}
SELECT CAST(NULL AS CHARACTER VARYING) AS n, CAST('' AS CHARACTER VARYING) AS e;

-- case: {"id": "char_varying_integer", "ordered": false}
SELECT CAST(-9223372036854775808 AS CHAR VARYING) AS v;

-- case: {"id": "char_varying_decimal", "ordered": false}
SELECT CAST(1.50 AS CHAR VARYING) AS p, CAST(-0.05 AS CHAR VARYING) AS n, CAST(0.00 AS CHAR VARYING) AS z;

-- case: {"id": "char_varying_date", "ordered": false}
SELECT CAST(DATE '2024-02-29' AS CHAR VARYING) AS v;

-- case: {"id": "char_varying_null_empty", "ordered": false}
SELECT CAST(NULL AS CHAR VARYING) AS n, CAST('' AS CHAR VARYING) AS e;

-- case: {"id": "double_identity", "ordered": false}
SELECT CAST(CAST(1.25 AS DOUBLE) AS FLOAT8) AS v;

-- case: {"id": "negative_zero_identity", "ordered": false, "assay_error": "unsupported: CAST AS FLOAT precision differs between references", "difference": "DuckDB 1.5.6 uses binary32 for bare FLOAT, unlike the card's binary64 premise and PostgreSQL. Assay refuses the ambiguous target pending a precision decision."}
SELECT CAST(-CAST(0 AS DOUBLE) AS FLOAT) AS v;

-- case: {"id": "varchar_unicode", "ordered": false}
SELECT CAST('é😀' AS VARCHAR(2)) AS v;

-- case: {"id": "varying_bound", "ordered": false}
SELECT CAST('é😀' AS CHARACTER VARYING(2)) AS a, CAST('é😀' AS CHAR VARYING(2)) AS b;

-- case: {"id": "varchar_empty_null", "ordered": false}
SELECT CAST('' AS VARCHAR(1)) AS e, CAST(NULL AS VARCHAR(1)) AS n;

-- case: {"id": "varchar_decimal_fits", "ordered": false}
SELECT CAST(-1.50 AS VARCHAR(5)) AS v;

-- case: {"id": "varchar_date_fits", "ordered": false}
SELECT CAST(DATE '0001-01-01' AS VARCHAR(10)) AS v;

-- case: {"id": "column_aliases_ordered", "ordered": true}
SELECT id, CAST(d AS DOUBLE) AS amount, CAST(d AS VARCHAR) AS label FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s ORDER BY id;

-- case: {"id": "duplicate_bag", "ordered": false}
SELECT CAST(d AS VARCHAR) AS v FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s;

-- case: {"id": "empty_input", "ordered": false}
SELECT CAST(d AS DOUBLE) AS amount, CAST(d AS VARCHAR(1)) AS label FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s WHERE id < 0;

-- case: {"id": "grouped_labels", "ordered": true}
SELECT CAST(d AS VARCHAR(5)) AS label, COUNT(*) AS n FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s GROUP BY d ORDER BY label NULLS LAST;

-- case: {"id": "invoice_total", "ordered": false}
SELECT CAST(SUM(d) AS VARCHAR) AS label, CAST(SUM(d) AS DOUBLE) AS amount FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s;

-- case: {"id": "repeated_aggregate", "ordered": false}
SELECT MIN(CAST(d AS VARCHAR(5))) AS lo, MAX(CAST(d AS VARCHAR(5))) AS hi FROM (SELECT 1 AS id, 1.50 AS d UNION ALL SELECT 2, -1.50 UNION ALL SELECT 3, 1.50 UNION ALL SELECT 4, NULL) s;

-- case: {"id": "reached_rows", "ordered": false}
SELECT CAST(s AS VARCHAR(1)) AS v FROM (SELECT 1 AS id, 'a' AS s UNION ALL SELECT 2, 'long') t WHERE id = 1;

-- case: {"id": "lazy_case", "ordered": false}
SELECT CASE WHEN TRUE THEN 'ok' ELSE CAST('long' AS VARCHAR(1)) END AS v;

-- case: {"id": "lazy_coalesce", "ordered": false}
SELECT COALESCE('ok', CAST('long' AS VARCHAR(1))) AS v;

-- case: {"id": "double_rounding_pair", "ordered": false}
SELECT CAST(9007199254740993 AS DOUBLE) AS even_down, CAST(9007199254740995 AS DOUBLE) AS even_up;

-- case: {"id": "varchar_too_long", "ordered": false, "assay_error": "unsupported: VARCHAR(3)", "difference": "DuckDB ignores the length; PostgreSQL truncates. Assay refuses the overlength result."}
SELECT CAST('abcd' AS VARCHAR(3)) AS v;

-- case: {"id": "varchar_trailing_space", "ordered": false, "assay_error": "unsupported: VARCHAR(1)", "difference": "No trimming or truncation of trailing spaces is authorized."}
SELECT CAST('a ' AS VARCHAR(1)) AS v;

-- case: {"id": "text_double_refusal", "ordered": false, "assay_error": "unsupported: DOUBLE/FLOAT cast requires a numeric source", "difference": "Text-to-DOUBLE parsing is deferred; REAL keeps its SQLite prefix parser."}
SELECT CAST('1.25' AS DOUBLE) AS v;

-- case: {"id": "invalid_text_double_refusal", "ordered": false, "assay_error": "unsupported: DOUBLE/FLOAT cast requires a numeric source", "reference_error": true, "difference": "DuckDB conversion error; Assay refuses all text-to-DOUBLE sources at binding."}
SELECT CAST('1.25tail' AS DOUBLE) AS v;

-- case: {"id": "float_text_refusal", "ordered": false, "superseded_refusal": "unsupported: CAST Float AS Text (until Oct 16; DuckDB shortest round-trip text approved)"}
SELECT CAST(CAST(1.25 AS DOUBLE) AS VARCHAR) AS v;

-- case: {"id": "bool_text_legacy", "ordered": false, "assay_rows": [["1", "0"]], "difference": "Aliases retain the existing TEXT boolean 1/0 format. DuckDB/PostgreSQL use true/false; changing it awaits Helen."}
SELECT CAST(TRUE AS VARCHAR) AS t, CAST(FALSE AS VARCHAR) AS f;

-- case: {"id": "bare_decimal_refusal", "ordered": false}
SELECT CAST(1.50 AS DECIMAL) AS v;

-- case: {"id": "float_decimal_refusal", "ordered": false}
SELECT CAST(CAST(1.25 AS DOUBLE) AS DECIMAL(6,2)) AS v;

-- case: {"id": "combined_numeric_0", "ordered": false}
SELECT CAST(1 AS DECIMAL);

-- case: {"id": "combined_numeric_1", "ordered": false}
SELECT CAST(1 AS NUMERIC);

-- case: {"id": "combined_numeric_2", "ordered": false}
SELECT CAST(CAST(1 AS DOUBLE) AS DECIMAL(5,2));

-- case: {"id": "combined_double_0_avg_0", "ordered": false}
SELECT CAST(AVG(CAST(l_discount AS DOUBLE)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_0_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT CAST(l_discount AS DOUBLE)) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_0_avg_2", "ordered": false}
SELECT AVG(CAST(l_discount AS DOUBLE)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_0_avg_3", "ordered": false}
SELECT AVG(CAST(l_discount AS DOUBLE)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_0_sum_0", "ordered": false}
SELECT CAST(SUM(CAST(l_discount AS DOUBLE)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_0_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT CAST(l_discount AS DOUBLE)) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_0_sum_2", "ordered": false}
SELECT SUM(CAST(l_discount AS DOUBLE)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_0_sum_3", "ordered": false}
SELECT SUM(CAST(l_discount AS DOUBLE)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_1_avg_0", "ordered": false}
SELECT CAST(AVG(((l_discount::DOUBLE))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_1_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT ((l_discount::DOUBLE))) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_1_avg_2", "ordered": false}
SELECT AVG(((l_discount::DOUBLE))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_1_avg_3", "ordered": false}
SELECT AVG(((l_discount::DOUBLE))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_1_sum_0", "ordered": false}
SELECT CAST(SUM(((l_discount::DOUBLE))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_1_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT ((l_discount::DOUBLE))) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_1_sum_2", "ordered": false}
SELECT SUM(((l_discount::DOUBLE))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_1_sum_3", "ordered": false}
SELECT SUM(((l_discount::DOUBLE))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_precision_0_avg_0", "ordered": false}
SELECT CAST(AVG(CAST(l_discount AS DOUBLE PRECISION)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_precision_0_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT CAST(l_discount AS DOUBLE PRECISION)) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_precision_0_avg_2", "ordered": false}
SELECT AVG(CAST(l_discount AS DOUBLE PRECISION)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_precision_0_avg_3", "ordered": false}
SELECT AVG(CAST(l_discount AS DOUBLE PRECISION)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_precision_0_sum_0", "ordered": false}
SELECT CAST(SUM(CAST(l_discount AS DOUBLE PRECISION)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_precision_0_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT CAST(l_discount AS DOUBLE PRECISION)) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_precision_0_sum_2", "ordered": false}
SELECT SUM(CAST(l_discount AS DOUBLE PRECISION)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_precision_0_sum_3", "ordered": false}
SELECT SUM(CAST(l_discount AS DOUBLE PRECISION)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_precision_1_avg_0", "ordered": false}
SELECT CAST(AVG(((l_discount::DOUBLE PRECISION))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_precision_1_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT ((l_discount::DOUBLE PRECISION))) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_precision_1_avg_2", "ordered": false}
SELECT AVG(((l_discount::DOUBLE PRECISION))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_precision_1_avg_3", "ordered": false}
SELECT AVG(((l_discount::DOUBLE PRECISION))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_double_precision_1_sum_0", "ordered": false}
SELECT CAST(SUM(((l_discount::DOUBLE PRECISION))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_double_precision_1_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT ((l_discount::DOUBLE PRECISION))) FROM discounts GROUP BY k;

-- case: {"id": "combined_double_precision_1_sum_2", "ordered": false}
SELECT SUM(((l_discount::DOUBLE PRECISION))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_double_precision_1_sum_3", "ordered": false}
SELECT SUM(((l_discount::DOUBLE PRECISION))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_float8_0_avg_0", "ordered": false}
SELECT CAST(AVG(CAST(l_discount AS FLOAT8)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_float8_0_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT CAST(l_discount AS FLOAT8)) FROM discounts GROUP BY k;

-- case: {"id": "combined_float8_0_avg_2", "ordered": false}
SELECT AVG(CAST(l_discount AS FLOAT8)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_float8_0_avg_3", "ordered": false}
SELECT AVG(CAST(l_discount AS FLOAT8)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_float8_0_sum_0", "ordered": false}
SELECT CAST(SUM(CAST(l_discount AS FLOAT8)) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_float8_0_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT CAST(l_discount AS FLOAT8)) FROM discounts GROUP BY k;

-- case: {"id": "combined_float8_0_sum_2", "ordered": false}
SELECT SUM(CAST(l_discount AS FLOAT8)) FROM discounts WHERE k < 0;

-- case: {"id": "combined_float8_0_sum_3", "ordered": false}
SELECT SUM(CAST(l_discount AS FLOAT8)) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_float8_1_avg_0", "ordered": false}
SELECT CAST(AVG(((l_discount::FLOAT8))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_float8_1_avg_1", "ordered": false}
SELECT k,AVG(DISTINCT ((l_discount::FLOAT8))) FROM discounts GROUP BY k;

-- case: {"id": "combined_float8_1_avg_2", "ordered": false}
SELECT AVG(((l_discount::FLOAT8))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_float8_1_avg_3", "ordered": false}
SELECT AVG(((l_discount::FLOAT8))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_float8_1_sum_0", "ordered": false}
SELECT CAST(SUM(((l_discount::FLOAT8))) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "combined_float8_1_sum_1", "ordered": false}
SELECT k,SUM(DISTINCT ((l_discount::FLOAT8))) FROM discounts GROUP BY k;

-- case: {"id": "combined_float8_1_sum_2", "ordered": false}
SELECT SUM(((l_discount::FLOAT8))) FROM discounts WHERE k < 0;

-- case: {"id": "combined_float8_1_sum_3", "ordered": false}
SELECT SUM(((l_discount::FLOAT8))) FROM discounts WHERE l_discount IS NULL;

-- case: {"id": "combined_exact_avg_cast", "ordered": false}
SELECT CAST(AVG(l_discount) AS DECIMAL(10,4)) FROM discounts;

-- case: {"id": "lab_double_0_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS DOUBLE)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_0_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS DOUBLE)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_0_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS DOUBLE)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_0_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS DOUBLE)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_1_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::DOUBLE))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_1_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::DOUBLE))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_1_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::DOUBLE))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_1_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::DOUBLE))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_0_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS DOUBLE PRECISION)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_0_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS DOUBLE PRECISION)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_0_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS DOUBLE PRECISION)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_0_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS DOUBLE PRECISION)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_1_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::DOUBLE PRECISION))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_1_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::DOUBLE PRECISION))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_1_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::DOUBLE PRECISION))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_double_precision_1_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::DOUBLE PRECISION))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_0_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS FLOAT8)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_0_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(CAST(r.reading AS FLOAT8)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_0_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS FLOAT8)) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_0_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(CAST(r.reading AS FLOAT8)) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_1_avg_4", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::FLOAT8))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_1_avg_6", "ordered": true}
SELECT g.label AS reading_group, CAST(AVG(((r.reading::FLOAT8))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_1_sum_4", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::FLOAT8))) AS DECIMAL(10,4)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

-- case: {"id": "lab_float8_1_sum_6", "ordered": true}
SELECT g.label AS reading_group, CAST(SUM(((r.reading::FLOAT8))) AS DECIMAL(18,6)) AS calibrated_value FROM readings r JOIN instruments i ON r.instrument_id = i.instrument_id JOIN lab_groups g ON i.group_id = g.group_id GROUP BY g.label ORDER BY reading_group;

