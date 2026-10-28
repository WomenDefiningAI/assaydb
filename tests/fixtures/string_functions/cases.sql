-- case: {"id": "trim_spaces", "ordered": true, "pg": "agree"}
SELECT id, TRIM(t) AS r FROM s ORDER BY id;

-- case: {"id": "ltrim_rtrim_spaces", "ordered": true, "pg": "agree"}
SELECT id, LTRIM(t) AS l, RTRIM(t) AS r FROM s WHERE id <= 5 ORDER BY id;

-- case: {"id": "trim_chars_function", "ordered": true, "pg": "agree (TRIM(s, chars) is btrim)"}
SELECT id, TRIM(t, 'x') AS r FROM s WHERE id IN (2, 3, 11) ORDER BY id;

-- case: {"id": "trim_both_from", "ordered": true, "pg": "agree"}
SELECT id, TRIM(BOTH 'x' FROM t) AS r FROM s WHERE id IN (0, 3, 11) ORDER BY id;

-- case: {"id": "trim_leading_trailing_from", "ordered": true, "pg": "agree"}
SELECT id, TRIM(LEADING 'x' FROM t) AS l, TRIM(TRAILING 'x' FROM t) AS r FROM s WHERE id IN (2, 11) ORDER BY id;

-- case: {"id": "ltrim_rtrim_char_set", "ordered": true, "pg": "agree"}
SELECT LTRIM('xyxhixy', 'yx') AS l, RTRIM('xyxhixy', 'yx') AS r, TRIM('xyxhixy', 'yx') AS b;

-- case: {"id": "trim_non_ascii_chars", "ordered": true, "pg": "agree"}
SELECT TRIM('ééaé', 'é') AS a, TRIM('aéa', 'a') AS b, LTRIM('é é', ' é') AS c;

-- case: {"id": "trim_combining_mark", "ordered": true, "pg": "agree (both trim code points)"}
SELECT TRIM('éaée', 'e') AS r;

-- case: {"id": "trim_null_and_empty_set", "ordered": true, "pg": "agree"}
SELECT TRIM('x', NULL) AS a, TRIM(NULL) AS b, TRIM('abc', '') AS c, TRIM('   ') AS d, LTRIM('') AS e;

-- case: {"id": "trim_tab_kept", "ordered": true, "pg": "agree (neither trims tab)"}
SELECT TRIM('	a b	') AS r;

-- case: {"id": "trim_nbsp_refused", "ordered": true, "pg": "differs: PostgreSQL trims only U+0020 and returns ' a'", "assay_error": "unsupported: TRIM of non-ASCII space characters", "difference": "DuckDB also trims Unicode space separators; PostgreSQL trims only U+0020."}
SELECT TRIM('  a') AS r;

-- case: {"id": "rtrim_ideographic_space_refused", "ordered": true, "pg": "differs: PostgreSQL returns 'a　'", "assay_error": "unsupported: TRIM of non-ASCII space characters", "difference": "DuckDB trims U+3000; PostgreSQL does not."}
SELECT RTRIM('a　') AS r;

-- case: {"id": "replace_table", "ordered": true, "pg": "agree"}
SELECT id, REPLACE(t, 'l', 'L') AS r FROM s ORDER BY id;

-- case: {"id": "replace_edge_cases", "ordered": true, "pg": "agree"}
SELECT REPLACE('abc', '', 'x') AS empty_from, REPLACE('aaa', 'aa', 'b') AS overlap, REPLACE('abcabc', 'bc', '') AS deleted, REPLACE('a', 'a', 'ééé') AS grows;

-- case: {"id": "replace_non_ascii", "ordered": true, "pg": "agree"}
SELECT id, REPLACE(t, 'ß', 'ss') AS r FROM s WHERE id IN (4, 9) ORDER BY id;

-- case: {"id": "replace_nulls", "ordered": true, "pg": "agree"}
SELECT REPLACE(NULL, 'a', 'b') AS a, REPLACE('a', NULL, 'b') AS b, REPLACE('a', 'a', NULL) AS c;

-- case: {"id": "concat_operator_table", "ordered": true, "pg": "agree"}
SELECT id, t || '!' AS r FROM s ORDER BY id;

-- case: {"id": "concat_operator_chain_null", "ordered": true, "pg": "agree", "null_result_type": "DuckDB's Python API reports its untyped NULL result columns as INTEGER; Assay types them TEXT. Values are compared exactly."}
SELECT 'a' || 'b' || NULL || 'c' AS r, NULL || NULL AS n;

-- case: {"id": "concat_operator_numbers", "ordered": true, "pg": "agree"}
SELECT id, 'id=' || id || ', d=' || d AS r FROM s ORDER BY id;

-- case: {"id": "concat_operator_date", "ordered": true, "pg": "agree"}
SELECT 'on ' || DATE '2024-02-29' AS r;

-- case: {"id": "concat_operator_double", "ordered": true, "pg": "differs for integer-valued doubles: PostgreSQL prints 2 where DuckDB prints 2.0 (DuckDB followed, Oct 16 decision)"}
SELECT id, 'x=' || CAST(d AS DOUBLE) AS r FROM s WHERE id IN (0, 1, 2, 3, 5, 11) ORDER BY id;

-- case: {"id": "concat_operator_bool_refused", "ordered": true, "pg": "agree with DuckDB ('atrue'); SQLite corpus semantics print 1", "assay_error": "unsupported: BOOLEAN in string concatenation", "difference": "Assay's existing BOOLEAN text is SQLite 1/0; concatenation of BOOLEAN stays refused."}
SELECT 'a' || TRUE AS r;

-- case: {"id": "concat_function_skips_nulls", "ordered": true, "pg": "agree"}
SELECT id, CONCAT(t, '-', NULL, id) AS r FROM s ORDER BY id;

-- case: {"id": "concat_function_all_null", "ordered": true, "pg": "agree"}
SELECT CONCAT(NULL, NULL) AS a, CONCAT('x') AS b, CONCAT(1, 2) AS c;

-- case: {"id": "concat_function_double", "ordered": true, "pg": "agree"}
SELECT CONCAT('v', CAST(2.5 AS DOUBLE), '/', CAST(0.1 AS DOUBLE)) AS r;

-- case: {"id": "concat_function_bool_refused", "ordered": true, "pg": "differs: PostgreSQL CONCAT prints 'at'", "assay_error": "unsupported: BOOLEAN in string concatenation", "difference": "DuckDB prints true, PostgreSQL CONCAT prints t."}
SELECT CONCAT('a', TRUE) AS r;

-- case: {"id": "concat_function_no_arguments", "ordered": true, "pg": "agree (error)", "assay_error": "CONCAT requires at least one argument", "difference": "Both references reject an empty argument list."}
SELECT CONCAT() AS r;

-- case: {"id": "left_right_table", "ordered": true, "pg": "agree"}
SELECT id, LEFT(t, 3) AS l, RIGHT(t, 3) AS r FROM s ORDER BY id;

-- case: {"id": "left_right_boundaries", "ordered": true, "pg": "agree"}
SELECT LEFT('abcde', -2) AS a, RIGHT('abcde', -2) AS b, LEFT('abcde', -10) AS c, RIGHT('abcde', -10) AS d, LEFT('abcde', 0) AS e, LEFT('abcde', 5) AS f, RIGHT('abcde', 6) AS g;

-- case: {"id": "left_right_column_count", "ordered": true, "pg": "agree"}
SELECT id, LEFT(t, k) AS l, RIGHT(t, k) AS r FROM s ORDER BY id;

-- case: {"id": "left_combining_mark", "ordered": true, "pg": "agree (both count code points)"}
SELECT LEFT('éx', 1) AS a, RIGHT('xé', 1) AS b;

-- case: {"id": "left_right_nulls", "ordered": true, "pg": "agree"}
SELECT LEFT(NULL, 1) AS a, LEFT('ab', NULL) AS b, RIGHT(NULL, NULL) AS c;

-- case: {"id": "left_huge_count_refused", "ordered": true, "pg": "differs: PostgreSQL LEFT takes integer and rejects the count", "assay_error": "unsupported: LEFT/RIGHT count outside the 32-bit range", "difference": "DuckDB accepts BIGINT counts; PostgreSQL takes int4."}
SELECT LEFT('abc', 3000000000) AS r;

-- case: {"id": "starts_with_table", "ordered": true, "pg": "agree"}
SELECT id, STARTS_WITH(t, 'he') AS a, STARTS_WITH(t, 'Ст') AS b, STARTS_WITH(t, 'É') AS c FROM s ORDER BY id;

-- case: {"id": "starts_with_edges", "ordered": true, "pg": "agree"}
SELECT STARTS_WITH('abc', '') AS a, STARTS_WITH('', '') AS b, STARTS_WITH(NULL, 'a') AS c, STARTS_WITH('a', 'ab') AS d;

-- case: {"id": "contains_table", "ordered": true, "pg": "no CONTAINS function in PostgreSQL; equivalent strpos(t, x) > 0 agrees"}
SELECT id, CONTAINS(t, 'ß') AS a, CONTAINS(t, 'ix') AS b, CONTAINS(t, '') AS c FROM s ORDER BY id;

-- case: {"id": "contains_untyped_null_refused", "ordered": true, "pg": "no CONTAINS function in PostgreSQL", "assay_error": "unsupported: CONTAINS with an untyped NULL argument", "difference": "DuckDB cannot choose between text, list and map overloads."}
SELECT CONTAINS('abc', NULL) AS r;

-- case: {"id": "strpos_table", "ordered": true, "pg": "agree"}
SELECT id, STRPOS(t, 'l') AS a, STRPOS(t, 'ı') AS b, STRPOS(t, '🙂') AS c FROM s ORDER BY id;

-- case: {"id": "strpos_edges", "ordered": true, "pg": "agree"}
SELECT STRPOS('abc', '') AS a, STRPOS('', '') AS b, STRPOS('', 'a') AS c, STRPOS('aéb', 'b') AS d, STRPOS(NULL, 'a') AS e, STRPOS('a', NULL) AS f;

-- case: {"id": "position_non_ascii", "ordered": true, "pg": "agree"}
SELECT id, POSITION('i' IN t) AS p FROM s ORDER BY id;

-- case: {"id": "strpos_filter", "ordered": true, "pg": "agree"}
SELECT id FROM s WHERE STRPOS(t, 'o') > 0 ORDER BY id;

-- case: {"id": "split_part_table", "ordered": true, "pg": "agree"}
SELECT id, SPLIT_PART(t, ',', 1) AS a, SPLIT_PART(t, ',', 3) AS b, SPLIT_PART(t, ' ', -1) AS c FROM s ORDER BY id;

-- case: {"id": "split_part_boundaries", "ordered": true, "pg": "agree (PostgreSQL 14+ negative fields)"}
SELECT SPLIT_PART('a,b,c', ',', 3) AS a, SPLIT_PART('a,b,c', ',', 4) AS b, SPLIT_PART('a,b,c', ',', -1) AS c, SPLIT_PART('a,b,c', ',', -3) AS d, SPLIT_PART('a,b,c', ',', -4) AS e;

-- case: {"id": "split_part_empty_fields", "ordered": true, "pg": "agree"}
SELECT SPLIT_PART('', ',', 1) AS a, SPLIT_PART(',a,', ',', 1) AS b, SPLIT_PART(',a,', ',', 3) AS c, SPLIT_PART('a', ',', 1) AS d, SPLIT_PART('a', ',', 2) AS e;

-- case: {"id": "split_part_multichar_delimiter", "ordered": true, "pg": "agree (fields are found left to right before negative indexing)"}
SELECT SPLIT_PART('aaa', 'aa', 2) AS a, SPLIT_PART('a||b', '||', 2) AS b, SPLIT_PART('ä—ö—ü', '—', -2) AS c, SPLIT_PART('aaa', 'aa', -1) AS d, SPLIT_PART('aaaa', 'aa', -2) AS e;

-- case: {"id": "split_part_nulls", "ordered": true, "pg": "agree"}
SELECT SPLIT_PART(NULL, ',', 1) AS a, SPLIT_PART('a', NULL, 1) AS b, SPLIT_PART('a', ',', NULL) AS c;

-- case: {"id": "split_part_column_index", "ordered": true, "pg": "agree"}
SELECT id, SPLIT_PART(t, 'l', k) AS r FROM s WHERE k <> 0 ORDER BY id;

-- case: {"id": "split_part_zero_refused", "ordered": true, "pg": "differs: PostgreSQL raises 'field position must not be zero'", "assay_error": "unsupported: SPLIT_PART field 0", "difference": "DuckDB returns an empty string; PostgreSQL errors."}
SELECT SPLIT_PART('a,b', ',', 0) AS r;

-- case: {"id": "split_part_empty_delimiter_refused", "ordered": true, "pg": "differs: PostgreSQL returns ''", "assay_error": "unsupported: SPLIT_PART with an empty delimiter", "difference": "DuckDB splits into characters ('b'); PostgreSQL treats the whole string as one field."}
SELECT SPLIT_PART('abc', '', 2) AS r;

-- case: {"id": "lpad_rpad_basic", "ordered": true, "pg": "agree"}
SELECT LPAD('abc', 5, 'xy') AS a, LPAD('abc', 6, 'xy') AS b, RPAD('abc', 6, 'xy') AS c, LPAD('abc', 2, 'xy') AS d, RPAD('abc', 2, 'xy') AS e, LPAD('abc', 3, 'xy') AS f;

-- case: {"id": "lpad_rpad_nonpositive", "ordered": true, "pg": "agree"}
SELECT LPAD('abc', 0, 'x') AS a, LPAD('abc', -1, 'x') AS b, RPAD('abc', -5, 'x') AS c;

-- case: {"id": "lpad_rpad_unicode", "ordered": true, "pg": "agree"}
SELECT LPAD('é', 3, 'ü') AS a, RPAD('日本', 5, '語') AS b, LPAD('🙂', 2, 'ab') AS c;

-- case: {"id": "lpad_zero_fill_ids", "ordered": true, "pg": "agree"}
SELECT id, LPAD(CAST(id AS TEXT), 4, '0') AS code, RPAD(t, 12, '.') AS dotted FROM s ORDER BY id;

-- case: {"id": "lpad_empty_fill_truncates", "ordered": true, "pg": "agree"}
SELECT LPAD('abc', 2, '') AS a, RPAD('abc', 3, '') AS b;

-- case: {"id": "lpad_empty_fill_refused", "ordered": true, "pg": "differs: PostgreSQL returns 'abc'", "assay_error": "unsupported: LPAD/RPAD with an empty fill", "difference": "DuckDB raises 'Insufficient padding'; PostgreSQL returns the input unpadded."}
SELECT LPAD('abc', 5, '') AS r;

-- case: {"id": "lpad_two_arguments_refused", "ordered": true, "pg": "differs: PostgreSQL pads with spaces", "assay_error": "unsupported: LPAD/RPAD require a fill argument", "difference": "DuckDB has no two-argument LPAD."}
SELECT LPAD('abc', 5) AS r;

-- case: {"id": "lpad_long_result", "ordered": true, "pg": "agree"}
SELECT LENGTH(LPAD('x', 1000, 'ab')) AS n, RIGHT(LPAD('x', 1000, 'ab'), 4) AS tail;

-- case: {"id": "lpad_nulls", "ordered": true, "pg": "agree"}
SELECT LPAD(NULL, 3, 'x') AS a, LPAD('a', NULL, 'x') AS b, RPAD('a', 3, NULL) AS c;

-- case: {"id": "reverse_ascii_rows", "ordered": true, "pg": "agree"}
SELECT id, REVERSE(t) AS r FROM s WHERE id IN (0, 1, 2, 3, 8, 10, 11, 13, 15) ORDER BY id;

-- case: {"id": "reverse_simple_non_ascii", "ordered": true, "pg": "agree (no combining or joining characters)"}
SELECT id, REVERSE(t) AS r FROM s WHERE id IN (4, 5, 9, 12) ORDER BY id;

-- case: {"id": "reverse_cjk_hangul", "ordered": true, "pg": "agree"}
SELECT REVERSE('日本語') AS a, REVERSE('가나다') AS b, REVERSE('€1 ©') AS c;

-- case: {"id": "reverse_combining_refused", "ordered": true, "pg": "differs: PostgreSQL reverses code points ('áe')", "assay_error": "unsupported: REVERSE of combining or joining characters", "difference": "DuckDB reverses grapheme clusters; PostgreSQL reverses code points."}
SELECT REVERSE('éa') AS r;

-- case: {"id": "reverse_zwj_emoji_refused", "ordered": true, "pg": "differs: PostgreSQL reverses code points", "assay_error": "unsupported: REVERSE of combining or joining characters", "difference": "DuckDB keeps the ZWJ emoji sequence together."}
SELECT REVERSE('👩‍👩‍👧x') AS r;

-- case: {"id": "ilike_table", "ordered": true, "pg": "agree in a UTF-8 database with a libc or ICU locale (C collation folds only ASCII)"}
SELECT id, t ILIKE '%WORLD' AS a, t ILIKE 'école%' AS b, t ILIKE '%STRASSE%' AS c, t ILIKE 'mixed_case' AS d FROM s ORDER BY id;

-- case: {"id": "not_ilike_filter", "ordered": true, "pg": "agree in a UTF-8 database with a libc or ICU locale"}
SELECT id FROM s WHERE t NOT ILIKE '%E%' ORDER BY id;

-- case: {"id": "ilike_nulls", "ordered": true, "pg": "agree"}
SELECT NULL ILIKE 'a' AS a, 'a' ILIKE NULL AS b, NULL NOT ILIKE NULL AS c;

-- case: {"id": "ilike_unicode_folding", "ordered": true, "pg": "agree with PostgreSQL libc UTF-8 lower(); ICU full folding can differ"}
SELECT 'É' ILIKE 'é' AS a, 'ẞ' ILIKE 'ß' AS b, 'K' ILIKE 'k' AS c, 'ß' ILIKE 'SS' AS d, 'É' ILIKE '_' AS e;

-- case: {"id": "ilike_turkish_dotted_i", "ordered": true, "pg": "differs under ICU (lower('İ') is 'i̇'); DuckDB simple mapping followed (Oct 16 decision)"}
SELECT 'İ' ILIKE 'i' AS a, 'ı' ILIKE 'I' AS b, 'I' ILIKE 'ı' AS c;

-- case: {"id": "ilike_backslash_refused", "ordered": true, "pg": "differs: PostgreSQL's default escape is backslash (true)", "assay_error": "unsupported: ILIKE pattern with a backslash requires ESCAPE", "difference": "DuckDB has no default escape character (false)."}
SELECT 'a_b' ILIKE 'A\_B' AS r;

-- case: {"id": "ilike_escape", "ordered": true, "pg": "agree"}
SELECT 'A_b' ILIKE 'a#_B' ESCAPE '#' AS a, 'Axb' ILIKE 'a#_B' ESCAPE '#' AS b, 'A_b' NOT ILIKE 'a#_B' ESCAPE '#' AS c;

-- case: {"id": "like_escape_controls", "ordered": true, "pg": "agree"}
SELECT 'a_b' LIKE 'a#_b' ESCAPE '#' AS a, 'axb' LIKE 'a#_b' ESCAPE '#' AS b, 'ab' LIKE 'a#b' ESCAPE '#' AS c, 'a%' LIKE 'a#%' ESCAPE '#' AS d, 'a#b' LIKE 'a##b' ESCAPE '#' AS e, 'a%c' LIKE 'a%%c' ESCAPE '%' AS f;

-- case: {"id": "like_escape_table", "ordered": true, "pg": "agree"}
SELECT id FROM s WHERE t LIKE '%!%!_%' ESCAPE '!' ORDER BY id;

-- case: {"id": "not_like_escape", "ordered": true, "pg": "agree"}
SELECT id, t NOT LIKE '%#_%' ESCAPE '#' AS r FROM s ORDER BY id;

-- case: {"id": "like_escape_empty_and_null", "ordered": true, "pg": "agree"}
SELECT 'a\b' LIKE 'a\b' ESCAPE '' AS a, 'a' LIKE 'a' ESCAPE NULL AS b;

-- case: {"id": "like_escape_trailing_error", "ordered": true, "pg": "agree (error)", "assay_error": "LIKE pattern must not end with escape character", "difference": "Both references reject a trailing escape."}
SELECT 'a#' LIKE 'a#' ESCAPE '#' AS r;

-- case: {"id": "like_escape_multichar_error", "ordered": true, "pg": "agree (error)", "assay_error": "invalid escape string: ESCAPE must be empty or one character", "difference": "Both references reject a multi-character escape."}
SELECT 'a' LIKE 'a' ESCAPE 'ab' AS r;

-- case: {"id": "like_escape_non_ascii_refused", "ordered": true, "pg": "differs: PostgreSQL accepts a multibyte escape character", "assay_error": "unsupported: LIKE ESCAPE requires an ASCII character", "difference": "DuckDB rejects a multibyte escape string."}
SELECT 'é' LIKE 'é' ESCAPE 'é' AS r;

-- case: {"id": "length_table", "ordered": true, "pg": "agree (code points)"}
SELECT id, LENGTH(t) AS a, CHAR_LENGTH(t) AS b, CHARACTER_LENGTH(t) AS c FROM s ORDER BY id;

-- case: {"id": "upper_lower_table", "ordered": true, "pg": "differs for ß: PostgreSQL libc/builtin keeps ß and ICU gives SS; DuckDB's ẞ followed (Oct 16 decision)"}
SELECT id, UPPER(t) AS u, LOWER(t) AS l FROM s ORDER BY id;

-- case: {"id": "case_turkish", "ordered": true, "pg": "differs under ICU (i̇); agrees with libc en_US.UTF-8"}
SELECT LOWER('İSTANBUL') AS a, UPPER('ıi') AS b, LOWER('I') AS c;

-- case: {"id": "case_final_sigma", "ordered": true, "pg": "differs under ICU (final ς); agrees with libc"}
SELECT LOWER('ΟΔΟΣ') AS a, UPPER('σς') AS b;

-- case: {"id": "case_ligatures_titlecase", "ordered": true, "pg": "differs under ICU full mapping (FI, ʼN, ἈΙ); agrees with simple mapping"}
SELECT UPPER('ﬁ') AS a, UPPER('ŉ') AS b, UPPER('ǅ') AS c, LOWER('ǅ') AS d, UPPER('ᾀ') AS e;

-- case: {"id": "case_emoji_and_empty", "ordered": true, "pg": "agree"}
SELECT UPPER('👩‍👩‍👧 ok') AS a, LOWER('') AS b, UPPER(NULL) AS c;

-- case: {"id": "double_to_varchar", "ordered": true, "pg": "differs for integer-valued doubles (PostgreSQL 2, DuckDB 2.0); DuckDB followed (Oct 16 decision)"}
SELECT id, CAST(CAST(d AS DOUBLE) AS VARCHAR) AS v FROM s ORDER BY id;

-- case: {"id": "double_to_text_spellings", "ordered": true, "pg": "agree"}
SELECT CAST(CAST(1 AS DOUBLE) / CAST(3 AS DOUBLE) AS TEXT) AS a, CAST(CAST(0.1 AS DOUBLE) AS TEXT) AS b, CAST(CAST(2.5 AS DOUBLE) AS VARCHAR(3)) AS c, CAST(0.1 AS DOUBLE)::VARCHAR AS d;

-- case: {"id": "double_to_text_exponents", "ordered": true, "pg": "differs at 1e15 (PostgreSQL 1e+15); others agree"}
SELECT CAST(CAST(100000000000000000000 AS DOUBLE) AS VARCHAR) AS a, CAST(CAST(1000000000000000 AS DOUBLE) AS VARCHAR) AS b, CAST(CAST(0.00001 AS DOUBLE) AS VARCHAR) AS c, CAST(CAST(0.0001 AS DOUBLE) AS VARCHAR) AS d, CAST(CAST(12345678901234567 AS DOUBLE) AS VARCHAR) AS e;

-- case: {"id": "double_negative_zero_text", "ordered": true, "pg": "differs: PostgreSQL's -0.0 is numeric zero and prints 0; a true negative zero prints -0", "assay_error": "unsupported: negative zero DOUBLE to TEXT", "difference": "Assay's existing SQLite rule types -0.0 as a negative-zero REAL while DuckDB reads DECIMAL zero (0.0); text would expose the sign."}
SELECT CAST(CAST(-0.0 AS DOUBLE) AS VARCHAR) AS r;

-- case: {"id": "group_by_normalized_name", "ordered": true, "pg": "agree"}
SELECT LOWER(TRIM(t)) AS k, COUNT(*) AS n FROM s WHERE t IS NOT NULL GROUP BY LOWER(TRIM(t)) ORDER BY k;

-- case: {"id": "label_from_parts", "ordered": true, "pg": "agree"}
SELECT id, UPPER(LEFT(t, 1)) || LOWER(SUBSTRING(t FROM 2 FOR 4)) AS label FROM s WHERE id IN (0, 8, 11, 13, 15) ORDER BY label, id;

-- case: {"id": "case_insensitive_join_key", "ordered": true, "pg": "differs where ß or Turkish i round-trips differ by locale; DuckDB followed"}
SELECT a.id AS a_id, b.id AS b_id FROM s a JOIN s b ON LOWER(a.t) = LOWER(UPPER(b.t)) ORDER BY a.id, b.id;
