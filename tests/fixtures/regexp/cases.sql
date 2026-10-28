-- case: {"id": "matches_table", "ordered": true, "pg_sql": "SELECT id, regexp_like(s, 'b') AS m FROM r ORDER BY id"}
SELECT id, regexp_matches(s, 'b') AS m FROM r ORDER BY id;

-- case: {"id": "like_table", "ordered": true, "duckdb_sql": "SELECT id, regexp_matches(s, '^[a-z]+$') AS m FROM r WHERE id <> 6 ORDER BY id"}
SELECT id, regexp_like(s, '^[a-z]+$') AS m FROM r WHERE id <> 6 ORDER BY id;

-- case: {"id": "like_ci_operator", "ordered": true, "duckdb_sql": "SELECT id, regexp_matches(s, 'ABC', 'i') AS a, NOT regexp_matches(s, 'B', 'i') AS b FROM r WHERE id <> 6 ORDER BY id"}
SELECT id, s ~* 'ABC' AS a, s !~* 'B' AS b FROM r WHERE id <> 6 ORDER BY id;

-- case: {"id": "like_ci_literals", "ordered": true, "duckdb_sql": "SELECT regexp_matches('abc', 'B', 'i') AS a, regexp_matches(NULL, 'a', 'i') AS b, regexp_matches('A', NULL, 'i') AS c"}
SELECT 'abc' ~* 'B' AS a, NULL ~* 'a' AS b, 'A' ~* NULL AS c;

-- case: {"id": "tilde_refused", "ordered": true, "assay_error": "unsupported: ~ ", "difference": "DuckDB's ~ is a full match (regexp_full_match, false here); PostgreSQL's ~ is a partial match (true)."}
SELECT 'abc' ~ 'b' AS m;

-- case: {"id": "not_tilde_refused", "ordered": true, "assay_error": "unsupported: !~ ", "difference": "DuckDB's !~ negates a full match (true here); PostgreSQL's negates a partial match (false)."}
SELECT 'abc' !~ 'b' AS m;

-- case: {"id": "full_match", "ordered": true, "pg": "skip"}
SELECT id, regexp_full_match(s, 'a.*') AS a, regexp_full_match(s, 'b|abc') AS b FROM r ORDER BY id;

-- case: {"id": "full_match_options", "ordered": true, "pg": "skip"}
SELECT regexp_full_match('ABC', 'abc', 'i') AS a, regexp_full_match('ABC', 'abc') AS b, regexp_full_match('', '') AS c, regexp_full_match('ab', 'a|ab') AS d;

-- case: {"id": "similar_to", "ordered": true}
SELECT id, s SIMILAR TO '(a|x)[a-z]*' AS a, s NOT SIMILAR TO '[a-c]+' AS b FROM r WHERE id <> 6 ORDER BY id;

-- case: {"id": "similar_to_brackets", "ordered": true}
SELECT '3.14' SIMILAR TO '[0-9.]+' AS a, 'ab' SIMILAR TO '(ab|cd)*' AS b, 'abcd' SIMILAR TO '(ab|cd)*' AS c, 'a' SIMILAR TO 'a{2}' AS d, NULL SIMILAR TO 'a' AS e;

-- case: {"id": "similar_percent_refused", "ordered": true, "assay_error": "unsupported: SIMILAR TO pattern with %", "difference": "PostgreSQL's SIMILAR TO uses SQL wildcards (% matches any text: true); DuckDB's SIMILAR TO is a regular expression where % is a literal (false)."}
SELECT 'abc' SIMILAR TO 'a%' AS m;

-- case: {"id": "similar_dot_refused", "ordered": true, "assay_error": "unsupported: SIMILAR TO pattern with .", "difference": "In PostgreSQL's SIMILAR TO . is a literal (false here); DuckDB treats it as any character (true)."}
SELECT 'abc' SIMILAR TO 'a.c' AS m;

-- case: {"id": "similar_escape_refused", "ordered": true, "assay_error": "unsupported: SIMILAR TO ... ESCAPE", "difference": "DuckDB does not implement a custom SIMILAR TO escape (error); PostgreSQL answers false."}
SELECT 'abc' SIMILAR TO 'a' ESCAPE '#' AS m;

-- case: {"id": "replace_first_and_global", "ordered": true}
SELECT id, regexp_replace(s, '[ab]', '-') AS first, regexp_replace(s, '[ab]', '-', 'g') AS every FROM r ORDER BY id;

-- case: {"id": "replace_group_refs", "ordered": true}
SELECT regexp_replace('Clerk#000000951', '^Clerk#0*([0-9]+)$', 'clerk \1') AS a, regexp_replace('john smith', '([a-z]+) ([a-z]+)', '\2, \1') AS b, regexp_replace('a1b22c333', '([0-9]+)', '<\1>', 'g') AS c, regexp_replace('abc', '(b)', '\\') AS d;

-- case: {"id": "replace_nulls", "ordered": true}
SELECT regexp_replace(NULL, 'a', 'b') AS a, regexp_replace('a', NULL, 'b') AS b, regexp_replace('a', 'a', NULL) AS c, regexp_replace('a', 'a', 'b', NULL) AS d;

-- case: {"id": "replace_whitespace", "ordered": true}
SELECT id, regexp_replace(s, '\s+', ' ', 'g') AS squeezed, regexp_replace(s, '\s*', '', 'g') AS stripped FROM r WHERE id IN (1, 3, 4, 8) ORDER BY id;

-- case: {"id": "replace_extent_refused", "ordered": true, "assay_error": "unsupported: regular expression match extent", "difference": "DuckDB (RE2) takes the first alternative that matches ('a': Xbcd); PostgreSQL takes the longest match ('ab': Xcd)."}
SELECT regexp_replace('abcd', 'a|ab', 'X') AS m;

-- case: {"id": "replace_extent_agrees", "ordered": true}
SELECT regexp_replace('abcd', 'ab|a', 'X') AS a, regexp_replace('xaaay', 'a+', '-') AS b, regexp_replace('abab', 'ab', '', 'g') AS c;

-- case: {"id": "replace_empty_after_refused", "ordered": true, "assay_error": "unsupported: global regular expression replacement of an empty match", "difference": "After replacing 'b', DuckDB skips the empty match that follows (-a-c-); PostgreSQL replaces it (-a--c-)."}
SELECT regexp_replace('abc', 'b*', '-', 'g') AS m;

-- case: {"id": "replace_empty_matches_agree", "ordered": true}
SELECT regexp_replace('abc', 'b*', '', 'g') AS a, regexp_replace('abc', 'x*', '-', 'g') AS b, regexp_replace('abc', '', '-', 'g') AS c, regexp_replace('', 'x*', '-') AS d;

-- case: {"id": "replace_zero_reference_refused", "ordered": true, "assay_error": "unsupported: regular expression replacement", "difference": "\\0 is the whole match in DuckDB (a[b]c) and a literal \\0 in PostgreSQL (a[\\0]c)."}
SELECT regexp_replace('abc', 'b', '[\0]') AS m;

-- case: {"id": "replace_ampersand_refused", "ordered": true, "assay_error": "unsupported: regular expression replacement", "difference": "\\& is the whole match in PostgreSQL (a[b]c); DuckDB rejects the rewrite and returns the text unchanged (abc)."}
SELECT regexp_replace('abc', 'b', '[\&]') AS m;

-- case: {"id": "replace_missing_group_refused", "ordered": true, "assay_error": "unsupported: replacement refers to a group", "difference": "DuckDB returns the text unchanged (abc); PostgreSQL inserts an empty string (a[]c)."}
SELECT regexp_replace('abc', '(b)', '[\2]') AS m;

-- case: {"id": "replace_alternation_reference_refused", "ordered": true, "assay_error": "unsupported: group reference", "difference": "DuckDB's submatches follow the first matching alternative ([a|bc]); PostgreSQL's prefer the longest earlier group ([ab|c])."}
SELECT regexp_replace('abc', '(a|ab)(bc|c)', '[\1|\2]') AS m;

-- case: {"id": "replace_repeated_group_refused", "ordered": true, "assay_error": "unsupported: group reference", "difference": "For a repeated group DuckDB reports one long iteration (<ab|ab>cab); PostgreSQL splits it into more iterations (<ab|b>cab)."}
SELECT regexp_replace('abcab', '([ab]+)+', '<\1>') AS m;

-- case: {"id": "replace_lazy_refused", "ordered": true, "assay_error": "unsupported: lazy quantifier in REGEXP_REPLACE", "difference": "PostgreSQL makes the whole match non-greedy when the first quantifier is lazy (x[a|]aay); DuckDB applies laziness per quantifier (x[a|aa]y)."}
SELECT regexp_replace('xaaay', '(a+?)(a*)', '[\1|\2]') AS m;

-- case: {"id": "replace_options", "ordered": true}
SELECT regexp_replace('AbA', 'a', '-', 'gi') AS a, regexp_replace('AbA', 'a', '-', 'ig') AS b, regexp_replace('AbA', 'a', '-', 'ic') AS c, regexp_replace('AbA', 'a', '-', 'ci') AS d, regexp_replace('AbA', 'a', '-', 'cg') AS e, regexp_replace('AbA', 'a', '-', '') AS f;

-- case: {"id": "replace_dotall_option", "ordered": true}
SELECT id, regexp_replace(s, 'a.b', '-', 's') AS a, regexp_replace(s, 'a.b', '-', 'gs') AS b FROM r WHERE id IN (1, 2) ORDER BY id;

-- case: {"id": "replace_newline_option_refused", "ordered": true, "assay_error": "unsupported: regular expression option n", "difference": "PostgreSQL's n makes ^ and $ match at newlines; DuckDB's n leaves them at the text ends."}
SELECT regexp_replace(s, '^b', '-', 'n') AS m FROM r WHERE id = 2;

-- case: {"id": "replace_option_both_reject", "ordered": true, "assay_error": "invalid regular expression option"}
SELECT regexp_replace('ab', 'a', 'x', 'z') AS m;

-- case: {"id": "matches_options", "ordered": true, "pg": "skip"}
SELECT regexp_matches('A', 'a', 'i') AS a, regexp_matches('A', 'a', 'ic') AS b, regexp_matches('A', 'a', 'ci') AS c, regexp_matches('A', 'a', '') AS d;

-- case: {"id": "matches_global_option_error", "ordered": true, "pg": "skip", "assay_error": "invalid regular expression option"}
SELECT regexp_matches('a', 'a', 'g') AS m;

-- case: {"id": "like_global_option_error", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', 'a', 'g') AS m", "assay_error": "invalid regular expression option"}
SELECT regexp_like('a', 'a', 'g') AS m;

-- case: {"id": "matches_null_options_error", "ordered": true, "pg": "skip", "assay_error": "REGEXP_MATCHES options must not be NULL"}
SELECT regexp_matches('a', 'a', NULL) AS m;

-- case: {"id": "like_null_flags", "ordered": true, "duckdb_sql": "SELECT CAST(NULL AS BOOLEAN) AS m"}
SELECT regexp_like('a', 'a', NULL) AS m;

-- case: {"id": "newline_dot_matches_follows_duckdb", "ordered": true, "pg": "skip"}
SELECT id, regexp_matches(s, 'a.b') AS m FROM r WHERE id IN (1, 2) ORDER BY id;

-- case: {"id": "newline_dot_like_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches(s, 'a.b') AS m FROM r WHERE id = 2", "assay_error": "unsupported: regular expression '.' on text containing a newline", "difference": "DuckDB's . does not match a newline (false); PostgreSQL's does (true)."}
SELECT regexp_like(s, 'a.b') AS m FROM r WHERE id = 2;

-- case: {"id": "newline_dot_s_option", "ordered": true, "duckdb_sql": "SELECT id, regexp_matches(s, 'a.b', 's') AS m FROM r WHERE id IN (1, 2) ORDER BY id"}
SELECT id, regexp_like(s, 'a.b', 's') AS m FROM r WHERE id IN (1, 2) ORDER BY id;

-- case: {"id": "class_non_ascii_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('é', '\\w') AS m", "assay_error": "unsupported: regular expression character class on non-ASCII text", "difference": "DuckDB's \\w is ASCII (false for é); PostgreSQL's follows the database locale (true in en_US.UTF-8)."}
SELECT regexp_like('é', '\w') AS m;

-- case: {"id": "class_non_ascii_matches_follows_duckdb", "ordered": true, "pg": "skip"}
SELECT regexp_matches('é', '\w') AS a, regexp_matches('é1', '\d') AS b, regexp_full_match('é', '[[:alpha:]]') AS c, regexp_extract('é_1', '\w+') AS d;

-- case: {"id": "vertical_tab_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches(u, 'a\\sb') AS m FROM r WHERE id = 10", "assay_error": "unsupported: regular expression \\s on a vertical tab", "difference": "DuckDB's \\s excludes vertical tab (false); PostgreSQL's includes it (true)."}
SELECT regexp_like(u, 'a\sb') AS m FROM r WHERE id = 10;

-- case: {"id": "case_fold_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('K', 'k', 'i') AS m", "assay_error": "unsupported: case-insensitive regular expression on non-ASCII text", "difference": "DuckDB folds the Kelvin sign to k (true); PostgreSQL does not (false)."}
SELECT 'K' ~* 'k' AS m;

-- case: {"id": "case_fold_refused_duckdb_function", "ordered": true, "pg": "skip", "assay_error": "unsupported: case-insensitive regular expression on non-ASCII text", "difference": "Case folding outside ASCII depends on each engine's Unicode tables; Assay refuses it in every function. DuckDB folds the long s to s (true)."}
SELECT regexp_matches('ſ', 'S', 'i') AS m;

-- case: {"id": "case_fold_pattern_refused", "ordered": true, "pg": "skip", "assay_error": "unsupported: case-insensitive regular expression with non-ASCII", "difference": "A non-ASCII letter in a case-insensitive pattern depends on Unicode case tables. DuckDB answers true."}
SELECT regexp_matches('É', 'é', 'i') AS m;

-- case: {"id": "word_boundary_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a b', 'a\\b') AS m", "assay_error": "unsupported: regular expression escape \\b", "difference": "\\b is a word boundary in DuckDB (true) and a backspace in PostgreSQL (false)."}
SELECT regexp_like('a b', 'a\b') AS m;

-- case: {"id": "lookahead_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('ab', 'a(?=b)') AS m", "assay_error": "unsupported: regular expression group (?", "difference": "DuckDB (RE2) rejects lookahead; PostgreSQL answers true."}
SELECT regexp_like('ab', 'a(?=b)') AS m;

-- case: {"id": "backreference_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('aa', '(a)\\1') AS m", "assay_error": "unsupported: regular expression escape \\1", "difference": "DuckDB (RE2) rejects backreferences; PostgreSQL answers true."}
SELECT regexp_like('aa', '(a)\1') AS m;

-- case: {"id": "repetition_above_255_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', 'a{256}') AS m", "assay_error": "unsupported: regular expression repetition count above 255", "difference": "DuckDB allows counts up to 1000 (false here); PostgreSQL rejects counts above 255."}
SELECT regexp_like('a', 'a{256}') AS m;

-- case: {"id": "unicode_property_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', '\\pL') AS m", "assay_error": "unsupported: regular expression escape \\p", "difference": "DuckDB supports Unicode properties (true); PostgreSQL rejects \\p."}
SELECT regexp_like('a', '\pL') AS m;

-- case: {"id": "inline_flags_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('A', '(?i)a') AS m", "assay_error": "unsupported: regular expression group (?", "difference": "Both engines accept a leading (?i) here, but their embedded-option syntax differs elsewhere; Assay takes options only from the options argument. Both answer true."}
SELECT regexp_like('A', '(?i)a') AS m;

-- case: {"id": "director_refused", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', '***=a') AS m", "assay_error": "unsupported: regular expression director ***", "difference": "PostgreSQL treats ***= as a literal-pattern director (true); DuckDB rejects the pattern."}
SELECT regexp_like('a', '***=a') AS m;

-- case: {"id": "unbalanced_pattern_error", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', '(a') AS m", "assay_error": "invalid regular expression"}
SELECT regexp_like('a', '(a') AS m;

-- case: {"id": "double_quantifier_error", "ordered": true, "pg": "skip", "assay_error": "invalid regular expression"}
SELECT regexp_matches('a', 'a**') AS m;

-- case: {"id": "reversed_range_error", "ordered": true, "pg": "skip", "assay_error": "invalid regular expression"}
SELECT regexp_matches('a', '[z-a]') AS m;

-- case: {"id": "trailing_backslash_error", "ordered": true, "duckdb_sql": "SELECT regexp_replace('a', 'a\\', 'b') AS m", "assay_error": "invalid regular expression"}
SELECT regexp_replace('a', 'a\', 'b') AS m;

-- case: {"id": "unknown_escape_error", "ordered": true, "duckdb_sql": "SELECT regexp_matches('a', '\\k') AS m", "assay_error": "invalid regular expression"}
SELECT regexp_like('a', '\k') AS m;

-- case: {"id": "column_patterns", "ordered": true, "pg": "skip"}
SELECT id, regexp_matches(s, p) AS m FROM r WHERE id IN (1, 2, 4, 6, 7, 8, 9, 10) ORDER BY id;

-- case: {"id": "column_patterns_like", "ordered": true, "duckdb_sql": "SELECT id, regexp_matches(s, p) AS m FROM r WHERE id IN (1, 4, 7, 8, 9, 10) ORDER BY id"}
SELECT id, regexp_like(s, p) AS m FROM r WHERE id IN (1, 4, 7, 8, 9, 10) ORDER BY id;

-- case: {"id": "column_pattern_newline_refused", "ordered": true, "duckdb_sql": "SELECT id, regexp_matches(s, p) AS m FROM r WHERE id IN (1, 2) ORDER BY id", "assay_error": "unsupported: regular expression '.' on text containing a newline", "difference": "Row 2 matches 'a.b' against a newline: DuckDB false, PostgreSQL true."}
SELECT id, regexp_like(s, p) AS m FROM r WHERE id IN (1, 2) ORDER BY id;

-- case: {"id": "column_pattern_invalid_error", "ordered": true, "pg": "skip", "assay_error": "invalid regular expression"}
SELECT id, regexp_matches(s, p) AS m FROM r WHERE id IN (1, 5) ORDER BY id;

-- case: {"id": "column_replacement", "ordered": true}
SELECT id, regexp_replace(s, '(b)', rep) AS m FROM r WHERE id IN (1, 10) ORDER BY id;

-- case: {"id": "column_replacement_missing_group_refused", "ordered": true, "assay_error": "unsupported: replacement refers to a group", "difference": "Row 1's replacement \\1 refers to a group 'b' lacks: DuckDB keeps abc, PostgreSQL gives a<>c."}
SELECT id, regexp_replace(s, 'b', rep) AS m FROM r WHERE id IN (1, 10) ORDER BY id;

-- case: {"id": "extract_groups", "ordered": true, "pg": "skip"}
SELECT id, regexp_extract(s, '([a-z])([a-z])') AS whole, regexp_extract(s, '([a-z])([a-z])', 2) AS second, regexp_extract(s, '([a-z])([a-z])', 3) AS missing, regexp_extract(s, 'B', 0, 'i') AS ci FROM r WHERE id <> 6 ORDER BY id;

-- case: {"id": "extract_clerk_number", "ordered": true, "pg": "skip"}
SELECT regexp_extract('Clerk#000000951', '#0*([0-9]+)', 1) AS n, regexp_extract('none', '[0-9]+') AS empty, regexp_extract(NULL, 'a') AS null_text, regexp_extract('é', '.') AS first_code_point;

-- case: {"id": "extract_alternation_follows_duckdb", "ordered": true, "pg": "skip"}
SELECT regexp_extract('abcd', 'a|ab') AS a, regexp_extract('xaaay', '(a+?)(a*)', 2) AS b, regexp_extract('abcab', '([ab]+)+', 1) AS c;

-- case: {"id": "extract_group_range_error", "ordered": true, "pg": "skip", "assay_error": "REGEXP_EXTRACT group must be"}
SELECT regexp_extract('a', 'a', 10) AS m;

-- case: {"id": "extract_group_column_error", "ordered": true, "pg": "skip", "assay_error": "REGEXP_EXTRACT group must be"}
SELECT regexp_extract(s, '(a)', g) AS m FROM r;

-- case: {"id": "options_column_error", "ordered": true, "pg": "skip", "assay_error": "REGEXP_MATCHES options must be a string literal"}
SELECT regexp_matches(s, 'a', u) AS m FROM r;

-- case: {"id": "regexp_extract_all_refused", "ordered": true, "pg": "skip", "assay_error": "unsupported: REGEXP_EXTRACT_ALL", "difference": "Returns a LIST in DuckDB; Assay has no LIST type."}
SELECT regexp_extract_all('a1b2', '[0-9]') AS m;

-- case: {"id": "regexp_split_refused", "ordered": true, "duckdb_sql": "SELECT regexp_split_to_array('a,b', ',') AS m", "assay_error": "unsupported: REGEXP_SPLIT_TO_ARRAY", "difference": "Returns a LIST/array in both engines; Assay has no LIST type."}
SELECT regexp_split_to_array('a,b', ',') AS m;

-- case: {"id": "regexp_count_refused", "ordered": true, "assay_error": "unsupported: REGEXP_COUNT", "difference": "PostgreSQL 15+ only (2); DuckDB has no REGEXP_COUNT and no equivalent without LIST."}
SELECT regexp_count('abcabc', 'b') AS m;

-- case: {"id": "regexp_substr_refused", "ordered": true, "assay_error": "unsupported: REGEXP_SUBSTR", "difference": "PostgreSQL 15+ only, NULL when nothing matches; DuckDB's nearest function REGEXP_EXTRACT returns ''."}
SELECT regexp_substr('abc123', '[0-9]+') AS m;

-- case: {"id": "regexp_match_refused", "ordered": true, "assay_error": "unsupported: REGEXP_MATCH", "difference": "PostgreSQL returns a text array; DuckDB has no REGEXP_MATCH; Assay has no array type."}
SELECT regexp_match('abc', 'b') AS m;

-- case: {"id": "group_counts", "ordered": true, "pg_sql": "SELECT g, count(*) AS n, sum(CASE WHEN regexp_like(u, '[0-9]') THEN 1 ELSE 0 END) AS digits FROM r WHERE id <> 10 GROUP BY g ORDER BY g"}
SELECT g, count(*) AS n, sum(CASE WHEN regexp_matches(u, '[0-9]') THEN 1 ELSE 0 END) AS digits FROM r WHERE id <> 10 GROUP BY g ORDER BY g;

-- case: {"id": "group_by_extracted_word", "ordered": true, "pg": "skip"}
SELECT regexp_extract(u, '^([a-z]+)', 1) AS w, count(*) AS n FROM r GROUP BY w ORDER BY w;

-- case: {"id": "where_filter", "ordered": true, "duckdb_sql": "SELECT id FROM r WHERE regexp_matches(u, '^[a-z]+ [a-z]+$') ORDER BY id"}
SELECT id FROM r WHERE regexp_like(u, '^[a-z]+ [a-z]+$') ORDER BY id;

-- case: {"id": "empty_input", "ordered": true, "pg": "skip"}
SELECT regexp_matches(s, 'a') AS m, regexp_replace(s, 'a', 'b') AS r2 FROM empty;

-- case: {"id": "empty_input_count", "ordered": true, "duckdb_sql": "SELECT count(*) AS n FROM empty WHERE regexp_matches(s, 'a')"}
SELECT count(*) AS n FROM empty WHERE regexp_like(s, 'a');

-- case: {"id": "single_row_literal", "ordered": true}
SELECT regexp_replace('Clerk#000000951', '^Clerk#0*', '') AS n;

-- case: {"id": "unicode_code_points", "ordered": true}
SELECT regexp_replace('😀x', '.', '-', 'g') AS a, regexp_replace('é', '́', '!') AS b, regexp_replace('éa', '[^a]', '?', 'g') AS c;

-- case: {"id": "distinct_and_order", "ordered": true, "pg_sql": "SELECT DISTINCT regexp_like(u, 'a') AS m FROM r WHERE u IS NOT NULL AND id <> 10 ORDER BY m"}
SELECT DISTINCT regexp_matches(u, 'a') AS m FROM r WHERE u IS NOT NULL AND id <> 10 ORDER BY m;

-- case: {"id": "text_argument_required", "ordered": true, "assay_error": "unsupported: REGEXP_LIKE requires TEXT arguments", "duckdb_sql": "SELECT regexp_matches(1, 'a') AS m", "difference": "Both references reject an INTEGER text argument (no matching function); Assay gives its typed TEXT-argument refusal, as for its other string functions."}
SELECT regexp_like(1, 'a') AS m;
