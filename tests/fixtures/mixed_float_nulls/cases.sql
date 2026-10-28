-- case: {"id": "slt_good_83_real", "source": "test/random/expr/slt_good_83.test", "line": 83665}
SELECT 38 * 83 * + COALESCE ( - CASE + 34 WHEN + - ( AVG ( + 1 ) ) THEN - CAST ( NULL AS REAL ) - - 47 + + 75 + + 60 + - 11 + CASE + 68 WHEN 94 THEN 93 + + 14 - + + 33 + - 84 + - CASE + CAST ( - 21 AS INTEGER ) + + 28 WHEN + 23 THEN + 44 WHEN 77 THEN NULL WHEN 45 * 14 THEN NULL WHEN - 34 THEN NULL END * 63 * 58 WHEN 12 / + 91 THEN NULL WHEN AVG ( 53 ) + + 77 * - 45 THEN NULL WHEN - 9 THEN NULL ELSE NULL END + - COUNT ( * ) * - 78 / - 26 WHEN + 34 THEN - 21 ELSE NULL END, 67 - - COUNT ( * ) ) * 73 + CAST ( NULL AS INTEGER )

-- case: {"id": "slt_good_83_decimal_signed", "source": "test/random/expr/slt_good_83.test", "line": 83661}
SELECT 38 * 83 * + COALESCE ( - CASE + 34 WHEN + - ( AVG ( + 1 ) ) THEN - CAST( NULL AS DECIMAL ) - - 47 + + 75 + + 60 + - 11 + CASE + 68 WHEN 94 THEN 93 + + 14 - + + 33 + - 84 + - CASE + CAST( - 21 AS SIGNED ) + + 28 WHEN + 23 THEN + 44 WHEN 77 THEN NULL WHEN 45 * 14 THEN NULL WHEN - 34 THEN NULL END * 63 * 58 WHEN 12 / + 91 THEN NULL WHEN AVG ( 53 ) + + 77 * - 45 THEN NULL WHEN - 9 THEN NULL ELSE NULL END + - COUNT( * ) * - 78 / - 26 WHEN + 34 THEN - 21 ELSE NULL END, 67 - - COUNT( * ) ) * 73 + CAST( NULL AS SIGNED )

-- case: {"id": "reduced_avg"}
SELECT COALESCE(CASE WHEN AVG(1) = 34 THEN CAST(NULL AS REAL) ELSE -21 END, 67 - -COUNT(*)) + CAST(NULL AS INTEGER)

-- case: {"id": "reduced_null_arithmetic"}
SELECT COALESCE(CASE WHEN AVG(1) = 34 THEN -CAST(NULL AS REAL) + COUNT(*) ELSE -21 END, COUNT(*) + 67) + CAST(NULL AS INTEGER)

-- case: {"id": "reduced_double"}
SELECT COALESCE(CASE WHEN COUNT(*) = 0 THEN CAST(NULL AS DOUBLE) ELSE 1 END, COUNT(*)) * CAST(NULL AS INTEGER)

-- case: {"id": "reduced_double_precision"}
SELECT COALESCE(CASE WHEN COUNT(*) = 0 THEN CAST(NULL AS DOUBLE PRECISION) ELSE 1 END, COUNT(*)) - CAST(NULL AS INTEGER)

-- case: {"id": "reduced_division"}
SELECT COALESCE(CASE WHEN AVG(1) = 34 THEN CAST(NULL AS REAL) ELSE -21 END, COUNT(*)) / 2 + CAST(NULL AS INTEGER)

-- case: {"id": "non_null_selected_literal"}
SELECT COALESCE(CASE WHEN AVG(1) = 34 THEN CAST(NULL AS REAL) ELSE -21 END, COUNT(*))

-- case: {"id": "non_null_selected_division"}
SELECT COALESCE(CASE WHEN AVG(1) = 34 THEN CAST(NULL AS REAL) ELSE -21 END, COUNT(*)) / 2

-- case: {"id": "nullable_rows"}
SELECT COALESCE(CASE WHEN flag THEN -CAST(NULL AS REAL) + n ELSE -21 END, n + 67) AS v FROM null_arms ORDER BY v
