-- case: {"id": "m001", "ordered": true, "assay_outcome": "rows"}
SELECT id, sqrt(i) AS a, sqrt(d) AS b, sqrt(f) AS c FROM n WHERE i >= 0 AND d >= 0 AND f >= 0 ORDER BY id;

-- case: {"id": "m002", "ordered": true, "assay_outcome": "rows"}
SELECT id, cbrt(i) AS a, cbrt(d) AS b, cbrt(f) AS c, exp(d / 100) AS e1, exp(f / 1000) AS e2 FROM n WHERE id <> 6 ORDER BY id;

-- case: {"id": "m003", "ordered": true, "assay_outcome": "rows"}
SELECT id, ln(i) AS a, ln(d) AS b, ln(f) AS c, log(i) AS l1, log10(d) AS l2, log2(f) AS l3, log(2, i) AS b1, log(2.5, f) AS b2 FROM n WHERE i > 0 AND d > 0 AND f > 0 ORDER BY id;

-- case: {"id": "m004", "ordered": true, "assay_outcome": "rows"}
SELECT id, sin(i) AS a, cos(d) AS b, tan(f) AS c, asin(e) AS s1, acos(e) AS s2, atan(i) AS t1, atan2(d, f) AS t2 FROM n WHERE e BETWEEN -1 AND 1 ORDER BY id;

-- case: {"id": "m005", "ordered": true, "assay_outcome": "rows"}
SELECT id, power(i, 2) AS a, pow(d, 2) AS b, power(f, 0.5) AS c, power(2, j) AS e FROM n WHERE f >= 0 AND id NOT IN (6, 7) ORDER BY id;

-- case: {"id": "m006", "ordered": true, "assay_outcome": "rows"}
SELECT id, radians(i) AS a, radians(d) AS b, radians(f) AS c, pi() AS p FROM n ORDER BY id;

-- case: {"id": "m007", "ordered": true, "assay_outcome": "rows"}
SELECT id, floor(i) AS a, floor(d) AS b, floor(e) AS c, floor(f) AS x FROM n ORDER BY id;

-- case: {"id": "m008", "ordered": true, "assay_outcome": "rows"}
SELECT id, ceil(i) AS a, ceiling(d) AS b, ceil(e) AS c, ceiling(f) AS x FROM n ORDER BY id;

-- case: {"id": "m009", "ordered": true, "assay_outcome": "rows"}
SELECT id, trunc(i) AS a, trunc(d) AS b, trunc(e) AS c, trunc(f) AS x FROM n ORDER BY id;

-- case: {"id": "m010", "ordered": true, "assay_outcome": "rows"}
SELECT id, trunc(i, -2) AS a, trunc(d, 1) AS b, trunc(d, -1) AS c, trunc(e, 5) AS e5, trunc(e, 2) AS e2, trunc(f, 1) AS f1, trunc(f, -2) AS f2 FROM n ORDER BY id;

-- case: {"id": "m011", "ordered": true, "assay_outcome": "rows", "assay_types": ["DECIMAL(38,2)", "BIGINT", "DOUBLE"], "type_note": "TRUNC with a NULL precision keeps the input scale, as ROUND does; DuckDB types the DECIMAL case as NULL. Values are NULL in both."}
SELECT trunc(d, NULL) AS a, trunc(i, NULL) AS b, trunc(f, NULL) AS c FROM n WHERE id = 1;

-- case: {"id": "m012", "ordered": true, "assay_outcome": "rows"}
SELECT id, sign(i) AS a, sign(d) AS b, sign(e) AS c, sign(f) AS x FROM n ORDER BY id;

-- case: {"id": "m013", "ordered": true, "assay_outcome": "rows"}
SELECT id, mod(i, 3) AS a, mod(i, j) AS b, i % 3 AS c, mod(j, NULL) AS e FROM n ORDER BY id;

-- case: {"id": "m014", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(i, j) AS a, least(i, j) AS b, greatest(i, j, 0) AS c, least(j, NULL, i) AS e FROM n ORDER BY id;

-- case: {"id": "m015", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(i, d) AS a, least(d, e) AS b, greatest(e, j) AS c FROM n ORDER BY id;

-- case: {"id": "m016", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(i, f) AS a, least(j, f) AS b, greatest(d, f) AS c, least(f, 0, d) AS e FROM n ORDER BY id;

-- case: {"id": "m017", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(t, 'b') AS a, least(t, 'b') AS b, greatest(dt, '2024-01-15') AS c, least(dt, DATE '2000-01-01') AS e, greatest(i > 0, f > 0) AS h, least(i > 0, f > 0) AS k FROM n ORDER BY id;

-- case: {"id": "m018", "ordered": true, "assay_outcome": "rows", "assay_types": ["BIGINT", "DOUBLE", "NULL", "BIGINT", "BIGINT"], "type_note": "GREATEST(NULL, NULL) is untyped NULL in both engines (DuckDB typeof: NULL); DuckDB's Python client reports that column as INTEGER."}
SELECT greatest(i) AS a, least(f) AS b, greatest(NULL, NULL) AS c, least(NULL, i) AS e, greatest(CAST(NULL AS BIGINT), NULL) AS h FROM n WHERE id = 5;

-- case: {"id": "m019", "ordered": true, "assay_outcome": "rows"}
SELECT sqrt(NULL) AS a, floor(NULL) AS b, ceil(NULL) AS c, trunc(NULL) AS e, sign(NULL) AS h, power(NULL, 2) AS k, log(2, NULL) AS l, trunc(NULL, 2) AS m;

-- case: {"id": "m020", "ordered": true, "assay_outcome": "rows"}
SELECT id, sqrt(d) AS a, floor(i) AS b, sign(f) AS c, greatest(i, f) AS e, trunc(e, 1) AS h, ln(i) AS k FROM n WHERE id = 5;

-- case: {"id": "m021", "ordered": true, "assay_outcome": "rows"}
SELECT sqrt(f) AS a, floor(d) AS b, greatest(i, f) AS c, trunc(e, 1) AS e2, sign(i) AS s, ln(f) AS l FROM empty;

-- case: {"id": "m022", "ordered": true, "assay_outcome": "rows"}
SELECT SUM(floor(d)) AS a, MAX(ln(f)) AS b, COUNT(greatest(i, j)) AS c FROM empty;

-- case: {"id": "m023", "ordered": true, "assay_outcome": "rows", "spill": true}
SELECT g, SUM(floor(d)) AS a, SUM(trunc(e, 1)) AS b, MIN(greatest(i, 0)) AS c, MAX(sign(f)) AS s, COUNT(sqrt(abs(f))) AS k FROM n GROUP BY g ORDER BY g;

-- case: {"id": "m024", "ordered": true, "assay_outcome": "rows", "spill": true}
SELECT floor(d / 50) * 50 AS bucket, COUNT(*) AS c, SUM(d) AS s FROM n WHERE d IS NOT NULL GROUP BY floor(d / 50) * 50 ORDER BY bucket;

-- case: {"id": "m025", "ordered": true, "assay_outcome": "rows", "spill": true}
SELECT floor(d) AS k, COUNT(*) AS c, MAX(id) AS m FROM n GROUP BY floor(d) ORDER BY k;

-- case: {"id": "m026", "ordered": true, "assay_outcome": "rows"}
SELECT id FROM n WHERE floor(f) = 2 OR sign(i) = -1 OR greatest(d, e) > 100 ORDER BY id;

-- case: {"id": "m027", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(i, 0) AS k FROM n ORDER BY greatest(i, 0) DESC, id;

-- case: {"id": "m028", "ordered": true, "assay_outcome": "rows"}
SELECT id, CASE WHEN f > 0 THEN ln(f) ELSE -1 END AS a, CASE WHEN d < 0 THEN sqrt(-d) END AS b FROM n ORDER BY id;

-- case: {"id": "m029", "ordered": true, "assay_outcome": "rows"}
SELECT id, round(sqrt(power(f, 2) + power(d, 2)), 6) AS r, greatest(floor(d), ceil(f)) AS g2, abs(trunc(e, 2)) AS t2, sign(floor(f)) AS s FROM n WHERE id NOT IN (6, 7) ORDER BY id;

-- case: {"id": "m030", "ordered": true, "assay_outcome": "rows"}
SELECT floor(-2.55) AS a, ceil(2.55) AS b, trunc(-2.55, 1) AS c, sign(-0.0) AS s, greatest(1, 2.5) AS g, least(-1, 0.5, NULL) AS l, power(2, 10) AS p, sqrt(16) AS q, log(2, 1024) AS r, mod(-7, 3) AS m, trunc(-77, -1) AS t;

-- case: {"id": "m031", "ordered": true, "assay_outcome": "rows", "float_tolerance": 1e-12}
SELECT SUM(power(i, 0)) AS a, AVG(sqrt(abs(i))) AS b, SUM(floor(f)) AS c FROM n WHERE id <> 7;

-- case: {"id": "m032", "ordered": true, "assay_outcome": "rows"}
SELECT id, greatest(trunc(d, 1), floor(e)) AS a, least(ceil(d), trunc(e)) AS b, greatest(sign(i), sign(f)) AS c FROM n ORDER BY id;

-- case: {"id": "e001", "ordered": true, "assay_outcome": "error", "assay_error": "cannot take logarithm of a negative number"}
SELECT ln(d) AS a FROM n;

-- case: {"id": "e002", "ordered": true, "assay_outcome": "error", "assay_error": "cannot take square root of a negative number"}
SELECT id, sqrt(f) AS a FROM n WHERE id >= 2;

-- case: {"id": "e003", "ordered": true, "assay_outcome": "error", "assay_error": "cannot take logarithm of zero"}
SELECT ln(f) AS a FROM n WHERE id IN (2, 3);

-- case: {"id": "e004", "ordered": true, "assay_outcome": "error", "assay_error": "ASIN is undefined outside [-1,1]"}
SELECT asin(e) AS a FROM n WHERE id = 1;

-- case: {"id": "e005", "ordered": true, "assay_outcome": "error", "assay_error": "division by zero"}
SELECT log(1, 8) AS a;

-- case: {"id": "e006", "ordered": true, "assay_outcome": "error", "assay_error": "cannot take logarithm of zero"}
SELECT log(0, 8) AS a;

-- case: {"id": "e007", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: POWER of zero to a negative exponent", "difference": "DuckDB returns infinity; PostgreSQL raises `zero raised to a negative power is undefined`"}
SELECT power(0, -1) AS a;

-- case: {"id": "e008", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: POWER of a negative number to a non-integer exponent", "difference": "DuckDB returns NaN; PostgreSQL raises `a negative number raised to a non-integer power yields a complex result`"}
SELECT power(f, 0.5) AS a FROM n WHERE id = 0;

-- case: {"id": "e009", "ordered": true, "assay_outcome": "error", "assay_error": "floating arithmetic overflow", "difference": "DuckDB returns infinity; PostgreSQL raises `value out of range: overflow`; Assay keeps its arithmetic overflow error"}
SELECT exp(f) AS a FROM n WHERE id = 6;

-- case: {"id": "e010", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: DEGREES", "difference": "DuckDB multiplies by 180/pi, PostgreSQL divides by pi/180: results differ in the last bit (see expected.json degrees)"}
SELECT degrees(f) AS a FROM n;

-- case: {"id": "e011", "ordered": true, "assay_outcome": "rows"}
SELECT mod(f, 2) AS a FROM n;

-- case: {"id": "e012", "ordered": true, "assay_outcome": "rows"}
SELECT mod(d, 2) AS a FROM n;

-- case: {"id": "e013", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: TRUNC precision must be an integer literal or NULL"}
SELECT trunc(d, j) AS a FROM n;

-- case: {"id": "e014", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: TRUNC precision must be an integer literal or NULL"}
SELECT trunc(d, 1.5) AS a FROM n;

-- case: {"id": "e015", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: expression FLOOR"}
SELECT floor(d, 1) AS a FROM n;

-- case: {"id": "e016", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: SQRT requires a numeric argument"}
SELECT sqrt(t) AS a FROM n;

-- case: {"id": "e017", "ordered": true, "assay_outcome": "error", "assay_error": "GREATEST arguments have incompatible types"}
SELECT greatest(i, t) AS a FROM n;

-- case: {"id": "e018", "ordered": true, "assay_outcome": "error", "assay_error": "PI requires 0 argument(s)"}
SELECT pi(1) AS a;

-- case: {"id": "e019", "ordered": true, "assay_outcome": "error", "assay_error": "SQRT requires 1 argument(s)"}
SELECT sqrt(4, 2) AS a;

-- case: {"id": "e020", "ordered": true, "assay_outcome": "error", "assay_error": "unsupported: scalar function cot", "difference": "COT(0): DuckDB raises, PostgreSQL returns Infinity; not implemented"}
SELECT cot(f) AS a FROM n WHERE id = 1;

-- case: {"id": "r001", "ordered": true, "assay_outcome": "rows"}
SELECT id, d % 1 AS a, d % 0.5 AS b, mod(d, e) AS c, i % d AS x, d % i AS y, e % j AS z FROM n WHERE i <> 0 AND d <> 0 AND e <> 0 AND j <> 0 ORDER BY id;

-- case: {"id": "r002", "ordered": true, "assay_outcome": "rows"}
SELECT id, f % 2 AS a, mod(f, 0.75) AS b, i % f AS c, f % i AS x, f % j AS y FROM n WHERE id NOT IN (2, 3) ORDER BY id;

-- case: {"id": "r003", "ordered": true, "assay_outcome": "rows"}
SELECT id, d % f AS a, f % d AS b, f % e AS c FROM n WHERE f <> 0 AND d <> 0 AND e <> 0 ORDER BY id;

-- case: {"id": "r004", "ordered": true, "assay_outcome": "rows", "difference": "Float zero divisor: DuckDB NaN, PostgreSQL error; Assay keeps its approved NULL (as division)", "assay_rows": [[1, null, null, null], [2, null, null, null], [5, null, null, null]]}
SELECT id, f % 0 AS a, i % 0 AS b, f % CAST(0 AS DOUBLE) AS c FROM n WHERE id IN (1, 2, 5) ORDER BY id;

-- case: {"id": "r005", "ordered": true, "assay_outcome": "error", "assay_error": "division by zero", "difference": "DECIMAL zero divisor: DuckDB NULL, PostgreSQL error `division by zero`; Assay keeps its DECIMAL division rule"}
SELECT d % 0 AS a FROM n WHERE id = 1;

-- case: {"id": "r006", "ordered": true, "assay_outcome": "error", "assay_error": "division by zero", "difference": "DECIMAL zero divisor: DuckDB NULL, PostgreSQL error `division by zero`; Assay keeps its DECIMAL division rule"}
SELECT i % d AS a FROM n WHERE id = 2;

-- case: {"id": "r007", "ordered": true, "assay_outcome": "rows", "spill": true}
SELECT g, SUM(d % 1) AS a, SUM(i % 7) AS b, MAX(f % 1) AS c, COUNT(mod(d, 2)) AS k FROM n GROUP BY g ORDER BY g;

-- case: {"id": "r008", "ordered": true, "assay_outcome": "rows"}
SELECT d % 1 AS k, COUNT(*) AS c FROM n GROUP BY d % 1 ORDER BY k;

-- case: {"id": "r009", "ordered": true, "assay_outcome": "rows"}
SELECT 7.5 % 2 AS a, -7.5 % 2 AS b, 7 % 2.5 AS c, mod(7.5, -2) AS d2, CAST(7.5 AS DOUBLE) % 2 AS e, mod(-7, 3) AS f, 7 % CAST(2.5 AS DOUBLE) AS g;

-- case: {"id": "r010", "ordered": true, "assay_outcome": "rows"}
SELECT id, CASE WHEN id > 5 THEN i ELSE f END % 2 AS a, COALESCE(i, f) % 3 AS b FROM n ORDER BY id;
