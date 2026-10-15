SELECT stock_code, MIN(unit_price) AS minimum_price, MAX(unit_price) AS maximum_price,
COUNT(DISTINCT unit_price) AS price_points FROM retail_line WHERE unit_price > 0
GROUP BY stock_code ORDER BY stock_code NULLS FIRST
