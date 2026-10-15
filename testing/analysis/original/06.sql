SELECT stock_code, SUM(quantity) AS negative_quantity, COUNT(*) AS negative_line_count
FROM retail_line WHERE quantity < 0 GROUP BY stock_code
ORDER BY negative_quantity, stock_code NULLS FIRST
