SELECT year, month, COUNT(*) AS line_count, COUNT(DISTINCT invoice_no) AS invoice_count,
SUM(CASE WHEN invoice_no IS NULL THEN 1 ELSE 0 END) AS missing_invoice_lines
FROM retail_line GROUP BY year, month ORDER BY year, month
