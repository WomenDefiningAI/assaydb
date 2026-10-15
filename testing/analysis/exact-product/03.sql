WITH totals AS (
SELECT country,SUM(quantity*unit_price) AS positive_sale_value,COUNT(DISTINCT invoice_no) AS invoice_count
FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 GROUP BY country)
SELECT country,positive_sale_value,invoice_count,DENSE_RANK() OVER (ORDER BY positive_sale_value DESC) AS value_rank,
DENSE_RANK() OVER (ORDER BY invoice_count DESC) AS invoice_rank
FROM totals ORDER BY positive_sale_value DESC,country ASC NULLS FIRST
