WITH totals AS (
SELECT customer_id,SUM(quantity*unit_price) AS positive_sale_value,COUNT(DISTINCT invoice_no) AS invoice_count
FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND customer_id IS NOT NULL GROUP BY customer_id)
SELECT customer_id,positive_sale_value,invoice_count,DENSE_RANK() OVER (ORDER BY positive_sale_value DESC) AS value_rank
FROM totals ORDER BY positive_sale_value DESC,customer_id ASC
