WITH totals AS (
SELECT stock_code,SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 THEN quantity ELSE 0 END) AS positive_quantity,
SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN CAST(quantity AS DECIMAL(38,0))*unit_price ELSE 0 END) AS positive_sale_value FROM retail_line GROUP BY stock_code)
SELECT stock_code,positive_quantity,positive_sale_value,DENSE_RANK() OVER (ORDER BY positive_quantity DESC) AS quantity_rank,
DENSE_RANK() OVER (ORDER BY positive_sale_value DESC) AS value_rank
FROM totals ORDER BY positive_quantity DESC,stock_code ASC NULLS FIRST
