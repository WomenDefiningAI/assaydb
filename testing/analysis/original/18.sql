SELECT stock_code,COUNT(DISTINCT CASE WHEN description IS NOT NULL AND description<>'' THEN description ELSE NULL END) AS description_count,
SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN quantity ELSE 0 END) AS positive_quantity,SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN CAST(quantity AS DECIMAL(38,0))*unit_price ELSE 0 END) AS positive_sale_value
FROM retail_line GROUP BY stock_code
HAVING COUNT(DISTINCT CASE WHEN description IS NOT NULL AND description<>'' THEN description ELSE NULL END)>1
