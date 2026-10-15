SELECT year,month,SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN CAST(quantity AS DECIMAL(38,0))*unit_price ELSE 0 END) AS positive_sale_value,
SUM(CASE WHEN is_cancel_invoice=1 OR quantity<0 OR unit_price<0 THEN CAST(quantity AS DECIMAL(38,0))*unit_price ELSE 0 END) AS cancellation_negative_value
FROM retail_line GROUP BY year,month ORDER BY year,month
