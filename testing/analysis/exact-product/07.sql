SELECT stock_code,SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN quantity*unit_price ELSE 0 END) AS positive_sale_value,SUM(CASE WHEN quantity<0 OR unit_price<0 THEN quantity*unit_price ELSE 0 END) AS negative_line_value
FROM retail_line GROUP BY stock_code
