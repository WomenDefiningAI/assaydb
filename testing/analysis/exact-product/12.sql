SELECT year,month,
SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND customer_id IS NULL THEN quantity*unit_price ELSE 0 END) AS missing_customer_value,
SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND customer_id IS NOT NULL THEN quantity*unit_price ELSE 0 END) AS present_customer_value,
SUM(CASE WHEN is_cancel_invoice=0 AND quantity>0 AND unit_price>0 THEN quantity*unit_price ELSE 0 END) AS total_positive_sale_value FROM retail_line GROUP BY year,month ORDER BY year,month
