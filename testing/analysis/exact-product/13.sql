SELECT weekday,hour,COUNT(DISTINCT invoice_no) AS invoice_count,SUM(quantity*unit_price) AS positive_sale_value
FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 GROUP BY weekday,hour ORDER BY weekday,hour
