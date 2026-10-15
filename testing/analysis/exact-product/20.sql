SELECT year,month,COUNT(*) AS positive_line_count,COUNT(DISTINCT invoice_no) AS invoice_count,SUM(quantity) AS positive_quantity,
SUM(quantity*unit_price) AS positive_sale_value,COUNT(DISTINCT customer_id) AS customer_count,COUNT(DISTINCT stock_code) AS product_count
FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND ((year=2010 AND month=12) OR (year=2011 AND month=11))
GROUP BY year,month ORDER BY year,month
