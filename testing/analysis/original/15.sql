SELECT customer_id,COUNT(DISTINCT stock_code) AS distinct_products,COUNT(DISTINCT invoice_no) AS purchase_invoice_count
FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND customer_id IS NOT NULL GROUP BY customer_id
