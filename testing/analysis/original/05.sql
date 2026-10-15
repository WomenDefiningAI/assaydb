SELECT country,COUNT(DISTINCT invoice_no) AS invoice_count,
COUNT(DISTINCT CASE WHEN is_cancel_invoice=1 THEN invoice_no ELSE NULL END) AS cancellation_invoice_count
FROM retail_line WHERE invoice_no IS NOT NULL GROUP BY country
