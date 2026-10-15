WITH frequency AS (
SELECT customer_id,COUNT(DISTINCT invoice_no) AS purchases FROM retail_line
WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0 AND customer_id IS NOT NULL AND invoice_no IS NOT NULL GROUP BY customer_id), bands AS (
SELECT CASE WHEN purchases=1 THEN '1' WHEN purchases<=5 THEN '2-5' WHEN purchases<=10 THEN '6-10' ELSE '11+' END AS purchase_band,
CASE WHEN purchases=1 THEN 0 WHEN purchases<=5 THEN 1 WHEN purchases<=10 THEN 2 ELSE 3 END AS band_order,purchases FROM frequency)
SELECT purchase_band,band_order,COUNT(*) AS customer_count,SUM(purchases) AS purchase_invoice_count
FROM bands GROUP BY purchase_band,band_order ORDER BY band_order
