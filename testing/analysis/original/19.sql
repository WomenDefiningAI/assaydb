WITH january AS (
SELECT DISTINCT customer_id FROM retail_line WHERE customer_id IS NOT NULL
AND year=2011 AND month=1 AND is_cancel_invoice=0 AND quantity>0 AND unit_price>0
), november AS (
SELECT DISTINCT customer_id FROM retail_line WHERE customer_id IS NOT NULL
AND year=2011 AND month=11 AND is_cancel_invoice=0 AND quantity>0 AND unit_price>0
)
SELECT j.customer_id AS customer_id FROM january AS j INNER JOIN november AS n ON j.customer_id=n.customer_id
ORDER BY customer_id
