WITH pairs AS (
SELECT DISTINCT invoice_no,stock_code FROM retail_line WHERE is_cancel_invoice=0 AND quantity>0 AND unit_price>0
AND invoice_no IS NOT NULL AND stock_code IS NOT NULL AND stock_code IN ('85123A','85099B','22423','47566','20725'))
SELECT a.stock_code AS stock_code_a,b.stock_code AS stock_code_b,COUNT(DISTINCT a.invoice_no) AS shared_invoice_count
FROM pairs a INNER JOIN pairs b ON a.invoice_no=b.invoice_no
WHERE a.stock_code<b.stock_code GROUP BY a.stock_code,b.stock_code
ORDER BY stock_code_a,stock_code_b
