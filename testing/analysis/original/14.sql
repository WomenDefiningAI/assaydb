WITH baskets AS (
SELECT country,invoice_no,COUNT(DISTINCT stock_code) AS products,SUM(CAST(quantity AS DECIMAL(38,0))*unit_price) AS invoice_value
FROM retail_line WHERE invoice_no IS NOT NULL GROUP BY country,invoice_no), classified AS (
SELECT CASE WHEN country='United Kingdom' THEN 'UK' WHEN country IS NULL THEN 'missing_country' ELSE 'non_UK' END AS region,products,invoice_value FROM baskets)
SELECT region,COUNT(*) AS basket_count,SUM(products) AS total_distinct_products,SUM(invoice_value) AS total_invoice_value
FROM classified GROUP BY region
