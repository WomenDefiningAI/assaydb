WITH baskets AS (
SELECT invoice_no, COUNT(DISTINCT stock_code) AS products
FROM retail_line WHERE invoice_no IS NOT NULL GROUP BY invoice_no
), bands AS (
SELECT CASE WHEN products=0 THEN '0' WHEN products=1 THEN '1'
WHEN products<=5 THEN '2-5' WHEN products<=10 THEN '6-10'
WHEN products<=20 THEN '11-20' ELSE '21+' END AS basket_band,
CASE WHEN products=0 THEN 0 WHEN products=1 THEN 1 WHEN products<=5 THEN 2
WHEN products<=10 THEN 3 WHEN products<=20 THEN 4 ELSE 5 END AS band_order
FROM baskets)
SELECT basket_band, band_order, COUNT(*) AS invoice_count
FROM bands GROUP BY basket_band, band_order ORDER BY band_order
