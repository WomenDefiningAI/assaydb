-- sales_rows
SELECT * FROM sales ORDER BY sale_id;

-- revenue_by_region
SELECT region, SUM(CAST(price AS DOUBLE) * quantity) AS revenue, COUNT(*) AS sales_count FROM sales GROUP BY region ORDER BY region;

-- top_categories
SELECT category, SUM(CAST(price AS DOUBLE) * quantity) AS revenue FROM sales GROUP BY category ORDER BY revenue DESC, category LIMIT 3;

-- average_price_per_store
SELECT store_id, AVG(CAST(price AS DOUBLE)) AS average_price, MIN(price), MAX(price), COUNT(price) FROM sales GROUP BY store_id ORDER BY store_id;

-- store_revenue
SELECT s.store_id, t.store_name, SUM(CAST(s.price AS DOUBLE) * s.quantity) AS revenue FROM sales s JOIN stores t ON s.store_id = t.store_id GROUP BY s.store_id, t.store_name ORDER BY s.store_id;

-- sales_filter
SELECT sale_id, price, notes FROM sales WHERE region = 'East' AND price >= 1.5 AND quantity < 5 ORDER BY sale_id;

-- sales_multiset
SELECT region, category, price, quantity, store_id, notes FROM sales;
