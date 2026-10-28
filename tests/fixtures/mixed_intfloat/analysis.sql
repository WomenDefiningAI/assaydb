-- Realistic analysis: fixed-cents or rate discounts, LEFT JOINed to order lines.
-- expected_rows were computed by hand (worked in comments) and fixed before
-- Assay ran; the reference generator only checks that DuckDB 1.5.6 agrees.
-- Per line discount = COALESCE(discount_cents, rate * qty * price, 0):
--   100/1 250; 100/2 0 (no row); 101/1 0.1*5*1250 = 0.5*1250 = 625.0 (exact);
--   102/1 0 (no row); 102/2 100; 103/1 0 (row with both NULL).

-- case: {"id": "net_revenue_per_order", "expected_rows": [[100, 3249.0], [101, 5625.0], [102, 3098.0], [103, 400.0]], "note": "2500-250+999; 6250-625; 1200+1998-100; 400"}
SELECT l.order_id, SUM(l.qty * l.unit_price_cents - COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0)) AS net_cents FROM order_lines l LEFT JOIN discounts d ON l.order_id = d.order_id AND l.line_no = d.line_no GROUP BY l.order_id ORDER BY l.order_id

-- case: {"id": "discount_totals", "expected_rows": [[975.0, 162.5, 6]], "note": "250+0+625+0+100+0 = 975; 975/6 = 162.5"}
SELECT SUM(COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0)) AS total_discount, AVG(COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0)) AS avg_discount, COUNT(*) FROM order_lines l LEFT JOIN discounts d ON l.order_id = d.order_id AND l.line_no = d.line_no

-- case: {"id": "discount_share_percent_by_sku", "expected_rows": [["A", 10.0], ["B", 3.33667000333667], ["C", 0.0]], "note": "A 875*100/8750; B 100*100/2997 = 10000.0/2997 in binary64; C 0*100/1600"}
SELECT l.sku, SUM(COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0)) * 100 / SUM(l.qty * l.unit_price_cents) AS discount_pct FROM order_lines l LEFT JOIN discounts d ON l.order_id = d.order_id AND l.line_no = d.line_no GROUP BY l.sku ORDER BY l.sku

-- case: {"id": "fixed_discount_lines", "expected_rows": [[2]], "note": "only 100/1 and 102/2 select discount_cents, which compares equal in DOUBLE"}
SELECT COUNT(*) FROM order_lines l LEFT JOIN discounts d ON l.order_id = d.order_id AND l.line_no = d.line_no WHERE COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0) = d.discount_cents

-- case: {"id": "discounted_line_rate", "expected_rows": [[0.5]], "note": "3 of 6 lines have a positive discount"}
SELECT AVG(CASE WHEN COALESCE(d.discount_cents, d.discount_rate * l.qty * l.unit_price_cents, 0) > 0 THEN 1 ELSE CAST(0 AS REAL) END) AS discounted_rate FROM order_lines l LEFT JOIN discounts d ON l.order_id = d.order_id AND l.line_no = d.line_no
