CREATE TABLE order_lines (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, sku TEXT NOT NULL, qty INTEGER NOT NULL, unit_price_cents INTEGER NOT NULL);
CREATE TABLE discounts (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, discount_cents INTEGER, discount_rate REAL);
INSERT INTO order_lines VALUES (100, 1, 'A', 2, 1250), (100, 2, 'B', 1, 999), (101, 1, 'A', 5, 1250), (102, 1, 'C', 3, 400), (102, 2, 'B', 2, 999), (103, 1, 'C', 1, 400);
INSERT INTO discounts VALUES (100, 1, 250, NULL), (101, 1, NULL, 0.1), (102, 2, 100, NULL), (103, 1, NULL, NULL);
