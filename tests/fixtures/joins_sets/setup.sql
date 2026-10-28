CREATE TABLE orders_a(id INTEGER, cust INTEGER, region TEXT, amount DECIMAL(10,2), day DATE);
INSERT INTO orders_a VALUES
(1, 10, 'north', 100.50, DATE '2024-01-05'),
(2, 10, 'north', 20.00, DATE '2024-01-06'),
(3, 20, 'south', 75.25, DATE '2024-02-01'),
(4, NULL, 'south', 5.00, DATE '2024-02-02'),
(5, 30, NULL, 60.00, NULL),
(6, 40, 'east', 0.00, DATE '2024-03-01');
CREATE TABLE orders_b(cust INTEGER, region TEXT, amount DECIMAL(10,2), note TEXT);
INSERT INTO orders_b VALUES
(10, 'north', 100.50, 'match'),
(20, 'south', 70.00, 'amount differs'),
(20, 'south', 5.25, 'second row'),
(50, 'west', 12.00, 'only b'),
(NULL, 'south', 5.00, 'null cust'),
(NULL, NULL, 1.00, 'all null');
CREATE TABLE custs(cust INTEGER, name TEXT, tier TEXT);
INSERT INTO custs VALUES (10, 'Ada', 'gold'), (20, 'Bo', 'silver'), (50, 'Cy', 'gold'), (60, 'Di', NULL), (NULL, 'Nil', 'bronze');
CREATE TABLE nothing(cust INTEGER, name TEXT);
CREATE TABLE pa(k INTEGER, k2 TEXT, x INTEGER);
INSERT INTO pa VALUES (1, 'a', 1), (1, 'b', 2), (2, 'a', 3), (2, NULL, 4), (NULL, 'a', 5), (1, 'a', 6);
CREATE TABLE pb(k INTEGER, k2 TEXT, y INTEGER);
INSERT INTO pb VALUES (1, 'a', 10), (2, 'a', 30), (2, 'b', 40), (NULL, 'a', 50), (2, NULL, 60), (1, 'a', 70);
CREATE TABLE fa(f DOUBLE, flag BOOLEAN, tag TEXT);
INSERT INTO fa VALUES (1.5, TRUE, 'x'), (2.25, FALSE, 'y'), (NULL, TRUE, 'z'), (-3.0, NULL, 'w');
CREATE TABLE fb(f DOUBLE, flag BOOLEAN, tag TEXT);
INSERT INTO fb VALUES (2.25, TRUE, 'y'), (4.0, FALSE, 'v'), (NULL, NULL, 'z'), (1.5, TRUE, 'x');
CREATE TABLE ma(a INTEGER, b TEXT);
INSERT INTO ma VALUES (1, 'x'), (1, 'x'), (1, 'x'), (2, 'y'), (2, 'y'), (NULL, 'n'), (NULL, 'n'), (3, NULL), (4, 'z');
CREATE TABLE mb(a INTEGER, b TEXT);
INSERT INTO mb VALUES (1, 'x'), (1, 'x'), (2, 'y'), (2, 'y'), (2, 'y'), (NULL, 'n'), (3, NULL), (3, NULL), (5, 'q');
CREATE TABLE md(a DECIMAL(6,1), b TEXT);
INSERT INTO md VALUES (1.0, 'x'), (2.0, 'y'), (2.5, 'y');
