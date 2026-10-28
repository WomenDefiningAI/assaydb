CREATE TABLE s(id BIGINT, t TEXT, k BIGINT, d DECIMAL(10,3));
INSERT INTO s VALUES
(0, 'hello world', 3, 1.500),
(1, '  padded  ', -2, 2.000),
(2, '', 0, -0.125),
(3, NULL, NULL, NULL),
(4, 'Straße', 2, 1234.500),
(5, 'İstanbul ıi', 5, 0.001),
(6, 'éclair', 1, 100.250),
(7, '👩‍👩‍👧 family 🙂', 2, -3.000),
(8, 'a,b,,c', -1, 7.125),
(9, 'ÉCOLE Ünïcödé', 4, 0.100),
(10, 'abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcZ', 300, 99.999),
(11, 'xxhixx', 10, -0.001),
(12, 'ΟΔΟΣ Σίσυφος', 3, 12.000),
(13, '50%_off', 6, 0.500),
(14, 'ﬁne ǅ ŉ', -10, 2.500),
(15, 'MiXeD CaSe', 7, 1000000.000);
