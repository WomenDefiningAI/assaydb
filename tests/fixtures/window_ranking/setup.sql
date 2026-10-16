CREATE TABLE wr(id INTEGER, region TEXT, product TEXT, sales INTEGER, month INTEGER);
INSERT INTO wr VALUES (1,'east','apple',10,1),(2,'east','pear',20,1),(3,'east','pear',20,2),(4,'west','apple',30,1),(5,'west','plum',30,1),(6,'west','pear',NULL,2),(7,NULL,'apple',5,1),(8,NULL,'pear',5,2),(9,'east',NULL,NULL,2),(10,'west','fig',7,2),(11,'east','fig',10,1),(12,NULL,NULL,NULL,2);
CREATE TABLE empty_wr(id INTEGER, region TEXT, sales INTEGER);
CREATE TABLE typed_wr(id INTEGER, amount DECIMAL(20,2), day DATE, text_key TEXT, real_key REAL, flag BOOLEAN);
INSERT INTO typed_wr VALUES (1,90071992547409.92,DATE '2024-01-01','a',0.5,TRUE),(2,90071992547409.93,DATE '2024-01-02','b',0.5,FALSE),(3,90071992547409.92,DATE '2024-01-01','a',-0.5,TRUE),(4,NULL,NULL,NULL,NULL,NULL),(5,-1.25,DATE '2023-12-31','é',1.25,FALSE);
