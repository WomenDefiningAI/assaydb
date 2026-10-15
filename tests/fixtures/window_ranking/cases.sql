-- case: {"id": "w001", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (ORDER BY sales ASC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w002", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (ORDER BY sales DESC NULLS FIRST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w003", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w004", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales ASC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w005", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC NULLS FIRST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w006", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w007", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY month, region ORDER BY sales ASC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w008", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY month, region ORDER BY sales DESC NULLS FIRST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w009", "ordered": true, "sqlite": true}
SELECT id, ROW_NUMBER() OVER (PARTITION BY month, region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST, id) AS r FROM wr ORDER BY id;

-- case: {"id": "w010", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w011", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w012", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w013", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY region ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w014", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY region ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w015", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w016", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY month, region ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w017", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY month, region ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w018", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY month, region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w019", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w020", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w021", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w022", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY region ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w023", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY region ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w024", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w025", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY month, region ORDER BY sales ASC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w026", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY month, region ORDER BY sales DESC NULLS FIRST) AS r FROM wr ORDER BY id;

-- case: {"id": "w027", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY month, region ORDER BY product ASC NULLS LAST, sales DESC NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w028", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER () AS r FROM wr ORDER BY id;

-- case: {"id": "w029", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY region) AS r FROM wr ORDER BY id;

-- case: {"id": "w030", "ordered": true, "sqlite": true}
SELECT id, RANK() OVER (PARTITION BY month, region) AS r FROM wr ORDER BY id;

-- case: {"id": "w031", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER () AS r FROM wr ORDER BY id;

-- case: {"id": "w032", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY region) AS r FROM wr ORDER BY id;

-- case: {"id": "w033", "ordered": true, "sqlite": true}
SELECT id, DENSE_RANK() OVER (PARTITION BY month, region) AS r FROM wr ORDER BY id;

-- case: {"id": "w034", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM empty_wr ORDER BY id;

-- case: {"id": "w035", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (PARTITION BY id ORDER BY sales NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w036", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (ORDER BY month NULLS LAST,id)+10 AS r FROM wr ORDER BY r;

-- case: {"id": "w037", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM wr WHERE id=2 ORDER BY id;

-- case: {"id": "w038", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (ORDER BY sales NULLS LAST,id) AS r FROM wr ORDER BY r DESC LIMIT 4;

-- case: {"id": "w039", "ordered": true, "sqlite": true}
SELECT id,RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM empty_wr ORDER BY id;

-- case: {"id": "w040", "ordered": true, "sqlite": true}
SELECT id,RANK() OVER (PARTITION BY id ORDER BY sales NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w041", "ordered": true, "sqlite": true}
SELECT id,RANK() OVER (ORDER BY month NULLS LAST,id)+10 AS r FROM wr ORDER BY r;

-- case: {"id": "w042", "ordered": true, "sqlite": true}
SELECT id,RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM wr WHERE id=2 ORDER BY id;

-- case: {"id": "w043", "ordered": true, "sqlite": true}
SELECT id,RANK() OVER (ORDER BY sales NULLS LAST,id) AS r FROM wr ORDER BY r DESC LIMIT 4;

-- case: {"id": "w044", "ordered": true, "sqlite": true}
SELECT id,DENSE_RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM empty_wr ORDER BY id;

-- case: {"id": "w045", "ordered": true, "sqlite": true}
SELECT id,DENSE_RANK() OVER (PARTITION BY id ORDER BY sales NULLS LAST) AS r FROM wr ORDER BY id;

-- case: {"id": "w046", "ordered": true, "sqlite": true}
SELECT id,DENSE_RANK() OVER (ORDER BY month NULLS LAST,id)+10 AS r FROM wr ORDER BY r;

-- case: {"id": "w047", "ordered": true, "sqlite": true}
SELECT id,DENSE_RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST,id) AS r FROM wr WHERE id=2 ORDER BY id;

-- case: {"id": "w048", "ordered": true, "sqlite": true}
SELECT id,DENSE_RANK() OVER (ORDER BY sales NULLS LAST,id) AS r FROM wr ORDER BY r DESC LIMIT 4;

-- case: {"id": "w049", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (ORDER BY amount NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w050", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (PARTITION BY amount ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w051", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (ORDER BY day NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w052", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (PARTITION BY day ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w053", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (ORDER BY text_key NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w054", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (PARTITION BY text_key ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w055", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (ORDER BY real_key NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w056", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (PARTITION BY real_key ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w057", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (ORDER BY flag NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w058", "ordered": true, "sqlite": false}
SELECT id,RANK() OVER (PARTITION BY flag ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w059", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (ORDER BY amount NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w060", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (PARTITION BY amount ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w061", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (ORDER BY day NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w062", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (PARTITION BY day ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w063", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (ORDER BY text_key NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w064", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (PARTITION BY text_key ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w065", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (ORDER BY real_key NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w066", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (PARTITION BY real_key ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w067", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (ORDER BY flag NULLS LAST) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w068", "ordered": true, "sqlite": false}
SELECT id,DENSE_RANK() OVER (PARTITION BY flag ORDER BY id) AS r FROM typed_wr ORDER BY id;

-- case: {"id": "w069", "ordered": true, "sqlite": true}
SELECT region,product,SUM(sales) AS revenue,DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(sales) DESC NULLS LAST) AS r FROM wr GROUP BY region,product ORDER BY region NULLS LAST,product NULLS LAST;

-- case: {"id": "w070", "ordered": true, "sqlite": true}
SELECT region,product,revenue,r FROM (SELECT region,product,SUM(sales) AS revenue,DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(sales) DESC NULLS LAST) AS r FROM wr GROUP BY region,product) q WHERE r<=3 ORDER BY region NULLS LAST,r,product NULLS LAST;

-- case: {"id": "w071", "ordered": true, "sqlite": true}
SELECT month,product,SUM(sales) AS spend,RANK() OVER (PARTITION BY month ORDER BY SUM(sales) DESC NULLS LAST) AS r FROM wr GROUP BY month,product ORDER BY month,r,product NULLS LAST;

-- case: {"id": "w072", "ordered": true, "sqlite": true}
SELECT region,SUM(sales) AS total,DENSE_RANK() OVER (ORDER BY SUM(sales) DESC NULLS LAST) AS r FROM wr GROUP BY region HAVING SUM(sales)>20 ORDER BY r,region NULLS LAST;

-- case: {"id": "w073", "ordered": true, "sqlite": true}
SELECT id FROM wr ORDER BY DENSE_RANK() OVER (PARTITION BY region ORDER BY sales DESC NULLS LAST), id LIMIT 7;

-- case: {"id": "w074", "ordered": true, "sqlite": true}
SELECT id FROM wr ORDER BY ROW_NUMBER() OVER (ORDER BY sales DESC NULLS LAST,id)+10;

-- case: {"id": "w075", "ordered": true, "sqlite": true}
SELECT DISTINCT DENSE_RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST) AS r FROM wr ORDER BY r;

-- case: {"id": "w076", "ordered": true, "sqlite": true}
SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS a,RANK() OVER (PARTITION BY region ORDER BY sales DESC NULLS LAST) AS b,DENSE_RANK() OVER (ORDER BY month NULLS LAST) AS c FROM wr ORDER BY id;

-- case: {"id": "w077", "ordered": true, "sqlite": true}
SELECT id,CASE WHEN DENSE_RANK() OVER (ORDER BY sales NULLS LAST)<=2 THEN 1 ELSE 0 END AS top_flag FROM wr ORDER BY id;

-- case: {"id": "w078", "ordered": true, "sqlite": true}
SELECT id,r FROM (SELECT id,ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC NULLS LAST,id) AS r FROM wr) q WHERE r<=2 ORDER BY id;

-- case: {"id": "w079", "ordered": false, "sqlite": true}
SELECT id,RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST) AS r FROM wr;

-- case: {"id": "w080", "ordered": false, "sqlite": true}
SELECT id,DENSE_RANK() OVER (ORDER BY sales NULLS LAST) AS r FROM wr;

