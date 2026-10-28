-- case: {"id": "eq_peer", "ordered": true}
SELECT id,reading = peer AS answer FROM t ORDER BY id;

-- case: {"id": "eq_literal", "ordered": true}
SELECT id FROM t WHERE reading = 0.2 ORDER BY id;

-- case: {"id": "eq_integer", "ordered": true}
SELECT id,reading = id AS a,id = reading AS b FROM t ORDER BY id;

-- case: {"id": "ne_peer", "ordered": true}
SELECT id,reading <> peer AS answer FROM t ORDER BY id;

-- case: {"id": "ne_literal", "ordered": true}
SELECT id FROM t WHERE reading <> 0.2 ORDER BY id;

-- case: {"id": "ne_integer", "ordered": true}
SELECT id,reading <> id AS a,id <> reading AS b FROM t ORDER BY id;

-- case: {"id": "lt_peer", "ordered": true}
SELECT id,reading < peer AS answer FROM t ORDER BY id;

-- case: {"id": "lt_literal", "ordered": true}
SELECT id FROM t WHERE reading < 0.2 ORDER BY id;

-- case: {"id": "lt_integer", "ordered": true}
SELECT id,reading < id AS a,id < reading AS b FROM t ORDER BY id;

-- case: {"id": "le_peer", "ordered": true}
SELECT id,reading <= peer AS answer FROM t ORDER BY id;

-- case: {"id": "le_literal", "ordered": true}
SELECT id FROM t WHERE reading <= 0.2 ORDER BY id;

-- case: {"id": "le_integer", "ordered": true}
SELECT id,reading <= id AS a,id <= reading AS b FROM t ORDER BY id;

-- case: {"id": "gt_peer", "ordered": true}
SELECT id,reading > peer AS answer FROM t ORDER BY id;

-- case: {"id": "gt_literal", "ordered": true}
SELECT id FROM t WHERE reading > 0.2 ORDER BY id;

-- case: {"id": "gt_integer", "ordered": true}
SELECT id,reading > id AS a,id > reading AS b FROM t ORDER BY id;

-- case: {"id": "ge_peer", "ordered": true}
SELECT id,reading >= peer AS answer FROM t ORDER BY id;

-- case: {"id": "ge_literal", "ordered": true}
SELECT id FROM t WHERE reading >= 0.2 ORDER BY id;

-- case: {"id": "ge_integer", "ordered": true}
SELECT id,reading >= id AS a,id >= reading AS b FROM t ORDER BY id;

-- case: {"id": "between", "ordered": true}
SELECT id,reading BETWEEN -1 AND 1 AS answer FROM t ORDER BY id;

-- case: {"id": "not_between", "ordered": true}
SELECT id,reading NOT BETWEEN -1 AND 1 AS answer FROM t ORDER BY id;

-- case: {"id": "between_nan", "ordered": true}
SELECT id,reading BETWEEN peer AND reading AS answer FROM t ORDER BY id;

-- case: {"id": "not_between_nan", "ordered": true}
SELECT id,reading NOT BETWEEN reading AND peer AS answer FROM t ORDER BY id;

-- case: {"id": "in_list", "ordered": true}
SELECT id,reading IN (peer,0,1) AS answer FROM t ORDER BY id;

-- case: {"id": "not_in_list", "ordered": true}
SELECT id,reading NOT IN (peer,0,1) AS answer FROM t ORDER BY id;

-- case: {"id": "in_null", "ordered": true}
SELECT id,reading IN (peer,NULL) AS answer FROM t ORDER BY id;

-- case: {"id": "not_in_null", "ordered": true}
SELECT id,reading NOT IN (peer,NULL) AS answer FROM t ORDER BY id;

-- case: {"id": "simple_case", "ordered": true}
SELECT id,CASE reading WHEN peer THEN 1 ELSE 0 END AS answer FROM t ORDER BY id;

-- case: {"id": "searched_case", "ordered": true}
SELECT id,CASE WHEN reading>0.2 THEN 1 ELSE 0 END AS answer FROM t ORDER BY id;

-- case: {"id": "nullif", "ordered": true}
SELECT id,NULLIF(reading,peer) AS answer FROM t ORDER BY id;

-- case: {"id": "reflexive_nullif", "ordered": true}
SELECT id,NULLIF(reading,reading) AS answer FROM t ORDER BY id;

-- case: {"id": "extremes", "ordered": true}
SELECT MIN(reading),MAX(reading),COUNT(reading) FROM t;

-- case: {"id": "group_extremes", "ordered": true}
SELECT sensor,MIN(reading),MAX(reading),COUNT(*) FROM t GROUP BY sensor ORDER BY sensor;

-- case: {"id": "distinct_count", "ordered": true}
SELECT COUNT(DISTINCT reading) FROM t;

-- case: {"id": "having_max", "ordered": true}
SELECT sensor,MAX(reading) AS hi FROM t GROUP BY sensor HAVING MAX(reading)>0.2 ORDER BY sensor;

-- case: {"id": "having_key", "ordered": true}
SELECT reading,COUNT(*) FROM t GROUP BY reading HAVING reading>=0 ORDER BY reading NULLS LAST;

-- case: {"id": "sort_ASC_FIRST", "ordered": true}
SELECT id,reading FROM t ORDER BY reading ASC NULLS FIRST,id;

-- case: {"id": "sort_ASC_LAST", "ordered": true}
SELECT id,reading FROM t ORDER BY reading ASC NULLS LAST,id;

-- case: {"id": "top_ASC", "ordered": true}
SELECT id,reading FROM t ORDER BY reading ASC NULLS LAST,id LIMIT 3;

-- case: {"id": "sort_DESC_FIRST", "ordered": true}
SELECT id,reading FROM t ORDER BY reading DESC NULLS FIRST,id;

-- case: {"id": "sort_DESC_LAST", "ordered": true}
SELECT id,reading FROM t ORDER BY reading DESC NULLS LAST,id;

-- case: {"id": "top_DESC", "ordered": true}
SELECT id,reading FROM t ORDER BY reading DESC NULLS LAST,id LIMIT 3;

-- case: {"id": "group", "ordered": true}
SELECT reading,COUNT(*) FROM t GROUP BY reading ORDER BY reading NULLS LAST;

-- case: {"id": "group_pair", "ordered": true}
SELECT reading,peer,COUNT(*) FROM t GROUP BY reading,peer ORDER BY reading NULLS LAST,peer NULLS LAST;

-- case: {"id": "distinct", "ordered": true}
SELECT DISTINCT reading FROM t ORDER BY reading NULLS LAST;

-- case: {"id": "distinct_pair", "ordered": true}
SELECT DISTINCT reading,peer FROM t ORDER BY reading NULLS LAST,peer NULLS LAST;

-- case: {"id": "union", "ordered": true}
SELECT reading FROM (SELECT reading FROM t WHERE id<24 UNION SELECT peer FROM t WHERE id>=8) combined ORDER BY reading NULLS LAST;

-- case: {"id": "intersect", "ordered": true}
SELECT reading FROM (SELECT reading FROM t WHERE id<24 INTERSECT SELECT peer FROM t WHERE id>=8) combined ORDER BY reading NULLS LAST;

-- case: {"id": "except", "ordered": true}
SELECT reading FROM (SELECT reading FROM t WHERE id<24 EXCEPT SELECT peer FROM t WHERE id>=8) combined ORDER BY reading NULLS LAST;

-- case: {"id": "union_all", "ordered": true}
SELECT reading FROM (SELECT reading FROM t WHERE id<24 UNION ALL SELECT peer FROM t WHERE id>=8) combined ORDER BY reading NULLS LAST;

-- case: {"id": "join", "ordered": true}
SELECT a.id,b.id FROM t a JOIN t b ON a.reading=b.peer WHERE a.id<8 ORDER BY a.id,b.id;

-- case: {"id": "join_pair", "ordered": true}
SELECT a.id,b.id FROM t a JOIN t b ON a.reading=b.peer AND a.sensor=b.sensor ORDER BY a.id,b.id;

-- case: {"id": "left_join", "ordered": true}
SELECT a.id,b.id FROM t a LEFT JOIN t b ON a.reading=b.peer WHERE a.id<8 ORDER BY a.id,b.id NULLS LAST;

-- case: {"id": "join_residual", "ordered": true}
SELECT a.id,b.id FROM t a JOIN t b ON a.sensor=b.sensor AND a.reading>b.reading WHERE a.id<8 ORDER BY a.id,b.id;

-- case: {"id": "join_nan", "ordered": true}
SELECT COUNT(*) FROM t a JOIN t b ON a.reading=b.reading WHERE a.reading>100;

-- case: {"id": "join_nan_residual", "ordered": true}
SELECT COUNT(*) FROM t a JOIN t b ON a.reading=b.reading AND a.peer>=b.peer;

-- case: {"id": "in_subquery", "ordered": true}
SELECT id,reading IN(SELECT peer FROM t WHERE id<8) AS m FROM t ORDER BY id;

-- case: {"id": "not_in_subquery", "ordered": true}
SELECT id,reading NOT IN(SELECT peer FROM t WHERE id<8) AS m FROM t ORDER BY id;

-- case: {"id": "in_where", "ordered": true}
SELECT id FROM t WHERE reading IN(SELECT peer FROM t WHERE id<8) ORDER BY id;

-- case: {"id": "not_in_where", "ordered": true}
SELECT id FROM t WHERE reading NOT IN(SELECT peer FROM t WHERE id<8) ORDER BY id;

-- case: {"id": "exists", "ordered": true}
SELECT a.id FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.peer=a.reading) ORDER BY a.id;

-- case: {"id": "not_exists", "ordered": true}
SELECT a.id FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE b.peer=a.reading) ORDER BY a.id;

-- case: {"id": "rank", "ordered": true}
SELECT id,RANK() OVER(ORDER BY reading NULLS LAST) AS answer FROM t ORDER BY id;

-- case: {"id": "dense_rank", "ordered": true}
SELECT id,DENSE_RANK() OVER(ORDER BY reading DESC NULLS LAST) AS answer FROM t ORDER BY id;

-- case: {"id": "row_number", "ordered": true}
SELECT id,ROW_NUMBER() OVER(PARTITION BY reading ORDER BY id) AS answer FROM t ORDER BY id;

-- case: {"id": "partition_count", "ordered": true}
SELECT id,COUNT(*) OVER(PARTITION BY reading) AS answer FROM t ORDER BY id;

-- case: {"id": "range_count", "ordered": true}
SELECT id,COUNT(*) OVER(ORDER BY reading NULLS LAST) AS answer FROM t ORDER BY id;

-- case: {"id": "window_min", "ordered": true}
SELECT id,MIN(reading) OVER(PARTITION BY sensor) AS answer FROM t ORDER BY id;

-- case: {"id": "window_max", "ordered": true}
SELECT id,MAX(reading) OVER(ORDER BY reading NULLS LAST) AS answer FROM t ORDER BY id;

-- case: {"id": "rows_max", "ordered": true}
SELECT id,MAX(reading) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) AS answer FROM t ORDER BY id;

-- case: {"id": "sensor_ranking", "ordered": true}
SELECT id,sensor,reading,RANK() OVER(PARTITION BY sensor ORDER BY reading DESC NULLS LAST) AS severity FROM t WHERE reading>0.2 ORDER BY sensor,severity,id;

-- case: {"id": "stats_nan", "ordered": true}
SELECT id FROM t WHERE reading>100 ORDER BY id;

-- case: {"id": "only_nan_extremes", "ordered": true}
SELECT MIN(reading),MAX(reading),COUNT(DISTINCT reading) FROM t WHERE id IN(0,10,11,13);

-- case: {"id": "only_null_extremes", "ordered": true}
SELECT MIN(reading),MAX(reading),COUNT(DISTINCT reading) FROM t WHERE reading IS NULL;

-- case: {"id": "filter_boundary", "ordered": true}
SELECT COUNT(*) FILTER(WHERE reading>0.2) FROM t;

-- case: {"id": "greatest_boundary", "ordered": true}
SELECT GREATEST(reading,peer) FROM t ORDER BY id;

-- case: {"id": "least_boundary", "ordered": true}
SELECT LEAST(reading,peer) FROM t ORDER BY id;

-- case: {"id": "cast_nan_boundary", "ordered": true, "refusal": "floating cast nonfinite result"}
SELECT CAST('NaN' AS DOUBLE);
