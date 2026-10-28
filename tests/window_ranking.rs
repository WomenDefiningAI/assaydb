//! Ranking semantics and canonical ordering, independently pinned to references.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    directory: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assay-window-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        Self { directory }
    }
    fn session(&self, workers: usize, batch_size: usize, spill: bool) -> Session {
        let mut session = Session::new(
            &self.directory,
            Options {
                workers,
                batch_size,
                memory_bytes: 64 << 20,
                spill: spill.then(|| SpillOptions {
                    directory: self.directory.clone(),
                    sort_run_bytes: 64 << 10,
                    merge_fan_in: 2,
                    ..SpillOptions::default()
                }),
                ..Options::default()
            },
            Limits {
                max_catalog_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap();
        for statement in include_str!("fixtures/window_ranking/setup.sql")
            .split(';')
            .filter(|s| !s.trim().is_empty())
        {
            session
                .execute_sql(statement)
                .unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        let typed = query(&mut session, "SELECT real_key FROM typed_wr");
        assert_eq!(typed.schema().field(0).data_type(), &DataType::Float64);
        session
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}
fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("expected query")
    };
    execution.batch.clone()
}
fn rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|array| {
                    if array.is_null(row) || array.data_type() == &DataType::Null {
                        return Value::Null;
                    }
                    match array.data_type() {
                        DataType::Int64 => json!(
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Utf8 => json!(
                            array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row)
                        ),
                        DataType::Boolean => json!(
                            array
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                        ),
                        other => panic!("unexpected output {other}"),
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn eighty_independent_duckdb_and_sqlite_cases() {
    let expected: Value = serde_json::from_str(
        &fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/window_ranking/expected.json"
        ))
        .expect("regenerate pinned window references"),
    )
    .unwrap();
    assert_eq!(expected["duckdb_version"], "1.5.6");
    assert_eq!(expected["disagreements"], json!([]));
    assert_eq!(
        expected["setup_sql"],
        include_str!("fixtures/window_ranking/setup.sql")
    );
    let cases = expected["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 80);
    let sources = include_str!("fixtures/window_ranking/cases.sql")
        .split("-- case: ")
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(sources.len(), cases.len());
    for (source, case) in sources.iter().zip(cases) {
        let (metadata, sql) = source.split_once('\n').unwrap();
        let metadata: Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(case["id"], metadata["id"]);
        assert_eq!(case["sql"], sql.trim());
        assert_eq!(case["ordered"], metadata["ordered"]);
        assert_eq!(case["sqlite"], metadata["sqlite"]);
    }
    let fixture = Fixture::new();
    for spill in [false, true] {
        let mut session = fixture.session(4, 7, spill);
        for case in cases {
            let sql = case["sql"].as_str().unwrap();
            let mut actual = rows(&query(&mut session, sql));
            let mut wanted: Vec<Vec<Value>> = serde_json::from_value(case["rows"].clone()).unwrap();
            if !case["ordered"].as_bool().unwrap() {
                actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                wanted.sort_by_key(|row| serde_json::to_string(row).unwrap());
            }
            assert_eq!(actual, wanted, "{} spill={spill}: {sql}", case["id"]);
        }
    }
}

#[test]
fn canonical_peers_and_output_are_independent_of_workers_batches_and_spill() {
    let fixture = Fixture::new();
    let mut serial = fixture.session(1, 1, false);
    let sqls = [
        "SELECT id,ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales NULLS LAST) AS rn,RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST) AS r,DENSE_RANK() OVER (PARTITION BY region ORDER BY sales NULLS LAST) AS d FROM wr",
        "SELECT id,ROW_NUMBER() OVER (PARTITION BY region) AS rn,RANK() OVER () AS r,DENSE_RANK() OVER () AS d FROM wr",
        "SELECT id,ROW_NUMBER() OVER (ORDER BY sales NULLS LAST) AS rn,DENSE_RANK() OVER (ORDER BY sales NULLS LAST) AS r FROM wr ORDER BY r",
        "SELECT * FROM (SELECT id,ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC NULLS LAST) AS rn FROM wr) ranked WHERE rn<=2",
        "SELECT region,SUM(sales) AS total,DENSE_RANK() OVER (ORDER BY SUM(sales) DESC NULLS LAST) AS r FROM wr GROUP BY region HAVING SUM(sales)>0",
        "SELECT a.id AS ai,b.id AS bi,ROW_NUMBER() OVER (PARTITION BY a.region ORDER BY a.sales NULLS LAST) AS r FROM wr a LEFT JOIN wr b ON a.region=b.region",
        "SELECT a.id AS ai,b.id AS bi,DENSE_RANK() OVER (PARTITION BY a.region ORDER BY a.sales DESC NULLS LAST) AS r FROM wr a JOIN wr b ON a.region=b.region",
        "SELECT id,RANK() OVER (ORDER BY NULL) AS r,DENSE_RANK() OVER (PARTITION BY NULL ORDER BY NULL) AS d FROM wr",
        "SELECT SUM(sales) AS total,ROW_NUMBER() OVER (ORDER BY SUM(sales)) AS r FROM wr",
        "SELECT DISTINCT region,RANK() OVER (ORDER BY month) AS r FROM wr ORDER BY r",
        "SELECT DISTINCT region,RANK() OVER (ORDER BY month) AS r FROM wr",
    ];
    let expected = sqls
        .iter()
        .map(|sql| query(&mut serial, sql))
        .collect::<Vec<_>>();
    // Input order and tie positions are asserted, not inferred from another run.
    let first = rows(&expected[0]);
    assert_eq!(
        first
            .iter()
            .map(|row| row[0].as_i64().unwrap())
            .collect::<Vec<_>>(),
        (1..=12).collect::<Vec<_>>()
    );
    assert_eq!(
        first
            .iter()
            .map(|row| row[1].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 3, 4, 2, 3, 4, 1, 2, 5, 1, 2, 3]
    );
    assert_eq!(
        first
            .iter()
            .map(|row| row[2].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 3, 3, 2, 2, 4, 1, 1, 5, 1, 1, 3]
    );
    assert_eq!(
        first
            .iter()
            .map(|row| row[3].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 2, 2, 2, 3, 1, 1, 3, 1, 1, 2]
    );
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch_size, spill);
                for (sql, wanted) in sqls.iter().zip(&expected) {
                    assert_eq!(
                        query(&mut session, sql),
                        *wanted,
                        "{workers}/{batch_size}/{spill}: {sql}"
                    );
                }
            }
        }
    }
}

#[test]
fn unsupported_window_forms_remain_specific_refusals() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, false);
    assert_eq!(
        session
            .execute_sql("SELECT DISTINCT region FROM wr ORDER BY RANK() OVER (ORDER BY sales)")
            .err()
            .unwrap()
            .message,
        "DISTINCT ORDER BY expression must appear in SELECT"
    );
    for (sql, expected) in [
        (
            "SELECT RANK() OVER () AS r FROM wr QUALIFY r=1",
            vec![vec![json!(1)]; 12],
        ),
        (
            "SELECT NTILE(2) OVER () FROM wr",
            (0..12).map(|i| vec![json!(1 + i / 6)]).collect(),
        ),
        (
            "SELECT FIRST_VALUE(sales) OVER () FROM wr",
            vec![vec![json!(10)]; 12],
        ),
    ] {
        assert_eq!(rows(&query(&mut session, sql)), expected);
    }
    // NTILE(2)/FIRST_VALUE over one twelve-row peer group are peer-order
    // dependent (and there is no final ORDER BY): require membership in the
    // complete permitted multiset set from the independent model, the
    // canonical answer, and record DuckDB's answer.
    let reference: Value =
        serde_json::from_str(include_str!("fixtures/window_frames/handwritten_ties.json")).unwrap();
    assert_eq!(reference["duckdb_version"], "1.5.6");
    let multiset = |rows: &Value| {
        let mut rows = rows.as_array().unwrap().clone();
        rows.sort_by_key(|row| row.to_string());
        Value::Array(rows)
    };
    for (id, sql) in [
        ("wr-ntile-2-over-empty", "SELECT NTILE(2) OVER () FROM wr"),
        (
            "wr-first-value-over-empty",
            "SELECT FIRST_VALUE(sales) OVER () FROM wr",
        ),
    ] {
        let case = reference["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["id"] == id)
            .unwrap();
        assert_eq!(case["sql"], sql);
        assert_eq!(case["compare"], "multiset");
        let permitted = case["permitted_multisets"]
            .as_array()
            .unwrap()
            .iter()
            .map(multiset)
            .collect::<Vec<_>>();
        let actual = json!(rows(&query(&mut session, sql)));
        assert!(permitted.contains(&multiset(&actual)), "{id}: {actual}");
        assert!(permitted.contains(&multiset(&case["canonical_rows"])));
        assert!(permitted.contains(&multiset(&case["duckdb_rows"])));
        assert_eq!(actual, case["canonical_rows"], "{id}: canonical order");
    }
    for (sql, message) in [
        (
            "SELECT RANK() OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM wr",
            "window frames",
        ),
        (
            "SELECT RANK() OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) FROM wr",
            "window frames",
        ),
        (
            "SELECT RANK() OVER (ORDER BY id GROUPS UNBOUNDED PRECEDING) FROM wr",
            "window frames",
        ),
        ("SELECT RANK() OVER w FROM wr", "named window"),
        ("SELECT LAG(sales) OVER () FROM wr", "window function lag"),
        ("SELECT LEAD(sales) OVER () FROM wr", "window function lead"),
        (
            "SELECT id FROM wr WHERE ROW_NUMBER() OVER ()=1",
            "window function in WHERE",
        ),
        (
            "SELECT id FROM wr GROUP BY RANK() OVER ()",
            "window function in GROUP BY",
        ),
        (
            "SELECT COUNT(*) FROM wr HAVING RANK() OVER ()=1",
            "window function in HAVING",
        ),
        (
            "SELECT RANK() OVER (ORDER BY ROW_NUMBER() OVER ()) FROM wr",
            "nested window",
        ),
        (
            "SELECT SUM(ROW_NUMBER() OVER ()) FROM wr",
            "inside aggregate",
        ),
        (
            "SELECT ROW_NUMBER(1) OVER () FROM wr",
            "arguments/modifiers",
        ),
    ] {
        let error = session
            .execute_sql(sql)
            .err()
            .unwrap_or_else(|| panic!("accepted {sql}"));
        assert!(
            error.message.starts_with("unsupported:") && error.message.contains(message),
            "{sql}: {error}"
        );
    }
    // October 18 window extras (old -> new): named windows and
    // PERCENT_RANK/CUME_DIST answer (DuckDB 1.5.6 and PostgreSQL 17 agree).
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT RANK() OVER w FROM wr WINDOW w AS (ORDER BY id) ORDER BY id"
        )),
        (1..=12).map(|n| vec![json!(n)]).collect::<Vec<_>>()
    );
    let batch = query(
        &mut session,
        "SELECT PERCENT_RANK() OVER (),CUME_DIST() OVER () FROM wr",
    );
    let column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>()
    };
    assert_eq!(column(0), vec![0.0_f64.to_bits(); 12]);
    assert_eq!(column(1), vec![1.0_f64.to_bits(); 12]);
}

#[test]
fn empty_in_validates_but_never_evaluates_window_keys() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, false);
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT ROW_NUMBER() OVER (ORDER BY 1.0/0) IN () AS v"
        )),
        vec![vec![json!(false)]]
    );
    assert!(
        session
            .execute_sql("SELECT ROW_NUMBER() OVER (ORDER BY missing) IN () FROM wr")
            .is_err()
    );
    assert!(
        session
            .execute_sql("SELECT LAG(id) OVER () IN () FROM wr")
            .is_err()
    );
}

#[test]
fn wildcards_keep_all_visible_columns_and_default_nulls_are_last() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, false);
    for wildcard in ["*", "wr.*"] {
        let batch = query(
            &mut session,
            &format!(
                "SELECT {wildcard}, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM wr ORDER BY rn"
            ),
        );
        assert_eq!(batch.num_columns(), 6);
        assert_eq!(batch.num_rows(), 12);
        assert_eq!(batch.schema().field(5).name(), "rn");
    }
    for direction in ["ASC", "DESC"] {
        let implicit = query(
            &mut session,
            &format!("SELECT id,RANK() OVER (ORDER BY sales {direction}) AS r FROM wr ORDER BY id"),
        );
        let explicit = query(
            &mut session,
            &format!(
                "SELECT id,RANK() OVER (ORDER BY sales {direction} NULLS LAST) AS r FROM wr ORDER BY id"
            ),
        );
        assert_eq!(implicit, explicit);
    }
    assert_eq!(
        rows(&query(
            &mut session,
            "SELECT DISTINCT DENSE_RANK() OVER (ORDER BY month) AS r FROM wr ORDER BY DENSE_RANK() OVER (ORDER BY month)"
        )),
        vec![vec![json!(1)], vec![json!(2)]]
    );
    let wide = query(
        &mut session,
        "SELECT id,RANK() OVER (ORDER BY 9223372036854775808+id) AS r FROM wr ORDER BY id",
    );
    for (i, row) in rows(&wide).iter().enumerate() {
        assert_eq!(row, &vec![json!(i + 1), json!(i + 1)]);
    }
}

#[test]
fn errors_in_keys_only_come_from_surviving_input_rows() {
    let fixture = Fixture::new();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = fixture.session(workers, batch_size, false);
            assert_eq!(
                query(
                    &mut session,
                    "SELECT ROW_NUMBER() OVER (ORDER BY 1.0/0) FROM wr WHERE id<0"
                )
                .num_rows(),
                0
            );
            for sql in [
                "SELECT RANK() OVER (ORDER BY 1.0/0) FROM wr",
                "SELECT DENSE_RANK() OVER (PARTITION BY 1.0/0) FROM wr",
            ] {
                assert_eq!(
                    session.execute_sql(sql).err().unwrap().message,
                    "division by zero"
                );
            }
            let competing = "SELECT RANK() OVER (ORDER BY CASE WHEN id=2 THEN 1.0/0 ELSE 0.0 END, CASE WHEN id=1 THEN 99999999999999999999999999999999999999*2 ELSE 0 END) FROM wr";
            let error = session.execute_sql(competing).err().unwrap();
            assert!(
                error.message.contains("overflow"),
                "canonical first row must supply the cause: {workers}/{batch_size}: {error}"
            );
        }
    }
}

#[test]
fn typed_float_zero_nan_and_exact_decimal_keys_use_existing_grouping_equality() {
    use arrow::datatypes::{Field, Schema};
    use assaydb::execute::{execute, prepare};
    use parquet::arrow::ArrowWriter;
    use std::{collections::HashMap, fs::File, sync::Arc};
    let fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
        Field::new("d", DataType::Decimal128(38, 2), true),
    ]));
    let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..8)),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(-0.0),
                Some(nan1),
                Some(nan1),
                Some(nan2),
                None,
                Some(0.5),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(9007199254740992_i128),
                    Some(9007199254740993),
                    Some(9007199254740992),
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(-1),
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
        ],
    )
    .unwrap();
    let path = fixture.directory.join("keys.parquet");
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    for offset in (0..batch.num_rows()).step_by(2) {
        writer.write(&batch.slice(offset, 2)).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);
    let sql = "SELECT id,RANK() OVER (ORDER BY f NULLS LAST) AS r,DENSE_RANK() OVER (ORDER BY f NULLS LAST) AS dr,ROW_NUMBER() OVER (PARTITION BY f ORDER BY id) AS n,RANK() OVER (ORDER BY d NULLS LAST) AS d FROM t ORDER BY id";
    let expected = vec![
        vec![0, 1, 1, 1, 5],
        vec![1, 1, 1, 2, 7],
        vec![2, 1, 1, 3, 5],
        vec![3, 5, 3, 1, 2],
        vec![4, 5, 3, 2, 2],
        vec![5, 5, 3, 3, 4],
        vec![6, 8, 4, 1, 8],
        vec![7, 4, 2, 1, 1],
    ]
    .into_iter()
    .map(|row| row.into_iter().map(|v| json!(v)).collect::<Vec<_>>())
    .collect::<Vec<_>>();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 16 << 20,
                    spill: spill.then(|| SpillOptions {
                        directory: fixture.directory.clone(),
                        sort_run_bytes: 64 << 10,
                        merge_fan_in: 2,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let result = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    rows(&result.batch),
                    expected,
                    "{workers}/{batch_size}/{spill}"
                );
                assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                assert!(result.metrics.worker_threads_peak <= workers);
                if workers > 1 {
                    assert!(
                        result.metrics.parallel_scan_tasks > 0,
                        "parallel scan must be exercised"
                    );
                }
                if spill {
                    assert!(result.metrics.external_sort_runs > 0);
                }
            }
        }
    }
    for spill in [false, true] {
        let options = Options {
            spill: spill.then(|| SpillOptions {
                directory: fixture.directory.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let result=execute(&prepare("SELECT id,f,ROW_NUMBER() OVER (PARTITION BY f ORDER BY id) AS n FROM t ORDER BY id",&tables,&options).unwrap(),&options).unwrap();
        let original = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let actual = result
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            actual
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            original
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn tight_budget_spills_both_key_order_and_ordinal_restore() {
    use assaydb::execute::{execute, prepare};
    use parquet::arrow::ArrowWriter;
    use std::{collections::HashMap, fs::File, sync::Arc};
    let fixture = Fixture::new();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..2048)) as ArrayRef,
        ),
        (
            "k",
            Arc::new(Int64Array::from_iter_values((0..2048).map(|i| i % 17))) as ArrayRef,
        ),
    ])
    .unwrap();
    let path = fixture.directory.join("large.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);
    let options = Options {
        memory_bytes: 4 << 20,
        batch_size: 7,
        spill: Some(SpillOptions {
            directory: fixture.directory.clone(),
            sort_run_bytes: 4096,
            merge_fan_in: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    let result = execute(
        &prepare(
            "SELECT id,ROW_NUMBER() OVER (PARTITION BY k ORDER BY id DESC) AS r FROM t",
            &tables,
            &options,
        )
        .unwrap(),
        &options,
    )
    .unwrap();
    let actual = rows(&result.batch);
    for (id, row) in actual.iter().enumerate() {
        assert_eq!(row[0], json!(id));
        assert_eq!(row[1], json!(1 + (2047 - id) / 17));
    }
    assert!(result.metrics.external_sort_runs > 2);
    assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
    let mut denied = options.clone();
    denied.spill.as_mut().unwrap().disk_quota_bytes = 1;
    assert!(
        execute(
            &prepare("SELECT RANK() OVER (ORDER BY id) FROM t", &tables, &denied).unwrap(),
            &denied
        )
        .err()
        .unwrap()
        .contains("resource-limit:")
    );
    assert!(fs::read_dir(&fixture.directory).unwrap().all(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|extension| extension == "parquet")
    }));
}

#[test]
fn upstream_limit_selects_the_same_rows_before_ranking_with_partitioned_spill() {
    let fixture = Fixture::new();
    let insert = format!(
        "INSERT INTO big VALUES {}",
        (0..128)
            .map(|i| format!("({i},{})", i % 17))
            .collect::<Vec<_>>()
            .join(",")
    );
    let cases = [
        (
            "SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS rn FROM (SELECT a.id AS id FROM big a JOIN big b ON a.id=b.id LIMIT 5) q",
            (0..5)
                .map(|i| vec![json!(i), json!(i + 1)])
                .collect::<Vec<_>>(),
            true,
        ),
        (
            "SELECT k,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM (SELECT k FROM big GROUP BY k LIMIT 5) q",
            (0..5).map(|i| vec![json!(i), json!(i + 1)]).collect(),
            false,
        ),
        (
            "SELECT k,id,ROW_NUMBER() OVER (ORDER BY k,id) AS rn FROM (SELECT k,id FROM big GROUP BY k,id ORDER BY k LIMIT 5) q",
            (0..5)
                .map(|i| vec![json!(0), json!(17 * i), json!(i + 1)])
                .collect(),
            false,
        ),
        (
            "SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS rn FROM (SELECT a.id AS id FROM big a JOIN big b ON a.id=b.id ORDER BY id DESC LIMIT 5) q ORDER BY id",
            (123..128).map(|i| vec![json!(i), json!(i - 122)]).collect(),
            true,
        ),
        (
            "WITH ranked AS (SELECT a.id AS id,ROW_NUMBER() OVER (ORDER BY a.id) AS inner_rank FROM big a JOIN big b ON a.id=b.id LIMIT 5) SELECT id,RANK() OVER (ORDER BY id) AS r FROM ranked",
            (0..5).map(|i| vec![json!(i), json!(i + 1)]).collect(),
            true,
        ),
        (
            "SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS rn FROM (SELECT a.id AS id FROM big a JOIN big b ON a.id=b.id ORDER BY ROW_NUMBER() OVER (ORDER BY b.k,a.id) LIMIT 5) q ORDER BY id",
            (0..5).map(|i| vec![json!(17 * i), json!(i + 1)]).collect(),
            true,
        ),
    ];
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let mut session = fixture.session(workers, batch_size, spill);
                session
                    .execute_sql("CREATE TABLE big(id INTEGER,k INTEGER)")
                    .unwrap();
                session.execute_sql(&insert).unwrap();
                for (sql, expected, join) in &cases {
                    let Output::Query { execution, .. } =
                        session.execute_sql(sql).unwrap_or_else(|error| {
                            panic!("{workers}/{batch_size}/{spill}: {sql}: {error}")
                        })
                    else {
                        panic!()
                    };
                    assert_eq!(
                        rows(&execution.batch),
                        *expected,
                        "{workers}/{batch_size}/{spill}: {sql}"
                    );
                    if spill {
                        let partitions = if *join {
                            execution.metrics.external_join_partitions
                        } else {
                            execution.metrics.external_aggregate_partitions
                        };
                        assert!(partitions > 1, "partitioned spill must be exercised: {sql}");
                    }
                }
                let error=session.execute_sql("WITH limited AS (SELECT a.id AS id FROM big a JOIN big b ON a.id=b.id LIMIT 5) SELECT id,ROW_NUMBER() OVER () FROM limited").err().expect("unresolved shared LIMIT must refuse");
                assert!(error.message.starts_with("unsupported: window input LIMIT requires canonical order inside a CTE materialization"),"{error}");
            }
        }
    }
}

#[test]
fn partial_sorts_over_scans_and_prior_windows_have_canonical_ties() {
    use assaydb::execute::{execute, prepare};
    use parquet::arrow::ArrowWriter;
    use std::{collections::HashMap, fs::File, sync::Arc};
    let fixture = Fixture::new();
    // Reverse physical order makes the completed tuple's id tie-break distinct
    // from source ordinals. Four row groups exercise actual parallel readers.
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values((0..128).rev())) as ArrayRef,
        ),
        (
            "k",
            Arc::new(Int64Array::from_iter_values((0..128).rev().map(|i| i % 17))) as ArrayRef,
        ),
    ])
    .unwrap();
    let path = fixture.directory.join("partial-sorts.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    for offset in (0..128).step_by(32) {
        writer.write(&batch.slice(offset, 32)).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let tables = HashMap::from([("t".to_string(), path)]);
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            for spill in [false, true] {
                let options = Options {
                    workers,
                    batch_size,
                    memory_bytes: 64 << 20,
                    spill: spill.then(|| SpillOptions {
                        directory: fixture.directory.clone(),
                        sort_run_bytes: 4096,
                        merge_fan_in: 2,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                for source in [
                    "t",
                    "(SELECT id,k,RANK() OVER (ORDER BY id) AS r FROM t) ranked",
                ] {
                    for limited in [false, true] {
                        let limit = if limited { " LIMIT 5" } else { "" };
                        let sql = format!(
                            "SELECT id,k,ROW_NUMBER() OVER (PARTITION BY k) AS rn FROM (SELECT id,k FROM {source} ORDER BY k DESC{limit}) q ORDER BY id"
                        );
                        let plan = prepare(&sql, &tables, &options)
                            .unwrap_or_else(|e| panic!("{sql}: {e}"));
                        let result = execute(&plan, &options).unwrap_or_else(|e| {
                            panic!("{workers}/{batch_size}/{spill}: {sql}: {e}")
                        });
                        let expected: Vec<_> = if limited {
                            (0..5)
                                .map(|i| vec![json!(16 + 17 * i), json!(16), json!(i + 1)])
                                .collect()
                        } else {
                            (0..128)
                                .map(|i| vec![json!(i), json!(i % 17), json!(i / 17 + 1)])
                                .collect()
                        };
                        assert_eq!(
                            rows(&result.batch),
                            expected,
                            "{workers}/{batch_size}/{spill}: {sql}"
                        );
                        if workers > 1 {
                            assert!(
                                result.metrics.parallel_scan_tasks > 0,
                                "actual parallel reader required: {sql}"
                            );
                        }
                        if spill {
                            assert!(
                                result.metrics.external_sort_runs > 1,
                                "actual external sorting required: {sql}"
                            );
                        }
                        assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn formerly_refused_exact_aggregate_windows_return_values() {
    let fixture = Fixture::new();
    let mut session = fixture.session(1, 7, false);
    let result = query(
        &mut session,
        "SELECT SUM(sales) OVER (),COUNT(*) OVER (),MIN(sales) OVER (),MAX(sales) OVER () FROM wr",
    );
    assert_eq!(
        rows(&result),
        vec![vec![json!(137), json!(12), json!(5), json!(30)]; 12]
    );
    let average = query(&mut session, "SELECT AVG(sales) OVER () FROM wr");
    let values = average
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[137.0 / 9.0; 12]);
}
