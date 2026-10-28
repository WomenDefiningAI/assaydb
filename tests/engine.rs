// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    memory::Budget,
};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{File, remove_file},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-test-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("d", DataType::Decimal128(15, 2), true),
            Field::new("g", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(2)])),
                Arc::new(
                    Decimal128Array::from(vec![Some(101), Some(250), None, Some(-50)])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec![Some("a"), None, None, Some("a")])),
            ],
        )
        .unwrap();
        let file = File::create_new(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn tables(&self) -> HashMap<String, PathBuf> {
        HashMap::from([("t".into(), self.0.clone())])
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        remove_file(&self.0).unwrap();
    }
}
fn integers(batch: &RecordBatch, col: usize) -> Vec<Option<i64>> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn coalesce_selects_first_non_null_and_short_circuits() {
    let fixture = Fixture::new();
    let options = Options::default();
    let p = prepare(
        "SELECT COALESCE(NULL, x, 9), COALESCE(g, 'fallback'), COALESCE(CAST(NULL AS BIGINT), NULL), COALESCE(NULL, d, 1.25) FROM t",
        &fixture.tables(),
        &options,
    )
    .unwrap();
    let batch = execute(&p, &options).unwrap().batch;
    assert_eq!(
        integers(&batch, 0),
        vec![Some(1), Some(2), Some(9), Some(2)]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("fallback"), Some("fallback"), Some("a")]
    );
    assert_eq!(integers(&batch, 2), vec![None; 4]);
    let decimal = batch
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(decimal.scale(), 2);
    assert_eq!(
        decimal.iter().collect::<Vec<_>>(),
        vec![Some(101), Some(250), Some(125), Some(-50)]
    );
    // Approved Int/Int division now binds Int; the dead divisor remains masked.
    let p = prepare(
        "SELECT COALESCE(7, CAST(1.0 / 0 AS INTEGER))",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    assert_eq!(
        execute(&p, &options)
            .unwrap()
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let p = prepare(
        "SELECT COALESCE(NULL, CAST(1.0 / 0 AS INTEGER))",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    assert!(
        execute(&p, &options)
            .err()
            .unwrap()
            .contains("division by zero")
    );
    let p = prepare(
        "SELECT SUM(COALESCE(x, 9)) FROM t",
        &fixture.tables(),
        &options,
    )
    .unwrap();
    assert_eq!(
        integers(&execute(&p, &options).unwrap().batch, 0),
        vec![Some(14)]
    );
    for sql in [
        "SELECT COALESCE()",
        "SELECT COALESCE(1)",
        "SELECT COALESCE(DISTINCT 1, 2)",
        "SELECT COALESCE(1, 'a')",
    ] {
        assert!(prepare(sql, &HashMap::new(), &options).is_err(), "{sql}");
    }
}
#[test]
fn distinct_set_operations_use_complete_rows_and_null_equality() {
    let fixture = Fixture::new();
    for workers in [1, 4] {
        let options = Options {
            workers,
            batch_size: 2,
            ..Options::default()
        };
        for (sql, expected) in [
            (
                "SELECT x FROM t UNION SELECT x FROM t",
                vec![Some(1), Some(2), None],
            ),
            (
                "SELECT x FROM t EXCEPT SELECT x FROM t WHERE x=2",
                vec![Some(1), None],
            ),
            (
                "SELECT x FROM t INTERSECT SELECT x FROM t WHERE x=2 OR x IS NULL",
                vec![Some(2), None],
            ),
            ("SELECT x FROM t EXCEPT SELECT x FROM t", vec![]),
            (
                "SELECT x FROM t WHERE FALSE INTERSECT SELECT x FROM t",
                vec![],
            ),
            (
                "SELECT x FROM t UNION DISTINCT SELECT x FROM t WHERE FALSE",
                vec![Some(1), Some(2), None],
            ),
            // Preserve GenericDialect / standard SQL INTERSECT precedence.
            (
                "SELECT 1 AS x UNION SELECT 2 AS x INTERSECT SELECT 2 AS x",
                vec![Some(1), Some(2)],
            ),
            (
                "SELECT x FROM t EXCEPT SELECT x FROM t WHERE x=2 UNION SELECT x FROM t WHERE x=2",
                vec![Some(1), None, Some(2)],
            ),
        ] {
            let p =
                prepare(sql, &fixture.tables(), &options).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let batch = execute(&p, &options)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .batch;
            let mut actual = integers(&batch, 0);
            let mut expected = expected;
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected, "{sql}; workers={workers}");
        }
        let p = prepare(
            "SELECT x AS a, g AS a FROM t UNION SELECT x, g FROM t",
            &fixture.tables(),
            &options,
        )
        .unwrap();
        let batch = execute(&p, &options).unwrap().batch;
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.schema().field(0).name(), "a");
        assert_eq!(batch.schema().field(1).name(), "a");
        let p = prepare(
            "SELECT 1 AS n UNION SELECT 1.00 AS n",
            &HashMap::new(),
            &options,
        )
        .unwrap();
        let batch = execute(&p, &options).unwrap().batch;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            100
        );
        // INTERSECT ALL / EXCEPT ALL now answer (DuckDB 1.5.6: one row 1; no rows).
        for (sql, expected) in [
            ("SELECT 1 INTERSECT ALL SELECT 1", vec![Some(1)]),
            ("SELECT 1 EXCEPT ALL SELECT 1", vec![]),
        ] {
            let p = prepare(sql, &HashMap::new(), &options).unwrap();
            let batch = execute(&p, &options).unwrap().batch;
            assert_eq!(batch.num_rows(), expected.len(), "{sql}");
            if !expected.is_empty() {
                assert_eq!(integers(&batch, 0), expected, "{sql}");
            }
        }
        for sql in ["SELECT 1 UNION SELECT 1,2", "SELECT 1 UNION SELECT 'a'"] {
            assert!(prepare(sql, &HashMap::new(), &options).is_err(), "{sql}");
        }
    }
}

#[test]
fn scalar_three_valued_logic() {
    let options = Options::default();
    let p=prepare("SELECT CAST(NULL AS BOOLEAN) AND FALSE AS a, CAST(NULL AS BOOLEAN) OR TRUE AS b, NOT CAST(NULL AS BOOLEAN) AS c",&HashMap::new(),&options).unwrap();
    let r = execute(&p, &options).unwrap();
    assert!(
        !r.batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        r.batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(r.batch.column(2).is_null(0));
}
#[test]
fn filtered_empty_scalar_aggregate() {
    let f = Fixture::new();
    let o = Options::default();
    let p = prepare(
        "SELECT COUNT(*) AS n, SUM(x) AS s, AVG(d) AS a FROM t WHERE x < 0",
        &f.tables(),
        &o,
    )
    .unwrap();
    let r = execute(&p, &o).unwrap();
    assert_eq!(integers(&r.batch, 0), vec![Some(0)]);
    assert!(r.batch.column(1).is_null(0));
    assert!(r.batch.column(2).is_null(0));
}
#[test]
fn grouped_nulls_duplicates_and_batches() {
    let f = Fixture::new();
    let mut previous = None;
    for n in [1, 2, 8192] {
        for projection_pushdown in [true, false] {
            let o = Options {
                batch_size: n,
                projection_pushdown,
                ..Options::default()
            };
            let p=prepare("SELECT g, COUNT(*) AS n, SUM(d) AS s, AVG(x) AS a FROM t GROUP BY g ORDER BY g DESC NULLS FIRST",&f.tables(),&o).unwrap();
            let r = execute(&p, &o).unwrap();
            assert_eq!(integers(&r.batch, 1), vec![Some(2), Some(2)]);
            assert_eq!(
                r.batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(250), Some(51)]
            );
            if let Some(last) = &previous {
                assert_eq!(&r.batch, last);
            }
            previous = Some(r.batch.clone());
        }
    }
}
#[test]
fn empty_grouped_output() {
    let f = Fixture::new();
    let o = Options::default();
    let p = prepare(
        "SELECT g, SUM(x) AS s FROM t WHERE FALSE GROUP BY g",
        &f.tables(),
        &o,
    )
    .unwrap();
    assert_eq!(execute(&p, &o).unwrap().batch.num_rows(), 0);
}
#[test]
fn projection_arithmetic_and_null_order() {
    let f = Fixture::new();
    let o = Options::default();
    let p = prepare(
        "SELECT x + 10 AS a FROM t WHERE x IS NOT NULL ORDER BY a DESC",
        &f.tables(),
        &o,
    )
    .unwrap();
    assert_eq!(p.plan.columns, vec![0]);
    let r = execute(&p, &o).unwrap();
    assert_eq!(integers(&r.batch, 0), vec![Some(12), Some(12), Some(11)]);
}
#[test]
fn no_from_where_is_respected() {
    let o = Options::default();
    let p = prepare(
        "SELECT 1 AS x WHERE CAST(NULL AS BIGINT) = 1",
        &HashMap::new(),
        &o,
    )
    .unwrap();
    assert_eq!(execute(&p, &o).unwrap().batch.num_rows(), 0);
}
#[test]
fn decimal_boundary_and_overflow() {
    let o = Options::default();
    let p = prepare(
        "SELECT CAST('9999999999999999.99' AS DECIMAL(18,2)) AS d",
        &HashMap::new(),
        &o,
    )
    .unwrap();
    let r = execute(&p, &o).unwrap();
    assert_eq!(
        r.batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        999999999999999999
    );
    let p = prepare("SELECT 9223372036854775807 + 1 AS x", &HashMap::new(), &o).unwrap();
    assert!(execute(&p, &o).is_err());
    assert!(
        prepare(
            "SELECT CAST('9999999999999999.99' AS DECIMAL(17,2)) AS x",
            &HashMap::new(),
            &o
        )
        .and_then(|p| execute(&p, &o))
        .err()
        .unwrap()
            == "decimal cast precision overflow"
    );
}
#[test]
fn unsupported_syntax_is_not_ignored() {
    let f = Fixture::new();
    let o = Options::default();
    for sql in ["SELECT x FROM t GROUP BY 1", "SELECT x FROM t ORDER BY x+1"] {
        let prepared = prepare(sql, &f.tables(), &o).unwrap();
        execute(&prepared, &o).unwrap();
    }
    for sql in ["SELECT x FROM t HAVING x>0", "SELECT x FROM t; SELECT 1"] {
        assert!(prepare(sql, &f.tables(), &o).is_err(), "{sql}");
    }
}
#[test]
fn approved_aggregate_window_sum_is_not_refused() {
    let f = Fixture::new();
    let o = Options::default();
    let p = prepare("SELECT SUM(x) OVER() FROM t", &f.tables(), &o).unwrap();
    assert_eq!(
        integers(&execute(&p, &o).unwrap().batch, 0),
        vec![Some(5); 4]
    );
}
#[test]
fn approved_distinct_deduplicates_values_and_nulls() {
    let f = Fixture::new();
    let o = Options::default();
    let p = prepare(
        "SELECT DISTINCT x FROM t ORDER BY x NULLS FIRST",
        &f.tables(),
        &o,
    )
    .unwrap();
    assert_eq!(
        integers(&execute(&p, &o).unwrap().batch, 0),
        vec![None, Some(1), Some(2)]
    );
}
#[test]
fn memory_limit_and_reservation_cleanup() {
    let budget = Budget::new(100);
    {
        let _lease = budget.reserve(60).unwrap();
        assert!(budget.reserve(60).is_err());
        assert_eq!(budget.used(), 60);
    }
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.peak(), 60);
    let o = Options {
        memory_bytes: 16,
        ..Options::default()
    };
    let p = prepare("SELECT 1 AS x", &HashMap::new(), &o).unwrap();
    assert!(
        execute(&p, &o)
            .err()
            .unwrap()
            .starts_with("resource-limit:")
    );
}

#[test]
fn literal_null_count() {
    let options = Options::default();
    let p = prepare(
        "SELECT COUNT(NULL) AS n, COUNT(*) AS all_rows",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    let r = execute(&p, &options).unwrap();
    assert_eq!(integers(&r.batch, 0), vec![Some(0)]);
    assert_eq!(integers(&r.batch, 1), vec![Some(1)]);
    let fixture = Fixture::new();
    for batch_size in [1, 8192] {
        let o = Options {
            batch_size,
            ..Options::default()
        };
        let p = prepare("SELECT g, COUNT(NULL) AS n, COUNT(*) AS all_rows FROM t GROUP BY g ORDER BY g NULLS FIRST", &fixture.tables(), &o).unwrap();
        let r = execute(&p, &o).unwrap();
        assert_eq!(integers(&r.batch, 1), vec![Some(0), Some(0)]);
        assert_eq!(integers(&r.batch, 2), vec![Some(2), Some(2)]);
    }
}

#[test]
fn literal_null_predicate() {
    let options = Options::default();
    let p = prepare(
        "SELECT NULL IS NULL AS yes, NULL IS NOT NULL AS no",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    let r = execute(&p, &options).unwrap();
    assert!(
        r.batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        !r.batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let p = prepare(
        "SELECT COUNT(*) AS n WHERE NULL IS NULL",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    assert_eq!(
        integers(&execute(&p, &options).unwrap().batch, 0),
        vec![Some(1)]
    );
    let p = prepare(
        "SELECT COUNT(*) AS n WHERE NULL IS NOT NULL",
        &HashMap::new(),
        &options,
    )
    .unwrap();
    assert_eq!(
        integers(&execute(&p, &options).unwrap().batch, 0),
        vec![Some(0)]
    );
}

#[test]
fn joins_preserve_multiplicity_nulls_and_on_residuals() {
    let f = Fixture::new();
    for batch_size in [1, 2, 8192] {
        let o = Options {
            batch_size,
            ..Options::default()
        };
        for (sql, expected) in [
            ("SELECT COUNT(*) AS n FROM t a JOIN t b ON a.x=b.x", 5),
            (
                "SELECT COUNT(*) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND b.d>1.00",
                4,
            ),
            (
                "SELECT COUNT(*) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND b.d>99.00",
                4,
            ),
            (
                "SELECT COUNT(*) AS n FROM t a LEFT SEMI JOIN t b ON a.x=b.x",
                3,
            ),
            (
                "SELECT COUNT(*) AS n FROM t a LEFT ANTI JOIN t b ON a.x=b.x",
                1,
            ),
            (
                "SELECT COUNT(b.x) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND b.d>99.00",
                0,
            ),
        ] {
            let p = prepare(sql, &f.tables(), &o).unwrap();
            assert_eq!(
                integers(&execute(&p, &o).unwrap().batch, 0),
                vec![Some(expected)],
                "{sql}"
            );
        }
    }
}
#[test]
fn derived_having_distinct_and_lazy_case() {
    let f = Fixture::new();
    let o = Options::default();
    let p=prepare("SELECT n, COUNT(*) AS k FROM (SELECT x, COUNT(*) AS n FROM t GROUP BY x HAVING COUNT(*)>0) a GROUP BY n ORDER BY n DESC LIMIT 1",&f.tables(),&o).unwrap();
    let r = execute(&p, &o).unwrap();
    assert_eq!(integers(&r.batch, 0), vec![Some(2)]);
    assert_eq!(integers(&r.batch, 1), vec![Some(1)]);
    let p = prepare(
        "SELECT COUNT(DISTINCT x) AS n, MIN(d) AS low, MAX(d) AS high FROM t",
        &f.tables(),
        &o,
    )
    .unwrap();
    let r = execute(&p, &o).unwrap();
    assert_eq!(integers(&r.batch, 0), vec![Some(2)]);
    assert_eq!(
        r.batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        -50
    );
    let p = prepare(
        "SELECT SUM(CASE WHEN x=2 THEN d ELSE 0 END) AS total FROM t",
        &f.tables(),
        &o,
    )
    .unwrap();
    let r = execute(&p, &o).unwrap();
    assert_eq!(
        r.batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        200
    );
    let p = prepare(
        "SELECT CASE WHEN TRUE THEN 3.0 ELSE CAST(1.0/0 AS INTEGER) END AS v",
        &HashMap::new(),
        &o,
    )
    .unwrap();
    let r = execute(&p, &o).unwrap();
    // Decimal(1) and the now-Int dead branch unify as Decimal(1), not Float.
    assert_eq!(r.batch.column(0).data_type(), &DataType::Decimal128(38, 1));
    assert_eq!(
        r.batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        30
    );
}

#[test]
fn subqueries_obey_null_empty_and_cardinality_rules() {
    let f = Fixture::new();
    let o = Options::default();
    for (sql, expected) in [
        (
            "SELECT COUNT(*) AS n FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND b.d>a.d)",
            1,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE b.x=a.x)",
            1,
        ),
        (
            "SELECT COUNT(*) AS n FROM t WHERE x NOT IN(SELECT x FROM t)",
            0,
        ),
        ("SELECT COUNT(*) AS n FROM t WHERE x IN(SELECT x FROM t)", 3),
        (
            "SELECT COUNT(*) AS n FROM t WHERE x NOT IN(SELECT x FROM t WHERE FALSE)",
            4,
        ),
        (
            "SELECT COUNT(*) AS n FROM t WHERE x IN(SELECT x FROM t WHERE FALSE)",
            0,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a WHERE d=(SELECT MIN(d) FROM t b WHERE b.x=a.x)",
            2,
        ),
        (
            "SELECT COUNT(*) AS n FROM t WHERE x=(SELECT MAX(x) FROM t)",
            2,
        ),
        (
            "SELECT COUNT(*) AS n FROM t WHERE x=(SELECT x FROM t WHERE FALSE)",
            0,
        ),
    ] {
        let p = prepare(sql, &f.tables(), &o).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            integers(&execute(&p, &o).unwrap().batch, 0),
            vec![Some(expected)],
            "{sql}"
        );
    }
    let p = prepare("SELECT (SELECT x FROM t) AS v", &f.tables(), &o).unwrap();
    assert!(execute(&p, &o).err().unwrap().contains("multiple rows"));
}

#[test]
fn correlated_count_missing_group_and_null_key() {
    let f = Fixture::new();
    let o = Options::default();
    for (sql, expected) in [
        (
            "SELECT (SELECT COUNT(*) FROM t b WHERE b.x=a.x) AS n FROM t a ORDER BY n",
            vec![Some(0), Some(1), Some(2), Some(2)],
        ),
        (
            "SELECT (SELECT COUNT(*)+1 FROM t b WHERE b.x=a.x AND b.d>99) AS n FROM t a ORDER BY n",
            vec![Some(1); 4],
        ),
    ] {
        let p = prepare(sql, &f.tables(), &o).unwrap();
        assert_eq!(integers(&execute(&p, &o).unwrap().batch, 0), expected);
    }
}

#[test]
fn relational_column_pruning_preserves_hidden_dependencies() {
    let f = Fixture::new();
    for sql in [
        "SELECT SUM(a.d) AS s FROM t a JOIN t b ON a.x=b.x",
        "SELECT a.x AS x FROM t a LEFT JOIN t b ON a.x=b.x AND b.d>a.d ORDER BY x",
        "SELECT x FROM (SELECT a.x AS x, SUM(b.d) AS s FROM t a JOIN t b ON a.x=b.x GROUP BY a.x ORDER BY s) q ORDER BY x",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON TRUE",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON FALSE",
        "SELECT CASE WHEN a.d>b.d THEN a.x ELSE b.x END AS x FROM t a JOIN t b ON a.x=b.x ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE a.x IN (SELECT b.x FROM t b WHERE b.d>0) ORDER BY x",
        "SELECT (SELECT COUNT(*) FROM t b WHERE b.x=a.x AND b.d>99) AS n FROM t a ORDER BY n",
    ] {
        let options = Options::default();
        let actual = execute(&prepare(sql, &f.tables(), &options).unwrap(), &options).unwrap();
        let off = Options {
            relational_optimizations: false,
            ..options
        };
        let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
        assert_eq!(actual.batch, expected.batch, "{sql}");
        if sql.starts_with("SELECT SUM") {
            assert!(
                actual.metrics.decoded_arrow_bytes_total
                    < expected.metrics.decoded_arrow_bytes_total
            );
        }
    }
    // Unused computed expressions still execute: projection must not hide errors.
    let sql = "SELECT COUNT(*) FROM (SELECT CAST(1.0/(a.x-a.x) AS INTEGER) AS bad FROM t a JOIN t b ON a.x=b.x) q";
    let o = Options::default();
    let p = prepare(sql, &f.tables(), &o).unwrap();
    assert!(execute(&p, &o).err().unwrap().contains("division by zero"));
}

#[test]
fn grouped_distinct_and_extrema_keep_separate_states() {
    let f = Fixture::new();
    for batch_size in [1, 2, 8192] {
        let o = Options {
            batch_size,
            ..Options::default()
        };
        let p = prepare("SELECT x, COUNT(DISTINCT d) AS n, SUM(DISTINCT x) AS s, MIN(g) AS low, MAX(d) AS high, AVG(d) AS a FROM t GROUP BY x ORDER BY x NULLS FIRST", &f.tables(), &o).unwrap();
        let result = execute(&p, &o).unwrap();
        assert_eq!(integers(&result.batch, 1), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(integers(&result.batch, 2), vec![None, Some(1), Some(2)]);
        let strings = result
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![None, Some("a"), Some("a")]
        );
        let max = result
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            max.iter().collect::<Vec<_>>(),
            vec![None, Some(101), Some(250)]
        );
        let avg = result
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            avg.iter().collect::<Vec<_>>(),
            vec![None, Some(1.01), Some(1.0)]
        );
    }
}

#[test]
fn positive_membership_pushdown_preserves_nulls_duplicates_and_outer_joins() {
    let f = Fixture::new();
    for sql in [
        "SELECT a.x AS x, b.d AS d FROM t a JOIN t b ON a.x=b.x WHERE a.x IN (SELECT x FROM t) ORDER BY x,d",
        "SELECT a.x AS x, b.d AS d FROM t a JOIN t b ON a.x=b.x WHERE b.d IN (SELECT d FROM t WHERE d>0) ORDER BY x,d",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.x=b.x WHERE a.x+b.x IN (SELECT x FROM t)",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.x=b.x WHERE a.x IN (SELECT x FROM t WHERE FALSE)",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.x=b.x WHERE a.x IN (SELECT x FROM t WHERE x IS NULL)",
        "SELECT a.x AS x, b.d AS d FROM t a LEFT JOIN t b ON a.x=b.x WHERE b.d IN (SELECT d FROM t) ORDER BY x,d",
        "SELECT a.x AS x FROM t a LEFT JOIN t b ON a.x=b.x WHERE a.x IN (SELECT x FROM t) ORDER BY x",
        "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.x=b.x WHERE a.x IN (SELECT x FROM t GROUP BY x HAVING COUNT(*)>1)",
        "SELECT COUNT(*) AS n FROM t WHERE x IN (SELECT x FROM t) OR x IS NULL",
        "SELECT COUNT(*) AS n FROM t WHERE x NOT IN (SELECT x FROM t)",
    ] {
        let on = Options::default();
        let off = Options {
            relational_optimizations: false,
            ..on.clone()
        };
        let run = |o: &Options| {
            execute(
                &prepare(sql, &f.tables(), o).unwrap_or_else(|e| panic!("{sql}: {e}")),
                o,
            )
            .unwrap()
        };
        assert_eq!(run(&on).batch, run(&off).batch, "{sql}");
    }
}

#[test]
fn reverse_existence_join_matches_forward_with_residuals_and_empty_sides() {
    use assaydb::rel::{JoinKind, Node, RelOp};
    fn force_reverse(node: &mut Node) {
        match &mut node.op {
            RelOp::Join {
                left, right, kind, ..
            } => {
                force_reverse(left);
                force_reverse(right);
                if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                    left.estimated_rows = 0;
                    right.estimated_rows = 1;
                }
            }
            RelOp::Unary { input, .. }
            | RelOp::Filter { input, .. }
            | RelOp::Project { input, .. }
            | RelOp::Rename { input }
            | RelOp::Limit { input, .. } => force_reverse(input),
            _ => {}
        }
    }
    let f = Fixture::new();
    for sql in [
        "SELECT a.x AS x, a.d AS d FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.x=a.x) ORDER BY x,d",
        "SELECT a.x AS x, a.d AS d FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE b.x=a.x) ORDER BY x,d",
        "SELECT a.x AS x, a.d AS d FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND b.d<a.d) ORDER BY x,d",
        "SELECT a.x AS x, a.d AS d FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND b.d<a.d) ORDER BY x,d",
        "SELECT a.x AS x FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.d>a.d) ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE FALSE AND b.x=a.x) ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE FALSE AND EXISTS(SELECT 1 FROM t b WHERE b.x=a.x) ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND b.g=a.g) ORDER BY x",
    ] {
        for batch_size in [1, 2, 8192] {
            let o = Options {
                batch_size,
                ..Options::default()
            };
            let mut p = prepare(sql, &f.tables(), &o).unwrap();
            force_reverse(p.relational.as_mut().unwrap());
            let actual = execute(&p, &o).unwrap();
            assert!(actual.metrics.reverse_existence_joins > 0, "{sql}");
            let off = Options {
                relational_optimizations: false,
                ..o
            };
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
        }
    }
}

#[test]
fn dynamic_scan_keys_preserve_nulls_projection_and_residuals() {
    use assaydb::rel::{JoinKind, Node, RelOp};
    let mut numeric_pair_filters = 0;
    fn encourage_filter(node: &mut Node) {
        node.estimated_rows = 10000;
        match &mut node.op {
            RelOp::Scan { distinct, .. } => distinct.fill(10000),
            RelOp::Join {
                left, right, kind, ..
            } => {
                encourage_filter(left);
                encourage_filter(right);
                if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
                    left.estimated_rows = 1;
                }
            }
            RelOp::Unary { input, .. }
            | RelOp::Filter { input, .. }
            | RelOp::Project { input, .. }
            | RelOp::Rename { input }
            | RelOp::Materialize { input, .. }
            | RelOp::Limit { input, .. } => encourage_filter(input),
            _ => {}
        }
    }
    let f = Fixture::new();
    for sql in [
        "SELECT a.x AS x, a.d AS d, a.g AS g FROM t a JOIN t b ON a.x=b.x WHERE b.d>0 ORDER BY x,d,g",
        "SELECT a.x AS x, a.g AS g, a.d AS d FROM t a JOIN t b ON a.d=b.d WHERE b.x=2 ORDER BY d,g,x",
        "SELECT a.x AS x, a.d AS d, a.g AS g FROM t a JOIN t b ON a.x=b.x AND a.d=b.d ORDER BY x,d,g",
        "SELECT a.x AS x FROM t a JOIN (SELECT x FROM t WHERE x IS NULL) b ON a.x=b.x ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND (b.d<a.d OR b.g=a.g)) ORDER BY x",
        "SELECT a.x AS x FROM t a WHERE NOT EXISTS(SELECT 1 FROM t b WHERE b.x=a.x AND (b.d<a.d OR b.g=a.g)) ORDER BY x",
        "SELECT a.x AS x, a.d AS d, a.g AS g FROM t a JOIN t b ON a.x=b.x JOIN t c ON b.x=c.x WHERE c.d>0 ORDER BY x,d,g",
    ] {
        for batch_size in [1, 2, 8192] {
            let o = Options {
                batch_size,
                ..Options::default()
            };
            let mut p = prepare(sql, &f.tables(), &o).unwrap();
            encourage_filter(p.relational.as_mut().unwrap());
            let actual = execute(&p, &o).unwrap();
            assert!(
                actual.metrics.dynamic_scan_filters > 0,
                "{sql}: {:?}",
                p.relational
            );
            assert!(actual.metrics.scan_decode_seconds >= 0.0);
            numeric_pair_filters += actual.metrics.numeric_pair_scan_filters;
            let off = Options {
                relational_optimizations: false,
                ..o
            };
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
            assert_eq!(
                actual.metrics.rows_read + 4 * actual.metrics.scan_reuses,
                expected.metrics.rows_read,
                "{sql}"
            );
        }
    }
    assert!(numeric_pair_filters > 0);
}

#[test]
fn on_pushdown_keeps_left_and_anti_preserved_rows() {
    let f = Fixture::new();
    let o = Options::default();
    for (sql, count) in [
        (
            "SELECT COUNT(*) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND a.d>99",
            4,
        ),
        (
            "SELECT COUNT(b.x) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND a.d>99",
            0,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a LEFT ANTI JOIN t b ON a.x=b.x AND a.d>99",
            4,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a LEFT SEMI JOIN t b ON a.x=b.x AND a.d>99",
            0,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a LEFT JOIN t b ON a.x=b.x AND b.d>99",
            4,
        ),
        (
            "SELECT COUNT(*) AS n FROM t a LEFT ANTI JOIN t b ON a.x=b.x AND b.d>99",
            4,
        ),
    ] {
        let p = prepare(sql, &f.tables(), &o).unwrap();
        assert_eq!(
            integers(&execute(&p, &o).unwrap().batch, 0),
            vec![Some(count)],
            "{sql}"
        );
    }
    assert!(prepare("SELECT COUNT(*) FROM t a JOIN t b ON x>0", &f.tables(), &o).is_err());
}

#[test]
fn shared_aggregate_expressions_preserve_nulls_distinct_and_lazy_case() {
    let f = Fixture::new();
    for sql in [
        "SELECT SUM(d*(1-x)) AS a, SUM(d*(1-x)*(1+x)) AS b, AVG(d*(1-x)) AS c FROM t",
        "SELECT g, SUM(d*(1-x)) AS a, AVG(d*(1-x)) AS b, SUM(d*(1-x)*(1+x)) AS c FROM t GROUP BY g ORDER BY g NULLS FIRST",
        "SELECT AVG(DISTINCT x) AS a, SUM(DISTINCT x) AS b, SUM(x) AS c, AVG(x) AS d, COUNT(DISTINCT x) AS n FROM t",
        "SELECT SUM(CASE WHEN x<0 THEN 99999999999999999999999999999999999999+1 ELSE x END) AS a, AVG(CASE WHEN x<0 THEN 99999999999999999999999999999999999999+1 ELSE x END) AS b FROM t",
        "SELECT SUM(d*(1-x)) AS a, AVG(d*(1-x)) AS b FROM t WHERE FALSE",
    ] {
        for batch_size in [1, 2, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let run = |o: &Options| {
                execute(
                    &prepare(sql, &f.tables(), o).unwrap_or_else(|e| panic!("{sql}: {e}")),
                    o,
                )
                .unwrap()
            };
            let actual = run(&on);
            let expected = run(&off);
            assert_eq!(actual.batch, expected.batch, "{sql}");
            assert!(actual.metrics.shared_aggregate_states > 0, "{sql}");
            assert_eq!(expected.metrics.shared_aggregate_states, 0);
            if sql.contains("*(1+x)") {
                assert!(actual.metrics.shared_expression_evaluations > 0);
            }
        }
    }
}

#[test]
fn repeated_ctes_reuse_only_within_one_execution() {
    let f = Fixture::new();
    for sql in [
        "WITH c AS (SELECT x,d FROM t) SELECT SUM(a.d) AS s FROM c a JOIN c b ON a.x=b.x",
        "WITH c AS (SELECT x, SUM(d) AS s FROM t GROUP BY x) SELECT SUM(s) AS n FROM c WHERE s=(SELECT MAX(s) FROM c)",
        "WITH c AS (SELECT x FROM t WHERE FALSE) SELECT COUNT(*) AS n FROM c a JOIN c b ON a.x=b.x",
        "WITH c AS (SELECT x FROM t) SELECT COUNT(*) AS n FROM c WHERE x NOT IN (SELECT x FROM c)",
        "WITH a AS (SELECT x,d FROM t), b AS (SELECT x,SUM(d) AS s FROM a GROUP BY x) SELECT SUM(s) AS n FROM b WHERE s=(SELECT MAX(s) FROM b)",
        "WITH c AS (SELECT x,d FROM t ORDER BY x,d LIMIT 2) SELECT COUNT(*) AS n FROM c a JOIN c b ON a.x=b.x",
    ] {
        for batch_size in [1, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let p = prepare(sql, &f.tables(), &on).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            for _ in 0..2 {
                let actual = execute(&p, &on).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}");
                assert_eq!(actual.metrics.rows_read, 4, "{sql}");
                assert!(actual.metrics.cte_reuses >= 1, "{sql}");
                assert!(actual.metrics.rows_read < expected.metrics.rows_read);
            }
        }
    }
    let o = Options::default();
    let p = prepare(
        "WITH c AS (SELECT x FROM t) SELECT COUNT(*) FROM c",
        &f.tables(),
        &o,
    )
    .unwrap();
    assert_eq!(execute(&p, &o).unwrap().metrics.cte_materializations, 0);
}

#[test]
fn direct_numeric_groups_preserve_nulls_decimals_and_extreme_values() {
    let f = Fixture::new();
    for sql in [
        "SELECT x, COUNT(*), SUM(d), AVG(d), COUNT(DISTINCT d) FROM t GROUP BY x ORDER BY x",
        "SELECT d, COUNT(*), MIN(x), MAX(x) FROM t GROUP BY d ORDER BY d",
        "SELECT CAST(NULL AS DECIMAL(38,0)) AS n, COUNT(*) FROM t GROUP BY CAST(NULL AS DECIMAL(38,0))",
        "SELECT k, COUNT(*) FROM (SELECT CASE WHEN x=1 THEN -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) WHEN x=2 THEN CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) ELSE NULL END AS k FROM t) a GROUP BY k ORDER BY k",
        "SELECT x, SUM(d) FROM t WHERE FALSE GROUP BY x",
    ] {
        for batch_size in [1, 2, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
        }
    }
}

#[test]
fn demanded_aggregate_groups_preserve_duplicates_nulls_and_empty_defaults() {
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(15, 2), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..256).map(|i| (i % 17 != 0).then_some(i % 128)),
            )),
            Arc::new(
                Decimal128Array::from_iter((0..256).map(|i| (i % 13 != 0).then_some(i * 7)))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    // Replace only this test's freshly-created, exclusively owned fixture.
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut exercised = 0;
    let mut cached_demand = 0;
    for sql in [
        "SELECT a.x, a.d, (SELECT AVG(b.d) FROM t b WHERE b.x=a.x) AS v FROM t a WHERE a.x IN (1,3) ORDER BY 1,2",
        "SELECT a.x, a.d, (SELECT COUNT(*) FROM t b WHERE b.x=a.x+1000) AS v FROM t a WHERE a.x IN (1,3) ORDER BY 1,2",
        "SELECT a.x, a.d, (SELECT SUM(b.d) FROM t b WHERE b.x=a.x AND b.d IS NULL) AS v FROM t a WHERE a.x IN (1,3) ORDER BY 1,2",
        "SELECT a.x, a.d, (SELECT COUNT(*) FROM t b WHERE b.x=a.x) AS v FROM t a WHERE a.x IS NULL ORDER BY 2",
        "SELECT a.x, a.d, (SELECT MIN(b.d) FROM t b WHERE b.x=a.x) AS v FROM t a WHERE FALSE ORDER BY 1,2",
        "SELECT a.x, a.d, g.s FROM t a LEFT JOIN (SELECT x, SUM(d) AS s FROM t GROUP BY x) g ON a.x=g.x AND a.d<g.s WHERE a.x IN (1,3) ORDER BY 1,2",
        "SELECT a.x, a.d, g.s FROM t a LEFT JOIN (SELECT x, SUM(d) AS s FROM t GROUP BY x ORDER BY x LIMIT 3) g ON a.x=g.x WHERE a.x IN (100,101) ORDER BY 1,2",
    ] {
        for batch_size in [1, 37, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
            exercised += actual.metrics.demand_filtered_aggregates;
            if actual.metrics.demand_filtered_aggregates > 0
                && actual.metrics.cached_scan_filters > 0
                && actual.metrics.scan_reuses > 0
            {
                cached_demand += 1;
            }
        }
    }
    assert!(
        cached_demand >= 3,
        "demand filters must also apply after shared cache lookup"
    );
    assert!(
        exercised >= 3,
        "the selective grouping path must actually run"
    );
}

#[test]
fn shared_physical_scans_preserve_filters_outer_rows_and_execution_scope() {
    let f = Fixture::new();
    for sql in [
        "SELECT a.x,a.d,b.d AS bd FROM t a JOIN t b ON a.x=b.x ORDER BY 1,2,3",
        "SELECT a.x,b.d FROM t a JOIN t b ON a.x=b.x ORDER BY 1,2",
        "SELECT a.x,a.d,b.d AS bd FROM t a LEFT JOIN t b ON a.x=b.x AND a.d<b.d ORDER BY 1,2,3",
        "SELECT COUNT(*) FROM (SELECT x,d FROM t WHERE d>1 UNION ALL SELECT x,d FROM t WHERE d<2) s",
    ] {
        for batch_size in [1, 2, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let plan = prepare(sql, &f.tables(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            for _ in 0..2 {
                let actual = execute(&plan, &on).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}");
                assert!(actual.metrics.scan_reuses > 0, "{sql}");
                assert_eq!(actual.metrics.rows_read, 4, "{sql}");
                assert_eq!(expected.metrics.rows_read, 8, "{sql}");
            }
        }
    }
    let on = Options::default();
    for sql in [
        "SELECT x FROM t",
        "SELECT a.d,b.g FROM t a JOIN t b ON a.x=b.x",
    ] {
        let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
        assert_eq!(actual.metrics.scan_materializations, 0, "{sql}");
    }
}

#[test]
fn redundant_join_keys_require_proven_child_equalities() {
    use assaydb::rel::{Node, RelOp};
    fn widest_key(node: &Node) -> usize {
        match &node.op {
            RelOp::Join {
                left, right, keys, ..
            } => keys.len().max(widest_key(left)).max(widest_key(right)),
            RelOp::Unary { input, .. }
            | RelOp::Rename { input }
            | RelOp::Filter { input, .. }
            | RelOp::Project { input, .. }
            | RelOp::Materialize { input, .. } => widest_key(input),
            _ => 0,
        }
    }
    let f = Fixture::new();
    for (sql, reduced) in [
        (
            "SELECT a.x,a.d,b.d AS bd,c.d AS cd FROM t a JOIN t b ON a.x=b.x JOIN t c ON a.x=c.x AND b.x=c.x AND c.x=a.x ORDER BY 1,2,3,4",
            true,
        ),
        (
            "SELECT s.x,s.y,c.d FROM (SELECT a.x AS x,b.x AS y FROM t a JOIN t b ON a.x=b.x) s JOIN t c ON c.x=s.x AND c.x=s.y ORDER BY 1,2,3",
            true,
        ),
        (
            "SELECT a.x,a.d,b.d AS bd,c.d AS cd FROM t a LEFT JOIN t b ON a.x=b.x AND b.d<0 JOIN t c ON a.x=c.x AND b.x=c.x ORDER BY 1,2,3,4",
            false,
        ),
        (
            "SELECT a.x,a.d,b.d AS bd,c.d AS cd FROM t a LEFT JOIN t b ON a.x=b.x AND b.d<0 LEFT JOIN t c ON a.x=c.x AND b.x=c.x ORDER BY 1,2,3,4",
            false,
        ),
        (
            "SELECT a.x,a.d,b.d AS bd,c.d AS cd FROM t a JOIN t b ON a.x=b.x JOIN t c ON a.d=c.d AND b.d=c.d ORDER BY 1,2,3,4",
            false,
        ),
    ] {
        for batch_size in [1, 2, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let plan = prepare(sql, &f.tables(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            let actual = execute(&plan, &on).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
            if reduced {
                assert_eq!(widest_key(plan.relational.as_ref().unwrap()), 1, "{sql}");
            }
        }
    }
}

#[test]
fn adaptive_numeric_groups_match_checked_execution_across_batches() {
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(15, 2), true),
    ]));
    let keys = (0..2048).map(|i| {
        if i % 19 == 0 {
            return None;
        }
        Some(match i / 512 {
            0 => i % 128,
            1 => -(i % 128) - 1,
            2 => i % 128,
            _ => {
                if i % 2 == 0 {
                    i64::MIN
                } else {
                    i64::MAX
                }
            }
        })
    });
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(keys)),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..2048).map(|i| (i % 13 != 0).then_some((i % 47 - 23) * 101)),
                )
                .with_precision_and_scale(15, 2)
                .unwrap(),
            ),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut rebuilds = 0;
    for sql in [
        "SELECT x,COUNT(*),COUNT(d),SUM(d),AVG(d),MIN(d),MAX(d),COUNT(DISTINCT d),SUM(DISTINCT d) FROM t GROUP BY x ORDER BY x",
        "SELECT x,SUM(d) FROM t GROUP BY x ORDER BY x",
        "SELECT x,COUNT(*) FROM t WHERE x IS NULL GROUP BY x",
        "SELECT x,COUNT(*) FROM t WHERE FALSE GROUP BY x",
    ] {
        for batch_size in [7, 64, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
            rebuilds += actual.metrics.dense_group_rebuilds;
            assert_eq!(expected.metrics.dense_group_rebuilds, 0);
        }
    }
    assert!(rebuilds >= 4);
}

#[test]
fn residual_index_filter_preserves_wide_payload_outer_and_existence_results() {
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(15, 2), true),
        Field::new("g", DataType::Utf8, true),
    ]));
    let strings: Vec<_> = (0..96)
        .map(|i| {
            (i % 11 != 0).then(|| format!("{i}:{}", "é雪".repeat(if i == 7 { 16384 } else { 32 })))
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..96).map(|i| (i % 13 != 0).then_some(i % 7)),
            )),
            Arc::new(
                Decimal128Array::from_iter(
                    (0..96).map(|i| (i % 17 != 0).then_some((i % 23 - 11) * 101)),
                )
                .with_precision_and_scale(15, 2)
                .unwrap(),
            ),
            Arc::new(StringArray::from_iter(strings.iter().map(|s| s.as_deref()))),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut candidates = 0;
    for sql in [
        "SELECT a.g,b.g,a.d,b.d FROM t a JOIN t b ON a.x=b.x AND a.d<b.d ORDER BY 1,2,3,4",
        "SELECT a.g,b.g,a.d,b.d FROM t a LEFT JOIN t b ON a.x=b.x AND a.d<b.d ORDER BY 1,2,3,4",
        "SELECT a.g,b.g,a.d,b.d FROM t a LEFT JOIN t b ON a.x=b.x AND a.d+b.d < -99999 ORDER BY 1,2,3,4",
        "SELECT a.g FROM t a LEFT SEMI JOIN t b ON a.x=b.x AND a.d<b.d ORDER BY 1",
        "SELECT a.g FROM t a LEFT ANTI JOIN t b ON a.x=b.x AND a.d<b.d ORDER BY 1",
        "SELECT a.g,b.g FROM t a JOIN t b ON a.x=b.x AND (a.d<b.d OR a.g=b.g) ORDER BY 1,2",
        "SELECT a.g,b.g FROM t a JOIN t b ON a.x=b.x AND (CASE WHEN a.x<0 THEN CAST(1.0/0 AS INTEGER) ELSE a.d END)<b.d ORDER BY 1,2",
    ] {
        for batch_size in [1, 7, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
            assert_eq!(expected.metrics.residual_candidate_rows, 0);
            candidates += actual.metrics.residual_candidate_rows;
        }
    }
    assert!(candidates > 1000);
    let sql = "SELECT a.g,b.g FROM t a JOIN t b ON a.x=b.x AND (CASE WHEN a.x=1 THEN CAST(1.0/0 AS INTEGER) ELSE a.d END)<b.d";
    for relational_optimizations in [false, true] {
        let o = Options {
            relational_optimizations,
            ..Options::default()
        };
        let err = execute(&prepare(sql, &f.tables(), &o).unwrap(), &o)
            .err()
            .unwrap();
        assert!(err.contains("division by zero"), "{err}");
    }
}

#[test]
fn compact_string_keys_match_sortable_encoding_for_groups_and_join_variants() {
    let f = Fixture::new();
    let long = "é雪".repeat(4096);
    let samples = [
        None,
        Some(""),
        Some("a"),
        Some("ab"),
        Some("bc"),
        Some("c"),
        Some("\0"),
        Some("ÿ"),
        Some(long.as_str()),
    ];
    let count = samples.len() * samples.len() * 2;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("n", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter(
                (0..count).map(|i| samples[i % samples.len()]),
            )),
            Arc::new(StringArray::from_iter(
                (0..count).map(|i| samples[(i / samples.len()) % samples.len()]),
            )),
            Arc::new(Int64Array::from_iter_values((0..count).map(|i| i as i64))),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for sql in [
        "SELECT a,COUNT(*),SUM(n) FROM t GROUP BY a ORDER BY a",
        "SELECT a,b,COUNT(*),SUM(n) FROM t GROUP BY a,b ORDER BY a,b",
        "SELECT a FROM t GROUP BY a ORDER BY a",
        "SELECT l.a,l.b,COUNT(*) FROM t l JOIN t r ON l.a=r.a AND l.b=r.b AND l.n<r.n GROUP BY l.a,l.b ORDER BY 1,2",
        "SELECT l.a,l.b,COUNT(r.n) FROM t l LEFT JOIN t r ON l.a=r.a AND l.b=r.b AND l.n<r.n GROUP BY l.a,l.b ORDER BY 1,2",
        "SELECT l.a,l.b,l.n FROM t l LEFT SEMI JOIN t r ON l.a=r.a AND l.b=r.b AND l.n<r.n ORDER BY 1,2,3",
        "SELECT l.a,l.b,l.n FROM t l LEFT ANTI JOIN t r ON l.a=r.a AND l.b=r.b AND l.n<r.n ORDER BY 1,2,3",
    ] {
        for batch_size in [1, 7, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
            let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
            assert_eq!(actual.batch, expected.batch, "{sql}");
        }
    }
}

#[test]
fn composite_scan_filter_costs_use_joint_domains_without_changing_answers() {
    use assaydb::rel::{Node, RelOp};
    fn actual_domains(node: &mut Node) {
        match &mut node.op {
            RelOp::Scan {
                columns, distinct, ..
            } => {
                for (i, column) in columns.iter().enumerate() {
                    distinct[i] = [24, 32, 1536, 1536][*column];
                }
            }
            RelOp::Join { left, right, .. } => {
                actual_domains(left);
                actual_domains(right);
            }
            RelOp::Unary { input, .. }
            | RelOp::Filter { input, .. }
            | RelOp::Project { input, .. }
            | RelOp::Rename { input }
            | RelOp::Materialize { input, .. }
            | RelOp::Limit { input, .. } => actual_domains(input),
            _ => {}
        }
    }
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("z", DataType::Int64, true),
        Field::new("n", DataType::Int64, false),
        Field::new("d", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..1536).map(|i| (i % 17 != 0).then_some(i % 24)),
            )),
            Arc::new(Int64Array::from_iter(
                (0..1536).map(|i| (i % 19 != 0).then_some((i / 24) % 32)),
            )),
            Arc::new(Int64Array::from_iter_values(0..1536)),
            Arc::new(StringArray::from_iter_values(
                (0..1536).map(|i| format!("{i}:{}", "payload".repeat(16))),
            )),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for (sql, pair, expects_filter) in [
        (
            "SELECT SUM(l.n),MAX(l.d) FROM t l JOIN (SELECT x,z FROM t WHERE n IN (1,26)) r ON l.x=r.x AND l.z=r.z",
            true,
            true,
        ),
        (
            "SELECT SUM(l.n),MAX(l.d) FROM t l JOIN (SELECT x FROM t WHERE n IN (1,26,3,4)) r ON l.x=r.x",
            false,
            true,
        ),
        (
            "SELECT SUM(l.n),MAX(l.d) FROM (SELECT x,n,d FROM t WHERE n<1000) l JOIN (SELECT x FROM t WHERE n IN (1,26,3,4)) r ON l.x=r.x",
            false,
            false,
        ),
    ] {
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 31, 8192] {
                let on = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let off = Options {
                    workers: 1,
                    relational_optimizations: false,
                    ..on.clone()
                };
                let mut p = prepare(sql, &f.tables(), &on).unwrap();
                actual_domains(p.relational.as_mut().unwrap());
                let actual = execute(&p, &on).unwrap();
                let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
                assert_eq!(actual.batch, expected.batch);
                assert_eq!(
                    actual.metrics.dynamic_scan_filters > 0,
                    expects_filter,
                    "{sql}"
                );
                if expects_filter {
                    assert!(actual.metrics.cached_scan_filters > 0);
                }
                if pair {
                    assert!(actual.metrics.numeric_pair_scan_filters > 0);
                } else {
                    assert_eq!(actual.metrics.bitmap_scan_filters > 0, expects_filter);
                    let sync = Options {
                        parquet_cache_bytes: None,
                        ..on.clone()
                    };
                    let mut p = prepare(sql, &f.tables(), &sync).unwrap();
                    actual_domains(p.relational.as_mut().unwrap());
                    let prior = execute(&p, &sync).unwrap();
                    assert_eq!(prior.batch, expected.batch);
                    assert_eq!(prior.metrics.dynamic_scan_filters, 0);
                }
            }
        }
    }
}

#[test]
fn metadata_costs_preserve_filtered_join_results_with_and_without_statistics() {
    use assaydb::rel::RelOp;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("x", DataType::Int64, false),
        Field::new("d", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..1000).map(|i| (i % 11 != 0).then_some(i % 31)),
            )),
            Arc::new(Int64Array::from_iter_values(0..1000)),
            Arc::new(Date32Array::from_iter_values(10000..11000)),
        ],
    )
    .unwrap();
    fn filter_estimate(node: &assaydb::rel::Node) -> Option<usize> {
        match &node.op {
            RelOp::Filter { .. } => Some(node.estimated_rows),
            RelOp::Unary { input, .. } | RelOp::Rename { input } | RelOp::Project { input, .. } => {
                filter_estimate(input)
            }
            RelOp::Join { left, right, .. } => {
                filter_estimate(left).or_else(|| filter_estimate(right))
            }
            _ => None,
        }
    }
    for statistics in [EnabledStatistics::Chunk, EnabledStatistics::None] {
        let props = WriterProperties::builder()
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(113)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&f.0).unwrap(), schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        for batch_size in [1, 37, 8192] {
            let on = Options {
                batch_size,
                ..Options::default()
            };
            let off = Options {
                relational_optimizations: false,
                ..on.clone()
            };
            for sql in [
                "SELECT COUNT(*),SUM(l.x) FROM (SELECT k,x FROM t WHERE x>=100 AND x<110 AND x>=100) l JOIN t r ON l.k=r.k",
                "SELECT COUNT(*),COUNT(r.x),SUM(l.x) FROM (SELECT k,x FROM t WHERE x>=100 AND x<110) l LEFT JOIN t r ON l.k=r.k AND r.x<50",
                "SELECT COUNT(*),SUM(l.x) FROM (SELECT k,x FROM t WHERE d>=DATE '1997-05-19' AND d<DATE '1997-05-29') l LEFT ANTI JOIN t r ON l.k=r.k AND r.x<5",
                "SELECT COUNT(*),SUM(l.x) FROM (SELECT k,x FROM t WHERE k IS NULL) l LEFT JOIN t r ON l.k=r.k",
                "SELECT COUNT(*) FROM (SELECT k,x FROM t WHERE x>500 AND x<100) l JOIN t r ON l.k=r.k",
            ] {
                let prepared = prepare(sql, &f.tables(), &on).unwrap();
                let actual = execute(&prepared, &on).unwrap();
                let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}");
                if sql.contains("AND x>=100") {
                    let estimate = filter_estimate(prepared.relational.as_ref().unwrap()).unwrap();
                    if statistics == EnabledStatistics::None {
                        assert_eq!(estimate, 250);
                    } else {
                        assert!((10..=11).contains(&estimate), "{estimate}");
                    }
                }
            }
            let sql = "SELECT SUM(v) FROM (SELECT x*10000000000000000000000000000000000000 AS v FROM t) l WHERE v<0";
            assert!(execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).is_err());
            assert!(execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).is_err());
        }
    }
}

#[test]
fn static_scan_predicates_keep_nulls_errors_dictionary_pages_and_reader_fallbacks() {
    use parquet::file::properties::WriterProperties;
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("n", DataType::Int64, false),
        Field::new("d", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..2048).map(|i| (i % 17 != 0).then_some(i)),
            )),
            Arc::new(Int64Array::from_iter_values(0..2048)),
            Arc::new(StringArray::from_iter((0..2048).map(|i| {
                (i % 19 != 0).then(|| format!("{i}:{}", "payload".repeat(16)))
            }))),
        ],
    )
    .unwrap();
    for dictionary in [false, true] {
        let props = WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_max_row_group_size(73)
            .set_data_page_size_limit(128)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&f.0).unwrap(), schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        for batch_size in [7, 8192] {
            for (sql, eligible) in [
                (
                    "SELECT SUM(n),MAX(d) FROM (SELECT x,n,d FROM t WHERE (x>=30 AND x<42) OR x IS NULL) a",
                    false,
                ),
                (
                    "SELECT SUM(n),MAX(d) FROM (SELECT x,n,d FROM t WHERE x<0) a",
                    true,
                ),
                (
                    "SELECT SUM(n),MAX(d) FROM (SELECT x,n,d FROM t WHERE x<42 AND d IS NOT NULL) a",
                    true,
                ),
                (
                    "SELECT SUM(n),MAX(d) FROM (SELECT x,n,d FROM t WHERE CASE WHEN x<42 THEN true ELSE false END) a",
                    false,
                ),
                (
                    "WITH a AS (SELECT x,n,d FROM t) SELECT COUNT(*),MAX(l.d) FROM a l LEFT JOIN a r ON l.x=r.x WHERE l.x<42",
                    false,
                ),
            ] {
                let off = Options {
                    batch_size,
                    relational_optimizations: false,
                    ..Options::default()
                };
                let expected = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off).unwrap();
                for (cache, memory) in [
                    (None, 256 * 1024 * 1024),
                    (Some(0), 256 * 1024 * 1024),
                    (Some(16 * 1024 * 1024), 256 * 1024 * 1024),
                    (Some(16 * 1024 * 1024), 2 * 1024 * 1024),
                ] {
                    let on = Options {
                        batch_size,
                        parquet_cache_bytes: cache,
                        memory_bytes: memory,
                        ..Options::default()
                    };
                    let actual = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on).unwrap();
                    assert_eq!(actual.batch, expected.batch, "{sql}");
                    assert_eq!(
                        actual.metrics.static_scan_filters > 0,
                        eligible && cache.is_some_and(|n| n > 0),
                        "{sql}"
                    );
                }
            }
            for bound in [0, 20, 80, 512, 2048] {
                let sql =
                    format!("SELECT SUM(n),MAX(d) FROM (SELECT x,n,d FROM t WHERE x<{bound}) a");
                let on = Options {
                    batch_size,
                    ..Options::default()
                };
                let off = Options {
                    relational_optimizations: false,
                    ..on.clone()
                };
                let actual = execute(&prepare(&sql, &f.tables(), &on).unwrap(), &on).unwrap();
                let expected = execute(&prepare(&sql, &f.tables(), &off).unwrap(), &off).unwrap();
                assert_eq!(actual.batch, expected.batch);
                assert_eq!(actual.metrics.static_scan_filters > 0, bound <= 80);
            }
            // Guarded AND (Helen, October 17, 2026; SEMANTICS-LOG "Guarded
            // AND/OR planner rules"): x<0 is FALSE or NULL on every row, so the
            // WHERE rejects all rows and n*10^37 is not raised; SUM is NULL at
            // both settings, as in DuckDB 1.5.6 and PostgreSQL 17. Old
            // expectation: an error at both settings. The unguarded projection
            // below still raises at both settings.
            for (sql, raises) in [
                (
                    "SELECT SUM(n) FROM (SELECT x,n FROM t WHERE x<0 AND n*10000000000000000000000000000000000000>0) a",
                    false,
                ),
                (
                    "SELECT SUM(v) FROM (SELECT n*10000000000000000000000000000000000000 AS v FROM t) a WHERE v<0",
                    true,
                ),
                (
                    "SELECT SUM(n) FROM (SELECT x,n FROM t WHERE x>0 AND n*10000000000000000000000000000000000000>0) a",
                    true,
                ),
            ] {
                let on = Options {
                    batch_size,
                    ..Options::default()
                };
                let off = Options {
                    relational_optimizations: false,
                    ..on.clone()
                };
                let a = execute(&prepare(sql, &f.tables(), &on).unwrap(), &on);
                let b = execute(&prepare(sql, &f.tables(), &off).unwrap(), &off);
                match (a, b) {
                    (Err(a), Err(b)) => {
                        assert!(raises, "{sql}: {a}");
                        assert_eq!(a, "decimal precision 38 overflow", "{sql}");
                        assert_eq!(a, b, "{sql}");
                    }
                    (Ok(a), Ok(b)) => {
                        assert!(!raises, "{sql}");
                        assert_eq!(a.batch, b.batch, "{sql}");
                        assert_eq!(a.batch.num_rows(), 1, "{sql}");
                        assert!(a.batch.column(0).is_null(0), "{sql}");
                    }
                    (a, b) => panic!(
                        "{sql}: optimizer settings disagree: {:?} vs {:?}",
                        a.err(),
                        b.err()
                    ),
                }
            }
        }
    }
}

#[test]
fn borrowed_column_filter_costs_cover_payload_width_and_selectivity() {
    let f = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..2048).map(|i| (i % 97 != 0).then_some(i % 1024)),
            )),
            Arc::new(Int64Array::from_iter_values(0..2048)),
            Arc::new(Int64Array::from_iter_values((0..2048).map(|i| i * 3))),
            Arc::new(Int64Array::from_iter_values((0..2048).map(|i| -i))),
            Arc::new(Int64Array::from_iter_values((0..2048).map(|i| i % 7))),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&f.0).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for width in 0..=4 {
        for limit in [2, 128] {
            for computed in [false, true] {
                let outputs = ["a", "b", "c", "d"][..width]
                    .iter()
                    .map(|n| format!(",SUM(l.{n})"))
                    .collect::<String>();
                let key = if computed { "l.x+0" } else { "l.x" };
                let sql = format!(
                    "SELECT COUNT(*){outputs} FROM t l JOIN (SELECT x FROM t WHERE x<{limit}) r ON {key}=r.x"
                );
                let off = Options {
                    relational_optimizations: false,
                    ..Options::default()
                };
                let expected = execute(&prepare(&sql, &f.tables(), &off).unwrap(), &off).unwrap();
                for cached in [false, true] {
                    let on = Options {
                        parquet_cache_bytes: cached.then_some(16 * 1024 * 1024),
                        ..Options::default()
                    };
                    let actual = execute(&prepare(&sql, &f.tables(), &on).unwrap(), &on).unwrap();
                    assert_eq!(actual.batch, expected.batch, "{sql}");
                    let admitted = if cached {
                        if width == 4 {
                            true
                        } else {
                            width >= 2 && limit == 2 && !computed
                        }
                    } else {
                        width == 4 && limit == 2
                    };
                    assert_eq!(
                        actual.metrics.dynamic_scan_filters > 0,
                        admitted,
                        "{sql}, cached={cached}"
                    );
                }
            }
        }
    }
}

#[test]
fn sparse_join_rejection_preserves_outer_existence_duplicates_nulls_and_residuals() {
    let make = |values: Vec<Option<i64>>| {
        let path = std::env::temp_dir().join(format!(
            "assaydb-bitmap-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Fixture(path)
    };
    let probe = make((0..4096).map(Some).chain([None, Some(64)]).collect());
    let build = make(
        (0..64)
            .flat_map(|i| [Some(i * 64), Some(i * 64)])
            .chain([None])
            .collect(),
    );
    let tables = HashMap::from([("p".into(), probe.0.clone()), ("b".into(), build.0.clone())]);
    let options = Options {
        batch_size: 37,
        ..Options::default()
    };
    let plain = Options {
        relational_optimizations: false,
        ..options.clone()
    };
    for sql in [
        "SELECT COUNT(*) FROM p JOIN b ON p.x = b.x",
        "SELECT COUNT(*), COUNT(b.x) FROM p LEFT JOIN b ON p.x = b.x",
        "SELECT COUNT(*), COUNT(b.x) FROM p LEFT JOIN b ON p.x = b.x AND p.x + b.x < 100",
        "SELECT COUNT(*) FROM p WHERE EXISTS (SELECT b.x FROM b WHERE p.x = b.x)",
        "SELECT COUNT(*) FROM p WHERE NOT EXISTS (SELECT b.x FROM b WHERE p.x = b.x)",
        "SELECT COUNT(*) FROM b WHERE EXISTS (SELECT p.x FROM p WHERE p.x = b.x)",
        "SELECT COUNT(*) FROM b WHERE NOT EXISTS (SELECT p.x FROM p WHERE p.x = b.x)",
        "SELECT COUNT(*) FROM b WHERE EXISTS (SELECT p.x FROM p WHERE p.x = b.x AND p.x + b.x < 100)",
    ] {
        let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
        let expected = execute(&prepare(sql, &tables, &plain).unwrap(), &plain).unwrap();
        assert_eq!(actual.batch, expected.batch, "{sql}");
        assert!(actual.metrics.join_rejection_bitmaps > 0, "{sql}");
        assert!(actual.metrics.join_rejection_misses > 0, "{sql}");
        assert_eq!(expected.metrics.join_rejection_bitmaps, 0);
    }
}

#[test]
fn subset_scan_reuse_preserves_schema_filter_scope_and_first_reference_order() {
    let f = Fixture::new();
    for sql in [
        "SELECT x FROM (SELECT x FROM t WHERE x>0 UNION ALL SELECT x FROM t WHERE d>1) s ORDER BY 1",
        "SELECT x FROM (SELECT x FROM t WHERE d>1 UNION ALL SELECT x FROM t WHERE x>0) s ORDER BY 1",
        "SELECT a.x,b.d FROM t a LEFT JOIN t b ON a.x=b.x AND b.d<2 ORDER BY 1,2",
        "SELECT a.x,a.d FROM t a WHERE NOT EXISTS (SELECT b.x FROM t b WHERE a.x=b.x AND b.x>1) ORDER BY 1,2",
        "SELECT x,d FROM t WHERE d>(SELECT AVG(d) FROM t) ORDER BY 1,2",
        "SELECT x FROM (SELECT x FROM t WHERE x>1 UNION ALL SELECT x FROM t WHERE g='a' UNION ALL SELECT x FROM t WHERE d>1 AND g='a') s ORDER BY 1",
    ] {
        for batch_size in [1, 3, 8192] {
            let options = Options {
                batch_size,
                ..Options::default()
            };
            let ordinary = Options {
                relational_optimizations: false,
                ..options.clone()
            };
            let plan = prepare(sql, &f.tables(), &options).unwrap();
            let expected =
                execute(&prepare(sql, &f.tables(), &ordinary).unwrap(), &ordinary).unwrap();
            for _ in 0..2 {
                let actual = execute(&plan, &options).unwrap();
                assert_eq!(actual.batch, expected.batch, "{sql}");
                assert!(actual.metrics.scan_reuses > 0, "{sql}");
                assert_eq!(actual.metrics.rows_read, 4, "{sql}");
            }
        }
    }
    // Cache admission stays bounded by the query budget. Inspection of the
    // prepared tree avoids confusing admission fallback with decoder overhead.
    // Fund the footer metadata; the cache's 4 KiB overhead still exceeds its
    // admission share of this budget.
    let options = Options {
        memory_bytes: 4096,
        ..Options::default()
    };
    let plan = prepare(
        "SELECT a.x,b.d FROM t a JOIN t b ON a.x=b.x",
        &f.tables(),
        &options,
    )
    .unwrap();
    let encoded = serde_json::to_string(&plan.relational).unwrap();
    assert!(!encoded.contains("Materialize"));
}

#[test]
fn wildcard_projection_preserves_logical_order_and_hides_markers() {
    let fixture = Fixture::new();
    let tables = fixture.tables();
    for optimized in [false, true] {
        let options = Options {
            relational_optimizations: optimized,
            ..Options::default()
        };
        for sql in [
            "SELECT * FROM t",
            "SELECT t.* FROM t",
            "SELECT z.* FROM t z",
            "SELECT * FROM (SELECT x,d,g FROM t) z",
        ] {
            let p = prepare(sql, &tables, &options).unwrap();
            let b = execute(&p, &options).unwrap().batch;
            assert_eq!(b.num_columns(), 3, "{sql}");
            assert_eq!(
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                ["x", "d", "g"]
            );
            assert_eq!(integers(&b, 0), [Some(1), Some(2), None, Some(2)]);
        }
        let sql = "SELECT b.*, a.x FROM t a, (SELECT x+10 AS n,g FROM t WHERE x=1) b WHERE a.x=2 ORDER BY 3";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(b.num_columns(), 3);
        assert_eq!(integers(&b, 0), [Some(11), Some(11)]);
        assert_eq!(integers(&b, 2), [Some(2), Some(2)]);
        let sql = "SELECT * FROM t a, (SELECT x+10 AS n FROM t WHERE x=1) b WHERE a.x=2 ORDER BY 1";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(integers(&b, 0), [Some(2), Some(2)]);
        assert_eq!(integers(&b, 3), [Some(11), Some(11)]);
        let sql = "SELECT * FROM t a JOIN t b ON a.x=b.x ORDER BY 1,2,5";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(b.num_columns(), 6);
        assert_eq!(b.num_rows(), 5);
        assert_eq!(integers(&b, 0), integers(&b, 3));
        assert_eq!(
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["x", "d", "g", "x", "d", "g"]
        );
        let sql =
            "SELECT t.*, EXISTS(SELECT 1 FROM t z WHERE z.x<t.x) AS e FROM t ORDER BY 1 NULLS LAST";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(b.num_columns(), 4);
        assert_eq!(
            b.column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(false), Some(true), Some(true), Some(false)]
        );
        let sql = "SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x=1)";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(b.num_columns(), 3);
        assert_eq!(integers(&b, 0), [Some(1)]);
        let sql = "SELECT q.* FROM t a LEFT JOIN (SELECT x AS n FROM t WHERE x=1) q ON a.x=q.n ORDER BY 1 NULLS LAST";
        let p = prepare(sql, &tables, &options).unwrap();
        let b = execute(&p, &options).unwrap().batch;
        assert_eq!(b.num_columns(), 1);
        assert_eq!(integers(&b, 0), [Some(1), None, None, None]);
        for sql in [
            "SELECT *",
            "SELECT missing.* FROM t",
            "SELECT * EXCLUDE (g) FROM t",
            "SELECT *,COUNT(*) FROM t",
            "SELECT * FROM (SELECT 1 AS \"a.b\") q",
            "SELECT q.* FROM t AS q(\"a.b\",d,g)",
        ] {
            assert!(prepare(sql, &tables, &options).is_err(), "{sql}");
        }
    }
}
