// Zero-NULL approval: unrelated error/laziness sentinels retain an Int type
// via CAST of DECIMAL zero division, whose checked error contract is unchanged.
use arrow::{
    array::*,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    spill::{SpillMode, SpillOptions},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    directory: PathBuf,
    tables: HashMap<String, PathBuf>,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "assaydb-correlated-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let mut tables = HashMap::new();
        for (name, a, b) in [
            (
                "t",
                vec![Some(1), Some(2), Some(2), Some(4), None, Some(5), None],
                vec![Some(3), Some(2), Some(2), Some(1), Some(5), None, None],
            ),
            ("empty", vec![], vec![]),
        ] {
            let arrays: Vec<ArrayRef> =
                vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))];
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", arrays[0].data_type().clone(), true),
                Field::new("b", arrays[1].data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
            let path = directory.join(format!("{name}.parquet"));
            let properties = WriterProperties::builder()
                .set_max_row_group_size(3)
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, Some(properties))
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            tables.insert(name.into(), path);
        }
        Self { directory, tables }
    }
    fn options(&self, workers: usize, batch_size: usize, optimize: bool, spill: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: 32 << 20,
            relational_optimizations: optimize,
            projection_pushdown: optimize,
            spill: spill.then(|| SpillOptions {
                directory: self.directory.clone(),
                mode: SpillMode::Force,
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }
    fn run(&self, sql: &str, options: &Options) -> RecordBatch {
        let prepared = prepare(sql, &self.tables, options).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let result = execute(&prepared, options).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(result.metrics.peak_reserved_bytes <= options.memory_bytes);
        assert_eq!(result.metrics.spill.live_files, 0);
        assert_eq!(result.metrics.spill.live_bytes, 0);
        result.batch
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}
fn ints(batch: &RecordBatch, col: usize) -> Vec<Option<i64>> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn bools(batch: &RecordBatch, col: usize) -> Vec<Option<bool>> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn correlated_markers_preserve_nulls_duplicates_and_order_in_all_execution_modes() {
    let fixture = Fixture::new();
    let sql = "SELECT t.a, (SELECT COUNT(*) FROM t x WHERE x.a<t.a) AS n, (SELECT COUNT(*) FROM t x WHERE x.a<t.a AND x.b>t.b) AS pair_n, EXISTS (SELECT 1 FROM t x WHERE x.a<t.a) AS yes, NOT EXISTS (SELECT 1 FROM t x WHERE x.a<t.a) AS no FROM t";
    for workers in [1, 4, 10] {
        for batch_size in [1, 2, 128] {
            for optimize in [false, true] {
                for spill in [false, true] {
                    let options = fixture.options(workers, batch_size, optimize, spill);
                    let b = fixture.run(sql, &options);
                    assert_eq!(
                        ints(&b, 0),
                        vec![Some(1), Some(2), Some(2), Some(4), None, Some(5), None]
                    );
                    assert_eq!(
                        ints(&b, 1),
                        vec![
                            Some(0),
                            Some(1),
                            Some(1),
                            Some(3),
                            Some(0),
                            Some(4),
                            Some(0)
                        ]
                    );
                    assert_eq!(
                        ints(&b, 2),
                        vec![
                            Some(0),
                            Some(1),
                            Some(1),
                            Some(3),
                            Some(0),
                            Some(0),
                            Some(0)
                        ]
                    );
                    assert_eq!(
                        bools(&b, 3),
                        vec![
                            Some(false),
                            Some(true),
                            Some(true),
                            Some(true),
                            Some(false),
                            Some(true),
                            Some(false)
                        ]
                    );
                    assert_eq!(
                        bools(&b, 4),
                        bools(&b, 3)
                            .iter()
                            .map(|b| b.map(|b| !b))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}

#[test]
fn markers_support_general_predicates_scopes_empty_sources_and_existing_equality() {
    let fixture = Fixture::new();
    let options = fixture.options(4, 2, true, false);
    for (predicate, expected) in [
        ("x.a+t.a=4", vec![0, 2, 2, 0, 0, 0, 0]),
        (
            "x.a<t.a OR (x.a IS NULL AND t.a IS NULL)",
            vec![0, 1, 1, 3, 2, 4, 2],
        ),
        ("x.a BETWEEN t.a-1 AND t.a+1", vec![3, 3, 3, 2, 0, 2, 0]),
        ("x.a=t.a", vec![1, 2, 2, 1, 0, 1, 0]),
    ] {
        let sql = format!("SELECT (SELECT COUNT(*) FROM t x WHERE {predicate}) FROM t");
        assert_eq!(
            ints(&fixture.run(&sql, &options), 0),
            expected.into_iter().map(Some).collect::<Vec<_>>()
        );
    }
    let empty = fixture.run("SELECT (SELECT COUNT(*) FROM empty x WHERE x.a<t.a), EXISTS (SELECT 1 FROM empty x WHERE x.a<t.a), NOT EXISTS (SELECT 1 FROM empty x WHERE x.a<t.a) FROM t", &options);
    assert_eq!(ints(&empty, 0), vec![Some(0); 7]);
    assert_eq!(bools(&empty, 1), vec![Some(false); 7]);
    assert_eq!(bools(&empty, 2), vec![Some(true); 7]);
    assert_eq!(
        fixture
            .run(
                "SELECT (SELECT COUNT(*) FROM t x WHERE x.a<e.a) FROM empty e",
                &options
            )
            .num_rows(),
        0
    );
    let uncorrelated = fixture.run("SELECT EXISTS (SELECT 1), EXISTS (SELECT 1 FROM empty), (SELECT COUNT(*) FROM t x WHERE x.a<x.b) FROM t", &options);
    assert_eq!(bools(&uncorrelated, 0), vec![Some(true); 7]);
    assert_eq!(bools(&uncorrelated, 1), vec![Some(false); 7]);
    assert_eq!(ints(&uncorrelated, 2), vec![Some(1); 7]);
    let shadowed = fixture.run(
        "SELECT (SELECT COUNT(*) FROM t x WHERE x.a<x.b) FROM t x",
        &options,
    );
    assert_eq!(ints(&shadowed, 0), vec![Some(1); 7]);
    // A local unqualified a shadows the same outer name; qualified x.b is outer.
    let local = fixture.run(
        "SELECT (SELECT COUNT(*) FROM t y WHERE a<x.b) FROM t x",
        &options,
    );
    assert_eq!(
        ints(&local, 0),
        vec![
            Some(3),
            Some(1),
            Some(1),
            Some(0),
            Some(4),
            Some(0),
            Some(0)
        ]
    );
    let derived = fixture.run(
        "SELECT (SELECT COUNT(*) FROM (SELECT a+1 AS v FROM t) y WHERE y.v<x.a) FROM t x",
        &options,
    );
    assert_eq!(
        ints(&derived, 0),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(3),
            Some(0),
            Some(3),
            Some(0)
        ]
    );
    let filter = fixture.run(
        "SELECT t.a FROM t WHERE t.a=1 OR EXISTS (SELECT 1 FROM t x WHERE x.a<t.a)",
        &options,
    );
    assert_eq!(
        ints(&filter, 0),
        vec![Some(1), Some(2), Some(2), Some(4), Some(5)]
    );
    // EXISTS projection is deliberately not evaluated.
    assert_eq!(
        bools(
            &fixture.run(
                "SELECT EXISTS (SELECT CAST(1.0/0 AS INTEGER) FROM t x WHERE x.a<t.a) FROM t",
                &options
            ),
            0
        ),
        vec![
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false)
        ]
    );
}

#[test]
fn markers_reject_unsafe_lazy_contexts_and_unsupported_aggregate_forms() {
    let fixture = Fixture::new();
    let options = fixture.options(1, 2, true, false);
    for sql in [
        "SELECT CASE WHEN a=1 THEN 0 ELSE (SELECT COUNT(*) FROM t x WHERE x.a<t.a OR CAST(1.0/(x.b-1) AS INTEGER)>0) END FROM t",
        "SELECT CASE WHEN a=1 THEN FALSE ELSE EXISTS (SELECT 1 FROM t x WHERE CAST(1.0/(x.b-1) AS INTEGER)>t.a) END FROM t",
        "SELECT COALESCE(TRUE, EXISTS (SELECT 1 FROM t x WHERE CAST(1.0/(x.b-1) AS INTEGER)>t.a)) FROM t",
    ] {
        let error = match prepare(sql, &fixture.tables, &options) {
            Err(e) => e,
            Ok(_) => panic!("unexpected admission: {sql}"),
        };
        assert!(error.contains("masked subquery evaluation"), "{error}");
    }
    for sql in [
        "SELECT EXISTS (SELECT COUNT(*) FROM empty) FROM t",
        "SELECT (SELECT COUNT(DISTINCT x.a) FROM t x WHERE x.a<t.a) FROM t",
        "SELECT (SELECT SUM(x.a) FROM t x WHERE x.a<t.a) FROM t",
        "SELECT (SELECT COUNT(*) FROM t x WHERE x.a<t.a GROUP BY x.b) FROM t",
        "SELECT EXISTS (SELECT 1 FROM t x WHERE x.a<t.a LIMIT 1) FROM t",
        "SELECT (SELECT COUNT(*) FROM t x JOIN t y ON x.a=y.a WHERE a<t.a) FROM t",
    ] {
        assert!(prepare(sql, &fixture.tables, &options).is_err(), "{sql}");
    }
}

#[test]
fn exists_projection_is_bound_in_its_scopes_but_never_evaluated() {
    let fixture = Fixture::new();
    for optimize in [false, true] {
        let options = fixture.options(4, 2, optimize, false);
        for projection in [
            "CAST(1.0/0 AS INTEGER)",
            "NULL",
            "ABS(x.a)",
            "x.a AS z",
            "a",
            "t.a",
            "*",
            "x.*",
            "t.*",
        ] {
            let sql = format!("SELECT EXISTS (SELECT {projection} FROM t x WHERE x.a<t.a) FROM t");
            assert_eq!(
                bools(&fixture.run(&sql, &options), 0),
                vec![
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false)
                ],
                "{sql}"
            );
        }
        for sql in [
            "SELECT EXISTS (SELECT missing_column FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT unknown_function(x.a) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT x.a+'text' FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT missing.* FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT x.* EXCLUDE (a) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT * REPLACE (missing AS a) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT (SELECT missing FROM t) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT EXISTS (SELECT 1) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT x.a IN (SELECT a FROM t) FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT ABS(x.a) OVER () FROM t x WHERE x.a<t.a) FROM t",
            "SELECT EXISTS (SELECT * ) FROM t",
            // Local x hides outer x, including columns absent in the local x.
            "SELECT EXISTS (SELECT x.b FROM (SELECT a FROM t) x) FROM t x",
            "SELECT EXISTS (SELECT a FROM t x JOIN t y ON x.a=y.a) FROM t",
        ] {
            assert!(
                prepare(sql, &fixture.tables, &options).is_err(),
                "unexpected projection admission: {sql}"
            );
        }
        // Unqualified and qualified names bind to the inner scope even though
        // the same outer names have incompatible types. Binding must not depend
        // on evaluating the discarded division expression.
        for projection in ["a+1", "x.a+1", "x.*", "*"] {
            let sql = format!(
                "SELECT EXISTS (SELECT {projection} FROM t x) FROM (SELECT 'text' AS a FROM t) x"
            );
            assert_eq!(bools(&fixture.run(&sql, &options), 0), vec![Some(true); 7]);
        }
        assert_eq!(
            bools(
                &fixture.run(
                    "SELECT EXISTS (SELECT x.a FROM (SELECT 'text' AS a FROM t) x) FROM t x",
                    &options
                ),
                0
            ),
            vec![Some(true); 7]
        );
    }
}

#[test]
fn markers_propagate_late_errors_resource_failure_and_cancellation_with_cleanup() {
    let fixture = Fixture::new();
    for spill in [false, true] {
        let options = fixture.options(4, 2, false, spill);
        for projection in ["COUNT(*)", "1"] {
            let sql = if projection == "1" {
                "SELECT EXISTS (SELECT 1 FROM t x WHERE x.a<t.a OR CAST(1.0/(x.b-1) AS INTEGER)>0) FROM t"
            } else {
                "SELECT (SELECT COUNT(*) FROM t x WHERE x.a<t.a OR CAST(1.0/(x.b-1) AS INTEGER)>0) FROM t"
            };
            let prepared = prepare(sql, &fixture.tables, &options).unwrap();
            let error = execute(&prepared, &options).err().unwrap();
            assert!(error.contains("division by zero"), "{error}");
            assert_eq!(fs::read_dir(&fixture.directory).unwrap().count(), 2);
        }
        let sql = "SELECT (SELECT COUNT(*) FROM t x WHERE x.a<t.a) FROM t";
        let prepared = prepare(sql, &fixture.tables, &options).unwrap();
        let mut tiny = options.clone();
        tiny.memory_bytes = 128;
        assert!(
            execute(&prepared, &tiny)
                .err()
                .unwrap()
                .starts_with("resource-limit:")
        );
        let cancelled = options.clone();
        cancelled.cancellation.cancel();
        assert!(
            execute(&prepared, &cancelled)
                .err()
                .unwrap()
                .starts_with("cancelled:")
        );
        assert_eq!(fs::read_dir(&fixture.directory).unwrap().count(), 2);
    }
}
