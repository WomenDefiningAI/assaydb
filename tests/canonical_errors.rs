//! Rule E expectations are handwritten; serial output is not the error oracle.
use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    rel,
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
const DECIMAL_ERROR: &str = "decimal precision 38 overflow";

struct Fixture {
    root: PathBuf,
    tables: HashMap<String, PathBuf>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "assay-canonical-errors-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("spill")).unwrap();
        let mut fixture = Self {
            root,
            tables: HashMap::new(),
        };
        // The first three rows and the build statistics are the F1 reproducer.
        let ids = (1..=512).collect::<Vec<i64>>();
        fixture.write(
            "p",
            vec![
                ("id", Arc::new(Int64Array::from(ids.clone()))),
                (
                    "x",
                    Arc::new(Int64Array::from_iter_values(
                        ids.iter().map(|&i| if i <= 3 { 1 } else { i }),
                    )),
                ),
                (
                    "y",
                    Arc::new(Int64Array::from_iter_values(
                        ids.iter().map(|&i| if i == 3 { 1 } else { i }),
                    )),
                ),
                ("payload", Arc::new(StringArray::from(vec!["p"; 512]))),
                (
                    "f",
                    Arc::new(Float64Array::from_iter_values(
                        ids.iter().map(|&i| if i == 3 { 1e308 } else { 1.0 }),
                    )),
                ),
                (
                    "a",
                    Arc::new(Int64Array::from_iter_values(
                        ids.iter().map(|&i| if i == 1 { 10 } else { 0 }),
                    )),
                ),
            ],
            512,
        );
        let mut build = vec![999; 128];
        build[0] = 1;
        fixture.write("b", vec![("y", Arc::new(Int64Array::from(build)))], 128);
        fixture.write("c", vec![("x", Arc::new(Int64Array::from(vec![1])))], 1);
        // Separate row groups put the early DECIMAL and later float failures on
        // independent scan tasks. The first row also has both causes at once.
        fixture.write(
            "t",
            vec![
                (
                    "k",
                    Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)])),
                ),
                (
                    "d",
                    Arc::new(
                        Decimal128Array::from(vec![10_i128.pow(37); 4])
                            .with_precision_and_scale(38, 0)
                            .unwrap(),
                    ),
                ),
                ("f", Arc::new(Float64Array::from(vec![1e308; 4]))),
            ],
            1,
        );
        fixture.write(
            "r",
            vec![
                (
                    "k",
                    Arc::new(Int64Array::from_iter_values((0..24).map(|i| i / 12))),
                ),
                (
                    "v",
                    Arc::new(Int64Array::from_iter_values(
                        (0..24).map(|i| if i < 12 { i64::MAX } else { 0 }),
                    )),
                ),
                (
                    "d",
                    Arc::new(
                        Decimal128Array::from_iter_values(
                            (0..24).map(|i| if i < 12 { 1 } else { 10_i128.pow(37) }),
                        )
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                    ),
                ),
            ],
            3,
        );
        fixture.write(
            "empty",
            vec![("k", Arc::new(Int64Array::from(Vec::<i64>::new())))],
            1,
        );
        fixture
    }

    fn write(&mut self, name: &str, columns: Vec<(&str, ArrayRef)>, row_group: usize) {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        let path = self.root.join(format!("{name}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        self.tables.insert(name.to_owned(), path);
    }

    fn options(&self, workers: usize, batch_size: usize, force: bool) -> Options {
        Options {
            workers,
            batch_size,
            memory_bytes: 64 << 20,
            spill: force.then(|| SpillOptions {
                mode: SpillMode::Force,
                directory: self.root.join("spill"),
                sort_run_bytes: 16 << 10,
                ..SpillOptions::default()
            }),
            ..Options::default()
        }
    }

    fn assert_error(&self, sql: &str, expected: &str) {
        // Prepare once: varying execution options must never replan the query.
        let prepared = rel::prepare(sql, &self.tables, &self.options(1, 8192, false)).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                for force in [false, true] {
                    let options = self.options(workers, batch_size, force);
                    let actual = execute(&prepared, &options).err().unwrap();
                    assert_eq!(
                        actual, expected,
                        "{sql}: workers={workers}, batch={batch_size}, spill={force}"
                    );
                    let replay = options.error_replay.metrics();
                    assert_eq!(replay.attempts, 1, "{sql}: {replay:?}");
                    assert_eq!(replay.unavailable, 0, "{sql}: {replay:?}");
                    assert_eq!(replay.succeeded, 0, "{sql}: {replay:?}");
                    assert!(replay.peak_reserved_bytes <= options.memory_bytes);
                    assert!(replay.peak_workers <= 1);
                    assert_eq!(fs::read_dir(self.root.join("spill")).unwrap().count(), 0);
                }
            }
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn first_row_then_first_expression_selects_the_error() {
    let fixture = Fixture::new();
    fixture.assert_error(
        "SELECT p.f*p.f, p.a*10000000000000000000000000000000000000, p.payload FROM p JOIN b ON p.y=b.y JOIN c ON p.x=c.x",
        DECIMAL_ERROR,
    );
    fixture.assert_error("SELECT d*10, f*f FROM t", DECIMAL_ERROR);
    // WHERE on a later row must not preempt an earlier projection error.
    fixture.assert_error(
        "SELECT a*10000000000000000000000000000000000000 FROM p WHERE f*f>0",
        DECIMAL_ERROR,
    );
    fixture.assert_error("SELECT f*f, d*10 FROM t", "floating arithmetic overflow");
}

#[test]
fn direct_scan_replays_the_same_prepared_plan() {
    let fixture = Fixture::new();
    let prepared = prepare(
        "SELECT f*f,a*10000000000000000000000000000000000000 FROM p",
        &fixture.tables,
        &Options::default(),
    )
    .unwrap();
    assert!(prepared.relational.is_none());
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = fixture.options(workers, batch_size, false);
            assert_eq!(execute(&prepared, &options).err().unwrap(), DECIMAL_ERROR);
            let replay = options.error_replay.metrics();
            assert_eq!(replay.semantic_errors, 1);
            assert_eq!(replay.unavailable, 0);
            assert_eq!(replay.succeeded, 0);
        }
    }
}

#[test]
fn computed_aggregate_arguments_decline_without_changing_the_failure() {
    let fixture = Fixture::new();
    let prepared = prepare(
        "SELECT SUM(d*10) FROM t",
        &fixture.tables,
        &Options::default(),
    )
    .unwrap();
    assert!(prepared.relational.is_none());
    let options = Options::default();
    assert_eq!(execute(&prepared, &options).err().unwrap(), DECIMAL_ERROR);
    let replay = options.error_replay.metrics();
    assert_eq!(replay.unavailable, 1);
    assert_eq!(replay.succeeded, 0);
    assert_eq!(
        replay.replay_error.as_deref(),
        Some("canonical replay unavailable: computed aggregate argument or grouping key")
    );
}

#[test]
fn breaker_and_join_errors_are_eligible_before_downstream_filtering() {
    let fixture = Fixture::new();
    // The completed DECIMAL total fails before HAVING can discard the group.
    fixture.assert_error("SELECT SUM(d) FROM r HAVING FALSE", DECIMAL_ERROR);
    // ON is evaluated on candidate pairs, before marking matches or emitting a
    // NULL extension. NULL probe keys and duplicate keys coexist in this input.
    fixture.assert_error(
        "SELECT t.k FROM t LEFT JOIN r ON t.k=r.k AND r.d*10>0",
        DECIMAL_ERROR,
    );
    // Group 0 exceeds public BIGINT; group 1 exceeds DECIMAL precision. The
    // latter is a breaker-completion failure, before any public SUM is returned.
    fixture.assert_error("SELECT SUM(v),SUM(d) FROM r GROUP BY k", DECIMAL_ERROR);
    fixture.assert_error("SELECT SUM(d),SUM(v) FROM r GROUP BY k", DECIMAL_ERROR);
    fixture.assert_error(
        "SELECT (SELECT k FROM t) FROM c",
        "scalar subquery returned multiple rows",
    );
}

#[test]
fn successful_short_circuit_null_duplicate_and_empty_queries_do_not_replay() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT CASE WHEN TRUE THEN 0 ELSE d*10 END, COALESCE(1,d*10) FROM t",
        // This binder orders by output aliases or ordinals, not qualified names.
        "SELECT t.k,r.k FROM t LEFT JOIN r ON t.k=r.k ORDER BY 1,2",
        "SELECT t.k,empty.k FROM t LEFT JOIN empty ON t.k=empty.k ORDER BY 1",
        "SELECT empty.k FROM empty JOIN t ON empty.k=t.k",
        "SELECT CAST(k AS REAL),CAST(k AS TEXT) FROM t ORDER BY 1",
        "SELECT SUM(k) FROM empty",
    ] {
        let serial = fixture.options(1, 1, false);
        let prepared = rel::prepare(sql, &fixture.tables, &serial).unwrap();
        let expected = execute(&prepared, &serial).unwrap();
        for workers in [1, 2, 4, 10] {
            for batch_size in [1, 7, 8192] {
                let options = fixture.options(workers, batch_size, false);
                let actual = execute(&prepared, &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "{sql}: workers={workers}, batch={batch_size}"
                );
                assert_eq!(options.error_replay.metrics().attempts, 0);
            }
        }
    }
}

#[test]
fn cancellation_is_not_a_semantic_error() {
    let fixture = Fixture::new();
    let prepared =
        rel::prepare("SELECT d*10 FROM t", &fixture.tables, &Options::default()).unwrap();
    for workers in [1, 2, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = fixture.options(workers, batch_size, false);
            options.cancellation.cancel();
            assert!(
                execute(&prepared, &options)
                    .err()
                    .unwrap()
                    .starts_with("cancelled:")
            );
            assert_eq!(options.error_replay.metrics().attempts, 0);
        }
    }
}
