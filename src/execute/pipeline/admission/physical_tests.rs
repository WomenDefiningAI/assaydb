//! Real SQL precedence at misaligned physical groups, including occupied budgets.
use crate::{
    execute::{self, Options, Prepared},
    memory::Budget,
    types::Type,
};
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assaydb-physical-admission-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", Type::Int.arrow(), false),
            Field::new("v", Type::Decimal(0).arrow(), false),
            Field::new("d", Type::Int.arrow(), false),
        ]));
        // Exceeds the baseline's 1024-row helper threshold. Physical groups have
        // two rows while requested batches have three. In serial mode E1's error
        // at row2 precedes E2's at row1; physical-group mode sees row1 first.
        let rows = 1026;
        let limit = 10_i128.pow(38) - 1;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0; rows])),
            Arc::new(
                Decimal128Array::from_iter_values(
                    (0..rows).map(|i| if i == 2 { limit } else { 1 }),
                )
                .with_precision_and_scale(38, 0)
                .unwrap(),
            ),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| if i == 1 { 0 } else { 1 }),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(2)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(path)
    }
    fn prepare(&self, sql: &str, options: &Options) -> Prepared {
        execute::prepare(sql, &HashMap::from([("t".into(), self.0.clone())]), options).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
fn run(
    prepared: &Prepared,
    options: &Options,
    occupied: usize,
) -> crate::Result<execute::Execution> {
    let budget = Budget::new(options.memory_bytes);
    let held = budget.workers(occupied, options.workers.saturating_sub(1));
    assert_eq!(held.count, occupied);
    let result = execute::execute_in_budget(prepared, options, budget.clone());
    drop(held);
    if result.is_err() {
        assert_eq!(budget.used(), 0);
    }
    result
}

#[test]
fn misaligned_competing_input_errors_match_same_settings_and_occupied_baseline() {
    let fixture = Fixture::new();
    let sql = "SELECT k,COUNT(v+v),COUNT(CAST(1.0/d AS INTEGER)) FROM t GROUP BY k";
    for workers in [1, 2, 4, 10] {
        let old = Options {
            workers,
            batch_size: 3,
            ..Options::default()
        };
        let candidate = Options {
            test_pipeline_partitions: Some(4),
            test_pipeline_slice_bytes: Some(128 * 1024),
            ..old.clone()
        };
        let prepared = fixture.prepare(sql, &old);
        let occupied_cases = if workers == 1 {
            vec![0]
        } else if workers == 2 {
            vec![0, 1]
        } else {
            vec![0, 1, workers - 1]
        };
        for occupied in occupied_cases {
            let expected = run(&prepared, &old, occupied)
                .err()
                .expect("competing errors");
            let actual = run(&prepared, &candidate, occupied)
                .err()
                .expect("candidate error");
            assert_eq!(actual, expected, "workers={workers}, occupied={occupied}");
            if workers == 1 || occupied == workers - 1 {
                assert_eq!(expected, "numeric overflow");
            } else {
                assert!(expected.contains("division by zero"), "{expected}");
            }
        }
    }
}

#[test]
fn misaligned_admission_runs_real_parallel_path_and_preserves_where_order() {
    let fixture = Fixture::new();
    for workers in [1, 2, 4, 10] {
        let old = Options {
            workers,
            batch_size: 3,
            ..Options::default()
        };
        let candidate = Options {
            test_pipeline_partitions: Some(4),
            test_pipeline_slice_bytes: Some(128 * 1024),
            ..old.clone()
        };
        for (sql, error) in [
            (
                "SELECT k,COUNT(v+v),COUNT(CAST(1.0/d AS INTEGER)) FROM t WHERE d<>0 GROUP BY k",
                true,
            ),
            (
                "SELECT k,COUNT(v+v),COUNT(CAST(1.0/d AS INTEGER)) FROM t WHERE k<0 GROUP BY k",
                false,
            ),
            ("SELECT k,COUNT(*) FROM t GROUP BY k", false),
        ] {
            let prepared = fixture.prepare(sql, &old);
            let expected = run(&prepared, &old, 0);
            let actual = run(&prepared, &candidate, 0);
            if error {
                let expected = expected.err().unwrap();
                assert_eq!(expected, "numeric overflow");
                assert_eq!(actual.err().unwrap(), expected);
            } else {
                let expected = expected.unwrap();
                let actual = actual.unwrap();
                assert_eq!(actual.batch, expected.batch);
                assert_eq!(actual.metrics.batches, expected.metrics.batches);
                assert_eq!(
                    actual.metrics.pipeline_producers,
                    if workers > 1 { workers } else { 0 }
                );
            }
        }
    }
}
