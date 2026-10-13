// Zero-NULL approval: deliberate error/laziness markers use checked DECIMAL
// division, with an INTEGER cast where the original scalar type matters.
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn independent_states_preserve_aliases_late_groups_and_final_overflow() {
    let root =
        std::env::temp_dir().join(format!("assaydb-aggregate-parallel-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let path = root.join("t.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 0), false),
    ]));
    let high = 10_i128.pow(38) - 1;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..2048)),
        Arc::new(Int64Array::from_iter(
            (0..2048).map(|i| (i % 11 != 0).then_some(i / 100)),
        )),
        Arc::new(
            Decimal128Array::from_iter_values(
                (0..2048).map(|i| if i % 2 == 0 { high } else { -high }),
            )
            .with_precision_and_scale(38, 0)
            .unwrap(),
        ),
    ];
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(127)
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    for batch_size in [17, 128] {
        let base = Options {
            batch_size,
            memory_bytes: 16 << 20,
            ..Options::default()
        };
        for (sql, parallel) in [
            (
                "SELECT k,COUNT(*) AS n,COUNT(k) AS nn,SUM(i) AS total,AVG(i) AS avg FROM t GROUP BY k ORDER BY k",
                true,
            ),
            (
                "SELECT COUNT(*) AS n,SUM(d) AS s,AVG(d) AS a,COUNT(NULL) AS z FROM t",
                true,
            ),
            ("SELECT COUNT(*) AS n,SUM(i) AS s FROM t WHERE i<0", true),
            (
                "SELECT COUNT(*) AS n,SUM(CASE WHEN i<0 THEN i ELSE NULL END) AS s,AVG(CASE WHEN i<0 THEN i ELSE NULL END) AS a FROM t",
                true,
            ),
            (
                "SELECT COUNT(*) AS n,SUM(i) AS s,MIN(i) AS lo FROM t",
                false,
            ),
            ("SELECT COUNT(*) AS n,SUM(DISTINCT i) AS s FROM t", false),
        ] {
            let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
            for workers in [4, 10] {
                let options = Options {
                    workers,
                    ..base.clone()
                };
                let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "workers={workers} batch={batch_size}: {sql}"
                );
                assert_eq!(
                    // Grouped exact plans may now use whole-pipeline producers;
                    // scalar plans still exercise the independent-state helpers.
                    // Both must preserve the same results and worker allowance.
                    actual.metrics.parallel_aggregate_batches > 0
                        || actual.metrics.pipeline_producers > 0,
                    parallel,
                    "{sql}"
                );
                assert!(actual.metrics.worker_threads_peak < workers);
                assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
            }
        }
        // Semantic v2: SUM overflow is checked at finalization. A later
        // input-expression error now precedes that final range check.
        for (index, sql) in [
            "SELECT COUNT(*) AS n,SUM(CASE WHEN i=200 THEN d WHEN i=201 THEN 1 ELSE 0 END) AS s FROM t",
            "SELECT COUNT(*) AS n,SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS s,SUM(CASE WHEN 100.0/(i-200)>0 THEN i ELSE 0 END) AS x FROM t",
        ].into_iter().enumerate() {
            let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base)
                .err()
                .unwrap();
            assert!(expected.contains(if index == 0 { "overflow" } else { "zero" }), "{expected}");
            for workers in [4, 10] {
                let options = Options {
                    workers,
                    ..base.clone()
                };
                let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                    .err()
                    .unwrap();
                assert_eq!(actual, expected, "{sql}");
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}
