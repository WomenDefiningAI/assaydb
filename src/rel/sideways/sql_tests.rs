//! Real SQL/planner controls: all admission estimates come from Parquet files.
use super::*;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::File,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Tables(HashMap<String, PathBuf>);
impl Tables {
    fn new() -> Self {
        let mut tables = Self(HashMap::new());
        for (name, rows) in [("sip_probe", 64), ("sip_source", 4), ("sip_target", 16384)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", arrow::datatypes::DataType::Int64, true),
                Field::new("n", arrow::datatypes::DataType::Int64, false),
                Field::new("f", arrow::datatypes::DataType::Float64, true),
                Field::new("d", arrow::datatypes::DataType::Decimal128(15, 2), true),
                Field::new("e", arrow::datatypes::DataType::Decimal128(15, 3), true),
            ]));
            let keys = (0..rows)
                .map(|i| {
                    if name == "sip_source" {
                        [Some(3), None, Some(1), Some(3)][i as usize]
                    } else {
                        (i % 1024 != 0).then_some(i % 1024)
                    }
                })
                .collect::<Vec<_>>();
            let decimal = |scale: i8| {
                Arc::new(
                    Decimal128Array::from(
                        keys.iter()
                            .map(|k| k.map(|k| i128::from(k) * 10_i128.pow(scale as u32)))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(15, scale)
                    .unwrap(),
                ) as ArrayRef
            };
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(Int64Array::from_iter_values(0..rows)),
                    Arc::new(Float64Array::from(
                        keys.iter().map(|k| k.map(|k| k as f64)).collect::<Vec<_>>(),
                    )),
                    decimal(2),
                    decimal(3),
                ],
            )
            .unwrap();
            let path = std::env::temp_dir().join(format!(
                "assaydb-sip-sql-{}-{}-{name}.parquet",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let properties = WriterProperties::builder()
                .set_max_row_group_size(1024)
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, Some(properties))
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            tables.0.insert(name.into(), path);
        }
        tables
    }
}
impl Drop for Tables {
    fn drop(&mut self) {
        for path in self.0.values() {
            std::fs::remove_file(path).unwrap();
        }
    }
}

fn check(tables: &Tables, from: &str, activates: bool, errors: bool) {
    let sql = format!("SELECT p.n,c.n,b.n FROM {from} ORDER BY 1,2,3");
    let off = Options {
        workers: 1,
        batch_size: 1,
        relational_optimizations: false,
        ..Options::default()
    };
    let baseline = execute::execute(&execute::prepare(&sql, &tables.0, &off).unwrap(), &off);
    assert_eq!(baseline.is_err(), errors, "{sql}");
    if activates {
        // k=1 contributes 16 rows; k=3 contributes 32 (duplicate source).
        assert_eq!(baseline.as_ref().unwrap().batch.num_rows(), 48, "{sql}");
    }
    for batch_size in [1, 7, 8192] {
        for workers in [1, 2, 4, 10] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let prepared = execute::prepare(&sql, &tables.0, &options).unwrap();
            // Even the filtered target must clear the cardinality gate. This
            // prevents a semantic decline control from merely testing size.
            let mut root = prepared.relational.as_deref().unwrap();
            loop {
                match &root.op {
                    RelOp::Join { right, .. } => {
                        assert!(right.estimated_rows >= 4096, "RHS too small: {sql}");
                        break;
                    }
                    RelOp::Unary { input, .. }
                    | RelOp::Project { input, .. }
                    | RelOp::Rename { input }
                    | RelOp::Filter { input, .. } => root = input,
                    _ => panic!("missing SQL join: {sql}"),
                }
            }
            let actual = execute::execute(&prepared, &options);
            match (&baseline, actual) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(
                        actual.batch, expected.batch,
                        "workers={workers} batch={batch_size}: {sql}"
                    );
                    assert_eq!(
                        actual.metrics.sideways_scan_filters > 0,
                        activates,
                        "workers={workers} batch={batch_size}: {sql}"
                    );
                    if !activates {
                        assert_eq!(actual.metrics.sideways_prepared_builds, 0, "{sql}");
                    }
                }
                (Err(expected), Err(actual)) => {
                    assert_eq!(
                        &actual, expected,
                        "workers={workers} batch={batch_size}: {sql}"
                    );
                    // The public error result carries no metrics. Inspect the
                    // same SQL-derived plan in a fresh context to prove decline
                    // before its error, rather than mistaking replay for safety.
                    let budget = Budget::new(64 << 20);
                    let used = AtomicBool::new(false);
                    let mut ctx = super::tests::context(&options, &budget, &used);
                    prepared
                        .relational
                        .as_ref()
                        .unwrap()
                        .run(&mut ctx, &mut |_| Ok(()))
                        .unwrap_err();
                    // Raw chunk execution precedes canonical error selection;
                    // exact public error text was compared above.
                    assert_eq!(ctx.metrics.sideways_scan_filters, 0, "{sql}");
                    assert_eq!(ctx.metrics.sideways_prepared_builds, 0, "{sql}");
                    drop(ctx);
                    assert_eq!(budget.used(), 0);
                }
                (expected, actual) => panic!(
                    "outcome mismatch for {sql}: expected error {:?}, actual error {:?}",
                    expected.as_ref().err(),
                    actual.err()
                ),
            }
            options.cancellation.cancel();
            assert_eq!(
                execute::execute(&prepared, &options).err().unwrap(),
                options.cancellation.check().unwrap_err()
            );
        }
    }
}

#[test]
fn sql_activates_sideways_with_real_sizes_nulls_duplicates_and_decimal_keys() {
    let tables = Tables::new();
    for key in ["k", "d"] {
        check(
            &tables,
            &format!(
                "sip_probe p JOIN sip_source c ON p.{key}=c.{key} JOIN sip_target b ON p.{key}=b.{key}"
            ),
            true,
            false,
        );
    }
}

#[test]
fn sql_declines_semantic_barriers_despite_admissible_sizes() {
    let tables = Tables::new();
    for from in [
        "sip_probe p JOIN sip_source c ON (p.k=c.k OR p.n=c.n) JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN sip_source c ON (p.k=c.k OR (p.k IS NULL AND c.k IS NULL)) JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN sip_source c ON p.k=c.k JOIN sip_target b ON p.k=b.f",
        "sip_probe p JOIN sip_source c ON p.k=c.k JOIN sip_target b ON p.k+0.00=b.d",
        "sip_probe p JOIN sip_source c ON p.d=c.d JOIN sip_target b ON p.d=b.e",
        "sip_probe p LEFT JOIN sip_source c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN (SELECT * FROM sip_source LIMIT 4) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN (SELECT k,MIN(n) AS n FROM sip_source GROUP BY k) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        // Actual values are small, but the physical INTEGER domain cannot
        // prove these products total; both source and target must decline.
        "sip_probe p JOIN (SELECT * FROM sip_source WHERE n*n*n>=0) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN sip_source c ON p.k=c.k JOIN (SELECT * FROM sip_target WHERE n*n*n>=0) b ON p.k=b.k",
    ] {
        check(&tables, from, false, false);
    }
}

#[test]
fn sql_declines_fallible_paths_and_preserves_errors() {
    let tables = Tables::new();
    for from in [
        "sip_probe p JOIN (SELECT * FROM sip_source WHERE n*99999999999999999999999999999999999999>0) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN sip_source c ON p.k=c.k JOIN (SELECT * FROM sip_target WHERE n*99999999999999999999999999999999999999>0) b ON p.k=b.k",
        "sip_probe p JOIN (SELECT * FROM sip_source WHERE n*99999999999999999999999999999999999999>0) c ON p.k=c.k JOIN (SELECT * FROM sip_target WHERE n*99999999999999999999999999999999999999+1>0) b ON p.k=b.k",
    ] {
        check(&tables, from, false, true);
    }
}

#[test]
fn sql_empty_source_probe_and_target_match_serial() {
    let tables = Tables::new();
    for from in [
        "sip_probe p JOIN (SELECT * FROM sip_source WHERE k<0) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "(SELECT * FROM sip_probe WHERE k<0) p JOIN sip_source c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
        "sip_probe p JOIN sip_source c ON p.k=c.k JOIN (SELECT * FROM sip_target WHERE k<0) b ON p.k=b.k",
        "sip_probe p JOIN (SELECT * FROM sip_source WHERE k IS NULL) c ON p.k=c.k JOIN sip_target b ON p.k=b.k",
    ] {
        let sql = format!("SELECT p.n,c.n,b.n FROM {from} ORDER BY 1,2,3");
        let off = Options {
            workers: 1,
            batch_size: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected =
            execute::execute(&execute::prepare(&sql, &tables.0, &off).unwrap(), &off).unwrap();
        assert_eq!(expected.batch.num_rows(), 0);
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let prepared = execute::prepare(&sql, &tables.0, &options).unwrap();
                let actual = execute::execute(&prepared, &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "workers={workers} batch={batch_size}: {sql}"
                );
                options.cancellation.cancel();
                assert_eq!(
                    execute::execute(&prepared, &options).err().unwrap(),
                    options.cancellation.check().unwrap_err()
                );
            }
        }
    }
}
