use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::{Options, execute, prepare},
    rel::{Node, RelOp},
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
struct Tables(HashMap<String, PathBuf>);
impl Tables {
    fn new() -> Self {
        let mut tables = Self(HashMap::new());
        for (name, rows) in [("probe", 513), ("build", 9), ("other", 7)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Int64, true),
                Field::new("n", DataType::Int64, false),
                Field::new("d", DataType::Decimal128(15, 2), true),
                Field::new("f", DataType::Float64, true),
                Field::new("payload", DataType::Utf8, false),
            ]));
            let x = (0..rows)
                .map(|i| (i % 17 != 0).then_some((i % 64) * 64))
                .collect::<Vec<_>>();
            let y = (0..rows)
                .map(|i| (i % 19 != 0).then_some(i % 8))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(x.clone())),
                    Arc::new(Int64Array::from(y)),
                    Arc::new(Int64Array::from_iter_values(0..rows)),
                    Arc::new(
                        Decimal128Array::from(
                            x.iter()
                                .map(|x| x.map(|x| i128::from(x) * 100))
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                    ),
                    Arc::new(Float64Array::from(
                        x.iter().map(|x| x.map(|x| x as f64)).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|i| format!("payload-{i:04}")),
                    )),
                ],
            )
            .unwrap();
            let path = std::env::temp_dir().join(format!(
                "assaydb-runtime-filters-{}-{}-{name}.parquet",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let properties = WriterProperties::builder()
                .set_max_row_group_size(31)
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&path).unwrap(), schema, Some(properties))
                    .unwrap();
            writer.write(&batch).unwrap();
            // Build-side duplicates must remain duplicate join matches.
            if name != "probe" {
                writer.write(&batch).unwrap();
            }
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
// Admission-only estimates exercise the same paths even on tiny accuracy data.
fn encourage(node: &mut Node) {
    node.estimated_rows = 1_000_000;
    match &mut node.op {
        RelOp::Scan { distinct, .. } => distinct.fill(1_000_000),
        RelOp::Join { left, right, .. }
        | RelOp::Mark { left, right, .. }
        | RelOp::TypedMark { left, right, .. } => {
            encourage(left);
            encourage(right);
        }
        RelOp::Unary { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Project { input, .. }
        | RelOp::Rename { input }
        | RelOp::Materialize { input, .. }
        | RelOp::Limit { input, .. }
        | RelOp::Scalar { input } => encourage(input),
        _ => {}
    }
}
#[test]
fn runtime_filter_sql_matches_serial_across_workers_and_boundaries() {
    let tables = Tables::new();
    for sql in [
        "SELECT p.n,p.x,p.y,p.d,p.payload FROM probe p JOIN build b ON p.x=b.x JOIN other c ON p.y=c.y ORDER BY 1",
        "SELECT p.n,p.x,p.y,p.payload FROM probe p JOIN build b ON p.x=b.x AND p.y=b.y JOIN other c ON p.y=c.y ORDER BY 1",
        "SELECT p.n,p.payload FROM probe p JOIN (SELECT x FROM build WHERE x<0) b ON p.x=b.x ORDER BY 1",
        "SELECT p.n,p.payload FROM (SELECT * FROM probe WHERE n<0) p JOIN build b ON p.x=b.x ORDER BY 1",
        "SELECT p.n,p.payload,b.x FROM probe p LEFT JOIN build b ON p.x=b.x AND p.y=b.y ORDER BY 1,3",
        "SELECT p.n,p.payload FROM probe p WHERE p.x NOT IN (SELECT x FROM build) ORDER BY 1",
        "SELECT p.n,p.payload FROM probe p JOIN build b ON p.x=b.f ORDER BY 1",
        // DECIMAL casts accept literals only; addition exercises column coercion.
        "SELECT p.n,p.payload FROM probe p JOIN build b ON p.x+0.00=b.d ORDER BY 1",
        "SELECT p.n,p.payload FROM probe p JOIN build b ON p.d=b.d AND p.y=b.y ORDER BY 1",
        "WITH raw AS (SELECT * FROM probe) SELECT p.n,p.payload FROM raw p JOIN raw q ON p.x=q.x JOIN build b ON p.y=b.y ORDER BY 1,2",
    ] {
        for batch_size in [1, 31, 128] {
            let off = Options {
                workers: 1,
                batch_size,
                relational_optimizations: false,
                ..Options::default()
            };
            let expected = execute(&prepare(sql, &tables.0, &off).unwrap(), &off).unwrap();
            let mut serial = None;
            for workers in [1, 2, 4, 10] {
                let on = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let mut prepared = prepare(sql, &tables.0, &on).unwrap();
                encourage(prepared.relational.as_mut().unwrap());
                let actual = execute(&prepared, &on).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "workers={workers}, batch={batch_size}: {sql}"
                );
                if let Some(serial) = &serial {
                    assert_eq!(&actual.batch, serial, "{sql}");
                } else {
                    serial = Some(actual.batch.clone());
                }
            }
        }
    }
}

#[test]
fn movable_hash_and_contiguous_memberships_keep_fixed_plan_order_and_errors() {
    let tables = Tables::new();
    let serial = Options {
        workers: 1,
        batch_size: 7,
        relational_optimizations: false,
        ..Options::default()
    };
    for sql in [
        "SELECT p.n,b.n FROM probe p JOIN build b ON p.x=b.x",
        "SELECT p.n,b.n FROM probe p LEFT JOIN build b ON p.x=b.x",
        "SELECT p.n FROM probe p WHERE EXISTS (SELECT 1 FROM build b WHERE p.x=b.x)",
        "SELECT p.n FROM probe p WHERE NOT EXISTS (SELECT 1 FROM build b WHERE p.x=b.x)",
        "SELECT p.n,b.n FROM probe p JOIN build b ON p.x=b.x AND p.y=b.y",
        "SELECT p.n,b.n FROM probe p LEFT JOIN build b ON p.x=b.x AND p.y=b.y",
        "SELECT p.n FROM probe p WHERE EXISTS (SELECT 1 FROM build b WHERE p.x=b.x AND p.y=b.y)",
        "SELECT p.n FROM probe p WHERE NOT EXISTS (SELECT 1 FROM build b WHERE p.x=b.x AND p.y=b.y)",
        "SELECT p.n,b.x FROM probe p JOIN (SELECT DISTINCT x FROM build) b ON p.x=b.x",
        "SELECT p.n,b.x FROM probe p JOIN (SELECT DISTINCT x,y FROM build) b ON p.x=b.x AND p.y=b.y",
        "SELECT p.n,b.n FROM probe p JOIN build b ON p.x=b.f",
        "SELECT p.n,b.n FROM probe p JOIN build b ON p.x+0.00=b.d AND p.y=b.y",
        "SELECT p.n,b.n FROM probe p LEFT JOIN (SELECT * FROM build WHERE n<0) b ON p.x=b.x",
        "SELECT p.n,b.n FROM (SELECT * FROM probe WHERE n<0) p JOIN build b ON p.x=b.x",
        "SELECT p.n FROM probe p WHERE NOT EXISTS (SELECT 1 FROM build b WHERE b.n<0 AND p.x=b.x)",
        "SELECT p.n*10000000000000000000000000000000000000 FROM probe p JOIN build b ON p.x=b.x",
    ] {
        let mut prepared = prepare(sql, &tables.0, &serial).unwrap();
        encourage(prepared.relational.as_mut().unwrap());
        let expected = execute(&prepared, &serial).map(|result| result.batch);
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                for membership in [false, true] {
                    let options = Options {
                        workers,
                        batch_size,
                        relational_optimizations: membership,
                        ..Options::default()
                    };
                    let actual = execute(&prepared, &options).map(|result| result.batch);
                    assert_eq!(
                        actual, expected,
                        "workers={workers} batch={batch_size} membership={membership}: {sql}"
                    );
                    options.cancellation.cancel();
                    assert_eq!(
                        execute(&prepared, &options).err().unwrap(),
                        options.cancellation.check().unwrap_err()
                    );
                }
            }
        }
    }
}

#[test]
fn narrow_probe_keeps_funded_build_rejection_and_matches_serial() {
    let tables = Tables::new();
    // The scan has only eight non-key payload bytes, so no scan filter is
    // worthwhile. Sparse keys force the hash index's retained rejection bitmap.
    // Keep the real estimates: equalizing them with encourage() would enable
    // an aggregate-demand filter on the DISTINCT build instead.
    let sql =
        "SELECT p.n,p.x FROM probe p JOIN (SELECT DISTINCT x FROM build) b ON p.x=b.x ORDER BY 1";
    for batch_size in [1, 31, 128] {
        let off = Options {
            workers: 1,
            batch_size,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables.0, &off).unwrap(), &off).unwrap();
        for workers in [1, 2, 4, 10] {
            let on = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let prepared = prepare(sql, &tables.0, &on).unwrap();
            let actual = execute(&prepared, &on).unwrap();
            assert_eq!(actual.batch, expected.batch);
            assert_eq!(actual.metrics.demand_filtered_aggregates, 0);
            assert_eq!(actual.metrics.dynamic_scan_filters, 0);
            assert_eq!(actual.metrics.join_rejection_bitmaps, 1);
            assert!(actual.metrics.join_rejection_misses > 0);
            on.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &on).err().unwrap(),
                on.cancellation.check().unwrap_err()
            );
        }
    }
}

#[test]
fn costed_narrow_scan_filter_matches_serial_with_nulls_duplicates_and_boundaries() {
    let tables = Tables::new();
    let sql = "SELECT p.n,p.x FROM probe p JOIN build b ON p.x=b.x ORDER BY 1";
    for batch_size in [1, 31, 128] {
        let off = Options {
            workers: 1,
            batch_size,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables.0, &off).unwrap(), &off).unwrap();
        for workers in [1, 2, 4, 10] {
            let on = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let mut prepared = prepare(sql, &tables.0, &on).unwrap();
            encourage(prepared.relational.as_mut().unwrap());
            let actual = execute(&prepared, &on).unwrap();
            assert_eq!(actual.batch, expected.batch);
            assert!(actual.metrics.dynamic_scan_filters > 0);
            assert!(actual.metrics.bitmap_scan_filters > 0);
            assert_eq!(actual.metrics.join_rejection_bitmaps, 0);
            on.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &on).err().unwrap(),
                on.cancellation.check().unwrap_err()
            );
        }
    }
}

#[test]
fn runtime_filters_preserve_error_and_cancellation_results() {
    let tables = Tables::new();
    let sql = "SELECT p.n*10000000000000000000000000000000000000 FROM probe p JOIN build b ON p.x=b.x JOIN other c ON p.y=c.y";
    for batch_size in [1, 31, 128] {
        let off = Options {
            workers: 1,
            batch_size,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables.0, &off).unwrap(), &off)
            .err()
            .unwrap();
        for workers in [1, 2, 4, 10] {
            let on = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            let mut prepared = prepare(sql, &tables.0, &on).unwrap();
            encourage(prepared.relational.as_mut().unwrap());
            assert_eq!(execute(&prepared, &on).err().unwrap(), expected);
            on.cancellation.cancel();
            assert_eq!(
                execute(&prepared, &on).err().unwrap(),
                on.cancellation.check().unwrap_err()
            );
        }
    }
}

#[test]
fn sideways_sql_keeps_typed_equalities_and_cardinality_barriers() {
    let tables = Tables::new();
    for sql in [
        "SELECT p.n,b.n,c.n FROM probe p JOIN (SELECT * FROM other WHERE y<2) c ON p.y=c.y JOIN build b ON p.y=b.y ORDER BY 1,2,3",
        "SELECT p.n,b.n,c.n FROM probe p JOIN (SELECT * FROM other WHERE y<2) c ON p.y=c.y JOIN build b ON p.x=b.f ORDER BY 1,2,3",
        "SELECT p.n,b.n FROM probe p JOIN other c ON p.y=c.y JOIN build b ON p.d=b.d ORDER BY 1,2",
        "SELECT p.n,b.n FROM probe p LEFT JOIN other c ON p.y=c.y JOIN build b ON p.y=b.y ORDER BY 1,2",
        "SELECT p.n,b.n FROM probe p JOIN (SELECT y FROM other LIMIT 1) c ON p.y=c.y JOIN build b ON p.y=b.y ORDER BY 1,2",
        "SELECT p.n,b.n FROM probe p JOIN (SELECT y,COUNT(*) AS n FROM other GROUP BY y) c ON p.y=c.y JOIN build b ON p.y=b.y ORDER BY 1,2",
    ] {
        let serial = Options {
            workers: 1,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables.0, &serial).unwrap(), &serial).unwrap();
        for batch_size in [1, 7, 8192] {
            for workers in [1, 2, 4, 10] {
                let options = Options {
                    workers,
                    batch_size,
                    ..Options::default()
                };
                let actual =
                    execute(&prepare(sql, &tables.0, &options).unwrap(), &options).unwrap();
                assert_eq!(
                    actual.batch, expected.batch,
                    "workers={workers} batch={batch_size}: {sql}"
                );
            }
        }
    }
}
