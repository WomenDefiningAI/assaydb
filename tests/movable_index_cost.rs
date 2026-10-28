use arrow::{
    array::{Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

struct Tables(HashMap<String, PathBuf>);
impl Tables {
    fn new() -> Self {
        let mut tables = Self(HashMap::new());
        for (name, rows) in [("customers", 257), ("purchases", 4096)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("balance", DataType::Decimal128(15, 2), true),
                Field::new("phone", DataType::Utf8, false),
                Field::new("nullable_k", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|i| if name == "customers" { i } else { i % 193 }),
                    )),
                    Arc::new(
                        Decimal128Array::from_iter(
                            (0..rows).map(|i| (i % 43 != 0).then_some(i128::from(i) * 101)),
                        )
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                    ),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|i| if i % 3 == 0 { "13-123" } else { "31-456" }),
                    )),
                    Arc::new(Int64Array::from_iter((0..rows).map(|i| {
                        (i % 37 != 0).then_some(if name == "customers" { i } else { i % 193 })
                    }))),
                ],
            )
            .unwrap();
            let path = std::env::temp_dir().join(format!(
                "assaydb-movable-index-cost-{}-{name}.parquet",
                std::process::id()
            ));
            let mut writer = ArrowWriter::try_new(
                File::create_new(&path).unwrap(),
                schema,
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(127)
                        .build(),
                ),
            )
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

#[test]
fn duplicate_numeric_anti_join_with_scalar_filter_matches_serial() {
    let tables = Tables::new();
    let serial = Options {
        workers: 1,
        batch_size: 127,
        relational_optimizations: false,
        ..Options::default()
    };
    for (case, sql) in [
        "SELECT c.k,c.balance FROM customers c WHERE substring(c.phone FROM 1 FOR 2) IN ('13','31') AND c.balance > (SELECT avg(balance) FROM customers WHERE balance>0.00) AND NOT EXISTS (SELECT 1 FROM purchases p WHERE p.k=c.k) ORDER BY 1",
        "SELECT c.k FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE p.k=c.k) ORDER BY 1",
        "SELECT c.k FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE p.k=c.k AND p.k<0) ORDER BY 1",
        "SELECT c.k FROM customers c WHERE c.k<0 AND NOT EXISTS (SELECT 1 FROM purchases p WHERE p.k=c.k) ORDER BY 1",
        "SELECT c.k FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE p.balance=c.k+0.00) ORDER BY 1",
        "SELECT c.nullable_k FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE p.nullable_k=c.nullable_k) ORDER BY 1",
        "SELECT c.k FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE CAST(p.k AS REAL)=c.k) ORDER BY 1",
        "SELECT c.k*10000000000000000000000000000000000000 FROM customers c WHERE NOT EXISTS (SELECT 1 FROM purchases p WHERE p.k=c.k) ORDER BY 1",
    ].into_iter().enumerate() {
        let plan = prepare(sql, &tables.0, &Options::default()).unwrap();
        let expected = execute(&plan, &serial).map(|result| result.batch);
        if case == 7 {
            assert!(expected.is_err(), "overflow control must fail");
        }
        for workers in [1, 2, 4, 10] {
            for batch_size in [31, 127, 8192] {
                let options = Options { workers, batch_size, ..Options::default() };
                let result = execute(&plan, &options);
                if case == 0 {
                    let metrics = &result.as_ref().unwrap().metrics;
                    // Complete AVG, then retain the 125 non-NULL balances
                    // above it (keys 130..256 except 172 and 215). The scalar
                    // cross join builds one row: 4097 -> 126 build rows.
                    assert_eq!(metrics.join_build_rows, 126);
                    assert_eq!(metrics.dense_joins, usize::from(workers == 1));
                    assert_eq!(metrics.reverse_existence_numeric_lane_joins, usize::from(workers > 1));
                    assert_eq!(metrics.reverse_existence_joins, 1);
                    assert_eq!(metrics.join_rejection_bitmaps, 0);
                }
                assert_eq!(result.map(|result| result.batch), expected, "workers={workers}, batch={batch_size}: {sql}");
                options.cancellation.cancel();
                assert_eq!(execute(&plan, &options).err().unwrap(), options.cancellation.check().unwrap_err());
            }
        }
    }
}
