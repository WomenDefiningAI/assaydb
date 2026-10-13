use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{Options, execute, prepare};
use parquet::arrow::ArrowWriter;
use std::{
    collections::HashMap,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn short_keys_nulls_aliases_and_late_cardinality_match_uncached_execution() {
    let root = std::env::temp_dir().join(format!("assaydb-short-cache-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut cache_hits = 0;
    for transition in [false, true] {
        let strings: Vec<Option<String>> = (0..2048)
            .map(|i| {
                if transition && i >= 1024 {
                    Some(format!("late-{i}"))
                } else {
                    [None, Some(""), Some("a\0"), Some("é"), Some("🦆")][i % 5].map(str::to_owned)
                }
            })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter(strings.iter().map(|s| s.as_deref()))),
            Arc::new(StringArray::from_iter(
                (0..2048).map(|i| [None, Some(""), Some("x")][i % 3]),
            )),
            Arc::new(Int64Array::from_iter_values((0..2048).map(|i| i % 23 - 11))),
        ];
        let path = root.join(format!("t-{transition}.parquet"));
        let mut writer =
            ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(&RecordBatch::try_new(schema.clone(), arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        let tables = HashMap::from([("t".into(), path)]);
        for sql in [
            "SELECT a,SUM(v) FROM t GROUP BY a ORDER BY 1",
            "SELECT a,COUNT(*),COUNT(a),SUM(v),AVG(v),MIN(b),MAX(b) FROM t GROUP BY a ORDER BY 1",
            "SELECT a,b,SUM(v),COUNT(*) FROM t GROUP BY a,b ORDER BY 1,2",
            "SELECT a,COUNT(DISTINCT b),SUM(DISTINCT v) FROM t GROUP BY a ORDER BY 1",
            "SELECT k,COUNT(*),SUM(v) FROM (SELECT a AS k,v FROM t) s GROUP BY k ORDER BY 1",
        ] {
            let base = Options {
                relational_optimizations: false,
                ..Options::default()
            };
            let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
            assert_eq!(expected.metrics.short_group_cache_hits, 0);
            for batch_size in [17, 128, 256] {
                for workers in [1, 4] {
                    for memory_bytes in [4 << 20, 256 << 20] {
                        let options = Options {
                            batch_size,
                            workers,
                            memory_bytes,
                            ..Options::default()
                        };
                        let result =
                            execute(&prepare(sql, &tables, &options).unwrap(), &options).unwrap();
                        assert_eq!(
                            result.batch, expected.batch,
                            "{sql} transition{transition} batch{batch_size} workers{workers} budget{memory_bytes}"
                        );
                        cache_hits += result.metrics.short_group_cache_hits;
                        if batch_size < 64 {
                            assert_eq!(result.metrics.short_group_cache_batches, 0);
                        } else if !transition {
                            assert!(
                                result.metrics.short_group_cache_hits > 0,
                                "cache not exercised: {sql}"
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(cache_hits > 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fused_sum_final_overflow_after_cached_batches_keeps_error() {
    let root =
        std::env::temp_dir().join(format!("assaydb-short-cache-error-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Decimal128(38, 0), false),
    ]));
    let values = (0..1024).map(|i| if i >= 512 { i128::MAX / 2 } else { 0 });
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["same"; 1024])),
        Arc::new(
            Decimal128Array::from_iter_values(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
    ];
    let path = root.join("t.parquet");
    let mut writer =
        ArrowWriter::try_new(File::create_new(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    let tables = HashMap::from([("t".into(), path)]);
    // Semantic v2 defers the same out-of-range final SUM error until output.
    let sql = "SELECT k,SUM(v) FROM t GROUP BY k";
    for batch_size in [64, 128, 256] {
        let options = Options {
            batch_size,
            relational_optimizations: false,
            ..Options::default()
        };
        let expected = execute(&prepare(sql, &tables, &options).unwrap(), &options)
            .err()
            .unwrap();
        assert!(expected.contains("overflow"), "{expected}");
        for workers in [1, 4] {
            let options = Options {
                workers,
                relational_optimizations: true,
                ..options.clone()
            };
            let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options)
                .err()
                .unwrap();
            assert_eq!(actual, expected);
        }
    }
    fs::remove_dir_all(root).unwrap();
}
