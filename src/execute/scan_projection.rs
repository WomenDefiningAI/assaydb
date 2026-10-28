//! Prepare aggregate input expressions on the existing ordered scan lanes.
//! Only independent batch work moves; checked state transitions remain ordered.
use super::{Options, Prepared};
use crate::{
    Result,
    expr::{Input, normalize_input},
    expr_program::{ExpressionProgram, Projection},
    memory::Budget,
    parquet_scan::{BatchTransform, TransformedBatch},
    types::Type,
};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Default)]
pub(super) struct Statistics {
    pub batches: usize,
    pub group_key_batches: usize,
    pub rows: usize,
    pub raw_bytes: usize,
    pub expression_seconds: f64,
    pub normalize_seconds: f64,
}

pub(super) fn transform(
    prepared: &Prepared,
    expressions: Arc<ExpressionProgram>,
    filter: Option<BatchTransform>,
    options: &Options,
    budget: &Budget,
    stats: Arc<Mutex<Statistics>>,
) -> Result<(BatchTransform, Arc<Projection>)> {
    let columns = prepared
        .plan
        .columns
        .iter()
        .map(|i| Type::from_field(prepared.schema.field(*i)).map(|ty| (*i, ty)))
        .collect::<Result<Vec<_>>>()?;
    let width = prepared.schema.fields().len();
    let layout = Arc::new(expressions.projection());
    let projection = layout.clone();
    let budget = budget.clone();
    let cancellation = options.cancellation.clone();
    let transform: BatchTransform = Arc::new(move |batch, lease| {
        cancellation.check()?;
        let raw_rows = batch.num_rows();
        let raw_bytes = batch.get_array_memory_size();
        let (batch, lease) = if let Some(filter) = &filter {
            let output = filter(batch, lease)?;
            (output.batch, output.lease)
        } else {
            (batch, lease)
        };
        let start = Instant::now();
        let rows = batch.num_rows();
        let mut input = Input {
            columns: vec![None; width],
            len: rows,
        };
        for (local, (original, ty)) in columns.iter().enumerate() {
            input.columns[*original] = Some(normalize_input(batch.column(local), ty, &budget)?);
        }
        drop(batch);
        drop(lease);
        let normalize_seconds = start.elapsed().as_secs_f64();
        let start = Instant::now();
        // Evaluate even empty batches, in the original binding/group/input order.
        let (keys, values) = expressions.evaluate(&mut input, &budget)?;
        drop(input);
        let (batch, lease) = projection.pack(rows, &keys, &values, &budget)?;
        drop(keys);
        drop(values);
        cancellation.check()?;
        let mut stats = stats.lock().unwrap();
        stats.batches += 1;
        stats.group_key_batches += usize::from(projection.prepares_text_keys());
        stats.rows += raw_rows;
        stats.raw_bytes += raw_bytes;
        stats.normalize_seconds += normalize_seconds;
        stats.expression_seconds += start.elapsed().as_secs_f64();
        Ok(TransformedBatch {
            batch,
            lease,
            preserve_empty: true,
        })
    });
    Ok((transform, layout))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::{execute, execute_in_budget, prepare};
    use crate::expr::Value;
    use arrow::{
        array::{
            ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
            Int64Array, StringArray,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::{
        collections::HashMap,
        fs::File,
        sync::atomic::{AtomicUsize, Ordering},
    };
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
    fn fixture(groups: usize) -> (Fixture, HashMap<String, std::path::PathBuf>) {
        let path = std::env::temp_dir().join(format!(
            "assaydb-scan-expressions-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("k", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(38, 0), false),
            Field::new("b", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
            Field::new("scaled", DataType::Decimal128(16, 3), true),
            Field::new("f", DataType::Float64, true),
        ]));
        let high = 10_i128.pow(38) - 1;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..257)),
            Arc::new(Int32Array::from_iter(
                (0..257).map(|i| (i % 7 != 0).then_some(i % 13)),
            )),
            Arc::new(StringArray::from_iter(
                (0..257).map(|i| (i % 11 != 0).then(|| format!("key_{}é", i % 5))),
            )),
            Arc::new(
                Decimal128Array::from_iter_values(
                    (0..257).map(|i| if i % 2 == 0 { high } else { -high }),
                )
                .with_precision_and_scale(38, 0)
                .unwrap(),
            ),
            Arc::new(BooleanArray::from_iter(
                (0..257).map(|i| (i % 17 != 0).then_some(i % 2 == 0)),
            )),
            Arc::new(Date32Array::from_iter(
                (0..257).map(|i| (i % 19 != 0).then_some(i + 100)),
            )),
            Arc::new(
                Decimal128Array::from_iter((0..257).map(|i| (i % 7 != 0).then_some(i * 123)))
                    .with_precision_and_scale(16, 3)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from_iter(
                (0..257).map(|i| (i % 11 != 0).then_some((i % 3) as f64 / 3.0)),
            )),
        ];
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(groups)
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        writer.close().unwrap();
        (Fixture(path.clone()), HashMap::from([("t".into(), path)]))
    }
    #[test]
    fn typed_projection_preserves_aliases_row_counts_and_synchronous_fallback() {
        for group_size in [31, 1024] {
            let (_file, tables) = fixture(group_size);
            for projection_pushdown in [false, true] {
                for cache in [None, Some(0), Some(1 << 20)] {
                    let base = Options {
                        batch_size: 17,
                        memory_bytes: 32 << 20,
                        projection_pushdown,
                        parquet_cache_bytes: cache,
                        ..Options::default()
                    };
                    for sql in [
                        "SELECT s,substring(s,1,3) AS prefix,COUNT(*) AS n,SUM(k) AS total,AVG(k) AS avg FROM t GROUP BY s,substring(s,1,3) ORDER BY s,prefix",
                        "SELECT s,CASE WHEN i<100 THEN NULL ELSE 'x' END AS g,COUNT(*) AS n FROM t GROUP BY s,CASE WHEN i<100 THEN NULL ELSE 'x' END ORDER BY s,g",
                        "SELECT s,substring(s,1,3) AS prefix,COUNT(*) AS n FROM t WHERE FALSE GROUP BY s,substring(s,1,3) ORDER BY s,prefix",
                        "SELECT COUNT(*) AS n FROM t",
                        "SELECT COUNT(*) AS n,COUNT(NULL) AS z FROM t WHERE i<0",
                        "SELECT COUNT(*) AS n,COUNT(NULL) AS z FROM t",
                        "SELECT s,b,day,COUNT(*) AS n,SUM(k) AS s1,AVG(k) AS a,COUNT(k) AS c FROM t GROUP BY s,b,day ORDER BY s,b,day",
                        "SELECT SUM(k+1) AS s,AVG(k+1) AS a,COUNT(k+1) AS c FROM t WHERE k<4",
                        "SELECT COUNT(DISTINCT s) AS n,MIN(day) AS lo,MAX(b) AS hi FROM t",
                        "SELECT f,COUNT(f) AS n,MIN(f) AS lo,SUM(scaled) AS total,AVG(scaled) AS a FROM t GROUP BY f ORDER BY f",
                    ] {
                        let expected =
                            execute(&prepare(sql, &tables, &base).unwrap(), &base).unwrap();
                        for occupied in [false, true] {
                            let options = Options {
                                workers: 4,
                                ..base.clone()
                            };
                            let budget = Budget::new(options.memory_bytes);
                            let permits = occupied.then(|| budget.workers(3, 3));
                            let prepared = prepare(sql, &tables, &options).unwrap();
                            assert!(prepared.relational.is_none(), "{sql}");
                            let actual =
                                execute_in_budget(&prepared, &options, budget.clone()).unwrap();
                            assert_eq!(actual.batch, expected.batch, "{sql}");
                            assert!(actual.metrics.scan_expression_batches > 0);
                            let text_groups = prepared.plan.groups.len() >= 2
                                && prepared.plan.groups.iter().all(|e| e.ty == Type::Text);
                            assert_eq!(actual.metrics.scan_group_key_batches > 0, text_groups);

                            assert_eq!(actual.metrics.rows_read, 257);
                            assert_eq!(
                                actual.metrics.rows_after_filter,
                                expected.metrics.rows_after_filter
                            );
                            if occupied || group_size == 1024 {
                                assert_eq!(actual.metrics.parallel_scan_tasks, 0);
                            }
                            drop(actual);
                            assert_eq!(budget.used(), 0);
                            drop(permits);
                            assert_eq!(budget.workers(3, 3).count, 3);
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn serial_exact_filter_preserves_results_raw_metrics_errors_and_cleanup() {
        let (_file, tables) = fixture(31);
        for projection_pushdown in [false, true] {
            let control = Options {
                workers: 1,
                batch_size: 17,
                memory_bytes: 16 << 20,
                relational_optimizations: false,
                projection_pushdown,
                ..Options::default()
            };
            let options = Options {
                relational_optimizations: true,
                ..control.clone()
            };
            for sql in [
                "SELECT SUM(k+1) AS s,COUNT(*) AS n FROM t WHERE k<4 OR k IS NULL",
                "SELECT s FROM t WHERE k IS NULL ORDER BY s",
                "SELECT COUNT(*) AS n,SUM(k) AS s FROM t WHERE i<0",
                "SELECT COUNT(*) AS n FROM t WHERE FALSE",
            ] {
                let expected =
                    execute(&prepare(sql, &tables, &control).unwrap(), &control).unwrap();
                let budget = Budget::new(options.memory_bytes);
                let actual = execute_in_budget(
                    &prepare(sql, &tables, &options).unwrap(),
                    &options,
                    budget.clone(),
                )
                .unwrap();
                assert_eq!(actual.batch, expected.batch);
                assert_eq!(actual.metrics.rows_read, 257);
                assert_eq!(
                    actual.metrics.decoded_arrow_bytes_total,
                    expected.metrics.decoded_arrow_bytes_total
                );
                assert_eq!(
                    actual.metrics.rows_after_filter,
                    expected.metrics.rows_after_filter
                );
                assert_eq!(actual.metrics.post_decode_scan_filters, 1);
                assert_eq!(actual.metrics.scan_expression_batches, 0);
                drop(actual);
                assert_eq!(budget.used(), 0);
            }
            let sql = "SELECT SUM(k) AS s FROM t WHERE 100.0/(i-40)>0";
            let expected = execute(&prepare(sql, &tables, &control).unwrap(), &control)
                .err()
                .unwrap();
            let budget = Budget::new(options.memory_bytes);
            let error = execute_in_budget(
                &prepare(sql, &tables, &options).unwrap(),
                &options,
                budget.clone(),
            )
            .err()
            .unwrap();
            assert_eq!(error, expected);
            assert_eq!(budget.used(), 0);
            for bytes in [1024, 8192, 32768, 131072] {
                let options = Options {
                    memory_bytes: bytes,
                    ..options.clone()
                };
                let budget = Budget::new(bytes);
                let result = execute_in_budget(
                    &prepare(
                        "SELECT SUM(k) AS s FROM t WHERE k<4",
                        &tables,
                        &Options {
                            memory_bytes: 16 << 20,
                            ..options.clone()
                        },
                    )
                    .unwrap(),
                    &options,
                    budget.clone(),
                );
                if let Err(error) = &result {
                    assert!(
                        error.starts_with("resource-limit: accounted memory"),
                        "{error}"
                    );
                }
                drop(result);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn prepared_text_projection_aliases_and_failed_handoffs_release_leases() {
        let (_file, tables) = fixture(31);
        let options = Options::default();
        let prepared = prepare("SELECT s,substring(s,1,2) AS p,SUM(k),AVG(k),COUNT(*) FROM t GROUP BY s,substring(s,1,2)", &tables, &options).unwrap();
        let program = ExpressionProgram::new(&prepared.plan, prepared.schema.fields().len(), true);
        let projection = program.projection();
        assert!(projection.prepares_text_keys());
        let budget = Budget::new(1 << 20);
        let keys = vec![
            Value::adopt(
                Arc::new(StringArray::from(vec![None, Some("a"), Some("ab"), None])),
                &budget,
            )
            .unwrap(),
            Value::adopt(
                Arc::new(StringArray::from(vec![Some("x"), None, Some("c"), None])),
                &budget,
            )
            .unwrap(),
        ];
        let value = Value::adopt(
            Arc::new(
                Decimal128Array::from(vec![Some(1), Some(2), None, Some(4)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            &budget,
        )
        .unwrap();
        let values = vec![Some(value.clone()), Some(value.clone()), None];
        let steady = budget.used();
        let blocker = budget.reserve(budget.limit() - steady).unwrap();
        assert!(projection.pack(4, &keys, &values, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), steady);
        let (batch, lease) = projection.pack(4, &keys, &values, &budget).unwrap();
        assert_eq!(batch.num_columns(), 4);
        let before_unpack = budget.used();
        let blocker = budget.reserve(budget.limit() - before_unpack).unwrap();
        assert!(projection.unpack(&batch, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), before_unpack);
        // Encoding succeeds, then the projection bridge is rejected.
        let probe = crate::keys::Keys::new(&keys, 4, &budget).unwrap();
        let encoding_charge = budget.used() - before_unpack;
        drop(probe);
        let blocker = budget
            .reserve(budget.limit() - before_unpack - encoding_charge)
            .unwrap();
        assert!(projection.pack(4, &keys, &values, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), before_unpack);
        // All ordinary Values are adopted before the private key lease fails.
        let ordinary_charge: usize = batch.columns()[..3]
            .iter()
            .map(|a| a.get_array_memory_size())
            .sum();
        let blocker = budget
            .reserve(budget.limit() - before_unpack - ordinary_charge)
            .unwrap();
        assert!(projection.unpack(&batch, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), before_unpack);
        let (unpacked, inputs, encoded) = projection.unpack(&batch, &budget).unwrap();
        assert!(Arc::ptr_eq(
            &inputs[0].as_ref().unwrap().array,
            &inputs[1].as_ref().unwrap().array
        ));
        let expected = crate::keys::Keys::new(&keys, 4, &budget).unwrap();
        for i in 0..4 {
            assert_eq!(encoded.as_ref().unwrap().row(i), expected.row(i));
        }
        drop(expected);
        drop(unpacked);
        drop(inputs);
        drop(encoded);
        drop(batch);
        drop(lease);
        assert_eq!(budget.used(), steady);
        drop(keys);
        drop(values);
        drop(value);
        assert_eq!(budget.used(), 0);
        for bytes in [1024, 8192, 32768, 131072] {
            let options = Options {
                workers: 4,
                batch_size: 17,
                memory_bytes: bytes,
                ..Options::default()
            };
            let budget = Budget::new(bytes);
            let result = execute_in_budget(&prepared, &options, budget.clone());
            if let Err(e) = &result {
                assert!(e.starts_with("resource-limit: accounted memory"), "{e}");
            }
            drop(result);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(3, 3).count, 3);
        }
    }
    #[test]
    fn projection_alias_handoffs_and_rejection_keep_one_owner_per_column() {
        let (_file, tables) = fixture(31);
        let options = Options::default();
        let prepared = prepare(
            "SELECT SUM(k) AS s,AVG(k) AS a,COUNT(k) AS n,COUNT(*) AS all_rows FROM t",
            &tables,
            &options,
        )
        .unwrap();
        let program = ExpressionProgram::new(&prepared.plan, prepared.schema.fields().len(), true);
        let projection = program.projection();
        let budget = Budget::new(1 << 20);
        let value = crate::expr::Value::adopt(
            Arc::new(
                Decimal128Array::from(vec![Some(1), None, Some(3)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            &budget,
        )
        .unwrap();
        let bytes = budget.used();
        let values = vec![
            Some(value.clone()),
            Some(value.clone()),
            Some(value.clone()),
            None,
        ];
        let blocker = budget.reserve(budget.limit() - budget.used()).unwrap();
        assert!(projection.pack(3, &[], &values, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), bytes);
        let (batch, lease) = projection.pack(3, &[], &values, &budget).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
        drop(values);
        drop(value);
        assert_eq!(budget.used(), bytes);
        let blocker = budget.reserve(budget.limit() - budget.used()).unwrap();
        assert!(projection.unpack(&batch, &budget).is_err());
        drop(blocker);
        assert_eq!(budget.used(), bytes);
        let (keys, values, encoded) = projection.unpack(&batch, &budget).unwrap();
        assert!(encoded.is_none());
        assert!(keys.is_empty());
        assert!(values[3].is_none());
        assert!(Arc::ptr_eq(
            &values[0].as_ref().unwrap().array,
            &values[1].as_ref().unwrap().array
        ));
        assert!(Arc::ptr_eq(
            &values[0].as_ref().unwrap().array,
            &values[2].as_ref().unwrap().array
        ));
        drop(batch);
        drop(lease);
        assert_eq!(budget.used(), bytes);
        drop(values);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn projection_retains_expression_and_aggregate_error_precedence_and_cleanup() {
        let (_file, tables) = fixture(31);
        let base = Options {
            batch_size: 17,
            memory_bytes: 16 << 20,
            ..Options::default()
        };
        // Semantic v2 defers SUM range checks to finalization. Both early
        // and later input-expression failures therefore precede SUM overflow.
        for (sql, expected_fragment) in [
            (
                "SELECT CASE WHEN i>=0 THEN 'x' ELSE 'z' END AS a,CASE WHEN i>=0 THEN 'y' ELSE 'z' END AS b,SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS s,SUM(CASE WHEN 100.0/(i-1)>0 THEN i ELSE 0 END) AS x FROM t GROUP BY CASE WHEN i>=0 THEN 'x' ELSE 'z' END,CASE WHEN i>=0 THEN 'y' ELSE 'z' END",
                "division by zero",
            ),
            (
                "SELECT CASE WHEN i>=0 THEN 'x' ELSE 'z' END AS a,CASE WHEN i>=0 THEN 'y' ELSE 'z' END AS b,SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS s,SUM(CASE WHEN 100.0/(i-100)>0 THEN i ELSE 0 END) AS x FROM t GROUP BY CASE WHEN i>=0 THEN 'x' ELSE 'z' END,CASE WHEN i>=0 THEN 'y' ELSE 'z' END",
                "division by zero",
            ),
            (
                "SELECT SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS s,SUM(CASE WHEN 100.0/(i-1)>0 THEN i ELSE 0 END) AS x FROM t",
                "division by zero",
            ),
            (
                "SELECT SUM(CASE WHEN i=1 THEN 1 ELSE d END) AS s,SUM(CASE WHEN 100.0/(i-100)>0 THEN i ELSE 0 END) AS x FROM t",
                "division by zero",
            ),
        ] {
            let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base)
                .err()
                .unwrap();
            assert!(expected.contains(expected_fragment), "{expected}");
            for workers in [2, 4, 10] {
                let options = Options {
                    workers,
                    ..base.clone()
                };
                let budget = Budget::new(options.memory_bytes);
                let error = execute_in_budget(
                    &prepare(sql, &tables, &options).unwrap(),
                    &options,
                    budget.clone(),
                )
                .err()
                .unwrap();
                assert_eq!(error, expected);
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.workers(workers - 1, workers - 1).count, workers - 1);
            }
        }
        // Empty batches retain evaluation, whether this expression succeeds or
        // fails under the existing zero-row arithmetic semantics.
        let sql = "SELECT SUM(CASE WHEN 100/(i-i)>0 THEN i ELSE 0 END) AS s FROM t WHERE i<0";
        let expected = execute(&prepare(sql, &tables, &base).unwrap(), &base).map(|v| v.batch);
        let options = Options { workers: 4, ..base };
        let actual = execute(&prepare(sql, &tables, &options).unwrap(), &options).map(|v| v.batch);
        assert_eq!(actual, expected);
        let mut successes = 0;
        let mut failures = 0;
        for bytes in [1024, 8192, 32768, 131072, 1 << 20, 16 << 20] {
            let options = Options {
                memory_bytes: bytes,
                ..options.clone()
            };
            let budget = Budget::new(bytes);
            let sql = "SELECT s,COUNT(*) AS n,SUM(k+1) AS total,AVG(k+1) AS a FROM t GROUP BY s";
            match execute_in_budget(
                &prepare(
                    sql,
                    &tables,
                    &Options {
                        memory_bytes: 16 << 20,
                        ..options.clone()
                    },
                )
                .unwrap(),
                &options,
                budget.clone(),
            ) {
                Ok(output) => {
                    successes += 1;
                    drop(output);
                }
                Err(error) => {
                    assert!(
                        error.starts_with("resource-limit: accounted memory"),
                        "{error}"
                    );
                    failures += 1;
                }
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.workers(3, 3).count, 3);
        }
        assert!(successes > 0 && failures > 0);
        let prepared = prepare(
            "SELECT s,substring(s,1,2) AS p,COUNT(*) AS n FROM t GROUP BY s,substring(s,1,2)",
            &tables,
            &options,
        )
        .unwrap();
        options.cancellation.cancel();
        let budget = Budget::new(options.memory_bytes);
        let error = execute_in_budget(&prepared, &options, budget.clone())
            .err()
            .unwrap();
        assert!(error.contains("cancel"), "{error}");
        assert_eq!(budget.used(), 0);
    }
}
