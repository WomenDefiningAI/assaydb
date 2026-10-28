//! Exercise production admission, including automatic mode's retained plans.
#[path = "support/auto_spill.rs"]
mod support;
use assaydb::{
    execute::{Options, execute, prepare},
    rel,
    spill::{SpillMode, SpillOptions},
};
use support::{Fixture, QUERIES, assert_bits};

fn options(fixture: &Fixture, workers: usize, mode: SpillMode) -> Options {
    Options {
        workers,
        batch_size: 64,
        memory_bytes: 64 << 20,
        parquet_cache_bytes: Some(0),
        spill: Some(SpillOptions {
            mode,
            directory: fixture.root.join("spill"),
            sort_run_bytes: 64 << 10,
            ..SpillOptions::default()
        }),
        ..Options::default()
    }
}

#[test]
fn auto_preserves_relational_overflow_precedence_at_every_worker_count() {
    use arrow::{
        array::{ArrayRef, Decimal128Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::{fs::File, sync::Arc};

    let mut fixture = Fixture::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("d", DataType::Decimal128(38, 0), false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1; 4096])),
        Arc::new(Int64Array::from(vec![i64::MAX; 4096])),
        Arc::new(
            Decimal128Array::from(vec![10_i128.pow(37); 4096])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let path = fixture.root.join("overflow.parquet");
    let mut writer = ArrowWriter::try_new(
        File::create_new(&path).unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(257)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    fixture.tables.insert("t".into(), path);

    for sql in [
        "SELECT SUM(v), SUM(d) FROM t GROUP BY k",
        "SELECT SUM(d), SUM(v) FROM t GROUP BY k",
    ] {
        for workers in [1, 2, 4] {
            let options = options(&fixture, workers, SpillMode::Auto);
            // Bypass auto_simple dispatch while retaining pre-card-06 auto
            // policy; forced external aggregation is a different algorithm.
            let relational = rel::prepare(sql, &fixture.tables, &options).unwrap();
            let expected = execute(&relational, &options).err().unwrap();
            assert_eq!(expected, "decimal precision 38 overflow");
            let prepared = prepare(sql, &fixture.tables, &options).unwrap();
            assert_eq!(
                execute(&prepared, &options).err().unwrap(),
                expected,
                "{sql}: workers={workers}"
            );
            fixture.assert_clean();
        }
    }
}

#[test]
fn every_direct_fixture_matches_relational_bits_with_auto_and_force() {
    let fixture = Fixture::new();
    let serial = options(&fixture, 1, SpillMode::Auto);
    for sql in QUERIES.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let relational = rel::prepare(sql, &fixture.tables, &serial).unwrap();
        let expected = execute(&relational, &serial).unwrap();
        for workers in [1, 2, 4, 10] {
            for mode in [SpillMode::Auto, SpillMode::Force] {
                let options = options(&fixture, workers, mode);
                let prepared = prepare(sql, &fixture.tables, &options).unwrap();
                assert!(prepared.relational.is_some());
                let actual = execute(&prepared, &options).unwrap();
                if mode == SpillMode::Force {
                    // External aggregation already emits hash-partition order
                    // without ORDER BY. Preserve that existing path exactly.
                    let forced = execute(&relational, &options).unwrap();
                    assert_bits(&actual.batch, &forced.batch, sql);
                } else {
                    assert_bits(&actual.batch, &expected.batch, sql);
                }
                assert_eq!(actual.metrics.auto_spill_retries, 0, "{sql}");
                assert_eq!(
                    actual.metrics.pipeline_candidate_attempts,
                    usize::from(mode == SpillMode::Auto && workers > 1),
                    "{sql}: workers={workers}, mode={mode:?}"
                );
                if mode == SpillMode::Auto && workers > 1 {
                    assert!(actual.metrics.pipeline_producers > 0, "{sql}");
                    assert_eq!(actual.metrics.pipeline_fallbacks, 0, "{sql}");
                }
                assert!(actual.metrics.peak_reserved_bytes <= options.memory_bytes);
                fixture.assert_clean();
            }
        }
    }
}

#[test]
fn auto_pipeline_keeps_sql_errors_and_noneligible_dispatch() {
    let fixture = Fixture::new();
    // Float SUM now succeeds, but cannot use reassociated partial aggregation.
    let sql = "SELECT k,SUM(f) FROM t GROUP BY k";
    for workers in [1, 2, 4, 10] {
        let options = options(&fixture, workers, SpillMode::Auto);
        let expected = rel::prepare(sql, &fixture.tables, &options)
            .and_then(|p| execute(&p, &options))
            .unwrap();
        let actual = prepare(sql, &fixture.tables, &options)
            .and_then(|p| execute(&p, &options))
            .unwrap();
        assert_bits(&actual.batch, &expected.batch, sql);
        assert_eq!(actual.metrics.pipeline_producers, 0);
        fixture.assert_clean();
    }
    for sql in [
        "SELECT k,COUNT(CAST(1.0 / denom AS INTEGER)) FROM t GROUP BY k",
        "SELECT k,SUM(wide) FROM t GROUP BY k",
    ] {
        let serial = options(&fixture, 1, SpillMode::Auto);
        let relational = rel::prepare(sql, &fixture.tables, &serial).unwrap();
        let expected = execute(&relational, &serial).err().unwrap();
        for workers in [1, 2, 4, 10] {
            let options = options(&fixture, workers, SpillMode::Auto);
            let prepared = prepare(sql, &fixture.tables, &options).unwrap();
            assert_eq!(execute(&prepared, &options).err().unwrap(), expected);
            fixture.assert_clean();
        }
    }
    for sql in [
        "SELECT k,MIN(v) FROM t GROUP BY k",
        "SELECT k,COUNT(DISTINCT v) FROM t GROUP BY k",
        "SELECT COUNT(*) FROM t",
        "SELECT k,MIN(f),MAX(f) FROM t GROUP BY k",
    ] {
        let options = options(&fixture, 10, SpillMode::Auto);
        let relational = rel::prepare(sql, &fixture.tables, &options).unwrap();
        let expected = execute(&relational, &options).unwrap();
        let prepared = prepare(sql, &fixture.tables, &options).unwrap();
        let actual = execute(&prepared, &options).unwrap();
        assert_bits(&actual.batch, &expected.batch, sql);
        assert_eq!(actual.metrics.pipeline_candidate_attempts, 0, "{sql}");
    }
}

#[test]
fn existing_grouped_decimal_fixtures_match_both_plans_bit_for_bit() {
    use arrow::compute::concat_batches;
    use assaydb::session::{Limits, Output, Session};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use std::fs::File;

    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/sqlite-decimal-float-sweep.json")).unwrap();
    let mut covered = 0;
    for case in cases["cases"].as_array().unwrap() {
        let sql = case["sql"].as_str().unwrap();
        if !sql.contains("GROUP BY") {
            continue;
        }
        let mut fixture = Fixture::new();
        let mut session =
            Session::new(&fixture.root, Options::default(), Limits::default()).unwrap();
        for setup in case["setup"].as_array().unwrap() {
            session.execute_sql(setup.as_str().unwrap()).unwrap();
        }
        let Output::Query { execution, .. } = session.execute_sql("SELECT * FROM t").unwrap()
        else {
            panic!("fixture table query");
        };
        // Repeat the original one-row decimal fixture to exercise production
        // admission across many misaligned row groups; AVG is unchanged.
        assert_eq!(execution.batch.num_rows(), 1);
        let batch = concat_batches(
            &execution.batch.schema(),
            std::iter::repeat_n(&execution.batch, 4096),
        )
        .unwrap();
        let path = fixture.root.join("decimal.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create_new(&path).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(257)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        fixture.tables.insert("t".into(), path);
        let serial = options(&fixture, 1, SpillMode::Auto);
        let relational = rel::prepare(sql, &fixture.tables, &serial).unwrap();
        let expected = execute(&relational, &serial).unwrap();
        let expected_bits =
            u64::from_str_radix(case["expected"]["bits"].as_str().unwrap(), 16).unwrap();
        assert_eq!(
            expected
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            expected_bits
        );
        for workers in [1, 2, 4, 10] {
            let options = options(&fixture, workers, SpillMode::Auto);
            let prepared = prepare(sql, &fixture.tables, &options).unwrap();
            let actual = execute(&prepared, &options).unwrap();
            assert_bits(&actual.batch, &expected.batch, sql);
            assert_eq!(
                actual.metrics.pipeline_candidate_attempts,
                usize::from(workers > 1)
            );
        }
        fixture.assert_clean();
        covered += 1;
    }
    assert_eq!(covered, 2);
}
