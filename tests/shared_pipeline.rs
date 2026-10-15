//! Public defaults: no test-only dispatch controls exist in this build.
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::execute::{self, Options};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new(source_domains: bool) -> Self {
        let path = std::env::temp_dir().join(format!(
            "shared-public-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                (0..4096)
                    .map(|i| if source_domains { i / 2 } else { i % 53 })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..4096).map(|i| i % 7 - 2).collect::<Vec<_>>(),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
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
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}
#[test]
fn public_shared_grouping_admits_metadata_owners_and_preserves_overlap_scan_reuse() {
    for source_domains in [false, true] {
        let fixture = Fixture::new(source_domains);
        let sql = "SELECT a.k,SUM(a.v) FROM t a WHERE a.k IN (SELECT k FROM t GROUP BY k HAVING SUM(v)>0) GROUP BY a.k ORDER BY 1";
        let mut expected = None;
        for workers in [1, 2, 10] {
            let options = Options {
                workers,
                batch_size: 64,
                memory_bytes: 64 << 20,
                ..Options::default()
            };
            let prepared = execute::prepare(
                sql,
                &HashMap::from([("t".into(), fixture.0.clone())]),
                &options,
            )
            .unwrap();
            let result = execute::execute(&prepared, &options).unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&result.batch, expected);
            } else {
                expected = Some(result.batch.clone());
            }
            assert_eq!(result.metrics.scan_materializations, 1);
            // Metadata ownership bypasses the inner materialization at every
            // width. Overlap keeps the old shared proof/cache reuse path.
            assert_eq!(
                result.metrics.scan_reuses,
                if source_domains {
                    0
                } else if workers > 1 {
                    2
                } else {
                    1
                }
            );
            assert_eq!(result.metrics.pipeline_candidate_attempts, 0);
            assert_eq!(result.metrics.pipeline_producers > 0, source_domains);
            assert_eq!(result.metrics.pipeline_source_routes, 0);
            assert_eq!(result.metrics.pipeline_runtime_submitted[1], 0);
            assert_eq!(
                result.metrics.clustered_row_groups,
                if source_domains { 16 } else { 0 }
            );
            if source_domains {
                assert!(result.metrics.source_domain_proof_seconds > 0.);
            }
            assert_eq!(result.metrics.pipeline_fallbacks, 0);
            assert_eq!(
                result.metrics.pipeline_runtime_submitted,
                result.metrics.pipeline_runtime_completed
            );
            assert!(result.metrics.worker_threads_peak <= workers);
        }
    }
}
