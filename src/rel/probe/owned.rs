//! Owned build/probe composition using the existing join kernel.
//! Production lanes admit only unique builds. Test-only duplicate chains and
//! their borrowed comparison oracle remain newest-first; these tests certify
//! ownership, accounting and kernel equivalence, not LEFT build-insertion order.
use super::*;
use crate::{
    expr::Expr, join_index::owned::OwnedJoinIndex, memory::Reservation, spill::Cancellation,
};
#[cfg(test)]
use std::sync::Arc;

impl Index for &OwnedJoinIndex {
    fn head(&self, key: &[u8], counts: &mut ProbeCounts) -> Option<u32> {
        OwnedJoinIndex::probe(self, key, counts)
    }
}
/// Build payload and keys remain immutable and charged until the last task drops.
pub(in crate::rel) struct OwnedBuild {
    index: OwnedJoinIndex,
    budget: Budget,
    payload: crate::rel::funded_concat::FundedBatch,
}
#[cfg(test)]
pub(in crate::rel) struct OutputBatch {
    pub batch: RecordBatch,
    // Transferred gather credits; the kernel reserves before Arrow allocation.
    pub(in crate::rel) _credits: Vec<Reservation>,
}
#[cfg(test)]
pub(in crate::rel) struct ProbeBatch {
    batch: RecordBatch,
    budget: Budget,
    _payload: Reservation,
    _envelope: Option<Reservation>,
}
#[cfg(test)]
impl ProbeBatch {
    pub fn adopt(batch: RecordBatch, budget: &Budget) -> Result<Self> {
        let credit = budget.reserve(batch.get_array_memory_size().saturating_add(256))?;
        Ok(Self {
            batch,
            budget: budget.clone(),
            _payload: credit,
            _envelope: None,
        })
    }
    /// Transfer a decoded batch's existing payload charge into the probe task.
    /// The caller retains source-position tokens before moving the batch here.
    #[cfg(test)]
    pub fn from_scan(
        scan: crate::parquet_scan::cursor::ScanBatch,
        budget: &Budget,
    ) -> Result<Self> {
        if !scan.lease.belongs_to(budget) {
            return Err("probe scan funding domain mismatch".into());
        }
        if scan.lease.bytes() < scan.batch.get_array_memory_size() {
            return Err("probe scan payload is underfunded".into());
        }
        let envelope = budget.reserve(256)?;
        Ok(Self {
            batch: scan.batch,
            budget: budget.clone(),
            _payload: scan.lease,
            _envelope: Some(envelope),
        })
    }
}
impl OwnedBuild {
    /// Admission proves a unique equality build; fund scratch and input through one ordered gather.
    pub fn probe_unique(
        &self,
        batch: RecordBatch,
        keys: &[(Expr, Expr)],
        kind: JoinKind,
        output_schema: SchemaRef,
        cancel: &Cancellation,
    ) -> Result<(RecordBatch, Reservation, Statistics)> {
        cancel.check()?;
        if batch.num_rows() > u32::MAX as usize {
            return Err("invalid owned probe row/batch limit".into());
        }
        let workspace = batch
            .num_rows()
            .checked_mul(32)
            .and_then(|n| n.checked_add(output_schema.fields().len().checked_mul(256)?))
            .and_then(|n| n.checked_add(keys.len().checked_mul(128)?))
            .and_then(|n| n.checked_add(batch.get_array_memory_size()))
            .and_then(|n| n.checked_add(1280))
            .ok_or("join candidate size overflow")?;
        let _workspace = self.budget.reserve(workspace)?;
        Kernel {
            map: &self.index,
            build: self.payload.batch(),
            next: self.index.links(),
            keys,
            kind,
            residual: None,
            residual_program: None,
            combined_schema: output_schema.clone(),
            output_schema,
            right_schema: self.payload.batch().schema(),
            budget: &self.budget,
            compact_text: true,
            batch_size: batch.num_rows().max(1),
            reserve_gathers: true,
        }
        .unique(batch, &|| cancel.check())
    }
    /// Adopt the same fully built keys and prepaid payload used by the serial join.
    pub fn from_index(
        payload: crate::rel::funded_concat::FundedBatch,
        index: OwnedJoinIndex,
        budget: &Budget,
    ) -> Self {
        Self {
            index,
            budget: budget.clone(),
            payload,
        }
    }
    #[cfg(test)]
    pub fn new(
        batch: RecordBatch,
        keys: &[Expr],
        budget: &Budget,
        compact: bool,
        cancel: &Cancellation,
    ) -> Result<Self> {
        let funded = crate::rel::funded_concat::concat(&batch.schema(), &[batch], budget, cancel)?;
        Self::from_funded(funded, keys, budget, compact, cancel)
    }
    #[cfg(test)]
    pub fn from_funded(
        payload: crate::rel::funded_concat::FundedBatch,
        keys: &[Expr],
        budget: &Budget,
        compact: bool,
        cancel: &Cancellation,
    ) -> Result<Self> {
        cancel.check()?;
        payload.validate(budget)?;
        let batch = payload.batch();
        if batch.num_rows() > u32::MAX as usize {
            return Err("join build row count exceeds u32 row IDs".into());
        }
        // Only evaluation descriptors are newly charged here. The concat
        // payload's original prepaid reservation moves intact into this owner.
        let descriptors = batch
            .num_columns()
            .checked_mul(256)
            .and_then(|n| n.checked_add(keys.len().checked_mul(128)?))
            .and_then(|n| n.checked_add(1024))
            .ok_or("join build expression descriptors overflow")?;
        let _descriptors = budget.reserve(descriptors)?;
        let input = input(batch, budget, keys)?;
        let values = keys
            .iter()
            .map(|e| e.eval(&input, budget))
            .collect::<Result<Vec<_>>>()?;
        let encoded = Arc::new(crate::keys::Keys::with_text_encoding(
            &values,
            batch.num_rows(),
            budget,
            compact,
        )?);
        let index = OwnedJoinIndex::new(encoded, batch.num_rows(), budget, true, cancel)?;
        Ok(Self {
            index,
            budget: budget.clone(),
            payload,
        })
    }
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn probe(
        &self,
        probe: ProbeBatch,
        keys: &[(Expr, Expr)],
        kind: JoinKind,
        residual: Option<&Expr>,
        combined_schema: SchemaRef,
        output_schema: SchemaRef,
        compact: bool,
        batch_size: usize,
        cancel: &Cancellation,
        emit: &mut dyn FnMut(OutputBatch) -> Result<()>,
    ) -> Result<Statistics> {
        if !self.budget.same_domain(&probe.budget) {
            return Err("probe/build funding domain mismatch".into());
        }
        if batch_size == 0 || probe.batch.num_rows() > u32::MAX as usize {
            return Err("invalid owned probe row/batch limit".into());
        }
        cancel.check()?;
        let workspace = batch_size
            .checked_mul(32)
            .and_then(|n| n.checked_add(probe.batch.num_rows().saturating_mul(16)))
            .and_then(|n| n.checked_add(combined_schema.fields().len().checked_mul(256)?))
            .and_then(|n| n.checked_add(keys.len().checked_mul(128)?))
            .and_then(|n| n.checked_add(1024))
            .ok_or("join candidate size overflow")?;
        let _workspace = self.budget.reserve(workspace)?;
        // Build this borrowed residual view inside the task, never store a
        // self-reference to an owned Expr. All counters stay task-local.
        let program = residual
            .map(|e| crate::join_filter::JoinFilter::new(e, &combined_schema))
            .transpose()?;
        let kernel = Kernel {
            map: &self.index,
            build: self.payload.batch(),
            next: self.index.links(),
            keys,
            kind,
            residual,
            residual_program: program.as_ref(),
            combined_schema,
            output_schema,
            right_schema: self.payload.batch().schema(),
            budget: &self.budget,
            compact_text: compact,
            batch_size,
            reserve_gathers: true,
        };
        kernel.run_with_credits(
            probe.batch,
            &mut |batch, credits| {
                emit(OutputBatch {
                    batch,
                    _credits: credits,
                })
            },
            &|| cancel.check(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::{Kind, Literal, Op},
        join_index::JoinIndex,
        pipeline_runtime::{JobKind, Runtime},
        types::Type,
    };
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    fn column(index: usize) -> Expr {
        Expr {
            ty: Type::Int,
            kind: Kind::Column(index),
        }
    }
    fn fixture(build: bool) -> RecordBatch {
        let prefix = if build { "b" } else { "p" };
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}k"), DataType::Int64, true),
            Field::new(format!("{prefix}v"), DataType::Int64, true),
        ]));
        let (keys, values) = if build {
            (
                vec![Some(1), Some(1), None, Some(2), Some(1)],
                vec![Some(10), Some(20), Some(99), Some(30), Some(40)],
            )
        } else {
            (
                vec![Some(1), None, Some(3), Some(2), Some(1)],
                vec![Some(25), Some(99), Some(1), Some(35), None],
            )
        };
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }
    fn combined(probe: &RecordBatch, build: &RecordBatch) -> SchemaRef {
        Arc::new(Schema::new(
            probe
                .schema()
                .fields()
                .iter()
                .chain(build.schema().fields())
                .cloned()
                .collect::<Vec<_>>(),
        ))
    }
    fn residual() -> Expr {
        Expr {
            ty: Type::Bool,
            kind: Kind::Binary(Op::Gt, Box::new(column(1)), Box::new(column(3))),
        }
    }
    fn key_pairs() -> Vec<(Expr, Expr)> {
        vec![(column(0), column(0))]
    }

    fn review_sparse_wide_fixture() -> (RecordBatch, RecordBatch) {
        use arrow::array::StringArray;
        let probe = RecordBatch::try_new(
            fixture(false).schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mut fields = fixture(true).schema().fields().to_vec();
        fields.push(Arc::new(Field::new("payload", DataType::Utf8, false)));
        let build = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(vec![1; 49])),
                Arc::new(Int64Array::from_iter_values(
                    (0..49).map(|i| if i % 7 == 0 { 0 } else { 2 }),
                )),
                Arc::new(StringArray::from_iter_values((0..49).map(|i| {
                    if i % 7 == 0 {
                        char::from(b'A' + (i / 7) as u8).to_string().repeat(1 << 20)
                    } else {
                        String::new()
                    }
                }))),
            ],
        )
        .unwrap();
        (probe, build)
    }

    #[test]
    fn left_sparse_wide_review_counterexample_keeps_unfused_success() {
        use arrow::array::StringArray;
        // The INNER kernel retains c0e3dd6da's candidate-chunk behavior. Here
        // every probe matches, so it is also a resource-admission control for
        // that unfused LEFT path (the owned harness uses newest-first links).
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let budget = Budget::new(64 << 20);
            let (probe, build) = review_sparse_wide_fixture();
            let schema = combined(&probe, &build);
            let cancel = Cancellation::default();
            let owned = OwnedBuild::new(build, &[column(0)], &budget, true, &cancel).unwrap();
            let blocker = budget
                .reserve(budget.limit() - budget.used() - (2 << 20))
                .unwrap();
            let before = budget.used();
            let mut ids = Vec::new();
            let stats = owned
                .probe(
                    ProbeBatch::adopt(probe, &budget).unwrap(),
                    &key_pairs(),
                    kind,
                    Some(&residual()),
                    schema.clone(),
                    schema,
                    true,
                    7,
                    &cancel,
                    &mut |output| {
                        assert_eq!(output.batch.num_rows(), 1);
                        assert!(
                            output
                                ._credits
                                .iter()
                                .map(Reservation::bytes)
                                .sum::<usize>()
                                >= output.batch.get_array_memory_size()
                        );
                        let text = output
                            .batch
                            .column(4)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        assert_eq!(text.value(0).len(), 1 << 20);
                        ids.push(text.value(0).as_bytes()[0]);
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(ids.as_slice(), b"GFEDCBA");
            assert_eq!(stats.output_rows, 7);
            assert_eq!(stats.residual_rows, 49);
            assert_eq!(budget.used(), before);
            assert!(budget.peak() <= budget.limit());
            drop(blocker);
            drop(owned);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn left_wide_row_spans_three_funded_output_chunks() {
        use arrow::array::StringArray;
        let budget = Budget::new(64 << 20);
        let (probe, build) = review_sparse_wide_fixture();
        let schema = combined(&probe, &build);
        let cancel = Cancellation::default();
        let owned = OwnedBuild::new(build, &[column(0)], &budget, true, &cancel).unwrap();
        let blocker = budget
            .reserve(budget.limit() - budget.used() - (16 << 20))
            .unwrap();
        let before = budget.used();
        let mut chunks = Vec::new();
        let mut ids = Vec::new();
        let stats = owned
            .probe(
                ProbeBatch::adopt(probe, &budget).unwrap(),
                &key_pairs(),
                JoinKind::Left,
                Some(&residual()),
                schema.clone(),
                schema,
                true,
                7,
                &cancel,
                &mut |output| {
                    chunks.push(output.batch.num_rows());
                    let credit = output
                        ._credits
                        .iter()
                        .map(Reservation::bytes)
                        .sum::<usize>();
                    assert!(credit >= output.batch.get_array_memory_size());
                    assert!(credit < 4 << 20);
                    let text = output
                        .batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for row in 0..output.batch.num_rows() {
                        assert_eq!(text.value(row).len(), 1 << 20);
                        ids.push(text.value(row).as_bytes()[0]);
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(chunks, [3, 3, 1]);
        assert_eq!(ids.as_slice(), b"GFEDCBA");
        assert_eq!(stats.output_rows, 7);
        assert_eq!(stats.residual_rows, 49);
        assert_eq!(budget.used(), before);
        drop(blocker);
        drop(owned);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn left_validation_and_suffix_cancellation_release_bounded_ids() {
        use std::cell::Cell;
        for phase in 0..3 {
            let budget = Budget::new(8 << 20);
            let rows = 16384;
            let build = RecordBatch::try_new(
                fixture(true).schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1; rows])),
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                ],
            )
            .unwrap();
            let probe = RecordBatch::try_new(
                fixture(false).schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![rows as i64 + 1])),
                ],
            )
            .unwrap();
            let schema = combined(&probe, &build);
            let cancel = Cancellation::default();
            let owned = OwnedBuild::new(build, &[column(0)], &budget, true, &cancel).unwrap();
            let probe_lease = budget.reserve(probe.get_array_memory_size()).unwrap();
            let blocker = budget
                .reserve(budget.limit() - budget.used() - (32 << 10))
                .unwrap();
            let before = budget.used();
            let keys = key_pairs();
            let residual = residual();
            let kernel = Kernel {
                map: &owned.index,
                build: owned.payload.batch(),
                next: owned.index.links(),
                keys: &keys,
                kind: JoinKind::Left,
                residual: Some(&residual),
                // Exercise the unoptimized residual-column selection too.
                residual_program: None,
                combined_schema: schema.clone(),
                output_schema: schema,
                right_schema: owned.payload.batch().schema(),
                budget: &budget,
                compact_text: true,
                batch_size: 7,
                reserve_gathers: true,
            };
            let checks = Cell::new(0);
            let mut outputs = 0;
            let error = kernel
                .run_with_credits(
                    probe,
                    &mut |batch, credits| {
                        outputs += 1;
                        assert_eq!(batch.num_rows(), 7);
                        assert!(
                            credits.iter().map(Reservation::bytes).sum::<usize>()
                                >= batch.get_array_memory_size()
                        );
                        if outputs == 2 {
                            if phase == 2 {
                                return Err("downstream deliberate failure".into());
                            }
                            cancel.cancel();
                        }
                        Ok(())
                    },
                    &|| {
                        checks.set(checks.get() + 1);
                        if phase == 0 && checks.get() == 6 {
                            cancel.cancel();
                        }
                        cancel.check()
                    },
                )
                .err()
                .unwrap();
            assert_eq!(outputs, if phase == 0 { 0 } else { 2 });
            if phase == 2 {
                assert_eq!(error, "downstream deliberate failure");
            } else {
                assert!(error.starts_with("cancelled:"), "{error}");
            }
            assert_eq!(budget.used(), before);
            drop(blocker);
            drop(probe_lease);
            drop(owned);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn scan_payload_moves_into_probe_without_a_second_payload_charge() {
        use crate::parquet_scan::{FilterFactory, cursor::ScanCursor};
        use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ArrowReaderMetadata};
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "assaydb-probe-scan-transfer-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let source = fixture(false);
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create_new(&path).unwrap(),
            source.schema(),
            None,
        )
        .unwrap();
        writer.write(&source).unwrap();
        writer.close().unwrap();
        for (cache, transformed) in [
            (None, false),
            (None, true),
            (Some(4096), false),
            (Some(4096), true),
        ] {
            let budget = Budget::new(1 << 20);
            let file = std::fs::File::open(&path).unwrap();
            let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
            let projection = ProjectionMask::all();
            let factory: FilterFactory = Arc::new(|_| None);
            let options = crate::execute::Options {
                batch_size: 8192,
                parquet_cache_bytes: cache,
                ..Default::default()
            };
            let transform = transformed.then(|| {
                crate::parquet_scan::exact_filter(
                    Expr {
                        ty: Type::Bool,
                        kind: Kind::Literal(Literal::Bool(true)),
                    },
                    &source.schema(),
                    &budget,
                    &options.cancellation,
                    true,
                    Arc::new(std::sync::Mutex::new(Default::default())),
                )
                .unwrap()
            });
            let mut cursor = ScanCursor::new(
                file, metadata, projection, &factory, transform, &options, &budget, 0,
            )
            .unwrap();
            let scan = cursor.next().unwrap().unwrap();
            assert_eq!(scan.batch, source);
            assert_eq!((scan.row_group, scan.batch_in_group), (0, 0));
            let payload_bytes = scan.lease.bytes();
            assert!(payload_bytes > 0);
            let payload = Arc::downgrade(scan.batch.column(0));
            let blocker = budget
                .reserve(budget.limit() - budget.used() - 256)
                .unwrap();
            let probe = ProbeBatch::from_scan(scan, &budget).unwrap();
            assert_eq!(budget.used(), budget.limit());
            assert_eq!(probe._payload.bytes(), payload_bytes);
            assert!(payload.upgrade().is_some());
            drop(probe);
            assert!(payload.upgrade().is_none());
            drop(blocker);
            drop(cursor);
            assert_eq!(budget.used(), 0);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn scan_transfer_rejects_foreign_underfunded_and_unfunded_envelopes() {
        use crate::parquet_scan::cursor::ScanBatch;
        for mode in 0..3 {
            let budget = Budget::new(1 << 20);
            let foreign = Budget::new(1 << 20);
            let batch = fixture(false);
            let payload = Arc::downgrade(batch.column(0));
            let bytes = batch.get_array_memory_size();
            let lease = match mode {
                0 => foreign.reserve(bytes).unwrap(),
                1 => budget.reserve(bytes - 1).unwrap(),
                _ => budget.reserve(bytes).unwrap(),
            };
            let blocker =
                (mode == 2).then(|| budget.reserve(budget.limit() - budget.used()).unwrap());
            let result = ProbeBatch::from_scan(
                ScanBatch {
                    row_group: 0,
                    batch_in_group: 0,
                    batch,
                    lease,
                },
                &budget,
            );
            let error = result.err().expect("invalid funding must fail");
            assert!(error.starts_with(match mode {
                0 => "probe scan funding domain mismatch",
                1 => "probe scan payload is underfunded",
                _ => "resource-limit: accounted memory",
            }));
            assert!(payload.upgrade().is_none());
            drop(blocker);
            assert_eq!(budget.used(), 0);
            assert_eq!(foreign.used(), 0);
        }
    }
    fn compare(kind: JoinKind, residual: Option<&Expr>, size: usize) {
        compare_outcome(kind, residual, size, false);
    }
    fn compare_outcome(kind: JoinKind, residual: Option<&Expr>, size: usize, fail: bool) {
        let budget = Budget::new(16 << 20);
        let build = fixture(true);
        let probe = fixture(false);
        let keys = key_pairs();
        let combined = combined(&probe, &build);
        let output = if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
            probe.schema()
        } else {
            combined.clone()
        };
        let right_input = input(&build, &budget, [&keys[0].1]).unwrap();
        let values = vec![keys[0].1.eval(&right_input, &budget).unwrap()];
        let encoded = crate::keys::Keys::new(&values, build.num_rows(), &budget).unwrap();
        let mut map = JoinIndex::new(&encoded, build.num_rows(), &budget, true).unwrap();
        let mut next = vec![u32::MAX; build.num_rows()];
        // Deliberately matches the owned index's legacy chain orientation.
        // This self-comparison is not a rule-O order oracle (see module docs).
        map.populate(&encoded, &mut next);
        let program = residual.map(|e| crate::join_filter::JoinFilter::new(e, &combined).unwrap());
        let kernel = Kernel {
            map: map.view(),
            build: &build,
            next: &next,
            keys: &keys,
            kind,
            residual,
            residual_program: program.as_ref(),
            combined_schema: combined.clone(),
            output_schema: output.clone(),
            right_schema: build.schema(),
            budget: &budget,
            compact_text: true,
            batch_size: size,
            reserve_gathers: true,
        };
        let mut expected = Vec::new();
        let expected_stats = kernel.run(
            probe.clone(),
            &mut |b| {
                expected.push(b);
                if fail {
                    Err("downstream deliberate failure".into())
                } else {
                    Ok(())
                }
            },
            &|| Ok(()),
        );
        let owned = OwnedBuild::new(
            build.clone(),
            &[column(0)],
            &budget,
            true,
            &Cancellation::default(),
        )
        .unwrap();
        let mut actual = Vec::new();
        let stats = owned.probe(
            ProbeBatch::adopt(probe, &budget).unwrap(),
            &keys,
            kind,
            residual,
            combined,
            output,
            true,
            size,
            &Cancellation::default(),
            &mut |b| {
                actual.push(b);
                if fail {
                    Err("downstream deliberate failure".into())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(stats.as_ref().err(), expected_stats.as_ref().err());
        assert_eq!(
            actual.iter().map(|b| &b.batch).collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        if let (Ok(stats), Ok(expected_stats)) = (stats, expected_stats) {
            assert_eq!(stats.output_rows, expected_stats.output_rows);
            assert_eq!(stats.residual_rows, expected_stats.residual_rows);
            assert_eq!(stats.residual_bytes, expected_stats.residual_bytes);
            assert_eq!(
                (stats.probes, stats.rejected),
                (expected_stats.probes, expected_stats.rejected)
            );
        }
        drop(owned);
        drop(map);
        drop(encoded);
        drop(values);
        drop(right_input);
        drop(expected);
        drop(build);
        assert_eq!(budget.used() > 0, !actual.is_empty());
        drop(actual);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn owned_probe_reuses_inner_left_semi_anti_null_duplicate_residual_order() {
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Semi,
            JoinKind::Anti,
        ] {
            for size in [1, 2, 7] {
                compare(kind, None, size);
                compare(kind, Some(&residual()), size);
                compare_outcome(kind, None, size, true);
                compare_outcome(kind, Some(&residual()), size, true);
            }
        }
    }
    #[test]
    fn owned_probe_error_prefix_and_output_credit_survive_producer_failure() {
        let budget = Budget::new(8 << 20);
        let build = fixture(true);
        let probe = fixture(false);
        let combined = combined(&probe, &build);
        let keys = key_pairs();
        let owned = OwnedBuild::new(
            build.clone(),
            &[column(0)],
            &budget,
            true,
            &Cancellation::default(),
        )
        .unwrap();
        let mut outputs = Vec::new();
        let error = owned
            .probe(
                ProbeBatch::adopt(probe.clone(), &budget).unwrap(),
                &keys,
                JoinKind::Inner,
                None,
                combined.clone(),
                combined.clone(),
                true,
                1,
                &Cancellation::default(),
                &mut |b| {
                    outputs.push(b);
                    Err("downstream deliberate failure".into())
                },
            )
            .err()
            .unwrap();
        assert_eq!(error, "downstream deliberate failure");
        assert_eq!(outputs.len(), 1);
        // DECIMAL zero division retains the failure used by this ownership test.
        let zero = Expr {
            ty: Type::Decimal(0),
            kind: Kind::Literal(Literal::Num(0)),
        };
        let invalid = Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                Op::Gt,
                Box::new(Expr {
                    ty: Type::Float,
                    kind: Kind::Binary(Op::Div, Box::new(column(1)), Box::new(zero)),
                }),
                Box::new(column(3)),
            ),
        };
        compare_outcome(JoinKind::Inner, Some(&invalid), 2, false);
        assert!(
            owned
                .probe(
                    ProbeBatch::adopt(probe.clone(), &budget).unwrap(),
                    &keys,
                    JoinKind::Inner,
                    Some(&invalid),
                    combined.clone(),
                    combined.clone(),
                    true,
                    2,
                    &Cancellation::default(),
                    &mut |_| panic!("failed residual must not emit")
                )
                .is_err()
        );
        let before = budget.used();
        let canceled = Cancellation::default();
        canceled.cancel();
        assert!(
            owned
                .probe(
                    ProbeBatch::adopt(probe.clone(), &budget).unwrap(),
                    &keys,
                    JoinKind::Left,
                    None,
                    combined.clone(),
                    combined.clone(),
                    true,
                    2,
                    &canceled,
                    &mut |_| panic!("canceled probe must not emit"),
                )
                .is_err()
        );
        assert_eq!(budget.used(), before);
        let admitted = ProbeBatch::adopt(probe.clone(), &budget).unwrap();
        let remaining = budget.reserve((8 << 20) - budget.used()).unwrap();
        assert!(
            owned
                .probe(
                    admitted,
                    &keys,
                    JoinKind::Left,
                    None,
                    combined.clone(),
                    combined.clone(),
                    true,
                    2,
                    &Cancellation::default(),
                    &mut |_| panic!("unfunded probe must not emit"),
                )
                .err()
                .unwrap()
                .starts_with("resource-limit:")
        );
        drop(remaining);
        assert_eq!(budget.used(), before);
        let foreign = Budget::new(8 << 20);
        assert!(
            owned
                .probe(
                    ProbeBatch::adopt(probe, &foreign).unwrap(),
                    &keys,
                    JoinKind::Inner,
                    None,
                    combined.clone(),
                    combined,
                    true,
                    2,
                    &Cancellation::default(),
                    &mut |_| Ok(())
                )
                .is_err()
        );
        assert_eq!(foreign.used(), 0);
        drop(owned);
        drop(build);
        assert!(budget.used() > 0);
        drop(outputs);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn left_residual_skew_uses_bounded_scratch_and_releases_it_on_cancellation() {
        for cancel_after_output in [false, true] {
            let rows = 16384;
            let budget = Budget::new(8 << 20);
            let build = RecordBatch::try_new(
                fixture(true).schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1; rows])),
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                ],
            )
            .unwrap();
            let probe = RecordBatch::try_new(
                fixture(false).schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![rows as i64 + 1])),
                ],
            )
            .unwrap();
            let schema = combined(&probe, &build);
            let cancel = Cancellation::default();
            let owned = OwnedBuild::new(build, &[column(0)], &budget, true, &cancel).unwrap();
            let retained = budget.used();
            let blocker = budget
                .reserve(budget.limit() - retained - 32 * 1024)
                .unwrap();
            let before = budget.used();
            // Even one additional row ID per match cannot fit. The seven-row
            // workspace must suffice, including evaluation and output peaks.
            assert!(budget.reserve(rows * std::mem::size_of::<u32>()).is_err());
            let mut emitted = 0;
            let result = owned.probe(
                ProbeBatch::adopt(probe, &budget).unwrap(),
                &key_pairs(),
                JoinKind::Left,
                Some(&residual()),
                schema.clone(),
                schema,
                true,
                7,
                &cancel,
                &mut |output| {
                    assert!(output.batch.num_rows() <= 7);
                    assert!(
                        output
                            ._credits
                            .iter()
                            .map(Reservation::bytes)
                            .sum::<usize>()
                            >= output.batch.get_array_memory_size()
                    );
                    emitted += output.batch.num_rows();
                    if cancel_after_output {
                        cancel.cancel();
                    }
                    Ok(())
                },
            );
            if cancel_after_output {
                assert!(result.err().unwrap().starts_with("cancelled:"));
                assert_eq!(emitted, 7);
            } else {
                let stats = result.unwrap();
                assert_eq!(emitted, rows);
                assert_eq!(stats.output_rows, rows);
                // Validation plus the unbuffered suffix, with no third pass.
                assert_eq!(stats.residual_rows, rows + rows - 7);
            }
            assert_eq!(budget.used(), before);
            assert!(budget.peak() <= budget.limit());
            drop(blocker);
            assert_eq!(budget.used(), retained);
            drop(owned);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn left_order_cancellation_after_output_releases_scratch_and_keeps_payload_funded() {
        for size in [1, 7, 8192] {
            let budget = Budget::new(8 << 20);
            let build = fixture(true);
            let probe = fixture(false);
            let schema = combined(&probe, &build);
            let cancel = Cancellation::default();
            let owned = OwnedBuild::new(build, &[column(0)], &budget, true, &cancel).unwrap();
            let before = budget.used();
            let mut outputs = Vec::new();
            let error = owned
                .probe(
                    ProbeBatch::adopt(probe, &budget).unwrap(),
                    &key_pairs(),
                    JoinKind::Left,
                    Some(&residual()),
                    schema.clone(),
                    schema,
                    true,
                    size,
                    &cancel,
                    &mut |output| {
                        assert!(
                            output
                                ._credits
                                .iter()
                                .map(Reservation::bytes)
                                .sum::<usize>()
                                >= output.batch.get_array_memory_size()
                        );
                        outputs.push(output);
                        cancel.cancel();
                        Ok(())
                    },
                )
                .err()
                .unwrap();
            assert!(error.starts_with("cancelled:"), "{error}");
            assert_eq!(outputs.len(), 1);
            assert!(budget.used() > before);
            drop(outputs);
            assert_eq!(budget.used(), before);
            drop(owned);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn funded_concat_transfers_large_payload_without_a_second_charge() {
        use arrow::array::StringArray;
        let budget = Budget::new(4 << 20);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let text = "雪".repeat(300_000);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![text.as_str()])),
            ],
        )
        .unwrap();
        let source = budget.reserve(batch.get_array_memory_size()).unwrap();
        let funded = crate::rel::funded_concat::concat(
            &schema,
            std::slice::from_ref(&batch),
            &budget,
            &Cancellation::default(),
        )
        .unwrap();
        drop(batch);
        drop(source);
        let payload_bytes = funded.batch().get_array_memory_size();
        let used = budget.used();
        let weak = Arc::downgrade(funded.batch().column(1));
        let blocker = budget.reserve(budget.limit() - used - 64 * 1024).unwrap();
        assert!(budget.reserve(payload_bytes).is_err());
        let build = OwnedBuild::from_funded(
            funded,
            &[column(0)],
            &budget,
            true,
            &Cancellation::default(),
        )
        .unwrap();
        assert!(weak.upgrade().is_some());
        drop(build);
        assert!(weak.upgrade().is_none());
        assert_eq!(budget.used(), blocker.bytes());
        drop(blocker);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn funded_build_failure_retains_credit_until_payload_cleanup() {
        for failure in 0..4 {
            let budget = Budget::new(1 << 20);
            let batch = fixture(true);
            let funded = crate::rel::funded_concat::concat(
                &batch.schema(),
                &[batch],
                &budget,
                &Cancellation::default(),
            )
            .unwrap();
            let weak = Arc::downgrade(funded.batch().column(1));
            let foreign = Budget::new(1 << 20);
            let cancel = Cancellation::default();
            if failure == 0 {
                cancel.cancel();
            }
            let blocker = if failure == 1 {
                Some(budget.reserve(budget.limit() - budget.used()).unwrap())
            } else {
                None
            };
            let target = if failure == 2 { &foreign } else { &budget };
            let key = if failure == 3 {
                Expr {
                    ty: Type::Float,
                    kind: Kind::Binary(
                        Op::Div,
                        Box::new(column(0)),
                        Box::new(Expr {
                            ty: Type::Decimal(0),
                            kind: Kind::Literal(Literal::Num(0)),
                        }),
                    ),
                }
            } else {
                column(0)
            };
            assert!(OwnedBuild::from_funded(funded, &[key], target, true, &cancel).is_err());
            assert!(weak.upgrade().is_none());
            drop(blocker);
            assert_eq!(budget.used(), 0);
            assert_eq!(foreign.used(), 0);
        }
    }
    #[test]
    fn owned_probe_runtime_keeps_build_and_output_backing_until_last_owner() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<OwnedBuild>();
        send_sync::<ProbeBatch>();
        send_sync::<OutputBatch>();
        let budget = Budget::new(16 << 20);
        let cancel = Cancellation::default();
        let build = fixture(true);
        let probe = fixture(false);
        let combined = combined(&probe, &build);
        let funded =
            crate::rel::funded_concat::concat(&build.schema(), &[build], &budget, &cancel).unwrap();
        let payload = Arc::downgrade(funded.batch().column(0));
        let owned = Arc::new(
            OwnedBuild::from_funded(funded, &[column(0)], &budget, true, &cancel).unwrap(),
        );
        let weak = Arc::downgrade(&owned);
        let mut runtime = Runtime::new(&budget, 3, 2, cancel.clone()).unwrap();
        let (entered, started) = std::sync::mpsc::channel();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (release, released) = std::sync::mpsc::channel();
            releases.push(release);
            let entered = entered.clone();
            let build = owned.clone();
            let probe = ProbeBatch::adopt(probe.clone(), &budget).unwrap();
            let schema = combined.clone();
            assert!(
                runtime
                    .try_submit(JobKind::Morsel, move |cancel| {
                        entered.send(()).unwrap();
                        released
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                        let mut output = Vec::new();
                        let stats = build.probe(
                            probe,
                            &key_pairs(),
                            JoinKind::Left,
                            Some(&residual()),
                            schema.clone(),
                            schema,
                            true,
                            2,
                            cancel,
                            &mut |b| {
                                output.push(b);
                                Ok(())
                            },
                        )?;
                        Ok((output, stats))
                    })
                    .is_ok()
            );
        }
        drop(owned);
        for _ in 0..2 {
            started
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        assert!(weak.upgrade().is_some());
        assert!(payload.upgrade().is_some());
        for release in releases {
            release.send(()).unwrap();
        }
        let mut results = Vec::new();
        while let Some(done) = runtime.next_completion() {
            results.push(done.result.unwrap());
        }
        drop(runtime);
        assert_eq!(results.len(), 2);
        assert!(weak.upgrade().is_none());
        assert!(payload.upgrade().is_none());
        assert_eq!(
            results[0].0.iter().map(|b| &b.batch).collect::<Vec<_>>(),
            results[1].0.iter().map(|b| &b.batch).collect::<Vec<_>>()
        );
        assert!(budget.used() > 0);
        drop(results);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak_workers(), 2);
    }
}
