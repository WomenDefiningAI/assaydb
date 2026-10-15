//! Residual-free reverse existence: lane-local head bits, then an ordered output barrier.
use super::{Context, Node, RelOp, lane_fusion, probe};
use crate::{
    Result,
    expr::{Expr, Kind},
    join_index::{ProbeCounts, owned::OwnedJoinIndex},
    memory::{Budget, Reservation},
    parquet_scan::{BatchTransform, TransformedBatch},
    spill::Cancellation,
    types::Type,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use std::{
    cell::Cell,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

static NEXT_POOL: AtomicUsize = AtomicUsize::new(1);
mod numeric;
use numeric::NumericIndex;

pub(super) struct Index(Storage);
enum Storage {
    Heads(OwnedJoinIndex),
    Numeric(NumericIndex),
}
impl From<OwnedJoinIndex> for Index {
    fn from(index: OwnedJoinIndex) -> Self {
        Self(Storage::Heads(index))
    }
}

thread_local! {
    static OWNER: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

struct Lane {
    heads: Vec<u64>,
    counts: ProbeCounts,
    batches: usize,
    seconds: f64,
}

struct Pool {
    id: usize,
    next: AtomicUsize,
    lanes: Vec<Mutex<Lane>>,
    index: Index,
    keys: Vec<(Expr, Expr)>,
    // Drop buffers before their backing reservation, including on cancellation.
    _funding: Reservation,
}

impl Pool {
    /// One slot per actual scan thread, including caller assistance; no shared bit writes.
    fn lane(&self) -> Result<std::sync::MutexGuard<'_, Lane>> {
        let slot = OWNER.with(|cached| {
            let (id, slot) = cached.get();
            if id == self.id {
                return slot;
            }
            let slot = self.next.fetch_add(1, Ordering::Relaxed);
            cached.set((self.id, slot));
            slot
        });
        self.lanes
            .get(slot)
            .ok_or("internal: reverse existence lane unavailable")?
            .try_lock()
            .map_err(|_| "internal: reverse existence lane busy".into())
    }

    /// Equality membership is idempotent; mark only the immutable duplicate-chain head.
    fn probe(&self, batch: &RecordBatch, budget: &Budget, cancel: &Cancellation) -> Result<()> {
        cancel.check()?;
        let start = Instant::now();
        let keys = probe::ProbeKeys::new(batch, &self.keys, budget, true)?;
        let mut lane = self.lane()?;
        match &self.index.0 {
            Storage::Numeric(index) => {
                index.probe(&keys, batch.num_rows(), &mut lane.heads, cancel)?;
            }
            Storage::Heads(index) => {
                let mut scratch = [0; 34];
                for row in 0..batch.num_rows() {
                    if row.is_multiple_of(1024) {
                        cancel.check()?;
                    }
                    if let Some(key) = keys.key(row, &mut scratch)
                        && let Some(head) = index.probe(key, &mut lane.counts)
                    {
                        let head = head as usize;
                        lane.heads[head / 64] |= 1 << (head % 64);
                    }
                }
            }
        }
        lane.batches += 1;
        lane.seconds += start.elapsed().as_secs_f64();
        Ok(())
    }

    /// Called only after source success and reader join; OR first, then expand each chain once.
    fn finish(
        &mut self,
        matched: &mut [bool],
        cancel: &Cancellation,
    ) -> Result<(usize, f64, ProbeCounts)> {
        cancel.check()?;
        let (first, rest) = self.lanes.split_first_mut().unwrap();
        let first = first.get_mut().unwrap();
        for lane in rest {
            let lane = lane.get_mut().unwrap();
            for (word, (all, local)) in first.heads.iter_mut().zip(&lane.heads).enumerate() {
                if word.is_multiple_of(1024) {
                    cancel.check()?;
                }
                *all |= *local;
            }
            first.counts.probes += lane.counts.probes;
            first.counts.rejected += lane.counts.rejected;
            first.batches += lane.batches;
            first.seconds += lane.seconds;
        }
        match &self.index.0 {
            Storage::Numeric(index) => index.finish(&first.heads, matched, cancel)?,
            Storage::Heads(index) => {
                for (word, &bits) in first.heads.iter().enumerate() {
                    cancel.check()?;
                    let mut bits = bits;
                    while bits != 0 {
                        let mut row = (word * 64 + bits.trailing_zeros() as usize) as u32;
                        bits &= bits - 1;
                        while row != u32::MAX {
                            if row.is_multiple_of(1024) {
                                cancel.check()?;
                            }
                            matched[row as usize] = true;
                            row = index.links()[row as usize];
                        }
                    }
                }
            }
        }
        Ok((
            first.batches,
            first.seconds,
            std::mem::take(&mut first.counts),
        ))
    }
}

pub(super) struct Prepared {
    lanes: Vec<Mutex<Lane>>,
    keys: Vec<(Expr, Expr)>,
    funding: Reservation,
}

pub(super) struct Report {
    pub batches: usize,
    pub probe_seconds: f64,
    pub merge_seconds: f64,
    pub counts: ProbeCounts,
}

/// A single scan has at most `workers` thread identities; nested joins/materializations decline.
fn single_scan(node: &Node, ctx: &Context<'_>) -> bool {
    let id = node as *const Node as usize;
    if ctx.lane_transforms.contains_key(&id) || ctx.replay_transforms.contains_key(&id) {
        return false;
    }
    match &node.op {
        RelOp::Scan { .. } => true,
        RelOp::Rename { input }
        | RelOp::Project { input, .. }
        | RelOp::Filter { input, .. }
        | RelOp::Unary { input, .. } => single_scan(input, ctx),
        _ => false,
    }
}

impl Prepared {
    /// Both indexes require allocation-free ProbeKeys before consuming the source.
    fn admitted(
        right: &Node,
        keys: &[(Expr, Expr)],
        rows: usize,
        ctx: &Context<'_>,
    ) -> Result<bool> {
        Ok(ctx.options.workers >= 2
            && rows != 0
            && single_scan(right, ctx)
            && lane_fusion::admitted(right, ctx)?
            && matches!(keys.len(), 1 | 2)
            && keys.iter().all(|(a, b)| {
                let Kind::Column(i) = b.kind else {
                    return false;
                };
                if !matches!(a.kind, Kind::Column(_)) || a.ty != b.ty {
                    return false;
                }
                let Some(field) = right.schema.fields().get(i) else {
                    return false;
                };
                // Mirror Numbers::column's exact physical layouts. Every other
                // layout would allocate batch-sized Keys in each concurrent lane,
                // after it is too late to restore the serial probe on denial.
                match (&b.ty, field.data_type()) {
                    (Type::Int, DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, 0)) => {
                        true
                    }
                    (Type::Decimal(scale), DataType::Decimal128(_, physical)) => scale == physical,
                    _ => false,
                }
            }))
    }

    /// Reserve every bitmap, owner and direct-key clone before replacing the serial index.
    pub(super) fn new(
        right: &Node,
        keys: &[(Expr, Expr)],
        rows: usize,
        ctx: &Context<'_>,
    ) -> Result<Option<(Self, Reservation)>> {
        if !Self::admitted(right, keys, rows, ctx)? {
            return Ok(None);
        }
        Self::allocate(right, keys, rows, rows, ctx)
    }

    /// A funded ranked bitmap replaces both hash builds for bounded single numeric keys.
    pub(super) fn numeric(
        right: &Node,
        keys: &[(Expr, Expr)],
        encoded: &Arc<crate::keys::Keys>,
        ctx: &Context<'_>,
    ) -> Result<Option<(Self, Index, Reservation)>> {
        if keys.len() != 1 || !Self::admitted(right, keys, encoded.len(), ctx)? {
            return Ok(None);
        }
        let Some(index) = NumericIndex::new(encoded, &ctx.budget, &ctx.options.cancellation)?
        else {
            return Ok(None);
        };
        let Some((prepared, credit)) =
            Self::allocate(right, keys, encoded.len(), index.slots(), ctx)?
        else {
            return Ok(None);
        };
        Ok(Some((prepared, Index(Storage::Numeric(index)), credit)))
    }

    /// Dense distinct ordinals bound private bits; retained occurrences get separate funded flags.
    fn allocate(
        right: &Node,
        keys: &[(Expr, Expr)],
        rows: usize,
        slots: usize,
        ctx: &Context<'_>,
    ) -> Result<Option<(Self, Reservation)>> {
        let workers = ctx.options.workers;
        let words = slots.div_ceil(64);
        let bytes = words
            .checked_mul(8)
            .and_then(|n| n.checked_add(std::mem::size_of::<Mutex<Lane>>()))
            .and_then(|n| n.checked_add(right.schema.fields().len().checked_mul(256)?))
            .and_then(|n| n.checked_mul(workers))
            .and_then(|n| n.checked_add(keys.len().checked_mul(256)?))
            .and_then(|n| n.checked_add(rows))
            .and_then(|n| n.checked_add(4096));
        let Some(mut funding) = bytes.and_then(|bytes| ctx.budget.reserve(bytes).ok()) else {
            return Ok(None);
        };
        let matched_credit = funding.split_off(rows)?;
        let lanes = (0..workers)
            .map(|_| {
                Mutex::new(Lane {
                    heads: vec![0; words],
                    counts: ProbeCounts::default(),
                    batches: 0,
                    seconds: 0.,
                })
            })
            .collect();
        // ProbeKeys expects the probe expression in the first position.
        let keys = keys.iter().map(|(a, b)| (b.clone(), a.clone())).collect();
        Ok(Some((
            Self {
                lanes,
                keys,
                funding,
            },
            matched_credit,
        )))
    }

    /// Drain all ordered source completions before publishing matches; existing scan lanes own permits.
    pub(super) fn run(
        self,
        index: impl Into<Index>,
        right: &Node,
        ctx: &mut Context<'_>,
        matched: &mut [bool],
    ) -> Result<Report> {
        let id = NEXT_POOL
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| "reverse existence pool ID exhausted")?;
        let Self {
            lanes,
            keys,
            funding,
        } = self;
        let pool = Arc::new(Pool {
            id,
            next: AtomicUsize::new(0),
            lanes,
            index: index.into(),
            keys,
            _funding: funding,
        });
        let cancel = ctx.options.cancellation.clone();
        let transform: BatchTransform = {
            let pool = pool.clone();
            let budget = ctx.budget.clone();
            let cancel = cancel.clone();
            let schema = Arc::new(Schema::empty());
            Arc::new(move |batch| {
                cancel.check()?;
                let decoded = budget.reserve(batch.get_array_memory_size())?;
                let result = pool.probe(&batch, &budget, &cancel);
                drop(batch);
                drop(decoded);
                result?;
                // No payload crosses the ordered scan channel, just source completion.
                let lease = budget.reserve(256)?;
                Ok(TransformedBatch {
                    batch: RecordBatch::new_empty(schema.clone()),
                    lease,
                    preserve_empty: true,
                })
            })
        };
        let (suffix, _) = lane_fusion::Suffix::prepend(transform, None);
        lane_fusion::run(right, ctx, suffix, &mut |_| Ok(()))?;
        let mut pool =
            Arc::try_unwrap(pool).map_err(|_| "internal: reverse existence scan still active")?;
        let start = Instant::now();
        let (batches, probe_seconds, counts) = pool.finish(matched, &cancel)?;
        Ok(Report {
            batches,
            probe_seconds,
            counts,
            merge_seconds: start.elapsed().as_secs_f64(),
        })
    }
}

#[cfg(test)]
mod tests;
