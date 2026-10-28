//! Cost estimates only. Bounds do not authorize pruning rows or expressions.
use crate::{
    bind::Output,
    expr::{Expr, Kind, Literal, Op},
    rel::{Node, RelOp},
    types::Type,
};
use parquet::file::{
    metadata::{ColumnChunkMetaData, ParquetMetaData},
    statistics::Statistics,
};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnStats {
    pub bounds: Option<(i128, i128)>,
    pub nulls: Option<u64>,
    pub rows: u64,
}

/// Parquet stores unsigned integers in signed physical columns, ordered as
/// unsigned. Reading their min/max as signed values gives a wrong logical range
/// (UInt32 4294967295 reads as -1), so integer bounds ignore unsigned columns.
pub(crate) fn signed_integer_statistics(column: &ColumnChunkMetaData) -> bool {
    use parquet::basic::{ConvertedType, LogicalType};
    let descr = column.column_descr();
    !matches!(
        descr.logical_type(),
        Some(LogicalType::Integer {
            is_signed: false,
            ..
        })
    ) && !matches!(
        descr.converted_type(),
        ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64
    )
}

pub(crate) fn column_stats(metadata: &ParquetMetaData, column: usize) -> ColumnStats {
    let mut bounds: Option<(i128, i128)> = None;
    let mut complete = true;
    let mut nulls = Some(0u64);
    for group in metadata.row_groups() {
        let stats = group.column(column).statistics();
        let n = stats.and_then(Statistics::null_count_opt);
        nulls = nulls.zip(n).and_then(|(a, b)| a.checked_add(b));
        if n == Some(group.num_rows() as u64) {
            continue;
        }
        let range = match stats.filter(|_| signed_integer_statistics(group.column(column))) {
            Some(Statistics::Int32(s)) => s
                .min_opt()
                .zip(s.max_opt())
                .map(|(a, b)| (i128::from(*a), i128::from(*b))),
            Some(Statistics::Int64(s)) => s
                .min_opt()
                .zip(s.max_opt())
                .map(|(a, b)| (i128::from(*a), i128::from(*b))),
            _ => None,
        };
        if let Some((a, b)) = range.filter(|(a, b)| a <= b) {
            bounds = Some(bounds.map_or((a, b), |(lo, hi)| (lo.min(a), hi.max(b))));
        } else {
            complete = false;
        }
    }
    ColumnStats {
        bounds: if complete { bounds } else { None },
        nulls,
        rows: metadata.file_metadata().num_rows() as u64,
    }
}

// Restrict propagation to row-preserving projections. A previously filtered,
// joined or aggregated distribution needs additional evidence, not raw marginals.
fn stats(node: &Node, column: usize) -> Option<&ColumnStats> {
    match &node.op {
        RelOp::Scan { statistics, .. } => statistics.get(column),
        RelOp::Rename { input } => stats(input, column),
        RelOp::Project { input, columns } => stats(input, *columns.get(column)?),
        RelOp::Unary { input, plan } if !plan.aggregation && plan.filter.is_none() => {
            match plan.output.get(column)? {
                Output::Expr(Expr {
                    kind: Kind::Column(i),
                    ..
                }) => stats(input, *i),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Default)]
struct Interval {
    low: Option<i128>,
    high: Option<i128>,
    empty: bool,
    null_only: bool,
    not_null: bool,
}
impl Interval {
    fn constrain(&mut self, op: Op, n: i128) {
        self.not_null = true;
        let low = match op {
            Op::Eq | Op::Ge => Some(n),
            Op::Gt => n.checked_add(1),
            _ => None,
        };
        let high = match op {
            Op::Eq | Op::Le => Some(n),
            Op::Lt => n.checked_sub(1),
            _ => None,
        };
        self.empty |= (op == Op::Gt && low.is_none()) || (op == Op::Lt && high.is_none());
        if let Some(n) = low {
            self.low = Some(self.low.map_or(n, |v| v.max(n)));
        }
        if let Some(n) = high {
            self.high = Some(self.high.map_or(n, |v| v.min(n)));
        }
    }
    fn fraction(&self, s: &ColumnStats) -> Option<f64> {
        if self.empty || (self.null_only && self.not_null) {
            return Some(0.0);
        }
        let null_fraction = s.nulls.map(|n| n as f64 / s.rows.max(1) as f64);
        if self.null_only {
            return null_fraction;
        }
        let non_null = null_fraction.map_or(1.0, |p| 1.0 - p);
        if self.low.is_none() && self.high.is_none() {
            return null_fraction.map(|_| non_null);
        }
        if non_null == 0.0 {
            return Some(0.0);
        }
        let (lo, hi) = s.bounds?;
        let width = hi.checked_sub(lo)?.checked_add(1)?;
        if width <= 0 {
            return None;
        }
        let a = self.low.map_or(lo, |v| lo.max(v));
        let b = self.high.map_or(hi, |v| hi.min(v));
        let kept = if a > b {
            0
        } else {
            b.checked_sub(a)?.checked_add(1)?
        };
        Some(non_null * kept as f64 / width as f64)
    }
}

fn comparison(expr: &Expr) -> Option<(usize, Op, i128)> {
    let Kind::Binary(op, a, b) = &expr.kind else {
        return None;
    };
    if !matches!(op, Op::Eq | Op::Lt | Op::Le | Op::Gt | Op::Ge) {
        return None;
    }
    let reversed = match op {
        Op::Lt => Op::Gt,
        Op::Le => Op::Ge,
        Op::Gt => Op::Lt,
        Op::Ge => Op::Le,
        _ => *op,
    };
    for (a, b, op) in [(a, b, *op), (b, a, reversed)] {
        let Kind::Column(i) = a.kind else {
            continue;
        };
        let value = match (&a.ty, &b.ty, &b.kind) {
            (Type::Int, Type::Int | Type::Decimal(0), Kind::Literal(Literal::Num(n))) => *n,
            (Type::Date, Type::Date, Kind::Literal(Literal::Date(n))) => i128::from(*n),
            _ => continue,
        };
        return Some((i, op, value));
    }
    None
}

fn collect(
    expr: &Expr,
    intervals: &mut BTreeMap<usize, Interval>,
    unknown: &mut bool,
    impossible: &mut bool,
) {
    match &expr.kind {
        Kind::Binary(Op::And, a, b) => {
            collect(a, intervals, unknown, impossible);
            collect(b, intervals, unknown, impossible);
        }
        Kind::Literal(Literal::Bool(true)) => {}
        Kind::Literal(Literal::Bool(false) | Literal::Null) => *impossible = true,
        Kind::IsNull(a, negated) if matches!(a.kind, Kind::Column(_)) => {
            let Kind::Column(i) = a.kind else {
                unreachable!()
            };
            let interval = intervals.entry(i).or_default();
            if *negated {
                interval.not_null = true;
            } else {
                interval.null_only = true;
            }
        }
        _ => {
            if let Some((i, op, n)) = comparison(expr) {
                intervals.entry(i).or_default().constrain(op, n);
            } else {
                *unknown = true;
            }
        }
    }
}

pub(crate) fn filtered_rows(input: &Node, predicate: &Expr) -> usize {
    let mut intervals = BTreeMap::new();
    let mut unknown = false;
    let mut impossible = false;
    collect(predicate, &mut intervals, &mut unknown, &mut impossible);
    let mut fractions = Vec::new();
    for (column, interval) in intervals {
        if let Some(p) = stats(input, column).and_then(|s| interval.fraction(s)) {
            fractions.push(p.clamp(0.0, 1.0));
        } else {
            unknown = true;
        }
    }
    // All unsupported conditions share one old quarter-row guess. Do not infer
    // independent reductions for each opaque or correlated expression.
    if unknown {
        fractions.push(0.25);
    }
    fractions.sort_by(f64::total_cmp);
    // Uniform marginal distributions are guesses. Dampen further columns rather
    // than treating them as independent; this is a costing heuristic only.
    let fraction = if impossible {
        0.0
    } else {
        fractions.first().copied().unwrap_or(1.0)
            * fractions.iter().skip(1).map(|p| p.sqrt()).product::<f64>()
    };
    // Never create an empty physical plan from an estimate, including FALSE:
    // input execution, errors and scalar-cardinality checks still have to run.
    ((input.estimated_rows as f64 * fraction).ceil() as usize).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn scan() -> Node {
        Node {
            schema: Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Int64, true),
            ])),
            estimated_rows: 120,
            op: RelOp::Scan {
                path: "unused".into(),
                columns: vec![0, 1],
                distinct: vec![100, 100],
                statistics: vec![
                    ColumnStats {
                        bounds: Some((-50, 49)),
                        nulls: Some(20),
                        rows: 120
                    };
                    2
                ],
            },
        }
    }
    fn cmp(i: usize, op: Op, n: i128) -> Expr {
        Expr {
            ty: Type::Bool,
            kind: Kind::Binary(
                op,
                Box::new(Expr {
                    ty: Type::Int,
                    kind: Kind::Column(i),
                }),
                Box::new(Expr {
                    ty: Type::Int,
                    kind: Kind::Literal(Literal::Num(n)),
                }),
            ),
        }
    }
    fn and(a: Expr, b: Expr) -> Expr {
        Expr {
            ty: Type::Bool,
            kind: Kind::Binary(Op::And, Box::new(a), Box::new(b)),
        }
    }

    #[test]
    fn uniform_interval_estimates_match_enumerated_nullable_values() {
        let input = scan();
        for lo in [-100, -50, -1, 0, 49, 50, 100] {
            for hi in [-100, -50, -1, 0, 49, 50, 100] {
                for (lop, hop) in [(Op::Ge, Op::Le), (Op::Gt, Op::Lt), (Op::Ge, Op::Lt)] {
                    let predicate = and(cmp(0, lop, lo), cmp(0, hop, hi));
                    let actual = (-50..50)
                        .filter(|v| {
                            (if lop == Op::Gt { *v > lo } else { *v >= lo })
                                && (if hop == Op::Lt { *v < hi } else { *v <= hi })
                        })
                        .count()
                        .max(1);
                    // A costing float may round upward by a row, never truncate.
                    assert!(filtered_rows(&input, &predicate).abs_diff(actual) <= 1);
                    assert_eq!(
                        filtered_rows(&input, &predicate),
                        filtered_rows(&input, &and(predicate.clone(), predicate))
                    );
                }
            }
        }
        for op in [Op::Eq, Op::Lt, Op::Le, Op::Gt, Op::Ge] {
            for n in [
                i128::MIN,
                i64::MIN as i128,
                -1,
                0,
                i64::MAX as i128,
                i128::MAX,
            ] {
                let p = cmp(0, op, n);
                assert!((1..=120).contains(&filtered_rows(&input, &p)));
                let Kind::Binary(_, a, b) = p.kind else {
                    unreachable!()
                };
                let reverse = match op {
                    Op::Lt => Op::Gt,
                    Op::Le => Op::Ge,
                    Op::Gt => Op::Lt,
                    Op::Ge => Op::Le,
                    _ => op,
                };
                let reversed = Expr {
                    ty: Type::Bool,
                    kind: Kind::Binary(reverse, b, a),
                };
                assert_eq!(
                    filtered_rows(&input, &cmp(0, op, n)),
                    filtered_rows(&input, &reversed)
                );
            }
        }
    }

    #[test]
    fn unknown_correlated_null_and_projected_costs_remain_conservative() {
        let mut input = scan();
        let p = cmp(0, Op::Lt, 0);
        let one = filtered_rows(&input, &p);
        let correlated = filtered_rows(&input, &and(p.clone(), cmp(1, Op::Lt, 0)));
        assert!(correlated < one && correlated > 20); // Not full independence.
        let null = Expr {
            ty: Type::Bool,
            kind: Kind::IsNull(
                Box::new(Expr {
                    ty: Type::Int,
                    kind: Kind::Column(0),
                }),
                false,
            ),
        };
        assert_eq!(filtered_rows(&input, &null), 20);
        assert_eq!(filtered_rows(&input, &and(null.clone(), p.clone())), 1);
        if let RelOp::Scan { statistics, .. } = &mut input.op {
            statistics[0].bounds = None;
            statistics[0].nulls = None;
        }
        assert_eq!(filtered_rows(&input, &p), 30);
        assert_eq!(filtered_rows(&input, &null), 30);
        let projected = Node {
            schema: input.schema.clone(),
            estimated_rows: 120,
            op: RelOp::Project {
                input: Box::new(input),
                columns: vec![1, 0],
            },
        };
        assert_eq!(filtered_rows(&projected, &p), one);
        assert_eq!(filtered_rows(&projected, &cmp(1, Op::Lt, 0)), 30);
    }
}
