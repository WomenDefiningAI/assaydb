//! Literal frame admission and frame-sensitive value functions.
use super::*;

pub use crate::frame_exclusion::Exclude;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Unit {
    Rows,
    Range,
    Groups,
}

/// A literal frame offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Amount {
    /// A non-negative integer literal (rows, peer groups or key units).
    Count(u64),
    /// A whole-day INTERVAL literal (DATE keys; TIMESTAMP keys as 24 hours).
    Days(u64),
    /// An INTERVAL HOUR/MINUTE/SECOND literal in microseconds (TIMESTAMP and
    /// TIMESTAMPTZ keys only; integration candidate 4).
    Micros(u64),
    /// A non-negative decimal literal `digits / 10^scale` with a decimal
    /// point (DECIMAL and REAL keys only).
    Exact { digits: i128, scale: i8 },
}

/// The start bound's `Unbounded` is UNBOUNDED PRECEDING and the end bound's
/// UNBOUNDED FOLLOWING (the references reject the other two spellings).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Bound {
    Unbounded,
    Current,
    Offset { amount: Amount, following: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum ValueKind {
    First,
    Last,
    Nth(u64),
    Ntile(u64),
    PercentRank,
    CumeDist,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValueFunction {
    pub input: Option<Expr>,
    pub kind: ValueKind,
    /// FIRST_VALUE/LAST_VALUE/NTH_VALUE(x [, n] IGNORE NULLS) (DuckDB 1.5.6;
    /// PostgreSQL 17 has no null treatment).
    pub ignore_nulls: bool,
}

impl ValueFunction {
    pub(super) fn result_type(&self) -> Type {
        match self.kind {
            ValueKind::PercentRank | ValueKind::CumeDist => Type::Float,
            _ => self.input.as_ref().map_or(Type::Int, |e| e.ty.clone()),
        }
    }
}

fn integer(text: &str) -> Result<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err("unsupported: window bound must be a non-negative integer literal".into());
    }
    text.parse::<i64>()
        .map(|n| n as u64)
        .map_err(|_| "unsupported: window bound exceeds BIGINT".into())
}

fn literal(expr: &s::Expr) -> Result<u64> {
    match expr {
        s::Expr::Nested(e)
        | s::Expr::UnaryOp {
            op: s::UnaryOperator::Plus,
            expr: e,
        } => literal(e),
        s::Expr::Value(s::ValueWithSpan {
            value: s::Value::Number(n, false),
            ..
        }) => integer(n),
        _ => Err("unsupported: window bound must be a non-negative integer literal".into()),
    }
}

/// A non-negative decimal literal with a decimal point, as `digits / 10^scale`.
fn exact(expr: &s::Expr) -> Option<Result<Amount>> {
    match expr {
        s::Expr::Nested(e)
        | s::Expr::UnaryOp {
            op: s::UnaryOperator::Plus,
            expr: e,
        } => exact(e),
        s::Expr::Value(s::ValueWithSpan {
            value: s::Value::Number(n, false),
            ..
        }) if n.contains('.') => {
            let (whole, fraction) = n.split_once('.').unwrap();
            if !(whole.bytes().all(|b| b.is_ascii_digit())
                && fraction.bytes().all(|b| b.is_ascii_digit())
                && !(whole.is_empty() && fraction.is_empty()))
            {
                return Some(Err(
                    "unsupported: window bound must be a non-negative numeric literal".into(),
                ));
            }
            let text = format!("{whole}{fraction}");
            let text = text.trim_start_matches('0');
            if text.len() > 38 || fraction.len() > 38 {
                return Some(Err(
                    "unsupported: window bound exceeds DECIMAL precision 38".into(),
                ));
            }
            let digits = if text.is_empty() {
                0
            } else {
                text.parse::<i128>().unwrap()
            };
            Some(Ok(Amount::Exact {
                digits,
                scale: fraction.len() as i8,
            }))
        }
        _ => None,
    }
}

fn offset(expr: &s::Expr) -> Result<Amount> {
    if let s::Expr::Interval(interval) = expr {
        if interval.leading_precision.is_some()
            || interval.last_field.is_some()
            || interval.fractional_seconds_precision.is_some()
        {
            return Err("unsupported: RANGE requires whole-day literal intervals".into());
        }
        if let s::Expr::Value(s::ValueWithSpan {
            value: s::Value::SingleQuotedString(text),
            ..
        }) = interval.value.as_ref()
        {
            let words = text.split_whitespace().collect::<Vec<_>>();
            const SECOND: u64 = 1_000_000;
            let (digits, micros) = match (&interval.leading_field, words.as_slice()) {
                (Some(s::DateTimeField::Day), [n]) => (*n, 86_400 * SECOND),
                (None, [n, unit])
                    if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") =>
                {
                    (*n, 86_400 * SECOND)
                }
                // TIMESTAMP keys: exact time units shared by both references.
                (Some(s::DateTimeField::Hour), [n]) => (*n, 3_600 * SECOND),
                (Some(s::DateTimeField::Minute), [n]) => (*n, 60 * SECOND),
                (Some(s::DateTimeField::Second), [n]) => (*n, SECOND),
                _ => return Err("unsupported: RANGE requires whole-day literal intervals (or HOUR/MINUTE/SECOND for TIMESTAMP keys)".into()),
            };
            // DATE's finite physical domain is days in i32. Larger offsets also
            // exceed the common reference interval domain.
            let count = integer(digits)?;
            if count > i32::MAX as u64 {
                return Err("unsupported: RANGE day offset exceeds INTEGER".into());
            }
            return Ok(if micros == 86_400 * SECOND {
                Amount::Days(count)
            } else {
                Amount::Micros(count * micros)
            });
        }
        return Err("unsupported: RANGE requires whole-day literal intervals".into());
    }
    if let Some(amount) = exact(expr) {
        return amount;
    }
    Ok(Amount::Count(literal(expr)?))
}

/// Decode a frame-exclusion marker (frame_exclusion.rs) in the start bound.
fn start_bound(bound: &s::WindowFrameBound) -> Result<(s::WindowFrameBound, Exclude)> {
    use crate::frame_exclusion::Shape;
    let (expr, following) = match bound {
        s::WindowFrameBound::Preceding(Some(e)) => (e, false),
        s::WindowFrameBound::Following(Some(e)) => (e, true),
        other => return Ok((other.clone(), Exclude::NoOthers)),
    };
    let s::Expr::Function(function) = expr.as_ref() else {
        return Ok((bound.clone(), Exclude::NoOthers));
    };
    let Some((exclude, shape)) = crate::frame_exclusion::decode(&function.name.to_string()) else {
        return Ok((bound.clone(), Exclude::NoOthers));
    };
    let args = match &function.args {
        s::FunctionArguments::List(list) => list.args.as_slice(),
        _ => &[],
    };
    let wrap = |e: Option<Box<s::Expr>>| {
        if following {
            s::WindowFrameBound::Following(e)
        } else {
            s::WindowFrameBound::Preceding(e)
        }
    };
    let decoded = match (shape, args) {
        (Shape::Unbounded, []) => wrap(None),
        (Shape::Current, []) => s::WindowFrameBound::CurrentRow,
        (Shape::Offset, [s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(e))]) => {
            wrap(Some(Box::new(e.clone())))
        }
        _ => return Err("internal: malformed frame exclusion marker".into()),
    };
    Ok((decoded, exclude))
}

pub(super) fn parse(spec: &s::WindowSpec) -> Result<Frame> {
    let Some(frame) = &spec.window_frame else {
        return Ok(if spec.order_by.is_empty() {
            Frame::Partition
        } else {
            Frame::Range
        });
    };
    let unit = match frame.units {
        s::WindowFrameUnits::Rows => Unit::Rows,
        s::WindowFrameUnits::Range => Unit::Range,
        s::WindowFrameUnits::Groups => Unit::Groups,
    };
    let (start_bound, exclude) = start_bound(&frame.start_bound)?;
    let start = match &start_bound {
        s::WindowFrameBound::Preceding(None) => Bound::Unbounded,
        s::WindowFrameBound::Preceding(Some(e)) => Bound::Offset {
            amount: offset(e)?,
            following: false,
        },
        s::WindowFrameBound::CurrentRow => Bound::Current,
        s::WindowFrameBound::Following(Some(e)) => Bound::Offset {
            amount: offset(e)?,
            following: true,
        },
        // Both references reject this frame.
        s::WindowFrameBound::Following(None) => {
            return Err("frame start cannot be UNBOUNDED FOLLOWING".into());
        }
    };
    let end = match &frame.end_bound {
        Some(s::WindowFrameBound::Following(None)) => Bound::Unbounded,
        Some(s::WindowFrameBound::Following(Some(e))) => Bound::Offset {
            amount: offset(e)?,
            following: true,
        },
        None | Some(s::WindowFrameBound::CurrentRow) => Bound::Current,
        Some(s::WindowFrameBound::Preceding(Some(e))) => Bound::Offset {
            amount: offset(e)?,
            following: false,
        },
        Some(s::WindowFrameBound::Preceding(None)) => {
            return Err("frame end cannot be UNBOUNDED PRECEDING".into());
        }
    };
    // Frame shapes both references reject (PostgreSQL's and DuckDB's messages).
    let following_start = matches!(
        start,
        Bound::Offset {
            following: true,
            ..
        }
    );
    let preceding_end = matches!(
        end,
        Bound::Offset {
            following: false,
            ..
        }
    );
    if start == Bound::Current && preceding_end {
        return Err("frame starting from current row cannot have preceding rows".into());
    }
    if following_start && end == Bound::Current {
        return Err("frame starting from following row cannot end with current row".into());
    }
    if following_start && preceding_end {
        return Err("frame starting from following row cannot have preceding rows".into());
    }
    for bound in [start, end] {
        if let Bound::Offset { amount, .. } = bound
            && unit != Unit::Range
        {
            match amount {
                Amount::Count(_) => {}
                Amount::Days(_) | Amount::Micros(_) => {
                    return Err(format!(
                        "unsupported: {} requires integer offsets",
                        if unit == Unit::Rows { "ROWS" } else { "GROUPS" }
                    )
                    .into());
                }
                Amount::Exact { .. } => {
                    return Err(
                        "unsupported: window bound must be a non-negative integer literal".into(),
                    );
                }
            }
        }
    }
    if let (
        Bound::Offset {
            amount: low,
            following: low_following,
        },
        Bound::Offset {
            amount: high,
            following: high_following,
        },
    ) = (start, end)
        && low_following == high_following
    {
        // A start that statically follows the end selects no rows, except
        // that the references disagree on it: DuckDB 1.5.6 returns negative
        // COUNT(*) values and empty RANGE frames for NULL keys, PostgreSQL 0
        // and the NULL peer group.
        // A DAY and an HOUR/MINUTE/SECOND interval compare in microseconds.
        let micros = matches!(low, Amount::Micros(_)) || matches!(high, Amount::Micros(_));
        let value = |amount: Amount| -> (arrow::datatypes::i256, i8) {
            match amount {
                Amount::Days(n) if micros => (
                    arrow::datatypes::i256::from_i128(i128::from(n) * 86_400_000_000),
                    0,
                ),
                Amount::Count(n) | Amount::Days(n) | Amount::Micros(n) => {
                    (arrow::datatypes::i256::from_i128(n.into()), 0)
                }
                Amount::Exact { digits, scale } => {
                    (arrow::datatypes::i256::from_i128(digits), scale)
                }
            }
        };
        let ((a, a_scale), (b, b_scale)) = (value(low), value(high));
        let ten = arrow::datatypes::i256::from_i128(10);
        let a = a * ten.wrapping_pow(b_scale as u32);
        let b = b * ten.wrapping_pow(a_scale as u32);
        let inverted = if low_following { a > b } else { a < b };
        if inverted {
            return Err("unsupported: frame whose start follows its end (DuckDB 1.5.6 and PostgreSQL disagree: negative COUNT(*), RANGE frames of NULL keys)".into());
        }
    }
    if unit == Unit::Groups && spec.order_by.is_empty() {
        return Err("unsupported: GROUPS frames without ORDER BY (PostgreSQL requires ORDER BY; DuckDB treats the partition as one peer group)".into());
    }
    Ok(match (unit, start, end, exclude) {
        (_, Bound::Unbounded, Bound::Unbounded, Exclude::NoOthers) => Frame::Partition,
        (Unit::Rows, Bound::Unbounded, Bound::Current, Exclude::NoOthers) => Frame::Rows,
        // RANGE and GROUPS through the current row both end at the peer group.
        (_, Bound::Unbounded, Bound::Current, Exclude::NoOthers) => Frame::Range,
        _ => Frame::Bounded {
            unit,
            start,
            end,
            exclude,
        },
    })
}

/// The key kind a RANGE offset compares (one ORDER BY key).
pub(super) fn validate(spec: &Specification) -> Result<()> {
    if let Frame::Bounded {
        unit: Unit::Range,
        start,
        end,
        ..
    } = spec.frame
    {
        for bound in [start, end] {
            if let Bound::Offset { amount, .. } = bound {
                if spec.order.len() != 1 {
                    return Err(
                        "unsupported: RANGE offsets require exactly one Int/Date order key".into(),
                    );
                }
                match (&spec.order[0].0.ty, amount) {
                    // TIMESTAMP keys (candidate 3): DAY/HOUR/MINUTE/SECOND
                    // intervals. TIMESTAMPTZ: exact units only, since a DAY step
                    // is a calendar day in the session zone (23 or 25 hours
                    // across DST).
                    (Type::Timestamp, Amount::Days(_) | Amount::Micros(_))
                    | (Type::TimestampTz, Amount::Micros(_)) => (),
                    (Type::TimestampTz, _) => return Err("unsupported: TIMESTAMPTZ RANGE offsets require INTERVAL HOUR/MINUTE/SECOND".into()),
                    (Type::Timestamp, _) => return Err("unsupported: TIMESTAMP RANGE offsets require an INTERVAL (DAY/HOUR/MINUTE/SECOND)".into()),
                    (_, Amount::Micros(_)) => return Err("unsupported: HOUR/MINUTE/SECOND RANGE offsets require a TIMESTAMP key".into()),
                    (Type::Int, Amount::Count(_))
                    | (Type::Date, Amount::Days(_))
                    | (Type::Decimal(_), Amount::Count(_) | Amount::Exact { .. }) => (),
                    (Type::Float, Amount::Count(n)) if n <= 1 << 53 => (),
                    (Type::Float, Amount::Exact { digits, scale })
                        if digits <= 1 << 53 && scale <= 22 => {}
                    (Type::Float, _) => {
                        return Err("unsupported: REAL RANGE offset is not exactly representable in both references' conversion".into());
                    }
                    (Type::Int, Amount::Exact { .. }) => {
                        return Err("unsupported: INTEGER RANGE key with a fractional offset (PostgreSQL rejects it; DuckDB accepts it)".into());
                    }
                    (Type::Date, Amount::Count(_) | Amount::Exact { .. }) => return Err("unsupported: DATE RANGE offsets require INTERVAL whole days (PostgreSQL compatibility)".into()),
                    _ => return Err("unsupported: RANGE offsets require an Int/Decimal/Real key with a numeric offset or a Date key with a day interval".into()),
                }
            }
        }
    }
    Ok(())
}

pub(super) fn bind_value(function: &s::Function, input: Option<Expr>) -> Result<ValueFunction> {
    let s::FunctionArguments::List(args) = &function.args else {
        unreachable!()
    };
    let positive = |index: usize| -> Result<u64> {
        let s::FunctionArg::Unnamed(s::FunctionArgExpr::Expr(expr)) = &args.args[index] else {
            return Err("unsupported: value window requires unnamed expressions".into());
        };
        let n = literal(expr)?;
        if n == 0 || n > i32::MAX as u64 {
            return Err("unsupported: NTH_VALUE/NTILE require a positive INTEGER literal".into());
        }
        Ok(n)
    };
    let kind = match function.name.to_string().to_lowercase().as_str() {
        "first_value" => ValueKind::First,
        "last_value" => ValueKind::Last,
        "nth_value" => ValueKind::Nth(positive(1)?),
        "ntile" => ValueKind::Ntile(positive(0)?),
        "percent_rank" => ValueKind::PercentRank,
        "cume_dist" => ValueKind::CumeDist,
        _ => unreachable!(),
    };
    let ignore_nulls = args.clauses.iter().any(|clause| {
        matches!(
            clause,
            s::FunctionArgumentClause::IgnoreOrRespectNulls(s::NullTreatment::IgnoreNulls)
        )
    });
    Ok(ValueFunction {
        input,
        kind,
        ignore_nulls,
    })
}

impl Planner<'_> {
    /// DuckDB QUALIFY: input columns win over aliases. Alias substitution is
    /// done before aggregate/window lifting, so the predicate uses that same
    /// completed input. A hidden projection also accounts for QUALIFY-only calls.
    pub(in crate::rel) fn qualify_projection(
        &mut self,
        select: &mut s::Select,
        input: &Schema,
    ) -> Result<Option<usize>> {
        let Some(mut predicate) = select.qualify.take() else {
            return Ok(None);
        };
        let mut windowed = contains(&predicate)?;
        for item in &select.projection {
            if let s::SelectItem::UnnamedExpr(e) | s::SelectItem::ExprWithAlias { expr: e, .. } =
                item
            {
                windowed |= contains(e)?;
            }
        }
        if !windowed {
            return Err("unsupported: QUALIFY requires a window function".into());
        }
        syntax::rewrite(&mut predicate, &mut |expr| {
            if let s::Expr::Identifier(id) = expr {
                if input.fields().iter().any(|field| {
                    field
                        .name()
                        .rsplit('.')
                        .next()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&id.value))
                }) {
                    // A real but ambiguous input name must remain an error;
                    // SELECT aliases cannot silently disambiguate joined inputs.
                    binder(input).expr(&s::Expr::Identifier(id.clone()))?;
                    return Ok(true);
                }
                let matches = select
                    .projection
                    .iter()
                    .filter_map(|item| match item {
                        s::SelectItem::ExprWithAlias { expr, alias }
                            if alias.value.eq_ignore_ascii_case(&id.value) =>
                        {
                            Some(expr)
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if matches.len() > 1 {
                    return Err("unsupported: ambiguous QUALIFY alias".into());
                }
                if let Some(value) = matches.first() {
                    *expr = (*value).clone();
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        let index = select.projection.len();
        select.projection.push(s::SelectItem::ExprWithAlias {
            expr: predicate,
            alias: s::Ident::new(self.fresh()),
        });
        Ok(Some(index))
    }
}
