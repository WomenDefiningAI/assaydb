//! Syntax binding and a deliberately small, explicit physical plan.
use crate::{
    Result,
    expr::{Expr, Kind, Literal, Op},
    types::{Type, decimal_literal},
};
use arrow::datatypes::Schema;
use chrono::NaiveDate;
use serde::Serialize;
use sqlparser::ast as sql;
use std::{collections::BTreeSet, path::PathBuf};
pub(crate) mod branch_types;
mod date_toolkit;
mod math_functions;
mod ordered_set;
mod regexp_functions;
pub(crate) mod statistics;
mod text_functions;
pub use ordered_set::{Fraction, Quantile};

#[derive(Debug, Clone, Serialize)]
pub enum Aggregate {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    /// STRING_AGG(text, separator ORDER BY ...). The input is a
    /// [`Kind::SortPack`] carrying the ordering key bytes and the value.
    StringAgg(String),
    /// VARIANCE/STDDEV/COVAR/CORR/REGR_* (see [`crate::moments`]). A
    /// one-argument form reads its numeric input directly; a two-argument
    /// form reads [`Kind::Pack`]`(y, x)`. `inputs` holds the argument types.
    Moment {
        function: Moment,
        inputs: Vec<Type>,
    },
    /// ARG_MAX/ARG_MIN (MAX_BY/MIN_BY) over [`Kind::Pack`]`(arg, value)`.
    /// Among rows tying on the extreme value, the first in canonical input
    /// order is returned (DuckDB 1.5.6 permits any of them).
    ArgExtreme {
        minimum: bool,
        arg: Type,
        value: Type,
    },
    /// MEDIAN, QUANTILE_CONT/QUANTILE_DISC/QUANTILE and PERCENTILE_CONT/
    /// PERCENTILE_DISC WITHIN GROUP (see [`ordered_set`]). The state keeps
    /// every non-NULL input value of a group; the result never depends on
    /// input order.
    Quantile(Quantile),
    /// MODE(x) and MODE() WITHIN GROUP (ORDER BY x [DESC]): among equally
    /// frequent values the first in the ordering wins (the smallest, or the
    /// largest with DESC), as PostgreSQL documents for mode(). The state is one
    /// count per distinct value of a group.
    Mode {
        descending: bool,
    },
}
impl Aggregate {
    /// Ordered-set aggregates keep per-group value state (see
    /// `execute::aggregation::ordered_set`).
    pub(crate) fn is_ordered_set(&self) -> bool {
        matches!(self, Self::Quantile(_) | Self::Mode { .. })
    }
    /// Whether two ordered-set aggregates over one input can share a state:
    /// quantiles keep every value, MODE keeps one count per distinct value.
    pub(crate) fn shares_ordered_state(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Quantile(_), Self::Quantile(_)) | (Self::Mode { .. }, Self::Mode { .. })
        )
    }
}
/// Second-moment statistics; argument order is (y, x) as in SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Moment {
    VarSamp,
    VarPop,
    StddevSamp,
    StddevPop,
    CovarSamp,
    CovarPop,
    Corr,
    RegrSlope,
    RegrIntercept,
    RegrR2,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrAvgX,
    RegrAvgY,
}
#[derive(Debug, Clone, Serialize)]
pub struct Agg {
    pub function: Aggregate,
    pub input: Option<Expr>,
    pub ty: Type,
    pub distinct: bool,
}
#[derive(Debug, Clone, Serialize)]
pub enum Output {
    Expr(Expr),
    Group(usize),
    Aggregate(usize),
}
#[derive(Debug, Clone, Serialize)]
pub struct Sort {
    pub column: usize,
    pub descending: bool,
    pub nulls_first: bool,
}
#[derive(Debug, Clone, Serialize)]
pub struct Plan {
    pub table: Option<String>,
    pub path: Option<PathBuf>,
    pub columns: Vec<usize>,
    pub filter: Option<Expr>,
    pub groups: Vec<Expr>,
    pub aggregates: Vec<Agg>,
    pub output: Vec<Output>,
    pub names: Vec<String>,
    pub types: Vec<Type>,
    pub order: Vec<Sort>,
    pub aggregation: bool,
    pub projection_pushdown: bool,
}
fn unsupported(s: impl std::fmt::Display) -> crate::Error {
    format!("unsupported: {s}").into()
}
fn ident(i: &sql::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    }
}
fn literal(ty: Type, v: Literal) -> Expr {
    Expr {
        ty,
        kind: Kind::Literal(v),
    }
}
fn bool_expr(kind: Kind) -> Expr {
    Expr {
        kind,
        ty: Type::Bool,
    }
}
fn coerce_null(e: &mut Expr, ty: &Type) {
    if e.ty == Type::Null {
        e.ty = ty.clone();
    }
}
pub(crate) fn binary(op: Op, mut a: Expr, mut b: Expr) -> Result<Expr> {
    // Resolve the DATE overload before general NULL coercion: DATE - NULL,
    // like DATE + NULL, selects DuckDB's nullable day-offset overload.
    if matches!(op, Op::Add | Op::Sub) && a.ty == Type::Date && b.ty == Type::Null {
        coerce_null(&mut b, &Type::Int);
    } else if op == Op::Add && a.ty == Type::Null && b.ty == Type::Date {
        coerce_null(&mut a, &Type::Int);
    }
    let at = a.ty.clone();
    coerce_null(&mut a, &b.ty);
    coerce_null(&mut b, &at);
    if a.ty == Type::Null
        && b.ty == Type::Null
        && matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod)
    {
        return Ok(Expr {
            ty: Type::Null,
            kind: Kind::Binary(op, Box::new(a), Box::new(b)),
        });
    }
    let ty = match op {
        Op::And | Op::Or => {
            coerce_null(&mut a, &Type::Bool);
            coerce_null(&mut b, &Type::Bool);
            if a.ty != Type::Bool || b.ty != Type::Bool {
                return Err("AND/OR require booleans".into());
            }
            Type::Bool
        }
        Op::Mod => {
            // Helen, October 17: DuckDB 1.5.6/PostgreSQL remainder. REAL is
            // fmod; INTEGER and DECIMAL are exact at the larger scale.
            if !(a.ty == Type::Float || a.ty.scale().is_ok())
                || !(b.ty == Type::Float || b.ty.scale().is_ok())
            {
                return Err("remainder requires numeric inputs".into());
            }
            if a.ty == Type::Float || b.ty == Type::Float {
                Type::Float
            } else if a.ty == Type::Int && b.ty == Type::Int {
                Type::Int
            } else {
                Type::Decimal(a.ty.scale()?.max(b.ty.scale()?))
            }
        }
        Op::Div => {
            if !(a.ty == Type::Float || a.ty.scale().is_ok())
                || !(b.ty == Type::Float || b.ty.scale().is_ok())
            {
                return Err("division requires numeric inputs".into());
            }
            // Approved SQL integer division: only two Int operands (after
            // ordinary NULL coercion) truncate toward zero. Decimal scale zero
            // remains Decimal and retains the existing floating division.
            if a.ty == Type::Int && b.ty == Type::Int {
                Type::Int
            } else {
                Type::Float
            }
        }
        Op::Add | Op::Sub | Op::Mul
            if op == Op::Sub
                && matches!(
                    (&a.ty, &b.ty),
                    (Type::Timestamp, Type::Timestamp | Type::Date)
                        | (Type::Date, Type::Timestamp)
                        | (
                            Type::TimestampTz,
                            Type::TimestampTz | Type::Timestamp | Type::Date
                        )
                        | (Type::Timestamp | Type::Date, Type::TimestampTz)
                ) =>
        {
            return Err(unsupported(
                "TIMESTAMP subtraction returns INTERVAL, which has no Assay value type; use EXTRACT(EPOCH FROM a) - EXTRACT(EPOCH FROM b)",
            ));
        }
        Op::Add | Op::Sub | Op::Mul => {
            // DATE arithmetic keeps the ordinary binary traversal/accounting.
            // Invalid combinations (including multiplication) remain errors.
            if matches!((&a.ty, &b.ty), (Type::Date, Type::Int)) && matches!(op, Op::Add | Op::Sub)
                || op == Op::Add && matches!((&a.ty, &b.ty), (Type::Int, Type::Date))
                || op == Op::Sub && matches!((&a.ty, &b.ty), (Type::Date, Type::Date))
            {
                for offset in [&a, &b] {
                    if offset.ty == Type::Int
                        && let Kind::Literal(Literal::Num(n)) = offset.kind
                        && i32::try_from(n).is_err()
                    {
                        return Err(unsupported(
                            "DATE day offset outside signed 32-bit INTEGER range",
                        ));
                    }
                }
                return Ok(Expr {
                    ty: if a.ty == Type::Date && b.ty == Type::Date {
                        Type::Int
                    } else {
                        Type::Date
                    },
                    kind: Kind::Binary(op, Box::new(a), Box::new(b)),
                });
            }
            if a.ty == Type::Float || b.ty == Type::Float {
                if !(a.ty == Type::Float || a.ty.scale().is_ok())
                    || !(b.ty == Type::Float || b.ty.scale().is_ok())
                {
                    return Err("arithmetic requires numeric inputs".into());
                }
                return Ok(Expr {
                    ty: Type::Float,
                    kind: Kind::Binary(op, Box::new(a), Box::new(b)),
                });
            }
            let sa = a.ty.scale()?;
            let sb = b.ty.scale()?;
            let scale = if op == Op::Mul { sa + sb } else { sa.max(sb) };
            if scale > 38 {
                return Err(unsupported("decimal scale >38"));
            }
            if a.ty == Type::Int && b.ty == Type::Int {
                Type::Int
            } else {
                Type::Decimal(scale)
            }
        }
        _ => {
            if a.ty != b.ty
                && !((a.ty.scale().is_ok() || a.ty == Type::Float)
                    && (b.ty.scale().is_ok() || b.ty == Type::Float))
            {
                if matches!(
                    (&a.ty, &b.ty),
                    (Type::Date, Type::Text) | (Type::Text, Type::Date)
                ) {
                    return Err(unsupported("comparison requires compatible types"));
                }
                return Err("comparison requires compatible types".into());
            }
            if a.ty == Type::Null {
                a.ty = Type::Int;
                b.ty = Type::Int;
            }
            Type::Bool
        }
    };
    Ok(Expr {
        ty,
        kind: Kind::Binary(op, Box::new(a), Box::new(b)),
    })
}
/// `FALSE OR (x = a) OR (x = b) ...` for an IN list. Lists of up to
/// `nesting::IN_LIST_CHAIN` items keep that left-deep shape. Longer lists
/// combine adjacent terms level by level: the same terms in the same order
/// (OR is associative under SQL's three-valued logic and every term is
/// evaluated either way), but the tree height grows with log2 of the list
/// length, so list width never becomes stack depth.
fn in_list_disjunction(mut terms: Vec<Expr>) -> Result<Expr> {
    if terms.len() <= crate::nesting::IN_LIST_CHAIN + 1 {
        let mut terms = terms.into_iter();
        let mut value = terms.next().expect("FALSE term");
        for term in terms {
            value = binary(Op::Or, value, term)?;
        }
        return Ok(value);
    }
    while terms.len() > 1 {
        let mut level = Vec::with_capacity(terms.len().div_ceil(2));
        let mut pairs = terms.into_iter();
        while let Some(left) = pairs.next() {
            level.push(match pairs.next() {
                Some(right) => binary(Op::Or, left, right)?,
                None => left,
            });
        }
        terms = level;
    }
    Ok(terms.pop().expect("FALSE term"))
}
fn binary_op(op: &sql::BinaryOperator) -> Result<Op> {
    Ok(match op {
        sql::BinaryOperator::Plus => Op::Add,
        sql::BinaryOperator::Minus => Op::Sub,
        sql::BinaryOperator::Multiply => Op::Mul,
        sql::BinaryOperator::Divide => Op::Div,
        sql::BinaryOperator::Modulo => Op::Mod,
        sql::BinaryOperator::Eq => Op::Eq,
        sql::BinaryOperator::NotEq => Op::Ne,
        sql::BinaryOperator::Lt => Op::Lt,
        sql::BinaryOperator::LtEq => Op::Le,
        sql::BinaryOperator::Gt => Op::Gt,
        sql::BinaryOperator::GtEq => Op::Ge,
        sql::BinaryOperator::And => Op::And,
        sql::BinaryOperator::Or => Op::Or,
        _ => return Err(unsupported(op)),
    })
}
/// An operator bound by `Binder::binary_chain`: an arithmetic/comparison/
/// logical operator or `||` (string concatenation).
#[derive(Clone, Copy)]
enum ChainOp {
    Op(Op),
    Concat,
}
fn chain_op(op: &sql::BinaryOperator) -> Option<ChainOp> {
    if *op == sql::BinaryOperator::StringConcat {
        return Some(ChainOp::Concat);
    }
    binary_op(op).ok().map(ChainOp::Op)
}
pub(crate) struct Binder<'a> {
    pub schema: &'a Schema,
    pub table: Option<&'a str>,
    pub facts: Option<&'a [crate::mixed_numeric::Fact]>,
}
impl Binder<'_> {
    /// Helen, October 15: a promoted mixed Int/Float value is compared in
    /// DOUBLE, as DuckDB 1.5.6 does. Comparing its rounded binary64 value
    /// exactly against the original Int would contradict SQLite, DuckDB and
    /// PostgreSQL alike. Ordinary Int/Float comparisons remain exact.
    fn promoted_comparison(&self, a: &mut Expr, b: &mut Expr) {
        let float = |e: &mut Expr| {
            *e = Expr {
                ty: Type::Float,
                kind: Kind::Cast(Box::new(e.clone())),
            };
        };
        let promoted = |e: &Expr| crate::mixed_numeric::promoted(e, self.schema, self.facts);
        match (&a.ty, &b.ty) {
            (Type::Float, Type::Int) if promoted(a) => float(b),
            (Type::Int, Type::Float) if promoted(b) => float(a),
            _ => {}
        }
    }
    /// Wrap a promoted Float operand in a runtime consumer guard.
    fn promoted_guard(&self, e: Expr, guard: &str) -> Expr {
        if crate::mixed_numeric::promoted(&e, self.schema, self.facts) {
            crate::mixed_numeric::guard(e, guard)
        } else {
            e
        }
    }
    /// Only SQL string literals get implicit DATE conversion. Inspect the AST,
    /// not a folded expression: a TEXT column/CAST/CASE is still typed TEXT.
    fn comparison_inputs(&self, inputs: &[&sql::Expr]) -> Result<Vec<Expr>> {
        let values = inputs
            .iter()
            .map(|input| self.expr(input))
            .collect::<Result<Vec<_>>>()?;
        Self::comparison_dates(inputs, values)
    }
    /// `comparison_inputs` after binding: DATE conversion of string literals.
    fn comparison_dates(inputs: &[&sql::Expr], mut values: Vec<Expr>) -> Result<Vec<Expr>> {
        if values.iter().any(|value| value.ty == Type::TimestampTz) {
            for (source, value) in inputs.iter().zip(&mut values) {
                if value.ty == Type::Text {
                    let mut source = *source;
                    while let sql::Expr::Nested(inner) = source {
                        source = inner;
                    }
                    let sql::Expr::Value(sql::ValueWithSpan {
                        value: sql::Value::SingleQuotedString(text),
                        ..
                    }) = source
                    else {
                        return Err(unsupported(
                            "TIMESTAMPTZ comparison requires TIMESTAMPTZ, TIMESTAMP, DATE or a string literal",
                        ));
                    };
                    *value = Self::timestamptz(text)?;
                } else {
                    *value = Self::to_timestamptz(value.clone())?;
                }
            }
            return Ok(values);
        }
        if values.iter().any(|value| value.ty == Type::Timestamp) {
            return Self::comparison_timestamps(inputs, values);
        }
        if values.iter().any(|value| value.ty == Type::Date) {
            for (source, value) in inputs.iter().zip(&mut values) {
                if value.ty == Type::Text {
                    let mut source = *source;
                    while let sql::Expr::Nested(inner) = source {
                        source = inner;
                    }
                    let sql::Expr::Value(sql::ValueWithSpan {
                        value: sql::Value::SingleQuotedString(text),
                        ..
                    }) = source
                    else {
                        return Err(unsupported("comparison requires compatible types"));
                    };
                    // chrono's parser also accepts non-padded dates. Implicit
                    // conversion deliberately has the card's stricter grammar.
                    let bytes = text.as_bytes();
                    if bytes.len() != 10
                        || bytes[4] != b'-'
                        || bytes[7] != b'-'
                        || !bytes
                            .iter()
                            .enumerate()
                            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
                        || &bytes[..4] == b"0000"
                    {
                        return Err(format!(
                            "invalid DATE literal {text:?}: expected YYYY-MM-DD with year 0001..9999"
                        ).into());
                    }
                    *value = Self::date(text)
                        .map_err(|error| format!("invalid DATE literal {text:?}: {error}"))?;
                } else if !matches!(value.ty, Type::Date | Type::Null) {
                    return Err(unsupported(
                        "DATE comparison requires DATE or a YYYY-MM-DD string literal",
                    ));
                }
            }
        }
        Ok(values)
    }

    /// TIMESTAMP comparisons: both references convert a string literal with
    /// TIMESTAMP input rules and a DATE operand to midnight.
    fn comparison_timestamps(inputs: &[&sql::Expr], mut values: Vec<Expr>) -> Result<Vec<Expr>> {
        for (source, value) in inputs.iter().zip(&mut values) {
            match value.ty {
                Type::Timestamp | Type::Null => {}
                Type::Date => {
                    *value = Expr {
                        ty: Type::Timestamp,
                        kind: Kind::Function("$ts_from_date".into(), Box::new(value.clone())),
                    };
                }
                Type::Text => {
                    let mut source = *source;
                    while let sql::Expr::Nested(inner) = source {
                        source = inner;
                    }
                    let sql::Expr::Value(sql::ValueWithSpan {
                        value: sql::Value::SingleQuotedString(text),
                        ..
                    }) = source
                    else {
                        return Err(unsupported(
                            "TIMESTAMP comparison requires TIMESTAMP, DATE or a string literal",
                        ));
                    };
                    *value = Self::timestamp(text)?;
                }
                _ => {
                    return Err(unsupported(
                        "TIMESTAMP comparison requires TIMESTAMP, DATE or a string literal",
                    ));
                }
            }
        }
        Ok(values)
    }

    fn extract_date(&self, field: &str, source: &sql::Expr) -> Result<Expr> {
        let mut input = self.expr(source)?;
        if input.ty == Type::TimestampTz {
            let zone = crate::zone::statement()?.zone;
            let (name, ty) = if let Some(offset) = crate::zone::offset_field(field) {
                (offset, Type::Int)
            } else {
                let field = crate::expr::timestamp::Field::parse(field)
                    .ok_or_else(|| unsupported(format!("TIMESTAMPTZ field {field}")))?;
                (field.name(), field.ty())
            };
            return Ok(Expr {
                ty,
                kind: Kind::Function(
                    crate::zone::function(&format!("part_{name}"), &zone),
                    Box::new(input),
                ),
            });
        }
        if input.ty == Type::Timestamp {
            let field = crate::expr::timestamp::Field::parse(field)
                .ok_or_else(|| unsupported(format!("TIMESTAMP field {field}")))?;
            return Ok(Expr {
                ty: field.ty(),
                kind: Kind::Function(field.function_name(), Box::new(input)),
            });
        }
        let field = crate::expr::date::DateField::parse(field)
            .ok_or_else(|| unsupported(format!("DATE field {field}")))?;
        coerce_null(&mut input, &Type::Date);
        if input.ty != Type::Date {
            return Err(unsupported("EXTRACT/DATE_PART requires DATE input"));
        }
        Ok(Expr {
            ty: Type::Int,
            // Retain the established YEAR representation and optimizer path.
            kind: if field == crate::expr::date::DateField::Year {
                Kind::Year(Box::new(input))
            } else {
                Kind::Function(field.function_name(), Box::new(input))
            },
        })
    }

    pub(crate) fn expr(&self, e: &sql::Expr) -> Result<Expr> {
        if let sql::Expr::BinaryOp { op, .. } = e
            && chain_op(op).is_some()
        {
            return self.binary_chain(e);
        }
        let bound = self.expr_inner(e)?;
        crate::mixed_numeric::normalize_expression(bound, self.schema, self.facts)
    }
    /// A left-deep operator chain (`a OR b OR ...`, `1 + 2 - ...`, `a || b`) is bound
    /// without recursing down its left spine: sqlparser builds such chains in
    /// a loop, so their length is limited by `nesting::HEIGHT_LIMIT`, not by
    /// nesting. Operators are checked top-down and operands bound left to
    /// right, as the recursive form did; every node is normalized.
    #[inline(never)]
    fn binary_chain(&self, e: &sql::Expr) -> Result<Expr> {
        let mut spine = Vec::new();
        let mut node = e;
        while let sql::Expr::BinaryOp { left, op, right } = node
            && let Some(bound_op) = chain_op(op)
        {
            // DATE +/- INTERVAL is validated top-down, before any operand is
            // bound, exactly as the recursive binder did (`||` never shifts).
            let shift = match bound_op {
                ChainOp::Op(_) => Self::date_interval_shift(op, right)?,
                ChainOp::Concat => None,
            };
            spine.push((node, bound_op, right.as_ref(), shift));
            node = left;
        }
        let mut value = self.expr(node)?;
        let mut left = node;
        for (source, op, right, shift) in spine.into_iter().rev() {
            value = match (shift, op) {
                (Some(shift), _) => Self::date_shift(value, shift)?,
                // `||` converts its left operand before binding the right one.
                (None, ChainOp::Concat) => {
                    let left_value = Self::concat_value(value)?;
                    Self::concat_finish(left_value, self.expr(right)?)?
                }
                (None, ChainOp::Op(op)) => {
                    let right_value = self.expr(right)?;
                    self.chain_step(op, left, right, value, right_value)?
                }
            };
            value = crate::mixed_numeric::normalize_expression(value, self.schema, self.facts)?;
            left = source;
        }
        Ok(value)
    }
    /// One bound operator of `binary_chain`; it never recurses, so its frame
    /// is not held while operands bind. Comparisons apply DATE literal
    /// coercion and promoted-DOUBLE comparison.
    #[inline(never)]
    fn chain_step(
        &self,
        op: Op,
        left: &sql::Expr,
        right: &sql::Expr,
        a: Expr,
        b: Expr,
    ) -> Result<Expr> {
        if matches!(op, Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge) {
            let mut values = Self::comparison_dates(&[left, right], vec![a, b])?.into_iter();
            let mut a = values.next().unwrap();
            let mut b = values.next().unwrap();
            self.promoted_comparison(&mut a, &mut b);
            return binary(op, a, b);
        }
        // REAL remainder is fmod on the binary64 operands (Helen, October 17),
        // so a promoted `%` operand needs no INTEGER-range guard: DuckDB
        // promotes and applies fmod identically.
        binary(op, a, b)
    }
    /// Kinds that can repeat along a chain (casts, `IS [NOT] NULL`, prefix
    /// operators, parentheses) recurse through this small frame; everything
    /// else is bound in `expr_node`, whose frame is several KiB.
    fn expr_inner(&self, e: &sql::Expr) -> Result<Expr> {
        use sql::Expr as E;
        match e {
            E::Nested(inner) => self.expr(inner),
            E::IsNull(a) => Ok(bool_expr(Kind::IsNull(Box::new(self.expr(a)?), false))),
            E::IsNotNull(a) => Ok(bool_expr(Kind::IsNull(Box::new(self.expr(a)?), true))),
            E::Cast {
                kind,
                expr,
                data_type,
                format,
            } if matches!(kind, sql::CastKind::Cast | sql::CastKind::DoubleColon)
                && format.is_none() =>
            {
                let value = self.expr(expr)?;
                self.cast(value, data_type)
            }
            E::UnaryOp { op, expr } => {
                let a = self.expr(expr)?;
                Self::unary(e, op, a)
            }
            _ => self.expr_node(e),
        }
    }
    #[inline(never)]
    fn expr_node(&self, e: &sql::Expr) -> Result<Expr> {
        use sql::Expr as E;
        Ok(match e {
            E::Identifier(i) => self.column(i)?,
            E::CompoundIdentifier(ids) if ids.len() == 2 => {
                if self.table == Some(ident(&ids[0]).as_str()) {
                    self.column(&ids[1])?
                } else {
                    self.column(&sql::Ident::new(format!(
                        "{}.{}",
                        ident(&ids[0]),
                        ident(&ids[1])
                    )))?
                }
            }
            E::BinaryOp {
                left,
                op:
                    op @ (sql::BinaryOperator::PGRegexMatch
                    | sql::BinaryOperator::PGRegexNotMatch
                    | sql::BinaryOperator::PGRegexIMatch
                    | sql::BinaryOperator::PGRegexNotIMatch),
                right,
            } => self.regexp_operator(left, op, right)?,
            // Chain operators (including `||`) are bound by `expr`; anything
            // else is refused.
            E::BinaryOp { op, .. } => return Err(unsupported(op)),
            E::Value(v) => match &v.value {
                sql::Value::Number(s, _) => {
                    let (n, s) = decimal_literal(s)?;
                    literal(
                        if s == 0 { Type::Int } else { Type::Decimal(s) },
                        Literal::Num(n),
                    )
                }
                sql::Value::SingleQuotedString(s) => literal(Type::Text, Literal::Text(s.clone())),
                sql::Value::HexStringLiteral(s) => literal(
                    Type::Binary,
                    Literal::Binary(crate::types::binary_literal(s)?),
                ),
                sql::Value::Boolean(v) => literal(Type::Bool, Literal::Bool(*v)),
                sql::Value::Null => literal(Type::Null, Literal::Null),
                _ => return Err(unsupported(e)),
            },
            E::TypedString { data_type, value } if *data_type == sql::DataType::Date => {
                match &value.value {
                    sql::Value::SingleQuotedString(s) => Self::date(s)?,
                    _ => return Err(unsupported(e)),
                }
            }
            E::TypedString {
                data_type:
                    sql::DataType::Timestamp(
                        None,
                        sql::TimezoneInfo::None | sql::TimezoneInfo::WithoutTimeZone,
                    ),
                value,
            } => match &value.value {
                sql::Value::SingleQuotedString(s) => Self::timestamp(s)?,
                _ => return Err(unsupported(e)),
            },
            E::TypedString {
                data_type:
                    sql::DataType::Timestamp(
                        None,
                        sql::TimezoneInfo::WithTimeZone | sql::TimezoneInfo::Tz,
                    ),
                value,
            } => match &value.value {
                sql::Value::SingleQuotedString(s) => Self::timestamptz(s)?,
                _ => return Err(unsupported(e)),
            },
            E::TypedString {
                data_type: sql::DataType::Timestamp(Some(_), _),
                ..
            } => {
                return Err(unsupported(
                    "TIMESTAMP(p) precision (PostgreSQL rounds to p digits, DuckDB ignores p)",
                ));
            }
            E::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                let value = self.expr(timestamp)?;
                Self::at_time_zone(value, time_zone)?
            }
            E::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // Resolve one DATE domain for all three operands, including a
                // literal on the left and only one explicitly typed bound.
                let mut values = self.comparison_inputs(&[expr, low, high])?.into_iter();
                let a = values.next().unwrap();
                let (mut a_low, mut low) = (a.clone(), values.next().unwrap());
                let (mut a_high, mut high) = (a, values.next().unwrap());
                self.promoted_comparison(&mut a_low, &mut low);
                self.promoted_comparison(&mut a_high, &mut high);
                let bound = binary(
                    Op::And,
                    binary(Op::Ge, a_low, low)?,
                    binary(Op::Le, a_high, high)?,
                )?;
                if *negated {
                    bool_expr(Kind::Not(Box::new(bound)))
                } else {
                    bound
                }
            }
            E::Function(f)
                if matches!(
                    f.name.to_string().to_lowercase().as_str(),
                    "listagg" | "group_concat" | "grouping_id"
                ) =>
            {
                let name = f.name.to_string().to_uppercase();
                return Err(unsupported(if name == "GROUPING_ID" {
                    "GROUPING_ID (PostgreSQL has no GROUPING_ID; use GROUPING(...))".to_owned()
                } else {
                    format!(
                        "{name} (PostgreSQL has no {name}; use STRING_AGG(value, separator ORDER BY ...))"
                    )
                }));
            }
            E::Function(f) if f.name.to_string().eq_ignore_ascii_case("grouping") => {
                return Err("GROUPING is only allowed in the SELECT, HAVING and ORDER BY of a grouped query".into());
            }
            E::Function(f) if f.over.is_some() => {
                return Err(unsupported(
                    "window function outside SELECT/ORDER BY window stage",
                ));
            }
            E::Function(f)
                if f.filter.is_none()
                    && f.over.is_none()
                    && f.null_treatment.is_none()
                    && f.within_group.is_empty()
                    && matches!(f.parameters, sql::FunctionArguments::None) =>
            {
                let name = f.name.to_string().to_lowercase();
                if matches!(
                    name.as_str(),
                    "current_date" | "current_timestamp" | "now" | "localtimestamp"
                ) {
                    return Self::clock(&name, &f.args);
                }
                let sql::FunctionArguments::List(args) = &f.args else {
                    return Err(unsupported("function arguments"));
                };
                if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
                    return Err(unsupported("scalar function arity/modifiers"));
                }
                if name == "date_trunc" {
                    return self.date_trunc(&args.args);
                }
                if name == "round" {
                    if !(1..=2).contains(&args.args.len()) {
                        return Err(unsupported("ROUND requires one or two arguments"));
                    }
                    let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(value)) =
                        &args.args[0]
                    else {
                        return Err(unsupported("ROUND requires unnamed arguments"));
                    };
                    let digits = if args.args.len() == 1 {
                        Some(0)
                    } else {
                        let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(digits)) =
                            &args.args[1]
                        else {
                            return Err(unsupported(
                                "ROUND precision must be an integer literal or NULL",
                            ));
                        };
                        if matches!(
                            digits,
                            sql::Expr::Value(sql::ValueWithSpan {
                                value: sql::Value::Null,
                                ..
                            })
                        ) {
                            None
                        } else {
                            if !branch_types::integer_literal_syntax(digits) {
                                return Err(unsupported(
                                    "ROUND precision must be an integer literal or NULL",
                                ));
                            }
                            Some(crate::expr::round::precision(&self.expr(digits)?)?)
                        }
                    };
                    return crate::expr::round::bind(self.expr(value)?, digits);
                }
                if name == "date_part" {
                    let [
                        sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(sql::Expr::Value(
                            sql::ValueWithSpan {
                                value: sql::Value::SingleQuotedString(field),
                                ..
                            },
                        ))),
                        sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(source)),
                    ] = args.args.as_slice()
                    else {
                        return Err(unsupported(
                            "DATE_PART requires a literal field name and one DATE input",
                        ));
                    };
                    return self.extract_date(field, source);
                }
                if name == "nullif" {
                    if args.args.len() != 2 {
                        return Err("NULLIF requires exactly two arguments".into());
                    }
                    let mut values = Vec::with_capacity(2);
                    for argument in &args.args {
                        let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(value)) = argument
                        else {
                            return Err(unsupported("scalar argument"));
                        };
                        values.push((
                            self.expr(value)?,
                            branch_types::integer_literal_syntax(value),
                        ));
                    }
                    let (mut right, right_literal) = values.pop().unwrap();
                    let (mut left, _) = values.pop().unwrap();
                    if left.ty == Type::Float && right_literal {
                        branch_types::promote_float_literal(&mut right);
                    }
                    let promoted =
                        |e: &Expr| crate::mixed_numeric::promoted(e, self.schema, self.facts);
                    if left.ty == Type::Float && right.ty == Type::Int && promoted(&left) {
                        // NULLIF returns the Float left operand; cast the Int.
                        self.promoted_comparison(&mut left, &mut right);
                    } else if left.ty == Type::Int && right.ty == Type::Float && promoted(&right) {
                        // The Int result type is kept; mark the comparison.
                        right = crate::mixed_numeric::guard(
                            right,
                            crate::mixed_numeric::PROMOTED_COMPARE,
                        );
                    }
                    // Validate comparison compatibility without coercing the
                    // result: NULLIF always retains its first argument's type.
                    binary(Op::Eq, left.clone(), right.clone())?;
                    return Ok(Expr {
                        ty: left.ty.clone(),
                        kind: Kind::NullIf(Box::new(left), Box::new(right)),
                    });
                }
                if name == "coalesce" {
                    if args.args.len() < 2 {
                        return Err("COALESCE requires at least two arguments".into());
                    }
                    let mut values = Vec::with_capacity(args.args.len());
                    let mut ty = Type::Null;
                    for argument in &args.args {
                        let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(value)) = argument
                        else {
                            return Err(unsupported("scalar argument"));
                        };
                        let bound = self.expr(value)?;
                        ty = common_type(&ty, &bound.ty)?;
                        values.push((bound, branch_types::integer_literal_syntax(value)));
                    }
                    return branch_types::coalesce(values, self);
                }
                if let Some(expr) = self.regexp_function(&name, &args.args)? {
                    return Ok(expr);
                }
                if let Some(expr) = self.text_function(&name, &args.args)? {
                    return Ok(expr);
                }
                if let Some(expr) = self.math_function(&name, &args.args)? {
                    return Ok(expr);
                }
                if args.args.len() != 1 {
                    return Err(unsupported("scalar function arity/modifiers"));
                }
                let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) = &args.args[0] else {
                    return Err(unsupported("scalar argument"));
                };
                let mut input = self.expr(e)?;
                let ty = match name.as_str() {
                    "abs" => {
                        coerce_null(&mut input, &Type::Int);
                        if input.ty.scale().is_err() && input.ty != Type::Float {
                            return Err("ABS requires number".into());
                        }
                        input.ty.clone()
                    }
                    "factorial" => {
                        coerce_null(&mut input, &Type::Int);
                        if input.ty != Type::Int {
                            return Err(unsupported("FACTORIAL requires integer"));
                        }
                        Type::Int
                    }
                    "length" | "char_length" | "character_length" => {
                        coerce_null(&mut input, &Type::Text);
                        if input.ty != Type::Text {
                            return Err("LENGTH requires text".into());
                        }
                        Type::Int
                    }
                    "to_timestamp" => {
                        if !matches!(
                            input.ty,
                            Type::Int | Type::Decimal(_) | Type::Float | Type::Null
                        ) {
                            return Err(unsupported(
                                "TO_TIMESTAMP requires numeric seconds (the text/format form differs between references)",
                            ));
                        }
                        return Ok(Expr {
                            ty: Type::TimestampTz,
                            kind: Kind::Function("$to_timestamp".into(), Box::new(input)),
                        });
                    }
                    "lower" | "upper" => {
                        coerce_null(&mut input, &Type::Text);
                        if input.ty != Type::Text {
                            return Err(unsupported(format!(
                                "{} requires TEXT input",
                                name.to_uppercase()
                            )));
                        }
                        Type::Text
                    }
                    _ => return Err(unsupported(format!("scalar function {name}"))),
                };
                let name = match name.as_str() {
                    "char_length" | "character_length" => "length".to_string(),
                    _ => name,
                };
                Expr {
                    ty,
                    kind: Kind::Function(name, Box::new(input)),
                }
            }
            E::Position { expr, r#in } => {
                let mut a = self.expr(expr)?;
                let mut b = self.expr(r#in)?;
                coerce_null(&mut a, &Type::Text);
                coerce_null(&mut b, &Type::Text);
                if a.ty != Type::Text || b.ty != Type::Text {
                    return Err("POSITION requires text".into());
                }
                Expr {
                    ty: Type::Int,
                    kind: Kind::Position(Box::new(a), Box::new(b)),
                }
            }
            E::InList {
                expr,
                list,
                negated,
            } => {
                // Bind the left operand even for empty lists: unknown names and
                // invalid types remain errors. With no comparisons its runtime
                // expression is unused, so NULL IN () is false and NOT IN () true.
                let sources = std::iter::once(expr.as_ref())
                    .chain(list.iter())
                    .collect::<Vec<_>>();
                let mut inputs = self.comparison_inputs(&sources)?.into_iter();
                let input = inputs.next().unwrap();
                let mut terms = Vec::with_capacity(list.len() + 1);
                terms.push(literal(Type::Bool, Literal::Bool(false)));
                for (item, right) in list.iter().zip(inputs) {
                    let mut left = input.clone();
                    let mut right = right;
                    if left.ty == Type::Float && branch_types::integer_literal_syntax(item) {
                        branch_types::promote_float_literal(&mut right);
                    } else if right.ty == Type::Float && branch_types::integer_literal_syntax(expr)
                    {
                        branch_types::promote_float_literal(&mut left);
                    }
                    self.promoted_comparison(&mut left, &mut right);
                    terms.push(binary(Op::Eq, left, right)?);
                }
                let value = in_list_disjunction(terms)?;
                if *negated {
                    bool_expr(Kind::Not(Box::new(value)))
                } else {
                    value
                }
            }
            E::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } if !any && escape_char.is_none() => {
                let input = self.expr(expr)?;
                let pattern = self.expr(pattern)?;
                if input.ty != Type::Text || pattern.ty != Type::Text {
                    return Err("LIKE requires strings".into());
                }
                bool_expr(Kind::Like(Box::new(input), Box::new(pattern), *negated))
            }
            E::Like {
                negated,
                any: false,
                expr,
                pattern,
                escape_char: Some(escape),
            } => self.like(expr, pattern, *negated, false, Some(escape))?,
            E::ILike {
                negated,
                any: false,
                expr,
                pattern,
                escape_char,
            } => self.like(expr, pattern, *negated, true, escape_char.as_ref())?,
            E::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.similar_to(expr, pattern, *negated, escape_char.as_ref())?,
            E::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => self.trim(
                expr,
                trim_where.as_ref(),
                trim_what.as_deref(),
                trim_characters.as_ref(),
            )?,
            E::Floor {
                expr,
                field: sql::CeilFloorKind::DateTimeField(sql::DateTimeField::NoDateTime),
            } => self.floor_ceil(expr, crate::expr::math::Function::Floor, "floor")?,
            E::Ceil {
                expr,
                field: sql::CeilFloorKind::DateTimeField(sql::DateTimeField::NoDateTime),
            } => self.floor_ceil(expr, crate::expr::math::Function::Ceil, "ceil")?,
            E::Extract { field, expr, .. } => self.extract_date(&field.to_string(), expr)?,
            E::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let input = self.expr(expr)?;
                if input.ty != Type::Text {
                    return Err("SUBSTRING requires text".into());
                }
                let number = |v: &Option<Box<sql::Expr>>, default: i128| -> Result<i128> {
                    match v {
                        None => Ok(default),
                        Some(e) => match self.expr(e)?.kind {
                            Kind::Literal(Literal::Num(n)) => Ok(n),
                            _ => Err(unsupported("SUBSTRING requires constant bounds")),
                        },
                    }
                };
                let from = number(substring_from, 1)?;
                let len = number(substring_for, i64::MAX as i128)?;
                if from < 1 || len < 0 {
                    return Err(unsupported("SUBSTRING bounds"));
                }
                Expr {
                    ty: Type::Text,
                    kind: Kind::Substring(
                        Box::new(input),
                        usize::try_from(from - 1).map_err(|e| e.to_string())?,
                        usize::try_from(len).map_err(|e| e.to_string())?,
                    ),
                }
            }
            E::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut branches = Vec::new();
                let mut ty = Type::Null;
                for branch in conditions {
                    let condition = if let Some(base) = operand {
                        let mut a = self.expr(base)?;
                        let mut b = self.expr(&branch.condition)?;
                        self.promoted_comparison(&mut a, &mut b);
                        binary(Op::Eq, a, b)?
                    } else {
                        self.expr(&branch.condition)?
                    };
                    if condition.ty != Type::Bool && condition.ty != Type::Null {
                        return Err("CASE WHEN requires boolean".into());
                    }
                    let value = self.expr(&branch.result)?;
                    ty = common_type(&ty, &value.ty)?;
                    branches.push((
                        condition,
                        value,
                        branch_types::integer_literal_syntax(&branch.result),
                    ));
                }
                let other = else_result
                    .as_ref()
                    .map(|e| self.expr(e))
                    .transpose()?
                    .unwrap_or(literal(Type::Null, Literal::Null));
                ty = common_type(&ty, &other.ty)?;
                let _validated_type = ty;
                branch_types::case(
                    branches,
                    other,
                    else_result
                        .as_ref()
                        .is_some_and(|e| branch_types::integer_literal_syntax(e)),
                    self,
                )?
            }
            _ => return Err(unsupported(format!("expression {e}"))),
        })
    }
    /// The CAST arm of `expr_inner`, after its operand is bound: PostgreSQL/
    /// DuckDB cast dialect, with mixed Int/Float promotion rule E (a promoted
    /// value outside the 64-bit INTEGER range raises `INTEGER out of range`
    /// before the dialect conversion runs).
    #[inline(never)]
    fn cast(&self, value: Expr, data_type: &sql::DataType) -> Result<Expr> {
        let value = if matches!(
            data_type,
            sql::DataType::Integer(None) | sql::DataType::Int(None)
        ) {
            self.promoted_guard(value, crate::mixed_numeric::PROMOTED_INTEGER)
        } else {
            value
        };
        crate::expr::cast_dialect::bind(value, data_type)
    }
    /// The prefix-operator arm of `expr_inner`, after its operand is bound.
    #[inline(never)]
    fn unary(e: &sql::Expr, op: &sql::UnaryOperator, a: Expr) -> Result<Expr> {
        Ok(match op {
            sql::UnaryOperator::Minus
                if a.ty.scale().is_ok() || matches!(a.ty, Type::Float | Type::Null) =>
            {
                // A fractional zero literal is REAL in SQLite. Convert
                // its exact zero before negation so binary64 keeps -0.
                // Exact DECIMAL columns retain their unsigned-zero domain.
                let a = if matches!(a.ty, Type::Decimal(_))
                    && matches!(a.kind, Kind::Literal(Literal::Num(0)))
                {
                    crate::expr::casts::bind(a, Type::Float)?
                } else {
                    a
                };
                Expr {
                    ty: a.ty.clone(),
                    kind: Kind::Neg(Box::new(a)),
                }
            }
            // SQLite unary + preserves every supported scalar value.
            sql::UnaryOperator::Plus => a,
            sql::UnaryOperator::Not if a.ty == Type::Bool => bool_expr(Kind::Not(Box::new(a))),
            _ => return Err(unsupported(e)),
        })
    }
    fn column(&self, i: &sql::Ident) -> Result<Expr> {
        let name = ident(i);
        // USING/NATURAL inputs merged into one column are reachable only by
        // a qualified name (`t.k`); unqualified names find the merged column.
        let qualified = name.split_once('.').is_some_and(|(q, _)| !q.is_empty());
        let hits: Vec<_> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| qualified || !crate::rel::using_hidden(f))
            .filter(|(_, f)| {
                if i.quote_style.is_some() {
                    f.name() == &name
                        || (!name.contains('.')
                            && f.name().rsplit('.').next() == Some(name.as_str()))
                } else {
                    f.name().to_lowercase() == name
                        || (!name.contains('.')
                            && f.name().to_lowercase().rsplit('.').next() == Some(name.as_str()))
                }
            })
            .collect();
        if hits.len() != 1 {
            return Err(format!("unknown or ambiguous column: {name}").into());
        }
        let ty = Type::from_field(hits[0].1)?;
        if ty == Type::Null {
            // A physical Null column contains no non-NULL values. Bind it as
            // the existing literal representation so later NULL coercion does
            // not claim a numeric/boolean array while retaining NullArray.
            // The child relation still runs, preserving its rows and errors.
            return Ok(literal(Type::Null, Literal::Null));
        }
        Ok(Expr {
            kind: Kind::Column(hits[0].0),
            ty,
        })
    }
    /// `TIMESTAMP '...'` on the grammar both references share.
    fn timestamp(s: &str) -> Result<Expr> {
        Ok(literal(
            Type::Timestamp,
            Literal::Timestamp(crate::expr::timestamp::parse(s)?),
        ))
    }
    /// NOW()/CURRENT_TIMESTAMP: the statement's start instant (TIMESTAMPTZ);
    /// CURRENT_DATE and LOCALTIMESTAMP: its date/wall clock in the session
    /// zone. One value per statement, like both references in autocommit.
    fn clock(name: &str, args: &sql::FunctionArguments) -> Result<Expr> {
        let empty = match args {
            sql::FunctionArguments::None => name != "now",
            sql::FunctionArguments::List(list) => {
                list.args.is_empty() && list.clauses.is_empty() && name == "now"
            }
            _ => false,
        };
        if !empty {
            return Err(unsupported(format!(
                "{} arguments/precision",
                name.to_uppercase()
            )));
        }
        let statement = crate::zone::statement()?;
        Ok(match name {
            "current_date" => literal(
                Type::Date,
                Literal::Date(crate::zone::days(statement.zone.local(statement.now)?)),
            ),
            "localtimestamp" => literal(
                Type::Timestamp,
                Literal::Timestamp(crate::expr::timestamp::from_naive(
                    statement.zone.local(statement.now)?,
                )?),
            ),
            _ => literal(Type::TimestampTz, Literal::Timestamp(statement.now)),
        })
    }
    /// `TIMESTAMPTZ '...'`: offsets/zone names, else the session zone.
    fn timestamptz(s: &str) -> Result<Expr> {
        let zone = crate::zone::statement()?.zone;
        Ok(literal(
            Type::TimestampTz,
            Literal::Timestamp(crate::zone::parse_text(s, &zone)?),
        ))
    }
    /// `ts AT TIME ZONE 'Area/City'` (wall clock there -> TIMESTAMPTZ) and
    /// `tstz AT TIME ZONE 'Area/City'` (instant -> wall clock there).
    fn at_time_zone(value: Expr, zone: &sql::Expr) -> Result<Expr> {
        let mut zone = zone;
        while let sql::Expr::Nested(inner) = zone {
            zone = inner;
        }
        let sql::Expr::Value(sql::ValueWithSpan {
            value: sql::Value::SingleQuotedString(name),
            ..
        }) = zone
        else {
            return Err(unsupported(
                "AT TIME ZONE requires a string literal zone name",
            ));
        };
        let zone = crate::zone::SessionZone::parse(name)?;
        let (op, ty) = match value.ty {
            Type::Timestamp => ("from_ts", Type::TimestampTz),
            Type::TimestampTz => ("to_ts", Type::Timestamp),
            _ => {
                return Err(unsupported(
                    "AT TIME ZONE requires TIMESTAMP or TIMESTAMPTZ input",
                ));
            }
        };
        Ok(Expr {
            ty,
            kind: Kind::Function(crate::zone::function(op, &zone), Box::new(value)),
        })
    }
    /// Convert a TIMESTAMP/DATE operand to TIMESTAMPTZ in the session zone,
    /// as both references do for comparisons.
    fn to_timestamptz(value: Expr) -> Result<Expr> {
        let op = match value.ty {
            Type::TimestampTz | Type::Null => return Ok(value),
            Type::Timestamp => "from_ts",
            Type::Date => "from_date",
            _ => {
                return Err(unsupported(
                    "TIMESTAMPTZ comparison requires TIMESTAMPTZ, TIMESTAMP, DATE or a string literal",
                ));
            }
        };
        let zone = crate::zone::statement()?.zone;
        Ok(Expr {
            ty: Type::TimestampTz,
            kind: Kind::Function(crate::zone::function(op, &zone), Box::new(value)),
        })
    }
    fn date(s: &str) -> Result<Expr> {
        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| e.to_string())?;
        let days = d
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();
        Ok(literal(
            Type::Date,
            Literal::Date(i32::try_from(days).map_err(|e| e.to_string())?),
        ))
    }
    fn aggregate(&self, f: &sql::Function) -> Result<Agg> {
        if let Some(agg) = self.ordered_set_aggregate(f)? {
            return Ok(agg);
        }
        // FILTER is lowered to a conditional argument by the relational
        // planner before binding; any FILTER reaching this point is refused.
        if f.filter.is_some()
            || f.over.is_some()
            || f.null_treatment.is_some()
            || !f.within_group.is_empty()
            || f.uses_odbc_syntax
            || !matches!(f.parameters, sql::FunctionArguments::None)
        {
            return Err(unsupported("aggregate modifiers"));
        }
        let sql::FunctionArguments::List(args) = &f.args else {
            return Err(unsupported("aggregate arguments"));
        };
        if f.name.to_string().eq_ignore_ascii_case("string_agg") {
            return self.string_agg(args);
        }
        if let Some(agg) = self.statistical_aggregate(&f.name.to_string(), args)? {
            return Ok(agg);
        }
        if !args.clauses.is_empty() || args.args.len() != 1 {
            return Err(unsupported("aggregate DISTINCT/clauses/arity"));
        }
        let input = match &args.args[0] {
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Some(self.expr(e)?),
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard) => None,
            _ => return Err(unsupported("named aggregate arguments")),
        };
        Agg::bind_input(
            &f.name.to_string(),
            input,
            args.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct),
        )
    }

    /// STRING_AGG(text, 'separator' ORDER BY key [ASC|DESC] [NULLS ...], ...).
    /// Only the form on which DuckDB 1.5.6 and PostgreSQL agree is accepted:
    /// TEXT input, a non-NULL string literal separator and an explicit,
    /// fully specified ORDER BY. Ties between different values are refused at
    /// finalization because their relative order is unspecified.
    fn string_agg(&self, args: &sql::FunctionArgumentList) -> Result<Agg> {
        let distinct = match args.duplicate_treatment {
            None | Some(sql::DuplicateTreatment::All) => false,
            Some(sql::DuplicateTreatment::Distinct) => true,
        };
        let mut order = None;
        for clause in &args.clauses {
            match clause {
                sql::FunctionArgumentClause::OrderBy(keys) if order.is_none() => order = Some(keys),
                _ => return Err(unsupported("STRING_AGG clause")),
            }
        }
        fn expression(argument: &sql::FunctionArg) -> Result<&sql::Expr> {
            match argument {
                sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Ok(e),
                _ => Err(unsupported(
                    "STRING_AGG arguments must be unnamed expressions",
                )),
            }
        }
        let (value_sql, separator) = match args.args.as_slice() {
            [_] => return Err(unsupported("STRING_AGG requires an explicit separator")),
            [value, separator] => (expression(value)?, expression(separator)?),
            _ => return Err("STRING_AGG requires two arguments".into()),
        };
        let separator = match separator {
            sql::Expr::Value(sql::ValueWithSpan {
                value: sql::Value::SingleQuotedString(text),
                ..
            }) => text.clone(),
            _ => {
                return Err(unsupported(
                    "STRING_AGG separator must be a non-NULL string literal",
                ));
            }
        };
        let value = self.expr(value_sql)?;
        if !matches!(value.ty, Type::Text | Type::Null) {
            return Err(unsupported(format!(
                "STRING_AGG requires TEXT input, got {:?}",
                value.ty
            )));
        }
        let Some(order) = order else {
            return Err(unsupported(
                "STRING_AGG without ORDER BY (result order is unspecified)",
            ));
        };
        let mut keys = Vec::with_capacity(order.len());
        for key in order {
            if key.with_fill.is_some() {
                return Err(unsupported("STRING_AGG ORDER BY fill"));
            }
            let descending = key.options.asc == Some(false);
            let nulls_first = match key.options.nulls_first {
                Some(first) => first,
                None if descending => {
                    return Err(unsupported(
                        "STRING_AGG ORDER BY DESC requires explicit NULLS FIRST or NULLS LAST",
                    ));
                }
                None => false,
            };
            let bound = self.expr(&key.expr)?;
            if !matches!(
                bound.ty,
                Type::Int
                    | Type::Decimal(_)
                    | Type::Float
                    | Type::Text
                    | Type::Date
                    | Type::Bool
                    | Type::Null
            ) {
                return Err(unsupported(format!(
                    "STRING_AGG ORDER BY {:?} key",
                    bound.ty
                )));
            }
            keys.push((bound, descending, nulls_first));
        }
        if distinct && !(keys.len() == 1 && keys[0].0 == value) {
            return Err(unsupported(
                "STRING_AGG DISTINCT requires ORDER BY the aggregated argument",
            ));
        }
        Ok(Agg {
            function: Aggregate::StringAgg(separator),
            input: Some(Expr {
                ty: Type::Binary,
                kind: Kind::SortPack(keys, Box::new(value)),
            }),
            ty: Type::Text,
            distinct,
        })
    }
}

impl Agg {
    /// Shared type/result policy for grouped and window aggregates.
    pub(crate) fn bind_input(name: &str, input: Option<Expr>, mut distinct: bool) -> Result<Self> {
        let (function, ty) = match name.to_lowercase().as_str() {
            "count" => (Aggregate::Count, Type::Int),
            "min" | "max" => {
                let value = input.as_ref().ok_or("MIN/MAX require an expression")?;
                if value.ty == Type::Binary {
                    return Err(unsupported("binary MIN/MAX"));
                }
                (
                    if name.eq_ignore_ascii_case("min") {
                        Aggregate::Min
                    } else {
                        Aggregate::Max
                    },
                    value.ty.clone(),
                )
            }
            "sum" | "avg" => {
                let value = input.as_ref().ok_or("SUM/AVG require an expression")?;
                if value.ty == Type::Bool {
                    return Err(unsupported(format!(
                        "expected numeric type, got {:?}",
                        value.ty
                    )));
                }
                if value.ty == Type::Float {
                    if distinct
                        && branch_types::constant(value)
                            .is_some_and(|e| matches!(e.kind, Kind::Literal(Literal::Null)))
                    {
                        // A total constant NULL contributes no values. DISTINCT
                        // and ordinary accumulation have the same empty fold;
                        // retain the typed input and its normal evaluation.
                        distinct = false;
                    }
                    // Other Float DISTINCT inputs follow DuckDB value identity
                    // (-0.0 equals 0.0, every NaN is one value) and add each
                    // distinct value once, at its first occurrence in canonical
                    // input order (SEMANTICS-LOG, October 16).
                } else if value.ty != Type::Null {
                    // Text/Date and other non-numeric inputs remain genuine type errors.
                    value.ty.scale()?;
                }
                if name.eq_ignore_ascii_case("sum") {
                    (Aggregate::Sum, value.ty.clone())
                } else {
                    (
                        Aggregate::Avg,
                        if value.ty == Type::Null {
                            Type::Null
                        } else {
                            Type::Float
                        },
                    )
                }
            }
            other => {
                if let Some(agg) = statistics::bind_one(other, input.clone(), distinct)? {
                    return Ok(agg);
                }
                return Err(unsupported(format!("function {}", name)));
            }
        };
        Ok(Agg {
            function,
            input,
            ty,
            distinct,
        })
    }
}

pub fn parse(sql: &str) -> Result<sql::Query> {
    if sql.len() > 64 * 1024 {
        return Err(unsupported("SQL >64 KiB"));
    }
    let mut statements = crate::nesting::parse(sql).map_err(|e| {
        if e == crate::nesting::REFUSAL || e == crate::nesting::HEIGHT_REFUSAL {
            unsupported(e)
        } else {
            crate::Error::from(e)
        }
    })?;
    if statements.len() != 1 {
        return Err(unsupported("exactly one SELECT required"));
    }
    match statements.remove(0) {
        sql::Statement::Query(q) => Ok(*q),
        _ => Err(unsupported("only SELECT")),
    }
}
pub fn table_name(q: &sql::Query) -> Result<Option<String>> {
    let sql::SetExpr::Select(s) = q.body.as_ref() else {
        return Err(unsupported("set operations/subqueries"));
    };
    if s.from.is_empty() {
        return Ok(None);
    }
    if s.from.len() != 1 || !s.from[0].joins.is_empty() {
        return Err(unsupported("joins/multiple tables"));
    }
    match &s.from[0].relation {
        sql::TableFactor::Table { name, .. }
            if s.from[0].relation.to_string() == name.to_string() =>
        {
            let name = name.to_string();
            if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(unsupported("qualified/quoted table names"));
            }
            Ok(Some(name.to_lowercase()))
        }
        _ => Err(unsupported("table expressions/aliases")),
    }
}
pub(crate) fn has_wildcards(items: &[sql::SelectItem]) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            sql::SelectItem::Wildcard(_) | sql::SelectItem::QualifiedWildcard(_, _)
        )
    })
}

/// Expand in logical FROM order, before join reordering or hidden marker columns.
/// Exact quoted internal field references avoid ambiguity between joined inputs.
pub(crate) fn expand_wildcards(
    items: &[sql::SelectItem],
    schema: &Schema,
    table: Option<&str>,
) -> Result<Vec<sql::SelectItem>> {
    let mut expanded = Vec::new();
    for item in items {
        let (qualifier, options) = match item {
            sql::SelectItem::Wildcard(options) => (None, options),
            sql::SelectItem::QualifiedWildcard(
                sql::SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) => {
                if name.0.len() != 1 {
                    return Err(unsupported("qualified wildcard namespace"));
                }
                let sql::ObjectNamePart::Identifier(id) = &name.0[0] else {
                    return Err(unsupported("wildcard namespace expression"));
                };
                (Some(ident(id)), options)
            }
            sql::SelectItem::QualifiedWildcard(_, _) => {
                return Err(unsupported("expression wildcard"));
            }
            _ => {
                expanded.push(item.clone());
                continue;
            }
        };
        if options.opt_ilike.is_some()
            || options.opt_exclude.is_some()
            || options.opt_except.is_some()
            || options.opt_replace.is_some()
            || options.opt_rename.is_some()
        {
            return Err(unsupported("wildcard modifiers"));
        }
        let start = expanded.len();
        if qualifier.is_none()
            && schema
                .fields()
                .iter()
                .any(|f| crate::rel::using_star_conflict(f))
        {
            return Err(unsupported(
                "SELECT * over a USING/NATURAL join whose USING columns are not the leading left-side columns (DuckDB and PostgreSQL order these columns differently); list the columns",
            ));
        }
        for field in schema.fields() {
            // Merged-away USING inputs appear only in their own `t.*`.
            if qualifier.is_none() && crate::rel::using_hidden(field) {
                continue;
            }
            let (field_qualifier, output_name) = match table {
                Some(table) => (Some(table), field.name().as_str()),
                None => field
                    .name()
                    .rsplit_once('.')
                    .map_or((None, field.name().as_str()), |(q, n)| (Some(q), n)),
            };
            if qualifier
                .as_deref()
                .is_some_and(|q| Some(q) != field_qualifier)
            {
                continue;
            }
            expanded.push(sql::SelectItem::ExprWithAlias {
                expr: sql::Expr::Identifier(sql::Ident::with_quote('"', field.name())),
                alias: sql::Ident::with_quote(
                    '"',
                    field
                        .metadata()
                        .get(Type::SERIES_WILDCARD)
                        .map_or(output_name, String::as_str),
                ),
            });
        }
        if expanded.len() == start {
            return Err("wildcard has no matching input columns".into());
        }
    }
    Ok(expanded)
}

pub fn bind(
    q: &sql::Query,
    schema: &Schema,
    path: Option<PathBuf>,
    pushdown: bool,
) -> Result<Plan> {
    // The relational planner resolves these before constructing its internal
    // aggregate projection. Only the public, direct binder needs this step.
    if let sql::SetExpr::Select(select) = q.body.as_ref()
        && let sql::GroupByExpr::Expressions(groups, _) = &select.group_by
        && groups.iter().any(|e| group_position(e).is_some())
    {
        let mut q = q.clone();
        let table = table_name(&q)?;
        let sql::SetExpr::Select(select) = q.body.as_mut() else {
            unreachable!()
        };
        select.projection = expand_wildcards(&select.projection, schema, table.as_deref())?;
        resolve_group_positions(select)?;
        return bind_output(&q, schema, path, pushdown, None);
    }
    bind_output(q, schema, path, pushdown, None)
}

fn group_position(e: &sql::Expr) -> Option<Result<usize>> {
    match e {
        sql::Expr::Nested(e) => group_position(e),
        sql::Expr::Value(v) => Some(match &v.value {
            sql::Value::Number(n, _) if n.bytes().all(|b| b.is_ascii_digit()) => n
                .parse::<usize>()
                .map_err(|_| "GROUP BY ordinal out of range".into()),
            _ => Err(unsupported("GROUP BY non-integer literals")),
        }),
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Minus,
            expr,
        } => {
            let mut operand = expr.as_ref();
            while let sql::Expr::Nested(inner) = operand {
                operand = inner;
            }
            if matches!(operand, sql::Expr::Value(v)
                if matches!(&v.value, sql::Value::Number(n, _) if n.bytes().all(|b| b.is_ascii_digit())))
            {
                Some(Err("GROUP BY ordinal out of range".into()))
            } else {
                // Preserve existing unary expressions (in particular -NULL).
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn resolve_group_positions(select: &mut sql::Select) -> Result<()> {
    let sql::GroupByExpr::Expressions(groups, _) = &mut select.group_by else {
        return Ok(());
    };
    for group in groups {
        let Some(position) = group_position(group) else {
            continue;
        };
        let index = position?
            .checked_sub(1)
            .ok_or("GROUP BY ordinal out of range")?;
        let expression = match select.projection.get(index) {
            Some(
                sql::SelectItem::UnnamedExpr(e) | sql::SelectItem::ExprWithAlias { expr: e, .. },
            ) => e,
            _ => return Err("GROUP BY ordinal out of range".into()),
        };
        crate::syntax::rewrite(&mut expression.clone(), &mut |e| {
            if crate::syntax::is_aggregate(e) {
                return Err("GROUP BY ordinal refers to an aggregate expression".into());
            }
            if matches!(e, sql::Expr::Function(f) if f.over.is_some()) {
                return Err("GROUP BY ordinal refers to a window expression".into());
            }
            Ok(false)
        })?;
        *group = expression.clone();
    }
    Ok(())
}

/// Internal binding with private producer facts; public Null columns remain supported.
pub(crate) fn bind_with_facts(
    q: &sql::Query,
    schema: &Schema,
    facts: &[crate::mixed_numeric::Fact],
) -> Result<Plan> {
    bind_output(q, schema, None, true, Some(facts))
}

fn bind_output(
    q: &sql::Query,
    schema: &Schema,
    path: Option<PathBuf>,
    pushdown: bool,
    facts: Option<&[crate::mixed_numeric::Fact]>,
) -> Result<Plan> {
    let table = table_name(q)?;
    let b = Binder {
        schema,
        table: table.as_deref(),
        facts,
    };
    if q.with.is_some()
        || q.limit_clause.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return Err(unsupported("query modifiers/CTE/LIMIT"));
    }
    let sql::SetExpr::Select(s) = q.body.as_ref() else {
        unreachable!()
    };
    if !s.named_window.is_empty() {
        return Err(unsupported("named WINDOW clauses"));
    }
    if s.qualify.is_some() {
        return Err(unsupported("QUALIFY"));
    }
    if s.distinct.is_some()
        || s.top.is_some()
        || s.exclude.is_some()
        || s.into.is_some()
        || !s.lateral_views.is_empty()
        || s.prewhere.is_some()
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || s.having.is_some()
        || !s.named_window.is_empty()
        || s.qualify.is_some()
        || s.value_table_mode.is_some()
        || s.connect_by.is_some()
        || s.flavor != sql::SelectFlavor::Standard
    {
        return Err(unsupported("SELECT modifiers"));
    }
    let mut filter = s.selection.as_ref().map(|e| b.expr(e)).transpose()?;
    if let Some(f) = &mut filter {
        coerce_null(f, &Type::Bool);
        if f.ty != Type::Bool {
            return Err("WHERE requires boolean".into());
        }
    }
    let groups = match &s.group_by {
        sql::GroupByExpr::Expressions(e, mods) if mods.is_empty() => {
            e.iter().map(|e| b.expr(e)).collect::<Result<Vec<_>>>()?
        }
        _ => return Err(unsupported("GROUP BY modifiers")),
    };
    let mut aggregates = Vec::new();
    let mut projected = Vec::new();
    let mut names = Vec::new();
    let mut types = Vec::new();
    let expanded;
    let projection = if has_wildcards(&s.projection) {
        expanded = expand_wildcards(&s.projection, schema, table.as_deref())?;
        &expanded
    } else {
        &s.projection
    };
    for item in projection {
        let (e, name) = match item {
            sql::SelectItem::ExprWithAlias { expr, alias } => (expr, ident(alias)),
            sql::SelectItem::UnnamedExpr(e) => {
                let generated_name = if matches!(
                    e,
                    sql::Expr::Identifier(_) | sql::Expr::CompoundIdentifier(_)
                ) {
                    b.expr(e).ok().and_then(|bound| match bound.kind {
                        Kind::Column(column)
                            if schema
                                .field(column)
                                .metadata()
                                .contains_key(Type::UNQUALIFIED_OUTPUT) =>
                        {
                            Some(
                                schema
                                    .field(column)
                                    .name()
                                    .rsplit('.')
                                    .next()
                                    .unwrap()
                                    .to_owned(),
                            )
                        }
                        _ => None,
                    })
                } else {
                    None
                };
                (e, generated_name.unwrap_or_else(|| e.to_string()))
            }
            _ => return Err(unsupported("wildcard projection")),
        };
        names.push(name);
        if let sql::Expr::Function(f) = e
            && f.over.is_none()
            && crate::syntax::AGGREGATE_NAMES.contains(&f.name.to_string().to_lowercase().as_str())
        {
            let agg = b.aggregate(f)?;
            types.push(agg.ty.clone());
            projected.push(Output::Aggregate(aggregates.len()));
            aggregates.push(agg);
        } else {
            let e = b.expr(e)?;
            types.push(e.ty.clone());
            projected.push(Output::Expr(e));
        }
    }
    let aggregation = !groups.is_empty() || !aggregates.is_empty();
    if aggregation {
        for output in &mut projected {
            if let Output::Expr(e) = output {
                let i = groups
                    .iter()
                    .position(|g| g == e)
                    .ok_or("nonaggregate output must match a GROUP BY expression")?;
                *output = Output::Group(i);
            }
        }
    }
    let mut order = Vec::new();
    if let Some(sort) = &q.order_by {
        if sort.interpolate.is_some() {
            return Err(unsupported("ORDER BY interpolate"));
        }
        let sql::OrderByKind::Expressions(keys) = &sort.kind else {
            return Err(unsupported("ORDER BY ALL"));
        };
        for key in keys {
            if key.with_fill.is_some() {
                return Err(unsupported("ORDER BY fill"));
            }
            let index = match &key.expr {
                sql::Expr::Identifier(i) => {
                    let name = ident(i);
                    let hits: Vec<_> = names
                        .iter()
                        .enumerate()
                        .filter(|(_, n)| **n == name)
                        .collect();
                    if hits.len() != 1 {
                        return Err("ORDER BY name must identify one output column".into());
                    }
                    hits[0].0
                }
                sql::Expr::Value(v) => match &v.value {
                    sql::Value::Number(s, _) => s
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| i.checked_sub(1))
                        .filter(|i| *i < names.len())
                        .ok_or("ORDER BY ordinal out of range")?,
                    _ => return Err(unsupported("ORDER BY expression")),
                },
                _ => return Err(unsupported("ORDER BY must name an output or ordinal")),
            };
            order.push(Sort {
                column: index,
                descending: key.options.asc == Some(false),
                nulls_first: key.options.nulls_first.unwrap_or(false),
            });
        }
    }
    let mut columns = BTreeSet::new();
    if let Some(f) = &filter {
        f.columns(&mut columns);
    }
    for e in &groups {
        e.columns(&mut columns);
    }
    for a in &aggregates {
        if let Some(e) = &a.input {
            e.columns(&mut columns);
        }
    }
    for o in &projected {
        if let Output::Expr(e) = o {
            e.columns(&mut columns);
        }
    }
    // Read one real column for COUNT(*) so row counts come from the reader.
    if table.is_some() && columns.is_empty() && !schema.fields().is_empty() {
        columns.insert(0);
    }
    if !pushdown {
        columns.extend(0..schema.fields().len());
    }
    Ok(Plan {
        table,
        path,
        columns: columns.into_iter().collect(),
        filter,
        groups,
        aggregates,
        output: projected,
        names,
        types,
        order,
        aggregation,
        projection_pushdown: pushdown,
    })
}

pub(crate) fn common_type(a: &Type, b: &Type) -> Result<Type> {
    if a == b || *b == Type::Null {
        return Ok(a.clone());
    }
    if *a == Type::Null {
        return Ok(b.clone());
    }
    if (a.scale().is_ok() || *a == Type::Float) && (b.scale().is_ok() || *b == Type::Float) {
        if *a == Type::Float || *b == Type::Float {
            return Ok(Type::Float);
        }
        return Ok(Type::Decimal(a.scale()?.max(b.scale()?)));
    }
    if matches!(
        (a, b),
        (
            Type::Date | Type::Timestamp | Type::TimestampTz,
            Type::Date | Type::Timestamp | Type::TimestampTz
        )
    ) {
        // Both references widen DATE to TIMESTAMP here; Assay does not yet.
        return Err(unsupported("mixed DATE/TIMESTAMP/TIMESTAMPTZ result types"));
    }
    Err("incompatible CASE result types".into())
}
