//! Syntax binding and a deliberately small, explicit physical plan.
use crate::{
    Result,
    expr::{Expr, Kind, Literal, Op},
    types::{Type, decimal_literal, rescale},
};
use arrow::datatypes::Schema;
use chrono::NaiveDate;
use serde::Serialize;
use sqlparser::{ast as sql, parser::Parser};
use std::{collections::BTreeSet, path::PathBuf};
pub(crate) mod branch_types;

#[derive(Debug, Clone, Serialize)]
pub enum Aggregate {
    Sum,
    Count,
    Avg,
    Min,
    Max,
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
fn unsupported(s: impl std::fmt::Display) -> String {
    format!("unsupported: {s}")
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
            if !matches!(a.ty, Type::Int | Type::Float) || !matches!(b.ty, Type::Int | Type::Float)
            {
                return Err(unsupported(
                    "remainder requires INTEGER/REAL inputs; exact DECIMAL remainder is not defined",
                ));
            }
            if a.ty == Type::Float || b.ty == Type::Float {
                Type::Float
            } else {
                Type::Int
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
        Op::Add | Op::Sub | Op::Mul => {
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
pub(crate) struct Binder<'a> {
    pub schema: &'a Schema,
    pub table: Option<&'a str>,
    pub facts: Option<&'a [crate::mixed_numeric::Fact]>,
}
impl Binder<'_> {
    pub(crate) fn expr(&self, e: &sql::Expr) -> Result<Expr> {
        let bound = self.expr_inner(e)?;
        crate::mixed_numeric::normalize_expression(bound, self.schema, self.facts)
    }
    fn expr_inner(&self, e: &sql::Expr) -> Result<Expr> {
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
            E::Nested(e) => self.expr(e)?,
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
            E::Cast {
                kind,
                expr,
                data_type,
                format,
            } if matches!(kind, sql::CastKind::Cast | sql::CastKind::DoubleColon)
                && format.is_none() =>
            {
                let value = self.expr(expr)?;
                // The approved SQLite-style targets are separate from the
                // existing strict BIGINT/DECIMAL/DATE/BOOLEAN cast contracts.
                let sqlite_target = match data_type {
                    sql::DataType::Integer(None) | sql::DataType::Int(None) => Some(Type::Int),
                    sql::DataType::Real => Some(Type::Float),
                    sql::DataType::Text => Some(Type::Text),
                    _ => None,
                };
                if let Some(ty) = sqlite_target {
                    return crate::expr::casts::bind(value, ty);
                }
                if let sql::DataType::Decimal(info)
                | sql::DataType::Numeric(info)
                | sql::DataType::Dec(info) = data_type
                {
                    let sql::ExactNumberInfo::PrecisionAndScale(p, s) = info else {
                        return Err(unsupported(format!("CAST AS {data_type}")));
                    };
                    if !(1..=38).contains(p) || *s > 18 || s > p {
                        return Err(unsupported(format!("CAST AS {data_type}")));
                    }
                    let ty = Type::Decimal(*s as i8);
                    // Retain the existing lossless literal-text contract.
                    if let Kind::Literal(Literal::Text(text)) = &value.kind {
                        let (n, old) = decimal_literal(text)?;
                        let n = rescale(n, old, *s as i8)?;
                        if n.unsigned_abs() >= crate::types::factor(*p as i8)? as u128 {
                            return Err("decimal cast precision overflow".into());
                        }
                        return Ok(literal(ty, Literal::Num(n)));
                    }
                    if !matches!(value.ty, Type::Int | Type::Decimal(_) | Type::Null) {
                        return Err(unsupported("DECIMAL cast requires an exact numeric source"));
                    }
                    return Ok(Expr {
                        kind: Kind::CastDecimal(Box::new(value), *p as u8, *s as i8),
                        ty,
                    });
                }
                let ty = match data_type {
                    sql::DataType::Date => Type::Date,
                    sql::DataType::BigInt(None) => Type::Int,
                    sql::DataType::Boolean => Type::Bool,
                    _ => return Err(unsupported(format!("CAST AS {data_type}"))),
                };
                match (&value.kind, &ty) {
                    (Kind::Literal(Literal::Null), _) => literal(ty, Literal::Null),
                    (Kind::Literal(Literal::Text(s)), Type::Date) => Self::date(s)?,
                    _ if value.ty == ty && !matches!(ty, Type::Decimal(_)) => value,
                    _ => return Err(unsupported("only lossless literal casts in M1")),
                }
            }
            E::BinaryOp { left, op, right } => {
                let op = match op {
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
                };
                binary(op, self.expr(left)?, self.expr(right)?)?
            }
            E::UnaryOp { op, expr } => {
                let a = self.expr(expr)?;
                match op {
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
                    sql::UnaryOperator::Not if a.ty == Type::Bool => {
                        bool_expr(Kind::Not(Box::new(a)))
                    }
                    _ => return Err(unsupported(e)),
                }
            }
            E::IsNull(a) => bool_expr(Kind::IsNull(Box::new(self.expr(a)?), false)),
            E::IsNotNull(a) => bool_expr(Kind::IsNull(Box::new(self.expr(a)?), true)),
            E::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let a = self.expr(expr)?;
                let bound = binary(
                    Op::And,
                    binary(Op::Ge, a.clone(), self.expr(low)?)?,
                    binary(Op::Le, a, self.expr(high)?)?,
                )?;
                if *negated {
                    bool_expr(Kind::Not(Box::new(bound)))
                } else {
                    bound
                }
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
                let sql::FunctionArguments::List(args) = &f.args else {
                    return Err(unsupported("function arguments"));
                };
                if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
                    return Err(unsupported("scalar function arity/modifiers"));
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
                        values.push(self.expr(value)?);
                    }
                    let right = values.pop().unwrap();
                    let left = values.pop().unwrap();
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
                        let value = self.expr(value)?;
                        ty = common_type(&ty, &value.ty)?;
                        values.push(value);
                    }
                    return branch_types::coalesce(values);
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
                    "length" => {
                        coerce_null(&mut input, &Type::Text);
                        if input.ty != Type::Text {
                            return Err("LENGTH requires text".into());
                        }
                        Type::Int
                    }
                    _ => return Err(unsupported(format!("scalar function {name}"))),
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
                let input = self.expr(expr)?;
                let mut value = literal(Type::Bool, Literal::Bool(false));
                for item in list {
                    value = binary(
                        Op::Or,
                        value,
                        binary(Op::Eq, input.clone(), self.expr(item)?)?,
                    )?;
                }
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
            E::Extract {
                field: sql::DateTimeField::Year,
                expr,
                ..
            } => {
                let input = self.expr(expr)?;
                if input.ty != Type::Date {
                    return Err("EXTRACT YEAR requires a date".into());
                }
                Expr {
                    ty: Type::Int,
                    kind: Kind::Year(Box::new(input)),
                }
            }
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
                        binary(Op::Eq, self.expr(base)?, self.expr(&branch.condition)?)?
                    } else {
                        self.expr(&branch.condition)?
                    };
                    if condition.ty != Type::Bool && condition.ty != Type::Null {
                        return Err("CASE WHEN requires boolean".into());
                    }
                    let value = self.expr(&branch.result)?;
                    ty = common_type(&ty, &value.ty)?;
                    branches.push((condition, value));
                }
                let other = else_result
                    .as_ref()
                    .map(|e| self.expr(e))
                    .transpose()?
                    .unwrap_or(literal(Type::Null, Literal::Null));
                ty = common_type(&ty, &other.ty)?;
                let _validated_type = ty;
                branch_types::case(branches, other)?
            }
            _ => return Err(unsupported(format!("expression {e}"))),
        })
    }
    fn column(&self, i: &sql::Ident) -> Result<Expr> {
        let name = ident(i);
        let hits: Vec<_> = self
            .schema
            .fields()
            .iter()
            .enumerate()
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
            return Err(format!("unknown or ambiguous column: {name}"));
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
        if !args.clauses.is_empty() || args.args.len() != 1 {
            return Err(unsupported("aggregate DISTINCT/clauses/arity"));
        }
        let input = match &args.args[0] {
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Some(self.expr(e)?),
            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard) => None,
            _ => return Err(unsupported("named aggregate arguments")),
        };
        let (function, ty) = match f.name.to_string().to_lowercase().as_str() {
            "count" => (Aggregate::Count, Type::Int),
            "min" | "max" => {
                let value = input.as_ref().ok_or("MIN/MAX require an expression")?;
                if value.ty == Type::Binary {
                    return Err(unsupported("binary MIN/MAX"));
                }
                (
                    if f.name.to_string().eq_ignore_ascii_case("min") {
                        Aggregate::Min
                    } else {
                        Aggregate::Max
                    },
                    value.ty.clone(),
                )
            }
            "sum" | "avg" => {
                let value = input.as_ref().ok_or("SUM/AVG require an expression")?;
                if value.ty != Type::Null {
                    value.ty.scale()?;
                }
                if f.name.to_string().eq_ignore_ascii_case("sum") {
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
            _ => return Err(unsupported(format!("function {}", f.name))),
        };
        Ok(Agg {
            function,
            input,
            ty,
            distinct: args.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct),
        })
    }
}

pub fn parse(sql: &str) -> Result<sql::Query> {
    if sql.len() > 64 * 1024 {
        return Err(unsupported("SQL >64 KiB"));
    }
    let mut statements =
        Parser::parse_sql(&crate::dialect::QueryDialect, sql).map_err(|e| e.to_string())?;
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
        for field in schema.fields() {
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
                alias: sql::Ident::with_quote('"', output_name),
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
    bind_output(q, schema, path, pushdown, None)
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
        sql::GroupByExpr::Expressions(e, mods) if mods.is_empty() => e
            .iter()
            .map(|e| {
                if matches!(e, sql::Expr::Value(_)) {
                    return Err(unsupported("GROUP BY literals/ordinals"));
                }
                b.expr(e)
            })
            .collect::<Result<Vec<_>>>()?,
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
            sql::SelectItem::UnnamedExpr(e) => (e, e.to_string()),
            _ => return Err(unsupported("wildcard projection")),
        };
        names.push(name);
        if let sql::Expr::Function(f) = e
            && f.over.is_none()
            && ["count", "sum", "avg", "min", "max"]
                .contains(&f.name.to_string().to_lowercase().as_str())
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
    Err("incompatible CASE result types".into())
}
