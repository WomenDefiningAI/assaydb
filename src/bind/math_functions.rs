//! Scalar math function binding. Result types follow DuckDB 1.5.6 `typeof`:
//! the binary64 functions return DOUBLE for every numeric input (DECIMAL too:
//! PostgreSQL's NUMERIC overloads compute exactly, DuckDB converts, the same
//! split as AVG(decimal), which follows DuckDB); FLOOR/CEIL return DOUBLE for
//! INTEGER (both references) and DECIMAL(p,0) for DECIMAL; TRUNC keeps INTEGER;
//! SIGN returns INTEGER (DuckDB TINYINT). Evaluation is in `expr::math`.
use super::*;
use crate::expr::math::Function as F;

fn argument<'a>(argument: &'a sql::FunctionArg, name: &str) -> Result<&'a sql::Expr> {
    match argument {
        sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => Ok(e),
        _ => Err(unsupported(format!(
            "{} requires unnamed arguments",
            name.to_uppercase()
        ))),
    }
}

fn numeric(mut e: Expr, null_type: &Type, name: &str) -> Result<Expr> {
    coerce_null(&mut e, null_type);
    if e.ty.scale().is_err() && e.ty != Type::Float {
        return Err(unsupported(format!(
            "{} requires a numeric argument",
            name.to_uppercase()
        )));
    }
    Ok(e)
}

/// An Int argument converted to DOUBLE like a mixed Int/Float conditional arm
/// (October 15 decision): exact integer literals stay exact Float literals;
/// every other Int is marked so consumers see promoted provenance.
pub(super) fn promote(mut e: Expr) -> Expr {
    if e.ty == Type::Int {
        branch_types::promote_float_literal(&mut e);
    }
    if e.ty == Type::Int {
        e = Expr {
            ty: Type::Float,
            kind: Kind::Function(crate::mixed_numeric::PROMOTE.into(), Box::new(e)),
        };
    }
    e
}

fn call(function: F, ty: Type, args: Vec<Expr>) -> Expr {
    Expr {
        ty,
        kind: Kind::Math(function, args),
    }
}

impl Binder<'_> {
    /// `None` when `name` is not a math function handled here.
    pub(super) fn math_function(
        &self,
        name: &str,
        args: &[sql::FunctionArg],
    ) -> Result<Option<Expr>> {
        let arity = |wanted: &[usize]| -> Result<()> {
            if wanted.contains(&args.len()) {
                Ok(())
            } else {
                let words = wanted
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" or ");
                Err(format!("{} requires {words} argument(s)", name.to_uppercase()).into())
            }
        };
        let float = match name {
            "sqrt" => Some(F::Sqrt),
            "cbrt" => Some(F::Cbrt),
            "exp" => Some(F::Exp),
            "ln" => Some(F::Ln),
            "log10" => Some(F::Log10),
            "log2" => Some(F::Log2),
            "log" if args.len() != 2 => Some(F::Log10),
            "sin" => Some(F::Sin),
            "cos" => Some(F::Cos),
            "tan" => Some(F::Tan),
            "asin" => Some(F::Asin),
            "acos" => Some(F::Acos),
            "atan" => Some(F::Atan),
            "radians" => Some(F::Radians),
            _ => None,
        };
        if let Some(function) = float {
            arity(if name == "log" { &[1, 2] } else { &[1] })?;
            let value = numeric(self.expr(argument(&args[0], name)?)?, &Type::Float, name)?;
            return Ok(Some(call(function, Type::Float, vec![value])));
        }
        let pair = match name {
            "power" | "pow" => Some(F::Power),
            "atan2" => Some(F::Atan2),
            "log" => Some(F::LogBase),
            _ => None,
        };
        if let Some(function) = pair {
            arity(&[2])?;
            let values = args
                .iter()
                .map(|a| numeric(self.expr(argument(a, name)?)?, &Type::Float, name))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(call(function, Type::Float, values)));
        }
        Ok(Some(match name {
            "pi" => {
                arity(&[0])?;
                call(F::Pi, Type::Float, Vec::new())
            }
            "floor" | "ceil" | "ceiling" => {
                arity(&[1])?;
                let function = if name == "floor" { F::Floor } else { F::Ceil };
                return self
                    .floor_ceil(argument(&args[0], name)?, function, name)
                    .map(Some);
            }
            "trunc" => {
                arity(&[1, 2])?;
                let value = numeric(self.expr(argument(&args[0], name)?)?, &Type::Int, name)?;
                let digits = if args.len() == 1 {
                    Some(0)
                } else {
                    self.literal_digits(argument(&args[1], name)?, "TRUNC")?
                };
                let ty = match value.ty {
                    Type::Decimal(scale) => {
                        Type::Decimal(digits.map_or(scale, |n| n.clamp(0, i32::from(scale)) as i8))
                    }
                    _ => value.ty.clone(),
                };
                call(F::Trunc(digits), ty, vec![value])
            }
            "sign" => {
                arity(&[1])?;
                let value = numeric(self.expr(argument(&args[0], name)?)?, &Type::Int, name)?;
                call(F::Sign, Type::Int, vec![value])
            }
            "greatest" | "least" => {
                if args.is_empty() {
                    return Err(
                        format!("{} requires at least one argument", name.to_uppercase()).into(),
                    );
                }
                let sources = args
                    .iter()
                    .map(|a| argument(a, name))
                    .collect::<Result<Vec<_>>>()?;
                let values = self.comparison_inputs(&sources)?;
                let function = if name == "greatest" {
                    F::Greatest
                } else {
                    F::Least
                };
                return extreme(function, values, name).map(Some);
            }
            "mod" => {
                // MOD is exactly the `%` operator (DuckDB 1.5.6 and PostgreSQL
                // agree): fmod for REAL, exact INTEGER/DECIMAL remainder.
                arity(&[2])?;
                let a = self.expr(argument(&args[0], name)?)?;
                let b = self.expr(argument(&args[1], name)?)?;
                binary(Op::Mod, a, b)?
            }
            "degrees" => {
                return Err(unsupported(
                    "DEGREES (DuckDB multiplies by 180/pi, PostgreSQL divides by pi/180; results differ in the last bit)",
                ));
            }
            _ => return Ok(None),
        }))
    }

    /// FLOOR/CEIL, also reached from sqlparser's dedicated FLOOR/CEIL syntax.
    pub(super) fn floor_ceil(&self, source: &sql::Expr, function: F, name: &str) -> Result<Expr> {
        let value = numeric(self.expr(source)?, &Type::Float, name)?;
        Ok(match value.ty {
            // Both references return DOUBLE for an INTEGER input.
            Type::Int => call(function, Type::Float, vec![promote(value)]),
            Type::Decimal(_) => call(function, Type::Decimal(0), vec![value]),
            _ => call(function, Type::Float, vec![value]),
        })
    }

    /// ROUND's precision contract: an integer literal (with sign) or NULL.
    fn literal_digits(&self, digits: &sql::Expr, name: &str) -> Result<Option<i32>> {
        if matches!(
            digits,
            sql::Expr::Value(sql::ValueWithSpan {
                value: sql::Value::Null,
                ..
            })
        ) {
            return Ok(None);
        }
        if !branch_types::integer_literal_syntax(digits) {
            return Err(unsupported(format!(
                "{name} precision must be an integer literal or NULL"
            )));
        }
        crate::expr::round::precision(&self.expr(digits)?)
            .map(Some)
            .map_err(|_| unsupported(format!("{name} precision must fit INTEGER")))
    }
}

/// GREATEST/LEAST over one common type, with the CASE/COALESCE numeric rule:
/// Int and Float promote to DOUBLE (October 15 decision), Int and DECIMAL to
/// the larger scale. TEXT, DATE and BOOLEAN require one type.
fn extreme(function: F, values: Vec<Expr>, name: &str) -> Result<Expr> {
    let ty = values.iter().try_fold(Type::Null, |ty, value| {
        let same = ty == Type::Null || value.ty == Type::Null || ty == value.ty;
        let numeric = |t: &Type| t.scale().is_ok() || *t == Type::Float;
        if same || (numeric(&ty) && numeric(&value.ty)) {
            common_type(&ty, &value.ty)
        } else {
            Err(format!("{} arguments have incompatible types", name.to_uppercase()).into())
        }
    })?;
    if ty == Type::Binary {
        return Err(unsupported(format!("{} over BLOB", name.to_uppercase())));
    }
    let values = values
        .into_iter()
        .map(|mut value| {
            coerce_null(&mut value, &ty);
            if ty == Type::Float {
                match value.ty {
                    Type::Int => return Ok(promote(value)),
                    Type::Decimal(_) => return crate::expr::casts::bind(value, Type::Float),
                    _ => {}
                }
            }
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(call(function, ty, values))
}
