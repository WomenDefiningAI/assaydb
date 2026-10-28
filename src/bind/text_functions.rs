//! Binding for string functions, `||`, TRIM syntax, ILIKE and LIKE ... ESCAPE.
//! Semantics follow what DuckDB 1.5.6 and PostgreSQL 17 share; forms whose
//! answers differ are refused with `unsupported:` (see SEMANTICS-LOG.md).
use super::*;
use crate::expr::{Call, Escape};

fn call(call: Call, args: Vec<Expr>) -> Expr {
    Expr {
        ty: call.result_type(),
        kind: Kind::Call(call, args),
    }
}

/// Type a NULL literal; other NULL-typed expressions evaluate to all-NULL
/// arrays, which the kernels accept for any argument.
fn typed(mut value: Expr, ty: Type) -> Expr {
    if matches!(value.kind, Kind::Literal(Literal::Null)) {
        value.ty = ty;
    }
    value
}

const BOOLEAN_CONCAT: &str = "BOOLEAN in string concatenation (DuckDB prints true, PostgreSQL CONCAT prints t, Assay BOOLEAN text is 1)";

impl Binder<'_> {
    pub(super) fn text_argument(&self, e: &sql::Expr, name: &str) -> Result<Expr> {
        let value = typed(self.expr(e)?, Type::Text);
        if !matches!(value.ty, Type::Text | Type::Null) {
            return Err(unsupported(format!("{name} requires TEXT arguments")));
        }
        Ok(value)
    }
    fn integer_argument(&self, e: &sql::Expr, name: &str) -> Result<Expr> {
        let value = typed(self.expr(e)?, Type::Int);
        if !matches!(value.ty, Type::Int | Type::Null) {
            return Err(unsupported(format!("{name} requires an INTEGER count")));
        }
        Ok(value)
    }

    /// An operand of `||` or CONCAT, converted to TEXT with the existing cast
    /// formats (DOUBLE uses DuckDB's shortest round-trip form). Also reports
    /// whether the operand was already TEXT or NULL.
    fn concat_operand(&self, e: &sql::Expr) -> Result<(Expr, bool)> {
        Self::concat_value(self.expr(e)?)
    }
    /// `concat_operand` after its operand is bound.
    pub(super) fn concat_value(value: Expr) -> Result<(Expr, bool)> {
        match value.ty {
            Type::Text => Ok((value, true)),
            Type::Null => Ok((typed(value, Type::Text), true)),
            Type::Int
            | Type::Decimal(_)
            | Type::Date
            | Type::Timestamp
            | Type::TimestampTz
            | Type::Float => Ok((crate::expr::casts::bind(value, Type::Text)?, false)),
            Type::Bool => Err(unsupported(BOOLEAN_CONCAT)),
            ref other => Err(unsupported(format!("{other:?} in string concatenation"))),
        }
    }

    /// `left || right` once the left operand is converted and the right one
    /// bound; operator chains bind `||` links iteratively through this (see
    /// `Binder::binary_chain`), left operand first as before.
    pub(super) fn concat_finish(a: (Expr, bool), b: Expr) -> Result<Expr> {
        let b = Self::concat_value(b)?;
        Self::concat_pair(a, b)
    }
    fn concat_pair((a, a_text): (Expr, bool), (b, b_text): (Expr, bool)) -> Result<Expr> {
        // PostgreSQL has no || for two non-text operands (DuckDB concatenates).
        if !a_text && !b_text {
            return Err(unsupported(
                "|| requires a TEXT operand (PostgreSQL has no || for two non-text values)",
            ));
        }
        Ok(call(Call::ConcatStrict, vec![a, b]))
    }

    pub(super) fn trim(
        &self,
        input: &sql::Expr,
        side: Option<&sql::TrimWhereField>,
        what: Option<&sql::Expr>,
        characters: Option<&Vec<sql::Expr>>,
    ) -> Result<Expr> {
        let (leading, trailing) = match side {
            None | Some(sql::TrimWhereField::Both) => (true, true),
            Some(sql::TrimWhereField::Leading) => (true, false),
            Some(sql::TrimWhereField::Trailing) => (false, true),
        };
        let mut args = vec![self.text_argument(input, "TRIM")?];
        match (what, characters) {
            (Some(what), None) => args.push(self.text_argument(what, "TRIM")?),
            (None, Some(list)) if list.len() == 1 && side.is_none() => {
                args.push(self.text_argument(&list[0], "TRIM")?)
            }
            (None, None) => {}
            _ => return Err(unsupported("TRIM form")),
        }
        Ok(call(Call::Trim { leading, trailing }, args))
    }

    pub(super) fn like(
        &self,
        input: &sql::Expr,
        pattern: &sql::Expr,
        negated: bool,
        case_insensitive: bool,
        escape: Option<&sql::Value>,
    ) -> Result<Expr> {
        let escape = match escape {
            None => Escape::Default,
            Some(sql::Value::Null) => Escape::Null,
            Some(sql::Value::SingleQuotedString(text)) => {
                let mut chars = text.chars();
                match (chars.next(), chars.next()) {
                    (None, _) => Escape::Disabled,
                    (Some(c), None) if !c.is_ascii() => {
                        return Err(unsupported(
                            "LIKE ESCAPE requires an ASCII character (DuckDB rejects multibyte escapes)",
                        ));
                    }
                    (Some(c), None) if case_insensitive && c.is_ascii_alphabetic() => {
                        return Err(unsupported(
                            "ILIKE ESCAPE with a cased letter (case folding order differs)",
                        ));
                    }
                    (Some(c), None) => Escape::Char(c),
                    _ => {
                        return Err(
                            "invalid escape string: ESCAPE must be empty or one character".into(),
                        );
                    }
                }
            }
            Some(_) => return Err(unsupported("LIKE ESCAPE requires a string literal")),
        };
        let name = if case_insensitive { "ILIKE" } else { "LIKE" };
        let input = self.text_argument(input, name)?;
        let pattern = self.text_argument(pattern, name)?;
        if escape != Escape::Null
            && let Kind::Literal(Literal::Text(text)) = &pattern.kind
        {
            crate::expr::text::validate_pattern(text, escape, case_insensitive)?;
        }
        Ok(call(
            Call::Like {
                case_insensitive,
                negated,
                escape,
            },
            vec![input, pattern],
        ))
    }

    /// Multi-argument string functions; `None` leaves the name to the caller.
    pub(super) fn text_function(
        &self,
        name: &str,
        args: &[sql::FunctionArg],
    ) -> Result<Option<Expr>> {
        let arity: &[usize] = match name {
            "ltrim" | "rtrim" => &[1, 2],
            "replace" | "split_part" | "translate" => &[3],
            "left" | "right" | "starts_with" | "contains" | "strpos" => &[2],
            "lpad" | "rpad" => &[2, 3],
            "reverse" => &[1],
            "concat" | "concat_ws" => &[],
            _ => return Ok(None),
        };
        let label = name.to_uppercase();
        let mut exprs = Vec::with_capacity(args.len());
        for argument in args {
            let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(value)) = argument else {
                return Err(unsupported("scalar argument"));
            };
            exprs.push(value);
        }
        if name == "concat_ws" {
            if exprs.len() < 2 {
                return Err("CONCAT_WS requires a separator and at least one value".into());
            }
            let mut values = vec![self.text_argument(exprs[0], "CONCAT_WS separator")?];
            for expression in &exprs[1..] {
                let value = self.expr(expression)?;
                // DuckDB spelling here is local to CONCAT_WS. Public casts and
                // the existing CONCAT/|| Boolean refusal remain unchanged.
                values.push(if value.ty == Type::Bool {
                    value
                } else {
                    Self::concat_value(value)?.0
                });
            }
            return Ok(Some(call(Call::ConcatWs, values)));
        }
        if name == "concat" {
            if exprs.is_empty() {
                return Err("CONCAT requires at least one argument".into());
            }
            let values = exprs
                .iter()
                .map(|e| self.concat_operand(e).map(|(value, _)| value))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(call(Call::Concat, values)));
        }
        if !arity.contains(&exprs.len()) {
            return Err(unsupported(format!("{label} arity")));
        }
        let text = |i: usize| self.text_argument(exprs[i], &label);
        let count = |i: usize| self.integer_argument(exprs[i], &label);
        Ok(Some(match name {
            "ltrim" | "rtrim" => {
                let mut values = vec![text(0)?];
                if exprs.len() == 2 {
                    values.push(text(1)?);
                }
                call(
                    Call::Trim {
                        leading: name == "ltrim",
                        trailing: name == "rtrim",
                    },
                    values,
                )
            }
            "replace" => call(Call::Replace, vec![text(0)?, text(1)?, text(2)?]),
            "translate" => call(Call::Translate, vec![text(0)?, text(1)?, text(2)?]),
            "left" => call(Call::Left, vec![text(0)?, count(1)?]),
            "right" => call(Call::Right, vec![text(0)?, count(1)?]),
            "starts_with" => call(Call::StartsWith, vec![text(0)?, text(1)?]),
            "contains" => {
                let mut values = Vec::with_capacity(2);
                for e in &exprs {
                    let value = self.expr(e)?;
                    if value.ty == Type::Null {
                        return Err(unsupported(
                            "CONTAINS with an untyped NULL argument (DuckDB cannot choose an overload)",
                        ));
                    }
                    if value.ty != Type::Text {
                        return Err(unsupported("CONTAINS requires TEXT arguments"));
                    }
                    values.push(value);
                }
                call(Call::Contains, values)
            }
            "strpos" => call(Call::Strpos, vec![text(0)?, text(1)?]),
            "split_part" => call(Call::SplitPart, vec![text(0)?, text(1)?, count(2)?]),
            "lpad" | "rpad" if exprs.len() == 2 => {
                return Err(unsupported(
                    "LPAD/RPAD require a fill argument (DuckDB has no two-argument form)",
                ));
            }
            "lpad" | "rpad" => call(
                Call::Pad {
                    left: name == "lpad",
                },
                vec![text(0)?, count(1)?, text(2)?],
            ),
            "reverse" => call(Call::Reverse, vec![text(0)?]),
            _ => unreachable!(),
        }))
    }
}
