//! Binding for the regular-expression functions, `~*`/`!~*` and SIMILAR TO
//! (SEMANTICS-LOG.md, October 18, 2026). Constant patterns, options and
//! replacements are validated here; values read from data are checked per
//! row by `crate::expr::regexp`.
use super::*;
use crate::expr::Call;
use crate::expr::regexp::{self, Definer, RegexCall, RegexOp, Template};

const TILDE: &str = "(DuckDB's ~ is a full match like REGEXP_FULL_MATCH; PostgreSQL's is a partial match); use REGEXP_MATCHES or REGEXP_LIKE for a partial match, REGEXP_FULL_MATCH for a full match";

fn call(call: RegexCall, args: Vec<Expr>) -> Expr {
    Expr {
        ty: call.result_type(),
        kind: Kind::Call(Call::Regex(call), args),
    }
}

fn constant_text(e: &Expr) -> Option<&str> {
    match &e.kind {
        Kind::Literal(Literal::Text(text)) => Some(text),
        _ => None,
    }
}

impl Binder<'_> {
    /// Check constant arguments and build the call. `options` is `None`
    /// for a NULL options literal (the result is NULL where the defining
    /// reference returns NULL).
    fn regexp_call(&self, call_: RegexCall, args: Vec<Expr>) -> Result<Expr> {
        let compiled = match constant_text(&args[1]) {
            Some(pattern) => Some(regexp::check_constant(pattern, call_)?),
            None => None,
        };
        if let Some(replacement) = args.get(2).and_then(constant_text) {
            let template = Template::parse(replacement)?;
            if let Some(compiled) = &compiled {
                template.check(&compiled.info)?;
            }
        }
        Ok(call(call_, args))
    }

    /// REGEXP_* functions; `None` leaves the name to the caller.
    pub(super) fn regexp_function(
        &self,
        name: &str,
        args: &[sql::FunctionArg],
    ) -> Result<Option<Expr>> {
        let (arity, definer): (std::ops::RangeInclusive<usize>, Definer) = match name {
            "regexp_matches" | "regexp_full_match" => (2..=3, Definer::DuckDb),
            "regexp_extract" => (2..=4, Definer::DuckDb),
            "regexp_like" => (2..=3, Definer::PostgreSql),
            "regexp_replace" => (3..=4, Definer::Both),
            "regexp_extract_all" => {
                return Err(unsupported(
                    "REGEXP_EXTRACT_ALL (returns a LIST; Assay has no LIST type)",
                ));
            }
            "regexp_split_to_array"
            | "regexp_split_to_table"
            | "string_split_regex"
            | "str_split_regex" => {
                return Err(unsupported(format!(
                    "{} (returns a LIST or a set of rows; Assay has no LIST type)",
                    name.to_uppercase()
                )));
            }
            "regexp_match" => {
                return Err(unsupported(
                    "REGEXP_MATCH (PostgreSQL returns a text array; Assay has no array type)",
                ));
            }
            "regexp_count" | "regexp_substr" | "regexp_instr" => {
                return Err(unsupported(format!(
                    "{} (PostgreSQL 15+ only; DuckDB has no equivalent with the same answers)",
                    name.to_uppercase()
                )));
            }
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
        if !arity.contains(&exprs.len()) {
            return Err(unsupported(format!("{label} arity")));
        }
        let text = self.text_argument(exprs[0], &label)?;
        let pattern = self.text_argument(exprs[1], &label)?;
        let mut bound = vec![text, pattern];
        let replace = name == "regexp_replace";
        if replace {
            bound.push(self.text_argument(exprs[2], &label)?);
        }
        let mut group = 0u8;
        let mut options_at = if replace { 3 } else { 2 };
        if name == "regexp_extract" && exprs.len() >= 3 {
            group = self.extract_group(exprs[2])?;
            options_at = 3;
        }
        let (mut ci, mut dot, mut global) = (false, false, false);
        if let Some(e) = exprs.get(options_at) {
            let value = self.expr(e)?;
            match &value.kind {
                Kind::Literal(Literal::Text(text)) => {
                    (ci, dot, global) = regexp::options(text, definer, replace)?;
                }
                Kind::Literal(Literal::Null) if definer == Definer::DuckDb => {
                    return Err(format!("{label} options must not be NULL").into());
                }
                Kind::Literal(Literal::Null) => {
                    // Strict in PostgreSQL (and DuckDB's REGEXP_REPLACE): NULL.
                    let ty = if replace { Type::Text } else { Type::Bool };
                    return Ok(Some(literal(ty, Literal::Null)));
                }
                _ if definer == Definer::DuckDb => {
                    return Err(format!("{label} options must be a string literal").into());
                }
                _ => {
                    return Err(unsupported(format!(
                        "{label} options must be a string literal"
                    )));
                }
            }
        }
        let op = match name {
            "regexp_matches" => RegexOp::Matches,
            "regexp_full_match" => RegexOp::FullMatch,
            "regexp_like" => RegexOp::Like,
            "regexp_extract" => RegexOp::Extract { group },
            _ => RegexOp::Replace { global },
        };
        let call_ = RegexCall {
            op,
            negated: false,
            case_insensitive: ci,
            dot_newline: dot,
        };
        self.regexp_call(call_, bound).map(Some)
    }

    /// REGEXP_EXTRACT's group: an integer literal 0..=9, as DuckDB requires.
    fn extract_group(&self, e: &sql::Expr) -> Result<u8> {
        let value = self.expr(e)?;
        let number = match &value.kind {
            Kind::Literal(Literal::Num(n)) if value.ty == Type::Int => Some(*n),
            Kind::Neg(inner) if inner.ty == Type::Int => match inner.kind {
                Kind::Literal(Literal::Num(n)) => Some(-n),
                _ => None,
            },
            Kind::Literal(Literal::Null) => {
                return Err(unsupported(
                    "REGEXP_EXTRACT with a NULL group (DuckDB returns '' for every row)",
                ));
            }
            _ => None,
        };
        match number {
            Some(n @ 0..=9) => Ok(n as u8),
            Some(_) => Err("REGEXP_EXTRACT group must be between 0 and 9".into()),
            None => Err("REGEXP_EXTRACT group must be an integer literal between 0 and 9".into()),
        }
    }

    /// `~*` and `!~*` (PostgreSQL; REGEXP_MATCHES(s, p, 'i') in DuckDB).
    /// `~` and `!~` are refused: the two references disagree on them.
    pub(super) fn regexp_operator(
        &self,
        left: &sql::Expr,
        op: &sql::BinaryOperator,
        right: &sql::Expr,
    ) -> Result<Expr> {
        let negated = match op {
            sql::BinaryOperator::PGRegexMatch => return Err(unsupported(format!("~ {TILDE}"))),
            sql::BinaryOperator::PGRegexNotMatch => {
                return Err(unsupported(format!("!~ {TILDE}")));
            }
            sql::BinaryOperator::PGRegexIMatch => false,
            _ => true,
        };
        let label = if negated { "!~*" } else { "~*" };
        let args = vec![
            self.text_argument(left, label)?,
            self.text_argument(right, label)?,
        ];
        let call_ = RegexCall {
            op: RegexOp::Like,
            negated,
            case_insensitive: true,
            dot_newline: false,
        };
        self.regexp_call(call_, args)
    }

    pub(super) fn similar_to(
        &self,
        input: &sql::Expr,
        pattern: &sql::Expr,
        negated: bool,
        escape: Option<&sql::Value>,
    ) -> Result<Expr> {
        if escape.is_some() {
            return Err(unsupported(
                "SIMILAR TO ... ESCAPE (DuckDB does not implement a custom escape)",
            ));
        }
        let args = vec![
            self.text_argument(input, "SIMILAR TO")?,
            self.text_argument(pattern, "SIMILAR TO")?,
        ];
        let call_ = RegexCall {
            op: RegexOp::Similar,
            negated,
            case_insensitive: false,
            dot_newline: false,
        };
        self.regexp_call(call_, args)
    }
}
