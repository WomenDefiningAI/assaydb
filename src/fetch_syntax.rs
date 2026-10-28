//! Preserve FETCH quantity expressions until sqlparser can represent them.
//! The pinned parser accepts only a single value token after FETCH FIRST/NEXT.
//! We parse the quantity's original tokens directly, temporarily substitute a
//! collision-free string token, and restore the expression through the AST
//! visitor. No generated SQL text, user string decoding, or LIMIT rewriting.
use sqlparser::{
    ast::{Expr, Query, Value, VisitMut, VisitorMut},
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan},
};
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

fn keyword(token: &Token, expected: &str) -> bool {
    matches!(token,Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(expected))
}

fn next(tokens: &[TokenWithSpan], mut index: usize) -> usize {
    while tokens
        .get(index)
        .is_some_and(|t| matches!(t.token, Token::Whitespace(_)))
    {
        index += 1;
    }
    index
}

fn parse_error(error: ParserError) -> String {
    match error {
        ParserError::RecursionLimitExceeded => crate::nesting::REFUSAL.into(),
        other => other.to_string(),
    }
}

pub(crate) struct Restore {
    expressions: HashMap<String, Expr>,
    occupied: HashSet<String>,
    serial: usize,
    recursion_limit: usize,
}

impl Restore {
    fn tokens(&mut self, tokens: Vec<TokenWithSpan>) -> Result<Vec<TokenWithSpan>, String> {
        let mut output = Vec::with_capacity(tokens.len());
        let mut i = 0;
        while i < tokens.len() {
            if !keyword(&tokens[i].token, "FETCH") {
                output.push(tokens[i].clone());
                i += 1;
                continue;
            }
            let first = next(&tokens, i + 1);
            if !tokens
                .get(first)
                .is_some_and(|t| keyword(&t.token, "FIRST") || keyword(&t.token, "NEXT"))
            {
                output.push(tokens[i].clone());
                i += 1;
                continue;
            }
            let start = next(&tokens, first + 1);
            if tokens
                .get(start)
                .is_none_or(|t| keyword(&t.token, "ROW") || keyword(&t.token, "ROWS"))
            {
                output.push(tokens[i].clone());
                i += 1;
                continue;
            }
            let mut end = start;
            let mut depth = 0usize;
            while end < tokens.len() {
                let token = &tokens[end].token;
                if depth == 0
                    && (keyword(token, "ROW")
                        || keyword(token, "ROWS")
                        || keyword(token, "PERCENT")
                        || keyword(token, "ONLY")
                        || keyword(token, "WITH"))
                {
                    break;
                }
                match token {
                    Token::LParen | Token::LBracket => depth += 1,
                    Token::RParen | Token::RBracket if depth > 0 => depth -= 1,
                    Token::RParen | Token::RBracket | Token::SemiColon | Token::EOF
                        if depth == 0 =>
                    {
                        break;
                    }
                    _ => {}
                }
                end += 1;
            }
            if end == start || end == tokens.len() || !matches!(&tokens[end].token, Token::Word(_))
            {
                // Leave incomplete/ambiguous syntax to the original parser.
                output.push(tokens[i].clone());
                i += 1;
                continue;
            }
            let quantity = self.tokens(tokens[start..end].to_vec())?;
            let mut parser = Parser::new(&crate::dialect::QueryDialect)
                .with_recursion_limit(self.recursion_limit)
                .with_tokens_with_locations(quantity);
            let expression = parser.parse_expr().map_err(parse_error)?;
            if parser.peek_token().token != Token::EOF {
                return Err("invalid FETCH quantity expression".into());
            }
            let marker = loop {
                let marker = format!("__assay_fetch_quantity_{}", self.serial);
                self.serial += 1;
                if self.occupied.insert(marker.clone()) {
                    break marker;
                }
            };
            self.expressions.insert(marker.clone(), expression);
            output.extend_from_slice(&tokens[i..start]);
            let mut replacement = tokens[start].clone();
            replacement.token = Token::SingleQuotedString(marker);
            output.push(replacement);
            i = end;
        }
        Ok(output)
    }

    pub(crate) fn apply(
        &mut self,
        statements: &mut Vec<sqlparser::ast::Statement>,
    ) -> Result<(), String> {
        if self.expressions.is_empty() {
            return Ok(());
        }
        if let ControlFlow::Break(error) = statements.visit(self) {
            return Err(error);
        }
        if self.expressions.is_empty() {
            Ok(())
        } else {
            Err("FETCH expression marker was not restored".into())
        }
    }
}

impl VisitorMut for Restore {
    type Break = String;
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(fetch) = &mut query.fetch
            && let Some(Expr::Value(value)) = &fetch.quantity
            && let Value::SingleQuotedString(marker) = &value.value
            && let Some(expression) = self.expressions.remove(marker)
        {
            fetch.quantity = Some(expression);
        }
        ControlFlow::Continue(())
    }
}

/// Caller checks the original token depth before invoking this bridge.
pub(crate) fn rewrite(
    tokens: Vec<TokenWithSpan>,
    recursion_limit: usize,
) -> Result<(Vec<TokenWithSpan>, Restore), String> {
    if !tokens.iter().any(|t| keyword(&t.token, "FETCH")) {
        return Ok((
            tokens,
            Restore {
                expressions: HashMap::new(),
                occupied: HashSet::new(),
                serial: 0,
                recursion_limit,
            },
        ));
    }
    let occupied = tokens
        .iter()
        .filter_map(|t| match &t.token {
            Token::SingleQuotedString(value) => Some(value.clone()),
            _ => None,
        })
        .collect();
    let mut restore = Restore {
        expressions: HashMap::new(),
        occupied,
        serial: 0,
        recursion_limit,
    };
    let tokens = restore.tokens(tokens)?;
    Ok((tokens, restore))
}
