//! Preserve explicit CTE materialization hints through the pinned generic parser.
//! Only remove hint tokens, retaining every other original token and span. The
//! closing-parenthesis span identifies the exact CTE restored after parsing;
//! a removed hint that does not map to a CTE is an error, never silently ignored.
use sqlparser::{
    ast::{CteAsMaterialized, Query, Statement, VisitMut, VisitorMut},
    tokenizer::{Token, TokenWithSpan},
};
use std::{collections::HashMap, ops::ControlFlow};

type Position = (u64, u64);
pub(crate) struct Restore {
    hints: HashMap<Position, CteAsMaterialized>,
}
fn keyword(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name))
}
fn next(tokens: &[TokenWithSpan], mut i: usize) -> usize {
    while tokens
        .get(i)
        .is_some_and(|t| matches!(t.token, Token::Whitespace(_)))
    {
        i += 1;
    }
    i
}
impl Restore {
    pub(crate) fn apply(&mut self, statements: &mut Vec<Statement>) -> Result<(), String> {
        if self.hints.is_empty() {
            return Ok(());
        }
        if let ControlFlow::Break(error) = statements.visit(self) {
            return Err(error);
        }
        if self.hints.is_empty() {
            Ok(())
        } else {
            Err("invalid CTE materialization hint placement".into())
        }
    }
}
impl VisitorMut for Restore {
    type Break = String;
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<String> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                let location = cte.closing_paren_token.0.span.start;
                if let Some(hint) = self.hints.remove(&(location.line, location.column)) {
                    if cte.materialized.is_some() {
                        return ControlFlow::Break("duplicate CTE materialization hint".into());
                    }
                    cte.materialized = Some(hint);
                }
            }
        }
        ControlFlow::Continue(())
    }
}
pub(crate) fn rewrite(tokens: Vec<TokenWithSpan>) -> Result<(Vec<TokenWithSpan>, Restore), String> {
    let mut restore = Restore {
        hints: HashMap::new(),
    };
    if !tokens.iter().any(|t| keyword(&t.token, "MATERIALIZED")) {
        return Ok((tokens, restore));
    }
    let mut stack = Vec::new();
    let mut closes = HashMap::new();
    for (i, token) in tokens.iter().enumerate() {
        match token.token {
            Token::LParen => stack.push(i),
            Token::RParen => {
                if let Some(open) = stack.pop() {
                    closes.insert(open, i);
                }
            }
            _ => {}
        }
    }
    let mut removed = std::collections::HashSet::new();
    // Recognize declaration headers, not arbitrary AS MATERIALIZED sequences:
    // an ordinary table alias `AS materialized(column)` must remain untouched.
    // The outer scan also discovers nested WITH clauses inside CTE bodies.
    for start in 0..tokens.len() {
        if !keyword(&tokens[start].token, "WITH") {
            continue;
        }
        let mut cursor = next(&tokens, start + 1);
        if tokens
            .get(cursor)
            .is_some_and(|t| keyword(&t.token, "RECURSIVE"))
        {
            cursor = next(&tokens, cursor + 1);
        }
        loop {
            if !tokens
                .get(cursor)
                .is_some_and(|t| matches!(t.token, Token::Word(_)))
            {
                break;
            }
            cursor = next(&tokens, cursor + 1); // CTE name
            if tokens
                .get(cursor)
                .is_some_and(|t| matches!(t.token, Token::LParen))
            {
                let Some(close) = closes.get(&cursor) else {
                    break;
                };
                cursor = next(&tokens, close + 1); // optional column names
            }
            if !tokens.get(cursor).is_some_and(|t| keyword(&t.token, "AS")) {
                break;
            }
            cursor = next(&tokens, cursor + 1);
            let negative = tokens.get(cursor).is_some_and(|t| keyword(&t.token, "NOT"));
            let not_index = cursor;
            if negative {
                cursor = next(&tokens, cursor + 1);
            }
            let hinted = tokens
                .get(cursor)
                .is_some_and(|t| keyword(&t.token, "MATERIALIZED"));
            let hint_index = cursor;
            if hinted {
                cursor = next(&tokens, cursor + 1);
            }
            // Malformed NOT without MATERIALIZED remains the parser's error.
            if negative && !hinted {
                break;
            }
            if !tokens
                .get(cursor)
                .is_some_and(|t| matches!(t.token, Token::LParen))
            {
                break;
            }
            let Some(&close) = closes.get(&cursor) else {
                if hinted {
                    return Err("unclosed CTE materialization body".into());
                }
                break;
            };
            if hinted {
                let location = tokens[close].span.start;
                let hint = if negative {
                    CteAsMaterialized::NotMaterialized
                } else {
                    CteAsMaterialized::Materialized
                };
                if restore
                    .hints
                    .insert((location.line, location.column), hint)
                    .is_some()
                {
                    return Err("duplicate CTE materialization hint location".into());
                }
                removed.insert(hint_index);
                if negative {
                    removed.insert(not_index);
                }
            }
            cursor = next(&tokens, close + 1);
            if !tokens
                .get(cursor)
                .is_some_and(|t| matches!(t.token, Token::Comma))
            {
                break;
            }
            cursor = next(&tokens, cursor + 1);
        }
    }
    let output = tokens
        .into_iter()
        .enumerate()
        .filter_map(|(index, token)| (!removed.contains(&index)).then_some(token))
        .collect();
    Ok((output, restore))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

    fn tokens(sql: &str) -> Vec<TokenWithSpan> {
        Tokenizer::new(&GenericDialect {}, sql)
            .tokenize_with_location()
            .unwrap()
    }
    fn parsed(sql: &str, count: usize) -> Vec<Statement> {
        let (tokens, mut restore) = rewrite(tokens(sql)).unwrap();
        assert_eq!(restore.hints.len(), count);
        let mut statements = Parser::new(&GenericDialect {})
            .with_tokens_with_locations(tokens)
            .parse_statements()
            .unwrap();
        restore.apply(&mut statements).unwrap();
        assert!(restore.hints.is_empty());
        statements
    }

    #[test]
    fn ordinary_materialized_alias_with_columns_is_unchanged() {
        for sql in [
            "SELECT * FROM (VALUES (1)) AS materialized(x)",
            "WITH x AS (SELECT 1) SELECT * FROM x AS materialized(y)",
            "SELECT * FROM (VALUES (1)) AS \"materialized\"(x)",
        ] {
            let original = tokens(sql);
            let (rewritten, restore) = rewrite(original.clone()).unwrap();
            assert_eq!(original, rewritten);
            assert!(restore.hints.is_empty());
            parsed(sql, 0);
        }
    }

    #[test]
    fn hints_mixed_with_ordinary_alias_and_nested_declarations() {
        let statements = parsed(
            "WITH a(x) AS MATERIALIZED (SELECT 1), b AS NOT MATERIALIZED (WITH c AS MATERIALIZED (SELECT 2) SELECT * FROM c) SELECT * FROM a AS materialized(y), b",
            3,
        );
        let Statement::Query(query) = &statements[0] else {
            panic!();
        };
        let ctes = &query.with.as_ref().unwrap().cte_tables;
        assert_eq!(ctes[0].materialized, Some(CteAsMaterialized::Materialized));
        assert_eq!(
            ctes[1].materialized,
            Some(CteAsMaterialized::NotMaterialized)
        );
        assert_eq!(
            ctes[1].query.with.as_ref().unwrap().cte_tables[0].materialized,
            Some(CteAsMaterialized::Materialized)
        );
        parsed(
            "WITH RECURSIVE a AS NOT MATERIALIZED (SELECT 1) SELECT * FROM a; WITH b AS MATERIALIZED (SELECT 2) SELECT * FROM b",
            2,
        );
        parsed(
            "WITH a AS (SELECT 1), b AS MATERIALIZED (SELECT 2) SELECT * FROM b",
            1,
        );
    }

    #[test]
    fn only_hint_words_are_removed_and_original_locations_survive() {
        let sql = "WITH a AS /*keep AS*/ NOT -- keep NOT\n MATERIALIZED /*keep hint*/ (SELECT 'AS MATERIALIZED (', 1) SELECT * FROM a";
        let original = tokens(sql);
        let (rewritten, mut restore) = rewrite(original.clone()).unwrap();
        let expected: Vec<_> = original
            .into_iter()
            .filter(|t| !keyword(&t.token, "NOT") && !keyword(&t.token, "MATERIALIZED"))
            .collect();
        assert_eq!(rewritten, expected); // TokenWithSpan includes original span.
        let mut statements = Parser::new(&GenericDialect {})
            .with_tokens_with_locations(rewritten)
            .parse_statements()
            .unwrap();
        restore.apply(&mut statements).unwrap();
    }

    #[test]
    fn malformed_or_unmatched_hints_cannot_be_silently_accepted() {
        for sql in [
            "WITH a AS MATERIALIZED MATERIALIZED (SELECT 1) SELECT * FROM a",
            "WITH a AS NOT (SELECT 1) SELECT * FROM a",
            "WITH a AS MATERIALIZED (SELECT 1 SELECT * FROM a",
            "SELECT 1 AS MATERIALIZED (SELECT 2)",
        ] {
            let accepted = rewrite(tokens(sql)).and_then(|(tokens, mut restore)| {
                let mut statements = Parser::new(&GenericDialect {})
                    .with_tokens_with_locations(tokens)
                    .parse_statements()
                    .map_err(|e| e.to_string())?;
                restore.apply(&mut statements)
            });
            assert!(accepted.is_err(), "{sql}");
        }
        let (_, mut restore) =
            rewrite(tokens("WITH a AS MATERIALIZED (SELECT 1) SELECT * FROM a")).unwrap();
        let mut unrelated = parsed("SELECT 1", 0);
        assert_eq!(
            restore.apply(&mut unrelated).unwrap_err(),
            "invalid CTE materialization hint placement"
        );
    }
}

#[cfg(test)]
mod integration_tests {
    #[test]
    fn hint_inside_fetch_quantity_survives_both_parser_bridges() {
        let sql = "SELECT 1 FETCH FIRST (WITH c AS MATERIALIZED (SELECT 1 AS n) SELECT n FROM c) ROWS ONLY";
        let statements =
            crate::nesting::parse(sql).expect("nested hint must be restored after FETCH quantity");
        assert!(statements[0].to_string().contains("AS MATERIALIZED"));
    }
}
