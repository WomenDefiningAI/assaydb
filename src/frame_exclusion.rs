//! Window-frame `EXCLUDE` clauses (SQL:2011; PostgreSQL 17 and DuckDB 1.5.6).
//!
//! The pinned sqlparser 0.58 does not parse `EXCLUDE CURRENT ROW | GROUP |
//! TIES | NO OTHERS` after a frame. Before parsing, each such clause is removed
//! from the token stream and recorded in the frame's start bound as a private
//! marker call, which sqlparser parses as an ordinary bound expression:
//!
//! - `UNBOUNDED PRECEDING` becomes `__assay_frame_exclude_<kind>_unbounded() PRECEDING`;
//! - `CURRENT ROW` becomes `__assay_frame_exclude_<kind>_current() PRECEDING`;
//! - `<n> PRECEDING|FOLLOWING` becomes `__assay_frame_exclude_<kind>_offset(<n>) PRECEDING|FOLLOWING`.
//!
//! `EXCLUDE NO OTHERS` is the default and is simply removed. The frame parser
//! (`rel::window::frames`) decodes the marker; `display` restores the original
//! spelling in column names. The marker therefore survives AST printing and
//! re-parsing (views) unchanged.
use sqlparser::tokenizer::{Token, TokenWithSpan};

pub(crate) const MARKER: &str = "__assay_frame_exclude_";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Exclude {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl Exclude {
    fn tag(self) -> &'static str {
        match self {
            Self::NoOthers => "no_others",
            Self::CurrentRow => "current_row",
            Self::Group => "group",
            Self::Ties => "ties",
        }
    }
    fn sql(self) -> &'static str {
        match self {
            Self::NoOthers => "NO OTHERS",
            Self::CurrentRow => "CURRENT ROW",
            Self::Group => "GROUP",
            Self::Ties => "TIES",
        }
    }
}

/// The start-bound shape recorded by a marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    Unbounded,
    Current,
    Offset,
}

/// Decode a marker function name into its exclusion and start-bound shape.
pub(crate) fn decode(name: &str) -> Option<(Exclude, Shape)> {
    let rest = name.strip_prefix(MARKER)?;
    let (exclude, rest) = [Exclude::CurrentRow, Exclude::Group, Exclude::Ties]
        .into_iter()
        .find_map(|e| rest.strip_prefix(e.tag()).map(|r| (e, r)))?;
    let shape = match rest {
        "_unbounded" => Shape::Unbounded,
        "_current" => Shape::Current,
        "_offset" => Shape::Offset,
        _ => return None,
    };
    Some((exclude, shape))
}

fn word(token: &Token) -> Option<&str> {
    match token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.as_str()),
        _ => None,
    }
}
fn is(token: &Token, text: &str) -> bool {
    word(token).is_some_and(|w| w.eq_ignore_ascii_case(text))
}
fn significant(tokens: &[TokenWithSpan]) -> Vec<usize> {
    (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        .collect()
}

/// One frame-exclusion clause located in the token stream.
struct Clause {
    /// Significant-token positions of EXCLUDE and its last keyword.
    exclude: usize,
    last: usize,
    kind: Exclude,
    /// First significant position of the frame's start bound.
    start: usize,
}

fn find(tokens: &[TokenWithSpan], sig: &[usize]) -> Option<Clause> {
    let at = |i: usize| &tokens[sig[i]].token;
    for p in 0..sig.len() {
        if !is(at(p), "EXCLUDE") || p == 0 {
            continue;
        }
        let (kind, last) = if p + 2 < sig.len() && is(at(p + 1), "CURRENT") && is(at(p + 2), "ROW")
        {
            (Exclude::CurrentRow, p + 2)
        } else if p + 1 < sig.len() && is(at(p + 1), "GROUP") {
            (Exclude::Group, p + 1)
        } else if p + 1 < sig.len() && is(at(p + 1), "TIES") {
            (Exclude::Ties, p + 1)
        } else if p + 2 < sig.len() && is(at(p + 1), "NO") && is(at(p + 2), "OTHERS") {
            (Exclude::NoOthers, p + 2)
        } else {
            continue;
        };
        if last + 1 >= sig.len() || at(last + 1) != &Token::RParen {
            continue;
        }
        if !["PRECEDING", "FOLLOWING", "ROW"]
            .iter()
            .any(|k| is(at(p - 1), k))
        {
            continue;
        }
        // The frame units keyword at this parenthesis level.
        let mut depth = 0_i32;
        let mut units = None;
        for i in (0..p).rev() {
            match at(i) {
                Token::RParen => depth += 1,
                Token::LParen if depth == 0 => break,
                Token::LParen => depth -= 1,
                t if depth == 0 && ["ROWS", "RANGE", "GROUPS"].iter().any(|k| is(t, k)) => {
                    units = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(units) = units else { continue };
        let mut start = units + 1;
        if start < p && is(at(start), "BETWEEN") {
            start += 1;
        }
        if start >= p {
            continue;
        }
        return Some(Clause {
            exclude: p,
            last,
            kind,
            start,
        });
    }
    None
}

/// Rewrite every frame `EXCLUDE` clause into its start-bound marker.
pub(crate) fn rewrite(mut tokens: Vec<TokenWithSpan>) -> Vec<TokenWithSpan> {
    use sqlparser::tokenizer::Whitespace;
    loop {
        let sig = significant(&tokens);
        let Some(clause) = find(&tokens, &sig) else {
            return tokens;
        };
        let span = tokens[sig[clause.exclude]].span;
        let make = |token: Token| TokenWithSpan { token, span };
        let at = |i: usize| &tokens[sig[i]].token;
        // Token-index edits: (first, last) replaced by `with`; an empty range
        // (last < first) inserts before `first`.
        let mut edits: Vec<(usize, usize, Vec<TokenWithSpan>)> = Vec::new();
        if clause.kind != Exclude::NoOthers {
            let name = |shape: &str| {
                make(Token::make_word(
                    &format!("{MARKER}{}_{shape}", clause.kind.tag()),
                    None,
                ))
            };
            let s = clause.start;
            if is(at(s), "UNBOUNDED") {
                edits.push((
                    sig[s],
                    sig[s],
                    vec![name("unbounded"), make(Token::LParen), make(Token::RParen)],
                ));
            } else if is(at(s), "CURRENT") && s + 1 < clause.exclude && is(at(s + 1), "ROW") {
                edits.push((
                    sig[s],
                    sig[s + 1],
                    vec![
                        name("current"),
                        make(Token::LParen),
                        make(Token::RParen),
                        make(Token::Whitespace(Whitespace::Space)),
                        make(Token::make_keyword("PRECEDING")),
                    ],
                ));
            } else {
                // `<expr> PRECEDING|FOLLOWING`: wrap the expression.
                let mut depth = 0_i32;
                let mut end = None;
                for i in s..clause.exclude {
                    match at(i) {
                        Token::LParen => depth += 1,
                        Token::RParen => depth -= 1,
                        t if depth == 0 && (is(t, "PRECEDING") || is(t, "FOLLOWING")) => {
                            end = Some(i);
                            break;
                        }
                        _ => {}
                    }
                }
                let Some(end) = end.filter(|e| *e > s) else {
                    // Not a recognisable bound: leave it for the parser to reject.
                    return tokens;
                };
                edits.push((
                    sig[s],
                    sig[s] - 1,
                    vec![name("offset"), make(Token::LParen)],
                ));
                edits.push((
                    sig[end],
                    sig[end] - 1,
                    vec![
                        make(Token::RParen),
                        make(Token::Whitespace(Whitespace::Space)),
                    ],
                ));
            }
        }
        // Remove `EXCLUDE ...` and the whitespace before it.
        let mut first = sig[clause.exclude];
        while first > 0 && matches!(tokens[first - 1].token, Token::Whitespace(_)) {
            first -= 1;
        }
        edits.push((first, sig[clause.last], Vec::new()));
        let mut out = Vec::with_capacity(tokens.len() + 6);
        let mut index = 0;
        while index < tokens.len() {
            let mut skipped = false;
            for (first, last, with) in &edits {
                if *first == index {
                    out.extend(with.iter().cloned());
                    if *last >= *first {
                        index = *last + 1;
                        skipped = true;
                        break;
                    }
                }
            }
            if !skipped {
                out.push(tokens[index].clone());
                index += 1;
            }
        }
        tokens = out;
    }
}

/// Restore `EXCLUDE ...` spelling in printed SQL (column names).
pub(crate) fn display(text: &str) -> String {
    if !text.contains(MARKER) {
        return text.to_string();
    }
    let Ok(mut tokens) =
        sqlparser::tokenizer::Tokenizer::new(&crate::dialect::QueryDialect, text).tokenize()
    else {
        return text.to_string();
    };
    loop {
        let Some(at) = tokens
            .iter()
            .position(|t| word(t).is_some_and(|w| decode(w).is_some()))
        else {
            break;
        };
        let (kind, shape) = decode(word(&tokens[at]).unwrap()).unwrap();
        // Matching parenthesis of the marker call.
        let open = at + 1;
        if tokens.get(open) != Some(&Token::LParen) {
            break;
        }
        let mut depth = 0_i32;
        let mut close = None;
        for (i, token) in tokens.iter().enumerate().skip(open) {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close) = close else { break };
        let Some(keyword) =
            (close + 1..tokens.len()).find(|&i| !matches!(tokens[i], Token::Whitespace(_)))
        else {
            break;
        };
        // End of the window specification: the next unmatched ')'.
        let mut depth = 0_i32;
        let mut spec_end = None;
        for (i, token) in tokens.iter().enumerate().skip(keyword + 1) {
            match token {
                Token::LParen => depth += 1,
                Token::RParen if depth == 0 => {
                    spec_end = Some(i);
                    break;
                }
                Token::RParen => depth -= 1,
                _ => {}
            }
        }
        let Some(spec_end) = spec_end else { break };
        let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
        out.extend(tokens[..at].iter().cloned());
        match shape {
            Shape::Unbounded => {
                out.push(Token::make_keyword("UNBOUNDED"));
                out.extend(tokens[close + 1..spec_end].iter().cloned());
            }
            Shape::Current => {
                out.push(Token::make_keyword("CURRENT"));
                out.push(Token::Whitespace(sqlparser::tokenizer::Whitespace::Space));
                out.push(Token::make_keyword("ROW"));
                out.extend(tokens[keyword + 1..spec_end].iter().cloned());
            }
            Shape::Offset => {
                out.extend(tokens[open + 1..close].iter().cloned());
                out.extend(tokens[close + 1..spec_end].iter().cloned());
            }
        }
        out.push(Token::Whitespace(sqlparser::tokenizer::Whitespace::Space));
        out.push(Token::make_keyword("EXCLUDE"));
        out.push(Token::Whitespace(sqlparser::tokenizer::Whitespace::Space));
        for (i, part) in kind.sql().split(' ').enumerate() {
            if i > 0 {
                out.push(Token::Whitespace(sqlparser::tokenizer::Whitespace::Space));
            }
            out.push(Token::make_keyword(part));
        }
        out.extend(tokens[spec_end..].iter().cloned());
        tokens = out;
    }
    tokens.iter().map(|t| t.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{parser::Parser, tokenizer::Tokenizer};

    fn roundtrip(sql: &str) -> String {
        let tokens = Tokenizer::new(&crate::dialect::QueryDialect, sql)
            .tokenize_with_location()
            .unwrap();
        let tokens = rewrite(tokens);
        let statements = Parser::new(&crate::dialect::QueryDialect)
            .with_tokens_with_locations(tokens)
            .parse_statements()
            .unwrap();
        display(&statements[0].to_string())
    }

    #[test]
    fn markers_round_trip_through_parse_and_display() {
        for (sql, expected) in [
            (
                "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
                "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
                "SELECT SUM(x) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING exclude group) FROM t",
                "SELECT SUM(x) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY g ROWS (1+1) PRECEDING EXCLUDE GROUP) FROM t",
                "SELECT SUM(x) OVER (ORDER BY g ROWS (1 + 1) PRECEDING EXCLUDE GROUP) FROM t",
            ),
            (
                "SELECT SUM(x) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t",
                "SELECT SUM(x) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING) FROM t",
            ),
            ("SELECT * EXCLUDE (a) FROM t", "SELECT * EXCLUDE (a) FROM t"),
        ] {
            assert_eq!(roundtrip(sql), expected, "{sql}");
        }
    }
}
