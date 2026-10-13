//! Catalog-only REINDEX for native nonunique indexes; no physical index exists.
use super::*;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

impl Session {
    /// Recognize only REINDEX here; normal SQL continues through the AST parser.
    pub(super) fn try_reindex(&mut self, sql: &str) -> Option<Result<Output>> {
        if sql.len() > MAX_SQL_BYTES {
            return Some(Err(resource("SQL exceeds 1 MiB")));
        }
        if !starts_reindex(sql) {
            return None;
        }
        Some(self.reindex(sql))
    }

    fn reindex(&mut self, sql: &str) -> Result<Output> {
        self.options.cancellation.check().map_err(resource)?;
        // Each byte can produce at most one token. Vec growth is bounded by
        // twice that count; text/identifier copies get four bytes per input byte.
        let bytes = sql
            .len()
            .checked_mul(
                std::mem::size_of::<TokenWithSpan>()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(4))
                    .ok_or_else(|| resource("REINDEX token metadata overflow"))?,
            )
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| resource("REINDEX token metadata overflow"))?;
        let _tokens_credit = self.budget.reserve(bytes).map_err(resource)?;
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize_with_location()
            .map_err(|e| unsupported(format!("SQL tokenizer: {e}")))?;
        let mut tokens = tokens
            .iter()
            .map(|t| &t.token)
            .filter(|t| !matches!(t, Token::Whitespace(_)));
        if !matches!(tokens.next(), Some(Token::Word(w))
            if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("REINDEX"))
        {
            return Err(unsupported("REINDEX requires an unquoted command"));
        }
        let target = match tokens.next() {
            None => None,
            Some(Token::SemiColon) if tokens.next().is_none() => None,
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                let target = ident(&s::Ident::new(&word.value))?;
                match tokens.next() {
                    None => {}
                    Some(Token::SemiColon) if tokens.next().is_none() => {}
                    _ => {
                        return Err(unsupported(
                            "REINDEX supports one unqualified table or index name",
                        ));
                    }
                }
                Some(target)
            }
            _ => {
                return Err(unsupported(
                    "REINDEX supports one unquoted table or index name",
                ));
            }
        };
        if let Some(target) = target
            && !self.tables.contains_key(&target)
            && !self.indexes.contains_key(&target)
        {
            return Err(semantic(format!("unknown table or index: {target}")));
        }
        self.options.cancellation.check().map_err(resource)?;
        Ok(Output::Statement { affected_rows: 0 })
    }
}

/// Cheap candidate classification only. The tokenizer below validates all syntax.
/// This avoids tokenizing every ordinary query twice, including comment-prefixed SQL.
fn starts_reindex(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut pos = 0;
    loop {
        while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
            pos += 1;
        }
        if bytes.get(pos..pos + 2) == Some(b"--") {
            pos += 2;
            while bytes.get(pos).is_some_and(|b| *b != b'\n' && *b != b'\r') {
                pos += 1;
            }
        } else if bytes.get(pos..pos + 2) == Some(b"/*") {
            pos += 2;
            let mut depth = 1usize;
            while pos < bytes.len() && depth != 0 {
                if bytes.get(pos..pos + 2) == Some(b"/*") {
                    depth += 1;
                    pos += 2;
                } else if bytes.get(pos..pos + 2) == Some(b"*/") {
                    depth -= 1;
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            if depth != 0 {
                return false;
            }
        } else {
            break;
        }
    }
    bytes
        .get(pos..pos + 7)
        .is_some_and(|word| word.eq_ignore_ascii_case(b"REINDEX"))
        && !bytes
            .get(pos + 7)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || !b.is_ascii())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_funding_and_cancellation_leave_catalog_untouched() {
        let options = Options::default();
        let cancel = options.cancellation.clone();
        let mut s = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
        s.execute_sql("CREATE TABLE t(x INT)").unwrap();
        let used = s.budget.used();
        let credit = s.budget.reserve(s.budget.limit() - used).unwrap();
        assert_eq!(
            s.try_reindex("REINDEX t").unwrap().err().unwrap().kind,
            "resource"
        );
        drop(credit);
        assert_eq!(s.budget.used(), used);
        cancel.cancel();
        assert_eq!(
            s.try_reindex("REINDEX t").unwrap().err().unwrap().kind,
            "resource"
        );
        assert_eq!(s.budget.used(), used);
    }
    #[test]
    fn unrelated_sql_is_not_intercepted() {
        for sql in [
            "SELECT 1",
            "/* REINDEX */ SELECT 1",
            "-- REINDEX\nSELECT 1",
            "REINDEXED t",
            "'REINDEX'",
            "/* unterminated",
        ] {
            assert!(!starts_reindex(sql), "{sql}");
        }
        for sql in [
            "REINDEX",
            "reindex t",
            "/* outer /* inner */ */ -- c\n REINDEX t;",
        ] {
            assert!(starts_reindex(sql), "{sql}");
        }
    }
}
