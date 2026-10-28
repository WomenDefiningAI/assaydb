//! String functions with the semantics DuckDB 1.5.6 and PostgreSQL 17 share.
//! Positions and counts are Unicode scalar values (code points), as in both
//! references. Where the references disagree, the row is refused with an
//! `unsupported:` message rather than choosing one answer; see SEMANTICS-LOG.md.
use super::*;
use std::fmt::Write as _;

/// LIKE/ILIKE escape handling. `Default` is the absent clause on ILIKE, where
/// PostgreSQL's implicit backslash escape differs from DuckDB's none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Escape {
    Default,
    Disabled,
    Char(char),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Call {
    /// One argument trims U+0020; two arguments trim any listed character.
    Trim {
        leading: bool,
        trailing: bool,
    },
    Replace,
    Translate,
    /// CONCAT: NULL arguments are skipped.
    Concat,
    /// NULL separator propagates; NULL values are skipped.
    ConcatWs,
    /// `||`: NULL if either operand is NULL.
    ConcatStrict,
    Left,
    Right,
    StartsWith,
    Contains,
    Strpos,
    SplitPart,
    Pad {
        left: bool,
    },
    Reverse,
    Like {
        case_insensitive: bool,
        negated: bool,
        escape: Escape,
    },
    /// Regular-expression functions; see `regexp::RegexCall`.
    Regex(regexp::RegexCall),
}

pub const TRIM_SPACES: &str = "unsupported: TRIM of non-ASCII space characters (DuckDB trims Unicode space separators; PostgreSQL trims only U+0020)";
pub const COUNT_RANGE: &str =
    "unsupported: LEFT/RIGHT count outside the 32-bit range (PostgreSQL takes integer)";
pub const SPLIT_ZERO: &str =
    "unsupported: SPLIT_PART field 0 (DuckDB returns an empty string; PostgreSQL errors)";
pub const SPLIT_RANGE: &str =
    "unsupported: SPLIT_PART field outside the 32-bit range (PostgreSQL takes integer)";
pub const SPLIT_EMPTY: &str = "unsupported: SPLIT_PART with an empty delimiter (DuckDB splits characters; PostgreSQL keeps one field)";
pub const PAD_EMPTY: &str =
    "unsupported: LPAD/RPAD with an empty fill (DuckDB errors; PostgreSQL returns the input)";
pub const PAD_RANGE: &str = "LPAD/RPAD length out of range for integer";
pub const REVERSE_JOINING: &str = "unsupported: REVERSE of combining or joining characters (DuckDB reverses grapheme clusters; PostgreSQL reverses code points)";
pub const ILIKE_BACKSLASH: &str = "unsupported: ILIKE pattern with a backslash requires ESCAPE (PostgreSQL's default escape is backslash; DuckDB has none)";
pub const TRAILING_ESCAPE: &str = "LIKE pattern must not end with escape character";

/// Data-dependent string failures, admitted to canonical error replay so the
/// reported error does not depend on worker count or batch size.
pub const RUNTIME_ERRORS: [&str; 12] = [
    TRIM_SPACES,
    COUNT_RANGE,
    SPLIT_ZERO,
    SPLIT_RANGE,
    SPLIT_EMPTY,
    PAD_EMPTY,
    PAD_RANGE,
    REVERSE_JOINING,
    ILIKE_BACKSLASH,
    TRAILING_ESCAPE,
    casts::FLOAT_TEXT_NONFINITE,
    casts::FLOAT_TEXT_NEGATIVE_ZERO,
];

impl Call {
    pub fn result_type(self) -> Type {
        match self {
            Call::StartsWith | Call::Contains | Call::Like { .. } => Type::Bool,
            Call::Strpos => Type::Int,
            Call::Regex(call) => call.result_type(),
            _ => Type::Text,
        }
    }
}

/// Characters, besides U+0020, that DuckDB's one-argument TRIM removes and
/// PostgreSQL's keeps (Unicode space separators). Checked exhaustively
/// against DuckDB in tests/string_functions.rs.
pub fn duckdb_trim_space(c: char) -> bool {
    matches!(
        c,
        '\u{A0}' | '\u{1680}' | '\u{2000}'..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}'
    )
}

/// Code point ranges whose characters each form their own grapheme cluster
/// next to one another, so grapheme and code point reversal coincide.
/// CR is excluded (CR LF is one cluster). Mirrors REVERSE_SAFE in
/// tools/testing/string_functions_reference.py, verified against DuckDB.
pub const REVERSE_SAFE: [(u32, u32); 20] = [
    (0x09, 0x0A),
    (0x20, 0x7E),
    (0xA0, 0xAC),
    (0xAE, 0x2FF),
    (0x370, 0x482),
    (0x48A, 0x52F),
    (0x1E00, 0x1FFF),
    (0x2010, 0x2027),
    (0x2030, 0x205E),
    (0x20A0, 0x20C0),
    (0x3000, 0x3029),
    (0x3030, 0x303F),
    (0x3041, 0x3096),
    (0x309B, 0x30FF),
    (0x4E00, 0x9FFF),
    (0xAC00, 0xD7A3),
    (0xFF01, 0xFF60),
    (0x1F300, 0x1F3FA),
    (0x1F400, 0x1F64F),
    (0x1F680, 0x1F6FF),
];
fn reverse_safe(c: char) -> bool {
    let c = c as u32;
    REVERSE_SAFE
        .iter()
        .any(|(low, high)| (*low..=*high).contains(&c))
}

enum Arg<'a> {
    Text(&'a StringArray),
    Int(&'a Decimal128Array),
    Int64(&'a Int64Array),
    Bool(&'a BooleanArray),
    Null,
}
impl<'a> Arg<'a> {
    fn new(value: &'a Value) -> Result<Self> {
        let array = value.array.as_any();
        Ok(match value.array.data_type() {
            DataType::Utf8 => Self::Text(array.downcast_ref().unwrap()),
            DataType::Decimal128(_, 0) => Self::Int(array.downcast_ref().unwrap()),
            DataType::Int64 => Self::Int64(array.downcast_ref().unwrap()),
            DataType::Boolean => Self::Bool(array.downcast_ref().unwrap()),
            DataType::Null => Self::Null,
            other => return Err(format!("invalid string function argument {other}").into()),
        })
    }
    fn text(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Text(a) => a.is_valid(row).then(|| a.value(row)),
            Self::Bool(a) => a
                .is_valid(row)
                .then(|| if a.value(row) { "true" } else { "false" }),
            _ => None,
        }
    }
    fn int(&self, row: usize) -> Option<i128> {
        match self {
            Self::Int(a) => a.is_valid(row).then(|| a.value(row)),
            Self::Int64(a) => a.is_valid(row).then(|| i128::from(a.value(row))),
            _ => None,
        }
    }
    /// Total payload of the non-NULL text values (an output bound).
    fn bytes(&self, n: usize) -> Result<usize> {
        (0..n).try_fold(0usize, |total, row| {
            total
                .checked_add(self.text(row).map_or(0, str::len))
                .ok_or_else(|| crate::Error::from("resource-limit: string size overflow"))
        })
    }
}

/// Fund the whole output (offsets, validity and `bound` payload bytes) before
/// allocating it; `bound` must not be exceeded by the row writer.
pub(super) fn build(
    n: usize,
    bound: usize,
    budget: &Budget,
    mut row: impl FnMut(usize, &mut StringBuilder) -> Result<()>,
) -> Result<Value> {
    if bound > i32::MAX as usize {
        return Err("resource-limit: string result exceeds 32-bit offsets".into());
    }
    let bytes = workspace(n, 0)?
        .checked_add(bound)
        .ok_or("resource-limit: string size overflow")?;
    let lease = budget.reserve(bytes)?;
    let mut out = StringBuilder::with_capacity(n, bound);
    for i in 0..n {
        row(i, &mut out)?;
    }
    Ok(Value::allocated(Arc::new(out.finish()), lease))
}
fn append(out: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => out.append_value(value),
        None => out.append_null(),
    }
}

fn char_prefix(text: &str, count: usize) -> &str {
    text.char_indices()
        .nth(count)
        .map_or(text, |(index, _)| &text[..index])
}
fn char_suffix_from(text: &str, skip: usize) -> &str {
    text.char_indices()
        .nth(skip)
        .map_or("", |(index, _)| &text[index..])
}
fn left_right(text: &str, count: i128, left: bool) -> Result<&str> {
    if count.unsigned_abs() > i32::MAX as u128 {
        return Err(COUNT_RANGE.into());
    }
    let magnitude = count.unsigned_abs() as usize;
    let total = || text.chars().count();
    Ok(match (left, count >= 0) {
        (true, true) => char_prefix(text, magnitude),
        (true, false) => char_prefix(text, total().saturating_sub(magnitude)),
        (false, true) => char_suffix_from(text, total().saturating_sub(magnitude)),
        (false, false) => char_suffix_from(text, magnitude),
    })
}

pub fn trim_spaces(text: &str, leading: bool, trailing: bool) -> Result<&str> {
    let mut result = text;
    if leading {
        result = result.trim_start_matches(' ');
        if result.chars().next().is_some_and(duckdb_trim_space) {
            return Err(TRIM_SPACES.into());
        }
    }
    if trailing {
        result = result.trim_end_matches(' ');
        if result.chars().next_back().is_some_and(duckdb_trim_space) {
            return Err(TRIM_SPACES.into());
        }
    }
    Ok(result)
}
fn trim_chars<'a>(text: &'a str, set: &str, leading: bool, trailing: bool) -> &'a str {
    let listed = |c: char| set.contains(c);
    let mut result = text;
    if leading {
        result = result.trim_start_matches(listed);
    }
    if trailing {
        result = result.trim_end_matches(listed);
    }
    result
}

fn split_part<'a>(text: &'a str, delimiter: &str, field: i128) -> Result<&'a str> {
    if field == 0 {
        return Err(SPLIT_ZERO.into());
    }
    if field.unsigned_abs() > i32::MAX as u128 {
        return Err(SPLIT_RANGE.into());
    }
    if text.is_empty() {
        // Both references return an empty string for empty input.
        return Ok("");
    }
    if delimiter.is_empty() {
        return Err(SPLIT_EMPTY.into());
    }
    let index = field.unsigned_abs() as usize - 1;
    if field > 0 {
        return Ok(text.split(delimiter).nth(index).unwrap_or(""));
    }
    // Fields are found left to right (non-overlapping) before counting back.
    let count = text.matches(delimiter).count() + 1;
    Ok(if index < count {
        text.split(delimiter).nth(count - 1 - index).unwrap()
    } else {
        ""
    })
}

/// Output byte length of LPAD/RPAD, computed without materializing the fill.
fn pad_len(text: &str, length: i128, fill: &str) -> Result<usize> {
    let length = i32::try_from(length).map_err(|_| PAD_RANGE.to_string())?;
    if length <= 0 {
        return Ok(0);
    }
    let length = length as usize;
    let chars = text.chars().count();
    if length <= chars {
        return Ok(char_prefix(text, length).len());
    }
    let pad = length - chars;
    let fill_chars = fill.chars().count();
    if fill_chars == 0 {
        return Err(PAD_EMPTY.into());
    }
    let cycles = pad / fill_chars;
    cycles
        .checked_mul(fill.len())
        .and_then(|n| n.checked_add(char_prefix(fill, pad % fill_chars).len()))
        .and_then(|n| n.checked_add(text.len()))
        .ok_or_else(|| "resource-limit: string size overflow".into())
}
fn pad_into(out: &mut StringBuilder, text: &str, length: i128, fill: &str, left: bool) {
    let length = length.max(0) as usize;
    let chars = text.chars().count();
    if length <= chars {
        out.append_value(char_prefix(text, length));
        return;
    }
    let pad = length - chars;
    let fill_chars = fill.chars().count();
    let write_pad = |out: &mut StringBuilder| {
        for _ in 0..pad / fill_chars {
            out.write_str(fill).unwrap();
        }
        out.write_str(char_prefix(fill, pad % fill_chars)).unwrap();
    };
    if left {
        write_pad(out);
        out.write_str(text).unwrap();
    } else {
        out.write_str(text).unwrap();
        write_pad(out);
    }
    out.append_value("");
}

fn reverse_into(out: &mut StringBuilder, text: &str) -> Result<()> {
    if !text.is_ascii() && text.chars().any(|c| !reverse_safe(c)) {
        return Err(REVERSE_JOINING.into());
    }
    for c in text.chars().rev() {
        out.write_char(c).unwrap();
    }
    out.append_value("");
    Ok(())
}

#[derive(Debug, PartialEq)]
enum Token {
    Any,
    One,
    Char(char),
}
fn fold(c: char, case_insensitive: bool) -> char {
    if case_insensitive {
        text_case::map_char(c, false)
    } else {
        c
    }
}
fn compile(pattern: &str, escape: Escape, case_insensitive: bool) -> Result<Vec<Token>> {
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if escape == Escape::Char(c) {
            let next = chars.next().ok_or(TRAILING_ESCAPE)?;
            tokens.push(Token::Char(fold(next, case_insensitive)));
            continue;
        }
        if escape == Escape::Default && c == '\\' {
            return Err(ILIKE_BACKSLASH.into());
        }
        match c {
            '%' if tokens.last() == Some(&Token::Any) => {}
            '%' => tokens.push(Token::Any),
            '_' => tokens.push(Token::One),
            c => tokens.push(Token::Char(fold(c, case_insensitive))),
        }
    }
    Ok(tokens)
}
/// Validate a literal pattern at binding, as both references do.
pub(crate) fn validate_pattern(
    pattern: &str,
    escape: Escape,
    case_insensitive: bool,
) -> Result<()> {
    compile(pattern, escape, case_insensitive).map(|_| ())
}
fn matches(text: &str, tokens: &[Token], case_insensitive: bool) -> bool {
    let s: Vec<char> = text.chars().map(|c| fold(c, case_insensitive)).collect();
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while i < s.len() {
        match tokens.get(j) {
            Some(Token::One) => {
                i += 1;
                j += 1;
            }
            Some(Token::Char(c)) if *c == s[i] => {
                i += 1;
                j += 1;
            }
            Some(Token::Any) => {
                star = Some(j);
                j += 1;
                retry = i;
            }
            _ => {
                let Some(k) = star else {
                    return false;
                };
                retry += 1;
                i = retry;
                j = k + 1;
            }
        }
    }
    tokens[j..].iter().all(|t| *t == Token::Any)
}

fn booleans(n: usize, mut row: impl FnMut(usize) -> Result<Option<bool>>) -> Result<ArrayRef> {
    let mut out = BooleanBuilder::with_capacity(n);
    for i in 0..n {
        out.append_option(row(i)?);
    }
    Ok(Arc::new(out.finish()))
}

// A funded sorted character map avoids quadratic rescans of a long FROM
// string for each input code point. Sorting is in place and keeps the first
// occurrence of each FROM character (indices break duplicate-key ties).
fn translated_chars(
    input: &str,
    from: &str,
    to: &str,
    budget: &Budget,
    mut emit: impl FnMut(char) -> Result<()>,
) -> Result<()> {
    let count = from.chars().count();
    let bytes = count
        .checked_mul(std::mem::size_of::<(char, usize, Option<char>)>())
        .ok_or("resource-limit: string size overflow")?;
    let _map_lease = budget.reserve(bytes)?;
    let mut mapping = Vec::with_capacity(count);
    let mut replacements = to.chars();
    for (index, character) in from.chars().enumerate() {
        mapping.push((character, index, replacements.next()));
    }
    mapping.sort_unstable_by_key(|&(character, index, _)| (character, index));
    mapping.dedup_by(|a, b| a.0 == b.0);
    for character in input.chars() {
        let replacement = match mapping.binary_search_by_key(&character, |entry| entry.0) {
            Ok(index) => mapping[index].2,
            Err(_) => Some(character),
        };
        if let Some(character) = replacement {
            emit(character)?;
        }
    }
    Ok(())
}

pub(super) fn evaluate(call: Call, args: &[Expr], input: &Input, budget: &Budget) -> Result<Value> {
    if let Call::Regex(call) = call {
        return regexp::evaluate(call, args, input, budget);
    }
    let n = input.len;
    let values = args
        .iter()
        .map(|a| a.eval(input, budget))
        .collect::<Result<Vec<_>>>()?;
    let a = values.iter().map(Arg::new).collect::<Result<Vec<_>>>()?;
    let text = |i: usize, row: usize| a[i].text(row);
    match call {
        Call::Trim { leading, trailing } => build(n, a[0].bytes(n)?, budget, |row, out| {
            let value = match (text(0, row), a.len()) {
                (Some(s), 1) => Some(trim_spaces(s, leading, trailing)?),
                (Some(s), _) => text(1, row).map(|set| trim_chars(s, set, leading, trailing)),
                (None, _) => None,
            };
            append(out, value);
            Ok(())
        }),
        Call::Replace => {
            let mut bound = 0usize;
            for row in 0..n {
                if let (Some(s), Some(from), Some(to)) = (text(0, row), text(1, row), text(2, row))
                {
                    let size = if from.is_empty() || to.len() <= from.len() {
                        s.len()
                    } else {
                        s.matches(from)
                            .count()
                            .checked_mul(to.len() - from.len())
                            .and_then(|grow| grow.checked_add(s.len()))
                            .ok_or("resource-limit: string size overflow")?
                    };
                    bound = bound
                        .checked_add(size)
                        .ok_or("resource-limit: string size overflow")?;
                }
            }
            build(n, bound, budget, |row, out| {
                match (text(0, row), text(1, row), text(2, row)) {
                    (Some(s), Some(""), Some(_)) => out.append_value(s),
                    (Some(s), Some(from), Some(to)) => {
                        let mut last = 0;
                        for (index, matched) in s.match_indices(from) {
                            out.write_str(&s[last..index]).unwrap();
                            out.write_str(to).unwrap();
                            last = index + matched.len();
                        }
                        out.write_str(&s[last..]).unwrap();
                        out.append_value("");
                    }
                    _ => out.append_null(),
                }
                Ok(())
            })
        }
        Call::Translate => {
            let mut bound = 0usize;
            for row in 0..n {
                if let (Some(s), Some(from), Some(to)) = (text(0, row), text(1, row), text(2, row))
                {
                    translated_chars(s, from, to, budget, |character| {
                        bound = bound
                            .checked_add(character.len_utf8())
                            .ok_or("resource-limit: string size overflow")?;
                        Ok(())
                    })?;
                }
            }
            build(n, bound, budget, |row, out| {
                match (text(0, row), text(1, row), text(2, row)) {
                    (Some(s), Some(from), Some(to)) => {
                        translated_chars(s, from, to, budget, |character| {
                            out.write_char(character).unwrap();
                            Ok(())
                        })?;
                        out.append_value("");
                    }
                    _ => out.append_null(),
                }
                Ok(())
            })
        }
        Call::ConcatWs => {
            let mut bound = 0usize;
            for row in 0..n {
                if let Some(separator) = text(0, row) {
                    let mut seen = false;
                    for i in 1..a.len() {
                        if let Some(value) = text(i, row) {
                            if seen {
                                bound = bound
                                    .checked_add(separator.len())
                                    .ok_or("resource-limit: string size overflow")?;
                            }
                            bound = bound
                                .checked_add(value.len())
                                .ok_or("resource-limit: string size overflow")?;
                            seen = true;
                        }
                    }
                }
            }
            build(n, bound, budget, |row, out| {
                let Some(separator) = text(0, row) else {
                    out.append_null();
                    return Ok(());
                };
                let mut seen = false;
                for i in 1..a.len() {
                    if let Some(value) = text(i, row) {
                        if seen {
                            out.write_str(separator).unwrap();
                        }
                        out.write_str(value).unwrap();
                        seen = true;
                    }
                }
                out.append_value("");
                Ok(())
            })
        }
        Call::Concat | Call::ConcatStrict => {
            let bound = a.iter().try_fold(0usize, |total, arg| {
                total
                    .checked_add(arg.bytes(n)?)
                    .ok_or_else(|| "resource-limit: string size overflow".to_string())
            })?;
            build(n, bound, budget, |row, out| {
                if call == Call::ConcatStrict && (0..a.len()).any(|i| text(i, row).is_none()) {
                    out.append_null();
                    return Ok(());
                }
                for i in 0..a.len() {
                    if let Some(s) = text(i, row) {
                        out.write_str(s).unwrap();
                    }
                }
                out.append_value("");
                Ok(())
            })
        }
        Call::Left | Call::Right => build(n, a[0].bytes(n)?, budget, |row, out| {
            let value = match (text(0, row), a[1].int(row)) {
                (Some(s), Some(count)) => Some(left_right(s, count, call == Call::Left)?),
                _ => None,
            };
            append(out, value);
            Ok(())
        }),
        Call::SplitPart => build(n, a[0].bytes(n)?, budget, |row, out| {
            let value = match (text(0, row), text(1, row), a[2].int(row)) {
                (Some(s), Some(delimiter), Some(field)) => Some(split_part(s, delimiter, field)?),
                _ => None,
            };
            append(out, value);
            Ok(())
        }),
        Call::Pad { left } => {
            let mut bound = 0usize;
            for row in 0..n {
                if let (Some(s), Some(length), Some(fill)) =
                    (text(0, row), a[1].int(row), text(2, row))
                {
                    bound = bound
                        .checked_add(pad_len(s, length, fill)?)
                        .ok_or("resource-limit: string size overflow")?;
                }
            }
            build(n, bound, budget, |row, out| {
                match (text(0, row), a[1].int(row), text(2, row)) {
                    (Some(s), Some(length), Some(fill)) => pad_into(out, s, length, fill, left),
                    _ => out.append_null(),
                }
                Ok(())
            })
        }
        Call::Regex(_) => unreachable!("dispatched to regexp::evaluate"),
        Call::Reverse => build(n, a[0].bytes(n)?, budget, |row, out| match text(0, row) {
            Some(s) => reverse_into(out, s),
            None => {
                out.append_null();
                Ok(())
            }
        }),
        Call::StartsWith | Call::Contains | Call::Strpos | Call::Like { .. } => {
            let lease = budget.reserve(workspace(n, 0)?)?;
            let array: ArrayRef = match call {
                Call::StartsWith => booleans(n, |row| {
                    Ok(text(0, row)
                        .zip(text(1, row))
                        .map(|(s, prefix)| s.starts_with(prefix)))
                })?,
                Call::Contains => booleans(n, |row| {
                    Ok(text(0, row)
                        .zip(text(1, row))
                        .map(|(s, needle)| s.contains(needle)))
                })?,
                Call::Strpos => decimal(
                    (0..n)
                        .map(|row| {
                            text(0, row).zip(text(1, row)).map(|(s, needle)| {
                                s.find(needle)
                                    .map_or(0, |i| s[..i].chars().count() as i128 + 1)
                            })
                        })
                        .collect(),
                    0,
                )?,
                Call::Like {
                    case_insensitive,
                    negated,
                    escape,
                } => {
                    let constant = match &args[1].kind {
                        Kind::Literal(Literal::Text(p)) if escape != Escape::Null => {
                            Some(compile(p, escape, case_insensitive)?)
                        }
                        _ => None,
                    };
                    booleans(n, |row| {
                        let (Some(s), Some(p)) = (text(0, row), text(1, row)) else {
                            return Ok(None);
                        };
                        if escape == Escape::Null {
                            return Ok(None);
                        }
                        let matched = match &constant {
                            Some(tokens) => matches(s, tokens, case_insensitive),
                            None => {
                                matches(s, &compile(p, escape, case_insensitive)?, case_insensitive)
                            }
                        };
                        Ok(Some(matched != negated))
                    })?
                }
                _ => unreachable!(),
            };
            Ok(Value::allocated(array, lease))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_tokens_match_reference_like_without_escape() {
        for (text, pattern) in [
            ("abc", "a%"),
            ("abc", "%c"),
            ("abc", "a_c"),
            ("abc", "%%"),
            ("", "%"),
            ("", "_"),
            ("é🦀a", "_🦀%"),
            ("aXbXc", "%X%X%"),
            ("mississippi", "%iss%ppi"),
        ] {
            let tokens = compile(pattern, Escape::Disabled, false).unwrap();
            assert_eq!(
                matches(text, &tokens, false),
                like(text, pattern),
                "{text} {pattern}"
            );
        }
        assert_eq!(
            compile("a#", Escape::Char('#'), false).err().unwrap(),
            TRAILING_ESCAPE
        );
        assert!(
            compile("a\\b", Escape::Default, true)
                .err()
                .unwrap()
                .starts_with("unsupported:")
        );
    }

    #[test]
    fn padding_lengths_are_exact_without_materializing() {
        assert_eq!(pad_len("abc", 6, "xy").unwrap(), 6);
        assert_eq!(pad_len("é", 3, "ü").unwrap(), 6);
        assert_eq!(pad_len("abc", 2, "").unwrap(), 2);
        assert_eq!(pad_len("abc", -1, "").unwrap(), 0);
        assert_eq!(
            pad_len("abc", i32::MAX as i128, "ab").unwrap(),
            i32::MAX as usize
        );
        assert_eq!(pad_len("abc", 5, "").err().unwrap(), PAD_EMPTY);
        assert_eq!(pad_len("abc", 1 << 31, "x").err().unwrap(), PAD_RANGE);
    }

    #[test]
    fn outputs_are_funded_and_released() {
        let budget = Budget::new(1 << 20);
        let input = Input {
            columns: vec![Some(
                Value::adopt(
                    Arc::new(StringArray::from(vec![Some("abc"), None, Some("é")])),
                    &budget,
                )
                .unwrap(),
            )],
            len: 3,
        };
        let s = Expr {
            kind: Kind::Column(0),
            ty: Type::Text,
        };
        let fill = Expr {
            kind: Kind::Literal(Literal::Text("xy".into())),
            ty: Type::Text,
        };
        let big = Expr {
            kind: Kind::Literal(Literal::Num(1 << 30)),
            ty: Type::Int,
        };
        let before = budget.used();
        let error = evaluate(
            Call::Pad { left: true },
            &[s.clone(), big, fill.clone()],
            &input,
            &budget,
        )
        .err()
        .unwrap();
        assert!(error.starts_with("resource-limit:"), "{error}");
        assert_eq!(budget.used(), before);
        let five = Expr {
            kind: Kind::Literal(Literal::Num(5)),
            ty: Type::Int,
        };
        let value = evaluate(Call::Pad { left: false }, &[s, five, fill], &input, &budget).unwrap();
        let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(0), "abcxy");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "éxyxy");
        drop(value);
        assert_eq!(budget.used(), before);
    }
}
