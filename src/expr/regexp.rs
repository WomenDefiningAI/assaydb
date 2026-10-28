//! Regular-expression functions, restricted to the pattern subset on which
//! DuckDB 1.5.6 (RE2) and PostgreSQL 17 (Spencer's ARE engine) give the same
//! answer (SEMANTICS-LOG.md, October 18, 2026).
//!
//! A pattern is parsed here, never by the matcher: constructs outside the
//! subset are refused with `unsupported:` (or, where both references reject
//! the pattern, an `invalid regular expression` error), and the accepted
//! pattern is re-emitted in `regex-automata` syntax with DuckDB's meaning
//! (ASCII `\d \w \s`, `.` excluding newline unless the `s` option is given).
//!
//! Functions PostgreSQL also defines (REGEXP_LIKE, `~*`, SIMILAR TO,
//! REGEXP_REPLACE) additionally refuse the rows where PostgreSQL's answer
//! would differ: classes or case folding on non-ASCII text, `\s` on a vertical
//! tab, `.` on text with a newline, and REGEXP_REPLACE matches whose extent
//! PostgreSQL's longest-match rule would choose differently. DuckDB-only
//! functions (REGEXP_MATCHES, REGEXP_FULL_MATCH, REGEXP_EXTRACT) follow DuckDB
//! and refuse only case-insensitive matching of non-ASCII text.
use super::*;
use regex_automata::{
    Anchored, Input as Haystack, MatchKind, hybrid, meta,
    nfa::thompson::{self, WhichCaptures},
    util::{captures::Captures, syntax},
};
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::{LazyLock, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RegexOp {
    /// DuckDB REGEXP_MATCHES: partial match, BOOLEAN.
    Matches,
    /// PostgreSQL REGEXP_LIKE and `~*` / `!~*`: partial match, BOOLEAN.
    Like,
    /// DuckDB REGEXP_FULL_MATCH.
    FullMatch,
    /// SQL SIMILAR TO (full match over the shared SIMILAR subset).
    Similar,
    /// REGEXP_REPLACE: first match, or every match with the `g` option.
    Replace { global: bool },
    /// DuckDB REGEXP_EXTRACT(s, p, group).
    Extract { group: u8 },
}

/// Arguments: text, pattern, and for `Replace` the replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RegexCall {
    pub op: RegexOp,
    pub negated: bool,
    pub case_insensitive: bool,
    /// The `s` option: `.` also matches a newline (both references).
    pub dot_newline: bool,
}

impl RegexCall {
    pub fn result_type(self) -> Type {
        match self.op {
            RegexOp::Replace { .. } | RegexOp::Extract { .. } => Type::Text,
            _ => Type::Bool,
        }
    }
    /// Functions both references define with this meaning (or PostgreSQL
    /// spellings equivalent to a DuckDB function); their rows must also
    /// agree with PostgreSQL.
    fn shared(self) -> bool {
        matches!(
            self.op,
            RegexOp::Like | RegexOp::Similar | RegexOp::Replace { .. }
        )
    }
    fn full(self) -> bool {
        matches!(self.op, RegexOp::FullMatch | RegexOp::Similar)
    }
    fn longest(self) -> bool {
        matches!(self.op, RegexOp::Replace { .. })
    }
}

pub const CASE_FOLD: &str = "unsupported: case-insensitive regular expression on non-ASCII text (DuckDB folds case by Unicode tables; PostgreSQL by the database locale)";
pub const CASE_FOLD_PATTERN: &str = "unsupported: case-insensitive regular expression with non-ASCII pattern characters (case folding outside ASCII differs between the engines)";
pub const CLASS_NON_ASCII: &str = "unsupported: regular expression character class on non-ASCII text (DuckDB's classes are ASCII; PostgreSQL's follow the database locale)";
pub const VERTICAL_TAB: &str = "unsupported: regular expression \\s on a vertical tab (PostgreSQL's \\s matches it; DuckDB's does not)";
pub const NEWLINE: &str = "unsupported: regular expression '.' on text containing a newline (DuckDB's '.' excludes newline, PostgreSQL's matches it; the 's' option makes both match it)";
pub const EXTENT: &str = "unsupported: regular expression match extent differs (DuckDB takes the first matching alternative, PostgreSQL the longest match)";
pub const EMPTY_AFTER: &str = "unsupported: global regular expression replacement of an empty match right after a match (DuckDB skips it; PostgreSQL replaces it)";
pub const PATTERN: &str =
    "unsupported: regular expression outside the subset DuckDB and PostgreSQL share";
pub const SIMILAR_PATTERN: &str =
    "unsupported: SIMILAR TO pattern outside the subset DuckDB and PostgreSQL share";
pub const INVALID: &str = "invalid regular expression";
pub const REPLACEMENT: &str = "unsupported: regular expression replacement other than \\1-\\9 and \\\\ (DuckDB and PostgreSQL read it differently)";
pub const MISSING_GROUP: &str = "unsupported: replacement refers to a group the pattern lacks (DuckDB leaves the text unchanged; PostgreSQL inserts nothing)";
pub const GROUP_REFERENCE: &str = "unsupported: group reference with alternation, a lazy quantifier or a repeated group (DuckDB and PostgreSQL choose different submatches)";
pub const LAZY_REPLACE: &str = "unsupported: lazy quantifier in REGEXP_REPLACE (PostgreSQL makes the whole match non-greedy; DuckDB only that quantifier)";
pub const TOO_LARGE: &str = "resource-limit: regular expression too large";

/// Data-dependent refusals and errors, admitted to canonical error replay so
/// the reported row does not depend on worker count or batch size.
pub const RUNTIME_ERRORS: [&str; 14] = [
    CASE_FOLD,
    CASE_FOLD_PATTERN,
    CLASS_NON_ASCII,
    VERTICAL_TAB,
    NEWLINE,
    EXTENT,
    EMPTY_AFTER,
    PATTERN,
    SIMILAR_PATTERN,
    INVALID,
    REPLACEMENT,
    MISSING_GROUP,
    GROUP_REFERENCE,
    LAZY_REPLACE,
];

/// Longest accepted pattern, in bytes.
pub const PATTERN_LIMIT: usize = 16 << 10;
const NEST_LIMIT: usize = 200;
const NFA_LIMIT: usize = 4 << 20;
const HYBRID_CACHE: usize = 512 << 10;
const BACKTRACK_VISITED: usize = 256 << 10;
const STORE_ENTRIES: usize = 256;
const STORE_BYTES: usize = 64 << 20;
/// Distinct non-constant patterns one batch keeps checked out at once.
const BATCH_SLOTS: usize = 8;

/// Why a pattern is not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PatternError {
    /// Outside the shared subset; the detail names the construct.
    Unsupported(String),
    /// Both references reject the pattern.
    Invalid(String),
    /// A non-ASCII letter in a case-insensitive pattern.
    CaseFoldPattern,
    TooLarge,
}
impl PatternError {
    /// The message for a constant pattern, reported while binding.
    pub(crate) fn bind_error(&self, similar: bool) -> crate::Error {
        match self {
            Self::Unsupported(detail) if similar => {
                format!("unsupported: SIMILAR TO pattern {detail}").into()
            }
            Self::Unsupported(detail) => format!("unsupported: regular expression {detail}").into(),
            Self::Invalid(detail) => format!("{INVALID}: {detail}").into(),
            Self::CaseFoldPattern => CASE_FOLD_PATTERN.into(),
            Self::TooLarge => TOO_LARGE.into(),
        }
    }
    /// The constant message for a pattern read from data.
    fn runtime_error(&self, similar: bool) -> crate::Error {
        match self {
            Self::Unsupported(_) if similar => SIMILAR_PATTERN.into(),
            Self::Unsupported(_) => PATTERN.into(),
            Self::Invalid(_) => INVALID.into(),
            Self::CaseFoldPattern => CASE_FOLD_PATTERN.into(),
            Self::TooLarge => TOO_LARGE.into(),
        }
    }
}

/// What the parser learned about an accepted pattern.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Info {
    /// An unescaped `.` outside brackets.
    has_dot: bool,
    /// `\d \D \w \W \s \S` or a POSIX class.
    locale: bool,
    /// `\s` or `\S`.
    vertical_tab: bool,
    non_ascii: bool,
    pub(crate) groups: usize,
    alternation: bool,
    lazy: bool,
    /// Alternation or a variable-count quantifier: matches starting at one
    /// position may have different lengths.
    ambiguous: bool,
    /// A capture group that is quantified or inside a quantified group.
    repeated_capture: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Prev {
    /// Start of the pattern, a group or a branch.
    Start,
    Atom {
        capture: bool,
        product: u32,
    },
    Anchor,
    Quantified,
}

struct Frame {
    capture: bool,
    product: u32,
}

const POSIX: [&str; 12] = [
    "alpha", "digit", "alnum", "upper", "lower", "space", "punct", "xdigit", "blank", "cntrl",
    "print", "graph",
];
/// Escapes both DuckDB (RE2) and PostgreSQL reject in any context.
const BOTH_ERROR_ESCAPES: &str = "ghijkloqEFGHIJKLNORTVX";

fn unsupported(detail: impl Into<String>) -> PatternError {
    PatternError::Unsupported(detail.into())
}
fn invalid(detail: impl Into<String>) -> PatternError {
    PatternError::Invalid(detail.into())
}
fn ascii_punct(c: char) -> bool {
    c == ' ' || c.is_ascii_punctuation()
}
fn literal(out: &mut String, c: char) {
    if c.is_ascii_alphanumeric() || !c.is_ascii() {
        out.push(c);
    } else {
        write!(out, "\\x{{{:X}}}", c as u32).unwrap();
    }
}
fn escaped_char(e: char) -> Option<char> {
    Some(match e {
        't' => '\t',
        'n' => '\n',
        'r' => '\r',
        'f' => '\x0C',
        'v' => '\x0B',
        _ => return None,
    })
}
/// DuckDB's ASCII class for `\d \w \s`, as bracket contents.
fn class_body(e: char) -> &'static str {
    match e.to_ascii_lowercase() {
        'd' => "0-9",
        'w' => "0-9A-Za-z_",
        _ => "\\x{9}\\x{A}\\x{C}\\x{D}\\x{20}",
    }
}

/// Parse `pattern` (SIMILAR TO syntax when `similar`) into the matcher's
/// syntax. The grammar and every rule mirror `parse` in
/// tools/testing/regexp_reference.py.
pub(crate) fn translate(
    pattern: &str,
    similar: bool,
) -> std::result::Result<(String, Info), PatternError> {
    if pattern.len() > PATTERN_LIMIT {
        return Err(PatternError::TooLarge);
    }
    if pattern.starts_with("***") {
        return Err(unsupported("director ***"));
    }
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut info = Info::default();
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut stack: Vec<Frame> = Vec::new();
    let mut top = Frame {
        capture: false,
        product: 1,
    };
    let mut prev = Prev::Start;
    let mut i = 0;
    fn atom(prev: &mut Prev, stack: &mut [Frame], top: &mut Frame, capture: bool, product: u32) {
        *prev = Prev::Atom { capture, product };
        let frame = stack.last_mut().unwrap_or(top);
        frame.capture |= capture;
        frame.product = frame.product.max(product);
    }
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                if similar {
                    return Err(unsupported("with \\"));
                }
                let Some(&e) = chars.get(i + 1) else {
                    return Err(invalid("trailing backslash"));
                };
                i += 2;
                if "dDwWsS".contains(e) {
                    info.locale = true;
                    info.vertical_tab |= e == 's' || e == 'S';
                    let negated = if e.is_ascii_uppercase() { "^" } else { "" };
                    write!(out, "[{negated}{}]", class_body(e)).unwrap();
                    atom(&mut prev, &mut stack, &mut top, false, 1);
                } else if let Some(c) = escaped_char(e) {
                    literal(&mut out, c);
                    atom(&mut prev, &mut stack, &mut top, false, 1);
                } else if e == 'A' {
                    if prev != Prev::Start {
                        return Err(unsupported("anchor inside a branch"));
                    }
                    out.push_str("\\A");
                    prev = Prev::Anchor;
                } else if e.is_ascii() && ascii_punct(e) {
                    literal(&mut out, e);
                    atom(&mut prev, &mut stack, &mut top, false, 1);
                } else if BOTH_ERROR_ESCAPES.contains(e) {
                    return Err(invalid(format!("invalid escape \\{e}")));
                } else {
                    return Err(unsupported(format!("escape \\{e}")));
                }
                continue;
            }
            '[' => {
                i = bracket(&chars, i + 1, &mut info, &mut out, similar)?;
                atom(&mut prev, &mut stack, &mut top, false, 1);
                continue;
            }
            '(' => {
                if chars.get(i + 1) == Some(&'?') {
                    if similar || chars.get(i + 2) != Some(&':') {
                        return Err(unsupported("group (?"));
                    }
                    out.push_str("(?:");
                    stack.push(Frame {
                        capture: false,
                        product: 1,
                    });
                    i += 3;
                } else {
                    info.groups += 1;
                    out.push('(');
                    stack.push(Frame {
                        capture: true,
                        product: 1,
                    });
                    i += 1;
                }
                if stack.len() > NEST_LIMIT {
                    return Err(unsupported(format!(
                        "nesting deeper than {NEST_LIMIT} groups"
                    )));
                }
                prev = Prev::Start;
                continue;
            }
            ')' => {
                let Some(frame) = stack.pop() else {
                    return Err(invalid("unbalanced )"));
                };
                out.push(')');
                i += 1;
                atom(
                    &mut prev,
                    &mut stack,
                    &mut top,
                    frame.capture,
                    frame.product,
                );
                continue;
            }
            '|' => {
                info.alternation = true;
                info.ambiguous = true;
                out.push('|');
                prev = Prev::Start;
                i += 1;
                continue;
            }
            '*' | '+' | '?' | '{' => {
                let (end, low, high) = if c == '{' {
                    braces(&chars, i).ok_or_else(|| unsupported("malformed braces"))?
                } else {
                    let low = u32::from(c == '+');
                    (i + 1, low, (c == '?').then_some(1))
                };
                let (capture, product) = match prev {
                    Prev::Start => return Err(invalid("quantifier without operand")),
                    Prev::Quantified => return Err(invalid("double quantifier")),
                    Prev::Anchor => return Err(unsupported("quantified anchor")),
                    Prev::Atom { capture, product } => (capture, product),
                };
                if c == '{' {
                    if high.is_some_and(|high| low > high) {
                        return Err(invalid("repetition count range reversed"));
                    }
                    let biggest = low.max(high.unwrap_or(0));
                    if biggest > 1000 {
                        return Err(invalid("repetition count above 1000"));
                    }
                    if biggest > 255 {
                        return Err(unsupported("repetition count above 255"));
                    }
                    let count = high.unwrap_or(low);
                    let product = if count > 0 { product * count } else { product };
                    if product > 1000 {
                        return Err(unsupported("nested repetition counts above 1000"));
                    }
                    let frame = stack.last_mut().unwrap_or(&mut top);
                    frame.product = frame.product.max(product);
                    match high {
                        Some(high) if high == low => write!(out, "{{{low}}}").unwrap(),
                        Some(high) => write!(out, "{{{low},{high}}}").unwrap(),
                        None => write!(out, "{{{low},}}").unwrap(),
                    }
                } else {
                    out.push(c);
                }
                if high != Some(low) {
                    info.ambiguous = true;
                }
                info.repeated_capture |= capture;
                i = end;
                if chars.get(i) == Some(&'?') {
                    if similar {
                        return Err(unsupported("with a lazy quantifier"));
                    }
                    info.lazy = true;
                    out.push('?');
                    i += 1;
                }
                prev = Prev::Quantified;
                continue;
            }
            '.' => {
                if similar {
                    return Err(unsupported("with ."));
                }
                info.has_dot = true;
                out.push('.');
                atom(&mut prev, &mut stack, &mut top, false, 1);
            }
            '^' | '$' => {
                if similar {
                    return Err(unsupported(format!("with {c}")));
                }
                // DuckDB rewrites literal-like patterns and then ignores an
                // anchor inside a branch ('a^b' matches 'ab'); only branch
                // edges are accepted.
                let edge = if c == '^' {
                    prev == Prev::Start
                } else {
                    chars.get(i + 1).is_none_or(|n| *n == ')' || *n == '|')
                };
                if !edge {
                    return Err(unsupported("anchor inside a branch"));
                }
                out.push(c);
                prev = Prev::Anchor;
            }
            '}' | ']' => return Err(unsupported(format!("unescaped {c}"))),
            '%' | '_' if similar => return Err(unsupported(format!("with {c}"))),
            c => {
                info.non_ascii |= !c.is_ascii();
                literal(&mut out, c);
                atom(&mut prev, &mut stack, &mut top, false, 1);
            }
        }
        i += 1;
    }
    if !stack.is_empty() {
        return Err(invalid("unbalanced ("));
    }
    Ok((out, info))
}

/// `{m}`, `{m,}` or `{m,n}` at `i`: (index after `}`, m, n).
fn braces(chars: &[char], i: usize) -> Option<(usize, u32, Option<u32>)> {
    let number = |mut j: usize| {
        let start = j;
        let mut value = 0u32;
        while let Some(d) = chars.get(j).and_then(|c| c.to_digit(10)) {
            if !chars[j].is_ascii_digit() {
                break;
            }
            value = value.saturating_mul(10).saturating_add(d).min(100_000);
            j += 1;
        }
        (j > start).then_some((j, value))
    };
    let (mut j, low) = number(i + 1)?;
    let mut high = Some(low);
    if chars.get(j) == Some(&',') {
        match number(j + 1) {
            Some((k, value)) => {
                j = k;
                high = Some(value);
            }
            None => {
                j += 1;
                high = None;
            }
        }
    }
    (chars.get(j) == Some(&'}')).then_some((j + 1, low, high))
}

#[derive(Clone, Copy, PartialEq)]
enum Item {
    Char(char),
    Class,
    Range,
}

/// A bracket expression whose `[` precedes `i`; returns the index after `]`.
fn bracket(
    chars: &[char],
    mut i: usize,
    info: &mut Info,
    out: &mut String,
    similar: bool,
) -> std::result::Result<usize, PatternError> {
    out.push('[');
    if chars.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    let mut first = true;
    let mut last: Option<Item> = None;
    loop {
        let Some(&c) = chars.get(i) else {
            return Err(invalid("unterminated ["));
        };
        if c == ']' && !first {
            out.push(']');
            return Ok(i + 1);
        }
        if c == ']' {
            literal(out, ']');
            last = Some(Item::Char(']'));
            i += 1;
            first = false;
            continue;
        }
        if similar && (c == '%' || c == '_') {
            return Err(unsupported(format!("with {c}")));
        }
        if c == '-' && !first && chars.get(i + 1) != Some(&']') {
            let Some(Item::Char(low)) = last else {
                return Err(unsupported("hyphen after a class or range in brackets"));
            };
            let (j, item) = bracket_item(chars, i + 1, info, &mut String::new(), similar)?;
            let Item::Char(high) = item else {
                return Err(unsupported("class as a range end"));
            };
            if !low.is_ascii() || !high.is_ascii() {
                return Err(unsupported("non-ASCII range"));
            }
            if low > high {
                return Err(invalid("reversed range"));
            }
            out.push('-');
            literal(out, high);
            last = Some(Item::Range);
            i = j;
            first = false;
            continue;
        }
        let (j, item) = bracket_item(chars, i, info, out, similar)?;
        last = Some(item);
        i = j;
        first = false;
    }
}

fn bracket_item(
    chars: &[char],
    i: usize,
    info: &mut Info,
    out: &mut String,
    similar: bool,
) -> std::result::Result<(usize, Item), PatternError> {
    let Some(&c) = chars.get(i) else {
        return Err(invalid("unterminated ["));
    };
    match c {
        '[' => {
            if chars.get(i + 1) != Some(&':') {
                return Err(unsupported("[ inside brackets"));
            }
            let rest = &chars[i + 2..];
            let Some(close) = rest.windows(2).position(|w| w == [':', ']']) else {
                return Err(unsupported("[: without :]"));
            };
            let name = rest[..close].iter().collect::<String>();
            if !POSIX.contains(&name.as_str()) {
                return Err(unsupported(format!("class [[:{name}:]]")));
            }
            info.locale = true;
            write!(out, "[:{name}:]").unwrap();
            Ok((i + 2 + close + 2, Item::Class))
        }
        '\\' => {
            if similar {
                return Err(unsupported("with \\"));
            }
            let Some(&e) = chars.get(i + 1) else {
                return Err(invalid("unterminated ["));
            };
            if "dDwWsS".contains(e) {
                info.locale = true;
                info.vertical_tab |= e == 's' || e == 'S';
                if e.is_ascii_uppercase() {
                    write!(out, "[^{}]", class_body(e)).unwrap();
                } else {
                    out.push_str(class_body(e));
                }
                Ok((i + 2, Item::Class))
            } else if let Some(c) = escaped_char(e) {
                literal(out, c);
                Ok((i + 2, Item::Char(c)))
            } else if e.is_ascii() && ascii_punct(e) {
                literal(out, e);
                Ok((i + 2, Item::Char(e)))
            } else {
                Err(unsupported(format!("bracket escape \\{e}")))
            }
        }
        c => {
            info.non_ascii |= !c.is_ascii();
            literal(out, c);
            Ok((i + 1, Item::Char(c)))
        }
    }
}

/// Parsed replacement text: literal pieces and `\1`-`\9` references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Template {
    pieces: Vec<Piece>,
    max_group: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Text(String),
    Group(usize),
}
impl Template {
    pub(crate) fn parse(text: &str) -> std::result::Result<Self, &'static str> {
        let mut pieces = Vec::new();
        let mut current = String::new();
        let mut chars = text.chars();
        let mut max_group = 0;
        while let Some(c) = chars.next() {
            if c != '\\' {
                current.push(c);
                continue;
            }
            match chars.next() {
                Some('\\') => current.push('\\'),
                Some(d @ '1'..='9') => {
                    if !current.is_empty() {
                        pieces.push(Piece::Text(std::mem::take(&mut current)));
                    }
                    let group = d as usize - '0' as usize;
                    max_group = max_group.max(group);
                    pieces.push(Piece::Group(group));
                }
                _ => return Err(REPLACEMENT),
            }
        }
        if !current.is_empty() {
            pieces.push(Piece::Text(current));
        }
        Ok(Self { pieces, max_group })
    }
    /// Check the references against the pattern they will read.
    pub(crate) fn check(&self, info: &Info) -> std::result::Result<(), &'static str> {
        if self.max_group > info.groups {
            return Err(MISSING_GROUP);
        }
        if self.max_group > 0 && (info.alternation || info.lazy || info.repeated_capture) {
            return Err(GROUP_REFERENCE);
        }
        Ok(())
    }
    fn literal_text(&self) -> Option<&str> {
        match self.pieces.as_slice() {
            [] => Some(""),
            [Piece::Text(text)] => Some(text),
            _ => None,
        }
    }
}

/// Options as the references read them. `function` names who defines the
/// function: an option letter neither reference accepts is an error, one
/// that a reference accepts with another meaning is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Definer {
    DuckDb,
    PostgreSql,
    Both,
}
pub(crate) fn options(
    text: &str,
    definer: Definer,
    replace: bool,
) -> crate::Result<(bool, bool, bool)> {
    let (mut ci, mut dot, mut global) = (false, false, false);
    for c in text.chars() {
        match c {
            'i' => ci = true,
            'c' => ci = false,
            's' => dot = true,
            'g' if replace => global = true,
            _ => {
                let duckdb = "cilmnpgs".contains(c);
                let postgresql = "bcegimnpqstwx".contains(c);
                let accepted = match definer {
                    Definer::DuckDb => duckdb && c != 'g',
                    Definer::PostgreSql => postgresql && c != 'g',
                    Definer::Both => duckdb || postgresql,
                };
                return Err(if accepted {
                    format!("unsupported: regular expression option {c} (the references give it different meanings)").into()
                } else {
                    format!("invalid regular expression option {c:?}").into()
                });
            }
        }
    }
    Ok((ci, dot, global))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    pattern: String,
    similar: bool,
    full: bool,
    ci: bool,
    dot: bool,
    longest: bool,
}

pub(crate) struct Compiled {
    regex: meta::Regex,
    /// PostgreSQL's choice for a greedy pattern: the longest match from a
    /// start (built only for REGEXP_REPLACE of ambiguous patterns).
    longest: Option<hybrid::dfa::DFA>,
    pub(crate) info: Info,
    caches: Mutex<Vec<meta::Cache>>,
    longest_caches: Mutex<Vec<hybrid::dfa::Cache>>,
    /// Bytes charged to a query while it uses this pattern.
    bytes: usize,
}

struct Store {
    map: HashMap<Key, Arc<Compiled>>,
    order: VecDeque<Key>,
    bytes: usize,
}
static STORE: LazyLock<Mutex<Store>> = LazyLock::new(|| {
    Mutex::new(Store {
        map: HashMap::new(),
        order: VecDeque::new(),
        bytes: 0,
    })
});

/// The translated pattern is valid matcher syntax by construction, so a
/// build failure is a size limit (NFA size, cache capacity or nesting).
fn meta_error(error: &meta::BuildError) -> PatternError {
    debug_assert!(error.syntax_error().is_none(), "translation bug: {error}");
    PatternError::TooLarge
}

fn compile(key: &Key) -> std::result::Result<Compiled, PatternError> {
    let (body, info) = translate(&key.pattern, key.similar)?;
    if key.ci && info.non_ascii {
        return Err(PatternError::CaseFoldPattern);
    }
    let mut pattern = String::with_capacity(body.len() + 16);
    if key.dot {
        pattern.push_str("(?s)");
    }
    if key.ci {
        pattern.push_str("(?i)");
    }
    if key.full {
        write!(pattern, "\\A(?:{body})\\z").unwrap();
    } else {
        pattern.push_str(&body);
    }
    let syntax = syntax::Config::new().nest_limit(1000);
    let regex = meta::Regex::builder()
        .syntax(syntax)
        .configure(
            meta::Config::new()
                .nfa_size_limit(Some(NFA_LIMIT))
                .hybrid_cache_capacity(HYBRID_CACHE),
        )
        .build(&pattern)
        .map_err(|e| meta_error(&e))?;
    let longest = if key.longest && info.ambiguous {
        Some(
            hybrid::dfa::Builder::new()
                .syntax(syntax)
                .thompson(
                    thompson::Config::new()
                        .nfa_size_limit(Some(NFA_LIMIT))
                        .which_captures(WhichCaptures::None),
                )
                .configure(
                    hybrid::dfa::Config::new()
                        .match_kind(MatchKind::All)
                        .cache_capacity(HYBRID_CACHE),
                )
                .build(&pattern)
                .map_err(|_| PatternError::TooLarge)?,
        )
    } else {
        None
    };
    let bytes = regex.memory_usage()
        + 2 * HYBRID_CACHE
        + BACKTRACK_VISITED
        + longest
            .as_ref()
            .map_or(0, |d| d.memory_usage() + HYBRID_CACHE);
    Ok(Compiled {
        regex,
        longest,
        info,
        caches: Mutex::new(Vec::new()),
        longest_caches: Mutex::new(Vec::new()),
        bytes,
    })
}

/// The compiled pattern for `call`, shared through a bounded process-wide
/// store so a constant pattern is compiled once, not once per batch.
pub(crate) fn compiled(
    pattern: &str,
    call: RegexCall,
) -> std::result::Result<Arc<Compiled>, PatternError> {
    let key = Key {
        pattern: pattern.to_owned(),
        similar: call.op == RegexOp::Similar,
        full: call.full(),
        ci: call.case_insensitive,
        dot: call.dot_newline,
        longest: call.longest(),
    };
    if let Some(found) = STORE.lock().unwrap().map.get(&key) {
        return Ok(found.clone());
    }
    let made = Arc::new(compile(&key)?);
    let mut store = STORE.lock().unwrap();
    if let Some(found) = store.map.get(&key) {
        return Ok(found.clone());
    }
    store.bytes += made.bytes;
    store.map.insert(key.clone(), made.clone());
    store.order.push_back(key);
    while store.map.len() > STORE_ENTRIES || (store.bytes > STORE_BYTES && store.map.len() > 1) {
        let oldest = store.order.pop_front().expect("store order");
        if let Some(gone) = store.map.remove(&oldest) {
            store.bytes -= gone.bytes;
        }
    }
    Ok(made)
}

/// Validate a constant pattern (and options) while binding.
pub(crate) fn check_constant(pattern: &str, call: RegexCall) -> crate::Result<Arc<Compiled>> {
    let similar = call.op == RegexOp::Similar;
    let compiled = compiled(pattern, call).map_err(|e| e.bind_error(similar))?;
    if call.longest() && compiled.info.lazy {
        return Err(LAZY_REPLACE.into());
    }
    Ok(compiled)
}

/// A compiled pattern checked out for one batch, with its search caches and
/// the reservation that funds them.
struct Slot {
    compiled: Arc<Compiled>,
    cache: Option<meta::Cache>,
    longest_cache: Option<hybrid::dfa::Cache>,
    captures: Captures,
    _lease: Reservation,
}
impl Slot {
    fn new(compiled: Arc<Compiled>, budget: &Budget) -> crate::Result<Self> {
        let lease = budget.reserve(compiled.bytes)?;
        let cache = compiled
            .caches
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| compiled.regex.create_cache());
        let longest_cache = compiled.longest.as_ref().map(|dfa| {
            compiled
                .longest_caches
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| dfa.create_cache())
        });
        let captures = compiled.regex.create_captures();
        Ok(Self {
            compiled,
            cache: Some(cache),
            longest_cache,
            captures,
            _lease: lease,
        })
    }
    fn is_match(&mut self, text: &str) -> bool {
        let input = Haystack::new(text).earliest(true);
        let regex = &self.compiled.regex;
        let cache = self.cache.as_mut().expect("checked-out cache");
        regex.search_half_with(cache, &input).is_some()
    }
    fn find(&mut self, text: &str, from: usize) -> Option<regex_automata::Match> {
        let input = Haystack::new(text).span(from..text.len());
        let regex = &self.compiled.regex;
        let cache = self.cache.as_mut().expect("checked-out cache");
        regex.search_with(cache, &input)
    }
    fn captures_at(&mut self, text: &str, from: usize) -> bool {
        let input = Haystack::new(text).span(from..text.len());
        let regex = &self.compiled.regex;
        let cache = self.cache.as_mut().expect("checked-out cache");
        regex.search_captures_with(cache, &input, &mut self.captures);
        self.captures.is_match()
    }
    /// End of PostgreSQL's (longest) match from `start`.
    fn longest_end(&mut self, text: &str, start: usize) -> crate::Result<Option<usize>> {
        let (Some(dfa), Some(cache)) = (&self.compiled.longest, self.longest_cache.as_mut()) else {
            return Ok(None);
        };
        let input = Haystack::new(text)
            .range(start..text.len())
            .anchored(Anchored::Yes);
        dfa.try_search_fwd(cache, &input)
            .map(|m| m.map(|m| m.offset()))
            .map_err(|_| TOO_LARGE.into())
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.take() {
            self.compiled.caches.lock().unwrap().push(cache);
        }
        if let Some(cache) = self.longest_cache.take() {
            self.compiled.longest_caches.lock().unwrap().push(cache);
        }
    }
}

/// Per-batch access to compiled patterns: one slot for a constant pattern,
/// a small most-recently-used set for patterns read from data.
struct Patterns<'a> {
    call: RegexCall,
    budget: &'a Budget,
    constant: Option<Slot>,
    slots: VecDeque<(String, Slot)>,
}
impl<'a> Patterns<'a> {
    fn new(call: RegexCall, pattern: &Expr, budget: &'a Budget) -> crate::Result<Self> {
        let constant = match &pattern.kind {
            Kind::Literal(Literal::Text(text)) => Some(Slot::new(
                compiled(text, call).map_err(|e| e.bind_error(call.op == RegexOp::Similar))?,
                budget,
            )?),
            _ => None,
        };
        Ok(Self {
            call,
            budget,
            constant,
            slots: VecDeque::new(),
        })
    }
    fn get(&mut self, pattern: &str) -> crate::Result<&mut Slot> {
        if self.constant.is_some() {
            return Ok(self.constant.as_mut().unwrap());
        }
        if let Some(index) = self.slots.iter().position(|(p, _)| p == pattern) {
            let entry = self.slots.remove(index).unwrap();
            self.slots.push_front(entry);
        } else {
            let made = compiled(pattern, self.call)
                .map_err(|e| e.runtime_error(self.call.op == RegexOp::Similar))?;
            if self.call.longest() && made.info.lazy {
                return Err(LAZY_REPLACE.into());
            }
            if self.slots.len() == BATCH_SLOTS {
                self.slots.pop_back();
            }
            let slot = Slot::new(made, self.budget)?;
            self.slots.push_front((pattern.to_owned(), slot));
        }
        Ok(&mut self.slots.front_mut().unwrap().1)
    }
}

/// The rows PostgreSQL would answer differently (shared functions) or whose
/// answer depends on Unicode case tables (every function).
fn check_row(call: RegexCall, info: &Info, text: &str) -> crate::Result<()> {
    let ascii = text.is_ascii();
    if call.case_insensitive && !ascii {
        return Err(CASE_FOLD.into());
    }
    if !call.shared() {
        return Ok(());
    }
    if info.locale && !ascii {
        return Err(CLASS_NON_ASCII.into());
    }
    if info.vertical_tab && text.as_bytes().contains(&0x0B) {
        return Err(VERTICAL_TAB.into());
    }
    if info.has_dot && !call.dot_newline && memchr::memchr(b'\n', text.as_bytes()).is_some() {
        return Err(NEWLINE.into());
    }
    Ok(())
}

fn strings(value: &Value) -> Option<&StringArray> {
    value.array.as_any().downcast_ref::<StringArray>()
}
fn text_at(array: Option<&StringArray>, row: usize) -> Option<&str> {
    array.and_then(|a| a.is_valid(row).then(|| a.value(row)))
}

/// Funded TEXT output whose payload grows with reservations taken before
/// each allocation (replacement output is not bounded by the input).
struct TextOut<'b> {
    budget: &'b Budget,
    lease: Reservation,
    data: Vec<u8>,
    offsets: Vec<i32>,
    valid: Vec<bool>,
}
impl<'b> TextOut<'b> {
    fn new(n: usize, initial: usize, budget: &'b Budget) -> crate::Result<Self> {
        let initial = initial.min(i32::MAX as usize);
        let bytes = workspace(n, 0)?
            .checked_add(initial)
            .ok_or("resource-limit: string size overflow")?;
        let lease = budget.reserve(bytes)?;
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0);
        Ok(Self {
            budget,
            lease,
            data: Vec::with_capacity(initial),
            offsets,
            valid: Vec::with_capacity(n),
        })
    }
    fn push(&mut self, text: &str) -> crate::Result<()> {
        let needed = self
            .data
            .len()
            .checked_add(text.len())
            .ok_or("resource-limit: string size overflow")?;
        if needed > i32::MAX as usize {
            return Err("resource-limit: string result exceeds 32-bit offsets".into());
        }
        if needed > self.data.capacity() {
            let target = needed.max(self.data.capacity().saturating_mul(2)).max(64);
            let target = target.min(i32::MAX as usize);
            let extra = target - self.data.capacity();
            self.lease.absorb(self.budget.reserve(extra)?);
            self.data.reserve_exact(target - self.data.len());
        }
        self.data.extend_from_slice(text.as_bytes());
        Ok(())
    }
    fn end_value(&mut self) {
        self.offsets.push(self.data.len() as i32);
        self.valid.push(true);
    }
    fn null(&mut self) {
        self.offsets.push(self.data.len() as i32);
        self.valid.push(false);
    }
    fn finish(self) -> crate::Result<Value> {
        let nulls = arrow::buffer::NullBuffer::from(self.valid);
        let array = StringArray::try_new(
            arrow::buffer::OffsetBuffer::new(self.offsets.into()),
            arrow::buffer::Buffer::from_vec(self.data),
            Some(nulls),
        )
        .map_err(|e| crate::Error::from(e.to_string()))?;
        Ok(Value::allocated(Arc::new(array), self.lease))
    }
}

/// Match spans of DuckDB's replacement loop (RE2 Replace/GlobalReplace):
/// after a match, an empty match where it ended is skipped.
fn duckdb_spans(slot: &mut Slot, text: &str, global: bool, spans: &mut Vec<(usize, usize)>) {
    spans.clear();
    let mut pos = 0;
    let mut last_end = None;
    while pos <= text.len() {
        let Some(found) = slot.find(text, pos) else {
            break;
        };
        if found.is_empty() && last_end == Some(found.start()) {
            let Some(c) = text[found.start()..].chars().next() else {
                break;
            };
            pos = found.start() + c.len_utf8();
            continue;
        }
        spans.push((found.start(), found.end()));
        pos = found.end();
        last_end = Some(pos);
        if !global {
            break;
        }
    }
}

/// Match spans of PostgreSQL's loop: the longest match at the leftmost
/// start, and after an empty match the search moves on one character.
fn postgresql_spans(
    slot: &mut Slot,
    text: &str,
    global: bool,
    spans: &mut Vec<(usize, usize)>,
) -> crate::Result<()> {
    spans.clear();
    let mut pos = 0;
    while pos <= text.len() {
        // The leftmost start is the same under both preference rules.
        let Some(found) = slot.find(text, pos) else {
            break;
        };
        let end = slot
            .longest_end(text, found.start())?
            .unwrap_or(found.end());
        spans.push((found.start(), end));
        pos = if end == found.start() {
            match text[end..].chars().next() {
                Some(c) => end + c.len_utf8(),
                None => break,
            }
        } else {
            end
        };
        if !global {
            break;
        }
    }
    Ok(())
}

/// The text with each span replaced by `literal`.
fn render(text: &str, spans: &[(usize, usize)], literal: &str, out: &mut String) {
    out.clear();
    let mut pos = 0;
    for &(start, end) in spans {
        out.push_str(&text[pos..start]);
        out.push_str(literal);
        pos = end;
    }
    out.push_str(&text[pos..]);
}

/// Reused per-batch buffers for the PostgreSQL comparison.
#[derive(Default)]
struct Scratch {
    duckdb: Vec<(usize, usize)>,
    postgresql: Vec<(usize, usize)>,
    left: String,
    right: String,
}

/// Write DuckDB's replacement after checking that PostgreSQL's would be the
/// same: identical match spans, or for a replacement without group
/// references, identical output text.
fn replace_row(
    slot: &mut Slot,
    text: &str,
    template: &Template,
    global: bool,
    scratch: &mut Scratch,
    out: &mut TextOut,
) -> crate::Result<()> {
    duckdb_spans(slot, text, global, &mut scratch.duckdb);
    // Without alternation or a variable quantifier every match from one start
    // has one length, so both loops find the same spans.
    if slot.compiled.info.ambiguous {
        postgresql_spans(slot, text, global, &mut scratch.postgresql)?;
        if scratch.duckdb != scratch.postgresql {
            let same = template.literal_text().is_some_and(|literal| {
                render(text, &scratch.duckdb, literal, &mut scratch.left);
                render(text, &scratch.postgresql, literal, &mut scratch.right);
                scratch.left == scratch.right
            });
            if !same {
                // PostgreSQL only added empty matches after nonempty ones.
                let mut kept = scratch.postgresql.clone();
                let mut previous_end = None;
                kept.retain(|&(start, end)| {
                    let extra = start == end && previous_end == Some(start);
                    previous_end = Some(end).filter(|_| end > start);
                    !extra
                });
                return Err(if kept == scratch.duckdb {
                    EMPTY_AFTER
                } else {
                    EXTENT
                }
                .into());
            }
        }
    }
    let mut pos = 0;
    for &(start, end) in &scratch.duckdb {
        out.push(&text[pos..start])?;
        if let Some(literal) = template.literal_text() {
            out.push(literal)?;
        } else {
            // Group references read the submatches of this same match.
            slot.captures_at(text, start);
            for piece in &template.pieces {
                match piece {
                    Piece::Text(t) => out.push(t)?,
                    Piece::Group(g) => {
                        if let Some(span) = slot.captures.get_group(*g) {
                            out.push(&text[span.start..span.end])?;
                        }
                    }
                }
            }
        }
        pos = end;
    }
    out.push(&text[pos..])?;
    out.end_value();
    Ok(())
}

pub(super) fn evaluate(
    call: RegexCall,
    args: &[Expr],
    input: &Input,
    budget: &Budget,
) -> crate::Result<Value> {
    let n = input.len;
    let values = args
        .iter()
        .map(|a| a.eval(input, budget))
        .collect::<crate::Result<Vec<_>>>()?;
    let texts = strings(&values[0]);
    let patterns_column = strings(&values[1]);
    let mut patterns = Patterns::new(call, &args[1], budget)?;
    match call.op {
        RegexOp::Replace { global } => {
            let replacements = strings(&values[2]);
            let constant = match &args[2].kind {
                Kind::Literal(Literal::Text(text)) => Some(Template::parse(text)?),
                _ => None,
            };
            let initial = texts.map_or(0, |a| a.value_data().len());
            let mut out = TextOut::new(n, initial, budget)?;
            let mut scratch = Scratch::default();
            for row in 0..n {
                let (Some(text), Some(pattern), Some(replacement)) = (
                    text_at(texts, row),
                    text_at(patterns_column, row),
                    text_at(replacements, row),
                ) else {
                    out.null();
                    continue;
                };
                let slot = patterns.get(pattern)?;
                let parsed;
                let template = match &constant {
                    Some(template) => template,
                    None => {
                        parsed = Template::parse(replacement)?;
                        &parsed
                    }
                };
                template.check(&slot.compiled.info)?;
                check_row(call, &slot.compiled.info, text)?;
                replace_row(slot, text, template, global, &mut scratch, &mut out)?;
            }
            out.finish()
        }
        RegexOp::Extract { group } => {
            let bound = texts.map_or(0, |a| a.value_data().len());
            let group = usize::from(group);
            super::text::build(n, bound, budget, |row, out| {
                let (Some(text), Some(pattern)) =
                    (text_at(texts, row), text_at(patterns_column, row))
                else {
                    out.append_null();
                    return Ok(());
                };
                let slot = patterns.get(pattern)?;
                check_row(call, &slot.compiled.info, text)?;
                let value = if slot.captures_at(text, 0) {
                    slot.captures
                        .get_group(group)
                        .map_or("", |span| &text[span.start..span.end])
                } else {
                    ""
                };
                out.append_value(value);
                Ok(())
            })
        }
        _ => {
            let lease = budget.reserve(workspace(n, 0)?)?;
            let mut out = BooleanBuilder::with_capacity(n);
            for row in 0..n {
                let (Some(text), Some(pattern)) =
                    (text_at(texts, row), text_at(patterns_column, row))
                else {
                    out.append_null();
                    continue;
                };
                let slot = patterns.get(pattern)?;
                check_row(call, &slot.compiled.info, text)?;
                out.append_value(slot.is_match(text) != call.negated);
            }
            Ok(Value::allocated(Arc::new(out.finish()), lease))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tr(pattern: &str) -> std::result::Result<(String, Info), PatternError> {
        translate(pattern, false)
    }

    #[test]
    fn translation_escapes_every_ascii_symbol_and_keeps_ascii_classes() {
        assert_eq!(tr("a.b").unwrap().0, "a.b");
        assert_eq!(tr("\\d+").unwrap().0, "[0-9]+");
        assert_eq!(
            tr("[^\\s]").unwrap().0,
            "[^\\x{9}\\x{A}\\x{C}\\x{D}\\x{20}]"
        );
        assert_eq!(tr("[a&&b]").unwrap().0, "[a\\x{26}\\x{26}b]");
        assert_eq!(tr("[\\D]").unwrap().0, "[[^0-9]]");
        assert_eq!(tr("a{2,}").unwrap().0, "a{2,}");
        assert_eq!(tr("(?:é)").unwrap().0, "(?:é)");
        assert!(tr("\\w").unwrap().1.locale);
        assert!(tr("\\s").unwrap().1.vertical_tab);
        assert!(tr("a|b").unwrap().1.ambiguous);
        assert!(!tr("a{2}").unwrap().1.ambiguous);
        assert!(tr("(a)+").unwrap().1.repeated_capture);
        assert!(!tr("(a)b+").unwrap().1.repeated_capture);
    }

    #[test]
    fn refusals_and_errors_are_classified() {
        for p in [
            "\\b",
            "\\y",
            "(?=a)",
            "(a)\\1",
            "a{256}",
            "\\pL",
            "(?i)a",
            "\\x41",
            "[a-é]",
            "***=a",
            "a^b",
            "a$b",
            "[[:word:]]",
            "[a[b]",
            "a{,2}",
            "]",
            "}",
            "^*",
            "(a{100}){100}",
            "[\\b]",
            "[a-c-e]",
        ] {
            assert!(
                matches!(tr(p), Err(PatternError::Unsupported(_))),
                "{p}: {:?}",
                tr(p)
            );
        }
        for p in [
            "(", ")", "a**", "a{2,1}", "[z-a]", "\\", "\\k", "*a", "a{1001}", "[", "[^]", "a|*",
        ] {
            assert!(
                matches!(tr(p), Err(PatternError::Invalid(_))),
                "{p}: {:?}",
                tr(p)
            );
        }
        assert!(matches!(
            translate("a%", true),
            Err(PatternError::Unsupported(_))
        ));
        assert!(translate("(ab|cd)*[0-9.]", true).is_ok());
        let long = "a".repeat(PATTERN_LIMIT + 1);
        assert_eq!(tr(&long), Err(PatternError::TooLarge));
    }

    #[test]
    fn replacement_templates() {
        let t = Template::parse("<\\1|\\\\>").unwrap();
        assert_eq!(t.max_group, 1);
        assert!(t.literal_text().is_none());
        assert_eq!(Template::parse("\\0").err(), Some(REPLACEMENT));
        assert_eq!(Template::parse("\\&").err(), Some(REPLACEMENT));
        assert_eq!(Template::parse("x\\").err(), Some(REPLACEMENT));
        let (_, info) = tr("(a|b)").unwrap();
        assert_eq!(
            Template::parse("\\1").unwrap().check(&info),
            Err(GROUP_REFERENCE)
        );
        assert_eq!(
            Template::parse("\\2").unwrap().check(&info),
            Err(MISSING_GROUP)
        );
        assert!(Template::parse("").unwrap().literal_text() == Some(""));
    }

    #[test]
    fn longest_match_models_postgresql() {
        let call = RegexCall {
            op: RegexOp::Replace { global: false },
            negated: false,
            case_insensitive: false,
            dot_newline: false,
        };
        let budget = Budget::new(64 << 20);
        let mut slot = Slot::new(compiled("a|ab", call).unwrap(), &budget).unwrap();
        let found = slot.find("abcd", 0).unwrap();
        assert_eq!((found.start(), found.end()), (0, 1));
        assert_eq!(slot.longest_end("abcd", 0).unwrap(), Some(2));
        let mut slot = Slot::new(compiled("^a", call).unwrap(), &budget).unwrap();
        assert!(slot.find("aa", 1).is_none());
    }

    #[test]
    fn evaluation_is_funded_and_released() {
        let call = RegexCall {
            op: RegexOp::Replace { global: true },
            negated: false,
            case_insensitive: false,
            dot_newline: false,
        };
        let text = |s: &str| Expr {
            kind: Kind::Literal(Literal::Text(s.into())),
            ty: Type::Text,
        };
        let column = Expr {
            kind: Kind::Column(0),
            ty: Type::Text,
        };
        let args = [column, text("b+"), text("[x]")];
        // A budget too small for the compiled pattern's search caches.
        let small = Budget::new(64 << 10);
        let input = Input {
            columns: vec![Some(
                Value::adopt(
                    Arc::new(StringArray::from(vec![Some("abbc"), None, Some("bb")])),
                    &small,
                )
                .unwrap(),
            )],
            len: 3,
        };
        let before = small.used();
        let error = evaluate(call, &args, &input, &small).err().unwrap();
        assert!(error.starts_with("resource-limit:"), "{error}");
        assert_eq!(small.used(), before);
        let budget = Budget::new(64 << 20);
        let input = Input {
            columns: vec![Some(
                Value::adopt(
                    Arc::new(StringArray::from(vec![Some("abbc"), None, Some("bb")])),
                    &budget,
                )
                .unwrap(),
            )],
            len: 3,
        };
        let before = budget.used();
        let value = evaluate(call, &args, &input, &budget).unwrap();
        let array = value.array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(0), "a[x]c");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "[x]");
        drop(value);
        assert_eq!(budget.used(), before);
    }
}
