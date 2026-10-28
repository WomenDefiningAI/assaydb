//! Expression-depth fence, applied to tokens before the parser builds a tree.
//!
//! Two measures of the tree that SQL text describes, each with its own limit:
//!
//! - **Nesting depth** (`LIMIT`): levels of parentheses, CASE, calls, prefix
//!   operators, subqueries and operators of increasing precedence. A left-deep
//!   run of one precedence level (`a OR b OR ...`, `1 + 2 - 3 ...`, casts,
//!   `IS NULL` tests, a UNION chain) is one level, as for an n-ary node:
//!   sqlparser parses such a run in a loop, so it costs the parser no
//!   recursion, while each nesting level costs several KiB of parser stack.
//! - **Height** (`HEIGHT_LIMIT`): the real binary-tree height, counting every
//!   link of those chains. The binder, planner, executor, plan printer and
//!   destructors recurse over it at a small cost per level.
//!
//! Width (IN elements, arguments, projections, rows, comma-separated items)
//! consumes neither, except that an IN list adds the height of the
//! disjunction the binder builds for it. A parenthesised query counts
//! `QUERY_WEIGHT` levels, because planning a query level needs several times
//! the stack of an expression level, and each set-operation link
//! `SET_OP_WEIGHT` levels of height (measured; see SEMANTICS-LOG.md). Both are
//! computed iteratively with an operator-precedence pass over sqlparser's own
//! tokens, so refusing a statement never builds, traverses or drops a tree.
use sqlparser::{
    ast::Statement,
    keywords::Keyword as K,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

/// Maximum accepted nesting depth. The deepest of the 275 SLT records that
/// hit sqlparser's default recursion limit measures 106 in height (103
/// expression levels plus the statement's query weight) and no more in
/// nesting; 250 is the smallest round number at least twice that. DuckDB's
/// default `max_expression_depth` (and SQLite's `SQLITE_MAX_EXPR_DEPTH`) is
/// 1000, but each counts its own internal nodes, so the numbers are not
/// directly equal.
pub const LIMIT: usize = 250;
/// Maximum accepted height, counting each link of an operator chain. Before
/// this fence, chains were limited only by the host stack: release builds
/// answered OR/AND/arithmetic chains of up to 3,298 operands and comparison
/// chains of up to 2,645 on an 8 MiB main thread (see SEMANTICS-LOG.md); this
/// limit admits all of them.
pub const HEIGHT_LIMIT: usize = 4096;
/// Stack cost of one query level, in expression levels (see module comment).
pub const QUERY_WEIGHT: usize = 4;
/// Height of one set-operation link (`... UNION ALL SELECT ...`). sqlparser
/// parses a run of set operations in a loop, but the planner recurses once
/// per operand; release builds answered chains of up to 481 operands on an
/// 8 MiB thread, and `HEIGHT_LIMIT / SET_OP_WEIGHT` is 512.
pub const SET_OP_WEIGHT: usize = 8;
/// sqlparser's own counter is a backstop only: the fence above is stricter.
const PARSER_LIMIT: usize = 4 * LIMIT + 64;
pub const REFUSAL: &str = "SQL expression nesting limit exceeded (maximum depth 250)";
pub const HEIGHT_REFUSAL: &str =
    "SQL expression height limit exceeded (maximum 4096 levels, counting each operator of a chain)";
/// IN lists longer than this are bound as a balanced disjunction (see
/// `bind::in_list_disjunction`); shorter ones as a left-deep chain.
pub const IN_LIST_CHAIN: usize = 64;

/// Height of the disjunction the binder builds for an IN list of `items`:
/// `FALSE` plus one equality per item, left-deep up to `IN_LIST_CHAIN` items,
/// balanced beyond.
fn in_list_height(items: usize) -> usize {
    if items <= IN_LIST_CHAIN {
        items
    } else {
        (usize::BITS - items.leading_zeros()) as usize
    }
}

/// Nesting depth and height of a statement (the maximum over statements).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Depth {
    pub nesting: usize,
    pub height: usize,
}
impl Depth {
    fn max(self, other: Depth) -> Depth {
        Depth {
            nesting: self.nesting.max(other.nesting),
            height: self.height.max(other.height),
        }
    }
    fn add(self, other: Depth) -> Depth {
        Depth {
            nesting: self.nesting + other.nesting,
            height: self.height + other.height,
        }
    }
    fn plus(self, levels: usize) -> Depth {
        self.add(Depth {
            nesting: levels,
            height: levels,
        })
    }
    fn within_limits(self) -> Result<(), &'static str> {
        if self.nesting > LIMIT {
            Err(REFUSAL)
        } else if self.height > HEIGHT_LIMIT {
            Err(HEIGHT_REFUSAL)
        } else {
            Ok(())
        }
    }
}

pub fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    let tokens = Tokenizer::new(&crate::dialect::QueryDialect, sql)
        .tokenize_with_location()
        .map_err(|e| e.to_string())?;
    // Check the original quantity expressions before replacing parser markers.
    if tokens.iter().any(
        |t| matches!(&t.token, Token::Word(w) if w.quote_style.is_none() && w.keyword == K::FETCH),
    ) {
        depth(tokens.iter().map(|t| &t.token))?;
    }
    let (tokens, mut cte_hints) = crate::cte_hint_syntax::rewrite(tokens)?;
    let (tokens, mut fetch) = crate::fetch_syntax::rewrite(tokens, PARSER_LIMIT)?;
    // Frame EXCLUDE clauses are not parsed by the pinned sqlparser; they are
    // recorded as start-bound markers first (see frame_exclusion.rs).
    let tokens = crate::frame_exclusion::rewrite(tokens);
    depth(tokens.iter().map(|t| &t.token))?;
    let mut statements = Parser::new(&crate::dialect::QueryDialect)
        .with_recursion_limit(PARSER_LIMIT)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(|e| match e {
            ParserError::RecursionLimitExceeded => REFUSAL.into(),
            _ => e.to_string(),
        })?;
    fetch.apply(&mut statements)?;
    cte_hints.apply(&mut statements)?;
    Ok(statements)
}

/// Expression-tree height of `sql` (the maximum over its statements), or
/// `None` when it exceeds either limit. Exposed for tests and corpus
/// measurement.
#[doc(hidden)]
pub fn expression_depth(sql: &str) -> Result<Option<usize>, String> {
    Ok(measure(sql)?.map(|depth| depth.height))
}

/// Nesting depth and height of `sql`, or `None` when either exceeds its limit.
#[doc(hidden)]
pub fn measure(sql: &str) -> Result<Option<Depth>, String> {
    let tokens = Tokenizer::new(&crate::dialect::QueryDialect, sql)
        .tokenize()
        .map_err(|e| e.to_string())?;
    Ok(depth(tokens.iter()).ok())
}

#[derive(Clone, Copy)]
struct Op {
    precedence: u8,
    arity: u8,
    /// A binary operator: a left operand that is already a chain of this
    /// precedence extends that chain instead of nesting under it.
    chains: bool,
    /// `BETWEEN` whose `AND` has not been seen yet.
    open_between: bool,
    /// Height the binder adds beyond this node (an IN list's disjunction).
    extra: usize,
}

/// A completed operand: its measures and, when it is an operator chain, that
/// chain's precedence (0 otherwise).
#[derive(Clone, Copy)]
struct Value {
    depth: Depth,
    chain: u8,
}
impl Value {
    fn leaf(depth: Depth) -> Self {
        Value { depth, chain: 0 }
    }
}

/// One parenthesis, bracket or CASE level. Its pending operators and operands
/// live in the shared `Fence` stacks above `ops`/`values`.
#[derive(Clone, Copy)]
struct Frame {
    ops: usize,
    values: usize,
    expect_operand: bool,
    /// Tallest completed comma/keyword-separated item in this frame.
    item_max: Depth,
    query: bool,
    set_ops: usize,
    /// The parent was not expecting an operand: a call, `OVER (...)`, etc.
    attached: bool,
    case: bool,
    /// The parenthesised list of an `IN` operator, and its comma count.
    in_list: bool,
    commas: usize,
    /// Levels that enclosing frames are known to add above this frame.
    outer: Depth,
}

impl Frame {
    /// Levels this frame adds above its items: one for the node itself, or
    /// `QUERY_WEIGHT` for a query, plus `SET_OP_WEIGHT` per set operation in
    /// height. A run of set operations is one more query level of nesting
    /// (sqlparser parses it in a loop).
    fn weight(&self) -> Depth {
        let node = if self.query { QUERY_WEIGHT } else { 1 };
        Depth {
            nesting: node + usize::from(self.set_ops > 0) * QUERY_WEIGHT,
            height: node + self.set_ops * SET_OP_WEIGHT,
        }
    }
}

/// Shunting-yard over measures instead of values; no allocation per frame.
struct Fence {
    ops: Vec<Op>,
    values: Vec<Value>,
    frames: Vec<Frame>,
    top: Frame,
    /// The previous token was `IN`: a following parenthesis is its list.
    after_in: bool,
}

impl Fence {
    fn new() -> Self {
        Fence {
            ops: Vec::new(),
            values: Vec::new(),
            frames: Vec::new(),
            top: Self::frame(0, 0, false, false, Depth::default()),
            after_in: false,
        }
    }
    fn frame(ops: usize, values: usize, attached: bool, case: bool, outer: Depth) -> Frame {
        Frame {
            ops,
            values,
            expect_operand: true,
            item_max: Depth::default(),
            query: false,
            set_ops: 0,
            attached,
            case,
            in_list: false,
            commas: 0,
            outer,
        }
    }
    fn open(&mut self, case: bool) {
        let parent = self.top;
        // A child is nested under every pending operator of its parent.
        let outer = parent
            .outer
            .add(parent.weight())
            .plus(self.ops.len() - parent.ops);
        let child = Self::frame(
            self.ops.len(),
            self.values.len(),
            !parent.expect_operand,
            case,
            outer,
        );
        self.frames.push(std::mem::replace(&mut self.top, child));
        self.top.in_list = std::mem::take(&mut self.after_in);
    }
    fn close(&mut self) {
        let in_list = (self.top.in_list && !self.top.query).then(|| {
            let empty = self.top.commas == 0
                && self.top.item_max == Depth::default()
                && self.values.len() == self.top.values;
            in_list_height(if empty { 0 } else { self.top.commas + 1 })
        });
        let depth = self.finish();
        let attached = self.top.attached;
        self.top = self.frames.pop().expect("parent frame");
        if let Some(extra) = in_list
            && let Some(op) = self.ops[self.top.ops..].last_mut()
        {
            op.extra = extra;
        }
        if attached {
            self.top.expect_operand = false;
        }
        self.operand(depth);
    }
    /// Measures of the top frame as a node of its parent.
    fn finish(&mut self) -> Depth {
        self.finish_item();
        self.top.item_max.add(self.top.weight())
    }
    fn operand(&mut self, depth: Depth) {
        let own = self.values.len() > self.top.values;
        match self.values.last_mut() {
            // Juxtaposed operands (qualified names, aliases, type names, call
            // suffixes) belong to the same node.
            Some(top) if own && !self.top.expect_operand => {
                *top = Value::leaf(top.depth.max(depth));
            }
            _ => self.values.push(Value::leaf(depth)),
        }
        self.top.expect_operand = false;
    }
    fn reduce(&mut self) {
        let op = self.ops.pop().expect("operator");
        let mut operands = [None; 3];
        for slot in operands.iter_mut().take(usize::from(op.arity)).rev() {
            if self.values.len() > self.top.values {
                *slot = self.values.pop();
            }
        }
        let tallest = operands
            .iter()
            .flatten()
            .fold(Depth::default(), |depth, value| depth.max(value.depth));
        let mut depth = tallest.plus(1);
        depth.height += op.extra;
        let chain = if op.chains && op.arity == 2 {
            if let [Some(left), right, _] = operands
                && left.chain == op.precedence
            {
                // `left` is already this chain's node: one more operand.
                let right = right.map_or(0, |value| value.depth.nesting);
                depth.nesting = left.depth.nesting.max(right + 1);
            }
            op.precedence
        } else {
            0
        };
        self.values.push(Value { depth, chain });
    }
    fn push(&mut self, precedence: u8, arity: u8) {
        self.ops.push(Op {
            precedence,
            arity,
            chains: false,
            open_between: false,
            extra: 0,
        });
        self.top.expect_operand = true;
    }
    fn binary(&mut self, precedence: u8) {
        while self.ops.len() > self.top.ops
            && self
                .ops
                .last()
                .is_some_and(|op| op.precedence >= precedence)
        {
            self.reduce();
        }
        self.push(precedence, 2);
        self.ops.last_mut().expect("binary").chains = true;
    }
    /// `AND` closing the nearest open `BETWEEN`, if only tighter operators
    /// (its lower bound) sit above it.
    fn between_and(&mut self) -> bool {
        let frame_ops = &self.ops[self.top.ops..];
        let Some(index) = frame_ops.iter().rposition(|op| op.open_between) else {
            return false;
        };
        if frame_ops[index + 1..].iter().any(|op| op.precedence <= 20) {
            return false;
        }
        while self.ops.len() > self.top.ops + index + 1 {
            self.reduce();
        }
        let op = self.ops.last_mut().expect("between");
        op.open_between = false;
        op.arity = 3;
        self.top.expect_operand = true;
        true
    }
    fn finish_item(&mut self) {
        while self.ops.len() > self.top.ops {
            self.reduce();
        }
        let depth = self
            .values
            .drain(self.top.values..)
            .fold(Depth::default(), |depth, value| depth.max(value.depth));
        self.top.item_max = self.top.item_max.max(depth);
        self.top.expect_operand = true;
    }
    /// Lower bound on the top frame's finished measures as a node of its
    /// parent, plus the levels above it. Pending operators nest (each is in
    /// the right operand of the one below it), so each adds at least one
    /// level; a completed operand adds one more height level under a pending
    /// operator, but may extend that operator's chain without nesting.
    fn bound(&self) -> Depth {
        let pending = self.ops.len() - self.top.ops;
        let tallest = self.values[self.top.values..]
            .iter()
            .fold(Depth::default(), |depth, value| depth.max(value.depth));
        let item = match pending {
            0 => tallest,
            n => Depth {
                nesting: (n + 1).max(tallest.nesting),
                height: (n + 1).max(tallest.height + 1),
            },
        };
        self.top
            .outer
            .add(self.top.item_max.max(item))
            .add(self.top.weight())
    }
    /// Close every frame and return the statement's measures. The statement
    /// itself is not a node of an enclosing expression, hence `- 1`.
    fn statement(&mut self) -> Depth {
        while !self.frames.is_empty() {
            self.close();
        }
        let depth = self.finish();
        self.top = Self::frame(0, 0, false, false, Depth::default());
        Depth {
            nesting: depth.nesting - 1,
            height: depth.height - 1,
        }
    }
}

/// Binary precedence, as sqlparser 0.58 assigns it for the generic dialect,
/// so that the fence's chains are the parser's chains.
fn precedence(token: &Token) -> Option<u8> {
    Some(match token {
        Token::Eq
        | Token::DoubleEq
        | Token::Neq
        | Token::Lt
        | Token::Gt
        | Token::LtEq
        | Token::GtEq
        | Token::Spaceship
        | Token::Assignment
        | Token::Tilde
        | Token::TildeAsterisk
        | Token::ExclamationMarkTilde
        | Token::ExclamationMarkTildeAsterisk
        | Token::DoubleTilde
        | Token::DoubleTildeAsterisk
        | Token::ExclamationMarkDoubleTilde
        | Token::ExclamationMarkDoubleTildeAsterisk => 20,
        Token::Plus | Token::Minus => 30,
        Token::Mul | Token::Div | Token::Mod | Token::DuckIntDiv | Token::StringConcat => 40,
        Token::Pipe
        | Token::QuestionMarkDash
        | Token::DoubleSharp
        | Token::Overlap
        | Token::AmpersandLeftAngleBracket
        | Token::AmpersandRightAngleBracket
        | Token::QuestionMarkDashVerticalBar
        | Token::AmpersandLeftAngleBracketVerticalBar
        | Token::VerticalBarAmpersandRightAngleBracket
        | Token::TwoWayArrow
        | Token::LeftAngleBracketCaret
        | Token::RightAngleBracketCaret
        | Token::QuestionMarkSharp
        | Token::QuestionMarkDoubleVerticalBar
        | Token::QuestionPipe
        | Token::TildeEqual
        | Token::AtSign
        | Token::ShiftLeftVerticalBar
        | Token::VerticalBarShiftRight => 21,
        Token::Caret | Token::Sharp | Token::ShiftLeft | Token::ShiftRight => 22,
        Token::Ampersand => 23,
        Token::DoubleColon | Token::ExclamationMark | Token::CaretAt => 50,
        _ => return None,
    })
}

fn is_separator(keyword: K) -> bool {
    matches!(
        keyword,
        K::SELECT
            | K::FROM
            | K::WHERE
            | K::GROUP
            | K::HAVING
            | K::ORDER
            | K::BY
            | K::LIMIT
            | K::OFFSET
            | K::FETCH
            | K::WINDOW
            | K::QUALIFY
            | K::ON
            | K::USING
            | K::JOIN
            | K::AS
            | K::WHEN
            | K::THEN
            | K::ELSE
            | K::VALUES
            | K::SET
            | K::INTO
            | K::WITH
            | K::RETURNING
            | K::PARTITION
            | K::ROWS
            | K::RANGE
            | K::GROUPS
    )
}

/// Iterative measurement; refuses as soon as either measure must exceed its
/// limit.
fn depth<'a>(tokens: impl Iterator<Item = &'a Token>) -> Result<Depth, &'static str> {
    let mut fence = Fence::new();
    let mut deepest = Depth::default();
    // Context for `IS [NOT] [DISTINCT FROM]`.
    let (mut after_is, mut distinct_from) = (false, false);
    for token in tokens {
        let mut is_token = false;
        let mut after_in = false;
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        let nested = !fence.frames.is_empty();
        let expect = fence.top.expect_operand;
        match token {
            Token::Whitespace(_) | Token::EOF | Token::Period => continue,
            Token::LParen | Token::LBracket | Token::LBrace => fence.open(false),
            Token::RParen | Token::RBracket | Token::RBrace => {
                if nested && !fence.top.case {
                    fence.close();
                }
            }
            Token::Word(word) if word.quote_style.is_none() => match word.keyword {
                K::CASE => fence.open(true),
                K::END if nested && fence.top.case => fence.close(),
                K::AND if fence.between_and() => {}
                K::AND => fence.binary(10),
                K::OR => fence.binary(5),
                K::XOR => fence.binary(24),
                // `x NOT IN`, `x NOT LIKE`, `IS NOT`: part of the operator.
                K::NOT if after_is || !expect => is_token = after_is,
                K::NOT => fence.push(15, 1),
                K::IS => {
                    fence.binary(17);
                    is_token = true;
                }
                K::DISTINCT if after_is => distinct_from = true,
                K::FROM if distinct_from => distinct_from = false,
                K::BETWEEN => {
                    fence.binary(20);
                    let op = fence.ops.last_mut().expect("between");
                    op.open_between = true;
                    op.chains = false;
                }
                K::IN => {
                    fence.binary(20);
                    after_in = true;
                }
                K::OVERLAPS => fence.binary(20),
                K::LIKE | K::ILIKE | K::SIMILAR | K::REGEXP | K::RLIKE => fence.binary(19),
                K::DIV => fence.binary(40),
                K::AT if !expect => fence.binary(41),
                K::COLLATE => fence.binary(42),
                K::UNION | K::INTERSECT | K::EXCEPT | K::MINUS => {
                    fence.finish_item();
                    fence.top.set_ops += 1;
                }
                K::SELECT => {
                    fence.finish_item();
                    fence.top.query = true;
                }
                keyword if is_separator(keyword) => fence.finish_item(),
                _ => fence.operand(Depth::default().plus(1)),
            },
            Token::Comma => {
                fence.finish_item();
                fence.top.commas += 1;
            }
            Token::SemiColon => deepest = deepest.max(fence.statement()),
            Token::Minus | Token::Plus if expect => fence.push(50, 1),
            Token::Tilde | Token::ExclamationMark | Token::Sharp | Token::AtSign if expect => {
                fence.push(50, 1)
            }
            other => match precedence(other) {
                Some(precedence) => fence.binary(precedence),
                None if is_operand(other) => fence.operand(Depth::default().plus(1)),
                // Other operators (JSON arrows, `@>`, `:` ...) still build
                // binary nodes, whether or not Assay later supports them.
                None => fence.binary(16),
            },
        }
        after_is = is_token;
        fence.after_in = after_in;
        // The outermost frame is the statement, not a node (hence `- 1`).
        let bound = fence.bound();
        Depth {
            nesting: bound.nesting - 1,
            height: bound.height - 1,
        }
        .within_limits()?;
    }
    let deepest = deepest.max(fence.statement());
    deepest.within_limits()?;
    Ok(deepest)
}

fn is_operand(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_)
            | Token::Number(..)
            | Token::Char(_)
            | Token::Placeholder(_)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
    )
}

// Stored views compose independently parsed statements, so the planner also
// counts the query levels it actually nests, across view expansions: nested
// queries (subqueries, derived tables, view bodies) against
// `QUERY_LEVEL_LIMIT`, and set-operation operands, which sqlparser parses in
// a loop and which cost far less stack per level, in height units against
// `HEIGHT_LIMIT`.
thread_local! {
    static QUERIES: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}
/// Maximum nested query levels during planning, counting the statement, its
/// subqueries and derived tables, and expanded view bodies. Before this fence
/// the planner had only its 64-view expansion guard; release builds answered
/// up to 242 such levels on an 8 MiB thread (64 views of three nested query
/// levels each). The fence itself admits at most 62 levels per statement.
pub const QUERY_LEVEL_LIMIT: usize = 256;
pub(crate) struct QueryDepth {
    set_operand: bool,
}
pub(crate) fn query_depth(set_operand: bool) -> crate::Result<QueryDepth> {
    let (levels, operands) = QUERIES.get();
    let next = if set_operand {
        (levels, operands + 1)
    } else {
        (levels + 1, operands)
    };
    if next.0 > QUERY_LEVEL_LIMIT {
        return Err(format!("unsupported: {REFUSAL}").into());
    }
    if next.1 * SET_OP_WEIGHT > HEIGHT_LIMIT {
        return Err(format!("unsupported: {HEIGHT_REFUSAL}").into());
    }
    QUERIES.set(next);
    Ok(QueryDepth { set_operand })
}
impl Drop for QueryDepth {
    fn drop(&mut self) {
        let (levels, operands) = QUERIES.get();
        QUERIES.set(if self.set_operand {
            (levels, operands - 1)
        } else {
            (levels - 1, operands)
        });
    }
}
