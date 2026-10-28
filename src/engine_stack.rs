//! Stack boundary for native entry points, including teardown and plan printing.
//! Keep an entire operation here: returning a recursive syntax/plan tree to a
//! small host stack does not protect its later traversal or destruction.
use std::{cell::Cell, io, panic::resume_unwind, thread};

/// Stack for the engine-owned thread that parses, plans, executes, prints and
/// drops each operation. Measured need at the limits in `nesting.rs`
/// (release / debug): expression constructs at the nesting or height limit
/// at most 5.5 / 66 MB, dominated by sqlparser (recursive descent for
/// nesting; its own Clone/Display/Drop of a chain's syntax tree for height);
/// plan-level constructs, over which the executor recurses per plan node, at
/// most 8.9 / 50 MB for a set-operation chain and 13.1 / 70 MB for nested
/// queries through views at `QUERY_LEVEL_LIMIT`; both together 21.9 / 119 MB.
/// `nesting_stack_headroom` requires every case to need at most half of this
/// reservation, and expression constructs at most 8 MiB in release. It is
/// virtual address space; only touched pages become resident.
pub const STACK_BYTES: usize = if cfg!(debug_assertions) { 256 } else { 64 } * 1024 * 1024;
/// Stack for engine worker threads, which evaluate (but never parse or plan)
/// bound expressions. Measured need with 4 and 10 workers, filtering with
/// chains at the height limit (alone and inside nested CASE, NOT or calls):
/// at most 2.0 MiB in release and 7.7 MiB in debug builds.
pub const WORKER_STACK_BYTES: usize = if cfg!(debug_assertions) { 32 } else { 8 } * 1024 * 1024;
thread_local! { static ACTIVE: Cell<bool> = const { Cell::new(false) }; }

pub fn run<T: Send>(operation: impl FnOnce() -> T + Send) -> io::Result<T> {
    if ACTIVE.get() {
        return Ok(operation());
    }
    thread::scope(|scope| {
        let handle = thread::Builder::new()
            .name("assaydb-engine".into())
            .stack_size(STACK_BYTES)
            .spawn_scoped(scope, || {
                ACTIVE.set(true);
                operation()
            })?;
        match handle.join() {
            Ok(result) => Ok(result),
            Err(panic) => resume_unwind(panic),
        }
    })
}

/// If a cleanup thread cannot be created, retain the value instead of dropping
/// a recursive tree on the untrusted host stack. Normal cleanup is synchronous.
pub(crate) fn drop_on_stack<T: Send>(value: T) {
    let mut value = Some(value);
    if run(|| drop(value.take())).is_err() {
        std::mem::forget(value);
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use crate::nesting::{LIMIT, QUERY_LEVEL_LIMIT, expression_depth, measure};

    /// Resident stack high-water mark of `operation` on a fresh thread of
    /// `STACK_BYTES`, the production engine stack. Untouched stack pages are
    /// not resident, so the deepest resident page bounds the stack used.
    fn resident_stack(operation: impl FnOnce() + Send) -> usize {
        thread::scope(|scope| {
            thread::Builder::new()
                .stack_size(STACK_BYTES)
                .spawn_scoped(scope, || {
                    ACTIVE.set(true);
                    operation();
                    // SAFETY: pthread returns this live thread's mapped stack
                    // bounds; mincore only queries page residency and writes
                    // into the correctly sized vector.
                    unsafe {
                        let current = libc::pthread_self();
                        let top = libc::pthread_get_stackaddr_np(current) as usize;
                        let size = libc::pthread_get_stacksize_np(current);
                        let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
                        let base = (top - size).div_ceil(page) * page;
                        let end = top / page * page;
                        let mut residency = vec![0; (end - base) / page];
                        let length = end - base;
                        assert_eq!(
                            libc::mincore(base as *mut _, length, residency.as_mut_ptr()),
                            0
                        );
                        let first = residency.iter().position(|v| v & 1 != 0).unwrap();
                        top - base - first * page
                    }
                })
                .unwrap()
                .join()
                .unwrap()
        })
    }

    /// The largest `n` whose statement the fence accepts within the 64 KiB
    /// SQL limit (acceptance is monotone in `n`).
    fn longest(sql: impl Fn(usize) -> String) -> String {
        let accepted = |n| {
            let text = sql(n);
            text.len() <= 64 * 1024 && measure(&text).unwrap().is_some()
        };
        assert!(accepted(1));
        let (mut low, mut high) = (1, 2);
        while accepted(high) {
            (low, high) = (high, high * 2);
        }
        while high - low > 1 {
            let middle = (low + high) / 2;
            if accepted(middle) {
                low = middle;
            } else {
                high = middle;
            }
        }
        sql(low)
    }

    /// Constructs not in the fixture: prefix, repeated opener and closer,
    /// innermost operand, suffix. Each is grown to the largest accepted size,
    /// whichever limit binds (`flat_plus`, `union` and `where_and` are
    /// chains, bounded by height rather than nesting).
    const EXTRA: [(&str, &str, &str, &str, &str, &str); 9] = [
        ("negation", "SELECT ", "- ", "", "7", ""),
        ("coalesce", "SELECT ", "COALESCE(NULL, ", ")", "7", ""),
        ("flat_plus", "SELECT ", "1 + ", "", "7", ""),
        (
            "derived",
            "SELECT x FROM ",
            "(SELECT x FROM ",
            ") AS d",
            "t",
            "",
        ),
        (
            "in_subquery",
            "SELECT x FROM t WHERE ",
            "x IN (SELECT x FROM t WHERE ",
            ")",
            "TRUE",
            "",
        ),
        ("union", "SELECT 1", " UNION ALL SELECT 1", "", "", ""),
        (
            "where_and",
            "SELECT x FROM t WHERE ",
            "x > -1 AND ",
            "",
            "x >= 0",
            "",
        ),
        (
            "where_between",
            "SELECT x FROM t WHERE ",
            "(x BETWEEN -5 AND 5 AND ",
            ")",
            "x >= 0",
            "",
        ),
        ("mixed", "SELECT ", "ABS(-((", ") + 0))", "x", " FROM t"),
    ];

    /// Operator chains, lists and mixtures at the height limit or the 64 KiB
    /// SQL limit: prefix, first operand, repeated link, suffix.
    const CHAINS: [(&str, &str, &str, &str, &str); 14] = [
        ("or", "SELECT x FROM t WHERE ", "x = 1", " OR x = 1", ""),
        ("and", "SELECT x FROM t WHERE ", "x > -5", " AND x > -5", ""),
        ("plus", "SELECT ", "x", " + x", " FROM t"),
        ("order_by", "SELECT x FROM t ORDER BY ", "x", " + x", ""),
        ("literal_or", "SELECT ", "TRUE", " OR TRUE", ""),
        ("literal_plus", "SELECT ", "1", " + 1", ""),
        ("comparison", "SELECT ", "TRUE", " = TRUE", ""),
        ("cast", "SELECT ", "1", "::INTEGER", ""),
        (
            "is_not_null",
            "SELECT x FROM t WHERE ",
            "x",
            " IS NOT NULL",
            "",
        ),
        ("calls", "SELECT ", "x", " + ABS(-(x))", " FROM t"),
        (
            "union_where",
            "SELECT x FROM t WHERE x > 0",
            " ",
            "UNION ALL SELECT x FROM t WHERE x > 0 ",
            "",
        ),
        ("in_list", "SELECT 1 IN (", "1", ", 1", ")"),
        (
            "string_in_list",
            "SELECT x FROM t WHERE s IN (",
            "'a'",
            ", 'a'",
            ")",
        ),
        (
            "not_in_list",
            "SELECT x FROM t WHERE x NOT IN (",
            "-9",
            ", -9",
            ")",
        ),
    ];

    /// A view chain at `QUERY_LEVEL_LIMIT` nested query levels: 63 views over
    /// `v0`, each adding `levels` (four, or six for `v1`) to the outer query
    /// and `v0`. `link` builds a view body around the previous view.
    fn view_chain(link: impl Fn(usize, &str) -> String, v0: &str) -> (Vec<String>, String) {
        let mut setup = vec![format!("CREATE VIEW v0 AS {v0}")];
        let mut levels = 2;
        for view in 1..64 {
            let nested = if view == 1 { 5 } else { 3 };
            setup.push(format!(
                "CREATE VIEW v{view} AS {}",
                link(nested, &format!("v{}", view - 1))
            ));
            levels += 1 + nested;
        }
        assert_eq!(levels, QUERY_LEVEL_LIMIT);
        (setup, "SELECT x FROM v63".into())
    }
    fn derived(nested: usize, inner: &str) -> String {
        format!(
            "SELECT x FROM {}{inner}{}",
            "(SELECT x FROM ".repeat(nested),
            (0..nested)
                .map(|i| format!(") AS d{i}"))
                .collect::<String>()
        )
    }
    fn in_subqueries(nested: usize, inner: &str) -> String {
        format!(
            "SELECT x FROM t WHERE {}x IN (SELECT x FROM {inner}){}",
            "x IN (SELECT x FROM t WHERE ".repeat(nested - 1),
            ")".repeat(nested - 1)
        )
    }

    // Each construct at its limit runs to completion, including plan
    // serialization and destruction, within half of the production stack.
    // Expression constructs (nesting and height limits) must also fit in
    // 8 MiB in release builds. Plan-level constructs (set-operation chains,
    // nested queries through views) recurse once per plan node in the
    // executor; before the fence they were answered up to the host's 8 MiB
    // main thread, so their limits need more than that. Run in debug and
    // release; not a timing test.
    #[test]
    fn nesting_stack_headroom() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/expression_nesting/cases.json"
        ))
        .unwrap();
        // (id, setup, SQL, also prepare and serialize the plan as CLI EXPLAIN
        // does, expression construct)
        let mut cases: Vec<(String, Vec<String>, String, bool, bool)> = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["depth"] == LIMIT)
            .map(|case| {
                let sql = case["sql"].as_str().unwrap().to_string();
                (
                    case["id"].as_str().unwrap().to_string(),
                    vec![],
                    sql,
                    true,
                    true,
                )
            })
            .collect();
        for (id, prefix, open, close, leaf, suffix) in EXTRA {
            let sql = longest(|n| {
                format!(
                    "{prefix}{}{leaf}{}{suffix}",
                    open.repeat(n),
                    close.repeat(n)
                )
            });
            let depth = expression_depth(&sql).unwrap().unwrap();
            assert!(
                depth > LIMIT - crate::nesting::QUERY_WEIGHT,
                "{id}: {depth}"
            );
            // A set-operation chain is a plan-level construct (see below).
            cases.push((id.to_string(), vec![], sql, false, id != "union"));
        }
        for (id, prefix, first, link, suffix) in CHAINS {
            let sql = longest(|n| format!("{prefix}{first}{}{suffix}", link.repeat(n)));
            let explain = !sql.contains(" FROM ");
            cases.push((
                format!("chain_{id}"),
                vec![],
                sql,
                explain,
                !id.starts_with("union"),
            ));
        }
        // A chain at the height limit inside parentheses at the nesting limit.
        let sql = longest(|n| {
            format!(
                "SELECT {}1{}{}",
                "(".repeat(LIMIT - 10),
                " + 1".repeat(n),
                ")".repeat(LIMIT - 10)
            )
        });
        assert!(measure(&sql).unwrap().unwrap().nesting > LIMIT - 10);
        cases.push(("chain_in_nesting".into(), vec![], sql, true, true));
        // Nested queries composed through views at QUERY_LEVEL_LIMIT, and the
        // same with an OR chain at the height limit in the innermost view.
        let (setup, sql) = view_chain(derived, "SELECT x FROM t");
        cases.push(("views_derived".into(), setup, sql, false, false));
        let (setup, sql) = view_chain(in_subqueries, "SELECT x FROM t");
        cases.push(("views_in_subqueries".into(), setup, sql, false, false));
        let chain = longest(|n| format!("SELECT x FROM t WHERE x = 1{}", " OR x = 1".repeat(n)));
        let (setup, sql) = view_chain(in_subqueries, &chain);
        cases.push(("views_with_chain".into(), setup, sql, false, false));
        let union =
            longest(|n| format!("SELECT x FROM t{}", " UNION ALL SELECT x FROM t".repeat(n)));
        let (setup, sql) = view_chain(in_subqueries, &union);
        cases.push(("views_with_union".into(), setup, sql, false, false));

        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        let (mut deepest, mut deepest_expression) = (0, 0);
        for (id, setup, sql, explain, expression) in &cases {
            let used = resident_stack(|| {
                let options = crate::execute::Options::default();
                let mut session = crate::session::Session::new(
                    &std::env::temp_dir(),
                    options.clone(),
                    Default::default(),
                )
                .unwrap();
                session
                    .execute_sql("CREATE TABLE t (x INTEGER, s VARCHAR)")
                    .unwrap();
                session
                    .execute_sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (-3, NULL)")
                    .unwrap();
                for statement in setup {
                    session.execute_sql(statement).unwrap();
                }
                session
                    .execute_sql(sql)
                    .unwrap_or_else(|error| panic!("{id}: {error}"));
                drop(session);
                if *explain {
                    let prepared =
                        crate::execute::prepare(sql, &Default::default(), &options).unwrap();
                    let printed = serde_json::json!({"plan": prepared.plan, "relational_plan": prepared.relational});
                    serde_json::to_writer_pretty(std::io::sink(), &printed).unwrap();
                    drop(printed);
                    drop(prepared);
                }
            });
            let measured = measure(sql).unwrap().unwrap();
            eprintln!(
                "nesting stack: profile={profile} case={id} nesting={} height={} sql_bytes={} resident_bytes={used}",
                measured.nesting,
                measured.height,
                sql.len()
            );
            deepest = deepest.max(used);
            if *expression {
                deepest_expression = deepest_expression.max(used);
            }
        }
        eprintln!(
            "nesting stack: profile={profile} max_resident_bytes={deepest} max_expression_resident_bytes={deepest_expression} stack_bytes={STACK_BYTES}"
        );
        assert!(
            deepest <= STACK_BYTES / 2,
            "stack headroom below 2x: {deepest}"
        );
        if !cfg!(debug_assertions) {
            assert!(
                deepest_expression <= 8 * 1024 * 1024,
                "release expression stack need above 8 MiB: {deepest_expression}"
            );
        }
    }
}
