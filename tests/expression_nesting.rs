//! Expression nesting at the documented limit and one level beyond it, and
//! operator chains at the height limit and one link beyond it, through
//! sessions on small host stacks (including Drop), the session protocol and CLI.
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    nesting::{HEIGHT_LIMIT, LIMIT, QUERY_LEVEL_LIMIT, expression_depth, measure},
    session::{Limits, Output, Session},
    spill::SpillOptions,
};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
    thread,
};

/// Smallest common host stack: macOS secondary threads, including Python's.
const HOST_STACK: usize = 512 * 1024;

fn cases() -> Vec<Value> {
    serde_json::from_str::<Value>(include_str!("fixtures/expression_nesting/cases.json")).unwrap()
        ["cases"]
        .as_array()
        .unwrap()
        .clone()
}
fn rows(batch: &RecordBatch) -> Value {
    json!(
        (0..batch.num_rows())
            .map(|row| batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(row) {
                        Value::Null
                    } else if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
                        json!(a.value(row))
                    } else if let Some(a) = a.as_any().downcast_ref::<BooleanArray>() {
                        json!(a.value(row))
                    } else {
                        panic!("unexpected type: {:?}", a.data_type())
                    }
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    )
}
fn session(options: Options) -> Session {
    Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap()
}
fn on_host_stack(test: impl FnOnce() + Send + 'static) {
    thread::Builder::new()
        .stack_size(HOST_STACK)
        .spawn(test)
        .unwrap()
        .join()
        .unwrap();
}

/// `n` repetitions of a construct, with `pad` parentheses around the innermost
/// operand (each adds exactly one level). Mirrors the reference generator.
fn ladder(shape: &str, n: usize, pad: usize) -> String {
    let leaf = |value: &str| format!("{}{value}{}", "(".repeat(pad), ")".repeat(pad));
    match shape {
        "parentheses" => format!("SELECT {}{}{}", "(".repeat(n), leaf("7"), ")".repeat(n)),
        "and" | "or" => format!(
            "SELECT {}{}",
            format!("TRUE {} ", shape.to_uppercase()).repeat(n),
            leaf("TRUE")
        ),
        "not" => format!("SELECT {}{}", "NOT ".repeat(n), leaf("TRUE")),
        "case" => format!(
            "SELECT {}{}{}",
            "CASE WHEN TRUE THEN ".repeat(n),
            leaf("7"),
            " ELSE 8 END".repeat(n)
        ),
        "function" => format!("SELECT {}{}{}", "ABS(".repeat(n), leaf("7"), ")".repeat(n)),
        "arithmetic" => format!("SELECT {}{}{}", "(0 + ".repeat(n), leaf("7"), ")".repeat(n)),
        "subquery" => format!(
            "SELECT {}{}{}",
            "(SELECT ".repeat(n),
            leaf("7"),
            ")".repeat(n)
        ),
        "in" => format!(
            "SELECT {}{} IN (1, 3, 7, NULL){}",
            "(".repeat(n),
            leaf("7"),
            ")".repeat(n)
        ),
        _ => panic!("unknown shape"),
    }
}
/// Constructs bounded by the nesting limit.
const SHAPES: [&str; 7] = [
    "parentheses",
    "not",
    "case",
    "function",
    "arithmetic",
    "subquery",
    "in",
];
/// Left-deep chains: sqlparser parses them in a loop, so each link adds
/// height but not nesting; they are bounded by the height limit. The fixture
/// records them at height 250 with a 251 variant, which is answered.
const CHAIN_SHAPES: [&str; 2] = ["and", "or"];

/// SQL at exactly height HEIGHT_LIMIT, and the same chain one link longer.
fn chain_boundary(shape: &str) -> (String, String) {
    let accepted = |n| measure(&ladder(shape, n, 0)).unwrap().is_some();
    let (mut low, mut high) = (1, HEIGHT_LIMIT);
    while high - low > 1 {
        let middle = (low + high) / 2;
        if accepted(middle) {
            low = middle;
        } else {
            high = middle;
        }
    }
    let at = measure(&ladder(shape, low, 0)).unwrap().unwrap();
    assert_eq!(at.height, HEIGHT_LIMIT, "{shape}");
    assert!(at.nesting < 10, "{shape}: {at:?}");
    assert_eq!(
        measure(&ladder(shape, low + 1, 0)).unwrap(),
        None,
        "{shape}"
    );
    (ladder(shape, low, 0), ladder(shape, low + 1, 0))
}

/// Nesting depth of `sql`, or `None` beyond either limit.
fn nesting(sql: &str) -> Option<usize> {
    measure(sql).unwrap().map(|depth| depth.nesting)
}

/// SQL at exactly nesting depth LIMIT, and the same SQL one level deeper.
fn boundary(shape: &str) -> (String, String) {
    let depth = |n, pad| nesting(&ladder(shape, n, pad));
    let mut n = 1;
    while depth(n + 1, 0).is_some() {
        n += 1;
    }
    let pad = LIMIT - depth(n, 0).unwrap();
    assert_eq!(depth(n, pad), Some(LIMIT), "{shape}");
    // An extra parenthesis deepens the tree unless it wraps a shorter operand
    // of a left-deep chain; then one more repetition deepens it by one.
    let over = if depth(n, pad + 1).is_none() {
        ladder(shape, n, pad + 1)
    } else {
        assert_eq!(depth(n + 1, 0), None, "{shape}");
        assert_eq!(depth(n, 0), Some(LIMIT), "{shape}");
        ladder(shape, n + 1, 0)
    };
    (ladder(shape, n, pad), over)
}

#[test]
fn fixture_constructs_are_exactly_at_the_limit() {
    let mut slt = 0;
    for case in cases() {
        let sql = case["sql"].as_str().unwrap();
        let measured = expression_depth(sql).unwrap();
        if let Some(shape) = case["shape"].as_str() {
            // `depth` is the chain's height for chains, otherwise its nesting
            // (an IN list adds its disjunction's height, not nesting).
            let measured = if CHAIN_SHAPES.contains(&shape) {
                measured
            } else {
                nesting(sql)
            };
            let (n, pad) = (case["n"].as_u64().unwrap(), case["pad"].as_u64().unwrap());
            assert_eq!(
                sql,
                ladder(shape, n as usize, pad as usize),
                "{}",
                case["id"]
            );
            assert_eq!(measured, Some(case["depth"].as_u64().unwrap() as usize));
            if case["depth"] == LIMIT {
                // The Python acceptance script uses the recorded L+1 shape.
                let over_n = case["over_n"].as_u64().unwrap() as usize;
                let over_pad = case["over_pad"].as_u64().unwrap() as usize;
                let over = ladder(shape, over_n, over_pad);
                if CHAIN_SHAPES.contains(&shape) {
                    // One more link is one more height level, far below
                    // HEIGHT_LIMIT, and no more nesting.
                    assert_eq!(expression_depth(&over).unwrap(), Some(LIMIT + 1));
                    assert!(measure(&over).unwrap().unwrap().nesting < 10);
                } else {
                    assert_eq!(boundary(shape), (sql.to_string(), over));
                }
            }
        } else {
            assert!(measured.is_some(), "{}", case["id"]);
        }
        if case.get("sqlite_expected").is_some() {
            slt += 1;
            assert!(
                expression_depth(sql).unwrap().unwrap() > 50,
                "{}",
                case["id"]
            );
        }
    }
    assert!(slt >= 12);
}

#[test]
fn fixture_worker_batch_spill_matrix_on_small_host_stack() {
    on_host_stack(|| {
        for workers in [1, 4, 10] {
            for batch_size in [1, 8192] {
                for spill in [false, true] {
                    let options = Options {
                        workers,
                        batch_size,
                        spill: spill.then(SpillOptions::default),
                        ..Options::default()
                    };
                    let mut s = session(options.clone());
                    let mut setup = Vec::<Value>::new();
                    for case in cases() {
                        let label =
                            format!("{} w{workers} b{batch_size} spill={spill}", case["id"]);
                        let needed = case["setup"].as_array().unwrap();
                        if !needed.starts_with(&setup) {
                            s = session(options.clone());
                            setup.clear();
                        }
                        for sql in &needed[setup.len()..] {
                            s.execute_sql(sql.as_str().unwrap()).unwrap();
                        }
                        setup.clone_from(needed);
                        let Output::Query { execution, .. } = s
                            .execute_sql(case["sql"].as_str().unwrap())
                            .unwrap_or_else(|e| panic!("{label}: {e}"))
                        else {
                            panic!("{label}: not a query")
                        };
                        let rowsort = case["header"]
                            .as_str()
                            .is_some_and(|h| h.contains("rowsort"));
                        let mut actual = rows(&execution.batch);
                        let mut expected = case["rows"].clone();
                        if rowsort {
                            for value in [&mut actual, &mut expected] {
                                value
                                    .as_array_mut()
                                    .unwrap()
                                    .sort_by_key(ToString::to_string);
                            }
                        }
                        assert_eq!(actual, expected, "{label}");
                    }
                }
            }
        }
    });
}

#[test]
fn limit_succeeds_and_limit_plus_one_is_typed_on_small_host_stack() {
    on_host_stack(|| {
        let mut s = session(Options::default());
        for shape in SHAPES {
            let (at, over) = boundary(shape);
            s.execute_sql(&at)
                .unwrap_or_else(|e| panic!("{shape} at limit: {e}"));
            let error = s.execute_sql(&over).err().expect("must refuse");
            assert_eq!(error.kind, "unsupported", "{shape}: {error}");
            assert!(
                error
                    .message
                    .contains("nesting limit exceeded (maximum depth 250)"),
                "{shape}: {error}"
            );
            // The refusal leaves the session usable.
            s.execute_sql("SELECT 7").unwrap();
        }
        for case in cases() {
            let Some(shape) = case["shape"].as_str() else {
                continue;
            };
            if !CHAIN_SHAPES.contains(&shape) || case["depth"] != LIMIT {
                continue;
            }
            // The recorded one-link-longer chain is answered (TRUE).
            let over = ladder(
                shape,
                case["over_n"].as_u64().unwrap() as usize,
                case["over_pad"].as_u64().unwrap() as usize,
            );
            let Output::Query { execution, .. } = s.execute_sql(&over).unwrap() else {
                panic!()
            };
            assert_eq!(rows(&execution.batch), json!([[true]]), "{shape}");
        }
        for shape in CHAIN_SHAPES {
            let (at, over) = chain_boundary(shape);
            let Output::Query { execution, .. } = s
                .execute_sql(&at)
                .unwrap_or_else(|e| panic!("{shape} at height limit: {e}"))
            else {
                panic!()
            };
            assert_eq!(rows(&execution.batch), json!([[true]]), "{shape}");
            let error = s.execute_sql(&over).err().expect("must refuse");
            assert_eq!(error.kind, "unsupported", "{shape}: {error}");
            assert!(
                error
                    .message
                    .contains("height limit exceeded (maximum 4096 levels"),
                "{shape}: {error}"
            );
            assert!(error.guidance().is_some(), "{shape}: {error}");
            s.execute_sql("SELECT 7").unwrap();
        }
        // Views are planned as query levels; their trees are dropped with the
        // session, still on this small host stack's engine boundary.
        let (at, _) = boundary("function");
        s.execute_sql(&format!("CREATE VIEW nested AS {at}"))
            .unwrap();
        let Output::Query { execution, .. } = s.execute_sql("SELECT * FROM nested").unwrap() else {
            panic!()
        };
        assert_eq!(rows(&execution.batch), json!([[7]]));
        // Before the fence, view expansion stopped only at 64 active views;
        // a chain of 64 plain views is still answered.
        let limits = Limits {
            max_tables: 4 * LIMIT,
            ..Limits::default()
        };
        let mut s =
            Session::new(&std::env::temp_dir(), Options::default(), limits.clone()).unwrap();
        s.execute_sql("CREATE VIEW v0 AS SELECT 7 AS x").unwrap();
        for level in 1..64 {
            s.execute_sql(&format!(
                "CREATE VIEW v{level} AS SELECT x FROM v{}",
                level - 1
            ))
            .unwrap();
        }
        let Output::Query { execution, .. } = s.execute_sql("SELECT x FROM v63").unwrap() else {
            panic!()
        };
        assert_eq!(rows(&execution.batch), json!([[7]]));
        // Nested query levels composed through views count against
        // QUERY_LEVEL_LIMIT: the outer query, each view body and its derived
        // tables. 63 views of four levels over v0, plus `extra` levels in w1.
        for extra in [0, 1] {
            let mut s =
                Session::new(&std::env::temp_dir(), Options::default(), limits.clone()).unwrap();
            s.execute_sql("CREATE VIEW w0 AS SELECT 7 AS x").unwrap();
            let mut levels = 2;
            for view in 1..64 {
                let nested = if view == 1 { 5 + extra } else { 3 };
                s.execute_sql(&format!(
                    "CREATE VIEW w{view} AS SELECT x FROM {}w{}{}",
                    "(SELECT x FROM ".repeat(nested),
                    view - 1,
                    (0..nested)
                        .map(|i| format!(") AS d{i}"))
                        .collect::<String>()
                ))
                .unwrap();
                levels += 1 + nested;
            }
            assert_eq!(levels, QUERY_LEVEL_LIMIT + extra);
            let result = s.execute_sql("SELECT x FROM w63");
            if extra == 0 {
                let Output::Query { execution, .. } = result.unwrap() else {
                    panic!()
                };
                assert_eq!(rows(&execution.batch), json!([[7]]));
            } else {
                let error = result
                    .err()
                    .expect("view chain beyond the limit must refuse");
                assert_eq!(error.kind, "unsupported", "{error}");
                assert!(error.message.contains("nesting limit exceeded"), "{error}");
            }
        }
    });
}

#[test]
fn cli_and_session_protocol_depth_boundary_and_explain() {
    let directory =
        std::env::temp_dir().join(format!("assaydb-nesting-protocol-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let names: Vec<&str> = SHAPES.iter().chain(&CHAIN_SHAPES).copied().collect();
    let boundaries: Vec<_> = SHAPES
        .iter()
        .map(|shape| boundary(shape))
        .chain(CHAIN_SHAPES.iter().map(|shape| chain_boundary(shape)))
        .collect();
    let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb-session"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(input, "{}", json!({"op":"open", "directory":directory})).unwrap();
    // Write concurrently: the boundary SQL can exceed an OS pipe buffer.
    let requests = boundaries.clone();
    let writer = thread::spawn(move || {
        for (at, over) in requests {
            writeln!(input, "{}", json!({ "sql": at })).unwrap();
            writeln!(input, "{}", json!({ "sql": over })).unwrap();
        }
    });
    let output = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let replies: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(replies.len(), 1 + 2 * names.len());
    for (index, shape) in names.iter().enumerate() {
        let (at, over) = (&replies[1 + index * 2], &replies[2 + index * 2]);
        assert_eq!(at["status"], "ok", "{shape}: {at}");
        assert_eq!(over["error_kind"], "unsupported", "{shape}: {over}");
    }
    for (shape, (at, over)) in names.iter().zip(&boundaries) {
        for (sql, accepted) in [(at, true), (over, false)] {
            // EXPLAIN pretty-prints the plan; for a chain at the height limit
            // that is about 800 MB of indentation, so check correctness mode
            // only there (plan serialization at the limit is covered by
            // `engine_stack::tests::nesting_stack_headroom`).
            let modes: &[&str] = if accepted && CHAIN_SHAPES.contains(shape) {
                &["correctness"]
            } else {
                &["correctness", "explain"]
            };
            for &mode in modes {
                let output_path = directory.join(format!("{shape}-{accepted}-{mode}.json"));
                let request = directory.join("request.json");
                fs::write(
                    &request,
                    json!({"sql": sql, "output": output_path, "mode": mode}).to_string(),
                )
                .unwrap();
                let output = Command::new(env!("CARGO_BIN_EXE_assaydb"))
                    .stdin(fs::File::open(&request).unwrap())
                    .output()
                    .unwrap();
                let stdout = String::from_utf8_lossy(&output.stdout);
                assert_eq!(
                    output.status.success(),
                    accepted,
                    "{shape} {accepted} {mode}: {stdout} {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                if !accepted {
                    assert!(stdout.contains("unsupported"), "{shape} {mode}: {stdout}");
                    let message = if CHAIN_SHAPES.contains(shape) {
                        "height limit exceeded"
                    } else {
                        "nesting limit exceeded"
                    };
                    assert!(stdout.contains(message), "{shape}: {stdout}");
                }
            }
        }
    }
    fs::remove_dir_all(directory).unwrap();
}

/// Measure corpus records: `ASSAY_NESTING_RECORDS` names a JSON array of
/// objects with `sql` (and optionally `file`/`line`). Prints the maximum depth.
#[test]
#[ignore]
fn measure_record_depths() {
    let path = std::env::var("ASSAY_NESTING_RECORDS").expect("ASSAY_NESTING_RECORDS");
    let records: Vec<Value> = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let mut depths: Vec<(usize, String)> = records
        .iter()
        .map(|record| {
            let depth = expression_depth(record["sql"].as_str().unwrap())
                .unwrap()
                .expect("record beyond the limit");
            (depth, format!("{}:{}", record["file"], record["line"]))
        })
        .collect();
    depths.sort();
    let (min, max) = (depths.first().unwrap(), depths.last().unwrap());
    println!("records={} min={min:?} max={max:?}", depths.len());
}
