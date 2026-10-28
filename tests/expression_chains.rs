//! Flat operator chains, long IN lists and set-operation chains: lengths the
//! pre-fence engine answered stay answered, through sessions on small host
//! stacks with several worker/batch settings and through the CLI. Answers are
//! DuckDB 1.5.6's (tests/fixtures/expression_chains/README.md).
use arrow::{
    array::{Array, Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    util::display::array_value_to_string,
};
use assaydb::{
    execute::Options,
    nesting::{HEIGHT_LIMIT, LIMIT, measure},
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::{
    fs,
    process::{Command, Stdio},
    sync::Arc,
    thread,
};

/// Smallest common host stack: macOS secondary threads, including Python's.
const HOST_STACK: usize = 512 * 1024;
const ROWS: i32 = 64;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/expression_chains/cases.json")).unwrap()
}

fn chain(separator: &str, term: impl Fn(usize) -> String, n: usize) -> String {
    (1..=n).map(term).collect::<Vec<_>>().join(separator)
}
fn or_eq(n: usize) -> String {
    format!(
        "SELECT count(*) FROM orders WHERE {}",
        chain(" OR ", |i| format!("o_custkey = {i}"), n)
    )
}
/// Mirrors `generate_sql` in tools/testing/expression_chains_reference.py.
fn generate_sql(shape: &str, n: usize) -> String {
    match shape {
        "or_eq" => or_eq(n),
        "and_ne" => format!(
            "SELECT count(*) FROM orders WHERE {}",
            chain(" AND ", |i| format!("o_custkey <> {i}"), n)
        ),
        "plus" => format!(
            "SELECT o_orderkey, {} AS s FROM orders ORDER BY o_orderkey LIMIT 5",
            chain(" + ", |_| "o_custkey".into(), n)
        ),
        "comparison" => format!(
            "SELECT count(*) FROM orders WHERE (o_custkey > 2000){}",
            " = TRUE".repeat(n)
        ),
        "chain_in_parentheses" => format!(
            "SELECT count(*) FROM orders WHERE {}{}{}",
            "(".repeat(240),
            &or_eq(n)[34..],
            ")".repeat(240)
        ),
        "calls_chain" => format!(
            "SELECT o_orderkey, {} AS s FROM orders ORDER BY o_orderkey LIMIT 5",
            chain(" + ", |_| "ABS(-(o_custkey))".into(), n)
        ),
        "case_chain" => format!(
            "SELECT count(*) FROM orders WHERE {}{}{}",
            "CASE WHEN o_orderkey > 0 THEN ".repeat(100),
            &or_eq(n)[34..],
            " ELSE FALSE END".repeat(100)
        ),
        "subquery_chain" => format!("SELECT ({}) AS c", or_eq(n)),
        "union_chain" => format!(
            "SELECT count(*), sum(o_custkey) FROM ({}) AS u",
            chain(
                " UNION ALL ",
                |i| format!(
                    "SELECT o_custkey FROM orders WHERE o_orderkey = {}",
                    i % ROWS as usize + 1
                ),
                n
            )
        ),
        "in_numbers" => format!(
            "SELECT count(*) FROM orders WHERE o_custkey IN ({})",
            chain(", ", |i| i.to_string(), n)
        ),
        "not_in_numbers" => format!(
            "SELECT count(*) FROM orders WHERE o_custkey NOT IN ({})",
            chain(", ", |i| i.to_string(), n)
        ),
        "in_strings" => format!(
            "SELECT count(*) FROM orders WHERE o_comment IN ({})",
            chain(", ", |i| format!("'c{}'", i % 13), n)
        ),
        _ => panic!("unknown shape {shape}"),
    }
}
fn fnv1a(text: &str) -> String {
    let mut value: u64 = 0xcbf29ce484222325;
    for byte in text.bytes() {
        value = (value ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
    format!("{value:016x}")
}

/// Every case with its regenerated SQL, checked against the fixture.
fn cases() -> Vec<(Value, String)> {
    fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| {
            let sql = generate_sql(
                case["shape"].as_str().unwrap(),
                case["n"].as_u64().unwrap() as usize,
            );
            assert_eq!(sql.len() as u64, case["sql_bytes"], "{}", case["id"]);
            assert!(sql.len() <= 64 * 1024);
            assert_eq!(fnv1a(&sql), case["sql_fnv1a"], "{}", case["id"]);
            (case.clone(), sql)
        })
        .collect()
}

/// Rows as JSON numbers (all fixture answers are integers).
fn rows(batch: &RecordBatch) -> Value {
    json!(
        (0..batch.num_rows())
            .map(|row| batch
                .columns()
                .iter()
                .map(|a| {
                    if a.is_null(row) {
                        Value::Null
                    } else {
                        let text = array_value_to_string(a, row).unwrap();
                        json!(text.parse::<i64>().unwrap_or_else(|_| panic!("{text}")))
                    }
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    )
}

fn session(options: Options) -> Session {
    let mut session = Session::new(&std::env::temp_dir(), options, Limits::default()).unwrap();
    for sql in fixture()["setup"].as_array().unwrap() {
        session.execute_sql(sql.as_str().unwrap()).unwrap();
    }
    session
}
fn on_host_stack(test: impl FnOnce() + Send + 'static) {
    thread::Builder::new()
        .stack_size(HOST_STACK)
        .spawn(test)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn fixture_lengths_and_limits() {
    let cases = cases();
    assert_eq!(cases.len(), 18);
    for (case, sql) in &cases {
        let id = case["id"].as_str().unwrap();
        let measured = measure(sql).unwrap();
        if case.get("refused").is_some() {
            assert_eq!(measured, None, "{id}");
            continue;
        }
        let measured = measured.unwrap_or_else(|| panic!("{id} refused"));
        assert!(measured.nesting <= LIMIT, "{id}");
        if case["height_limit"] == true {
            // Exactly at the height limit; one more link is refused.
            assert_eq!(measured.height, HEIGHT_LIMIT, "{id}");
            let shape = case["shape"].as_str().unwrap();
            let longer = generate_sql(shape, case["n"].as_u64().unwrap() as usize + 1);
            assert_eq!(measure(&longer).unwrap(), None, "{id}");
        }
        if case.get("longest_within_64_kib").is_some() {
            let shape = case["shape"].as_str().unwrap();
            let longer = generate_sql(shape, case["n"].as_u64().unwrap() as usize + 1);
            assert!(longer.len() > 64 * 1024, "{id}");
        }
    }
}

#[test]
fn chains_answer_on_small_host_stack_across_workers_and_batches() {
    on_host_stack(|| {
        for (workers, batch_size) in [(1, 8192), (4, 16), (10, 7)] {
            let mut s = session(Options {
                workers,
                batch_size,
                ..Options::default()
            });
            for (case, sql) in cases() {
                let label = format!("{} w{workers} b{batch_size}", case["id"]);
                let result = s.execute_sql(&sql);
                if let Some(refusal) = case["refused"].as_str() {
                    let error = result.err().unwrap_or_else(|| panic!("{label}: answered"));
                    assert_eq!(error.kind, "unsupported", "{label}: {error}");
                    assert!(error.message.contains(refusal), "{label}: {error}");
                    continue;
                }
                let Output::Query { execution, .. } =
                    result.unwrap_or_else(|error| panic!("{label}: {error}"))
                else {
                    panic!("{label}: not a query")
                };
                assert_eq!(rows(&execution.batch), case["rows"], "{label}");
            }
        }
    });
}

/// Before the fence, `||` was refused by the binder at the chain's first
/// operator. With the string-functions card merged (integration candidate 1)
/// `||` is supported, and a `||` chain binds iteratively like the other
/// operator chains: one nesting level, one height per link. The 300-link
/// chain answers DuckDB 1.5.6's rows (each comment repeated 300 times), and a
/// chain exactly at the height limit answers on a small host stack.
#[test]
fn string_concatenation_chain_answers_like_duckdb() {
    on_host_stack(|| {
        for (workers, batch_size) in [(1, 8192), (4, 16), (10, 7)] {
            let mut s = session(Options {
                workers,
                batch_size,
                ..Options::default()
            });
            let text = |s: &mut Session, sql: &str| {
                let Output::Query { execution, .. } =
                    s.execute_sql(sql).unwrap_or_else(|e| panic!("{e}"))
                else {
                    panic!("not a query")
                };
                let column = execution.batch.column(0);
                let column = column.as_any().downcast_ref::<StringArray>().unwrap();
                (0..column.len())
                    .map(|i| column.value(i).to_string())
                    .collect::<Vec<_>>()
            };
            let comments = text(&mut s, "SELECT o_comment FROM orders ORDER BY o_orderkey");
            assert_eq!(comments.len(), ROWS as usize);
            let sql = format!(
                "SELECT {} AS c FROM orders ORDER BY o_orderkey",
                chain(" || ", |_| "o_comment".into(), 300)
            );
            assert_eq!(
                text(&mut s, &sql),
                comments.iter().map(|c| c.repeat(300)).collect::<Vec<_>>(),
                "w{workers} b{batch_size}"
            );
            let literal = |n: usize| format!("SELECT {}", chain(" || ", |_| "'x'".into(), n));
            let n = (2..=HEIGHT_LIMIT + 1)
                .rev()
                .find(|n| {
                    measure(&literal(*n))
                        .unwrap()
                        .is_some_and(|m| m.height == HEIGHT_LIMIT)
                })
                .expect("a || chain at the height limit");
            assert_eq!(measure(&literal(n + 1)).unwrap(), None);
            assert_eq!(text(&mut s, &literal(n)), vec!["x".repeat(n)]);
        }
    });
}

/// The reported reproduction: CLI, Parquet input, default options.
#[test]
fn cli_answers_long_chains_over_parquet() {
    let directory = std::env::temp_dir().join(format!("assaydb-chains-cli-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let table = directory.join("orders.parquet");
    let keys: Vec<i32> = (1..=ROWS).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int32, false),
            Field::new("o_custkey", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(keys.clone())),
            Arc::new(Int32Array::from(
                keys.iter().map(|k| (k * 61) % 4000 + 1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                keys.iter()
                    .map(|k| format!("c{}", k % 9))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create(&table).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for (case, sql) in cases() {
        let id = case["id"].as_str().unwrap();
        if !matches!(
            case["shape"].as_str().unwrap(),
            "or_eq" | "and_ne" | "plus" | "in_numbers"
        ) {
            continue;
        }
        let output_path = directory.join(format!("{id}.json"));
        let request = json!({"sql": sql, "tables": {"orders": table}, "output": output_path});
        let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let writer = thread::spawn(move || {
            std::io::Write::write_all(&mut input, request.to_string().as_bytes()).unwrap();
        });
        let output = child.wait_with_output().unwrap();
        writer.join().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(refusal) = case["refused"].as_str() {
            assert!(!output.status.success(), "{id}: {stdout}");
            assert!(
                stdout.contains("\"status\":\"unsupported\""),
                "{id}: {stdout}"
            );
            assert!(stdout.contains(refusal), "{id}: {stdout}");
            continue;
        }
        assert!(
            output.status.success(),
            "{id}: {stdout} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value =
            serde_json::from_str(&fs::read_to_string(&output_path).unwrap()).unwrap();
        let actual = json!(
            result["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| json!(
                        v.as_i64()
                            .unwrap_or_else(|| v.as_str().unwrap().parse::<i64>().unwrap())
                    ))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
        assert_eq!(actual, case["rows"], "{id}");
    }
    fs::remove_dir_all(directory).unwrap();
}
