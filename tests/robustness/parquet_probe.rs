//! Test-only local-reader entry point. Not installed or part of the engine CLI.
use arrow::array::{Array, Int64Array};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::Path,
};

fn failure(error: assaydb::session::Error) -> Value {
    let status = match error.kind {
        "unsupported" => "unsupported",
        "resource" => "resource",
        "internal" => "internal",
        _ => "error",
    };
    json!({"status": status, "error_kind": error.kind, "message": error.message})
}

fn handle(request: Value, session: &mut Option<Session>) -> Value {
    if session.is_none() {
        // Keep the preregistered native request budgets and options. This probe
        // deliberately does not supply an alternative SQL or Parquet decoder.
        assert_eq!(request["op"].as_str(), Some("open"));
        let options = Options {
            workers: usize::try_from(request["threads"].as_u64().unwrap()).unwrap(),
            memory_bytes: usize::try_from(request["memory_bytes"].as_u64().unwrap()).unwrap(),
            ..Options::default()
        };
        let limits = Limits {
            max_catalog_bytes: usize::try_from(request["max_catalog_bytes"].as_u64().unwrap())
                .unwrap(),
            ..Limits::default()
        };
        match Session::new(
            Path::new(request["directory"].as_str().unwrap()),
            options,
            limits,
        ) {
            Ok(mut opened) => {
                // The public session CLI intentionally disables this capability.
                // Enable it only in this isolated, process-limited test helper.
                opened.set_local_files_enabled(true);
                *session = Some(opened);
                json!({"status": "ok", "kind": "session", "local_files_enabled": true})
            }
            Err(error) => failure(error),
        }
    } else {
        match session
            .as_mut()
            .unwrap()
            .execute_sql(request["sql"].as_str().unwrap())
        {
            Err(error) => failure(error),
            Ok(Output::Statement { affected_rows }) => {
                json!({"status": "ok", "kind": "statement", "affected_rows": affected_rows})
            }
            Ok(Output::Query { execution, .. }) => {
                // Both control queries have one Int64 output. Preserve every
                // actual row/null so an unexpected answer fails the outer gate.
                let batch = &execution.batch;
                if batch.num_columns() != 1 {
                    return json!({"status": "error", "message": "probe expected one output column"});
                }
                let Some(array) = batch.column(0).as_any().downcast_ref::<Int64Array>() else {
                    return json!({"status": "error", "message": "probe expected Int64 output"});
                };
                let rows: Vec<Vec<Value>> = (0..array.len())
                    .map(|index| {
                        vec![if array.is_null(index) {
                            Value::Null
                        } else {
                            json!(array.value(index))
                        }]
                    })
                    .collect();
                json!({"status": "ok", "kind": "query", "result": {"rows": rows}})
            }
        }
    }
}

fn main() {
    let mut session = None;
    let mut stdout = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let reply = handle(request, &mut session);
        serde_json::to_writer(&mut stdout, &reply).unwrap();
        writeln!(stdout).unwrap();
        stdout.flush().unwrap();
    }
}
