//! Dedicated native session protocol; the benchmark executable is unchanged.
use arrow::{array::*, record_batch::RecordBatch};
use assaydb::{
    Result,
    execute::Options,
    expr::is_sql_null,
    session::{self, Limits, Output, Session},
    types::{Type, binary_hex, decimal_text},
};
use chrono::{Duration, NaiveDate};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::PathBuf,
};
const MAX_REPLY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    op: String,
    directory: PathBuf,
    memory_bytes: Option<usize>,
    threads: Option<usize>,
    max_catalog_bytes: Option<usize>,
    /// Session time zone (IANA Area/City, UTC or GMT); default UTC.
    #[serde(default)]
    timezone: Option<String>,
    /// Fixed statement clock as TIMESTAMPTZ text; else ASSAY_FIXED_CLOCK.
    #[serde(default)]
    fixed_clock: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    sql: String,
}
fn failure(error: session::Error) -> Value {
    let status = match error.kind {
        "unsupported" => "unsupported",
        "resource" => "resource",
        "internal" => "internal",
        _ => "error",
    };
    json!({"status":status,"error_kind":error.kind,"message":error.message,"error_cause":error.cause,"error_cause_schema":"assay-error-cause/v1"})
}
fn protocol(message: impl ToString) -> Value {
    json!({"status":"error","error_kind":"protocol","message":message.to_string()})
}
fn handle(text: &str, session: &mut Option<Session>) -> Value {
    if session.is_none() {
        let open: Open = match serde_json::from_str(text) {
            Ok(o) => o,
            Err(e) => return protocol(e),
        };
        if open.op != "open" {
            return protocol("first request must open a session");
        }
        let mut options = Options::default();
        if let Some(n) = open.threads {
            options.workers = n;
        }
        if let Some(n) = open.memory_bytes {
            options.memory_bytes = n;
        }
        if let Some(name) = &open.timezone {
            match assaydb::zone::SessionZone::parse(name) {
                Ok(zone) => options.time_zone = zone,
                Err(e) => return protocol(e),
            }
        }
        if let Some(text) = open
            .fixed_clock
            .clone()
            .or_else(|| std::env::var("ASSAY_FIXED_CLOCK").ok())
        {
            match assaydb::zone::fixed_clock(&text) {
                Ok(micros) => options.fixed_clock = Some(micros),
                Err(e) => return protocol(e),
            }
        }
        let mut limits = Limits::default();
        if let Some(n) = open.max_catalog_bytes {
            limits.max_catalog_bytes = n;
        }
        let reply = json!({"status":"ok","kind":"session","workers":options.workers,"memory_bytes":options.memory_bytes,"max_catalog_bytes":limits.max_catalog_bytes,"max_tables":limits.max_tables,"max_columns":limits.max_columns,"max_rows_per_table":limits.max_rows,"max_sql_bytes":session::MAX_SQL_BYTES,"max_request_bytes":session::MAX_SQL_BYTES,"max_reply_bytes":MAX_REPLY_BYTES,"memory_scope":"accounted native catalog, bounded requests/insert cells/replies and existing query accounting; allocator/parser/codec overhead is not a hard process cap"});
        match Session::new(&open.directory, options, limits) {
            Ok(s) => {
                let mut reply = reply;
                reply["directory"] = json!(s.directory());
                *session = Some(s);
                reply
            }
            Err(e) => failure(e),
        }
    } else {
        let query: Query = match serde_json::from_str(text) {
            Ok(q) => q,
            Err(e) => return protocol(e),
        };
        match session.as_mut().unwrap().execute_sql(&query.sql) {
            Ok(Output::Statement { affected_rows }) => {
                json!({"status":"ok","kind":"statement","affected_rows":affected_rows})
            }
            Ok(Output::Query { execution, types }) => {
                let batch = &execution.batch;
                // Bound JSON Value allocation independently from engine output accounting.
                let estimate = reply_allocation_bytes(batch);
                if estimate.is_none_or(|n| n > MAX_REPLY_BYTES) {
                    return failure(session::Error {
                        cause: None,
                        kind: "resource",
                        message: "query reply allocation limit exceeded".into(),
                    });
                }
                match wire(batch, &types) {
                    Ok(result) => json!({"status":"ok","kind":"query","result":result}),
                    Err(e) => failure(session::Error {
                        cause: None,
                        kind: "internal",
                        message: e.to_string(),
                    }),
                }
            }
            Err(e) => failure(e),
        }
    }
}
struct ReplyBuffer(Vec<u8>);
impl Write for ReplyBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_REPLY_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("reply limit exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn run() -> io::Result<()> {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    let mut session = None;
    loop {
        let mut line = Vec::new();
        let mut too_long = false;
        let mut found = false;
        loop {
            let bytes = input.fill_buf()?;
            if bytes.is_empty() {
                break;
            }
            found = true;
            let end = bytes.iter().position(|b| *b == b'\n').map(|n| n + 1);
            let take = end.unwrap_or(bytes.len());
            if line.len() + take > session::MAX_SQL_BYTES {
                too_long = true;
            }
            if !too_long {
                line.extend_from_slice(&bytes[..take]);
            }
            input.consume(take);
            if end.is_some() {
                break;
            }
        }
        if !found {
            break;
        }
        let reply = if too_long {
            failure(session::Error {
                cause: None,
                kind: "resource",
                message: "request exceeds 1 MiB".into(),
            })
        } else {
            match std::str::from_utf8(&line) {
                Ok(text) => handle(text, &mut session),
                Err(e) => protocol(e),
            }
        };
        let mut buffer = ReplyBuffer(Vec::new());
        if serde_json::to_writer(&mut buffer, &reply).is_err() {
            buffer.0.clear();
            serde_json::to_writer(
                &mut buffer,
                &failure(session::Error {
                    cause: None,
                    kind: "resource",
                    message: "serialized reply exceeds 16 MiB".into(),
                }),
            )?;
        }
        output.write_all(&buffer.0)?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
fn main() {
    if let Err(e) = assaydb::engine_stack::run(run).and_then(|result| result) {
        eprintln!("session protocol I/O: {e}");
        std::process::exit(1);
    }
}

// Include expanded hexadecimal strings before constructing JSON Values.
fn reply_allocation_bytes(batch: &RecordBatch) -> Option<usize> {
    let base = batch
        .num_rows()
        .checked_mul(batch.num_columns())?
        .checked_mul(64)?
        .checked_add(batch.get_array_memory_size())?;
    batch.columns().iter().try_fold(base, |bytes, array| {
        if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
            array.iter().flatten().try_fold(bytes, |bytes, value| {
                bytes.checked_add(value.len().checked_mul(2)?)
            })
        } else {
            Some(bytes)
        }
    })
}

fn wire(batch: &RecordBatch, types: &[Type]) -> Result<Value> {
    let schema=batch.schema().fields().iter().zip(types).map(|(f,t)|json!({"name":f.name(),"type":match t{Type::Int=>"int",Type::Decimal(_)=>"decimal",Type::Float=>"float",Type::Bool=>"bool",Type::Text=>"str",Type::Binary=>"binary",Type::Date=>"date",Type::Timestamp=>"timestamp",Type::TimestampTz=>"timestamptz",Type::Null=>"null"}})).collect::<Vec<_>>();
    let mut rows = Vec::new();
    for i in 0..batch.num_rows() {
        let mut row = Vec::new();
        for (a, t) in batch.columns().iter().zip(types) {
            let v = if is_sql_null(a.as_ref(), i) {
                Value::Null
            } else {
                match t {
                    Type::Int => json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(i)),
                    Type::Decimal(s) => json!(decimal_text(
                        a.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .value(i),
                        *s
                    )),
                    Type::Float => json!(
                        a.as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(i)
                            .to_string()
                    ),
                    Type::Bool => {
                        json!(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i))
                    }
                    Type::Text => json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(i)),
                    Type::Binary => json!(binary_hex(
                        a.as_any().downcast_ref::<BinaryArray>().unwrap().value(i)
                    )?),
                    Type::Date => {
                        let d = a.as_any().downcast_ref::<Date32Array>().unwrap().value(i);
                        let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_signed(Duration::days(i64::from(d)))
                            .ok_or("date outside output range")?;
                        json!(date.to_string())
                    }
                    Type::Timestamp => json!(assaydb::expr::timestamp::format(
                        a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                    )?),
                    Type::TimestampTz => json!(assaydb::zone::format_labelled(a.as_ref(), i)?),
                    Type::Null => Value::Null,
                }
            };
            row.push(v);
        }
        rows.push(row);
    }
    Ok(json!({"schema":schema,"rows":rows,"physical_schema":batch.schema().to_string()}))
}

#[cfg(test)]
#[path = "../../tests/support/unsupported_labels_input.rs"]
mod unsupported_labels_input;

#[cfg(test)]
mod unsupported_labels_tests {
    use super::*;

    #[test]
    fn unsupported_parquet_types_reach_the_session_reply() {
        let root =
            std::env::temp_dir().join(format!("assay-session-labels-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let mut native = Session::new(&root, Options::default(), Limits::default()).unwrap();
        // File readers are an existing library opt-in. Exercise the real JSON
        // reply handler with that opt-in; do not add a protocol capability.
        native.set_local_files_enabled(true);
        let mut session = Some(native);
        for (index, ty) in unsupported_labels_input::input_types().iter().enumerate() {
            let path = root.join(format!("input-{index}.parquet"));
            unsupported_labels_input::write_input(&path, ty);
            for projection in ["x", "COUNT(x)"] {
                let sql = format!(
                    "SELECT {projection} FROM read_parquet('{}')",
                    path.to_str().unwrap().replace('\'', "''")
                );
                let reply = handle(&json!({"sql":sql}).to_string(), &mut session);
                assert_eq!(reply["status"], "unsupported", "{sql}: {reply}");
                assert_eq!(reply["error_kind"], "unsupported");
                assert_eq!(
                    reply["message"],
                    unsupported_labels_input::expected_message(ty)
                );
            }
        }
        let reply = handle(r#"{"sql":"SELECT 7 AS value"}"#, &mut session);
        assert_eq!(reply["status"], "ok");
        assert_eq!(reply["result"]["rows"], json!([[7]]));
        drop(session);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod binary_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    #[test]
    fn binary_hex_expansion_is_rejected_before_json_allocation() {
        let payload = vec![255; 6 * 1024 * 1024];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, false)])),
            vec![Arc::new(BinaryArray::from(vec![payload.as_slice()]))],
        )
        .unwrap();
        assert!(batch.get_array_memory_size() < MAX_REPLY_BYTES);
        assert!(reply_allocation_bytes(&batch).unwrap() > MAX_REPLY_BYTES);
    }
}
