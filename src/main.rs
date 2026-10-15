use arrow::{array::*, ipc::writer::FileWriter, record_batch::RecordBatch};
use assaydb::{
    Result,
    execute::{Options, execute, prepare},
    expr::is_sql_null,
    types::{Type, binary_hex, decimal_text},
};
use chrono::{Duration, NaiveDate};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

mod cli_signals;

#[derive(Deserialize)]
struct Request {
    #[serde(default)]
    threads: Option<usize>,
    sql: String,
    #[serde(default)]
    tables: HashMap<String, PathBuf>,
    output: PathBuf,
    #[serde(default)]
    plan_output: Option<PathBuf>,
    #[serde(default)]
    arrow_output: Option<PathBuf>,
    #[serde(default = "mode")]
    mode: String,
    #[serde(default = "repetitions")]
    repetitions: usize,
    #[serde(default)]
    batch_size: Option<usize>,
    #[serde(default)]
    memory_bytes: Option<usize>,
    #[serde(default)]
    no_projection_pushdown: bool,
    #[serde(default)]
    execution_only: bool,
    #[serde(default)]
    no_relational_optimizations: bool,
    #[serde(default)]
    no_parquet_cache: bool,
    #[serde(default)]
    parquet_cache_bytes: Option<usize>,
    #[serde(default)]
    spill: Option<assaydb::spill::SpillOptions>,
}
fn mode() -> String {
    "correctness".into()
}
fn repetitions() -> usize {
    5
}
fn usage() -> Option<(u64, f64)> {
    let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the complete C structure on success. No
    // value is read when the call fails. This measures only this worker.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) } != 0 {
        return None;
    }
    let value = unsafe { value.assume_init() };
    let unit = if cfg!(target_os = "macos") { 1 } else { 1024 };
    let rss = u64::try_from(value.ru_maxrss).ok()?.checked_mul(unit)?;
    let cpu = value.ru_utime.tv_sec as f64
        + value.ru_stime.tv_sec as f64
        + (value.ru_utime.tv_usec + value.ru_stime.tv_usec) as f64 / 1e6;
    Some((rss, cpu))
}
fn event(v: Value) {
    println!("{v}");
    io::stdout().flush().ok();
}
fn new_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| e.to_string())
}
fn write_json(path: &Path, value: &Value) -> Result<()> {
    serde_json::to_writer_pretty(new_file(path)?, value).map_err(|e| e.to_string())
}
fn wire(batch: &RecordBatch, types: &[Type]) -> Result<Value> {
    let schema=batch.schema().fields().iter().zip(types).map(|(f,t)|json!({"name":f.name(),"type":match t{Type::Int=>"int",Type::Decimal(_)=>"decimal",Type::Float=>"float",Type::Bool=>"bool",Type::Text=>"str",Type::Binary=>"binary",Type::Date=>"date",Type::Null=>"null"}})).collect::<Vec<_>>();
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
                    Type::Null => Value::Null,
                }
            };
            row.push(v);
        }
        rows.push(row);
    }
    Ok(json!({"schema":schema,"rows":rows,"physical_schema":batch.schema().to_string()}))
}
fn run() -> Result<()> {
    let mut text = String::new();
    io::stdin()
        .take(1024 * 1024 + 1)
        .read_to_string(&mut text)
        .map_err(|e| e.to_string())?;
    if text.len() > 1024 * 1024 {
        return Err("request >1 MiB".into());
    }
    let r: Request = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if !matches!(r.mode.as_str(), "correctness" | "timing" | "explain") {
        return Err("unsupported mode".into());
    }
    if r.mode == "timing" && !(5..=100).contains(&r.repetitions) {
        return Err("timing repetitions must be 5..100".into());
    }
    let signals = cli_signals::Signals::install()?;
    let defaults = Options::default();
    let options = Options {
        workers: r.threads.unwrap_or(defaults.workers),
        batch_size: r.batch_size.unwrap_or(defaults.batch_size),
        memory_bytes: r.memory_bytes.unwrap_or(defaults.memory_bytes),
        projection_pushdown: !r.no_projection_pushdown,
        relational_optimizations: !r.no_relational_optimizations,
        spill: r.spill.clone(),
        cancellation: signals.cancellation(),
        parquet_cache_bytes: if r.no_parquet_cache {
            None
        } else {
            r.parquet_cache_bytes.or(defaults.parquet_cache_bytes)
        },
        error_replay: defaults.error_replay,
    };
    event(
        json!({"event":"ready","engine_info":{"engine":"assaydb","stage":assaydb::IMPLEMENTATION_STAGE,"workers":options.workers,"worker_scope":"total execution thread limit including coordinator","spill_enabled":options.spill.is_some(),"spill_mode":options.spill.as_ref().map(|s|s.mode),"batch_size":options.batch_size,"parquet_cache_bytes":options.parquet_cache_bytes,"memory_budget_bytes":options.memory_bytes,"hard_process_memory_cap":false,"memory_scope":"accounted Arrow batches, estimated expression/state/output reservations; decoder, parser, allocator overhead, thread stacks and sort internals not hard capped"}}),
    );
    let mut prepared = None;
    if r.execution_only {
        let start = Instant::now();
        prepared = Some(prepare(&r.sql, &r.tables, &options)?);
        event(json!({"event":"prepared","planning_seconds":start.elapsed().as_secs_f64()}));
    }
    let count = if r.mode == "timing" {
        r.repetitions + 1
    } else {
        1
    };
    for index in 0..count {
        event(json!({"event":"start","index":index}));
        let usage_before = usage();
        let start = Instant::now();
        let planned;
        let plan = if let Some(p) = &prepared {
            p
        } else {
            planned = prepare(&r.sql, &r.tables, &options)?;
            &planned
        };
        let planning = start.elapsed().as_secs_f64();
        if r.mode == "explain" {
            write_json(
                &r.output,
                &json!({"plan":plan.plan,"relational_plan":plan.relational,"input_schema":plan.schema.to_string()}),
            )?;
            break;
        }
        let execution = execute(plan, &options)?;
        options.cancellation.check()?;
        let seconds = start.elapsed().as_secs_f64();
        let usage_after = usage();
        let cpu_seconds = usage_before.zip(usage_after).map(|(a, b)| b.1 - a.1);
        event(
            json!({"event":"sample","index":index,"warmup":r.mode=="timing"&&index==0,"seconds":seconds,"planning_seconds":planning,"row_count":execution.batch.num_rows(),"metrics":execution.metrics,"cpu_seconds":cpu_seconds,"peak_rss_bytes":usage_after.map(|u|u.0),"process_cpu_seconds":usage_after.map(|u|u.1),"resource_sampling_error":if usage_after.is_none(){Some("getrusage failed")}else{None},"timing_boundary":if r.execution_only{"prepared-plan-to-arrow"}else{"sql-to-materialized-arrow"}}),
        );
        if r.mode == "correctness" {
            write_json(&r.output, &wire(&execution.batch, &plan.plan.types)?)?;
            if let Some(path) = &r.plan_output {
                write_json(
                    path,
                    &json!({"plan":plan.plan,"relational_plan":plan.relational,"metrics":execution.metrics,"input_schema":plan.schema.to_string()}),
                )?;
            }
            if let Some(path) = &r.arrow_output {
                let mut writer = FileWriter::try_new(new_file(path)?, &execution.batch.schema())
                    .map_err(|e| e.to_string())?;
                writer.write(&execution.batch).map_err(|e| e.to_string())?;
                writer.finish().map_err(|e| e.to_string())?;
            }
        }
    }
    event(json!({"event":"done"}));
    Ok(())
}
fn main() {
    if let Err(message) = run() {
        let status = if message.starts_with("cancelled:") {
            "cancelled"
        } else if message.starts_with("resource-limit:") {
            "resource-limit"
        } else if message.starts_with("unsupported:") {
            "unsupported"
        } else {
            "error"
        };
        event(json!({"event":"failure","status":status,"message":message}));
        std::process::exit(1);
    }
}
