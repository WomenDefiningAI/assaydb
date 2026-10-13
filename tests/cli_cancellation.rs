use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::{self, File},
    io::Write,
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[test]
fn cli_signals_cancel_active_spills_and_leave_no_results_or_temporary_files() {
    let root = std::env::temp_dir().join(format!("assaydb-cli-cancel-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let spill = root.join("spill");
    fs::create_dir(&spill).unwrap();
    let input = root.join("input.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..4096)),
        Arc::new(StringArray::from_iter_values(
            (0..4096).map(|i| format!("{i:08}{}", "w".repeat(2048))),
        )),
    ];
    let mut writer = ArrowWriter::try_new(
        File::create_new(&input).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(128)
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(&RecordBatch::try_new(schema, arrays).unwrap())
        .unwrap();
    writer.close().unwrap();
    for workers in [1, 4, 10] {
        for (index, sql) in [
        "SELECT i,s FROM t ORDER BY s DESC,i",
        "SELECT s,COUNT(*) AS n FROM t GROUP BY s ORDER BY s",
        "SELECT a.i AS i,b.s AS s FROM t a LEFT JOIN t b ON a.i=b.i ORDER BY i",
        "WITH x AS (SELECT i,s FROM t) SELECT a.i AS i,b.s AS s FROM x a JOIN x b ON a.i=b.i ORDER BY i",
    ]
    .iter()
    .enumerate()
    {
        for signal in [libc::SIGINT, libc::SIGTERM] {
            let output = root.join(format!("result-{index}-{signal}.json"));
            let arrow = root.join(format!("result-{index}-{signal}.arrow"));
            let request = serde_json::json!({"threads":workers,"sql":sql,"tables":{"t":input},"output":output,"arrow_output":arrow,"batch_size":64,"memory_bytes":32<<20,"no_parquet_cache":true,"spill":{"directory":spill,"disk_quota_bytes":256<<20,"sort_run_bytes":16384,"merge_fan_in":2}});
            let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(request.to_string().as_bytes())
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut saw_file = false;
            while Instant::now() < deadline {
                saw_file = fs::read_dir(&spill)
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|dir| {
                        fs::read_dir(dir.path()).is_ok_and(|mut files| files.next().is_some())
                    });
                if saw_file || child.try_wait().unwrap().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            if !saw_file {
                let _ = child.kill();
                let out = child.wait_with_output().unwrap();
                panic!(
                    "no active spill observed: {} {}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            // SAFETY: send the selected Unix signal only to our live child PID.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
            let deadline = Instant::now() + Duration::from_secs(5);
            while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            if child.try_wait().unwrap().is_none() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("cooperative cancellation exceeded five seconds");
            }
            let out = child.wait_with_output().unwrap();
            assert_eq!(
                out.status.code(),
                Some(1),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let events: Vec<serde_json::Value> = String::from_utf8(out.stdout)
                .unwrap()
                .lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
            let failure = events.iter().find(|e| e["event"] == "failure").unwrap();
            assert_eq!(failure["status"], "cancelled", "{events:?}");
            assert!(
                !events
                    .iter()
                    .any(|e| e["event"] == "sample" || e["event"] == "done")
            );
            assert!(!output.exists() && !arrow.exists());
            assert_eq!(fs::read_dir(&spill).unwrap().count(), 0);
        }
    }
    }
    fs::remove_file(input).unwrap();
    fs::remove_dir(spill).unwrap();
    fs::remove_dir(root).unwrap();
}
