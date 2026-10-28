//! The wire cause comes from the failing operation, never from rendered text.
use assaydb::{
    ErrorCause,
    execute::{Options, execute, prepare},
};
use std::{
    collections::HashMap,
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn decimal_precision_cause_survives_preparation_and_execution() {
    for workers in [1, 4] {
        for relational_optimizations in [false, true] {
            let options = Options {
                workers,
                relational_optimizations,
                ..Options::default()
            };
            for sql in [
                "SELECT CAST(1000 AS DECIMAL(3,0))",
                "SELECT CAST(999.99 AS DECIMAL(3,2))",
                "SELECT CAST(CAST(1000000000 AS DOUBLE) AS DECIMAL(9,2))",
            ] {
                let error = prepare(sql, &HashMap::new(), &options)
                    .and_then(|p| execute(&p, &options))
                    .err()
                    .expect("overflow");
                assert_eq!(
                    error.cause(),
                    Some(ErrorCause::DecimalPrecisionOverflow),
                    "{sql}: {error}"
                );
            }
            let error = prepare(
                "SELECT missing_for_error_control(1)",
                &HashMap::new(),
                &options,
            )
            .and_then(|p| execute(&p, &options))
            .err()
            .expect("unrelated error");
            assert_eq!(error.cause(), None);
        }
    }
}

#[test]
fn cli_failure_preserves_typed_cause_and_does_not_label_unrelated_errors() {
    for (sql, cause) in [
        (
            "SELECT CAST(1000 AS DECIMAL(3,0))",
            Some("decimal_precision_overflow"),
        ),
        ("SELECT missing_for_error_control(1)", None),
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_assaydb"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let request = serde_json::json!({"sql":sql,"tables":{},"output":std::env::temp_dir().join(format!("assay-error-cause-{}.json",child.id())),"mode":"correctness","threads":4});
        child
            .stdin
            .take()
            .unwrap()
            .write_all(serde_json::to_string(&request).unwrap().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let failure = events
            .iter()
            .find(|v| v["event"] == "failure")
            .expect("failure event");
        assert_eq!(failure["error_cause_schema"], "assay-error-cause/v1");
        assert_eq!(failure["error_cause"].as_str(), cause);
    }
}
