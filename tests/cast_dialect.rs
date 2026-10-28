//! Independent DuckDB observations and documented PostgreSQL common-domain cases.
use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use serde_json::{Value, json};

fn rows(batch: &RecordBatch) -> Value {
    json!((0..batch.num_rows()).map(|r| batch.columns().iter().map(|a| {
        if a.is_null(r) || a.data_type() == &DataType::Null { return Value::Null; }
        match a.data_type() {
            DataType::Int64 => json!(a.as_any().downcast_ref::<Int64Array>().unwrap().value(r)),
            DataType::Float64 => json!({"f64":format!("{:016x}", a.as_any().downcast_ref::<Float64Array>().unwrap().value(r).to_bits())}),
            DataType::Decimal128(_, s) => json!({"decimal":assaydb::types::decimal_text(a.as_any().downcast_ref::<Decimal128Array>().unwrap().value(r), *s)}),
            DataType::Utf8 => json!(a.as_any().downcast_ref::<StringArray>().unwrap().value(r)),
            DataType::Boolean => json!(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(r)),
            DataType::Date32 => {
                let days = a.as_any().downcast_ref::<Date32Array>().unwrap().value(r);
                json!({"date": (chrono::NaiveDate::from_ymd_opt(1970,1,1).unwrap() + chrono::Duration::days(i64::from(days))).to_string()})
            }
            other => panic!("unhandled {other:?}"),
        }
    }).collect::<Vec<_>>()).collect::<Vec<_>>())
}

#[test]
fn failed_literal_date_casts_remain_runtime_errors() {
    use assaydb::execute::{execute, prepare};
    use std::collections::HashMap;

    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let options = Options {
                workers,
                batch_size,
                ..Options::default()
            };
            // The base literal parser's answers are retained (owner decision,
            // October 16, 2026). DuckDB 1.5.6 returns 1970-02-01 and
            // 0001-01-01 (BC), the same proleptic day as year 0.
            for (text, day) in [("1970-2-01", "1970-02-01"), ("0000-01-01", "0000-01-01")] {
                for cast in [format!("CAST('{text}' AS DATE)"), format!("'{text}'::DATE")] {
                    let query =
                        prepare(&format!("SELECT {cast}"), &HashMap::new(), &options).unwrap();
                    assert_eq!(
                        rows(&execute(&query, &options).unwrap().batch),
                        json!([[{ "date": day }]])
                    );
                }
            }
            for (text, error) in [(
                "1970-02-30",
                r#"date/time field value out of range: "1970-02-30""#,
            )] {
                for cast in [format!("CAST('{text}' AS DATE)"), format!("'{text}'::DATE")] {
                    let query =
                        prepare(&format!("SELECT {cast}"), &HashMap::new(), &options).unwrap();
                    assert_eq!(execute(&query, &options).err().as_deref(), Some(error));

                    let empty = prepare(
                        &format!("SELECT {cast} AS d WHERE FALSE"),
                        &HashMap::new(),
                        &options,
                    )
                    .unwrap();
                    assert_eq!(execute(&empty, &options).unwrap().batch.num_rows(), 0);

                    let dead_arm = prepare(
                        &format!(
                            "SELECT CASE WHEN FALSE THEN {cast} ELSE DATE '1970-01-01' END AS d"
                        ),
                        &HashMap::new(),
                        &options,
                    )
                    .unwrap();
                    assert_eq!(
                        rows(&execute(&dead_arm, &options).unwrap().batch),
                        json!([[{"date": "1970-01-01"}]])
                    );
                }
            }
        }
    }
}

enum Decided {
    Rows(&'static str),
    Error(&'static str),
}

/// Cells governed by explicit owner decisions rather than the generic
/// "refuse unless PostgreSQL and DuckDB agree" rule. Expected answers come
/// from references, never from Assay output:
/// - October 15, 2026: Boolean text is 'true'/'false' and Boolean INTEGER is
///   1/0 (DuckDB 1.5.6; the PostgreSQL cast is not established).
/// - October 16, 2026: casts that returned a result on f4377d5d and are not
///   covered by a decision keep it: the base literal DATE parser accepts
///   year 0 (DuckDB: 0001-01-01 (BC), the same proleptic day).
/// - October 15, 2026: Float to DECIMAL(p,s) rounds the exact binary value
///   half away from zero (2.675 is 2.67499999999999982236431605997495353221893310546875,
///   so 2.67; DuckDB 1.5.6 reports 2.68), a value that does not fit is the
///   base "decimal cast precision overflow" error, and bare DECIMAL is
///   DECIMAL(18,3) like DuckDB.
/// - VARCHAR(n) over-length text keeps the base refusal message.
/// - October 16, 2026 (about 15:10): REAL keeps Assay's existing binary64
///   meaning and INTEGER width follows the same rule, so CAST AS INTEGER checks
///   the signed 64-bit range of Assay's INTEGER column type. PostgreSQL 17 and
///   DuckDB 1.5.6 INTEGER is 32-bit and raises `integer out of range` here (a
///   documented difference). The values are SQLite's (3.54 fixture for c026,
///   3.53.4 for the others) and equal DuckDB 1.5.6 and PostgreSQL 17.11 with
///   INTEGER read as BIGINT; c022, c026 and c094 also equal f4377d5d, which
///   refused c058's text-to-DOUBLE step. Casts at or
///   past 2^63 originally raised the shared error; the October 21 public
///   compatibility decision below supersedes that for INTEGER conversions.
const DECIDED: &[(&str, Decided)] = &[
    // October 21: preserve public INTEGER/REAL conversion behavior. These
    // hand-derived expectations apply truncation, signed saturation and numeric
    // prefixes. The PostgreSQL/DuckDB fixture remains unchanged as observations.
    ("c028", Decided::Error("BIGINT overflow")),
    ("c000", Decided::Rows("[[-3]]")),
    ("c036", Decided::Rows("[[-3]]")),
    ("c072", Decided::Rows("[[-3]]")),
    ("c002", Decided::Rows("[[-2]]")),
    ("c038", Decided::Rows("[[-2]]")),
    ("c074", Decided::Rows("[[-2]]")),
    ("c004", Decided::Rows("[[-1]]")),
    ("c040", Decided::Rows("[[-1]]")),
    ("c076", Decided::Rows("[[-1]]")),
    ("c006", Decided::Rows("[[0]]")),
    ("c008", Decided::Rows("[[0]]")),
    ("c042", Decided::Rows("[[0]]")),
    ("c044", Decided::Rows("[[0]]")),
    ("c078", Decided::Rows("[[0]]")),
    ("c080", Decided::Rows("[[0]]")),
    ("c010", Decided::Rows("[[1]]")),
    ("c046", Decided::Rows("[[1]]")),
    ("c082", Decided::Rows("[[1]]")),
    ("c012", Decided::Rows("[[2]]")),
    ("c048", Decided::Rows("[[2]]")),
    ("c084", Decided::Rows("[[2]]")),
    ("c014", Decided::Rows("[[3]]")),
    ("c050", Decided::Rows("[[3]]")),
    ("c086", Decided::Rows("[[3]]")),
    ("c062", Decided::Rows("[[9223372036854775807]]")),
    ("c064", Decided::Rows("[[9223372036854775807]]")),
    ("c098", Decided::Rows("[[9223372036854775807]]")),
    ("c100", Decided::Rows("[[9223372036854775807]]")),
    ("c102", Decided::Rows("[[0]]")),
    ("c103", Decided::Rows("[[0]]")),
    ("c104", Decided::Rows("[[0]]")),
    ("c105", Decided::Rows("[[0]]")),
    ("c106", Decided::Rows("[[0]]")),
    ("c107", Decided::Rows("[[0]]")),
    ("c126", Decided::Rows("[[0]]")),
    ("c132", Decided::Rows("[[0]]")),
    ("c156", Decided::Rows("[[0]]")),
    ("c138", Decided::Rows("[[12]]")),
    ("c144", Decided::Rows("[[1]]")),
    ("c150", Decided::Rows("[[1]]")),
    ("c162", Decided::Rows("[[1]]")),
    ("c129", Decided::Rows(r#"[[{"f64":"0000000000000000"}]]"#)),
    ("c135", Decided::Rows(r#"[[{"f64":"0000000000000000"}]]"#)),
    ("c141", Decided::Rows(r#"[[{"f64":"4028000000000000"}]]"#)),
    ("c159", Decided::Rows(r#"[[{"f64":"0000000000000000"}]]"#)),
    ("c165", Decided::Rows(r#"[[{"f64":"3ff0000000000000"}]]"#)),
    ("c022", Decided::Rows("[[2147483648]]")),
    ("c026", Decided::Rows("[[9223372036854775807]]")),
    ("c058", Decided::Rows("[[2147483648]]")),
    ("c094", Decided::Rows("[[2147483648]]")),
    ("c172", Decided::Rows(r#"[[{"date": "0000-01-01"}]]"#)),
    ("c188", Decided::Rows(r#"[["true"]]"#)),
    ("c189", Decided::Rows("[[1]]")),
    ("c191", Decided::Rows(r#"[["false"]]"#)),
    ("c192", Decided::Rows("[[0]]")),
    ("c211", Decided::Rows(r#"[[{"decimal": "1.25"}]]"#)),
    ("c212", Decided::Rows(r#"[[{"decimal": "2.67"}]]"#)),
    ("c213", Decided::Rows(r#"[[{"decimal": "0.50"}]]"#)),
    ("c215", Decided::Error("decimal cast precision overflow")),
    (
        "c230",
        Decided::Error(
            "unsupported: VARCHAR(3) result exceeds its character limit; truncation semantics differ",
        ),
    ),
    ("c232", Decided::Rows(r#"[[{"decimal": "1.235"}]]"#)),
    // Integration candidate 1 (string-functions card, Helen, October 16,
    // 2026): DOUBLE/REAL text uses DuckDB 1.5.6's shortest round-trip form, so
    // these formerly refused cells answer exactly DuckDB's recorded rows.
    ("c200", Decided::Rows(r#"[["0.0"]]"#)),
    ("c204", Decided::Rows(r#"[["1.2"]]"#)),
    ("c206", Decided::Rows(r#"[["-12.5"]]"#)),
    ("c208", Decided::Rows(r#"[["1e-06"]]"#)),
    ("c210", Decided::Rows(r#"[["1.2345678901234567e+19"]]"#)),
];

const NONFINITE: &str = "unsupported: floating cast nonfinite result";
/// Exact messages for every cell where PostgreSQL and DuckDB both raise an
/// error. Data errors follow PostgreSQL's wording (owner decision, October 16,
/// 2026). NaN/infinity intermediates stay refusals: DOUBLE cannot hold them in
/// Assay, and numeric NaN/Infinity text is accepted by PostgreSQL only.
const SHARED_ERRORS: &[(&str, &str)] = &[
    ("c028", "integer out of range"),
    ("c029", "bigint out of range"),
    (
        "c030",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    (
        "c031",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    (
        "c032",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    (
        "c033",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    (
        "c034",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    (
        "c035",
        "unsupported: CAST Text AS Decimal(10, 2) (PostgreSQL/DuckDB disagree)",
    ),
    ("c062", "integer out of range"),
    ("c063", "bigint out of range"),
    ("c064", "integer out of range"),
    ("c065", "bigint out of range"),
    ("c066", NONFINITE),
    ("c067", NONFINITE),
    ("c068", NONFINITE),
    ("c069", NONFINITE),
    ("c070", NONFINITE),
    ("c071", NONFINITE),
    ("c098", "integer out of range"),
    ("c099", "bigint out of range"),
    ("c100", "integer out of range"),
    ("c101", "bigint out of range"),
    ("c102", NONFINITE),
    ("c103", NONFINITE),
    ("c104", NONFINITE),
    ("c105", NONFINITE),
    ("c106", NONFINITE),
    ("c107", NONFINITE),
    ("c126", r#"invalid input syntax for type integer: "+""#),
    ("c127", r#"invalid input syntax for type bigint: "+""#),
    (
        "c128",
        r#"invalid input syntax for type double precision: "+""#,
    ),
    (
        "c129",
        r#"invalid input syntax for type double precision: "+""#,
    ),
    ("c130", r#"invalid input syntax for type numeric: "+""#),
    ("c132", r#"invalid input syntax for type integer: """#),
    ("c133", r#"invalid input syntax for type bigint: """#),
    (
        "c134",
        r#"invalid input syntax for type double precision: """#,
    ),
    (
        "c135",
        r#"invalid input syntax for type double precision: """#,
    ),
    ("c136", r#"invalid input syntax for type numeric: """#),
    ("c138", r#"invalid input syntax for type integer: "12abc""#),
    ("c139", r#"invalid input syntax for type bigint: "12abc""#),
    (
        "c140",
        r#"invalid input syntax for type double precision: "12abc""#,
    ),
    (
        "c141",
        r#"invalid input syntax for type double precision: "12abc""#,
    ),
    ("c142", r#"invalid input syntax for type numeric: "12abc""#),
    (
        "c170",
        r#"date/time field value out of range: "2023-02-29""#,
    ),
    (
        "c186",
        r#"invalid input syntax for type boolean: "unknown""#,
    ),
    ("c187", r#"invalid input syntax for type boolean: """#),
];

#[test]
fn postgres_duckdb_matrix_and_analysis_across_batch_and_worker_counts() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/cast_dialect/expected-v2.json")).unwrap();
    assert_eq!(fixture["duckdb_version"], "1.5.6");
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    batch_size,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            for case in fixture["cases"].as_array().unwrap() {
                for sql in [
                    case["sql"].as_str().unwrap().to_owned(),
                    format!(
                        "SELECT ({})::{}",
                        case["expression"].as_str().unwrap(),
                        case["target"].as_str().unwrap()
                    ),
                ] {
                    let result = session.execute_sql(&sql);
                    let id = case["id"].as_str().unwrap();
                    if let Some((_, expected)) = DECIDED.iter().find(|(case, _)| *case == id) {
                        match expected {
                            Decided::Rows(expected) => {
                                let Output::Query { execution, .. } =
                                    result.unwrap_or_else(|e| panic!("{sql}: {e}"))
                                else {
                                    panic!("query required")
                                };
                                assert_eq!(
                                    rows(&execution.batch),
                                    serde_json::from_str::<Value>(expected).unwrap(),
                                    "{sql}, workers={workers}, batch={batch_size}"
                                );
                            }
                            Decided::Error(message) => {
                                assert_eq!(result.err().unwrap().message, *message, "{sql}");
                            }
                        }
                        continue;
                    }
                    if case["agreement"] == "agree-error" {
                        // Both references raise a data error, so Assay raises
                        // one too, never an "unsupported:" refusal, except
                        // where an intermediate value is a NaN or infinity
                        // that Assay cannot represent.
                        let expected = SHARED_ERRORS
                            .iter()
                            .find(|(case, _)| *case == id)
                            .unwrap_or_else(|| panic!("no exact message for {id}"))
                            .1;
                        assert_eq!(result.err().unwrap().message, expected, "{sql}");
                        continue;
                    }
                    if case["expect"] == "refuse" {
                        let err = result
                            .err()
                            .unwrap_or_else(|| panic!("expected refusal: {sql}"));
                        assert!(
                            err.message.starts_with("unsupported:"),
                            "{sql}: {}",
                            err.message
                        );
                        if case["agreement"] == "disagree" {
                            assert!(
                                err.message.contains("PostgreSQL/DuckDB disagree"),
                                "{sql}: {}",
                                err.message
                            );
                        }
                    } else {
                        let Output::Query { execution, .. } =
                            result.unwrap_or_else(|e| panic!("{sql}: {e}"))
                        else {
                            panic!("query required")
                        };
                        assert_eq!(
                            rows(&execution.batch),
                            case["duckdb"]["rows"],
                            "{sql}, workers={workers}, batch={batch_size}"
                        );
                    }
                }
            }
            let analysis = &fixture["analysis"];
            for setup in analysis["setup"].as_array().unwrap() {
                session.execute_sql(setup.as_str().unwrap()).unwrap();
            }
            // The original analysis remains an immutable dialect observation.
            // Its DECIMAL amounts 12.50 and -2.50 truncate to 12 and -2 under
            // the public INTEGER contract. The BIGINT variant still rounds.
            for (sql, expected) in [
                (
                    analysis["sql"].as_str().unwrap().to_owned(),
                    json!([[1, 12, 12], [2, -2, 7], [3, null, null]]),
                ),
                (
                    analysis["sql"]
                        .as_str()
                        .unwrap()
                        .replace("AS INTEGER", "AS BIGINT"),
                    analysis["rows"].clone(),
                ),
            ] {
                let Output::Query { execution, .. } = session.execute_sql(&sql).unwrap() else {
                    panic!("query required");
                };
                assert_eq!(rows(&execution.batch), expected, "{sql}");
            }
        }
    }
}

#[test]
fn grouped_null_having_validates_discarded_narrowing_casts() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    batch_size,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            session
                .execute_sql("CREATE TABLE gc(k INTEGER, v INTEGER)")
                .unwrap();
            session
                .execute_sql("INSERT INTO gc VALUES(1,2),(1,-3),(2,NULL)")
                .unwrap();
            for sql in [
                "SELECT k FROM gc GROUP BY k HAVING NULL <> CAST(v AS INTEGER)",
                "SELECT k FROM gc GROUP BY k HAVING NOT NULL < -v*CAST(v AS INTEGER)",
                "SELECT k FROM gc GROUP BY k HAVING NOT CAST(-v AS INTEGER)*k+v/-81 BETWEEN NULL AND NULL",
            ] {
                let Output::Query { execution, .. } = session
                    .execute_sql(sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                else {
                    panic!("query")
                };
                assert_eq!(execution.batch.num_rows(), 0, "{sql}");
            }
            // INTEGER is Assay's 64-bit type (owner decision, October 16,
            // 2026), so 2147483648 is in range: no rows, as on f4377d5d and in
            // SQLite 3.53.4. (PostgreSQL 17 and DuckDB 1.5.6 reject the bare
            // ungrouped column at binding.)
            session
                .execute_sql("INSERT INTO gc VALUES(1,2147483648)")
                .unwrap();
            let sql = "SELECT k FROM gc GROUP BY k HAVING NULL <> CAST(v AS INTEGER)";
            let Output::Query { execution, .. } = session
                .execute_sql(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
            else {
                panic!("query")
            };
            assert_eq!(execution.batch.num_rows(), 0, "{sql}");
            // A discarded cast past the 64-bit range is still validated on
            // every row: -(-2^63) = 2^63 overflows BIGINT in PostgreSQL 17
            // ("bigint out of range") and DuckDB 1.5.6 (Out of Range Error).
            session
                .execute_sql("INSERT INTO gc VALUES(1,-9223372036854775808)")
                .unwrap();
            let sql = "SELECT k FROM gc GROUP BY k HAVING NULL <> CAST(-v AS BIGINT)";
            assert_eq!(
                session.execute_sql(sql).err().unwrap().message,
                "bigint out of range"
            );
            let Output::Query { execution, .. } = session
                .execute_sql(
                    "SELECT k FROM gc WHERE v<10 GROUP BY k HAVING NULL <> CAST(v AS INTEGER)",
                )
                .unwrap()
            else {
                panic!("query")
            };
            assert_eq!(execution.batch.num_rows(), 0);
            for sql in [
                "SELECT CAST(v AS INTEGER) FROM gc GROUP BY k",
                "SELECT k FROM gc GROUP BY k HAVING CAST(v AS INTEGER)>0",
                "SELECT k FROM gc GROUP BY k HAVING NULL=CAST(v/0.0 AS INTEGER)",
            ] {
                assert!(session.execute_sql(sql).is_err(), "{sql}");
            }
        }
    }
}

/// Hunt regression (311 records, harness v2 qualification of c764dcf6): a value
/// stored in an INTEGER column must cast to INTEGER. INTEGER is Assay's 64-bit
/// type (owner decision, October 16, 2026). Expected rows are DuckDB 1.5.6 and
/// PostgreSQL 17.11 answers with INTEGER read as BIGINT and REAL as DOUBLE
/// (identical to each other, to SQLite and to f4377d5d); -2^63 is exact in
/// binary64 and fits.
#[test]
fn integer_casts_accept_every_value_an_integer_column_holds() {
    for workers in [1, 4, 10] {
        for batch_size in [1, 7, 8192] {
            let mut session = Session::new(
                &std::env::temp_dir(),
                Options {
                    workers,
                    batch_size,
                    ..Options::default()
                },
                Limits::default(),
            )
            .unwrap();
            session
                .execute_sql("CREATE TABLE t(id INTEGER, n INTEGER)")
                .unwrap();
            session
                .execute_sql(
                    "INSERT INTO t VALUES (0,3000000000),(1,-9223372036854775807),(2,NULL)",
                )
                .unwrap();
            for (sql, expected) in [
                (
                    "SELECT id, CAST(n AS INTEGER) FROM t ORDER BY id",
                    json!([
                        [0, 3000000000_i64],
                        [1, -9223372036854775807_i64],
                        [2, null]
                    ]),
                ),
                (
                    "SELECT id, CAST(CAST(n AS REAL) AS INTEGER) FROM t ORDER BY id",
                    json!([[0, 3000000000_i64], [1, i64::MIN], [2, null]]),
                ),
                (
                    "SELECT id, CAST(COALESCE(n,5.0) AS INTEGER) / 2 FROM t ORDER BY id",
                    json!([[0, 1500000000_i64], [1, -4611686018427387903_i64], [2, 2]]),
                ),
                (
                    "SELECT id, CAST(n AS BIGINT) FROM t ORDER BY id",
                    json!([
                        [0, 3000000000_i64],
                        [1, -9223372036854775807_i64],
                        [2, null]
                    ]),
                ),
            ] {
                let Output::Query { execution, .. } = session
                    .execute_sql(sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                else {
                    panic!("query required")
                };
                assert_eq!(
                    rows(&execution.batch),
                    expected,
                    "{sql}, workers={workers}, batch={batch_size}"
                );
            }
            // INTEGER preserves public saturation; the separately introduced
            // BIGINT spelling retains its checked conversion error.
            let Output::Query { execution, .. } = session
                .execute_sql("SELECT CAST(CAST(9223372036854775807 AS REAL) AS INTEGER)")
                .unwrap()
            else {
                panic!("query required");
            };
            assert_eq!(rows(&execution.batch), json!([[i64::MAX]]));
            let sql = "SELECT CAST(CAST(9223372036854775807 AS REAL) AS BIGINT)";
            assert_eq!(
                session.execute_sql(sql).err().unwrap().message,
                "bigint out of range",
                "{sql}"
            );
        }
    }
}
