#[path = "support/promoted_reference.rs"]
mod promoted_reference;
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assaydb::{
    execute::Options,
    session::{Limits, Output, Session},
};
use parquet::arrow::ArrowWriter;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "assay-reader-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn csv(&self, text: &[u8]) -> PathBuf {
        let path = self.0.join("input's.csv");
        fs::write(&path, text).unwrap();
        path
    }
    fn session(&self) -> Session {
        let mut s = Session::new(&self.0, Options::default(), Limits::default()).unwrap();
        s.set_local_files_enabled(true);
        s
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn quoted(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "''"))
}
fn query(session: &mut Session, sql: &str) -> RecordBatch {
    let Output::Query { execution, .. } = session.execute_sql(sql).unwrap() else {
        panic!("not a query")
    };
    execution.batch.clone()
}
fn ints(batch: &RecordBatch, col: usize) -> Vec<i64> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
fn assert_no_imports(s: &Session) {
    assert!(fs::read_dir(s.directory()).unwrap().all(|e| {
        !e.unwrap()
            .file_name()
            .to_str()
            .unwrap()
            .starts_with("reader-")
    }));
}
#[test]
fn local_readers_require_explicit_capability_even_inside_views() {
    let f = Files::new();
    let path = f.csv(b"id\n3\n");
    let mut s = Session::new(&f.0, Options::default(), Limits::default()).unwrap();
    let sql = format!("SELECT * FROM read_csv({})", quoted(&path));
    assert!(
        s.execute_sql(&sql)
            .unwrap_err_message()
            .contains("local file readers are disabled")
    );
    s.execute_sql(&format!("CREATE VIEW v AS {sql}")).unwrap();
    assert!(s.execute_sql("SELECT * FROM v").is_err());
    s.set_local_files_enabled(true);
    assert_eq!(query(&mut s, "SELECT * FROM v").num_rows(), 1);
    s.set_local_files_enabled(false);
    assert!(s.execute_sql("SELECT * FROM v").is_err());
    assert!(
        assaydb::execute::prepare(
            "SELECT * FROM read_parquet('/does/not/exist')",
            &Default::default(),
            &Options::default()
        )
        .err()
        .unwrap()
        .contains("local file readers are disabled")
    );
}
// Output intentionally has no Debug implementation; keep diagnostic assertions explicit.
trait ErrorMessage {
    fn unwrap_err_message(self) -> String;
}
impl ErrorMessage for Result<Output, assaydb::session::Error> {
    fn unwrap_err_message(self) -> String {
        match self {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected refusal"),
        }
    }
}
#[test]
fn csv_preserves_text_quotes_unicode_empty_fields_and_cleans_snapshots() {
    let f = Files::new();
    let path =
        f.csv("id,label,note\r\n2,\"猫,\"\"hi\"\"\",\"line1\nline2\"\r\n1,,NULL\r\n".as_bytes());
    let mut s = f.session();
    let b = query(
        &mut s,
        &format!(
            "SELECT CAST(id AS INTEGER),label,note FROM read_csv({}) ORDER BY 1",
            quoted(&path)
        ),
    );
    assert_eq!(ints(&b, 0), vec![1, 2]);
    let labels = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(labels.value(0), "");
    assert!(!labels.is_null(0));
    assert_eq!(labels.value(1), "猫,\"hi\"");
    let notes = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(notes.value(0), "NULL");
    assert_eq!(notes.value(1), "line1\nline2");
    assert_no_imports(&s);
    assert!(path.is_file());
    assert!(
        s.execute_sql(&format!("SELECT missing FROM read_csv({})", quoted(&path)))
            .is_err()
    );
    assert_no_imports(&s);
}
#[test]
fn parquet_csv_join_and_insert_select_use_native_session() {
    let f = Files::new();
    let csv = f.csv(b"id\n2\n1\n");
    let parquet = f.0.join("native.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(2), None, Some(4)]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&parquet).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut s = f.session();
    let b = query(
        &mut s,
        &format!(
            "SELECT p.id FROM read_parquet({}) p JOIN read_csv({}) c ON p.id=CAST(c.id AS INTEGER)",
            quoted(&parquet),
            quoted(&csv)
        ),
    );
    assert_eq!(ints(&b, 0), vec![2]);
    s.execute_sql("CREATE TABLE t(id INT)").unwrap();
    s.execute_sql(&format!(
        "INSERT INTO t SELECT CAST(id AS INTEGER) FROM read_csv({})",
        quoted(&csv)
    ))
    .unwrap();
    assert_eq!(
        ints(&query(&mut s, "SELECT id FROM t ORDER BY id"), 0),
        vec![1, 2]
    );
    assert_no_imports(&s);
}
#[test]
fn csv_rejects_malformed_records_headers_limits_and_keeps_inputs() {
    let f = Files::new();
    let mut s = f.session();
    for text in [
        "",
        "a,A\n1,2\n",
        "\n",
        "a.b\n1\n",
        "a,b\n1\n",
        "a\n\"bad",
        "a\n\"bad\"tail\n",
        "a\nwrong\"quote\n",
        "a\n1\r2",
    ] {
        let path = f.csv(text.as_bytes());
        assert!(
            s.execute_sql(&format!("SELECT * FROM read_csv({})", quoted(&path)))
                .is_err(),
            "{text:?}"
        );
        assert_no_imports(&s);
        assert_eq!(fs::read(&path).unwrap(), text.as_bytes());
    }
    let invalid = f.csv(b"a\n\xff\n");
    assert!(
        s.execute_sql(&format!("SELECT * FROM read_csv({})", quoted(&invalid)))
            .is_err()
    );
    assert_no_imports(&s);
    let path = f.csv(b"a,b\n");
    assert_eq!(
        query(
            &mut s,
            &format!("SELECT * FROM read_csv({})", quoted(&path))
        )
        .num_rows(),
        0
    );
    let limits = Limits {
        max_rows: 1,
        ..Limits::default()
    };
    let mut limited = Session::new(&f.0, Options::default(), limits).unwrap();
    limited.set_local_files_enabled(true);
    let path = f.csv(b"a\n1\n2\n");
    assert!(
        limited
            .execute_sql(&format!("SELECT * FROM read_csv({})", quoted(&path)))
            .is_err()
    );
    assert_no_imports(&limited);
}
#[test]
fn former_guidance_forms_promote_and_explicit_cast_examples_execute() {
    let f = Files::new();
    let mut s = f.session();
    s.execute_sql("CREATE TABLE t(x INTEGER)").unwrap();
    s.execute_sql("INSERT INTO t VALUES(NULL),(2)").unwrap();
    // Old: the mixed conditional and mixed IN subquery below were refused with
    // cast guidance. New (Helen, Oct 15): both promote to DOUBLE (DuckDB
    // answers), so that guidance no longer applies; explicit casts still work.
    let group = promoted_reference::group("guidance");
    promoted_reference::check(
        &mut s,
        &group,
        "SELECT COALESCE(x,CAST(3 AS REAL)) FROM t ORDER BY 1",
        "guidance",
    );
    assert_eq!(
        ints(
            &query(
                &mut s,
                "SELECT COALESCE(x,CAST(3 AS INTEGER)) FROM t ORDER BY 1"
            ),
            0
        ),
        vec![2, 3]
    );
    promoted_reference::check(
        &mut s,
        &group,
        "SELECT 2 IN (SELECT CAST(2 AS REAL))",
        "guidance",
    );
    let b = query(&mut s, "SELECT 2 IN (SELECT CAST(2 AS INTEGER))");
    assert!(
        b.column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap()
            .value(0)
    );
}

#[test]
fn csv_prepayment_and_cancellation_leave_no_owned_files() {
    let f = Files::new();
    let path = f.csv(b"x\n1\n");
    let limits = Limits {
        max_catalog_bytes: 1024,
        ..Limits::default()
    };
    let mut s = Session::new(&f.0, Options::default(), limits).unwrap();
    s.set_local_files_enabled(true);
    // Merely enabling readers must not consume the small catalog allowance.
    assert_eq!(ints(&query(&mut s, "SELECT 1"), 0), vec![1]);
    assert!(
        s.execute_sql(&format!("SELECT * FROM read_csv({})", quoted(&path)))
            .is_err()
    );
    assert_no_imports(&s);
    let options = Options::default();
    let cancel = options.cancellation.clone();
    let mut s = Session::new(&f.0, options, Limits::default()).unwrap();
    s.set_local_files_enabled(true);
    cancel.cancel();
    assert!(
        s.execute_sql(&format!("SELECT * FROM read_csv({})", quoted(&path)))
            .is_err()
    );
    assert_no_imports(&s);
    assert!(path.is_file());
}
