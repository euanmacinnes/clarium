use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn make_minute_series(start_ms: i64, minutes: i64) -> Vec<Record> {
    let mut recs: Vec<Record> = Vec::with_capacity(minutes as usize);
    for i in 0..minutes {
        let mut m = serde_json::Map::new();
        m.insert("value".into(), json!((i + 1) as f64));
        recs.push(Record { _time: start_ms + i * 60_000, sensors: m });
    }
    recs
}

#[test]
fn test_time_by_5m_aggregates() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "demo/public/tt_by.time";
    let base: i64 = 1_700_000_000_000; // ms
    let recs = make_minute_series(base, 10); // 10 minutes
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let qtext = format!("SELECT COUNT(value) AS cnt, SUM(value) AS s FROM {} BY 5m", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Expect at least one result row and correct aggregate columns
    assert!(df.height() >= 1);
    let names = df.get_column_names();
    assert!(names.iter().any(|c| c.as_str() == "cnt" || c.as_str() == "COUNT(value)"));
    assert!(names.iter().any(|c| c.as_str() == "s" || c.as_str() == "SUM(value)"));

    // Check that the sum across all BY windows equals 55.0
    let sum_col_name = if names.iter().any(|c| c.as_str() == "s") { "s" } else { "SUM(value)" };
    let sums = df.column(sum_col_name).unwrap().f64().unwrap();
    let mut total = 0.0;
    for i in 0..sums.len() { total += sums.get(i).unwrap_or(0.0); }
    assert!((total - 55.0).abs() < 1e-9);
}

#[test]
fn test_time_query_dotted_and_path_resolution() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create at fully-qualified path on disk
    let fq = "demo/public/tt2.time";
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::from_iter(vec![("value".into(), json!(1.0))]) }];
    store.write_records(fq, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Dotted fully-qualified should work
    let q1 = match query::parse("SELECT COUNT(value) AS c FROM demo/public/tt2.time").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    let c1 = df1.column("c").or_else(|_| df1.column("COUNT(value)"));
    assert!(c1.is_ok());
}

// Note: The following test mirrors the user's example, but only asserts that it executes without panicking.
// Depending on engine semantics, non-aggregate projections with BY may compute window-wise representative values.
#[test]
fn test_time_by_5m_user_example_executes() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "demo.time";
    let base: i64 = 1_700_010_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..10 { let mut m = serde_json::Map::new(); m.insert("value".into(), json!(i as f64)); recs.push(Record { _time: base + i * 60_000, sensors: m }); }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let qtext = format!("SELECT _time, value FROM {} BY 5m", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Should execute and return at least one row
    assert!(df.height() >= 1);
}



