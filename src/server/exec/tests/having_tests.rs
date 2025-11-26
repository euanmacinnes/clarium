use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_having_rejects_non_final_column() {
    // Build a small regular table and run an aggregate projection with HAVING referencing a non-final column
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_having"; // regular table is fine for simple aggregation

    // Write a few rows with column 'v'
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(i as f64));
        // regular tables ignore _time; still provide to satisfy Record
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // HAVING references a non-final column 'v' (final projection only has AVG(v) or alias)
    let qtext = format!("SELECT AVG(v) AS avgv FROM {} HAVING v > 0", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some(), "expected HAVING to fail when referencing non-final column");
    let msg = format!("{}", err.unwrap());
    assert!(msg.contains("Column not found in HAVING:"), "unexpected error: {}", msg);
}

#[test]
fn test_having_udf_not_found_message() {
    // Build a small table
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_having_udf";

    // Write a few rows with column 'v'
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(i as f64));
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // HAVING uses an unknown UDF 'nosuch'
    let qtext = format!("SELECT AVG(v) FROM {} HAVING nosuch(v) > 0", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some(), "expected HAVING to fail with unknown UDF");
    let msg = format!("{}", err.unwrap());
    assert!(msg.contains("UDF 'nosuch' not found in HAVING clause"), "unexpected error: {}", msg);
}



