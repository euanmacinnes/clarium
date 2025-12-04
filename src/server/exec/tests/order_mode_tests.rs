use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;
use crate::system;

fn seed_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3i64 {
        let mut m = serde_json::Map::new();
        m.insert("a".into(), json!(i));
        m.insert("b".into(), json!(2*i));
        recs.push(Record { _time: i, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn test_order_by_strict_requires_projected_column() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_order_strict";
    let shared = seed_table(&tmp, db);
    let prev = system::get_strict_projection();
    system::set_strict_projection(true);
    // ORDER BY non-projected column 'b' should error in strict mode
    let q = format!("SELECT a FROM {} ORDER BY b", db);
    let q = match query::parse(&q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some());
    let msg = format!("{}", err.unwrap());
    assert!(msg.contains("Column b not found in ORDER BY:"), "unexpected: {}", msg);
    system::set_strict_projection(prev);
}

#[test]
fn test_order_by_loose_temp_inject_and_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_order_loose";
    let shared = seed_table(&tmp, db);
    let prev = system::get_strict_projection();
    system::set_strict_projection(false);
    // ORDER BY on a non-projected column should still sort and then drop the temp column
    let q = format!("SELECT a FROM {} ORDER BY b DESC LIMIT 1", db);
    let q = match query::parse(&q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    // Only projected column 'a' should remain; 'b' must not appear in the final projection
    assert!(df.get_column_names().iter().all(|c| c.as_str() != "b"));
    system::set_strict_projection(prev);
}



