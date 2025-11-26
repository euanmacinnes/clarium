use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3 {
        let mut m = serde_json::Map::new();
        m.insert("a".into(), json!(i as i64));
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn test_where_column_not_found_message() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_where_missing";
    let shared = seed_table(&tmp, db);

    // WHERE references a missing column 'nope'
    let qtext = format!("SELECT * FROM {} WHERE nope > 0", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some());
    let msg = format!("{}", err.unwrap());
    assert!(msg.contains("Column not found in WHERE:"), "unexpected error: {}", msg);
}

#[test]
fn test_where_udf_not_found_message() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_where_udf";
    let shared = seed_table(&tmp, db);

    // WHERE uses an unknown UDF 'nosuch'
    let qtext = format!("SELECT * FROM {} WHERE nosuch(a) > 0", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some());
    let msg = format!("{}", err.unwrap());
    assert!(msg.contains("UDF 'nosuch' not found in WHERE clause"), "unexpected error: {}", msg);
}



