use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_stdev_global_aggregate() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_stdev.time";
    let base: i64 = 1_700_010_000_000;
    let vals = [1.0, 2.0, 3.0, 4.0];
    let mut recs: Vec<Record> = Vec::new();
    for (i, v) in vals.iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(*v));
        recs.push(Record { _time: base + (i as i64)*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT STDEV(v) FROM {}", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("STDEV(v)").unwrap().f64().unwrap();
    let val = st.get(0).unwrap();
    let expected = (5.0f64/3.0).sqrt();
    assert!((val - expected).abs() < 1e-9);
}

#[test]
fn test_stdev_by_window_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_stdev_by.time";
    let base: i64 = 1_700_020_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..120 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!((i % 10) as f64));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT STDEV(v) FROM {} BY 1m", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() >= 2);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
    assert!(cols.iter().any(|c| c.as_str() == "STDEV(v)"));
}


