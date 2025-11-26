use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn write_time_series(store: &Store, path: &str, base: i64, n: i64) {
    let _recs: Vec<Record> = Vec::with_capacity(n as usize);
    for i in 0..n {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!((i + 1) as f64));
        store.write_records(path, &vec![Record { _time: base + i * 1000, sensors: m }]).unwrap();
    }
}

#[test]
fn test_join_with_aliases_and_column_resolution() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let base: i64 = 1_700_020_000_000;
    let left = "timeline/public/left_t.time";
    let right = "timeline/public/right_t.time";
    // write a few points with identical timestamps
    for i in 0..3i64 {
        let mut lm = serde_json::Map::new(); lm.insert("v".into(), json!((i + 1) as f64));
        store.write_records(left, &vec![Record { _time: base + i * 1000, sensors: lm }]).unwrap();
        let mut rm = serde_json::Map::new(); rm.insert("v".into(), json!(((i + 1) * 10) as f64));
        store.write_records(right, &vec![Record { _time: base + i * 1000, sensors: rm }]).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();

    let qtext = "SELECT a.v AS av, b.v AS bv FROM timeline.public.left_t.time a INNER JOIN timeline.public.right_t.time b ON a._time = b._time";
    let q = match query::parse(qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "av"));
    assert!(cols.iter().any(|c| c.as_str() == "bv"));
    assert_eq!(df.height(), 3);
}

#[test]
fn test_join_unqualified_ambiguous_column_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let base: i64 = 1_700_030_000_000;
    let left = "j1.time";
    let right = "j2.time";
    for i in 0..2i64 {
        let mut lm = serde_json::Map::new(); lm.insert("v".into(), json!((i + 1) as f64));
        store.write_records(left, &vec![Record { _time: base + i * 1000, sensors: lm }]).unwrap();
        let mut rm = serde_json::Map::new(); rm.insert("v".into(), json!(((i + 1) * 10) as f64));
        store.write_records(right, &vec![Record { _time: base + i * 1000, sensors: rm }]).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();

    // SELECT v without alias should be ambiguous in a join of two tables that both have column 'v'
    let qtext = "SELECT v FROM timeline.public.j1.time a INNER JOIN timeline.public.j2.time b ON a._time = b._time";
    let q = match query::parse(qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some(), "expected ambiguity error when selecting unqualified column in JOIN");
}



