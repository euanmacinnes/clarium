use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;
use std::time::Instant;
use crate::tprintln;

#[test]
fn test_perf_retrieval_one_day_per_second() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "clarium/public/db1.time";
    let start_ms: i64 = 1_700_000_000_000;
    let count: i64 = 86_400;
    let mut records: Vec<Record> = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut sensors = serde_json::Map::new();
        sensors.insert("v".into(), json!(i as f64));
        records.push(Record { _time: start_ms + i * 1000, sensors });
    }
    store.write_records(db, &records).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let end_ms = start_ms + (count - 1) * 1000;
    let qtext = format!(
        "SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {}",
        db, start_ms, end_ms
    );
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let t0 = Instant::now();
    let df = run_select(&shared, &q).unwrap();
    let dur = t0.elapsed();
    assert_eq!(df.height(), 86_400);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
    assert!(cols.iter().any(|c| c.as_str() == "v"));
    tprintln!(
        "perf(retrieval one day per-second): rows={}, cols={:?}, elapsed_ms={}",
        df.height(), cols, dur.as_millis()
    );
}

#[test]
fn test_perf_aggregate_one_day_per_second() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "clarium/public/db1.time";
    let start_ms: i64 = 1_700_000_000_000;
    let count: i64 = 86_400;
    let mut records: Vec<Record> = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut sensors = serde_json::Map::new();
        sensors.insert("v".into(), json!((i % 100) as f64));
        records.push(Record { _time: start_ms + i * 1000, sensors });
    }
    store.write_records(db, &records).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let end_ms = start_ms + (count - 1) * 1000;
    let qtext = format!(
        "SELECT AVG(v), COUNT(v) FROM {} BY 1m WHERE _time BETWEEN {} AND {}",
        db, start_ms, end_ms
    );
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let t0 = Instant::now();
    let df = run_select(&shared, &q).unwrap();
    let dur = t0.elapsed();
    let start_bucket = (start_ms / 60_000) * 60_000;
    let end_bucket = (end_ms / 60_000) * 60_000;
    let expected_buckets = ((end_bucket - start_bucket) / 60_000 + 1) as usize;
    assert_eq!(df.height(), expected_buckets);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
    assert!(cols.iter().any(|c| c.as_str() == "AVG(v)"));
    assert!(cols.iter().any(|c| c.as_str() == "COUNT(v)"));
    tprintln!(
        "perf(aggregate one day per-second BY 1m): rows={}, cols={:?}, elapsed_ms={}",
        df.height(), cols, dur.as_millis()
    );
}


