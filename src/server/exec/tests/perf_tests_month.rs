use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;
use std::time::Instant;
use crate::tprintln;

#[test]
fn test_perf_retrieval_one_month_per_second() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Use fully-qualified canonical path (db/schema/table.time)
    let db = "db_month.time";
    let start_ms: i64 = 1_700_000_000_000;
    let days: i64 = 30;
    let seconds_per_day: i64 = 86_400;
    let total_seconds = days * seconds_per_day;
    for d in 0..days {
        let day_start = start_ms + d * seconds_per_day * 1000;
        let mut records: Vec<Record> = Vec::with_capacity(seconds_per_day as usize);
        for i in 0..seconds_per_day {
            let mut sensors = serde_json::Map::new();
            sensors.insert("v".into(), json!(((d * seconds_per_day + i) % 100) as f64));
            records.push(Record { _time: day_start + i * 1000, sensors });
        }
        store.write_records(db, &records).unwrap();
    }
    tprintln!("demo(created per second data)");
    let shared = SharedStore::new(tmp.path()).unwrap();
    let end_ms = start_ms + (total_seconds - 1) * 1000;
    let qtext = format!(
        "SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {}",
        db, start_ms, end_ms
    );
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let t0 = Instant::now();
    let df = run_select(&shared, &q).unwrap();
    let dur = t0.elapsed();
    assert_eq!(df.height(), (total_seconds as usize));
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
    assert!(cols.iter().any(|c| c.as_str() == "v"));
    tprintln!(
        "perf(retrieval one month per-second): rows={}, cols={:?}, elapsed_ms={}",
        df.height(), cols, dur.as_millis()
    );
}

#[test]
fn test_perf_aggregate_one_month_per_second() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_month.time";
    let start_ms: i64 = 1_700_000_000_000;
    let days: i64 = 30;
    let seconds_per_day: i64 = 86_400;
    for d in 0..days {
        let day_start = start_ms + d * seconds_per_day * 1000;
        let mut records: Vec<Record> = Vec::with_capacity(seconds_per_day as usize);
        for i in 0..seconds_per_day {
            let mut sensors = serde_json::Map::new();
            sensors.insert("v".into(), json!(((d * seconds_per_day + i) % 100) as f64));
            records.push(Record { _time: day_start + i * 1000, sensors });
        }
        store.write_records(db, &records).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let total_seconds = days * seconds_per_day;
    let end_ms = start_ms + (total_seconds - 1) * 1000;
    let qtext = format!(
        "SELECT AVG(v), COUNT(v) FROM {} BY 1h WHERE _time BETWEEN {} AND {}",
        db, start_ms, end_ms
    );
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let t0 = Instant::now();
    let df = run_select(&shared, &q).unwrap();
    let dur = t0.elapsed();
    let start_bucket = (start_ms / 3_600_000) * 3_600_000;
    let end_bucket = (end_ms / 3_600_000) * 3_600_000;
    let expected_buckets = ((end_bucket - start_bucket) / 3_600_000 + 1) as usize;
    let cols = df.get_column_names();
    assert_eq!(df.height(), expected_buckets);
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
    assert!(cols.iter().any(|c| c.as_str() == "AVG(v)"));
    assert!(cols.iter().any(|c| c.as_str() == "COUNT(v)"));
    tprintln!(
        "perf(aggregate one month per-second BY 1h): rows={}, cols={:?}, elapsed_ms={}",
        df.height(), cols, dur.as_millis()
    );
}


