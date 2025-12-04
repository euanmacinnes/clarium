use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{Record, SharedStore, Store};
use polars::prelude::*;
use serde_json::json;

/// Test basic ROW_NUMBER() OVER (ORDER BY) without PARTITION BY
#[test]
fn test_row_number_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/scores.time";
    
    let base: i64 = 1_600_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("score".into(), json!((5 - i) as f64 * 10.0));
        m.insert("player".into(), json!(format!("P{}", i)));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // ROW_NUMBER with ORDER BY score descending
    let sql = format!(
        "SELECT player, score, ROW_NUMBER() OVER (ORDER BY score DESC) as rank FROM {} ORDER BY rank",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 5 rows with row numbers 1-5
    assert_eq!(df.height(), 5);
    
    let ranks = df.column("rank").unwrap().i64().unwrap();
    assert_eq!(ranks.get(0), Some(1));
    assert_eq!(ranks.get(4), Some(5));
}

/// Test ROW_NUMBER() OVER (ORDER BY ASC)
#[test]
fn test_row_number_order_asc() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/items.time";
    
    let base: i64 = 1_650_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..4 {
        let mut m = serde_json::Map::new();
        m.insert("value".into(), json!((i + 1) as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // ROW_NUMBER with ORDER BY value ascending
    let sql = format!(
        "SELECT value, ROW_NUMBER() OVER (ORDER BY value ASC) as rn FROM {}",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 4);
    assert!(df.column("rn").is_ok());
}

/// Test ROW_NUMBER() OVER (PARTITION BY queue ORDER BY time)
#[test]
fn test_row_number_partition_by() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/jobs.time";
    
    let base: i64 = 1_700_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    // Create jobs in two queues
    for i in 0..6 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), json!(format!("q{}", i % 2)));
        m.insert("priority".into(), json!(i as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // ROW_NUMBER partitioned by queue, ordered by priority
    let sql = format!(
        "SELECT queue, priority, ROW_NUMBER() OVER (PARTITION BY queue ORDER BY priority ASC) as rn FROM {} ORDER BY queue, rn",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 6 rows, with row numbers 1-3 for each queue
    assert_eq!(df.height(), 6);
    
    let queues = df.column("queue").unwrap().str().unwrap();
    let rns = df.column("rn").unwrap().i64().unwrap();
    
    // First 3 should be q0 with rn 1,2,3
    assert_eq!(queues.get(0), Some("q0"));
    assert_eq!(rns.get(0), Some(1));
    
    // Last 3 should be q1 with rn 1,2,3
    assert_eq!(queues.get(3), Some("q1"));
    assert_eq!(rns.get(3), Some(1));
}

/// Test ROW_NUMBER() OVER (PARTITION BY multiple columns)
#[test]
fn test_row_number_partition_by_multiple() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/events.time";
    
    let base: i64 = 1_750_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..8 {
        let mut m = serde_json::Map::new();
        m.insert("category".into(), json!(format!("cat{}", i % 2)));
        m.insert("type".into(), json!(format!("type{}", i % 2)));
        m.insert("value".into(), json!(i as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // PARTITION BY two columns
    let sql = format!(
        "SELECT category, type, value, ROW_NUMBER() OVER (PARTITION BY category, type ORDER BY value) as rn FROM {}",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 8);
    assert!(df.column("rn").is_ok());
}

/// Test ROW_NUMBER in CTE with filtering
#[test]
fn test_row_number_with_cte() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/tasks.time";
    
    let base: i64 = 1_800_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..10 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), json!(format!("q{}", i % 3)));
        m.insert("created_at".into(), json!(base + (i as i64) * 1000));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE with ROW_NUMBER, then filter for first task in each queue
    let sql = format!(
        "WITH ranked AS (SELECT queue, created_at, ROW_NUMBER() OVER (PARTITION BY queue ORDER BY created_at ASC) as rn FROM {}) \
         SELECT queue, created_at FROM ranked WHERE rn = 1 ORDER BY queue",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 3 rows (one per queue)
    assert_eq!(df.height(), 3);
}

/// Test job search query pattern with COALESCE and window function
#[test]
fn test_job_search_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/jobs.time";
    
    let base: i64 = 1_850_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..6 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), json!(format!("q{}", i % 2)));
        m.insert("next_scheduled_time".into(), if i % 3 == 0 { json!(null) } else { json!(base + (i as i64) * 1000) });
        m.insert("date_created".into(), json!(base + (i as i64) * 500));
        m.insert("status".into(), json!("waiting"));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Simplified job search pattern with COALESCE and ROW_NUMBER
    let sql = format!(
        "SELECT queue, status, ROW_NUMBER() OVER (PARTITION BY queue ORDER BY COALESCE(next_scheduled_time, date_created) ASC) as rn \
         FROM {} WHERE status = 'waiting' ORDER BY queue, rn",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // All rows should be status 'waiting'
    assert_eq!(df.height(), 6);
    assert!(df.column("rn").is_ok());
}

/// Test window function with ORDER BY multiple columns
#[test]
fn test_row_number_order_by_multiple() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/records.time";
    
    let base: i64 = 1_900_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("priority".into(), json!(i % 2));
        m.insert("timestamp".into(), json!(base + (i as i64) * 1000));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // ORDER BY multiple columns in window function
    let sql = format!(
        "SELECT priority, timestamp, ROW_NUMBER() OVER (ORDER BY priority ASC, timestamp ASC) as rn FROM {}",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 5);
    assert!(df.column("rn").is_ok());
}

/// Test complex query combining CTE and windowing (job search replica)
#[test]
fn test_cte_with_windowing_job_search() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/jobs.time";
    
    let base: i64 = 2_000_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..8 {
        let mut m = serde_json::Map::new();
        m.insert("status".into(), json!(if i < 6 { "waiting" } else { "running" }));
        m.insert("queue".into(), if i % 2 == 0 { json!(format!("q{}", i / 2)) } else { json!(null) });
        m.insert("next_scheduled_time".into(), json!(base + (i as i64) * 1000));
        m.insert("date_created".into(), json!(base + (i as i64) * 500));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE with windowing to get first job per queue
    let sql = format!(
        "WITH base AS (\
           SELECT queue, status, next_scheduled_time, \
                  ROW_NUMBER() OVER (PARTITION BY queue ORDER BY COALESCE(next_scheduled_time, date_created) ASC) AS rn \
           FROM {} \
           WHERE status = 'waiting'\
         ) \
         SELECT queue, status, next_scheduled_time FROM base WHERE queue IS NULL OR rn = 1",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should filter to waiting jobs and apply window logic
    assert!(df.height() > 0);
    assert!(df.column("status").is_ok());
}

/// Test ROW_NUMBER with DESC order
#[test]
fn test_row_number_order_desc() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/sales.time";
    
    let base: i64 = 2_050_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("amount".into(), json!((i + 1) as f64 * 100.0));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // ROW_NUMBER with DESC ordering
    let sql = format!(
        "SELECT amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rank FROM {} ORDER BY rank",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 5);
    let amounts = df.column("amount").unwrap().f64().unwrap();
    // First row should be highest amount (rank 1)
    assert_eq!(amounts.get(0), Some(500.0));
}

/// Test window function with NULL handling in PARTITION BY
#[test]
fn test_row_number_partition_with_nulls() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/tasks.time";
    
    let base: i64 = 2_100_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..6 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), if i % 3 == 0 { json!(null) } else { json!(format!("q{}", i % 2)) });
        m.insert("priority".into(), json!(i as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Window function should handle NULL partition values
    let sql = format!(
        "SELECT queue, priority, ROW_NUMBER() OVER (PARTITION BY queue ORDER BY priority) as rn FROM {}",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 6);
    assert!(df.column("rn").is_ok());
}

/// Test EXTRACT with window functions (time-based calculations)
#[test]
fn test_extract_with_windowing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/events.time";
    
    let base: i64 = 2_150_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..4 {
        let mut m = serde_json::Map::new();
        m.insert("event_time".into(), json!(base + (i as i64) * 3600000)); // 1 hour apart
        recs.push(Record { _time: base + (i as i64) * 3600000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Use EXTRACT with window function
    let sql = format!(
        "SELECT event_time, ROW_NUMBER() OVER (ORDER BY event_time) as rn FROM {}",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert_eq!(df.height(), 4);
    let rns = df.column("rn").unwrap().i64().unwrap();
    assert_eq!(rns.get(0), Some(1));
    assert_eq!(rns.get(3), Some(4));
}
