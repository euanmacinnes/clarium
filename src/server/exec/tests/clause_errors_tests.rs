use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_time_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    // two seconds of data
    let base: i64 = 1_700_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..4i64 {
        let mut m = serde_json::Map::new();
        m.insert("a".into(), json!(i as f64));
        m.insert("b".into(), json!((i * 2) as f64));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn test_by_column_missing_and_udf_not_found() {
    // Ensure global UDF registry is initialized for tests that reference UDFs
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_by.time";
    let shared = seed_time_table(&tmp, db);

    // BY with missing column in aggregate
    let q1 = format!("SELECT AVG(nope) FROM {} BY 1s", db);
    let q1 = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e1 = run_select(&shared, &q1).err();
    assert!(e1.is_some());
    let m1 = format!("{}", e1.unwrap());
    assert!(m1.contains("Column not found in BY:"), "unexpected: {}", m1);

    // BY with unknown UDF
    let q2 = format!("SELECT nosuch(a) FROM {} BY 1s", db);
    let q2 = match query::parse(&q2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e2 = run_select(&shared, &q2).err();
    assert!(e2.is_some());
    let m2 = format!("{}", e2.unwrap());
    assert!(m2.contains("UDF 'nosuch' not found in BY clause"), "unexpected: {}", m2);
}

#[test]
fn test_group_by_nonagg_violation_and_udf_missing() {
    // Ensure global UDF registry is initialized for tests that reference UDFs
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_group.time";
    let shared = seed_time_table(&tmp, db);

    // Non-agg selection not in GROUP BY
    let q1 = format!("SELECT a, AVG(b) FROM {} GROUP BY b", db);
    let q1 = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e1 = run_select(&shared, &q1).err();
    assert!(e1.is_some());
    let m1 = format!("{}", e1.unwrap());
    assert!(m1.contains("must appear in GROUP BY or be aggregated"), "unexpected: {}", m1);

    // Unknown UDF in GROUP BY aggregate expression
    let q2 = format!("SELECT nosuch(a) FROM {} GROUP BY a", db);
    let q2 = match query::parse(&q2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e2 = run_select(&shared, &q2).err();
    assert!(e2.is_some());
    let m2 = format!("{}", e2.unwrap());
    assert!(m2.contains("UDF 'nosuch' not found in GROUP BY clause"), "unexpected: {}", m2);
}

#[test]
fn test_rolling_missing_column_and_expr_rejected() {
    // Ensure global UDF registry is initialized for tests that reference UDFs
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_roll2.time";
    let shared = seed_time_table(&tmp, db);

    // Missing column in ROLLING
    let q1 = format!("SELECT AVG(nope) FROM {} ROLLING BY 2s", db);
    let q1 = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e1 = run_select(&shared, &q1).err();
    assert!(e1.is_some());
    let m1 = format!("{}", e1.unwrap());
    assert!(m1.contains("\"nope\" not found"), "unexpected: {}", m1);

    // Expression in AVG rejected
    let q2 = format!("SELECT AVG(a+1) FROM {} ROLLING BY 2s", db);
    let q2 = match query::parse(&q2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e2 = run_select(&shared, &q2).err();
    assert!(e2.is_some());
}

#[test]
fn test_select_expr_missing_column_and_udf_not_found() {
    // Ensure global UDF registry is initialized for tests that reference UDFs
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_sel";
    // regular table ok
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..2 {
        let mut m = serde_json::Map::new();
        m.insert("x".into(), json!(i as i64));
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Missing column in SELECT expression
    let q1 = format!("SELECT x + nope FROM {}", db);
    let q1 = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e1 = run_select(&shared, &q1).err();
    assert!(e1.is_some());
    let m1 = format!("{}", e1.unwrap());
    assert!(m1.contains("Column not found in SELECT:"), "unexpected: {}", m1);

    // UDF not found in SELECT expression
    let q2 = format!("SELECT nosuch(x) FROM {}", db);
    let q2 = match query::parse(&q2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e2 = run_select(&shared, &q2).err();
    assert!(e2.is_some());
    let m2 = format!("{}", e2.unwrap());
    assert!(m2.contains("UDF 'nosuch' not found in registry"), "unexpected: {}", m2);
}

#[test]
fn test_order_by_missing_column_message() {
    // Ensure global UDF registry is initialized for tests that reference UDFs
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_order";
    let shared = seed_time_table(&tmp, db);

    // ORDER BY references a missing column
    let q = format!("SELECT a FROM {} ORDER BY nope", db);
    let q = match query::parse(&q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let e = run_select(&shared, &q).err();
    assert!(e.is_some());
    let msg = format!("{}", e.unwrap());
    assert!(msg.contains("Column not found in ORDER BY:"), "unexpected: {}", msg);
}



