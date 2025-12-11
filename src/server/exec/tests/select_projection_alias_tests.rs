use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{SharedStore, Store, Record};
use polars::prelude::*;

fn write_demo_time(store: &Store, path: &str, n: usize) {
    let base: i64 = 1_700_000_000_000; // ms
    let mut recs: Vec<Record> = Vec::with_capacity(n);
    for i in 0..n {
        let mut m = serde_json::Map::new();
        m.insert("value".into(), serde_json::json!(i as f64));
        recs.push(Record { _time: base + i as i64, sensors: m });
    }
    store.write_records(path, &recs).unwrap();
}

fn write_demo_negative_events(shared: &SharedStore, path: &str) {
    // Ensure table exists, then overwrite with a small DataFrame with expected columns
    {
        let g = shared.0.lock();
        // Create as regular table if not exists
        let _ = g.create_table(path);
        let s1 = Series::new("_start_date".into(), vec![1_i64, 3_i64]);
        let s2 = Series::new("_end_date".into(), vec![2_i64, 4_i64]);
        let s3 = Series::new("label".into(), vec!["W1", "W3"]);
        let df = DataFrame::new(vec![s1.into(), s2.into(), s3.into()]).unwrap();
        g.rewrite_table_df(path, df).unwrap();
    }
}

#[test]
fn test_time_table_star_with_and_without_schema_qualifier_same_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Prepare demo time table under default db/schema
    write_demo_time(&store, "clarium/public/test.time", 5);

    // Query 1: default schema (public) implied
    let q1 = match query::parse("SELECT dt.* FROM \"test.time\" AS dt LIMIT 100").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();

    // Query 2: explicit schema qualifier public.
    let q2 = match query::parse("SELECT dt.* FROM public.\"test.time\" AS dt LIMIT 100").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();

    // Both should project the same columns in the same order
    let c1 = df1.get_column_names();
    let c2 = df2.get_column_names();
    assert_eq!(c1, c2, "Column sets/order should match: {:?} vs {:?}", c1, c2);

    // And specifically demo.time should expose [_time, value]
    assert!(c1.len() >= 2, "expected at least 2 columns for time table");
    assert_eq!(c1[0].as_str(), "_time");
    // The value column name can be either `value` or derived, but for demo it's `value`
    assert_eq!(c1[1].as_str(), "dt.value");
}

#[test]
fn test_regular_table_alias_star_projects_only_table_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Prepare regular events table at default path
    write_demo_negative_events(&shared, "clarium/public/test_negative_events");

    let q = match query::parse("SELECT dne.* FROM test_negative_events AS dne").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();

    let cols = df.get_column_names();
    let expected = vec!["dne._start_date", "dne._end_date", "dne.label"];
    assert_eq!(cols, expected, "unexpected columns for test_negative_events: {:?}", cols);
}
