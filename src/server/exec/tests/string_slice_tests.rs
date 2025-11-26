use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;
use polars::prelude::{AnyValue, DataFrame};

fn sval(df: &DataFrame, col: &str, idx: usize) -> Option<String> {
    match df.column(col).ok()?.get(idx) {
        Ok(AnyValue::String(s)) => Some(s.to_string()),
        Ok(AnyValue::StringOwned(s)) => Some(s.to_string()),
        Ok(AnyValue::Null) => None,
        _ => None,
    }
}

#[test]
fn string_slice_integer_indices() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/strings1.time";
    let base: i64 = 1_900_000_000_000;
    let recs = vec![
        { let mut m = serde_json::Map::new(); m.insert("s".into(), json!("abcdefg")); Record{ _time: base, sensors: m }},
        { let mut m = serde_json::Map::new(); m.insert("s".into(), json!("hijk")); Record{ _time: base+1000, sensors: m }},
    ];
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // start:stop
    let qtxt = format!("SELECT s[1:4] AS x FROM {}", db);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(sval(&df, "x", 0).as_deref(), Some("bcd"));
    assert_eq!(sval(&df, "x", 1).as_deref(), Some("ijk"));
    // [:stop]
    let qtxt2 = format!("SELECT s[:3] AS y FROM {}", db);
    let q2 = match query::parse(&qtxt2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(sval(&df2, "y", 0).as_deref(), Some("abc"));
    assert_eq!(sval(&df2, "y", 1).as_deref(), Some("hij"));
    // [start:]
    let qtxt3 = format!("SELECT s[2:] AS z FROM {} ", db);
    let q3 = match query::parse(&qtxt3).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    assert_eq!(sval(&df3, "z", 0).as_deref(), Some("cdefg"));
    assert_eq!(sval(&df3, "z", 1).as_deref(), Some("jk"));
}

#[test]
fn string_slice_with_step_and_patterns() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/strings2.time";
    let base: i64 = 1_900_000_100_000;
    let recs = vec![
        { let mut m = serde_json::Map::new(); m.insert("s".into(), json!("foo_bar_baz")); Record{ _time: base, sensors: m }},
    ];
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // step
    let qtxt = format!("SELECT s[0:10:2] AS e FROM {}", db);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(sval(&df, "e", 0).as_deref(), Some("fobrb"));
/*    // pattern include (from 'bar')
    let qtxt2 = format!("SELECT s['bar':] AS p FROM {}", db);
    let q2 = match query::parse(&qtxt2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(sval(&df2, "p", 0).as_deref(), Some("bar_baz"));
    // pattern exclude (after 'bar')
    let qtxt3 = format!("SELECT s[-'bar':] AS q FROM {}", db);
    let q3 = match query::parse(&qtxt3).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    assert_eq!(sval(&df3, "q", 0).as_deref(), Some("_baz"));*/
}

/*#[test]
fn string_slice_stop_literal_inclusive_and_exclusive() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/strings3.time";
    let base: i64 = 1_900_000_200_000;
    let recs = vec![
        { let mut m = serde_json::Map::new(); m.insert("s".into(), json!("foo_bar_baz")); Record{ _time: base, sensors: m }},
        { let mut m = serde_json::Map::new(); m.insert("s".into(), json!("hello world")); Record{ _time: base+1000, sensors: m }},
    ];
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Inclusive stop: include the matched stop literal in the slice
    let q_incl = format!("SELECT s[:'bar'] AS incl FROM {} ORDER BY _time", db);
    let q = match query::parse(&q_incl).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(sval(&df, "incl", 0).as_deref(), Some("foo_bar"));
    // If pattern not found in stop, default to end of string
    assert_eq!(sval(&df, "incl", 1).as_deref(), Some("hello world"));

    // Exclusive stop: exclude the matched stop literal (slice ends before it)
    let q_excl = format!("SELECT s[:-'bar'] AS excl FROM {} ORDER BY _time", db);
    let q2 = match query::parse(&q_excl).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(sval(&df2, "excl", 0).as_deref(), Some("foo_"));
    // Not found pattern still returns full string (stop at len)
    assert_eq!(sval(&df2, "excl", 1).as_deref(), Some("hello world"));
}*/



