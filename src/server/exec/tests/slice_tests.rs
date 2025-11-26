use super::super::run_slice;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::server::data_context::DataContext;
use serde_json::json;

#[test]
fn test_slice_using_where_and_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let tbl = "clarium/public/sl_where.time";
    let base: i64 = 1_800_000_000_000;
    // two intervals with a reason label
    let recs = vec![
        Record { _time: base, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(base)),
            ("_end_date".into(), json!(base + 10_000)),
            ("reason".into(), json!("power")),
        ])},
        Record { _time: base + 20_000, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(base + 20_000)),
            ("_end_date".into(), json!(base + 30_000)),
            ("reason".into(), json!("net")),
        ])},
    ];
    store.write_records(tbl, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // WHERE variant
    let q1 = format!("SLICE USING {} WHERE reason = 'power'", tbl);
    let p1 = match query::parse(&q1).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx1 = DataContext::with_defaults("clarium", "public");
    let df1 = run_slice(&shared, &p1, &ctx1).unwrap();
    assert_eq!(df1.height(), 1);
    let st1 = df1.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en1 = df1.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st1, base);
    assert_eq!(en1, base + 10_000);
    // FILTER alias
    let q2 = format!("SLICE USING {} FILTER reason = 'net'", tbl);
    let p2 = match query::parse(&q2).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx2 = DataContext::with_defaults("clarium", "public");
    let df2 = run_slice(&shared, &p2, &ctx2).unwrap();
    assert_eq!(df2.height(), 1);
    let st2 = df2.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en2 = df2.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st2, base + 20_000);
    assert_eq!(en2, base + 30_000);
}
 
#[test]
fn test_slice_union_intersect_where() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let a = "clarium/public/sl_a.time";
    let b = "clarium/public/sl_b.time";
    let base: i64 = 1_800_000_100_000;
    // a: [0, 30s]
    let recs_a = vec![
        Record { _time: base, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(base)),
            ("_end_date".into(), json!(base + 30_000)),
            ("kind".into(), json!("X")),
        ])},
    ];
    store.write_records(a, &recs_a).unwrap();
    // b: two intervals with reason labels that overlap a partially
    let recs_b = vec![
        Record { _time: base + 10_000, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(base + 10_000)),
            ("_end_date".into(), json!(base + 20_000)),
            ("reason".into(), json!("power")),
        ])},
        Record { _time: base + 20_000, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(base + 20_000)),
            ("_end_date".into(), json!(base + 40_000)),
            ("reason".into(), json!("net")),
        ])},
    ];
    store.write_records(b, &recs_b).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // INTERSECT only rows from b where reason='power' -> expect [10s,20s]
    let q = format!("SLICE USING {} INTERSECT {} WHERE reason = 'power'", a, b);
    let p = match query::parse(&q).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &p, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base + 10_000);
    assert_eq!(en, base + 20_000);
}



