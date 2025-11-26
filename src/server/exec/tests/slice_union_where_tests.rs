use super::super::run_slice;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::server::data_context::DataContext;
use serde_json::json;

#[test]
fn union_rhs_where_filters_before_union() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let a = "clarium/public/su_a.time";
    let b = "clarium/public/su_b.time";
    let base: i64 = 1_860_000_000_000;
    // a: [0,10s]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record { _time: base, sensors: m });
        store.write_records(a, &recs).unwrap();
    }
    // b: [5,15] reason='power'; [20,30] reason='net'
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m1 = serde_json::Map::new();
        m1.insert("_start_date".into(), json!(base + 5_000));
        m1.insert("_end_date".into(), json!(base + 15_000));
        m1.insert("reason".into(), json!("power"));
        recs.push(Record { _time: base + 5_000, sensors: m1 });
        let mut m2 = serde_json::Map::new();
        m2.insert("_start_date".into(), json!(base + 20_000));
        m2.insert("_end_date".into(), json!(base + 30_000));
        m2.insert("reason".into(), json!("net"));
        recs.push(Record { _time: base + 20_000, sensors: m2 });
        store.write_records(b, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    // UNION rows from b where reason='power' -> union [0,10] with [5,15] => [0,15]
    let q = format!("SLICE USING {} UNION {} WHERE reason = 'power'", a, b);
    let p = match query::parse(&q).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &p, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base);
    assert_eq!(en, base + 15_000);
}




