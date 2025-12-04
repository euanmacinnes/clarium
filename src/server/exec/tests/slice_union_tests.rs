use super::super::run_slice;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::server::data_context::DataContext;
use serde_json::json;

#[test]
fn test_slice_union_coalesce_labels_lhs_sticky() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db1 = "clarium/public/union_a.time";
    let db2 = "clarium/public/union_b.time";
    let base: i64 = 1_900_000_000_000;
    // db1: [0,10] with labels M1/X
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 0));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record{ _time: base, sensors: m });
        store.write_records(db1, &recs).unwrap();
    }
    // db2: [5,15] with labels M2/Y
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 5_000));
        m.insert("_end_date".into(), json!(base + 15_000));
        recs.push(Record{ _time: base + 5_000, sensors: m });
        store.write_records(db2, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SLICE USING LABELS(machine, kind) {} LABEL('M1','X') UNION {} LABEL('M2','Y')", db1, db2);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    // Expect single coalesced interval [0,15]
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base + 0);
    assert_eq!(en, base + 15_000);
    // Labels should come from LHS (db1)
    let mach = df.column("machine").unwrap().str().unwrap();
    let kind = df.column("kind").unwrap().str().unwrap();
    assert_eq!(mach.get(0).unwrap(), "M1");
    assert_eq!(kind.get(0).unwrap(), "X");
}

#[test]
fn test_slice_union_coalesce_labels_lhs_null_filled_by_rhs() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db1 = "clarium/public/union_c.time";
    let db2 = "clarium/public/union_d.time";
    let base: i64 = 1_900_100_000_000;
    // db1: [0,10] with no labels (will set NULL, '')
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 0));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record{ _time: base, sensors: m });
        store.write_records(db1, &recs).unwrap();
    }
    // db2: [5,15] with labels M2/Y
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 5_000));
        m.insert("_end_date".into(), json!(base + 15_000));
        recs.push(Record{ _time: base + 5_000, sensors: m });
        store.write_records(db2, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    // LHS provides NULL and empty string; RHS provides non-empty -> should fill from RHS
    let qtext = format!("SLICE USING LABELS(machine, kind) {} LABEL(NULL, '') UNION {} LABEL('M2','Y')", db1, db2);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base + 0);
    assert_eq!(en, base + 15_000);
    let mach = df.column("machine").unwrap().str().unwrap();
    let kind = df.column("kind").unwrap().str().unwrap();
    assert_eq!(mach.get(0).unwrap(), "M2");
    assert_eq!(kind.get(0).unwrap(), "Y");
}




