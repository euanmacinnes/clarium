use super::super::run_slice;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::server::data_context::DataContext;
use serde_json::json;

#[test]
fn union_table_and_manual_unlabeled() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/blend_union_unlabeled.time";
    let base: i64 = 1_950_000_000_000;
    // Table interval: [base, base+10_000]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record { _time: base, sensors: m });
        store.write_records(db, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let m_start = base + 5_000;
    let m_end = base + 15_000;
    let qtext = format!("SLICE USING {} UNION ({}, {}, 'x')", db, m_start, m_end);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base);
    assert_eq!(en, m_end);
}

#[test]
fn intersect_table_and_manual_unlabeled() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/blend_intersect_unlabeled.time";
    let base: i64 = 1_950_100_000_000;
    // Table interval: [base, base+10_000]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record { _time: base, sensors: m });
        store.write_records(db, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let m_start = base + 5_000;
    let m_end = base + 15_000;
    let qtext = format!("SLICE USING {} INTERSECT ({}, {})", db, m_start, m_end);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, m_start);
    assert_eq!(en, base + 10_000);
}

#[test]
fn union_table_and_manual_with_labels_lhs_sticky() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/blend_union_labeled.time";
    let base: i64 = 1_950_200_000_000;
    // Table interval: [base, base+10_000]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record { _time: base, sensors: m });
        store.write_records(db, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let m_start = base;
    let m_end = base + 15_000;
    // LHS table provides labels T1/X; RHS manual provides M2/Y. UNION should coalesce to [base, base+15_000] with LHS labels.
    let qtext = format!(
        "SLICE USING LABELS(machine, kind) {} LABEL('T1','X') UNION ({}, {}, machine:='M2', kind:='Y')",
        db, m_start, m_end
    );
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base);
    assert_eq!(en, m_end);
    let mach = df.column("machine").unwrap().str().unwrap();
    let kind = df.column("kind").unwrap().str().unwrap();
    assert_eq!(mach.get(0).unwrap(), "T1");
    assert_eq!(kind.get(0).unwrap(), "X");
}

#[test]
fn intersect_table_and_manual_with_labels_rhs_overrides_empty_lhs() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/blend_intersect_labeled.time";
    let base: i64 = 1_950_300_000_000;
    // Table interval: [base, base+10_000]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base));
        m.insert("_end_date".into(), json!(base + 10_000));
        recs.push(Record { _time: base, sensors: m });
        store.write_records(db, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let m_start = base + 5_000;
    let m_end = base + 15_000;
    // LHS table gives NULL/''; RHS manual has non-empty -> intersect should take RHS values per precedence when overlapping
    let qtext = format!(
        "SLICE USING LABELS(machine, kind) {} LABEL(NULL, '') INTERSECT ({}, {}, machine:='M2', kind:='Y')",
        db, m_start, m_end
    );
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, m_start);
    assert_eq!(en, base + 10_000);
    let mach = df.column("machine").unwrap().str().unwrap();
    let kind = df.column("kind").unwrap().str().unwrap();
    assert_eq!(mach.get(0).unwrap(), "M2");
    assert_eq!(kind.get(0).unwrap(), "Y");
}




