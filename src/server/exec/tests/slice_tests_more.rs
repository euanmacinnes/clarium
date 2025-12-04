use super::super::run_slice;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::server::data_context::DataContext;
use serde_json::json;

#[test]
fn test_slice_labels_using_and_assignments() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "clarium/public/slices_labeled.time";
    let base: i64 = 1_810_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    // two rows with start/end and a 'kind' column
    for (s,e, kind) in [(0,10,"X"),(15,20,"Y")].iter() {
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + s*1000));
        m.insert("_end_date".into(), json!(base + e*1000));
        m.insert("kind".into(), json!(kind.to_string()));
        recs.push(Record{ _time: base + s*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Declare labels and assign constant + column using LABEL(...)
    // Using LABELS(machine, kind), we pass values positionally via LABEL('M1', kind)
    let qtext = format!("SLICE USING LABELS(machine, kind) {} LABEL('M1', kind)", db);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 2);
    let names = df.get_column_names();
    assert!(names.iter().any(|c| c.as_str()=="_start_date"));
    assert!(names.iter().any(|c| c.as_str()=="_end_date"));
    assert!(names.iter().any(|c| c.as_str()=="machine"));
    assert!(names.iter().any(|c| c.as_str()=="kind"));
    let mach = df.column("machine").unwrap().str().unwrap();
    let kind = df.column("kind").unwrap().str().unwrap();
    assert_eq!(mach.get(0).unwrap(), "M1");
    assert_eq!(mach.get(1).unwrap(), "M1");
    assert_eq!(kind.get(0).unwrap(), "X");
    assert_eq!(kind.get(1).unwrap(), "Y");
}

#[test]
fn test_slice_using_defaults_and_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "clarium/public/slices.time";
    // Create rows with _start_date/_end_date
    let base: i64 = 1_800_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    // intervals: [0,10], [5,15] -> merged [0,15]
    for (s,e) in [(0,10),(5,15)].iter() {
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + s*1000));
        m.insert("_end_date".into(), json!(base + e*1000));
        recs.push(Record{ _time: base + s*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SLICE USING {}", db);
    // parse and run via exec path
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base);
    assert_eq!(en, base + 15*1000);
}

#[test]
fn test_slice_intersect_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db1 = "clarium/public/a.time";
    let db2 = "clarium/public/b.time";
    let base: i64 = 1_800_000_100_000;
    // a: [0,20]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 0));
        m.insert("_end_date".into(), json!(base + 20_000));
        recs.push(Record{ _time: base, sensors: m });
        store.write_records(db1, &recs).unwrap();
    }
    // b: [10,30]
    {
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(base + 10_000));
        m.insert("_end_date".into(), json!(base + 30_000));
        recs.push(Record{ _time: base + 10_000, sensors: m });
        store.write_records(db2, &recs).unwrap();
    }
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SLICE USING {} INTERSECT {}", db1, db2);
    let plan = match query::parse(&qtext).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("clarium", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let st = df.column("_start_date").unwrap().i64().unwrap().get(0).unwrap();
    let en = df.column("_end_date").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!(st, base + 10_000);
    assert_eq!(en, base + 20_000);
}




