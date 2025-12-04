use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_group_by_aggregates_and_time_bounds() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_group.time";
    let base: i64 = 1_700_001_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in [0,2,4] { let mut m = serde_json::Map::new(); m.insert("v".into(), json!((i/2 + 1) as f64)); m.insert("device".into(), json!("A")); recs.push(Record{ _time: base + i*1000, sensors: m}); }
    for i in [1,3] { let mut m = serde_json::Map::new(); let val = if i==1 { 10.0 } else { 20.0 }; m.insert("v".into(), json!(val)); m.insert("device".into(), json!("B")); recs.push(Record{ _time: base + i*1000, sensors: m}); }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT AVG(v), COUNT(v) FROM {} GROUP BY device", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 2);
    let names = df.get_column_names();
    assert!(names.iter().any(|c| c.as_str()=="device"));
    assert!(names.iter().any(|c| c.as_str()=="AVG(v)"));
    assert!(names.iter().any(|c| c.as_str()=="COUNT(v)"));
    assert!(names.iter().any(|c| c.as_str()=="_start_time"));
    assert!(names.iter().any(|c| c.as_str()=="_end_time"));
    let device = df.column("device").unwrap().str().unwrap();
    let avg = df.column("AVG(v)").unwrap().f64().unwrap();
    let cnt = df.column("COUNT(v)").unwrap().i64().unwrap();
    let st = df.column("_start_time").unwrap().i64().unwrap();
    let en = df.column("_end_time").unwrap().i64().unwrap();
    for i in 0..df.height() {
        let d = device.get(i).unwrap().to_string();
        if d == "A" {
            assert_eq!(cnt.get(i).unwrap(), 3);
            assert!((avg.get(i).unwrap() - 2.0).abs() < 1e-9);
            assert_eq!(st.get(i).unwrap(), base + 0);
            assert_eq!(en.get(i).unwrap(), base + 4*1000);
        } else if d == "B" {
            assert_eq!(cnt.get(i).unwrap(), 2);
            assert!((avg.get(i).unwrap() - 15.0).abs() < 1e-9);
            assert_eq!(st.get(i).unwrap(), base + 1*1000);
            assert_eq!(en.get(i).unwrap(), base + 3*1000);
        } else { panic!("unexpected device"); }
    }
}

#[test]
fn test_group_by_multi_cols_with_select_projection() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_group_multi.time";
    let base: i64 = 1_700_003_000_000;
    let rows = vec![
        (0, "A", "R1", 1.0),
        (1, "A", "R1", 2.0),
        (2, "B", "R2", 10.0),
        (3, "B", "R2", 20.0),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, dev, reg, v) in rows {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        m.insert("device".into(), json!(dev));
        m.insert("region".into(), json!(reg));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT device, region, SUM(v) FROM {} GROUP BY device, region", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 2);
    let names = df.get_column_names();
    assert!(names.iter().any(|c| c.as_str()=="device"));
    assert!(names.iter().any(|c| c.as_str()=="region"));
    assert!(names.iter().any(|c| c.as_str()=="SUM(v)"));
    assert!(names.iter().any(|c| c.as_str()=="_start_time"));
    assert!(names.iter().any(|c| c.as_str()=="_end_time"));
    let device = df.column("device").unwrap().str().unwrap();
    let region = df.column("region").unwrap().str().unwrap();
    let sumv = df.column("SUM(v)").unwrap().f64().unwrap();
    for i in 0..df.height() {
        let d = device.get(i).unwrap().to_string();
        let r = region.get(i).unwrap().to_string();
        if d == "A" && r == "R1" { assert!((sumv.get(i).unwrap() - 3.0).abs() < 1e-9); }
        else if d == "B" && r == "R2" { assert!((sumv.get(i).unwrap() - 30.0).abs() < 1e-9); }
        else { panic!("unexpected group"); }
    }
}

#[test]
fn test_group_by_rejects_by_combo() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_group2.time";
    let recs = vec![Record{ _time: 1_700_000_000_000, sensors: serde_json::Map::from_iter(vec![("v".into(), json!(1.0)), ("device".into(), json!("A"))]) }];
    store.write_records(db, &recs).unwrap();
    let err = query::parse(&format!("SELECT AVG(v) FROM {} BY 1m GROUP BY device", db)).err();
    assert!(err.is_some());
}


