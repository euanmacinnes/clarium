use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_rolling_by_avg_simple() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_roll.time";
    let base: i64 = 1_700_100_000_000;
    let vals = [1.0, 2.0, 3.0, 4.0, 5.0];
    let mut recs: Vec<Record> = Vec::new();
    for (i, v) in vals.iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(*v));
        recs.push(Record { _time: base + (i as i64)*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT AVG(v) FROM {} ROLLING BY 3s", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 5);
    let avg = df.column("AVG(v)").unwrap().f64().unwrap();
    let expected = [1.0, (1.0+2.0)/2.0, (1.0+2.0+3.0)/3.0, (2.0+3.0+4.0)/3.0, (3.0+4.0+5.0)/3.0];
    for i in 0..5 {
        let got = avg.get(i).unwrap();
        assert!((got - expected[i]).abs() < 1e-9, "idx {} expected {} got {}", i, expected[i], got);
    }
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "_time"));
}


