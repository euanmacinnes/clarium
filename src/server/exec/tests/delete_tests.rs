use super::super::execute_query;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[tokio::test]
async fn test_delete_rows_between() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "timeline/public/db_del.time";
    let base: i64 = 1_700_000_000_000;
    let mut recs = Vec::new();
    for i in 0..5 { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(i as i64)); recs.push(Record{ _time: base + i*1000, sensors: m}); }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let start = base + 2*1000;
    let end = base + 3*1000;
    let q = format!("DELETE FROM {} WHERE _time BETWEEN {} AND {}", db, start, end);
    let _ = execute_query(&shared, &q).await.unwrap();
    let df = { let g = shared.0.lock(); g.read_df(db).unwrap() };
    assert_eq!(df.height(), 3);
    let times = df.column("_time").unwrap().i64().unwrap().into_no_null_iter().collect::<Vec<i64>>();
    assert_eq!(times, vec![base, base+1000, base+4000]);
}

#[tokio::test]
async fn test_delete_columns_where_and_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_cols.time";
    let base: i64 = 1_700_000_100_000;
    let mut recs = Vec::new();
    for i in 0..4 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(i as f64));
        m.insert("label".into(), json!(format!("L{}", i)));
        recs.push(Record{ _time: base + i*1000, sensors: m});
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let start = base + 1000;
    let end = base + 2000;
    let q1 = format!("DELETE COLUMNS (label) FROM {} WHERE _time BETWEEN {} AND {}", db, start, end);
    let _ = execute_query(&shared, &q1).await.unwrap();
    let df1 = { let g = shared.0.lock(); g.read_df(db).unwrap() };
    assert!(df1.get_column_names().iter().any(|c| c.as_str()=="label"));
    let lab = df1.column("label").unwrap().str().unwrap();
    assert_eq!(lab.get(0), Some("L0".into()));
    assert!(lab.get(1).is_none());
    assert!(lab.get(2).is_none());
    assert_eq!(lab.get(3), Some("L3".into()));
    let q2 = format!("DELETE COLUMNS (label) FROM {}", db);
    let _ = execute_query(&shared, &q2).await.unwrap();
    let df2 = { let g = shared.0.lock(); g.read_df(db).unwrap() };
    assert!(!df2.get_column_names().iter().any(|c| c.as_str()=="label"));
}


