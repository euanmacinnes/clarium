use super::*;
use serde_json::json;

#[test]
fn test_write_and_read_roundtrip() {
    // Use a temp directory under target to avoid clutter; Windows-safe
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let mut sensors1 = serde_json::Map::new();
    sensors1.insert("v".into(), json!(1.0));
    sensors1.insert("label".into(), json!("a"));
    let mut sensors2 = serde_json::Map::new();
    sensors2.insert("v".into(), json!(2)); // int, should merge to float64 for v due to 1.0
    let recs = vec![
        Record { _time: 1_000, sensors: sensors1 },
        Record { _time: 2_000, sensors: sensors2 },
    ];
    store.write_records("db1", &recs).unwrap();
    // Read back
    let df = store.read_df("db1").unwrap();
    assert_eq!(df.height(), 2);
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "_time"));
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "v"));
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "label"));
}

#[test]
fn test_regular_table_partitioning_writes_multiple_files() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let table = "mydb/public/rtab"; // regular table (no .time)
    store.create_table(table).unwrap();
    // Set partitions metadata: partition by region
    store.set_table_metadata(table, None, Some(vec!["region".to_string()])).unwrap();
    // Write rows across two regions
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..10 {
        let mut m = serde_json::Map::new();
        m.insert("region".into(), json!(if i % 2 == 0 { "north" } else { "south" }));
        m.insert("v".into(), json!(i as i64));
        recs.push(Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
    }
    store.write_records(table, &recs).unwrap();
    // Count files
    let dir = store.db_dir(table);
    let mut count = 0usize;
    for e in std::fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
            if name.starts_with("data-") && name.ends_with(".parquet") { count += 1; }
        }
    }
    assert!(count >= 2, "expected >=2 parquet files, found {}", count);
    // Read back and ensure all rows present
    let df = store.read_df(table).unwrap();
    assert_eq!(df.height(), recs.len());
}

#[test]
fn test_out_of_order_insert_is_sorted_on_disk_and_read_sorted() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // write out-of-order
    let mut s1 = serde_json::Map::new(); s1.insert("a".into(), json!(1));
    let mut s2 = serde_json::Map::new(); s2.insert("a".into(), json!(2));
    let mut s3 = serde_json::Map::new(); s3.insert("a".into(), json!(3));
    let recs = vec![
        Record { _time: 2000, sensors: s2 },
        Record { _time: 1000, sensors: s1 },
        Record { _time: 3000, sensors: s3 },
    ];
    store.write_records("db", &recs).unwrap();
    let df = store.read_df("db").unwrap();
    let times: Vec<i64> = df.column("_time").unwrap().i64().unwrap().into_iter().map(|o| o.unwrap()).collect();
    assert_eq!(times, vec![1000, 2000, 3000]);
}
