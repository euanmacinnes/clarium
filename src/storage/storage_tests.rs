use super::*;
use serde_json::json;
use polars::prelude::*;

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
    store.write_records("db1.time", &recs).unwrap();
    // Read back
    let df = store.read_df("db1.time").unwrap();
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
    store.write_records("db.time", &recs).unwrap();
    let df = store.read_df("db.time").unwrap();
    let times: Vec<i64> = df.column("_time").unwrap().i64().unwrap().into_iter().map(|o| o.unwrap()).collect();
    assert_eq!(times, vec![1000, 2000, 3000]);
}

#[test]
fn test_all_column_types_roundtrip_regular_table() {
    // Create a regular table with explicit schema covering all supported scalar types
    // and the supported array type (vector = List(Float64)). Then write rows and
    // verify round-trip values and dtypes.
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();

    let table = "clarium/public/alltypes"; // regular table
    store.create_table(table).unwrap();

    // Declare schema explicitly and lock it via schema_add so writer uses these dtypes
    store
        .schema_add(
            table,
            &[
                ("a_f64".to_string(), DataType::Float64),
                ("a_i64".to_string(), DataType::Int64),
                ("a_str".to_string(), DataType::String),
                ("v_f64".to_string(), DataType::List(Box::new(DataType::Float64))), // vector
            ],
        )
        .unwrap();

    // Two rows with mixed literal types to exercise parsing
    let mut r1 = serde_json::Map::new();
    r1.insert("a_f64".into(), json!(1.5));
    r1.insert("a_i64".into(), json!(42));
    r1.insert("a_str".into(), json!("hello"));
    r1.insert("v_f64".into(), json!([1.0, 2.5, 3.0]));

    let mut r2 = serde_json::Map::new();
    r2.insert("a_f64".into(), json!("3.14")); // string that parses to f64
    r2.insert("a_i64".into(), json!("7"));    // string that parses to i64
    r2.insert("a_str".into(), json!(9001));     // number coerced to string in string column
    r2.insert("v_f64".into(), json!("1, 2, 3")); // string-encoded vector

    let recs = vec![
        Record { _time: 0, sensors: r1 },
        Record { _time: 0, sensors: r2 },
    ];

    store.write_records(table, &recs).unwrap();

    let df = store.read_df(table).unwrap();
    assert_eq!(df.height(), 2);

    // Check presence and dtypes
    let c_f64 = df.column("a_f64").unwrap();
    let c_i64 = df.column("a_i64").unwrap();
    let c_str = df.column("a_str").unwrap();
    let c_vec = df.column("v_f64").unwrap();

    assert_eq!(c_f64.dtype(), &DataType::Float64);
    assert_eq!(c_i64.dtype(), &DataType::Int64);
    assert_eq!(c_str.dtype(), &DataType::String);
    assert_eq!(c_vec.dtype(), &DataType::List(Box::new(DataType::Float64)));

    // Row 0 assertions
    let v0_f64 = c_f64.get(0).ok().and_then(|v| v.try_extract::<f64>().ok()).unwrap();
    let v0_i64 = c_i64.get(0).ok().and_then(|v| v.try_extract::<i64>().ok()).unwrap();
    let v0_str = c_str
        .get(0)
        .ok()
        .and_then(|v| v.get_str().map(|s| s.to_string()))
        .unwrap();
    // vector extraction via AnyValue::List
    let v0_vec: Vec<f64> = match c_vec.get(0).unwrap() {
        AnyValue::List(s) => {
            let mut out = Vec::with_capacity(s.len());
            for i in 0..s.len() {
                let n = s.get(i).ok().and_then(|vv| vv.try_extract::<f64>().ok()).unwrap_or(0.0);
                out.push(n);
            }
            out
        }
        _ => Vec::new(),
    };

    assert!((v0_f64 - 1.5).abs() < 1e-9);
    assert_eq!(v0_i64, 42);
    assert_eq!(v0_str, "hello");
    assert_eq!(v0_vec, vec![1.0, 2.5, 3.0]);

    // Row 1 assertions
    let v1_f64 = c_f64.get(1).ok().and_then(|v| v.try_extract::<f64>().ok()).unwrap();
    let v1_i64 = c_i64.get(1).ok().and_then(|v| v.try_extract::<i64>().ok()).unwrap();
    let v1_str = c_str
        .get(1)
        .ok()
        .and_then(|v| v.get_str().map(|s| s.to_string()))
        .unwrap();
    let v1_vec: Vec<f64> = match c_vec.get(1).unwrap() {
        AnyValue::List(s) => {
            let mut out = Vec::with_capacity(s.len());
            for i in 0..s.len() {
                let n = s.get(i).ok().and_then(|vv| vv.try_extract::<f64>().ok()).unwrap_or(0.0);
                out.push(n);
            }
            out
        }
        _ => Vec::new(),
    };

    assert!((v1_f64 - 3.14).abs() < 1e-9);
    assert_eq!(v1_i64, 7);
    assert_eq!(v1_str, "9001");
    assert_eq!(v1_vec, vec![1.0, 2.0, 3.0]);
}

#[test]
fn test_array_type_schema_roundtrip_and_enforcement_int64() {
    // Ensure schema supports int64[] via [] syntax and writer enforces/coerces values
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let table = "clarium/security/users"; // regular table
    store.create_table(table).unwrap();

    // Declare an int array column and lock it
    store
        .schema_add(
            table,
            &[("roles".to_string(), DataType::List(Box::new(DataType::Int64)))],
        )
        .unwrap();

    // Write mixed inputs: JSON array and string-encoded array
    let mut r1 = serde_json::Map::new();
    r1.insert("roles".into(), json!([1, 2, 3]));
    let mut r2 = serde_json::Map::new();
    r2.insert("roles".into(), json!("[4,5,6]"));
    let recs = vec![
        Record { _time: 0, sensors: r1 },
        Record { _time: 0, sensors: r2 },
    ];
    store.write_records(table, &recs).unwrap();

    // Verify dtype and values
    let df = store.read_df(table).unwrap();
    let col = df.column("roles").unwrap();
    assert_eq!(col.dtype(), &DataType::List(Box::new(DataType::Int64)));
    // row 0
    if let AnyValue::List(s) = col.get(0).unwrap() {
        let mut v: Vec<i64> = Vec::new();
        for i in 0..s.len() { v.push(s.get(i).ok().and_then(|av| av.try_extract::<i64>().ok()).unwrap_or_default()); }
        assert_eq!(v, vec![1,2,3]);
    } else { panic!("expected list for row 0"); }
    // row 1
    if let AnyValue::List(s) = col.get(1).unwrap() {
        let mut v: Vec<i64> = Vec::new();
        for i in 0..s.len() { v.push(s.get(i).ok().and_then(|av| av.try_extract::<i64>().ok()).unwrap_or_default()); }
        assert_eq!(v, vec![4,5,6]);
    } else { panic!("expected list for row 1"); }

    // Verify schema.json uses [] syntax
    let sj = store.schema_path(table);
    let text = std::fs::read_to_string(&sj).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    let dtype = v.get("columns").and_then(|o| o.get("roles")).and_then(|x| x.as_str()).unwrap().to_string();
    assert!(dtype.eq_ignore_ascii_case("int64[]") || dtype.eq_ignore_ascii_case("vector"), "dtype was {}", dtype);
}

#[test]
fn test_array_type_schema_roundtrip_and_enforcement_string() {
    // Ensure schema supports string[] and we can write string arrays
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let table = "clarium/security/publications"; // regular table
    store.create_table(table).unwrap();

    store
        .schema_add(
            table,
            &[("tags".to_string(), DataType::List(Box::new(DataType::String)))],
        )
        .unwrap();

    let mut r1 = serde_json::Map::new();
    r1.insert("tags".into(), json!(["alpha", "beta"]));
    let mut r2 = serde_json::Map::new();
    r2.insert("tags".into(), json!("alpha, gamma"));
    let recs = vec![
        Record { _time: 0, sensors: r1 },
        Record { _time: 0, sensors: r2 },
    ];
    store.write_records(table, &recs).unwrap();

    let df = store.read_df(table).unwrap();
    let col = df.column("tags").unwrap();
    assert_eq!(col.dtype(), &DataType::List(Box::new(DataType::String)));
    if let AnyValue::List(s) = col.get(0).unwrap() {
        let mut v: Vec<String> = Vec::new();
        for i in 0..s.len() { v.push(s.get(i).ok().and_then(|av| av.get_str().map(|x| x.to_string())).unwrap_or_default()); }
        assert_eq!(v, vec!["alpha".to_string(), "beta".to_string()]);
    } else { panic!("expected list for row 0"); }
}

#[test]
fn test_all_column_types_roundtrip_time_table() {
    // Mirror the regular table test but on a time-series table to ensure
    // persistence includes `_time` and values round-trip correctly.
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();

    let table = "clarium/public/alltypes.time";
    store.create_table(table).unwrap();

    store
        .schema_add(
            table,
            &[
                ("a_f64".to_string(), DataType::Float64),
                ("a_i64".to_string(), DataType::Int64),
                ("a_str".to_string(), DataType::String),
                ("v_f64".to_string(), DataType::List(Box::new(DataType::Float64))),
            ],
        )
        .unwrap();

    let mut r1 = serde_json::Map::new();
    r1.insert("a_f64".into(), json!(10.25));
    r1.insert("a_i64".into(), json!(5));
    r1.insert("a_str".into(), json!("x"));
    r1.insert("v_f64".into(), json!([0.5, 1.5]));

    let mut r2 = serde_json::Map::new();
    r2.insert("a_f64".into(), json!("2.5"));
    r2.insert("a_i64".into(), json!("9"));
    r2.insert("a_str".into(), json!("y"));
    r2.insert("v_f64".into(), json!("4, 5, 6"));

    let recs = vec![
        Record { _time: 1_000, sensors: r1 },
        Record { _time: 2_000, sensors: r2 },
    ];

    store.write_records(table, &recs).unwrap();

    let df = store.read_df(table).unwrap();
    assert_eq!(df.height(), 2);
    // _time should be present
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "_time"));

    let t0 = df.column("_time").unwrap().i64().unwrap().get(0).unwrap();
    let t1 = df.column("_time").unwrap().i64().unwrap().get(1).unwrap();
    assert_eq!(t0, 1_000);
    assert_eq!(t1, 2_000);

    // Type checks
    assert_eq!(df.column("a_f64").unwrap().dtype(), &DataType::Float64);
    assert_eq!(df.column("a_i64").unwrap().dtype(), &DataType::Int64);
    assert_eq!(df.column("a_str").unwrap().dtype(), &DataType::String);
    assert_eq!(df.column("v_f64").unwrap().dtype(), &DataType::List(Box::new(DataType::Float64)));

    // Value checks row 0
    let f0 = df
        .column("a_f64").unwrap()
        .get(0).ok().and_then(|v| v.try_extract::<f64>().ok()).unwrap();
    let i0 = df
        .column("a_i64").unwrap()
        .get(0).ok().and_then(|v| v.try_extract::<i64>().ok()).unwrap();
    let s0 = df
        .column("a_str").unwrap()
        .get(0).ok().and_then(|v| v.get_str().map(|x| x.to_string())).unwrap();
    let v0: Vec<f64> = match df.column("v_f64").unwrap().get(0).unwrap() {
        AnyValue::List(s) => (0..s.len())
            .map(|i| s.get(i).ok().and_then(|vv| vv.try_extract::<f64>().ok()).unwrap_or(0.0))
            .collect(),
        _ => Vec::new(),
    };
    assert!((f0 - 10.25).abs() < 1e-9);
    assert_eq!(i0, 5);
    assert_eq!(s0, "x");
    assert_eq!(v0, vec![0.5, 1.5]);

    // Value checks row 1
    let f1 = df
        .column("a_f64").unwrap()
        .get(1).ok().and_then(|v| v.try_extract::<f64>().ok()).unwrap();
    let i1 = df
        .column("a_i64").unwrap()
        .get(1).ok().and_then(|v| v.try_extract::<i64>().ok()).unwrap();
    let s1 = df
        .column("a_str").unwrap()
        .get(1).ok().and_then(|v| v.get_str().map(|x| x.to_string())).unwrap();
    let v1: Vec<f64> = match df.column("v_f64").unwrap().get(1).unwrap() {
        AnyValue::List(s) => (0..s.len())
            .map(|i| s.get(i).ok().and_then(|vv| vv.try_extract::<f64>().ok()).unwrap_or(0.0))
            .collect(),
        _ => Vec::new(),
    };
    assert!((f1 - 2.5).abs() < 1e-9);
    assert_eq!(i1, 9);
    assert_eq!(s1, "y");
    assert_eq!(v1, vec![4.0, 5.0, 6.0]);
}
