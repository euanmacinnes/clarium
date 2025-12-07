use super::super::execute_query;
use crate::storage::{SharedStore, Store};

// These tests exercise metadata introspection paths similar to the failing Python test.
// They validate current behavior of SHOW TABLES and document that DESCRIBE <table>
// is not implemented yet (only DESCRIBE KEY exists), which explains the Python failure.

#[tokio::test]
async fn test_show_tables_lists_created_table() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create a simple regular table
    let table_fqn = "clarium/public/rust_meta_test";
    let _ = execute_query(&shared, &format!("CREATE TABLE {}", table_fqn))
        .await
        .unwrap();

    // SHOW TABLES should include it, returning an Array of objects with key `table_name`
    let val = execute_query(&shared, "SHOW TABLES").await.unwrap();
    let arr = val.as_array().expect("SHOW TABLES should return a JSON array");
    let names: std::collections::BTreeSet<String> = arr
        .iter()
        .filter_map(|v| v.get("table_name").and_then(|s| s.as_str()).map(|s| s.to_string()))
        .collect();
    assert!(names.contains("rust_meta_test"), "SHOW TABLES did not list the created table. Got: {:?}", names);

    // Parquet exists and is empty immediately after CREATE
    let df = { let g = shared.0.lock(); g.read_df("clarium/public/rust_meta_test").unwrap() };
    assert_eq!(df.height(), 0, "newly created table should have 0 rows");
}

#[tokio::test]
async fn test_describe_table_returns_schema_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create and populate a table so schema.json contains column dtypes
    let table_fqn = "clarium/public/rust_meta_desc";
    let _ = execute_query(&shared, &format!("CREATE TABLE {}", table_fqn))
        .await
        .unwrap();
    let _ = execute_query(&shared, r#"INSERT INTO clarium/public/rust_meta_desc (id, name, value) VALUES (1, 'A', 10), (2, 'B', 20)"#)
        .await
        .unwrap();

    // Verify parquet contents before DESCRIBE
    let dfp = { let g = shared.0.lock(); g.read_df(table_fqn).unwrap() };
    assert_eq!(dfp.height(), 2);
    let cols = dfp.get_column_names();
    for need in ["id","name","value"].iter() { assert!(cols.iter().any(|c| c.as_str()==*need), "missing col {}", need); }

    // DESCRIBE should return a tabular schema with a 'Column' header and rows for id,name,value
    let val = execute_query(&shared, "DESCRIBE rust_meta_desc").await.unwrap();
    // Expect array of objects with headers as keys; convert to cols/rows shape assumptions
    // Extract columns present in the describe output
    let arr = val.as_array().expect("DESCRIBE should return JSON array rows");
    assert!(!arr.is_empty(), "DESCRIBE returned no rows");
    // Ensure keys include 'Column'
    let first_obj = arr[0].as_object().expect("row must be object");
    assert!(first_obj.contains_key("Column"), "Describe output missing 'Column' header: keys={:?}", first_obj.keys());
    // Collect described column names
    let mut described: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for row in arr.iter() {
        if let Some(obj) = row.as_object() {
            if let Some(c) = obj.get("Column").and_then(|v| v.as_str()) {
                described.insert(c.to_string());
            }
        }
    }
    for expected in ["id", "name", "value"].iter() {
        assert!(described.contains(&expected.to_string()), "Missing column '{}' in describe output: {:?}", expected, described);
    }
}
