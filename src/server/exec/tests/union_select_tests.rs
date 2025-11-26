use super::super::{execute_query};
use crate::storage::SharedStore;
use serde_json::json;

#[test]
fn test_union_all_sourceless() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT 1 AS a UNION ALL SELECT 2 AS a";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    // Expect two rows with column a
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["a"], json!(1.0));
    assert_eq!(arr[1]["a"], json!(2.0));
}

#[test]
fn test_union_distinct_sourceless() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT 1 AS a UNION SELECT 1 AS a";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["a"], json!(1.0));
}

#[test]
fn test_union_column_alignment() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT 1 AS a UNION ALL SELECT 2 AS b";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // First row has a=1, b=null
    assert_eq!(arr[0]["a"], json!(1.0));
    assert!(arr[0]["b"].is_null());
    // Second row has a=null, b=2
    assert!(arr[1]["a"].is_null());
    assert_eq!(arr[1]["b"], json!(2.0));
}



