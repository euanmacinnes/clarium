use super::super::execute_query;
use crate::storage::{Store, SharedStore};
use polars::prelude::*;
use serde_json::json;

#[test]
fn test_left_join_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // table a: ids 1,2
    let df_a = DataFrame::new(vec![Series::new("id".into(), &[1i64, 2i64]).into(), Series::new("aval".into(), &[10i64, 20i64]).into()]).unwrap();
    store.rewrite_table_df("a", df_a).unwrap();
    // table b: ids 2,3
    let df_b = DataFrame::new(vec![Series::new("id".into(), &[2i64, 3i64]).into(), Series::new("bval".into(), &[200i64, 300i64]).into()]).unwrap();
    store.rewrite_table_df("b", df_b).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT a.id, b.bval FROM a AS a LEFT JOIN b AS b ON a.id = b.id ORDER BY a.id";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["a.id"], json!(1));
    assert!(arr[0]["b.bval"].is_null());
    assert_eq!(arr[1]["a.id"], json!(2));
    assert_eq!(arr[1]["b.bval"], json!(200));
}

#[test]
fn test_right_join_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // table a: ids 1,2
    let df_a = DataFrame::new(vec![Series::new("id".into(), &[1i64, 2i64]).into(), Series::new("aval".into(), &[10i64, 20i64]).into()]).unwrap();
    store.rewrite_table_df("a", df_a).unwrap();
    // table b: ids 2,3
    let df_b = DataFrame::new(vec![Series::new("id".into(), &[2i64, 3i64]).into(), Series::new("bval".into(), &[200i64, 300i64]).into()]).unwrap();
    store.rewrite_table_df("b", df_b).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT a.aval, b.id FROM a AS a RIGHT JOIN b AS b ON a.id = b.id ORDER BY b.id";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // ORDER BY b.id: row 0 is b.id=2 (matched with a.id=2), row 1 is b.id=3 (no match)
    assert_eq!(arr[0]["b.id"], json!(2));
    assert_eq!(arr[0]["a.aval"], json!(20));
    assert_eq!(arr[1]["b.id"], json!(3));
    assert!(arr[1]["a.aval"].is_null()); // b.id=3 has no a
}

#[test]
fn test_full_outer_join_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // table a: ids 1,2
    let df_a = DataFrame::new(vec![Series::new("id".into(), &[1i64, 2i64]).into(), Series::new("aval".into(), &[10i64, 20i64]).into()]).unwrap();
    store.rewrite_table_df("a", df_a).unwrap();
    // table b: ids 2,3
    let df_b = DataFrame::new(vec![Series::new("id".into(), &[2i64, 3i64]).into(), Series::new("bval".into(), &[200i64, 300i64]).into()]).unwrap();
    store.rewrite_table_df("b", df_b).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Use COALESCE in SELECT with alias, then ORDER BY the alias
    let q = "SELECT a.id, b.id, COALESCE(a.id, b.id) AS sort_key FROM a AS a FULL JOIN b AS b ON a.id = b.id ORDER BY sort_key";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    // Expect rows for ids 1,2,3
    assert_eq!(arr.len(), 3);
    // id 1: b.id is null
    assert_eq!(arr[0]["a.id"], json!(1));
    assert!(arr[0]["b.id"].is_null());
    assert_eq!(arr[0]["sort_key"], json!(1));
    // id 2: both present
    assert_eq!(arr[1]["a.id"], json!(2));
    assert_eq!(arr[1]["b.id"], json!(2));
    assert_eq!(arr[1]["sort_key"], json!(2));
    // id 3: a.id is null
    assert!(arr[2]["a.id"].is_null());
    assert_eq!(arr[2]["b.id"], json!(3));
    assert_eq!(arr[2]["sort_key"], json!(3));
}

#[test]
fn test_left_join_nonnequi() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // a: x 1,2 ; b: x 2,3
    let df_a = DataFrame::new(vec![Series::new("x".into(), &[1i64, 2i64]).into()]).unwrap();
    store.rewrite_table_df("a", df_a).unwrap();
    let df_b = DataFrame::new(vec![Series::new("x".into(), &[2i64, 3i64]).into()]).unwrap();
    store.rewrite_table_df("b", df_b).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // ON a.x > b.x should match (2,2?) no, 2>2 false; matches (2,? <2) none. So only left rows remain with null right.
    let q = "SELECT a.x, b.x AS bx FROM a AS a LEFT JOIN b AS b ON a.x > b.x ORDER BY a.x";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    // Since there are no matches, both rows should appear with bx null.
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["a.x"], json!(1));
    assert!(arr[0]["bx"].is_null());
    assert_eq!(arr[1]["a.x"], json!(2));
    assert!(arr[1]["bx"].is_null());
}



