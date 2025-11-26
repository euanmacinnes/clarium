use super::super::execute_query;
use crate::storage::{Store, SharedStore};
use polars::prelude::*;
use serde_json::json;

#[test]
fn test_inner_join_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create table a
    let a_id: Series = Series::new("id".into(), &[1i64, 2i64]);
    let a_val: Series = Series::new("aval".into(), &[10i64, 20i64]);
    let df_a = DataFrame::new(vec![a_id.clone().into(), a_val.clone().into()]).unwrap();
    store.rewrite_table_df("a", df_a).unwrap();
    // Create table b
    let b_id: Series = Series::new("id".into(), &[2i64, 3i64]);
    let b_val: Series = Series::new("bval".into(), &[200i64, 300i64]);
    let df_b = DataFrame::new(vec![b_id.clone().into(), b_val.clone().into()]).unwrap();
    store.rewrite_table_df("b", df_b).unwrap();

    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SELECT a.id, b.bval FROM a AS a INNER JOIN b AS b ON a.id = b.id";
    let v = futures::executor::block_on(async { execute_query(&shared, q).await }).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["a.id"], json!(2));
    assert_eq!(arr[0]["b.bval"], json!(200));
}



