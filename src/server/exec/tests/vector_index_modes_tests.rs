use futures::executor::block_on;
use crate::server::exec::tests::fixtures::*;

#[test]
fn create_show_alter_modes_and_status_contains_mode() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    seed_docs_with_embeddings(&store, "clarium/public/docs");

    // Create with explicit mode=BATCHED (should be normalized uppercase)
    let sql = "CREATE VECTOR INDEX idx_docs_body ON clarium/public/docs(body_embed) USING HNSW WITH (metric='l2', dim=3, mode='batched')";
    block_on(crate::server::exec::execute_query(&store, sql)).unwrap();

    // SHOW VECTOR INDEX should include mode
    let js = block_on(crate::server::exec::execute_query(&store, "SHOW VECTOR INDEX idx_docs_body")).unwrap();
    let arr = js.as_array().cloned().unwrap();
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_object().unwrap();
    assert_eq!(row.get("metric").unwrap().as_str().unwrap(), "l2");
    assert_eq!(row.get("dim").unwrap().as_i64().unwrap(), 3);
    if let Some(m) = row.get("mode").and_then(|v| v.as_str()) { assert!(["BATCHED","IMMEDIATE","ASYNC","REBUILD_ONLY"].contains(&m)); }

    // Alter mode to IMMEDIATE
    block_on(crate::server::exec::execute_query(&store, "ALTER VECTOR INDEX idx_docs_body SET MODE IMMEDIATE")).unwrap();

    // Status should surface mode as IMMEDIATE
    let status = block_on(crate::server::exec::execute_query(&store, "SHOW VECTOR INDEX STATUS clarium/public/idx_docs_body")).unwrap();
    let arr = status.as_array().cloned().unwrap();
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_object().unwrap();
    if let Some(m) = row.get("mode").and_then(|v| v.as_str()) { assert_eq!(m, "IMMEDIATE"); }
}
