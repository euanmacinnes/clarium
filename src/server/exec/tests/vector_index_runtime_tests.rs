use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::exec_vector_index::{read_vindex_file};
use crate::server::exec::exec_vector_runtime;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    // Simple 3-dim vectors
    for i in 0..10i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(i+1));
        m.insert("vec".into(), json!(format!("{},0,0", (i as f32)/10.0)));
        recs.push(Record { _time: 1_700_000_000_000 + i, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn build_and_search_vector_index_flat_v2() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let table = "clarium/public/t";
    let shared = seed_table(&tmp, table);

    // Create and build index
    let sql_create = "CREATE VECTOR INDEX idx_t_vec ON clarium/public/t(vec) USING HNSW WITH (metric='l2', dim=3, M=16, ef_build=64)";
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create));
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_t_vec"));

    // Status should exist
    let status = futures::executor::block_on(crate::server::exec::execute_query(&shared, "SHOW VECTOR INDEX STATUS clarium/public/idx_t_vec")).unwrap();
    let arr = status.as_array().unwrap();
    assert_eq!(arr.len(), 1);

    // Inspect vindex and run a direct runtime search API
    let vf = read_vindex_file(&shared, "clarium/public/idx_t_vec").unwrap().unwrap();
    let q = vec![0.55f32, 0.0, 0.0];
    let res = exec_vector_runtime::search_vector_index(&shared, &vf, &q, 3).unwrap();
    assert!(!res.is_empty());
}

#[test]
fn build_with_dim_policy_skip_and_error_behavior() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let table = "clarium/public/t_bad";
    let store = Store::new(tmp.path()).unwrap();
    // Seed rows with mixed quality: correct 3-dim, wrong dim, and invalid
    let mut recs: Vec<Record> = Vec::new();
    // 4 good rows
    for i in 0..4i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(i+1));
        m.insert("vec".into(), json!(format!("{},{},{}", i as f32, 0.0f32, 1.0f32)));
        recs.push(Record { _time: 1_700_000_000_000 + i, sensors: m });
    }
    // 2 rows with wrong dimension (2-dim)
    for i in 0..2i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(100 + i));
        m.insert("vec".into(), json!(format!("{},{}", i as f32, 0.0f32)));
        recs.push(Record { _time: 1_700_000_010_000 + i, sensors: m });
    }
    // 2 invalid rows (non-numeric in list)
    for i in 0..2i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(200 + i));
        m.insert("vec".into(), json!("[1, \"x\", 3]"));
        recs.push(Record { _time: 1_700_000_020_000 + i, sensors: m });
    }
    store.write_records(table, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create with dim=3 and dim_policy='skip' → build should succeed and index only the 4 valid rows
    let sql_create = "CREATE VECTOR INDEX idx_t_bad ON clarium/public/t_bad(vec) USING HNSW WITH (metric='l2', dim=3, dim_policy='skip')";
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create)).unwrap();
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_t_bad")).unwrap();
    let status = futures::executor::block_on(crate::server::exec::execute_query(&shared, "SHOW VECTOR INDEX STATUS clarium/public/idx_t_bad")).unwrap();
    let arr = status.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let row = &arr[0];
    assert_eq!(row.get("dim").and_then(|x| x.as_i64()).unwrap(), 3);
    let rows_indexed = row.get("rows_indexed").and_then(|x| x.as_u64()).unwrap();
    assert!(rows_indexed <= 4, "rows_indexed should be <= valid rows (4), got {}", rows_indexed);

    // Recreate with dim_policy='error' → BUILD should return an error (graceful, not panic)
    let sql_create2 = "CREATE VECTOR INDEX idx_t_bad2 ON clarium/public/t_bad(vec) USING HNSW WITH (metric='l2', dim=3, dim_policy='error')";
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create2)).unwrap();
    let res = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_t_bad2"));
    assert!(res.is_err(), "BUILD should error under dim_policy=error");
}

#[test]
fn query_time_dimension_mismatch_returns_error() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let table = "clarium/public/t2";
    let shared = seed_table(&tmp, table);
    // Create and build a 3-dim index
    let sql_create = "CREATE VECTOR INDEX idx_t2_vec ON clarium/public/t2(vec) USING HNSW WITH (metric='l2', dim=3)";
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create));
    let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_t2_vec"));
    let vf = read_vindex_file(&shared, "clarium/public/idx_t2_vec").unwrap().unwrap();
    // Use a 2-dim query → expect an error, not a panic
    let q = vec![0.5f32, 0.1f32];
    let err = exec_vector_runtime::search_vector_index(&shared, &vf, &q, 3).err();
    assert!(err.is_some());
}
