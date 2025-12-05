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
    let _ = crate::server::exec::execute_query(&shared, sql_create);
    let _ = crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_t_vec");

    // Status should exist
    let status = crate::server::exec::execute_query(&shared, "SHOW VECTOR INDEX STATUS clarium/public/idx_t_vec").unwrap();
    let arr = status.as_array().unwrap();
    assert_eq!(arr.len(), 1);

    // Inspect vindex and run a direct runtime search API
    let vf = read_vindex_file(&shared, "clarium/public/idx_t_vec").unwrap().unwrap();
    let q = vec![0.55f32, 0.0, 0.0];
    let res = exec_vector_runtime::search_vector_index(&shared, &vf, &q, 3).unwrap();
    assert!(!res.is_empty());
}
