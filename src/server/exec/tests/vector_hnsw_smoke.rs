#[cfg(feature = "ann_hnsw")]
mod hnsw_smoke {
    use crate::server::exec::exec_vector_index::read_vindex_file;
    use crate::server::exec::exec_vector_runtime;
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    fn seed_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
        let store = Store::new(tmp.path()).unwrap();
        let mut recs: Vec<Record> = Vec::new();
        // Simple 3-dim vectors along x-axis
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
    fn hnsw_build_search_l2_and_cosine() {
        super::super::udf_common::init_all_test_udfs();
        let tmp = tempfile::tempdir().unwrap();
        let table = "clarium/public/t";
        let shared = seed_table(&tmp, table);

        // L2 index
        let sql_create_l2 = "CREATE VECTOR INDEX idx_l2 ON clarium/public/t(vec) USING HNSW WITH (metric='l2', dim=3, M=8, ef_build=32)";
        let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create_l2));
        let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_l2"));
        let vf_l2 = read_vindex_file(&shared, "clarium/public/idx_l2").unwrap().unwrap();
        let q = vec![0.55f32, 0.0, 0.0];
        let res_l2 = exec_vector_runtime::search_vector_index(&shared, &vf_l2, &q, 3).unwrap();
        assert!(!res_l2.is_empty());

        // Cosine index
        let sql_create_cos = "CREATE VECTOR INDEX idx_cos ON clarium/public/t(vec) USING HNSW WITH (metric='cosine', dim=3, M=8, ef_build=32)";
        let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, sql_create_cos));
        let _ = futures::executor::block_on(crate::server::exec::execute_query(&shared, "BUILD VECTOR INDEX clarium/public/idx_cos"));
        let vf_cos = read_vindex_file(&shared, "clarium/public/idx_cos").unwrap().unwrap();
        let res_cos = exec_vector_runtime::search_vector_index(&shared, &vf_cos, &q, 3).unwrap();
        assert!(!res_cos.is_empty());

        // Ensure cosine scores are within [-1, 1]
        for (_id, s) in res_cos { assert!(s <= 1.0 + 1e-5 && s >= -1.0 - 1e-5); }
    }
}
