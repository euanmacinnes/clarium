use crate::query::{self, Command};
use crate::server::exec::exec_views::read_view_file;
use crate::server::exec::exec_vector_index::VIndexFile;
use crate::server::exec::exec_select::run_select;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_simple_table(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(i));
        m.insert("body_embed".into(), json!("0.1,0.0,0.0"));
        recs.push(Record { _time: i, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

fn read_vindex_sidecar(store: &SharedStore, qualified: &str) -> Option<VIndexFile> {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("vindex");
    let text = std::fs::read_to_string(&p).ok()?;
    serde_json::from_str::<VIndexFile>(&text).ok()
}

#[test]
fn create_show_drop_vector_index_happy() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs"; // qualified path style
    let shared = seed_simple_table(&tmp, db);

    // CREATE VECTOR INDEX
    let sql = "CREATE VECTOR INDEX idx_docs_body ON clarium/public/docs(body_embed) USING hnsw WITH (metric='l2', dim=3)";
    let cmd = query::parse(sql).unwrap();
    let out = crate::server::exec::execute_query(&shared, sql);
    assert!(out.is_ok(), "CREATE VECTOR INDEX failed: {:?}", out.err());

    // SHOW VECTOR INDEX
    let show = crate::server::exec::execute_query(&shared, "SHOW VECTOR INDEX idx_docs_body").unwrap();
    let arr = show.as_array().cloned().unwrap();
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_object().unwrap();
    assert_eq!(row.get("algo").unwrap().as_str().unwrap(), "hnsw");
    assert_eq!(row.get("metric").unwrap().as_str().unwrap(), "l2");
    assert_eq!(row.get("dim").unwrap().as_i64().unwrap(), 3);

    // Sidecar exists
    let vf = read_vindex_sidecar(&shared, "clarium/public/idx_docs_body").unwrap();
    assert_eq!(vf.table, "clarium/public/docs");
    assert_eq!(vf.column, "body_embed");

    // DROP VECTOR INDEX
    crate::server::exec::execute_query(&shared, "DROP VECTOR INDEX idx_docs_body").unwrap();
    let dropped = read_vindex_sidecar(&shared, "clarium/public/idx_docs_body").is_none();
    assert!(dropped);
}

#[test]
fn create_vector_index_missing_table_and_duplicate() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Missing table should error
    let sql = "CREATE VECTOR INDEX idx_x ON clarium/public/missing(body_embed) USING hnsw WITH (metric='cosine', dim=3)";
    let err = crate::server::exec::execute_query(&shared, sql).err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.to_lowercase().contains("table not found"));

    // Seed table and create
    let _ = seed_simple_table(&tmp, "clarium/public/docs");
    crate::server::exec::execute_query(&shared, "CREATE VECTOR INDEX idx1 ON clarium/public/docs(body_embed) USING hnsw WITH (metric='cosine', dim=3)").unwrap();
    // Duplicate name should conflict
    let err = crate::server::exec::execute_query(&shared, "CREATE VECTOR INDEX idx1 ON clarium/public/docs(body_embed) USING hnsw WITH (metric='cosine', dim=3)").err().unwrap();
    assert!(format!("{}", err).to_lowercase().contains("already exists"));
}

#[test]
fn show_vector_indexes_enumerates() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = seed_simple_table(&tmp, "clarium/public/docs");
    crate::server::exec::execute_query(&shared, "CREATE VECTOR INDEX idxa ON clarium/public/docs(body_embed) USING hnsw WITH (metric='cosine', dim=3)").unwrap();
    crate::server::exec::execute_query(&shared, "CREATE VECTOR INDEX idxb ON clarium/public/docs(body_embed) USING hnsw WITH (metric='l2', dim=3)").unwrap();
    let json = crate::server::exec::execute_query(&shared, "SHOW VECTOR INDEXES").unwrap();
    let arr = json.as_array().cloned().unwrap();
    assert!(arr.len() >= 2);
}
