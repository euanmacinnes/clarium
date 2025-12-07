use super::*;
use tempfile::tempdir;
use crate::storage::{SharedStore, KvValue};

#[tokio::test]
async fn commit_parent_inference_works() {
    let tmp = tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium";
    let fs = "docs";
    let kv = store.kv_store(db, fs);

    // Seed two live files
    let now = chrono::Utc::now().timestamp();
    for (i, name) in ["a.txt", "b.txt"].iter().enumerate() {
        let meta = FileMeta {
            id: uuid::Uuid::new_v4().to_string(),
            logical_path: name.to_string(),
            size: (i as u64) + 1,
            etag: format!("etag{i}"),
            version: 1,
            created_at: now,
            updated_at: now,
            content_type: Some("text/plain".to_string()),
            deleted: false,
            description_html: None,
            custom: None,
            chunking: None,
        };
        let key = super::Keys::path(db, fs, &meta.logical_path);
        kv.set(key, KvValue::Json(serde_json::to_value(&meta).unwrap()), None, None);
    }

    // Create a tree from all files
    let tree1 = create_tree_from_prefix(&store, db, fs, None).unwrap();

    // Commit 1 without explicit parents → should have empty parents
    let author = CommitAuthor { name: "u".into(), email: "u@l".into(), time_unix: now };
    let c1 = commit_tree(&store, db, fs, &tree1.id, &[], &author, "init", &[], "main").unwrap();
    assert_eq!(c1.parents.len(), 0);

    // New file to change tree
    let meta_new = FileMeta {
        id: uuid::Uuid::new_v4().to_string(),
        logical_path: "c.txt".to_string(),
        size: 3,
        etag: "etag2".to_string(),
        version: 1,
        created_at: now,
        updated_at: now,
        content_type: Some("text/plain".to_string()),
        deleted: false,
        description_html: None,
        custom: None,
        chunking: None,
    };
    let k3 = super::Keys::path(db, fs, &meta_new.logical_path);
    kv.set(k3, KvValue::Json(serde_json::to_value(&meta_new).unwrap()), None, None);

    let tree2 = create_tree_from_prefix(&store, db, fs, None).unwrap();

    // Commit 2 without explicit parents → should infer c1.id
    let c2 = commit_tree(&store, db, fs, &tree2.id, &[], &author, "second", &[], "main").unwrap();
    assert_eq!(c2.parents.len(), 1);
    assert_eq!(c2.parents[0], c1.id);
}
